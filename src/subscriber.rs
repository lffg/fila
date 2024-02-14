use std::collections::HashSet;

use sqlx::{
    postgres::{PgListener, PgNotification},
    prelude::FromRow,
    PgExecutor, PgPool,
};
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::{error, instrument, trace, warn};

use crate::{
    error::{Result, ResultExt},
    job, PG_TOPIC_NAME,
};

// Public modules.
mod builder;
pub use builder::Builder;

// Private modules.
mod magic;

pub struct Subscriber {
    job_registry: magic::JobRegistry,
    queue_names: HashSet<&'static str>,
    pool: PgPool,
    cancellation_token: CancellationToken,
}

impl Subscriber {
    /// Returns a new [subscriber builder](Builder). Consult its module-level
    /// documentation for more information.
    pub fn builder() -> Builder {
        Builder::new()
    }

    /// Starts all of the Fila's background services related to the subscriber.
    ///
    /// The future returned by this method runs indefinitely until the
    /// cancellation token provided by [`Builder::with_cancellation_token`]
    /// gets fulfilled.
    ///
    /// Fails to start if the database pool can't establish a connection or the
    /// PostgreSQL listener fails to start. Errors related to jobs are
    /// internally handled and exposed through the error-reporting facilities
    /// provided by Fila.
    pub async fn start(self) -> Result<()> {
        // Tries to connect to the database to minimize connection errors next
        sqlx::query("SELECT 1;")
            .fetch_one(&self.pool)
            .await
            .with_ctx("failed to connect to the database")?;

        let mut listener = PgListener::connect_with(&self.pool)
            .await
            .with_ctx("failed to create postgres listener")?;
        listener
            .listen(PG_TOPIC_NAME)
            .await
            .with_ctx("failed to listen for postgres topic")?;

        let mut imp = SubscriberImpl {
            job_registry: self.job_registry,
            queue_names: self.queue_names,
            cancellation_token: self.cancellation_token,
            pool: self.pool,
        };

        imp.listen(&mut listener).await;
        Ok(())
    }
}

struct SubscriberImpl {
    job_registry: magic::JobRegistry,
    queue_names: HashSet<&'static str>,
    cancellation_token: CancellationToken,
    pool: PgPool,
}

impl SubscriberImpl {
    async fn listen(&mut self, listener: &mut PgListener) {
        trace!(topic = PG_TOPIC_NAME, "listening");
        loop {
            select! {
                notification = listener.recv() => {
                    self.handle_pg_notification(notification).await;
                },
                () = self.cancellation_token.cancelled() => {
                    trace!("shutting subscriber down");
                    break;
                },
            }
        }
    }

    async fn handle_pg_notification(&mut self, notification: Result<PgNotification, sqlx::Error>) {
        match notification {
            Ok(event) => {
                let payload = event.payload();
                trace!(?payload, "received listener message");
                let Some(queue_name) = self.parse_queue_name(payload) else {
                    trace!("unrecognized message format, skipping");
                    return;
                };
                self.handle_message(queue_name).await;
            }
            Err(error) => {
                warn!(?error, "failed to receive listener message");
            }
        }
    }

    #[instrument(level = "debug", skip(self))]
    async fn handle_message(&self, queue_name: &str) {
        // TODO: Refac this mess
        // TODO: Remove unwraps
        // TODO: Move this to worker
        // TODO: Handle concurrency limit

        // get job
        // =====================================================================
        let row = {
            let mut tx = self.pool.begin().await.expect("should begin tx");

            #[derive(FromRow, Debug)]
            struct Row {
                id: uuid::Uuid,
                name: String,
                payload: String,
                attempts: i16,
            }
            let row: Option<Row> = sqlx::query_as(
                r#"
                SELECT id, name, payload::TEXT, attempts FROM fila.jobs
                WHERE queue = $1 AND state = $2
                FOR UPDATE SKIP LOCKED
                LIMIT 1;
                "#,
            )
            .bind(queue_name)
            .bind(job::State::Available)
            .fetch_optional(&mut *tx)
            .await
            .unwrap();

            let Some(row) = row else {
                return;
            };

            sqlx::query(
                r#"
                UPDATE fila.jobs
                    SET state = $2, attempts = attempts + 1
                    WHERE id = $1;
                "#,
            )
            .bind(row.id)
            .bind(job::State::Processing)
            .execute(&mut *tx)
            .await
            .unwrap();

            tx.commit().await.expect("failed to commit tx");
            row
        };
        let id = row.id;
        let job_name = &row.name;

        // exec job
        // =====================================================================
        trace!(?id, "job in progress");
        let current_attempt = u16::try_from(row.attempts).unwrap();
        let ctx = magic::ErasedJobContext {
            name: job_name,
            encoded_payload: &row.payload,
            attempt: current_attempt,
        };
        let result = self.job_registry.dispatch_and_exec(ctx).await;

        let finish_state = match result {
            Ok(Ok(_)) => {
                trace!(?id, "job successful");
                job::State::Successful
            }
            Ok(Err(job::Error {
                error,
                kind: job::ErrorKind::Cancellation,
            })) => {
                error!(?id, ?error, "job cancelled");
                job::State::Cancelled
            }
            Ok(Err(job::Error {
                error,
                kind: job::ErrorKind::Failure,
            })) => {
                let &job::Config {
                    max_attempts,
                    queue,
                    ..
                } = self.job_registry.config_for(job_name);

                if current_attempt < max_attempts {
                    error!(
                        ?id,
                        ?error,
                        "job failed attempt ({current_attempt}/{max_attempts})"
                    );
                    let new_attempt = current_attempt + 1;
                    trace!(?id, "scheduling new attempt ({new_attempt}/{max_attempts})");
                    sqlx::query(
                        r#"
                        WITH upd AS (UPDATE fila.jobs SET state = $1 WHERE id = $2)
                        SELECT pg_notify($3, 'q:' || $4);
                        "#,
                    )
                    .bind(job::State::Available)
                    .bind(id)
                    .bind(PG_TOPIC_NAME)
                    .bind(queue)
                    .execute(&self.pool)
                    .await
                    .unwrap();

                    // It's not a finished state, so we return
                    return;
                } else {
                    error!(
                        ?id,
                        ?error,
                        "job failed final attempt ({current_attempt}/{max_attempts})"
                    );
                    job::State::Cancelled
                }
            }
            Err(error) => {
                error!(?error, "failed to dispatch job (lib side)");
                return;
            }
        };
        Self::set_job_finished_state(row.id, finish_state, &self.pool)
            .await
            .unwrap();
    }

    /// Updates the job with the given state.
    async fn set_job_finished_state<'c>(
        id: uuid::Uuid,
        new_state: job::State,
        db: impl PgExecutor<'c>,
    ) -> sqlx::Result<()> {
        sqlx::query(r"UPDATE fila.jobs SET state = $2, finished_at = now() WHERE id = $1;")
            .bind(id)
            .bind(new_state)
            .execute(db)
            .await?;
        Ok(())
    }

    // XX: Maybe create a `QueueName` type which implements SQLx's `Encode` and
    // `Decode` traits.
    fn parse_queue_name<'a>(&self, message: &'a str) -> Option<&'a str> {
        let name = message.strip_prefix("q:")?;
        self.queue_names.contains(name).then_some(name)
    }
}
