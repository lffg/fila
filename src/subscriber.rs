use sqlx::{postgres::PgListener, prelude::FromRow, PgExecutor, PgPool};
use tracing::{error, instrument, trace, warn};

use crate::{
    error::{Result, ResultExt},
    job, PG_TOPIC_NAME,
};

use self::builder::SubscriberBuilder;

pub mod builder;

pub struct Subscriber {
    inner: SubscriberBuilder,
}

impl Subscriber {
    pub fn builder() -> SubscriberBuilder {
        SubscriberBuilder::new()
    }

    pub fn with_pool(self, pool: PgPool) -> SubscriberListener {
        SubscriberListener {
            inner: self.inner,
            pool,
        }
    }
}

pub struct SubscriberListener {
    inner: SubscriberBuilder,
    pool: PgPool,
}

impl SubscriberListener {
    pub async fn listen(&mut self) -> Result<()> {
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

        // TODO: Spawn task to consume "abandoned" available jobs.
        self.run_listener(&mut listener).await;

        Ok(())
    }

    async fn run_listener(&mut self, listener: &mut PgListener) {
        trace!(topic = PG_TOPIC_NAME, "listening");
        loop {
            match listener.recv().await {
                Ok(event) => {
                    let payload = event.payload();
                    trace!(?payload, "received listener message");
                    let Some(queue_name) = self.parse_queue_name(payload) else {
                        trace!("unrecognized message format, skipping");
                        continue;
                    };
                    self.handle_message(queue_name).await;
                }
                Err(error) => {
                    warn!(?error, "failed to receive listener message");
                }
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

        // exec job
        // =====================================================================
        trace!(?id, "job in progress");
        let job_handle = &self.inner.jobs[&*row.name];
        let current_attempt = u16::try_from(row.attempts).unwrap();
        let ctx = builder::ContextWithoutState {
            attempt: current_attempt,
        };
        let job_result = job_handle
            .run(&row.payload, ctx, &self.inner.state_map)
            .await;

        let finish_state = match job_result {
            Ok(_) => {
                trace!(?id, "job successful");
                job::State::Successful
            }
            Err(job::Error {
                error,
                kind: job::ErrorKind::Cancellation,
            }) => {
                error!(?id, ?error, "job cancelled");
                job::State::Cancelled
            }
            Err(job::Error {
                error,
                kind: job::ErrorKind::Failure,
            }) => {
                let &job::Config {
                    max_attempts,
                    queue,
                    ..
                } = job_handle.config();

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
        self.inner.queue_names.contains(name).then_some(name)
    }
}
