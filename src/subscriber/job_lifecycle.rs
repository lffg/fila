use sqlx::{FromRow, PgPool, Postgres, Transaction};
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::{instrument, trace};
use uuid::Uuid;

use crate::{
    error::{Result, ResultExt},
    job,
    subscriber::magic::{self, JobRegistry},
    PG_TOPIC_NAME,
};

type PgTx<'c> = Transaction<'c, Postgres>;

#[derive(Debug, FromRow)]
struct JobRow {
    id: Uuid,
    name: String,
    payload: String,
    attempts: i16,
}

pub enum LifecycleStatus {
    /// Processed the job.
    Processed,
    /// Didn't found any jobs while attempting to acquire one.
    NotFound,
}

#[instrument(fields(job_id), skip_all)]
pub async fn run_lifecycle(
    cancellation_token: CancellationToken,
    pool: &PgPool,
    job_registry: &JobRegistry,
    queue_name: &str,
) -> Result<LifecycleStatus> {
    trace!("acquiring job");
    let Some(job) = acquire_available_job(&queue_name, &pool).await? else {
        return Ok(LifecycleStatus::NotFound);
    };

    // XX: Could we *not* use to_string here?
    tracing::Span::current().record("job_id", job.id.to_string());

    trace!("executing job");
    dispatch_and_exec(cancellation_token.clone(), &pool, &job_registry, &job).await?;

    trace!("finished job lifecycle");

    Ok(LifecycleStatus::Processed)
}

/// Fetches an available job and acquires it (i.e., mark the row as "processing"
/// so that no other subscribers run it). Also increments the job's attempt
/// count.
///
/// All errors are properly treated by this function. We just use the error
/// variant to make the caller exit early in case of any errors.
async fn acquire_available_job(queue: &str, pool: &PgPool) -> Result<Option<JobRow>> {
    let mut tx = pool
        .begin()
        .await
        .with_ctx("failed to acquire transaction")?;

    let Some(job) = fetch_available(queue, &mut tx).await? else {
        return Ok(None);
    };

    mark_as_processing_and_increment_attempts(job.id, &mut tx).await?;

    tx.commit().await.with_ctx("failed to commit transaction")?;

    Ok(Some(job))
}

async fn fetch_available<'c>(queue: &str, tx: &mut PgTx<'c>) -> Result<Option<JobRow>> {
    sqlx::query_as(
        r#"
        SELECT id, name, payload::TEXT, attempts FROM fila.jobs
            WHERE queue = $1 AND state = $2
            FOR UPDATE SKIP LOCKED
            LIMIT 1;
        "#,
    )
    .bind(queue)
    .bind(job::State::Available)
    .fetch_optional(&mut **tx)
    .await
    .with_ctx("failed to fetch available job")
}

async fn mark_as_processing_and_increment_attempts<'c>(id: Uuid, tx: &mut PgTx<'c>) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE fila.jobs
            SET state = $2, attempts = attempts + 1
            WHERE id = $1;
        "#,
    )
    .bind(id)
    .bind(job::State::Processing)
    .execute(&mut **tx)
    .await
    .with_ctx("failed to mark job as processing")?;
    Ok(())
}

async fn dispatch_and_exec(
    _cancellation_token: CancellationToken,
    pool: &PgPool,
    job_registry: &JobRegistry,
    job: &JobRow,
) -> Result<()> {
    // TODO: Better error handling.

    let current_attempt = u16::try_from(job.attempts).unwrap();
    let ctx = magic::ErasedJobContext {
        name: &job.name,
        encoded_payload: &job.payload,
        attempt: current_attempt,
    };
    let result = job_registry.dispatch_and_exec(ctx).await;

    let finish_state = match result {
        Ok(Ok(_)) => {
            trace!("job successful");
            job::State::Successful
        }
        Ok(Err(job::Error {
            error,
            kind: job::ErrorKind::Cancellation,
        })) => {
            error!(?error, "job cancelled");
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
            } = job_registry.config_for(&job.name);

            if current_attempt < max_attempts {
                error!(
                    ?error,
                    "job failed attempt ({current_attempt}/{max_attempts})"
                );
                let new_attempt = current_attempt + 1;
                trace!("scheduling new attempt ({new_attempt}/{max_attempts})");
                sqlx::query(
                    r#"
                    WITH upd AS (UPDATE fila.jobs SET state = $1 WHERE id = $2)
                    SELECT pg_notify($3, 'q:' || $4);
                    "#,
                )
                .bind(job::State::Available)
                .bind(&job.id)
                .bind(PG_TOPIC_NAME)
                .bind(queue)
                .execute(pool)
                .await
                .unwrap();

                return Ok(());
            } else {
                error!(
                    ?error,
                    "job failed final attempt ({current_attempt}/{max_attempts})"
                );
                job::State::Cancelled
            }
        }
        Err(error) => {
            error!(?error, "failed to dispatch job (lib side)");
            return Ok(());
        }
    };

    sqlx::query(r"UPDATE fila.jobs SET state = $2, finished_at = now() WHERE id = $1;")
        .bind(&job.id)
        .bind(finish_state)
        .execute(pool)
        .await
        .unwrap();

    Ok(())
}
