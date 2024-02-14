use serde::Serialize;

use crate::{
    error::{Result, ResultExt},
    job::{Config, Job},
    PG_TOPIC_NAME,
};

const INITIAL_ATTEMPT: i16 = 0;

/// Saves the given job on the database and notifies the interest subscribers.
///
/// # Errors
///
/// Fails if the job couldn't be persisted on the database.
pub async fn send<'c, J, E>(payload: J, db: E) -> Result<()>
where
    J: Job + Serialize,
    E: sqlx::PgExecutor<'c>,
{
    let payload = serde_json::to_string(&payload).with_ctx("failed to serialize job payload")?;
    let config = J::config();
    send_impl(db, &payload, J::NAME, &config).await
}

async fn send_impl<'c, E>(db: E, payload: &str, job_name: &str, config: &Config) -> Result<()>
where
    E: sqlx::PgExecutor<'c>,
{
    sqlx::query(
        r"
        WITH insert AS (
            INSERT INTO fila.jobs
                    (id, queue, name, payload, attempts, state, scheduled_at)
            VALUES ($1, $2, $3, $4::JSONB, $5, 'available', now())
        )
        SELECT pg_notify($6, 'q:' || $2);
        ",
    )
    .bind(uuid::Uuid::now_v7())
    .bind(config.queue)
    .bind(job_name)
    .bind(payload)
    .bind(INITIAL_ATTEMPT)
    .bind(PG_TOPIC_NAME)
    .execute(db)
    .await
    .with_ctx("failed to insert new job")?;
    Ok(())
}
