use std::cmp::Ordering;

use sqlx::{FromRow, PgPool, Postgres, Transaction};
use tracing::{instrument, trace};
use uuid::Uuid;

use crate::{
    error::{InternalError, JobExecutionError, ResultExt},
    job,
    subscriber::magic::{self, JobRegistry},
    sync::CancellationNotify,
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

#[instrument(
    level = "error",
    fields(job_id, current_attempt, max_attempts),
    skip_all
)]
pub async fn run_lifecycle(
    cancellation_notify: CancellationNotify,
    pool: &PgPool,
    job_registry: &JobRegistry,
    queue_name: &str,
) -> Result<LifecycleStatus, InternalError> {
    trace!("acquiring job");
    let Some(job) = acquire_available_job(queue_name, pool).await? else {
        return Ok(LifecycleStatus::NotFound);
    };

    // XX: Could we *not* use to_string here?
    tracing::Span::current().record("job_id", job.id.to_string());

    trace!("executing job");
    let execution_result =
        execute_and_mark(cancellation_notify.clone(), pool, job_registry, &job).await?;
    if let Err(error) = execution_result {
        // TODO: Report error back to the user.
        tracing::error!(?error, "job execution error");
    }

    trace!("finished job lifecycle");
    Ok(LifecycleStatus::Processed)
}

/// Fetches an available job and acquires it (i.e., mark the row as "processing"
/// so that no other subscribers run it).
async fn acquire_available_job(
    queue: &str,
    pool: &PgPool,
) -> Result<Option<JobRow>, InternalError> {
    let mut tx = pool
        .begin()
        .await
        .map_err_into(InternalError::DatabaseFailedToAcquireTransaction)?;

    let Some(job) = fetch_available(queue, &mut tx).await? else {
        return Ok(None);
    };

    mark_as_processing(job.id, &mut tx).await?;

    tx.commit()
        .await
        .map_err_into(InternalError::DatabaseFailedToCommitTransaction)?;

    Ok(Some(job))
}

async fn fetch_available(queue: &str, tx: &mut PgTx<'_>) -> Result<Option<JobRow>, InternalError> {
    sqlx::query_as(
        r"
        SELECT id, name, payload::TEXT, attempts FROM fila.jobs
            WHERE queue = $1 AND state = $2
            FOR UPDATE SKIP LOCKED
            LIMIT 1;
        ",
    )
    .bind(queue)
    .bind(job::State::Available)
    .fetch_optional(&mut **tx)
    .await
    .map_err_into(InternalError::DatabaseFailedToFetchJob)
}

async fn mark_as_processing(id: Uuid, tx: &mut PgTx<'_>) -> Result<(), InternalError> {
    sqlx::query(r"UPDATE fila.jobs SET state = $2 WHERE id = $1;")
        .bind(id)
        .bind(job::State::Processing)
        .execute(&mut **tx)
        .await
        .map_err(|error| {
            InternalError::DatabaseFailedToMarkJob(error.into(), job::State::Processing)
        })?;
    Ok(())
}

/// Executes the job and marks the resulting state on the database.
///
/// See remarks on [`handle_job_execution_result`].
async fn execute_and_mark(
    cancellation_notify: CancellationNotify,
    pool: &PgPool,
    job_registry: &JobRegistry,
    job: &JobRow,
) -> Result<Result<(), JobExecutionError>, InternalError> {
    let previous_attempts = u16::try_from(job.attempts).unwrap_or_else(|error| {
        trace!(?error, "invalid `job.attempts` value");
        i16::MAX.try_into().unwrap() // Infallible
    });

    let &job::Config {
        max_attempts,
        queue,
        ..
    } = job_registry.config_for(&job.name);

    // We only store the number of already executed attempts on the database;
    // i.e., the current one is only stored *after* the job finishes executing,
    // so we need to manually increment it here to define the *current* job
    // execution number.
    let current_attempt = previous_attempts + 1;

    let span = tracing::Span::current();
    span.record("current_attempt", current_attempt);
    span.record("max_attempts", max_attempts);

    let ctx = magic::ErasedJobContext {
        name: &job.name,
        encoded_payload: &job.payload,
        attempt: current_attempt,
        cancellation_notify,
    };

    let result = job_registry.dispatch_and_exec(ctx).await;

    let (new_state, result) = handle_job_execution_result(current_attempt, max_attempts, result);
    save_execution_state(job.id, new_state, queue, pool).await?;
    result
}

/// Handles the job execution result.
///
/// Returns the state that must be applied to the job's database record.
///
/// Also returns a nested result. The outer result represents the library-level
/// "operational result" of the job's lifecycle; the inner result represents the
/// actual user's job execution's result.
///
/// A failure in the outer result *may* lead to an invalid job's state. In such
/// abnormal cases, the database may become temporarily inconsistent with the
/// actual job's execution state until Fila's Maintainer task recovers it,
/// possibly scheduling a new execution under normal conditions. For example,
/// this kind of situation may happen if the database becomes inaccessible after
/// the job starts being executed. In this case, when the execution finishes,
/// Fila wouldn't be able to persist the appropriate state on the job's row
/// (e.g. successful, cancelled, etc), thus making the database state
/// temporarily inconsistent.
///
/// A failure in the inner result simply means that the job execution failed,
/// being guaranteed to not leave the database in an inconsistent state. This
/// inner result is only returned by this function to communicate to its caller
/// that it *must* notify the user that some kind of failure happened during
/// some of their job's execution.
fn handle_job_execution_result(
    current_attempt: u16,
    max_attempts: u16,
    result: Result<Result<(), job::Error>, InternalError>,
) -> (
    job::State,
    Result<Result<(), JobExecutionError>, InternalError>,
) {
    match result {
        Ok(Ok(())) => {
            trace!("job execution successful");
            (job::State::Successful, Ok(Ok(())))
        }
        Ok(Err(job::Error {
            error,
            kind: job::ErrorKind::Cancellation,
        })) => {
            trace!(?error, "job execution requested cancellation");
            (
                job::State::Cancelled,
                Ok(Err(JobExecutionError::ExecutionCancelled(error))),
            )
        }
        Ok(Err(job::Error {
            error,
            kind: job::ErrorKind::Failure,
        })) => {
            let new_state = match current_attempt.cmp(&max_attempts) {
                Ordering::Less => {
                    trace!(?error, "job execution failed; will retry");
                    // There are available attempts, make the job available again to
                    // perform a retry.
                    job::State::Available
                }
                Ordering::Equal => {
                    trace!(?error, "job execution failed final attempt; will cancel");
                    // No more retries available; cancel job.
                    job::State::Cancelled
                }
                // Job should have been cancelled on the previous attempt.
                Ordering::Greater => unreachable!("broken invariant: invalid attempt"),
            };
            (
                new_state,
                Ok(Err(JobExecutionError::ExecutionFailed(error))),
            )
        }
        Err(InternalError::JobExecution(error)) => {
            trace!(?error, "job dispatching failed; will retry");
            (job::State::Available, Ok(Err(error)))
        }
        Err(error) => {
            trace!(
                ?error,
                "failed to dispatch job (internal library error). {bug}",
                bug = "[!!!] this is a bug. please report."
            );
            // Reschedule even though the failure is not the user's fault.
            (job::State::Available, Err(error))
        }
    }
}

/// Saves the execution state for the corresponding job. Also increments the
/// number of attempts.
///
/// If `new_state` is [`job::State::Available`], will also send a notification
/// to schedule a new job execution.
async fn save_execution_state(
    id: Uuid,
    new_state: job::State,
    queue: &str,
    pool: &PgPool,
) -> Result<(), InternalError> {
    debug_assert_ne!(
        new_state,
        job::State::Processing,
        "dev: you probably didn't mean to do this"
    );

    let (attempts, rescheduled): (i16, bool) = sqlx::query_as(
        r"
        WITH updated_job AS (
            UPDATE fila.jobs SET
                state = $1,
                attempts = attempts + 1,
                finished_at = CASE
                    WHEN $1 <> 'available' THEN now()
                    ELSE NULL
                END
            WHERE id = $2
            RETURNING attempts
        )
        SELECT
            updated_job.attempts,
            CASE
                WHEN $1 = 'available' THEN
                    ('y' || pg_notify($3, 'q:' || $4)::TEXT) = 'y'
                ELSE false
            END
        FROM updated_job;
    ",
    )
    .bind(new_state)
    .bind(id)
    .bind(PG_TOPIC_NAME)
    .bind(queue)
    .fetch_one(pool)
    .await
    .map_err(|error| InternalError::DatabaseFailedToMarkJob(error.into(), new_state))?;

    trace!(?new_state, "changed job state");
    assert_eq!(
        new_state == job::State::Available,
        rescheduled,
        "bug: query didn't work as expected"
    );
    if rescheduled {
        let new_attempt = (/* number of executed attempts */attempts) + 1;
        trace!(new_attempt, "scheduled new job execution attempt");
    }

    Ok(())
}
