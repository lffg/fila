#![allow(clippy::module_name_repetitions)]

use thiserror::Error;

use crate::job;
use std::error::Error as StdError;

type AnyError = Box<dyn StdError + Send + 'static>;

/// Internal error.
#[derive(Debug, Error)]
pub(crate) enum InternalError {
    #[error("database failed to acquire transaction")]
    DatabaseFailedToAcquireTransaction(DatabaseError),
    #[error("database failed to commit transaction")]
    DatabaseFailedToCommitTransaction(DatabaseError),
    #[error("failed to fetch available job")]
    DatabaseFailedToFetchJob(DatabaseError),
    #[error("failed to mark job as processing")]
    DatabaseFailedToMarkJob(DatabaseError),
    #[error("job execution error: {0}")]
    JobExecution(JobExecutionError),
}

/// Represents any error that may occur during a job's lifecycle, including an
/// job execution error, which is in turn represented by a [`job::Error`].
///
/// [`job::Error`]: crate::job::Error
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum JobExecutionError {
    #[error("failed to deserialize job payload")]
    PayloadFailedToDeserialize(serde_json::Error),
    #[error("job execution failed due to: {0}")]
    ExecutionFailed(AnyError),
    #[error("job execution cancelled due to: {0}")]
    ExecutionCancelled(AnyError),
    #[error("job execution failed due to panic")]
    ExecutionPanicked(() /* TODO */),
}

impl From<job::Error> for JobExecutionError {
    fn from(value: job::Error) -> Self {
        match value.kind {
            job::ErrorKind::Failure => JobExecutionError::ExecutionFailed(value.error),
            job::ErrorKind::Cancellation => JobExecutionError::ExecutionCancelled(value.error),
        }
    }
}

/// Represents any error that may occur when publishing a job.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum JobPublishError {
    #[error("failed to serialize job payload")]
    PayloadFailedToSerialize(serde_json::Error),
    #[error("failed to save job on the database")]
    DatabaseFailedToSave(DatabaseError),
}

/// Represents any error that may occur during Fila's setup.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum SetupError {
    #[error("failed to connect to the database")]
    DatabaseFailedToConnect(DatabaseError),
    #[error("failed to create postgres listener")]
    DatabaseFailedToCreateListener(DatabaseError),
    #[error("failed to listen to postgres listener's topic")]
    DatabaseFailedToListen(DatabaseError),
}

#[derive(Debug, Error)]
pub enum DatabaseError {
    #[error("{0}")]
    Sqlx(Box<sqlx::Error>),
}

impl From<sqlx::Error> for DatabaseError {
    fn from(error: sqlx::Error) -> Self {
        Self::Sqlx(Box::new(error))
    }
}

pub(crate) trait ResultExt<T, E1> {
    fn map_err_into<F, I, E2>(self, f: F) -> Result<T, E2>
    where
        F: FnOnce(I) -> E2,
        I: From<E1>;
}

impl<T, E1> ResultExt<T, E1> for Result<T, E1> {
    fn map_err_into<F, I, E2>(self, f: F) -> Result<T, E2>
    where
        F: FnOnce(I) -> E2,
        I: From<E1>,
    {
        self.map_err(|error| f(error.into()))
    }
}
