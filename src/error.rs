#![allow(clippy::module_name_repetitions)]

use thiserror::Error;

use crate::job;
use std::{any::Any, borrow::Cow, error::Error as StdError, fmt};

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
    #[error("failed to mark job as {1:?}")]
    DatabaseFailedToMarkJob(DatabaseError, job::State),
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
    /// Fila wasn't able to deserialize the job's payload, before executing the
    /// job's execution code.
    #[error("failed to deserialize job payload")]
    PayloadFailedToDeserialize(serde_json::Error),
    /// Some error was returned from the job execution code.
    #[error("job execution failed due to: {0}")]
    ExecutionFailed(AnyError),
    /// Execution *explicitly* cancelled by the job's execution code.
    ///
    /// This variant is not returned when the cancellation is triggered by the
    /// maximum number of retries being reached. In such a case, which happens
    /// due to a failure during the job execution, the error is reported using
    /// the [`ExecutionFailed`] variant.
    ///
    /// [`ExecutionFailed`]: JobExecutionError::ExecutionFailed
    #[error("job execution cancelled due to: {0}")]
    ExecutionCancelled(AnyError),
    #[error("job execution failed due to panic")]
    /// The job's execution code panicked.
    ExecutionPanicked(PanicContent),
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

pub enum PanicContent {
    String(Cow<'static, str>),
    Unknown(Box<dyn Any + Send + 'static>),
}

impl From<Box<dyn Any + Send + 'static>> for PanicContent {
    fn from(value: Box<dyn Any + Send + 'static>) -> Self {
        if let Some(borrowed) = value.downcast_ref::<&str>() {
            return Self::String(Cow::Borrowed(borrowed));
        }
        match value.downcast() {
            Ok(owned) => Self::String(Cow::Owned(*owned)),
            Err(unknown) => Self::Unknown(unknown),
        }
    }
}

impl fmt::Debug for PanicContent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::String(str) => f.write_str(str),
            Self::Unknown(_) => f.write_str("<unknown content>"),
        }
    }
}
