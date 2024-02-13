use std::{error::Error as StdError, future::Future, time::Duration};

pub type Name = &'static str;
pub type BorrowedName<'a> = &'a str;

pub type QueueName = &'static str;

pub trait Job: Send + Sync {
    const NAME: Name;

    type State;

    fn exec(
        self,
        ctx: &Context<Self::State>,
    ) -> impl Future<Output = Result<(), Error>> + Send + Sync;

    /// Defines the configuration for this job type (mapped by this job's name).
    ///
    /// There's no guarantee about how many times this function is called per
    /// job. Hence user's are encouraged to *not* perform expensive computations
    /// to create a config.
    fn config() -> Config {
        Config::default()
    }
}

#[derive(Debug)]
pub struct Context<S> {
    /// The job's state, which is used to pass shared state between jobs.
    pub state: S,

    /// The attempt number of the job. Starts with 0 if it's the first attempt.
    pub attempt: u16,
}

pub const DEFAULT_QUEUE: &str = "default";
pub const DEFAULT_MAX_ATTEMPTS: u16 = 1;
pub const DEFAULT_TIMEOUT: Option<Duration> = None;

#[derive(Debug, Clone)]
pub struct Config {
    /// The name of the queue on which the job will run on.
    pub queue: QueueName,

    /// The maximum number of attempts for a given job, _including_ the first
    /// execution. By default, is set to `1`, meaning that Fila will only
    /// execute it once (the "first attempt"), without any retries in case of
    /// failures.
    ///
    /// ### Value constraints
    ///
    /// - Can't be smaller than 1.
    /// - Can't be greater than [`i16::MAX`].
    pub max_attempts: u16,

    /// The timeout for the job execution. By default, set to `None`.
    pub timeout: Option<Duration>,
}

impl Config {
    /// Validates the configuration values.
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.max_attempts < 1 {
            return Err("max_attempts can't be smaller than 1");
        }
        if (i16::MAX as u16) < self.max_attempts {
            return Err("max_attempts can't be greater than `i16::MAX`");
        }
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            queue: DEFAULT_QUEUE,
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            timeout: DEFAULT_TIMEOUT,
        }
    }
}

#[derive(Debug)]
pub struct Error {
    pub(crate) kind: ErrorKind,
    pub(crate) error: Box<dyn StdError + Send + 'static>,
}

impl Error {
    pub fn cancel<E>(error: E) -> Error
    where
        E: StdError + Send + 'static,
    {
        Error {
            kind: ErrorKind::Cancellation,
            error: Box::new(error),
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub(crate) enum ErrorKind {
    Failure,
    Cancellation,
}

// NOTE: Sadly this blanket implementation would conflict with an eventual
// `StdError` implementation for the `job::Error` type, as lib `core` provides a
// `From<T> for T` blanket implementation.
impl<T> From<T> for Error
where
    T: StdError + Send + 'static,
{
    fn from(error: T) -> Self {
        Error {
            kind: ErrorKind::Failure,
            error: Box::new(error),
        }
    }
}

#[derive(Debug, Copy, Clone, sqlx::Type)]
#[sqlx(type_name = "fila.job_state", rename_all = "lowercase")]
pub(crate) enum State {
    Available,
    Processing,
    Successful,
    Cancelled,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_validity_for_max_attempts() {
        let mut config = Config {
            queue: "default",
            max_attempts: 1,
            timeout: None,
        };

        assert!(config.validate().is_ok());

        config.max_attempts = 0;
        assert_eq!(
            config.validate(),
            Err("max_attempts can't be smaller than 1")
        );

        config.max_attempts = i16::MAX as u16;
        assert!(config.validate().is_ok());

        config.max_attempts += 1;
        assert_eq!(
            config.validate(),
            Err("max_attempts can't be greater than `i16::MAX`")
        );
    }
}
