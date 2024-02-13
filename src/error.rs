use std::{error::Error as StdError, fmt};

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// This type represents all possible errors that can happen during Fila's
/// operations.
///
/// Do not confuse this type with [`job::Error`](crate::job::Error), which
/// represents an error that may be returned by a [`Job`](crate::job::Job)'s
/// execution.
pub struct Error {
    error: Box<ErrorImpl>,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &*self.error {
            ErrorImpl::Serde(msg, inner) => write!(f, "{msg} (caused by: {inner})"),
            ErrorImpl::Database(msg, inner) => todo!("{msg} (caused by: {inner})"),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&*self.error, f)
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match &*self.error {
            ErrorImpl::Serde(_, src) => Some(src),
            ErrorImpl::Database(_, src) => Some(&**src),
        }
    }
}

/// The actual error implementation.
#[derive(Debug)]
pub(crate) enum ErrorImpl {
    Serde(&'static str, serde_json::Error),
    Database(&'static str, Box<dyn StdError + Send + Sync + 'static>),
}

pub(crate) trait ResultExt<T> {
    fn with_ctx(self, msg: &'static str) -> Result<T>;
}

macro_rules! impl_result_ext {
    ($variant:ident($error:ty)) => {
        impl<T> ResultExt<T> for Result<T, $error> {
            fn with_ctx(self, msg: &'static str) -> Result<T> {
                self.map_err(|orig| Error {
                    error: Box::new(ErrorImpl::$variant(msg, orig.into())),
                })
            }
        }
    };
}

impl_result_ext!(Serde(serde_json::Error));
impl_result_ext!(Database(sqlx::Error));
