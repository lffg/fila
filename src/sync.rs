use std::{fmt, sync::Arc};

use tokio::sync::Notify;

/// A cancellation token to aid the implementation of graceful shutdown.
#[derive(Clone, Default)]
pub struct CancellationToken {
    inner: tokio_util::sync::CancellationToken,
    waker: Arc<Notify>,
}

impl CancellationToken {
    /// Creates a new token.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Requests a cancellation and waits for the cancellation itself, after
    /// all cancellation requests have been carried out by child tasks.
    pub async fn cancel_and_wait(self) {
        self.inner.cancel();
        self.waker.notified().await;
    }

    /// Returns a future that gets fulfilled when cancellation is requested.
    pub async fn cancelled(&self) {
        self.inner.cancelled().await;
    }

    /// Returns a clone of the underlying cancellation notifier.
    pub(crate) fn clone_inner(&self) -> CancellationNotify {
        CancellationNotify(self.inner.clone())
    }

    /// Notifies that the cancellation process has finished.
    pub(crate) fn notify_finished_cancellation(&self) {
        self.waker.notify_waiters();
    }
}

#[allow(clippy::missing_fields_in_debug)]
impl fmt::Debug for CancellationToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancellationToken")
            .field("is_cancelled", &self.inner.is_cancelled())
            .finish()
    }
}

#[derive(Clone)]
pub(crate) struct CancellationNotify(tokio_util::sync::CancellationToken);

impl CancellationNotify {
    /// Returns a future that gets fulfilled when cancellation is requested.
    pub async fn cancelled(&self) {
        self.0.cancelled().await;
    }
}

impl fmt::Debug for CancellationNotify {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancellationTokenNotify")
            .field("is_cancelled", &self.0.is_cancelled())
            .finish()
    }
}
