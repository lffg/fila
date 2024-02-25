use std::{borrow::Cow, collections::HashSet, sync::Arc};

use sqlx::{postgres::PgListener, PgPool};

use crate::{
    error::{ResultExt, SetupError},
    subscriber::{coordinator::Coordinator, listener::Listener},
    sync::CancellationToken,
    PG_TOPIC_NAME,
};

// Public modules.
mod builder;
pub use builder::Builder;

// Private modules.
mod coordinator;
mod job_lifecycle;
mod listener;
mod magic;
mod queue_worker;

pub struct Subscriber {
    job_registry: magic::JobRegistry,
    queue_names: HashSet<&'static str>,
    pool: PgPool,
    cancellation_token: CancellationToken,
}

impl Subscriber {
    /// Returns a new [subscriber builder](Builder). Consult its module-level
    /// documentation for more information.
    #[must_use]
    pub fn builder() -> Builder {
        Builder::new()
    }

    /// Starts all of the Fila's background services related to the subscriber.
    ///
    /// The future returned by this method runs indefinitely until the
    /// cancellation token provided by [`Builder::with_cancellation_token`]
    /// gets fulfilled.
    ///
    /// # Errors
    ///
    /// Fails to start if the database pool can't establish a connection or the
    /// Postgres listener fails to start. Errors related to jobs are internally
    /// handled and exposed through the error-reporting facilities provided by
    /// Fila.
    ///
    /// # Panics
    ///
    /// We try our best to avoid panics, as they'd crash the entire subscriber
    /// task tree. If you experience a panic from this routine we kindly ask for
    /// a bug report with a minimal reproducible example.
    pub async fn start(self) -> Result<(), SetupError> {
        // Tries to connect to the database to minimize connection errors next
        sqlx::query("SELECT 1;")
            .fetch_one(&self.pool)
            .await
            .map_err_into(SetupError::DatabaseFailedToConnect)?;

        // Starts the actual PostgreSQL topic listener.
        let mut listener = PgListener::connect_with(&self.pool)
            .await
            .map_err_into(SetupError::DatabaseFailedToCreateListener)?;
        listener
            .listen(PG_TOPIC_NAME)
            .await
            .map_err_into(SetupError::DatabaseFailedToListen)?;

        // The job registry is shared between all queue workers, which are
        // responsible for the actual job dispatch.
        let job_registry = Arc::new(self.job_registry);

        // Starts the coordinator.
        let (coordinator_chan, coordinator) = Coordinator::new(
            Arc::clone(&job_registry),
            self.pool,
            self.cancellation_token.clone_inner(),
        );
        let queues = clone_queue_names(&self.queue_names);
        let coordinator_task = tokio::spawn(async move {
            coordinator.start(queues).await;
        });

        // Starts the listener task.
        let listener = Listener {
            queue_names: self.queue_names,
            cancellation_notify: self.cancellation_token.clone_inner(),
            coordinator_chan,
            inner: listener,
        };
        let listener_task = tokio::spawn(async move {
            listener.listen().await;
        });

        // Wait for termination (triggered by the cancellation token).
        //
        // Unwraps are safe since we make an effort to ensure the tasks don't
        // panic. If they do, we sure must forward those panics as the
        // application is probably in an inconsistent state.
        coordinator_task.await.unwrap();
        listener_task.await.unwrap();

        self.cancellation_token.notify_finished_cancellation();

        Ok(())
    }
}

fn clone_queue_names(set: &HashSet<&'static str>) -> impl Iterator<Item = Cow<'static, str>> {
    set.iter()
        .map(|c| Cow::Borrowed(*c))
        // Since we're moving the returned iterator to (possibly) another
        // thread, it must be 'static and hence we need to allocate a new
        // "iterator source".
        // Still, it's better to pass an iterator rather than a vector.
        .collect::<Vec<_>>()
        .into_iter()
}
