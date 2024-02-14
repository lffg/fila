use std::sync::Arc;
use std::{borrow::Cow, collections::HashSet};

use sqlx::{postgres::PgListener, PgPool};
use tokio_util::sync::CancellationToken;

use crate::subscriber::{coordinator::Coordinator, listener::Listener};
use crate::{
    error::{Result, ResultExt},
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
    pub fn builder() -> Builder {
        Builder::new()
    }

    /// Starts all of the Fila's background services related to the subscriber.
    ///
    /// The future returned by this method runs indefinitely until the
    /// cancellation token provided by [`Builder::with_cancellation_token`]
    /// gets fulfilled.
    ///
    /// Fails to start if the database pool can't establish a connection or the
    /// PostgreSQL listener fails to start. Errors related to jobs are
    /// internally handled and exposed through the error-reporting facilities
    /// provided by Fila.
    pub async fn start(self) -> Result<()> {
        // Tries to connect to the database to minimize connection errors next
        sqlx::query("SELECT 1;")
            .fetch_one(&self.pool)
            .await
            .with_ctx("failed to connect to the database")?;

        // Starts the actual PostgreSQL topic listener.
        let mut listener = PgListener::connect_with(&self.pool)
            .await
            .with_ctx("failed to create postgres listener")?;
        listener
            .listen(PG_TOPIC_NAME)
            .await
            .with_ctx("failed to listen for postgres topic")?;

        // The job registry is shared between all queue workers, which are
        // responsible for the actual job dispatch.
        let job_registry = Arc::new(self.job_registry);

        // Starts the coordinator.
        let (coordinator_chan, coordinator) = Coordinator::new(
            Arc::clone(&job_registry),
            self.pool,
            self.cancellation_token.clone(),
        );
        let queues = clone_queue_names(&self.queue_names);
        let coordinator_task = tokio::spawn(async move {
            coordinator.start(queues).await;
        });

        // Starts the listener task.
        let listener = Listener {
            queue_names: self.queue_names,
            cancellation_token: self.cancellation_token,
            coordinator_chan,
            listener,
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
