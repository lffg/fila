use std::{borrow::Cow, collections::HashMap, sync::Arc};

use sqlx::PgPool;
use tokio::{
    select,
    sync::mpsc::{self, Receiver, Sender},
};
use tokio_util::task::TaskTracker;
use tracing::{instrument, trace, warn};

use crate::{
    subscriber::{
        magic::JobRegistry,
        queue_worker::{self, QueueWorker},
    },
    sync::CancellationNotify,
};

pub type QueueName = Cow<'static, str>;

#[derive(Debug)]
pub enum Msg {
    NewJob { queue: QueueName },
}

pub struct Coordinator {
    queue_worker_map: HashMap<QueueName, QueueWorkerHandle>,
    queue_workers_tracker: TaskTracker,
    rx: Receiver<Msg>,
    cancellation_notify: CancellationNotify,
    job_registry: Arc<JobRegistry>,
    pool: PgPool,
}

impl Coordinator {
    pub fn new(
        job_registry: Arc<JobRegistry>,
        pool: PgPool,
        cancellation_notify: CancellationNotify,
    ) -> (Sender<Msg>, Self) {
        const COORDINATOR_CHANNEL_CAPACITY: usize = 32;
        let (tx, rx) = mpsc::channel(COORDINATOR_CHANNEL_CAPACITY);

        let coordinator = Coordinator {
            job_registry,
            pool,
            queue_worker_map: HashMap::new(),
            queue_workers_tracker: TaskTracker::new(),
            cancellation_notify,
            rx,
        };
        (tx, coordinator)
    }

    pub async fn start(mut self, queues: impl Iterator<Item = QueueName>) {
        // Start a new task for each queue.
        for name in queues {
            let (queue_worker_chan, queue_worker) = QueueWorker::new(
                name.clone(),
                Arc::clone(&self.job_registry),
                self.pool.clone(),
                self.cancellation_notify.clone(),
            );
            self.queue_workers_tracker.spawn(async move {
                queue_worker.start().await;
            });
            let handle = QueueWorkerHandle {
                chan: queue_worker_chan,
            };
            let prev = self.queue_worker_map.insert(name, handle);
            assert!(prev.is_none(), "broken invariant: duplicate queue name");
        }

        loop {
            select! {
                maybe_message = self.rx.recv() => {
                    let Some(message) = maybe_message else {
                        trace!("sender closed, skipping and waiting for shutdown");
                        continue;
                    };
                    self.handle_message(message).await;

                },
                () = self.cancellation_notify.cancelled() => break,
            }
        }

        trace!("shutting down; waiting for queue workers to terminate");
        self.queue_workers_tracker.close();
        self.queue_workers_tracker.wait().await;
    }

    #[instrument(level = "trace", skip(self))]
    async fn handle_message(&mut self, message: Msg) {
        match message {
            Msg::NewJob { queue } => {
                trace!(%queue, "got new job message; forwarding");
                self.send_job_notify_to_queue_worker(queue).await;
            }
        }
    }

    async fn send_job_notify_to_queue_worker(&self, queue: QueueName) {
        let Some(queue_worker_handle) = self.queue_worker_map.get(&queue) else {
            warn!(%queue, "got message for unsubscribed queue");
            return;
        };
        queue_worker_handle
            .chan
            .send(queue_worker::Msg::NewJob)
            .await
            // We don't care if the message doesn't get delivered.
            .ok();
    }
}

// XX: Do we really need this type, or can we just inline it as the sender?
struct QueueWorkerHandle {
    chan: Sender<queue_worker::Msg>,
}
