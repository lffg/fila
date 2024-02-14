use std::sync::Arc;

use sqlx::PgPool;
use tokio::{
    select,
    sync::{
        mpsc::{self, Receiver, Sender},
        Semaphore, TryAcquireError,
    },
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{instrument, trace};

use crate::subscriber::{coordinator::QueueName, job_lifecycle, magic::JobRegistry};

// TODO: Parameterize this.
const DEFAULT_QUEUE_CONCURRENCY_LIMIT: usize = 5;

#[derive(Debug)]
pub enum Msg {
    /// A message that notifies that there's a job to be processed on this
    /// queue.
    NewJob,
    /// Internal message which can't be constructed by other modules.
    #[allow(private_interfaces)]
    Internal(InternalMsg),
}

#[derive(Debug)]
enum InternalMsg {
    /// Finished processing a job.
    ProcessedJob,
    /// There are no more jobs to fetch.
    ///
    /// The counter was probably stable due to concurrency job processing.
    ExhaustedJobs,
}

struct JobContext {
    queue_name: QueueName,
    chan: Sender<Msg>,
    cancellation_token: CancellationToken,
    job_registry: Arc<JobRegistry>,
    pool: PgPool,
}

pub struct QueueWorker {
    name: QueueName,
    /// Non-authoritatively counts the number of available jobs to be processed
    /// on the queue associated with this queue worker. The authoritative
    /// number, as always, comes from the database.
    ///
    /// This counter is used to increase the velocity at which we can process
    /// "new job" events. In the pathological scenario where jobs are
    /// enqueued faster than they are processed, if the implementation were
    /// to wait for an available job worker to consume more messages, we
    /// would have to use an unbounded channel which could, in practice,
    /// grow forever.
    ///
    /// By using a counter, we just an estimate number about how many jobs are
    /// available, and decrement it while processing.
    ///
    /// The number may not be always accurate due to multiple nodes
    /// (subscribers) concurrently consuming from the same queue. Hence, we must
    /// handle the case there the counter is a non-zero number and there aren't
    /// any more available jobs. In this case, we simply reset the counter to
    /// zero and wait for more messages.
    available_jobs: usize,
    job_worker_semaphore: Arc<Semaphore>,
    job_worker_tasks: TaskTracker,
    rx: Receiver<Msg>,
    cancellation_token: CancellationToken,
    job_context: Arc<JobContext>,
}

impl QueueWorker {
    pub fn new(
        queue_name: QueueName,
        job_registry: Arc<JobRegistry>,
        pool: PgPool,
        cancellation_token: CancellationToken,
    ) -> (Sender<Msg>, Self) {
        // XX: Honestly, I don't have *any* idea of what's the best channel
        // capacity to configure here.
        //
        // Since a queue worker consume messages quickly enough (we just
        // increment a counter), a small number such as this seems appropriate.
        const QUEUE_WORKER_CHANNEL_CAPACITY: usize = 8;
        let (tx, rx) = mpsc::channel(QUEUE_WORKER_CHANNEL_CAPACITY);

        let job_context = JobContext {
            queue_name: queue_name.clone(),
            chan: tx.clone(),
            cancellation_token: cancellation_token.clone(),
            job_registry,
            pool,
        };

        let qw = QueueWorker {
            name: queue_name,
            available_jobs: 0,
            job_worker_semaphore: Arc::new(Semaphore::new(DEFAULT_QUEUE_CONCURRENCY_LIMIT)),
            job_worker_tasks: TaskTracker::new(),
            rx,
            cancellation_token,
            job_context: Arc::new(job_context),
        };
        (tx, qw)
    }

    pub async fn start(mut self) {
        loop {
            select! {
                maybe_message = self.rx.recv() => {
                    let Some(message) = maybe_message else {
                        trace!("sender closed, skipping and waiting for shutdown");
                        continue;
                    };
                    self.handle_message(message).await;

                },
                () =  self.cancellation_token.cancelled() => break,
            }
        }

        // Close the channel to ensure we don't receive any new job
        // notifications.
        self.rx.close();

        trace!("shutting queue worker down; waiting for job workers to terminate");
        self.job_worker_tasks.close();
        self.job_worker_tasks.wait().await;
    }

    #[instrument(
        level = "trace",
        skip(self),
        fields(queue = %self.name, available_jobs = self.available_jobs),
    )]
    async fn handle_message(&mut self, message: Msg) {
        trace!("got message");
        match message {
            Msg::NewJob => {
                self.available_jobs += 1;
                self.attempt_process_job().await;
            }
            Msg::Internal(InternalMsg::ProcessedJob) => {
                self.attempt_process_job().await;
            }
            Msg::Internal(InternalMsg::ExhaustedJobs) => {
                self.available_jobs = 0;
            }
        }
    }

    #[allow(clippy::unused_async)]
    async fn attempt_process_job(&mut self) {
        if self.available_jobs == 0 {
            trace!("no more jobs");
            return;
        }
        let semaphore = Arc::clone(&self.job_worker_semaphore);
        let permit = match semaphore.try_acquire_owned() {
            Ok(it) => it,
            Err(TryAcquireError::NoPermits) => return,
            Err(TryAcquireError::Closed) => unreachable!("we never close the semaphore"),
        };

        // Decrement the counter as we're processing one.
        self.available_jobs -= 1;

        let ctx = Arc::clone(&self.job_context);
        self.job_worker_tasks.spawn(async move {
            let _permit = permit; // Release permit after task ends.

            let status = job_lifecycle::run_lifecycle(
                ctx.cancellation_token.clone(),
                &ctx.pool,
                &ctx.job_registry,
                &ctx.queue_name,
            )
            .await
            // TODO: Improve error handling.
            .unwrap();

            let msg = match status {
                job_lifecycle::LifecycleStatus::Processed => InternalMsg::ProcessedJob,
                job_lifecycle::LifecycleStatus::NotFound => InternalMsg::ExhaustedJobs,
            };
            ctx.chan.send(Msg::Internal(msg)).await.ok();
        });
    }
}
