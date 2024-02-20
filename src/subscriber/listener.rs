use std::collections::HashSet;

use sqlx::postgres::{PgListener, PgNotification};
use tokio::{select, sync::mpsc::Sender};
use tracing::{trace, warn};

use crate::{subscriber::coordinator, sync::CancellationNotify, PG_TOPIC_NAME};

pub struct Listener {
    pub queue_names: HashSet<&'static str>,
    pub cancellation_notify: CancellationNotify,
    pub coordinator_chan: Sender<coordinator::Msg>,
    /// The underlying Postgres listener.
    pub inner: PgListener,
}

impl Listener {
    pub async fn listen(mut self) {
        trace!(topic = PG_TOPIC_NAME, "listening");
        loop {
            select! {
                biased;

                () = self.cancellation_notify.cancelled() => {
                    self.inner.unlisten(PG_TOPIC_NAME).await.ok();
                    break;
                },
                notification = self.inner.recv() => {
                    self.handle_pg_notification(notification).await;
                },
            }
        }
        trace!("shutting listener down");
    }

    async fn handle_pg_notification(&mut self, notification: Result<PgNotification, sqlx::Error>) {
        match notification {
            Ok(event) => {
                let payload = event.payload();
                trace!(?payload, "received listener message");
                let Some(queue_name) = self.parse_queue_name(payload) else {
                    trace!("unrecognized message format, skipping");
                    return;
                };
                self.handle_message(queue_name).await;
            }
            Err(sqlx::Error::PoolClosed) => {
                trace!("failed to receive listener message: pool closed");
            }
            Err(error) => {
                warn!(?error, "failed to receive listener message");
            }
        }
    }

    async fn handle_message(&self, queue: &str) {
        self.coordinator_chan
            .send(coordinator::Msg::NewJob {
                queue: queue.to_owned().into(),
            })
            .await
            // We don't care if the message doesn't get delivered.
            .ok();
    }

    // XX: Maybe create a `QueueName` type which implements SQLx's `Encode` and
    // `Decode` traits.
    fn parse_queue_name<'a>(&self, message: &'a str) -> Option<&'a str> {
        let name = message.strip_prefix("q:")?;
        self.queue_names.contains(name).then_some(name)
    }
}
