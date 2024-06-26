use std::{
    sync::atomic::{AtomicU16, Ordering},
    time::Duration,
};

use fila::{job, sync::CancellationToken};
use serde::{Deserialize, Serialize};
use sqlx::{Executor, PgPool, Row as _};
use tokio::time::sleep;

use crate::SCHEMA_QUERY;

static COUNTER: AtomicU16 = AtomicU16::new(0);

#[derive(Serialize, Deserialize)]
struct MyJobThatWillPanic;

impl job::Job for MyJobThatWillPanic {
    const NAME: job::Name = "my-job-that-will-panic";

    type State = ();

    async fn exec(self, _: &job::Context<Self::State>) -> Result<(), job::Error> {
        COUNTER.fetch_add(1, Ordering::SeqCst);
        panic!("woo!");
    }

    fn config() -> job::Config {
        job::Config {
            max_attempts: 999, // Jobs that panic should be cancelled.
            ..Default::default()
        }
    }
}

#[sqlx::test]
async fn test(pool: PgPool) {
    let ct = CancellationToken::new();
    pool.execute(SCHEMA_QUERY).await.unwrap();

    let subscriber = fila::subscriber::Subscriber::builder()
        .register::<MyJobThatWillPanic>()
        .with_cancellation_token(ct.clone())
        .with_pool(pool.clone())
        .build();

    tokio::spawn(async move {
        subscriber.start().await.unwrap();
    });

    let send_pool = pool.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(10)).await;
        fila::send(MyJobThatWillPanic, &send_pool).await.unwrap();
    });

    sleep(Duration::from_millis(100)).await;

    let row = sqlx::query("SELECT state::TEXT, attempts FROM fila.jobs")
        .fetch_one(&pool)
        .await
        .unwrap();
    let state: String = row.get("state");
    let attempt = u16::try_from(row.get::<i16, _>("attempts")).unwrap();

    assert_eq!(state, "cancelled");
    assert_eq!(attempt, 1);
    assert_eq!(COUNTER.load(Ordering::SeqCst), 1);

    ct.cancel_and_wait().await;
}
