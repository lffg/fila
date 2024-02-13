use std::{
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
    time::Duration,
};

use fila::job;
use serde::{Deserialize, Serialize};
use sqlx::{Executor, PgPool};
use tokio::time::sleep;

use crate::SCHEMA_QUERY;

#[derive(Serialize, Deserialize)]
struct MyJobThatWillSucceed {
    factor: u32,
}

impl job::Job for MyJobThatWillSucceed {
    const NAME: job::Name = "my-job-that-will-succeed";

    // Execution counter.
    type State = Arc<AtomicU32>;

    async fn exec(self, ctx: &job::Context<Self::State>) -> Result<(), job::Error> {
        ctx.state.fetch_add(1, Ordering::SeqCst);
        OUTER_COUNTER.store(self.factor * MAGIC_FACTOR_A, Ordering::SeqCst);
        Ok(())
    }
}

const MAGIC_FACTOR_A: u32 = 3;
const MAGIC_FACTOR_B: u32 = 7;
static OUTER_COUNTER: AtomicU32 = AtomicU32::new(0);

#[sqlx::test]
async fn test(pool: PgPool) {
    pool.execute(SCHEMA_QUERY).await.unwrap();

    let state = Arc::new(AtomicU32::new(0));

    let subscriber = fila::subscriber::Subscriber::builder()
        .with_state(&state, |b| b.register::<MyJobThatWillSucceed>())
        .build();
    let sub_pool = pool.clone();
    let sub_handle = tokio::spawn(async move {
        subscriber
            .with_pool(sub_pool.clone())
            .listen()
            .await
            .unwrap();
    });

    let send_pool = pool.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(10)).await;
        let payload = MyJobThatWillSucceed {
            factor: MAGIC_FACTOR_B,
        };
        fila::send(payload, &send_pool).await.unwrap();
    });

    sleep(Duration::from_millis(50)).await;

    assert_eq!(
        OUTER_COUNTER.load(Ordering::SeqCst),
        MAGIC_FACTOR_A * MAGIC_FACTOR_B
    );
    assert_eq!(state.load(Ordering::SeqCst), 1);

    let state: String = sqlx::query_scalar("SELECT state::TEXT FROM fila.jobs")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(state, "successful");

    // FIXME: Add graceful shutdown to the listener task.
    sub_handle.abort();
}
