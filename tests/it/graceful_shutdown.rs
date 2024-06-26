use std::time::Duration;

use fila::{job, sync::CancellationToken};
use serde::{Deserialize, Serialize};
use sqlx::{Executor, PgPool, Row};
use tokio::time::sleep;

use crate::SCHEMA_QUERY;

#[derive(Serialize, Deserialize)]
struct MyJobThatWillBeCancelled;

impl job::Job for MyJobThatWillBeCancelled {
    const NAME: job::Name = "my-job-that-will-be-cancelled";

    type State = ();

    async fn exec(self, ctx: &job::Context<Self::State>) -> Result<(), job::Error> {
        loop {
            tokio::select! {
                _ = sleep(Duration::from_millis(50)) => (),
                _ = ctx.cancelled() => break,
            }
        }
        Ok(())
    }
}

#[sqlx::test]
async fn test(pool: PgPool) {
    let ct = CancellationToken::new();

    pool.execute(SCHEMA_QUERY).await.unwrap();

    let subscriber = fila::subscriber::Subscriber::builder()
        .register::<MyJobThatWillBeCancelled>()
        .with_cancellation_token(ct.clone())
        .with_pool(pool.clone())
        .build();

    tokio::spawn(async move {
        subscriber.start().await.unwrap();
    });

    let send_pool = pool.clone();
    tokio::spawn(async move {
        sleep(Duration::from_millis(10)).await;
        fila::send(MyJobThatWillBeCancelled, &send_pool)
            .await
            .unwrap();
    });

    sleep(Duration::from_millis(50)).await;
    ct.cancel_and_wait().await;

    let row = sqlx::query("SELECT attempts, state::TEXT FROM fila.jobs")
        .fetch_one(&pool)
        .await
        .unwrap();

    assert_eq!(row.get::<i16, _>("attempts"), 1);
    assert_eq!(row.get::<String, _>("state"), "successful");
}
