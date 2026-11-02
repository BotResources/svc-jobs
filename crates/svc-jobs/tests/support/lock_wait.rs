use std::time::Duration;

use sqlx::PgPool;

const PROBES: u32 = 200;

pub async fn a_lock_wait_exists(pool: &PgPool) -> bool {
    sqlx::query_scalar(
        "SELECT count(*) > 0 FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid \
         WHERE NOT l.granted AND a.datname = current_database()",
    )
    .fetch_one(pool)
    .await
    .expect("the lock-wait probe runs")
}

pub async fn await_a_lock_wait(pool: &PgPool, what: &str) {
    for _ in 0..PROBES {
        if a_lock_wait_exists(pool).await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("{what} never waited for a lock: the interleaving this scenario needs never happened");
}
