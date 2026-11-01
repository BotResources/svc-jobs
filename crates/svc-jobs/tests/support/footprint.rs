use uuid::Uuid;

use super::db::Durable;

#[derive(Debug, PartialEq, Eq)]
pub struct Footprint {
    pub events: i64,
    pub outbox: i64,
    pub notices: i64,
}

const EVENTS_OF_JOB: &str = "SELECT count(*) FROM domain_events WHERE aggregate_id = $1";
const OUTBOX_OF_JOB_OR_RUN: &str = "SELECT count(*) FROM integration_outbox \
     WHERE payload::text LIKE $1 OR payload::text LIKE $2";
const NOTICES: &str = "SELECT count(*) FROM inbox_items WHERE kind = 'NOTICE'";

impl Footprint {
    pub async fn of(durable: &Durable, job_id: Uuid, run_id: Uuid) -> Self {
        let outbox: i64 = sqlx::query_scalar(OUTBOX_OF_JOB_OR_RUN)
            .bind(format!("%{job_id}%"))
            .bind(format!("%{run_id}%"))
            .fetch_one(durable.pool())
            .await
            .expect("the outbox records of a job and its run count");
        let notices: i64 = sqlx::query_scalar(NOTICES)
            .fetch_one(durable.pool())
            .await
            .expect("the notices count");
        Self {
            events: durable.count(EVENTS_OF_JOB, job_id).await,
            outbox,
            notices,
        }
    }
}
