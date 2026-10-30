use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use uuid::Uuid;

pub struct Durable {
    pool: PgPool,
}

impl Durable {
    pub async fn open(app_url: &str) -> Self {
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(app_url)
            .await
            .expect("the durable assertions read through the least-privilege application role");
        Self { pool }
    }

    pub async fn count(&self, sql: &str, id: Uuid) -> i64 {
        sqlx::query(sql)
            .bind(id)
            .fetch_one(&self.pool)
            .await
            .unwrap_or_else(|error| panic!("counting durable rows with `{sql}`: {error}"))
            .get::<i64, _>(0)
    }

    pub async fn count_like(&self, sql: &str, pattern: &str) -> i64 {
        sqlx::query(sql)
            .bind(pattern)
            .fetch_one(&self.pool)
            .await
            .unwrap_or_else(|error| panic!("counting durable rows with `{sql}`: {error}"))
            .get::<i64, _>(0)
    }

    pub async fn await_count(&self, sql: &str, id: Uuid, expected: i64, what: &str) {
        let waited = tokio::time::Instant::now();
        loop {
            let observed = self.count(sql, id).await;
            if observed == expected {
                return;
            }
            assert!(
                waited.elapsed() < super::LONG,
                "{what}: expected {expected} durable rows for {id}, still {observed}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    pub async fn assert_count(&self, sql: &str, id: Uuid, expected: i64, what: &str) {
        let observed = self.count(sql, id).await;
        assert_eq!(
            observed, expected,
            "{what}: expected {expected} durable rows for {id}, found {observed} — the GraphQL \
             surface cannot see this, so only the database proves it"
        );
    }

    pub async fn assert_all(&self, job_id: Uuid, expectations: &[(&str, i64, &str)]) {
        for (sql, expected, what) in expectations {
            self.assert_count(sql, job_id, *expected, what).await;
        }
    }

    pub async fn rings_for(&self, run_id: Uuid, item_kind: Option<&str>) -> i64 {
        sqlx::query_scalar(RINGS_FOR_RUN)
            .bind(contract_jobs::event_v2::EVENT_TYPE_ITEM_ARRIVED)
            .bind(run_id.to_string())
            .bind(item_kind)
            .fetch_one(&self.pool)
            .await
            .unwrap_or_else(|error| panic!("counting the doorbell rings of {run_id}: {error}"))
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn close(self) {
        self.pool.close().await;
    }
}

pub const JOBS_WITH_ID: &str = "SELECT count(*) FROM jobs WHERE id = $1";
pub const RUNS_OF_JOB: &str = "SELECT count(*) FROM runs WHERE job_id = $1";
pub const RETRY_SCHEDULES_OF_JOB: &str = "SELECT count(*) FROM run_retry_schedules rrs \
     JOIN runs r ON r.id = rrs.failed_run_id WHERE r.job_id = $1";
pub const PLAN_DECLARATIONS_OF_JOB: &str = "SELECT count(*) FROM run_plan_declarations rpd \
     JOIN runs r ON r.id = rpd.run_id WHERE r.job_id = $1";
pub const LOGS_OF_JOB: &str = "SELECT count(*) FROM run_logs rl \
     JOIN runs r ON r.id = rl.run_id WHERE r.job_id = $1";
pub const RESOLUTIONS_OF_JOB: &str = "SELECT count(*) FROM job_resolutions WHERE job_id = $1";
pub const CANCEL_REQUESTS_OF_JOB: &str = "SELECT count(*) FROM run_cancellation_requests rcr \
     JOIN runs r ON r.id = rcr.run_id WHERE r.job_id = $1";
pub const DELETIONS_OF_JOB: &str = "SELECT count(*) FROM job_deletions WHERE job_id = $1";
pub const SOURCE_CLAIMS_OF_ENTITY: &str =
    "SELECT count(*) FROM source_entities WHERE external_id = $1";
pub const OUTBOX_MENTIONING: &str =
    "SELECT count(*) FROM integration_outbox WHERE payload::text LIKE $1";
pub const AGENTS_OF_RUN: &str = "SELECT count(*) FROM directory_agents WHERE run_id = $1";
pub const FINISHED_AGENTS_OF_RUN: &str =
    "SELECT count(*) FROM directory_agents WHERE run_id = $1 AND state = 'FINISHED'";
pub const SAVED_STATES_OF_RUN: &str = "SELECT count(*) FROM saved_states WHERE run_id = $1";
pub const SAVED_STATE_AGENTS_OF_RUN: &str =
    "SELECT count(*) FROM saved_state_agents WHERE run_id = $1";
pub const TERMINALS_OF_RUN: &str = "SELECT count(*) FROM run_terminals WHERE run_id = $1";
pub const CURRENT_RUNS_OF_RUN: &str = "SELECT count(*) FROM current_runs WHERE run_id = $1";
pub const RINGS_FOR_RUN: &str = "SELECT count(*) FROM integration_outbox \
     WHERE payload->>'event_type' = $1 AND payload #>> '{payload,run_id}' = $2 \
     AND ($3::text IS NULL OR payload #>> '{payload,item_kind}' = $3)";
