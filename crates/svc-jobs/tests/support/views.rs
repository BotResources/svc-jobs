use br_core_auth::Passport;
use br_test_harness::GraphqlClient;
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

use super::db::Durable;
use super::gql;

pub async fn assert_views_agree(
    durable: &Durable,
    client: &GraphqlClient,
    admin: &Passport,
    job_id: Uuid,
    what: &str,
) {
    let job = gql::job(client, admin, job_id).await;
    let derived = job["status"]
        .as_str()
        .unwrap_or_else(|| panic!("{what}: the read answers a status: {job}"));
    let stored = durable
        .text("SELECT status FROM job_states WHERE job_id = $1", job_id)
        .await;
    assert_eq!(
        stored.as_deref(),
        Some(derived),
        "{what}: the job_states view and the aggregate must derive the same status from the same \
         facts — the day one of the two state machines evolves alone, an administrator's list and \
         an administrator's detail start disagreeing in silence"
    );

    let stored_runs = durable
        .pairs(
            "SELECT rs.run_id::uuid AS id, rs.status FROM run_states rs \
             JOIN runs r ON r.id = rs.run_id WHERE r.job_id = $1",
            job_id,
        )
        .await;
    for (run_id, stored_status) in stored_runs {
        let run = gql::run_by_id(&job, run_id);
        assert_eq!(
            run["status"],
            json!(stored_status),
            "{what}: the run_states view and the Run aggregate must agree on run {run_id}"
        );
    }

    let stored_progressions = durable
        .pairs(
            "SELECT rp.run_id::uuid AS id, \
                coalesce(rp.current_step_index::text, 'none') AS status \
             FROM run_progressions rp JOIN runs r ON r.id = rp.run_id WHERE r.job_id = $1",
            job_id,
        )
        .await;
    for (run_id, stored_step) in stored_progressions {
        let run = gql::run_by_id(&job, run_id);
        let derived_step = run["progression"]["currentStep"]["index"]
            .as_i64()
            .map_or_else(|| "none".to_owned(), |index| index.to_string());
        assert_eq!(
            stored_step, derived_step,
            "{what}: the run_progressions view and the Run aggregate must agree on the current \
             step of run {run_id}"
        );
    }
}

impl Durable {
    pub async fn text(&self, sql: &str, id: Uuid) -> Option<String> {
        sqlx::query(sql)
            .bind(id)
            .fetch_optional(self.pool())
            .await
            .unwrap_or_else(|error| panic!("reading a view with `{sql}`: {error}"))
            .map(|row| row.get::<String, _>(0))
    }

    pub async fn pairs(&self, sql: &str, id: Uuid) -> Vec<(Uuid, String)> {
        sqlx::query(sql)
            .bind(id)
            .fetch_all(self.pool())
            .await
            .unwrap_or_else(|error| panic!("reading a view with `{sql}`: {error}"))
            .iter()
            .map(|row| (row.get::<Uuid, _>("id"), row.get::<String, _>("status")))
            .collect()
    }
}
