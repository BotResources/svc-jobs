use br_core_auth::Passport;
use br_test_harness::GraphqlClient;
use serde_json::{Value, json};
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
    let derived = format!(
        "{}|{}|{}|{}|{}",
        text_or_none(&job["status"]),
        text_or_none(&job["attemptCount"]),
        text_or_none(&job["activeRunId"]),
        second_precision(&job["nextAttemptAt"]),
        text_or_none(&job["isDeleted"]),
    );
    let stored = durable
        .text(
            "SELECT status || '|' || attempt_count::text || '|' \
                || coalesce(active_run_id::text, 'none') || '|' \
                || coalesce( \
                    to_char(next_attempt_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS'), \
                    'none') \
                || '|' || is_deleted::text \
             FROM job_states WHERE job_id = $1",
            job_id,
        )
        .await;
    assert_eq!(
        stored.as_deref(),
        Some(derived.as_str()),
        "{what}: the job_states view and the aggregate must derive the same state from the same \
         facts — status, attempt count, active run, next attempt and deletion are all read from \
         the view by the list and from the aggregate by the detail, so the day one of the two \
         state machines evolves alone the two surfaces start disagreeing in silence"
    );

    let projected_runs = job["runs"]
        .as_array()
        .unwrap_or_else(|| panic!("{what}: the read answers with the job's runs: {job}"))
        .len();
    let stored_runs = durable
        .pairs(
            "SELECT rs.run_id::uuid AS id, rs.status FROM run_states rs \
             JOIN runs r ON r.id = rs.run_id WHERE r.job_id = $1",
            job_id,
        )
        .await;
    assert_eq!(
        stored_runs.len(),
        projected_runs,
        "{what}: the run_states view must answer for every run the aggregate holds — a view that \
         returns nothing agrees with everything, which is exactly the silent divergence this \
         comparison exists to catch"
    );
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
    assert_eq!(
        stored_progressions.len(),
        projected_runs,
        "{what}: the run_progressions view must answer for every run the aggregate holds, or the \
         cursor comparison below silently checks nothing"
    );
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

    let stored_plans = durable
        .pairs(
            "SELECT rp.run_id::uuid AS id, \
                coalesce(rp.current_plan_declaration_id::text, 'none') || '|' || \
                coalesce(rp.declaration_number::text, 'none') || '|' || \
                coalesce(rp.planned_step_count::text, 'none') AS status \
             FROM run_progressions rp JOIN runs r ON r.id = rp.run_id WHERE r.job_id = $1",
            job_id,
        )
        .await;
    for (run_id, stored_plan) in stored_plans {
        let plan = gql::run_by_id(&job, run_id)["declaredPlan"].clone();
        let derived_plan = format!(
            "{}|{}|{}",
            text_or_none(&plan["declarationId"]),
            text_or_none(&plan["declarationNumber"]),
            plan["items"]
                .as_array()
                .map_or_else(|| "none".to_owned(), |items| items.len().to_string()),
        );
        assert_eq!(
            stored_plan, derived_plan,
            "{what}: the run_progressions view and the Run aggregate must name the same winning \
             plan declaration for run {run_id} — last declaration wins is exactly the rule the two \
             state machines can drift apart on in silence"
        );
    }
}

fn text_or_none(value: &Value) -> String {
    match value {
        Value::Null => "none".to_owned(),
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

fn second_precision(value: &Value) -> String {
    match value.as_str() {
        None => "none".to_owned(),
        Some(stamp) => stamp
            .get(..19)
            .unwrap_or_else(|| panic!("a timestamp the read answers is dated: {stamp}"))
            .to_owned(),
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
