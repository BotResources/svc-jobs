use serde_json::Value;
use uuid::Uuid;

use super::db::Durable;
use super::fixture::JobsFixture;
use super::gql;

pub const REQUEST_DATA_VIEW: &str = r#"
query($id: UUID!) {
  jobsJob(id: $id) {
    job {
      id status config contentErasedAt
      passport { subjectKind subjectId }
      payload { form message }
    }
  }
}
"#;

pub const REQUEST_DATA_LEFT: &str = "SELECT count(*) FROM jobs WHERE id = $1 \
     AND (config IS NOT NULL OR passport IS NOT NULL OR payload IS NOT NULL)";
pub const REQUEST_DATA_HELD: &str = "SELECT count(*) FROM jobs WHERE id = $1 \
     AND config IS NOT NULL AND passport IS NOT NULL AND payload IS NOT NULL";
pub const ERASED_STAMP: &str = "SELECT count(*) FROM jobs WHERE id = $1 \
     AND content_erased_at IS NOT NULL";
pub const KEPT_PAYLOADS_OF_JOB: &str = "SELECT count(*) FROM kept_payloads WHERE job_id = $1";
pub const QUEUED_BODY_WITH_REQUEST_DATA: &str = "SELECT count(*) FROM domain_events \
     WHERE aggregate_id = $1 AND event_type = 'JobQueued' \
       AND payload ?| ARRAY['config', 'passport', 'payload']";
pub const QUEUED_BODIES: &str = "SELECT count(*) FROM domain_events \
     WHERE aggregate_id = $1 AND event_type = 'JobQueued'";
pub const RESULTS_OF_JOB: &str = "SELECT count(*) FROM job_results WHERE job_id = $1";
pub const ITEMS_AT: &str = "SELECT count(*) FROM inbox_items WHERE address = $1";
pub const MAIL_ITEMS_AT: &str =
    "SELECT count(*) FROM inbox_items WHERE address = $1 AND kind = 'MAIL'";
pub const ITEMS_FOR_RUN: &str = "SELECT count(*) FROM inbox_items WHERE run_id = $1 \
     OR address IN (SELECT agent_id FROM directory_agents WHERE run_id = $1)";
pub const NOTICES_FOR_RUN: &str =
    "SELECT count(*) FROM inbox_items WHERE run_id = $1 AND kind = 'NOTICE'";
pub const MAIL_WITH_CONTENT: &str = "SELECT count(*) FROM mails WHERE mail_id = $1 \
     AND subject <> '' AND body <> '' AND author <> 'erased' AND content_erased_at IS NULL";
pub const MAIL_ERASED: &str = "SELECT count(*) FROM mails WHERE mail_id = $1 \
     AND subject = '' AND body = '' AND author = 'erased' \
     AND source_conversation_id = '00000000-0000-7000-8000-000000000000' \
     AND content_erased_at IS NOT NULL";
pub const ATTACHMENTS_OF_MAIL: &str = "SELECT count(*) FROM mail_attachments WHERE mail_id = $1";
pub const RECIPIENT_ROWS_OF_MAIL: &str = "SELECT count(*) FROM mail_recipients WHERE mail_id = $1";
pub const WAITING_FOR_ERASURE: &str =
    "SELECT count(*) FROM cancelled_request_data WHERE job_id = $1";

pub async fn view_of(fixture: &JobsFixture, job: Uuid) -> Value {
    let response = fixture
        .gql()
        .query(
            fixture.admin(),
            REQUEST_DATA_VIEW,
            serde_json::json!({ "id": job.to_string() }),
        )
        .await;
    gql::data_of(&response, "jobsJob")["job"].clone()
}

pub async fn assert_request_data_erased(
    fixture: &JobsFixture,
    durable: &Durable,
    job: Uuid,
    what: &str,
) {
    durable
        .assert_count(REQUEST_DATA_LEFT, job, 0, &format!("{what}: columns"))
        .await;
    durable
        .assert_count(
            ERASED_STAMP,
            job,
            1,
            &format!("{what}: time of the erasure"),
        )
        .await;
    durable
        .assert_count(
            KEPT_PAYLOADS_OF_JOB,
            job,
            0,
            &format!("{what}: kept payload"),
        )
        .await;
    durable
        .assert_count(
            QUEUED_BODY_WITH_REQUEST_DATA,
            job,
            0,
            &format!("{what}: the JobQueued body"),
        )
        .await;
    durable
        .assert_count(
            QUEUED_BODIES,
            job,
            1,
            &format!("{what}: the JobQueued fact itself stays"),
        )
        .await;
    let view = view_of(fixture, job).await;
    assert!(view["passport"].is_null(), "{what}: {view}");
    assert!(view["payload"].is_null(), "{what}: {view}");
    assert!(view["config"].is_null(), "{what}: {view}");
    assert!(view["contentErasedAt"].is_string(), "{what}: {view}");
}

pub async fn assert_request_data_held(
    fixture: &JobsFixture,
    durable: &Durable,
    job: Uuid,
    what: &str,
) {
    durable
        .assert_count(REQUEST_DATA_HELD, job, 1, &format!("{what}: columns"))
        .await;
    durable
        .assert_count(ERASED_STAMP, job, 0, &format!("{what}: no erasure time"))
        .await;
    durable
        .assert_count(
            QUEUED_BODY_WITH_REQUEST_DATA,
            job,
            1,
            &format!("{what}: the JobQueued body"),
        )
        .await;
    let view = view_of(fixture, job).await;
    assert!(view["passport"].is_object(), "{what}: {view}");
    assert!(view["payload"].is_object(), "{what}: {view}");
    assert!(view["config"].is_object(), "{what}: {view}");
    assert!(view["contentErasedAt"].is_null(), "{what}: {view}");
}
