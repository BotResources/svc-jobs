mod support;

use support::db::Durable;
use support::erasure::{
    ATTACHMENTS_OF_MAIL, ITEMS_AT, MAIL_ERASED, MAIL_WITH_CONTENT, RECIPIENT_ROWS_OF_MAIL,
    RESULTS_OF_JOB, assert_request_data_erased, assert_request_data_held,
};
use support::erasure_scene::{Scene, build};
use support::fixture::{JobsFixture, runner_access};
use support::gesture;
use support::restart::read_result;
use support::scrub::{SCRUBBED_MARK, age_tree, assert_not_scrubbed_for, await_scrubbed};
use support::scrub_scene::{finish, finished_root, tree_jobs};
use support::{QUIET, gql};

const LOG_LINES_OF_RUN: &str = "SELECT count(*) FROM run_logs WHERE run_id = $1";
const LOG_LINES_ERASED: &str =
    "SELECT count(*) FROM run_logs WHERE run_id = $1 AND message = 'erased'";
const STEPS_OF_RUN: &str = "SELECT count(*) FROM steps WHERE run_id = $1";
const STEPS_ERASED: &str = "SELECT count(*) FROM steps WHERE run_id = $1 AND label = 'erased'";
const FAILURE_KEPT: &str = "SELECT count(*) FROM run_terminals WHERE run_id = $1 \
     AND kind = 'FAILED' AND reason_code = 'provider_refused' AND failure_kind = 'PERMANENT' \
     AND params = '{}'::jsonb AND diagnostic = '{}'::jsonb";
const FAILURE_BODIES_WITH_DIAGNOSTIC: &str = "SELECT count(*) FROM domain_events \
     WHERE aggregate_id = $1 AND event_type IN ('RunFailed', 'JobFailed') \
       AND payload #> '{report,diagnostic}' <> '{}'::jsonb";
const STEP_EVENTS_WITH_LABEL: &str = "SELECT count(*) FROM domain_events \
     WHERE aggregate_id = $1 AND event_type = 'RunStepStarted' AND payload ->> 'label' <> 'erased'";
const SAVED_STATES_OF_RUN: &str = "SELECT count(*) FROM saved_states WHERE run_id = $1";
const RUNS_OF_JOB: &str = "SELECT count(*) FROM runs WHERE job_id = $1";
const JOBS_OF_TREE: &str = "SELECT count(*) FROM jobs WHERE root_job_id = $1";

#[tokio::test]
async fn a_tree_is_scrubbed_after_the_retention_and_keeps_its_skeleton() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let s = build(&fixture, &durable, "scrub").await;
    finish(&fixture, &durable, &s).await;
    let other = finished_root(&fixture, &durable, "scrub-other").await;
    for (job, what) in tree_jobs(&s) {
        assert_request_data_held(&fixture, &durable, job, what).await;
    }
    durable
        .assert_count(
            FAILURE_BODIES_WITH_DIAGNOSTIC,
            s.b,
            2,
            "B reported a diagnostic",
        )
        .await;

    age_tree(&durable, s.a, 29).await;
    assert_not_scrubbed_for(&durable, s.a, QUIET, "29 days is inside the retention").await;
    for (job, what) in tree_jobs(&s) {
        assert_request_data_held(&fixture, &durable, job, what).await;
    }

    age_tree(&durable, s.a, 2).await;
    await_scrubbed(&durable, s.a, "31 days is past the retention").await;

    for (job, what) in tree_jobs(&s) {
        assert_request_data_erased(&fixture, &durable, job, what).await;
    }
    for (job, status) in [(s.a, "COMPLETED"), (s.b, "FAILED"), (s.c, "COMPLETED")] {
        assert_eq!(
            gql::status_of(&fixture.gql(), fixture.admin(), job).await,
            status
        );
    }
    durable
        .assert_count(JOBS_OF_TREE, s.a, 4, "the Jobs of the Tree stay")
        .await;
    durable
        .assert_count(RUNS_OF_JOB, s.b, 1, "the Run of B stays")
        .await;
    durable
        .assert_count(RESULTS_OF_JOB, s.e, 0, "the Result of E is erased")
        .await;
    durable
        .assert_count(
            SAVED_STATES_OF_RUN,
            s.b_work.run_id,
            0,
            "the saved state of B is erased",
        )
        .await;
    durable
        .assert_count(ITEMS_AT, s.q, 0, "no item waits at the address of Q")
        .await;
    for mail in [s.mail_to_q, s.mail_p_s] {
        durable
            .assert_count(MAIL_ERASED, mail, 1, "the mail is erased")
            .await;
        durable
            .assert_count(MAIL_WITH_CONTENT, mail, 0, "the mail keeps no content")
            .await;
        durable
            .assert_count(RECIPIENT_ROWS_OF_MAIL, mail, 1, "the recipients stay")
            .await;
    }
    durable
        .assert_count(
            ATTACHMENTS_OF_MAIL,
            s.mail_p_s,
            0,
            "the attachment is erased",
        )
        .await;
    durable
        .assert_count(
            FAILURE_KEPT,
            s.b_work.run_id,
            1,
            "the failure keeps its kind and reason code and loses params and diagnostic",
        )
        .await;
    durable
        .assert_count(
            FAILURE_BODIES_WITH_DIAGNOSTIC,
            s.b,
            0,
            "no event of B keeps a diagnostic",
        )
        .await;
    durable
        .assert_count(
            STEP_EVENTS_WITH_LABEL,
            s.a,
            0,
            "no step event of A keeps a label",
        )
        .await;
    durable
        .assert_count(STEPS_OF_RUN, s.a_work.run_id, 1, "the step of A stays")
        .await;
    durable
        .assert_count(STEPS_ERASED, s.a_work.run_id, 1, "its label is erased")
        .await;
    durable
        .assert_count(LOG_LINES_OF_RUN, s.a_work.run_id, 1, "the line of A stays")
        .await;
    durable
        .assert_count(
            LOG_LINES_ERASED,
            s.a_work.run_id,
            1,
            "its message is erased",
        )
        .await;
    let lines = gql::logs_of_run(&fixture.gql(), fixture.admin(), s.b, s.b_work.run_id).await;
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["level"], "ERROR");
    assert_eq!(lines[0]["message"], "erased");
    assert!(lines[0]["loggedAt"].is_string());
    assert_the_erased_jobs_still_read(&fixture, &s).await;
    let work = &s.a_work;
    let reads = [
        (
            "jobsPayload",
            gesture::payload(&fixture.gql(), &runner_access(), work).await,
        ),
        (
            "jobsUnread",
            gesture::unread(&fixture.gql(), &runner_access(), work, None).await,
        ),
        (
            "jobsReadResult",
            read_result(&fixture, work, s.b_result).await,
        ),
    ];
    for (name, response) in &reads {
        assert_eq!(gesture::refusal(response, name), "run_ended", "{name}");
    }
    assert_request_data_held(&fixture, &durable, other, "the recent Tree").await;
    durable
        .assert_count(SCRUBBED_MARK, other, 0, "the recent Tree is not scrubbed")
        .await;
    durable.close().await;
    fixture.shutdown().await;
}

async fn assert_the_erased_jobs_still_read(fixture: &JobsFixture, s: &Scene<'_>) {
    let view = gql::job_view(&fixture.gql(), fixture.admin(), s.b).await;
    let run = &view["job"]["runs"][0];
    assert_eq!(run["status"], "FAILED", "{view}");
    assert_eq!(run["failureReport"]["kind"], "PERMANENT", "{view}");
    assert_eq!(
        run["failureReport"]["reasonCode"], "provider_refused",
        "{view}"
    );
    assert_eq!(
        run["failureReport"]["params"],
        serde_json::json!({}),
        "{view}"
    );
    assert_eq!(
        run["failureReport"]["diagnostic"],
        serde_json::json!({}),
        "{view}"
    );
    assert_eq!(view["job"]["resolution"]["kind"], "FAILED", "{view}");
    let root = gql::job_view(&fixture.gql(), fixture.admin(), s.a).await;
    let children: Vec<&str> = root["job"]["children"]
        .as_array()
        .expect("the children of A")
        .iter()
        .filter_map(|child| child["job"]["id"].as_str())
        .collect();
    assert_eq!(children.len(), 3, "{root}");
    assert_eq!(
        root["job"]["progression"]["plan"]["items"][0]["label"], "erased",
        "{root}"
    );
    let listed = gql::list_jobs(&fixture.gql(), fixture.admin(), serde_json::json!({})).await;
    assert!(
        listed.iter().any(|row| row["job"]["id"] == s.a.to_string()),
        "the erased Tree still lists: {listed:?}"
    );
}
