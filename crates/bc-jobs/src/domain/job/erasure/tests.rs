use super::*;
use crate::domain::job::JobState;
use crate::domain::job::resolution::JobResolution;
use crate::error::JobsError;
use crate::fixtures::{JobBuilder, RunBuilder, resolution_id, ts};
use chrono::TimeDelta;

fn retention(cancelled_days: i64) -> Retention {
    Retention::from_days(30, cancelled_days).unwrap()
}

fn cancelled_at(at: DateTime<Utc>) -> JobBuilder {
    JobBuilder::new().with_resolution(JobResolution::cancelled(resolution_id(), at))
}

#[test]
fn with_no_delay_a_cancelled_job_is_due_the_moment_it_is_cancelled() {
    let job = cancelled_at(ts(100)).build();
    assert!(job.request_data_erasure_due(retention(0), ts(100)));
    assert!(retention(0).erases_cancelled_request_data_at_once());
}

#[test]
fn with_a_delay_a_cancelled_job_is_due_only_once_the_days_have_passed_since_the_cancel() {
    let at = ts(100);
    let job = cancelled_at(at).build();
    let three_days = retention(3);
    assert!(!three_days.erases_cancelled_request_data_at_once());
    assert!(
        !job.request_data_erasure_due(three_days, at + TimeDelta::days(3) - TimeDelta::seconds(1))
    );
    assert!(job.request_data_erasure_due(three_days, at + TimeDelta::days(3)));
    assert!(job.request_data_erasure_due(three_days, at + TimeDelta::days(30)));
}

#[test]
fn a_job_that_is_not_cancelled_is_never_due() {
    let live = JobBuilder::new()
        .with_run(RunBuilder::new(1).build())
        .build();
    let completed = JobBuilder::new()
        .with_resolution(JobResolution::completed(resolution_id(), ts(100)))
        .build();
    for job in [live, completed] {
        assert!(!job.request_data_erasure_due(retention(0), ts(10_000_000)));
    }
}

#[test]
fn a_job_whose_request_data_was_erased_is_not_due_again_and_keeps_its_first_time() {
    let first = ts(200);
    let job = cancelled_at(ts(100))
        .with_request_data_erased(first)
        .build();
    assert!(!job.request_data_erasure_due(retention(0), ts(10_000_000)));
    assert_eq!(job.content_erased_at(), Some(first));
    assert!(job.has_request_data_erased());
}

#[test]
fn an_erased_job_loads_with_no_passport_no_payload_and_no_configuration() {
    let job = cancelled_at(ts(100))
        .with_request_data_erased(ts(100))
        .build();
    assert!(job.passport().is_none());
    assert!(job.payload().is_none());
    assert!(job.config().is_none());
    assert_eq!(
        job.status(),
        crate::domain::job::status::JobStatus::Cancelled
    );
}

#[test]
fn a_job_marked_erased_that_still_holds_request_data_cannot_be_loaded() {
    let state = JobState {
        content_erased_at: Some(ts(100)),
        ..cancelled_at(ts(100)).state()
    };
    assert_eq!(
        Job::hydrate(state).err(),
        Some(JobsError::CorruptState {
            reason_code: "erased_job_holds_request_data"
        })
    );
}

#[test]
fn a_stored_job_queued_body_without_its_request_data_still_reads_back() {
    use crate::event::job::JobEvent;
    use crate::event::job_facts::JobQueued;
    use crate::event::run_inbox::samples::{Kind, every_kind_of_event, kind_of};

    let (JobEvent::JobQueued(fact), _) = every_kind_of_event()
        .into_iter()
        .find(|(event, _)| kind_of(event) == Kind::JobQueued)
        .expect("a queued sample")
    else {
        panic!("the queued sample is a queued event");
    };
    assert!(fact.passport.is_some(), "the sample carries a Passport");
    let mut body = serde_json::to_value(&*fact).unwrap();
    let keys = body.as_object_mut().unwrap();
    for key in ["config", "passport", "payload"] {
        keys.remove(key);
    }
    let read: JobQueued = serde_json::from_value(body).expect("the rewritten body reads back");
    assert_eq!(read.job_id, fact.job_id);
    assert!(read.config.is_none() && read.passport.is_none() && read.payload.is_none());
}
