use crate::domain::job::resolution::JobResolution;
use crate::domain::job::{Job, JobState};
use crate::error::JobsError;
use crate::fixtures::{JobBuilder, resolution_id, ts};
use chrono::{DateTime, Utc};

fn cancelled_at(at: DateTime<Utc>) -> JobBuilder {
    JobBuilder::new().with_resolution(JobResolution::cancelled(resolution_id(), at))
}

#[test]
fn a_job_whose_request_data_was_erased_keeps_its_first_time() {
    let first = ts(200);
    let job = cancelled_at(ts(100))
        .with_request_data_erased(first)
        .build();
    assert_eq!(job.content_erased_at(), Some(first));
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
fn a_job_that_is_not_resolved_cannot_carry_an_erasure_mark() {
    let unresolved = JobBuilder::new().state();
    let erased = JobState {
        content_erased_at: Some(ts(100)),
        config: None,
        passport: None,
        payload: None,
        ..unresolved.clone()
    };
    let scrubbed = JobState {
        tree_scrubbed: true,
        ..unresolved
    };
    for state in [erased, scrubbed] {
        assert_eq!(
            Job::hydrate(state).err(),
            Some(JobsError::CorruptState {
                reason_code: "erased_job_not_resolved"
            })
        );
    }
}

#[test]
fn a_resolved_job_of_a_scrubbed_tree_loads() {
    let job = cancelled_at(ts(100)).in_a_scrubbed_tree(ts(200)).build();
    assert!(job.tree_scrubbed());
    assert_eq!(job.content_erased_at(), Some(ts(200)));
}

#[test]
fn a_completed_job_whose_request_data_was_erased_loads() {
    let job = JobBuilder::new()
        .with_resolution(JobResolution::completed(resolution_id(), ts(100)))
        .with_request_data_erased(ts(150))
        .build();
    assert_eq!(job.content_erased_at(), Some(ts(150)));
    assert!(job.passport().is_none());
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
