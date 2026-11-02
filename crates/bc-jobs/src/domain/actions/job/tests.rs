use super::*;
use crate::domain::access::{PlatformRole, PrincipalKind};
use crate::domain::job::resolution::{JobFailureCause, JobResolution};
use crate::domain::run::failure::RunFailureKind;
use crate::fixtures::{JobBuilder, RunBuilder, job_id, resolution_id, ts};
use serde_json::json;

fn super_user() -> PrincipalFacts {
    PrincipalFacts::new(PrincipalKind::Human, true, PlatformRole::SuperUser)
}

fn member() -> PrincipalFacts {
    PrincipalFacts::new(PrincipalKind::Human, true, PlatformRole::Member)
}

fn failed_job() -> JobBuilder {
    JobBuilder::new()
        .with_run(
            RunBuilder::new(1)
                .started(ts(5))
                .failed(ts(20), RunFailureKind::Permanent)
                .build(),
        )
        .with_resolution(
            JobResolution::failed(
                resolution_id(),
                ts(30),
                JobFailureCause::DeclaredByOwner,
                None,
            )
            .unwrap(),
        )
}

#[test]
fn a_live_job_may_be_cancelled_but_neither_retried_nor_deleted() {
    let job = JobBuilder::new()
        .with_run(RunBuilder::new(1).build())
        .build();
    assert_eq!(job.can_cancel(), Availability::Available);
    assert_eq!(
        job.can_manual_retry(ParentContext::Root)
            .unwrap()
            .reason_code(),
        Some("job_not_failed")
    );
    assert_eq!(job.can_delete().reason_code(), Some("job_not_terminal"));
}

#[test]
fn a_failed_job_may_be_retried_and_deleted_but_no_longer_cancelled() {
    let job = failed_job().build();
    assert_eq!(
        job.can_manual_retry(ParentContext::Root).unwrap(),
        Availability::Available
    );
    assert_eq!(job.can_delete(), Availability::Available);
    assert_eq!(job.can_cancel().reason_code(), Some("job_already_terminal"));
}

#[test]
fn a_failed_job_of_a_scrubbed_tree_is_refused_a_retry_with_the_scrub_reason() {
    let job = failed_job().in_a_scrubbed_tree(ts(900)).build();
    assert_eq!(
        job.can_manual_retry(ParentContext::Root).unwrap(),
        Availability::Blocked {
            reason_code: "tree_scrubbed".to_owned(),
            params: json!({})
        }
    );
    assert_eq!(
        job.guard_manual_retry(ParentContext::Root),
        Err(JobsError::TreeScrubbed)
    );
    let listed = job.affordances(ParentContext::Root, &super_user()).unwrap();
    let retry = listed
        .iter()
        .find(|affordance| affordance.action() == MANUAL_RETRY)
        .expect("the retry is always listed");
    assert!(!retry.allowed());
    assert_eq!(retry.reason_code(), Some("tree_scrubbed"));
}

#[test]
fn a_failed_job_already_retried_may_not_be_retried_again() {
    let successor = job_id();
    let job = failed_job().manually_retried_by(successor, false).build();
    assert_eq!(
        job.can_manual_retry(ParentContext::Root).unwrap(),
        Availability::Blocked {
            reason_code: "manual_retry_already_started".to_owned(),
            params: json!({ "successorJobId": successor.as_uuid() })
        }
    );
}

#[test]
fn a_failed_child_may_be_retried_while_the_parent_that_owns_it_still_runs() {
    let parent = JobBuilder::new()
        .with_run(RunBuilder::new(1).started(ts(5)).build())
        .build();
    let child = failed_job().with_parent(parent.id()).build();
    assert_eq!(
        child
            .can_manual_retry(ParentContext::Loaded(&parent))
            .unwrap(),
        Availability::Available
    );
}

#[test]
fn a_childs_affordances_are_never_computed_without_its_parent() {
    let child = failed_job().with_parent(job_id()).build();
    let result = child.affordances(ParentContext::Root, &super_user());
    assert_eq!(
        result.err(),
        Some(JobsError::CorruptState {
            reason_code: "parent_job_not_loaded"
        })
    );
}

#[test]
fn a_failed_child_is_never_retried_under_a_parent_that_has_finished() {
    let parent = failed_job().build();
    let child = failed_job().with_parent(parent.id()).build();
    let verdict = child
        .can_manual_retry(ParentContext::Loaded(&parent))
        .unwrap();
    assert_eq!(
        child.guard_manual_retry(ParentContext::Loaded(&parent)),
        Err(JobsError::ParentJobTerminal {
            parent_job_id: parent.id().as_uuid()
        })
    );
    assert_eq!(verdict.reason_code(), Some("parent_job_terminal"));
}

#[test]
fn deletion_waits_while_the_manual_retry_successor_is_still_running() {
    let successor = job_id();
    let job = failed_job().manually_retried_by(successor, false).build();
    assert_eq!(
        job.can_delete(),
        Availability::Blocked {
            reason_code: "successor_still_active".to_owned(),
            params: json!({ "successorJobId": successor.as_uuid() })
        }
    );
}

#[test]
fn deletion_opens_once_the_manual_retry_successor_is_terminal() {
    let job = failed_job().manually_retried_by(job_id(), true).build();
    assert_eq!(job.can_delete(), Availability::Available);
}

#[test]
fn a_deleted_job_offers_no_action_at_all() {
    let job = failed_job().deleted(ts(90)).build();
    let affordances = job.affordances(ParentContext::Root, &super_user()).unwrap();
    assert_eq!(
        affordances
            .iter()
            .map(|entry| (entry.action(), entry.reason_code()))
            .collect::<Vec<_>>(),
        vec![
            (CANCEL, Some("job_deleted")),
            (MANUAL_RETRY, Some("job_deleted")),
            (DELETE, Some("job_already_deleted")),
            (FORCE_PRIORITY, Some("job_deleted")),
        ]
    );
}

#[test]
fn the_affordance_surface_names_every_administrator_action() {
    let job = JobBuilder::new().build();
    let actions: Vec<&str> = job
        .affordances(ParentContext::Root, &super_user())
        .unwrap()
        .iter()
        .map(|entry| entry.action())
        .collect();
    assert_eq!(actions, vec![CANCEL, MANUAL_RETRY, DELETE, FORCE_PRIORITY]);
}

#[test]
fn the_affordance_and_the_command_refusal_are_the_same_verdict() {
    let job = JobBuilder::new().build();
    let refusal = job.guard_delete().unwrap_err();
    assert_eq!(job.can_delete().reason_code(), Some(refusal.code()));
}

#[test]
fn a_super_user_may_force_a_live_job_whatever_it_is_doing() {
    let waiting = JobBuilder::new().build();
    let running = JobBuilder::new()
        .with_run(RunBuilder::new(1).started(ts(5)).build())
        .build();
    assert_eq!(
        waiting.can_force_priority(&super_user()),
        Availability::Available
    );
    assert_eq!(
        running.can_force_priority(&super_user()),
        Availability::Available
    );
}

#[test]
fn a_principal_who_is_not_a_super_user_may_not_force_a_priority() {
    let job = JobBuilder::new().build();
    for principal in [
        member(),
        PrincipalFacts::new(PrincipalKind::Service, true, PlatformRole::SuperUser),
    ] {
        assert_eq!(
            job.can_force_priority(&principal).reason_code(),
            Some("not_super_user")
        );
    }
}

#[test]
fn a_terminal_job_has_no_priority_left_to_force() {
    let job = failed_job().build();
    assert_eq!(
        job.can_force_priority(&super_user()).reason_code(),
        Some("job_already_terminal")
    );
}

#[test]
fn the_force_affordance_and_the_force_guard_are_the_same_verdict() {
    let job = failed_job().build();
    for principal in [super_user(), member()] {
        let refusal = job.guard_force_priority(&principal).unwrap_err();
        assert_eq!(
            job.can_force_priority(&principal).reason_code(),
            Some(refusal.code())
        );
    }
}
