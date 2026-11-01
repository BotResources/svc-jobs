use super::*;
use serde_json::json;

fn analyst() -> SubjectSegment {
    SubjectSegment::runner_type("analyst").unwrap()
}

fn code_of(reason: SurfaceReason) -> &'static str {
    match reason {
        SurfaceReason::RunUnknown => "run_unknown",
        SurfaceReason::RunNotStarted => "run_not_started",
        SurfaceReason::RunEnded => "run_ended",
        SurfaceReason::RunRemoved => "run_removed",
        SurfaceReason::InstanceMismatch => "instance_mismatch",
        SurfaceReason::AgentIdInOtherTree => "agent_id_in_other_tree",
        SurfaceReason::AgentIdNotFinished => "agent_id_not_finished",
        SurfaceReason::ItemNotYours => "item_not_yours",
        SurfaceReason::Malformed => "malformed",
        SurfaceReason::InstanceUnknown => "instance_unknown",
        SurfaceReason::RunHeld => "run_held",
        SurfaceReason::RunReservedElsewhere => "run_reserved_elsewhere",
        SurfaceReason::RunNotPending => "run_not_pending",
    }
}

const ALL_REASONS: [SurfaceReason; 13] = [
    SurfaceReason::RunUnknown,
    SurfaceReason::RunNotStarted,
    SurfaceReason::RunEnded,
    SurfaceReason::RunRemoved,
    SurfaceReason::InstanceMismatch,
    SurfaceReason::AgentIdInOtherTree,
    SurfaceReason::AgentIdNotFinished,
    SurfaceReason::ItemNotYours,
    SurfaceReason::Malformed,
    SurfaceReason::InstanceUnknown,
    SurfaceReason::RunHeld,
    SurfaceReason::RunReservedElsewhere,
    SurfaceReason::RunNotPending,
];

#[test]
fn each_request_renders_its_own_subject_for_the_runner_type() {
    let analyst = analyst();
    assert_eq!(
        register_agents_subject(&analyst),
        "jobs.runner.register_agents.analyst"
    );
    assert_eq!(mark_read_subject(&analyst), "jobs.runner.mark_read.analyst");
    assert_eq!(
        list_agents_subject(&analyst),
        "jobs.runner.list_agents.analyst"
    );
    assert_eq!(started_subject(&analyst), "jobs.runner.started.analyst");
}

#[test]
fn the_four_requests_have_each_a_filter_and_a_prefix_that_match_their_subject() {
    let analyst = analyst();
    let surface = [
        (STARTED_FILTER, STARTED_PREFIX, started_subject(&analyst)),
        (
            REGISTER_AGENTS_FILTER,
            REGISTER_AGENTS_PREFIX,
            register_agents_subject(&analyst),
        ),
        (
            MARK_READ_FILTER,
            MARK_READ_PREFIX,
            mark_read_subject(&analyst),
        ),
        (
            LIST_AGENTS_FILTER,
            LIST_AGENTS_PREFIX,
            list_agents_subject(&analyst),
        ),
    ];
    for (filter, prefix, subject) in &surface {
        assert_eq!(*filter, format!("{prefix}*"));
        assert_eq!(subject, &format!("{prefix}analyst"));
    }
    assert_eq!(STARTED_FILTER, "jobs.runner.started.*");
    assert_eq!(STARTED_PREFIX, "jobs.runner.started.");
    let mut filters: Vec<&str> = surface.iter().map(|(filter, _, _)| *filter).collect();
    filters.sort_unstable();
    filters.dedup();
    assert_eq!(filters.len(), 4);
}

#[test]
fn a_runner_type_that_could_widen_the_started_subject_is_refused_before_it_is_rendered() {
    for text in ["a.b", "*", ">", "a b", ""] {
        assert!(SubjectSegment::runner_type(text).is_err(), "'{text}'");
    }
}

#[test]
fn a_registration_without_a_declared_restart_reads_as_not_restartable() {
    let agent: AgentDeclaration = serde_json::from_value(json!({
        "agent_id": Uuid::nil().to_string(),
        "has_inbox": true,
    }))
    .unwrap();
    assert!(!agent.declares_restart);
}

#[test]
fn an_agent_declaration_requires_its_inbox_flag() {
    let missing = serde_json::from_value::<AgentDeclaration>(json!({
        "agent_id": Uuid::nil().to_string(),
    }));
    assert!(missing.is_err());
}

#[test]
fn the_three_older_requests_read_a_missing_version_as_one() {
    let run_id = Uuid::now_v7().to_string();
    let register: RegisterAgents =
        serde_json::from_value(json!({ "run_id": run_id, "instance_key": "pod-7", "agents": [] }))
            .unwrap();
    let mark: MarkRead = serde_json::from_value(
        json!({ "run_id": run_id, "instance_key": "pod-7", "item_ids": [] }),
    )
    .unwrap();
    let list: ListAgents =
        serde_json::from_value(json!({ "run_id": run_id, "instance_key": "pod-7" })).unwrap();
    assert_eq!((register.version, mark.version, list.version), (1, 1, 1));
}

#[test]
fn a_refusal_carries_its_reason_and_its_words_and_an_acceptance_neither() {
    let accepted = serde_json::to_value(SurfaceReply::accepted()).unwrap();
    assert_eq!(accepted, json!({ "version": 1, "accepted": true }));
    let refused = serde_json::to_value(SurfaceReply::refused(
        SurfaceReason::RunNotStarted,
        "The run has not started.",
    ))
    .unwrap();
    assert_eq!(refused["reason"], "run_not_started");
    assert_eq!(refused["accepted"], false);
    assert_eq!(refused["message"], "The run has not started.");
}

#[test]
fn the_list_reply_shares_the_thirteen_codes() {
    for reason in ALL_REASONS {
        let reply = ListAgentsReply::refused(reason, "No.");
        let wire = serde_json::to_value(&reply).unwrap();
        assert_eq!(wire["reason"], code_of(reason));
        assert_eq!(
            serde_json::from_value::<ListAgentsReply>(wire).unwrap(),
            reply
        );
    }
}

#[test]
fn every_reason_travels_as_its_snake_case_code() {
    for reason in ALL_REASONS {
        let code = code_of(reason);
        assert_eq!(serde_json::to_value(reason).unwrap(), code);
        assert_eq!(
            serde_json::from_value::<SurfaceReason>(json!(code)).unwrap(),
            reason
        );
    }
}

#[test]
fn the_reasons_are_thirteen_distinct_codes() {
    let mut codes: Vec<&str> = ALL_REASONS.iter().map(|reason| code_of(*reason)).collect();
    codes.sort_unstable();
    codes.dedup();
    assert_eq!(codes.len(), 13);
}

#[test]
fn a_word_outside_the_set_is_not_a_surface_reason() {
    for word in [
        "unavailable",
        "start_id_conflict",
        "RUN_HELD",
        "run-held",
        "",
    ] {
        assert!(
            serde_json::from_value::<SurfaceReason>(json!(word)).is_err(),
            "'{word}'"
        );
    }
    assert!(serde_json::from_value::<SurfaceReason>(json!(7)).is_err());
}

#[test]
fn a_reply_keeps_a_reason_outside_the_set_as_the_word_it_read() {
    let reply: SurfaceReply = serde_json::from_value(json!({
        "version": 1,
        "accepted": false,
        "reason": "start_id_conflict",
        "message": "No.",
    }))
    .unwrap();
    assert_eq!(
        reply.reason,
        Some(Reason::Unknown("start_id_conflict".to_owned()))
    );
    assert_eq!(
        serde_json::to_value(&reply).unwrap()["reason"],
        "start_id_conflict"
    );
}

#[test]
fn a_reply_requires_its_version_and_reads_a_null_reason_as_absent() {
    assert!(serde_json::from_value::<SurfaceReply>(json!({ "accepted": true })).is_err());
    assert!(serde_json::from_value::<ListAgentsReply>(json!({ "accepted": true })).is_err());
    let reply: SurfaceReply = serde_json::from_value(
        json!({ "version": 1, "accepted": true, "reason": null, "message": null }),
    )
    .unwrap();
    assert_eq!(reply.reason, None);
    assert!(
        serde_json::from_value::<SurfaceReply>(
            json!({ "version": 1, "accepted": false, "reason": 5 })
        )
        .is_err()
    );
}
