use super::*;
use serde_json::json;
use uuid::Uuid;

fn trigger_v2(with_passport: bool) -> serde_json::Value {
    let mut wire = json!({
        "run_id": Uuid::now_v7(),
        "job_id": Uuid::now_v7(),
        "attempt": 1,
        "parent_job_id": Uuid::now_v7(),
        "initial_job_id": Uuid::now_v7(),
    });
    if with_passport {
        wire["passport"] = json!({ "kind": "service" });
    }
    wire
}

#[test]
fn a_v2_claim_reads_a_missing_version_as_2_and_a_v1_claim_as_1() {
    let wire = json!({ "instance_key": "pod-7", "claim_id": Uuid::now_v7(), "wait_ms": 10 });
    let v2: runner::ClaimV2 = serde_json::from_value(wire.clone()).unwrap();
    let v1: runner::Claim = serde_json::from_value(wire).unwrap();
    assert_eq!(v2.version, runner::WIRE_VERSION_V2);
    assert_eq!(v1.version, runner::WIRE_VERSION);
}

#[test]
fn a_v2_trigger_without_a_passport_does_not_parse_and_one_with_it_does() {
    assert!(serde_json::from_value::<runner::TriggerV2>(trigger_v2(false)).is_err());
    let parsed: runner::TriggerV2 = serde_json::from_value(trigger_v2(true)).unwrap();
    assert_eq!(parsed.passport, json!({ "kind": "service" }));
}

#[test]
fn a_v2_claim_reply_serving_a_run_without_a_passport_does_not_parse() {
    let without = json!({ "version": 2, "run": trigger_v2(false) });
    assert!(serde_json::from_value::<runner::ClaimReplyV2>(without).is_err());
    let with = json!({ "version": 2, "run": trigger_v2(true) });
    assert!(serde_json::from_value::<runner::ClaimReplyV2>(with).is_ok());
}

#[test]
fn a_surface_refusal_outside_the_closed_set_of_nine_keeps_its_word() {
    let known = [
        "run_unknown",
        "run_not_started",
        "run_ended",
        "run_removed",
        "instance_mismatch",
        "agent_id_in_other_tree",
        "agent_id_not_finished",
        "item_not_yours",
        "malformed",
    ];
    for word in known {
        let reply: runner_surface::SurfaceReply =
            serde_json::from_value(json!({ "version": 1, "accepted": false, "reason": word }))
                .unwrap();
        assert!(reply.reason.as_ref().unwrap().known().is_some(), "{word}");
    }
    for word in ["job_not_of_run", "something_new"] {
        let reply: runner_surface::SurfaceReply =
            serde_json::from_value(json!({ "version": 1, "accepted": false, "reason": word }))
                .unwrap();
        assert_eq!(reply.reason, Some(reason::Reason::Unknown(word.to_owned())));
        assert_eq!(serde_json::to_value(&reply).unwrap()["reason"], word);
        let listing: runner_surface::ListAgentsReply =
            serde_json::from_value(json!({ "version": 1, "accepted": false, "reason": word }))
                .unwrap();
        assert_eq!(
            listing.reason,
            Some(reason::Reason::Unknown(word.to_owned()))
        );
    }
}

#[test]
fn a_v2_claim_refusal_outside_the_set_keeps_its_word_while_v1_refuses_to_parse_it() {
    let wire = json!({ "version": 2, "reason": "overloaded" });
    let v2: runner::ClaimReplyV2 = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(
        v2.reason,
        Some(reason::Reason::Unknown("overloaded".to_owned()))
    );
    assert!(serde_json::from_value::<runner::ClaimReply>(wire).is_err());
}
