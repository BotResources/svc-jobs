use super::*;
use serde_json::json;

#[test]
fn the_started_request_of_the_offer_reads_and_writes_the_same_fields() {
    let run_id = Uuid::now_v7();
    let start_id = Uuid::now_v7();
    let wire = json!({
        "version": 1,
        "run_id": run_id.to_string(),
        "instance_key": "pod-7",
        "start_id": start_id.to_string(),
    });
    let request: StartedRequest = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(
        request,
        StartedRequest {
            version: 1,
            run_id,
            instance_key: "pod-7".to_owned(),
            start_id,
        }
    );
    assert_eq!(serde_json::to_value(&request).unwrap(), wire);
}

#[test]
fn the_started_request_reads_a_missing_version_as_one_and_requires_every_other_field() {
    let run_id = Uuid::now_v7().to_string();
    let start_id = Uuid::now_v7().to_string();
    let without_version: StartedRequest = serde_json::from_value(
        json!({ "run_id": run_id, "instance_key": "pod-7", "start_id": start_id }),
    )
    .unwrap();
    assert_eq!(without_version.version, SURFACE_WIRE_VERSION);
    let complete = json!({
        "version": 1,
        "run_id": run_id,
        "instance_key": "pod-7",
        "start_id": start_id,
    });
    for field in ["run_id", "instance_key", "start_id"] {
        let mut incomplete = complete.clone();
        incomplete.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<StartedRequest>(incomplete).is_err(),
            "a started request without {field} must not parse"
        );
    }
}

#[test]
fn the_started_request_does_not_judge_its_ids_nor_its_instance_key() {
    let request: StartedRequest = serde_json::from_value(json!({
        "version": 7,
        "run_id": Uuid::nil().to_string(),
        "instance_key": "pod 7.>",
        "start_id": "3f2b8c1e-5d4a-4e6f-9a7b-1c2d3e4f5a6b",
    }))
    .unwrap();
    assert_eq!(request.version, 7);
    assert_eq!(request.instance_key, "pod 7.>");
}

#[test]
fn the_answers_of_the_started_request_travel_as_the_surface_reply() {
    for (reason, code) in [
        (SurfaceReason::RunUnknown, "run_unknown"),
        (SurfaceReason::InstanceUnknown, "instance_unknown"),
        (SurfaceReason::RunHeld, "run_held"),
        (
            SurfaceReason::RunReservedElsewhere,
            "run_reserved_elsewhere",
        ),
        (SurfaceReason::RunNotPending, "run_not_pending"),
        (SurfaceReason::Malformed, "malformed"),
    ] {
        let wire = json!({
            "version": 1,
            "accepted": false,
            "reason": code,
            "message": "Plain words.",
        });
        let reply: SurfaceReply = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(reply.reason, Some(Reason::Known(reason)));
        assert_eq!(
            serde_json::to_value(SurfaceReply::refused(reason, "Plain words.")).unwrap(),
            wire
        );
    }
}
