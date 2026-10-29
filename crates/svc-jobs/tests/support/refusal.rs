//! What every refusal of the surface owes, whatever the capability: a closed
//! door at establishment, a structured stable code, and no data behind it.

use br_test_harness::verdict;
use reqwest::StatusCode;
use serde_json::Value;

/// A subscription refused during establishment: never opened, and carrying a
/// structured stable code. Returns the code.
pub fn subscription_refusal(status: StatusCode, body: &Value, what: &str) -> String {
    assert_ne!(
        status,
        StatusCode::OK,
        "{what} must be refused during establishment, never opened: {body}"
    );
    let code = verdict::mutation_error_code(body).unwrap_or_else(|| {
        panic!("{what}: a refused subscription still owes a structured code: {body}")
    });
    assert!(
        verdict::is_code_shaped(&code),
        "{what}: the refusal code must be a stable code, not prose: {code}"
    );
    code
}

/// A request closed at the trust boundary — no passport, or one the edge cannot
/// decode: 401 with a structured stable code. Returns the code.
pub fn unauthenticated_refusal(status: StatusCode, body: &Value, what: &str) -> String {
    assert_eq!(
        status,
        StatusCode::UNAUTHORIZED,
        "{what} must be closed at the trust boundary, never answered on a permissive default: \
         {body}"
    );
    let code = verdict::mutation_error_code(body).unwrap_or_else(|| {
        panic!("{what}: a refusal at the boundary still owes a structured code: {body}")
    });
    assert!(
        verdict::is_code_shaped(&code),
        "{what}: the refusal code must be a stable code, not prose: {code}"
    );
    code
}

/// A refused response carries none of `secrets` and no data at all.
pub fn assert_nothing_leaked(response: &Value, secrets: &[String], what: &str) {
    let rendered = response.to_string();
    for secret in secrets {
        assert!(
            !rendered.contains(secret.as_str()),
            "{what} must not leak state to a caller who may not read it ({secret}): {response}"
        );
    }
    let data = &response["data"];
    assert!(
        data.is_null()
            || data
                .as_object()
                .is_some_and(|fields| fields.values().all(Value::is_null)),
        "{what} must return no data at all: {response}"
    );
}

/// The first `next` frame of an event-stream body read whole — the one frame a
/// subscription refused by its resolver sends before it ends. Returns the
/// GraphQL response it carries, errors included.
pub fn first_stream_frame(body: &Value, what: &str) -> Value {
    let text = body
        .as_str()
        .unwrap_or_else(|| panic!("{what}: an event-stream answers with frames: {body}"));
    let data = text
        .lines()
        .find_map(|line| line.strip_prefix("data:"))
        .unwrap_or_else(|| panic!("{what}: the stream carried no frame: {text}"));
    serde_json::from_str(data.trim()).unwrap_or_else(|error| {
        panic!("{what}: a frame carries a GraphQL response ({error}): {data}")
    })
}
