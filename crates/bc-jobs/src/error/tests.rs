use super::*;

#[test]
fn the_code_is_the_stable_key_never_a_sentence() {
    // Given: a refusal carrying structured params
    let error = JobsError::RetryBudgetExhausted {
        attempts: 3,
        max_attempts: 3,
    };
    // When: the edge asks for its code and params
    // Then: the code is a snake_case key and the data lives in params
    assert_eq!(error.code(), "retry_budget_exhausted");
    assert_eq!(error.params(), json!({ "attempts": 3, "maxAttempts": 3 }));
}

#[test]
fn the_rendered_error_is_the_code_itself_never_an_interpolated_sentence() {
    // Given: refusals carrying ids, statuses and free text
    let carriers = [
        JobsError::JobIdConflict {
            job_id: Uuid::from_u128(7),
        },
        JobsError::JobAlreadyTerminal { status: "FAILED" },
        JobsError::InvalidSegment {
            field: "producer",
            value: "Not A Key".to_owned(),
        },
    ];
    // When: the edge renders them
    // Then: Display is the code, so no payload can ever reach the user as language
    for error in carriers {
        assert_eq!(error.to_string(), error.code());
        assert!(
            error
                .code()
                .chars()
                .all(|character| character.is_ascii_lowercase() || character == '_'),
            "{} is not a stable snake_case key",
            error.code()
        );
    }
}

#[test]
fn params_never_repeat_the_code() {
    // Given: an error whose params identify the offending aggregate
    let job_id = Uuid::from_u128(7);
    let error = JobsError::JobIdConflict { job_id };
    // When/Then: params carry the id, the code stays free of interpolation
    assert_eq!(error.code(), "job_id_conflict");
    assert_eq!(error.params(), json!({ "jobId": job_id }));
}
