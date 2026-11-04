use super::*;

#[test]
fn a_job_loaded_under_another_id_never_absorbs_a_creation() {
    // Given: a lookup that handed back a job which is not the command's target
    let stranger = JobBuilder::new().build();
    let command = command();
    // When: the creation is judged against it
    let result = create_job(
        command,
        Some(&stranger),
        None,
        None,
        &ServiceLimits::default(),
    );
    // Then: the mismatch fails loud rather than swallowing a legitimate creation
    assert_eq!(
        result,
        Err(JobsError::CorruptState {
            reason_code: "existing_job_is_not_the_command_target"
        })
    );
}
