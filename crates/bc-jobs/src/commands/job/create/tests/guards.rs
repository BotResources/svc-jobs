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

#[test]
fn a_live_job_holding_another_source_never_blocks_a_creation() {
    // Given: a live job that holds a different source reference
    let unrelated = JobBuilder::new().with_source(source_entity_id()).build();
    let command = CreateJob {
        source_entity_id: Some(source_entity_id()),
        ..command()
    };
    // When: it is offered as the holder of this command's source
    let result = create_job(
        command,
        None,
        Some(&unrelated),
        None,
        &ServiceLimits::default(),
    );
    // Then: the mismatch fails loud rather than rejecting a source that is in fact free
    assert_eq!(
        result,
        Err(JobsError::CorruptState {
            reason_code: "active_job_is_not_for_the_command_source"
        })
    );
}
