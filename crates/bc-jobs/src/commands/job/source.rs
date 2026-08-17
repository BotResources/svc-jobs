use crate::domain::job::Job;
use crate::domain::references::SourceReference;
use crate::error::JobsError;

pub(crate) fn guard_source_free(
    source: Option<SourceReference>,
    active_for_source: Option<&Job>,
) -> Result<(), JobsError> {
    let Some(active) = active_for_source else {
        return Ok(());
    };
    let Some(source) = source else {
        return Err(JobsError::CorruptState {
            reason_code: "active_job_supplied_for_a_sourceless_command",
        });
    };
    if active.source().as_ref() != Some(&source) {
        return Err(JobsError::CorruptState {
            reason_code: "active_job_is_not_for_the_command_source",
        });
    }
    if active.is_terminal() {
        Ok(())
    } else {
        Err(JobsError::SourceAlreadyActive {
            active_job_id: active.id().as_uuid(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::job::resolution::JobResolution;
    use crate::fixtures::{JobBuilder, resolution_id, source_entity_id, ts};

    #[test]
    fn a_live_job_holding_the_same_source_blocks_the_command() {
        // Given: a live job already covering this source reference
        let entity = source_entity_id();
        let active = JobBuilder::new().with_source(entity).build();
        // When: a command carrying that same source is judged
        let result = guard_source_free(active.source(), Some(&active));
        // Then: it is refused, naming the job that holds the source
        assert_eq!(
            result,
            Err(JobsError::SourceAlreadyActive {
                active_job_id: active.id().as_uuid()
            })
        );
    }

    #[test]
    fn a_settled_holder_of_the_source_frees_it() {
        // Given: the previous job for this source reached a terminal state
        let entity = source_entity_id();
        let settled = JobBuilder::new()
            .with_source(entity)
            .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
            .build();
        // When/Then: the source is free again
        assert_eq!(guard_source_free(settled.source(), Some(&settled)), Ok(()));
    }

    #[test]
    fn a_job_holding_another_source_is_never_taken_for_this_one() {
        // Given: a live job covering a different source reference
        let other = JobBuilder::new().with_source(source_entity_id()).build();
        let command_source = JobBuilder::new()
            .with_source(source_entity_id())
            .build()
            .source();
        // When: it is offered as the holder of this command's source
        let result = guard_source_free(command_source, Some(&other));
        // Then: the mismatch fails loud rather than blocking a legitimate command
        assert_eq!(
            result,
            Err(JobsError::CorruptState {
                reason_code: "active_job_is_not_for_the_command_source"
            })
        );
    }

    #[test]
    fn a_holder_offered_for_a_sourceless_command_fails_loud() {
        // Given: a command that carries no source reference at all
        let active = JobBuilder::new().with_source(source_entity_id()).build();
        // When: a source holder is supplied anyway
        let result = guard_source_free(None, Some(&active));
        // Then: the wiring gap is refused rather than silently rejecting the command
        assert_eq!(
            result,
            Err(JobsError::CorruptState {
                reason_code: "active_job_supplied_for_a_sourceless_command"
            })
        );
    }

    #[test]
    fn a_command_with_no_holder_to_compare_passes() {
        // Given: no job currently holds the command's source
        let command_source = JobBuilder::new()
            .with_source(source_entity_id())
            .build()
            .source();
        // When/Then: nothing stands in the way
        assert_eq!(guard_source_free(command_source, None), Ok(()));
    }
}
