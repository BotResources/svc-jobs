pub fn plain_message(reason_code: &str) -> String {
    let words = match reason_code {
        "id_reuse" => "The job id is already used by a different declaration.",
        "duplicate_active_entity" => "Another job of the same source entity is still active.",
        "malformed_payload" => "The command could not be read.",
        "invalid_priority" => "The priority must be a level from 1 to 5.",
        "passport_required" => "A job with no parent needs the Passport of its sender.",
        "passport_malformed" => "The Passport is not a valid Passport.",
        "passport_user_mismatch" => "The triggering user is not the user of the Passport.",
        "passport_parent_mismatch" => "The Passport is not the one of the parent job.",
        "initial_job_mismatch" => "The initial job is not the initial job of the parent.",
        "parent_terminal" => "The parent job has already ended.",
        "parent_job_unknown" => "The parent job does not exist.",
        "parent_not_of_run" => "The parent job is not the job the acting run works.",
        "runner_type_retired" => "The runner type is retired and accepts no new job.",
        "payload_too_large" => "The inline payload is larger than the setting allows.",
        "payload_not_uploaded" => "The payload kept by Jobs was not uploaded under this job id.",
        "payload_uploader_mismatch" => "The payload was uploaded by another sender.",
        "mode_required" => "A call from a runner must say whether it is SYNC or BACKGROUND.",
        "caller_address_not_held" => "A BACKGROUND call needs a caller address.",
        "caller_address_not_of_run" => "The caller address is not an address of the acting run.",
        "max_attempts_above_ceiling" => "The attempts asked are above the ceiling of the service.",
        "acting_run_missing" => "The acting run does not exist.",
        "acting_run_not_live" => "The acting run is not started on the named instance.",
        "not_requester" => "Only the requester of the job may do this.",
        "runner_not_working_the_job" => "Only the runner that works the job may cancel it.",
        "job_unknown" => "The job does not exist.",
        "job_terminal" => "The job has already ended.",
        _ => "The command was refused.",
    };
    words.to_owned()
}

pub fn failure_message(
    job_id: uuid::Uuid,
    cause: &str,
    reason_code: Option<&str>,
    retryable: bool,
    run_id: Option<uuid::Uuid>,
) -> String {
    let run = run_id.map_or_else(String::new, |run| format!(" in run {run}"));
    match (cause, reason_code) {
        ("TERMINAL_RUN_FAILURE", Some(reason)) if retryable => format!(
            "Job {job_id} failed{run}: {reason}. The failure can be retried but the attempts are spent."
        ),
        ("TERMINAL_RUN_FAILURE", Some(reason)) => format!(
            "Job {job_id} failed{run}: {reason}. The failure is permanent, so no retry was made."
        ),
        ("DECLARED_BY_OWNER", _) => {
            format!("Job {job_id} was failed by its requester.")
        }
        ("INACTIVITY_TIMEOUT", _) => {
            format!("Job {job_id} failed{run}: it made no progress for too long.")
        }
        _ => format!("Job {job_id} failed{run}."),
    }
}
