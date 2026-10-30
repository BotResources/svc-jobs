pub const UPLOAD_PAYLOAD_ROOT: &str = "jobsUploadPayload";
pub const PAYLOAD_ROOT: &str = "jobsPayload";
pub const UPLOAD_RESULT_ROOT: &str = "jobsUploadResult";
pub const READ_RESULT_ROOT: &str = "jobsReadResult";
pub const UPLOAD_ATTACHMENT_ROOT: &str = "jobsUploadAttachment";
pub const ATTACHMENT_ROOT: &str = "jobsAttachment";
pub const SEND_MAIL_ROOT: &str = "jobsSendMail";
pub const UNREAD_ROOT: &str = "jobsUnread";
pub const MAIL_ROOT: &str = "jobsMail";
pub const SAVE_STATE_ROOT: &str = "jobsSaveState";
pub const SAVED_STATE_ROOT: &str = "jobsSavedState";

pub const RUNNER_ROOTS: [&str; 11] = [
    UPLOAD_PAYLOAD_ROOT,
    PAYLOAD_ROOT,
    UPLOAD_RESULT_ROOT,
    READ_RESULT_ROOT,
    UPLOAD_ATTACHMENT_ROOT,
    ATTACHMENT_ROOT,
    SEND_MAIL_ROOT,
    UNREAD_ROOT,
    MAIL_ROOT,
    SAVE_STATE_ROOT,
    SAVED_STATE_ROOT,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_runner_roots_are_eleven_distinct_names() {
        let mut names: Vec<&str> = RUNNER_ROOTS.to_vec();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), 11);
    }
}
