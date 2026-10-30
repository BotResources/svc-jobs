mod guard;
mod input;
mod mutation;
mod output;
mod query;
mod unread;

pub use guard::{RUNNER_SCOPE, holds_runner_scope};
pub use mutation::RunnerMutation;
pub use query::RunnerQuery;

pub const RUNNER_SURFACE: &[&str] = &[
    "jobsUploadPayload",
    "jobsPayload",
    "jobsUploadResult",
    "jobsReadResult",
    "jobsUploadAttachment",
    "jobsAttachment",
    "jobsSendMail",
    "jobsUnread",
    "jobsMail",
    "jobsSaveState",
    "jobsSavedState",
];
