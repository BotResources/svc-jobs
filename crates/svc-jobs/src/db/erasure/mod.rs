mod mail;
mod request_data;

pub use mail::{CANDIDATE_MAILS_SQL, erase_mail_content_between_removed_addresses};
pub use request_data::{await_request_data_erasure, erase_request_data, erase_results_of_job};
