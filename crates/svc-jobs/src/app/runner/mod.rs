mod answer;
mod inbox;
mod mail;
mod payload;
mod state;

pub use answer::{hand_over_result, read_result};
pub use inbox::unread;
pub use mail::{SendMailInput, fetch_attachment, read_mail, send_mail, upload_attachment};
pub use payload::{Uploader, fetch_payload, upload_payload};
pub use state::{read_saved_state, save_state};

use bc_jobs::domain::ids::RunId;
use bc_jobs::domain::runner::refusal::RunnerRefusal;
use uuid::Uuid;

use super::{Jobs, catch_up};
use crate::db::work::WorkNamed;
use crate::error::ServiceError;

pub type Outcome<T> = Result<Result<T, RunnerRefusal>, ServiceError>;

#[derive(Debug, Clone)]
pub struct WorkIds {
    pub job_id: Uuid,
    pub run_id: Uuid,
    pub instance_key: String,
}

impl WorkIds {
    pub fn named(&self) -> WorkNamed<'_> {
        WorkNamed {
            job_id: self.job_id,
            run_id: self.run_id,
            instance_key: &self.instance_key,
        }
    }
}

pub async fn started(jobs: &Jobs, work: &WorkIds) -> Result<(), ServiceError> {
    match RunId::new(work.run_id) {
        Ok(run_id) => catch_up::apply_acknowledged_start(jobs, run_id).await,
        Err(_) => Ok(()),
    }
}
