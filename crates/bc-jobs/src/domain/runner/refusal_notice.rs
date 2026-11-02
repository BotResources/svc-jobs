use uuid::Uuid;

use super::call_notices::CallNotices;
use crate::domain::directory::caller_address::guard_caller_address;
use crate::domain::ids::{AgentId, JobId, RunId};
use crate::domain::mail::inbox::{Notice, NoticeType};
use crate::domain::payload::CallMode;
use crate::error::JobsError;

#[derive(Debug, Clone, PartialEq)]
pub struct RefusalNotice {
    pub caller_run: RunId,
    pub address: AgentId,
    pub notice: Notice,
}

impl CallNotices {
    pub fn refusal_notice(&self, job_id: Uuid, words: String) -> Option<RefusalNotice> {
        let address = self.refusal_address()?;
        let job_id = JobId::new(job_id).ok()?;
        Some(RefusalNotice {
            caller_run: self.caller_run,
            address,
            notice: Notice {
                message: Some(words),
                ..Notice::of_job(NoticeType::Refused, job_id)
            },
        })
    }
}

impl RefusalNotice {
    pub fn guard_address_held(&self, holder: Option<RunId>) -> Result<(), JobsError> {
        guard_caller_address(
            CallMode::Background,
            Some(self.address.as_uuid()),
            self.caller_run,
            holder,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::run_id;

    fn call(mode: CallMode, address: Option<AgentId>) -> CallNotices {
        CallNotices {
            caller_run: run_id(),
            mode,
            address,
        }
    }

    #[test]
    fn a_background_call_with_an_address_is_told_which_job_was_refused_and_why() {
        let address = AgentId::new(Uuid::now_v7());
        let job = Uuid::now_v7();
        let told = call(CallMode::Background, Some(address))
            .refusal_notice(job, "no".to_owned())
            .expect("a notice");
        assert_eq!(told.address, address);
        assert_eq!(told.notice.notice_type, NoticeType::Refused);
        assert_eq!(told.notice.job_id, JobId::new(job).ok());
        assert_eq!(told.notice.message.as_deref(), Some("no"));
    }

    #[test]
    fn a_sync_call_or_a_call_with_no_address_or_an_id_that_names_no_job_is_told_nothing() {
        let address = Some(AgentId::new(Uuid::now_v7()));
        let words = || "no".to_owned();
        assert!(
            call(CallMode::Sync, address)
                .refusal_notice(Uuid::now_v7(), words())
                .is_none()
        );
        assert!(
            call(CallMode::Background, None)
                .refusal_notice(Uuid::now_v7(), words())
                .is_none()
        );
        assert!(
            call(CallMode::Background, address)
                .refusal_notice(Uuid::nil(), words())
                .is_none()
        );
    }

    #[test]
    fn the_notice_is_left_only_at_an_address_held_by_the_run_of_the_caller() {
        let told = call(CallMode::Background, Some(AgentId::new(Uuid::now_v7())))
            .refusal_notice(Uuid::now_v7(), "no".to_owned())
            .unwrap();
        assert_eq!(told.guard_address_held(Some(told.caller_run)), Ok(()));
        assert_eq!(
            told.guard_address_held(Some(run_id())),
            Err(JobsError::CallerAddressNotOfRun)
        );
        assert_eq!(
            told.guard_address_held(None),
            Err(JobsError::CallerAddressNotHeld)
        );
    }
}
