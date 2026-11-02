use serde::{Deserialize, Serialize};

use crate::domain::ids::{JobId, MailId, ResultId};
use crate::error::JobsError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum NoticeType {
    Accepted,
    QueuePosition,
    Started,
    RunCompleted,
    JobFailed,
    JobCancelled,
    Refused,
}

impl NoticeType {
    pub const WAKING: [Self; 3] = [Self::JobFailed, Self::JobCancelled, Self::Refused];

    pub fn as_wire_str(self) -> &'static str {
        match self {
            Self::Accepted => "ACCEPTED",
            Self::QueuePosition => "QUEUE_POSITION",
            Self::Started => "STARTED",
            Self::RunCompleted => "RUN_COMPLETED",
            Self::JobFailed => "JOB_FAILED",
            Self::JobCancelled => "JOB_CANCELLED",
            Self::Refused => "REFUSED",
        }
    }

    pub fn starts_a_restart(self) -> bool {
        Self::WAKING.contains(&self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoticeSubject {
    Job(JobId),
    Mail(MailId),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(try_from = "StoredNotice", into = "StoredNotice")]
pub struct Notice {
    pub notice_type: NoticeType,
    pub subject: NoticeSubject,
    pub position: Option<u32>,
    pub failure_report: Option<serde_json::Value>,
    pub message: Option<String>,
    pub answer: Option<serde_json::Value>,
    pub result_id: Option<ResultId>,
}

#[derive(Serialize, Deserialize)]
struct StoredNotice {
    #[serde(rename = "type")]
    notice_type: NoticeType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    job_id: Option<JobId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    mail_id: Option<MailId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    position: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    failure_report: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    answer: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    result_id: Option<ResultId>,
}

impl TryFrom<StoredNotice> for Notice {
    type Error = JobsError;

    fn try_from(stored: StoredNotice) -> Result<Self, JobsError> {
        let subject = match (stored.job_id, stored.mail_id) {
            (Some(job_id), None) => NoticeSubject::Job(job_id),
            (None, Some(mail_id)) => NoticeSubject::Mail(mail_id),
            _ => {
                return Err(JobsError::CorruptState {
                    reason_code: "notice_subject",
                });
            }
        };
        Ok(Self {
            notice_type: stored.notice_type,
            subject,
            position: stored.position,
            failure_report: stored.failure_report,
            message: stored.message,
            answer: stored.answer,
            result_id: stored.result_id,
        })
    }
}

impl From<Notice> for StoredNotice {
    fn from(notice: Notice) -> Self {
        let (job_id, mail_id) = match notice.subject {
            NoticeSubject::Job(job_id) => (Some(job_id), None),
            NoticeSubject::Mail(mail_id) => (None, Some(mail_id)),
        };
        Self {
            notice_type: notice.notice_type,
            job_id,
            mail_id,
            position: notice.position,
            failure_report: notice.failure_report,
            message: notice.message,
            answer: notice.answer,
            result_id: notice.result_id,
        }
    }
}

impl Notice {
    pub fn of_job(notice_type: NoticeType, job_id: JobId) -> Self {
        Self::about(notice_type, NoticeSubject::Job(job_id))
    }

    pub fn of_mail(notice_type: NoticeType, mail_id: MailId) -> Self {
        Self::about(notice_type, NoticeSubject::Mail(mail_id))
    }

    fn about(notice_type: NoticeType, subject: NoticeSubject) -> Self {
        Self {
            notice_type,
            subject,
            position: None,
            failure_report: None,
            message: None,
            answer: None,
            result_id: None,
        }
    }

    pub fn job_id(&self) -> Option<JobId> {
        match self.subject {
            NoticeSubject::Job(job_id) => Some(job_id),
            NoticeSubject::Mail(_) => None,
        }
    }

    pub fn mail_id(&self) -> Option<MailId> {
        match self.subject {
            NoticeSubject::Mail(mail_id) => Some(mail_id),
            NoticeSubject::Job(_) => None,
        }
    }

    pub fn starts_a_restart(&self) -> bool {
        self.notice_type.starts_a_restart()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::job_id;
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn a_notice_round_trips_through_the_object_the_inbox_stores() {
        let notice = Notice {
            position: Some(3),
            ..Notice::of_job(NoticeType::QueuePosition, job_id())
        };
        let stored = serde_json::to_value(&notice).unwrap();
        assert_eq!(stored["type"], "QUEUE_POSITION");
        assert_eq!(serde_json::from_value::<Notice>(stored).unwrap(), notice);
    }

    #[test]
    fn a_cancel_notice_is_stored_with_its_words() {
        let notice = Notice {
            message: Some("Job was cancelled.".to_owned()),
            ..Notice::of_job(NoticeType::JobCancelled, job_id())
        };
        let stored = serde_json::to_value(&notice).unwrap();
        assert_eq!(stored["type"], "JOB_CANCELLED");
        assert_eq!(serde_json::from_value::<Notice>(stored).unwrap(), notice);
    }

    #[test]
    fn a_refusal_notice_names_the_job_and_a_mail_notice_names_the_mail() {
        let refusal = Notice {
            message: Some("The call was refused.".to_owned()),
            ..Notice::of_job(NoticeType::Refused, job_id())
        };
        let stored = serde_json::to_value(&refusal).unwrap();
        assert_eq!(stored["type"], "REFUSED");
        assert!(stored.get("mail_id").is_none());
        assert_eq!(serde_json::from_value::<Notice>(stored).unwrap(), refusal);

        let mail = MailId::new(Uuid::now_v7());
        let mail_notice = Notice {
            message: Some("The mail was refused.".to_owned()),
            ..Notice::of_mail(NoticeType::Refused, mail)
        };
        let stored = serde_json::to_value(&mail_notice).unwrap();
        assert!(stored.get("job_id").is_none());
        assert_eq!(stored["mail_id"], json!(mail.as_uuid().to_string()));
        assert_eq!(
            serde_json::from_value::<Notice>(stored).unwrap(),
            mail_notice
        );
    }

    #[test]
    fn a_notice_stored_before_the_mail_notice_existed_still_reads() {
        let job = job_id();
        let stored = json!({"type": "JOB_FAILED", "job_id": job.as_uuid().to_string()});
        let read: Notice = serde_json::from_value(stored).unwrap();
        assert_eq!(read.job_id(), Some(job));
        assert_eq!(read.mail_id(), None);
    }

    #[test]
    fn a_stored_notice_that_names_both_a_job_and_a_mail_or_neither_fails_to_load() {
        let job = job_id().as_uuid().to_string();
        let mail = Uuid::now_v7().to_string();
        for stored in [
            json!({"type": "REFUSED", "job_id": job, "mail_id": mail}),
            json!({"type": "REFUSED"}),
        ] {
            assert!(serde_json::from_value::<Notice>(stored).is_err());
        }
    }

    #[test]
    fn the_wire_word_of_a_notice_type_is_the_word_the_inbox_stores() {
        for notice_type in [
            NoticeType::Accepted,
            NoticeType::QueuePosition,
            NoticeType::Started,
            NoticeType::RunCompleted,
            NoticeType::JobFailed,
            NoticeType::JobCancelled,
            NoticeType::Refused,
        ] {
            assert_eq!(
                serde_json::to_value(notice_type).unwrap(),
                json!(notice_type.as_wire_str())
            );
        }
    }

    #[test]
    fn a_failure_a_cancel_and_a_refusal_start_a_restart_and_nothing_else_does() {
        let waking: Vec<&str> = [
            NoticeType::Accepted,
            NoticeType::QueuePosition,
            NoticeType::Started,
            NoticeType::RunCompleted,
            NoticeType::JobFailed,
            NoticeType::JobCancelled,
            NoticeType::Refused,
        ]
        .into_iter()
        .filter(|notice_type| notice_type.starts_a_restart())
        .map(NoticeType::as_wire_str)
        .collect();
        assert_eq!(waking, ["JOB_FAILED", "JOB_CANCELLED", "REFUSED"]);
    }
}
