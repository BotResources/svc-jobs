use serde::{Deserialize, Serialize};

use crate::domain::ids::{JobId, MailId, ResultId};
use crate::error::JobsError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ItemKind {
    Mail,
    CcNotice,
    Notice,
    MailNotice,
}

impl ItemKind {
    pub const WAKING: [Self; 1] = [Self::MailNotice];

    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Mail => "MAIL",
            Self::CcNotice => "CC_NOTICE",
            Self::Notice => "NOTICE",
            Self::MailNotice => "MAIL_NOTICE",
        }
    }

    pub fn from_db_str(value: &str) -> Result<Self, JobsError> {
        match value {
            "MAIL" => Ok(Self::Mail),
            "CC_NOTICE" => Ok(Self::CcNotice),
            "NOTICE" => Ok(Self::Notice),
            "MAIL_NOTICE" => Ok(Self::MailNotice),
            other => Err(JobsError::UnknownEnumValue {
                field: "item_kind",
                value: other.to_owned(),
            }),
        }
    }
}

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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Notice {
    #[serde(rename = "type")]
    pub notice_type: NoticeType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<JobId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mail_id: Option<MailId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub failure_report: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub answer: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_id: Option<ResultId>,
}

impl Notice {
    pub fn of_job(notice_type: NoticeType, job_id: JobId) -> Self {
        Self::new(notice_type, Some(job_id))
    }

    pub fn new(notice_type: NoticeType, job_id: Option<JobId>) -> Self {
        Self {
            notice_type,
            job_id,
            mail_id: None,
            position: None,
            failure_report: None,
            message: None,
            answer: None,
            result_id: None,
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
            notice_type: NoticeType::Refused,
            job_id: None,
            mail_id: Some(mail),
            message: Some("The mail was refused.".to_owned()),
            ..Notice::of_job(NoticeType::Refused, job_id())
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
        assert_eq!(read.job_id, Some(job));
        assert_eq!(read.mail_id, None);
    }

    #[test]
    fn an_item_kind_maps_totally_to_the_database() {
        for kind in [
            ItemKind::Mail,
            ItemKind::CcNotice,
            ItemKind::Notice,
            ItemKind::MailNotice,
        ] {
            assert_eq!(ItemKind::from_db_str(kind.as_db_str()), Ok(kind));
        }
        assert_eq!(ItemKind::MailNotice.as_db_str(), "MAIL_NOTICE");
        assert!(ItemKind::from_db_str("OTHER").is_err());
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
    fn a_failure_a_cancel_a_refusal_and_a_mail_notice_start_a_restart_and_nothing_else_does() {
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
        assert_eq!(ItemKind::WAKING, [ItemKind::MailNotice]);
    }
}
