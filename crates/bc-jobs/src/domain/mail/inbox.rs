use serde::{Deserialize, Serialize};

use crate::domain::ids::{JobId, ResultId};
use crate::error::JobsError;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ItemKind {
    Mail,
    CcNotice,
    Notice,
}

impl ItemKind {
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Mail => "MAIL",
            Self::CcNotice => "CC_NOTICE",
            Self::Notice => "NOTICE",
        }
    }

    pub fn from_db_str(value: &str) -> Result<Self, JobsError> {
        match value {
            "MAIL" => Ok(Self::Mail),
            "CC_NOTICE" => Ok(Self::CcNotice),
            "NOTICE" => Ok(Self::Notice),
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
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Notice {
    #[serde(rename = "type")]
    pub notice_type: NoticeType,
    pub job_id: JobId,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::job_id;

    #[test]
    fn a_notice_round_trips_through_the_object_the_inbox_stores() {
        let notice = Notice {
            notice_type: NoticeType::QueuePosition,
            job_id: job_id(),
            position: Some(3),
            failure_report: None,
            message: None,
            answer: None,
            result_id: None,
        };
        let stored = serde_json::to_value(&notice).unwrap();
        assert_eq!(stored["type"], "QUEUE_POSITION");
        assert_eq!(serde_json::from_value::<Notice>(stored).unwrap(), notice);
    }

    #[test]
    fn an_item_kind_maps_totally_to_the_database() {
        for kind in [ItemKind::Mail, ItemKind::CcNotice, ItemKind::Notice] {
            assert_eq!(ItemKind::from_db_str(kind.as_db_str()), Ok(kind));
        }
        assert!(ItemKind::from_db_str("OTHER").is_err());
    }
}
