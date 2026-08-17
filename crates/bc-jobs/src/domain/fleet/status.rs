use serde::{Deserialize, Serialize};

use crate::error::JobsError;

pub const READY: &str = "READY";
pub const DRAINING: &str = "DRAINING";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ReportedStatus {
    Ready,
    Draining,
}

impl ReportedStatus {
    pub fn new(value: impl AsRef<str>) -> Result<Self, JobsError> {
        match value.as_ref() {
            READY => Ok(Self::Ready),
            DRAINING => Ok(Self::Draining),
            unknown => Err(JobsError::UnknownEnumValue {
                field: "reported_status",
                value: unknown.to_owned(),
            }),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => READY,
            Self::Draining => DRAINING,
        }
    }

    pub fn accepts_new_work(self) -> bool {
        matches!(self, Self::Ready)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_status_an_instance_reports_is_a_closed_set_of_two_codes() {
        // Given: the codes a runner may report, and codes it may not
        // When/Then: only the two contract codes are admitted
        assert_eq!(ReportedStatus::new(READY), Ok(ReportedStatus::Ready));
        assert_eq!(ReportedStatus::new(DRAINING), Ok(ReportedStatus::Draining));
        for refused in ["ready", "IDLE", "BUSY", "", "READY "] {
            assert_eq!(
                ReportedStatus::new(refused),
                Err(JobsError::UnknownEnumValue {
                    field: "reported_status",
                    value: refused.to_owned()
                }),
                "'{refused}' is not a status this service can act on, so it may not enter",
            );
        }
    }

    #[test]
    fn a_draining_instance_takes_no_new_work_while_a_ready_one_does() {
        // Given: the two reportable statuses
        // When/Then: draining means "finishing what I hold", never "send me more"
        assert!(ReportedStatus::Ready.accepts_new_work());
        assert!(!ReportedStatus::Draining.accepts_new_work());
    }

    #[test]
    fn the_wire_form_of_a_status_is_its_code() {
        // Given: a status travelling in an event payload
        // When: it is serialised
        let encoded = serde_json::to_string(&ReportedStatus::Draining).unwrap();
        // Then: it is the contract code, and it round-trips
        assert_eq!(encoded, "\"DRAINING\"");
        assert_eq!(
            serde_json::from_str::<ReportedStatus>(&encoded).unwrap(),
            ReportedStatus::Draining
        );
        assert!(serde_json::from_str::<ReportedStatus>("\"BUSY\"").is_err());
    }
}
