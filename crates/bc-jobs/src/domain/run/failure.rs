use chrono::TimeDelta;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::domain::keys::ReasonCode;
use crate::domain::wire::db_string_serde;
use crate::error::JobsError;

pub const INSTANCE_LOST: &str = "instance_lost";
pub const RUN_TIMEOUT: &str = "run_timeout";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub enum RunFailureKind {
    Transient,
    Permanent,
}

impl RunFailureKind {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::Transient => "TRANSIENT",
            Self::Permanent => "PERMANENT",
        }
    }

    pub fn from_db_str(value: &str) -> Result<Self, JobsError> {
        match value {
            "TRANSIENT" => Ok(Self::Transient),
            "PERMANENT" => Ok(Self::Permanent),
            other => Err(JobsError::UnknownEnumValue {
                field: "run_failure_kind",
                value: other.to_owned(),
            }),
        }
    }

    pub fn from_declared(value: Option<&str>) -> Result<Self, JobsError> {
        match value {
            None => Ok(Self::Permanent),
            Some(declared) => Self::from_db_str(declared),
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Transient)
    }
}

fn guard_object(field: &'static str, value: &Value) -> Result<(), JobsError> {
    if value.is_object() {
        Ok(())
    } else {
        Err(JobsError::NotAJsonObject { field })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunFailureReport {
    kind: RunFailureKind,
    reason_code: ReasonCode,
    params: Value,
    diagnostic: Value,
    retry_after_seconds: Option<i64>,
}

impl RunFailureReport {
    pub fn new(
        kind: RunFailureKind,
        reason_code: ReasonCode,
        params: Value,
        diagnostic: Value,
        retry_after: Option<TimeDelta>,
    ) -> Result<Self, JobsError> {
        guard_object("params", &params)?;
        guard_object("diagnostic", &diagnostic)?;
        let retry_after_seconds = match retry_after {
            None => None,
            Some(hint) if hint >= TimeDelta::zero() => Some(hint.num_seconds()),
            Some(_) => {
                return Err(JobsError::InvalidDuration {
                    field: "retry_after",
                });
            }
        };
        Ok(Self {
            kind,
            reason_code,
            params,
            diagnostic,
            retry_after_seconds,
        })
    }

    pub fn platform(kind: RunFailureKind, reason_code: &str) -> Result<Self, JobsError> {
        Self::new(
            kind,
            ReasonCode::new(reason_code)?,
            Value::Object(serde_json::Map::new()),
            Value::Object(serde_json::Map::new()),
            None,
        )
    }

    pub fn kind(&self) -> RunFailureKind {
        self.kind
    }

    pub fn reason_code(&self) -> &ReasonCode {
        &self.reason_code
    }

    pub fn params(&self) -> &Value {
        &self.params
    }

    pub fn diagnostic(&self) -> &Value {
        &self.diagnostic
    }

    pub fn retry_after(&self) -> Option<TimeDelta> {
        self.retry_after_seconds.map(TimeDelta::seconds)
    }
}

db_string_serde!(RunFailureKind);

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn an_unspecified_failure_kind_is_permanent() {
        // Given: a runner that reported a failure without naming its kind
        // When: the kind is read
        // Then: it is permanent — an unclassified failure never burns retry budget
        assert_eq!(
            RunFailureKind::from_declared(None).unwrap(),
            RunFailureKind::Permanent
        );
    }

    #[test]
    fn only_a_transient_failure_is_retryable() {
        // Given: the two failure kinds
        // When/Then: retry policy applies to the transient one alone
        assert!(RunFailureKind::Transient.is_retryable());
        assert!(!RunFailureKind::Permanent.is_retryable());
    }

    #[test]
    fn a_report_carries_its_code_params_and_diagnostic_untouched() {
        // Given: a structured failure report from a runner
        let report = RunFailureReport::new(
            RunFailureKind::Transient,
            ReasonCode::new("provider_rate_limited").unwrap(),
            json!({ "provider": "acme" }),
            json!({ "http_status": 429 }),
            Some(TimeDelta::seconds(90)),
        )
        .unwrap();
        // When/Then: every part survives for escalation to the owner as-is
        assert_eq!(report.reason_code().as_str(), "provider_rate_limited");
        assert_eq!(report.params(), &json!({ "provider": "acme" }));
        assert_eq!(report.diagnostic(), &json!({ "http_status": 429 }));
        assert_eq!(report.retry_after(), Some(TimeDelta::seconds(90)));
    }

    #[test]
    fn a_report_whose_params_are_not_an_object_is_refused() {
        // Given: a runner reporting `"params": null`, which the served contract declares non-null
        let result = RunFailureReport::new(
            RunFailureKind::Transient,
            ReasonCode::new("provider_rate_limited").unwrap(),
            Value::Null,
            json!({}),
            None,
        );
        // Then: it never reaches storage, so no read can break on it later
        assert_eq!(result, Err(JobsError::NotAJsonObject { field: "params" }));
    }

    #[test]
    fn a_report_whose_diagnostic_is_a_bare_value_is_refused() {
        // Given: a diagnostic sent as a string rather than a structured payload
        let result = RunFailureReport::new(
            RunFailureKind::Permanent,
            ReasonCode::new("provider_unavailable").unwrap(),
            json!({}),
            json!("boom"),
            None,
        );
        // Then: the same refusal applies to both structured halves of the report
        assert_eq!(
            result,
            Err(JobsError::NotAJsonObject {
                field: "diagnostic"
            })
        );
    }

    #[test]
    fn a_negative_retry_hint_is_refused() {
        // Given: a runner hint that would pull the next attempt into the past
        let result = RunFailureReport::new(
            RunFailureKind::Transient,
            ReasonCode::new("provider_rate_limited").unwrap(),
            json!({}),
            json!({}),
            Some(TimeDelta::seconds(-1)),
        );
        // Then: the report is refused at construction
        assert_eq!(
            result,
            Err(JobsError::InvalidDuration {
                field: "retry_after"
            })
        );
    }
}
