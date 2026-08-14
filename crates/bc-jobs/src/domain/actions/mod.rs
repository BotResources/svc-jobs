pub mod fleet;
pub mod job;

use serde_json::Value;

use crate::error::JobsError;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Availability {
    Available,
    Blocked { reason_code: String, params: Value },
}

impl Availability {
    pub fn blocked(reason_code: &str) -> Self {
        Self::Blocked {
            reason_code: reason_code.to_owned(),
            params: Value::Object(serde_json::Map::new()),
        }
    }

    pub fn from_guard(verdict: Result<(), JobsError>) -> Self {
        match verdict {
            Ok(()) => Self::Available,
            Err(refusal) => Self::Blocked {
                reason_code: refusal.code(),
                params: refusal.params(),
            },
        }
    }

    pub fn is_available(&self) -> bool {
        matches!(self, Self::Available)
    }

    pub fn reason_code(&self) -> Option<&str> {
        match self {
            Self::Available => None,
            Self::Blocked { reason_code, .. } => Some(reason_code),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Affordance {
    action: &'static str,
    allowed: bool,
    reason_code: Option<String>,
    params: Option<Value>,
}

impl Affordance {
    pub fn new(action: &'static str, availability: Availability) -> Self {
        match availability {
            Availability::Available => Self {
                action,
                allowed: true,
                reason_code: None,
                params: None,
            },
            Availability::Blocked {
                reason_code,
                params,
            } => Self {
                action,
                allowed: false,
                reason_code: Some(reason_code),
                params: Some(params),
            },
        }
    }

    pub fn action(&self) -> &'static str {
        self.action
    }

    pub fn allowed(&self) -> bool {
        self.allowed
    }

    pub fn reason_code(&self) -> Option<&str> {
        self.reason_code.as_deref()
    }

    pub fn params(&self) -> Option<&Value> {
        self.params.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    #[test]
    fn a_blocked_affordance_reuses_the_refusal_the_command_would_return() {
        // Given: the very refusal the command guard produces
        let successor_job_id = Uuid::from_u128(7);
        let refusal = JobsError::SuccessorStillActive { successor_job_id };
        // When: it is projected as an affordance
        let affordance = Affordance::new("delete", Availability::from_guard(Err(refusal)));
        // Then: the client receives the same code and params, never a sentence
        assert!(!affordance.allowed());
        assert_eq!(affordance.reason_code(), Some("successor_still_active"));
        assert_eq!(
            affordance.params(),
            Some(&json!({ "successorJobId": successor_job_id }))
        );
    }

    #[test]
    fn an_available_affordance_carries_no_reason() {
        // Given: a guard that admits the action
        let affordance = Affordance::new("cancel", Availability::from_guard(Ok(())));
        // When/Then: nothing has to be explained
        assert!(affordance.allowed());
        assert_eq!(affordance.reason_code(), None);
        assert_eq!(affordance.params(), None);
    }
}
