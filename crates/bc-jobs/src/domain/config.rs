use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::JobsError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Value", into = "Value")]
pub struct RunnerConfig(Value);

impl RunnerConfig {
    pub fn new(value: Value) -> Result<Self, JobsError> {
        if value.is_null() {
            Err(JobsError::BlankValue { field: "config" })
        } else {
            Ok(Self(value))
        }
    }

    pub fn as_value(&self) -> &Value {
        &self.0
    }
}

impl From<RunnerConfig> for Value {
    fn from(value: RunnerConfig) -> Self {
        value.0
    }
}

impl TryFrom<Value> for RunnerConfig {
    type Error = JobsError;

    fn try_from(value: Value) -> Result<Self, JobsError> {
        Self::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn runner_configuration_is_forwarded_unchanged() {
        // Given: a configuration whose schema belongs to the runner, not to jobs
        let declared = json!({ "prompt": "summarise", "temperature": 0.2, "tools": ["web"] });
        // When: this context takes it in
        let config = RunnerConfig::new(declared.clone()).unwrap();
        // Then: nothing is validated, reshaped or dropped
        assert_eq!(config.as_value(), &declared);
    }

    #[test]
    fn an_absent_configuration_is_expressed_as_absence_never_as_null() {
        // Given: a JSON null offered as a configuration
        // When/Then: it is refused — the optional field carries absence
        assert_eq!(
            RunnerConfig::new(Value::Null),
            Err(JobsError::BlankValue { field: "config" })
        );
    }
}
