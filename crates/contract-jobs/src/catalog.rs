use serde::{Deserialize, Serialize};

pub const RUNNER_TYPE_PREFIX: &str = "jobs.runner_type.";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RunnerTypeLifecycle {
    Active,
    Deprecated,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublishedRunnerType {
    pub runner_type: String,
    pub lifecycle: RunnerTypeLifecycle,
}

pub fn runner_type_key(runner_type: &str) -> String {
    format!("{RUNNER_TYPE_PREFIX}{runner_type}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_catalog_key_uses_the_declared_dot_separated_offer() {
        assert_eq!(runner_type_key("analyst"), "jobs.runner_type.analyst");
    }

    #[test]
    fn the_published_entry_carries_the_current_non_retired_lifecycle() {
        let entry = PublishedRunnerType {
            runner_type: "analyst".to_owned(),
            lifecycle: RunnerTypeLifecycle::Deprecated,
        };
        assert_eq!(
            serde_json::to_value(entry).unwrap(),
            serde_json::json!({
                "runner_type": "analyst",
                "lifecycle": "DEPRECATED"
            })
        );
    }
}
