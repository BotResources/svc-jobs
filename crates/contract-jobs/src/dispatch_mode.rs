use serde::{Deserialize, Serialize};

use crate::runner::WIRE_VERSION;

pub const DISPATCH_MODE_PREFIX: &str = "jobs.dispatch_mode.";

pub const DISPATCH_MODE_PUSH: &str = "PUSH";
pub const DISPATCH_MODE_CLAIM: &str = "CLAIM";

fn wire_version() -> u8 {
    WIRE_VERSION
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum DispatchMode {
    Push,
    Claim,
}

impl DispatchMode {
    pub const ALL: [Self; 2] = [Self::Push, Self::Claim];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Push => DISPATCH_MODE_PUSH,
            Self::Claim => DISPATCH_MODE_CLAIM,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunnerTypeDispatchMode {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub runner_type: String,
    pub dispatch_mode: DispatchMode,
}

pub fn dispatch_mode_key(runner_type: &str) -> String {
    format!("{DISPATCH_MODE_PREFIX}{runner_type}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_key_is_the_declared_dot_separated_offer() {
        assert_eq!(dispatch_mode_key("analyst"), "jobs.dispatch_mode.analyst");
    }

    #[test]
    fn the_entry_names_its_version_its_type_and_its_mode() {
        let entry = RunnerTypeDispatchMode {
            version: WIRE_VERSION,
            runner_type: "analyst".to_owned(),
            dispatch_mode: DispatchMode::Claim,
        };
        assert_eq!(
            serde_json::to_value(entry).unwrap(),
            serde_json::json!({ "version": 1, "runner_type": "analyst", "dispatch_mode": "CLAIM" })
        );
    }

    #[test]
    fn every_mode_is_written_as_its_published_code_and_nothing_else_parses() {
        for mode in DispatchMode::ALL {
            let written = serde_json::to_value(mode).unwrap();
            assert_eq!(written, serde_json::json!(mode.as_str()));
            assert_eq!(
                serde_json::from_value::<DispatchMode>(written).unwrap(),
                mode
            );
        }
        assert!(serde_json::from_value::<DispatchMode>(serde_json::json!("POLL")).is_err());
    }
}
