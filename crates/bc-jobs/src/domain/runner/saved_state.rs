#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SavedStateRefusal {
    StateTooLarge,
    NotARestartRun,
}

impl SavedStateRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::StateTooLarge => "state_too_large",
            Self::NotARestartRun => "not_a_restart_run",
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            Self::StateTooLarge => "The saved state is larger than the service allows.",
            Self::NotARestartRun => "The Job of the Run does not restart another Run.",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SavedState(serde_json::Value);

impl SavedState {
    pub fn new(value: serde_json::Value, maximum_bytes: usize) -> Result<Self, SavedStateRefusal> {
        let size = serde_json::to_vec(&value).map_or(usize::MAX, |bytes| bytes.len());
        if size > maximum_bytes {
            Err(SavedStateRefusal::StateTooLarge)
        } else {
            Ok(Self(value))
        }
    }

    pub fn as_value(&self) -> &serde_json::Value {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_state_up_to_the_setting_is_kept_and_a_larger_one_is_refused() {
        let state = serde_json::json!("12345678");
        assert!(SavedState::new(state.clone(), 10).is_ok());
        assert_eq!(
            SavedState::new(state, 9),
            Err(SavedStateRefusal::StateTooLarge)
        );
    }
}
