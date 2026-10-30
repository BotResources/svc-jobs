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
    pub fn new(value: serde_json::Value) -> Self {
        Self(value)
    }

    pub fn as_value(&self) -> &serde_json::Value {
        &self.0
    }

    pub fn guard_within(&self, maximum: usize) -> Result<(), SavedStateRefusal> {
        let size = serde_json::to_vec(&self.0).map_or(usize::MAX, |bytes| bytes.len());
        if size > maximum {
            Err(SavedStateRefusal::StateTooLarge)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_state_up_to_the_setting_is_kept_and_a_larger_one_is_refused() {
        let state = SavedState::new(serde_json::json!("12345678"));
        assert_eq!(state.guard_within(10), Ok(()));
        assert_eq!(state.guard_within(9), Err(SavedStateRefusal::StateTooLarge));
    }
}
