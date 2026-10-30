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

pub fn guard_state_within(size: usize, maximum: usize) -> Result<(), SavedStateRefusal> {
    if size > maximum {
        Err(SavedStateRefusal::StateTooLarge)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_state_up_to_the_setting_is_kept_and_a_larger_one_is_refused() {
        assert_eq!(guard_state_within(10, 10), Ok(()));
        assert_eq!(
            guard_state_within(11, 10),
            Err(SavedStateRefusal::StateTooLarge)
        );
    }
}
