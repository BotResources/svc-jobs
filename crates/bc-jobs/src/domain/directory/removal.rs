use std::collections::BTreeSet;

use crate::domain::ids::{AgentId, RunId};
use crate::domain::payload::CallMode;
use crate::domain::runner::call_notices::CallNotices;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DirectoryRemoval {
    runs: BTreeSet<RunId>,
    acted_for_runs: BTreeSet<RunId>,
    addresses: BTreeSet<AgentId>,
}

impl DirectoryRemoval {
    pub fn new(
        runs: impl IntoIterator<Item = RunId>,
        acted_for_runs: impl IntoIterator<Item = RunId>,
        addresses: impl IntoIterator<Item = AgentId>,
    ) -> Self {
        Self {
            runs: runs.into_iter().collect(),
            acted_for_runs: acted_for_runs.into_iter().collect(),
            addresses: addresses.into_iter().collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.runs.is_empty() && self.acted_for_runs.is_empty() && self.addresses.is_empty()
    }

    pub fn removes_the_caller_of(&self, call: &CallNotices) -> bool {
        self.runs.contains(&call.caller_run)
            || self.acted_for_runs.contains(&call.caller_run)
            || (call.mode == CallMode::Background
                && call
                    .address
                    .is_some_and(|address| self.addresses.contains(&address)))
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::*;

    fn run() -> RunId {
        RunId::new(Uuid::now_v7()).unwrap()
    }

    fn address() -> AgentId {
        AgentId::new(Uuid::now_v7())
    }

    fn call(mode: CallMode, caller_run: RunId, address: Option<AgentId>) -> CallNotices {
        CallNotices {
            caller_run,
            mode,
            address,
        }
    }

    #[test]
    fn a_cancel_that_removes_nothing_keeps_every_caller() {
        let removal = DirectoryRemoval::default();
        assert!(removal.is_empty());
        for mode in [CallMode::Sync, CallMode::Background] {
            assert!(!removal.removes_the_caller_of(&call(mode, run(), Some(address()))));
        }
    }

    #[test]
    fn a_caller_whose_run_is_removed_gets_no_notice_in_either_mode() {
        let (caller, other) = (run(), run());
        let removal = DirectoryRemoval::new([caller], [], []);
        assert!(removal.removes_the_caller_of(&call(CallMode::Sync, caller, None)));
        assert!(removal.removes_the_caller_of(&call(
            CallMode::Background,
            caller,
            Some(address())
        )));
        assert!(!removal.removes_the_caller_of(&call(CallMode::Sync, other, None)));
    }

    #[test]
    fn a_caller_whose_restart_run_is_removed_gets_no_notice_in_either_mode() {
        let origin = run();
        let removal = DirectoryRemoval::new([run()], [origin], []);
        assert!(removal.removes_the_caller_of(&call(CallMode::Sync, origin, None)));
        assert!(removal.removes_the_caller_of(&call(CallMode::Background, origin, None)));
    }

    #[test]
    fn a_background_caller_whose_address_is_removed_gets_no_notice() {
        let (kept, gone) = (run(), address());
        let removal = DirectoryRemoval::new([], [], [gone]);
        assert!(removal.removes_the_caller_of(&call(CallMode::Background, kept, Some(gone))));
        assert!(!removal.removes_the_caller_of(&call(CallMode::Background, kept, Some(address()))));
        assert!(!removal.removes_the_caller_of(&call(CallMode::Background, kept, None)));
    }

    #[test]
    fn a_sync_caller_is_not_judged_by_an_address_it_does_not_write_to() {
        let (kept, gone) = (run(), address());
        let removal = DirectoryRemoval::new([], [], [gone]);
        assert!(!removal.removes_the_caller_of(&call(CallMode::Sync, kept, Some(gone))));
    }
}
