use std::collections::BTreeSet;

use crate::domain::ids::{AgentId, RunId};
use crate::domain::payload::CallMode;
use crate::domain::runner::call_notices::CallNotices;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DirectoryRemoval {
    runs: BTreeSet<RunId>,
    emptied_origin_runs: BTreeSet<RunId>,
    addresses: BTreeSet<AgentId>,
}

impl DirectoryRemoval {
    pub fn new(
        runs: impl IntoIterator<Item = RunId>,
        emptied_origin_runs: impl IntoIterator<Item = RunId>,
        addresses: impl IntoIterator<Item = AgentId>,
    ) -> Self {
        Self {
            runs: runs.into_iter().collect(),
            emptied_origin_runs: emptied_origin_runs.into_iter().collect(),
            addresses: addresses.into_iter().collect(),
        }
    }

    pub fn runs(&self) -> impl Iterator<Item = RunId> + '_ {
        self.runs.iter().copied()
    }

    pub fn removes_the_caller_of(&self, call: &CallNotices) -> bool {
        if self.runs.contains(&call.caller_run) {
            return true;
        }
        match call.mode {
            CallMode::Sync => self.emptied_origin_runs.contains(&call.caller_run),
            CallMode::Background => call
                .address
                .is_some_and(|address| self.addresses.contains(&address)),
        }
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
        assert_eq!(removal.runs().count(), 0);
        for mode in [CallMode::Sync, CallMode::Background] {
            assert!(!removal.removes_the_caller_of(&call(mode, run(), Some(address()))));
        }
    }

    #[test]
    fn the_runs_a_removal_names_are_the_runs_it_was_built_with() {
        let (first, second) = (run(), run());
        let removal = DirectoryRemoval::new([second, first], [run()], [address()]);
        let mut expected = vec![first, second];
        expected.sort();
        assert_eq!(removal.runs().collect::<Vec<_>>(), expected);
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
    fn a_sync_caller_whose_origin_run_is_left_without_agent_gets_no_notice() {
        let origin = run();
        let removal = DirectoryRemoval::new([run()], [origin], []);
        assert!(removal.removes_the_caller_of(&call(CallMode::Sync, origin, None)));
        assert!(!removal.removes_the_caller_of(&call(CallMode::Sync, run(), None)));
    }

    #[test]
    fn a_background_caller_whose_agent_stays_keeps_its_notice_although_its_origin_run_is_left_without_the_restarted_agent()
     {
        let (origin, stays) = (run(), address());
        let removal = DirectoryRemoval::new([run()], [origin], [address()]);
        assert!(!removal.removes_the_caller_of(&call(CallMode::Background, origin, Some(stays))));
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
