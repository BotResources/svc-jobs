use chrono::{DateTime, Utc};

use super::Retention;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TreeFacts {
    pub last_activity: DateTime<Utc>,
    pub unresolved_jobs: u64,
    pub live_runs: u64,
    pub scrubbed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScrubStanding {
    Scrubbed,
    WorkRemains,
    Waiting,
    Due,
}

impl ScrubStanding {
    pub fn is_due(self) -> bool {
        self == Self::Due
    }
}

impl Retention {
    pub fn scrub_due_before(&self, now: DateTime<Utc>) -> DateTime<Utc> {
        now - self.tree()
    }

    pub fn scrub_standing(&self, tree: &TreeFacts, now: DateTime<Utc>) -> ScrubStanding {
        if tree.scrubbed {
            ScrubStanding::Scrubbed
        } else if tree.unresolved_jobs > 0 || tree.live_runs > 0 {
            ScrubStanding::WorkRemains
        } else if tree.last_activity > self.scrub_due_before(now) {
            ScrubStanding::Waiting
        } else {
            ScrubStanding::Due
        }
    }
}
