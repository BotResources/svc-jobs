use crate::domain::directory::{AgentId, AgentState};
use crate::domain::ids::RunId;
use crate::domain::keys::RunnerTypeKey;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectoryEntry {
    pub agent_id: AgentId,
    pub run_id: RunId,
    pub runner_type: RunnerTypeKey,
    pub state: AgentState,
    pub has_inbox: bool,
    pub declares_restart: bool,
    pub holds_saved_state: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RestartObstacles {
    pub runner_type_retired: bool,
    pub parent_job_terminal: bool,
    pub restart_job_live: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartStanding {
    Running,
    NoSavedState,
    ParentJobTerminal,
    RestartInFlight,
    TypeRetired,
    Allowed,
}

impl RestartStanding {
    pub fn is_allowed(self) -> bool {
        self == Self::Allowed
    }
}

impl DirectoryEntry {
    pub fn can_restart(&self) -> bool {
        self.declares_restart && self.holds_saved_state
    }

    pub fn mail_address(&self) -> Option<AgentId> {
        self.has_inbox.then_some(self.agent_id)
    }

    pub fn is_listed(&self) -> bool {
        match self.state {
            AgentState::Running => true,
            AgentState::Finished => self.can_restart(),
        }
    }

    pub fn restart_standing(&self, obstacles: RestartObstacles) -> RestartStanding {
        match self.state {
            AgentState::Running => RestartStanding::Running,
            AgentState::Finished if !self.can_restart() => RestartStanding::NoSavedState,
            AgentState::Finished if obstacles.parent_job_terminal => {
                RestartStanding::ParentJobTerminal
            }
            AgentState::Finished if obstacles.restart_job_live => RestartStanding::RestartInFlight,
            AgentState::Finished if obstacles.runner_type_retired => RestartStanding::TypeRetired,
            AgentState::Finished => RestartStanding::Allowed,
        }
    }

    pub fn restartable_now(&self, obstacles: RestartObstacles) -> Option<bool> {
        match self.restart_standing(obstacles) {
            RestartStanding::Running => None,
            standing => Some(standing.is_allowed()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::run_id;
    use uuid::Uuid;

    fn entry(state: AgentState, declares_restart: bool, holds_saved_state: bool) -> DirectoryEntry {
        DirectoryEntry {
            agent_id: AgentId::new(Uuid::now_v7()),
            run_id: run_id(),
            runner_type: RunnerTypeKey::new("analyst").unwrap(),
            state,
            has_inbox: true,
            declares_restart,
            holds_saved_state,
        }
    }

    const CLEAR: RestartObstacles = RestartObstacles {
        runner_type_retired: false,
        parent_job_terminal: false,
        restart_job_live: false,
    };

    #[test]
    fn a_running_agent_is_listed_and_a_finished_one_only_when_it_can_restart() {
        assert!(entry(AgentState::Running, false, false).is_listed());
        assert!(entry(AgentState::Finished, true, true).is_listed());
        assert!(!entry(AgentState::Finished, true, false).is_listed());
        assert!(!entry(AgentState::Finished, false, true).is_listed());
    }

    #[test]
    fn a_finished_agent_is_restartable_now_unless_its_type_is_retired_or_its_parent_ended() {
        let finished = entry(AgentState::Finished, true, true);
        assert_eq!(finished.restartable_now(CLEAR), Some(true));
        assert_eq!(
            finished.restartable_now(RestartObstacles {
                runner_type_retired: true,
                ..CLEAR
            }),
            Some(false)
        );
        assert_eq!(
            finished.restartable_now(RestartObstacles {
                parent_job_terminal: true,
                ..CLEAR
            }),
            Some(false)
        );
        assert_eq!(
            finished.restartable_now(RestartObstacles {
                restart_job_live: true,
                ..CLEAR
            }),
            Some(false)
        );
        assert_eq!(
            entry(AgentState::Running, true, true).restartable_now(CLEAR),
            None
        );
    }

    #[test]
    fn a_live_restart_job_stands_before_a_retired_type_and_after_an_ended_parent() {
        let finished = entry(AgentState::Finished, true, true);
        let live_and_retired = RestartObstacles {
            runner_type_retired: true,
            restart_job_live: true,
            ..CLEAR
        };
        assert_eq!(
            finished.restart_standing(live_and_retired),
            RestartStanding::RestartInFlight
        );
        assert_eq!(
            finished.restart_standing(RestartObstacles {
                parent_job_terminal: true,
                ..live_and_retired
            }),
            RestartStanding::ParentJobTerminal
        );
    }

    #[test]
    fn only_an_agent_with_an_inbox_has_a_mail_address_and_it_is_its_agent_id() {
        let mut with_inbox = entry(AgentState::Running, false, false);
        assert_eq!(with_inbox.mail_address(), Some(with_inbox.agent_id));
        with_inbox.has_inbox = false;
        assert_eq!(with_inbox.mail_address(), None);
    }
}
