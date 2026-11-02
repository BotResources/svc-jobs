use crate::domain::directory::listing::{DirectoryEntry, RestartObstacles, RestartStanding};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestartWake {
    Due,
    NothingWaiting,
    Blocked(RestartStanding),
}

impl RestartWake {
    pub fn is_due(self) -> bool {
        self == Self::Due
    }
}

pub fn decide_restart_wake(
    entry: &DirectoryEntry,
    obstacles: RestartObstacles,
    unread_waiting: bool,
) -> RestartWake {
    match entry.restart_standing(obstacles) {
        RestartStanding::Allowed if unread_waiting => RestartWake::Due,
        RestartStanding::Allowed => RestartWake::NothingWaiting,
        standing => RestartWake::Blocked(standing),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::directory::AgentState;
    use crate::domain::ids::AgentId;
    use crate::domain::keys::RunnerTypeKey;
    use crate::domain::mail::delivery::{
        AddressStanding, HeldAddress, Recipient, RecipientKind, decide_deliveries,
    };
    use crate::fixtures::{job_id, run_id};
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

    fn sent_starts_a_restart(entry: &DirectoryEntry, obstacles: RestartObstacles) -> bool {
        let tree = job_id();
        let recipient = Recipient {
            address: entry.agent_id,
            kind: RecipientKind::Direct,
            standing: AddressStanding::Held(HeldAddress {
                entry: entry.clone(),
                initial_job_id: tree,
                obstacles,
            }),
        };
        match decide_deliveries(tree, &[recipient]) {
            Ok(plan) => plan.0[0].starts_restart,
            Err(_) => false,
        }
    }

    #[test]
    fn list_agents_send_mail_and_the_wake_agree_on_every_combination_of_facts() {
        let mut compared = 0;
        for state in [AgentState::Running, AgentState::Finished] {
            for (declares, holds) in [(true, true), (true, false), (false, true)] {
                for bits in 0..8u8 {
                    let obstacles = RestartObstacles {
                        runner_type_retired: bits & 1 != 0,
                        parent_job_terminal: bits & 2 != 0,
                        restart_job_live: bits & 4 != 0,
                    };
                    let entry = entry(state, declares, holds);
                    let listed = entry.restartable_now(obstacles) == Some(true);
                    let wakes = decide_restart_wake(&entry, obstacles, true).is_due();
                    assert_eq!(listed, wakes, "{state:?} {obstacles:?}");
                    assert_eq!(
                        sent_starts_a_restart(&entry, obstacles),
                        wakes,
                        "{state:?} {obstacles:?}"
                    );
                    compared += 1;
                }
            }
        }
        assert_eq!(compared, 48);
    }

    #[test]
    fn a_restartable_agent_with_nothing_unread_is_not_woken_and_a_live_restart_blocks_it() {
        let finished = entry(AgentState::Finished, true, true);
        assert_eq!(
            decide_restart_wake(&finished, CLEAR, false),
            RestartWake::NothingWaiting
        );
        assert_eq!(
            decide_restart_wake(
                &finished,
                RestartObstacles {
                    restart_job_live: true,
                    ..CLEAR
                },
                true
            ),
            RestartWake::Blocked(RestartStanding::RestartInFlight)
        );
    }
}
