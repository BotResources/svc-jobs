use std::collections::HashSet;

use crate::domain::directory::{AgentId, AgentState};
use crate::domain::ids::{JobId, RunId};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStanding {
    Unknown,
    NotStarted,
    Removed,
    StartedOnAnotherInstance,
    Started { initial_job_id: JobId },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentDeclaration {
    pub agent_id: AgentId,
    pub has_inbox: bool,
    pub declares_restart: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HeldAgent {
    pub agent_id: AgentId,
    pub run_id: RunId,
    pub initial_job_id: JobId,
    pub state: AgentState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistrationRefusal {
    RunUnknown,
    RunNotStarted,
    RunRemoved,
    InstanceMismatch,
    AgentIdInOtherTree { agent_id: AgentId },
    AgentIdNotFinished { agent_id: AgentId },
    Malformed,
}

impl RegistrationRefusal {
    pub fn code(&self) -> &'static str {
        match self {
            Self::RunUnknown => "run_unknown",
            Self::RunNotStarted => "run_not_started",
            Self::RunRemoved => "run_removed",
            Self::InstanceMismatch => "instance_mismatch",
            Self::AgentIdInOtherTree { .. } => "agent_id_in_other_tree",
            Self::AgentIdNotFinished { .. } => "agent_id_not_finished",
            Self::Malformed => "malformed",
        }
    }

    pub fn message(&self) -> &'static str {
        match self {
            Self::RunUnknown => "Jobs holds no such run.",
            Self::RunNotStarted => "The run is not started, so it cannot register agents.",
            Self::RunRemoved => "The run was removed from the agent directory.",
            Self::InstanceMismatch => "The run is not held by the instance that sent the request.",
            Self::AgentIdInOtherTree { .. } => {
                "An agent id of the request belongs to a run of another tree."
            }
            Self::AgentIdNotFinished { .. } => {
                "An agent id of the request belongs to an agent of another run that has not finished."
            }
            Self::Malformed => "The request names the same agent twice.",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placement {
    New,
    SameRun,
    TakesOverFinished { from_run: RunId },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlacedAgent {
    pub declaration: AgentDeclaration,
    pub placement: Placement,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registration {
    pub run_id: RunId,
    pub initial_job_id: JobId,
    pub agents: Vec<PlacedAgent>,
}

pub fn require_live(standing: RunStanding) -> Result<JobId, RegistrationRefusal> {
    match standing {
        RunStanding::Unknown => Err(RegistrationRefusal::RunUnknown),
        RunStanding::NotStarted => Err(RegistrationRefusal::RunNotStarted),
        RunStanding::Removed => Err(RegistrationRefusal::RunRemoved),
        RunStanding::StartedOnAnotherInstance => Err(RegistrationRefusal::InstanceMismatch),
        RunStanding::Started { initial_job_id } => Ok(initial_job_id),
    }
}

pub fn decide_registration(
    run_id: RunId,
    standing: RunStanding,
    declarations: &[AgentDeclaration],
    held: &[HeldAgent],
) -> Result<Registration, RegistrationRefusal> {
    let initial_job_id = require_live(standing)?;
    let mut seen = HashSet::new();
    if !declarations
        .iter()
        .all(|declaration| seen.insert(declaration.agent_id))
    {
        return Err(RegistrationRefusal::Malformed);
    }
    let mut agents = Vec::with_capacity(declarations.len());
    for declaration in declarations {
        let placement = placement_of(run_id, initial_job_id, declaration.agent_id, held)?;
        agents.push(PlacedAgent {
            declaration: *declaration,
            placement,
        });
    }
    Ok(Registration {
        run_id,
        initial_job_id,
        agents,
    })
}

fn placement_of(
    run_id: RunId,
    initial_job_id: JobId,
    agent_id: AgentId,
    held: &[HeldAgent],
) -> Result<Placement, RegistrationRefusal> {
    let Some(entry) = held.iter().find(|entry| entry.agent_id == agent_id) else {
        return Ok(Placement::New);
    };
    if entry.run_id == run_id {
        return Ok(Placement::SameRun);
    }
    if entry.initial_job_id != initial_job_id {
        return Err(RegistrationRefusal::AgentIdInOtherTree { agent_id });
    }
    match entry.state {
        AgentState::Finished => Ok(Placement::TakesOverFinished {
            from_run: entry.run_id,
        }),
        AgentState::Running => Err(RegistrationRefusal::AgentIdNotFinished { agent_id }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures::{job_id, run_id};
    use uuid::Uuid;

    fn agent() -> AgentId {
        AgentId::new(Uuid::now_v7())
    }

    fn declaring(agent_id: AgentId) -> AgentDeclaration {
        AgentDeclaration {
            agent_id,
            has_inbox: true,
            declares_restart: true,
        }
    }

    fn started(initial_job_id: JobId) -> RunStanding {
        RunStanding::Started { initial_job_id }
    }

    #[test]
    fn only_a_started_run_on_the_sending_instance_registers() {
        let run = run_id();
        for (standing, refusal) in [
            (RunStanding::Unknown, RegistrationRefusal::RunUnknown),
            (RunStanding::NotStarted, RegistrationRefusal::RunNotStarted),
            (RunStanding::Removed, RegistrationRefusal::RunRemoved),
            (
                RunStanding::StartedOnAnotherInstance,
                RegistrationRefusal::InstanceMismatch,
            ),
        ] {
            assert_eq!(
                decide_registration(run, standing, &[declaring(agent())], &[]),
                Err(refusal)
            );
        }
    }

    #[test]
    fn a_new_agent_is_registered_under_the_tree_of_its_run() {
        let (run, tree, id) = (run_id(), job_id(), agent());
        let registration = decide_registration(run, started(tree), &[declaring(id)], &[]).unwrap();
        assert_eq!(registration.initial_job_id, tree);
        assert_eq!(registration.agents[0].placement, Placement::New);
    }

    #[test]
    fn a_later_registration_of_an_agent_of_the_same_run_replaces_what_it_declared() {
        let (run, tree, id) = (run_id(), job_id(), agent());
        let held = [HeldAgent {
            agent_id: id,
            run_id: run,
            initial_job_id: tree,
            state: AgentState::Running,
        }];
        let registration =
            decide_registration(run, started(tree), &[declaring(id)], &held).unwrap();
        assert_eq!(registration.agents[0].placement, Placement::SameRun);
    }

    #[test]
    fn a_finished_agent_of_the_same_tree_is_taken_over_by_a_restart_run() {
        let (restart, finished, tree, id) = (run_id(), run_id(), job_id(), agent());
        let held = [HeldAgent {
            agent_id: id,
            run_id: finished,
            initial_job_id: tree,
            state: AgentState::Finished,
        }];
        let registration =
            decide_registration(restart, started(tree), &[declaring(id)], &held).unwrap();
        assert_eq!(
            registration.agents[0].placement,
            Placement::TakesOverFinished { from_run: finished }
        );
    }

    #[test]
    fn a_running_agent_of_another_run_is_refused_and_so_is_one_of_another_tree() {
        let (run, other, tree, id) = (run_id(), run_id(), job_id(), agent());
        let running = [HeldAgent {
            agent_id: id,
            run_id: other,
            initial_job_id: tree,
            state: AgentState::Running,
        }];
        assert_eq!(
            decide_registration(run, started(tree), &[declaring(id)], &running),
            Err(RegistrationRefusal::AgentIdNotFinished { agent_id: id })
        );
        let foreign = [HeldAgent {
            initial_job_id: job_id(),
            state: AgentState::Finished,
            ..running[0]
        }];
        assert_eq!(
            decide_registration(run, started(tree), &[declaring(id)], &foreign),
            Err(RegistrationRefusal::AgentIdInOtherTree { agent_id: id })
        );
    }

    #[test]
    fn one_bad_agent_registers_nothing_and_a_repeated_agent_is_malformed() {
        let (run, tree, good, bad) = (run_id(), job_id(), agent(), agent());
        let held = [HeldAgent {
            agent_id: bad,
            run_id: run_id(),
            initial_job_id: tree,
            state: AgentState::Running,
        }];
        assert!(
            decide_registration(
                run,
                started(tree),
                &[declaring(good), declaring(bad)],
                &held
            )
            .is_err()
        );
        assert_eq!(
            decide_registration(run, started(tree), &[declaring(good), declaring(good)], &[]),
            Err(RegistrationRefusal::Malformed)
        );
    }
}
