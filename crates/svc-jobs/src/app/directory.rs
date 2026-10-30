use bc_jobs::domain::directory::listing::RestartObstacles;
use bc_jobs::domain::directory::registration::{
    AgentDeclaration, RegistrationRefusal, decide_registration, require_live,
};
use bc_jobs::domain::directory::{AgentId, AgentState};
use bc_jobs::domain::ids::RunId;
use bc_jobs::domain::runner::work::{MarkReadRefusal, RunRefusal};
use bc_jobs::ports::directory::{AgentDirectory, ListedEntry, RunCaller};
use bc_jobs::ports::inbox::Inbox;
use contract_jobs::runner_surface as wire;

use super::{Jobs, catch_up};

pub async fn register_agents(
    jobs: &Jobs,
    runner_type: &str,
    request: wire::RegisterAgents,
) -> Option<wire::SurfaceReply> {
    if request.version != wire::SURFACE_WIRE_VERSION {
        return Some(refused(RegistrationRefusal::Malformed));
    }
    let Some((run_id, declarations)) = decode_registration(&request) else {
        return Some(refused(RegistrationRefusal::Malformed));
    };
    if let Err(error) = catch_up::apply_acknowledged_start(jobs, run_id).await {
        tracing::warn!(error = %error, "the acknowledged start of a run could not be applied");
        return None;
    }
    let caller = RunCaller {
        run_id,
        runner_type,
        instance_key: &request.instance_key,
    };
    let decide = |standing, held: &[_]| decide_registration(run_id, standing, &declarations, held);
    match jobs
        .store
        .register(caller, &declarations, &decide, jobs.clock.now())
        .await
    {
        Ok(Ok(())) => Some(wire::SurfaceReply::accepted()),
        Ok(Err(refusal)) => Some(refused(refusal)),
        Err(error) => {
            tracing::warn!(error = %error, "an agent registration could not be stored");
            None
        }
    }
}

pub async fn mark_read(
    jobs: &Jobs,
    runner_type: &str,
    request: wire::MarkRead,
) -> Option<wire::SurfaceReply> {
    if request.version != wire::SURFACE_WIRE_VERSION {
        return Some(refused(RegistrationRefusal::Malformed));
    }
    let Ok(run_id) = RunId::new(request.run_id) else {
        return Some(refused(RegistrationRefusal::Malformed));
    };
    if let Err(error) = catch_up::apply_acknowledged_start(jobs, run_id).await {
        tracing::warn!(error = %error, "the acknowledged start of a run could not be applied");
        return None;
    }
    let outcome = jobs
        .store
        .mark_read(
            request.run_id,
            &request.instance_key,
            runner_type,
            &request.item_ids,
            jobs.clock.now(),
        )
        .await;
    match outcome {
        Ok(Ok(())) => Some(wire::SurfaceReply::accepted()),
        Ok(Err(refusal)) => Some(wire::SurfaceReply::refused(
            mark_read_reason(&refusal),
            refusal.message(),
        )),
        Err(error) => {
            tracing::warn!(error = %error, "a read marker could not be stored");
            None
        }
    }
}

pub async fn list_agents(
    jobs: &Jobs,
    runner_type: &str,
    request: wire::ListAgents,
) -> Option<wire::ListAgentsReply> {
    if request.version != wire::SURFACE_WIRE_VERSION {
        return Some(refused_listing(RegistrationRefusal::Malformed));
    }
    let Ok(run_id) = RunId::new(request.run_id) else {
        return Some(refused_listing(RegistrationRefusal::Malformed));
    };
    if let Err(error) = catch_up::apply_acknowledged_start(jobs, run_id).await {
        tracing::warn!(error = %error, "the acknowledged start of a run could not be applied");
        return None;
    }
    let caller = RunCaller {
        run_id,
        runner_type,
        instance_key: &request.instance_key,
    };
    match jobs.store.list(caller, &require_live).await {
        Ok(Ok(rows)) => Some(wire::ListAgentsReply::listed(
            rows.into_iter()
                .filter(|row| row.entry.is_listed())
                .map(listed)
                .collect(),
        )),
        Ok(Err(refusal)) => Some(refused_listing(refusal)),
        Err(error) => {
            tracing::warn!(error = %error, "the agents of a tree could not be read");
            None
        }
    }
}

fn decode_registration(request: &wire::RegisterAgents) -> Option<(RunId, Vec<AgentDeclaration>)> {
    let run_id = RunId::new(request.run_id).ok()?;
    let declarations = request
        .agents
        .iter()
        .map(|agent| {
            Some(AgentDeclaration {
                agent_id: AgentId::new(agent.agent_id),
                has_inbox: agent.has_inbox,
                declares_restart: agent.declares_restart,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some((run_id, declarations))
}

fn refused(refusal: RegistrationRefusal) -> wire::SurfaceReply {
    wire::SurfaceReply::refused(registration_reason(&refusal), refusal.message())
}

fn refused_listing(refusal: RegistrationRefusal) -> wire::ListAgentsReply {
    wire::ListAgentsReply::refused(registration_reason(&refusal), refusal.message())
}

fn registration_reason(refusal: &RegistrationRefusal) -> wire::SurfaceReason {
    match refusal {
        RegistrationRefusal::RunUnknown => wire::SurfaceReason::RunUnknown,
        RegistrationRefusal::RunNotStarted => wire::SurfaceReason::RunNotStarted,
        RegistrationRefusal::RunEnded => wire::SurfaceReason::RunEnded,
        RegistrationRefusal::RunRemoved => wire::SurfaceReason::RunRemoved,
        RegistrationRefusal::InstanceMismatch => wire::SurfaceReason::InstanceMismatch,
        RegistrationRefusal::AgentIdInOtherTree { .. } => wire::SurfaceReason::AgentIdInOtherTree,
        RegistrationRefusal::AgentIdNotFinished { .. } => wire::SurfaceReason::AgentIdNotFinished,
        RegistrationRefusal::AgentNamedTwice | RegistrationRefusal::Malformed => {
            wire::SurfaceReason::Malformed
        }
    }
}

fn mark_read_reason(refusal: &MarkReadRefusal) -> wire::SurfaceReason {
    match refusal {
        MarkReadRefusal::Run(run) => match run {
            RunRefusal::RunUnknown => wire::SurfaceReason::RunUnknown,
            RunRefusal::RunNotStarted => wire::SurfaceReason::RunNotStarted,
            RunRefusal::RunEnded => wire::SurfaceReason::RunEnded,
            RunRefusal::RunRemoved => wire::SurfaceReason::RunRemoved,
            RunRefusal::InstanceMismatch => wire::SurfaceReason::InstanceMismatch,
            RunRefusal::Malformed => wire::SurfaceReason::Malformed,
        },
        MarkReadRefusal::ItemNotYours => wire::SurfaceReason::ItemNotYours,
    }
}

fn listed(row: ListedEntry) -> wire::ListedAgent {
    let obstacles = RestartObstacles {
        runner_type_retired: row.runner_type_retired,
        parent_job_terminal: row.parent_job_terminal,
    };
    let entry = row.entry;
    wire::ListedAgent {
        agent_id: entry.agent_id.as_uuid(),
        run_id: entry.run_id.as_uuid(),
        runner_type: entry.runner_type.as_str().to_owned(),
        state: match entry.state {
            AgentState::Running => wire::AgentState::Running,
            AgentState::Finished => wire::AgentState::Finished,
        },
        has_inbox: entry.has_inbox,
        mail_address: entry.mail_address().map(|address| address.as_uuid()),
        restartable_now: entry.restartable_now(obstacles),
    }
}
