use bc_jobs::domain::directory::listing::RestartObstacles;
use bc_jobs::domain::directory::registration::{AgentDeclaration, RegistrationRefusal};
use bc_jobs::domain::directory::{AgentId, AgentState};
use bc_jobs::domain::ids::RunId;
use contract_jobs::runner_surface as wire;

use super::Jobs;
use crate::db::directory::ListedRow;

pub async fn register_agents(
    jobs: &Jobs,
    runner_type: &str,
    request: wire::RegisterAgents,
) -> wire::SurfaceReply {
    let Some((run_id, declarations)) = decode_registration(&request) else {
        return refused(RegistrationRefusal::Malformed);
    };
    let outcome = jobs
        .store
        .register_agents(
            run_id,
            runner_type,
            &request.instance_key,
            &declarations,
            jobs.clock.now(),
        )
        .await;
    match outcome {
        Ok(Ok(())) => wire::SurfaceReply::accepted(),
        Ok(Err(refusal)) => refused(refusal),
        Err(error) => {
            tracing::warn!(error = %error, "an agent registration could not be stored");
            wire::SurfaceReply::refused(wire::REASON_UNAVAILABLE, "Jobs could not answer now.")
        }
    }
}

pub async fn list_agents(
    jobs: &Jobs,
    runner_type: &str,
    request: wire::ListAgents,
) -> wire::ListAgentsReply {
    let Ok(run_id) = RunId::new(request.run_id) else {
        let refusal = RegistrationRefusal::Malformed;
        return wire::ListAgentsReply::refused(refusal.code(), refusal.message());
    };
    match jobs
        .store
        .list_agents(run_id, runner_type, &request.instance_key)
        .await
    {
        Ok(Ok(rows)) => wire::ListAgentsReply::listed(
            rows.into_iter()
                .filter(|row| row.entry.is_listed())
                .map(listed)
                .collect(),
        ),
        Ok(Err(refusal)) => wire::ListAgentsReply::refused(refusal.code(), refusal.message()),
        Err(error) => {
            tracing::warn!(error = %error, "the agents of a tree could not be read");
            wire::ListAgentsReply::refused(wire::REASON_UNAVAILABLE, "Jobs could not answer now.")
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
                agent_id: AgentId::new(agent.agent_id).ok()?,
                has_inbox: agent.has_inbox,
                declares_restart: agent.declares_restart,
            })
        })
        .collect::<Option<Vec<_>>>()?;
    Some((run_id, declarations))
}

fn refused(refusal: RegistrationRefusal) -> wire::SurfaceReply {
    wire::SurfaceReply::refused(refusal.code(), refusal.message())
}

fn listed(row: ListedRow) -> wire::ListedAgent {
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
