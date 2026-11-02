use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::reason::Reason;
use crate::segment::SubjectSegment;
use crate::{
    CMD_JOBS_RUNNER_LIST_AGENTS_RUNNER_TYPE, CMD_JOBS_RUNNER_MARK_READ_RUNNER_TYPE,
    CMD_JOBS_RUNNER_REGISTER_AGENTS_RUNNER_TYPE, CMD_JOBS_RUNNER_STARTED_RUNNER_TYPE,
};

pub const SURFACE_WIRE_VERSION: u8 = 1;

pub const REGISTER_AGENTS_FILTER: &str = "jobs.runner.register_agents.*";
pub const MARK_READ_FILTER: &str = "jobs.runner.mark_read.*";
pub const LIST_AGENTS_FILTER: &str = "jobs.runner.list_agents.*";
pub const STARTED_FILTER: &str = "jobs.runner.started.*";

pub const REGISTER_AGENTS_PREFIX: &str = "jobs.runner.register_agents.";
pub const MARK_READ_PREFIX: &str = "jobs.runner.mark_read.";
pub const LIST_AGENTS_PREFIX: &str = "jobs.runner.list_agents.";
pub const STARTED_PREFIX: &str = "jobs.runner.started.";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SurfaceReason {
    RunUnknown,
    RunNotStarted,
    RunEnded,
    RunRemoved,
    InstanceMismatch,
    AgentIdInOtherTree,
    AgentIdNotFinished,
    ItemNotYours,
    Malformed,
    InstanceUnknown,
    RunHeld,
    RunReservedElsewhere,
    RunNotPending,
}

fn wire_version() -> u8 {
    SURFACE_WIRE_VERSION
}

fn render(template: &str, runner_type: &SubjectSegment) -> String {
    template.replace("{runner_type}", runner_type.as_str())
}

pub fn register_agents_subject(runner_type: &SubjectSegment) -> String {
    render(CMD_JOBS_RUNNER_REGISTER_AGENTS_RUNNER_TYPE, runner_type)
}

pub fn mark_read_subject(runner_type: &SubjectSegment) -> String {
    render(CMD_JOBS_RUNNER_MARK_READ_RUNNER_TYPE, runner_type)
}

pub fn list_agents_subject(runner_type: &SubjectSegment) -> String {
    render(CMD_JOBS_RUNNER_LIST_AGENTS_RUNNER_TYPE, runner_type)
}

pub fn started_subject(runner_type: &SubjectSegment) -> String {
    render(CMD_JOBS_RUNNER_STARTED_RUNNER_TYPE, runner_type)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentDeclaration {
    pub agent_id: Uuid,
    pub has_inbox: bool,
    #[serde(default)]
    pub declares_restart: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisterAgents {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub run_id: Uuid,
    pub instance_key: String,
    pub agents: Vec<AgentDeclaration>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkRead {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub run_id: Uuid,
    pub instance_key: String,
    pub item_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListAgents {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub run_id: Uuid,
    pub instance_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartedRequest {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub run_id: Uuid,
    pub instance_key: String,
    pub start_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SurfaceReply {
    pub version: u8,
    pub accepted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<Reason<SurfaceReason>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl SurfaceReply {
    pub fn accepted() -> Self {
        Self {
            version: SURFACE_WIRE_VERSION,
            accepted: true,
            reason: None,
            message: None,
        }
    }

    pub fn refused(reason: SurfaceReason, message: &str) -> Self {
        Self {
            version: SURFACE_WIRE_VERSION,
            accepted: false,
            reason: Some(reason.into()),
            message: Some(message.to_owned()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum AgentState {
    Running,
    Finished,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListedAgent {
    pub agent_id: Uuid,
    pub run_id: Uuid,
    pub runner_type: String,
    pub state: AgentState,
    pub has_inbox: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mail_address: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restartable_now: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListAgentsReply {
    pub version: u8,
    pub accepted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agents: Option<Vec<ListedAgent>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<Reason<SurfaceReason>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

impl ListAgentsReply {
    pub fn listed(agents: Vec<ListedAgent>) -> Self {
        Self {
            version: SURFACE_WIRE_VERSION,
            accepted: true,
            agents: Some(agents),
            reason: None,
            message: None,
        }
    }

    pub fn refused(reason: SurfaceReason, message: &str) -> Self {
        Self {
            version: SURFACE_WIRE_VERSION,
            accepted: false,
            agents: None,
            reason: Some(reason.into()),
            message: Some(message.to_owned()),
        }
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod tests_started;
