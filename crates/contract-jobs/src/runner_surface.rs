use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::segment::SubjectSegment;
use crate::{
    CMD_JOBS_RUNNER_LIST_AGENTS_RUNNER_TYPE, CMD_JOBS_RUNNER_MARK_READ_RUNNER_TYPE,
    CMD_JOBS_RUNNER_REGISTER_AGENTS_RUNNER_TYPE,
};

pub const SURFACE_WIRE_VERSION: u8 = 1;

pub const REGISTER_AGENTS_FILTER: &str = "jobs.runner.register_agents.*";
pub const MARK_READ_FILTER: &str = "jobs.runner.mark_read.*";
pub const LIST_AGENTS_FILTER: &str = "jobs.runner.list_agents.*";

pub const REGISTER_AGENTS_PREFIX: &str = "jobs.runner.register_agents.";
pub const MARK_READ_PREFIX: &str = "jobs.runner.mark_read.";
pub const LIST_AGENTS_PREFIX: &str = "jobs.runner.list_agents.";

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
    JobNotOfRun,
    Malformed,
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
pub struct SurfaceReply {
    pub version: u8,
    pub accepted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<SurfaceReason>,
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
            reason: Some(reason),
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
    pub reason: Option<SurfaceReason>,
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
            reason: Some(reason),
            message: Some(message.to_owned()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_request_renders_its_own_subject_for_the_runner_type() {
        let analyst = SubjectSegment::runner_type("analyst").unwrap();
        assert_eq!(
            register_agents_subject(&analyst),
            "jobs.runner.register_agents.analyst"
        );
        assert_eq!(mark_read_subject(&analyst), "jobs.runner.mark_read.analyst");
        assert_eq!(
            list_agents_subject(&analyst),
            "jobs.runner.list_agents.analyst"
        );
    }

    #[test]
    fn a_registration_without_a_declared_restart_reads_as_not_restartable() {
        let agent: AgentDeclaration = serde_json::from_value(serde_json::json!({
            "agent_id": Uuid::nil().to_string(),
            "has_inbox": true,
        }))
        .unwrap();
        assert!(!agent.declares_restart);
    }

    #[test]
    fn a_refusal_carries_its_reason_and_its_words_and_an_acceptance_neither() {
        let accepted = serde_json::to_value(SurfaceReply::accepted()).unwrap();
        assert_eq!(
            accepted,
            serde_json::json!({ "version": 1, "accepted": true })
        );
        let refused = serde_json::to_value(SurfaceReply::refused(
            SurfaceReason::RunNotStarted,
            "The run has not started.",
        ))
        .unwrap();
        assert_eq!(refused["reason"], "run_not_started");
        assert_eq!(refused["accepted"], false);
    }

    #[test]
    fn every_reason_travels_as_its_snake_case_code() {
        let codes = [
            (SurfaceReason::RunUnknown, "run_unknown"),
            (SurfaceReason::RunNotStarted, "run_not_started"),
            (SurfaceReason::RunEnded, "run_ended"),
            (SurfaceReason::RunRemoved, "run_removed"),
            (SurfaceReason::InstanceMismatch, "instance_mismatch"),
            (SurfaceReason::AgentIdInOtherTree, "agent_id_in_other_tree"),
            (SurfaceReason::AgentIdNotFinished, "agent_id_not_finished"),
            (SurfaceReason::ItemNotYours, "item_not_yours"),
            (SurfaceReason::JobNotOfRun, "job_not_of_run"),
            (SurfaceReason::Malformed, "malformed"),
        ];
        for (reason, code) in codes {
            assert_eq!(serde_json::to_value(reason).unwrap(), code);
            assert_eq!(
                serde_json::from_value::<SurfaceReason>(serde_json::json!(code)).unwrap(),
                reason
            );
        }
    }
}
