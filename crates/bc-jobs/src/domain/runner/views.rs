use crate::domain::ids::AgentId;
use crate::domain::payload::PayloadAttachment;

#[derive(Debug, Clone, PartialEq)]
pub struct SavedStateView {
    pub state: serde_json::Value,
    pub agent_ids: Vec<AgentId>,
    pub mail_addresses: Vec<AgentId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredResult {
    pub message: String,
    pub attachments: Vec<PayloadAttachment>,
}
