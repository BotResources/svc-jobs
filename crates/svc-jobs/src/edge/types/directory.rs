use async_graphql::{Enum, SimpleObject};
use bc_jobs::domain::directory::AgentState;
use bc_jobs::ports::directory::ListedEntry;
use uuid::Uuid;

#[derive(Enum, Clone, Copy, PartialEq, Eq)]
#[graphql(name = "JobsAgentState")]
pub enum GqlAgentState {
    Running,
    Finished,
}

#[derive(SimpleObject)]
#[graphql(name = "JobsDirectoryAgent")]
pub struct GqlDirectoryAgent {
    pub agent_id: Uuid,
    pub run_id: Uuid,
    pub runner_type: String,
    pub state: GqlAgentState,
    pub mail_address: Option<Uuid>,
    pub restartable_now: Option<bool>,
}

impl From<ListedEntry> for GqlDirectoryAgent {
    fn from(row: ListedEntry) -> Self {
        let entry = row.entry;
        Self {
            agent_id: entry.agent_id.as_uuid(),
            run_id: entry.run_id.as_uuid(),
            runner_type: entry.runner_type.as_str().to_owned(),
            state: match entry.state {
                AgentState::Running => GqlAgentState::Running,
                AgentState::Finished => GqlAgentState::Finished,
            },
            mail_address: entry.mail_address().map(|address| address.as_uuid()),
            restartable_now: entry.restartable_now(row.obstacles),
        }
    }
}
