use async_trait::async_trait;
use chrono::{DateTime, Utc};

use super::mail::Stored;
use crate::domain::runner::saved_state::SavedState;
use crate::domain::runner::views::SavedStateView;
use crate::domain::runner::work::WorkNamed;

#[async_trait]
pub trait SavedStates: Send + Sync {
    async fn save_state(
        &self,
        work: WorkNamed<'_>,
        state: &SavedState,
        maximum_bytes: usize,
        at: DateTime<Utc>,
    ) -> Stored<()>;

    async fn read_saved_state(&self, work: WorkNamed<'_>) -> Stored<SavedStateView>;
}
