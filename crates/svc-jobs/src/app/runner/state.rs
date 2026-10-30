use super::{Jobs, Outcome, WorkIds, started};
use crate::db::saved_states::SavedStateView;

pub async fn save_state(jobs: &Jobs, work: &WorkIds, state: serde_json::Value) -> Outcome<()> {
    started(jobs, work).await?;
    Ok(jobs
        .store
        .save_state(
            work.named(),
            &state,
            jobs.limits.saved_state_max_bytes(),
            jobs.clock.now(),
        )
        .await?)
}

pub async fn read_saved_state(jobs: &Jobs, work: &WorkIds) -> Outcome<SavedStateView> {
    started(jobs, work).await?;
    Ok(jobs.store.read_saved_state(work.named()).await?)
}
