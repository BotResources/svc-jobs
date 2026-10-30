use super::{Jobs, Outcome, WorkIds, started};
use bc_jobs::domain::runner::saved_state::SavedState;
use bc_jobs::domain::runner::views::SavedStateView;
use bc_jobs::ports::saved_state::SavedStates;

pub async fn save_state(jobs: &Jobs, work: &WorkIds, state: serde_json::Value) -> Outcome<()> {
    started(jobs, work).await?;
    let state = match SavedState::new(state, jobs.limits.saved_state_max_bytes()) {
        Ok(state) => state,
        Err(refusal) => return Ok(Err(refusal.into())),
    };
    Ok(jobs
        .store
        .save_state(work.named(), &state, jobs.clock.now())
        .await?)
}

pub async fn read_saved_state(jobs: &Jobs, work: &WorkIds) -> Outcome<SavedStateView> {
    started(jobs, work).await?;
    Ok(jobs.store.read_saved_state(work.named()).await?)
}
