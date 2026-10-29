//! The administrator's read of users' maximum priorities: for each user asked
//! for, the level that applies — their own or the service default, and which
//! of the two — with the caller's affordances of setting and clearing it.
//!
//! The snapshot and the live changes share one projection,
//! `UserMaxPriority::view`, which takes its affordances from the very
//! decisions the set and clear commands enforce.

use bc_jobs::domain::access::PrincipalFacts;
use bc_jobs::domain::priority::{UserMaxPriorityView, UserSelection};
use bc_jobs::ports::user::UserMaxPriorityReader;
use uuid::Uuid;

use super::Jobs;
use crate::error::ServiceError;

/// Admits the caller to the read and resolves the users it asks for. Reading
/// users' maxima is an administrator gesture.
pub fn select(
    principal: &PrincipalFacts,
    user_ids: &[Uuid],
) -> Result<UserSelection, ServiceError> {
    principal.guard_platform_administrator()?;
    Ok(UserSelection::new(user_ids)?)
}

/// The read itself: the views of the users asked for, each once, in the order
/// first asked for.
pub async fn read(
    jobs: &Jobs,
    principal: &PrincipalFacts,
    user_ids: &[Uuid],
) -> Result<Vec<UserMaxPriorityView>, ServiceError> {
    let users = select(principal, user_ids)?;
    snapshot(jobs, principal, &users).await
}

/// The views of the selected users, in the selection's order, read in one
/// statement — the read's answer and a live view's opening snapshot alike.
pub async fn snapshot(
    jobs: &Jobs,
    principal: &PrincipalFacts,
    users: &UserSelection,
) -> Result<Vec<UserMaxPriorityView>, ServiceError> {
    let maxima = UserMaxPriorityReader::load_selected(&jobs.store, users).await?;
    Ok(maxima
        .iter()
        .map(|user| user.view(principal, &jobs.limits))
        .collect())
}
