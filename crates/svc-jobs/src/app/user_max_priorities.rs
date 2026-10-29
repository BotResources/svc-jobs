//! The administrator's read of users' maximum priorities: for each user asked
//! for, the level that applies — their own or the service default, and which
//! of the two — with the caller's affordances of setting and clearing it.
//!
//! The read and its live changes (`user_max_priorities_live`) share one
//! projection, `UserMaxPriority::view`, which takes its affordances from the
//! very decisions the set and clear commands enforce.

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
/// first asked for, read in one statement.
pub async fn read(
    jobs: &Jobs,
    principal: &PrincipalFacts,
    user_ids: &[Uuid],
) -> Result<Vec<UserMaxPriorityView>, ServiceError> {
    let users = select(principal, user_ids)?;
    let maxima = UserMaxPriorityReader::load_selected(&jobs.store, &users).await?;
    Ok(maxima
        .iter()
        .map(|found| found.maximum.view(principal, &jobs.limits))
        .collect())
}
