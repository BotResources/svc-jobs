use std::sync::Arc;

use async_graphql::{Context, Result};
use bc_jobs::domain::keys::DisplayName;
use bc_jobs::domain::references::KnownUser;
use br_core_auth::Passport;
use br_core_events::UserId;
use br_util_graphql::EdgeError;

use crate::app::Jobs;
use crate::db::PgStore;
use crate::stream::Hub;

#[derive(Clone)]
pub struct EdgeState {
    pub store: PgStore,
    pub jobs: Arc<Jobs>,
    pub hub: Hub,
}

pub fn administrator(ctx: &Context<'_>) -> Result<KnownUser> {
    let passport = ctx
        .data_opt::<Passport>()
        .ok_or_else(|| async_graphql::Error::from(EdgeError::unauthenticated()))?;
    known_administrator(passport)
}

pub fn known_administrator(passport: &Passport) -> Result<KnownUser> {
    if !is_platform_administrator(passport) {
        return Err(EdgeError::forbidden().into());
    }
    let display_name = passport
        .claim::<String>("display_name")
        .filter(|claimed| !claimed.trim().is_empty())
        .unwrap_or_else(|| passport.actor_id().to_string());
    KnownUser::new(
        UserId(passport.actor_id()),
        DisplayName::new(display_name)
            .map_err(|_| async_graphql::Error::from(EdgeError::forbidden()))?,
    )
    .map_err(|_| EdgeError::forbidden().into())
}

pub fn is_platform_administrator(passport: &Passport) -> bool {
    matches!(passport, Passport::Human { .. }) && passport.is_super_admin() && passport.is_active()
}
