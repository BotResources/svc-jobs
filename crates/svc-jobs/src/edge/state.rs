use std::sync::Arc;

use async_graphql::{Context, Result};
use bc_jobs::domain::access::PrincipalFacts;
use bc_jobs::domain::keys::DisplayName;
use bc_jobs::domain::references::KnownUser;
use br_core_auth::Passport;
use br_core_events::UserId;
use br_util_graphql::EdgeError;

use super::access::principal_of;
use super::error::of_domain;
use crate::app::Jobs;
use crate::db::PgStore;
use crate::stream::Hub;

#[derive(Clone)]
pub struct EdgeState {
    pub store: PgStore,
    pub jobs: Arc<Jobs>,
    pub hub: Hub,
}

pub fn passport_of<'a>(ctx: &'a Context<'_>) -> Result<&'a Passport> {
    ctx.data_opt::<Passport>()
        .ok_or_else(|| async_graphql::Error::from(EdgeError::unauthenticated()))
}

pub fn principal(ctx: &Context<'_>) -> Result<PrincipalFacts> {
    Ok(principal_of(passport_of(ctx)?))
}

pub fn administrator(ctx: &Context<'_>) -> Result<KnownUser> {
    known_administrator(passport_of(ctx)?)
}

pub fn actor(ctx: &Context<'_>) -> Result<KnownUser> {
    known_user(passport_of(ctx)?)
}

pub fn own_user(ctx: &Context<'_>) -> Result<uuid::Uuid> {
    let passport = passport_of(ctx)?;
    principal_of(passport)
        .guard_reads_own_max_priority()
        .map_err(|refusal| async_graphql::Error::from(of_domain(&refusal)))?;
    Ok(passport.actor_id())
}

pub fn known_administrator(passport: &Passport) -> Result<KnownUser> {
    if !is_platform_administrator(passport) {
        return Err(EdgeError::forbidden().into());
    }
    known_user(passport)
}

fn known_user(passport: &Passport) -> Result<KnownUser> {
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
    principal_of(passport)
        .guard_platform_administrator()
        .is_ok()
}
