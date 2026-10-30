use async_graphql::{Context, Result};
use br_core_auth::Passport;
use br_core_scope::ScopeKey;
use br_util_graphql::EdgeError;

use crate::edge::access::principal_of;
use crate::edge::state::passport_of;

pub const RUNNER_SCOPE: &str = "jobs:runner";

pub fn holds_runner_scope(passport: &Passport) -> bool {
    let scope_held = ScopeKey::new(RUNNER_SCOPE)
        .map(|scope| passport.has_scope(&scope))
        .unwrap_or(false);
    principal_of(passport)
        .guard_holds_runner_scope(scope_held)
        .is_ok()
}

pub fn runner_caller<'a>(ctx: &'a Context<'_>) -> Result<&'a Passport> {
    let passport = passport_of(ctx)?;
    if holds_runner_scope(passport) {
        Ok(passport)
    } else {
        Err(EdgeError::forbidden().into())
    }
}
