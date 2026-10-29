//! The trusted frontier: the Passport mapped to the principal facts the
//! domain decides on, and the admission of a request to the surface it asks
//! for.
//!
//! Platform administrators reach the whole surface. Any other active human
//! reaches the user surface alone — their own maximum priority — so a request
//! of theirs is admitted only when every root field it selects belongs to that
//! surface; anything else is refused at the door, before a resolver runs.

use async_graphql::parser::parse_query;
use async_graphql::parser::types::Selection;
use axum::http::StatusCode;
use bc_jobs::domain::access::{PlatformRole, PrincipalFacts, PrincipalKind};
use br_core_auth::Passport;
use br_util_graphql::EdgeError;

/// The root fields of the user surface, as the schema names them.
pub const USER_SURFACE: &[&str] = &["jobsMyMaxPriority", "jobsMyMaxPriorityChanged"];

const TYPENAME: &str = "__typename";

pub fn principal_of(passport: &Passport) -> PrincipalFacts {
    let kind = match passport {
        Passport::Human { .. } => PrincipalKind::Human,
        Passport::Service { .. } => PrincipalKind::Service,
    };
    // The Passport carries one platform privilege, the admin flag of the
    // platform org: its bearers are the super users, who are the platform
    // administrators.
    let role = if passport.is_super_admin() {
        PlatformRole::SuperUser
    } else {
        PlatformRole::Member
    };
    PrincipalFacts::new(kind, passport.is_active(), role)
}

pub fn admit(passport: &Passport, query: &str) -> Result<(), (StatusCode, EdgeError)> {
    let principal = principal_of(passport);
    if principal.guard_platform_administrator().is_ok() {
        return Ok(());
    }
    // One closed door for every caller turned away here, whatever the reason:
    // the refusal says nothing about what the caller might have reached.
    if principal.guard_reads_own_max_priority().is_ok() && selects_user_surface_only(query) {
        Ok(())
    } else {
        Err((StatusCode::FORBIDDEN, EdgeError::forbidden()))
    }
}

fn selects_user_surface_only(query: &str) -> bool {
    let Ok(document) = parse_query(query) else {
        return false;
    };
    document.operations.iter().all(|(_, operation)| {
        operation
            .node
            .selection_set
            .node
            .items
            .iter()
            .all(|selection| match &selection.node {
                Selection::Field(field) => {
                    let name = field.node.name.node.as_str();
                    name == TYPENAME || USER_SURFACE.contains(&name)
                }
                Selection::FragmentSpread(_) | Selection::InlineFragment(_) => false,
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_reading_only_the_users_own_maximum_is_the_user_surface() {
        // Given: the query and the subscription of the user surface
        // When/Then: both are recognised as selecting nothing else
        assert!(selects_user_surface_only(
            "{ jobsMyMaxPriority { maxPriority } }"
        ));
        assert!(selects_user_surface_only(
            "subscription S { jobsMyMaxPriorityChanged { __typename } }"
        ));
    }

    #[test]
    fn a_request_reaching_for_anything_else_is_not() {
        // Given: requests mixing in an administrator field, hiding one in a fragment, or
        // introspecting the schema
        for query in [
            "{ jobsMyMaxPriority { maxPriority } jobsFleet { runnerType { typeKey } } }",
            "{ ... on Query { jobsFleet { runnerType { typeKey } } } }",
            "{ __schema { types { name } } }",
            "not graphql",
        ] {
            // When/Then: none is the user surface
            assert!(!selects_user_surface_only(query), "{query}");
        }
    }
}
