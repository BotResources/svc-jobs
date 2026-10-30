use async_graphql::parser::parse_query;
use async_graphql::parser::types::Selection;
use axum::http::StatusCode;
use bc_jobs::domain::access::{PlatformRole, PrincipalFacts, PrincipalKind};
use br_core_auth::Passport;
use br_util_graphql::EdgeError;

use super::runner::{RUNNER_SURFACE, holds_runner_scope};

pub const USER_SURFACE: &[&str] = &["jobsMyMaxPriority", "jobsMyMaxPriorityChanged"];

const TYPENAME: &str = "__typename";

pub fn principal_of(passport: &Passport) -> PrincipalFacts {
    let kind = match passport {
        Passport::Human { .. } => PrincipalKind::Human,
        Passport::Service { .. } => PrincipalKind::Service,
    };
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
    let user_surface =
        principal.guard_reads_own_max_priority().is_ok() && selects_only(query, USER_SURFACE);
    let runner_surface = holds_runner_scope(passport) && selects_only(query, RUNNER_SURFACE);
    if user_surface || runner_surface {
        Ok(())
    } else {
        Err((StatusCode::FORBIDDEN, EdgeError::forbidden()))
    }
}

fn selects_only(query: &str, surface: &[&str]) -> bool {
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
                    name == TYPENAME || surface.contains(&name)
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
        assert!(selects_only(
            "{ jobsMyMaxPriority { maxPriority } }",
            USER_SURFACE
        ));
        assert!(selects_only(
            "subscription S { jobsMyMaxPriorityChanged { __typename } }",
            USER_SURFACE
        ));
    }

    #[test]
    fn a_request_reaching_for_anything_else_is_not() {
        for query in [
            "{ jobsMyMaxPriority { maxPriority } jobsFleet { runnerType { typeKey } } }",
            "{ ... on Query { jobsFleet { runnerType { typeKey } } } }",
            "{ __schema { types { name } } }",
            "not graphql",
        ] {
            assert!(!selects_only(query, USER_SURFACE), "{query}");
        }
    }

    #[test]
    fn a_request_selecting_runner_fields_only_is_the_runner_surface() {
        assert!(selects_only(
            "mutation { jobsSendMail(input: {}) { success } }",
            RUNNER_SURFACE
        ));
        assert!(selects_only(
            "{ jobsUnread(work: {}) { success } __typename }",
            RUNNER_SURFACE
        ));
        assert!(!selects_only(
            "{ jobsUnread(work: {}) { success } jobs { edges { cursor } } }",
            RUNNER_SURFACE
        ));
        assert!(!selects_only(
            "{ ... on Query { jobsUnread(work: {}) { success } } }",
            RUNNER_SURFACE
        ));
    }
}
