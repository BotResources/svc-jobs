use br_core_auth::Passport;
use br_test_harness::{FabricTestNats, GraphqlClient, SseSubscription, verdict};
use reqwest::StatusCode;
use serde_json::{Value, json};

use super::fixture::{impersonating_member, machine_caller};
use super::{QUIET, catalog, docs, gql, stream};

pub struct RunnerTypeLifecycleAttack<'a> {
    pub client: &'a GraphqlClient,
    pub admin: &'a Passport,
    pub member: &'a Passport,
    pub fabric: &'a FabricTestNats,
    pub runner_type: &'a str,
    pub unknown_runner_type: &'a str,
}

const FORBIDDEN: &str = "FORBIDDEN";
const UNAUTHENTICATED: &str = "UNAUTHENTICATED";

const LIFECYCLE_MUTATIONS: [(&str, &str); 3] = [
    (docs::DEPRECATE_RUNNER_TYPE, "deprecating"),
    (docs::REACTIVATE_RUNNER_TYPE, "reactivating"),
    (docs::RETIRE_RUNNER_TYPE, "retiring"),
];

pub async fn assert_no_hostile_caller_governs_a_runner_type(
    attack: &RunnerTypeLifecycleAttack<'_>,
    watch: &mut SseSubscription,
) {
    // Given: the current lifecycle state on every channel, captured before the attack
    let before_view = gql::fleet_of(attack.client, attack.admin, attack.runner_type).await;
    let before_entry = catalog::entry(attack.fabric, attack.runner_type).await;

    // When: every caller who is not a platform administrator attacks every lifecycle mutation,
    // against a runner type that exists and one that does not
    let hostile = [
        (attack.member.clone(), "an ordinary organization member"),
        (
            impersonating_member(attack.admin),
            "a member impersonating an administrator",
        ),
        (machine_caller(), "a machine passport"),
    ];
    let mut authorization_codes = Vec::new();
    for (caller, who) in &hostile {
        for (document, action) in LIFECYCLE_MUTATIONS {
            for target in [attack.runner_type, attack.unknown_runner_type] {
                let existence = if target == attack.runner_type {
                    "a known"
                } else {
                    "an unknown"
                };
                let what = format!("{who} {action} {existence} runner type");
                let refused = attack
                    .client
                    .query(
                        caller,
                        document,
                        json!({ "input": { "runnerType": target } }),
                    )
                    .await;
                authorization_codes.push(verdict::expect_code_shaped(&refused, &what));
                assert_no_runner_type_leaked(&refused, attack.runner_type, &what);
            }
        }
    }

    // When: a caller presents no passport at all, or one the edge cannot decode
    let mut boundary_codes = Vec::new();
    for (forged, who) in [
        (None, "a caller with no passport at all"),
        (
            Some("not-a-passport"),
            "a caller with an undecodable passport",
        ),
    ] {
        for (document, action) in LIFECYCLE_MUTATIONS {
            let mut headers: Vec<(&str, &str)> = Vec::new();
            if let Some(value) = forged {
                headers.push(("X-Passport", value));
            }
            let what = format!("{who} {action} a runner type");
            let (status, body) = attack
                .client
                .post_raw(
                    "/graphql",
                    &headers,
                    json!({
                        "query": document,
                        "variables": { "input": { "runnerType": attack.runner_type } },
                    }),
                )
                .await;
            assert_eq!(
                status,
                StatusCode::UNAUTHORIZED,
                "{what} must be closed at the trust boundary, never answered on a permissive \
                 default: {body}"
            );
            boundary_codes.push(refusal_code(&body, &what));
            assert_no_runner_type_leaked(&body, attack.runner_type, &what);
        }
    }

    // Then: one stable code answers every unauthorized caller, whatever the mutation and whether
    // the named runner type exists — authorization runs before lookup, so no refusal is an
    // existence oracle
    for code in &authorization_codes {
        assert_eq!(
            code, FORBIDDEN,
            "every unauthorized lifecycle attempt answers the one stable authorization code, or \
             the client learns from the difference which type exists and which mutation the caller \
             almost had: {authorization_codes:?}"
        );
    }
    for code in &boundary_codes {
        assert_eq!(
            code, UNAUTHENTICATED,
            "a missing passport and an undecodable one are the same closed door under one stable \
             code — an edge that decodes leniently into an empty caller would answer differently: \
             {boundary_codes:?}"
        );
    }

    // Then: nothing moved on any of the three remaining channels
    stream::expect_total_silence(
        watch,
        "a refused caller pushes no lifecycle delta to any subscriber",
        QUIET,
    )
    .await;
    assert_eq!(
        gql::fleet_of(attack.client, attack.admin, attack.runner_type).await,
        before_view,
        "a refused caller leaves the administrator's read and its affordances identical",
    );
    assert_eq!(
        catalog::entry(attack.fabric, attack.runner_type).await,
        before_entry,
        "a refused caller rewrites no Published Language entry, so no downstream context ever \
         observes the transition it was denied",
    );
}

fn refusal_code(body: &Value, what: &str) -> String {
    let code = verdict::mutation_error_code(body).unwrap_or_else(|| {
        panic!("{what}: a refusal at the boundary still owes a structured code: {body}")
    });
    assert!(
        verdict::is_code_shaped(&code),
        "{what}: the refusal code must be a stable code, not prose: {code}"
    );
    code
}

fn assert_no_runner_type_leaked(response: &Value, runner_type: &str, what: &str) {
    assert!(
        !response.to_string().contains(runner_type),
        "{what} must leak neither the runner-type identity nor its lifecycle to a caller who may \
         not govern it: {response}"
    );
    let data = &response["data"];
    assert!(
        data.is_null()
            || data
                .as_object()
                .is_some_and(|fields| fields.values().all(Value::is_null)),
        "{what} must return no fleet data at all: {response}"
    );
}
