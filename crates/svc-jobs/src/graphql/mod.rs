//! The GraphQL edge of `svc-jobs` — one subgraph, autonomous.
//!
//! The gateway composes this schema into the supergraph and routes to it; it
//! does not resolve entities across subgraphs, and nothing here should assume it
//! will. Composition serves edge clients only — a call from another service
//! belongs on the bus, never here.
//!
//! Two entry points build the same roots, and the duplication is deliberate:
//! [`build_schema`] injects the runtime state a resolver needs, [`sdl`] builds
//! the schema with none of it so the `schema` CLI arg can print the document
//! without a database in reach. The SDL never reflects injected data, so the two
//! agree by construction.

use async_graphql::{EmptyMutation, EmptySubscription, Object, Schema};
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::Extension;
use axum::extract::State;
use infra::Passport;
use sqlx::PgPool;

pub type JobsSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

/// The query root.
///
/// Every root field on it — query, mutation, subscription alike — is prefixed
/// with the service name, because the gateway flattens all subgraphs into one
/// namespace and an unprefixed `health` would collide with every other service's.
pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// The version of the running binary.
    // Every word of the doc comment above is published in the SDL and
    // photographed onto the registry, which is why the rationale is a plain
    // comment: a schema with no fields is not a valid GraphQL document, so one
    // placeholder query is the doctrinal minimum rather than a convenience.
    // Replace it with the first real query of this context — there is no reason
    // to keep it once one exists.
    async fn jobs_health(&self) -> String {
        env!("CARGO_PKG_VERSION").to_owned()
    }
}

/// The schema that serves, with the runtime state resolvers read from the
/// context.
pub fn build_schema(pool: PgPool) -> JobsSchema {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(pool)
        .finish()
}

/// The served SDL, built state-free.
///
/// The `schema` CLI arg prints this, and CD photographs it onto the registry
/// PatchVersion from the very binary it is about to publish — so it is the
/// schema that ships, not one exported from a source tree that may have moved.
pub fn sdl() -> String {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .finish()
        .sdl()
}

/// The `/graphql` handler, for POST and GET alike.
///
/// The Passport arrives already decoded by the middleware and is put into the
/// request context, where resolvers read it to do authorization. This service
/// does no authentication of its own: that happened at the gateway edge, and a
/// second opinion here would be a second thing to keep in sync.
pub async fn graphql_route(
    State(schema): State<JobsSchema>,
    passport: Option<Extension<Passport>>,
    request: GraphQLRequest,
) -> GraphQLResponse {
    let mut request = request.into_inner();
    if let Some(Extension(passport)) = passport {
        request = request.data(passport);
    }
    schema.execute(request).await.into()
}
