//! `svc-jobs` — the binary half of the `jobs` bounded context.
//!
//! Everything impure lives on this side of the hexagon: the pools, the bus, the
//! GraphQL edge, the projectors. [`run`] is the composition root — the one place
//! that knows which concrete adapter satisfies which port, so every other module
//! can be written against the trait and tested without any of this.
//!
//! One thing separates this composition root from its monorepo sibling: the
//! dependencies go **direct to `br-rust-common`** — `br_util_postgres`,
//! `br_util_axum_auth`, `br_util_observability` — because a standalone service
//! repository has no shared `infra` crate to lean on. The hexagon itself is
//! unchanged: `bc-jobs` stays pure, and this crate is still the only
//! place that knows which adapter satisfies which port.

pub mod error;
pub mod graphql;

use axum::Router;
use axum::routing::{get, post};
use br_util_axum_auth::passport_header_middleware;
use br_util_axum_readiness::{ReadinessHandle, readiness_route};
use br_util_observability::{http_metrics_layer, init_metrics, liveness_route, metrics_route};
use br_util_postgres::{ensure_app_role, grant_app_access, init_migration_pool, init_pool};

pub use error::ServiceError;

/// The port used when the environment does not say otherwise.
///
/// Every real deployment sets `PORT`, so this only ever decides a local run —
/// which is exactly why it may collide with a sibling service's default without
/// anyone noticing in production. Pick this service's own number early.
const DEFAULT_PORT: u16 = 8006;

/// Boot the service: provision the runtime role, migrate, open the runtime pool,
/// then serve the GraphQL edge. Returns when the listener stops.
pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // A `PORT` that is set but unparseable fails the boot rather than falling
    // back: a typo that silently serves on a different port than the one the
    // deployment declared is a service the probes cannot find.
    let port: u16 = match std::env::var("PORT") {
        Ok(raw) => raw.parse()?,
        Err(_) => DEFAULT_PORT,
    };
    let database_url = std::env::var("DATABASE_URL")?;

    // `/readyz` starts DOWN, carrying the reason it is down, and flips UP only
    // once everything below has succeeded. The handle is a cloneable toggle, so
    // a boot step that moves into a spawned task later — a declared stream
    // verified present, a scope declaration round-tripped, a listener
    // established — takes its clone and flips it from there, and nothing about
    // the shape here has to change. A service that reports Ready while silently
    // dropping the traffic it was deployed to handle is the failure this gate
    // exists to prevent.
    let readiness = ReadinessHandle::not_ready("migrating and opening the database pool");

    // Prometheus. `init_metrics` installs a process-global recorder, so it
    // happens exactly once and the handle it returns is what `/metrics` renders.
    // A second install returns an error rather than panicking, and propagating
    // it is right: two composition roots in one process is not a condition to
    // boot through.
    let metrics = init_metrics("svc-jobs")?;

    // Two PostgreSQL roles, one boot, in this exact order.
    //
    // `jobs_owner` is the GitOps-declared migration role, reached through
    // `init_migration_pool` (`DATABASE_URL_OWNER`, falling back to
    // `DATABASE_URL`). It owns the schema and is the only agent allowed to run
    // DDL — and it is, by definition, an RLS-bypassing DB-management agent,
    // which is why its pool is closed before anything serves a request.
    // `jobs_app` is the least-privilege runtime role (`DATABASE_URL`) every
    // query afterwards runs as.
    //
    // Role provisioning and grants are done HERE, in one sequential process,
    // rather than split between a migration and the cluster: `ensure_app_role`
    // completes before `migrate`, and `grant_app_access` after it, so the role is
    // guaranteed to exist at the moment it is granted. Two asynchronous actors
    // (a migration that grants `IF EXISTS`, a cluster that creates the role
    // whenever it gets round to it) have no such ordering, and the failure mode
    // is a silently skipped grant that never re-runs.
    //
    // Both steps are gated on the password being present. When it is not — local
    // dev, a single-superuser test database — the owner already holds full
    // access and there is nothing to provision.
    let app_password = std::env::var("JOBS_APP_PASSWORD").ok();
    let migration_pool = init_migration_pool().await?;
    if let Some(password) = app_password.as_deref() {
        ensure_app_role(&migration_pool, "jobs_app", password).await?;
    }
    sqlx::migrate!("./migrations").run(&migration_pool).await?;
    if app_password.is_some() {
        grant_app_access(&migration_pool, "jobs_app").await?;
    }
    migration_pool.close().await;

    let pool = init_pool(&database_url).await?;

    // The single NATS seam: every publish, consume, and KV access this service
    // ever grows binds to this handle, and `async-nats` never becomes a direct
    // dependency. The registry declares this major as one that speaks on the
    // bus, so connecting is a boot gate rather than a convenience — a service
    // that cannot reach the fabric must fail loudly here instead of serving
    // queries while silently dropping every integration message.
    //
    // Bound but unused until the first publisher or consumer exists. Give it a
    // real name the moment it has one.
    readiness.set_not_ready("connecting to the NATS fabric");
    let nats_url = std::env::var("NATS_URL")?;
    let _fabric = br_util_nats_fabric::Fabric::connect(&nats_url).await?;

    // Every dependency named above is up. Add the next gate before this line,
    // not after it.
    readiness.set_ready();

    let schema = graphql::build_schema(pool);

    // The SDL is served from the schema that is actually serving, not rebuilt on
    // demand: the gateway's composer reads `/sdl` to compose the supergraph, and
    // a second construction is a second chance for the two to disagree.
    let sdl = schema.sdl();

    let app = Router::new()
        .route(
            "/graphql",
            post(graphql::graphql_route).get(graphql::graphql_route),
        )
        // Authentication happened at the gateway edge; this middleware only
        // decodes the trusted `X-Passport` it injected (and strips any a client
        // forged). It layers over `/graphql` alone — the probe routes below are
        // called by kubelet and the composer, which carry no passport.
        .layer(axum::middleware::from_fn(passport_header_middleware))
        .route("/livez", liveness_route())
        .route("/readyz", readiness_route(readiness))
        .route("/metrics", metrics_route(metrics))
        .route("/sdl", get(move || async move { sdl }))
        // Outermost, and after every route, so it measures all of them: the
        // layer keys its labels off the MATCHED path, so a route added below it
        // would be counted as unmatched and disappear into one bucket.
        .layer(http_metrics_layer())
        .with_state(schema);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(port, "svc-jobs listening");
    axum::serve(listener, app).await?;

    Ok(())
}
