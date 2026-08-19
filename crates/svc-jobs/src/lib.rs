pub mod app;
pub mod bus;
pub mod config;
pub mod db;
pub mod edge;
pub mod error;
mod pg;
pub mod runner_transport;
pub mod runtime;
pub mod stream;
pub mod supervision;
pub mod tasks;

use std::future::IntoFuture;
use std::sync::{Arc, OnceLock};

use axum::Router;
use axum::routing::{get, post};
use bc_jobs::ports::environment::Clock;
use br_util_axum_readiness::{ReadinessHandle, readiness_route};
use br_util_nats_fabric::{Fabric, NatsAuth};
use br_util_observability::{http_metrics_layer, init_metrics, liveness_route, metrics_route};
use br_util_postgres::{ensure_app_role, grant_app_access, init_migration_pool, init_pool};

use app::Jobs;
use app::environment::{NanosecondJitter, SystemClock, UuidV7Factory};
use config::Settings;
use db::PgStore;
use edge::HttpState;
pub use error::ServiceError;
use runner_transport::RunnerChannels;
use stream::Hub;
use supervision::Supervisor;

const APP_ROLE: &str = "jobs_app";

pub async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let settings = Settings::from_environment()?;
    let metrics = init_metrics("svc-jobs")?;
    let readiness = ReadinessHandle::not_ready("migrating and opening the database pool");
    let schema: Arc<OnceLock<edge::JobsSchema>> = Arc::new(OnceLock::new());
    let port = settings.port;
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;

    let booting = Arc::clone(&schema);
    let boot_readiness = readiness.clone();
    let (report, boot_failure) = tokio::sync::oneshot::channel::<ServiceError>();
    tokio::spawn(async move {
        if let Err(error) = boot(settings, &booting, &boot_readiness).await {
            tracing::error!(error = %error, "svc-jobs could not open its declared infrastructure");
            boot_readiness.set_not_ready("the declared infrastructure could not be opened");
            let _ = report.send(error);
        }
    });

    let app = Router::new()
        .route("/graphql", post(edge::graphql_route).get(edge::playground))
        .layer(axum::middleware::from_fn(edge::auth::passport_layer))
        .route("/livez", liveness_route())
        .route("/readyz", readiness_route(readiness))
        .route("/metrics", metrics_route(metrics))
        .route("/sdl", get(|| async { edge::sdl() }))
        .layer(http_metrics_layer())
        .with_state(HttpState { schema });

    tracing::info!(port, "svc-jobs listening");
    tokio::select! {
        served = axum::serve(listener, app).into_future() => served?,
        Ok(failure) = boot_failure => return Err(Box::new(failure)),
    }
    Ok(())
}

async fn boot(
    settings: Settings,
    schema: &OnceLock<edge::JobsSchema>,
    readiness: &ReadinessHandle,
) -> Result<(), ServiceError> {
    migrate(&settings).await?;
    let pool = init_pool(&settings.database_url).await.map_err(infra)?;
    let store = PgStore::new(pool.clone());
    db::partitions::report_run_log_horizon(
        &pool,
        settings.log_partition_horizon_warning,
        SystemClock.now(),
    )
    .await;

    readiness.set_not_ready("binding the declared NATS infrastructure");
    let fabric = connect_fabric(&settings).await?;
    let channels = RunnerChannels::bind(
        &settings.nats_url,
        settings.nats_credentials.as_ref(),
        settings.consumer_tuning,
    )
    .await?;
    channels.verify_declared_streams().await?;
    bus::verify_durables(&fabric).await?;

    let hub = Hub::new();
    let jobs = Arc::new(Jobs {
        store: store.clone(),
        transport: Arc::new(channels.clone()),
        clock: Arc::new(SystemClock),
        ids: Arc::new(UuidV7Factory),
        jitter: Arc::new(NanosecondJitter),
        limits: settings.limits,
        retry: settings.retry_policy,
    });

    let supervisor = Supervisor::new(readiness.clone(), settings.restart_policy);
    tasks::spawn_supervised(
        &supervisor,
        &settings,
        &fabric,
        &channels,
        &jobs,
        &hub,
        pool,
    );

    let _ = schema.set(edge::build_schema(edge::state::EdgeState {
        store,
        jobs,
        hub,
    }));
    supervisor.boot_complete();
    Ok(())
}

/// Two PostgreSQL roles, one boot, in this exact order.
///
/// `jobs_owner` is the GitOps-declared migration role reached through
/// `init_migration_pool` (`DATABASE_URL_OWNER`). It owns the schema, it is by
/// definition an RLS-bypassing DB-management agent, and its pool is closed
/// before anything serves a request. `jobs_app` (`DATABASE_URL`) is the
/// least-privilege role every query afterwards runs as.
///
/// Provisioning happens here rather than split between a migration and the
/// cluster: `ensure_app_role` completes before the migrations, `grant_app_access`
/// after them, so the role exists at the moment it is granted. Two asynchronous
/// actors have no such ordering, and the failure mode is a silently skipped
/// grant that never re-runs.
///
/// Provisioning is guarded by an observation rather than assumed idempotent.
/// `ensure_app_role` guards its CREATE with `IF NOT EXISTS` but then runs
/// `ALTER ROLE … PASSWORD` unconditionally, and under PostgreSQL 16 that ALTER
/// is denied on the second boot: the implicit membership `jobs_owner` acquired
/// by creating `jobs_app` is revoked by the CNPG roles reconciler (the owner
/// declares no `inRoles`), and CREATEROLE alone no longer confers authority
/// over a role the grantee holds no ADMIN OPTION on. So we ask the catalog the
/// only question that matters — does the role already accept the configured
/// password? — and touch nothing when the answer is yes.
async fn migrate(settings: &Settings) -> Result<(), ServiceError> {
    let migration_pool = init_migration_pool().await.map_err(infra)?;
    if let Some(password) = settings.app_password.as_deref() {
        if pg::role_password_already_works(&settings.database_url, APP_ROLE, password).await? {
            tracing::info!(
                role = APP_ROLE,
                "runtime role already accepts the configured password — skipping ensure_app_role"
            );
        } else {
            ensure_app_role(&migration_pool, APP_ROLE, password)
                .await
                .map_err(infra)?;
        }
    }
    sqlx::migrate!("./migrations")
        .run(&migration_pool)
        .await
        .map_err(infra)?;
    if settings.app_password.is_some() {
        grant_app_access(&migration_pool, APP_ROLE)
            .await
            .map_err(infra)?;
    }
    migration_pool.close().await;
    Ok(())
}

fn infra(error: impl std::fmt::Display) -> ServiceError {
    ServiceError::Infra(error.to_string())
}

async fn connect_fabric(settings: &Settings) -> Result<Fabric, ServiceError> {
    let fabric = match &settings.nats_credentials {
        Some(credentials) => {
            Fabric::connect_with(
                &settings.nats_url,
                &NatsAuth {
                    user: credentials.user.clone(),
                    password: credentials.password.clone(),
                },
            )
            .await?
        }
        None => Fabric::connect(&settings.nats_url).await?,
    };
    Ok(fabric)
}
