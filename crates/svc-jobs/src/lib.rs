pub mod app;
pub mod bus;
pub mod config;
pub mod db;
pub mod edge;
pub mod error;
pub mod runner_transport;
pub mod runtime;
pub mod stream;

use std::future::IntoFuture;
use std::sync::{Arc, OnceLock};

use axum::Router;
use axum::routing::{get, post};
use br_util_axum_readiness::{ReadinessHandle, readiness_route};
use br_util_nats_fabric::{Fabric, NatsAuth, OutboxRelay};
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

    readiness.set_not_ready("binding the declared NATS infrastructure");
    let fabric = connect_fabric(&settings).await?;
    let channels =
        RunnerChannels::bind(&settings.nats_url, settings.nats_credentials.as_ref()).await?;
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

    spawn_background(&settings, &fabric, &channels, &jobs, &hub, pool);

    let _ = schema.set(edge::build_schema(edge::state::EdgeState {
        store,
        jobs,
        hub,
    }));
    readiness.set_ready();
    Ok(())
}

async fn migrate(settings: &Settings) -> Result<(), ServiceError> {
    let migration_pool = init_migration_pool().await.map_err(infra)?;
    if let Some(password) = settings.app_password.as_deref() {
        ensure_app_role(&migration_pool, APP_ROLE, password)
            .await
            .map_err(infra)?;
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

fn spawn_background(
    settings: &Settings,
    fabric: &Fabric,
    channels: &RunnerChannels,
    jobs: &Arc<Jobs>,
    hub: &Hub,
    pool: sqlx::PgPool,
) {
    let relay = OutboxRelay::new(pool.clone(), fabric.clone());
    let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        let _keep_open = shutdown;
        if let Err(error) = relay.run(shutdown_rx).await {
            tracing::error!(error = %error, "the integration outbox relay stopped");
        }
    });

    let listening_hub = hub.clone();
    tokio::spawn(async move {
        if let Err(error) = stream::listen(pool, listening_hub).await {
            tracing::error!(error = %error, "the durable fact listener stopped");
        }
    });

    spawn_transport(channels.clone(), Arc::clone(jobs));
    spawn_bus(fabric.clone(), Arc::clone(jobs));

    tokio::spawn(runtime::dispatch_loop(
        Arc::clone(jobs),
        hub.clone(),
        settings.backstop_interval,
        settings.dispatch_minimum_wake,
    ));
    tokio::spawn(runtime::backstop_loop(
        Arc::clone(jobs),
        channels.clone(),
        settings.backstop_interval,
    ));
}

fn spawn_transport(channels: RunnerChannels, jobs: Arc<Jobs>) {
    let presence_channels = channels.clone();
    let presence_jobs = Arc::clone(&jobs);
    tokio::spawn(async move {
        if let Err(error) =
            runner_transport::presence::watch(presence_channels, presence_jobs).await
        {
            tracing::error!(error = %error, "the runner presence watch stopped");
        }
    });
    let status_channels = channels.clone();
    let status_jobs = Arc::clone(&jobs);
    tokio::spawn(async move {
        if let Err(error) =
            runner_transport::consume::consume_status(status_channels, status_jobs).await
        {
            tracing::error!(error = %error, "the runner status consumer stopped");
        }
    });
    tokio::spawn(async move {
        if let Err(error) = runner_transport::consume::consume_logs(channels, jobs).await {
            tracing::error!(error = %error, "the runner log consumer stopped");
        }
    });
}

fn spawn_bus(fabric: Fabric, jobs: Arc<Jobs>) {
    let creations = fabric.clone();
    let creation_jobs = Arc::clone(&jobs);
    tokio::spawn(async move {
        if let Err(error) = bus::consume_creations(creations, creation_jobs).await {
            tracing::error!(error = %error, "the job-creation consumer stopped");
        }
    });
    let cancellations = fabric.clone();
    let cancellation_jobs = Arc::clone(&jobs);
    tokio::spawn(async move {
        if let Err(error) = bus::consume_cancellations(cancellations, cancellation_jobs).await {
            tracing::error!(error = %error, "the job-cancellation consumer stopped");
        }
    });
    let completions = fabric.clone();
    let completion_jobs = Arc::clone(&jobs);
    tokio::spawn(async move {
        if let Err(error) = bus::consume_completions(completions, completion_jobs).await {
            tracing::error!(error = %error, "the job-completion consumer stopped");
        }
    });
    tokio::spawn(async move {
        if let Err(error) = bus::consume_failures(fabric, jobs).await {
            tracing::error!(error = %error, "the job-failure consumer stopped");
        }
    });
}
