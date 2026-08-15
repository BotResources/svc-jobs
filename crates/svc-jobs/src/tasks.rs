use std::sync::Arc;

use br_util_nats_fabric::{Fabric, OutboxRelay};

use crate::app::Jobs;
use crate::config::Settings;
use crate::error::ServiceError;
use crate::runner_transport::RunnerChannels;
use crate::stream::Hub;
use crate::supervision::Supervisor;
use crate::{bus, runner_transport, runtime, stream};

pub fn spawn_supervised(
    supervisor: &Supervisor,
    settings: &Settings,
    fabric: &Fabric,
    channels: &RunnerChannels,
    jobs: &Arc<Jobs>,
    hub: &Hub,
    pool: sqlx::PgPool,
) {
    spawn_durable_fanout(supervisor, pool.clone(), fabric.clone(), hub.clone());
    spawn_transport(supervisor, channels.clone(), Arc::clone(jobs));
    spawn_bus(supervisor, fabric.clone(), Arc::clone(jobs));
    spawn_timers(
        supervisor,
        settings,
        channels.clone(),
        Arc::clone(jobs),
        hub,
    );
}

fn spawn_durable_fanout(supervisor: &Supervisor, pool: sqlx::PgPool, fabric: Fabric, hub: Hub) {
    let relay_pool = pool.clone();
    supervisor.spawn("integration outbox relay", move || {
        let relay = OutboxRelay::new(relay_pool.clone(), fabric.clone());
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        async move {
            let _keep_open = shutdown;
            relay
                .run(shutdown_rx)
                .await
                .map_err(|error| ServiceError::Infra(error.to_string()))
        }
    });

    let listening_hub = hub.clone();
    supervisor.spawn_with_teardown(
        "durable fact listener",
        move || {
            let pool = pool.clone();
            let hub = listening_hub.clone();
            async move { stream::listen(pool, hub).await }
        },
        move || hub.end_every_subscription(),
    );
}

fn spawn_transport(supervisor: &Supervisor, channels: RunnerChannels, jobs: Arc<Jobs>) {
    let presence_channels = channels.clone();
    let presence_jobs = Arc::clone(&jobs);
    supervisor.spawn("runner presence watch", move || {
        let channels = presence_channels.clone();
        let jobs = Arc::clone(&presence_jobs);
        async move { runner_transport::presence::watch(channels, jobs).await }
    });

    let status_channels = channels.clone();
    let status_jobs = Arc::clone(&jobs);
    supervisor.spawn("runner status consumer", move || {
        let channels = status_channels.clone();
        let jobs = Arc::clone(&status_jobs);
        async move { runner_transport::consume::consume_status(channels, jobs).await }
    });

    supervisor.spawn("runner log consumer", move || {
        let channels = channels.clone();
        let jobs = Arc::clone(&jobs);
        async move { runner_transport::consume::consume_logs(channels, jobs).await }
    });
}

fn spawn_bus(supervisor: &Supervisor, fabric: Fabric, jobs: Arc<Jobs>) {
    let creations = fabric.clone();
    let creation_jobs = Arc::clone(&jobs);
    supervisor.spawn("job-creation consumer", move || {
        let fabric = creations.clone();
        let jobs = Arc::clone(&creation_jobs);
        async move { bus::consume_creations(fabric, jobs).await }
    });

    let cancellations = fabric.clone();
    let cancellation_jobs = Arc::clone(&jobs);
    supervisor.spawn("job-cancellation consumer", move || {
        let fabric = cancellations.clone();
        let jobs = Arc::clone(&cancellation_jobs);
        async move { bus::consume_cancellations(fabric, jobs).await }
    });

    let completions = fabric.clone();
    let completion_jobs = Arc::clone(&jobs);
    supervisor.spawn("job-completion consumer", move || {
        let fabric = completions.clone();
        let jobs = Arc::clone(&completion_jobs);
        async move { bus::consume_completions(fabric, jobs).await }
    });

    supervisor.spawn("job-failure consumer", move || {
        let fabric = fabric.clone();
        let jobs = Arc::clone(&jobs);
        async move { bus::consume_failures(fabric, jobs).await }
    });
}

fn spawn_timers(
    supervisor: &Supervisor,
    settings: &Settings,
    channels: RunnerChannels,
    jobs: Arc<Jobs>,
    hub: &Hub,
) {
    let dispatch_jobs = Arc::clone(&jobs);
    let dispatch_hub = hub.clone();
    let backstop_interval = settings.backstop_interval;
    let minimum_wake = settings.dispatch_minimum_wake;
    supervisor.spawn("dispatch loop", move || {
        let jobs = Arc::clone(&dispatch_jobs);
        let hub = dispatch_hub.clone();
        async move {
            runtime::dispatch_loop(jobs, hub, backstop_interval, minimum_wake).await;
            Ok(())
        }
    });

    supervisor.spawn("backstop loop", move || {
        let jobs = Arc::clone(&jobs);
        let channels = channels.clone();
        async move {
            runtime::backstop_loop(jobs, channels, backstop_interval).await;
            Ok(())
        }
    });
}
