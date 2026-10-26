mod support;

use std::borrow::Cow;

use br_test_harness::{E2eDatabase, SseSubscription};
use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::PgPool;
use sqlx::migrate::Migrator;
use uuid::Uuid;

use support::events::EventLog;
use support::fixture::{
    APP_PASSWORD, APP_ROLE, JobsFixture, Knobs, require_provisioned_infrastructure,
};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{JOB_CHANGED, LONG, SHORT, delta, dispatch, gql, stream, subs, wire};

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

const PRE_0_4_MIGRATIONS: usize = 2;
const SERVICE_DEFAULT_SECONDS: u64 = 3;
const CHANGED_DEFAULT_SECONDS: u64 = 7;

#[tokio::test]
async fn a_runner_type_from_before_0_4_follows_the_service_maximum_run_duration() {
    // Given: a database carrying the pre-0.4 shape with one runner type a live instance once
    // announced, applied through the real migrator — the only way such a type comes to exist
    require_provisioned_infrastructure();
    let db = E2eDatabase::create(true, &[])
        .await
        .with_app_role(APP_ROLE, APP_PASSWORD)
        .await;
    let pool = PgPool::connect(&db.owner_migration_url())
        .await
        .expect("the owner connection opens");
    apply_the_first_migrations(&pool, 1).await;
    let legacy_type = wire::unique_runner_type("legacy");
    seed_a_type_announced_before_0_4(&pool, &legacy_type).await;
    apply_the_first_migrations(&pool, PRE_0_4_MIGRATIONS).await;
    pool.close().await;

    // When: the 0.4 service boots on it with a service maximum run duration of three seconds
    let knobs = Knobs {
        run_max_duration_seconds: SERVICE_DEFAULT_SECONDS,
        backstop_interval_seconds: 1,
        ..Knobs::default()
    };
    let mut fixture = JobsFixture::start_on(db, knobs).await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin().clone();
    let producer = Producer::new(fixture.fabric(), "projects");

    // Then: the pre-0.4 type reads the service maximum, not a value frozen at migration
    assert_eq!(
        dispatch::runner_type_view(&client, &admin, &legacy_type).await["runnerType"]["maxRunDurationSeconds"],
        json!(SERVICE_DEFAULT_SECONDS),
        "a runner type from before 0.4 has no maximum of its own: it follows \
         JOBS_RUN_MAX_DURATION_SECONDS",
    );

    // When: a run of that type starts and overruns the service maximum
    let mut instance = FakeRunner::new(fixture.nats(), &legacy_type, "instance-a");
    instance.connect().await;
    let declaration = JobDeclaration::new(&legacy_type).with_max_attempts(1);
    let job_id = declaration.job_id;
    producer.declare_v2(&declaration).await;
    let trigger = instance.next_trigger(LONG).await;
    let mut watch = SseSubscription::open(
        fixture.url(),
        &admin,
        &subs::job_changed_with_priority(job_id),
    )
    .await;
    stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;
    instance.start_run(&trigger).await;

    // Then: it fails as transient at the service maximum
    let overrun =
        stream::await_delta_of(&mut watch, JOB_CHANGED, wire::EVT_RUN_FAILED, job_id, LONG).await;
    let event = delta::event_of(&overrun, wire::EVT_RUN_FAILED, job_id);
    assert_eq!(event["runId"], json!(runner::run_id(&trigger).to_string()));
    assert_eq!(event["failureKind"], json!("TRANSIENT"));
    gql::wait_for_status(&client, &admin, job_id, "FAILED", LONG).await;

    // When: the service restarts with another maximum
    fixture
        .restart_first(&Knobs {
            run_max_duration_seconds: CHANGED_DEFAULT_SECONDS,
            backstop_interval_seconds: 1,
            ..Knobs::default()
        })
        .await;

    // Then: the pre-0.4 type follows the new service maximum
    assert_eq!(
        dispatch::runner_type_view(&fixture.gql(), &admin, &legacy_type).await["runnerType"]["maxRunDurationSeconds"],
        json!(CHANGED_DEFAULT_SECONDS),
        "the service maximum is read, never copied: a changed JOBS_RUN_MAX_DURATION_SECONDS \
         applies to every type without a maximum of its own",
    );

    events.stop().await;
    fixture.shutdown().await;
}

async fn apply_the_first_migrations(pool: &PgPool, count: usize) {
    let migrations = MIGRATOR.iter().take(count).cloned().collect::<Vec<_>>();
    assert_eq!(
        migrations.len(),
        count,
        "the pre-0.4 schema is the first {count} declared migrations"
    );
    Migrator {
        migrations: Cow::Owned(migrations),
        ignore_missing: true,
        locking: MIGRATOR.locking,
        no_tx: MIGRATOR.no_tx,
    }
    .run(pool)
    .await
    .expect("the pre-0.4 schema applies through the real migrator");
}

async fn seed_a_type_announced_before_0_4(pool: &PgPool, type_key: &str) {
    let runner_type_id = Uuid::now_v7();
    let instance_id = Uuid::now_v7();
    let session_id = Uuid::now_v7();
    let connected_at = "2025-01-02T03:04:05Z"
        .parse::<DateTime<Utc>>()
        .expect("a fixed connection time");
    sqlx::query("INSERT INTO runner_types (id, type_key) VALUES ($1, $2)")
        .bind(runner_type_id)
        .bind(type_key)
        .execute(pool)
        .await
        .expect("the legacy runner type inserts");
    sqlx::query(
        "INSERT INTO runner_instances (id, runner_type_id, instance_key) \
         VALUES ($1, $2, 'legacy-1')",
    )
    .bind(instance_id)
    .bind(runner_type_id)
    .execute(pool)
    .await
    .expect("the legacy instance inserts");
    sqlx::query(
        "INSERT INTO runner_presence_sessions \
         (id, instance_id, version, connected_at, last_observed_at) \
         VALUES ($1, $2, '0.3.0', $3, $3)",
    )
    .bind(session_id)
    .bind(instance_id)
    .bind(connected_at)
    .execute(pool)
    .await
    .expect("the legacy presence inserts");
    sqlx::query(
        "INSERT INTO runner_status_changes \
         (session_id, change_number, reported_status, capacity, observed_at) \
         VALUES ($1, 1, 'READY', 1, $2)",
    )
    .bind(session_id)
    .bind(connected_at)
    .execute(pool)
    .await
    .expect("a legacy presence session carries the status its instance announced");
}
