mod support;

use std::borrow::Cow;

use br_test_harness::{E2eDatabase, SseSubscription};
use chrono::{DateTime, Utc};
use contract_jobs::catalog::RunnerTypeLifecycle as PublishedLifecycle;
use serde_json::json;
use sqlx::migrate::Migrator;
use sqlx::{PgPool, Row};
use uuid::Uuid;

use support::events::EventLog;
use support::fixture::{
    APP_PASSWORD, APP_ROLE, JobsFixture, Knobs, require_provisioned_infrastructure,
};
use support::producer::{JobDeclaration, Producer};
use support::{FLEET_CHANGED, LONG, SHORT, catalog, gql, stream, subs, wire};

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

const PRESENCE_BACKED_KEY: &str = "presence";
const ROUTING_ONLY_KEY: &str = "job-only";
const NEVER_ANNOUNCED_KEY: &str = "runtime-job-only";

#[tokio::test]
async fn legacy_runner_types_are_backfilled_as_active_without_breaking_references() {
    // Given: a database carrying the 0.1 shape, applied through the real migrator so the service
    // that boots on it later finds its own bookkeeping
    require_provisioned_infrastructure();
    let db = E2eDatabase::create(true, &[])
        .await
        .with_app_role(APP_ROLE, APP_PASSWORD)
        .await;
    let pool = PgPool::connect(&db.owner_migration_url())
        .await
        .expect("the owner connection opens");
    apply_the_0_1_schema(&pool).await;

    let presence_type_id = Uuid::now_v7();
    let job_type_id = Uuid::now_v7();
    let instance_id = Uuid::now_v7();
    let session_id = Uuid::now_v7();
    let producer_id = Uuid::now_v7();
    let job_id = Uuid::now_v7();
    let connected_at = "2025-01-02T03:04:05Z"
        .parse::<DateTime<Utc>>()
        .expect("a fixed connection time");
    let job_created_at = "2025-02-03T04:05:06Z"
        .parse::<DateTime<Utc>>()
        .expect("a fixed job creation time");

    // Given: one routing key that a live instance once announced, and one that only ever appeared
    // on a Job
    sqlx::query("INSERT INTO runner_types (id, type_key) VALUES ($1, $3), ($2, $4)")
        .bind(presence_type_id)
        .bind(job_type_id)
        .bind(PRESENCE_BACKED_KEY)
        .bind(ROUTING_ONLY_KEY)
        .execute(&pool)
        .await
        .expect("legacy runner types insert");
    sqlx::query(
        "INSERT INTO runner_instances (id, runner_type_id, instance_key) \
         VALUES ($1, $2, 'presence-1')",
    )
    .bind(instance_id)
    .bind(presence_type_id)
    .execute(&pool)
    .await
    .expect("the legacy instance inserts");
    sqlx::query(
        "INSERT INTO runner_presence_sessions \
         (id, instance_id, version, connected_at, last_observed_at) \
         VALUES ($1, $2, '0.1.0', $3, $3)",
    )
    .bind(session_id)
    .bind(instance_id)
    .bind(connected_at)
    .execute(&pool)
    .await
    .expect("the legacy presence inserts");
    sqlx::query(
        "INSERT INTO runner_status_changes \
         (session_id, change_number, reported_status, capacity, observed_at) \
         VALUES ($1, 1, 'READY', 1, $2)",
    )
    .bind(session_id)
    .bind(connected_at)
    .execute(&pool)
    .await
    .expect("a legacy presence session always carried the status its instance announced");
    sqlx::query("INSERT INTO producers (id, bc_key) VALUES ($1, 'projects')")
        .bind(producer_id)
        .execute(&pool)
        .await
        .expect("the legacy producer inserts");
    sqlx::query(
        "INSERT INTO jobs (id, runner_type_id, producer_id, created_at) \
         VALUES ($1, $2, $3, $4)",
    )
    .bind(job_id)
    .bind(job_type_id)
    .bind(producer_id)
    .bind(job_created_at)
    .execute(&pool)
    .await
    .expect("the legacy job inserts");

    // When: the 0.2 migration applies over that legacy data
    MIGRATOR
        .run(&pool)
        .await
        .expect("the 0.2 migration applies over legacy data");

    // Then: presence alone is what promotes a routing key into a governed aggregate
    let rows = sqlx::query(
        "SELECT rt.type_key, registered.lifecycle::text AS lifecycle, registered.registered_at \
         FROM registered_runner_types registered \
         JOIN runner_types rt ON rt.id = registered.runner_type_id ORDER BY rt.type_key",
    )
    .fetch_all(&pool)
    .await
    .expect("the migrated catalog reads");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("type_key"), PRESENCE_BACKED_KEY);
    assert_eq!(rows[0].get::<String, _>("lifecycle"), "ACTIVE");
    assert_eq!(
        rows[0].get::<DateTime<Utc>, _>("registered_at"),
        connected_at
    );
    let job_only_registered: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM registered_runner_types WHERE runner_type_id = $1)",
    )
    .bind(job_type_id)
    .fetch_one(&pool)
    .await
    .expect("the job-only route can be distinguished from a registered aggregate");
    assert!(
        !job_only_registered,
        "historical routing alone must not invent a RunnerType entity"
    );

    let instance_backref: Uuid =
        sqlx::query_scalar("SELECT runner_type_id FROM runner_instances WHERE id = $1")
            .bind(instance_id)
            .fetch_one(&pool)
            .await
            .expect("the instance back-reference survives");
    let job_backref: Uuid = sqlx::query_scalar("SELECT runner_type_id FROM jobs WHERE id = $1")
        .bind(job_id)
        .fetch_one(&pool)
        .await
        .expect("the job back-reference survives");
    assert_eq!(instance_backref, presence_type_id);
    assert_eq!(job_backref, job_type_id);

    pool.close().await;

    // When: the real binary boots on that migrated database
    let fixture = JobsFixture::start_on(db, Knobs::default()).await;
    let client = fixture.gql();
    let admin = fixture.admin();

    // Then: the backfilled aggregate answers the front door as a governed ACTIVE runner type,
    // carrying the whole backend-owned action surface — a migration that only satisfies SQL leaves
    // an administrator with a type they cannot govern
    let migrated = gql::fleet_of(&client, admin, PRESENCE_BACKED_KEY).await;
    assert_eq!(
        migrated.len(),
        1,
        "the backfilled aggregate is readable through the real fleet query: {migrated:?}",
    );
    let view = &migrated[0];
    assert_eq!(
        view["runnerType"]["lifecycle"],
        json!("ACTIVE"),
        "a migrated type is governed from the lifecycle the backfill posed: {view}",
    );
    gql::assert_affordances_well_formed(view, "a backfilled runner type");
    for action in [
        wire::ACTION_DISPATCH,
        wire::ACTION_DEPRECATE,
        wire::ACTION_REACTIVATE,
        wire::ACTION_RETIRE,
    ] {
        gql::affordance(view, action);
    }
    gql::assert_allowed(view, wire::ACTION_DEPRECATE);
    assert_eq!(
        gql::assert_blocked(view, wire::ACTION_REACTIVATE),
        "runner_type_already_active",
    );
    assert_eq!(
        gql::assert_blocked(view, wire::ACTION_RETIRE),
        "runner_type_not_deprecated",
    );

    // Then: the routing-only key stays a route and never becomes something to govern
    assert!(
        gql::fleet_of(&client, admin, ROUTING_ONLY_KEY)
            .await
            .is_empty(),
        "a key that only ever routed a Job exposes no fleet view, so no administrator is offered \
         a lifecycle action on an aggregate that does not exist",
    );

    // Then: startup healing publishes the migrated aggregate under the frozen catalog key, so a
    // consumer that only ever reads the Published Language sees the migration too
    let published = catalog::wait_for_lifecycle(
        fixture.fabric(),
        PRESENCE_BACKED_KEY,
        PublishedLifecycle::Active,
        LONG,
    )
    .await;
    assert_eq!(published.runner_type, PRESENCE_BACKED_KEY);
    assert!(
        catalog::entry(fixture.fabric(), ROUTING_ONLY_KEY)
            .await
            .is_none(),
        "a routing-only key is never published: the catalog offers governed runner types, not \
         every string a Job once named",
    );

    // When: a producer declares work over the real bus for a key no instance ever announced
    let producer = Producer::new(fixture.fabric(), "projects");
    let events = EventLog::open(fixture.fabric()).await;
    let declaration = JobDeclaration::new(NEVER_ANNOUNCED_KEY);
    let routed_job = declaration.job_id;
    producer.declare(&declaration).await;
    events.expect_one(wire::FACT_QUEUED, routed_job, LONG).await;

    // Then: routing a Job at runtime creates a route, never a governable aggregate — the Job is
    // accepted, yet the key surfaces on neither the fleet read nor the published catalog
    assert!(
        gql::fleet_of(&client, admin, NEVER_ANNOUNCED_KEY)
            .await
            .is_empty(),
        "accepting a Job for an unannounced key must not register a RunnerType an administrator \
         could then deprecate or retire",
    );
    assert!(
        catalog::entry(fixture.fabric(), NEVER_ANNOUNCED_KEY)
            .await
            .is_none(),
        "an unannounced routing key is never published to the catalog",
    );

    // When: the administrator governs the migrated aggregate through the real mutation
    let mut watch = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::fleet_changed(PRESENCE_BACKED_KEY),
    )
    .await;
    stream::snapshot(&mut watch, FLEET_CHANGED, SHORT).await;
    gql::expect_success(
        &gql::deprecate_runner_type(&client, admin, PRESENCE_BACKED_KEY).await,
        wire::FIELD_DEPRECATE_RUNNER_TYPE,
        "deprecating a runner type the migration backfilled",
    );

    // Then: a migrated aggregate is governable on every channel — the delta is pushed, the read
    // follows, and the published entry is rewritten in place
    let deprecated = stream::await_fleet_event(&mut watch, wire::KIND_TYPE_DEPRECATED, LONG).await;
    assert_eq!(deprecated["runnerType"]["lifecycle"], json!("DEPRECATED"));
    assert_eq!(
        gql::fleet_of(&client, admin, PRESENCE_BACKED_KEY).await[0]["runnerType"]["lifecycle"],
        json!("DEPRECATED"),
    );
    catalog::wait_for_lifecycle(
        fixture.fabric(),
        PRESENCE_BACKED_KEY,
        PublishedLifecycle::Deprecated,
        LONG,
    )
    .await;

    events.stop().await;
    fixture.shutdown().await;
}

async fn apply_the_0_1_schema(pool: &PgPool) {
    let first = MIGRATOR
        .iter()
        .next()
        .expect("the 0.1 schema is the first declared migration");
    Migrator {
        migrations: Cow::Owned(vec![first.clone()]),
        ignore_missing: true,
        locking: MIGRATOR.locking,
        no_tx: MIGRATOR.no_tx,
    }
    .run(pool)
    .await
    .expect("the 0.1 schema applies through the real migrator");
}
