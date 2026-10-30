mod support;

use std::time::Duration;

use br_test_harness::{SseSubscription, verdict};
use serde_json::json;
use support::claim;
use support::events::EventLog;
use support::fixture::{self, JobsFixture, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{
    FLEET_CHANGED, JOB_CHANGED, LONG, QUIET, SHORT, catalog, delta, dispatch, docs, gql, stream,
    subs, wire,
};

const CLAIM_WAIT: Duration = Duration::from_secs(6);
const DEFAULT_MAX_RUN_SECONDS: u64 = 3;
const NINETY_SIX_HOURS_SECONDS: i64 = 96 * 3_600;
const PAST_THE_DEFAULT: Duration = Duration::from_secs(9);
const FORBIDDEN: &str = "FORBIDDEN";
const RESTARTED_DEFAULT_SECONDS: u64 = 5;
const STRANDED_AFTER_SECONDS: u64 = 8;
// The service refuses a reservation delay that is not above its longest claim wait.
const MAX_CLAIM_WAIT_SECONDS: u64 = 6;
const WITHIN_MAX_CLAIM_WAIT: Duration = Duration::from_secs(MAX_CLAIM_WAIT_SECONDS);
const BEFORE_THE_SWITCH: Duration = Duration::from_secs(4);
const SINCE_THE_SWITCH: Duration = Duration::from_secs(6);

#[tokio::test]
async fn switching_a_runner_type_to_claim_moves_its_new_runs_off_the_trigger_stream() {
    // Given: two PUSH types with live instances, one job already executing on the first
    let fixture = JobsFixture::start_with(Knobs {
        retry_base_delay_seconds: 1,
        ..Knobs::default()
    })
    .await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let switching = wire::unique_runner_type("switching");
    let untouched = wire::unique_runner_type("untouched");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &switching, "instance-a");
    let mut second = FakeRunner::new(fixture.nats(), &switching, "instance-b");
    let mut pusher = FakeRunner::new(fixture.nats(), &untouched, "pusher-a");
    instance.connect().await;
    second.connect().await;
    pusher.connect().await;
    dispatch::await_live_instances(&client, admin, &switching, 2, LONG).await;
    dispatch::await_live_instances(&client, admin, &untouched, 1, LONG).await;
    for runner_type in [&switching, &untouched] {
        let view = dispatch::runner_type_view(&client, admin, runner_type).await;
        assert_eq!(
            view["runnerType"]["dispatchMode"],
            json!(wire::MODE_PUSH),
            "a new runner type is PUSH",
        );
        assert_settings_offered(&view, "a new runner type read by a platform administrator");
    }

    let executing = JobDeclaration::new(&switching).with_max_attempts(2);
    producer.declare_v2(&executing).await;
    let first_trigger = instance.next_trigger(LONG).await;
    // Both instances compete on one trigger stream: the second reads on from the trigger already
    // consumed, so only triggers published from here on can reach it
    second.resume_after(&instance);
    assert_eq!(runner::job_id(&first_trigger), executing.job_id);
    instance.start_run(&first_trigger).await;
    events
        .expect_one(wire::FACT_STARTED, executing.job_id, LONG)
        .await;
    claim::claim(fixture.nats(), &second, Duration::from_secs(2))
        .await
        .expect_refused(wire::REASON_NOT_CLAIM_MODE, "a claim on a PUSH type");

    let mut fleet_watch = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::fleet_changed_with_dispatch(&switching),
    )
    .await;
    stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;
    let mut untouched_watch = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::fleet_changed_with_dispatch(&untouched),
    )
    .await;
    stream::snapshot(&mut untouched_watch, FLEET_CHANGED, SHORT).await;
    let published_before = catalog::entry(fixture.fabric(), &switching).await;

    // When: hostile callers try to switch it first
    for (caller, who) in [
        (fixture.member().clone(), "an ordinary member"),
        (
            fixture::impersonating_member(admin),
            "a member impersonating an administrator",
        ),
        (fixture::machine_caller(), "a machine passport"),
    ] {
        let refused =
            dispatch::set_dispatch_mode(&client, &caller, &switching, wire::MODE_CLAIM).await;
        assert_eq!(
            verdict::expect_code_shaped(&refused, &format!("{who} switching a dispatch mode")),
            FORBIDDEN,
        );
    }
    stream::expect_total_silence(&mut fleet_watch, "a refused switch moves nothing", QUIET).await;

    // When: a platform administrator switches the first type to CLAIM
    gql::expect_success(
        &dispatch::set_dispatch_mode(&client, admin, &switching, wire::MODE_CLAIM).await,
        wire::FIELD_SET_DISPATCH_MODE,
        "a platform administrator switches a PUSH type to CLAIM",
    );

    // Then: the fleet stream, the read and the affordances follow; the other type does not
    let switched = stream::await_fleet_event(
        &mut fleet_watch,
        wire::KIND_TYPE_DISPATCH_MODE_CHANGED,
        LONG,
    )
    .await;
    assert_eq!(
        delta::fleet_projection(&switched, &switching)["dispatchMode"],
        json!(wire::MODE_CLAIM)
    );
    assert_eq!(
        delta::fleet_projection(&switched, &switching)["lifecycle"],
        json!("ACTIVE"),
        "the dispatch mode is independent of the lifecycle",
    );
    gql::assert_allowed(&switched, wire::ACTION_DEPRECATE);
    assert_settings_offered(&switched, "the fleet delta of a switched runner type");
    assert_eq!(
        dispatch::runner_type_view(&client, admin, &switching).await["runnerType"]["dispatchMode"],
        json!(wire::MODE_CLAIM),
    );
    assert_eq!(
        dispatch::runner_type_view(&client, admin, &untouched).await["runnerType"]["dispatchMode"],
        json!(wire::MODE_PUSH),
    );
    stream::expect_total_silence(
        &mut untouched_watch,
        "another type's switch never reaches this type",
        QUIET,
    )
    .await;
    assert_eq!(
        catalog::entry(fixture.fabric(), &switching).await,
        published_before,
        "the published catalog entry carries the lifecycle only and is not rewritten",
    );

    // When: the executing run fails transiently after the switch, so its retry is dispatched afterwards
    instance
        .fail_run(&first_trigger, "TRANSIENT", "provider_busy", None)
        .await;

    // Then: the retry is served by a claim, never published on the trigger stream
    let retried = claim::claim(fixture.nats(), &instance, CLAIM_WAIT)
        .await
        .expect_run("a claim after the switch");
    assert_eq!(runner::job_id(&retried), executing.job_id);
    assert_eq!(runner::attempt_number(&retried), 2);

    // When: a new job is created on the switched type
    let fresh = JobDeclaration::new(&switching);
    producer.declare_v2(&fresh).await;
    events
        .expect_one(wire::FACT_QUEUED, fresh.job_id, LONG)
        .await;

    // Then: no trigger is published for it; a claim serves it
    instance.expect_no_trigger(QUIET).await;
    second.expect_no_trigger(QUIET).await;
    let served = claim::claim(fixture.nats(), &second, CLAIM_WAIT)
        .await
        .expect_run("a claim on the switched type");
    assert_eq!(runner::job_id(&served), fresh.job_id);

    // Then: the PUSH type beside it still dispatches by trigger, and refuses claims
    let pushed = JobDeclaration::new(&untouched);
    producer.declare_v2(&pushed).await;
    assert_eq!(
        runner::job_id(&pusher.next_trigger(LONG).await),
        pushed.job_id
    );
    claim::claim(fixture.nats(), &pusher, Duration::from_secs(2))
        .await
        .expect_refused(
            wire::REASON_NOT_CLAIM_MODE,
            "a claim on the untouched PUSH type",
        );

    // When: the administrator names a runner type that does not exist
    let unknown = dispatch::set_dispatch_mode(
        &client,
        admin,
        &wire::unique_runner_type("unknown"),
        wire::MODE_CLAIM,
    )
    .await;
    assert_eq!(
        gql::mutation_error_reason(&unknown, "switching an unknown runner type"),
        wire::REASON_RUNNER_TYPE_NOT_FOUND,
    );

    events.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn each_runner_type_bounds_its_runs_by_its_own_maximum_duration() {
    // Given: a service default maximum run duration of three seconds standing for the 72 h
    // default, and two PUSH types with live instances
    let fixture = JobsFixture::start_with(Knobs {
        run_max_duration_seconds: DEFAULT_MAX_RUN_SECONDS,
        backstop_interval_seconds: 1,
        ..Knobs::default()
    })
    .await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let long_type = wire::unique_runner_type("long");
    let default_type = wire::unique_runner_type("bounded");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut long_instance = FakeRunner::new(fixture.nats(), &long_type, "long-a");
    let mut default_instance = FakeRunner::new(fixture.nats(), &default_type, "bounded-a");
    long_instance.connect().await;
    default_instance.connect().await;
    dispatch::await_live_instances(&client, admin, &long_type, 1, LONG).await;
    dispatch::await_live_instances(&client, admin, &default_type, 1, LONG).await;
    for runner_type in [&long_type, &default_type] {
        let view = dispatch::runner_type_view(&client, admin, runner_type).await;
        assert_eq!(
            view["runnerType"]["maxRunDurationSeconds"],
            json!(DEFAULT_MAX_RUN_SECONDS),
            "a new runner type carries the service's default maximum duration",
        );
        assert_settings_offered(&view, "a new runner type read by a platform administrator");
    }
    let mut fleet_watch = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::fleet_changed_with_dispatch(&long_type),
    )
    .await;
    stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;

    // When: a member, or an impossible duration, tries to set the maximum
    let refused = dispatch::set_max_run_duration(
        &client,
        fixture.member(),
        &long_type,
        NINETY_SIX_HOURS_SECONDS,
    )
    .await;
    assert_eq!(
        verdict::expect_code_shaped(&refused, "a member setting a maximum duration"),
        FORBIDDEN,
    );
    for seconds in [0, -5] {
        let invalid = dispatch::set_max_run_duration(&client, admin, &long_type, seconds).await;
        assert_eq!(
            gql::mutation_error_reason(&invalid, "a non-positive maximum duration"),
            wire::REASON_INVALID_DURATION,
        );
    }
    stream::expect_total_silence(&mut fleet_watch, "a refused setting moves nothing", QUIET).await;

    // When: a platform administrator sets the first type to 96 hours
    gql::expect_success(
        &dispatch::set_max_run_duration(&client, admin, &long_type, NINETY_SIX_HOURS_SECONDS).await,
        wire::FIELD_SET_MAX_RUN_DURATION,
        "a platform administrator sets a runner type's maximum run duration",
    );
    let changed = stream::await_fleet_event(
        &mut fleet_watch,
        wire::KIND_TYPE_MAX_RUN_DURATION_CHANGED,
        LONG,
    )
    .await;
    assert_eq!(
        delta::fleet_projection(&changed, &long_type)["maxRunDurationSeconds"],
        json!(NINETY_SIX_HOURS_SECONDS)
    );
    assert_settings_offered(&changed, "the fleet delta of a re-bounded runner type");
    assert_eq!(
        dispatch::runner_type_view(&client, admin, &default_type).await["runnerType"]["maxRunDurationSeconds"],
        json!(DEFAULT_MAX_RUN_SECONDS),
        "the other type keeps the default",
    );

    // When: a run starts on each type
    let long_job = JobDeclaration::new(&long_type).with_max_attempts(1);
    let bounded_job = JobDeclaration::new(&default_type).with_max_attempts(1);
    producer.declare_v2(&long_job).await;
    producer.declare_v2(&bounded_job).await;
    let long_trigger = long_instance.next_trigger(LONG).await;
    let bounded_trigger = default_instance.next_trigger(LONG).await;
    let long_run = runner::run_id(&long_trigger);
    let bounded_run = runner::run_id(&bounded_trigger);
    let mut long_watch = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::job_changed_with_priority(long_job.job_id),
    )
    .await;
    stream::snapshot(&mut long_watch, JOB_CHANGED, SHORT).await;
    let mut bounded_watch = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::job_changed_with_priority(bounded_job.job_id),
    )
    .await;
    stream::snapshot(&mut bounded_watch, JOB_CHANGED, SHORT).await;
    long_instance.start_run(&long_trigger).await;
    default_instance.start_run(&bounded_trigger).await;

    // Then: the run on the type left at the default fails at the default, as transient
    default_instance.await_cancel_entry(bounded_run, LONG).await;
    let overrun =
        stream::await_delta(&mut bounded_watch, JOB_CHANGED, wire::EVT_RUN_FAILED, LONG).await;
    let event = delta::event_of(&overrun, wire::EVT_RUN_FAILED, bounded_job.job_id);
    assert_eq!(event["runId"], json!(bounded_run.to_string()));
    assert_eq!(event["failureKind"], json!("TRANSIENT"));
    gql::wait_for_status(&client, admin, bounded_job.job_id, "FAILED", LONG).await;

    // Then: the run on the 96-hour type is still alive well past the default
    stream::expect_no_delta(
        &mut long_watch,
        JOB_CHANGED,
        wire::EVT_RUN_FAILED,
        PAST_THE_DEFAULT,
    )
    .await;
    let alive = gql::job(&client, admin, long_job.job_id).await;
    assert_eq!(alive["status"], json!("IN_PROGRESS"));
    assert_eq!(alive["activeRunId"], json!(long_run.to_string()));
    long_instance.expect_no_cancel_entry(long_run, QUIET).await;
    events
        .expect_none(wire::FACT_FAILED, long_job.job_id, QUIET)
        .await;

    // When: the long run completes on its own
    long_instance.complete_run(&long_trigger).await;
    producer.finish(long_job.job_id).await;
    events
        .expect_one(wire::FACT_COMPLETED, long_job.job_id, LONG)
        .await;

    events.stop().await;
    fixture.shutdown().await;
}

fn assert_settings_offered(view: &serde_json::Value, what: &str) {
    for action in [
        wire::ACTION_SET_DISPATCH_MODE,
        wire::ACTION_SET_MAX_RUN_DURATION,
    ] {
        let entry = gql::affordance(view, action);
        assert_eq!(
            entry["allowed"],
            json!(true),
            "{what}: a platform administrator may set '{action}' at any time: {entry}"
        );
    }
}

#[tokio::test]
async fn a_trigger_stranded_by_a_switch_to_claim_is_cancelled_and_its_job_served_by_a_claim() {
    // Given: a reservation delay clearly longer than every pause of this scenario, a PUSH type
    // whose only instance never consumes its trigger stream, and a job whose trigger is published
    let fixture = JobsFixture::start_with(Knobs {
        claim_reservation_seconds: STRANDED_AFTER_SECONDS,
        claim_max_wait_seconds: MAX_CLAIM_WAIT_SECONDS,
        backstop_interval_seconds: 1,
        retry_base_delay_seconds: 1,
        ..Knobs::default()
    })
    .await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("stranded");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;
    dispatch::await_live_instances(&client, admin, &runner_type, 1, LONG).await;
    let declaration = JobDeclaration::new(&runner_type).with_max_attempts(2);
    let job_id = declaration.job_id;
    producer.declare_v2(&declaration).await;
    let published = instance.next_trigger(LONG).await;
    let stranded_run = runner::run_id(&published);
    assert_eq!(runner::job_id(&published), job_id);
    let mut watch = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::job_changed_with_priority(job_id),
    )
    .await;
    stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;
    let attempts_before = gql::job(&client, admin, job_id).await["attemptCount"].clone();

    // When: some time after the dispatch, the type is switched to CLAIM with that trigger still
    // unconsumed
    tokio::time::sleep(BEFORE_THE_SWITCH).await;
    dispatch::switch_to_claim(&client, admin, &runner_type).await;

    // Then: the reservation delay counts from the switch — nothing happens to the stranded run
    // before it has elapsed since the switch
    stream::expect_no_delta(
        &mut watch,
        JOB_CHANGED,
        wire::EVT_RUN_CANCELLED,
        SINCE_THE_SWITCH,
    )
    .await;

    // Then: the stranded run is then cancelled, reason trigger_stranded — not failed
    let cancelled = stream::await_delta_of(
        &mut watch,
        JOB_CHANGED,
        wire::EVT_RUN_CANCELLED,
        job_id,
        LONG,
    )
    .await;
    assert_eq!(
        delta::event_of(&cancelled, wire::EVT_RUN_CANCELLED, job_id)["runId"],
        json!(stranded_run.to_string())
    );
    let job = run_cancellation_view(&client, admin, job_id).await;
    let run = job["runs"]
        .as_array()
        .and_then(|runs| {
            runs.iter()
                .find(|run| run["id"] == json!(stranded_run.to_string()))
                .cloned()
        })
        .unwrap_or_else(|| panic!("the stranded run stays in the job's history: {job}"));
    assert_eq!(run["status"], json!("CANCELLED"));
    assert_eq!(
        run["cancellationReason"],
        json!(wire::REASON_TRIGGER_STRANDED)
    );
    assert_eq!(
        job["attemptCount"], attempts_before,
        "a stranded trigger costs the job no attempt",
    );
    stream::expect_no_delta(&mut watch, JOB_CHANGED, wire::EVT_RUN_FAILED, QUIET).await;
    for fact in [wire::FACT_FAILED, wire::FACT_CANCELLED, wire::FACT_STARTED] {
        events.expect_none(fact, job_id, QUIET).await;
    }

    // When: the instance claims, and starts what it is served at once
    let served = claim::claim_until_served(
        fixture.nats(),
        &instance,
        WITHIN_MAX_CLAIM_WAIT,
        LONG,
        "a claim after the stranded trigger was cancelled",
    )
    .await;
    instance.start_run(&served).await;

    // Then: the job is served by the claim, on a new run, never by a new trigger
    assert_eq!(runner::job_id(&served), job_id);
    assert_ne!(runner::run_id(&served), stranded_run);
    assert_eq!(
        runner::attempt_number(&served),
        runner::attempt_number(&published),
        "the claimed run carries the stranded run's attempt number: a stranded trigger costs no \
         attempt",
    );
    instance.expect_no_trigger(QUIET).await;
    instance.complete_run(&served).await;
    producer.finish(job_id).await;
    events.expect_one(wire::FACT_COMPLETED, job_id, LONG).await;

    events.stop().await;
    fixture.shutdown().await;
}

async fn run_cancellation_view(
    client: &br_test_harness::GraphqlClient,
    admin: &br_core_auth::Passport,
    job_id: uuid::Uuid,
) -> serde_json::Value {
    let response = client
        .query(
            admin,
            docs::JOB_RUN_CANCELLATION,
            json!({ "id": job_id.to_string() }),
        )
        .await;
    gql::data_of(&response, "jobsJob")["job"].clone()
}

#[tokio::test]
async fn the_dispatch_mode_of_each_runner_type_is_published_and_reconciled_at_boot() {
    // Given: two registered PUSH types and their published catalog entries
    let mut fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let admin = fixture.admin().clone();
    let switched = wire::unique_runner_type("published");
    let restored = wire::unique_runner_type("restored");
    let mut switched_instance = FakeRunner::new(fixture.nats(), &switched, "instance-a");
    let mut restored_instance = FakeRunner::new(fixture.nats(), &restored, "instance-a");
    switched_instance.connect().await;
    restored_instance.connect().await;
    let mut catalog_bytes = Vec::new();
    for runner_type in [&switched, &restored] {
        dispatch::await_live_instances(&client, &admin, runner_type, 1, LONG).await;
        catalog::wait_for_lifecycle(
            fixture.fabric(),
            runner_type,
            contract_jobs::catalog::RunnerTypeLifecycle::Active,
            LONG,
        )
        .await;
        catalog_bytes.push(
            catalog::runner_type_raw(fixture.fabric(), runner_type)
                .await
                .expect("a registered type has its catalog entry"),
        );
    }

    // When: a platform administrator switches the first type to CLAIM, then back to PUSH
    dispatch::switch_to_claim(&client, &admin, &switched).await;

    // Then: its dispatch mode is published under jobs.dispatch_mode.{runner_type}
    catalog::wait_for_dispatch_mode(fixture.fabric(), &switched, wire::MODE_CLAIM, LONG).await;
    gql::expect_success(
        &dispatch::set_dispatch_mode(&client, &admin, &switched, wire::MODE_PUSH).await,
        wire::FIELD_SET_DISPATCH_MODE,
        "a platform administrator switches the type back to PUSH",
    );
    catalog::wait_for_dispatch_mode(fixture.fabric(), &switched, wire::MODE_PUSH, LONG).await;

    // Then: the catalog entry is untouched, byte for byte
    assert_eq!(
        catalog::runner_type_raw(fixture.fabric(), &switched).await,
        Some(catalog_bytes[0].clone()),
        "the jobs.runner_type. value is a sealed offer: the dispatch mode never rides on it",
    );

    // Given: the second type switched to CLAIM; then, behind the service's back, the first
    // type's published mode goes stale and the second type's goes missing
    dispatch::switch_to_claim(&client, &admin, &restored).await;
    catalog::wait_for_dispatch_mode(fixture.fabric(), &restored, wire::MODE_CLAIM, LONG).await;
    catalog::put_dispatch_mode(fixture.fabric(), &switched, wire::MODE_CLAIM).await;
    catalog::retract_dispatch_mode(fixture.fabric(), &restored).await;

    // When: the service restarts
    fixture.restart_first(&Knobs::default()).await;

    // Then: boot reconciles both published modes with the ledger, and the catalog entries are
    // still the bytes they were
    catalog::wait_for_dispatch_mode(fixture.fabric(), &switched, wire::MODE_PUSH, LONG).await;
    catalog::wait_for_dispatch_mode(fixture.fabric(), &restored, wire::MODE_CLAIM, LONG).await;
    for (runner_type, bytes) in [&switched, &restored].into_iter().zip(&catalog_bytes) {
        assert_eq!(
            catalog::runner_type_raw(fixture.fabric(), runner_type).await,
            Some(bytes.clone()),
            "the catalog entry of '{runner_type}' is unchanged byte for byte",
        );
    }

    fixture.shutdown().await;
}

#[tokio::test]
async fn clearing_a_runner_types_maximum_returns_it_to_the_service_default() {
    // Given: a service default maximum run duration of three seconds, and a type set to 96 hours
    let mut fixture = JobsFixture::start_with(Knobs {
        run_max_duration_seconds: DEFAULT_MAX_RUN_SECONDS,
        backstop_interval_seconds: 1,
        ..Knobs::default()
    })
    .await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin().clone();
    let member = fixture.member().clone();
    let runner_type = wire::unique_runner_type("cleared");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    instance.connect().await;
    dispatch::await_live_instances(&client, &admin, &runner_type, 1, LONG).await;
    assert_eq!(
        gql::assert_blocked(
            &dispatch::runner_type_view(&client, &admin, &runner_type).await,
            wire::ACTION_CLEAR_MAX_RUN_DURATION,
        ),
        wire::REASON_NO_MAX_RUN_DURATION_SET,
        "a type on the service default has no maximum of its own to clear",
    );
    gql::expect_success(
        &dispatch::set_max_run_duration(&client, &admin, &runner_type, NINETY_SIX_HOURS_SECONDS)
            .await,
        wire::FIELD_SET_MAX_RUN_DURATION,
        "a platform administrator sets the type to 96 hours",
    );
    let before = dispatch::runner_type_view(&client, &admin, &runner_type).await;
    assert_eq!(
        before["runnerType"]["maxRunDurationSeconds"],
        json!(NINETY_SIX_HOURS_SECONDS)
    );
    gql::assert_allowed(&before, wire::ACTION_CLEAR_MAX_RUN_DURATION);
    let mut fleet_watch = SseSubscription::open(
        fixture.url(),
        &admin,
        &subs::fleet_changed_with_dispatch(&runner_type),
    )
    .await;
    stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;

    // When: callers who are not platform administrators try to clear it
    for (caller, who) in [
        (member.clone(), "an ordinary member"),
        (
            fixture::impersonating_member(&admin),
            "a member impersonating an administrator",
        ),
        (fixture::machine_caller(), "a machine passport"),
    ] {
        let refused = dispatch::clear_max_run_duration(&client, &caller, &runner_type).await;
        assert_eq!(
            verdict::expect_code_shaped(&refused, &format!("{who} clearing a maximum")),
            FORBIDDEN,
        );
    }

    // Then: nothing moved
    stream::expect_total_silence(&mut fleet_watch, "a refused clear moves nothing", QUIET).await;
    assert_eq!(
        dispatch::runner_type_view(&client, &admin, &runner_type).await,
        before
    );

    // When: a platform administrator clears it, then names a type that does not exist
    gql::expect_success(
        &dispatch::clear_max_run_duration(&client, &admin, &runner_type).await,
        wire::FIELD_CLEAR_MAX_RUN_DURATION,
        "a platform administrator clears the type's maximum",
    );
    let unknown =
        dispatch::clear_max_run_duration(&client, &admin, &wire::unique_runner_type("unknown"))
            .await;
    assert_eq!(
        gql::mutation_error_reason(&unknown, "clearing an unknown runner type"),
        wire::REASON_RUNNER_TYPE_NOT_FOUND,
    );

    // Then: the type is back on the service default, on the stream and the read
    let changed = stream::await_fleet_event(
        &mut fleet_watch,
        wire::KIND_TYPE_MAX_RUN_DURATION_CHANGED,
        LONG,
    )
    .await;
    assert_eq!(
        delta::fleet_projection(&changed, &runner_type)["maxRunDurationSeconds"],
        json!(DEFAULT_MAX_RUN_SECONDS)
    );
    assert_eq!(
        dispatch::runner_type_view(&client, &admin, &runner_type).await["runnerType"]["maxRunDurationSeconds"],
        json!(DEFAULT_MAX_RUN_SECONDS)
    );

    assert_eq!(
        gql::assert_blocked(
            &dispatch::runner_type_view(&client, &admin, &runner_type).await,
            wire::ACTION_CLEAR_MAX_RUN_DURATION,
        ),
        wire::REASON_NO_MAX_RUN_DURATION_SET,
        "once cleared, nothing is left to clear",
    );

    // Then: a run dispatched afterwards is bounded by the service default
    let declaration = JobDeclaration::new(&runner_type).with_max_attempts(1);
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
    let overrun =
        stream::await_delta_of(&mut watch, JOB_CHANGED, wire::EVT_RUN_FAILED, job_id, LONG).await;
    assert_eq!(
        delta::event_of(&overrun, wire::EVT_RUN_FAILED, job_id)["failureKind"],
        json!("TRANSIENT")
    );

    // When: the service restarts with another default
    fixture
        .restart_first(&Knobs {
            run_max_duration_seconds: RESTARTED_DEFAULT_SECONDS,
            backstop_interval_seconds: 1,
            ..Knobs::default()
        })
        .await;

    // Then: a cleared type follows the service default, whatever it becomes
    assert_eq!(
        dispatch::runner_type_view(&fixture.gql(), &admin, &runner_type).await["runnerType"]["maxRunDurationSeconds"],
        json!(RESTARTED_DEFAULT_SECONDS)
    );

    events.stop().await;
    fixture.shutdown().await;
}
