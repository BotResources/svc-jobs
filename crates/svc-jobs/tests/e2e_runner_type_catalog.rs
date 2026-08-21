mod support;

use br_core_auth::PassportHeader;
use br_test_harness::{SseSubscription, verdict, wait_until};
use chrono::Utc;
use contract_jobs::catalog::RunnerTypeLifecycle as PublishedLifecycle;
use serde_json::{Value, json};
use support::adversary::{self, RunnerTypeLifecycleAttack};
use support::events::EventLog;
use support::fixture::{self, JobsFixture, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::FakeRunner;
use support::{FLEET_CHANGED, LONG, QUIET, SHORT, catalog, delta, docs, gql, stream, subs, wire};
use uuid::Uuid;

const RETIREMENT_WINDOW_SECONDS: u64 = 20;
const RETIREMENT_REOPENING: std::time::Duration = std::time::Duration::from_secs(45);

#[tokio::test]
async fn an_administrator_governs_a_runner_type_through_its_durable_lifecycle() {
    // Given: an unknown runner type watched before its first presence signal
    let mut fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let member = fixture.member();
    let runner_type = wire::unique_runner_type("catalog");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    let producer = Producer::new(fixture.fabric(), "projects");

    let mut fleet_watch =
        SseSubscription::open(fixture.url(), admin, &subs::fleet_changed(&runner_type)).await;
    let opening = stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;
    assert_eq!(
        opening["runnerTypes"],
        json!([]),
        "a type is absent until presence first registers it: {opening}",
    );
    assert!(
        catalog::entry(fixture.fabric(), &runner_type)
            .await
            .is_none(),
        "Published Language starts without a never-seen type",
    );

    // When: one instance of the previously unknown type announces presence
    let mut cursors: Vec<Value> = Vec::new();
    instance.connect().await;
    let registered =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_TYPE_REGISTERED, LONG).await;
    cursors.push(registered["cursor"].clone());
    assert_lifecycle(&registered, "ACTIVE");
    assert_active_actions(&registered, true);
    let connected =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_INSTANCE_CONNECTED, LONG).await;
    cursors.push(connected["cursor"].clone());
    assert_lifecycle(&connected, "ACTIVE");
    assert_active_actions(&connected, true);

    // Then: the read, affordances and Published Language agree on one ACTIVE aggregate
    let active_view = fleet_view(&fixture, &runner_type).await;
    assert_lifecycle(&active_view, "ACTIVE");
    assert_active_actions(&active_view, true);
    assert_same_action_surface(&connected, &active_view);
    let published_active = catalog::wait_for_lifecycle(
        fixture.fabric(),
        &runner_type,
        PublishedLifecycle::Active,
        LONG,
    )
    .await;
    assert_eq!(published_active.runner_type, runner_type);

    // When: every caller who is not a platform administrator tries to observe the known runner
    // type through both read transports
    let mut member_read_code = String::new();
    for (caller, who) in [
        (member.clone(), "an ordinary member"),
        (
            fixture::impersonating_member(admin),
            "a member impersonating an administrator",
        ),
        (fixture::machine_caller(), "a machine passport"),
    ] {
        let refused = client
            .query(&caller, docs::FLEET, json!({ "runnerType": &runner_type }))
            .await;
        let code =
            verdict::expect_code_shaped(&refused, &format!("{who} reading the runner-type fleet"));
        assert_eq!(code, "FORBIDDEN");
        assert!(
            !refused.to_string().contains(&runner_type),
            "a refused read leaks neither the runner-type identity nor its affordances to {who}: \
             {refused}",
        );
        member_read_code = code;
    }
    let member_header = member.to_header();
    let (subscription_status, subscription_body) = client
        .post_raw(
            "/graphql",
            &[
                ("X-Passport", member_header.as_str()),
                ("Accept", "text/event-stream"),
            ],
            json!({ "query": subs::fleet_changed(&runner_type) }),
        )
        .await;
    assert!(
        !subscription_status.is_success(),
        "an ordinary member must be refused during subscription establishment: {subscription_body}",
    );
    let member_subscription_code = refusal_code(
        &subscription_body,
        "an ordinary member subscribing to the runner-type fleet",
    );
    assert_eq!(member_subscription_code, member_read_code);
    assert!(
        !subscription_body.to_string().contains(&runner_type),
        "a refused subscription leaks no RunnerType data: {subscription_body}",
    );

    // When: an ordinary organization member attacks the lifecycle action, for a known and an
    // unknown type alike
    let unknown = wire::unique_runner_type("catalog-unknown");
    let member_known = gql::deprecate_runner_type(&client, member, &runner_type).await;
    let member_unknown = gql::deprecate_runner_type(&client, member, &unknown).await;
    let known_code = verdict::expect_code_shaped(
        &member_known,
        "an ordinary member deprecating a known runner type",
    );
    let unknown_code = verdict::expect_code_shaped(
        &member_unknown,
        "an ordinary member probing an unknown runner type",
    );
    assert_eq!(
        known_code, unknown_code,
        "authorization runs before lookup, so the refusal leaks no runner-type existence",
    );
    assert_eq!(known_code, "FORBIDDEN");
    stream::expect_total_silence(
        &mut fleet_watch,
        "a refused member changes no fleet view",
        QUIET,
    )
    .await;
    assert_eq!(fleet_view(&fixture, &runner_type).await, active_view);
    assert_eq!(
        catalog::entry(fixture.fabric(), &runner_type).await,
        Some(published_active.clone()),
        "a refused action changes no Published Language state",
    );

    // When: the platform administrator deprecates the active type
    let deprecated_ack = gql::deprecate_runner_type(&client, admin, &runner_type).await;
    gql::expect_success(
        &deprecated_ack,
        wire::FIELD_DEPRECATE_RUNNER_TYPE,
        "deprecating an active runner type",
    );

    // Then: one typed delta carries DEPRECATED plus the new backend-owned decisions, and the KV
    // entry is updated in place
    let deprecated =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_TYPE_DEPRECATED, LONG).await;
    cursors.push(deprecated["cursor"].clone());
    assert_lifecycle(&deprecated, "DEPRECATED");
    assert_deprecated_actions(&deprecated, true, None);
    let deprecated_view = fleet_view(&fixture, &runner_type).await;
    assert_lifecycle(&deprecated_view, "DEPRECATED");
    assert_deprecated_actions(&deprecated_view, true, None);
    assert_same_action_surface(&deprecated, &deprecated_view);
    let published_deprecated = catalog::wait_for_lifecycle(
        fixture.fabric(),
        &runner_type,
        PublishedLifecycle::Deprecated,
        LONG,
    )
    .await;

    // When: an ordinary member attacks both actions the administrator is now offered, against a
    // known and an unknown type alike
    assert_member_action_forbidden(
        gql::reactivate_runner_type(&client, member, &runner_type).await,
        gql::reactivate_runner_type(&client, member, &unknown).await,
        "reactivating",
    );
    assert_member_action_forbidden(
        gql::retire_runner_type(&client, member, &runner_type).await,
        gql::retire_runner_type(&client, member, &unknown).await,
        "retiring",
    );
    stream::expect_total_silence(
        &mut fleet_watch,
        "refused member actions push no lifecycle delta",
        QUIET,
    )
    .await;
    assert_eq!(fleet_view(&fixture, &runner_type).await, deprecated_view);
    assert_eq!(
        catalog::entry(fixture.fabric(), &runner_type).await,
        Some(published_deprecated.clone()),
        "refused member actions change no Published Language state",
    );

    // When: every other caller who is not a platform administrator attacks the three lifecycle
    // mutations — an impersonated member, a machine passport, no passport, an undecodable one
    adversary::assert_no_hostile_caller_governs_a_runner_type(
        &RunnerTypeLifecycleAttack {
            client: &client,
            admin,
            member,
            fabric: fixture.fabric(),
            runner_type: &runner_type,
            unknown_runner_type: &unknown,
        },
        &mut fleet_watch,
    )
    .await;

    // When: deprecation is repeated in a lifecycle that already blocks it
    let before_repeat = fleet_view(&fixture, &runner_type).await;
    let repeated = gql::deprecate_runner_type(&client, admin, &runner_type).await;
    assert_invalid_state(&repeated, "deprecating an already deprecated runner type");
    let repeated_reason = gql::mutation_error_reason(&repeated, "repeated deprecation");
    assert_eq!(
        repeated_reason,
        gql::assert_blocked(&before_repeat, wire::ACTION_DEPRECATE),
        "the mutation guard and its affordance are the same decision",
    );
    stream::expect_total_silence(
        &mut fleet_watch,
        "a refused lifecycle transition pushes no delta",
        QUIET,
    )
    .await;
    assert_eq!(fleet_view(&fixture, &runner_type).await, before_repeat);
    assert_eq!(
        catalog::entry(fixture.fabric(), &runner_type).await,
        Some(published_deprecated.clone()),
        "a refused transition never rewrites the catalog",
    );

    // Given: the deprecated type is live but currently serves no job
    // When: its instance leaves and a producer declares new work for it
    instance.disconnect().await;
    stream::await_fleet_event(&mut fleet_watch, wire::KIND_INSTANCE_DISCONNECTED, LONG).await;
    let declaration = JobDeclaration::new(&runner_type).with_max_attempts(1);
    let waiting_job = declaration.job_id;
    producer.declare(&declaration).await;
    events
        .expect_one(wire::FACT_QUEUED, waiting_job, LONG)
        .await;

    // Then: deprecation is a warning, not a creation ban; the fleet delta carries the changed
    // retirement affordance without a client-side inference
    let waiting =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_BEGAN_WAITING, LONG).await;
    cursors.push(waiting["cursor"].clone());
    assert_lifecycle(&waiting, "DEPRECATED");
    assert_eq!(
        gql::assert_blocked(&waiting, wire::ACTION_DISPATCH),
        "runner_type_unavailable",
        "job acceptance remains allowed while dispatch waits for a live instance",
    );
    assert_eq!(
        gql::assert_blocked(&waiting, wire::ACTION_DEPRECATE),
        "runner_type_not_active",
    );
    gql::assert_allowed(&waiting, wire::ACTION_REACTIVATE);
    assert_eq!(
        gql::assert_blocked(&waiting, wire::ACTION_RETIRE),
        "runner_type_has_non_terminal_jobs",
    );
    assert_eq!(
        gql::affordance(&waiting, wire::ACTION_RETIRE)["params"]["count"],
        json!("1"),
        "the live affordance tells the client exactly what blocks retirement: {waiting}",
    );

    // When: the administrator nevertheless tries to retire it
    let before_refusal = fleet_view(&fixture, &runner_type).await;
    let retire_refusal = gql::retire_runner_type(&client, admin, &runner_type).await;
    assert_invalid_state(&retire_refusal, "retiring a type with a non-terminal job");
    assert_eq!(
        gql::mutation_error_reason(&retire_refusal, "retirement with live work"),
        gql::assert_blocked(&before_refusal, wire::ACTION_RETIRE),
    );
    assert_eq!(
        gql::mutation_error_params(&retire_refusal, "retirement with live work")["count"],
        gql::affordance(&before_refusal, wire::ACTION_RETIRE)["params"]["count"],
        "mutation and affordance carry the same structured blocking fact",
    );
    stream::expect_total_silence(
        &mut fleet_watch,
        "a refused retirement pushes no fleet delta",
        QUIET,
    )
    .await;
    assert_eq!(fleet_view(&fixture, &runner_type).await, before_refusal);
    assert_eq!(
        catalog::entry(fixture.fabric(), &runner_type).await,
        Some(published_deprecated),
        "a refused retirement leaves the deprecated entry intact",
    );

    // When: the administrator cancels the queued job, leaving no terminal Run inside the 24-hour
    // retirement quiet period, then retires the now-eligible type
    let cancelled = gql::cancel_job(&client, admin, Uuid::now_v7(), waiting_job).await;
    gql::expect_success(
        &cancelled,
        wire::FIELD_CANCEL_JOB,
        "cancelling the queued job that blocked retirement",
    );
    events
        .expect_one(wire::FACT_CANCELLED, waiting_job, LONG)
        .await;
    let stopped_waiting =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_STOPPED_WAITING, LONG).await;
    cursors.push(stopped_waiting["cursor"].clone());
    assert_deprecated_actions(&stopped_waiting, false, None);

    let retired_ack = gql::retire_runner_type(&client, admin, &runner_type).await;
    gql::expect_success(
        &retired_ack,
        wire::FIELD_RETIRE_RUNNER_TYPE,
        "retiring an eligible deprecated runner type",
    );
    let retired = stream::await_fleet_event(&mut fleet_watch, wire::KIND_TYPE_RETIRED, LONG).await;
    cursors.push(retired["cursor"].clone());
    assert_lifecycle(&retired, "RETIRED");
    assert_retired_actions(&retired, false);
    let retired_view = fleet_view(&fixture, &runner_type).await;
    assert_lifecycle(&retired_view, "RETIRED");
    assert_retired_actions(&retired_view, false);
    assert_same_action_surface(&retired, &retired_view);
    catalog::wait_until_absent(fixture.fabric(), &runner_type, LONG).await;

    // When: a producer targets the retired type
    let rejected = JobDeclaration::new(&runner_type);
    let rejected_id = rejected.job_id;
    producer.declare(&rejected).await;
    let rejection = events
        .expect_one(wire::FACT_CREATION_REJECTED, rejected_id, LONG)
        .await;
    assert_eq!(
        rejection.payload()["reason_code"],
        json!("runner_type_retired"),
        "retirement rejects new work under the same stable reason as dispatch",
    );
    assert!(
        gql::list_jobs(&client, admin, json!({ "runnerTypes": [&runner_type] }))
            .await
            .iter()
            .all(|view| view["job"]["id"] != json!(rejected_id.to_string())),
        "a rejected declaration leaves no orphaned Job",
    );
    stream::expect_total_silence(
        &mut fleet_watch,
        "a rejected creation moves no fleet projection",
        QUIET,
    )
    .await;

    // When: presence returns for the retired type
    instance.connect().await;
    let present_but_retired =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_INSTANCE_CONNECTED, LONG).await;
    cursors.push(present_but_retired["cursor"].clone());

    // Then: the instance is recorded, but presence cannot reactivate policy state
    let retired_projection = delta::fleet_projection(&present_but_retired, &runner_type);
    assert_eq!(retired_projection["lifecycle"], json!("RETIRED"));
    assert_eq!(retired_projection["isAvailable"], json!(false));
    let retired_instance =
        delta::assert_instance(&retired_projection, "instance-a", false, &[], "0.1.0");
    assert_eq!(retired_instance["reportedStatus"], json!("READY"));
    assert_eq!(retired_instance["capacity"], json!(1));
    assert_retired_actions(&present_but_retired, true);
    let present_retired_view = fleet_view(&fixture, &runner_type).await;
    assert_lifecycle(&present_retired_view, "RETIRED");
    assert_retired_actions(&present_retired_view, true);
    assert_same_action_surface(&present_but_retired, &present_retired_view);
    assert_eq!(
        present_retired_view["runnerType"]["instances"][0],
        retired_instance
    );
    assert!(
        catalog::entry(fixture.fabric(), &runner_type)
            .await
            .is_none(),
        "presence never recreates a retired catalog entry",
    );

    // Given: the Published Language bucket becomes unavailable after service boot
    catalog::make_unavailable(fixture.fabric()).await;

    // When: the administrator explicitly reactivates the retired type
    let reactivated_ack = gql::reactivate_runner_type(&client, admin, &runner_type).await;
    gql::expect_success(
        &reactivated_ack,
        wire::FIELD_REACTIVATE_RUNNER_TYPE,
        "reactivating a retired type with live presence",
    );
    let reactivated =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_TYPE_REACTIVATED, LONG).await;
    cursors.push(reactivated["cursor"].clone());
    assert_lifecycle(&reactivated, "ACTIVE");
    assert_active_actions(&reactivated, true);

    // Then: the cursors the subscriber received advance strictly, one position per transition
    assert_cursors_advance_strictly(&cursors);

    // Then: the KV projection failure cannot turn the already-committed lifecycle mutation into
    // an error or roll its canonical query/subscription state back
    let reactivated_view = fleet_view(&fixture, &runner_type).await;
    assert_lifecycle(&reactivated_view, "ACTIVE");
    assert_active_actions(&reactivated_view, true);
    assert_same_action_surface(&reactivated, &reactivated_view);
    assert!(
        !fixture.fabric().published_language_present().await,
        "the fault injection must keep Published Language unavailable during the mutation",
    );

    // Then: reconnecting starts from a fresh current snapshot, never a replayed transition or an
    // invalidation that requires a query
    drop(fleet_watch);
    let mut reconnected =
        SseSubscription::open(fixture.url(), admin, &subs::fleet_changed(&runner_type)).await;
    let reset = stream::snapshot(&mut reconnected, FLEET_CHANGED, SHORT).await;
    let reset_view = reset["runnerTypes"][0].clone();
    assert_lifecycle(&reset_view, "ACTIVE");
    assert_active_actions(&reset_view, true);
    stream::expect_total_silence(
        &mut reconnected,
        "the reset is current and not followed by duplicate history",
        QUIET,
    )
    .await;

    // Given: the declared bucket is restored empty while the authoritative ACTIVE aggregate
    // remains stored
    catalog::restore_empty_bucket(fixture.fabric()).await;
    assert!(
        catalog::entry(fixture.fabric(), &runner_type)
            .await
            .is_none(),
        "restoring infrastructure does not itself invent projection state",
    );

    // When: the service restarts on the same database and fabric
    fixture.restart_first(&Knobs::default()).await;

    // Then: startup healing recreates the current entry without changing the aggregate
    let healed = catalog::wait_for_lifecycle(
        fixture.fabric(),
        &runner_type,
        PublishedLifecycle::Active,
        LONG,
    )
    .await;
    assert_eq!(healed.runner_type, runner_type);
    assert_lifecycle(&fleet_view(&fixture, &runner_type).await, "ACTIVE");

    events.stop().await;
    fixture.shutdown().await;

    assert_recent_terminal_run_blocks_retirement().await;
    assert_startup_heals_deprecated_and_retired_catalog_drift().await;
}

async fn assert_recent_terminal_run_blocks_retirement() {
    // Given: a deprecated runner type whose live instance executes one real Run to completion
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("recent-terminal");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    let producer = Producer::new(fixture.fabric(), "projects");

    let mut setup_watch =
        SseSubscription::open(fixture.url(), admin, &subs::fleet_changed(&runner_type)).await;
    stream::snapshot(&mut setup_watch, FLEET_CHANGED, SHORT).await;
    instance.connect().await;
    stream::await_fleet_event(&mut setup_watch, wire::KIND_TYPE_REGISTERED, LONG).await;
    stream::await_fleet_event(&mut setup_watch, wire::KIND_INSTANCE_CONNECTED, LONG).await;
    gql::expect_success(
        &gql::deprecate_runner_type(&client, admin, &runner_type).await,
        wire::FIELD_DEPRECATE_RUNNER_TYPE,
        "deprecating the type before its run",
    );
    stream::await_fleet_event(&mut setup_watch, wire::KIND_TYPE_DEPRECATED, LONG).await;
    drop(setup_watch);
    catalog::wait_for_lifecycle(
        fixture.fabric(),
        &runner_type,
        PublishedLifecycle::Deprecated,
        LONG,
    )
    .await;

    let declaration = JobDeclaration::new(&runner_type).with_max_attempts(1);
    let job_id = declaration.job_id;
    producer.declare(&declaration).await;
    let trigger = instance.next_trigger(LONG).await;
    instance.start_run(&trigger).await;
    instance.complete_run(&trigger).await;
    let terminal_run_landed = wait_until(LONG, || async {
        gql::job(&client, admin, job_id).await["runs"][0]["status"] == json!("COMPLETED")
    })
    .await;
    assert!(
        terminal_run_landed,
        "the real terminal Run must be recorded before the owner settles its Job",
    );
    producer.finish(job_id).await;
    gql::wait_for_status(&client, admin, job_id, "COMPLETED", LONG).await;

    // Then: a fresh snapshot already blocks retirement with the exact eligible-at fact; no client
    // derives the 24-hour rule from the run timestamp
    let before = fleet_view(&fixture, &runner_type).await;
    assert_lifecycle(&before, "DEPRECATED");
    let blocked = gql::affordance(&before, wire::ACTION_RETIRE);
    assert_eq!(blocked["allowed"], json!(false));
    assert_eq!(
        blocked["reasonCode"],
        json!("runner_type_has_recent_terminal_runs"),
    );
    assert!(
        blocked["params"]["eligibleAt"]
            .as_str()
            .is_some_and(|value| !value.is_empty()),
        "the backend supplies the end of the 24-hour quiet period: {blocked}",
    );

    let mut watch =
        SseSubscription::open(fixture.url(), admin, &subs::fleet_changed(&runner_type)).await;
    let snapshot = stream::snapshot(&mut watch, FLEET_CHANGED, SHORT).await;
    assert_eq!(
        gql::affordance(&snapshot["runnerTypes"][0], wire::ACTION_RETIRE),
        blocked,
        "query and reconnect snapshot carry the same current decision",
    );

    // When: the administrator attempts retirement during the quiet period
    let refused = gql::retire_runner_type(&client, admin, &runner_type).await;
    assert_invalid_state(&refused, "retirement inside the terminal-run quiet period");

    // Then: enforcement, affordance, state, subscription and Published Language all agree that
    // nothing changed
    assert_eq!(
        gql::mutation_error_reason(&refused, "recent-terminal retirement"),
        blocked["reasonCode"],
    );
    assert_eq!(
        gql::mutation_error_params(&refused, "recent-terminal retirement")["eligibleAt"],
        blocked["params"]["eligibleAt"],
    );
    stream::expect_total_silence(
        &mut watch,
        "a quiet-period refusal pushes no lifecycle delta",
        QUIET,
    )
    .await;
    assert_eq!(fleet_view(&fixture, &runner_type).await, before);
    assert_eq!(
        catalog::entry(fixture.fabric(), &runner_type)
            .await
            .expect("the deprecated catalog entry remains")
            .lifecycle,
        PublishedLifecycle::Deprecated,
    );

    fixture.shutdown().await;
}

async fn assert_startup_heals_deprecated_and_retired_catalog_drift() {
    // Given: two durable types whose Published Language projections drift in opposite ways
    let mut fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let deprecated_type = wire::unique_runner_type("heal-deprecated");
    let retired_type = wire::unique_runner_type("heal-retired");
    let mut deprecated_instance =
        FakeRunner::new(fixture.nats(), &deprecated_type, "deprecated-instance");
    let mut retired_instance = FakeRunner::new(fixture.nats(), &retired_type, "retired-instance");

    deprecated_instance.connect().await;
    retired_instance.connect().await;
    let registered = wait_until(LONG, || async {
        gql::fleet_of(&client, admin, &deprecated_type).await.len() == 1
            && gql::fleet_of(&client, admin, &retired_type).await.len() == 1
    })
    .await;
    assert!(
        registered,
        "presence must durably register both healing fixtures"
    );

    gql::expect_success(
        &gql::deprecate_runner_type(&client, admin, &deprecated_type).await,
        wire::FIELD_DEPRECATE_RUNNER_TYPE,
        "deprecating the projection-missing type",
    );
    gql::expect_success(
        &gql::deprecate_runner_type(&client, admin, &retired_type).await,
        wire::FIELD_DEPRECATE_RUNNER_TYPE,
        "deprecating the projection-stale type",
    );
    catalog::wait_for_lifecycle(
        fixture.fabric(),
        &deprecated_type,
        PublishedLifecycle::Deprecated,
        LONG,
    )
    .await;
    catalog::wait_for_lifecycle(
        fixture.fabric(),
        &retired_type,
        PublishedLifecycle::Deprecated,
        LONG,
    )
    .await;

    gql::expect_success(
        &gql::retire_runner_type(&client, admin, &retired_type).await,
        wire::FIELD_RETIRE_RUNNER_TYPE,
        "retiring the type whose catalog entry will be made stale",
    );
    catalog::wait_until_absent(fixture.fabric(), &retired_type, LONG).await;
    catalog::retract(fixture.fabric(), &deprecated_type).await;
    catalog::put(fixture.fabric(), &retired_type, PublishedLifecycle::Active).await;
    assert!(
        catalog::entry(fixture.fabric(), &deprecated_type)
            .await
            .is_none(),
        "the DEPRECATED entry is deliberately missing before restart",
    );
    assert_eq!(
        catalog::entry(fixture.fabric(), &retired_type)
            .await
            .expect("the deliberately stale retired entry exists")
            .lifecycle,
        PublishedLifecycle::Active,
    );

    // When: the real service restarts from canonical Postgres state
    fixture.restart_first(&Knobs::default()).await;

    // Then: it fills the missing DEPRECATED entry and retracts stale RETIRED publication
    catalog::wait_for_lifecycle(
        fixture.fabric(),
        &deprecated_type,
        PublishedLifecycle::Deprecated,
        LONG,
    )
    .await;
    catalog::wait_until_absent(fixture.fabric(), &retired_type, LONG).await;
    assert_lifecycle(&fleet_view(&fixture, &deprecated_type).await, "DEPRECATED");
    assert_lifecycle(&fleet_view(&fixture, &retired_type).await, "RETIRED");

    fixture.shutdown().await;
}

#[tokio::test]
async fn the_configured_quiet_period_reopens_retirement_without_a_client_timer() {
    // Given: a service whose retirement quiet period is a handful of seconds instead of a day,
    // and a deprecated runner type with one live instance
    let fixture = JobsFixture::start_with(Knobs {
        retirement_quiet_period_seconds: RETIREMENT_WINDOW_SECONDS,
        runner_type_impact_interval_seconds: 1,
        ..Knobs::default()
    })
    .await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("quiet-period");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");
    let producer = Producer::new(fixture.fabric(), "projects");

    let mut watch =
        SseSubscription::open(fixture.url(), admin, &subs::fleet_changed(&runner_type)).await;
    stream::snapshot(&mut watch, FLEET_CHANGED, SHORT).await;
    instance.connect().await;
    stream::await_fleet_event(&mut watch, wire::KIND_TYPE_REGISTERED, LONG).await;
    gql::expect_success(
        &gql::deprecate_runner_type(&client, admin, &runner_type).await,
        wire::FIELD_DEPRECATE_RUNNER_TYPE,
        "deprecating the type before its last run",
    );
    stream::await_fleet_event(&mut watch, wire::KIND_TYPE_DEPRECATED, LONG).await;
    catalog::wait_for_lifecycle(
        fixture.fabric(),
        &runner_type,
        PublishedLifecycle::Deprecated,
        LONG,
    )
    .await;

    // When: one real Run executes to completion on the real runner transport and its owner settles
    // the Job, leaving the configured quiet period as the only thing blocking retirement
    let declaration = JobDeclaration::new(&runner_type).with_max_attempts(1);
    let job_id = declaration.job_id;
    producer.declare(&declaration).await;
    let trigger = instance.next_trigger(LONG).await;
    instance.start_run(&trigger).await;
    instance.complete_run(&trigger).await;
    producer.finish(job_id).await;
    gql::wait_for_status(&client, admin, job_id, "COMPLETED", LONG).await;

    // Then: retirement is blocked by the window alone, and the backend names the instant it opens
    let settled = fleet_view(&fixture, &runner_type).await;
    let blocked = gql::affordance(&settled, wire::ACTION_RETIRE);
    assert_eq!(
        blocked["reasonCode"],
        json!("runner_type_has_recent_terminal_runs"),
        "with its Job settled, the only thing left blocking retirement is the configured quiet \
         period: {settled}",
    );
    let eligible_at = delta::instant(&blocked["params"]["eligibleAt"]);

    // Then: nothing reopens retirement before that instant — the window is a real lower bound, not
    // a delta the service happens to emit as soon as a run ends
    stream::expect_no_fleet_event(&mut watch, wire::KIND_TYPE_BECAME_RETIRABLE, QUIET).await;
    assert!(
        Utc::now() < eligible_at,
        "this scenario only proves a lower bound while the window is still open: the {QUIET:?} \
         silence must fall inside the configured {RETIREMENT_WINDOW_SECONDS}s period",
    );

    // Then: the service reopens retirement on its own, as a typed delta carrying the decision —
    // no client ever computes the end of the window from a run timestamp
    let retirable = stream::await_fleet_event(
        &mut watch,
        wire::KIND_TYPE_BECAME_RETIRABLE,
        RETIREMENT_REOPENING,
    )
    .await;
    assert!(
        Utc::now() >= eligible_at,
        "the reopening delta must never arrive before the eligible-at instant the backend itself \
         published, or the quiet period is decoration: {retirable}",
    );
    assert_lifecycle(&retirable, "DEPRECATED");
    gql::assert_allowed(&retirable, wire::ACTION_RETIRE);

    // Then: the canonical read carries exactly the same reopened decision
    let reopened = fleet_view(&fixture, &runner_type).await;
    assert_lifecycle(&reopened, "DEPRECATED");
    gql::assert_allowed(&reopened, wire::ACTION_RETIRE);
    assert_same_action_surface(&retirable, &reopened);

    // Then: the reopened affordance is the real guard — the administrator retires the type and the
    // catalog entry is retracted for every downstream consumer
    gql::expect_success(
        &gql::retire_runner_type(&client, admin, &runner_type).await,
        wire::FIELD_RETIRE_RUNNER_TYPE,
        "retiring once the configured quiet period elapsed",
    );
    let retired = stream::await_fleet_event(&mut watch, wire::KIND_TYPE_RETIRED, LONG).await;
    assert_lifecycle(&retired, "RETIRED");
    assert_retired_actions(&retired, true);
    catalog::wait_until_absent(fixture.fabric(), &runner_type, LONG).await;

    fixture.shutdown().await;
}

fn assert_cursors_advance_strictly(cursors: &[Value]) {
    assert!(
        cursors.len() > 1,
        "the cursor check must run over the transitions the subscriber actually received, not one \
         delta: {cursors:?}"
    );
    let mut previous: Option<Uuid> = None;
    for cursor in cursors {
        let raw = cursor
            .as_str()
            .unwrap_or_else(|| panic!("every fleet delta carries its own cursor: {cursor}"));
        let current = Uuid::parse_str(raw).unwrap_or_else(|error| {
            panic!("a cursor is a resumable position, never prose: {raw} ({error})")
        });
        if let Some(previous) = previous {
            assert!(
                previous.as_u128() < current.as_u128(),
                "fleet cursors must advance strictly in delivery order — a client that resumes \
                 from the last cursor it saw would otherwise replay a transition it already \
                 applied, or skip one it never did: {previous} then {current}",
            );
        }
        previous = Some(current);
    }
}

async fn fleet_view(fixture: &JobsFixture, runner_type: &str) -> Value {
    let mut views = gql::fleet_of(&fixture.gql(), fixture.admin(), runner_type).await;
    assert_eq!(
        views.len(),
        1,
        "the durable catalog returns one runner type per unique type key: {views:?}",
    );
    views.remove(0)
}

fn assert_lifecycle(view: &Value, expected: &str) {
    let projection = if view["runnerType"].is_object() {
        &view["runnerType"]
    } else {
        view
    };
    assert_eq!(
        projection["lifecycle"],
        json!(expected),
        "the pushed/read projection carries the durable RunnerType lifecycle: {view}",
    );
    gql::assert_affordances_well_formed(view, &format!("a {expected} runner type"));
}

fn assert_active_actions(view: &Value, dispatch_allowed: bool) {
    if dispatch_allowed {
        gql::assert_allowed(view, wire::ACTION_DISPATCH);
    } else {
        assert_eq!(
            gql::assert_blocked(view, wire::ACTION_DISPATCH),
            "runner_type_unavailable",
        );
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
}

fn assert_deprecated_actions(view: &Value, dispatch_allowed: bool, retire_reason: Option<&str>) {
    if dispatch_allowed {
        gql::assert_allowed(view, wire::ACTION_DISPATCH);
    } else {
        assert_eq!(
            gql::assert_blocked(view, wire::ACTION_DISPATCH),
            "runner_type_unavailable",
        );
    }
    assert_eq!(
        gql::assert_blocked(view, wire::ACTION_DEPRECATE),
        "runner_type_not_active",
    );
    gql::assert_allowed(view, wire::ACTION_REACTIVATE);
    if let Some(reason) = retire_reason {
        assert_eq!(gql::assert_blocked(view, wire::ACTION_RETIRE), reason);
    } else {
        gql::assert_allowed(view, wire::ACTION_RETIRE);
    }
}

fn assert_retired_actions(view: &Value, has_live_instance: bool) {
    assert_eq!(
        gql::assert_blocked(view, wire::ACTION_DISPATCH),
        "runner_type_retired",
    );
    assert_eq!(
        gql::assert_blocked(view, wire::ACTION_DEPRECATE),
        "runner_type_not_active",
    );
    if has_live_instance {
        gql::assert_allowed(view, wire::ACTION_REACTIVATE);
    } else {
        assert_eq!(
            gql::assert_blocked(view, wire::ACTION_REACTIVATE),
            "runner_type_has_no_live_instances",
        );
    }
    assert_eq!(
        gql::assert_blocked(view, wire::ACTION_RETIRE),
        "runner_type_not_deprecated",
    );
}

fn assert_same_action_surface(delta: &Value, query: &Value) {
    for action in [
        wire::ACTION_DISPATCH,
        wire::ACTION_DEPRECATE,
        wire::ACTION_REACTIVATE,
        wire::ACTION_RETIRE,
    ] {
        assert_eq!(
            gql::affordance(delta, action),
            gql::affordance(query, action),
            "typed delta and canonical query must carry the same current '{action}' decision",
        );
    }
}

fn assert_member_action_forbidden(known: Value, unknown: Value, action: &str) {
    let known_code = verdict::expect_code_shaped(
        &known,
        &format!("an ordinary member {action} a known runner type"),
    );
    let unknown_code = verdict::expect_code_shaped(
        &unknown,
        &format!("an ordinary member {action} an unknown runner type"),
    );
    assert_eq!(known_code, "FORBIDDEN");
    assert_eq!(unknown_code, "FORBIDDEN");
    assert_eq!(
        known_code, unknown_code,
        "authorization precedes lookup for the {action} resolver",
    );
}

fn assert_invalid_state(response: &Value, what: &str) {
    assert_eq!(
        verdict::expect_code_shaped(response, what),
        "INVALID_STATE",
        "lifecycle refusals use the stable edge code while reasonCode carries domain detail",
    );
}

fn refusal_code(response: &Value, what: &str) -> String {
    let code = verdict::mutation_error_code(response)
        .unwrap_or_else(|| panic!("{what}: a structured refusal owes its code: {response}"));
    assert!(
        verdict::is_code_shaped(&code),
        "{what}: the refusal code must be stable, not prose: {code}",
    );
    code
}
