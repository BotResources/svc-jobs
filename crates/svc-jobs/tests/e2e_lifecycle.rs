mod support;

use br_test_harness::{SseSubscription, verdict};
use serde_json::json;
use support::db::{self, Durable};
use support::events::EventLog;
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{
    FLEET_CHANGED, JOB_CHANGED, JOBS_CHANGED, LOG_TAIL, LONG, QUIET, SHORT, delta, gql, stream,
    subs, wire,
};
use uuid::Uuid;

#[tokio::test]
async fn an_administrator_follows_a_job_from_declaration_to_audited_deletion() {
    // Given: a runner type nobody serves yet, watched from the list and fleet streams
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("lifecycle");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a");

    let mut listing =
        SseSubscription::open(fixture.url(), admin, &subs::jobs_changed(&runner_type)).await;
    let opening_list = stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;
    delta::assert_page_info(&opening_list["jobs"]);
    let mut fleet_watch =
        SseSubscription::open(fixture.url(), admin, &subs::fleet_changed(&runner_type)).await;
    stream::snapshot(&mut fleet_watch, FLEET_CHANGED, SHORT).await;

    // When: the producer declares the job
    let entity_id = Uuid::now_v7();
    let operator_id = Uuid::now_v7();
    let declaration = JobDeclaration::new(&runner_type)
        .with_config(json!({ "prompt": "summarise", "depth": 2 }))
        .triggered_by(operator_id, "Amelie")
        .with_source("projects", entity_id)
        .with_max_attempts(2);
    let job_id = declaration.job_id;
    producer.declare(&declaration).await;

    // Then: one acceptance, and the list stream carries the queued job with its affordances
    events.expect_one(wire::FACT_QUEUED, job_id, LONG).await;
    let queued = stream::await_delta(&mut listing, JOBS_CHANGED, wire::EVT_QUEUED, LONG).await;
    delta::event_of(&queued, wire::EVT_QUEUED, job_id);
    let queued_row = delta::assert_upserted_summary(&queued, job_id, "PENDING");
    delta::assert_active_affordances(&queued_row);

    let waiting =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_BEGAN_WAITING, LONG).await;
    assert_eq!(waiting["event"]["jobId"], json!(job_id.to_string()));
    delta::assert_fleet(&waiting, &runner_type, 1, 0, 0, 0);
    assert_eq!(
        delta::fleet_projection(&waiting, &runner_type)["isAvailable"],
        json!(false),
        "no instance has signalled presence yet",
    );

    let mut watch = SseSubscription::open(fixture.url(), admin, &subs::job_changed(job_id)).await;
    let opening = stream::snapshot(&mut watch, JOB_CHANGED, SHORT).await;
    assert_eq!(opening["job"]["status"], json!("PENDING"));
    assert_eq!(opening["job"]["attemptCount"], json!(0));
    delta::assert_active_affordances(&opening);
    let mut tail = SseSubscription::open(fixture.url(), admin, &subs::log_tail(job_id)).await;
    stream::snapshot(&mut tail, LOG_TAIL, SHORT).await;

    let view = gql::job_view(&client, admin, job_id).await;
    let job = view["job"].clone();
    assert_eq!(job["runnerType"], json!(runner_type));
    assert_eq!(job["producer"], json!("projects"));
    assert_eq!(job["config"], json!({ "prompt": "summarise", "depth": 2 }));
    assert_eq!(job["maxAttempts"], json!(2));
    assert_eq!(job["triggeredBy"]["id"], json!(operator_id.to_string()));
    assert_eq!(job["source"]["entityId"], json!(entity_id.to_string()));
    delta::assert_active_affordances(&view);

    support::views::assert_views_agree(&durable, &client, admin, job_id, "a job nobody serves yet")
        .await;

    let by_source = gql::job_by_source(&client, admin, "projects", entity_id).await;
    let listed = gql::list_jobs(&client, admin, json!({ "runnerTypes": [runner_type] })).await;
    assert_eq!(by_source["job"]["id"], json!(job_id.to_string()));
    assert_eq!(by_source["job"]["status"], json!("PENDING"));
    assert_eq!(
        listed
            .iter()
            .filter(|row| row["job"]["id"] == json!(job_id.to_string()))
            .count(),
        1,
        "`jobs`, `jobsJob` and `jobsJobBySource` answer with the same single identity",
    );
    delta::assert_active_affordances(&by_source);
    delta::assert_active_affordances(&gql::listed_row(&listed, job_id));

    let idle_fleet = gql::fleet_of(&client, admin, &runner_type).await;
    assert_eq!(
        idle_fleet.len(),
        1,
        "a type nobody serves yet still answers the fleet read — it is exactly the state an \
         administrator opens the page to diagnose: {idle_fleet:?}",
    );
    let unserved = delta::assert_fleet(&idle_fleet[0], &runner_type, 1, 0, 0, 0);
    assert_eq!(
        unserved["isAvailable"],
        json!(false),
        "no instance has signalled presence, so the read says so exactly as the stream did: \
         {unserved}",
    );
    assert_eq!(
        unserved["instances"],
        json!([]),
        "an unserved type shows no live instance: {unserved}",
    );
    gql::assert_affordances_well_formed(&idle_fleet[0], "the fleet read of an unserved type");
    instance.expect_no_trigger(QUIET).await;

    // When: the first instance of the type signals presence
    instance.connect().await;
    let registered =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_TYPE_REGISTERED, LONG).await;
    assert_eq!(registered["event"]["runnerType"], json!(runner_type));
    let connected =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_INSTANCE_CONNECTED, LONG).await;
    assert_eq!(connected["event"]["instanceKey"], json!("instance-a"));
    assert_eq!(
        delta::fleet_projection(&connected, &runner_type)["isAvailable"],
        json!(true)
    );

    let trigger = instance.next_trigger(LONG).await;
    assert_eq!(runner::attempt_number(&trigger), 1);
    assert_eq!(runner::job_id(&trigger), job_id);
    assert_eq!(
        trigger["config"],
        json!({ "prompt": "summarise", "depth": 2 }),
        "the runner configuration is forwarded unchanged",
    );
    let run = runner::run_id(&trigger);

    let dispatched =
        stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_RUN_DISPATCHED, LONG).await;
    assert_eq!(
        delta::event_of(&dispatched, wire::EVT_RUN_DISPATCHED, job_id)["runId"],
        json!(run.to_string()),
        "the dispatch event names the run the trigger carries",
    );
    delta::projection(&dispatched, job_id, "IN_PROGRESS");

    let stopped_waiting =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_STOPPED_WAITING, LONG).await;
    delta::assert_instance(
        &delta::assert_fleet(&stopped_waiting, &runner_type, 0, 0, 0, 1),
        "instance-a",
        false,
        &[],
        &instance.version,
    );

    instance.start_run(&trigger).await;
    events.expect_one(wire::FACT_STARTED, job_id, LONG).await;
    let started = stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_RUN_STARTED, LONG).await;
    let projection = delta::projection(&started, job_id, "IN_PROGRESS");
    assert_eq!(projection["activeRunId"], json!(run.to_string()));
    delta::assert_active_affordances(&started);
    let executing =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_BEGAN_EXECUTING, LONG).await;
    delta::assert_instance(
        &delta::assert_fleet(&executing, &runner_type, 0, 1, 1, 0),
        "instance-a",
        true,
        &[run],
        &instance.version,
    );

    support::views::assert_views_agree(&durable, &client, admin, job_id, "a job under execution")
        .await;

    // When: the runner declares a plan, revises it, and advances its cursor
    instance
        .declare_plan(&trigger, &["fetch", "summarise", "store"])
        .await;
    let declared =
        stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_PLAN_DECLARED, LONG).await;
    assert_eq!(
        delta::event_of(&declared, wire::EVT_PLAN_DECLARED, job_id)["declarationNumber"],
        json!(1)
    );
    assert_eq!(
        delta::projection(&declared, job_id, "IN_PROGRESS")["progression"]["plan"]["items"]
            .as_array()
            .map(Vec::len),
        Some(3),
        "the delta carries the full latest plan, never a pointer to refetch",
    );

    instance.declare_plan(&trigger, &["fetch", "store"]).await;
    let revised = stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_PLAN_DECLARED, LONG).await;
    assert_eq!(
        delta::event_of(&revised, wire::EVT_PLAN_DECLARED, job_id)["declarationNumber"],
        json!(2)
    );
    assert_eq!(
        delta::projection(&revised, job_id, "IN_PROGRESS")["progression"]["plan"]["items"],
        json!([{ "index": 0, "label": "fetch" }, { "index": 1, "label": "store" }]),
        "the latest declaration replaces the plan, it never appends to it",
    );
    events
        .expect_exactly(wire::FACT_PLAN_DECLARED, job_id, 2, LONG)
        .await;

    instance.start_step(&trigger, 0, "fetch").await;
    let step = stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_STEP_STARTED, LONG).await;
    assert_eq!(
        delta::event_of(&step, wire::EVT_STEP_STARTED, job_id)["stepIndex"],
        json!(0)
    );

    // When: a run-level log arrives, then a log naming a step that has not started yet
    instance.log_line(&trigger, None, "INFO", "fetching").await;
    let run_level = stream::drain_appended(&mut tail, LOG_TAIL, 1, LONG).await;
    delta::log_line(&run_level[0], run, None);
    watch
        .expect_silence("a log never pushes job state — it is informational", QUIET)
        .await;

    instance
        .log_line(&trigger, Some(1), "WARNING", "slow provider")
        .await;
    let ahead_of_step = stream::drain_appended(&mut tail, LOG_TAIL, 1, LONG).await;
    delta::log_line(&ahead_of_step[0], run, Some(1));
    instance.start_step(&trigger, 1, "store").await;
    let second_step =
        stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_STEP_STARTED, LONG).await;
    assert_eq!(
        delta::event_of(&second_step, wire::EVT_STEP_STARTED, job_id)["stepIndex"],
        json!(1)
    );
    assert_eq!(
        delta::projection(&second_step, job_id, "IN_PROGRESS")["progression"]["currentStep"]["index"],
        json!(1),
        "starting step 1 implicitly closes step 0",
    );
    events
        .expect_exactly(wire::FACT_STEP_STARTED, job_id, 2, LONG)
        .await;

    let logs = gql::logs_of(&client, admin, job_id).await;
    assert_eq!(
        logs.len(),
        2,
        "a job's log is the ordered union of its runs'"
    );
    assert_eq!(logs[0]["level"], json!("INFO"));
    assert!(
        logs[0]["stepIndex"].is_null(),
        "a run-level log keeps no step association: {:?}",
        logs[0]
    );
    assert_eq!(
        logs[1]["stepIndex"],
        json!(1),
        "a log that preceded its step keeps the association the runner supplied: {:?}",
        logs[1]
    );

    // When: the run completes — which is not the job completing
    instance.complete_run(&trigger).await;
    let run_completed =
        stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_RUN_COMPLETED, LONG).await;
    delta::event_of(&run_completed, wire::EVT_RUN_COMPLETED, job_id);
    delta::projection(&run_completed, job_id, "IN_PROGRESS");
    delta::assert_active_affordances(&run_completed);
    let stopped =
        stream::await_fleet_event(&mut fleet_watch, wire::KIND_JOB_STOPPED_EXECUTING, LONG).await;
    delta::assert_instance(
        &delta::assert_fleet(&stopped, &runner_type, 0, 0, 0, 1),
        "instance-a",
        false,
        &[],
        &instance.version,
    );
    events
        .expect_none(wire::FACT_COMPLETED, job_id, QUIET)
        .await;
    stream::expect_no_delta(&mut watch, JOB_CHANGED, wire::EVT_JOB_COMPLETED, QUIET).await;

    // When: the owner declares the job finished — the only path to COMPLETED
    let resolution_id = Uuid::now_v7();
    producer.finish(job_id, resolution_id).await;
    let completed = events.expect_one(wire::FACT_COMPLETED, job_id, LONG).await;
    let payload = completed.payload();
    assert!(
        payload["result"].is_null() && payload["output"].is_null(),
        "a completion event never carries a work product: {payload}",
    );
    let job_completed =
        stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_JOB_COMPLETED, LONG).await;
    assert_eq!(
        delta::event_of(&job_completed, wire::EVT_JOB_COMPLETED, job_id)["resolutionId"],
        json!(resolution_id.to_string())
    );
    delta::projection(&job_completed, job_id, "COMPLETED");
    delta::assert_completed_affordances(&job_completed);
    let listed_completed =
        stream::await_delta(&mut listing, JOBS_CHANGED, wire::EVT_JOB_COMPLETED, LONG).await;
    delta::assert_completed_affordances(&delta::assert_upserted_summary(
        &listed_completed,
        job_id,
        "COMPLETED",
    ));
    assert!(
        gql::job_by_source(&client, admin, "projects", entity_id)
            .await
            .is_null(),
        "the source lookup answers with the single NON-TERMINAL job for that reference, so a \
         finished one no longer answers for its source",
    );

    support::views::assert_views_agree(&durable, &client, admin, job_id, "a completed job").await;

    // When: the administrator soft-deletes the terminal job
    let mut deleted_window = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::jobs_changed_including_deleted(&runner_type),
    )
    .await;
    stream::snapshot(&mut deleted_window, JOBS_CHANGED, SHORT).await;

    let deletion = gql::delete_job(&client, admin, job_id).await;
    verdict::expect_ack(
        &deletion,
        "an administrator deletes an eligible terminal job",
    );
    assert_eq!(
        deletion["data"]["jobsDeleteJob"],
        json!({ "success": true }),
        "a mutation returns a verdict, never the mutated state",
    );

    let deleted = stream::await_delta(&mut watch, JOB_CHANGED, wire::EVT_JOB_DELETED, LONG).await;
    assert_eq!(
        delta::event_of(&deleted, wire::EVT_JOB_DELETED, job_id)["deletedById"],
        json!(admin.actor_id().to_string())
    );
    assert_eq!(
        delta::projection(&deleted, job_id, "COMPLETED")["isDeleted"],
        json!(true)
    );
    let removed =
        stream::await_delta(&mut listing, JOBS_CHANGED, wire::EVT_JOB_DELETED, LONG).await;
    delta::assert_removes(&removed, job_id);
    let kept = stream::await_delta(
        &mut deleted_window,
        JOBS_CHANGED,
        wire::EVT_JOB_DELETED,
        LONG,
    )
    .await;
    let kept_row = delta::assert_upserted_summary(&kept, job_id, "COMPLETED");
    assert_eq!(kept_row["job"]["isDeleted"], json!(true));

    let audited = gql::job_view(&client, admin, job_id).await;
    assert_eq!(audited["job"]["isDeleted"], json!(true));
    assert_eq!(
        audited["job"]["deletion"]["deletedBy"]["id"],
        json!(admin.actor_id().to_string())
    );
    assert!(!audited["job"]["deletion"]["deletedAt"].is_null());
    assert_eq!(audited["job"]["runs"].as_array().map(Vec::len), Some(1));
    assert_eq!(audited["job"]["resolution"]["kind"], json!("COMPLETED"));
    gql::assert_blocked(&audited, wire::ACTION_DELETE);

    let mut replay_tail =
        SseSubscription::open(fixture.url(), admin, &subs::log_tail(job_id)).await;
    let replayed = stream::snapshot(&mut replay_tail, LOG_TAIL, SHORT).await;
    assert_eq!(
        replayed["logs"]["edges"].as_array().map(Vec::len),
        Some(2),
        "logs remain part of the audit record after soft deletion",
    );
    assert_eq!(gql::logs_of(&client, admin, job_id).await.len(), 2);

    // Then: the default reads hide the deleted job, and the deliberate ones surface it
    gql::assert_lists_exactly(
        &gql::list_page(
            &client,
            admin,
            json!({ "runnerTypes": [&runner_type], "deleted": "ONLY_DELETED" }),
            50,
            None,
        )
        .await,
        &[job_id],
        "listing only the deleted jobs of this type",
    );
    gql::assert_lists_none_of(
        &gql::list_page(&client, admin, json!(null), 50, None).await,
        &[job_id],
        "the administrator's unfiltered list leaves deleted jobs out by default — an audit record \
         must be asked for, never volunteered into every page",
    );
    let whole_fleet = gql::fleet_of_every_type(&client, admin).await;
    assert!(
        whole_fleet
            .iter()
            .any(|view| view["runnerType"]["typeKey"] == json!(runner_type)),
        "reading the fleet without naming a type answers with every type the platform knows: \
         {whole_fleet:?}",
    );
    for view in &whole_fleet {
        gql::assert_affordances_well_formed(view, "the unfiltered fleet read");
    }

    // Then: the soft deletion stays an administrative decision — no producer hears about it
    events
        .expect_none(wire::FACT_CANCELLED, job_id, QUIET)
        .await;
    for (fact, published) in [
        (wire::FACT_COMPLETED, 1),
        (wire::FACT_FAILED, 0),
        (wire::FACT_CREATION_REJECTED, 0),
    ] {
        events.expect_exactly(fact, job_id, published, QUIET).await;
    }

    // Then: the durable record the edge cannot show
    durable
        .assert_all(
            job_id,
            &[
                (db::JOBS_WITH_ID, 1, "one job"),
                (db::RUNS_OF_JOB, 1, "one run"),
                (db::PLAN_DECLARATIONS_OF_JOB, 2, "both plan declarations"),
                (db::LOGS_OF_JOB, 2, "both logical log lines"),
                (db::RESOLUTIONS_OF_JOB, 1, "one completion resolution"),
                (db::DELETIONS_OF_JOB, 1, "one soft-deletion record"),
            ],
        )
        .await;

    let repeat = gql::delete_job(&client, admin, job_id).await;
    verdict::expect_code_shaped(&repeat, "deleting an already-deleted job");

    let pending = JobDeclaration::new(&runner_type);
    let pending_id = pending.job_id;
    producer.declare(&pending).await;
    events.expect_one(wire::FACT_QUEUED, pending_id, LONG).await;
    let refused = gql::delete_job(&client, admin, pending_id).await;
    verdict::expect_code_shaped(&refused, "deleting a non-terminal job");
    assert_eq!(
        gql::job(&client, admin, pending_id).await["isDeleted"],
        json!(false),
        "a refused deletion leaves no trace on the job",
    );

    // Then: a terminal job frees its source reference for the work that comes after it
    let reclaiming = JobDeclaration::new(&runner_type).with_source("projects", entity_id);
    let reclaiming_id = reclaiming.job_id;
    producer.declare(&reclaiming).await;
    events
        .expect_one(wire::FACT_QUEUED, reclaiming_id, LONG)
        .await;
    events
        .expect_none(wire::FACT_CREATION_REJECTED, reclaiming_id, QUIET)
        .await;
    assert_eq!(
        gql::job_by_source(&client, admin, "projects", entity_id).await["job"]["id"],
        json!(reclaiming_id.to_string()),
        "at most one NON-TERMINAL job per source is a rule about live work — a claim kept for good \
         would bar every future job on an entity already processed once",
    );

    // Then: the administrator's list is windowed and filtered exactly as asked
    let listing_type = wire::unique_runner_type("listing");
    let other_producer = Producer::new(fixture.fabric(), "chat");
    let mut waiting_ids = Vec::new();
    for index in 0..3 {
        let queued_job = JobDeclaration::new(&listing_type);
        waiting_ids.push(queued_job.job_id);
        if index == 2 {
            other_producer.declare(&queued_job).await;
        } else {
            producer.declare(&queued_job).await;
        }
        events
            .expect_one(wire::FACT_QUEUED, waiting_ids[index], LONG)
            .await;
    }
    let by_type = json!({ "runnerTypes": [&listing_type] });

    let first_page = gql::list_page(&client, admin, by_type.clone(), 2, None).await;
    assert_eq!(
        gql::edges_of(&first_page).len(),
        2,
        "a window of two holds two rows, however many jobs match: {first_page}",
    );
    assert_eq!(
        first_page["pageInfo"]["hasNextPage"],
        json!(true),
        "a window that does not hold everything must say so: {first_page}",
    );
    let cursor = first_page["pageInfo"]["endCursor"]
        .as_str()
        .unwrap_or_else(|| panic!("a non-empty page carries its end cursor: {first_page}"))
        .to_string();
    let second_page = gql::list_page(&client, admin, by_type.clone(), 2, Some(&cursor)).await;
    assert_eq!(gql::edges_of(&second_page).len(), 1);
    assert_eq!(second_page["pageInfo"]["hasNextPage"], json!(false));
    let mut walked = gql::listed_ids(&first_page);
    walked.extend(gql::listed_ids(&second_page));
    walked.sort();
    let mut declared: Vec<String> = waiting_ids.iter().map(Uuid::to_string).collect();
    declared.sort();
    assert_eq!(
        walked, declared,
        "walking the window from a cursor loses no row and repeats none",
    );

    for (filter, expected, what) in [
        (
            json!({ "runnerTypes": [&listing_type], "statuses": ["PENDING"] }),
            waiting_ids.clone(),
            "filtering by the status they all hold",
        ),
        (
            json!({ "runnerTypes": [&listing_type], "producers": ["chat"] }),
            waiting_ids[2..].to_vec(),
            "filtering by the producer of one of them",
        ),
        (
            json!({ "runnerTypes": [&listing_type], "producers": ["projects"] }),
            waiting_ids[..2].to_vec(),
            "filtering by the producer of the other two",
        ),
    ] {
        gql::assert_lists_exactly(
            &gql::list_page(&client, admin, filter, 50, None).await,
            &expected,
            what,
        );
    }
    gql::assert_lists_none_of(
        &gql::list_page(
            &client,
            admin,
            json!({ "runnerTypes": [&listing_type], "statuses": ["FAILED"] }),
            50,
            None,
        )
        .await,
        &waiting_ids,
        "filtering by a status none of them holds",
    );

    // Then: a subscription window is bounded, and a job leaving the filter leaves it exactly
    let mut narrow = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::jobs_changed_pending(&listing_type, 2),
    )
    .await;
    let narrow_opening = stream::snapshot(&mut narrow, JOBS_CHANGED, SHORT).await;
    assert_eq!(
        gql::edges_of(&narrow_opening["jobs"]).len(),
        2,
        "a subscription opens on its window, not on everything that matches: {narrow_opening}",
    );
    assert_eq!(
        narrow_opening["jobs"]["pageInfo"]["hasNextPage"],
        json!(true)
    );

    let mut pending_window = SseSubscription::open(
        fixture.url(),
        admin,
        &subs::jobs_changed_pending(&listing_type, 50),
    )
    .await;
    let pending_opening = stream::snapshot(&mut pending_window, JOBS_CHANGED, SHORT).await;
    assert_eq!(gql::edges_of(&pending_opening["jobs"]).len(), 3);

    verdict::expect_ack(
        &gql::cancel_job(&client, admin, Uuid::now_v7(), waiting_ids[0]).await,
        "an administrator cancels one of the waiting jobs",
    );
    let left_the_window = stream::await_delta(
        &mut pending_window,
        JOBS_CHANGED,
        wire::EVT_JOB_CANCELLED,
        LONG,
    )
    .await;
    delta::assert_removes(&left_the_window, waiting_ids[0]);
    assert!(
        !gql::listed_ids(
            &gql::list_page(
                &client,
                admin,
                json!({ "runnerTypes": [&listing_type], "statuses": ["PENDING"] }),
                50,
                None,
            )
            .await
        )
        .contains(&waiting_ids[0].to_string()),
        "a job that no longer matches the filter is gone from the read as it is from the window",
    );

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}
