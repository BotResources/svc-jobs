mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use br_test_harness::SseSubscription;
use futures::future::join_all;
use serde_json::json;
use support::claim::{self, ClaimAnswer};
use support::db::{self, Durable};
use support::events::EventLog;
use support::fixture::{JobsFixture, Knobs};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::{JOBS_CHANGED, LONG, QUIET, SHORT, dispatch, gql, stream, subs, wire};
use uuid::Uuid;

const INSTANCES: usize = 30;
const JOBS: usize = 24;
const LATE_JOBS: usize = 6;
const CLAIM_WAIT: Duration = Duration::from_secs(3);

#[tokio::test]
async fn two_jobs_pods_never_reserve_one_job_to_two_claims() {
    // Given: two Jobs pods over one database and one NATS, and a CLAIM type with thirty live
    // instances seen by both pods
    let fixture = JobsFixture::start_cluster(2, Knobs::default()).await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let admin = fixture.admin();
    let pods = [fixture.gql_of(0), fixture.gql_of(1)];
    let runner_type = wire::unique_runner_type("contended");
    let producer = Producer::new(fixture.fabric(), "projects");
    let mut instances = Vec::new();
    for index in 0..INSTANCES {
        let mut instance =
            FakeRunner::new(fixture.nats(), &runner_type, &format!("slot-{index:02}"));
        instance.connect().await;
        instances.push(instance);
    }
    for pod in &pods {
        dispatch::await_live_instances(pod, admin, &runner_type, INSTANCES, LONG).await;
    }
    dispatch::switch_to_claim(&pods[0], admin, &runner_type).await;
    assert_eq!(
        dispatch::runner_type_view(&pods[1], admin, &runner_type).await["runnerType"]["dispatchMode"],
        json!(wire::MODE_CLAIM),
        "both pods read the one dispatch mode",
    );

    // Given: many jobs waiting
    let mut declared = BTreeSet::new();
    for _ in 0..JOBS {
        let declaration = JobDeclaration::new(&runner_type);
        producer.declare_v2(&declaration).await;
        events
            .expect_one(wire::FACT_QUEUED, declaration.job_id, LONG)
            .await;
        declared.insert(declaration.job_id);
    }
    let mut listings = Vec::new();
    for pod in 0..2 {
        let mut listing = SseSubscription::open(
            fixture.url_of(pod),
            admin,
            &subs::jobs_changed_with_priority(&runner_type),
        )
        .await;
        stream::snapshot(&mut listing, JOBS_CHANGED, SHORT).await;
        listings.push(listing);
    }

    // When: every instance claims at the same moment
    let answers: Vec<ClaimAnswer> = join_all(
        instances
            .iter()
            .map(|instance| claim::claim(fixture.nats(), instance, CLAIM_WAIT)),
    )
    .await;

    // Then: every job is reserved exactly once, each to exactly one claim
    let mut reserved: BTreeMap<String, (Uuid, Uuid)> = BTreeMap::new();
    let mut empty = Vec::new();
    for (instance, answer) in instances.iter().zip(&answers) {
        if answer.reply["run"].is_object() {
            let run = answer.expect_run("a concurrent claim served");
            assert_eq!(runner::attempt_number(&run), 1);
            reserved.insert(
                instance.instance_key.clone(),
                (runner::job_id(&run), runner::run_id(&run)),
            );
        } else {
            answer.expect_empty("a concurrent claim with no job left");
            empty.push(instance.instance_key.clone());
        }
    }
    let served_jobs: BTreeSet<Uuid> = reserved.values().map(|(job, _)| *job).collect();
    let served_runs: BTreeSet<Uuid> = reserved.values().map(|(_, run)| *run).collect();
    assert_eq!(
        reserved.len(),
        JOBS,
        "as many claims served as jobs waited, no more: {reserved:?}"
    );
    assert_eq!(
        served_jobs, declared,
        "every waiting job was served, none twice"
    );
    assert_eq!(served_runs.len(), JOBS, "one distinct run per served claim");
    assert_eq!(empty.len(), INSTANCES - JOBS);
    for job_id in &declared {
        durable
            .assert_count(
                db::RUNS_OF_JOB,
                *job_id,
                1,
                "a job is reserved to one claim only, whatever the number of pods — a second \
                 reservation answered to nobody would be a hidden run",
            )
            .await;
    }

    // Then: both pods announce each dispatch exactly once
    for listing in listings.iter_mut() {
        let mut announced = BTreeSet::new();
        while announced.len() < JOBS {
            let dispatched =
                stream::await_delta(listing, JOBS_CHANGED, wire::EVT_RUN_DISPATCHED, LONG).await;
            let job_id = Uuid::parse_str(dispatched["event"]["jobId"].as_str().unwrap_or_default())
                .expect("a dispatch names its job");
            assert!(
                announced.insert(job_id),
                "job {job_id} announced as dispatched twice"
            );
        }
        stream::expect_no_delta(listing, JOBS_CHANGED, wire::EVT_RUN_DISPATCHED, QUIET).await;
    }

    // Given: a few more jobs arrive
    let mut late = BTreeSet::new();
    for _ in 0..LATE_JOBS {
        let declaration = JobDeclaration::new(&runner_type);
        producer.declare_v2(&declaration).await;
        events
            .expect_one(wire::FACT_QUEUED, declaration.job_id, LONG)
            .await;
        late.insert(declaration.job_id);
    }

    // When: every instance claims another free slot at the same moment, under a new claim id
    let second_round: Vec<ClaimAnswer> = join_all(
        instances
            .iter()
            .map(|instance| claim::claim(fixture.nats(), instance, CLAIM_WAIT)),
    )
    .await;

    // Then: only the late jobs are served, each once — a new claim id never re-serves a job
    // already reserved, even to an instance that holds it
    let mut late_served = BTreeSet::new();
    for answer in &second_round {
        if answer.reply["run"].is_object() {
            let run = answer.expect_run("a second-slot claim served");
            assert!(
                late_served.insert(runner::job_id(&run)),
                "late job {} reserved to two claims",
                runner::job_id(&run)
            );
        } else {
            answer.expect_empty("a second-slot claim with no job left");
        }
    }
    assert_eq!(
        late_served, late,
        "the second round serves exactly the late jobs, none of the first ones again",
    );
    for job_id in declared.iter().chain(&late) {
        durable
            .assert_count(
                db::RUNS_OF_JOB,
                *job_id,
                1,
                "one run per job across both rounds",
            )
            .await;
    }

    // When: every instance repeats its first-round claim id at the same moment
    let repeats: Vec<ClaimAnswer> =
        join_all(instances.iter().zip(&answers).map(|(instance, first)| {
            claim::claim_again(fixture.nats(), instance, first.claim_id, CLAIM_WAIT)
        }))
        .await;

    // Then: holders get their own reservation back, the others nothing, on whichever pod
    for (instance, answer) in instances.iter().zip(&repeats) {
        match reserved.get(&instance.instance_key) {
            Some((job_id, run_id)) => {
                let run = answer.expect_run("a repeated claim of a holder");
                assert_eq!(
                    (runner::job_id(&run), runner::run_id(&run)),
                    (*job_id, *run_id),
                    "a repeated claim id returns the run already reserved, whatever pod answers",
                );
            }
            None => answer.expect_empty("a repeated claim with nothing reserved"),
        }
    }
    for job_id in declared.iter().chain(&late) {
        durable
            .assert_count(db::RUNS_OF_JOB, *job_id, 1, "repeats create no run")
            .await;
    }
    for index in 0..2 {
        assert!(
            gql::ready(fixture.url_of(index)).await,
            "both pods stay ready throughout the contention",
        );
    }

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}
