mod support;

use bc_jobs::domain::ids::JobId;
use bc_jobs::domain::job::Job;
use bc_jobs::domain::policy::{Retention, ScrubStanding, TreeFacts};
use bc_jobs::ports::scrub::{DueTree, ScrubOutcome, TreeScrubs};
use chrono::{DateTime, TimeDelta, Utc};
use uuid::Uuid;

use support::clock;
use support::contention::Fixture;
use support::contention::a_registered_instance;
use support::scrub_race::{
    RUNNER_TYPE, a_failed_job_with_a_retry_due_in, a_job_with_a_started_run, a_queued_job_under,
    failed, scrub,
};

fn due_before() -> DateTime<Utc> {
    Retention::default().scrub_due_before(clock::now() + TimeDelta::days(40))
}

async fn due(fixture: &Fixture, after: Option<DueTree>, batch: usize) -> Vec<JobId> {
    fixture
        .store
        .trees_due_for_scrub(due_before(), batch, after)
        .await
        .expect("the candidates read")
        .into_iter()
        .map(|tree| tree.initial_job_id)
        .collect()
}

async fn make_legacy(fixture: &Fixture, jobs: &[&Job]) {
    let ids: Vec<Uuid> = jobs.iter().map(|job| job.id().as_uuid()).collect();
    sqlx::query("UPDATE jobs SET root_job_id = NULL WHERE id = ANY($1)")
        .bind(&ids)
        .execute(&fixture.pool)
        .await
        .expect("the Jobs lose their stored root");
}

fn standing_by_the_aggregate(job: &Job) -> ScrubStanding {
    let live_runs = job
        .runs()
        .iter()
        .filter(|run| run.terminal().is_none())
        .count();
    Retention::default().scrub_standing(
        &TreeFacts {
            last_activity: job.last_activity_at(),
            unresolved_jobs: u64::from(job.resolution().is_none()),
            live_runs: u64::try_from(live_runs).expect("a count fits"),
            scrubbed: false,
        },
        clock::now() + TimeDelta::days(40),
    )
}

#[tokio::test]
async fn a_tree_with_legacy_jobs_under_a_job_with_a_stored_root_is_found_whole() {
    let fixture = Fixture::start().await;
    let root = a_queued_job_under(&fixture, None).await;
    let rooted_child = a_queued_job_under(&fixture, Some(&root)).await;
    let legacy_grandchild = a_queued_job_under(&fixture, Some(&rooted_child)).await;
    let legacy_child = a_queued_job_under(&fixture, Some(&root)).await;
    let other = a_queued_job_under(&fixture, None).await;
    for job in [
        &legacy_grandchild,
        &rooted_child,
        &legacy_child,
        &root,
        &other,
    ] {
        failed(&fixture, job.clone()).await;
    }
    make_legacy(&fixture, &[&root, &legacy_grandchild, &legacy_child]).await;

    let candidates = due(&fixture, None, 10).await;
    assert!(candidates.contains(&root.id()), "{candidates:?}");
    assert!(candidates.contains(&other.id()), "{candidates:?}");
    assert_eq!(
        scrub(&fixture, root.id()).await,
        ScrubOutcome::Scrubbed { jobs: 4 },
        "the Initial Job, its Job with a stored root and the two Jobs without one are one Tree"
    );
    assert_eq!(
        scrub(&fixture, other.id()).await,
        ScrubOutcome::Scrubbed { jobs: 1 }
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_candidate_scan_and_the_domain_standing_agree_on_every_kind_of_tree() {
    let fixture = Fixture::start().await;
    a_registered_instance(&fixture, RUNNER_TYPE).await;
    let settled = failed(&fixture, a_queued_job_under(&fixture, None).await).await;
    let unresolved = a_queued_job_under(&fixture, None).await;
    let (with_a_live_run, _) = a_job_with_a_started_run(&fixture).await;
    let with_a_live_run = failed(&fixture, with_a_live_run).await;
    let recent = failed(&fixture, a_queued_job_under(&fixture, None).await).await;
    sqlx::query("UPDATE jobs SET created_at = created_at + interval '35 days' WHERE id = $1")
        .bind(recent.id().as_uuid())
        .execute(&fixture.pool)
        .await
        .expect("the Tree is made recent");
    sqlx::query(
        "UPDATE job_resolutions SET occurred_at = occurred_at + interval '35 days' \
         WHERE job_id = $1",
    )
    .bind(recent.id().as_uuid())
    .execute(&fixture.pool)
    .await
    .expect("the Tree is made recent");
    let scrubbed = failed(&fixture, a_queued_job_under(&fixture, None).await).await;
    assert_eq!(
        scrub(&fixture, scrubbed.id()).await,
        ScrubOutcome::Scrubbed { jobs: 1 }
    );

    let candidates = due(&fixture, None, 100).await;
    for (name, tree) in [
        ("settled", &settled),
        ("unresolved", &unresolved),
        ("with a live run", &with_a_live_run),
        ("recent", &recent),
        ("already scrubbed", &scrubbed),
    ] {
        let by_the_scan = candidates.contains(&tree.id());
        let by_the_domain = matches!(
            scrub(&fixture, tree.id()).await,
            ScrubOutcome::Scrubbed { .. }
        );
        assert_eq!(by_the_scan, by_the_domain, "{name}");
    }
    assert!(candidates.contains(&settled.id()));
    assert_eq!(candidates.len(), 1, "only the settled Tree is due");
    fixture.shutdown().await;
}

#[tokio::test]
async fn the_due_time_of_a_retry_is_part_of_the_last_activity_for_the_scan_and_for_the_aggregate() {
    let fixture = Fixture::start().await;
    a_registered_instance(&fixture, RUNNER_TYPE).await;
    let retry_far = a_failed_job_with_a_retry_due_in(&fixture, 60).await;
    let retry_near = a_failed_job_with_a_retry_due_in(&fixture, 1).await;

    assert_eq!(
        standing_by_the_aggregate(&retry_far),
        ScrubStanding::Waiting
    );
    assert_eq!(standing_by_the_aggregate(&retry_near), ScrubStanding::Due);
    let candidates = due(&fixture, None, 100).await;
    assert!(!candidates.contains(&retry_far.id()), "{candidates:?}");
    assert!(candidates.contains(&retry_near.id()), "{candidates:?}");
    assert_eq!(
        scrub(&fixture, retry_far.id()).await,
        ScrubOutcome::Standing(ScrubStanding::Waiting)
    );
    assert_eq!(
        scrub(&fixture, retry_near.id()).await,
        ScrubOutcome::Scrubbed { jobs: 1 }
    );
    fixture.shutdown().await;
}
