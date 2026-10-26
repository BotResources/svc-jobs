mod support;

use std::time::Duration;

use br_test_harness::wait_until;
use futures::future::join_all;
use serde_json::{Value, json};
use support::db::Durable;
use support::events::EventLog;
use support::fixture::{self, JobsFixture};
use support::producer::{JobDeclaration, Producer};
use support::{LONG, gql, priority, wire};
use uuid::Uuid;

const FAN_OUT: usize = 20;
const WINDOW: Duration = Duration::from_secs(1);
const TREES: usize = 12;
const GRANDCHILDREN: usize = 4;

#[tokio::test]
async fn a_force_racing_a_fan_out_reaches_every_waiting_descendant() {
    // Given: a waiting root at 3 on a type nobody serves
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("forced_fanout");
    let producer = Producer::new(fixture.fabric(), "projects");
    let root = JobDeclaration::new(&runner_type).with_priority(3);
    producer.declare_v2(&root).await;
    events
        .expect_one(wire::FACT_QUEUED, root.job_id, LONG)
        .await;

    // Given: a share lock on the root, as an in-flight child admission holds while it re-reads
    // its parent — it only widens a window that exists without it
    let mut admission = durable.pool().begin().await.expect("a transaction opens");
    sqlx::query("SELECT 1 FROM jobs WHERE id = $1 FOR SHARE")
        .bind(root.job_id)
        .execute(&mut *admission)
        .await
        .expect("a share lock on the root");

    // When: a super user forces the root to 5 while a child is admitted under it meanwhile
    let in_window = JobDeclaration::new(&runner_type)
        .with_parent(root.job_id)
        .with_priority(1);
    let (forced, ()) = tokio::join!(
        priority::force_job_priority(&client, admin, root.job_id, 5),
        async {
            tokio::time::sleep(WINDOW).await;
            producer.declare_v2(&in_window).await;
            events
                .expect_one(wire::FACT_QUEUED, in_window.job_id, LONG)
                .await;
            admission.commit().await.expect("the lock is released");
        }
    );

    // Then: the force is acknowledged and the child admitted in its window ends at the forced
    // level, as requested, effective and forced
    gql::expect_success(
        &forced,
        wire::FIELD_FORCE_JOB_PRIORITY,
        "a force racing a child admission",
    );
    priority::assert_priorities(
        &priority::job(&client, admin, root.job_id).await,
        3,
        5,
        Some(5),
        "the forced root",
    );
    priority::assert_priorities(
        &priority::job(&client, admin, in_window.job_id).await,
        5,
        5,
        Some(5),
        "a child admitted while the force was in flight is forced like any waiting descendant",
    );

    // When: the super user forces the root to 4 while a burst of children is being admitted
    let burst: Vec<JobDeclaration> = (0..FAN_OUT)
        .map(|_| {
            JobDeclaration::new(&runner_type)
                .with_parent(root.job_id)
                .with_priority(1)
        })
        .collect();
    let (reforced, _) = tokio::join!(
        async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            priority::force_job_priority(&client, admin, root.job_id, 4).await
        },
        join_all(burst.iter().map(|child| producer.declare_v2(child)))
    );
    gql::expect_success(
        &reforced,
        wire::FIELD_FORCE_JOB_PRIORITY,
        "a force racing a fan-out",
    );
    for child in &burst {
        events
            .expect_one(wire::FACT_QUEUED, child.job_id, LONG)
            .await;
    }

    // Then: every waiting descendant, whenever it was admitted, ends at the forced level
    for job_id in std::iter::once(in_window.job_id).chain(burst.iter().map(|child| child.job_id)) {
        priority::assert_priorities(
            &priority::job(&client, admin, job_id).await,
            4,
            4,
            Some(4),
            "every waiting descendant ends at the level its root was forced to",
        );
    }

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_cap_change_racing_admissions_reaches_every_waiting_job_of_the_user() {
    // Given: a user capped at 2 with one waiting root requesting 5, on a type nobody serves
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let user = fixture::ordinary_member();
    let runner_type = wire::unique_runner_type("capped_fanout");
    let producer = Producer::new(fixture.fabric(), "projects");
    gql::expect_success(
        &priority::set_user_max_priority(&client, admin, user.actor_id(), 2).await,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a super user caps the user at 2",
    );
    let root = JobDeclaration::new(&runner_type)
        .triggered_by_user(user.actor_id())
        .with_priority(5);
    producer.declare_v2(&root).await;
    events
        .expect_one(wire::FACT_QUEUED, root.job_id, LONG)
        .await;

    // Given: a share lock on the user's maximum, as an in-flight admission holds while it reads
    // the cap — it only widens a window that exists without it
    let mut admission = durable.pool().begin().await.expect("a transaction opens");
    sqlx::query("SELECT 1 FROM user_max_priorities WHERE user_id = $1 FOR SHARE")
        .bind(user.actor_id())
        .execute(&mut *admission)
        .await
        .expect("a share lock on the user's maximum");

    // When: the super user raises the maximum to 4 while one of the user's jobs is admitted
    let in_window = JobDeclaration::new(&runner_type)
        .triggered_by_user(user.actor_id())
        .with_priority(5);
    let (raised, ()) = tokio::join!(
        priority::set_user_max_priority(&client, admin, user.actor_id(), 4),
        async {
            tokio::time::sleep(WINDOW).await;
            producer.declare_v2(&in_window).await;
            events
                .expect_one(wire::FACT_QUEUED, in_window.job_id, LONG)
                .await;
            admission.commit().await.expect("the lock is released");
        }
    );
    gql::expect_success(
        &raised,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a cap change racing an admission",
    );

    // Then: the job admitted in the window is capped by the new maximum
    for job_id in [root.job_id, in_window.job_id] {
        priority::assert_priorities(
            &priority::job(&client, admin, job_id).await,
            5,
            4,
            None,
            "a waiting job of the user ends at the new maximum, whenever it was admitted",
        );
    }

    // When: the super user lowers the maximum to 3 while the user's jobs and children of the
    // root are admitted in a burst
    let own_jobs: Vec<JobDeclaration> = (0..FAN_OUT / 2)
        .map(|_| {
            JobDeclaration::new(&runner_type)
                .triggered_by_user(user.actor_id())
                .with_priority(5)
        })
        .collect();
    let children: Vec<JobDeclaration> = (0..FAN_OUT / 2)
        .map(|_| {
            JobDeclaration::new(&runner_type)
                .with_parent(root.job_id)
                .with_priority(1)
        })
        .collect();
    let (lowered, _, _) = tokio::join!(
        async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            priority::set_user_max_priority(&client, admin, user.actor_id(), 3).await
        },
        join_all(own_jobs.iter().map(|job| producer.declare_v2(job))),
        join_all(children.iter().map(|child| producer.declare_v2(child)))
    );
    gql::expect_success(
        &lowered,
        wire::FIELD_SET_USER_MAX_PRIORITY,
        "a cap change racing a burst of admissions",
    );
    for declaration in own_jobs.iter().chain(&children) {
        events
            .expect_one(wire::FACT_QUEUED, declaration.job_id, LONG)
            .await;
    }

    // Then: every waiting job of the user ends at the new maximum, and every inheriting child at
    // its root's new effective priority
    for job_id in [root.job_id, in_window.job_id]
        .into_iter()
        .chain(own_jobs.iter().map(|job| job.job_id))
    {
        priority::assert_priorities(
            &priority::job(&client, admin, job_id).await,
            5,
            3,
            None,
            "a waiting job of the user ends at the lowered maximum",
        );
    }
    for child in &children {
        priority::assert_priorities(
            &priority::job(&client, admin, child.job_id).await,
            3,
            3,
            None,
            "an inheriting child ends at its root's new effective priority",
        );
    }
    assert_eq!(priority::my_max_priority(&client, &user).await, 3);

    durable.close().await;
    events.stop().await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn cancelling_or_forcing_a_root_while_grandchildren_are_admitted_never_deadlocks() {
    // Given: a type nobody serves
    let fixture = JobsFixture::start().await;
    let events = EventLog::open(fixture.fabric()).await;
    let client = fixture.gql();
    let admin = fixture.admin();
    let runner_type = wire::unique_runner_type("contended_tree");
    let producer = Producer::new(fixture.fabric(), "projects");

    for iteration in 0..TREES {
        let cancelling = iteration % 2 == 0;

        // Given: a root and a mid-level job under it
        let root = JobDeclaration::new(&runner_type);
        producer.declare_v2(&root).await;
        events
            .expect_one(wire::FACT_QUEUED, root.job_id, LONG)
            .await;
        let mid = JobDeclaration::new(&runner_type).with_parent(root.job_id);
        producer.declare_v2(&mid).await;
        events.expect_one(wire::FACT_QUEUED, mid.job_id, LONG).await;

        // When: the root is cancelled or forced while the mid-level job admits grandchildren
        let grandchildren: Vec<JobDeclaration> = (0..GRANDCHILDREN)
            .map(|_| JobDeclaration::new(&runner_type).with_parent(mid.job_id))
            .collect();
        let (gesture, _) = tokio::join!(
            async {
                if cancelling {
                    gql::cancel_job(&client, admin, Uuid::now_v7(), root.job_id).await
                } else {
                    priority::force_job_priority(&client, admin, root.job_id, 5).await
                }
            },
            join_all(grandchildren.iter().map(|job| producer.declare_v2(job)))
        );

        // Then: the gesture is acknowledged — a live root offers both — never an internal error
        let what = format!(
            "iteration {iteration}: {} a root while grandchildren are admitted",
            if cancelling { "cancelling" } else { "forcing" }
        );
        assert_not_internal(&gesture, &what);
        gql::expect_success(
            &gesture,
            if cancelling {
                wire::FIELD_CANCEL_JOB
            } else {
                wire::FIELD_FORCE_JOB_PRIORITY
            },
            &what,
        );

        // Then: every grandchild has one definite outcome, and none is left behind
        for grandchild in &grandchildren {
            let outcome = admission_outcome(&events, grandchild.job_id).await;
            match outcome {
                Admission::Queued if cancelling => {
                    gql::wait_for_status(&client, admin, grandchild.job_id, "CANCELLED", LONG)
                        .await;
                }
                Admission::Queued => priority::assert_priorities(
                    &priority::job(&client, admin, grandchild.job_id).await,
                    5,
                    5,
                    Some(5),
                    "a grandchild admitted around a force ends forced",
                ),
                Admission::Rejected(reason) => {
                    assert!(
                        cancelling,
                        "{what}: a force never makes a grandchild's admission refused ({reason})"
                    );
                    assert_eq!(
                        reason,
                        wire::REASON_PARENT_JOB_TERMINAL,
                        "{what}: a grandchild refused around a cancellation is refused for its \
                         terminal parent, under a domain code",
                    );
                }
            }
        }
    }

    // Then: the database never had to break a deadlock between those gestures
    let logs = fixture.logs();
    assert!(
        !logs.contains("deadlock detected"),
        "cancelling or forcing a root while its tree grows must never deadlock with the \
         admissions:\n{logs}"
    );

    // When: a child is declared under a parent already cancelled
    let parent = JobDeclaration::new(&runner_type);
    producer.declare_v2(&parent).await;
    events
        .expect_one(wire::FACT_QUEUED, parent.job_id, LONG)
        .await;
    gql::expect_success(
        &gql::cancel_job(&client, admin, Uuid::now_v7(), parent.job_id).await,
        wire::FIELD_CANCEL_JOB,
        "an administrator cancels the parent",
    );
    gql::wait_for_status(&client, admin, parent.job_id, "CANCELLED", LONG).await;
    let orphan = JobDeclaration::new(&runner_type).with_parent(parent.job_id);
    producer.declare_v2(&orphan).await;

    // Then: it is refused for its terminal parent, and no job exists
    let rejected = events
        .expect_one(wire::FACT_CREATION_REJECTED, orphan.job_id, LONG)
        .await;
    assert_eq!(
        rejected.payload()["reason_code"],
        json!(wire::REASON_PARENT_JOB_TERMINAL)
    );
    events
        .expect_none(wire::FACT_QUEUED, orphan.job_id, Duration::from_secs(1))
        .await;

    events.stop().await;
    fixture.shutdown().await;
}

enum Admission {
    Queued,
    Rejected(String),
}

async fn admission_outcome(events: &EventLog, job_id: Uuid) -> Admission {
    let decided = wait_until(LONG, || async {
        events.count_of(wire::FACT_QUEUED, job_id)
            + events.count_of(wire::FACT_CREATION_REJECTED, job_id)
            >= 1
    })
    .await;
    assert!(
        decided,
        "job {job_id} got no definite admission outcome within {LONG:?} — creation always ends \
         accepted or rejected"
    );
    let queued = events.count_of(wire::FACT_QUEUED, job_id);
    let rejected = events.of(wire::FACT_CREATION_REJECTED, job_id);
    assert_eq!(
        queued + rejected.len(),
        1,
        "job {job_id} has exactly one admission outcome"
    );
    match rejected.first() {
        Some(event) => Admission::Rejected(
            event.payload()["reason_code"]
                .as_str()
                .unwrap_or_default()
                .to_owned(),
        ),
        None => Admission::Queued,
    }
}

fn assert_not_internal(response: &Value, what: &str) {
    let code = response["errors"][0]["extensions"]["code"]
        .as_str()
        .unwrap_or_default();
    assert!(
        !code.to_ascii_uppercase().contains("INTERNAL"),
        "{what}: a gesture is acknowledged or refused with a domain code, never an internal \
         error: {response}"
    );
}
