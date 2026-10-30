mod support;

use std::time::Duration;

use sqlx::Connection;
use support::LONG;
use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail, Work};
use support::infra;
use support::lineage::live_runner;
use support::producer::{JobDeclaration, Producer};
use support::restart::running_child;
use support::runner;
use uuid::Uuid;

const STARTS_OF_RUN: &str = "SELECT count(*) FROM run_starts WHERE run_id = $1";
const WAITING_ITEM_INSERTS: &str = "SELECT count(*) FROM pg_stat_activity \
     WHERE datname = current_database() AND wait_event_type = 'Lock' \
       AND query LIKE 'INSERT INTO inbox_items%'";

enum StartOutcome {
    Landed,
    TimedOutOnTheTreeLock,
}

async fn settled_start(
    fixture: &JobsFixture,
    durable: &Durable,
    run: Uuid,
    redelivered_before: i64,
) -> StartOutcome {
    let waited = tokio::time::Instant::now();
    loop {
        if durable.count(STARTS_OF_RUN, run).await == 1 {
            return StartOutcome::Landed;
        }
        if infra::status_frames_redelivered(fixture.nats()).await > redelivered_before {
            return StartOutcome::TimedOutOnTheTreeLock;
        }
        assert!(
            waited.elapsed() < LONG,
            "the start frame neither landed nor was refused for redelivery while a mail held the \
             Tree: the interleaving this scenario needs never happened and it would prove nothing"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn await_blocked_mail(durable: &Durable) {
    let waited = tokio::time::Instant::now();
    loop {
        let blocked: i64 = sqlx::query_scalar(WAITING_ITEM_INSERTS)
            .fetch_one(durable.pool())
            .await
            .expect("the lock waits read");
        if blocked >= 1 {
            return;
        }
        assert!(
            waited.elapsed() < LONG,
            "the mail never reached the delivery of its second recipient"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn await_unrelated_start_applied_after_the_refusal(fixture: &JobsFixture, durable: &Durable) {
    let mut other = live_runner(fixture, "other-tree").await;
    Producer::new(fixture.fabric(), "projects")
        .declare_v2(&JobDeclaration::new(&other.runner_type))
        .await;
    let trigger = other.next_trigger(LONG).await;
    other.start_run(&trigger).await;
    durable
        .await_count(
            STARTS_OF_RUN,
            runner::run_id(&trigger),
            1,
            "a start of another Tree is applied once the first start was refused on its Tree lock",
        )
        .await;
}

fn mail_ids(items: &[serde_json::Value]) -> Vec<String> {
    items
        .iter()
        .filter_map(|item| item["mailId"].as_str().map(str::to_owned))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mail_racing_the_start_of_a_restart_run_is_rung_or_listed_by_its_first_read() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let mut scene = running_child(&fixture, "ring-start-race").await;
    scene.finish_child(&durable).await;
    scene.mail_receiver(&fixture, scene.sender).await;
    let trigger = scene.pair.child.next_trigger(LONG).await;
    let restart_run = runner::run_id(&trigger);
    let restart_work = Work::of(&scene.pair.child, &trigger);

    let (racing_to, blocked) = if scene.receiver < scene.bystander {
        (scene.receiver, scene.bystander)
    } else {
        (scene.bystander, scene.receiver)
    };
    let mut holder = sqlx::PgConnection::connect(&fixture.app_url())
        .await
        .expect("a second connection opens");
    let mut held = holder
        .begin()
        .await
        .expect("the holder opens a transaction");
    sqlx::query("SELECT 1 FROM directory_agents WHERE agent_id = $1 FOR UPDATE")
        .bind(blocked)
        .execute(&mut *held)
        .await
        .expect("the holder locks the entry of the second recipient");

    let racing = Mail::new(scene.sender, &[racing_to, blocked]).with_body("racing");
    let racing_id = racing.mail_id.to_string();
    let gql = fixture.gql();
    let sender_work = scene.parent_work.clone();
    let send = tokio::spawn(async move {
        gesture::send_mail(&gql, &runner_access(), &sender_work, &racing).await
    });
    await_blocked_mail(&durable).await;

    let redelivered_before = infra::status_frames_redelivered(fixture.nats()).await;
    scene.pair.child.start_run(&trigger).await;
    let early_read = match settled_start(&fixture, &durable, restart_run, redelivered_before).await
    {
        StartOutcome::Landed => Some(gesture::items(
            &gesture::unread(
                &fixture.gql(),
                &runner_access(),
                &restart_work,
                Some(racing_to),
            )
            .await,
        )),
        StartOutcome::TimedOutOnTheTreeLock => {
            assert_eq!(
                durable.count(STARTS_OF_RUN, restart_run).await,
                0,
                "a start refused on the Tree lock has not been applied"
            );
            await_unrelated_start_applied_after_the_refusal(&fixture, &durable).await;
            None
        }
    };

    held.rollback().await.expect("the holder lets the mail go");
    holder.close().await.expect("the holder closes");
    gesture::accepted(
        &send.await.expect("the racing mail task ends"),
        "jobsSendMail",
    );
    durable
        .await_count(STARTS_OF_RUN, restart_run, 1, "the restart Run starts")
        .await;

    let racing_item: Uuid = sqlx::query_scalar(
        "SELECT item_id::uuid FROM inbox_items WHERE mail_id = $1 AND address = $2",
    )
    .bind(Uuid::parse_str(&racing_id).expect("a mail id"))
    .bind(racing_to)
    .fetch_one(durable.pool())
    .await
    .expect("the racing mail waits at the restarting address");
    let rung = durable.rings_of_item(racing_item).await;
    let listed_by_first_read = match early_read {
        Some(items) => mail_ids(&items).contains(&racing_id),
        None => mail_ids(&gesture::items(
            &gesture::unread(
                &fixture.gql(),
                &runner_access(),
                &restart_work,
                Some(racing_to),
            )
            .await,
        ))
        .contains(&racing_id),
    };
    assert!(
        listed_by_first_read || rung >= 1,
        "a mail accepted for an agent whose restart Run starts at the same time is either rung \
         to that Run or listed by the first read the Run makes after its start; here it was \
         neither (rings: {rung})"
    );
    durable.close().await;
    fixture.shutdown().await;
}
