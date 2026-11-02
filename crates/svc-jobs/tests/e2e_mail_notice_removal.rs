mod support;

use support::call_notices::wait_for_mail_notices;
use support::db::Durable;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail};
use support::gql;
use support::tree::correspondents;
use uuid::Uuid;

const MAIL_NOTICES_OF_RUN: &str =
    "SELECT count(*) FROM inbox_items WHERE kind = 'MAIL_NOTICE' AND run_id = $1";
const MAIL_NOTICES_AT: &str =
    "SELECT count(*) FROM inbox_items WHERE kind = 'MAIL_NOTICE' AND address = $1";

#[tokio::test]
async fn the_cancel_of_a_tree_removes_the_mail_notices_waiting_in_its_runs_and_at_its_addresses() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let tree = correspondents(&fixture, "mail-notice-removal").await;
    let refused = Mail::new(tree.sender, &[Uuid::now_v7()]);
    gesture::send_mail(
        &fixture.gql(),
        &runner_access(),
        &tree.parent_work,
        &refused,
    )
    .await;
    wait_for_mail_notices(&fixture, &tree.parent_work, Some(tree.sender), 1).await;
    durable
        .assert_count(
            MAIL_NOTICES_OF_RUN,
            tree.parent_work.run_id,
            1,
            "the mail notice waits in the Run",
        )
        .await;

    gql::cancel_job(&fixture.gql(), fixture.admin(), Uuid::now_v7(), tree.root).await;

    durable
        .await_count(
            MAIL_NOTICES_OF_RUN,
            tree.parent_work.run_id,
            0,
            "the cancel removes the mail notice with the Run",
        )
        .await;
    durable
        .assert_count(
            MAIL_NOTICES_AT,
            tree.sender,
            0,
            "no mail notice stays at the address of the removed Run",
        )
        .await;
    durable.close().await;
    fixture.shutdown().await;
}
