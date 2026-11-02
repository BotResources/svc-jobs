mod support;

use support::auto_finish::{
    background, becomes_completed, complete, mark_the_mail, register_author, send_answer, stays,
};
use support::call_notices::caller;
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Mail};
use support::surface::{self, Agent};

#[tokio::test]
async fn the_answer_is_the_last_direct_mail_of_the_run_not_the_first() {
    let fixture = JobsFixture::start().await;
    let mut caller = caller(&fixture, "auto-last-mail").await;
    let first_read = background(&fixture, &mut caller).await;
    let last_read = background(&fixture, &mut caller).await;
    let first_author = register_author(&fixture, &caller, &first_read).await;
    let last_author = register_author(&fixture, &caller, &last_read).await;

    let early = send_answer(&fixture, &caller, &first_read, first_author).await;
    send_answer(&fixture, &caller, &first_read, first_author).await;
    send_answer(&fixture, &caller, &last_read, last_author).await;
    let late = send_answer(&fixture, &caller, &last_read, last_author).await;
    mark_the_mail(&fixture, &caller, early).await;
    mark_the_mail(&fixture, &caller, late).await;
    complete(&caller, &first_read).await;
    complete(&caller, &last_read).await;

    caller.pair.parent.complete_run(&caller.trigger).await;

    becomes_completed(&fixture, &[last_read.job]).await;
    stays(&fixture, &[first_read.job], "IN_PROGRESS").await;
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_later_copy_of_a_mail_from_the_run_does_not_replace_its_direct_answer() {
    let fixture = JobsFixture::start().await;
    let mut caller = caller(&fixture, "auto-copy-mail").await;
    let called = background(&fixture, &mut caller).await;
    let (author, colleague) = (Agent::with_inbox(), Agent::with_inbox());
    let (author_id, colleague_id) = (author.agent_id, colleague.agent_id);
    surface::register(
        fixture.nats(),
        &caller.pair.child,
        called.work.run_id,
        &[author, colleague],
    )
    .await;

    let answer = send_answer(&fixture, &caller, &called, author_id).await;
    let copied = Mail::new(author_id, &[colleague_id]).cc(&[caller.address]);
    gesture::accepted(
        &gesture::send_mail(&fixture.gql(), &runner_access(), &called.work, &copied).await,
        "jobsSendMail",
    );
    mark_the_mail(&fixture, &caller, answer).await;
    complete(&caller, &called).await;

    caller.pair.parent.complete_run(&caller.trigger).await;

    becomes_completed(&fixture, &[called.job]).await;
    fixture.shutdown().await;
}
