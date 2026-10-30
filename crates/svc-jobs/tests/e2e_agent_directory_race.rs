mod support;

use support::db::{self, Durable};
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::surface::{self, Agent};
use support::{LONG, dispatch, wire};

const ROUNDS: usize = 25;

#[tokio::test]
async fn a_registration_racing_the_failure_of_its_run_never_leaves_an_agent_behind() {
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let producer = Producer::new(fixture.fabric(), "projects");
    let runner_type = wire::unique_runner_type("directory-race");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a")
        .declaring_capacity(u32::try_from(ROUNDS).unwrap_or(u32::MAX));
    instance.connect().await;
    dispatch::await_live_instances(&client, fixture.admin(), &runner_type, 1, LONG).await;
    let durable = Durable::open(&fixture.app_url()).await;
    let nats = fixture.nats();

    for _ in 0..ROUNDS {
        producer
            .declare_v2(&JobDeclaration::new(&runner_type).with_max_attempts(1))
            .await;
        let trigger = instance.next_trigger(LONG).await;
        instance.start_applied(&fixture, &trigger).await;
        let run = runner::run_id(&trigger);

        let registrations = async {
            let mut replies = Vec::new();
            for _ in 0..3 {
                replies.push(surface::register(nats, &instance, run, &[Agent::with_inbox()]).await);
            }
            replies
        };
        let (replies, ()) = tokio::join!(registrations, async {
            instance
                .fail_run(&trigger, "PERMANENT", "provider_refused", None)
                .await;
        });
        for reply in &replies {
            assert!(
                reply["accepted"] == true
                    || matches!(
                        reply["reason"].as_str(),
                        Some("run_not_started" | "run_removed" | "run_ended")
                    ),
                "{reply}"
            );
        }
        durable
            .await_count(db::TERMINALS_OF_RUN, run, 1, "the Run ends")
            .await;
        durable
            .assert_count(
                db::AGENTS_OF_RUN,
                run,
                0,
                "a failed Run leaves no agent in the directory",
            )
            .await;
    }
    durable.close().await;
    fixture.shutdown().await;
}
