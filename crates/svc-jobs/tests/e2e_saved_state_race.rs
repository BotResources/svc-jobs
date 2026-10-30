mod support;

use serde_json::json;
use support::db::{self, Durable};
use support::fixture::{JobsFixture, runner_access};
use support::gesture::{self, Work};
use support::producer::{JobDeclaration, Producer};
use support::runner::{self, FakeRunner};
use support::surface::{self, Agent};
use support::{LONG, dispatch, wire};

const ROUNDS: usize = 25;
const SAVES: usize = 4;

#[tokio::test]
async fn a_save_racing_the_failure_of_its_run_never_leaves_a_state_behind() {
    let fixture = JobsFixture::start().await;
    let client = fixture.gql();
    let producer = Producer::new(fixture.fabric(), "projects");
    let runner_type = wire::unique_runner_type("saved-state-race");
    let mut instance = FakeRunner::new(fixture.nats(), &runner_type, "instance-a")
        .declaring_capacity(u32::try_from(ROUNDS).unwrap_or(u32::MAX));
    instance.connect().await;
    dispatch::await_live_instances(&client, fixture.admin(), &runner_type, 1, LONG).await;
    let durable = Durable::open(&fixture.app_url()).await;

    for _ in 0..ROUNDS {
        producer
            .declare_v2(&JobDeclaration::new(&runner_type).with_max_attempts(1))
            .await;
        let trigger = instance.next_trigger(LONG).await;
        instance.start_applied(&fixture, &trigger).await;
        let run = runner::run_id(&trigger);
        let registered =
            surface::register(fixture.nats(), &instance, run, &[Agent::with_inbox()]).await;
        assert_eq!(registered["accepted"], true, "{registered}");
        let work = Work::of(&instance, &trigger);

        let saves = async {
            let mut replies = Vec::new();
            for index in 0..SAVES {
                replies.push(
                    client
                        .query(
                            &runner_access(),
                            gesture::SAVE_STATE,
                            json!({ "input": { "work": work.wire(), "state": { "step": index } } }),
                        )
                        .await,
                );
            }
            replies
        };
        let (replies, ()) = tokio::join!(saves, async {
            instance
                .fail_run(&trigger, "PERMANENT", "provider_refused", None)
                .await;
        });
        for reply in &replies {
            let outcome = &gesture::field(reply, "jobsSaveState");
            assert!(
                outcome["success"] == true
                    || matches!(
                        outcome["reason"].as_str(),
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
                db::SAVED_STATES_OF_RUN,
                run,
                0,
                "a failed Run keeps no saved state",
            )
            .await;
        durable
            .assert_count(
                db::SAVED_STATE_AGENTS_OF_RUN,
                run,
                0,
                "a failed Run keeps no saved agent list",
            )
            .await;
    }
    durable.close().await;
    fixture.shutdown().await;
}
