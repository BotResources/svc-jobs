use uuid::Uuid;

use super::fixture::{JobsFixture, runner_access};
use super::gesture::{self, Mail, Work};
use super::lineage::{live_runner, started_with_agent};
use super::producer::{JobDeclaration, Producer};

pub async fn a_mail_to_is_refused_as_removed(fixture: &JobsFixture, label: &str, address: Uuid) {
    let mut sender = live_runner(fixture, label).await;
    let declared = JobDeclaration::new(&sender.runner_type).with_max_attempts(1);
    Producer::new(fixture.fabric(), "projects")
        .declare_v2(&declared)
        .await;
    let (trigger, from) = started_with_agent(fixture, &mut sender).await;
    let refused = gesture::send_mail(
        &fixture.gql(),
        &runner_access(),
        &Work::of(&sender, &trigger),
        &Mail::new(from, &[address]),
    )
    .await;
    assert_eq!(gesture::refusal(&refused, "jobsSendMail"), "bad_addresses");
    assert_eq!(
        gesture::field(&refused, "jobsSendMail")["badAddresses"][0]["reason"],
        "recipient_removed"
    );
}
