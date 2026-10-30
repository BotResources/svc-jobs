use br_core_events::UserId;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::domain::attempts::{AttemptNumber, MaxAttempts};
use crate::domain::config::RunnerConfig;
use crate::domain::fleet::run_duration::MaxRunDuration;
use crate::domain::ids::{
    ClaimId, JobId, ManualRetryId, PlanDeclarationId, ResolutionId, RetryScheduleId, RunId,
    SourceEntityId,
};
use crate::domain::job::parts::{JobDeletion, ManualRetryRecord};
use crate::domain::job::resolution::JobResolution;
use crate::domain::job::{Job, JobState};
use crate::domain::keys::{DisplayName, InstanceKey, ProducerKey, RunnerTypeKey, StepLabel};
use crate::domain::ownership::JobOwner;
use crate::domain::passport::{JobPassport, PassportSubject};
use crate::domain::priority::{JobPriority, Priority};
use crate::domain::references::KnownUser;
use crate::domain::requester::Requester;
use crate::domain::run::failure::{RunFailureKind, RunFailureReport};
use crate::domain::run::parts::{
    RetrySchedule, RunReservation, RunStart, RunTerminal, RunnerInstanceReference,
};
use crate::domain::run::plan::{DeclarationNumber, RunPlan, RunPlanItem};
use crate::domain::run::step::{Step, StepIndex};
use crate::domain::run::{Run, RunState};

pub fn ts(offset: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000 + offset, 0).expect("fixture timestamp is in range")
}

pub fn job_id() -> JobId {
    JobId::new(Uuid::now_v7()).expect("uuid v7")
}

pub fn run_id() -> RunId {
    RunId::new(Uuid::now_v7()).expect("uuid v7")
}

pub fn resolution_id() -> ResolutionId {
    ResolutionId::new(Uuid::now_v7()).expect("uuid v7")
}

pub fn schedule_id() -> RetryScheduleId {
    RetryScheduleId::new(Uuid::now_v7()).expect("uuid v7")
}

pub fn manual_retry_id() -> ManualRetryId {
    ManualRetryId::new(Uuid::now_v7()).expect("uuid v7")
}

pub fn claim_id() -> ClaimId {
    ClaimId::new(Uuid::now_v7()).expect("not nil")
}

pub fn declaration_id() -> PlanDeclarationId {
    PlanDeclarationId::new(Uuid::now_v7()).expect("uuid v7")
}

pub fn source_entity_id() -> SourceEntityId {
    SourceEntityId::new(Uuid::now_v7()).expect("uuid v7")
}

pub fn runner_type() -> RunnerTypeKey {
    RunnerTypeKey::new("analyst").expect("valid runner type")
}

pub fn producer() -> ProducerKey {
    ProducerKey::new("projects").expect("valid producer")
}

pub fn instance() -> RunnerInstanceReference {
    RunnerInstanceReference::new(runner_type(), InstanceKey::new("pod-7").expect("valid key"))
}

pub fn service_passport() -> JobPassport {
    JobPassport::new(
        PassportSubject::Service(Uuid::from_u128(7)),
        None,
        serde_json::json!({ "kind": "service" }),
    )
    .expect("valid passport")
}

pub fn user_passport(known: &KnownUser) -> JobPassport {
    JobPassport::new(
        PassportSubject::User(known.id().0),
        Some(known.display_name().as_str().to_owned()),
        serde_json::json!({ "kind": "user" }),
    )
    .expect("valid passport")
}

pub fn user() -> KnownUser {
    KnownUser::new(
        UserId(Uuid::now_v7()),
        DisplayName::new("Operator").expect("valid name"),
    )
    .expect("uuid v7")
}

pub fn report(kind: RunFailureKind) -> RunFailureReport {
    RunFailureReport::platform(kind, "provider_unavailable").expect("valid report")
}

pub fn plan(labels: &[&str]) -> RunPlan {
    let items = labels
        .iter()
        .enumerate()
        .map(|(index, label)| {
            RunPlanItem::new(
                StepIndex::new(u32::try_from(index).expect("small plan")),
                StepLabel::new(label).expect("valid label"),
            )
        })
        .collect();
    RunPlan::new(declaration_id(), DeclarationNumber::FIRST, ts(0), items).expect("valid plan")
}

mod job_builder;
mod run_builder;

pub use job_builder::JobBuilder;
pub use run_builder::RunBuilder;
