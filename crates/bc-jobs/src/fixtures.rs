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

pub struct RunBuilder {
    state: RunState,
}

impl RunBuilder {
    pub fn new(attempt: u32) -> Self {
        let attempt_number = AttemptNumber::new(attempt).expect("positive attempt");
        Self {
            state: RunState {
                id: run_id(),
                attempt_number,
                dispatched_at: ts(0),
                automatic_retry_schedule_id: (!attempt_number.is_first()).then(schedule_id),
                max_duration: None,
                reservation: None,
                start: None,
                terminal: None,
                plan: None,
                steps: vec![],
                retry_schedule: None,
                cancellation_request: None,
            },
        }
    }

    pub fn retry_of_schedule(mut self, id: RetryScheduleId) -> Self {
        self.state.automatic_retry_schedule_id = Some(id);
        self
    }

    pub fn started(self, at: DateTime<Utc>) -> Self {
        self.started_on(instance(), at)
    }

    pub fn reserved(self, at: DateTime<Utc>) -> Self {
        self.reserved_on(instance(), at)
    }

    pub fn reserved_on(mut self, instance: RunnerInstanceReference, at: DateTime<Utc>) -> Self {
        self.state.reservation = Some(RunReservation::new(instance, claim_id(), at));
        self
    }

    pub fn with_max_duration(mut self, seconds: i64) -> Self {
        self.state.max_duration =
            Some(MaxRunDuration::from_seconds(seconds).expect("positive duration"));
        self
    }

    pub fn started_on(mut self, instance: RunnerInstanceReference, at: DateTime<Utc>) -> Self {
        self.state.start = Some(RunStart::new(instance, at));
        self
    }

    pub fn completed(mut self, at: DateTime<Utc>) -> Self {
        self.state.terminal = Some(RunTerminal::completed(at));
        self
    }

    pub fn cancelled(mut self, at: DateTime<Utc>) -> Self {
        self.state.terminal = Some(RunTerminal::cancelled(at));
        self
    }

    pub fn withdrawn(mut self, at: DateTime<Utc>, reason: &str) -> Self {
        self.state.terminal = Some(RunTerminal::withdrawn(
            at,
            crate::domain::keys::ReasonCode::new(reason).unwrap(),
        ));
        self
    }

    pub fn failed(self, at: DateTime<Utc>, kind: RunFailureKind) -> Self {
        self.failed_with(at, report(kind))
    }

    pub fn failed_with(mut self, at: DateTime<Utc>, failure: RunFailureReport) -> Self {
        self.state.terminal = Some(RunTerminal::failed(at, failure));
        self
    }

    pub fn retry_due(mut self, at: DateTime<Utc>) -> Self {
        self.state.retry_schedule = Some(RetrySchedule::new(schedule_id(), at));
        self
    }

    pub fn with_plan(mut self, plan: RunPlan) -> Self {
        self.state.plan = Some(plan);
        self
    }

    pub fn with_step(mut self, index: u32, label: &str, at: DateTime<Utc>) -> Self {
        self.state.steps.push(Step::new(
            StepIndex::new(index),
            StepLabel::new(label).expect("valid label"),
            at,
        ));
        self
    }

    pub fn build(self) -> Run {
        Run::hydrate(self.state).expect("fixture run is valid")
    }
}

pub struct JobBuilder {
    state: JobState,
}

impl Default for JobBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl JobBuilder {
    pub fn new() -> Self {
        Self {
            state: JobState {
                id: job_id(),
                runner_type: runner_type(),
                producer: producer(),
                config: None,
                parent_job_id: None,
                predecessor_job_id: None,
                triggered_by: None,
                source_entity_id: None,
                max_attempts: None,
                priority: JobPriority::default(),
                created_at: ts(0),
                owner: JobOwner::Producer(producer()),
                runs: vec![],
                resolution: None,
                deletion: None,
                manual_retry: None,
                initial_job_id: None,
                requester: Requester::Service(None),
                passport: Some(service_passport()),
                payload: None,
                call: None,
                restart_of_run_id: None,
            },
        }
    }

    pub fn with_id(mut self, id: JobId) -> Self {
        self.state.id = id;
        self
    }

    pub fn with_runner_type(mut self, key: &str) -> Self {
        self.state.runner_type = RunnerTypeKey::new(key).expect("valid runner type");
        self
    }

    pub fn with_config(mut self, config: RunnerConfig) -> Self {
        self.state.config = Some(config);
        self
    }

    pub fn with_requester(mut self, requester: Requester) -> Self {
        self.state.requester = requester;
        self
    }

    pub fn with_passport(mut self, passport: JobPassport) -> Self {
        self.state.passport = Some(passport);
        self
    }

    pub fn with_initial_job(mut self, initial_job_id: JobId) -> Self {
        self.state.initial_job_id = Some(initial_job_id);
        self
    }

    pub fn with_parent(mut self, parent_job_id: JobId) -> Self {
        self.state.parent_job_id = Some(parent_job_id);
        self.state.initial_job_id = Some(parent_job_id);
        self.state.owner = JobOwner::Runner {
            parent_job_id,
            runner_type: runner_type(),
        };
        self
    }

    pub fn with_restart_of(mut self, run_id: RunId) -> Self {
        self.state.restart_of_run_id = Some(run_id);
        self
    }

    pub fn with_predecessor(mut self, predecessor_job_id: JobId) -> Self {
        self.state.predecessor_job_id = Some(predecessor_job_id);
        self
    }

    pub fn with_max_attempts(mut self, value: u32) -> Self {
        self.state.max_attempts = Some(MaxAttempts::new(value).expect("positive budget"));
        self
    }

    pub fn with_priority(
        mut self,
        requested: Priority,
        effective: Priority,
        forced: Option<Priority>,
    ) -> Self {
        self.state.priority =
            JobPriority::hydrate(requested, effective, forced).expect("valid fixture priority");
        self
    }

    pub fn with_triggered_by(mut self, user: KnownUser) -> Self {
        self.state.triggered_by = Some(user);
        self
    }

    pub fn with_source(mut self, entity_id: SourceEntityId) -> Self {
        self.state.source_entity_id = Some(entity_id);
        self
    }

    pub fn with_run(mut self, run: Run) -> Self {
        self.state.runs.push(run);
        self
    }

    pub fn with_resolution(mut self, resolution: JobResolution) -> Self {
        self.state.resolution = Some(resolution);
        self
    }

    pub fn with_manual_retry(mut self, record: ManualRetryRecord) -> Self {
        self.state.manual_retry = Some(record);
        self
    }

    pub fn manually_retried_by(
        self,
        successor_job_id: JobId,
        successor_settled_at_load: bool,
    ) -> Self {
        let failed_resolution_id = self
            .state
            .resolution
            .as_ref()
            .map_or_else(resolution_id, JobResolution::id);
        self.with_manual_retry(ManualRetryRecord::new(
            manual_retry_id(),
            failed_resolution_id,
            successor_job_id,
            user(),
            ts(60),
            successor_settled_at_load,
        ))
    }

    pub fn deleted(mut self, at: DateTime<Utc>) -> Self {
        self.state.deletion = Some(JobDeletion::new(user(), at));
        self
    }

    pub fn state(self) -> JobState {
        self.state
    }

    pub fn build(self) -> Job {
        Job::hydrate(self.state).expect("fixture job is valid")
    }
}
