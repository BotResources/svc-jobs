use bc_jobs::JobsError;
use bc_jobs::commands::fleet::{ObserveLoss, ObservePresence, observe_loss, observe_presence};
use bc_jobs::domain::fleet::capacity::Capacity;
use bc_jobs::domain::fleet::status::ReportedStatus;
use bc_jobs::domain::ids::{PresenceSessionId, RunnerTypeId};
use bc_jobs::domain::keys::{InstanceKey, ReasonCode, RunnerTypeKey, RunnerVersion};
use bc_jobs::ports::environment::{Clock, IdFactory};
use bc_jobs::ports::fleet::{FleetReader, OpenPresenceSession};
use contract_jobs::runner as wire;

use super::{Jobs, reclaim, service_metadata, write};
use crate::db::PgStore;
use crate::error::ServiceError;

pub async fn observed(jobs: &Jobs, presence: &wire::Presence) -> Result<(), ServiceError> {
    match accepted(jobs, presence).await {
        Err(ServiceError::Domain(refusal)) => refused(jobs, presence, &refusal).await,
        outcome => outcome,
    }
}

async fn accepted(jobs: &Jobs, presence: &wire::Presence) -> Result<(), ServiceError> {
    let key = RunnerTypeKey::new(&presence.runner_type)?;
    let instance_key = InstanceKey::new(&presence.instance_key)?;
    let version = RunnerVersion::new(&presence.runner_version)?;
    let reported_status = ReportedStatus::new(presence.status.as_str())?;
    let capacity = Capacity::new(presence.capacity.get())?;
    for _ in 0..PRESENCE_DECISION_ATTEMPTS {
        let known = FleetReader::load(&jobs.store, &key).await?;
        let runner_type_id = match &known {
            Some(runner_type) => runner_type.id(),
            None => RunnerTypeId::new(
                jobs.store
                    .runner_type_route_id(&key)
                    .await?
                    .unwrap_or_else(|| jobs.ids.next()),
            )?,
        };
        let result = observe_presence(
            known.as_ref(),
            ObservePresence {
                runner_type_id,
                runner_type: key.clone(),
                instance_key: instance_key.clone(),
                session_id: PresenceSessionId::new(jobs.ids.next())?,
                version: version.clone(),
                reported_status,
                capacity,
            },
        )?;
        if result.events.is_empty() {
            return Ok(());
        }
        match write::commit_fleet_events(
            &jobs.store,
            jobs.ids.as_ref(),
            write::FleetChange {
                runner_type_id,
                runner_type: &key,
                decided_on: known.as_ref(),
                events: &result.events,
            },
            &service_metadata(),
            jobs.clock.now(),
        )
        .await
        {
            Ok(()) => {
                jobs.project_runner_type_best_effort(&key).await;
                return Ok(());
            }
            Err(ServiceError::Contended) => continue,
            Err(other) => return Err(other),
        }
    }
    Err(ServiceError::Contended)
}

async fn refused(
    jobs: &Jobs,
    presence: &wire::Presence,
    refusal: &JobsError,
) -> Result<(), ServiceError> {
    tracing::warn!(
        runner_type = %presence.runner_type,
        instance_key = %presence.instance_key,
        refusal_code = refusal.code(),
        refusal_params = %refusal.params(),
        "a presence entry the domain refuses is never taken for READY: a live instance is drained \
         so it takes no new work while keeping the runs it holds, and an entry naming no live \
         instance is ignored"
    );
    drained(jobs, &presence.runner_type, &presence.instance_key).await
}

pub async fn drained(
    jobs: &Jobs,
    runner_type: &str,
    instance_key: &str,
) -> Result<(), ServiceError> {
    let key = RunnerTypeKey::new(runner_type)?;
    let instance_key = InstanceKey::new(instance_key)?;
    for _ in 0..PRESENCE_DECISION_ATTEMPTS {
        let Some(known) = FleetReader::load(&jobs.store, &key).await? else {
            return Ok(());
        };
        let Some(live) = known.instance(&instance_key) else {
            return Ok(());
        };
        let result = observe_presence(
            Some(&known),
            ObservePresence {
                runner_type_id: known.id(),
                runner_type: key.clone(),
                instance_key: instance_key.clone(),
                session_id: live.session_id(),
                version: live.version().clone(),
                reported_status: ReportedStatus::Draining,
                capacity: live.capacity(),
            },
        )?;
        if result.events.is_empty() {
            return Ok(());
        }
        tracing::warn!(
            runner_type = %key.as_str(),
            instance_key = %instance_key.as_str(),
            "a live instance whose presence entry this service refuses is recorded as draining"
        );
        match write::commit_fleet_events(
            &jobs.store,
            jobs.ids.as_ref(),
            write::FleetChange {
                runner_type_id: known.id(),
                runner_type: &key,
                decided_on: Some(&known),
                events: &result.events,
            },
            &service_metadata(),
            jobs.clock.now(),
        )
        .await
        {
            Ok(()) => {
                jobs.project_runner_type_best_effort(&key).await;
                return Ok(());
            }
            Err(ServiceError::Contended) => continue,
            Err(other) => return Err(other),
        }
    }
    Err(ServiceError::Contended)
}

const PRESENCE_DECISION_ATTEMPTS: usize = 5;

pub struct ObservedLoss<'a> {
    pub runner_type: &'a RunnerTypeKey,
    pub instance_key: &'a InstanceKey,
    pub reason_code: &'a ReasonCode,
    pub session_id: Option<PresenceSessionId>,
}

pub async fn lost(
    jobs: &Jobs,
    runner_type: &str,
    instance_key: &str,
    reason_code: &str,
) -> Result<(), ServiceError> {
    let key = RunnerTypeKey::new(runner_type)?;
    let instance_key = InstanceKey::new(instance_key)?;
    let reason_code = ReasonCode::new(reason_code)?;
    settle(
        jobs,
        ObservedLoss {
            runner_type: &key,
            instance_key: &instance_key,
            reason_code: &reason_code,
            session_id: None,
        },
    )
    .await
}

pub async fn lost_session(
    jobs: &Jobs,
    session: &OpenPresenceSession,
    reason_code: &ReasonCode,
) -> Result<(), ServiceError> {
    settle(
        jobs,
        ObservedLoss {
            runner_type: &session.runner_type,
            instance_key: &session.instance_key,
            reason_code,
            session_id: Some(session.session_id),
        },
    )
    .await
}

async fn settle(jobs: &Jobs, observed: ObservedLoss<'_>) -> Result<(), ServiceError> {
    let runner_type = observed.runner_type.clone();
    let instance_key = observed.instance_key.clone();
    let closed_by_this_pod = record_loss(
        &jobs.store,
        jobs.ids.as_ref(),
        jobs.clock.as_ref(),
        observed,
    )
    .await?;
    let Some(session_id) = closed_by_this_pod else {
        return Ok(());
    };
    reclaim::runs_of(jobs, &runner_type, &instance_key, session_id).await
}

pub async fn record_loss(
    store: &PgStore,
    ids: &dyn IdFactory,
    clock: &dyn Clock,
    observed: ObservedLoss<'_>,
) -> Result<Option<PresenceSessionId>, ServiceError> {
    let mut pinned = observed.session_id;
    for _ in 0..PRESENCE_DECISION_ATTEMPTS {
        let Some(known) = FleetReader::load(store, observed.runner_type).await? else {
            return Ok(None);
        };
        let Some(live) = known.instance(observed.instance_key) else {
            return Ok(None);
        };
        let session_id = *pinned.get_or_insert(live.session_id());
        let result = match observe_loss(
            &known,
            ObserveLoss {
                instance_key: observed.instance_key.clone(),
                session_id,
                reason_code: observed.reason_code.clone(),
            },
        ) {
            Ok(result) => result,
            Err(JobsError::StaleLoss { .. }) => {
                tracing::debug!(
                    runner_type = %observed.runner_type.as_str(),
                    instance_key = %observed.instance_key.as_str(),
                    session_id = %session_id.as_uuid(),
                    "the session this loss was observed on is already closed and the instance is \
                     live again; the loss belongs to whoever closed it"
                );
                return Ok(None);
            }
            Err(other) => return Err(other.into()),
        };
        match write::commit_fleet_events(
            store,
            ids,
            write::FleetChange {
                runner_type_id: known.id(),
                runner_type: observed.runner_type,
                decided_on: Some(&known),
                events: &result.events,
            },
            &service_metadata(),
            clock.now(),
        )
        .await
        {
            Ok(()) => return Ok(Some(session_id)),
            Err(ServiceError::Contended) => continue,
            Err(other) => return Err(other),
        }
    }
    tracing::error!(
        runner_type = %observed.runner_type.as_str(),
        instance_key = %observed.instance_key.as_str(),
        attempts = PRESENCE_DECISION_ATTEMPTS,
        "an instance loss found the fleet moving under every attempt; its presence session stays \
         open and its runs wait for the backstop instead of being reclaimed"
    );
    Err(ServiceError::Contended)
}
