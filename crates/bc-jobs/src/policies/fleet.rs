use chrono::{DateTime, Utc};

use crate::domain::ids::{JobId, RunId};
use crate::domain::job::Job;
use crate::domain::run::Run;
use crate::domain::run::parts::RunnerInstanceReference;
use crate::event::job::JobEvent;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FleetSignal {
    JobBeganWaiting,
    JobStoppedWaiting,
    JobBeganExecuting,
    JobStoppedExecuting,
}

impl FleetSignal {
    pub fn as_db_str(&self) -> &'static str {
        match self {
            Self::JobBeganWaiting => "JOB_BEGAN_WAITING",
            Self::JobStoppedWaiting => "JOB_STOPPED_WAITING",
            Self::JobBeganExecuting => "JOB_BEGAN_EXECUTING",
            Self::JobStoppedExecuting => "JOB_STOPPED_EXECUTING",
        }
    }
}

pub fn fleet_signals(event: &JobEvent) -> Vec<FleetSignal> {
    match event {
        JobEvent::JobQueued(_) | JobEvent::RetryScheduled(_) => vec![FleetSignal::JobBeganWaiting],
        JobEvent::RunDispatched(_) => vec![FleetSignal::JobStoppedWaiting],
        JobEvent::RunStarted(_) => vec![FleetSignal::JobBeganExecuting],
        JobEvent::RunCancelled(fact) if fact.withdrawn_for.is_some() => {
            vec![FleetSignal::JobBeganWaiting]
        }
        JobEvent::RunCompleted(_) | JobEvent::RunFailed(_) | JobEvent::RunCancelled(_) => {
            vec![FleetSignal::JobStoppedExecuting]
        }
        JobEvent::JobCompleted(_) | JobEvent::JobFailed(_) | JobEvent::JobCancelled(_) => {
            vec![FleetSignal::JobStoppedWaiting]
        }
        JobEvent::RunPlanDeclared(_)
        | JobEvent::RunStepStarted(_)
        | JobEvent::RunCancellationRequested(_)
        | JobEvent::ManualRetryStarted(_)
        | JobEvent::JobDeleted(_)
        | JobEvent::JobAffordancesChanged(_)
        | JobEvent::JobPriorityForced(_)
        | JobEvent::JobPriorityRecalculated(_) => vec![],
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunLostWithInstance {
    pub job_id: JobId,
    pub run_id: RunId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LostPresenceSession {
    instance: RunnerInstanceReference,
    connected_at: DateTime<Utc>,
    disconnected_at: DateTime<Utc>,
}

impl LostPresenceSession {
    pub fn new(
        instance: RunnerInstanceReference,
        connected_at: DateTime<Utc>,
        disconnected_at: DateTime<Utc>,
    ) -> Self {
        Self {
            instance,
            connected_at,
            disconnected_at,
        }
    }

    fn carried(&self, run: &Run) -> bool {
        match (run.start(), run.reservation()) {
            (Some(start), _) => self.held(start.instance(), start.started_at()),
            (None, Some(reservation)) => {
                self.held(reservation.instance(), reservation.reserved_at())
            }
            (None, None) => false,
        }
    }

    fn held(&self, instance: &RunnerInstanceReference, at: DateTime<Utc>) -> bool {
        instance == &self.instance && at >= self.connected_at && at <= self.disconnected_at
    }
}

pub fn runs_lost_with_instance(
    session: &LostPresenceSession,
    jobs: &[Job],
) -> Vec<RunLostWithInstance> {
    jobs.iter()
        .filter_map(|job| {
            job.active_run()
                .filter(|run| session.carried(run))
                .map(|run| RunLostWithInstance {
                    job_id: job.id(),
                    run_id: run.id(),
                })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::keys::{InstanceKey, RunnerTypeKey};
    use crate::event::job_facts::{RunCompleted, RunStarted};
    use crate::fixtures::{JobBuilder, RunBuilder, instance, job_id, run_id, runner_type, ts};

    fn instance_of(runner_type: &str, instance_key: &str) -> RunnerInstanceReference {
        RunnerInstanceReference::new(
            RunnerTypeKey::new(runner_type).unwrap(),
            InstanceKey::new(instance_key).unwrap(),
        )
    }

    fn session(instance: RunnerInstanceReference) -> LostPresenceSession {
        LostPresenceSession::new(instance, ts(0), ts(10))
    }

    #[test]
    fn a_started_run_moves_its_job_into_executing() {
        let event = JobEvent::RunStarted(RunStarted {
            job_id: job_id(),
            run_id: run_id(),
            runner_type: runner_type(),
            instance_key: InstanceKey::new("pod-7").unwrap(),
        });
        assert_eq!(fleet_signals(&event), vec![FleetSignal::JobBeganExecuting]);
    }

    #[test]
    fn a_finished_run_stops_the_job_executing_without_settling_the_job() {
        let event = JobEvent::RunCompleted(RunCompleted {
            job_id: job_id(),
            run_id: run_id(),
            attempt_number: crate::domain::attempts::AttemptNumber::FIRST,
        });
        assert_eq!(
            fleet_signals(&event),
            vec![FleetSignal::JobStoppedExecuting]
        );
    }

    #[test]
    fn losing_an_instance_names_the_runs_it_was_executing() {
        let running = RunBuilder::new(1).started(ts(5)).build();
        let running_id = running.id();
        let executing = JobBuilder::new().with_run(running).build();
        let waiting = JobBuilder::new()
            .with_run(RunBuilder::new(1).build())
            .build();
        let intents = runs_lost_with_instance(&session(instance()), &[executing.clone(), waiting]);
        assert_eq!(
            intents,
            vec![RunLostWithInstance {
                job_id: executing.id(),
                run_id: running_id
            }]
        );
    }

    #[test]
    fn a_finished_job_whose_run_is_still_live_loses_that_run_with_its_instance() {
        use crate::domain::job::resolution::JobResolution;
        use crate::fixtures::resolution_id;
        let running = RunBuilder::new(1).started(ts(5)).build();
        let running_id = running.id();
        let finished = JobBuilder::new()
            .with_run(running)
            .with_resolution(JobResolution::completed(resolution_id(), ts(30)))
            .build();
        let intents =
            runs_lost_with_instance(&session(instance()), std::slice::from_ref(&finished));
        assert_eq!(
            intents,
            vec![RunLostWithInstance {
                job_id: finished.id(),
                run_id: running_id
            }]
        );
    }

    #[test]
    fn a_run_claimed_by_another_instance_is_left_alone() {
        let executing = JobBuilder::new()
            .with_run(RunBuilder::new(1).started(ts(5)).build())
            .build();
        let intents =
            runs_lost_with_instance(&session(instance_of("analyst", "pod-9")), &[executing]);
        assert!(intents.is_empty());
    }

    #[test]
    fn a_run_claimed_by_a_namesake_instance_of_another_type_is_left_alone() {
        let executing = JobBuilder::new()
            .with_run(RunBuilder::new(1).started(ts(5)).build())
            .build();
        let intents =
            runs_lost_with_instance(&session(instance_of("scribe", "pod-7")), &[executing]);
        assert!(intents.is_empty());
    }

    #[test]
    fn a_run_started_under_the_replacement_session_survives_its_predecessors_reclaim() {
        let executing = JobBuilder::new()
            .with_run(RunBuilder::new(1).started(ts(50)).build())
            .build();
        let intents = runs_lost_with_instance(&session(instance()), &[executing]);
        assert!(intents.is_empty());
    }

    #[test]
    fn a_run_the_instance_started_before_this_session_opened_is_left_alone() {
        let executing = JobBuilder::new()
            .with_run(RunBuilder::new(1).started(ts(5)).build())
            .build();
        let lost = LostPresenceSession::new(instance(), ts(20), ts(30));
        let intents = runs_lost_with_instance(&lost, &[executing]);
        assert!(intents.is_empty());
    }

    #[test]
    fn losing_an_instance_reclaims_the_run_it_had_claimed_but_not_started() {
        let reserved = RunBuilder::new(1).reserved(ts(5)).build();
        let reserved_id = reserved.id();
        let claimed = JobBuilder::new().with_run(reserved).build();
        let intents = runs_lost_with_instance(&session(instance()), std::slice::from_ref(&claimed));
        assert_eq!(
            intents,
            vec![RunLostWithInstance {
                job_id: claimed.id(),
                run_id: reserved_id
            }]
        );
    }
}
