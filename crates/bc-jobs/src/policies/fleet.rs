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
        JobEvent::RunStarted(_) => vec![
            FleetSignal::JobStoppedWaiting,
            FleetSignal::JobBeganExecuting,
        ],
        JobEvent::RunCompleted(_) | JobEvent::RunFailed(_) | JobEvent::RunCancelled(_) => {
            vec![FleetSignal::JobStoppedExecuting]
        }
        JobEvent::JobCompleted(_) | JobEvent::JobFailed(_) | JobEvent::JobCancelled(_) => {
            vec![FleetSignal::JobStoppedWaiting]
        }
        JobEvent::RunDispatched(_)
        | JobEvent::RunPlanDeclared(_)
        | JobEvent::RunStepStarted(_)
        | JobEvent::RunCancellationRequested(_)
        | JobEvent::ManualRetryStarted(_)
        | JobEvent::JobDeleted(_)
        | JobEvent::JobAffordancesChanged(_) => vec![],
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunLostWithInstance {
    pub job_id: JobId,
    pub run_id: RunId,
}

pub fn runs_lost_with_instance(
    instance: &RunnerInstanceReference,
    jobs: &[Job],
) -> Vec<RunLostWithInstance> {
    jobs.iter()
        .filter(|job| !job.is_terminal())
        .filter_map(|job| {
            job.active_run()
                .filter(|run| run_belongs_to(run, instance))
                .map(|run| RunLostWithInstance {
                    job_id: job.id(),
                    run_id: run.id(),
                })
        })
        .collect()
}

fn run_belongs_to(run: &Run, instance: &RunnerInstanceReference) -> bool {
    run.start()
        .is_some_and(|start| start.instance() == instance)
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

    #[test]
    fn a_started_run_moves_its_job_from_waiting_to_executing() {
        // Given: a run that an instance just claimed
        let event = JobEvent::RunStarted(RunStarted {
            job_id: job_id(),
            run_id: run_id(),
            runner_type: runner_type(),
            instance_key: InstanceKey::new("pod-7").unwrap(),
        });
        // When/Then: the fleet view learns both halves of the transition
        assert_eq!(
            fleet_signals(&event),
            vec![
                FleetSignal::JobStoppedWaiting,
                FleetSignal::JobBeganExecuting
            ]
        );
    }

    #[test]
    fn a_finished_run_stops_the_job_executing_without_settling_the_job() {
        // Given: a run that completed while its job stays in progress
        let event = JobEvent::RunCompleted(RunCompleted {
            job_id: job_id(),
            run_id: run_id(),
            attempt_number: crate::domain::attempts::AttemptNumber::FIRST,
        });
        // When/Then: only the executing signal is emitted
        assert_eq!(
            fleet_signals(&event),
            vec![FleetSignal::JobStoppedExecuting]
        );
    }

    #[test]
    fn losing_an_instance_names_the_runs_it_was_executing() {
        // Given: two jobs, one running on the lost instance and one not started
        let running = RunBuilder::new(1).started(ts(5)).build();
        let running_id = running.id();
        let executing = JobBuilder::new().with_run(running).build();
        let waiting = JobBuilder::new()
            .with_run(RunBuilder::new(1).build())
            .build();
        // When: the loss is observed
        let intents = runs_lost_with_instance(&instance(), &[executing.clone(), waiting]);
        // Then: only the run that instance had claimed is reclaimed
        assert_eq!(
            intents,
            vec![RunLostWithInstance {
                job_id: executing.id(),
                run_id: running_id
            }]
        );
    }

    #[test]
    fn a_run_claimed_by_another_instance_is_left_alone() {
        // Given: a job executing on a different instance of the same type
        let executing = JobBuilder::new()
            .with_run(RunBuilder::new(1).started(ts(5)).build())
            .build();
        // When: an unrelated instance is lost
        let intents = runs_lost_with_instance(&instance_of("analyst", "pod-9"), &[executing]);
        // Then: nothing is reclaimed
        assert!(intents.is_empty());
    }

    #[test]
    fn a_run_claimed_by_a_namesake_instance_of_another_type_is_left_alone() {
        // Given: an analyst instance named pod-7 executing a run
        let executing = JobBuilder::new()
            .with_run(RunBuilder::new(1).started(ts(5)).build())
            .build();
        // When: a scribe instance that happens to share the name pod-7 is lost
        let intents = runs_lost_with_instance(&instance_of("scribe", "pod-7"), &[executing]);
        // Then: the analyst's work is untouched — an instance is a type and a key, never a key alone
        assert!(intents.is_empty());
    }
}
