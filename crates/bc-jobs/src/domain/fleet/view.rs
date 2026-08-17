use crate::domain::fleet::RunnerType;
use crate::domain::fleet::capacity::Capacity;
use crate::domain::fleet::instance::RunnerInstance;
use crate::domain::ids::RunId;
use crate::domain::job::Job;
use crate::domain::run::Run;
use crate::domain::run::parts::RunnerInstanceReference;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceLoad {
    instance: RunnerInstanceReference,
    capacity: Capacity,
    current_run_ids: Vec<RunId>,
}

impl InstanceLoad {
    pub fn instance(&self) -> &RunnerInstanceReference {
        &self.instance
    }

    pub fn current_run_ids(&self) -> &[RunId] {
        &self.current_run_ids
    }

    pub fn capacity(&self) -> Capacity {
        self.capacity
    }

    pub fn is_busy(&self) -> bool {
        self.capacity.is_saturated_by(self.current_run_ids.len())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetView {
    instances: Vec<InstanceLoad>,
    total_capacity: u32,
    waiting_job_count: u32,
    executing_job_count: u32,
}

impl FleetView {
    pub fn instances(&self) -> &[InstanceLoad] {
        &self.instances
    }

    pub fn busy_instance_count(&self) -> u32 {
        count(self.instances.iter().filter(|load| load.is_busy()).count())
    }

    pub fn idle_instance_count(&self) -> u32 {
        count(self.instances.iter().filter(|load| !load.is_busy()).count())
    }

    pub fn total_capacity(&self) -> u32 {
        self.total_capacity
    }

    pub fn waiting_job_count(&self) -> u32 {
        self.waiting_job_count
    }

    pub fn executing_job_count(&self) -> u32 {
        self.executing_job_count
    }
}

pub fn fleet_view(runner_type: &RunnerType, jobs: &[Job]) -> FleetView {
    let of_this_type: Vec<&Job> = jobs
        .iter()
        .filter(|job| job.runner_type() == runner_type.key())
        .collect();
    FleetView {
        instances: runner_type
            .instances()
            .iter()
            .map(|live| {
                load_of(
                    reference(runner_type, live),
                    live.capacity(),
                    of_this_type.iter().copied(),
                )
            })
            .collect(),
        total_capacity: declared_capacity(runner_type),
        waiting_job_count: count(of_this_type.iter().filter(|job| job.is_waiting()).count()),
        executing_job_count: count(of_this_type.iter().filter(|job| job.is_executing()).count()),
    }
}

pub fn unregistered_fleet_view(
    runner_type: &crate::domain::keys::RunnerTypeKey,
    jobs: &[Job],
) -> FleetView {
    let of_this_type: Vec<&Job> = jobs
        .iter()
        .filter(|job| job.runner_type() == runner_type)
        .collect();
    FleetView {
        instances: vec![],
        total_capacity: 0,
        waiting_job_count: count(of_this_type.iter().filter(|job| job.is_waiting()).count()),
        executing_job_count: count(of_this_type.iter().filter(|job| job.is_executing()).count()),
    }
}

pub fn instance_load(
    instance: &RunnerInstanceReference,
    capacity: Capacity,
    jobs: &[Job],
) -> InstanceLoad {
    load_of(instance.clone(), capacity, jobs.iter())
}

fn declared_capacity(runner_type: &RunnerType) -> u32 {
    runner_type
        .instances()
        .iter()
        .filter(|live| live.accepts_new_work())
        .map(|live| live.capacity().get())
        .fold(0u32, u32::saturating_add)
}

fn load_of<'a>(
    instance: RunnerInstanceReference,
    capacity: Capacity,
    jobs: impl Iterator<Item = &'a Job>,
) -> InstanceLoad {
    let current_run_ids = jobs
        .filter_map(Job::active_run)
        .filter(|run| run.is_executed_by(&instance))
        .map(Run::id)
        .collect();
    InstanceLoad {
        instance,
        capacity,
        current_run_ids,
    }
}

fn reference(runner_type: &RunnerType, live: &RunnerInstance) -> RunnerInstanceReference {
    RunnerInstanceReference::new(runner_type.key().clone(), live.key().clone())
}

fn count(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests;
