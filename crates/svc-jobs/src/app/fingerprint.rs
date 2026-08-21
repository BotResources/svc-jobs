use bc_jobs::domain::fleet::RunnerType;
use bc_jobs::domain::fleet::instance::RunnerInstance;
use bc_jobs::domain::job::Job;
use bc_jobs::domain::run::Run;
use bc_jobs::domain::run::status::RunTerminalKind;
use uuid::Uuid;

#[derive(Debug, PartialEq, Eq)]
pub struct JobFingerprint {
    resolution_id: Option<Uuid>,
    is_deleted: bool,
    manual_retry_successor_id: Option<Uuid>,
    runs: Vec<RunFingerprint>,
}

#[derive(Debug, PartialEq, Eq)]
struct RunFingerprint {
    run_id: Uuid,
    attempt_number: u32,
    has_started: bool,
    terminal: Option<RunTerminalKind>,
    plan_declaration_number: Option<u32>,
    step_count: usize,
    cancellation_requested: bool,
    retry_scheduled: bool,
}

pub fn of_job(job: &Job) -> JobFingerprint {
    JobFingerprint {
        resolution_id: job.resolution().map(|resolution| resolution.id().as_uuid()),
        is_deleted: job.is_deleted(),
        manual_retry_successor_id: job
            .manual_retry()
            .map(|retry| retry.successor_job_id().as_uuid()),
        runs: job.runs().iter().map(of_run).collect(),
    }
}

fn of_run(run: &Run) -> RunFingerprint {
    RunFingerprint {
        run_id: run.id().as_uuid(),
        attempt_number: run.attempt_number().get(),
        has_started: run.has_started(),
        terminal: run
            .terminal()
            .map(bc_jobs::domain::run::parts::RunTerminal::kind),
        plan_declaration_number: run.plan().map(|plan| plan.declaration_number().get()),
        step_count: run.steps().len(),
        cancellation_requested: run.cancellation_request().is_some(),
        retry_scheduled: run.retry_schedule().is_some(),
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct FleetFingerprint {
    lifecycle: Option<&'static str>,
    instances: Vec<InstanceFingerprint>,
}

#[derive(Debug, PartialEq, Eq)]
struct InstanceFingerprint {
    instance_key: String,
    session_id: Uuid,
    version: String,
    reported_status: String,
    status_change_number: u32,
}

pub fn of_fleet(runner_type: Option<&RunnerType>) -> FleetFingerprint {
    let mut instances: Vec<InstanceFingerprint> = runner_type
        .map(RunnerType::instances)
        .unwrap_or_default()
        .iter()
        .map(of_instance)
        .collect();
    instances.sort_by(|left, right| left.instance_key.cmp(&right.instance_key));
    FleetFingerprint {
        lifecycle: runner_type.map(|known| known.lifecycle().as_str()),
        instances,
    }
}

fn of_instance(instance: &RunnerInstance) -> InstanceFingerprint {
    InstanceFingerprint {
        instance_key: instance.key().as_str().to_owned(),
        session_id: instance.session_id().as_uuid(),
        version: instance.version().as_str().to_owned(),
        reported_status: instance.reported_status().as_str().to_owned(),
        status_change_number: instance.status_change_number(),
    }
}
