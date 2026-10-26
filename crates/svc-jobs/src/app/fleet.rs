use bc_jobs::domain::actions::Affordance;
use bc_jobs::domain::actions::fleet::RetirementWindow;
use bc_jobs::domain::fleet::RunnerType;
use bc_jobs::domain::fleet::run_duration::MaxRunDuration;
use bc_jobs::domain::fleet::view::{FleetView, fleet_view};
use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::ports::PortError;
use bc_jobs::ports::fleet::FleetReader;

pub struct RunnerTypeView {
    pub runner_type: RunnerType,
    pub fleet: FleetView,
    pub affordances: Vec<Affordance>,
    /// The maximum its Runs are dispatched with: its own, or the service
    /// default it follows.
    pub max_run_duration: MaxRunDuration,
}

pub async fn views(
    reader: &dyn FleetReader,
    watched: Option<&RunnerTypeKey>,
    window: RetirementWindow,
    service_default: MaxRunDuration,
) -> Result<Vec<RunnerTypeView>, PortError> {
    let source = reader.projection_source(watched, window).await?;
    Ok(source
        .runner_types
        .into_iter()
        .map(|decidable| RunnerTypeView {
            fleet: fleet_view(&decidable.runner_type, &source.active_jobs),
            affordances: decidable.runner_type.affordances(decidable.decision_facts),
            max_run_duration: decidable.runner_type.max_run_duration_or(service_default),
            runner_type: decidable.runner_type,
        })
        .collect())
}
