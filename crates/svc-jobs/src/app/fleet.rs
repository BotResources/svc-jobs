use bc_jobs::domain::actions::Affordance;
use bc_jobs::domain::fleet::RunnerType;
use bc_jobs::domain::fleet::view::{FleetView, fleet_view};
use bc_jobs::domain::keys::RunnerTypeKey;
use bc_jobs::ports::PortError;
use bc_jobs::ports::fleet::FleetReader;
use chrono::{DateTime, Utc};

pub struct RunnerTypeView {
    pub runner_type: RunnerType,
    pub fleet: FleetView,
    pub affordances: Vec<Affordance>,
}

pub async fn views(
    reader: &dyn FleetReader,
    watched: Option<&RunnerTypeKey>,
    evaluated_at: DateTime<Utc>,
) -> Result<Vec<RunnerTypeView>, PortError> {
    let source = reader.projection_source(watched, evaluated_at).await?;
    source
        .runner_types
        .into_iter()
        .map(|runner_type| {
            let facts = source
                .decision_facts
                .get(runner_type.key().as_str())
                .copied()
                .ok_or(PortError::ConcurrentModification)?;
            Ok(RunnerTypeView {
                fleet: fleet_view(&runner_type, &source.active_jobs),
                affordances: runner_type.affordances(facts),
                runner_type,
            })
        })
        .collect()
}
