use br_core_scope::{ScopeDeclaration, ScopeKey, ScopeSpec, ServiceKey, ServiceManifest};
use br_util_axum_readiness::ReadinessHandle;
use br_util_nats_fabric::Fabric;
use br_util_scope_declaration::{ScopeDeclarationConfig, ScopeDeclarationOutcome, declare_scopes};

use crate::edge::runner::RUNNER_SCOPE;
use crate::error::ServiceError;
use crate::supervision::Supervisor;

const SERVICE_KEY: &str = "jobs";

pub fn declaration() -> Result<ScopeDeclaration, ServiceError> {
    let invalid = |error: &dyn std::fmt::Display| ServiceError::Infra(error.to_string());
    ScopeDeclaration::new(
        ServiceManifest::new(
            ServiceKey::new(SERVICE_KEY).map_err(|error| invalid(&error))?,
            "label.jobs",
            "desc.jobs",
        ),
        vec![ScopeSpec::new(
            ScopeKey::new(RUNNER_SCOPE).map_err(|error| invalid(&error))?,
            "label.jobs.runner",
            "desc.jobs.runner",
            false,
        )],
    )
    .map_err(|error| invalid(&error))
}

pub fn spawn(supervisor: &Supervisor, fabric: &Fabric, enabled: bool) {
    let fabric = fabric.clone();
    supervisor.spawn("scope declaration", move |established| {
        let fabric = fabric.clone();
        async move {
            let gate = ReadinessHandle::not_ready("declaring the scopes of jobs");
            let config = if enabled {
                ScopeDeclarationConfig::enabled()
            } else {
                ScopeDeclarationConfig::disabled()
            };
            match declare_scopes(&fabric, declaration()?, gate, config).await? {
                ScopeDeclarationOutcome::Accepted | ScopeDeclarationOutcome::Disabled => {
                    established.signal();
                }
                _ => {}
            }
            std::future::pending::<()>().await;
            Ok(())
        }
    });
}
