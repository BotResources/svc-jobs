use bc_jobs::commands::fleet::PRESENCE_EXPIRED;
use bc_jobs::domain::keys::ReasonCode;
use bc_jobs::ports::fleet::FleetReader;

use super::{Jobs, presence};
use crate::error::ServiceError;

pub async fn presence_sessions(jobs: &Jobs) -> Result<(), ServiceError> {
    let expired = ReasonCode::new(PRESENCE_EXPIRED)?;
    for session in FleetReader::open_presence_sessions(&jobs.store).await? {
        let still_announced = jobs
            .transport
            .presence_is_live(&session.runner_type, &session.instance_key)
            .await?;
        if still_announced {
            continue;
        }
        tracing::warn!(
            runner_type = %session.runner_type.as_str(),
            instance_key = %session.instance_key.as_str(),
            session_id = %session.session_id.as_uuid(),
            "an open presence session has no presence entry left; the eviction that closed it was \
             never recorded, so the loss is recorded now and its runs are reclaimed"
        );
        if let Err(error) = presence::lost_session(jobs, &session, &expired).await {
            tracing::warn!(
                runner_type = %session.runner_type.as_str(),
                instance_key = %session.instance_key.as_str(),
                error = %error,
                "recording the missed presence loss failed; the next sweep tries again"
            );
        }
    }
    Ok(())
}
