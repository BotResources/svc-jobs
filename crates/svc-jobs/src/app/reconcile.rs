use std::collections::HashSet;
use std::future::Future;

use bc_jobs::commands::fleet::PRESENCE_EXPIRED;
use bc_jobs::domain::keys::ReasonCode;
use bc_jobs::ports::PortError;
use bc_jobs::ports::fleet::{FleetReader, OpenPresenceSession};
use bc_jobs::ports::transport::AnnouncedInstance;

use super::{Jobs, presence};
use crate::error::ServiceError;

async fn sessions_without_presence<S, SF, A, AF>(
    read_sessions: S,
    read_announced: A,
) -> Result<Vec<OpenPresenceSession>, PortError>
where
    S: FnOnce() -> SF,
    SF: Future<Output = Result<Vec<OpenPresenceSession>, PortError>>,
    A: FnOnce() -> AF,
    AF: Future<Output = Result<HashSet<AnnouncedInstance>, PortError>>,
{
    let sessions = read_sessions().await?;
    let announced = read_announced().await?;
    Ok(sessions
        .into_iter()
        .filter(|session| !announced.contains(&announced_form(session)))
        .collect())
}

async fn confirmed_without_presence<C, CF>(
    candidates: Vec<OpenPresenceSession>,
    announces: C,
) -> Result<Vec<OpenPresenceSession>, PortError>
where
    C: Fn(AnnouncedInstance) -> CF,
    CF: Future<Output = Result<bool, PortError>>,
{
    let mut confirmed = Vec::new();
    for session in candidates {
        if !announces(announced_form(&session)).await? {
            confirmed.push(session);
        }
    }
    Ok(confirmed)
}

pub async fn presence_sessions(jobs: &Jobs) -> Result<(), ServiceError> {
    let expired = ReasonCode::new(PRESENCE_EXPIRED)?;
    let candidates = sessions_without_presence(
        || FleetReader::open_presence_sessions(&jobs.store),
        || jobs.transport.announced_instances(),
    )
    .await?;
    let lost = confirmed_without_presence(candidates, |instance| async move {
        jobs.transport.announces(&instance).await
    })
    .await?;
    for session in lost {
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

fn announced_form(session: &OpenPresenceSession) -> AnnouncedInstance {
    AnnouncedInstance {
        runner_type: session.runner_type.clone(),
        instance_key: session.instance_key.clone(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use bc_jobs::domain::ids::PresenceSessionId;
    use bc_jobs::domain::keys::{InstanceKey, RunnerTypeKey};
    use uuid::Uuid;

    use super::*;

    fn session(instance: &str) -> OpenPresenceSession {
        OpenPresenceSession {
            runner_type: RunnerTypeKey::new("analyst").unwrap(),
            instance_key: InstanceKey::new(instance).unwrap(),
            session_id: PresenceSessionId::new(Uuid::now_v7()).unwrap(),
        }
    }

    #[tokio::test]
    async fn a_session_opened_while_the_presence_keys_are_listed_is_not_reported_lost() {
        let open = Arc::new(Mutex::new(vec![session("old")]));
        let announced_old: HashSet<AnnouncedInstance> =
            HashSet::from([announced_form(&session("old"))]);
        let opening = Arc::clone(&open);
        let read = Arc::clone(&open);

        let lost = sessions_without_presence(
            move || async move { Ok(read.lock().unwrap().clone()) },
            move || async move {
                opening.lock().unwrap().push(session("newcomer"));
                Ok(announced_old)
            },
        )
        .await
        .unwrap();

        assert!(
            lost.is_empty(),
            "the newcomer opened after the sessions were read: {lost:?}"
        );
    }

    #[tokio::test]
    async fn a_session_whose_key_the_listing_missed_but_a_read_finds_is_not_lost() {
        let rewritten = session("rewritten");
        let gone = session("gone");
        let present = announced_form(&rewritten);

        let lost = confirmed_without_presence(vec![rewritten, gone.clone()], move |instance| {
            let present = present.clone();
            async move { Ok(instance == present) }
        })
        .await
        .unwrap();

        assert_eq!(lost, vec![gone]);
    }

    #[tokio::test]
    async fn a_session_without_a_presence_key_is_reported_lost() {
        let gone = session("gone");
        let kept = session("kept");
        let sessions = vec![gone.clone(), kept.clone()];
        let announced = HashSet::from([announced_form(&kept)]);

        let lost = sessions_without_presence(
            move || async move { Ok(sessions) },
            move || async move { Ok(announced) },
        )
        .await
        .unwrap();

        assert_eq!(lost, vec![gone]);
    }
}
