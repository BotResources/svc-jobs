use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard};

use bc_jobs::domain::ids::{JobId, RunId, RunLogId};
use bc_jobs::event::fleet::{FleetEvent, RUNNER_TYPE_AGGREGATE_TYPE};
use bc_jobs::event::job::JobEvent;
use chrono::{DateTime, Utc};
use sqlx::postgres::PgListener;
use sqlx::{PgPool, Row};
use tokio::sync::broadcast;
use uuid::Uuid;

use crate::db::notify::{DOMAIN_EVENT_CHANNEL, DomainEventSignal, RUN_LOG_CHANNEL, RunLogSignal};
use crate::error::ServiceError;
use crate::supervision::Established;

const CAPACITY: usize = 1_024;

#[derive(Clone)]
pub enum Fact {
    Job {
        event_id: Uuid,
        occurred_at: DateTime<Utc>,
        job_id: JobId,
        event: Arc<JobEvent>,
    },
    Fleet {
        event_id: Uuid,
        occurred_at: DateTime<Utc>,
        event: Arc<FleetEvent>,
    },
    Log {
        log_id: RunLogId,
        job_id: JobId,
        run_id: RunId,
    },
}

#[derive(Clone)]
pub struct Hub {
    sender: Arc<RwLock<broadcast::Sender<Fact>>>,
}

impl Default for Hub {
    fn default() -> Self {
        Self::new()
    }
}

impl Hub {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(CAPACITY);
        Self {
            sender: Arc::new(RwLock::new(sender)),
        }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Fact> {
        self.read_sender().subscribe()
    }

    pub fn end_every_subscription(&self) {
        let (replacement, _) = broadcast::channel(CAPACITY);
        *self.sender.write().unwrap_or_else(PoisonError::into_inner) = replacement;
    }

    fn publish(&self, fact: Fact) {
        let _ = self.read_sender().send(fact);
    }

    fn read_sender(&self) -> RwLockReadGuard<'_, broadcast::Sender<Fact>> {
        self.sender.read().unwrap_or_else(PoisonError::into_inner)
    }
}

pub async fn listen(pool: PgPool, hub: Hub, established: Established) -> Result<(), ServiceError> {
    let mut listener = PgListener::connect_with(&pool).await?;
    listener
        .listen_all([DOMAIN_EVENT_CHANNEL, RUN_LOG_CHANNEL])
        .await?;
    established.signal();
    loop {
        let Some(notification) = listener.try_recv().await? else {
            return Err(ServiceError::Infra(
                "the durable fact listener lost its connection; notifications raised while it \
                 reconnects are never replayed, so the listener restarts and every subscription \
                 is cut rather than left live and lossy"
                    .to_owned(),
            ));
        };
        let outcome = match notification.channel() {
            DOMAIN_EVENT_CHANNEL => domain_event(&pool, &hub, notification.payload()).await,
            RUN_LOG_CHANNEL => run_log(&hub, notification.payload()),
            _ => Ok(()),
        };
        if let Err(error) = outcome {
            tracing::warn!(error = %error, "a durable notification could not be fanned out");
        }
    }
}

async fn domain_event(pool: &PgPool, hub: &Hub, payload: &str) -> Result<(), ServiceError> {
    let signal: DomainEventSignal = serde_json::from_str(payload)?;
    let row = sqlx::query("SELECT payload, occurred_at FROM domain_events WHERE id = $1")
        .bind(signal.event_id)
        .fetch_optional(pool)
        .await?;
    let Some(row) = row else {
        return Ok(());
    };
    let stored: serde_json::Value = row.get("payload");
    let occurred_at: DateTime<Utc> = row.get("occurred_at");
    if signal.aggregate_type == RUNNER_TYPE_AGGREGATE_TYPE {
        let event = FleetEvent::decode(&signal.event_type, stored)?;
        hub.publish(Fact::Fleet {
            event_id: signal.event_id,
            occurred_at,
            event: Arc::new(event),
        });
    } else {
        let event = JobEvent::decode(&signal.event_type, stored)?;
        hub.publish(Fact::Job {
            event_id: signal.event_id,
            occurred_at,
            job_id: JobId::new(signal.aggregate_id)?,
            event: Arc::new(event),
        });
    }
    Ok(())
}

fn run_log(hub: &Hub, payload: &str) -> Result<(), ServiceError> {
    let signal: RunLogSignal = serde_json::from_str(payload)?;
    hub.publish(Fact::Log {
        log_id: RunLogId::new(signal.log_id)?,
        job_id: JobId::new(signal.job_id)?,
        run_id: RunId::new(signal.run_id)?,
    });
    Ok(())
}
