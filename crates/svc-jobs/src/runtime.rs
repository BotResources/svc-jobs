use std::sync::Arc;
use std::time::Duration;

use bc_jobs::domain::ids::RunId;
use bc_jobs::event::job::JobEvent;
use chrono::Utc;
use tokio::time::{Instant, sleep_until};

use crate::app::{Jobs, backstop, dispatch};
use crate::error::ServiceError;
use crate::runner_transport::{RunnerChannels, cancel};
use crate::stream::{Fact, Hub};

pub async fn dispatch_loop(jobs: Arc<Jobs>, hub: Hub, interval: Duration) {
    let mut facts = hub.subscribe();
    loop {
        if let Err(error) = dispatch::dispatch_due_work(&jobs).await {
            tracing::error!(error = %error, "the dispatch pass failed");
        }
        let wake = next_wake(&jobs, interval).await;
        tokio::select! {
            () = sleep_until(wake) => {}
            fact = facts.recv() => {
                match fact {
                    Ok(fact) if !unblocks_dispatch(&fact) => continue,
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                }
            }
        }
    }
}

async fn next_wake(jobs: &Jobs, interval: Duration) -> Instant {
    let ceiling = Instant::now() + interval;
    match jobs.store.next_retry_due_at().await {
        Err(error) => {
            tracing::warn!(error = %error, "reading the next retry due time failed");
            ceiling
        }
        Ok(None) => ceiling,
        Ok(Some(due)) => {
            let remaining = (due - Utc::now()).to_std().unwrap_or(Duration::ZERO);
            ceiling.min(Instant::now() + remaining)
        }
    }
}

fn unblocks_dispatch(fact: &Fact) -> bool {
    match fact {
        Fact::Fleet { .. } => true,
        Fact::Log { .. } => false,
        Fact::Job { event, .. } => matches!(
            **event,
            JobEvent::JobQueued(_)
                | JobEvent::RetryScheduled(_)
                | JobEvent::RunCompleted(_)
                | JobEvent::RunFailed(_)
                | JobEvent::RunCancelled(_)
        ),
    }
}

pub async fn backstop_loop(jobs: Arc<Jobs>, channels: RunnerChannels, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        if let Err(error) = backstop::sweep(&jobs).await {
            tracing::error!(error = %error, "the backstop sweep failed");
        }
        if let Err(error) = sweep_cancel_entries(&jobs, &channels, interval).await {
            tracing::warn!(error = %error, "the cancel-entry sweep failed");
        }
    }
}

async fn sweep_cancel_entries(
    jobs: &Jobs,
    channels: &RunnerChannels,
    grace: Duration,
) -> Result<(), ServiceError> {
    let standing = cancel::standing_requests(channels).await?;
    if standing.is_empty() {
        return Ok(());
    }
    let cutoff = Utc::now() - chrono::TimeDelta::from_std(grace).unwrap_or_default();
    for run_id in jobs.store.runs_terminal_before(&standing, cutoff).await? {
        if let Ok(run_id) = RunId::new(run_id)
            && let Err(error) = cancel::withdraw_stop(channels, run_id).await
        {
            tracing::warn!(error = %error, "removing a settled cancel entry failed");
        }
    }
    Ok(())
}
