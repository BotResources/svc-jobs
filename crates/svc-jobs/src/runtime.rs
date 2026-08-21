use std::sync::Arc;
use std::time::Duration;

use bc_jobs::domain::ids::RunId;
use bc_jobs::event::job::JobEvent;
use tokio::time::{Instant, sleep_until};

use crate::app::dispatch::DispatchPass;
use crate::app::{Jobs, backstop, dispatch, runner_type_catalog, runner_type_impacts};
use crate::error::ServiceError;
use crate::runner_transport::{RunnerChannels, cancel};
use crate::stream::{Fact, Hub};

pub async fn dispatch_loop(jobs: Arc<Jobs>, hub: Hub, interval: Duration, minimum_wake: Duration) {
    let mut facts = hub.subscribe();
    loop {
        let pass = match dispatch::dispatch_due_work(&jobs).await {
            Ok(pass) => pass,
            Err(error) => {
                tracing::error!(error = %error, "the dispatch pass failed");
                DispatchPass::default()
            }
        };
        let wake = next_wake(&jobs, interval, minimum_wake, pass).await;
        tokio::select! {
            () = sleep_until(wake) => {}
            fact = facts.recv() => {
                match fact {
                    Ok(fact) if !unblocks_dispatch(&fact) => continue,
                    Ok(_) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        facts = hub.subscribe();
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                }
            }
        }
    }
}

async fn next_wake(
    jobs: &Jobs,
    interval: Duration,
    minimum_wake: Duration,
    pass: DispatchPass,
) -> Instant {
    let ceiling = interval.max(minimum_wake);
    let waited = match jobs.store.next_retry_due_at().await {
        Err(error) => {
            tracing::warn!(error = %error, "reading the next retry due time failed");
            ceiling
        }
        Ok(None) => ceiling,
        Ok(Some(due)) => match (due - jobs.clock.now()).to_std() {
            Ok(remaining) => remaining.clamp(minimum_wake, ceiling),
            Err(_) => wait_on_a_retry_already_due(pass, minimum_wake, ceiling),
        },
    };
    Instant::now() + waited
}

fn wait_on_a_retry_already_due(
    pass: DispatchPass,
    minimum_wake: Duration,
    ceiling: Duration,
) -> Duration {
    if pass.skipped_for_unavailability && !pass.failed && !pass.truncated {
        ceiling
    } else {
        minimum_wake
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

pub async fn runner_type_impact_loop(jobs: Arc<Jobs>, interval: Duration) {
    loop {
        if let Err(error) = runner_type_impacts::sweep(
            &jobs.store,
            jobs.ids.as_ref(),
            jobs.clock.as_ref(),
            jobs.limits.retirement_quiet_period(),
        )
        .await
        {
            tracing::error!(error = %error, "the runner-type affordance impact sweep failed");
        }
        tokio::time::sleep(interval).await;
    }
}

pub async fn runner_type_catalog_reconciliation_loop(jobs: Arc<Jobs>, interval: Duration) {
    loop {
        tokio::time::sleep(interval).await;
        if let Err(error) = runner_type_catalog::reconcile(&jobs).await {
            tracing::error!(error = %error, "the runner-type catalog reconciliation failed");
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
    let cutoff = jobs.clock.now() - chrono::TimeDelta::from_std(grace).unwrap_or_default();
    for run_id in jobs.store.runs_terminal_before(&standing, cutoff).await? {
        if let Ok(run_id) = RunId::new(run_id)
            && let Err(error) = cancel::withdraw_stop(channels, run_id).await
        {
            tracing::warn!(error = %error, "removing a settled cancel entry failed");
        }
    }
    Ok(())
}
