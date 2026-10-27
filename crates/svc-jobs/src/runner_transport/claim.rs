//! The claim responder: core NATS request/reply on `jobs.claim.{runner_type}`,
//! served by every pod in one queue group so each claim reaches exactly one
//! pod. Each claim is answered on its own task — a held claim never delays
//! another.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use bc_jobs::JobsError;
use contract_jobs::runner as wire;
use futures::StreamExt;

use super::RunnerChannels;
use super::trigger::wire_trigger;
use crate::app::Jobs;
use crate::app::claim::{self, ClaimAnswer, ClaimRequest};
use crate::error::ServiceError;
use crate::supervision::Established;

pub const CLAIM_QUEUE_GROUP: &str = "svc_jobs_claims";

/// How long an Instance told `busy` waits before repeating the same claim.
pub const BUSY_RETRY_AFTER_MS: u64 = 1_000;

const CLAIM_PREFIX: &str = "jobs.claim.";

pub async fn respond(
    channels: RunnerChannels,
    jobs: Arc<Jobs>,
    established: Established,
) -> Result<(), ServiceError> {
    let client = channels.client();
    let held = Arc::new(Semaphore::new(jobs.claims.max_held));
    let mut claims = client
        .queue_subscribe(wire::CLAIM_FILTER, CLAIM_QUEUE_GROUP.to_owned())
        .await
        .map_err(|error| ServiceError::Infra(error.to_string()))?;
    established.signal();
    while let Some(message) = claims.next().await {
        let Some(reply_to) = message.reply.clone() else {
            tracing::debug!(subject = %message.subject, "a claim with no reply subject, ignored");
            continue;
        };
        let client = client.clone();
        let jobs = Arc::clone(&jobs);
        // A pod holds a bounded number of claims open: past the bound a claim is
        // answered `busy` at once, and its Instance repeats the same claim after
        // the back-off — on this pod or another one of the queue group.
        let permit = Arc::clone(&held).try_acquire_owned().ok();
        tokio::spawn(async move {
            let reply = match permit {
                Some(_permit) => answer(&jobs, message.subject.as_str(), &message.payload).await,
                None => {
                    tracing::debug!("the pod holds as many claims as it may; answered busy");
                    wire::ClaimReply::busy(BUSY_RETRY_AFTER_MS)
                }
            };
            let bytes = match serde_json::to_vec(&reply) {
                Ok(bytes) => bytes,
                Err(error) => {
                    tracing::error!(error = %error, "a claim reply could not be encoded");
                    return;
                }
            };
            if let Err(error) = client.publish(reply_to, bytes.into()).await {
                tracing::warn!(
                    error = %error,
                    "a claim reply could not be sent; the instance claims again and a reserved \
                     run is handed back to the same claim"
                );
            }
        });
    }
    Err(ServiceError::Infra(
        "the claim subscription ended; the responder restarts and resubscribes".to_owned(),
    ))
}

async fn answer(jobs: &Jobs, subject: &str, payload: &[u8]) -> wire::ClaimReply {
    let Some(runner_type) = subject.strip_prefix(CLAIM_PREFIX) else {
        return wire::ClaimReply::refused(wire::ClaimRefusal::MalformedClaim);
    };
    let claim: wire::Claim = match serde_json::from_slice(payload) {
        Ok(claim) => claim,
        Err(error) => {
            tracing::warn!(subject, error = %error, "a claim this service cannot read, refused");
            return wire::ClaimReply::refused(wire::ClaimRefusal::MalformedClaim);
        }
    };
    let request = ClaimRequest {
        runner_type: runner_type.to_owned(),
        instance_key: claim.instance_key,
        claim_id: claim.claim_id,
        wait: Duration::from_millis(claim.wait_ms),
    };
    reply_of(claim::serve(jobs, request).await)
}

/// The wire words of an answer. Every refusal the domain gives a claim has its
/// published code; anything else — an error the claim path does not expect —
/// is answered `busy`, so the Instance backs off and repeats the same claim
/// rather than reading a refusal that would make it stop.
fn reply_of(answer: ClaimAnswer) -> wire::ClaimReply {
    match answer {
        ClaimAnswer::Served(trigger) => wire::ClaimReply::served(wire_trigger(&trigger)),
        ClaimAnswer::Empty => wire::ClaimReply::empty(),
        ClaimAnswer::Spent => wire::ClaimReply::refused(wire::ClaimRefusal::ClaimSpent),
        ClaimAnswer::Malformed => wire::ClaimReply::refused(wire::ClaimRefusal::MalformedClaim),
        ClaimAnswer::Unavailable => wire::ClaimReply::busy(BUSY_RETRY_AFTER_MS),
        ClaimAnswer::Refused(refusal) => match refusal {
            JobsError::InstanceUnknown { .. } => {
                wire::ClaimReply::refused(wire::ClaimRefusal::InstanceUnknown)
            }
            JobsError::NotClaimMode { .. } => {
                wire::ClaimReply::refused(wire::ClaimRefusal::NotClaimMode)
            }
            JobsError::InstanceDraining { .. } => {
                wire::ClaimReply::refused(wire::ClaimRefusal::InstanceDraining)
            }
            JobsError::RunnerTypeRetired { .. } => {
                wire::ClaimReply::refused(wire::ClaimRefusal::RunnerTypeRetired)
            }
            unexpected => {
                tracing::warn!(
                    code = unexpected.code(),
                    "a claim met a refusal it has no published code for; answered busy"
                );
                wire::ClaimReply::busy(BUSY_RETRY_AFTER_MS)
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unexpected_refusal_or_a_failure_is_busy_never_instance_unknown() {
        let unexpected = reply_of(ClaimAnswer::Refused(JobsError::RetryNotScheduled));
        let failed = reply_of(ClaimAnswer::Unavailable);
        for reply in [unexpected, failed] {
            assert_eq!(reply.reason, Some(wire::ClaimRefusal::Busy));
            assert_eq!(reply.retry_after_ms, Some(BUSY_RETRY_AFTER_MS));
        }
    }

    #[test]
    fn every_domain_refusal_of_a_claim_keeps_its_published_code() {
        let draining = reply_of(ClaimAnswer::Refused(JobsError::InstanceDraining {
            instance_key: "pod-1".to_owned(),
        }));
        assert_eq!(draining.reason, Some(wire::ClaimRefusal::InstanceDraining));
        assert_eq!(draining.retry_after_ms, None);
    }
}
