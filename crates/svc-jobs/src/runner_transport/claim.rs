//! The claim responder: core NATS request/reply on `jobs.claim.{runner_type}`,
//! served by every pod in one queue group so each claim reaches exactly one
//! pod. Each claim is answered on its own task — a held claim never delays
//! another.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use contract_jobs::runner as wire;
use futures::StreamExt;

use super::RunnerChannels;
use super::trigger::wire_trigger;
use crate::app::Jobs;
use crate::app::claim::{self, ClaimAnswer, ClaimRequest};
use crate::error::ServiceError;
use crate::supervision::Established;

pub const CLAIM_QUEUE_GROUP: &str = "svc_jobs_claims";

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
        // answered empty at once, and its Instance claims again — on this pod or
        // another one of the queue group.
        let permit = Arc::clone(&held).try_acquire_owned().ok();
        tokio::spawn(async move {
            let reply = match permit {
                Some(_permit) => answer(&jobs, message.subject.as_str(), &message.payload).await,
                None => {
                    tracing::debug!("the pod holds as many claims as it may; answered empty");
                    wire::ClaimReply::empty()
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
    match claim::serve(jobs, request).await {
        ClaimAnswer::Served(trigger) => wire::ClaimReply::served(wire_trigger(&trigger)),
        ClaimAnswer::Empty => wire::ClaimReply::empty(),
        ClaimAnswer::Refused(reason) => wire::ClaimReply::refused(reason),
    }
}
