use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

pub const WIRE_VERSION: u8 = 1;
pub const WIRE_VERSION_V2: u8 = 2;

pub const TRIGGER_STREAM: &str = "JOBS_TRIGGER";
pub const STATUS_STREAM: &str = "JOBS_STATUS";
pub const LOG_STREAM: &str = "JOBS_LOG";
pub const TRIGGER_FILTER: &str = "jobs.trigger.>";
/// Every claim subject: one segment per runner type.
pub const CLAIM_FILTER: &str = "jobs.claim.*";
pub const CLAIM_V2_FILTER: &str = "jobs.claim.v2.*";
pub const STATUS_FILTER: &str = "jobs.status.>";
pub const LOG_FILTER: &str = "jobs.log.>";
pub const CANCEL_BUCKET: &str = "JOBS_CANCEL";
pub const PRESENCE_BUCKET: &str = "JOBS_PRESENCE";

fn wire_version() -> u8 {
    WIRE_VERSION
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trigger {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub run_id: Uuid,
    pub job_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<Value>,
    pub attempt: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub triggered_by: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunStarted {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub run_id: Uuid,
    pub instance_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanDeclared {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub run_id: Uuid,
    pub steps: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declaration_id: Option<Uuid>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepStarted {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub run_id: Uuid,
    pub index: u32,
    pub label: String,
    pub started_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunCompleted {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub run_id: Uuid,
}

pub const FAILURE_KIND_TRANSIENT: &str = "TRANSIENT";
pub const FAILURE_KIND_PERMANENT: &str = "PERMANENT";

/// The closed retry vocabulary of a terminal failure report.
///
/// Jobs reads it as retry policy and nothing else: `TRANSIENT` may buy another
/// attempt against the job's budget, `PERMANENT` ends automatic retry at once.
/// There is no third word — a report naming one does not parse, so a runner
/// declaring an unclassified failure fails to compile rather than fails to be
/// understood in production.
///
/// [`Default`] is the contract's absent-kind rule: an unclassified failure is
/// permanent, because a kind nobody chose must never burn a retry.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FailureKind {
    Transient,
    #[default]
    Permanent,
}

impl FailureKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Transient => FAILURE_KIND_TRANSIENT,
            Self::Permanent => FAILURE_KIND_PERMANENT,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailureReport {
    /// Absent means [`FailureKind::Permanent`]. The field stays optional so a
    /// runner that never classifies its failures keeps writing the exact bytes
    /// it writes today.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<FailureKind>,
    pub reason_code: String,
    #[serde(default)]
    pub params: Value,
    #[serde(default)]
    pub diagnostic: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunFailed {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub run_id: Uuid,
    pub report: FailureReport,
    #[serde(
        default,
        alias = "retry_after",
        skip_serializing_if = "Option::is_none"
    )]
    pub retry_after_seconds: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogLine {
    #[serde(default = "wire_version")]
    pub version: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<Uuid>,
    pub run_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_index: Option<u32>,
    pub level: String,
    pub message: String,
    pub logged_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelRun {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub run_id: Uuid,
}

pub const STATUS_READY: &str = "READY";
pub const STATUS_DRAINING: &str = "DRAINING";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RunnerStatus {
    Ready,
    Draining,
}

impl RunnerStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ready => STATUS_READY,
            Self::Draining => STATUS_DRAINING,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct Capacity(u32);

impl Capacity {
    pub fn get(self) -> u32 {
        self.0
    }
}

impl TryFrom<u32> for Capacity {
    type Error = &'static str;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        if value == 0 {
            return Err("capacity declares how many runs an instance carries; it is at least 1");
        }
        Ok(Self(value))
    }
}

impl From<Capacity> for u32 {
    fn from(value: Capacity) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Presence {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub runner_type: String,
    pub instance_key: String,
    pub runner_version: String,
    pub status: RunnerStatus,
    pub capacity: Capacity,
}

pub const CLAIM_REASON_INSTANCE_UNKNOWN: &str = "instance_unknown";
pub const CLAIM_REASON_NOT_CLAIM_MODE: &str = "not_claim_mode";
pub const CLAIM_REASON_INSTANCE_DRAINING: &str = "instance_draining";
pub const CLAIM_REASON_RUNNER_TYPE_RETIRED: &str = "runner_type_retired";
pub const CLAIM_REASON_CLAIM_SPENT: &str = "claim_spent";
pub const CLAIM_REASON_MALFORMED_CLAIM: &str = "malformed_claim";
pub const CLAIM_REASON_BUSY: &str = "busy";
/// The reason a reserved Run its Instance never started fails with.
pub const RUN_FAILURE_CLAIM_LOST: &str = "claim_lost";
/// The reason a Run left on the trigger stream of a type switched to `CLAIM`
/// is cancelled with, once nobody started it within the reservation delay
/// after the switch. It does not count as an attempt: its Job waits to be
/// claimed.
pub const RUN_CANCELLATION_TRIGGER_STRANDED: &str = "trigger_stranded";

/// Why a claim was refused rather than served or answered empty. A closed
/// vocabulary: a reason outside it does not parse, so a runner compiled
/// against this contract handles every refusal Jobs can give.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimRefusal {
    /// Jobs has no live presence for this Instance of this type: announce
    /// presence (or wait for it to be observed), then claim again later.
    InstanceUnknown,
    /// The type is dispatched by `PUSH`: stop claiming and consume the trigger
    /// stream. Claim again only once the type is switched back to `CLAIM`.
    NotClaimMode,
    /// The Instance reports `DRAINING`: stop claiming until it reports `READY`.
    InstanceDraining,
    /// The type is retired and takes no new Job: stop claiming.
    RunnerTypeRetired,
    /// The `claim_id` already served a Run that was started or settled: mint a
    /// new `claim_id` for a new slot. Never reuse one.
    ClaimSpent,
    /// The claim could not be read — an unparseable payload, a nil `claim_id`,
    /// an invalid subject: fix the claim; repeating it unchanged gets the same
    /// answer.
    MalformedClaim,
    /// The pod that received the claim could not hold it — it holds as many
    /// claims as it may, or its store is momentarily unavailable. Nothing was
    /// reserved for this answer: wait `retry_after_ms` (carried by the reply),
    /// then repeat the SAME claim, under the same `claim_id`.
    Busy,
}

impl ClaimRefusal {
    pub const ALL: [Self; 7] = [
        Self::InstanceUnknown,
        Self::NotClaimMode,
        Self::InstanceDraining,
        Self::RunnerTypeRetired,
        Self::ClaimSpent,
        Self::MalformedClaim,
        Self::Busy,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::InstanceUnknown => CLAIM_REASON_INSTANCE_UNKNOWN,
            Self::NotClaimMode => CLAIM_REASON_NOT_CLAIM_MODE,
            Self::InstanceDraining => CLAIM_REASON_INSTANCE_DRAINING,
            Self::RunnerTypeRetired => CLAIM_REASON_RUNNER_TYPE_RETIRED,
            Self::ClaimSpent => CLAIM_REASON_CLAIM_SPENT,
            Self::MalformedClaim => CLAIM_REASON_MALFORMED_CLAIM,
            Self::Busy => CLAIM_REASON_BUSY,
        }
    }
}

/// An Instance of a `CLAIM` runner type asking for its next Run, on
/// `jobs.claim.{runner_type}` (core NATS request/reply).
///
/// `claim_id` identifies one free slot of the Instance: repeating a claim
/// under the same `claim_id` — because its reply was lost — returns the Run
/// already reserved to it, never a second one. A different `claim_id` from the
/// same Instance is another slot. `wait_ms` is the longest the Instance waits
/// for the reply; Jobs holds the claim at most that long, within its own bound.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claim {
    #[serde(default = "wire_version")]
    pub version: u8,
    pub instance_key: String,
    pub claim_id: Uuid,
    pub wait_ms: u64,
}

/// The answer to a [`Claim`]. `run` carries exactly what a trigger carries;
/// absent, it means nothing was waiting during the wait — or, with a `reason`,
/// that the claim was refused. An empty answer is claimed again; a refusal is
/// handled as its [`ClaimRefusal`] says. `retry_after_ms` comes with `busy`
/// only: how long to wait before repeating the same claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimReply {
    #[serde(default = "wire_version")]
    pub version: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<Trigger>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<ClaimRefusal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

impl ClaimReply {
    pub fn served(run: Trigger) -> Self {
        Self {
            version: WIRE_VERSION,
            run: Some(run),
            reason: None,
            retry_after_ms: None,
        }
    }

    pub fn empty() -> Self {
        Self {
            version: WIRE_VERSION,
            run: None,
            reason: None,
            retry_after_ms: None,
        }
    }

    /// A refusal other than `busy`, which always names its back-off.
    pub fn refused(reason: ClaimRefusal) -> Self {
        Self {
            version: WIRE_VERSION,
            run: None,
            reason: Some(reason),
            retry_after_ms: None,
        }
    }

    /// The pod could not hold the claim: repeat the same claim after
    /// `retry_after_ms`.
    pub fn busy(retry_after_ms: u64) -> Self {
        Self {
            version: WIRE_VERSION,
            run: None,
            reason: Some(ClaimRefusal::Busy),
            retry_after_ms: Some(retry_after_ms),
        }
    }
}

fn wire_version_v2() -> u8 {
    WIRE_VERSION_V2
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RestartBlock {
    pub restart_of_run_id: Uuid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TriggerV2 {
    #[serde(default = "wire_version_v2")]
    pub version: u8,
    pub run_id: Uuid,
    pub job_id: Uuid,
    pub attempt: u32,
    pub parent_job_id: Uuid,
    pub initial_job_id: Uuid,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passport: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub triggered_by: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<crate::command_v3::Payload>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<crate::command_v3::CallMode>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub caller_address: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub restart: Option<RestartBlock>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClaimReplyV2 {
    #[serde(default = "wire_version_v2")]
    pub version: u8,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run: Option<TriggerV2>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<ClaimRefusal>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
}

impl ClaimReplyV2 {
    pub fn served(run: TriggerV2) -> Self {
        Self {
            version: WIRE_VERSION_V2,
            run: Some(run),
            reason: None,
            retry_after_ms: None,
        }
    }

    pub fn empty() -> Self {
        Self {
            version: WIRE_VERSION_V2,
            run: None,
            reason: None,
            retry_after_ms: None,
        }
    }

    pub fn refused(reason: ClaimRefusal) -> Self {
        Self {
            version: WIRE_VERSION_V2,
            run: None,
            reason: Some(reason),
            retry_after_ms: None,
        }
    }

    pub fn busy(retry_after_ms: u64) -> Self {
        Self {
            version: WIRE_VERSION_V2,
            run: None,
            reason: Some(ClaimRefusal::Busy),
            retry_after_ms: Some(retry_after_ms),
        }
    }
}
