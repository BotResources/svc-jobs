use async_graphql::Enum;
use bc_jobs::domain::fleet::lifecycle::RunnerTypeLifecycle;
use bc_jobs::domain::job::resolution::{JobFailureCause, JobResolutionKind};
use bc_jobs::domain::job::status::JobStatus;
use bc_jobs::domain::log::RunLogLevel;
use bc_jobs::domain::run::failure::RunFailureKind;
use bc_jobs::domain::run::origin::RunOrigin;
use bc_jobs::domain::run::status::RunStatus;
use bc_jobs::policies::fleet::FleetSignal;

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "JobsJobStatus")]
pub enum GqlJobStatus {
    Pending,
    InProgress,
    Completed,
    Failed,
    Cancelled,
}

impl From<JobStatus> for GqlJobStatus {
    fn from(status: JobStatus) -> Self {
        match status {
            JobStatus::Pending => Self::Pending,
            JobStatus::InProgress => Self::InProgress,
            JobStatus::Completed => Self::Completed,
            JobStatus::Failed => Self::Failed,
            JobStatus::Cancelled => Self::Cancelled,
        }
    }
}

impl GqlJobStatus {
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::InProgress => "IN_PROGRESS",
            Self::Completed => "COMPLETED",
            Self::Failed => "FAILED",
            Self::Cancelled => "CANCELLED",
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "JobsJobResolutionKind")]
pub enum GqlJobResolutionKind {
    Completed,
    Failed,
    Cancelled,
}

impl From<JobResolutionKind> for GqlJobResolutionKind {
    fn from(kind: JobResolutionKind) -> Self {
        match kind {
            JobResolutionKind::Completed => Self::Completed,
            JobResolutionKind::Failed => Self::Failed,
            JobResolutionKind::Cancelled => Self::Cancelled,
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "JobsJobFailureCause")]
pub enum GqlJobFailureCause {
    TerminalRunFailure,
    DeclaredByOwner,
    InactivityTimeout,
}

impl From<JobFailureCause> for GqlJobFailureCause {
    fn from(cause: JobFailureCause) -> Self {
        match cause {
            JobFailureCause::TerminalRunFailure => Self::TerminalRunFailure,
            JobFailureCause::DeclaredByOwner => Self::DeclaredByOwner,
            JobFailureCause::InactivityTimeout => Self::InactivityTimeout,
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "JobsRunStatus")]
pub enum GqlRunStatus {
    Pending,
    Started,
    Completed,
    Failed,
    Cancelled,
}

impl From<RunStatus> for GqlRunStatus {
    fn from(status: RunStatus) -> Self {
        match status {
            RunStatus::Pending => Self::Pending,
            RunStatus::Started => Self::Started,
            RunStatus::Completed => Self::Completed,
            RunStatus::Failed => Self::Failed,
            RunStatus::Cancelled => Self::Cancelled,
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "JobsRunFailureKind")]
pub enum GqlRunFailureKind {
    Transient,
    Permanent,
}

impl From<RunFailureKind> for GqlRunFailureKind {
    fn from(kind: RunFailureKind) -> Self {
        match kind {
            RunFailureKind::Transient => Self::Transient,
            RunFailureKind::Permanent => Self::Permanent,
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "JobsRunOrigin")]
pub enum GqlRunOrigin {
    Initial,
    AutomaticRetry,
    ManualRetry,
}

impl From<RunOrigin> for GqlRunOrigin {
    fn from(origin: RunOrigin) -> Self {
        match origin {
            RunOrigin::Initial => Self::Initial,
            RunOrigin::AutomaticRetry => Self::AutomaticRetry,
            RunOrigin::ManualRetry => Self::ManualRetry,
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "JobsRunLogLevel")]
pub enum GqlRunLogLevel {
    Info,
    Warning,
    Error,
}

impl From<RunLogLevel> for GqlRunLogLevel {
    fn from(level: RunLogLevel) -> Self {
        match level {
            RunLogLevel::Info => Self::Info,
            RunLogLevel::Warning => Self::Warning,
            RunLogLevel::Error => Self::Error,
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "JobsDeletedFilter")]
pub enum GqlDeletedFilter {
    ExcludeDeleted,
    IncludeDeleted,
    OnlyDeleted,
}

impl GqlDeletedFilter {
    pub fn as_db_str(self) -> &'static str {
        match self {
            Self::ExcludeDeleted => "EXCLUDE_DELETED",
            Self::IncludeDeleted => "INCLUDE_DELETED",
            Self::OnlyDeleted => "ONLY_DELETED",
        }
    }
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "JobsFleetEventKind")]
pub enum GqlFleetEventKind {
    RunnerTypeRegistered,
    RunnerTypeDeprecated,
    RunnerTypeReactivated,
    RunnerTypeRetired,
    RunnerTypeAffordancesChanged,
    InstanceConnected,
    InstanceStatusReported,
    InstanceDisconnected,
    JobBeganWaiting,
    JobStoppedWaiting,
    JobBeganExecuting,
    JobStoppedExecuting,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "JobsRunnerTypeLifecycle")]
pub enum GqlRunnerTypeLifecycle {
    Active,
    Deprecated,
    Retired,
}

impl From<RunnerTypeLifecycle> for GqlRunnerTypeLifecycle {
    fn from(lifecycle: RunnerTypeLifecycle) -> Self {
        match lifecycle {
            RunnerTypeLifecycle::Active => Self::Active,
            RunnerTypeLifecycle::Deprecated => Self::Deprecated,
            RunnerTypeLifecycle::Retired => Self::Retired,
        }
    }
}

impl From<FleetSignal> for GqlFleetEventKind {
    fn from(signal: FleetSignal) -> Self {
        match signal {
            FleetSignal::JobBeganWaiting => Self::JobBeganWaiting,
            FleetSignal::JobStoppedWaiting => Self::JobStoppedWaiting,
            FleetSignal::JobBeganExecuting => Self::JobBeganExecuting,
            FleetSignal::JobStoppedExecuting => Self::JobStoppedExecuting,
        }
    }
}
