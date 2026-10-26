pub const JOB_DETAIL: &str = r#"
query($id: UUID!) {
  jobsJob(id: $id) {
    job {
      id runnerType producer config parentJobId maxAttempts status attemptCount
      activeRunId nextAttemptAt isDeleted createdAt
      source { bc entityId }
      triggeredBy { id displayName }
      progression { plan { declarationNumber items { index label } } currentStep { index label startedAt } }
      resolution { id kind occurredAt failureCause causedByRunId }
      manualRetry { id failedResolutionId predecessorJobId successorJobId requestedAt requestedBy { id displayName } }
      deletion { deletedAt deletedBy { id displayName } }
      runs {
        id attemptNumber origin automaticRetryOfRunId status retryDueAt
        cancellationRequestedAt dispatchedAt startedAt finishedAt
        instance { runnerType instanceKey }
        declaredPlan { declarationId declarationNumber items { index label } }
        progression { currentStep { index label } }
        steps { index label startedAt }
        failureReport { kind reasonCode params diagnostic retryAfterSeconds }
      }
      children {
        job { id status attemptCount activeRunId parentJobId resolution { kind failureCause } runs { id status } }
        affordances { action allowed reasonCode params }
      }
    }
    affordances { action allowed reasonCode params }
  }
}
"#;

pub const JOBS_LIST: &str = r#"
query($filter: JobsJobFilterInput, $first: Int!, $after: String) {
  jobs(filter: $filter, first: $first, after: $after) {
    edges {
      cursor
      node {
        job {
          id runnerType producer status attemptCount activeRunId nextAttemptAt
          failureCause isDeleted parentJobId predecessorJobId successorJobId
          source { bc entityId }
        }
        affordances { action allowed reasonCode params }
      }
    }
    pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
  }
}
"#;

pub const JOB_BY_SOURCE: &str = r#"
query($bc: String!, $entityId: UUID!) {
  jobsJobBySource(sourceBc: $bc, sourceEntityId: $entityId) {
    job {
      id status attemptCount activeRunId runnerType producer isDeleted
      source { bc entityId }
    }
    affordances { action allowed reasonCode params }
  }
}
"#;

pub const JOB_LOGS: &str = r#"
query($jobId: UUID!, $runId: UUID, $first: Int, $after: String, $last: Int, $before: String) {
  jobsLogs(jobId: $jobId, runId: $runId, first: $first, after: $after, last: $last, before: $before) {
    edges { cursor node { id jobId runId stepIndex level message loggedAt } }
    pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
  }
}
"#;

pub const FLEET: &str = r#"
query($runnerType: String) {
  jobsFleet(runnerType: $runnerType) {
    runnerType {
      typeKey lifecycle isAvailable totalCapacity busyInstanceCount idleInstanceCount
      waitingJobCount executingJobCount
      instances { instanceKey version reportedStatus capacity isBusy currentRunIds }
    }
    affordances { action allowed reasonCode params }
  }
}
"#;

pub const CANCEL_JOB: &str = r#"
mutation($input: JobsCancelJobInput!) { jobsCancelJob(input: $input) { success } }
"#;

pub const MANUAL_RETRY_JOB: &str = r#"
mutation($input: JobsManualRetryJobInput!) { jobsManualRetryJob(input: $input) { success } }
"#;

pub const DELETE_JOB: &str = r#"
mutation($input: JobsDeleteJobInput!) { jobsDeleteJob(input: $input) { success } }
"#;

pub const DEPRECATE_RUNNER_TYPE: &str = r#"
mutation($input: JobsRunnerTypeInput!) { jobsDeprecateRunnerType(input: $input) { success } }
"#;

pub const REACTIVATE_RUNNER_TYPE: &str = r#"
mutation($input: JobsRunnerTypeInput!) { jobsReactivateRunnerType(input: $input) { success } }
"#;

pub const RETIRE_RUNNER_TYPE: &str = r#"
mutation($input: JobsRunnerTypeInput!) { jobsRetireRunnerType(input: $input) { success } }
"#;

pub const JOB_PRIORITY: &str = r#"
query($id: UUID!) {
  jobsJob(id: $id) {
    job {
      id status attemptCount activeRunId nextAttemptAt parentJobId
      requestedPriority effectivePriority forcedPriority
      triggeredBy { id }
      resolution { id kind failureCause }
      runs { id attemptNumber status origin failureReport { kind reasonCode params } }
    }
    affordances { action allowed reasonCode params }
  }
}
"#;

pub const JOBS_LIST_PRIORITY: &str = r#"
query($filter: JobsJobFilterInput, $first: Int!) {
  jobs(filter: $filter, first: $first) {
    edges {
      node {
        job { id status requestedPriority effectivePriority forcedPriority }
        affordances { action allowed reasonCode params }
      }
    }
  }
}
"#;

pub const MY_MAX_PRIORITY: &str = r#"
query { jobsMyMaxPriority { maxPriority } }
"#;

pub const MY_MAX_PRIORITY_FOR_SUPPLIED_USER: &str = r#"
query($userId: UUID!) { jobsMyMaxPriority(userId: $userId) { maxPriority } }
"#;

pub const SET_USER_MAX_PRIORITY: &str = r#"
mutation($input: JobsSetUserMaxPriorityInput!) { jobsSetUserMaxPriority(input: $input) { success } }
"#;

pub const CLEAR_USER_MAX_PRIORITY: &str = r#"
mutation($input: JobsClearUserMaxPriorityInput!) { jobsClearUserMaxPriority(input: $input) { success } }
"#;

pub const FORCE_JOB_PRIORITY: &str = r#"
mutation($input: JobsForceJobPriorityInput!) { jobsForceJobPriority(input: $input) { success } }
"#;

pub const FLEET_DISPATCH: &str = r#"
query($runnerType: String) {
  jobsFleet(runnerType: $runnerType) {
    runnerType {
      typeKey lifecycle isAvailable dispatchMode maxRunDurationSeconds
      waitingJobCount executingJobCount
      instances { instanceKey reportedStatus isBusy currentRunIds }
    }
    affordances { action allowed reasonCode params }
  }
}
"#;

pub const SET_RUNNER_TYPE_DISPATCH_MODE: &str = r#"
mutation($input: JobsSetRunnerTypeDispatchModeInput!) {
  jobsSetRunnerTypeDispatchMode(input: $input) { success }
}
"#;

pub const SET_RUNNER_TYPE_MAX_RUN_DURATION: &str = r#"
mutation($input: JobsSetRunnerTypeMaxRunDurationInput!) {
  jobsSetRunnerTypeMaxRunDuration(input: $input) { success }
}
"#;
