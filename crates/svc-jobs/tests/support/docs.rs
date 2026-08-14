use uuid::Uuid;

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
        declaredPlan { declarationNumber items { index label } }
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
query($filter: JobsJobFilterInput, $first: Int!) {
  jobs(filter: $filter, first: $first) {
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
    job { id status isDeleted predecessorJobId }
    affordances { action allowed reasonCode params }
  }
}
"#;

pub const JOB_LOGS: &str = r#"
query($jobId: UUID!, $runId: UUID, $first: Int) {
  jobsLogs(jobId: $jobId, runId: $runId, first: $first) {
    edges { cursor node { id jobId runId stepIndex level message loggedAt } }
    pageInfo { hasNextPage hasPreviousPage startCursor endCursor }
  }
}
"#;

pub const FLEET: &str = r#"
query($runnerType: String) {
  jobsFleet(runnerType: $runnerType) {
    runnerType {
      typeKey isAvailable busyInstanceCount idleInstanceCount
      waitingJobCount executingJobCount
      instances { instanceKey version reportedStatus isBusy currentRunIds }
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

pub fn job_changed_subscription(job_id: Uuid) -> String {
    format!(
        r#"subscription {{
  jobsJobChanged(jobId: "{job_id}") {{
    __typename
    ... on JobsJobSnapshot {{
      cursor
      job {{ id status attemptCount activeRunId isDeleted runs {{ id status attemptNumber }} }}
      affordances {{ action allowed reasonCode params }}
    }}
    ... on JobsJobDelta {{
      cursor
      event {{ __typename id jobId occurredAt }}
      job {{ id status attemptCount activeRunId isDeleted runs {{ id status attemptNumber }} }}
      affordances {{ action allowed reasonCode params }}
    }}
  }}
}}"#
    )
}

pub fn jobs_changed_subscription(runner_type: &str) -> String {
    format!(
        r#"subscription {{
  jobsChanged(filter: {{ runnerTypes: ["{runner_type}"] }}, window: {{ first: 50 }}) {{
    __typename
    ... on JobsJobsSnapshot {{ cursor jobs {{ edges {{ node {{ job {{ id status }} }} }} }} }}
    ... on JobsJobsDelta {{
      cursor
      event {{ __typename id jobId occurredAt }}
      upserted {{ job {{ id status attemptCount }} affordances {{ action allowed reasonCode }} }}
      removedIds
    }}
  }}
}}"#
    )
}

pub fn log_tail_subscription(job_id: Uuid) -> String {
    format!(
        r#"subscription {{
  jobsJobLogTail(jobId: "{job_id}", window: {{ last: 200 }}) {{
    __typename
    ... on JobsJobLogSnapshot {{ cursor logs {{ edges {{ node {{ id runId stepIndex level message }} }} }} }}
    ... on JobsRunLogAppended {{ cursor log {{ id runId stepIndex level message }} }}
  }}
}}"#
    )
}

pub fn fleet_changed_subscription(runner_type: &str) -> String {
    format!(
        r#"subscription {{
  jobsFleetChanged(runnerType: "{runner_type}") {{
    __typename
    ... on JobsFleetSnapshot {{ cursor runnerTypes {{ runnerType {{ typeKey isAvailable idleInstanceCount }} }} }}
    ... on JobsRunnerTypeDelta {{
      cursor
      event {{ id kind occurredAt runnerType instanceKey jobId runId }}
      runnerType {{ typeKey isAvailable busyInstanceCount idleInstanceCount waitingJobCount executingJobCount }}
      affordances {{ action allowed reasonCode }}
    }}
  }}
}}"#
    )
}
