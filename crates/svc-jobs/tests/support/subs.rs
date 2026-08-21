use uuid::Uuid;

const AFFORDANCES: &str = "affordances { action allowed reasonCode params }";

const JOB_EVENT: &str = r#"event {
      __typename id jobId occurredAt
      ... on JobsJobQueuedEvent { parentJobId }
      ... on JobsRunDispatchedEvent { runId attemptNumber }
      ... on JobsRunStartedEvent { runId instanceKey }
      ... on JobsRunPlanDeclaredEvent { runId declarationNumber }
      ... on JobsRunStepStartedEvent { runId stepIndex }
      ... on JobsRunCompletedEvent { runId }
      ... on JobsRunFailedEvent { runId failureKind reasonCode }
      ... on JobsRunCancelledEvent { runId }
      ... on JobsRetryScheduledEvent { failedRunId dueAt }
      ... on JobsJobCompletedEvent { resolutionId }
      ... on JobsJobFailedEvent { resolutionId failureCause }
      ... on JobsJobCancelledEvent { resolutionId }
      ... on JobsManualRetryStartedEvent { successorJobId manualRetryId runId }
      ... on JobsJobDeletedEvent { deletedById }
      ... on JobsJobAffordancesChangedEvent { causedByJobId }
    }"#;

const JOB_DETAIL: &str = r#"id status attemptCount activeRunId nextAttemptAt isDeleted
      resolution { id kind failureCause causedByRunId }
      manualRetry { id successorJobId }
      deletion { deletedAt deletedBy { id } }
      progression { plan { declarationNumber items { index label } } currentStep { index label } }
      runs {
        id attemptNumber status origin retryDueAt
        declaredPlan { declarationNumber items { index label } }
        steps { index label }
        failureReport { kind reasonCode params }
      }
      children { job { id status attemptCount } affordances { action allowed reasonCode params } }"#;

const JOB_SUMMARY: &str = r#"job {
          id status attemptCount activeRunId nextAttemptAt failureCause isDeleted
          runnerType producer predecessorJobId successorJobId
        }"#;

pub fn job_changed(job_id: Uuid) -> String {
    format!(
        r#"subscription {{
  jobsJobChanged(jobId: "{job_id}") {{
    __typename
    ... on JobsJobSnapshot {{ cursor job {{ {JOB_DETAIL} }} {AFFORDANCES} }}
    ... on JobsJobDelta {{ cursor {JOB_EVENT} job {{ {JOB_DETAIL} }} {AFFORDANCES} }}
  }}
}}"#
    )
}

pub fn jobs_changed(runner_type: &str) -> String {
    jobs_changed_with(&[runner_type], "EXCLUDE_DELETED")
}

pub fn jobs_changed_for(runner_types: &[&str]) -> String {
    jobs_changed_with(runner_types, "EXCLUDE_DELETED")
}

pub fn jobs_changed_including_deleted(runner_type: &str) -> String {
    jobs_changed_with(&[runner_type], "INCLUDE_DELETED")
}

pub fn jobs_changed_pending(runner_type: &str, first: i64) -> String {
    jobs_changed_window(
        &format!(
            r#"runnerTypes: ["{runner_type}"], statuses: [PENDING], deleted: EXCLUDE_DELETED"#
        ),
        first,
    )
}

fn jobs_changed_with(runner_types: &[&str], deleted: &str) -> String {
    let types = runner_types
        .iter()
        .map(|name| format!("\"{name}\""))
        .collect::<Vec<_>>()
        .join(", ");
    jobs_changed_window(&format!("runnerTypes: [{types}], deleted: {deleted}"), 50)
}

fn jobs_changed_window(filter: &str, first: i64) -> String {
    format!(
        r#"subscription {{
  jobsChanged(
    filter: {{ {filter} }}
    window: {{ first: {first} }}
  ) {{
    __typename
    ... on JobsJobsSnapshot {{
      cursor
      jobs {{
        edges {{ node {{ {JOB_SUMMARY} {AFFORDANCES} }} }}
        pageInfo {{ hasNextPage hasPreviousPage startCursor endCursor }}
      }}
    }}
    ... on JobsJobsDelta {{
      cursor
      {JOB_EVENT}
      upserted {{ {JOB_SUMMARY} {AFFORDANCES} }}
      removedIds
      pageInfo {{ hasNextPage hasPreviousPage startCursor endCursor }}
    }}
  }}
}}"#
    )
}

pub fn log_tail(job_id: Uuid) -> String {
    format!(
        r#"subscription {{
  jobsJobLogTail(jobId: "{job_id}", window: {{ last: 200 }}) {{
    __typename
    ... on JobsJobLogSnapshot {{
      cursor
      logs {{ edges {{ node {{ id jobId runId stepIndex level message }} }} }}
    }}
    ... on JobsRunLogAppended {{
      cursor
      log {{ id jobId runId stepIndex level message }}
    }}
  }}
}}"#
    )
}

pub fn fleet_changed(runner_type: &str) -> String {
    format!(
        r#"subscription {{
  jobsFleetChanged(runnerType: "{runner_type}") {{
    __typename
    ... on JobsFleetSnapshot {{
      cursor
      runnerTypes {{
        runnerType {{
          typeKey lifecycle isAvailable totalCapacity busyInstanceCount idleInstanceCount
          waitingJobCount executingJobCount
          instances {{ instanceKey version reportedStatus capacity isBusy currentRunIds }}
        }}
        {AFFORDANCES}
      }}
    }}
    ... on JobsRunnerTypeDelta {{
      cursor
      event {{ id kind occurredAt runnerType instanceKey jobId runId }}
      runnerType {{
        typeKey lifecycle isAvailable totalCapacity busyInstanceCount idleInstanceCount
        waitingJobCount executingJobCount
        instances {{ instanceKey version reportedStatus capacity isBusy currentRunIds }}
      }}
      {AFFORDANCES}
    }}
  }}
}}"#
    )
}
