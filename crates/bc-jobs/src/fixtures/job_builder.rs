use super::*;

pub struct JobBuilder {
    state: JobState,
}

impl Default for JobBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl JobBuilder {
    pub fn new() -> Self {
        Self {
            state: JobState {
                id: job_id(),
                runner_type: runner_type(),
                producer: producer(),
                config: None,
                parent_job_id: None,
                predecessor_job_id: None,
                triggered_by: None,
                source_entity_id: None,
                max_attempts: None,
                priority: JobPriority::default(),
                created_at: ts(0),
                owner: JobOwner::Producer(producer()),
                runs: vec![],
                resolution: None,
                deletion: None,
                manual_retry: None,
                initial_job_id: None,
                requester: Requester::Service(None),
                passport: Some(service_passport()),
                payload: None,
                call: None,
                restart_of_run_id: None,
            },
        }
    }

    pub fn with_id(mut self, id: JobId) -> Self {
        self.state.id = id;
        self
    }

    pub fn with_runner_type(mut self, key: &str) -> Self {
        self.state.runner_type = RunnerTypeKey::new(key).expect("valid runner type");
        self
    }

    pub fn with_config(mut self, config: RunnerConfig) -> Self {
        self.state.config = Some(config);
        self
    }

    pub fn with_requester(mut self, requester: Requester) -> Self {
        self.state.requester = requester;
        self
    }

    pub fn with_passport(mut self, passport: JobPassport) -> Self {
        self.state.passport = Some(passport);
        self
    }

    pub fn without_passport(mut self) -> Self {
        self.state.passport = None;
        self
    }

    pub fn with_initial_job(mut self, initial_job_id: JobId) -> Self {
        self.state.initial_job_id = Some(initial_job_id);
        self
    }

    pub fn with_parent(mut self, parent_job_id: JobId) -> Self {
        self.state.parent_job_id = Some(parent_job_id);
        self.state.initial_job_id = Some(parent_job_id);
        self.state.owner = JobOwner::Runner {
            parent_job_id,
            runner_type: runner_type(),
        };
        self
    }

    pub fn with_restart_of(mut self, run_id: RunId) -> Self {
        self.state.restart_of_run_id = Some(run_id);
        self
    }

    pub fn with_predecessor(mut self, predecessor_job_id: JobId) -> Self {
        self.state.predecessor_job_id = Some(predecessor_job_id);
        self
    }

    pub fn with_max_attempts(mut self, value: u32) -> Self {
        self.state.max_attempts = Some(MaxAttempts::new(value).expect("positive budget"));
        self
    }

    pub fn with_priority(
        mut self,
        requested: Priority,
        effective: Priority,
        forced: Option<Priority>,
    ) -> Self {
        self.state.priority =
            JobPriority::hydrate(requested, effective, forced).expect("valid fixture priority");
        self
    }

    pub fn with_triggered_by(mut self, user: KnownUser) -> Self {
        self.state.triggered_by = Some(user);
        self
    }

    pub fn with_source(mut self, entity_id: SourceEntityId) -> Self {
        self.state.source_entity_id = Some(entity_id);
        self
    }

    pub fn with_run(mut self, run: Run) -> Self {
        self.state.runs.push(run);
        self
    }

    pub fn with_resolution(mut self, resolution: JobResolution) -> Self {
        self.state.resolution = Some(resolution);
        self
    }

    pub fn with_manual_retry(mut self, record: ManualRetryRecord) -> Self {
        self.state.manual_retry = Some(record);
        self
    }

    pub fn manually_retried_by(
        self,
        successor_job_id: JobId,
        successor_settled_at_load: bool,
    ) -> Self {
        let failed_resolution_id = self
            .state
            .resolution
            .as_ref()
            .map_or_else(resolution_id, JobResolution::id);
        self.with_manual_retry(ManualRetryRecord::new(
            manual_retry_id(),
            failed_resolution_id,
            successor_job_id,
            user(),
            ts(60),
            successor_settled_at_load,
        ))
    }

    pub fn deleted(mut self, at: DateTime<Utc>) -> Self {
        self.state.deletion = Some(JobDeletion::new(user(), at));
        self
    }

    pub fn state(self) -> JobState {
        self.state
    }

    pub fn build(self) -> Job {
        Job::hydrate(self.state).expect("fixture job is valid")
    }
}
