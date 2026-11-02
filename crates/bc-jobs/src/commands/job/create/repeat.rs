use crate::commands::job::create::CreateJob;
use crate::domain::job::Job;
use crate::domain::references::KnownUser;

fn unless_given_or_equal<T: PartialEq>(given: Option<&T>, stored: Option<&T>) -> bool {
    given.is_none() || given == stored
}

fn same_user(given: Option<&KnownUser>, stored: Option<&KnownUser>) -> bool {
    given.is_none() || given.map(KnownUser::id) == stored.map(KnownUser::id)
}

impl CreateJob {
    pub fn is_the_same_declaration_as(&self, existing: &Job) -> bool {
        self.has_the_skeleton_of(existing)
            && (existing.content_erased_at().is_some() || self.has_the_content_of(existing))
    }

    fn has_the_skeleton_of(&self, existing: &Job) -> bool {
        existing.runner_type() == &self.runner_type
            && existing.producer() == &self.producer
            && existing.parent_job_id() == self.parent_job_id
            && self.names_the_user_of(existing)
            && unless_given_or_equal(
                self.initial_job_id.as_ref(),
                Some(&existing.initial_job_id()),
            )
            && existing.requester() == &self.requester
            && existing.source_entity_id() == self.source_entity_id
            && existing.max_attempts() == self.max_attempts
            && (self.parent_job_id.is_some()
                || existing.priority().requested() == self.requested_priority)
    }

    fn has_the_content_of(&self, existing: &Job) -> bool {
        existing.config() == self.config.as_ref()
            && unless_given_or_equal(self.passport.as_ref(), existing.passport())
            && existing.payload() == self.payload.as_ref()
    }

    fn names_the_user_of(&self, existing: &Job) -> bool {
        if existing.content_erased_at().is_some() {
            same_user(self.triggered_by.as_ref(), existing.triggered_by())
        } else {
            unless_given_or_equal(self.triggered_by.as_ref(), existing.triggered_by())
        }
    }
}
