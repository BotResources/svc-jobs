use chrono::{DateTime, Utc};

use super::Job;
use super::resolution::JobResolutionKind;
use crate::domain::policy::Retention;

impl Job {
    pub fn request_data_erasure_due(&self, retention: Retention, now: DateTime<Utc>) -> bool {
        self.content_erased_at.is_none()
            && self.resolution.as_ref().is_some_and(|resolution| {
                resolution.kind() == JobResolutionKind::Cancelled
                    && retention.cancelled_request_data_is_due(resolution.occurred_at(), now)
            })
    }

    pub fn has_request_data_erased(&self) -> bool {
        self.content_erased_at.is_some()
    }
}

#[cfg(test)]
mod tests;
