use chrono::{DateTime, TimeDelta, Utc};

use crate::error::JobsError;

pub const DEFAULT_RETENTION_DAYS: i64 = 30;
pub const DEFAULT_CANCELLED_RETENTION_DAYS: i64 = 0;
pub const MIN_RETENTION_DAYS: i64 = 1;
pub const MAX_RETENTION_DAYS: i64 = 395;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Retention {
    tree: TimeDelta,
    cancelled: TimeDelta,
}

impl Default for Retention {
    fn default() -> Self {
        Self {
            tree: TimeDelta::days(DEFAULT_RETENTION_DAYS),
            cancelled: TimeDelta::days(DEFAULT_CANCELLED_RETENTION_DAYS),
        }
    }
}

impl Retention {
    pub fn from_days(retention_days: i64, cancelled_days: i64) -> Result<Self, JobsError> {
        if !(MIN_RETENTION_DAYS..=MAX_RETENTION_DAYS).contains(&retention_days) {
            return Err(JobsError::OutOfRange {
                field: "retention_days",
                value: retention_days,
            });
        }
        if !(0..=MAX_RETENTION_DAYS).contains(&cancelled_days) {
            return Err(JobsError::OutOfRange {
                field: "cancelled_retention_days",
                value: cancelled_days,
            });
        }
        if cancelled_days > retention_days {
            return Err(JobsError::CancelledRetentionAboveRetention {
                cancelled: cancelled_days,
                retention: retention_days,
            });
        }
        Ok(Self {
            tree: TimeDelta::days(retention_days),
            cancelled: TimeDelta::days(cancelled_days),
        })
    }

    pub fn tree(&self) -> TimeDelta {
        self.tree
    }

    pub fn cancelled(&self) -> TimeDelta {
        self.cancelled
    }

    pub fn erases_cancelled_request_data_at_once(&self) -> bool {
        self.cancelled.is_zero()
    }

    pub fn cancelled_request_data_is_due(
        &self,
        cancelled_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> bool {
        cancelled_at + self.cancelled <= now
    }
}

#[cfg(test)]
mod tests;
