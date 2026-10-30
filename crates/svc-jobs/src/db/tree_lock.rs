use bc_jobs::domain::ids::JobId;
use bc_jobs::ports::PortError;
use metrics::{counter, describe_counter};
use sqlx::PgConnection;

use super::hydrate::unavailable;

pub const TREE_LOCK_TIMEOUTS_TOTAL: &str = "jobs_tree_lock_timeouts_total";

const TREE_LOCK_WAIT: &str = "5s";
const LOCK_NOT_AVAILABLE: &str = "55P03";

pub fn describe() {
    describe_counter!(
        TREE_LOCK_TIMEOUTS_TOTAL,
        "Writes and runner gestures refused because the Tree lock stayed held past the bounded \
         wait. A rising series means a long write holds a Tree"
    );
    counter!(TREE_LOCK_TIMEOUTS_TOTAL).absolute(0);
}

pub async fn lock_tree(tx: &mut PgConnection, initial_job_id: JobId) -> Result<(), PortError> {
    sqlx::query(&format!("SET LOCAL lock_timeout = '{TREE_LOCK_WAIT}'"))
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
        .bind(format!("directory:{}", initial_job_id.as_uuid()))
        .execute(&mut *tx)
        .await
        .map_err(|error| lock_failure(error, initial_job_id))?;
    sqlx::query("SET LOCAL lock_timeout TO DEFAULT")
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}

fn lock_failure(error: sqlx::Error, initial_job_id: JobId) -> PortError {
    if !is_lock_timeout(&error) {
        return unavailable(error);
    }
    counter!(TREE_LOCK_TIMEOUTS_TOTAL).increment(1);
    tracing::warn!(
        initial_job_id = %initial_job_id.as_uuid(),
        wait = TREE_LOCK_WAIT,
        "the Tree lock stayed held past the bounded wait; the write is refused"
    );
    PortError::TreeLockTimeout
}

fn is_lock_timeout(error: &sqlx::Error) -> bool {
    error
        .as_database_error()
        .and_then(|database| database.code())
        .is_some_and(|code| code == LOCK_NOT_AVAILABLE)
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::error::Error;
    use std::fmt;

    use sqlx::error::{DatabaseError, ErrorKind};
    use uuid::Uuid;

    use super::*;

    #[derive(Debug)]
    struct Refused(&'static str);

    impl fmt::Display for Refused {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(formatter, "sqlstate {}", self.0)
        }
    }

    impl Error for Refused {}

    impl DatabaseError for Refused {
        fn message(&self) -> &str {
            "refused"
        }

        fn code(&self) -> Option<Cow<'_, str>> {
            Some(Cow::Borrowed(self.0))
        }

        fn as_error(&self) -> &(dyn Error + Send + Sync + 'static) {
            self
        }

        fn as_error_mut(&mut self) -> &mut (dyn Error + Send + Sync + 'static) {
            self
        }

        fn into_error(self: Box<Self>) -> Box<dyn Error + Send + Sync + 'static> {
            self
        }

        fn kind(&self) -> ErrorKind {
            ErrorKind::Other
        }
    }

    fn failure(sqlstate: &'static str) -> PortError {
        let tree = JobId::new(Uuid::now_v7()).expect("a UUIDv7");
        lock_failure(sqlx::Error::Database(Box::new(Refused(sqlstate))), tree)
    }

    #[test]
    fn a_lock_not_available_failure_is_a_tree_lock_timeout() {
        assert!(matches!(failure("55P03"), PortError::TreeLockTimeout));
    }

    #[test]
    fn a_deadlock_stays_a_lost_write_race() {
        assert!(matches!(
            failure("40P01"),
            PortError::ConcurrentModification
        ));
    }

    #[test]
    fn any_other_database_failure_is_infrastructure() {
        assert!(matches!(failure("57P01"), PortError::Unavailable { .. }));
    }

    #[test]
    fn a_failure_that_is_not_a_database_error_is_infrastructure() {
        let tree = JobId::new(Uuid::now_v7()).expect("a UUIDv7");
        assert!(matches!(
            lock_failure(sqlx::Error::PoolTimedOut, tree),
            PortError::Unavailable { .. }
        ));
    }
}
