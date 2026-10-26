//! The local facts priority decisions read: the maximum stored for a
//! KnownUser, and the Jobs a change of that maximum reaches.

use bc_jobs::domain::ids::{JobId, KnownUserId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::priority::{Priority, UserMaxPriority};
use bc_jobs::ports::PortError;
use sqlx::PgConnection;
use uuid::Uuid;

use super::PgStore;
use super::hydrate::{load_many, unavailable};
use super::jobs::{NO_ACTIVE_RUN, NOT_RESOLVED};
use super::rows;

const MAXIMUM_SQL: &str = "SELECT m.max_priority::smallint FROM user_max_priorities m \
     WHERE m.user_id = $1";

/// One advisory lock per user, namespaced to this table: a change of the
/// user's maximum takes it exclusively, an admission that read the maximum
/// takes it shared to check the maximum still holds. The user need not be a
/// row anywhere for the lock to exist.
const LOCK_SQL: &str = "SELECT pg_advisory_xact_lock(hashtext('user_max_priorities'), \
     hashtext($1::text))";
const SHARED_LOCK_SQL: &str =
    "SELECT pg_advisory_xact_lock_shared(hashtext('user_max_priorities'), hashtext($1::text))";

impl PgStore {
    /// The stored maximum of a user, Jobs knowing them or not.
    pub async fn user_max_priority(
        &self,
        user_id: KnownUserId,
    ) -> Result<UserMaxPriority, PortError> {
        let mut connection = self.pool().acquire().await.map_err(unavailable)?;
        Ok(UserMaxPriority::new(
            user_id,
            maximum(&mut connection, user_id).await?,
        ))
    }

    /// Locks the user for a change of their maximum and reads the maximum
    /// stored under that lock.
    pub async fn lock_user_max_priority(
        tx: &mut PgConnection,
        user_id: KnownUserId,
    ) -> Result<Option<Priority>, PortError> {
        sqlx::query(LOCK_SQL)
            .bind(user_id.as_uuid())
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        maximum(tx, user_id).await
    }

    /// The maximum an admission decided on still holds: read under the shared
    /// form of the lock a change of the maximum takes exclusively.
    pub async fn user_max_priority_in(
        tx: &mut PgConnection,
        user_id: KnownUserId,
    ) -> Result<Option<Priority>, PortError> {
        sqlx::query(SHARED_LOCK_SQL)
            .bind(user_id.as_uuid())
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
        maximum(tx, user_id).await
    }

    /// The effective priority a child admission inherited still holds: read
    /// under a share lock on the parent, which forcing or recalculating it
    /// locks exclusively.
    pub async fn effective_priority_in(
        tx: &mut PgConnection,
        job_id: JobId,
    ) -> Result<Option<Priority>, PortError> {
        let stored: Option<i16> = sqlx::query_scalar(
            "SELECT effective_priority::smallint FROM jobs WHERE id = $1 FOR SHARE",
        )
        .bind(job_id.as_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        stored
            .map(rows::priority)
            .transpose()
            .map_err(PortError::from)
    }

    /// The root Jobs a change of the user's maximum may recalculate — waiting
    /// and not forced — and every descendant of them.
    pub async fn priority_reach_of(
        &self,
        user_id: KnownUserId,
    ) -> Result<(Vec<Job>, Vec<Job>), PortError> {
        let sql = format!(
            "SELECT j.id::uuid AS id FROM jobs j \
             WHERE j.triggered_by_id = $1 AND j.parent_job_id IS NULL \
               AND j.forced_priority IS NULL AND {NOT_RESOLVED} AND {NO_ACTIVE_RUN}"
        );
        let mut connection = self.pool().acquire().await.map_err(unavailable)?;
        let roots: Vec<Uuid> = sqlx::query_scalar(&sql)
            .bind(user_id.as_uuid())
            .fetch_all(&mut *connection)
            .await
            .map_err(unavailable)?;
        if roots.is_empty() {
            return Ok((vec![], vec![]));
        }
        let descendants: Vec<Uuid> = sqlx::query_scalar(
            "WITH RECURSIVE descendants AS ( \
                SELECT id FROM jobs WHERE parent_job_id = ANY($1) \
                UNION ALL \
                SELECT child.id FROM jobs child \
                JOIN descendants d ON child.parent_job_id = d.id) \
             SELECT id::uuid FROM descendants",
        )
        .bind(&roots)
        .fetch_all(&mut *connection)
        .await
        .map_err(unavailable)?;
        Ok((
            load_many(&mut connection, &roots).await?,
            load_many(&mut connection, &descendants).await?,
        ))
    }
}

async fn maximum(
    executor: &mut PgConnection,
    user_id: KnownUserId,
) -> Result<Option<Priority>, PortError> {
    let stored: Option<i16> = sqlx::query_scalar(MAXIMUM_SQL)
        .bind(user_id.as_uuid())
        .fetch_optional(&mut *executor)
        .await
        .map_err(unavailable)?;
    stored
        .map(rows::priority)
        .transpose()
        .map_err(PortError::from)
}
