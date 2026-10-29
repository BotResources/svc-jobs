//! The local facts priority decisions read: the maximum stored for a
//! KnownUser, and the Jobs a change of that maximum reaches.

use std::collections::HashMap;

use async_trait::async_trait;
use bc_jobs::domain::ids::{JobId, KnownUserId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::priority::{Priority, UserMaxPriority, UserSelection};
use bc_jobs::ports::PortError;
use bc_jobs::ports::user::UserMaxPriorityReader;
use sqlx::{PgConnection, Row};
use uuid::Uuid;

use super::PgStore;
use super::hydrate::{load_many, unavailable};
use super::lineage::root_of;
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

    /// Share-locks the lineage a child admission inherits from — the root of
    /// the parent's tree first, then the parent, the order every writer takes
    /// Job rows in — and reads the parent's state under that lock: its
    /// effective level, whether it is forced, and whether it still admits
    /// work. `None` when the parent is gone.
    pub async fn share_lock_lineage(
        tx: &mut PgConnection,
        parent_job_id: JobId,
    ) -> Result<Option<ParentState>, PortError> {
        let sql = format!("SELECT {}::uuid FROM jobs p WHERE p.id = $1", root_of("p"));
        let root: Option<Uuid> = sqlx::query_scalar(&sql)
            .bind(parent_job_id.as_uuid())
            .fetch_optional(&mut *tx)
            .await
            .map_err(unavailable)?;
        let Some(root) = root else {
            return Ok(None);
        };
        for row in [root, parent_job_id.as_uuid()] {
            sqlx::query("SELECT id FROM jobs WHERE id = $1 FOR SHARE")
                .bind(row)
                .execute(&mut *tx)
                .await
                .map_err(unavailable)?;
            if row == parent_job_id.as_uuid() {
                break;
            }
        }
        let row = sqlx::query(
            "SELECT j.effective_priority::smallint AS effective_priority, \
                (j.forced_priority IS NOT NULL) AS forced, \
                EXISTS (SELECT 1 FROM job_resolutions r WHERE r.job_id = j.id) AS resolved, \
                EXISTS (SELECT 1 FROM job_deletions d WHERE d.job_id = j.id) AS deleted \
             FROM jobs j WHERE j.id = $1",
        )
        .bind(parent_job_id.as_uuid())
        .fetch_optional(&mut *tx)
        .await
        .map_err(unavailable)?;
        row.map(|row| -> Result<ParentState, bc_jobs::JobsError> {
            Ok(ParentState {
                effective: rows::priority(row.get("effective_priority"))?,
                forced: row.get("forced"),
                resolved: row.get("resolved"),
                deleted: row.get("deleted"),
            })
        })
        .transpose()
        .map_err(PortError::from)
    }

    /// The waiting, non-forced root Jobs of a user — those a change of their
    /// maximum recalculates — read from the waiting access path.
    pub async fn waiting_roots_of_in(
        tx: &mut PgConnection,
        user_id: KnownUserId,
    ) -> Result<Vec<Uuid>, PortError> {
        sqlx::query_scalar(
            "SELECT w.job_id::uuid FROM waiting_jobs w JOIN jobs j ON j.id = w.job_id \
             WHERE j.triggered_by_id = $1 AND j.parent_job_id IS NULL \
               AND j.forced_priority IS NULL",
        )
        .bind(user_id.as_uuid())
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)
    }

    /// Locks the given Jobs for update in one statement, in id order — the
    /// order every writer takes Job rows in. Returns the ids that exist.
    pub async fn lock_jobs_in(tx: &mut PgConnection, ids: &[Uuid]) -> Result<Vec<Uuid>, PortError> {
        if ids.is_empty() {
            return Ok(vec![]);
        }
        sqlx::query_scalar("SELECT id::uuid FROM jobs WHERE id = ANY($1) ORDER BY id FOR UPDATE")
            .bind(ids)
            .fetch_all(&mut *tx)
            .await
            .map_err(unavailable)
    }

    pub async fn descendant_ids_in(
        tx: &mut PgConnection,
        roots: &[Uuid],
    ) -> Result<Vec<Uuid>, PortError> {
        if roots.is_empty() {
            return Ok(vec![]);
        }
        sqlx::query_scalar(
            "WITH RECURSIVE descendants AS ( \
                SELECT id FROM jobs WHERE parent_job_id = ANY($1) \
                UNION ALL \
                SELECT child.id FROM jobs child \
                JOIN descendants d ON child.parent_job_id = d.id) \
             SELECT id::uuid FROM descendants",
        )
        .bind(roots)
        .fetch_all(&mut *tx)
        .await
        .map_err(unavailable)
    }

    pub async fn load_jobs_in(tx: &mut PgConnection, ids: &[Uuid]) -> Result<Vec<Job>, PortError> {
        load_many(tx, ids).await
    }
}

#[async_trait]
impl UserMaxPriorityReader for PgStore {
    /// One statement for the whole selection: the users with a maximum of
    /// their own are rows, every other one is on the default.
    async fn load_selected(
        &self,
        users: &UserSelection,
    ) -> Result<Vec<UserMaxPriority>, PortError> {
        if users.is_empty() {
            return Ok(vec![]);
        }
        let ids: Vec<Uuid> = users.ids().iter().map(KnownUserId::as_uuid).collect();
        let rows = sqlx::query(
            "SELECT m.user_id::uuid AS user_id, m.max_priority::smallint AS max_priority \
             FROM user_max_priorities m WHERE m.user_id = ANY($1)",
        )
        .bind(&ids)
        .fetch_all(self.pool())
        .await
        .map_err(unavailable)?;
        let mut stored: HashMap<Uuid, Priority> = HashMap::with_capacity(rows.len());
        for row in rows {
            stored.insert(
                row.get("user_id"),
                rows::priority(row.get("max_priority")).map_err(PortError::from)?,
            );
        }
        Ok(users
            .ids()
            .iter()
            .map(|user_id| UserMaxPriority::new(*user_id, stored.get(&user_id.as_uuid()).copied()))
            .collect())
    }
}

/// The parent of a child admission, as read under the lineage share lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParentState {
    pub effective: Priority,
    pub forced: bool,
    pub resolved: bool,
    pub deleted: bool,
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
