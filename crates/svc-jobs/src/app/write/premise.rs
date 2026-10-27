//! The facts an admission derived its priority from, read again under lock
//! before it commits.

use bc_jobs::domain::ids::{JobId, KnownUserId};
use bc_jobs::domain::job::Job;
use bc_jobs::domain::priority::{Priority, PriorityBasis, UserMaxPriority};

use crate::db::PgStore;
use crate::error::ServiceError;

/// The fact an admission derived its effective priority from. It is read
/// again under lock before the admission commits: a maximum changed or a
/// parent forced in between makes the decision stale, never silently wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PriorityPremise {
    UserMaximum {
        user_id: KnownUserId,
        maximum: Option<Priority>,
    },
    ParentEffective {
        parent_job_id: JobId,
        effective: Priority,
        forced: bool,
    },
}

impl PriorityPremise {
    /// The premise of an admission on `basis`: the parent it inherited from,
    /// or the stored maximum of the user it runs for. A Job that runs for
    /// nobody is capped by the service default alone, which nothing changes
    /// at run time.
    pub fn of(
        basis: PriorityBasis,
        parent: Option<&Job>,
        user: Option<&UserMaxPriority>,
    ) -> Option<Self> {
        match (basis, parent) {
            (
                PriorityBasis::InheritedFromParent { .. } | PriorityBasis::ForcedByParent { .. },
                Some(parent),
            ) => Some(Self::ParentEffective {
                parent_job_id: parent.id(),
                effective: parent.priority().effective(),
                forced: parent.priority().is_forced(),
            }),
            _ => user.map(|user| Self::UserMaximum {
                user_id: user.user_id(),
                maximum: user.maximum(),
            }),
        }
    }
}

pub(super) async fn guard_priority_premise_still_holds(
    tx: &mut sqlx::PgConnection,
    premise: Option<PriorityPremise>,
) -> Result<(), ServiceError> {
    let holds = match premise {
        None => true,
        Some(PriorityPremise::UserMaximum { user_id, maximum }) => {
            PgStore::user_max_priority_in(tx, user_id).await? == maximum
        }
        Some(PriorityPremise::ParentEffective {
            parent_job_id,
            effective,
            forced,
        }) => {
            let Some(parent) = PgStore::share_lock_lineage(tx, parent_job_id).await? else {
                return Err(ServiceError::Domain(bc_jobs::JobsError::ParentJobUnknown {
                    parent_job_id: parent_job_id.as_uuid(),
                }));
            };
            // The parent settled between the decision and this lock: a child
            // under it could never be resolved, so the admission is refused,
            // exactly as the domain refuses it when it reads a settled parent.
            if parent.deleted {
                return Err(ServiceError::Domain(bc_jobs::JobsError::ParentJobDeleted {
                    parent_job_id: parent_job_id.as_uuid(),
                }));
            }
            if parent.resolved {
                return Err(ServiceError::Domain(
                    bc_jobs::JobsError::ParentJobTerminal {
                        parent_job_id: parent_job_id.as_uuid(),
                    },
                ));
            }
            parent.effective == effective && parent.forced == forced
        }
    };
    if holds {
        Ok(())
    } else {
        Err(ServiceError::Contended)
    }
}
