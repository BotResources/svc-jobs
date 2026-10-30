pub mod acts_for;
pub mod answers;
pub mod apply;
pub mod call_notices;
pub mod claim;
pub mod directory;
pub mod dispatch_order;
pub mod doorbell;
pub mod due_work;
pub mod fleet;
pub mod hydrate;
pub mod impacts;
pub mod inbox;
pub mod jobs;
pub mod kept_payloads;
pub mod lineage;
pub mod list;
pub mod logs;
pub mod mail;
pub mod notify;
pub mod partitions;
pub mod priority;
pub mod reach;
pub mod restart;
pub mod rows;
pub mod saved_states;
pub mod waiting_repair;
pub mod work;

use sqlx::PgPool;

#[derive(Clone)]
pub struct PgStore {
    pool: PgPool,
}

impl PgStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}
