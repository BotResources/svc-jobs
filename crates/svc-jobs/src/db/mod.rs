pub mod apply;
pub mod claim;
pub mod directory;
pub mod dispatch_order;
pub mod fleet;
pub mod hydrate;
pub mod impacts;
pub mod jobs;
pub mod lineage;
pub mod list;
pub mod logs;
pub mod notify;
pub mod partitions;
pub mod priority;
pub mod rows;
pub mod waiting_repair;

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
