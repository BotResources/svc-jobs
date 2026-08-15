pub mod apply;
pub mod fleet;
pub mod hydrate;
pub mod jobs;
pub mod list;
pub mod logs;
pub mod notify;
pub mod rows;

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
