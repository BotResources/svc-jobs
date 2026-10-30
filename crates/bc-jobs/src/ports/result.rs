use async_trait::async_trait;
use chrono::{DateTime, Utc};

use super::environment::IdFactory;
use super::mail::Stored;
use crate::domain::ids::ResultId;
use crate::domain::runner::views::StoredResult;
use crate::domain::runner::work::WorkNamed;

#[async_trait]
pub trait Results: Send + Sync {
    async fn hand_over_result(
        &self,
        work: WorkNamed<'_>,
        answer: &StoredResult,
        ids: &dyn IdFactory,
        at: DateTime<Utc>,
    ) -> Stored<ResultId>;

    async fn read_result(
        &self,
        work: WorkNamed<'_>,
        result_id: ResultId,
        keep: bool,
        at: DateTime<Utc>,
    ) -> Stored<StoredResult>;
}
