mod support;

use bc_jobs::domain::attempts::AttemptNumber;
use bc_jobs::domain::ids::{JobId, RunId};
use bc_jobs::event::job::JobEvent;
use bc_jobs::event::job_facts::RunCompleted;
use support::contention::{self, Fixture};
use svc_jobs::ServiceError;
use svc_jobs::app::write::JobChange;
use uuid::Uuid;

#[tokio::test]
async fn a_run_end_on_a_job_whose_tree_is_unknown_is_refused_not_written_without_the_tree_lock() {
    let fixture = Fixture::start().await;
    let unknown = JobId::new(Uuid::now_v7()).expect("a v7 job id");
    let ended = JobEvent::RunCompleted(RunCompleted {
        job_id: unknown,
        run_id: RunId::new(Uuid::now_v7()).expect("a v7 run id"),
        attempt_number: AttemptNumber::FIRST,
    });

    let outcome =
        contention::commit(&fixture, vec![JobChange::new(unknown, None, vec![ended])]).await;

    assert!(
        matches!(outcome, Err(ServiceError::JobNotFound)),
        "an event that ends a Run names a Job with no Tree: it is refused, got {outcome:?}"
    );
    assert_eq!(
        fixture
            .count(
                "SELECT count(*) AS count FROM domain_events WHERE aggregate_id::text = $1",
                &unknown.as_uuid().to_string()
            )
            .await,
        0,
        "nothing was written"
    );
    fixture.shutdown().await;
}
