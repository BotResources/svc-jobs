mod support;

use bc_jobs::JobsError;
use bc_jobs::commands::job::create::{CreateJob, CreateOutcome, create_job};
use bc_jobs::domain::ids::JobId;
use bc_jobs::domain::keys::{ProducerKey, RunnerTypeKey};
use bc_jobs::domain::payload::JobPayload;
use bc_jobs::domain::priority::Priority;
use bc_jobs::domain::requester::Requester;
use bc_jobs::ports::scrub::{OrphanUploads, TreeScrubs};
use chrono::TimeDelta;
use svc_jobs::ServiceError;
use svc_jobs::app::write::JobChange;
use svc_jobs::db::kept_payloads::lock_kept_payload;
use svc_jobs::db::mail::lock_mail_attachments;
use uuid::Uuid;

use support::clock;
use support::contention::{Fixture, commit, limits, service_passport};
use support::orphan_race::{
    an_old_kept_payload, an_old_unlisted_attachment, await_a_writer_blocked_on_an_advisory_lock,
    holding, uploader,
};

async fn sweep(fixture: &Fixture) -> OrphanUploads {
    fixture
        .store
        .erase_orphan_uploads(clock::now() - TimeDelta::days(1), 10)
        .await
        .expect("the sweep runs")
}

#[tokio::test]
async fn a_payload_whose_job_is_being_created_is_not_erased_by_the_sweep() {
    let fixture = Fixture::start().await;
    let job_id = JobId::new(Uuid::now_v7()).expect("a v7 job id");
    an_old_kept_payload(&fixture, job_id).await;
    let mut creating = holding(&fixture).await;
    lock_kept_payload(&mut creating, job_id.as_uuid())
        .await
        .expect("the creation holds the payload");

    assert_eq!(sweep(&fixture).await, OrphanUploads::default());
    let kept: i64 = fixture
        .count(
            "SELECT count(*) AS count FROM kept_payloads WHERE job_id::text = $1",
            &job_id.as_uuid().to_string(),
        )
        .await;
    assert_eq!(kept, 1, "the payload of a Job in creation stays");

    creating.commit().await.expect("the creation ends");
    assert_eq!(
        sweep(&fixture).await,
        OrphanUploads {
            payloads: 1,
            attachments: 0
        }
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn attachments_of_a_mail_being_sent_are_not_erased_by_the_sweep() {
    let fixture = Fixture::start().await;
    let mail_id = Uuid::now_v7();
    an_old_unlisted_attachment(&fixture, mail_id).await;
    let mut sending = holding(&fixture).await;
    lock_mail_attachments(&mut sending, mail_id)
        .await
        .expect("the send holds the attachments");

    assert_eq!(sweep(&fixture).await, OrphanUploads::default());
    sending.commit().await.expect("the send ends");
    assert_eq!(
        sweep(&fixture).await,
        OrphanUploads {
            payloads: 0,
            attachments: 1
        }
    );
    fixture.shutdown().await;
}

#[tokio::test]
async fn a_creation_that_waits_for_a_payload_erased_meanwhile_is_refused_and_creates_no_job() {
    let fixture = Fixture::start().await;
    let job_id = JobId::new(Uuid::now_v7()).expect("a v7 job id");
    an_old_kept_payload(&fixture, job_id).await;
    let requester = Requester::Service(Some(uploader()));
    let declaration = CreateJob {
        id: job_id,
        runner_type: RunnerTypeKey::new("archivist").expect("a valid runner type"),
        producer: ProducerKey::new("projects").expect("a valid producer"),
        config: None,
        parent_job_id: None,
        triggered_by: None,
        source_entity_id: None,
        max_attempts: None,
        requested_priority: Priority::DEFAULT,
        initial_job_id: None,
        requester,
        passport: Some(service_passport()),
        payload: Some(JobPayload::Kept),
        call: None,
    };
    let CreateOutcome::Queued(queued) =
        create_job(declaration, None, None, None, &limits()).expect("the declaration is accepted")
    else {
        panic!("a fresh job id is queued");
    };
    let mut sweeping = holding(&fixture).await;
    lock_kept_payload(&mut sweeping, job_id.as_uuid())
        .await
        .expect("the sweep holds the payload");

    let creation = commit(
        &fixture,
        vec![JobChange::new(job_id, None, queued.events).creating_from_kept_payload(requester)],
    );
    let erasure = async {
        await_a_writer_blocked_on_an_advisory_lock(&fixture.pool).await;
        sqlx::query("DELETE FROM kept_payloads WHERE job_id = $1")
            .bind(job_id.as_uuid())
            .execute(&mut *sweeping)
            .await
            .expect("the sweep erases the payload");
        sweeping.commit().await.expect("the sweep ends");
    };
    let (committed, ()) = tokio::join!(creation, erasure);

    assert!(
        matches!(
            committed,
            Err(ServiceError::Domain(JobsError::PayloadNotUploaded))
        ),
        "got {committed:?}"
    );
    assert_eq!(
        fixture
            .count(
                "SELECT count(*) AS count FROM jobs WHERE id::text = $1",
                &job_id.as_uuid().to_string()
            )
            .await,
        0,
        "no Job is created without its payload"
    );
    fixture.shutdown().await;
}
