mod support;

use bc_jobs::ports::job::DUE_WORK_BATCH;
use support::db::{self, Durable};
use support::fixture::JobsFixture;
use support::producer::{JobDeclaration, Producer};
use support::scrub::{age_tree, await_scrubbed};
use support::{LONG, gql, metrics, wire};

const POISONED_TREES: usize = DUE_WORK_BATCH + 1;

const POISON_TREES: &str = "\
    INSERT INTO jobs (id, runner_type_id, producer_id, created_at, root_job_id, requester) \
    SELECT x.id, g.runner_type_id, g.producer_id, \
           now() - interval '400 days' + n * interval '1 second', x.id, '{\"bogus\": 1}'::jsonb \
    FROM generate_series(1, $2::int) n \
    CROSS JOIN jobs g \
    CROSS JOIN LATERAL (SELECT overlay(overlay(md5(random()::text || n::text) \
        placing '7' from 13) placing '8' from 17)::uuid AS id) x \
    WHERE g.id = $1";

const RESOLVE_POISONED: &str = "\
    INSERT INTO job_resolutions (id, job_id, kind, occurred_at, failure_cause) \
    SELECT overlay(overlay(md5(random()::text || j.id::text) placing '7' from 13) \
               placing '8' from 17)::uuid, \
           j.id, 'FAILED', j.created_at, 'DECLARED_BY_OWNER' \
    FROM jobs j WHERE j.requester = '{\"bogus\": 1}'::jsonb";

#[tokio::test]
async fn trees_that_cannot_be_scrubbed_do_not_stop_the_scrub_of_the_trees_behind_them() {
    let fixture = JobsFixture::start().await;
    let durable = Durable::open(&fixture.app_url()).await;
    let client = fixture.gql();
    let producer = Producer::new(fixture.fabric(), "projects");
    let declaration = JobDeclaration::new(&wire::unique_runner_type("scrub-stall"));
    producer.declare_v2(&declaration).await;
    durable
        .await_count(
            db::JOBS_WITH_ID,
            declaration.job_id,
            1,
            "the Job is created",
        )
        .await;
    producer
        .fail_v2(declaration.job_id, Some("the producer gave up"))
        .await;
    gql::wait_for_status(&client, fixture.admin(), declaration.job_id, "FAILED", LONG).await;
    sqlx::query(POISON_TREES)
        .bind(declaration.job_id)
        .bind(i32::try_from(POISONED_TREES).expect("a small count"))
        .execute(durable.pool())
        .await
        .expect("the unreadable Trees are inserted");
    sqlx::query(RESOLVE_POISONED)
        .execute(durable.pool())
        .await
        .expect("the unreadable Trees are resolved");
    let failures_before = metrics::scrub_failures(&client).await;

    age_tree(&durable, declaration.job_id, 31).await;

    await_scrubbed(
        &durable,
        declaration.job_id,
        "the Tree behind more unreadable Trees than one sweep takes is scrubbed",
    )
    .await;
    assert!(
        metrics::scrub_failures(&client).await > failures_before,
        "the Trees that cannot be scrubbed are counted"
    );
    durable.close().await;
    fixture.shutdown().await;
}
