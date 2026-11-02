use bc_jobs::domain::erasure::ERASED_TEXT;
use bc_jobs::event::job::JOB_AGGREGATE_TYPE;
use bc_jobs::ports::PortError;
use sqlx::PgConnection;
use uuid::Uuid;

use crate::db::hydrate::unavailable;

const REWRITE_DOMAIN_EVENTS: [(&str, &str); 5] = [
    (
        "JobQueued",
        "CASE WHEN jsonb_typeof(payload -> 'triggered_by') = 'object' \
            THEN jsonb_set(payload - 'config' - 'passport' - 'payload', \
                 '{triggered_by,display_name}', to_jsonb($3::text)) \
            ELSE payload - 'config' - 'passport' - 'payload' END",
    ),
    (
        "RunFailed",
        "CASE WHEN jsonb_typeof(payload -> 'report') = 'object' \
            THEN jsonb_set(jsonb_set(payload, '{report,params}', '{}'::jsonb), \
                 '{report,diagnostic}', '{}'::jsonb) \
            ELSE payload END",
    ),
    (
        "JobFailed",
        "CASE WHEN jsonb_typeof(payload -> 'report') = 'object' \
            THEN jsonb_set(jsonb_set(payload, '{report,params}', '{}'::jsonb), \
                 '{report,diagnostic}', '{}'::jsonb) \
            ELSE payload END",
    ),
    (
        "RunPlanDeclared",
        "CASE WHEN jsonb_typeof(payload -> 'items') = 'array' \
            THEN jsonb_set(payload, '{items}', \
                 (SELECT coalesce(jsonb_agg(jsonb_set(item.value, '{label}', to_jsonb($3::text)) \
                                            ORDER BY item.position), '[]'::jsonb) \
                  FROM jsonb_array_elements(payload -> 'items') \
                       WITH ORDINALITY AS item(value, position))) \
            ELSE payload END",
    ),
    (
        "RunStepStarted",
        "CASE WHEN payload ? 'label' \
            THEN jsonb_set(payload, '{label}', to_jsonb($3::text)) ELSE payload END",
    ),
];

pub async fn rewrite_domain_events(tx: &mut PgConnection, jobs: &[Uuid]) -> Result<(), PortError> {
    for (event_type, body) in REWRITE_DOMAIN_EVENTS {
        let sql = format!(
            "UPDATE domain_events SET payload = {body} \
             WHERE aggregate_type = $1 AND aggregate_id = ANY($2) AND event_type = '{event_type}'"
        );
        sqlx::query(&sql)
            .bind(JOB_AGGREGATE_TYPE)
            .bind(jobs)
            .bind(ERASED_TEXT)
            .execute(&mut *tx)
            .await
            .map_err(unavailable)?;
    }
    Ok(())
}

const REWRITE_OUTBOX: &str = "\
    UPDATE integration_outbox o SET payload = jsonb_set(o.payload, '{payload}', ( \
        SELECT s5.body \
        FROM (SELECT (o.payload -> 'payload') - 'config' - 'passport' - 'payload' - 'text' \
                     - 'subject' - 'body' - 'note' AS body) s1 \
        CROSS JOIN LATERAL (SELECT CASE WHEN s1.body ? 'label' \
            THEN jsonb_set(s1.body, '{label}', to_jsonb($2::text)) ELSE s1.body END AS body) s2 \
        CROSS JOIN LATERAL (SELECT CASE WHEN jsonb_typeof(s2.body -> 'steps') = 'array' \
            THEN jsonb_set(s2.body, '{steps}', \
                 (SELECT coalesce(jsonb_agg(to_jsonb($2::text)), '[]'::jsonb) \
                  FROM jsonb_array_elements(s2.body -> 'steps'))) \
            ELSE s2.body END AS body) s3 \
        CROSS JOIN LATERAL (SELECT CASE WHEN s3.body ? 'params' \
            THEN jsonb_set(s3.body, '{params}', '{}'::jsonb) ELSE s3.body END AS body) s4 \
        CROSS JOIN LATERAL (SELECT CASE WHEN jsonb_typeof(s4.body -> 'failure_report') = 'object' \
            THEN jsonb_set(jsonb_set(s4.body, '{failure_report,params}', '{}'::jsonb), \
                 '{failure_report,diagnostic}', '{}'::jsonb) \
            ELSE s4.body END AS body) s5)) \
    WHERE o.status = 'PUBLISHED' AND (o.payload -> 'payload' ->> 'job_id') = ANY($1)";

pub async fn rewrite_published_outbox(
    tx: &mut PgConnection,
    jobs: &[Uuid],
) -> Result<(), PortError> {
    let texts: Vec<String> = jobs.iter().map(Uuid::to_string).collect();
    sqlx::query(REWRITE_OUTBOX)
        .bind(texts)
        .bind(ERASED_TEXT)
        .execute(&mut *tx)
        .await
        .map_err(unavailable)?;
    Ok(())
}
