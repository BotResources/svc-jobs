use chrono::{DateTime, TimeDelta, Utc};
use sqlx::PgPool;

const RUN_LOG_HORIZON_SQL: &str = "SELECT max(( \
        substring(pg_get_expr(child.relpartbound, child.oid) from 'TO \\(''(.*)''\\)') \
     )::timestamptz) AS horizon \
     FROM pg_class parent \
     JOIN pg_inherits inherited ON inherited.inhparent = parent.oid \
     JOIN pg_class child ON child.oid = inherited.inhrelid \
     JOIN pg_namespace ns ON ns.oid = parent.relnamespace \
     WHERE parent.relname = 'run_logs' AND ns.nspname = current_schema()";

pub async fn report_run_log_horizon(pool: &PgPool, warning: TimeDelta, now: DateTime<Utc>) {
    let horizon: Option<DateTime<Utc>> = match sqlx::query_scalar(RUN_LOG_HORIZON_SQL)
        .fetch_one(pool)
        .await
    {
        Ok(horizon) => horizon,
        Err(error) => {
            tracing::error!(
                error = %error,
                "the run_logs partition horizon could not be read; an exhausted horizon would \
                 reject every log line without warning"
            );
            return;
        }
    };
    let Some(horizon) = horizon else {
        tracing::error!(
            "run_logs declares no range partition at all; every log line will be rejected \
             because the schema deliberately carries no DEFAULT partition"
        );
        return;
    };
    if horizon <= now {
        tracing::error!(
            horizon = %horizon,
            "the run_logs partition horizon is exhausted: every log line is now rejected. \
             Lifecycle facts are unaffected, but an operator must add partitions"
        );
    } else if horizon - now <= warning {
        tracing::error!(
            horizon = %horizon,
            "the run_logs partition horizon is within the configured warning margin; past that \
             date every log line is rejected until an operator adds partitions"
        );
    } else {
        tracing::info!(horizon = %horizon, "run_logs accepts log lines up to its declared horizon");
    }
}
