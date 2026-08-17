use bc_jobs::ports::environment::Clock;
use chrono::{DateTime, Utc};
use svc_jobs::app::environment::SystemClock;

pub fn now() -> DateTime<Utc> {
    SystemClock.now()
}

pub fn now_rfc3339() -> String {
    now().to_rfc3339()
}
