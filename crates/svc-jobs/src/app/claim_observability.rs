use bc_jobs::domain::keys::RunnerTypeKey;
use metrics::{describe_gauge, gauge};

pub const CLAIMS_HELD: &str = "jobs_claims_held";

pub fn describe() {
    describe_gauge!(
        CLAIMS_HELD,
        "Claims registered on this pod to wait for work, by runner type: a claim counts from its \
         registration until the pod answers it"
    );
}

pub fn held(runner_type: &RunnerTypeKey) {
    gauge!(CLAIMS_HELD, "runner_type" => runner_type.as_str().to_owned()).increment(1.0);
}

pub fn released(runner_type: &RunnerTypeKey) {
    gauge!(CLAIMS_HELD, "runner_type" => runner_type.as_str().to_owned()).decrement(1.0);
}
