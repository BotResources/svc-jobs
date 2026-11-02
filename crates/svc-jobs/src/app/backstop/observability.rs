use metrics::{counter, describe_counter};

pub const SCRUB_FAILURES_TOTAL: &str = "jobs_scrub_failures_total";

pub fn describe() {
    describe_counter!(
        SCRUB_FAILURES_TOTAL,
        "Trees whose scrub failed. The sweep goes on with the Trees behind a failing one; a \
         series that keeps rising means a Tree cannot be scrubbed"
    );
    counter!(SCRUB_FAILURES_TOTAL).absolute(0);
}

pub fn scrub_failed() {
    counter!(SCRUB_FAILURES_TOTAL).increment(1);
}
