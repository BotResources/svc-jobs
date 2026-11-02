use chrono::{DateTime, Utc};

use super::facts::AnswerRecord;
use crate::domain::ids::RunId;
use crate::domain::payload::CallMode;

pub(super) fn read_at(
    mode: CallMode,
    answers: &[AnswerRecord],
    latest_run: RunId,
    latest_is_of_a_restart_job: bool,
) -> Option<DateTime<Utc>> {
    let own = answers.iter().find(|answer| answer.run_id == latest_run);
    let earlier_counts = latest_is_of_a_restart_job
        && match (mode, own) {
            (CallMode::Background, Some(_)) => false,
            (CallMode::Background, None) | (CallMode::Sync, _) => true,
        };
    own.and_then(|answer| answer.read_at).or_else(|| {
        if earlier_counts {
            answers
                .iter()
                .filter(|answer| answer.run_id != latest_run)
                .max_by_key(|answer| (answer.recorded_at, answer.run_id))
                .and_then(|answer| answer.read_at)
        } else {
            None
        }
    })
}
