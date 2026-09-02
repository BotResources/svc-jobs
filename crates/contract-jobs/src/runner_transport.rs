use uuid::Uuid;

use crate::segment::SubjectSegment;
use crate::{
    CMD_JOBS_TRIGGER_RUNNER_TYPE, EVT_JOBS_LOG_RUNNER_TYPE, EVT_JOBS_STATUS_RUNNER_TYPE_COMPLETED,
    EVT_JOBS_STATUS_RUNNER_TYPE_FAILED, EVT_JOBS_STATUS_RUNNER_TYPE_PLAN_DECLARED,
    EVT_JOBS_STATUS_RUNNER_TYPE_STARTED, EVT_JOBS_STATUS_RUNNER_TYPE_STEP_STARTED, KV_RUN_ID,
    KV_RUNNER_TYPE_INSTANCE_KEY,
};

const RUNNER_TYPE: &str = "{runner_type}";
const INSTANCE_KEY: &str = "{instance_key}";
const RUN_ID: &str = "{run_id}";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunnerStatusFact {
    Started,
    PlanDeclared,
    StepStarted,
    Completed,
    Failed,
}

impl RunnerStatusFact {
    pub const ALL: [Self; 5] = [
        Self::Started,
        Self::PlanDeclared,
        Self::StepStarted,
        Self::Completed,
        Self::Failed,
    ];

    pub fn template(self) -> &'static str {
        match self {
            Self::Started => EVT_JOBS_STATUS_RUNNER_TYPE_STARTED,
            Self::PlanDeclared => EVT_JOBS_STATUS_RUNNER_TYPE_PLAN_DECLARED,
            Self::StepStarted => EVT_JOBS_STATUS_RUNNER_TYPE_STEP_STARTED,
            Self::Completed => EVT_JOBS_STATUS_RUNNER_TYPE_COMPLETED,
            Self::Failed => EVT_JOBS_STATUS_RUNNER_TYPE_FAILED,
        }
    }

    /// The fact segment of the subject — the word a receiver dispatches on.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::PlanDeclared => "plan_declared",
            Self::StepStarted => "step_started",
            Self::Completed => "completed",
            Self::Failed => "failed",
        }
    }

    /// Reads a fact segment back. A receiver that dispatches on this, and names
    /// what it dropped with the same value, cannot end up disagreeing with
    /// itself about which fact a frame carried.
    pub fn parse(segment: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|fact| fact.as_str() == segment)
    }
}

fn render(template: &str, placeholder: &str, value: &str) -> String {
    template.replace(placeholder, value)
}

pub fn trigger_subject(runner_type: &SubjectSegment) -> String {
    render(
        CMD_JOBS_TRIGGER_RUNNER_TYPE,
        RUNNER_TYPE,
        runner_type.as_str(),
    )
}

pub fn status_subject(runner_type: &SubjectSegment, fact: RunnerStatusFact) -> String {
    render(fact.template(), RUNNER_TYPE, runner_type.as_str())
}

pub fn log_subject(runner_type: &SubjectSegment) -> String {
    render(EVT_JOBS_LOG_RUNNER_TYPE, RUNNER_TYPE, runner_type.as_str())
}

pub fn run_cancel_key(run_id: Uuid) -> String {
    render(KV_RUN_ID, RUN_ID, &run_id.to_string())
}

pub fn runner_presence_key(runner_type: &SubjectSegment, instance_key: &SubjectSegment) -> String {
    render(
        render(
            KV_RUNNER_TYPE_INSTANCE_KEY,
            RUNNER_TYPE,
            runner_type.as_str(),
        )
        .as_str(),
        INSTANCE_KEY,
        instance_key.as_str(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const FACTS: &[(RunnerStatusFact, &str)] = &[
        (RunnerStatusFact::Started, "jobs.status.analyst.started"),
        (
            RunnerStatusFact::PlanDeclared,
            "jobs.status.analyst.plan_declared",
        ),
        (
            RunnerStatusFact::StepStarted,
            "jobs.status.analyst.step_started",
        ),
        (RunnerStatusFact::Completed, "jobs.status.analyst.completed"),
        (RunnerStatusFact::Failed, "jobs.status.analyst.failed"),
    ];

    fn analyst() -> SubjectSegment {
        SubjectSegment::runner_type("analyst").unwrap()
    }

    #[test]
    fn every_runner_subject_substitutes_its_runner_type() {
        // Given: a registered runner type key
        let runner_type = analyst();
        // When: each subject of the proprietary transport is rendered
        // Then: the rendering matches the declared template with no placeholder left
        assert_eq!(trigger_subject(&runner_type), "jobs.trigger.analyst");
        assert_eq!(log_subject(&runner_type), "jobs.log.analyst");
        for (fact, expected) in FACTS {
            assert_eq!(&status_subject(&runner_type, *fact), expected);
        }
    }

    #[test]
    fn the_word_a_receiver_dispatches_on_is_the_word_the_subject_carries() {
        // Given: every fact of the transport
        for fact in RunnerStatusFact::ALL {
            // When/Then: its segment is the one its own subject template ends with, and it reads
            // back to itself — one vocabulary, not a builder's and a parser's
            assert!(
                fact.template().ends_with(&format!(".{}", fact.as_str())),
                "{} does not end the subject {}",
                fact.as_str(),
                fact.template(),
            );
            assert_eq!(RunnerStatusFact::parse(fact.as_str()), Some(fact));
        }
    }

    #[test]
    fn a_segment_that_names_no_fact_reads_as_none_never_as_a_neighbour() {
        // Given: segments a publisher could put where a fact belongs
        for segment in ["", "analyst", "FAILED", "fail", "failed_v2", "v2"] {
            // When/Then: none of them borrows the identity of a real fact
            assert_eq!(
                RunnerStatusFact::parse(segment),
                None,
                "'{segment}' must name no fact",
            );
        }
    }

    #[test]
    fn every_status_fact_renders_a_distinct_subject() {
        // Given: the five facts a runner reports on one runner type
        let rendered: Vec<String> = FACTS
            .iter()
            .map(|(fact, _)| status_subject(&analyst(), *fact))
            .collect();
        // When/Then: no two facts collide on the same subject
        for (index, subject) in rendered.iter().enumerate() {
            assert!(
                !rendered[index + 1..].contains(subject),
                "{subject} repeats"
            );
        }
    }

    #[test]
    fn a_rendered_subject_never_keeps_a_placeholder() {
        // Given: every constructor of the proprietary grammar
        let rendered = [
            trigger_subject(&analyst()),
            log_subject(&analyst()),
            status_subject(&analyst(), RunnerStatusFact::Failed),
            run_cancel_key(Uuid::now_v7()),
            runner_presence_key(&analyst(), &SubjectSegment::instance_key("pod-7").unwrap()),
        ];
        // When/Then: a forgotten substitution can never reach the broker
        for subject in rendered {
            assert!(!subject.contains('{'), "{subject} keeps a placeholder");
        }
    }

    #[test]
    fn the_kv_keys_address_one_run_and_one_instance() {
        // Given: a run under cancellation and a live instance announcing itself
        let run_id = Uuid::now_v7();
        // When: the key of each bucket is built
        // Then: the cancel key is the run alone, presence is the runner type and its instance
        assert_eq!(run_cancel_key(run_id), run_id.to_string());
        assert_eq!(
            runner_presence_key(&analyst(), &SubjectSegment::instance_key("pod-7").unwrap()),
            "analyst.pod-7"
        );
    }

    #[test]
    fn no_subject_of_the_transport_can_be_widened_by_its_inputs() {
        // Given: a runner type and an instance key smuggling a separator and a wildcard
        // When: they are offered as transport segments
        // Then: the contract refuses them, so no builder can ever render `jobs.log.*`
        assert!(SubjectSegment::runner_type("a.b").is_err());
        assert!(SubjectSegment::runner_type("*").is_err());
        assert!(SubjectSegment::instance_key("pod-7.>").is_err());
    }
}
