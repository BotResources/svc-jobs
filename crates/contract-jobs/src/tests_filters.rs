use super::runner::{CLAIM_FILTER, CLAIM_V2_FILTER, LOG_FILTER, STATUS_FILTER, TRIGGER_FILTER};
use super::runner_surface::*;
use super::runner_transport::*;
use super::segment::SubjectSegment;

fn matches(filter: &str, subject: &str) -> bool {
    let filter_tokens: Vec<&str> = filter.split('.').collect();
    let subject_tokens: Vec<&str> = subject.split('.').collect();
    for (index, token) in filter_tokens.iter().enumerate() {
        if *token == ">" {
            return index == filter_tokens.len() - 1 && subject_tokens.len() > index;
        }
        match subject_tokens.get(index) {
            Some(subject_token) if *token == "*" || token == subject_token => {}
            _ => return false,
        }
    }
    filter_tokens.len() == subject_tokens.len()
}

fn runner_type(text: &str) -> SubjectSegment {
    SubjectSegment::runner_type(text).unwrap()
}

fn subjects_of(text: &str) -> Vec<(&'static str, String)> {
    let runner_type = runner_type(text);
    let mut subjects = vec![
        ("trigger_v1", trigger_subject(&runner_type)),
        ("trigger_v2", trigger_v2_subject(&runner_type)),
        ("claim_v1", claim_subject(&runner_type)),
        ("claim_v2", claim_v2_subject(&runner_type)),
        ("log", log_subject(&runner_type)),
        ("register_agents", register_agents_subject(&runner_type)),
        ("mark_read", mark_read_subject(&runner_type)),
        ("list_agents", list_agents_subject(&runner_type)),
        ("started", started_subject(&runner_type)),
    ];
    for fact in RunnerStatusFact::ALL {
        subjects.push(("status", status_subject(&runner_type, fact)));
    }
    subjects
}

#[test]
fn the_matcher_reads_star_as_one_token_and_greater_than_as_one_or_more_trailing_tokens() {
    assert!(matches("a.*", "a.b"));
    assert!(!matches("a.*", "a"));
    assert!(!matches("a.*", "a.b.c"));
    assert!(matches("a.>", "a.b"));
    assert!(matches("a.>", "a.b.c"));
    assert!(!matches("a.>", "a"));
    assert!(!matches("a.>", "b.c"));
}

#[test]
fn each_stream_and_surface_filter_matches_exactly_its_own_subjects() {
    let filters: [(&str, &[&str]); 9] = [
        (TRIGGER_FILTER, &["trigger_v1", "trigger_v2"]),
        (CLAIM_FILTER, &["claim_v1"]),
        (CLAIM_V2_FILTER, &["claim_v2"]),
        (STATUS_FILTER, &["status"]),
        (LOG_FILTER, &["log"]),
        (REGISTER_AGENTS_FILTER, &["register_agents"]),
        (MARK_READ_FILTER, &["mark_read"]),
        (LIST_AGENTS_FILTER, &["list_agents"]),
        (STARTED_FILTER, &["started"]),
    ];
    for type_name in ["analyst", "v2"] {
        let subjects = subjects_of(type_name);
        for (filter, expected) in filters {
            for (kind, subject) in &subjects {
                assert_eq!(
                    matches(filter, subject),
                    expected.contains(kind),
                    "{filter} against {kind} of runner type {type_name}: {subject}"
                );
            }
        }
    }
}

#[test]
fn the_trigger_filter_covers_both_trigger_versions_of_a_runner_type_named_v2() {
    let twin = runner_type("v2");
    assert_eq!(trigger_subject(&twin), "jobs.trigger.v2");
    assert_eq!(trigger_v2_subject(&twin), "jobs.trigger.v2.v2");
    assert!(matches(TRIGGER_FILTER, &trigger_subject(&twin)));
    assert!(matches(TRIGGER_FILTER, &trigger_v2_subject(&twin)));
}

#[test]
fn the_version_1_claim_filter_leaves_the_version_2_claim_subjects_out_even_for_a_runner_type_named_v2()
 {
    let twin = runner_type("v2");
    assert_eq!(claim_subject(&twin), "jobs.claim.v2");
    assert_eq!(claim_v2_subject(&twin), "jobs.claim.v2.v2");
    assert!(matches(CLAIM_FILTER, &claim_subject(&twin)));
    assert!(!matches(CLAIM_FILTER, &claim_v2_subject(&twin)));
    assert!(matches(CLAIM_V2_FILTER, &claim_v2_subject(&twin)));
    assert!(!matches(CLAIM_V2_FILTER, &claim_subject(&twin)));
}

#[test]
fn the_status_and_log_filters_cover_every_status_fact_and_the_log_of_a_runner_type() {
    for type_name in ["analyst", "v2"] {
        let runner_type = runner_type(type_name);
        for fact in RunnerStatusFact::ALL {
            assert!(matches(STATUS_FILTER, &status_subject(&runner_type, fact)));
        }
        assert!(matches(LOG_FILTER, &log_subject(&runner_type)));
    }
}

#[test]
fn each_surface_filter_is_its_prefix_followed_by_one_wildcard_token() {
    let surface = [
        (REGISTER_AGENTS_FILTER, REGISTER_AGENTS_PREFIX),
        (MARK_READ_FILTER, MARK_READ_PREFIX),
        (LIST_AGENTS_FILTER, LIST_AGENTS_PREFIX),
        (STARTED_FILTER, STARTED_PREFIX),
    ];
    for (filter, prefix) in surface {
        assert_eq!(filter, format!("{prefix}*"));
    }
}
