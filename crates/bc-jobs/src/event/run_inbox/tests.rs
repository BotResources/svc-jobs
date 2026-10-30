use super::samples::{Kind, every_kind_of_event, kind_of, samples_of};

#[test]
fn every_kind_of_event_is_classified_by_what_it_does_to_the_inbox_of_a_run() {
    for (event, expected) in every_kind_of_event() {
        assert_eq!(event.run_inbox_effect(), expected, "{}", event.event_type());
    }
}

#[test]
fn the_sample_holds_every_kind_of_event_the_aggregate_records() {
    for kind in Kind::ALL {
        assert!(
            every_kind_of_event()
                .iter()
                .any(|(event, _)| kind_of(event) == *kind),
            "{kind:?} has no sample"
        );
    }
}

#[test]
fn every_sample_is_of_the_kind_it_was_built_for() {
    for kind in Kind::ALL {
        assert!(!samples_of(*kind).is_empty(), "{kind:?} has no sample");
        for (event, _) in samples_of(*kind) {
            assert_eq!(kind_of(&event), *kind, "{}", event.event_type());
        }
    }
}

#[test]
fn only_the_events_that_start_or_end_a_run_or_write_a_notice_serialize_with_the_directory() {
    let serializing: Vec<&str> = every_kind_of_event()
        .iter()
        .filter(|(event, _)| {
            event
                .run_inbox_effect()
                .serializes_with_the_tree_directory()
        })
        .map(|(event, _)| event.event_type())
        .collect();
    assert_eq!(
        serializing,
        [
            "JobQueued",
            "RunStarted",
            "RunCompleted",
            "RunFailed",
            "RunCancelled",
            "RunCancelled",
            "RetryScheduled",
            "JobFailed"
        ]
    );
}
