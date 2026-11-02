use super::*;
use crate::domain::policy::ServiceLimits;
use chrono::TimeZone;

fn refused_retention(value: i64) -> JobsError {
    JobsError::OutOfRange {
        field: "retention_days",
        value,
    }
}

fn refused_cancelled(value: i64) -> JobsError {
    JobsError::OutOfRange {
        field: "cancelled_retention_days",
        value,
    }
}

#[test]
fn the_defaults_keep_a_tree_thirty_days_and_erase_a_cancelled_request_at_once() {
    let retention = Retention::default();
    assert_eq!(retention.tree(), TimeDelta::days(30));
    assert_eq!(retention.cancelled(), TimeDelta::zero());
    assert_eq!(
        Retention::from_days(DEFAULT_RETENTION_DAYS, DEFAULT_CANCELLED_RETENTION_DAYS),
        Ok(retention)
    );
}

#[test]
fn the_service_limits_start_with_the_default_retention_and_carry_a_given_one() {
    let defaults = ServiceLimits::default();
    assert_eq!(defaults.retention(), Retention::default());
    assert_eq!(defaults.tree_retention(), TimeDelta::days(30));
    assert_eq!(defaults.cancelled_retention(), TimeDelta::zero());

    let given = defaults.with_retention(Retention::from_days(90, 7).unwrap());
    assert_eq!(given.tree_retention(), TimeDelta::days(90));
    assert_eq!(given.cancelled_retention(), TimeDelta::days(7));
}

#[test]
fn the_retention_accepts_one_day_up_to_thirteen_months() {
    for days in [1, 2, 30, 394, 395] {
        let retention = Retention::from_days(days, 0).unwrap();
        assert_eq!(retention.tree(), TimeDelta::days(days));
    }
}

#[test]
fn the_retention_refuses_zero_negative_and_above_thirteen_months() {
    for days in [0, -1, -30, 396, 1_000, i64::MAX, i64::MIN] {
        assert_eq!(Retention::from_days(days, 0), Err(refused_retention(days)));
    }
}

#[test]
fn the_cancelled_retention_accepts_zero_up_to_the_retention() {
    for (days, cancelled) in [(30, 0), (30, 1), (30, 30), (1, 1), (395, 395), (395, 394)] {
        let retention = Retention::from_days(days, cancelled).unwrap();
        assert_eq!(retention.cancelled(), TimeDelta::days(cancelled));
    }
}

#[test]
fn the_cancelled_retention_refuses_negative_and_above_thirteen_months() {
    for (days, cancelled) in [(30, -1), (395, 396), (395, i64::MAX), (395, i64::MIN)] {
        assert_eq!(
            Retention::from_days(days, cancelled),
            Err(refused_cancelled(cancelled))
        );
    }
}

#[test]
fn the_cancelled_retention_above_the_retention_is_refused_with_both_values() {
    for (days, cancelled) in [(30, 31), (1, 2), (394, 395)] {
        assert_eq!(
            Retention::from_days(days, cancelled),
            Err(JobsError::CancelledRetentionAboveRetention {
                cancelled,
                retention: days,
            })
        );
    }
}

#[test]
fn an_invalid_retention_is_reported_before_an_invalid_cancelled_retention() {
    assert_eq!(Retention::from_days(0, -1), Err(refused_retention(0)));
}

fn retention(cancelled_days: i64) -> Retention {
    Retention::from_days(30, cancelled_days).unwrap()
}

#[test]
fn with_no_delay_the_request_data_of_a_cancel_is_due_the_moment_of_the_cancel() {
    let at = Utc.timestamp_opt(100, 0).unwrap();
    assert!(retention(0).cancelled_request_data_is_due(at, at));
    assert_eq!(retention(0).cancelled_request_data_due_before(at), at);
}

#[test]
fn with_a_delay_the_request_data_is_due_only_once_the_days_have_passed_since_the_cancel() {
    let at = Utc.timestamp_opt(100, 0).unwrap();
    let three_days = retention(3);
    assert!(!three_days.cancelled_request_data_is_due(at, at));
    assert!(
        !three_days
            .cancelled_request_data_is_due(at, at + TimeDelta::days(3) - TimeDelta::seconds(1))
    );
    assert!(three_days.cancelled_request_data_is_due(at, at + TimeDelta::days(3)));
    assert!(three_days.cancelled_request_data_is_due(at, at + TimeDelta::days(30)));
}

#[test]
fn the_instant_the_sweep_selects_by_and_the_due_test_of_a_cancel_are_the_same_rule() {
    let now = Utc.timestamp_opt(10_000_000, 0).unwrap();
    for days in [0, 1, 3, 30] {
        let retention = retention(days);
        let boundary = retention.cancelled_request_data_due_before(now);
        assert!(retention.cancelled_request_data_is_due(boundary, now));
        assert!(!retention.cancelled_request_data_is_due(boundary + TimeDelta::seconds(1), now));
    }
}

fn quiet_tree(last_activity: DateTime<Utc>) -> TreeFacts {
    TreeFacts {
        last_activity,
        unresolved_jobs: 0,
        live_runs: 0,
        scrubbed: false,
    }
}

fn at(day: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 1, day, 12, 0, 0).unwrap()
}

#[test]
fn a_tree_is_scrubbed_once_the_retention_has_passed_since_its_last_activity() {
    let retention = Retention::from_days(30, 0).unwrap();
    let last = Utc.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
    let thirty = last + TimeDelta::days(30);
    assert_eq!(
        retention.scrub_standing(&quiet_tree(last), thirty - TimeDelta::seconds(1)),
        ScrubStanding::Waiting
    );
    assert_eq!(
        retention.scrub_standing(&quiet_tree(last), thirty),
        ScrubStanding::Due
    );
    assert!(retention.scrub_standing(&quiet_tree(last), thirty).is_due());
    assert_eq!(retention.scrub_due_before(thirty), last);
}

#[test]
fn a_tree_with_a_job_that_is_not_terminal_or_a_run_that_is_live_is_never_scrubbed() {
    let retention = Retention::from_days(1, 0).unwrap();
    let long_after = at(28) + TimeDelta::days(300);
    let working = TreeFacts {
        unresolved_jobs: 1,
        ..quiet_tree(at(1))
    };
    let running = TreeFacts {
        live_runs: 2,
        ..quiet_tree(at(1))
    };
    assert_eq!(
        retention.scrub_standing(&working, long_after),
        ScrubStanding::WorkRemains
    );
    assert_eq!(
        retention.scrub_standing(&running, long_after),
        ScrubStanding::WorkRemains
    );
}

#[test]
fn a_scrubbed_tree_is_not_scrubbed_again() {
    let retention = Retention::from_days(1, 0).unwrap();
    let tree = TreeFacts {
        scrubbed: true,
        ..quiet_tree(at(1))
    };
    assert_eq!(
        retention.scrub_standing(&tree, at(28) + TimeDelta::days(300)),
        ScrubStanding::Scrubbed
    );
}

#[test]
fn a_lower_setting_applies_to_the_trees_that_exist_and_a_higher_one_restores_nothing() {
    let tree = quiet_tree(at(1));
    let now = at(1) + TimeDelta::days(6);
    assert_eq!(
        Retention::from_days(30, 0)
            .unwrap()
            .scrub_standing(&tree, now),
        ScrubStanding::Waiting
    );
    assert_eq!(
        Retention::from_days(5, 0)
            .unwrap()
            .scrub_standing(&tree, now),
        ScrubStanding::Due
    );
}
