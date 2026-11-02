use chrono::TimeDelta;

use super::*;

fn refused(result: Result<Retention, ServiceError>) -> (&'static str, String) {
    match result {
        Err(ServiceError::Configuration { key, value }) => (key, value),
        other => panic!("expected a refused configuration, got {other:?}"),
    }
}

#[test]
fn nothing_declared_gives_thirty_days_and_an_erasure_at_once_for_a_cancel() {
    let retention = retention_from(None, None).unwrap();
    assert_eq!(retention.tree(), TimeDelta::days(30));
    assert_eq!(retention.cancelled(), TimeDelta::zero());
}

#[test]
fn the_bounds_themselves_are_accepted() {
    for (days, cancelled) in [
        ("1", "0"),
        ("1", "1"),
        ("395", "395"),
        ("395", "0"),
        ("30", "30"),
    ] {
        let retention = retention_from(Some(days), Some(cancelled)).unwrap();
        assert_eq!(retention.tree(), TimeDelta::days(days.parse().unwrap()));
        assert_eq!(
            retention.cancelled(),
            TimeDelta::days(cancelled.parse().unwrap())
        );
    }
}

#[test]
fn one_value_declared_leaves_the_other_at_its_default() {
    let only_retention = retention_from(Some("90"), None).unwrap();
    assert_eq!(only_retention.tree(), TimeDelta::days(90));
    assert_eq!(only_retention.cancelled(), TimeDelta::zero());

    let only_cancelled = retention_from(None, Some("30")).unwrap();
    assert_eq!(only_cancelled.tree(), TimeDelta::days(30));
    assert_eq!(only_cancelled.cancelled(), TimeDelta::days(30));
}

#[test]
fn a_retention_of_zero_negative_or_above_thirteen_months_is_refused_by_name() {
    for value in ["0", "-1", "396", "100000"] {
        assert_eq!(
            refused(retention_from(Some(value), None)),
            ("JOBS_RETENTION_DAYS", value.to_owned())
        );
    }
}

#[test]
fn a_cancelled_retention_negative_or_above_thirteen_months_is_refused_by_name() {
    for value in ["-1", "396"] {
        assert_eq!(
            refused(retention_from(Some("395"), Some(value))),
            ("JOBS_CANCELLED_RETENTION_DAYS", value.to_owned())
        );
    }
}

#[test]
fn a_cancelled_retention_above_the_retention_is_refused_and_says_why() {
    let (key, value) = refused(retention_from(Some("10"), Some("11")));
    assert_eq!(key, "JOBS_CANCELLED_RETENTION_DAYS");
    assert_eq!(value, "11 (must not exceed JOBS_RETENTION_DAYS = 10)");

    let (key, value) = refused(retention_from(None, Some("31")));
    assert_eq!(key, "JOBS_CANCELLED_RETENTION_DAYS");
    assert_eq!(value, "31 (must not exceed JOBS_RETENTION_DAYS = 30)");
}

#[test]
fn a_value_that_is_not_a_whole_number_is_refused_by_name() {
    for (retention, cancelled, key, value) in [
        (Some("abc"), None, "JOBS_RETENTION_DAYS", "abc"),
        (Some("1.5"), None, "JOBS_RETENTION_DAYS", "1.5"),
        (Some(""), None, "JOBS_RETENTION_DAYS", ""),
        (None, Some("x"), "JOBS_CANCELLED_RETENTION_DAYS", "x"),
        (None, Some("0.5"), "JOBS_CANCELLED_RETENTION_DAYS", "0.5"),
    ] {
        assert_eq!(
            refused(retention_from(retention, cancelled)),
            (key, value.to_owned())
        );
    }
}
