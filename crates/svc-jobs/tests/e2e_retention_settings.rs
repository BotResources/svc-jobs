mod support;

use support::fixture::{JobsFixture, Knobs};
use support::spawn::{attempt_boot, spawn_instance};

fn retention(days: Option<&str>, cancelled: Option<&str>) -> Knobs {
    Knobs {
        retention_days: days.map(str::to_owned),
        cancelled_retention_days: cancelled.map(str::to_owned),
        ..Knobs::default()
    }
}

async fn assert_boots(fixture: &JobsFixture, knobs: Knobs) {
    let (service, _base_url) =
        spawn_instance(fixture.database(), &fixture.fabric().url(), &knobs).await;
    service.shutdown().await;
}

async fn assert_refused(fixture: &JobsFixture, knobs: Knobs, case: &str, key: &str, value: &str) {
    let attempt = attempt_boot(fixture.database(), &fixture.fabric().url(), &knobs).await;
    let status = attempt.outcome.exit_status();
    assert!(
        status.is_some_and(|status| !status.success()),
        "{case}: Jobs refuses to boot outside the bounds, it never serves ({:?})\nlogs:\n{}",
        attempt.outcome,
        attempt.logs,
    );
    let expected = format!("invalid_configuration: {key}={value}");
    assert!(
        attempt.logs.contains(&expected),
        "{case}: the refusal names the setting and its value, `{expected}`\nlogs:\n{}",
        attempt.logs,
    );
}

#[tokio::test]
async fn the_retention_settings_have_defaults_and_bounds() {
    let fixture = JobsFixture::start().await;

    assert_boots(&fixture, retention(None, None)).await;
    assert_boots(&fixture, retention(Some("1"), Some("0"))).await;
    assert_boots(&fixture, retention(Some("1"), Some("1"))).await;
    assert_boots(&fixture, retention(Some("395"), Some("395"))).await;
    assert_boots(&fixture, retention(None, Some("30"))).await;

    let retention_key = "JOBS_RETENTION_DAYS";
    for value in ["0", "-1", "396", "abc"] {
        let case = format!("retention {value}");
        assert_refused(
            &fixture,
            retention(Some(value), None),
            &case,
            retention_key,
            value,
        )
        .await;
    }

    let cancelled_key = "JOBS_CANCELLED_RETENTION_DAYS";
    for value in ["-1", "396", "abc"] {
        let case = format!("cancelled retention {value}");
        assert_refused(
            &fixture,
            retention(Some("395"), Some(value)),
            &case,
            cancelled_key,
            value,
        )
        .await;
    }

    assert_refused(
        &fixture,
        retention(Some("10"), Some("11")),
        "cancelled above retention",
        cancelled_key,
        "11 (must not exceed JOBS_RETENTION_DAYS = 10)",
    )
    .await;
    assert_refused(
        &fixture,
        retention(None, Some("31")),
        "cancelled above the default retention",
        cancelled_key,
        "31 (must not exceed JOBS_RETENTION_DAYS = 30)",
    )
    .await;

    fixture.shutdown().await;
}
