mod support;

use support::chart::rendered_integer_env;
use support::fixture::{JobsFixture, Knobs};

#[tokio::test]
async fn the_binary_boots_with_the_environment_the_chart_renders_by_default() {
    let chart_env = rendered_integer_env();
    for (name, value) in &chart_env {
        assert!(
            value.chars().all(|character| character.is_ascii_digit()),
            "{name} renders as a plain integer, not `{value}`"
        );
    }

    let fixture = JobsFixture::start_with(Knobs {
        chart_env,
        ..Knobs::default()
    })
    .await;

    fixture.shutdown().await;
}
