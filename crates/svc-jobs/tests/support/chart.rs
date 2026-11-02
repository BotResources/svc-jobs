use std::path::Path;
use std::process::Command;

pub fn rendered_integer_env() -> Vec<(String, String)> {
    let chart = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../charts/br-svc-jobs");
    let output = Command::new("helm")
        .arg("template")
        .arg("ci")
        .arg(&chart)
        .output()
        .expect("helm is installed to render the chart");
    assert!(
        output.status.success(),
        "helm template failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let rendered = String::from_utf8(output.stdout).expect("the rendered chart is utf-8");

    let mut env = Vec::new();
    let mut pending: Option<String> = None;
    for line in rendered.lines().map(str::trim) {
        if let Some(name) = line.strip_prefix("- name: ") {
            pending = name.starts_with("JOBS_").then(|| name.to_owned());
        } else if let (Some(name), Some(value)) = (pending.take(), line.strip_prefix("value: ")) {
            env.push((name, value.trim_matches('"').to_owned()));
        }
    }
    assert!(
        env.len() >= 10,
        "the chart renders its JOBS_* settings ({env:?})"
    );
    env
}
