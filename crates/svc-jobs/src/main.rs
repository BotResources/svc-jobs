//! Thin binary entry point: the SDL escape hatch, dotenv, logging, then
//! [`svc_jobs::run`].
//!
//! Everything real lives in the library so the e2e suite can spawn exactly the
//! binary CD publishes, and the bin target keeps its name for the same reason —
//! `CARGO_BIN_EXE_svc-jobs` is how the suite finds it.

use br_util_observability::init_logging;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    // `svc-jobs schema` prints the SDL and exits. It MUST come before
    // dotenv and the logging init: `init_logging` writes JSON to stdout, and CD
    // parses this command's stdout as the SDL document it poses on the registry
    // PatchVersion. One log line ahead of it and the document is corrupt.
    if std::env::args().nth(1).as_deref() == Some("schema") {
        println!("{}", svc_jobs::graphql::sdl());
        return std::process::ExitCode::SUCCESS;
    }

    let _ = dotenvy::dotenv();
    init_logging("svc-jobs");

    match svc_jobs::run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(err) => {
            tracing::error!(error = %err, "svc-jobs failed to boot");
            std::process::ExitCode::FAILURE
        }
    }
}
