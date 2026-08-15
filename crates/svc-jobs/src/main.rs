use br_util_observability::init_logging;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    if std::env::args().nth(1).as_deref() == Some("schema") {
        println!("{}", svc_jobs::edge::sdl());
        return std::process::ExitCode::SUCCESS;
    }

    let _ = dotenvy::dotenv();
    init_logging("svc-jobs");

    match svc_jobs::run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(error = %error, "svc-jobs failed to boot");
            std::process::ExitCode::FAILURE
        }
    }
}
