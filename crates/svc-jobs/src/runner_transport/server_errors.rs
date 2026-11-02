use async_nats::{ConnectOptions, Event};
use metrics::{counter, describe_counter};

pub const SERVER_ERRORS_TOTAL: &str = "jobs_nats_server_errors_total";

pub fn describe() {
    describe_counter!(
        SERVER_ERRORS_TOTAL,
        "Errors the NATS server reported to this service on its runner-transport connection, \
         among them a refused subscription or publish for want of a right. The server refuses \
         such an operation without failing the call that made it, so this series is the only \
         signal of a missing right"
    );
    counter!(SERVER_ERRORS_TOTAL).absolute(0);
}

pub fn observed(options: ConnectOptions) -> ConnectOptions {
    options.event_callback(|event| async move { observe(&event) })
}

pub fn observe(event: &Event) {
    if let Event::ServerError(error) = event {
        counter!(SERVER_ERRORS_TOTAL).increment(1);
        tracing::warn!(
            error = %error,
            "the NATS server reported an error on the runner-transport connection; a missing \
             subscribe or publish right is reported this way and the operation does nothing"
        );
    }
}
