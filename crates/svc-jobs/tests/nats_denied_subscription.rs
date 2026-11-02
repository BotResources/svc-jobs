use std::collections::HashMap;
use std::net::TcpListener;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use metrics::{Counter, CounterFn, Gauge, Histogram, Key, KeyName, Metadata, Recorder, Unit};
use svc_jobs::config::NatsCredentials;
use svc_jobs::runner_transport::connection_options;
use svc_jobs::runner_transport::server_errors::{self, SERVER_ERRORS_TOTAL};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata as SpanMetadata, Subscriber};

const ADVISORY: &str = "$JS.EVENT.ADVISORY.CONSUMER.MSG_TERMINATED.JOBS_TRIGGER.>";
const GRANTED: &str = "jobs.claim.analyst";

type Tally = Arc<Mutex<HashMap<String, u64>>>;
type Lines = Arc<Mutex<Vec<String>>>;

struct Cell {
    name: String,
    tally: Tally,
}

impl CounterFn for Cell {
    fn increment(&self, value: u64) {
        *self
            .tally
            .lock()
            .unwrap()
            .entry(self.name.clone())
            .or_default() += value;
    }

    fn absolute(&self, value: u64) {
        self.tally.lock().unwrap().insert(self.name.clone(), value);
    }
}

struct Recording(Tally);

impl Recorder for Recording {
    fn describe_counter(&self, _: KeyName, _: Option<Unit>, _: metrics::SharedString) {}
    fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: metrics::SharedString) {}
    fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: metrics::SharedString) {}

    fn register_counter(&self, key: &Key, _: &Metadata<'_>) -> Counter {
        Counter::from_arc(Arc::new(Cell {
            name: key.name().to_owned(),
            tally: Arc::clone(&self.0),
        }))
    }

    fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
        Gauge::noop()
    }

    fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
        Histogram::noop()
    }
}

struct Warnings(Lines);

struct Message(String);

impl Visit for Message {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.push_str(&format!("{}={value:?} ", field.name()));
    }
}

impl Subscriber for Warnings {
    fn enabled(&self, _: &SpanMetadata<'_>) -> bool {
        true
    }

    fn new_span(&self, _: &Attributes<'_>) -> Id {
        Id::from_u64(1)
    }

    fn record(&self, _: &Id, _: &Record<'_>) {}

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, event: &Event<'_>) {
        if *event.metadata().level() == tracing::Level::WARN {
            let mut message = Message(String::new());
            event.record(&mut message);
            self.0.lock().unwrap().push(message.0);
        }
    }

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

struct Server {
    child: Child,
    port: u16,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("a free port is found")
        .local_addr()
        .expect("the port is read")
        .port()
}

fn start_server_denying_the_advisory() -> Server {
    let port = free_port();
    let dir = std::env::temp_dir().join(format!("jobs-denied-subscription-{port}"));
    std::fs::create_dir_all(&dir).expect("the configuration directory is made");
    let conf = dir.join("nats.conf");
    std::fs::write(
        &conf,
        format!(
            "port: {port}\nauthorization {{\n  users = [\n    {{ user: \"jobs\", password: \"pw\", \
             permissions: {{\n      publish: {{ allow: [\">\"] }}\n      subscribe: {{ allow: \
             [\">\"], deny: [\"$JS.EVENT.ADVISORY.>\"] }}\n    }} }}\n  ]\n}}\n"
        ),
    )
    .expect("the configuration is written");
    let child = Command::new("nats-server")
        .arg("-c")
        .arg(&conf)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("nats-server starts");
    Server { child, port }
}

async fn connect(server: &Server) -> async_nats::Client {
    let url = format!("127.0.0.1:{}", server.port);
    for _ in 0..50 {
        let credentials = NatsCredentials {
            user: "jobs".into(),
            password: "pw".into(),
        };
        if let Ok(client) = connection_options(Some(&credentials)).connect(&url).await {
            return client;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the server never accepted the connection");
}

fn count(tally: &Tally) -> u64 {
    tally
        .lock()
        .unwrap()
        .get(SERVER_ERRORS_TOTAL)
        .copied()
        .unwrap_or(0)
}

async fn await_count(tally: &Tally, expected: u64) {
    for _ in 0..50 {
        if count(tally) >= expected {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the server error was never counted: {}", count(tally));
}

#[tokio::test]
async fn a_subscription_the_server_refuses_for_want_of_a_right_is_logged_and_counted() {
    let tally: Tally = Arc::default();
    let lines: Lines = Arc::default();
    metrics::set_global_recorder(Recording(Arc::clone(&tally))).expect("the recorder installs");
    tracing::subscriber::set_global_default(Warnings(Arc::clone(&lines)))
        .expect("the subscriber installs");
    server_errors::describe();
    let server = start_server_denying_the_advisory();
    let client = connect(&server).await;

    let granted = client
        .queue_subscribe(GRANTED.to_owned(), "group".to_owned())
        .await
        .expect("a subscription the server grants succeeds");
    client.flush().await.expect("the flush succeeds");
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(
        count(&tally),
        0,
        "a granted subscription is no server error"
    );

    let denied = client
        .queue_subscribe(ADVISORY.to_owned(), "group".to_owned())
        .await;
    assert!(
        denied.is_ok(),
        "the call that subscribes succeeds although the server refuses the subscription"
    );
    client.flush().await.expect("the flush succeeds");

    await_count(&tally, 1).await;
    let warned = lines
        .lock()
        .unwrap()
        .iter()
        .any(|line| line.contains("Permissions Violation"));
    assert!(
        warned,
        "the refusal is logged at warn with the server's wording: {:?}",
        lines.lock().unwrap()
    );
    drop(granted);
}
