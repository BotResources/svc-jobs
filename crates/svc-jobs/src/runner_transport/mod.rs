pub mod cancel;
pub mod consume;
pub mod observability;
pub mod presence;
pub mod trigger;

use async_nats::jetstream::kv::Store;
use async_nats::jetstream::stream::Stream;
use async_nats::jetstream::{self, Context};
use bc_jobs::ports::PortError;
use contract_jobs::runner as wire;

use crate::config::{ConsumerTuning, NatsCredentials};
use crate::error::ServiceError;

#[derive(Clone)]
pub struct RunnerChannels {
    jetstream: Context,
    triggers: Stream,
    cancel: Store,
    presence: Store,
    tuning: ConsumerTuning,
}

impl RunnerChannels {
    pub async fn bind(
        nats_url: &str,
        credentials: Option<&NatsCredentials>,
        tuning: ConsumerTuning,
    ) -> Result<Self, ServiceError> {
        let options = match credentials {
            Some(credentials) => async_nats::ConnectOptions::with_user_and_password(
                credentials.user.clone(),
                credentials.password.clone(),
            ),
            None => async_nats::ConnectOptions::new(),
        };
        let client = options
            .connect(nats_url)
            .await
            .map_err(|error| ServiceError::Infra(error.to_string()))?;
        let jetstream = jetstream::new(client);
        Ok(Self {
            triggers: bind_stream(&jetstream, wire::TRIGGER_STREAM).await?,
            cancel: bind_bucket(&jetstream, wire::CANCEL_BUCKET).await?,
            presence: bind_bucket(&jetstream, wire::PRESENCE_BUCKET).await?,
            jetstream,
            tuning,
        })
    }

    pub fn context(&self) -> &Context {
        &self.jetstream
    }

    pub fn consumer_tuning(&self) -> ConsumerTuning {
        self.tuning
    }

    pub fn triggers(&self) -> &Stream {
        &self.triggers
    }

    pub fn cancel_bucket(&self) -> &Store {
        &self.cancel
    }

    pub fn presence_bucket(&self) -> &Store {
        &self.presence
    }

    pub async fn verify_declared_streams(&self) -> Result<(), ServiceError> {
        bind_stream(&self.jetstream, wire::STATUS_STREAM).await?;
        bind_stream(&self.jetstream, wire::LOG_STREAM).await?;
        Ok(())
    }
}

async fn bind_stream(jetstream: &Context, name: &str) -> Result<Stream, ServiceError> {
    jetstream.get_stream(name).await.map_err(|error| {
        ServiceError::Infra(format!(
            "the declared runner-transport stream '{name}' is absent: {error}"
        ))
    })
}

async fn bind_bucket(jetstream: &Context, name: &str) -> Result<Store, ServiceError> {
    jetstream.get_key_value(name).await.map_err(|error| {
        ServiceError::Infra(format!(
            "the declared runner-transport bucket '{name}' is absent: {error}"
        ))
    })
}

pub fn transport_error(detail: impl std::fmt::Display) -> PortError {
    PortError::Unavailable {
        detail: detail.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use bc_jobs::domain::keys::{InstanceKey, RunnerTypeKey};
    use contract_jobs::segment::SubjectSegment;

    const CANDIDATES: &[&str] = &[
        "analyst",
        "worker-python",
        "worker_python",
        "a1",
        "",
        "-leading",
        "_leading",
        "with.dot",
        "with space",
        "with*star",
        "with>arrow",
        "Uppercase",
        "9numeric",
    ];

    #[test]
    fn the_domain_key_and_the_transport_segment_admit_exactly_the_same_alphabet() {
        // Given: the values a runner type or an instance key could ever carry
        for candidate in CANDIDATES {
            // When: the domain key and the published transport segment each judge it
            let domain = RunnerTypeKey::new(candidate).is_ok();
            let transport = SubjectSegment::runner_type(candidate).is_ok();
            // Then: neither may admit what the other refuses, or a key the domain accepted
            // would be unroutable — or worse, would widen a subject the contract validated
            assert_eq!(
                domain, transport,
                "the domain key and the transport segment disagree on '{candidate}'"
            );
            assert_eq!(
                InstanceKey::new(candidate).is_ok(),
                SubjectSegment::instance_key(candidate).is_ok(),
                "the domain instance key and the transport segment disagree on '{candidate}'"
            );
        }
    }

    #[test]
    fn both_alphabets_cap_a_segment_at_the_same_length() {
        // Given: the longest value either side accepts, and one character more
        let longest = "a".repeat(64);
        let over = "a".repeat(65);
        // When/Then: the cap is one contract, not two that drift apart
        assert!(RunnerTypeKey::new(&longest).is_ok());
        assert!(SubjectSegment::runner_type(&longest).is_ok());
        assert!(RunnerTypeKey::new(&over).is_err());
        assert!(SubjectSegment::runner_type(&over).is_err());
    }
}
