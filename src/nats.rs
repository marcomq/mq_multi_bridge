use crate::config::Config;
use crate::model::CanonicalMessage;
use crate::sinks::MessageSink;
use crate::sources::{BoxedMessageStream, BoxFuture, MessageSource};
use anyhow::anyhow;
use async_nats::{jetstream, Client};
use async_trait::async_trait;
use futures::StreamExt;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

pub struct NatsSink {
    client: Client,
    subject: String,
}

impl NatsSink {
    pub async fn new(config: &Config) -> Result<Self, async_nats::ConnectError> {
        let client = async_nats::connect(&config.nats_url).await?;
        Ok(Self {
            client,
            subject: config.nats_out_subject.clone(),
        })
    }
}

#[async_trait]
impl MessageSink for NatsSink {
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<()> {
        let payload = serde_json::to_vec(&message)?;
        self.client.publish(self.subject.clone(), payload.into()).await?;
        self.client.flush().await?; // Ensures the message is sent
        Ok(())
    }
}

pub struct NatsSource {
    subscription: Arc<Mutex<jetstream::consumer::pull::Stream>>,
}

impl NatsSource {
    pub async fn new(config: &Config) -> anyhow::Result<Self> {
        let client = async_nats::connect(&config.nats_url).await?;
        let jetstream = jetstream::new(client);

        // This assumes a stream is already created that binds the subject.
        // For durable consumer, we'd need more config.
        let stream = jetstream.get_stream("events").await?;
        let consumer = stream
            .create_consumer(jetstream::consumer::pull::Config {
                durable_name: Some("mq-bridge-nats-consumer".to_string()),
                ..Default::default()
            })
            .await?;

        let subscription = consumer.messages().await?;

        info!(subject = %config.nats_in_subject, "NATS source subscribed");

        Ok(Self {
            subscription: Arc::new(Mutex::new(subscription)),
        })
    }
}

#[async_trait]
impl MessageSource for NatsSource {
    async fn receive(&self) -> anyhow::Result<(CanonicalMessage, BoxedMessageStream)> {
        let mut sub = self.subscription.lock().await;
        let message = sub
            .next()
            .await
            .ok_or_else(|| anyhow!("NATS subscription stream ended"))??;

        let canonical_message: CanonicalMessage = serde_json::from_slice(&message.payload).map_err(anyhow::Error::from)?;

        let commit = Box::new(move || {
            Box::pin(async move { message.ack().await.unwrap_or_else(|e| tracing::error!("Failed to ACK NATS message: {:?}", e)) }) as BoxFuture<'static, ()>
        });

        Ok((canonical_message, commit))
    }
}