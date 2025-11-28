use crate::model::CanonicalMessage;
use crate::sinks::MessageSink;
use crate::sources::{BoxFuture, BoxedMessageStream, MessageSource};
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
    pub async fn new(url: &str, subject: &str) -> Result<Self, async_nats::ConnectError> {
        let client = async_nats::connect(url).await?;
        Ok(Self {
            client,
            subject: subject.to_string(),
        })
    }

    pub fn with_subject(&self, subject: &str) -> Self {
        Self {
            client: self.client.clone(),
            subject: subject.to_string(),
        }
    }
}

#[async_trait]
impl MessageSink for NatsSink {
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<()> {
        let payload = serde_json::to_vec(&message)?;
        self.client
            .publish(self.subject.clone(), payload.into())
            .await?;
        self.client.flush().await?; // Ensures the message is sent
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct NatsSource {
    subscription: Arc<Mutex<jetstream::consumer::pull::Stream>>,
}
use std::any::Any;

impl NatsSource {
    pub async fn new(url: &str, subject: &str) -> anyhow::Result<Self> {
        let client = async_nats::connect(url).await?;
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

        info!(subject = %subject, "NATS source subscribed");

        Ok(Self {
            subscription: Arc::new(Mutex::new(subscription)),
        })
    }

    pub async fn with_subject(&self, _subject: &str) -> anyhow::Result<Self> {
        // For NATS JetStream, the subject is often bound to the stream/consumer at creation.
        // Re-using the existing subscription is the correct approach here.
        // If different subjects required different consumers, a new consumer would be created here.
        Ok(self.clone())
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

        let canonical_message: CanonicalMessage =
            serde_json::from_slice(&message.payload).map_err(anyhow::Error::from)?;

        let commit = Box::new(move || {
            Box::pin(async move {
                message
                    .ack()
                    .await
                    .unwrap_or_else(|e| tracing::error!("Failed to ACK NATS message: {:?}", e))
            }) as BoxFuture<'static, ()>
        });

        Ok((canonical_message, commit))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Clone for NatsSource {
    fn clone(&self) -> Self {
        Self { subscription: self.subscription.clone() }
    }
}
