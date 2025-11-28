use crate::model::CanonicalMessage;
use crate::sources::{BoxedMessageStream, MessageSource};
use anyhow::anyhow;
use async_trait::async_trait;
use futures::future::BoxFuture;
use futures::StreamExt;
use lapin::{
    options::{BasicAckOptions, BasicConsumeOptions, QueueDeclareOptions},
    types::FieldTable,
    Connection, ConnectionProperties, Consumer,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

pub struct AmqpSource {
    consumer: Arc<Mutex<Consumer>>,
}

use std::any::Any;
impl AmqpSource {
    pub async fn new(url: &str, queue: &str) -> anyhow::Result<Self> {
        info!(url = %url, "Connecting to AMQP broker");
        let conn = Connection::connect(url, ConnectionProperties::default()).await?;
        let channel = conn.create_channel().await?;

        info!(queue = %queue, "Declaring AMQP queue");
        channel
            .queue_declare(queue, QueueDeclareOptions::default(), FieldTable::default())
            .await?;

        let consumer = channel
            .basic_consume(
                queue,
                "mq_multi_bridge_amqp_consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await?;

        Ok(Self {
            consumer: Arc::new(Mutex::new(consumer)),
        })
    }

    pub async fn with_queue(&self, _queue: &str) -> anyhow::Result<Self> {
        // For this implementation, the consumer is tied to a single queue on creation.
        // We will reuse the existing consumer.
        Ok(self.clone())
    }
}

#[async_trait]
impl MessageSource for AmqpSource {
    async fn receive(&self) -> anyhow::Result<(CanonicalMessage, BoxedMessageStream)> {
        let mut consumer_lock = self.consumer.lock().await;
        let delivery = consumer_lock
            .next()
            .await
            .ok_or_else(|| anyhow!("AMQP consumer stream ended"))??;

        let payload: serde_json::Value =
            serde_json::from_slice(&delivery.data).map_err(anyhow::Error::from)?;
        let message = CanonicalMessage::new(payload);

        let commit = Box::new(move || {
            let delivery_tag = delivery.delivery_tag;
            Box::pin(async move {
                // This async block becomes the future
                delivery
                    .ack(BasicAckOptions::default())
                    .await
                    .expect("Failed to ack AMQP message");
                info!(delivery_tag, "AMQP message acknowledged");
            }) as BoxFuture<'static, ()> // Explicitly cast to a trait object
        });

        Ok((message, commit))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Clone for AmqpSource {
    fn clone(&self) -> Self {
        Self { consumer: self.consumer.clone() }
    }
}
