use crate::config::AmqpConfig;
use crate::model::CanonicalMessage;
use crate::sinks::MessageSink;
use crate::sources::{BoxFuture, BoxedMessageStream, MessageSource};
use anyhow::anyhow;
use async_trait::async_trait;
use futures::StreamExt;
use lapin::tcp::{OwnedIdentity, OwnedTLSConfig};
use lapin::{
    options::{
        BasicAckOptions, BasicConsumeOptions, BasicPublishOptions, BasicQosOptions,
        QueueDeclareOptions,
    },
    types::FieldTable,
    BasicProperties, Channel, Connection, ConnectionProperties, Consumer,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::Mutex;
use tracing::info;

pub struct AmqpSink {
    channel: Channel,
    exchange: String,
    routing_key: String,
}

impl AmqpSink {
    pub async fn new(config: &AmqpConfig, routing_key: &str) -> anyhow::Result<Self> {
        let conn = create_amqp_connection(config).await?;
        let channel = conn.create_channel().await?;

        // Ensure the queue exists before we try to publish to it. This is idempotent.
        info!(queue = %routing_key, "Declaring AMQP queue in sink");
        channel
            .queue_declare(routing_key, QueueDeclareOptions::default(), FieldTable::default())
            .await?;

        Ok(Self {
            channel,
            exchange: "".to_string(),    // Default exchange
            routing_key: routing_key.to_string(),
        })
    }

    pub fn with_routing_key(&self, routing_key: &str) -> Self {
        Self {
            channel: self.channel.clone(),
            exchange: self.exchange.clone(),
            routing_key: routing_key.to_string(),
        }
    }
}

#[async_trait]
impl MessageSink for AmqpSink {
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<Option<CanonicalMessage>> {
        let payload = serde_json::to_vec(&message)?;
        self.channel
            .basic_publish(
                &self.exchange,
                &self.routing_key,
                BasicPublishOptions::default(),
                &payload,
                BasicProperties::default(),
            )
            .await?
            .await?; // Wait for publisher confirm
        Ok(None)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct AmqpSource {
    consumer: Arc<Mutex<Consumer>>,
}

use std::any::Any;
impl AmqpSource {
    pub async fn new(config: &AmqpConfig, queue: &str) -> anyhow::Result<Self> {
        let conn = create_amqp_connection(config).await?;
        let channel = conn.create_channel().await?;

        info!(queue = %queue, "Declaring AMQP queue");
        channel
            .queue_declare(queue, QueueDeclareOptions::default(), FieldTable::default())
            .await?;

        // Set prefetch count to 1 to ensure we only process one message at a time
        channel.basic_qos(1, BasicQosOptions::default()).await?;

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

async fn create_amqp_connection(config: &AmqpConfig) -> anyhow::Result<Connection> {
    info!(url = %config.url, "Connecting to AMQP broker");
    let mut conn_uri = config.url.clone();

    if let (Some(user), Some(pass)) = (&config.username, &config.password) {
        let mut url = url::Url::parse(&conn_uri)?;
        url.set_username(user)
            .map_err(|_| anyhow!("Failed to set username on AMQP URL"))?;
        url.set_password(Some(pass))
            .map_err(|_| anyhow!("Failed to set password on AMQP URL"))?;
        conn_uri = url.to_string();
    }

    let mut last_error = None;
    for attempt in 1..=5 {
        info!(url = %conn_uri, attempt = attempt, "Attempting to connect to AMQP broker");
        let conn_props = ConnectionProperties::default();
        let result = if config.tls.required {
            let tls_config = build_tls_config(config).await?;
            Connection::connect_with_config(&conn_uri, conn_props, tls_config).await
        } else {
            Connection::connect(&conn_uri, conn_props).await
        };

        match result {
            Ok(conn) => return Ok(conn),
            Err(e) => {
                last_error = Some(e);
                tokio::time::sleep(Duration::from_secs(attempt * 2)).await; // Exponential backoff
            }
        }
    }
    Err(anyhow!("Failed to connect to AMQP after multiple attempts: {:?}", last_error.unwrap()))
}

async fn build_tls_config(config: &AmqpConfig) -> anyhow::Result<OwnedTLSConfig> {
    // For AMQP, cert_chain is the CA file.
    let ca_file = config.tls.ca_file.clone();

    let identity = if let Some(cert_file) = &config.tls.cert_file {
        // For lapin, client identity is provided via a PKCS12 file.
        // The `cert_file` is assumed to be the PKCS12 bundle. The `key_file` is not used.
        let der = tokio::fs::read(cert_file).await?;
        let password = config.tls.cert_password.clone().unwrap_or_default();
        Some(OwnedIdentity::PKCS12 { der, password })
    } else {
        None
    };

    Ok(OwnedTLSConfig {
        identity,
        cert_chain: ca_file,
    })
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
        let message = CanonicalMessage::deserialized_new(payload);

        let commit = Box::new(move |_response| {
            Box::pin(async move {
                delivery
                    .ack(BasicAckOptions::default())
                    .await
                    .expect("Failed to ack AMQP message");
                info!(
                    delivery_tag = delivery.delivery_tag,
                    "AMQP message acknowledged"
                );
            }) as BoxFuture<'static, ()>
        });

        Ok((message, commit))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Clone for AmqpSource {
    fn clone(&self) -> Self {
        Self {
            consumer: self.consumer.clone(),
        }
    }
}
