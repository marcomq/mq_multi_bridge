use crate::config::KafkaConfig;
use crate::model::CanonicalMessage;
use crate::sinks::MessageSink;
use crate::sources::{BoxFuture, BoxedMessageStream, MessageSource};
use anyhow::anyhow;
use async_trait::async_trait;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::Offset;
use rdkafka::{
    consumer::{CommitMode, Consumer, StreamConsumer},
    ClientConfig, Message, TopicPartitionList,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::info;

pub struct KafkaSink {
    producer: FutureProducer,
    topic: String,
}

impl KafkaSink {
    pub fn new(config: &KafkaConfig, topic: &str) -> Result<Self, rdkafka::error::KafkaError> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &config.brokers)
            .set("message.timeout.ms", "5000");

        if config.tls.required {
            client_config.set("security.protocol", "ssl");
            if let Some(ca_file) = &config.tls.ca_file {
                client_config.set("ssl.ca.location", ca_file);
            }
            if let Some(cert_file) = &config.tls.cert_file {
                client_config.set("ssl.certificate.location", cert_file);
            }
            if let Some(key_file) = &config.tls.key_file {
                client_config.set("ssl.key.location", key_file);
            }
            client_config.set(
                "enable.ssl.certificate.verification",
                (!config.tls.accept_invalid_certs).to_string(),
            );
        }
        let producer: FutureProducer = client_config.create()?;
        Ok(Self {
            producer,
            topic: topic.to_string(),
        })
    }

    pub fn with_topic(&self, topic: &str) -> Self {
        Self {
            producer: self.producer.clone(),
            topic: topic.to_string(),
        }
    }
}

#[async_trait]
impl MessageSink for KafkaSink {
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<Option<CanonicalMessage>> {
        let payload = serde_json::to_string(&message)?;
        let key: String = message.message_id.to_string();
        let record = FutureRecord::to(&self.topic).payload(&payload).key(&key);

        self.producer
            .send(record, Duration::from_secs(5))
            .await
            .map_err(|(e, _)| anyhow!("Kafka send error: {}", e))?;
        Ok(None)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

pub struct KafkaSource {
    consumer: Arc<Mutex<StreamConsumer>>,
}
use std::any::Any;

impl KafkaSource {
    pub fn new(config: &KafkaConfig, topic: &str) -> Result<Self, rdkafka::error::KafkaError> {
        let mut client_config = ClientConfig::new();
        client_config
            .set("group.id", &config.group_id)
            .set("bootstrap.servers", &config.brokers)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("socket.connection.setup.timeout.ms", "10000"); // 10 seconds

        if config.tls.required {
            client_config.set("security.protocol", "ssl");
            if let Some(ca_file) = &config.tls.ca_file {
                client_config.set("ssl.ca.location", ca_file);
            }
            if let Some(cert_file) = &config.tls.cert_file {
                client_config.set("ssl.certificate.location", cert_file);
            }
            if let Some(key_file) = &config.tls.key_file {
                client_config.set("ssl.key.location", key_file);
            }
            client_config.set(
                "enable.ssl.certificate.verification",
                (!config.tls.accept_invalid_certs).to_string(),
            );
        }

        let consumer: StreamConsumer = client_config.create()?;
        if !topic.is_empty() {
            consumer.subscribe(&[topic])?;

            info!(topic = %topic, "Kafka source subscribed");
        }

        Ok(Self {
            consumer: Arc::new(Mutex::new(consumer)),
        })
    }

    pub async fn with_topic(&self, topic: &str) -> Result<Self, rdkafka::error::KafkaError> {
        let new_source = self.clone();
        {
            let consumer = new_source.consumer.lock().await;
            consumer.subscribe(&[topic])?;
        }
        info!(topic = %topic, "Kafka source subscribed to new topic");
        Ok(new_source)
    }
}

#[async_trait]
impl MessageSource for KafkaSource {
    async fn receive(&self) -> anyhow::Result<(CanonicalMessage, BoxedMessageStream)> {
        let consumer_arc = self.consumer.clone();
        let lock = self.consumer.lock().await;
        let message = lock.recv().await?;

        let payload = message
            .payload()
            .ok_or_else(|| anyhow!("Kafka message has no payload"))?;
        let canonical_message: CanonicalMessage =
            serde_json::from_slice(payload).map_err(anyhow::Error::from)?;

        // We can't move the `BorrowedMessage` into the commit closure because it has a limited lifetime.
        // Instead, we extract the information needed for the commit.
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(
            message.topic(),
            message.partition(),
            Offset::Offset(message.offset() + 1),
        )?;
        let commit = Box::new(move |_response| {
            let consumer_arc_clone = consumer_arc.clone();
            Box::pin(async move {
                let consumer = consumer_arc_clone.lock().await;
                consumer
                    .commit(&tpl, CommitMode::Async)
                    .unwrap_or_else(|e| tracing::error!("Failed to commit Kafka message: {:?}", e));
            }) as BoxFuture<'static, ()>
        });

        Ok((canonical_message, commit))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Clone for KafkaSource {
    fn clone(&self) -> Self {
        Self {
            consumer: self.consumer.clone(),
        }
    }
}
