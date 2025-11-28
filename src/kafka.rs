use crate::model::CanonicalMessage;
use crate::sinks::MessageSink;
use crate::sources::{BoxedMessageStream, BoxFuture, MessageSource};
use anyhow::anyhow;
use async_trait::async_trait;
use rdkafka::Offset;
use rdkafka::producer::{FutureProducer, FutureRecord};
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
    pub fn new(brokers: &str, topic: &str) -> Result<Self, rdkafka::error::KafkaError> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .create()?;
        Ok(Self {
            producer,
            topic: topic.to_string(),
        })
    }
}

#[async_trait]
impl MessageSink for KafkaSink {
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<()> {
        let payload = serde_json::to_string(&message)?;
        let key: String = message.message_id.to_string();
        let record = FutureRecord::to(&self.topic)
            .payload(&payload)
            .key(&key);

        self.producer.send(record, Duration::from_secs(0)).await.map_err(|(e, _)| e)?;
        Ok(())
    }
}

pub struct KafkaSource {
    consumer: Arc<Mutex<StreamConsumer>>,
}

impl KafkaSource {
    pub fn new(brokers: &str, group_id: &str, topic: &str) -> Result<Self, rdkafka::error::KafkaError> {
        let consumer: StreamConsumer = ClientConfig::new()
            .set("group.id", group_id)
            .set("bootstrap.servers", brokers)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .create()?;

        consumer.subscribe(&[topic])?;

        info!(topic = %topic, "Kafka source subscribed");

        Ok(Self {
            consumer: Arc::new(Mutex::new(consumer)),
        })
    }
}

#[async_trait]
impl MessageSource for KafkaSource {
    async fn receive(&self) -> anyhow::Result<(CanonicalMessage, BoxedMessageStream)> {
        let consumer_arc = self.consumer.clone();
        let lock =  self.consumer.lock().await;
        let message = lock.recv().await?;

        let payload = message.payload().ok_or_else(|| anyhow!("Kafka message has no payload"))?;
        let canonical_message: CanonicalMessage = serde_json::from_slice(payload).map_err(anyhow::Error::from)?;

        // We can't move the `BorrowedMessage` into the commit closure because it has a limited lifetime.
        // Instead, we extract the information needed for the commit.
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(message.topic(), message.partition(), Offset::Offset(message.offset() + 1))?;
        let commit = Box::new(move || {
            let consumer_arc_clone = consumer_arc.clone();
            Box::pin(async move {
                let consumer = consumer_arc_clone.lock().await;
                consumer
                    .commit(&tpl, CommitMode::Async)
                    .unwrap_or_else(|e| {
                        tracing::error!("Failed to commit Kafka message: {:?}", e)
                    });
            }) as BoxFuture<'static, ()>
        });

        Ok((canonical_message, commit))
    }
}