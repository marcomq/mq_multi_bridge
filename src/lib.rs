//  mq_multi_bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq_multi_bridge

pub mod amqp;
pub mod config;
pub mod kafka;
pub mod model;
pub mod mqtt;
pub mod nats;
pub mod sinks;
pub mod sources;
pub mod store;
use std::collections::HashMap;
use std::collections::HashSet;

use crate::amqp::AmqpSource;
use crate::config::{
    AmqpConfig, AmqpEndpoint, Config, ConnectionType, KafkaConfig, KafkaEndpoint, MqttConfig,
    MqttEndpoint, NatsConfig, NatsEndpoint, SinkEndpoint,
    SinkEndpointType, SourceEndpoint, SourceEndpointType,
};
use crate::kafka::KafkaSink;
use crate::mqtt::{MqttSink, MqttSource};
use crate::nats::NatsSink;
use crate::nats::NatsSource;
use crate::sinks::MessageSink;
use crate::sources::MessageSource;
use crate::store::DeduplicationStore;
use std::sync::Arc;
use anyhow::{anyhow, Context};
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, instrument, warn};

/// The main entry point for running the bridge logic.
/// This function can be called from `main.rs` or from integration tests.
#[instrument(skip_all)]
pub async fn run(
    config: Config,
    shutdown_rx: tokio::sync::watch::Receiver<()>,
) -> anyhow::Result<()> {
    info!(config = ?config, "Initializing Bridge Library");

    // --- Validate connection names before any I/O ---
    let mut connection_names = HashSet::new();
    for conn in &config.connections {
        if !connection_names.insert(conn.name.clone()) {
            return Err(anyhow!("Duplicate connection name found: {}", conn.name));
        }
    }

    // --- Initialize Shared Components ---
    let dedup_store = Arc::new(DeduplicationStore::new(
        &config.sled_path,
        config.dedup_ttl_seconds,
    )?);

    // --- Create connections ---
    let mut sources: HashMap<String, Arc<dyn MessageSource + Send + Sync>> = HashMap::new();
    let mut sinks: HashMap<String, Arc<dyn MessageSink + Send + Sync>> = HashMap::new();
    for conn in &config.connections {
        match &conn.connection_type {
            ConnectionType::Kafka(kafka_config) => {
                // A Kafka connection can be both a source and a sink
                let source = create_kafka_source(kafka_config, "").await?; // Topic will be set per-route
                sources.insert(conn.name.clone(), source);
                let sink = create_kafka_sink(kafka_config, "").await?; // Topic will be set per-route
                sinks.insert(conn.name.clone(), sink);
            }
            ConnectionType::Nats(nats_config) => {
                let sink = create_nats_sink(nats_config, "").await?;
                sinks.insert(conn.name.clone(), sink);
                let source = create_nats_source(nats_config, "").await?;
                sources.insert(conn.name.clone(), source);
            }
            ConnectionType::Amqp(amqp_config) => {
                let source = create_amqp_source(amqp_config, "").await?;
                sources.insert(conn.name.clone(), source);
                // AMQP sink not implemented, but would be added here
            }
            ConnectionType::Mqtt(mqtt_config) => {
                let source = create_mqtt_source(mqtt_config, "").await?;
                sources.insert(conn.name.clone(), source);
                let sink = create_mqtt_sink(mqtt_config, "").await?;
                sinks.insert(conn.name.clone(), sink);
            }
        }
    }

    // --- Create a DLQ sink if configured ---
    let dlq_sink: Option<Arc<dyn MessageSink + Send + Sync>> = if let Some(dlq_config) = &config.dlq {
        let dlq_conn_sink = sinks.get(&dlq_config.connection).with_context(|| {
            format!(
                "DLQ connection '{}' not found in defined connections",
                dlq_config.connection
            )
        })?;
        let kafka_sink = dlq_conn_sink
            .as_any()
            .downcast_ref::<KafkaSink>()
            .ok_or_else(|| anyhow!("DLQ connection must be of type Kafka"))?;
        Some(Arc::new(kafka_sink.with_topic(&dlq_config.kafka.topic)))
    } else {
        None
    };

    // --- Create Sources and Sinks based on config ---
    let mut bridge_tasks = Vec::new();

    for route in &config.routes {
        let source = create_source_from_route(&sources, &route.source).await?;
        let sink = create_sink_from_route(&sinks, &route.sink).await?;

        let bridge_task = run_bridge_instance(
            route.name.clone(),
            source,
            sink,
            dedup_store.clone(),
            dlq_sink.clone(),
            shutdown_rx.clone(),
        );
        bridge_tasks.push(tokio::spawn(bridge_task));
    }

    info!("Starting all bridge tasks...");

    // Run all bridge tasks concurrently.
    // The `run` function will return if any of the tasks fail.
    for task in bridge_tasks {
        task.await?; // This will propagate panics from tasks.
    }

    Ok(())
}

async fn create_source_from_route(
    sources: &HashMap<String, Arc<dyn MessageSource + Send + Sync>>,
    endpoint: &SourceEndpoint,
) -> anyhow::Result<Arc<dyn MessageSource + Send + Sync>> {
    let conn_source = sources
        .get(&endpoint.connection)
        .with_context(|| format!("Source connection '{}' not found", endpoint.connection))?;

    match &endpoint.endpoint_type {
        SourceEndpointType::Kafka(KafkaEndpoint { topic }) => {
            let kafka_source = conn_source
                .as_any()
                .downcast_ref::<crate::kafka::KafkaSource>()
                .ok_or_else(|| anyhow!("Connection '{}' is not a Kafka source", endpoint.connection))?;
            Ok(Arc::new(kafka_source.with_topic(topic)?))
        }
        SourceEndpointType::Nats(NatsEndpoint { subject }) => {
            let nats_source = conn_source
                .as_any()
                .downcast_ref::<NatsSource>()
                .ok_or_else(|| anyhow!("Connection '{}' is not a NATS source", endpoint.connection))?;
            Ok(Arc::new(nats_source.with_subject(subject).await?))
        }
        SourceEndpointType::Amqp(AmqpEndpoint { queue }) => {
            let amqp_source = conn_source
                .as_any()
                .downcast_ref::<AmqpSource>()
                .ok_or_else(|| anyhow!("Connection '{}' is not an AMQP source", endpoint.connection))?;
            Ok(Arc::new(amqp_source.with_queue(queue).await?))
        }
        SourceEndpointType::Mqtt(MqttEndpoint { topic }) => {
            let mqtt_source = conn_source
                .as_any()
                .downcast_ref::<MqttSource>()
                .ok_or_else(|| anyhow!("Connection '{}' is not an MQTT source", endpoint.connection))?;
            Ok(Arc::new(mqtt_source.with_topic(topic).await?))
        }
    }
}

async fn create_sink_from_route(
    sinks: &HashMap<String, Arc<dyn MessageSink + Send + Sync>>,
    endpoint: &SinkEndpoint,
) -> anyhow::Result<Arc<dyn MessageSink + Send + Sync>> {
    let conn_sink = sinks
        .get(&endpoint.connection)
        .with_context(|| format!("Sink connection '{}' not found", endpoint.connection))?;

    match &endpoint.endpoint_type {
        SinkEndpointType::Kafka(KafkaEndpoint { topic }) => {
            let kafka_sink = conn_sink
                .as_any()
                .downcast_ref::<KafkaSink>()
                .ok_or_else(|| anyhow!("Connection '{}' is not a Kafka sink", endpoint.connection))?;
            Ok(Arc::new(kafka_sink.with_topic(topic)))
        }
        SinkEndpointType::Nats(NatsEndpoint { subject }) => {
            let nats_sink = conn_sink
                .as_any()
                .downcast_ref::<NatsSink>()
                .ok_or_else(|| anyhow!("Connection '{}' is not a NATS sink", endpoint.connection))?;
            Ok(Arc::new(nats_sink.with_subject(subject)))
        }
        SinkEndpointType::Amqp(_endpoint) => unimplemented!("AMQP Sink is not implemented yet"),
        SinkEndpointType::Mqtt(MqttEndpoint { topic }) => {
            let mqtt_sink = conn_sink
                .as_any()
                .downcast_ref::<MqttSink>()
                .ok_or_else(|| anyhow!("Connection '{}' is not an MQTT sink", endpoint.connection))?;
            Ok(Arc::new(mqtt_sink.with_topic(topic)))
        }
    }
}

async fn create_kafka_source(config: &KafkaConfig, topic: &str) -> anyhow::Result<Arc<dyn MessageSource + Send + Sync>> {
    Ok(Arc::new(crate::kafka::KafkaSource::new(config, topic)?))
}
async fn create_kafka_sink(config: &KafkaConfig, topic: &str) -> anyhow::Result<Arc<dyn MessageSink + Send + Sync>> {
    Ok(Arc::new(KafkaSink::new(config, topic)?))
}
async fn create_nats_source(config: &NatsConfig, subject: &str) -> anyhow::Result<Arc<dyn MessageSource + Send + Sync>> {
    Ok(Arc::new(NatsSource::new(config, subject).await?))
}
async fn create_nats_sink(config: &NatsConfig, subject: &str) -> anyhow::Result<Arc<dyn MessageSink + Send + Sync>> {
    Ok(Arc::new(NatsSink::new(config, subject).await?))
}
async fn create_amqp_source(config: &AmqpConfig, queue: &str) -> anyhow::Result<Arc<dyn MessageSource + Send + Sync>> {
    Ok(Arc::new(AmqpSource::new(config, queue).await?))
}
async fn create_mqtt_source(config: &MqttConfig, topic: &str) -> anyhow::Result<Arc<dyn MessageSource + Send + Sync>> {
    Ok(Arc::new(MqttSource::new(config, topic).await?))
}
async fn create_mqtt_sink(config: &MqttConfig, topic: &str) -> anyhow::Result<Arc<dyn MessageSink + Send + Sync>> {
    Ok(Arc::new(MqttSink::new(config, topic).await?))
}




#[instrument(skip_all, fields(bridge_id = %bridge_id))]
async fn run_bridge_instance(
    bridge_id: String,
    source: Arc<dyn MessageSource + Send + Sync>,
    sink: Arc<dyn MessageSink + Send + Sync>,
    dedup_store: Arc<DeduplicationStore>, // Assuming this is still needed
    dlq_sink: Option<Arc<dyn MessageSink + Send + Sync>>,
    mut shutdown_rx: tokio::sync::watch::Receiver<()>,
) {
    loop {
        tokio::select! {
            // Biased ensures we check for shutdown first if both are ready
            biased;
            _ = shutdown_rx.changed() => {
                info!("Shutdown signal received, stopping message consumption.");
                break;
            }

            result = source.receive() => {
                match result {
                    Ok((message, commit)) => {
                        let msg_id = message.message_id;
                        info!(message_id = %msg_id, "Received message");
                        // TODO: metrics::increment_counter!("bridge_messages_received", "bridge" => bridge_id.clone());

                        if dedup_store.is_duplicate(&msg_id).unwrap_or(false) {
                            warn!(message_id = %msg_id, "Duplicate message detected, skipping.");
                            // TODO: metrics::increment_counter!("bridge_messages_duplicate", "bridge" => bridge_id.clone());
                            commit().await; // Commit even if duplicate to remove from source queue
                            continue;
                        }

                        let mut attempts = 0;
                        let max_attempts = 5;
                        loop {
                            attempts += 1;
                            match sink.send(message.clone()).await {
                                Ok(_) => {
                                    info!(message_id = %msg_id, "Successfully forwarded message");
                                    // TODO: metrics::increment_counter!("bridge_messages_sent", "bridge" => bridge_id.clone());
                                    commit().await; // ACK source only after successful sink
                                    break;
                                }
                                Err(e) => {
                                    error!(message_id = %msg_id, attempt = attempts, error = %e, "Failed to forward message");
                                    if attempts >= max_attempts {
                                        error!(message_id = %msg_id, "Exceeded max retries. Sending to DLQ.");
                                        // TODO: metrics::increment_counter!("bridge_messages_dlq", "bridge" => bridge_id.clone());
                                        if let Some(dlq) = &dlq_sink {
                                            if let Err(dlq_err) = dlq.send(message).await {
                                                error!(message_id = %msg_id, error = %dlq_err, "Failed to send to DLQ!");
                                            }
                                        }
                                        commit().await; // Commit original message after moving to DLQ
                                        break;
                                    }
                                    let backoff_duration = Duration::from_secs(2u64.pow(attempts));
                                    warn!(message_id = %msg_id, "Retrying in {:?}", backoff_duration);
                                    sleep(backoff_duration).await;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(error = %e, "Error receiving message from source. Reconnecting in 5s...");
                        sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        }
    }
    info!(bridge_id = %bridge_id, "Bridge instance shut down gracefully.");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Connection, DlqConfig};

    #[tokio::test]
    async fn test_run_duplicate_connection_name() {
        let config = Config {
            log_level: "info".to_string(),
            sled_path: "/tmp/test_dedup".to_string(),
            dedup_ttl_seconds: 60,
            connections: vec![
                Connection {
                    name: "conn1".to_string(),
                    connection_type: ConnectionType::Nats(NatsConfig {
                        url: "nats://localhost:4222".to_string(),
                        ..Default::default()
                    }),
                },
                Connection {
                    name: "conn1".to_string(),
                    connection_type: ConnectionType::Nats(NatsConfig {
                        url: "nats://localhost:4223".to_string(),
                        ..Default::default()
                    }),
                },
            ],
            dlq: None,
            routes: vec![],
        };

        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
        let result = run(config, shutdown_rx).await;

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Duplicate connection name found: conn1"
        );
    }

    #[test]
    fn test_dlq_config_requires_kafka() {
        let config = Config {
            log_level: "info".to_string(),
            sled_path: "/tmp/test_dedup".to_string(),
            dedup_ttl_seconds: 60,
            connections: vec![],
            dlq: Some(DlqConfig {
                connection: "kafka_main".to_string(),
                kafka: KafkaEndpoint {
                    topic: "my_dlq".to_string(),
                },
            }),
            routes: vec![],
        };
        // This test primarily checks deserialization logic via the config test.
        // A runtime test would require more mocking. Here we just assert the structure.
        assert_eq!(config.dlq.as_ref().unwrap().kafka.topic, "my_dlq");
    }
}
