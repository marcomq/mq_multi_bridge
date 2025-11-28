//  mq_multi_bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq_multi_bridge

pub mod config;
pub mod model;
pub mod sinks;
pub mod store;
pub mod sources;
pub mod kafka;
pub mod nats;
pub mod amqp;
use std::collections::HashMap;

use crate::amqp::AmqpSource;
use crate::config::{AmqpEndpoint, KafkaEndpoint, NatsEndpoint, SinkEndpoint, SourceEndpoint};
use crate::nats::NatsSource;
use crate::config::Config;
use crate::kafka::KafkaSink;
use crate::nats::NatsSink;
use crate::sinks::MessageSink;
use crate::sources::MessageSource;
use crate::store::DeduplicationStore;
use std::sync::Arc;
use std::time::{Duration};
use tokio::time::sleep;
use tracing::{error, info, instrument, warn};

/// The main entry point for running the bridge logic.
/// This function can be called from `main.rs` or from integration tests.
#[instrument(skip_all)]
pub async fn run(config: Config, shutdown_rx: tokio::sync::watch::Receiver<()>) -> anyhow::Result<()> {
    info!(config = ?config, "Initializing Bridge Library");

    // --- Initialize Shared Components ---
    let dedup_store = Arc::new(DeduplicationStore::new(
        &config.sled_path,
        config.dedup_ttl_seconds,
    )?);

    // --- Create a single DLQ sink ---
    let dlq_sink: Arc<dyn MessageSink + Send + Sync> = Arc::new(KafkaSink::new(
        &config.kafka.brokers,
        &config.dlq.kafka.topic,
    )?);

    // --- Create Sources and Sinks based on config ---
    // We use HashMaps to avoid creating duplicate connections for the same broker.
    let mut sources: HashMap<String, Arc<dyn MessageSource + Send + Sync>> = HashMap::new();
    let mut sinks: HashMap<String, Arc<dyn MessageSink + Send + Sync>> = HashMap::new();
    let mut bridge_tasks = Vec::new();

    for route in &config.routes {
        let source_key = serde_json::to_string(&route.source)?;
        let source = match sources.get(&source_key) {
            Some(s) => s.clone(),
            None => {
                let new_source = create_source(&config, &route.source).await?;
                sources.insert(source_key, new_source.clone());
                new_source
            }
        };

        let sink_key = serde_json::to_string(&route.sink)?;
        let sink = match sinks.get(&sink_key) {
            Some(s) => s.clone(),
            None => {
                let new_sink = create_sink(&config, &route.sink).await?;
                sinks.insert(sink_key, new_sink.clone());
                new_sink
            }
        };

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

async fn create_source(config: &Config, endpoint: &SourceEndpoint) -> anyhow::Result<Arc<dyn MessageSource + Send + Sync>> {
    match endpoint {
        SourceEndpoint::Kafka(KafkaEndpoint { topic }) => Ok(Arc::new(crate::kafka::KafkaSource::new(&config.kafka.brokers, &config.kafka.group_id, topic)?)),
        SourceEndpoint::Nats(NatsEndpoint { subject }) => Ok(Arc::new(NatsSource::new(&config.nats.url, subject).await?)),
        SourceEndpoint::Amqp(AmqpEndpoint { queue }) => Ok(Arc::new(AmqpSource::new(&config.amqp.url, queue).await?)),
    }
}

async fn create_sink(config: &Config, endpoint: &SinkEndpoint) -> anyhow::Result<Arc<dyn MessageSink + Send + Sync>> {
    match endpoint {
        SinkEndpoint::Kafka(KafkaEndpoint { topic }) => Ok(Arc::new(KafkaSink::new(&config.kafka.brokers, topic)?)),
        SinkEndpoint::Nats(NatsEndpoint { subject }) => Ok(Arc::new(NatsSink::new(&config.nats.url, subject).await?)),
        // Note: AMQP sink implementation is assumed to exist and be similar.
        // If it doesn't exist, this would need to be created.
        SinkEndpoint::Amqp(_endpoint) => unimplemented!("AMQP Sink is not implemented yet"),
    }
}

#[instrument(skip_all, fields(bridge_id = %bridge_id))]
async fn run_bridge_instance(
    bridge_id: String,
    source: Arc<dyn MessageSource + Send + Sync>,
    sink: Arc<dyn MessageSink + Send + Sync>,
    dedup_store: Arc<DeduplicationStore>,
    dlq_sink: Arc<dyn MessageSink + Send + Sync>,
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
                                        if let Err(dlq_err) = dlq_sink.send(message).await {
                                            error!(message_id = %msg_id, error = %dlq_err, "Failed to send to DLQ!");
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