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


use crate::amqp::AmqpSource;
use crate::kafka::KafkaSource;
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

    // --- Initialize Components from the library ---
    let dedup_store = Arc::new(DeduplicationStore::new(
        &config.sled_path,
        config.dedup_ttl_seconds,
    )?);

    // --- Create shared Sinks and Sources ---
    let kafka_source = Arc::new(KafkaSource::new(&config)?);
    let nats_source = Arc::new(NatsSource::new(&config).await?);
    let amqp_source = Arc::new(AmqpSource::new(&config).await?);
    
    let nats_sink = Arc::new(NatsSink::new(&config).await?);
    let kafka_sink = Arc::new(KafkaSink::new(&config)?);
    
    let dlq_sink: Arc<dyn MessageSink + Send + Sync> = kafka_sink.clone(); // Re-use Kafka sink for DLQ

    // --- Start Bridge Tasks ---
    let kafka_to_nats_task = run_bridge_instance(
        "kafka_to_nats".to_string(),
        kafka_source,
        nats_sink.clone(),
        dedup_store.clone(),
        dlq_sink.clone(),
        shutdown_rx.clone(),
    );

    let nats_to_kafka_task = run_bridge_instance(
        "nats_to_kafka".to_string(),
        nats_source,
        kafka_sink.clone(),
        dedup_store.clone(),
        dlq_sink.clone(),
        shutdown_rx.clone(),
    );

    let amqp_to_nats_task = run_bridge_instance(
        "amqp_to_nats".to_string(),
        amqp_source,
        nats_sink.clone(),
        dedup_store.clone(),
        dlq_sink.clone(),
        shutdown_rx.clone(),
    );

    info!("Starting all bridge tasks...");

    // Run all bridge tasks concurrently.
    // The `run` function will return if any of the tasks fail.
    tokio::try_join!(
        tokio::spawn(amqp_to_nats_task),
        tokio::spawn(kafka_to_nats_task),
        tokio::spawn(nats_to_kafka_task)
    )?;

    Ok(())
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