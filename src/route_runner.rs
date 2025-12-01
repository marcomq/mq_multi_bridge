//  mq_multi_bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq_multi_bridge

use crate::config::{Config, Route};
use crate::consumers::MessageConsumer;
use crate::deduplication::DeduplicationStore;
use crate::endpoints::{
    create_consumer_from_route, create_dlq_from_route, create_publisher_from_route,
};
use crate::publishers::MessagePublisher;
use anyhow::{anyhow, Result};
use futures::stream::{self, StreamExt};
use metrics::{counter, histogram};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::sleep;
use tracing::{debug, error, info, instrument, trace};

/// Manages the lifecycle of a single route.
pub(crate) struct RouteRunner {
    name: String,
    route: Route,
    config: Config,
    shutdown_rx: watch::Receiver<()>,
}

impl RouteRunner {
    pub(crate) fn new(
        name: String,
        route: Route,
        config: Config,
        shutdown_rx: watch::Receiver<()>,
    ) -> Self {
        Self {
            name,
            route,
            config,
            shutdown_rx,
        }
    }

    #[instrument(name = "route", skip_all, fields(route.name = %self.name))]
    pub(crate) async fn run(mut self) {
        info!("Initializing route");

        let dedup_store = self.setup_deduplication().await;

        loop {
            // Check for shutdown before attempting to connect.
            if self.shutdown_rx.has_changed().unwrap_or(false) {
                info!("Shutdown signal received during initialization. Route stopping.");
                return;
            }

            // 1. Connect to the publisher (and DLQ) first. Retry on failure.
            let publisher = match self
                .connect_with_retry("publisher", || {
                    create_publisher_from_route(&self.name, &self.route.out)
                })
                .await
            {
                Some(p) => p,
                None => return, // Shutdown was triggered
            };

            // The DLQ is optional, so we handle it separately.
            let dlq_publisher = match self
                .connect_with_retry("dlq", || create_dlq_from_route(&self.route))
                .await
            {
                Some(d) => d,
                None => return, // Shutdown was triggered
            };

            // 2. Connect to the consumer. Retry on failure.
            let consumer = match self
                .connect_with_retry("consumer", || {
                    create_consumer_from_route(&self.name, &self.route.r#in)
                })
                .await
            {
                Some(c) => c,
                None => return, // Shutdown was triggered
            };

            info!("Route connected. Starting message processing.");
            let processing_result = self
                .process_messages(consumer, publisher, dlq_publisher, dedup_store.clone())
                .await;

            match processing_result {
                Ok(_) => {
                    info!("Consumer stream ended (e.g., EOF). Route finished.");
                    break; // Exit the loop, route is done.
                }
                Err(e) => {
                    error!(error = %e, "An unrecoverable error occurred during message processing. Re-establishing connections in 5s.");
                    sleep(Duration::from_secs(5)).await;
                }
            }
        }
    }

    /// Connects to an endpoint, retrying with exponential backoff until successful or shutdown.
    async fn connect_with_retry<T, F, Fut>(&self, endpoint_name: &str, connect_fn: F) -> Option<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut attempts = 0;
        let mut shutdown_rx = self.shutdown_rx.clone();
        loop {
            tokio::select! {
                // Clone the receiver to get a mutable copy for `changed()`
                _ = shutdown_rx.changed() => {
                    info!("Shutdown signal received while connecting to {}. Aborting.", endpoint_name);
                    return None;
                }
                result = connect_fn() => {
                    match result {
                        Ok(endpoint) => {
                            info!("Successfully connected to {}", endpoint_name);
                            return Some(endpoint);
                        }
                        Err(e) => {
                            attempts += 1;
                            let backoff = Duration::from_secs(2u64.pow(attempts).min(60));
                            error!(error = %e, attempt = attempts, "Failed to connect to {}. Retrying in {}s...", endpoint_name, backoff.as_secs());
                            sleep(backoff).await;
                        }
                    }
                }
            }
        }
    }

    /// The main loop for receiving, processing, and acknowledging messages.
    async fn process_messages(
        &mut self,
        consumer: Arc<dyn MessageConsumer>,
        publisher: Arc<dyn MessagePublisher>,
        dlq_publisher: Option<Arc<dyn MessagePublisher>>,
        dedup_store: Arc<DeduplicationStore>,
    ) -> Result<()> {
        let concurrency = self.route.concurrency.unwrap_or(100);

        loop {
            tokio::select! {
                _ = self.shutdown_rx.changed() => {
                    info!("Shutdown signal received. Stopping message processing.");
                    return Ok(());
                }
                // Batch receive messages for higher throughput.
                messages_result = consumer.receive_batch(10) => {
                    let messages = match messages_result {
                        Ok(msgs) => msgs,
                        Err(e) => {
                            // Check for EOF or other terminal conditions.
                            if e.to_string().contains("End of file") {
                                info!("Consumer reached end of stream.");
                                return Ok(());
                            }
                            // For other errors, we break and trigger a reconnect.
                            return Err(anyhow!("Consumer error: {}", e));
                        }
                    };

                    if messages.is_empty() {
                        // Some consumers might return an empty batch without an error.
                        // We can pause briefly to prevent busy-looping.
                        sleep(Duration::from_millis(100)).await;
                        continue;
                    }

                    let route_name = self.name.clone();
                    let dedup = dedup_store.clone();
                    let pub_clone = publisher.clone();
                    let dlq_clone = dlq_publisher.clone();

                    // Process the batch concurrently.
                    stream::iter(messages)
                        .for_each_concurrent(concurrency, move |(message, commit_fn)| {
                            let route_name = route_name.clone();
                            let dedup = dedup.clone();
                            let publisher = pub_clone.clone();
                            let dlq_publisher = dlq_clone.clone();

                            async move {
                                let msg_id = message.message_id;
                                let start_time = std::time::Instant::now();

                                if dedup.is_duplicate(&msg_id).unwrap_or(false) {
                                    trace!(%msg_id, "Duplicate message, skipping.");
                                    counter!("bridge_messages_duplicate_total", "route" => route_name).increment(1);
                                    commit_fn(None).await; // Acknowledge the duplicate
                                    return;
                                }

                                counter!("bridge_messages_received_total", "route" => route_name.clone()).increment(1);

                                match publisher.send(message.clone()).await {
                                    Ok(response) => {
                                        histogram!("bridge_message_processing_duration_seconds", "route" => route_name.clone()).record(start_time.elapsed().as_secs_f64());
                                        counter!("bridge_messages_sent_total", "route" => route_name.clone()).increment(1);
                                        debug!(%msg_id, "Message sent successfully.");
                                        commit_fn(response).await;
                                    }
                                    Err(e) => {
                                        error!(%msg_id, error = %e, "Failed to send message, attempting DLQ.");
                                        counter!("bridge_messages_dlq_total", "route" => route_name.clone()).increment(1);
                                        if let Some(dlq) = dlq_publisher {
                                            if let Err(dlq_err) = dlq.send(message).await {
                                                error!(%msg_id, error = %dlq_err, "Failed to send message to DLQ.");
                                            }
                                        }
                                        commit_fn(None).await; // Acknowledge after DLQ attempt
                                    }
                                }
                            }
                        })
                        .await;
                }
            }
        }
    }

    async fn setup_deduplication(&self) -> Arc<DeduplicationStore> {
        let db_path = Path::new(&self.config.sled_path).join(&self.name);
        let dedup_store = Arc::new(
            DeduplicationStore::new(db_path, self.config.dedup_ttl_seconds)
                .expect("Failed to create instance-specific deduplication store"),
        );

        // Spawn a task to periodically flush the deduplication database
        let dedup_flusher = dedup_store.clone();
        let mut shutdown_rx = self.shutdown_rx.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(e) = dedup_flusher.flush_async().await {
                            error!(error = %e, "Failed to flush deduplication store");
                        }
                    }
                    _ = shutdown_rx.changed() => {
                        info!("Shutdown signal received, performing final deduplication flush.");
                        if let Err(e) = dedup_flusher.flush_async().await {
                             error!(error = %e, "Failed to perform final flush of deduplication store");
                        }
                        break;
                    }
                }
            }
        });

        dedup_store
    }
}
