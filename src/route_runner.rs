//  mq_multi_bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
use crate::config::Route;
use crate::consumers::MessageConsumer;
use crate::deduplication::DeduplicationStore;
use crate::endpoints::{
    create_consumer_from_route, create_dlq_from_route, create_publisher_from_route,
};
use crate::publishers::MessagePublisher;
use anyhow::Result;
use metrics::{counter, histogram};
use std::path::Path;
use std::sync::{Arc};
use std::time::Duration;
use tokio::sync::{watch, Barrier};
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{debug, error, info, instrument, trace};

/// Manages the lifecycle of a single route.
pub(crate) struct RouteRunner {
    name: String,
    route: Route,
    barrier: Arc<Barrier>,
    shutdown_rx: watch::Receiver<()>,
}

impl RouteRunner {
    pub(crate) fn new(
        name: String,
        route: Route,
        barrier: Arc<Barrier>,
        shutdown_rx: watch::Receiver<()>,
    ) -> Self {
        Self {
            name,
            route,
            barrier,
            shutdown_rx,
        }
    }

    #[instrument(name = "route", skip_all, fields(route.name = %self.name))]
    pub(crate) async fn run(mut self) {
        info!("Initializing route {}", self.name);
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
            let dlq_publisher = if self.route.dlq.is_some() {
                match self
                    .connect_with_retry("dlq", || {
                        create_dlq_from_route(&self.route, self.name.as_str())
                    })
                    .await
                {
                    Some(Some(d)) => Some(d), // Successfully connected to DLQ
                    Some(None) => None, // Should not happen if dlq is configured, but handle it.
                    None => return,     // Shutdown was triggered during connection attempt.
                }
            } else {
                None // No DLQ configured
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

            // Wait for all other routes to also connect their consumers.
            // This prevents fast producers from starting before slow consumers are ready.
            info!("Consumer connected. Waiting for other routes to be ready...");
            self.barrier.wait().await;

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
        let concurrency = self.route.concurrency.unwrap_or(10);
        // Use an MPMC channel to allow multiple workers to receive concurrently without a mutex.
        let (tx, rx) = async_channel::unbounded();
        let mut tasks = JoinSet::new();

        // --- Producer Task ---
        // This task's only job is to receive messages and send them to the processing channel.
        let mut producer_shutdown_rx = self.shutdown_rx.clone();
        tasks.spawn(async move {
            loop {
                tokio::select! {
                    _ = producer_shutdown_rx.changed() => {
                        info!("Shutdown signal received in producer. Stopping message reception.");
                        break;
                    }
                    result = consumer.receive() => {
                        match result {
                            Ok(message_data) => {
                                let msg_id = message_data.0.message_id;
                                trace!(%msg_id, "Message received from source consumer, queuing for processing.");
                                if tx.send(message_data).await.is_err() {
                                    // If the send fails, the channel is closed, so we stop.
                                    info!("Processing channel closed. Stopping producer.");
                                    break;
                                }
                            }
                            Err(e) => {
                                if e.to_string().contains("End of file") {
                                    info!("Consumer reached end of stream. Shutting down producer.");
                                } else {
                                    error!(error = %e, "Unrecoverable consumer error. Shutting down producer.");
                                }
                                break; // Exit loop on any consumer error or EOF
                            }
                        }
                    }
                }
            }
        });

        // --- Consumer Tasks ---
        // Spawn a pool of workers to process messages concurrently.
        let consumer_shutdown_rx = self.shutdown_rx.clone();
        let deduplication_enabled = self.route.deduplication_enabled;
        for _ in 0..concurrency {
            let route_name = self.name.clone();
            let dedup = dedup_store.clone();
            let publisher = publisher.clone();
            let dlq_publisher = dlq_publisher.clone();
            let rx_clone = rx.clone();
            let mut consumer_shutdown_rx = consumer_shutdown_rx.clone();

            tasks.spawn(async move {
                // Each worker task runs a loop, processing messages until the channel is closed.
                loop {
                    tokio::select! {
                        biased;
                        _ = consumer_shutdown_rx.changed() => {
                            info!("Shutdown signal received in worker. Exiting.");
                            break;
                        }
                        result = rx_clone.recv() => {
                            let Ok((message, commit_fn)) = result else {
                                // Channel is empty and disconnected, so we can exit.
                                break;
                            };
                            trace!(msg_id = %message.message_id, "Worker picked up message from channel.");

                            let msg_id = message.message_id;
                            let start_time = std::time::Instant::now();

                            if deduplication_enabled {
                                if dedup.is_duplicate(&msg_id).unwrap_or(false) {
                                    trace!(%msg_id, "Duplicate message, skipping.");
                                    counter!("bridge_messages_duplicate_total", "route" => route_name.clone()).increment(1);
                                    commit_fn(None).await; // Acknowledge the duplicate
                                    continue;
                                }
                            }

                            counter!("bridge_messages_received_total", "route" => route_name.clone()).increment(1);

                            trace!(%msg_id, "Sending message to publisher.");
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
                                    if let Some(dlq) = dlq_publisher.clone() {
                                        if let Err(dlq_err) = dlq.send(message).await {
                                            error!(%msg_id, error = %dlq_err, "Failed to send message to DLQ.");
                                        }
                                    }
                                    commit_fn(None).await; // Acknowledge after DLQ attempt
                                }
                            }
                        }
                    }
                }
            });
        }

        // Wait for all tasks to complete.
        // This loop will exit when all tasks in the JoinSet have completed.
        // This happens naturally when the producer task finishes (e.g., on EOF)
        // and the worker tasks drain the channel.
        while let Some(res) = tasks.join_next().await {
            if let Err(e) = res {
                error!("A route processing task failed: {}", e);
            }
        }

        // After all tasks are done, explicitly flush the publisher to ensure all buffered
        // messages are written before the route runner exits. This is crucial for file-based
        // publishers where the process might exit before the OS flushes the buffer.
        info!("Flushing final messages for route.");
        publisher.flush().await?;
        Ok(())
    }

    async fn setup_deduplication(&self) -> Arc<DeduplicationStore> {
        // The sled_path should come from a global config, but for now, we'll hardcode a temp path.
        let db_path = Path::new("/tmp/mq_multi_bridge/db").join(&self.name);
        let dedup_store = Arc::new(
            DeduplicationStore::new(db_path, 86400) // Default TTL
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
