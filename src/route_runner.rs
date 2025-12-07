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
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Barrier, Mutex, mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::sleep;
use tracing::{error, info, instrument, trace};

pub(crate) enum RouteRunnerCommand {
    Flush,
}

/// Manages the lifecycle of a single route.
pub(crate) struct RouteRunner {
    name: String,
    route: Route,
    barrier: Arc<Barrier>,
    shutdown_rx: watch::Receiver<()>,
    command_rx: mpsc::Receiver<RouteRunnerCommand>,
}

impl RouteRunner {
    pub(crate) fn new(
        name: String,
        route: Route,
        barrier: Arc<Barrier>,
        shutdown_rx: watch::Receiver<()>,
        command_rx: mpsc::Receiver<RouteRunnerCommand>,
    ) -> Self {
        Self {
            name,
            route,
            barrier,
            shutdown_rx,
            command_rx,
        }
    }

    #[instrument(name = "route", skip_all, fields(route.name = %self.name))]
    pub(crate) async fn run(self) {
        let RouteRunner {
            name,
            route,
            barrier,
            mut shutdown_rx,
            mut command_rx,
        } = self;

        info!("Initializing route {}", name);
        let dedup_store = Self::setup_deduplication(&name, &mut shutdown_rx).await;
        // Take ownership of the command receiver. It cannot be cloned.
        loop {
            // Check for shutdown before attempting to connect.
            if shutdown_rx.has_changed().unwrap_or(false) {
                info!("Shutdown signal received during initialization. Route stopping.");
                return;
            }

            // 1. Connect to the publisher (and DLQ) first. Retry on failure.
            let publisher = match Self::connect_with_retry(
                "publisher",
                || create_publisher_from_route(&name, &route.out),
                &mut shutdown_rx,
            )
            .await
            {
                Some(p) => p,
                None => return, // Shutdown was triggered
            };

            // The DLQ is optional, so we handle it separately.
            let dlq_publisher = if route.dlq.is_some() {
                match Self::connect_with_retry(
                    "dlq",
                    || create_dlq_from_route(&route, name.as_str()),
                    &mut shutdown_rx,
                )
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
            let consumer = match Self::connect_with_retry(
                "consumer",
                || create_consumer_from_route(&name, &route.r#in),
                &mut shutdown_rx,
            )
            .await
            {
                Some(c) => c,
                None => return, // Shutdown was triggered
            };

            // Wait for all other routes to also connect their consumers.
            // This prevents fast producers from starting before slow consumers are ready.
            info!("Consumer connected. Waiting for other routes to be ready...");
            barrier.wait().await;

            info!("Route connected. Starting message processing.");
            let processing_result = Self::process_messages(
                &name,
                &route,
                &mut shutdown_rx,
                consumer,
                publisher,
                dlq_publisher,
                dedup_store.clone(),
                command_rx,
            )
            .await;

            match processing_result {
                Ok(_) => {
                    info!("Consumer stream ended (e.g., EOF). Route finished.");
                    break; // Exit the loop, route is done.
                }
                Err((e, returned_command_rx)) => {
                    error!(error = %e, "An unrecoverable error occurred during message processing. Re-establishing connections in 5s.");
                    command_rx = returned_command_rx; // Recover ownership for the next loop iteration
                    tokio::select! { _ = sleep(Duration::from_secs(5)) => {}, _ = shutdown_rx.changed() => return }
                }
            }
        }
    }

    /// Connects to an endpoint, retrying with exponential backoff until successful or shutdown.
    async fn connect_with_retry<T, F, Fut>(
        endpoint_name: &str,
        connect_fn: F,
        shutdown_rx: &mut watch::Receiver<()>,
    ) -> Option<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut attempts = 0;
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
    #[allow(clippy::too_many_arguments)]
    async fn process_messages(
        name: &str,
        route: &Route,
        shutdown_rx: &mut watch::Receiver<()>,
        consumer: Box<dyn MessageConsumer>,
        publisher: Arc<dyn MessagePublisher>,
        dlq_publisher: Option<Arc<dyn MessagePublisher>>,
        dedup_store: Arc<DeduplicationStore>,
        mut command_rx: mpsc::Receiver<RouteRunnerCommand>,
    ) -> Result<(), (anyhow::Error, mpsc::Receiver<RouteRunnerCommand>)> {
        let concurrency = route.concurrency.unwrap_or(10);

        // For I/O-bound consumers, wrap them in a BufferedConsumer to pre-fetch
        // messages and improve performance. For in-memory consumers, which are
        // already fast, adding another buffer is unnecessary overhead.
        let consumer = if consumer.as_any().is::<crate::endpoints::memory::MemoryConsumer>() {
            info!("Using direct MemoryConsumer for maximum performance.");
            consumer
        } else {
            if consumer.as_any().is::<crate::consumers::BufferedConsumer>() {
                consumer
            } else {
                info!("Wrapping I/O-bound consumer in a BufferedConsumer for improved performance.");
                // Use concurrency * 2 as a heuristic for a good buffer size.
                Box::new(crate::consumers::BufferedConsumer::new(
                    consumer,
                    concurrency * 2,
                ))
            }
        };

        let consumer = Arc::new(Mutex::new(consumer));
        let mut tasks = JoinSet::new();

        // --- Message Worker Tasks ---
        // Spawn a pool of workers to process messages concurrently.
        let deduplication_enabled = route.deduplication_enabled;
        for _ in 0..concurrency {
            let route_name = name.to_string();
            let dedup = dedup_store.clone();
            let publisher = publisher.clone();
            let dlq_publisher = dlq_publisher.clone();
            let consumer = consumer.clone();
            let mut worker_shutdown_rx = shutdown_rx.clone();

            tasks.spawn(async move {
                // Each worker task runs a loop, processing messages until the channel is closed.
                loop {
                    let mut guard = tokio::select! {
                        biased;
                        _ = worker_shutdown_rx.changed() => {
                            info!("Shutdown signal received in worker. Exiting.");
                            break;
                        }
                        guard = consumer.lock() => guard,
                    };

                    let result = guard.receive().await;

                    let (message, commit_fn) = match result {
                        Ok(data) => data,
                        Err(e) => {
                            // The consumer has failed or the stream has ended (e.g., EOF).
                            // This worker's job is done.
                            if !e.to_string().contains("BufferedConsumer channel has been closed") {
                                trace!(error = %e, "Consumer failed, worker exiting.");
                            } else {
                                trace!("Consumer stream ended, worker exiting.");
                            }
                            break;
                        }
                    };
                    trace!(msg_id = %message.message_id, "Worker picked up message from channel.");

                    let msg_id = message.message_id;
                    let start_time = std::time::Instant::now();

                    if deduplication_enabled && dedup.is_duplicate(&msg_id).unwrap_or(false) {
                        trace!(%msg_id, "Duplicate message, skipping.");
                        counter!("bridge_messages_duplicate_total", "route" => route_name.clone()).increment(1);
                        commit_fn(None).await; // Acknowledge the duplicate
                        continue;
                    }

                    counter!("bridge_messages_received_total", "route" => route_name.clone()).increment(1);

                    trace!(%msg_id, "Sending message to publisher.");
                    match publisher.send(message.clone()).await {
                        Ok(response) => {
                            histogram!("bridge_message_processing_duration_seconds", "route" => route_name.clone()).record(start_time.elapsed().as_secs_f64());
                            counter!("bridge_messages_sent_total", "route" => route_name.clone()).increment(1);
                            trace!(%msg_id, "Message sent successfully.");
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
                Ok(())
            });
        }

        // --- Main Processing and Command Loop ---
        let mut flush_interval = tokio::time::interval(Duration::from_millis(100));
        'main_loop: loop {
            tokio::select! {
                // Handle commands for this route
                Some(command) = command_rx.recv() => {
                    match command {
                        RouteRunnerCommand::Flush => {
                            trace!("Manual flush command received.");
                            Self::flush_publishers(publisher.clone(), dlq_publisher.clone()).await;
                        }
                    }
                },
                // Handle periodic flushing
                _ = flush_interval.tick() => {
                    trace!("Periodic flush triggered.");
                    Self::flush_publishers(publisher.clone(), dlq_publisher.clone()).await;
                },
                // Check for task completion or failure
                Some(res) = tasks.join_next() => {
                    match res {
                        Ok(Ok(())) => {
                            trace!("A worker task finished gracefully.");
                            // If all workers have finished, we can exit the main loop.
                            if tasks.is_empty() {
                                info!("All worker tasks have completed.");
                                break 'main_loop;
                            }
                        },
                        Ok(Err(e)) => {
                            error!(error = %e, "A consumer task finished with an error.");
                            tasks.abort_all();
                            return Err((e, command_rx));
                        },
                        Err(join_err) if join_err.is_cancelled() => {
                            trace!("A task was cancelled.");
                        },
                        Err(join_err) => {
                            error!(error = %join_err, "A route task panicked.");
                            let panic_err = anyhow::anyhow!("A route task panicked: {}", join_err);
                            tasks.abort_all();
                            return Err((panic_err, command_rx));
                        }
                    }
                },
                else => {
                    info!("All route tasks have completed.");
                    break 'main_loop;
                }
            }
        }

        info!("Shutting down and waiting for all worker tasks to complete.");
        tasks.shutdown().await;

        // After all tasks are done, explicitly flush the publisher to ensure all buffered
        // messages are written before the route runner exits. This is crucial for file-based
        // publishers where the process might exit before the OS flushes the buffer.
        info!("Flushing final messages for route.");
        if let Err(e) = publisher.flush().await {
            return Err((e, command_rx));
        }
        if let Some(dlq) = dlq_publisher {
            if let Err(e) = dlq.flush().await {
                return Err((e, command_rx));
            }
        }
        Ok(())
    }

    async fn flush_publishers(
        publisher: Arc<dyn MessagePublisher>,
        dlq_publisher: Option<Arc<dyn MessagePublisher>>,
    ) {
        if let Err(e) = publisher.flush().await {
            error!(error = %e, "Failed to flush publisher");
        }
        if let Some(dlq) = dlq_publisher {
            if let Err(e) = dlq.flush().await {
                error!(error = %e, "Failed to flush DLQ publisher");
            }
        }
    }

    async fn setup_deduplication(
        name: &str,
        shutdown_rx: &mut watch::Receiver<()>,
    ) -> Arc<DeduplicationStore> {
        // The sled_path should come from a global config, but for now, we'll hardcode a temp path.
        let db_path = Path::new("/tmp/mq_multi_bridge/db").join(name);
        let dedup_store = Arc::new(
            DeduplicationStore::new(db_path, 86400) // Default TTL
                .expect("Failed to create instance-specific deduplication store"),
        );

        // Spawn a task to periodically flush the deduplication database
        let dedup_flusher = dedup_store.clone();
        let mut shutdown_rx = shutdown_rx.clone();
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
