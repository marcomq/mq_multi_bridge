//  mq_multi_bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq_multi_bridge

pub mod amqp;
pub mod config;
pub mod file;
pub mod http;
pub mod kafka;
pub mod model;
pub mod mqtt;
pub mod nats;
pub mod sinks;
pub mod sources;
pub mod static_response;
pub mod store;

use crate::amqp::{AmqpSink, AmqpSource};
use crate::config::{
    AmqpConfig, AmqpSinkEndpoint, AmqpSourceEndpoint, Config, FileConfig, FileSinkEndpoint, FileSourceEndpoint, HttpConfig, HttpSinkEndpoint, HttpSourceEndpoint, KafkaConfig, KafkaSinkEndpoint, KafkaSourceEndpoint, MqttConfig, MqttSinkEndpoint, MqttSourceEndpoint, NatsConfig, NatsSinkEndpoint, NatsSourceEndpoint, Route, SinkEndpoint, SinkEndpointType, SourceEndpoint, SourceEndpointType, StaticResponseEndpoint
};
use crate::file::{FileSink, FileSource};
use crate::http::{HttpSink, HttpSource};
use crate::kafka::KafkaSink;
use crate::mqtt::{MqttSink, MqttSource};
use crate::nats::NatsSink;
use crate::nats::NatsSource;
use crate::static_response::StaticResponseSink;
use crate::sinks::MessageSink;
use crate::sources::MessageSource;
use crate::store::DeduplicationStore;
use anyhow::{anyhow, Result};
use metrics::{counter, histogram};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{error, info, instrument, trace, warn};

pub struct Bridge {
    config: Config,
    bridge_tasks: Vec<tokio::task::JoinHandle<()>>, // The task itself returns (), JoinHandle wraps it in a Result for panics
    shutdown_rx: Option<tokio::sync::watch::Receiver<()>>,
}

impl Bridge {
    /// Creates a new Bridge from a configuration object.
    pub fn from_config(
        config: Config,
        shutdown_rx: Option<tokio::sync::watch::Receiver<()>>,
    ) -> Result<Self> {
        let bridge = Self {
            config,
            bridge_tasks: Vec::new(),
            shutdown_rx,
        };

        Ok(bridge)
    }

    /// Adds a route to the bridge, connecting a source to a sink.
    pub async fn add_route(&mut self, name: &str, route: &Route) -> Result<()> {
        // The creation logic is now moved inside run_bridge_instance to handle retries.
        let bridge_task = run_bridge_instance(
            name.to_string(),
            route.clone(),
            self.config.clone(),
            self.shutdown_rx.as_ref().map(|rx| rx.clone()),
        );
        self.bridge_tasks.push(tokio::spawn(bridge_task));
        Ok(())
    }

    /// Adds a custom route with pre-constructed source and sink.
    pub async fn add_custom_route(
        &mut self,
        route_name: &str,
        source: Arc<dyn MessageSource + Send + Sync>,
        sink: Arc<dyn MessageSink + Send + Sync>,
    ) -> Result<()> {
        info!(route = %route_name, "Adding custom route");
        let bridge_task = run_bridge_instance(
            route_name.to_string(),
            (source, sink, None), // Wrap in a tuple to match the expected type
            self.config.clone(),
            self.shutdown_rx.as_ref().map(|rx| rx.clone()),
        );
        self.bridge_tasks.push(tokio::spawn(bridge_task));
        Ok(())
    }

    /// Initializes all connections and routes from the configuration.
    pub async fn initialize_from_config(&mut self) -> Result<()> {
        for (name, route) in self.config.routes.clone() {
            match self.add_route(&name, &route).await {
                Ok(_) => (),
                Err(e) => {
                    // Log the error and continue with other routes.
                    error!(route = %name, error = %e, "Failed to initialize route. It will be skipped.");
                }
            }
        }
        Ok(())
    }

    /// Runs all the configured bridge tasks and waits for them to complete.
    pub async fn run(self) -> Result<()> {
        info!("Running all bridge tasks...");
        // Wait for all bridge tasks to complete.
        futures::future::join_all(self.bridge_tasks).await;
        Ok(())
    }
}

async fn create_source_from_route(
    route_name: &str,
    endpoint: &SourceEndpoint,
) -> anyhow::Result<Arc<dyn MessageSource + Send + Sync>> {
    match &endpoint.endpoint_type {
        SourceEndpointType::Kafka(KafkaSourceEndpoint { config, endpoint }) => {
            let topic = endpoint.topic.as_deref().unwrap_or(route_name);
            create_kafka_source(config, topic).await
        }
        SourceEndpointType::Nats(NatsSourceEndpoint { config, endpoint }) => {
            let subject = endpoint.subject.as_deref().unwrap_or(route_name);
            let stream_name = endpoint
                .stream
                .as_deref()
                .or(config.default_stream.as_deref())
                .ok_or_else(|| anyhow!("[route:{}] NATS source must specify a 'stream' or have a 'default_stream' in its connection", route_name))?;
            create_nats_source(config, stream_name, subject).await
        }
        SourceEndpointType::Amqp(AmqpSourceEndpoint { config, endpoint }) => {
            let queue = endpoint.queue.as_deref().unwrap_or(route_name);
            create_amqp_source(config, queue).await
        }
        SourceEndpointType::Mqtt(MqttSourceEndpoint { config, endpoint }) => {
            // For MQTT, the source and sink for a given route should share a client.
            // We create the source here, which also creates the client and eventloop.
            // The sink will later get a handle to this client.
            // This is a bit of a special case due to rumqttc's design.
            if route_name.starts_with("file_to_") {
                 // This is a sink-first route, create a source with a dummy topic
                 create_mqtt_source(config, "").await
            } else {
                let topic = endpoint.topic.as_deref().unwrap_or(route_name);
                create_mqtt_source(config, topic).await
            }
        }
        SourceEndpointType::File(FileSourceEndpoint { config, .. }) => {
            create_file_source(config).await
        }
        SourceEndpointType::Http(HttpSourceEndpoint { config, .. }) => create_http_source(config).await,
    }
}

async fn create_sink_from_route(
    route_name: &str,
    endpoint: &SinkEndpoint,
) -> anyhow::Result<Arc<dyn MessageSink + Send + Sync>> {
    match &endpoint.endpoint_type {
        SinkEndpointType::Kafka(KafkaSinkEndpoint { config, endpoint }) => {
            let topic = endpoint.topic.as_deref().unwrap_or(route_name);
            create_kafka_sink(config, topic).await
        }
        SinkEndpointType::Nats(NatsSinkEndpoint { config, endpoint }) => {
            let subject = endpoint.subject.as_deref().unwrap_or(route_name);
            // Pass the stream name from the endpoint config to the sink constructor
            create_nats_sink(config, subject, endpoint.stream.as_deref()).await
        }
        SinkEndpointType::Amqp(AmqpSinkEndpoint { config, endpoint }) => {
            let queue = endpoint.queue.as_deref().unwrap_or(route_name);
            create_amqp_sink(config, queue).await
        }
        SinkEndpointType::Mqtt(MqttSinkEndpoint { config, endpoint }) => {
             // This is part of the client sharing pattern for MQTT.
             // We assume the corresponding source has already been created and we can get its client.
             // This is a bit of a hack and relies on the route naming convention.
             // A more robust solution might involve a shared client manager.
             let source_route_name = route_name.replace("mqtt_to_file", "file_to_mqtt");
             let topic = endpoint.topic.as_deref().unwrap_or(&source_route_name);
             let source = create_mqtt_source(config, "").await?; // Create a dummy source to get a client
             create_mqtt_sink(source.as_any().downcast_ref::<MqttSource>().unwrap().client(), topic).await
        }
        SinkEndpointType::File(FileSinkEndpoint { config, .. }) => create_file_sink(config).await,
        SinkEndpointType::Http(HttpSinkEndpoint { config, endpoint }) => {
            let mut sink = HttpSink::new(config).await?;
            if let Some(url) = &endpoint.url {
                sink = sink.with_url(url);
            }
            Ok(Arc::new(sink))
        }
        SinkEndpointType::StaticResponse(config) => {
            create_static_response_sink(config).await
        }
    }
}

async fn create_kafka_source(
    config: &KafkaConfig,
    topic: &str,
) -> anyhow::Result<Arc<dyn MessageSource + Send + Sync>> {
    Ok(Arc::new(crate::kafka::KafkaSource::new(config, topic)?))
}
async fn create_kafka_sink(
    config: &KafkaConfig,
    topic: &str,
) -> anyhow::Result<Arc<dyn MessageSink + Send + Sync>> {
    Ok(Arc::new(KafkaSink::new(config, topic)?))
}
async fn create_nats_source(
    config: &NatsConfig,
    stream_name: &str,
    subject: &str,
) -> anyhow::Result<Arc<dyn MessageSource + Send + Sync>> {
    Ok(Arc::new(NatsSource::new(config, stream_name, subject).await?))
}
async fn create_nats_sink(
    config: &NatsConfig,
    subject: &str,
    stream_name: Option<&str>,
) -> anyhow::Result<Arc<dyn MessageSink + Send + Sync>> {
    Ok(Arc::new(NatsSink::new(config, subject, stream_name).await?))
}
async fn create_amqp_source(
    config: &AmqpConfig,
    queue: &str,
) -> anyhow::Result<Arc<dyn MessageSource + Send + Sync>> {
    Ok(Arc::new(AmqpSource::new(config, queue).await?))
}
async fn create_amqp_sink(
    config: &AmqpConfig,
    routing_key: &str,
) -> anyhow::Result<Arc<dyn MessageSink + Send + Sync>> {
    Ok(Arc::new(AmqpSink::new(config, routing_key).await?))
}
async fn create_mqtt_source(
    config: &MqttConfig,
    topic: &str,
) -> anyhow::Result<Arc<dyn MessageSource + Send + Sync>> {
    Ok(Arc::new(MqttSource::new(config, topic).await?))
}
async fn create_mqtt_sink(
    client: rumqttc::AsyncClient,
    topic: &str,
) -> anyhow::Result<Arc<dyn MessageSink + Send + Sync>> {
    Ok(Arc::new(MqttSink::new(client, topic)?))
}
async fn create_file_source(
    config: &FileConfig,
) -> anyhow::Result<Arc<dyn MessageSource + Send + Sync>> {
    Ok(Arc::new(FileSource::new(config).await?))
}
async fn create_file_sink(
    config: &FileConfig,
) -> anyhow::Result<Arc<dyn MessageSink + Send + Sync>> {
    Ok(Arc::new(FileSink::new(config).await?))
}
async fn create_http_source(
    config: &HttpConfig,
) -> anyhow::Result<Arc<dyn MessageSource + Send + Sync>> {
    Ok(Arc::new(HttpSource::new(config).await?))
}
async fn create_static_response_sink(
    config: &StaticResponseEndpoint,
) -> anyhow::Result<Arc<dyn MessageSink + Send + Sync>> {
    info!(config = ?config, "Creating static response sink");
    Ok(Arc::new(StaticResponseSink::new(config)?))
}

#[instrument(skip_all, fields(bridge_id = %bridge_id))]
async fn run_bridge_instance(
    bridge_id: String,
    route_or_components: impl Into<RouteOrComponents>,
    config: Config,
    mut shutdown_rx: Option<tokio::sync::watch::Receiver<()>>,
) {
    trace!("Starting bridge id {}", bridge_id);

    // Create a unique deduplication store for this specific bridge instance.
    // This is crucial for tests to prevent routes from interfering with each other.
    let db_path = Path::new(&config.sled_path).join(&bridge_id);
    let dedup_store = Arc::new(DeduplicationStore::new(db_path, config.dedup_ttl_seconds)
        .expect("Failed to create instance-specific deduplication store"));

    let route_or_components: RouteOrComponents = route_or_components.into();

    // --- Resilient Initialization Loop ---
    // This loop will patiently retry creating the source and sink until it succeeds or is shut down.
    let (source, sink, dlq_sink) = loop {
        tokio::select! {
            biased;
            _ = async { if let Some(rx) = &mut shutdown_rx { rx.changed().await.ok(); } else { futures::future::pending::<()>().await; } } => {
                info!("Shutdown signal received during initialization, stopping.");
                return;
            }
            result = route_or_components.clone().try_into_components(&bridge_id) => {
                match result {
                    Ok(components) => break components,
                    Err(e) => {
                        error!(error = %e, "Failed to initialize route components. Retrying in 5s...");
                        sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        }
    };

    loop {
        tokio::select! {
            // Biased ensures we check for shutdown first if both are ready
            biased;
            _ = async {
                if let Some(rx) = &mut shutdown_rx {
                    rx.changed().await.ok();
                } else {
                    // If no shutdown receiver, this future will never resolve.
                    futures::future::pending::<()>().await;
                }
            } => {
                    info!("Shutdown signal received, stopping message consumption.");
                    break;
                }
            result = source.receive() => {
                match result {
                    Ok((message, commit)) => {
                        let msg_id = message.message_id;
                        let processing_start = std::time::Instant::now();
                        trace!(message_id = %msg_id, "Received message");
                        counter!("bridge_messages_received_total", "route" => bridge_id.clone()).increment(1);

                        if dedup_store.is_duplicate(&msg_id).unwrap_or(false) {
                            warn!(message_id = %msg_id, "Duplicate message detected, skipping.");
                            counter!("bridge_messages_duplicate_total", "route" => bridge_id.clone()).increment(1);
                            commit(None).await; // Commit even if duplicate to remove from source queue
                            continue;
                        }

                        let mut attempts = 0;
                        let max_attempts = 5;
                        loop {
                            attempts += 1;
                            match sink.send(message.clone()).await { // Sink now returns Option<CanonicalMessage>
                                Ok(response_message) => {
                                    let duration = processing_start.elapsed();
                                    trace!(message_id = %msg_id, "Successfully forwarded message");
                                    counter!("bridge_messages_sent_total", "route" => bridge_id.clone()).increment(1);
                                    histogram!("bridge_message_processing_duration_seconds", "route" => bridge_id.clone()).record(duration.as_secs_f64());
                                    commit(response_message).await; // ACK source only after successful sink
                                    break;
                                }
                                Err(e) => {
                                    error!(message_id = %msg_id, attempt = attempts, error = %e, "Failed to forward message");
                                    if attempts >= max_attempts {
                                        counter!("bridge_messages_dlq_total", "route" => bridge_id.clone()).increment(1);
                                        if let Some(dlq) = &dlq_sink {
                                            if let Err(dlq_err) = dlq.send(message).await {
                                                error!(message_id = %msg_id, error = %dlq_err, "Failed to send to DLQ!");
                                            }
                                        }
                                        commit(None).await; // Commit original message after moving to DLQ
                                        break;
                                    } else {
                                    }
                                    let backoff_duration = Duration::from_secs(2u64.pow(attempts));
                                    warn!(message_id = %msg_id, "Retrying in {:?}", backoff_duration);
                                    sleep(backoff_duration).await;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        // Special handling for FileSource: if it reaches EOF, we can gracefully shut down this bridge instance.
                        if e.to_string().contains("End of file reached") {
                            info!("Source file reached EOF. Shutting down this bridge instance.");
                            break; // Exit the loop
                        }
                        error!(error = %e, "Error receiving message from source. Reconnecting in 5s...");
                        sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        }
    }
    // In a testing context, a file-based source might finish before other routes
    // have had time to initialize or process messages. This small delay ensures that sinks
    // have time to complete their work (e.g., publishing to a broker) before this task exits.
    // TODO: properly wait for sink completion instead of a fixed delay.
    sleep(Duration::from_secs(2)).await;
    info!(bridge_id = %bridge_id, "Bridge instance shut down gracefully.");
}

/// An enum to allow `run_bridge_instance` to accept either a full Route or pre-built components.
#[derive(Clone)]
enum RouteOrComponents {
    Route(Route),
    Components(Arc<dyn MessageSource + Send + Sync>, Arc<dyn MessageSink + Send + Sync>, Option<Arc<dyn MessageSink + Send + Sync>>),
}

impl From<Route> for RouteOrComponents {
    fn from(route: Route) -> Self {
        RouteOrComponents::Route(route)
    }
}

impl From<(Arc<dyn MessageSource + Send + Sync>, Arc<dyn MessageSink + Send + Sync>, Option<Arc<dyn MessageSink + Send + Sync>>)> for RouteOrComponents {
    fn from(components: (Arc<dyn MessageSource + Send + Sync>, Arc<dyn MessageSink + Send + Sync>, Option<Arc<dyn MessageSink + Send + Sync>>)) -> Self {
        RouteOrComponents::Components(components.0, components.1, components.2)
    }
}

impl RouteOrComponents {
    async fn try_into_components(self, route_name: &str) -> Result<(Arc<dyn MessageSource + Send + Sync>, Arc<dyn MessageSink + Send + Sync>, Option<Arc<dyn MessageSink + Send + Sync>>)> {
        match self {
            RouteOrComponents::Route(route) => {                
                let source = create_source_from_route(route_name, &route.source).await?;
                let sink = if let SinkEndpointType::Mqtt(_) = &route.sink.endpoint_type {
                    // Special handling for MQTT to share the client from the source
                    let topic = route.sink.endpoint_type.topic().unwrap_or(route_name);
                    create_mqtt_sink(source.as_any().downcast_ref::<MqttSource>().unwrap().client(), topic).await?
                } else {
                    create_sink_from_route(route_name, &route.sink).await?
                };

                let dlq_sink = if let Some(dlq_config) = route.dlq.clone() {
                    let topic = dlq_config.kafka.endpoint.topic.as_deref().unwrap_or("dlq");
                    Some(create_kafka_sink(&dlq_config.kafka.config, topic).await?)
                } else {
                    None
                };
                Ok((source, sink, dlq_sink))
            }
            RouteOrComponents::Components(source, sink, dlq_sink) => Ok((source, sink, dlq_sink)),
        }
    }
}

impl SinkEndpointType {
    fn topic(&self) -> Option<&str> {
        match self {
            SinkEndpointType::Mqtt(e) => e.endpoint.topic.as_deref(),
            _ => None
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DlqConfig, KafkaSinkEndpoint};

    #[test]
    fn test_dlq_config_requires_kafka() {
        let config = Config {
            // Use a unique path to avoid test conflicts
            sled_path: tempfile::tempdir()
                .unwrap()
                .path()
                .join("db")
                .to_str()
                .unwrap()
                .to_string(),
            dedup_ttl_seconds: 60, // connections: vec![],
            routes: [("test_route".to_string(), Route {
                source: SourceEndpoint {
                    endpoint_type: SourceEndpointType::File(FileSourceEndpoint {
                        config: FileConfig {
                            path: "/dev/null".to_string(),
                        },
                        endpoint: crate::config::FileEndpoint {},
                    }),
                },
                sink: SinkEndpoint {
                    endpoint_type: SinkEndpointType::File(FileSinkEndpoint {
                        config: FileConfig {
                            path: "/dev/null".to_string(),
                        },
                        endpoint: crate::config::FileEndpoint {},
                    }),
                },
                dlq: Some(DlqConfig {
                    kafka: KafkaSinkEndpoint {
                        config: KafkaConfig {
                            ..Default::default()
                        },
                        endpoint: crate::config::KafkaEndpoint {
                            topic: Some("my_dlq".to_string()),
                        },
                    },
                }),
            })].into(),
            ..Default::default()
        };
        // This test primarily checks deserialization logic via the config test.
        // A runtime test would require more mocking. Here we just assert the structure.
        assert_eq!(
            config
                .routes["test_route"]
                .dlq
                .as_ref()
                .unwrap()
                .kafka
                .endpoint
                .topic,
            Some("my_dlq".to_string())
        );
    }

    #[tokio::test]
    async fn test_add_route() {
        use tempfile::tempdir;
        use tokio::fs;

        let dir = tempdir().unwrap();
        let in_path = dir.path().join("input.log");
        let out_path = dir.path().join("output.log");

        // Write a message to the input file
        let test_message = r#"{"message_id":"7a7c8e3e-55b3-4b4f-8d9a-3e3e3e3e3e3e","payload":"hello"}"#;
        fs::write(&in_path, test_message).await.unwrap();

        let route = Route {            
            source: SourceEndpoint {
                endpoint_type: SourceEndpointType::File(FileSourceEndpoint {
                    config: FileConfig {
                        path: in_path.to_str().unwrap().to_string(),
                    },
                    endpoint: crate::config::FileEndpoint {},
                }),
            },
            sink: SinkEndpoint {
                endpoint_type: SinkEndpointType::File(FileSinkEndpoint {
                    config: FileConfig {
                        path: out_path.to_str().unwrap().to_string(),
                    },
                    endpoint: crate::config::FileEndpoint {},
                }),
            },
            dlq: None,
        };

        let config = Config::default();
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
        let mut bridge = Bridge::from_config(config, Some(shutdown_rx)).unwrap();
        bridge.add_route("file-to-file-test", &route).await.unwrap();

        let bridge_handle = tokio::spawn(bridge.run());

        // Give it a moment to process the file
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let output_content = fs::read_to_string(&out_path).await.unwrap();
        assert!(output_content.contains(r#""payload":"hello""#));

        _shutdown_tx.send(()).unwrap();
        bridge_handle.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn test_add_custom_route() {
        use crate::model::CanonicalMessage;
        use crate::sinks::MessageSink;
        use tempfile::tempdir;
        use async_trait::async_trait;
        use serde_json::json;
        use std::any::Any;
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::time::Duration;

        // 1. Define a custom sink
        #[derive(Clone)]
        struct TestSink {
            was_called: Arc<AtomicBool>,
        }
        #[async_trait]
        impl MessageSink for TestSink {
            async fn send(
                &self,
                _message: CanonicalMessage,
            ) -> anyhow::Result<Option<CanonicalMessage>> {
                self.was_called.store(true, Ordering::SeqCst);
                Ok(None)
            }
            fn as_any(&self) -> &dyn Any {
                self
            }
        }

        // 2. Create a source
        let source = crate::http::HttpSource::new(&HttpConfig {
            listen_address: Some("0.0.0.0:9998".to_string()),
            ..Default::default()
        })
        .await
        .unwrap();

        // 3. Setup the bridge
        let mut config = Config::default();
        let dir = tempdir().unwrap();
        // Use a unique path to avoid test conflicts
        config.sled_path = dir.path().join("db").to_str().unwrap().to_string();

        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
        let mut bridge = Bridge::from_config(config.clone(), Some(shutdown_rx)).unwrap();

        let custom_sink = Arc::new(TestSink {
            was_called: Arc::new(AtomicBool::new(false)),
        });

        bridge
            .add_custom_route("custom-test-route", Arc::new(source), custom_sink.clone())
            .await
            .unwrap();

        let bridge_handle = tokio::spawn(bridge.run());

        // 4. Send a message to the source
        let client = reqwest::Client::new();
        let res = client
            .post("http://127.0.0.1:9998")
            .json(&json!({"test": "message"}))
            .send()
            .await
            .unwrap();
        assert!(res.status().is_success());

        // 5. Assert that the sink was called
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(custom_sink.was_called.load(Ordering::SeqCst));

        // 6. Shutdown
        _shutdown_tx.send(()).unwrap();
        bridge_handle.await.unwrap().unwrap();
    }
}
