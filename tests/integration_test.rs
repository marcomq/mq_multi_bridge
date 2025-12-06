// cargo test --test integration_test --features integration-test --release -- --ignored --nocapture --test-threads=1 --show-output

use async_channel::{bounded, Receiver, Sender};
use chrono;
use config::File as ConfigFile; // Use an alias for the File type from the config crate
use ctor::{ctor, dtor};
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use metrics_util::MetricKind;
use mq_multi_bridge::config::Config as AppConfig; // Use an alias for our app's config struct
use mq_multi_bridge::consumers::MessageConsumer;
use mq_multi_bridge::endpoints::{
    amqp::{AmqpConsumer, AmqpPublisher},
    file::{FileConsumer, FilePublisher},
    kafka::{KafkaConsumer, KafkaPublisher},
    mqtt::{MqttConsumer, MqttPublisher},
    nats::{NatsConsumer, NatsPublisher},
};
use mq_multi_bridge::model::CanonicalMessage;
use mq_multi_bridge::publishers::MessagePublisher;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::tempdir;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// A helper for capturing and querying metrics during tests.
struct TestMetrics {
    // The snapshotter is used to get the latest deltas from the global recorder.
    snapshotter: Snapshotter,
    // This HashMap will store the cumulative totals for each counter.
    cumulative_counters: Mutex<HashMap<(String, String), u64>>,
}

impl TestMetrics {
    fn new() -> Self {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        // We install our recorder globally for the test. The recorder is consumed,
        // but the snapshotter (which holds an Arc to the recorder's inner state)
        // will continue to work.
        metrics::set_global_recorder(recorder).expect("Failed to install testing recorder");
        Self {
            snapshotter,
            cumulative_counters: Mutex::new(HashMap::new()),
        }
    }

    /// Gets the current cumulative value of a specific counter metric.
    /// This method now updates its internal cumulative state with the latest delta from the snapshot.
    fn get_cumulative_counter(&self, name: &str, route: &str) -> u64 {
        let snapshot = self.snapshotter.snapshot(); // This resets the internal metrics-util counters
        let mut cumulative_counters = self.cumulative_counters.lock().unwrap();
        for (key, _, _, value) in snapshot.into_vec() {
            if key.kind() == MetricKind::Counter && key.key().name() == name {
                if key
                    .key()
                    .labels()
                    .any(|l| l.key() == "route" && l.value() == route)
                {
                    // Add the delta from this snapshot to the cumulative total
                    let entry = cumulative_counters
                        .entry((name.to_string(), route.to_string()))
                        .or_insert(0);
                    if let DebugValue::Counter(delta_value) = value {
                        *entry += delta_value;
                    }
                }
            }
        }
        // Return the current cumulative total for the requested metric
        *cumulative_counters
            .get(&(name.to_string(), route.to_string()))
            .unwrap_or(&0)
    }
}

struct DockerCompose {
    // No longer need to hold the child process, as we'll manage it globally.
}

impl DockerCompose {
    fn up() {
        println!("Starting docker-compose services...");
        let status = Command::new("docker-compose")
            .arg("-f")
            .arg("tests/docker-compose.integration.yml")
            .arg("up")
            .arg("-d") // Detached mode
            .arg("--wait") // Wait for services to be healthy
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .expect("Failed to start docker-compose");

        assert!(status.success(), "docker-compose up --wait failed");
        println!("Services should be up.");
    }

    fn down() {
        println!("Stopping docker-compose services...");
        Command::new("docker-compose")
            .arg("-f")
            .arg("tests/docker-compose.integration.yml")
            .arg("down")
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .expect("Failed to stop docker-compose");
        println!("Services stopped.");
    }
}

fn read_output_file(path: &std::path::Path) -> HashSet<String> {
    if !path.exists() {
        // Return an empty set if the file doesn't exist. The assertion on message count will fail with a clearer message.
        println!(
            "WARNING: Output file not found: {:?}. Assuming no messages were received.",
            path
        );
        return HashSet::new();
    }
    let file =
        File::open(path).unwrap_or_else(|_| panic!("Failed to open output file: {:?}", path));
    let reader = BufReader::new(file);
    let mut received_messages = HashSet::new();

    for line in reader.lines() {
        let line = line.unwrap();
        if line.is_empty() {
            continue;
        }
        // The file contains the JSON representation of the payload, not the whole message
        let payload: Value = serde_json::from_str(&line)
            .unwrap_or_else(|_| panic!("Failed to parse line: {}", line));
        let msg = CanonicalMessage::from_json(payload).unwrap();
        received_messages.insert(msg.message_id.to_string());
    }
    received_messages
}

/// Helper function to run a single end-to-end test pipeline.
async fn run_pipeline_test(broker_name: &str, config_file_name: &str) {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.log");
    let output_path = temp_dir
        .path()
        .join(format!("output_{}.log", broker_name.to_lowercase()));

    // Generate a test file with unique messages
    let num_messages = 5;
    let sent_message_ids = generate_test_file(&input_path, num_messages);

    // Load the specific config for this test
    let full_config_settings = config::Config::builder()
        .add_source(ConfigFile::with_name(config_file_name).required(true))
        .build()
        .unwrap();
    let full_config: AppConfig = full_config_settings.try_deserialize().unwrap();

    // Programmatically build a minimal config with only the routes we need for this specific test.
    // This prevents interference from other routes (like other MQTT clients).
    let mut test_config = AppConfig::default();
    test_config.log_level = "info".to_string();
    test_config.sled_path = temp_dir.path().join("db").to_str().unwrap().to_string();

    // The routes are now named consistently in the small config files
    let file_to_broker_route = format!("file_to_{}", broker_name.to_lowercase());
    let broker_to_file_route = format!("{}_to_file", broker_name.to_lowercase());
    let mut route_to_broker = full_config
        .routes
        .get(&file_to_broker_route)
        .unwrap()
        .clone();
    let mut route_from_broker = full_config
        .routes
        .get(&broker_to_file_route)
        .unwrap()
        .clone();

    // Manually override the file paths in our cloned route objects
    if let mq_multi_bridge::config::ConsumerEndpointType::File(f) =
        &mut route_to_broker.r#in.endpoint_type
    {
        f.config.path = input_path.to_str().unwrap().to_string();
    }
    if let mq_multi_bridge::config::PublisherEndpointType::File(f) =
        &mut route_from_broker.out.endpoint_type
    {
        f.config.path = output_path.to_str().unwrap().to_string();
    }

    test_config
        .routes
        .insert(file_to_broker_route.to_string(), route_to_broker);
    test_config
        .routes
        .insert(broker_to_file_route.to_string(), route_from_broker);

    println!("--- Using Test Configuration for {} ---", broker_name);

    // Install a metrics recorder that we can query.
    let metrics = TestMetrics::new();

    // Run the bridge in a separate task
    let mut bridge = mq_multi_bridge::Bridge::new(test_config);
    let shutdown_tx = bridge.get_shutdown_handle();
    let bridge_task = bridge.run();

    // Poll the metrics until all messages are processed or we time out.
    let timeout = Duration::from_secs(30);
    let start_time = std::time::Instant::now();

    while start_time.elapsed() < timeout {
        // We poll the "sent" counter of the route that writes to the file.
        let sent_count =
            metrics.get_cumulative_counter("bridge_messages_received_total", &broker_to_file_route);
        if sent_count >= num_messages as u64 {
            println!(
                "[{}] Metrics show {} messages sent. Proceeding to verification.",
                broker_name, sent_count
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    bridge.flush_routes().await;
    // The file-based route should finish on its own, but we send a shutdown
    // signal to ensure the bridge task terminates cleanly, especially if the
    // timeout was hit.
    if shutdown_tx.send(()).is_err() {
        println!("WARN: Could not send shutdown signal, bridge may have already stopped.");
    }
    let _ = bridge_task.await; // Wait for the bridge to fully shut down.

    // Verify the output file
    let received_ids = read_output_file(&output_path);
    assert_eq!(
        received_ids.len(),
        num_messages,
        "TEST FAILED for [{}]: Expected {} messages, but found {}. The output file might be missing or empty.",
        broker_name,
        num_messages,
        received_ids.len(),
    );
    assert_eq!(
        sent_message_ids, received_ids,
        "TEST FAILED for [{}]: The set of received message IDs does not match the set of sent message IDs.",
        broker_name,
    );
    println!("Successfully verified {} route!", broker_name);
}

/// Helper function to run a single end-to-end performance test pipeline.
async fn run_performance_pipeline_test(
    broker_name: &str,
    config_file_name: &str,
    num_messages: usize,
) {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.log");
    let output_path = temp_dir
        .path()
        .join(format!("output_{}.log", broker_name.to_lowercase()));

    // Generate a test file with unique messages
    println!(
        "[{}] Generating {} messages for performance test...",
        broker_name, num_messages
    );
    let sent_message_ids = generate_test_file(&input_path, num_messages);
    println!("[{}] Finished generating messages.", broker_name);

    // Load the specific config for this test
    let full_config_settings = config::Config::builder()
        .add_source(ConfigFile::with_name(config_file_name).required(true))
        .build()
        .unwrap();
    let full_config: AppConfig = full_config_settings.try_deserialize().unwrap();

    let mut test_config = AppConfig::default();
    test_config.log_level = "info".to_string(); // Revert to info level
    test_config.sled_path = temp_dir.path().join("db").to_str().unwrap().to_string();

    let file_to_broker_route = format!("file_to_{}", broker_name.to_lowercase());
    let broker_to_file_route = format!("{}_to_file", broker_name.to_lowercase());
    let mut route_to_broker = full_config
        .routes
        .get(&file_to_broker_route)
        .unwrap()
        .clone();
    let mut route_from_broker = full_config
        .routes
        .get(&broker_to_file_route)
        .unwrap()
        .clone();

    // Manually override the file paths
    if let mq_multi_bridge::config::ConsumerEndpointType::File(f) =
        &mut route_to_broker.r#in.endpoint_type
    {
        f.config.path = input_path.to_str().unwrap().to_string();
    }
    if let mq_multi_bridge::config::PublisherEndpointType::File(f) =
        &mut route_from_broker.out.endpoint_type
    {
        f.config.path = output_path.to_str().unwrap().to_string();
    }

    test_config
        .routes
        .insert(file_to_broker_route.to_string(), route_to_broker);
    test_config
        .routes
        .insert(broker_to_file_route.to_string(), route_from_broker);

    // Run the bridge and measure
    let mut bridge = mq_multi_bridge::Bridge::new(test_config);
    let shutdown_tx = bridge.get_shutdown_handle();
    println!("[{}] Starting performance test...", broker_name);

    let metrics = TestMetrics::new();

    let start_time = std::time::Instant::now();
    let bridge_handle = bridge.run();

    let timeout = Duration::from_secs(40);
    while start_time.elapsed() < timeout {
        // We poll the "sent" counter of the route that writes to the file, accumulating its value.
        let sent_count =
            metrics.get_cumulative_counter("bridge_messages_received_total", &broker_to_file_route);
        if sent_count >= num_messages as u64 {
            println!(
                "[{}] Metrics show {} messages sent. Proceeding to verification.",
                broker_name, sent_count
            );
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    bridge.flush_routes().await;
    // Poll the output file until all messages are received or we time out.

    let received_ids = read_output_file(&output_path);
    assert_eq!(received_ids.len(), num_messages,
        "TEST FAILED for [{}]: The set of received message IDs does not match the set of sent message IDs.", broker_name);
    let duration = start_time.elapsed();
    if shutdown_tx.send(()).is_err() {
        println!("WARN: Could not send shutdown signal, bridge may have already stopped.");
    }

    let _ = bridge_handle.await; // Wait for the bridge to fully shut down.

    let messages_per_second = num_messages as f64 / duration.as_secs_f64();

    println!("\n--- {} Performance Test Results ---", broker_name);
    println!(
        "Processed {} messages in {:.3} seconds.",
        received_ids.len(),
        duration.as_secs_f64()
    );
    println!("Rate: {:.2} messages/second", messages_per_second);
    println!("--------------------------------\n");

    // Verification
    assert_eq!(
        received_ids.len(),
        num_messages,
        "Did not receive all messages for {}.",
        broker_name
    );
    assert_eq!(
        sent_message_ids, received_ids,
        "Message IDs do not match for {}.",
        broker_name
    );
    println!("[{}] Verification successful.", broker_name);
}

// --- Performance Test Helpers ---

const PERF_TEST_CONCURRENCY: usize = 100;

fn generate_message() -> CanonicalMessage {
    CanonicalMessage::from_json(json!({ "perf_test": true, "ts": chrono::Utc::now().to_rfc3339() }))
        .unwrap()
}

async fn measure_write_performance(
    name: &str,
    publisher: Arc<dyn MessagePublisher>,
    num_messages: usize,
    concurrency: usize,
) {
    println!("\n--- Measuring Write Performance for {} ---", name);
    // Use an MPMC channel for efficient work distribution among workers.
    let (tx, rx): (Sender<CanonicalMessage>, Receiver<CanonicalMessage>) = bounded(concurrency * 2);

    // Producer task to fill the channel with messages
    tokio::spawn(async move {
        for _ in 0..num_messages {
            if tx.send(generate_message()).await.is_err() {
                // Channel closed, stop producing.
                break;
            }
        }
        // Close the channel to signal that no more messages will be sent.
        tx.close();
    });

    let start_time = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();

    for _ in 0..concurrency {
        let rx_clone = rx.clone();
        let publisher_clone = publisher.clone();
        tasks.spawn(async move {
            while let Ok(message) = rx_clone.recv().await {
                if let Err(e) = publisher_clone.send(message).await {
                    eprintln!("Error sending message: {}", e);
                }
            }
        });
    }

    // Wait for all worker tasks to finish
    while tasks.join_next().await.is_some() {}
    publisher.flush().await.unwrap();

    let duration = start_time.elapsed();
    let msgs_per_sec = num_messages as f64 / duration.as_secs_f64();

    println!(
        "  Wrote {} messages in {:.2?} ({:.2} msgs/sec)",
        num_messages, duration, msgs_per_sec
    );
}

async fn measure_read_performance(
    name: &str,
    consumer: Arc<Mutex<dyn MessageConsumer>>,
    num_messages: usize,
) {
    println!("\n--- Measuring Read Performance for {} ---", name);
    let start_time = Instant::now();

    for i in 0..num_messages {
        match consumer.lock().unwrap().receive().await {
            Ok((_, commit)) => {
                commit(None).await;
            }
            Err(e) => {
                eprintln!(
                    "Error receiving message {}/{}: {}. Stopping test.",
                    i + 1,
                    num_messages,
                    e
                );
                break;
            }
        }
    }

    let duration = start_time.elapsed();
    let msgs_per_sec = num_messages as f64 / duration.as_secs_f64();

    println!(
        "  Read {} messages in {:.2?} ({:.2} msgs/sec)",
        num_messages, duration, msgs_per_sec
    );
}

// --- End-to-End Pipeline Tests (Existing) ---

static LOG_GUARD: Mutex<Option<WorkerGuard>> = Mutex::new(None);

#[ctor]
fn startup() {
    // Initialize logging once for the entire test run.
    // This prevents the "global logger already set" panic.
    let file_appender = tracing_appender::rolling::never(".", "integration_test.log");
    let (non_blocking_writer, guard) = tracing_appender::non_blocking(file_appender);

    // Store the guard in a static to ensure it lives for the duration of the test run.
    *LOG_GUARD.lock().unwrap() = Some(guard);

    // Use EnvFilter to allow log levels to be set by the RUST_LOG environment variable.
    // Default to "info" if RUST_LOG is not set.
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking_writer)
        .with_ansi(false);

    let stdout_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stdout);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(file_layer)
        .with(stdout_layer)
        .init();

    DockerCompose::up();
}

#[dtor]
fn shutdown() {
    // DockerCompose::down();
}

fn generate_test_file(path: &std::path::Path, num_messages: usize) -> HashSet<String> {
    let mut file = File::create(path).unwrap();
    let mut sent_messages = HashSet::new();

    for i in 0..num_messages {
        let payload = json!({ "message_num": i, "test_id": "integration" });
        let msg = CanonicalMessage::from_json(payload.clone()).unwrap();
        writeln!(file, "{}", serde_json::to_string(&payload).unwrap()).unwrap();
        sent_messages.insert(msg.message_id.to_string());
    }
    sent_messages
}

#[tokio::test]
async fn test_kafka_pipeline() {
    run_pipeline_test("Kafka", "tests/config.kafka").await;
}

#[tokio::test]
async fn test_nats_pipeline() {
    run_pipeline_test("NATS", "tests/config.nats").await;
}

#[tokio::test]
async fn test_amqp_pipeline() {
    run_pipeline_test("AMQP", "tests/config.amqp").await;
}

#[tokio::test]
async fn test_mqtt_pipeline() {
    run_pipeline_test("MQTT", "tests/config.mqtt").await;
}

#[tokio::test]
async fn test_all_pipelines_together() {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.log");
    let num_messages = 5;
    let sent_message_ids = generate_test_file(&input_path, num_messages);

    // Define output paths for each broker
    let output_paths = [
        ("kafka", temp_dir.path().join("output_kafka.log")),
        ("nats", temp_dir.path().join("output_nats.log")),
        ("amqp", temp_dir.path().join("output_amqp.log")),
        ("mqtt", temp_dir.path().join("output_mqtt.log")),
    ];

    // Load the comprehensive config
    let full_config_settings = config::Config::builder()
        .add_source(ConfigFile::with_name("tests/config.all").required(true))
        .build()
        .unwrap();
    let mut test_config: AppConfig = full_config_settings.try_deserialize().unwrap();

    // Override file paths for all routes
    for (route_name, route) in test_config.routes.iter_mut() {
        // Override source file paths
        if let mq_multi_bridge::config::ConsumerEndpointType::File(f) =
            &mut route.r#in.endpoint_type
        {
            f.config.path = input_path.to_str().unwrap().to_string();
        }
        // Override sink file paths
        if let mq_multi_bridge::config::PublisherEndpointType::File(f) =
            &mut route.out.endpoint_type
        {
            for (broker_name, path) in &output_paths {
                if route_name.contains(broker_name) {
                    f.config.path = path.to_str().unwrap().to_string();
                }
            }
        }
    }

    println!("--- Using Comprehensive Test Configuration ---");
    println!("{:#?}", test_config);
    println!("------------------------------------------");

    // Run the bridge
    let mut bridge = mq_multi_bridge::Bridge::new(test_config);
    let shutdown_tx = bridge.get_shutdown_handle();
    let _bridge_handle = bridge.run();

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(30)).await;
    if shutdown_tx.send(()).is_err() {
        println!("WARN: Could not send shutdown signal, bridge may have already stopped.");
    }

    // Verify all output files
    for (broker_name, path) in &output_paths {
        println!("Verifying output for {}...", broker_name);
        let received_ids = read_output_file(path);
        assert_eq!(
            received_ids.len(),
            num_messages,
            "TEST FAILED for [{}]: Expected {} messages, but found {}.",
            broker_name,
            num_messages,
            received_ids.len()
        );
        assert_eq!(
            sent_message_ids, received_ids,
            "TEST FAILED for [{}]: Message IDs do not match.",
            broker_name
        );
        println!("Successfully verified {} route!", broker_name);
    }
}

// cargo test --release --test integration_test -- --ignored --nocapture --test-threads=1 --show-output
#[tokio::test]
async fn test_file_to_file_performance() {
    // 1. Setup: Define test parameters and create temporary files.
    let num_messages = 100_000; // Use a large number for a meaningful performance test.
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("perf_input.log");
    let output_path = temp_dir.path().join("perf_output.log");

    println!(
        "Generating {} messages for file-to-file performance test...",
        num_messages
    );
    let sent_message_ids = generate_test_file(&input_path, num_messages);
    println!("Finished generating messages.");

    // 2. Configure the bridge for a simple file-to-file route.
    let route = mq_multi_bridge::config::Route {
        r#in: mq_multi_bridge::config::ConsumerEndpoint {
            endpoint_type: mq_multi_bridge::config::ConsumerEndpointType::File(
                mq_multi_bridge::config::FileConsumerEndpoint {
                    config: mq_multi_bridge::config::FileConfig {
                        path: input_path.to_str().unwrap().to_string(),
                    },
                    endpoint: mq_multi_bridge::config::FileEndpoint {},
                },
            ),
        },
        out: mq_multi_bridge::config::PublisherEndpoint {
            endpoint_type: mq_multi_bridge::config::PublisherEndpointType::File(
                mq_multi_bridge::config::FilePublisherEndpoint {
                    config: mq_multi_bridge::config::FileConfig {
                        path: output_path.to_str().unwrap().to_string(),
                    },
                    endpoint: mq_multi_bridge::config::FileEndpoint {},
                },
            ),
        },
        dlq: None,
        concurrency: Some(100), // Set concurrency for this test route
        deduplication_enabled: false,
    };

    let mut test_config = AppConfig::default();
    // Reduce logging verbosity to avoid impacting performance measurement.
    test_config.log_level = "warn".to_string();
    test_config.sled_path = temp_dir.path().join("db").to_str().unwrap().to_string();
    test_config
        .routes
        .insert("file_to_file_perf".to_string(), route);

    // 3. Run the bridge and measure execution time.
    let mut bridge = mq_multi_bridge::Bridge::new(test_config);
    let shutdown_tx = bridge.get_shutdown_handle();

    println!("Starting performance test...");
    let start_time = std::time::Instant::now();

    // The bridge will run and the file-to-file route will complete when the source file reaches EOF.
    let bridge_handle = bridge.run();
    bridge_handle.await.unwrap().unwrap();

    let duration = start_time.elapsed();
    let messages_per_second = num_messages as f64 / duration.as_secs_f64();

    println!("\n--- Performance Test Results ---");
    println!(
        "Processed {} messages in {:.3} seconds.",
        num_messages,
        duration.as_secs_f64()
    );
    println!("Rate: {:.2} messages/second", messages_per_second);
    println!("--------------------------------\n");

    // Explicitly drop the shutdown handle.
    drop(shutdown_tx);

    // 4. Verification: Ensure all messages were transferred correctly.
    let received_ids = read_output_file(&output_path);
    assert_eq!(
        received_ids.len(),
        num_messages,
        "Expected {} messages, but found {}.",
        num_messages,
        received_ids.len()
    );
    assert_eq!(sent_message_ids, received_ids, "Message IDs do not match.");
    println!("Verification successful: All messages were transferred correctly.");
}

// --- Individual Broker Performance Tests ---

const PERF_TEST_MESSAGE_COUNT: usize = 20_000;

#[tokio::test]
async fn test_kafka_performance_pipeline() {
    run_performance_pipeline_test("Kafka", "tests/config.kafka", PERF_TEST_MESSAGE_COUNT).await;
}

#[tokio::test]
async fn test_nats_performance_pipeline() {
    run_performance_pipeline_test("NATS", "tests/config.nats", PERF_TEST_MESSAGE_COUNT).await;
}

#[tokio::test]
async fn test_amqp_performance_pipeline() {
    run_performance_pipeline_test("AMQP", "tests/config.amqp", PERF_TEST_MESSAGE_COUNT).await;
}

#[tokio::test]
async fn performance_test_file_direct() {
    let dir = tempfile::tempdir().unwrap();
    let file_path = dir.path().join("perf_test.log");
    let config = mq_multi_bridge::config::FileConfig {
        path: file_path.to_str().unwrap().to_string(),
    };

    let publisher = Arc::new(FilePublisher::new(&config).await.unwrap());
    measure_write_performance(
        "File",
        publisher.clone(),
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY,
    )
    .await;

    // Crucial: Drop the publisher and flush to ensure all data is written and file handle is released
    // before the consumer tries to open it.
    drop(publisher);
    tokio::time::sleep(Duration::from_millis(50)).await; // Give OS a moment

    let consumer = Arc::new(Mutex::new(FileConsumer::new(&config).await.unwrap()));
    measure_read_performance("File", consumer, PERF_TEST_MESSAGE_COUNT).await;
}

#[tokio::test]
async fn performance_test_kafka_direct() {
    let topic = "perf_test_kafka_direct";
    let config = mq_multi_bridge::config::KafkaConfig {
        brokers: "localhost:9092".to_string(),
        group_id: Some("perf_test_group_kafka".to_string()),
        ..Default::default()
    };

    let publisher = Arc::new(KafkaPublisher::new(&config, &topic).await.unwrap());
    measure_write_performance(
        "Kafka",
        publisher,
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY,
    )
    .await;

    // Give Kafka time to process publishes before subscribing
    tokio::time::sleep(Duration::from_secs(5)).await;

    let consumer = Arc::new(Mutex::new(KafkaConsumer::new(&config, &topic).unwrap()));
    measure_read_performance("Kafka", consumer, PERF_TEST_MESSAGE_COUNT).await;
}

#[tokio::test]
async fn performance_test_nats_direct() {
    // Use a consistent stream name for all performance tests.
    let stream_name = "perf_stream_nats_direct";
    // Use a static subject that matches the stream's wildcard filter ("perf_stream_nats.>")
    let subject = format!("{}.direct", stream_name);
    let config = mq_multi_bridge::config::NatsConfig {
        url: "nats://localhost:4222".to_string(),
        await_ack: true, // Test with acks for reliability, can be set to false for max throughput
        ..Default::default()
    };

    // The publisher will create the stream if it doesn't exist.
    // We pass the specific subject for publishing.
    let publisher = Arc::new(
        NatsPublisher::new(&config, &subject, Some(stream_name))
            .await
            .unwrap(),
    );
    measure_write_performance(
        "NATS",
        publisher,
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY,
    )
    .await;

    // Give NATS time to process publishes before subscribing
    tokio::time::sleep(Duration::from_secs(5)).await;

    // The consumer will subscribe to the specific subject within the existing stream.
    let consumer = Arc::new(Mutex::new(
        NatsConsumer::new(&config, stream_name, &subject)
            .await
            .unwrap(),
    ));
    measure_read_performance("NATS", consumer, PERF_TEST_MESSAGE_COUNT).await;
}

#[tokio::test]
async fn performance_test_amqp_direct() {
    let queue = "perf_test_amqp_direct";
    let config = mq_multi_bridge::config::AmqpConfig {
        url: "amqp://guest:guest@localhost:5672/%2f".to_string(),
        await_ack: false, // Test with acks for reliability, can be set to false for max throughput
        ..Default::default()
    };

    let publisher = Arc::new(AmqpPublisher::new(&config, &queue).await.unwrap());
    measure_write_performance(
        "AMQP",
        publisher,
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY,
    )
    .await;

    // Give AMQP time to process publishes before subscribing
    tokio::time::sleep(Duration::from_secs(10)).await;

    let consumer = Arc::new(Mutex::new(
        AmqpConsumer::new(&config, &queue).await.unwrap(),
    ));
    measure_read_performance("AMQP", consumer, PERF_TEST_MESSAGE_COUNT).await;
}

#[tokio::test]
async fn performance_test_mqtt_direct() {
    let topic = "perf_test_mqtt/direct";
    let bridge_id = "perf_test_mqtt_direct";
    let config = mq_multi_bridge::config::MqttConfig {
        url: "mqtt://localhost:1883".to_string(),
        ..Default::default()
    };

    let publisher = Arc::new(
        MqttPublisher::new(&config, &topic, bridge_id)
            .await
            .unwrap(),
    );
    measure_write_performance(
        "MQTT",
        publisher,
        PERF_TEST_MESSAGE_COUNT,
        PERF_TEST_CONCURRENCY,
    )
    .await;

    // Give broker time to process publishes before subscribing
    tokio::time::sleep(Duration::from_secs(5)).await;

    let consumer = Arc::new(Mutex::new(
        MqttConsumer::new(&config, &topic, bridge_id).await.unwrap(),
    ));
    measure_read_performance("MQTT", consumer, PERF_TEST_MESSAGE_COUNT).await;
}

#[tokio::test]
async fn test_mqtt_performance_pipeline() {
    run_performance_pipeline_test("MQTT", "tests/config.mqtt", PERF_TEST_MESSAGE_COUNT).await;
}
