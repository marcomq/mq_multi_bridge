use async_channel::{bounded, Receiver, Sender};
use chrono;
use mq_multi_bridge::config::Config as AppConfig;
use mq_multi_bridge::consumers::MessageConsumer;
use mq_multi_bridge::model::CanonicalMessage;
use mq_multi_bridge::publishers::MessagePublisher;
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::tempdir;

use config::File as ConfigFile;
use metrics_util::debugging::{DebugValue, DebuggingRecorder, Snapshotter};
use metrics_util::MetricKind;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::filter::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// A helper for capturing and querying metrics during tests.
pub struct TestMetrics {
    snapshotter: Snapshotter,
    cumulative_counters: Mutex<HashMap<(String, String), u64>>,
}

impl TestMetrics {
    pub fn new() -> Self {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::set_global_recorder(recorder).expect("Failed to install testing recorder");
        Self {
            snapshotter,
            cumulative_counters: Mutex::new(HashMap::new()),
        }
    }

    pub fn get_cumulative_counter(&self, name: &str, route: &str) -> u64 {
        let snapshot = self.snapshotter.snapshot();
        let mut cumulative_counters = self.cumulative_counters.lock().unwrap();
        for (key, _, _, value) in snapshot.into_vec() {
            if key.kind() == MetricKind::Counter && key.key().name() == name {
                if key
                    .key()
                    .labels()
                    .any(|l| l.key() == "route" && l.value() == route)
                {
                    let entry = cumulative_counters
                        .entry((name.to_string(), route.to_string()))
                        .or_insert(0);
                    if let DebugValue::Counter(delta_value) = value {
                        *entry += delta_value;
                    }
                }
            }
        }
        *cumulative_counters
            .get(&(name.to_string(), route.to_string()))
            .unwrap_or(&0)
    }
}

pub struct DockerCompose {
    compose_file: String,
}

impl DockerCompose {
    pub fn new(compose_file: &str) -> Self {
        Self {
            compose_file: compose_file.to_string(),
        }
    }

    pub fn up(&self) {
        println!(
            "Starting docker-compose services from {}...",
            self.compose_file
        );
        let status = Command::new("docker-compose")
            .arg("-f")
            .arg(&self.compose_file)
            .arg("up")
            .arg("-d")
            .arg("--wait")
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .expect("Failed to start docker-compose");

        assert!(status.success(), "docker-compose up --wait failed");
        println!("Services from {} should be up.", self.compose_file);
    }

    pub fn down(&self) {
        println!(
            "Stopping docker-compose services from {}...",
            self.compose_file
        );
        Command::new("docker-compose")
            .arg("-f")
            .arg(&self.compose_file)
            .arg("down")
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .expect("Failed to stop docker-compose");
        println!("Services from {} stopped.", self.compose_file);
    }
}

pub fn read_output_file(path: &std::path::Path) -> HashSet<String> {
    if !path.exists() {
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
        let payload: Value = serde_json::from_str(&line)
            .unwrap_or_else(|_| panic!("Failed to parse line: {}", line));
        let msg = CanonicalMessage::from_json(payload).unwrap();
        received_messages.insert(msg.message_id.to_string());
    }
    received_messages
}

pub fn generate_test_file(path: &std::path::Path, num_messages: usize) -> HashSet<String> {
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

pub async fn run_pipeline_test(broker_name: &str, config_file_name: &str) {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.log");
    let output_path = temp_dir
        .path()
        .join(format!("output_{}.log", broker_name.to_lowercase()));

    let num_messages = 5;
    let sent_message_ids = generate_test_file(&input_path, num_messages);

    let full_config_settings = config::Config::builder()
        .add_source(ConfigFile::with_name(config_file_name).required(true))
        .build()
        .unwrap();
    let full_config: AppConfig = full_config_settings.try_deserialize().unwrap();

    let mut test_config = AppConfig::default();
    test_config.log_level = "info".to_string();
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

    let metrics = TestMetrics::new();

    let mut bridge = mq_multi_bridge::Bridge::new(test_config);
    let shutdown_tx = bridge.get_shutdown_handle();
    let bridge_task = bridge.run();

    let timeout = Duration::from_secs(30);
    let start_time = std::time::Instant::now();

    while start_time.elapsed() < timeout {
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

    if shutdown_tx.send(()).is_err() {
        println!("WARN: Could not send shutdown signal, bridge may have already stopped.");
    }
    let _ = bridge_task.await;

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

pub async fn run_performance_pipeline_test(
    broker_name: &str,
    config_file_name: &str,
    num_messages: usize,
) {
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.log");
    let output_path = temp_dir
        .path()
        .join(format!("output_{}.log", broker_name.to_lowercase()));

    println!(
        "[{}] Generating {} messages for performance test...",
        broker_name, num_messages
    );
    let sent_message_ids = generate_test_file(&input_path, num_messages);
    println!("[{}] Finished generating messages.", broker_name);

    let full_config_settings = config::Config::builder()
        .add_source(ConfigFile::with_name(config_file_name).required(true))
        .build()
        .unwrap();
    let full_config: AppConfig = full_config_settings.try_deserialize().unwrap();

    let mut test_config = AppConfig::default();
    test_config.log_level = "info".to_string();
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

    let mut bridge = mq_multi_bridge::Bridge::new(test_config);
    let shutdown_tx = bridge.get_shutdown_handle();
    println!("[{}] Starting performance test...", broker_name);

    let metrics = TestMetrics::new();

    let start_time = std::time::Instant::now();
    let bridge_handle = bridge.run();

    let timeout = Duration::from_secs(40);
    while start_time.elapsed() < timeout {
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

    let received_ids = read_output_file(&output_path);
    assert_eq!(
        received_ids.len(),
        num_messages,
        "TEST FAILED for [{}]: The set of received message IDs does not match the set of sent message IDs.",
        broker_name
    );
    let duration = start_time.elapsed();
    if shutdown_tx.send(()).is_err() {
        println!("WARN: Could not send shutdown signal, bridge may have already stopped.");
    }

    let _ = bridge_handle.await;

    let messages_per_second = num_messages as f64 / duration.as_secs_f64();

    println!("\n--- {} Performance Test Results ---", broker_name);
    println!(
        "Processed {} messages in {:.3} seconds.",
        received_ids.len(),
        duration.as_secs_f64()
    );
    println!("Rate: {:.2} messages/second", messages_per_second);
    println!("--------------------------------\n");

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

static LOG_GUARD: Mutex<Option<WorkerGuard>> = Mutex::new(None);

pub fn setup_logging() {
    // Using a std::sync::Once ensures this is only run once per test binary.
    static START: std::sync::Once = std::sync::Once::new();
    START.call_once(|| {
        let file_appender = tracing_appender::rolling::never(".", "integration_test.log");
        let (non_blocking_writer, guard) = tracing_appender::non_blocking(file_appender);

        *LOG_GUARD.lock().unwrap() = Some(guard);

        let env_filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

        let file_layer = tracing_subscriber::fmt::layer()
            .with_writer(non_blocking_writer)
            .with_ansi(false);

        let stdout_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stdout);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(file_layer)
            .with(stdout_layer)
            .init();
    });
}

pub const PERF_TEST_MESSAGE_COUNT: usize = 20_000;
pub const PERF_TEST_CONCURRENCY: usize = 100;

pub fn generate_message() -> CanonicalMessage {
    CanonicalMessage::from_json(json!({ "perf_test": true, "ts": chrono::Utc::now().to_rfc3339() }))
        .unwrap()
}

pub async fn measure_write_performance(
    name: &str,
    publisher: Arc<dyn MessagePublisher>,
    num_messages: usize,
    concurrency: usize,
) {
    println!("\n--- Measuring Write Performance for {} ---", name);
    let (tx, rx): (Sender<CanonicalMessage>, Receiver<CanonicalMessage>) = bounded(concurrency * 2);

    tokio::spawn(async move {
        for _ in 0..num_messages {
            if tx.send(generate_message()).await.is_err() {
                break;
            }
        }
        tx.close();
    });

    let start_time = Instant::now();
    let mut tasks = tokio::task::JoinSet::new();

    for _ in 0..concurrency {
        let rx_clone = rx.clone();
        let publisher_clone = publisher.clone();
        tasks.spawn(async move {
            while let Ok(message) = rx_clone.recv().await {
                // Loop to retry sending the message in case of backpressure.
                // This is common with async clients like rumqttc where the internal
                // buffer can fill up under high load.
                let mut first_try = true;
                loop {
                    let res = publisher_clone.send(message.clone()).await;
                    if let Err(e) = res {
                        if first_try {
                            eprintln!("Error sending message: {}", e);
                        }
                        first_try = false;
                    } else {
                        break;
                    }
                    // Backpressure detected, yield and retry.
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        });
    }

    while tasks.join_next().await.is_some() {}
    publisher.flush().await.unwrap();

    let duration = start_time.elapsed();
    let msgs_per_sec = num_messages as f64 / duration.as_secs_f64();

    println!(
        "  Wrote {} messages in {:.2?} ({:.2} msgs/sec)",
        num_messages, duration, msgs_per_sec
    );
}

pub async fn measure_read_performance(
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

    let duration: Duration = start_time.elapsed();
    let msgs_per_sec = num_messages as f64 / duration.as_secs_f64();

    println!(
        "  Read {} messages in {:.2?} ({:.2} msgs/sec)",
        num_messages, duration, msgs_per_sec
    );
}
