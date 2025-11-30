// cargo test --test integration_test -- --ignored --nocapture --test-threads=1 --show-output

use config::File as ConfigFile; // Use an alias for the File type from the config crate
use mq_multi_bridge::config::{Config as AppConfig}; // Use an alias for our app's config struct
use mq_multi_bridge::model::CanonicalMessage;
use serde_json::json;
use tracing_appender::non_blocking::WorkerGuard;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::process::Command;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use ctor::{ctor, dtor};
use tempfile::tempdir;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

struct DockerCompose {
    // No longer need to hold the child process, as we'll manage it globally.
}

impl DockerCompose {
    fn up() {
        println!("Starting docker-compose services...");
        let _child = Command::new("docker-compose")
            .arg("-f")
            .arg("tests/docker-compose.integration.yml")
            .arg("up")
            .arg("-d") // Detached mode
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .expect("Failed to start docker-compose");

        // We don't wait for the command to finish, but we can check its status later if needed.
        // For now, we just sleep to let services initialize.

        // Give services time to initialize
        println!("Waiting for services to initialize...");
        thread::sleep(Duration::from_secs(15)); // May need adjustment
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
        println!("WARNING: Output file not found: {:?}. Assuming no messages were received.", path);
        return HashSet::new();
    }
    let file = File::open(path).unwrap_or_else(|_| panic!("Failed to open output file: {:?}", path));
    let reader = BufReader::new(file);
    let mut received_messages = HashSet::new();

    for line in reader.lines() {
        let line = line.unwrap();
        if line.is_empty() {
            continue;
        }
        let msg: CanonicalMessage =
            serde_json::from_str(&line).expect(&format!("Failed to parse line: {}", line));
        received_messages.insert(msg.message_id.to_string());
    }
    received_messages
}

/// Helper function to run a single end-to-end test pipeline.
async fn run_pipeline_test(
    broker_name: &str,
    config_file_name: &str,
) {    

    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.log");
    let output_path = temp_dir.path().join(format!("output_{}.log", broker_name.to_lowercase()));

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
    test_config.log_level = "trace".to_string();
    test_config.sled_path = temp_dir.path().join("db").to_str().unwrap().to_string();

    // The routes are now named consistently in the small config files
    let file_to_broker_route = format!("file_to_{}", broker_name.to_lowercase());
    let broker_to_file_route = format!("{}_to_file", broker_name.to_lowercase());
    let mut route_to_broker = full_config.routes.get(&file_to_broker_route).unwrap().clone();
    let mut route_from_broker = full_config.routes.get(&broker_to_file_route).unwrap().clone();

    // Manually override the file paths in our cloned route objects
    if let mq_multi_bridge::config::SourceEndpointType::File(f) = &mut route_to_broker.source.endpoint_type { f.config.path = input_path.to_str().unwrap().to_string(); }
    if let mq_multi_bridge::config::SinkEndpointType::File(f) = &mut route_from_broker.sink.endpoint_type { f.config.path = output_path.to_str().unwrap().to_string(); }

    test_config.routes.insert(file_to_broker_route.to_string(), route_to_broker);
    test_config.routes.insert(broker_to_file_route.to_string(), route_from_broker);

    // --- DIAGNOSTIC STEP 1: Print the configuration being used ---
    // This ensures that the file path overrides are correctly applied.
    println!("--- Using Test Configuration for {} ---", broker_name);

    // Run the bridge in a separate task
    let mut bridge = mq_multi_bridge::Bridge::from_config(test_config, None).unwrap();
    bridge.initialize_from_config().await.unwrap();

    // --- DIAGNOSTIC STEP 2: Assert that the bridge tasks were created ---
    // assert!(test_config.routes.is_empty(), "FATAL: Bridge did not initialize any routes. No tasks were created.");
    
    let bridge_task = tokio::spawn(bridge.run());

    // Give the bridge time to process all messages.
    // The file source will error on EOF, and the bridge will log it and continue.
    // This is a simple way to wait for completion in this test setup.
    tokio::time::sleep(Duration::from_secs(25)).await;

    // The bridge task should have finished or be idle. We can drop the shutdown receiver.
    drop(bridge_task);

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

static LOG_GUARD: Mutex<Option<WorkerGuard>> = Mutex::new(None);

#[ctor]
fn startup() {
    // Initialize logging once for the entire test run.
    // This prevents the "global logger already set" panic.
    let file_appender = tracing_appender::rolling::never(".", "integration_test.log");
    let (non_blocking_writer, guard) = tracing_appender::non_blocking(file_appender);

    // Store the guard in a static to ensure it lives for the duration of the test run.
    *LOG_GUARD.lock().unwrap() = Some(guard);

    let file_layer = tracing_subscriber::fmt::layer()
        .with_writer(non_blocking_writer)
        .with_ansi(false);

    let stdout_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stdout);

    tracing_subscriber::registry().with(file_layer).with(stdout_layer).init();

    DockerCompose::up();
}

#[dtor]
fn shutdown() {
    DockerCompose::down();
}

fn generate_test_file(path: &std::path::Path, num_messages: usize) -> HashSet<String> {
    let mut file = File::create(path).unwrap();
    let mut sent_messages = HashSet::new();

    for i in 0..num_messages {
        let msg = CanonicalMessage::new(json!({ "message_num": i, "test_id": "integration" }));
        let json_string = serde_json::to_string(&msg).unwrap();
        writeln!(file, "{}", json_string).unwrap();
        sent_messages.insert(msg.message_id.to_string());
    }
    sent_messages
}

#[tokio::test]
#[ignore]
async fn test_kafka_pipeline() {
    run_pipeline_test("Kafka", "tests/config.kafka").await;
}

#[tokio::test]
#[ignore]
async fn test_nats_pipeline() {
    run_pipeline_test("NATS", "tests/config.nats").await;
}

#[tokio::test]
#[ignore]
async fn test_amqp_pipeline() {
    run_pipeline_test("AMQP", "tests/config.amqp").await;
}

#[tokio::test]
#[ignore]
async fn test_mqtt_pipeline() {
    run_pipeline_test("MQTT", "tests/config.mqtt").await;
}

#[tokio::test]
#[ignore]
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
        if let mq_multi_bridge::config::SourceEndpointType::File(f) = &mut route.source.endpoint_type {
            f.config.path = input_path.to_str().unwrap().to_string();
        }
        // Override sink file paths
        if let mq_multi_bridge::config::SinkEndpointType::File(f) = &mut route.sink.endpoint_type {
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
    let mut bridge = mq_multi_bridge::Bridge::from_config(test_config, None).unwrap();
    bridge.initialize_from_config().await.unwrap();
    let bridge_task = tokio::spawn(bridge.run());

    // Wait for processing
    tokio::time::sleep(Duration::from_secs(30)).await;
    drop(bridge_task);

    // Verify all output files
    for (broker_name, path) in &output_paths {
        println!("Verifying output for {}...", broker_name);
        let received_ids = read_output_file(path);
        assert_eq!(
            received_ids.len(),
            num_messages,
            "TEST FAILED for [{}]: Expected {} messages, but found {}.",
            broker_name, num_messages, received_ids.len()
        );
        assert_eq!(
            sent_message_ids, received_ids,
            "TEST FAILED for [{}]: Message IDs do not match.",
            broker_name
        );
        println!("Successfully verified {} route!", broker_name);
    }
}
