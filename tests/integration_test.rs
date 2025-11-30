use config::File as ConfigFile; // Use an alias for the File type from the config crate
use mq_multi_bridge::config::{Config as AppConfig}; // Use an alias for our app's config struct
use mq_multi_bridge::model::CanonicalMessage;
use serde_json::json;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;
use tracing_subscriber::fmt::format::FmtSpan;

struct DockerCompose {
    child: Child,
}

impl DockerCompose {
    fn up() -> Self {
        println!("Starting docker-compose services...");
        let child = Command::new("docker-compose")
            .arg("-f")
            .arg("tests/docker-compose.integration.yml")
            .arg("up")
            .arg("-d") // Detached mode
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .expect("Failed to start docker-compose");

        // Give services time to initialize
        println!("Waiting for services to initialize...");
        thread::sleep(Duration::from_secs(15)); // May need adjustment
        println!("Services should be up.");

        Self { child }
    }
}

impl Drop for DockerCompose {
    fn drop(&mut self) {
        println!("Stopping docker-compose services...");
        Command::new("docker-compose")
            .arg("-f")
            .arg("tests/docker-compose.integration.yml")
            .arg("down")
            .stdout(std::process::Stdio::inherit())
            .stderr(std::process::Stdio::inherit())
            .status()
            .expect("Failed to stop docker-compose");
        self.child.kill().ok();
        println!("Services stopped.");
    }
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
    file_to_broker_route: &str,
    broker_to_file_route: &str,
) {    
    let _logger = tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE) // Log entry and exit of spans
        .with_target(true).init();
    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.log");
    let output_path = temp_dir.path().join(format!("output_{}.log", broker_name.to_lowercase()));

    // Generate a test file with unique messages
    let num_messages = 5;
    let sent_message_ids = generate_test_file(&input_path, num_messages);

    // Load the full config first to get the route definitions
    let full_config_settings = config::Config::builder()
        .add_source(ConfigFile::with_name("tests/config.integration").required(true))
        .build()
        .unwrap();
    let full_config: AppConfig = full_config_settings.try_deserialize().unwrap();

    // Programmatically build a minimal config with only the routes we need for this specific test.
    // This prevents interference from other routes (like other MQTT clients).
    let mut test_config = AppConfig::default();
    test_config.log_level = "trace".to_string();
    test_config.sled_path = temp_dir.path().join("db").to_str().unwrap().to_string();

    let mut route_to_broker = full_config.routes.get(file_to_broker_route).unwrap().clone();
    let mut route_from_broker = full_config.routes.get(broker_to_file_route).unwrap().clone();

    // Manually override the file paths in our cloned route objects
    if let mq_multi_bridge::config::SourceEndpointType::File(f) = &mut route_to_broker.source.endpoint_type { f.config.path = input_path.to_str().unwrap().to_string(); }
    if let mq_multi_bridge::config::SinkEndpointType::File(f) = &mut route_from_broker.sink.endpoint_type { f.config.path = output_path.to_str().unwrap().to_string(); }

    test_config.routes.insert(file_to_broker_route.to_string(), route_to_broker);
    test_config.routes.insert(broker_to_file_route.to_string(), route_from_broker);

    // --- DIAGNOSTIC STEP 1: Print the configuration being used ---
    // This ensures that the file path overrides are correctly applied.
    println!("--- Using Test Configuration for {} ---", broker_name);
    println!("{:#?}", test_config);
    println!("------------------------------------");

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

#[tokio::test]
#[ignore]
async fn test_kafka_pipeline() {
    let _docker_guard = DockerCompose::up();
    run_pipeline_test("Kafka", "file_to_kafka", "kafka_to_file").await;
}

#[tokio::test]
#[ignore]
async fn test_nats_pipeline() {
    let _docker_guard = DockerCompose::up();
    run_pipeline_test("NATS", "file_to_nats", "nats_to_file").await;
}

#[tokio::test]
#[ignore]
async fn test_amqp_pipeline() {
    let _docker_guard = DockerCompose::up();
    run_pipeline_test("AMQP", "file_to_amqp", "amqp_to_file").await;
}

#[tokio::test]
#[ignore]
async fn test_mqtt_pipeline() {
    let _docker_guard = DockerCompose::up();
    run_pipeline_test("MQTT", "file_to_mqtt", "mqtt_to_file").await;
}
