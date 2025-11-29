use config::File as ConfigFile; // Use an alias for the File type from the config crate
use mq_multi_bridge::config::Config as AppConfig; // Use an alias for our app's config struct
use mq_multi_bridge::model::CanonicalMessage;
use serde_json::json;
use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::process::{Child, Command};
use std::thread;
use std::time::Duration;
use tempfile::tempdir;

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
        thread::sleep(Duration::from_secs(45)); // May need adjustment
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
    let file = File::open(path).expect(&format!("Failed to open output file: {:?}", path));
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

#[tokio::test]
#[ignore] // This test is long and requires docker, run explicitly with `cargo test -- --ignored`
async fn test_end_to_end_routing() {
    let _docker_guard = DockerCompose::up();

    let temp_dir = tempdir().unwrap();
    let input_path = temp_dir.path().join("input.log");
    let output_kafka_path = temp_dir.path().join("output_kafka.log");
    let output_nats_path = temp_dir.path().join("output_nats.log");
    let output_amqp_path = temp_dir.path().join("output_amqp.log");
    let output_mqtt_path = temp_dir.path().join("output_mqtt.log");

    // Generate a test file with unique messages
    let num_messages = 5;
    let sent_message_ids = generate_test_file(&input_path, num_messages);

    // Load config and override file paths
    let settings = config::Config::builder()
        .add_source(ConfigFile::with_name("tests/config.integration").required(true))
        .set_override("connections.0.file.path", input_path.to_str().unwrap())
        .unwrap()
        .set_override(
            "connections.1.file.path",
            output_kafka_path.to_str().unwrap(),
        )
        .unwrap()
        .set_override(
            "connections.2.file.path",
            output_nats_path.to_str().unwrap(),
        )
        .unwrap()
        .set_override(
            "connections.3.file.path",
            output_amqp_path.to_str().unwrap(),
        )
        .unwrap()
        .set_override(
            "connections.4.file.path",
            output_mqtt_path.to_str().unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();

    let test_config: AppConfig = settings.try_deserialize().unwrap();

    // Run the bridge in a separate task
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
    let mut bridge = mq_multi_bridge::Bridge::from_config(test_config, shutdown_rx).unwrap();
    bridge.initialize_from_config().await.unwrap();

    let bridge_task = tokio::spawn(bridge.run());

    // Give the bridge time to process all messages.
    // The file source will error on EOF, and the bridge will log it and continue.
    // This is a simple way to wait for completion in this test setup.
    tokio::time::sleep(Duration::from_secs(20)).await;

    // The bridge task should have finished or be idle. We can drop the shutdown receiver.
    drop(bridge_task);

    // Verify the output files
    let output_paths = [
        ("Kafka", &output_kafka_path),
        ("NATS", &output_nats_path),
        ("AMQP", &output_amqp_path),
        ("MQTT", &output_mqtt_path),
    ];

    for (name, path) in &output_paths {
        let received_ids = read_output_file(path);
        assert_eq!(
            received_ids.len(),
            num_messages,
            "Mismatch in message count for {}",
            name
        );
        assert_eq!(
            sent_message_ids, received_ids,
            "Mismatch in message content for {}",
            name
        );
        println!("Successfully verified {} route!", name);
    }
}
