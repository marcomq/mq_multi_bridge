mod common;

use config::File as ConfigFile; // Use an alias for the File type from the config crate
use ctor::{ctor, dtor};
use mq_multi_bridge::config::Config as AppConfig; // Use an alias for our app's config struct
use common::{generate_test_file, read_output_file, DockerCompose};
use std::collections::HashSet;

use std::io::{BufRead, BufReader, Write};
use std::time::Duration;
use tempfile::tempdir;

struct TestManager {
    docker: DockerCompose,
}

#[ctor]
static ALL_ENDPOINTS_TEST_MANAGER: TestManager = {
    common::setup_logging();
    let manager = TestManager {
        docker: DockerCompose::new("tests/docker-compose.all.yml"),
    };
    manager.docker.up();
    manager
};

#[dtor]
fn shutdown() {
    ALL_ENDPOINTS_TEST_MANAGER.docker.down();
}

pub async fn test_all_pipelines_together() {
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
