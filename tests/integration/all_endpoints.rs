#![allow(dead_code)]
use config::File as ConfigFile; // Use an alias for the File type from the config crate
use mq_multi_bridge::config::Config as AppConfig; // Use an alias for our app's config struct
use super::common::{
    generate_test_messages, read_and_drain_memory_channel, run_test_with_docker, setup_logging,
};

use std::time::Duration;

pub async fn test_all_pipelines_together() {
    setup_logging();
    run_test_with_docker("tests/integration/docker-compose.all.yml", || async {
        let num_messages = 5;
        let (messages_to_send, sent_message_ids) = generate_test_messages(num_messages);

        // Load the comprehensive config
        // This config should define routes like `memory_to_kafka`, `kafka_to_memory`, etc.
        let full_config_settings = config::Config::builder()
            .add_source(ConfigFile::with_name("tests/config.all").required(true))
            .build()
            .unwrap();
        let test_config: AppConfig = full_config_settings.try_deserialize().unwrap();

        // Override file paths for all routes

        println!("--- Using Comprehensive Test Configuration ---");
        println!("{:#?}", test_config);
        println!("------------------------------------------");

        // Run the bridge
        let mut bridge = mq_multi_bridge::Bridge::new(test_config);
        let shutdown_tx = bridge.get_shutdown_handle();
        let _bridge_handle = bridge.run();

        // Get memory channels for each route
        let in_kafka = mq_multi_bridge::endpoints::memory::get_or_create_channel(
            &mq_multi_bridge::config::MemoryConfig {
                topic: "in-kafka".to_string(),
                ..Default::default()
            },
        );
        let out_kafka = mq_multi_bridge::endpoints::memory::get_or_create_channel(
            &mq_multi_bridge::config::MemoryConfig {
                topic: "out-kafka".to_string(),
                ..Default::default()
            },
        );
        // ... create channels for nats, amqp, mqtt as needed by config.all.yml

        // Send messages to the input channel for the kafka route
        in_kafka.fill_messages(messages_to_send).await.unwrap();

        // Wait for processing
        tokio::time::sleep(Duration::from_secs(15)).await;
        if shutdown_tx.send(()).is_err() {
            println!("WARN: Could not send shutdown signal, bridge may have already stopped.");
        }

        let output_channels = [
            ("kafka", out_kafka),
            // Add other output channels here
        ];

        // Verify all output files
        for (broker_name, channel) in &output_channels {
            println!("Verifying output for {}...", broker_name);
            let received_ids = read_and_drain_memory_channel(channel);
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
    })
    .await;
}
