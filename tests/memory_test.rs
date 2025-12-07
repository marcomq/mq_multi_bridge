use mq_multi_bridge::config::{
    Config, ConsumerEndpoint, ConsumerEndpointType, MemoryConfig, MemoryConsumerEndpoint,
    MemoryEndpoint, MemoryPublisherEndpoint, PublisherEndpoint, PublisherEndpointType, Route,
};
use mq_multi_bridge::endpoints::memory::get_or_create_channel;
use std::collections::HashSet;
use std::time::{Duration, Instant};

mod integration;

// run in release:
// cargo test --package mq_multi_bridge --test memory_test --features integration-test --release -- test_memory_to_memory_pipeline --exact --nocapture

#[tokio::test]
async fn test_memory_to_memory_pipeline() {
    integration::common::setup_logging();

    let num_messages = 500_000;
    let (messages_to_send, sent_message_ids) =
        integration::common::generate_test_messages(num_messages);

    let mut config = Config::default();
    config.log_level = "info".to_string();
    let in_memory_config = MemoryConfig {
        topic: "mem-in".to_string(),
        capacity: Some(200), // A reasonable capacity for the input channel
    };
    let out_memory_config = MemoryConfig {
        topic: "mem-out".to_string(),
        capacity: Some(num_messages + 10_000), // Ensure output can hold all messages
    };
    config.routes.insert(
        "memory-pipe".to_string(),
        Route {
            r#in: ConsumerEndpoint {
                endpoint_type: ConsumerEndpointType::Memory(MemoryConsumerEndpoint {
                    config: in_memory_config.clone(),
                    endpoint: MemoryEndpoint {},
                }),
            },
            out: PublisherEndpoint {
                endpoint_type: PublisherEndpointType::Memory(MemoryPublisherEndpoint {
                    config: out_memory_config.clone(),
                    endpoint: MemoryEndpoint {},
                }),
            },
            dlq: None,
            concurrency: Some(4),
            deduplication_enabled: false,
        },
    );

    let mut bridge = mq_multi_bridge::Bridge::new(config);
    let bridge_handle = bridge.run();

    let in_channel = get_or_create_channel(&in_memory_config);
    let out_channel = get_or_create_channel(&out_memory_config);

    println!("--- Starting Memory-to-Memory Pipeline Test ---");
    let start_time = Instant::now();

    let fill_task = tokio::spawn(async move {
        in_channel.fill_messages(messages_to_send).await.unwrap();
        in_channel.close();
    });

    // Wait for the bridge to finish processing.
    let _ = tokio::time::timeout(Duration::from_secs(20), bridge_handle)
        .await
        .expect("Test timed out waiting for bridge to complete");

    // Ensure the fill task also completed without error
    fill_task.await.unwrap();

    let duration = start_time.elapsed();
    let received_ids: HashSet<_> = out_channel.drain_messages().into_iter().map(|m| m.message_id.to_string()).collect();

    let msgs_per_sec = num_messages as f64 / duration.as_secs_f64();
    println!("Processed {} messages in {:.2?} ({:.2} msgs/sec)", num_messages, duration, msgs_per_sec);
    println!("-------------------------------------------------");

    assert_eq!(received_ids.len(), num_messages);
    assert_eq!(sent_message_ids, received_ids);
}