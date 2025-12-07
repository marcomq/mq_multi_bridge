#![allow(dead_code)]
mod common;

use common::{
    measure_read_performance, measure_write_performance, run_performance_pipeline_test,
    run_pipeline_test, DockerCompose, PERF_TEST_CONCURRENCY, PERF_TEST_MESSAGE_COUNT,
};
use ctor::{ctor, dtor};
use mq_multi_bridge::endpoints::mqtt::{MqttConsumer, MqttPublisher};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use uuid::Uuid;

const PERF_TEST_MESSAGE_COUNT_DIRECT: usize = 20_000;

struct TestManager {
    docker: DockerCompose,
}

#[ctor]
static TEST_MANAGER: TestManager = {
    common::setup_logging();
    let manager = TestManager {
        docker: DockerCompose::new("tests/docker-compose.mqtt.yml"),
    };
    manager.docker.up();
    manager
};

#[dtor]
fn shutdown() {
    TEST_MANAGER.docker.down();
}

#[tokio::test]
async fn test_mqtt_pipeline() {
    let test_name = format!("MQTT-{}", Uuid::new_v4().as_simple());
    run_pipeline_test(&test_name, "tests/config.mqtt").await;
}

#[tokio::test]
async fn test_mqtt_performance_pipeline() {
    let test_name = format!("MQTT-Perf-{}", Uuid::new_v4().as_simple());
    run_performance_pipeline_test(&test_name, "tests/config.mqtt", PERF_TEST_MESSAGE_COUNT).await;
}

#[tokio::test]
async fn test_mqtt_performance_direct() {
    let unique_id = Uuid::new_v4().as_simple().to_string();
    let topic = "test_topic_mqtt/direct";
    let publisher_id = format!("perftest-pub-{}", unique_id);
    let consumer_id = format!("perftest-sub-{}", unique_id);
    let config = mq_multi_bridge::config::MqttConfig {
        url: "mqtt://localhost:1883".to_string(),
        // Increase the client's incoming message buffer to hold all messages from the test run.
        queue_capacity: Some(PERF_TEST_MESSAGE_COUNT_DIRECT),
        ..Default::default()
    };

    // Create the consumer and subscribe before publishing messages.
    let consumer = Arc::new(Mutex::new(
        MqttConsumer::new(&config, topic, &consumer_id)
            .await
            .unwrap(),
    ));

    // Give the consumer a moment to connect and subscribe.
    // This helps ensure the subscription is active before messages are published.
    tokio::time::sleep(Duration::from_millis(100)).await;

    let publisher = Arc::new(
        MqttPublisher::new(&config, topic, &publisher_id)
            .await
            .unwrap(),
    );
    measure_write_performance(
        "MQTT",
        publisher.clone(),
        PERF_TEST_MESSAGE_COUNT_DIRECT,
        PERF_TEST_CONCURRENCY,
    )
    .await;

    // Explicitly disconnect the publisher to release the client ID and connection gracefully.
    // It's possible the publisher is already disconnected due to errors during the
    // performance test, so we don't unwrap the result.
    // let _ = publisher.disconnect().await;

    // Give the broker a moment to process the backlog of messages before the consumer connects.
    // This helps prevent the broker from overwhelming the new consumer and dropping the connection.
    tokio::time::sleep(Duration::from_secs(3)).await;

    measure_read_performance("MQTT", consumer, PERF_TEST_MESSAGE_COUNT_DIRECT).await;
}
