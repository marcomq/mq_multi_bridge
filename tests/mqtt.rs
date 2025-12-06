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
    run_pipeline_test("MQTT", "tests/config.mqtt").await;
}

#[tokio::test]
async fn test_mqtt_performance_pipeline() {
    run_performance_pipeline_test("MQTT", "tests/config.mqtt", PERF_TEST_MESSAGE_COUNT).await;
}

#[tokio::test]
async fn test_mqtt_performance_direct() {
    let unique_id = Uuid::new_v4().as_simple().to_string();
    let topic = "perf_test_mqtt/direct";
    let publisher_id = format!("perftestmqttdirectpub{}", unique_id);
    let consumer_id = format!("perftestmqttdirectsub{}", unique_id);
    let config = mq_multi_bridge::config::MqttConfig {
        url: "mqtt://localhost:1883".to_string(),
        ..Default::default()
    };

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
    let _ = publisher.disconnect().await;

    // Give the broker a moment to process the backlog of messages before the consumer connects.
    // This helps prevent the broker from overwhelming the new consumer and dropping the connection.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let consumer = Arc::new(Mutex::new(
        MqttConsumer::new(&config, topic, &consumer_id)
            .await
            .unwrap(),
    ));
    measure_read_performance("MQTT", consumer, PERF_TEST_MESSAGE_COUNT_DIRECT).await;
}
