#![allow(dead_code)]
mod common;

use common::{
    measure_read_performance, measure_write_performance, run_performance_pipeline_test,
    run_pipeline_test, DockerCompose, PERF_TEST_CONCURRENCY, PERF_TEST_MESSAGE_COUNT,
};
use ctor::{ctor, dtor};
use mq_multi_bridge::endpoints::kafka::{KafkaConsumer, KafkaPublisher};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

const PERF_TEST_MESSAGE_COUNT_DIRECT: usize = 20_000;

struct TestManager {
    docker: DockerCompose,
}

#[ctor]
static TEST_MANAGER: TestManager = {
    common::setup_logging();
    let manager = TestManager {
        docker: DockerCompose::new("tests/docker-compose.kafka.yml"),
    };
    manager.docker.up();
    manager
};

#[dtor]
fn shutdown() {
    TEST_MANAGER.docker.down();
}

#[tokio::test]
async fn test_kafka_pipeline() {
    run_pipeline_test("Kafka", "tests/config.kafka").await;
}

#[tokio::test]
async fn test_kafka_performance_pipeline() {
    run_performance_pipeline_test("Kafka", "tests/config.kafka", PERF_TEST_MESSAGE_COUNT).await;
}

#[tokio::test]
async fn test_kafka_performance_direct() {
    let topic = "perf_test_kafka_direct";
    let config = mq_multi_bridge::config::KafkaConfig {
        brokers: "localhost:9092".to_string(),
        group_id: Some("perf_test_group_kafka".to_string()),
        producer_options: Some(vec![
            ("queue.buffering.max.ms".to_string(), "50".to_string()), // Linger for 50ms to batch messages
            ("acks".to_string(), "1".to_string()), // Wait for leader ack, a good balance
            ("compression.type".to_string(), "snappy".to_string()), // Use snappy compression
        ]),
        await_ack: false, // Use "fire-and-forget" for high throughput
        ..Default::default()
    };

    let publisher = Arc::new(KafkaPublisher::new(&config, topic).await.unwrap());
    measure_write_performance(
        "Kafka",
        publisher,
        PERF_TEST_MESSAGE_COUNT_DIRECT,
        PERF_TEST_CONCURRENCY,
    )
    .await;

    tokio::time::sleep(Duration::from_secs(5)).await;

    let consumer = Arc::new(Mutex::new(KafkaConsumer::new(&config, topic).unwrap()));
    measure_read_performance("Kafka", consumer, PERF_TEST_MESSAGE_COUNT_DIRECT).await;
}
