#![allow(dead_code)]
mod common;

use common::{
    measure_read_performance, measure_write_performance, run_performance_pipeline_test,
    run_pipeline_test, DockerCompose, PERF_TEST_CONCURRENCY, PERF_TEST_MESSAGE_COUNT,
};
use ctor::{ctor, dtor};
use mq_multi_bridge::endpoints::amqp::{AmqpConsumer, AmqpPublisher};
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
        docker: DockerCompose::new("tests/docker-compose.amqp.yml"),
    };
    manager.docker.up();
    manager
};

#[dtor]
fn shutdown() {
    TEST_MANAGER.docker.down();
}

#[tokio::test]
async fn test_amqp_pipeline() {
    run_pipeline_test("AMQP", "tests/config.amqp").await;
}

#[tokio::test]
async fn test_amqp_performance_pipeline() {
    run_performance_pipeline_test("AMQP", "tests/config.amqp", PERF_TEST_MESSAGE_COUNT).await;
}

#[tokio::test]
async fn test_amqp_performance_direct() {
    let queue = "perf_test_amqp_direct";
    let config = mq_multi_bridge::config::AmqpConfig {
        url: "amqp://guest:guest@localhost:5672/%2f".to_string(),
        await_ack: false,
        ..Default::default()
    };

    let publisher = Arc::new(AmqpPublisher::new(&config, queue).await.unwrap());
    measure_write_performance(
        "AMQP",
        publisher,
        PERF_TEST_MESSAGE_COUNT_DIRECT,
        PERF_TEST_CONCURRENCY,
    )
    .await;

    tokio::time::sleep(Duration::from_secs(10)).await;

    let consumer = Arc::new(Mutex::new(AmqpConsumer::new(&config, queue).await.unwrap()));
    measure_read_performance("AMQP", consumer, PERF_TEST_MESSAGE_COUNT_DIRECT).await;
}
