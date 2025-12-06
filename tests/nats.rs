#![allow(dead_code)]
mod common;

use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use common::{
    measure_read_performance, measure_write_performance, run_performance_pipeline_test,
    run_pipeline_test, DockerCompose, PERF_TEST_CONCURRENCY, PERF_TEST_MESSAGE_COUNT,
};
use ctor::{ctor, dtor};
use mq_multi_bridge::endpoints::nats::{NatsConsumer, NatsPublisher};
const PERF_TEST_MESSAGE_COUNT_DIRECT: usize = 20_000;

struct TestManager {
    docker: DockerCompose,
}

#[ctor]
static TEST_MANAGER: TestManager = {
    common::setup_logging();
    let manager = TestManager {
        docker: DockerCompose::new("tests/docker-compose.nats.yml"),
    };
    manager.docker.up();
    manager
};

#[dtor]
fn shutdown() {
    TEST_MANAGER.docker.down();
}

#[tokio::test]
async fn test_nats_pipeline() {
    run_pipeline_test("NATS", "tests/config.nats").await;
}

#[tokio::test]
async fn test_nats_performance_pipeline() {
    run_performance_pipeline_test("NATS", "tests/config.nats", PERF_TEST_MESSAGE_COUNT).await;
}

#[tokio::test]
async fn test_nats_performance_direct() {
    let stream_name = "perf_stream_nats_direct";
    let subject = format!("{}.direct", stream_name);
    let config = mq_multi_bridge::config::NatsConfig {
        url: "nats://localhost:4222".to_string(),
        await_ack: true,
        ..Default::default()
    };

    let publisher = Arc::new(
        NatsPublisher::new(&config, &subject, Some(stream_name))
            .await
            .unwrap(),
    );
    measure_write_performance(
        "NATS",
        publisher,
        PERF_TEST_MESSAGE_COUNT_DIRECT,
        PERF_TEST_CONCURRENCY,
    )
    .await;

    tokio::time::sleep(Duration::from_secs(5)).await;

    let consumer = Arc::new(Mutex::new(
        NatsConsumer::new(&config, stream_name, &subject)
            .await
            .unwrap(),
    ));
    measure_read_performance("NATS", consumer, PERF_TEST_MESSAGE_COUNT_DIRECT).await;
}
