#![allow(dead_code)]
 use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use super::common::{
    measure_read_performance, measure_write_performance, run_performance_pipeline_test,
    run_pipeline_test, run_test_with_docker, setup_logging, PERF_TEST_CONCURRENCY,
    PERF_TEST_MESSAGE_COUNT,
};
use mq_multi_bridge::endpoints::nats::{NatsConsumer, NatsPublisher};
const PERF_TEST_MESSAGE_COUNT_DIRECT: usize = 20_000;

pub async fn test_nats_pipeline() {
    setup_logging();
    run_test_with_docker("tests/integration/docker-compose.nats.yml", || async {
        run_pipeline_test("NATS", "tests/config.nats").await;
    })
    .await;
}

pub async fn test_nats_performance_pipeline() {
    setup_logging();
    run_test_with_docker("tests/integration/docker-compose.nats.yml", || async {
        run_performance_pipeline_test("NATS", "tests/config.nats", PERF_TEST_MESSAGE_COUNT).await;
    })
    .await;
}

pub async fn test_nats_performance_direct() {
    setup_logging();
    run_test_with_docker("tests/integration/docker-compose.nats.yml", || async {
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
    })
    .await;
}
