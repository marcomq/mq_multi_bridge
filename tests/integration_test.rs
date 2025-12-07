// To run these tests, use the command from the project root:
// cargo test --test integration_test -- --ignored --nocapture --test-threads=1

mod integration;

#[tokio::test]
async fn test_all_pipelines_together() {
    integration::all_endpoints::test_all_pipelines_together().await;
}

#[tokio::test]
async fn test_amqp_pipeline() {
    integration::amqp::test_amqp_pipeline().await;
}

#[tokio::test]
async fn test_amqp_performance_pipeline() {
    integration::amqp::test_amqp_performance_pipeline().await;
}

#[tokio::test]
async fn test_amqp_performance_direct() {
    integration::amqp::test_amqp_performance_direct().await;
}

#[tokio::test]
async fn test_kafka_pipeline() {
    integration::kafka::test_kafka_pipeline().await;
}

#[tokio::test]
async fn test_kafka_performance_pipeline() {
    integration::kafka::test_kafka_performance_pipeline().await;
}

#[tokio::test]
async fn test_kafka_performance_direct() {
    integration::kafka::test_kafka_performance_direct().await;
}

#[tokio::test]
async fn test_mqtt_pipeline() {
    integration::mqtt::test_mqtt_pipeline().await;
}

#[tokio::test]
async fn test_mqtt_performance_pipeline() {
    integration::mqtt::test_mqtt_performance_pipeline().await;
}

#[tokio::test]
async fn test_mqtt_performance_direct() {
    integration::mqtt::test_mqtt_performance_direct().await;
}

#[tokio::test]
async fn test_nats_pipeline() {
    integration::nats::test_nats_pipeline().await;
}

#[tokio::test]
async fn test_nats_performance_pipeline() {
    integration::nats::test_nats_performance_pipeline().await;
}

#[tokio::test]
async fn test_nats_performance_direct() {
    integration::nats::test_nats_performance_direct().await;
}