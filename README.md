# MQ Multi Bridge

A flexible and resilient message queue bridge written in Rust, designed to connect different messaging systems seamlessly like RabbitMQ, Kafka and NATS.

# Status
Current status is work in progress. Don't use it without testing and fixing.

## Features

- **Multiple Broker Support**: Connects Kafka, NATS, AMQP (e.g., RabbitMQ), MQTT, and HTTP in any direction.
- **HTTP Integration**: Expose an HTTP endpoint as a message source (e.g., for webhooks) or use an HTTP endpoint as a sink to call external APIs. Supports request-response patterns.
- **File I/O**: Use local files as a source (reading line-by-line) or a sink (appending messages), perfect for testing and simple logging.
- **Performant**: Built with Tokio for asynchronous, non-blocking I/O.
- **Deduplication**: Prevents processing of duplicate messages within a configurable time window when running in single instance mode.
- **Observable**: Emits logs in JSON format and exposes Prometheus metrics for easy integration with modern monitoring and logging platforms.
- **Configurable**: Easily configured via a file or environment variables.

## Getting Started

### Prerequisites

- Rust toolchain (latest stable version recommended)
- Access to the message brokers you want to connect (e.g., Kafka, NATS, RabbitMQ)


## Build

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/marcomq/mq-bridge-app
    cd mq-bridge-app
    ```

2.  **Configure the application:**
    Create a `config.yaml` file in the project root or set environment variables. See the Configuration section for details.
    
### Build Docker Image (doesn't require local Rust)

1.  **Prerequisites**: Docker and Docker Compose must be installed.

2.  **Start Services**:

    ```bash
    docker-compose up --build
    ```
    

    This will start Kafka, NATS, and the bridge application.

### Building and Running Locally


**Build and run the application:**
    ```bash
    cargo run --release
    ```

## Configuration

The application can be configured in three ways, with the following order of precedence (lower numbers are overridden by higher numbers):

1.  **Default Values**: The application has built-in default values for most settings.
2.  **Configuration File**: A file named `config.[yaml|json|toml]` can be placed in the application's working directory.
3.  **Environment Variables**: Any setting can be overridden using environment variables.

### Configuration File

You can create a configuration file (e.g., `config.yaml`) to specify your settings. This is the recommended approach for managing complex route configurations.

**Example `config.yaml`:**

```yaml
# General settings
log_level: "info"
sled_path: "/var/data/dedup_db"
dedup_ttl_seconds: 86400 # 24 hours

# Metrics configuration
metrics:
  enabled: true
  listen_address: "0.0.0.0:9090"

# Define bridge routes from a source to a sink
routes:
  my_kafka_to_nats:
    in:
      kafka:
        brokers: "kafka-us.example.com:9092"
        group_id: "bridge-group-us"
        # topic is optional, defaults to route name
    out:
      nats:
        url: "nats://nats.example.com:4222"
        stream: "events"
        # subject is optional, defaults to route name
    dlq:
      brokers: "kafka-dlq.example.com:9092"
      group_id: "bridge-dlq-group"
      topic: "dlq-kafka-us-to-nats"

  amqp_to_kafka_orders:
    in:
      amqp:
        url: "amqp://user:pass@rabbitmq.example.com:5672"
        # queue is optional, defaults to route name
    out:
      kafka:
        brokers: "kafka-eu.example.com:9092"
        group_id: "bridge-group-eu"
        # topic is optional, defaults to route name

  webhook_to_kafka:
    in:
      http:
        listen_address: "0.0.0.0:8080"
    out:
      kafka:
        brokers: "kafka-eu.example.com:9092"
        group_id: "bridge-group-eu"
        # topic defaults to "webhook_to_kafka"

  kafka_to_url:
    in:
      kafka:
        brokers: "kafka-eu.example.com:9092"
        group_id: "bridge-group-eu"
        topic: "outgoing.events"
    out:
      http:
        url: "https://api.example.com/ingest" # Override default URL

  file_to_kafka:
    in:
      file:
        path: "/var/data/input.log"
    out:
      kafka:
        brokers: "kafka-eu.example.com:9092"
        group_id: "bridge-group-eu"
        topic: "from_file"
```

### Environment Variables

All configuration parameters can be set via environment variables. This is particularly useful for containerized deployments (e.g., Docker, Kubernetes). The variables must be prefixed with `BRIDGE_`, and nested keys are separated by a double underscore `__`. For map-like structures such as `routes`, the key becomes part of the variable name.

**Example using environment variables:**

```bash
# General settings
export BRIDGE__LOG_LEVEL="info"
export BRIDGE__LOGGER="json"
export BRIDGE__SLED_PATH="/var/data/dedup_db"
export BRIDGE__DEDUP_TTL_SECONDS=86400

# Metrics
export BRIDGE__METRICS__ENABLED=true
export BRIDGE__METRICS__LISTEN_ADDRESS="0.0.0.0:9090"

# Route 'kafka_us_to_nats_events': kafka -> nats
export BRIDGE__ROUTES__MY_KAFKA_TO_NATS__IN__KAFKA__BROKERS="kafka-us.example.com:9092"
export BRIDGE__ROUTES__MY_KAFKA_TO_NATS__IN__KAFKA__GROUP_ID="bridge-group-us"
export BRIDGE__ROUTES__MY_KAFKA_TO_NATS__IN__KAFKA__TOPIC="raw_events" # topic is optional

export BRIDGE__ROUTES__MY_KAFKA_TO_NATS__OUT__NATS__SUBJECT="processed.events"
export BRIDGE__ROUTES__MY_KAFKA_TO_NATS__OUT__NATS__URL="nats://nats.example.com:4222"
export BRIDGE__ROUTES__MY_KAFKA_TO_NATS__OUT__NATS__STREAM="events"

# DLQ for Route 'kafka_us_to_nats_events'
export BRIDGE__ROUTES__MY_KAFKA_TO_NATS__DLQ__KAFKA__BROKERS="kafka-dlq.example.com:9092"
export BRIDGE__ROUTES__MY_KAFKA_TO_NATS__DLQ__KAFKA__GROUP_ID="bridge-dlq-group"
export BRIDGE__ROUTES__MY_KAFKA_TO_NATS__DLQ__KAFKA__TOPIC="dlq-kafka-us-to-nats"
```

### Using a `.env` file

For local development, you can place a `.env` file in the root of the project. The application will automatically load the variables from this file.

## Using as a Library

Beyond running as a standalone application, `mq-multi-bridge` can be used as a library to interact with various message brokers using a unified API. This is useful for building custom applications that need to produce or consume messages without being tied to a specific broker's SDK.

The core of the library are the `MessageConsumer` and `MessagePublisher` traits.

### 1. Publishing Messages

Here's how you can create a publisher and send a message.

```rust,ignore
use mq-bridge-app::{
    config::{KafkaConfig, NatsConfig, AmqpConfig},
    endpoints::{kafka::KafkaPublisher, nats::NatsPublisher, amqp::AmqpPublisher},
    model::CanonicalMessage,
    publishers::MessagePublisher,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // --- Example: Publishing to Kafka ---
    let kafka_config = KafkaConfig {
        brokers: "localhost:9092".to_string(),
        ..Default::default()
    };
    let kafka_publisher = KafkaPublisher::new(&kafka_config, "my-kafka-topic").await?;
    let kafka_message = CanonicalMessage::new(b"Hello, Kafka!".to_vec());
    kafka_publisher.send(kafka_message).await?;
    println!("Message sent to Kafka.");

    // --- Example: Publishing to NATS ---
    let nats_config = NatsConfig {
        url: "nats://localhost:4222".to_string(),
        ..Default::default()
    };
    let nats_publisher = NatsPublisher::new(&nats_config, "my-nats-subject", Some("my-stream")).await?;
    let nats_message = CanonicalMessage::new(b"Hello, NATS!".to_vec());
    nats_publisher.send(nats_message).await?;
    println!("Message sent to NATS.");

    // --- Example: Publishing to AMQP (RabbitMQ) ---
    let amqp_config = AmqpConfig {
        url: "amqp://guest:guest@localhost:5672".to_string(),
        ..Default::default()
    };
    let amqp_publisher = AmqpPublisher::new(&amqp_config, "my-amqp-queue").await?;
    let amqp_message = CanonicalMessage::new(b"Hello, AMQP!".to_vec());
    amqp_publisher.send(amqp_message).await?;
    println!("Message sent to AMQP.");

    Ok(())
}
```

### 2. Consuming Messages

Here's how you can create a consumer and receive messages in a loop.

```rust,ignore
use mq-bridge-app::{
    config::{KafkaConfig, NatsConfig, AmqpConfig},
    endpoints::{kafka::KafkaConsumer, nats::NatsConsumer, amqp::AmqpConsumer},
    consumers::MessageConsumer,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // --- Example: Consuming from Kafka ---
    let kafka_config = KafkaConfig {
        brokers: "localhost:9092".to_string(),
        group_id: Some("my-consumer-group".to_string()),
        ..Default::default()
    };
    let mut kafka_consumer = KafkaConsumer::new(&kafka_config, "my-kafka-topic")?;

    println!("Waiting for Kafka messages...");
    let (message, commit) = kafka_consumer.receive().await?;
    println!("Received from Kafka: {:?}", String::from_utf8_lossy(&message.payload));
    commit(None).await; // Acknowledge the message

    Ok(())
}
```

### 3. Implementing a Custom Publisher

You can extend the bridge with your own logic by implementing the `MessagePublisher` or `MessageConsumer` traits.

```rust,ignore
use mq-bridge-app::{model::CanonicalMessage, publishers::MessagePublisher};
use async_trait::async_trait;
use std::any::Any;

// A simple publisher that just prints messages to the console.
#[async_trait]
impl MessagePublisher for MyCustomConsolePublisher {
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<Option<CanonicalMessage>> {
        println!("Custom publisher received message: {:?}", message);
        Ok(None)
    }
    fn as_any(&self) -> &dyn Any { self }
}
```


## License

This project is licensed under the MIT License - see the LICENSE file for details.
