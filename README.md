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
    git clone https://github.com/marcomq/mq_multi_bridge
    cd mq_multi_bridge
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
    source:
      kafka:
        brokers: "kafka-us.example.com:9092"
        group_id: "bridge-group-us"
        # topic is optional, defaults to route name
    sink:
      nats:
        url: "nats://nats.example.com:4222"
        stream: "events"
        # subject is optional, defaults to route name
    dlq:
      brokers: "kafka-dlq.example.com:9092"
      group_id: "bridge-dlq-group"
      topic: "dlq-kafka-us-to-nats"

  amqp_to_kafka_orders:
    source:
      amqp:
        url: "amqp://user:pass@rabbitmq.example.com:5672"
        # queue is optional, defaults to route name
    sink:
      kafka:
        brokers: "kafka-eu.example.com:9092"
        group_id: "bridge-group-eu"
        # topic is optional, defaults to route name

  webhook_to_kafka:
    source:
      http:
        listen_address: "0.0.0.0:8080"
    sink:
      kafka:
        brokers: "kafka-eu.example.com:9092"
        group_id: "bridge-group-eu"
        # topic defaults to "webhook_to_kafka"

  kafka_to_url:
    source:
      kafka:
        brokers: "kafka-eu.example.com:9092"
        group_id: "bridge-group-eu"
        topic: "outgoing.events"
    sink:
      http:
        url: "https://api.example.com/ingest" # Override default URL

  file_to_kafka:
    source:
      file:
        path: "/var/data/input.log"
    sink:
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
export BRIDGE__ROUTES__MY_KAFKA_TO_NATS__SOURCE__KAFKA__BROKERS="kafka-us.example.com:9092"
export BRIDGE__ROUTES__MY_KAFKA_TO_NATS__SOURCE__KAFKA__GROUP_ID="bridge-group-us"
export BRIDGE__ROUTES__MY_KAFKA_TO_NATS__SOURCE__KAFKA__TOPIC="raw_events" # topic is optional

export BRIDGE__ROUTES__MY_KAFKA_TO_NATS__SINK__NATS__SUBJECT="processed.events"
export BRIDGE__ROUTES__MY_KAFKA_TO_NATS__SINK__NATS__URL="nats://nats.example.com:4222"
export BRIDGE__ROUTES__MY_KAFKA_TO_NATS__SINK__NATS__STREAM="events"

# DLQ for Route 'kafka_us_to_nats_events'
export BRIDGE__ROUTES__MY_KAFKA_TO_NATS__DLQ__KAFKA__BROKERS="kafka-dlq.example.com:9092"
export BRIDGE__ROUTES__MY_KAFKA_TO_NATS__DLQ__KAFKA__GROUP_ID="bridge-dlq-group"
export BRIDGE__ROUTES__MY_KAFKA_TO_NATS__DLQ__KAFKA__TOPIC="dlq-kafka-us-to-nats"
```

### Using a `.env` file

For local development, you can place a `.env` file in the root of the project. The application will automatically load the variables from this file.

## Using as a Library 
Beyond running as a standalone application, mq-multi-bridge can be used as a library to build custom bridging logic directly in your Rust code. This is useful for embedding, creating custom sources/sinks, or dynamically managing routes. 

### 1. Implement a Custom Source or Sink 
You can create your own sources and sinks by implementing the MessageSource and MessageSink traits. 
```rust 
use mq_multi_bridge::sinks::MessageSink;
use mq_multi_bridge::model::CanonicalMessage;
use async_trait::async_trait;
use std::any::Any;

// A simple in-memory sink for demonstration 
#[derive(Clone)]
pub struct MyCustomSink {
  // ... internal state 
}
#[async_trait]
impl MessageSink for MyCustomSink {
  async fn send(&self, message: CanonicalMessage) -> anyhow::Result<Option<CanonicalMessage>> {
    println!("Custom sink received message: {:?}", message);
    // Return Ok(None) for one-way sinks
    Ok(None)
  }
  fn as_any(&self) -> &dyn Any {
    self
  }
}
```

### 2. Build a Bridge Programmatically 
Use the Bridge struct to configure your application. You can mix and match connections from your config.yml with sources and sinks you create in code.
```rust,ignore
use mq_multi_bridge::Bridge;
use mq_multi_bridge::config::{load_config, Config};
use mq_multi_bridge::sources::MessageSource;
use mq_multi_bridge::http::HttpSource; // Using a built-in source for the example
use std::sync::Arc;

// Assuming MyCustomSink from the example above
// use crate::my_sinks::MyCustomSink;
async fn run_custom_bridge() -> anyhow::Result<()> {
  // Load base configuration from file/env
  let config: Config = load_config()?;
  let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
  
  // Create a bridge
  let mut bridge = Bridge::from_config(config, shutdown_rx)?;
  
  // Initialize routes from config
  bridge.initialize_from_config().await?;
  
  // Create an instance of a source (can be custom or built-in)
  let source_config = mq_multi_bridge::config::HttpConfig { 
      listen_address: Some("0.0.0.0:9000".to_string()), 
      ..Default::default() 
  };
  let my_source: Arc<dyn MessageSource> = Arc::new(HttpSource::new(&source_config).await?);
  
  // Create an instance of your custom sink
  let my_sink = Arc::new(MyCustomSink { /* ... */ });
  
  // Add the custom route to the bridge
  bridge.add_custom_route("http-to-custom", my_source, my_sink, None).await?;

  // Run the bridge
  bridge.run().await 
}
```


## License

This project is licensed under the MIT License - see the LICENSE file for details.
