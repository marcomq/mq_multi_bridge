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

# Define named connections to message brokers
# Connections are a list, where each item has a unique 'name'.
connections:
  - name: "kafka_us_east"
    kafka:
      brokers: "kafka-us.example.com:9092"
      group_id: "bridge-group-us"
  - name: "kafka_eu_west"
    kafka:
      brokers: "kafka-eu.example.com:9092"
      group_id: "bridge-group-eu"
  - name: "nats_main"
    nats:
      url: "nats://nats.example.com:4222"
  - name: "rabbitmq_prod"
    amqp:
      url: "amqp://user:pass@rabbitmq.example.com:5672"
  - name: "mqtt_iot"
    mqtt:
      url: "tcp://mqtt.example.com:1883"
      client_id: "bridge-iot-client"
  - name: "http_api"
    http:
      listen_address: "0.0.0.0:8080" # For source
      url: "http://localhost:9999/default" # Default for sink
  - name: "input_file"
    file:
      path: "/var/data/input.log"
  - name: "output_file"
    file:
      path: "/var/data/output.log"

# Dead-Letter Queue (DLQ) configuration
dlq:
  connection: "kafka_us_east" # Use one of the defined connections
  kafka:
    topic: "my-bridge-dlq"

# Define bridge routes from a source to a sink
routes:
  - name: "kafka_us_to_nats_events"
    source:
      connection: "kafka_us_east"
      kafka: # Topic is optional, defaults to route name: "kafka_us_to_nats_events"
    sink:
      connection: "nats_main"
      nats: # Subject is optional, defaults to route name
  - name: "amqp_to_kafka_eu_orders"
    source:
      connection: "rabbitmq_prod"
      amqp: # Queue is optional, defaults to route name
    sink:
      connection: "kafka_eu_west"
      kafka: # Topic is optional, defaults to route name
  - name: "nats_to_mqtt_iot"
    source:
      connection: "nats_main"
      nats:
        subject: "processed.events"
    sink:
      connection: "mqtt_iot"
      mqtt: # Topic is optional, defaults to route name
  - name: "webhook_to_kafka"
    source:
      connection: "http_api"
      http: {} # No config needed for source
    sink:
      connection: "kafka_eu_west"
      kafka: {} # Topic defaults to "webhook_to_kafka"
  - name: "kafka_to_external_api"
    source:
      connection: "kafka_eu_west"
      kafka:
        topic: "outgoing.events"
    sink:
      connection: "http_api"
      http:
        url: "https://api.example.com/ingest" # Override default URL
  - name: "file_to_kafka"
    source:
      connection: "input_file"
      file: {} # No extra config needed for file source
    sink:
      connection: "kafka_eu_west"
      kafka:
        topic: "from_file"
```

### Environment Variables

All configuration parameters can be set via environment variables. This is particularly useful for containerized deployments (e.g., Docker, Kubernetes).

- The variables must be prefixed with `MQ_MULTI_BRIDGE_`.
- Nested keys are separated by a double underscore `__`.
- Arrays (like `routes`) are configured using array indexing for each field.

**Example using environment variables:**

```bash
# General settings
export BRIDGE_LOG_LEVEL="info"
export BRIDGE_LOGGGER="json"

# Metrics
export BRIDGE_METRICS__ENABLED=true
export BRIDGE_METRICS__LISTEN_ADDRESS="0.0.0.0:9090"

# Connection: kafka_us_east
export BRIDGE_CONNECTIONS__0__NAME="kafka_us_east"
export BRIDGE_CONNECTIONS__0__KAFKA__BROKERS="kafka-us.example.com:9092"
export BRIDGE_CONNECTIONS__0__KAFKA__GROUP_ID="bridge-group-us"

# Connection: nats_main
export BRIDGE_CONNECTIONS__1__NAME="nats_main"
export BRIDGE_CONNECTIONS__1__NATS__URL="nats://nats.example.com:4222"

# Route 0: kafka_us_east -> nats_main
export BRIDGE_ROUTES__0__NAME="kafka_us_to_nats_events"
export BRIDGE_ROUTES__0__SOURCE__CONNECTION="kafka_us_east"
export BRIDGE_ROUTES__0__SOURCE__KAFKA__TOPIC="raw_events"
export BRIDGE_ROUTES__0__SINK__CONNECTION="nats_main"
export BRIDGE_ROUTES__0__SINK__NATS__SUBJECT="processed.events"
```

### Using a `.env` file

For local development, you can place a `.env` file in the root of the project. The application will automatically load the variables from this file.

## Using as a Library 
Beyond running as a standalone application, mq-multi-bridge can be used as a library to build custom bridging logic directly in your Rust code. This is useful for embedding, creating custom sources/sinks, or dynamically managing routes. 

### 1. Implement a Custom Source or Sink 
You can create your own sources and sinks by implementing the MessageSource and MessageSink traits. 
```rust 
use mq_multi_bridge::sources::{MessageSource, BoxedMessageStream};
use mq_multi_bridge::sinks::MessageSink;
use mq_multi_bridge::model::CanonicalMessage;
use async_trait::async_trait; +use std::any::Any;
use futures::future::BoxFuture;

// A simple in-memory sink for demonstration 
pub struct MyCustomSink {
  // ... internal state 
}
#[async_trait]
impl MessageSink for MyCustomSink {
  async fn send(&self, message: CanonicalMessage) -> anyhow::Result<Option> {
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
```rust
use mq_multi_bridge::Bridge;
use mq_multi_bridge::config::{load_config, SourceEndpoint, SinkEndpoint, SourceEndpointType, SinkEndpointType}; +use std::sync::Arc;

// Assuming MyCustomSink from the example above
// use crate::my_sinks::MyCustomSink;
async fn run_custom_bridge() -> anyhow::Result<()> {
  // Load base configuration from file/env
  let config = load_config()?;
  let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(());
  // Create a bridge
  let mut bridge = Bridge::from_config(config, shutdown_rx)?;
  // Initialize all connections defined in config.yml
  bridge.initialize_connections().await?;
  // Add a custom sink programmatically
  bridge.add_sink("my_custom_sink", Arc::new(MyCustomSink { /* ... */ }));
  // Create a route from a config-defined source to our custom sink
  let source_endpoint = SourceEndpoint { connection: "kafka_us_east".to_string(), endpoint_type: SourceEndpointType::Kafka(Default::default()) };
  let sink_endpoint = SinkEndpoint { connection: "my_custom_sink".to_string(), endpoint_type: SinkEndpointType::File(Default::default()) }; // Endpoint type for custom sink doesn't matter
  bridge.add_route("kafka_to_custom", &source_endpoint, &sink_endpoint).await?;
  // Run the bridge
  bridge.run().await 
}
```


## License

This project is licensed under the MIT License - see the LICENSE file for details.
