# MQ Multi Bridge

Bridge between multiple MQ message queues and streams like Kafka and NATS.

This project provides a robust, observable, and resilient message bridge written in Rust.

## Features

- **Kafka to NATS Bridge**: Forwards messages from a Kafka topic to a NATS subject.
- **At-Least-Once Delivery**: Commits/acks a message at the source only after it has been successfully forwarded to the sink.
- **Deduplication**: Uses an embedded `sled` database to prevent processing of duplicate messages.
- **Retry & DLQ**: Implements an exponential backoff retry mechanism. After several failed attempts, messages are moved to a Dead-Letter Queue (DLQ in Kafka).
- **Prometheus Metrics**: Exposes metrics on `/metrics` at port `9090`.
- **Structured Logging**: JSON-formatted logs using `tracing`.
- **Configuration**: Fully configurable via environment variables.

## Docker Build and Image (doesn't require local Rust)

1.  **Prerequisites**: Docker and Docker Compose must be installed.

2.  **Start Services**:

    ```bash
    docker-compose up --build
    ```
    

    This will start Kafka, NATS, and the bridge application.

3.  **Test the Data Flow (Kafka -> NATS)**

    a. **(Optional) Create Kafka Topics**: The bridge expects topics `events-in`, `events-out`, and `events-dlq`. You can create them manually if auto-creation is disabled.

    b. **Produce a message to Kafka**:
    Open a new terminal and send a JSON message to the `events-in` topic.
    ```bash
    docker-compose exec kafka kafka-console-producer --broker-list kafka:9092 --topic events-in
    > {"data": "hello world"}
    ```

    c. **Observe Logs**: You should see logs in the `docker-compose` output indicating that the bridge received the message from Kafka and forwarded it to NATS.

    d. **Check Metrics**: Access `http://localhost:9090/metrics` in your browser to see the Prometheus metrics.


## Local build (without Docker)

You also just use your local rust installation to build and run the binary.

```bash
cargo run --release
```