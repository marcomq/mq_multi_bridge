# --- Builder Stage ---
FROM rust:1.78 as builder

WORKDIR /usr/src/mq_multi_bridge

COPY . .

# Build the application in release mode
RUN cargo build --release

# --- Final Stage ---
FROM debian:bullseye-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy the built binary from the builder stage
COPY --from=builder /usr/src/mq_multi_bridge/target/release/mq_multi_bridge /usr/local/bin/mq_multi_bridge

# Set the working directory
WORKDIR /

CMD ["mq_multi_bridge"]