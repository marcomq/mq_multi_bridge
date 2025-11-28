# --- Builder Stage ---
FROM rust:1.90-bookworm as builder

WORKDIR /usr/src/mq_multi_bridge

# Install build dependencies
RUN apt-get update && apt-get install -y pkg-config gcc libssl-dev

# Copy only the necessary files to cache dependencies
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Build the application in release mode
RUN cargo build --release

# --- Final Stage ---
# FROM gcr.io/distroless/cc-debian11
FROM debian:bookworm-slim

# Copy the built binary from the builder stage
COPY --from=builder /usr/src/mq_multi_bridge/target/release/mq_multi_bridge /usr/local/bin/mq_multi_bridge

CMD ["mq_multi_bridge"]