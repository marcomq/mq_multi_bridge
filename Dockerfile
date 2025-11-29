# --- Builder Stage ---
FROM rust:1.90-bookworm AS builder

WORKDIR /usr/src/mq_multi_bridge

# Install build dependencies
RUN apt-get update && apt-get install -y pkg-config gcc libssl-dev zlib1g-dev zlib1g

# Copy only the necessary files to cache dependencies
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Build the application in release mode
RUN cargo build --release
# Strip the binary to reduce its size
RUN strip /usr/src/mq_multi_bridge/target/release/mq_multi_bridge

# --- Final Stage ---
FROM gcr.io/distroless/cc-debian12 AS final

# Copy the built binary from the builder stage
COPY --from=builder /usr/src/mq_multi_bridge/target/release/mq_multi_bridge /usr/local/bin/mq_multi_bridge
# Copy the required shared library from the builder stage.
# The wildcard '*' handles different architecture paths (e.g., x86_64-linux-gnu, aarch64-linux-gnu).
COPY --from=builder /usr/lib/*-linux-gnu/libz.so.* /lib/

CMD ["mq_multi_bridge"]