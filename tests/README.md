This is supposed to run via `cargo test --test integration_test --features integration-test --release -- --ignored --nocapture --test-threads=1 --show-output`

Alternatively, you can use this to start a test bridge manually from project root:
```bash
docker-compose -f tests/docker-compose.integration.yml up -d
CONFIG_FILE=tests/config.integration.yml cargo run
```