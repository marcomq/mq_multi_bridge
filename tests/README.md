This is supposed to run via `cargo test -- --ignored --test-threads=1`

Alternatively, you can use this to start a test bridge manually from project root:
```bash
docker-compose -f tests/docker-compose.integration.yml up -d
CONFIG_FILE=tests/config.integration.yml cargo run
```