use std::{collections::HashMap, path::Path};

use anyhow::Result;
use config::{Config, ConfigError};

use mq_bridge::{Route};

fn default_log_level() -> String {
    "info".to_string()
}

#[derive(Debug, serde::Deserialize)]
pub struct AppConfig {
    #[serde(default = "default_log_level")]
    pub log_level: String,
    #[serde(default)]
    pub logger: String,
    #[serde(default)]
    pub metrics_addr: String,
    #[serde(default)]
    pub routes: HashMap<String, Route>,
}

pub fn load_config() -> Result<AppConfig, ConfigError> {
    // Attempt to load .env file
    dotenvy::dotenv().ok();
    let config_file = std::env::var("CONFIG_FILE").unwrap_or_else(|_| "config.yml".to_string());

    let settings = Config::builder()
        // Start with default values
        .set_default("log_level", "info")?
        // Load from a configuration file, if it exists.
        .add_source(config::File::from(Path::new(&config_file)).required(false))
        // Load from environment variables, which will override file and defaults.
        .add_source(
            config::Environment::default()
                .prefix("MQB")
                .separator("__")
                .ignore_empty(true)
                .try_parsing(true),
        )
        .build()?;
    settings.try_deserialize()
}

#[allow(unused_imports)]
mod tests {
    use super::*;

    #[test]
    fn test_config_deserialization() {
        let yaml_config = r#"
log_level: debug
logger: plain
routes:
  kafka_to_nats:
    input:
      kafka:
        brokers: "kafka:9092"
        group_id: "bridge_group"
        topic: "in_topic"
    output:
      nats:
        url: "nats://nats:4222"
        subject: "out_subject"
"#;

        let config: Result<AppConfig, _> = serde_yaml_ng::from_str(yaml_config);
        dbg!(&config);
        assert!(config.is_ok());
        let config = config.unwrap();

        assert_eq!(config.log_level, "debug");
        assert_eq!(config.routes.len(), 1);

        let route = &config.routes["kafka_to_nats"];
        if let mq_bridge::models::EndpointType::Kafka(k) = &route.input.endpoint_type {
            assert_eq!(k.config.brokers, "kafka:9092");
            assert_eq!(k.topic.as_deref(), Some("in_topic"));
        }
    }
    #[test]
    fn test_config_from_env_vars() {
        // Set environment variables
        // Clear the var first to avoid interference from other tests
        std::env::remove_var("MQB__LOG_LEVEL");
        std::env::set_var("MQB__LOG_LEVEL", "trace");
        std::env::set_var("MQB__LOGGER", "json");

        // Route 0: Kafka to NATS
        std::env::set_var(
            "MQB__ROUTES__KAFKA_TO_NATS_FROM_ENV__INPUT__KAFKA__BROKERS",
            "env-kafka:9092",
        );
        // Source
        std::env::set_var(
            "MQB__ROUTES__KAFKA_TO_NATS_FROM_ENV__INPUT__KAFKA__GROUP_ID",
            "env-group",
        );
        std::env::set_var(
            "MQB__ROUTES__KAFKA_TO_NATS_FROM_ENV__INPUT__KAFKA__TOPIC",
            "env-in-topic",
        );
        // Sink
        std::env::set_var(
            "MQB__ROUTES__KAFKA_TO_NATS_FROM_ENV__OUTPUT__NATS__URL",
            "nats://env-nats:4222",
        );
        std::env::set_var(
            "MQB__ROUTES__KAFKA_TO_NATS_FROM_ENV__OUTPUT__NATS__SUBJECT",
            "env-out-subject",
        );

        std::env::set_var("CONFIG_FILE", "");
        // Load config
        let config = load_config().unwrap();

        // Assertions
        dbg!(&config.routes);
        assert_eq!(config.log_level, "trace");
        assert_eq!(config.logger, "json");
        assert_eq!(config.routes.len(), 1);

        let (name, route) = config.routes.iter().next().unwrap();
        assert_eq!(name, "kafka_to_nats_from_env");

        // Assert source
        if let mq_bridge::models::EndpointType::Kafka(k) = &route.input.endpoint_type {
            assert_eq!(k.config.brokers, "env-kafka:9092"); // group_id is now optional
            assert_eq!(k.topic.as_deref(), Some("env-in-topic"));
        } else {
            panic!("Expected Kafka source endpoint");
        }
    }
}
