use std::path::Path;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq, Hash)]
pub struct KafkaConfig {
    pub brokers: String,
    pub group_id: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    #[serde(default = "default_true")]
    pub await_ack: bool,
    #[serde(default)]
    pub tls: ClientTlsConfig,
    #[serde(default)]
    pub producer_options: Option<Vec<(String, String)>>,
    #[serde(default)]
    pub consumer_options: Option<Vec<(String, String)>>,
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq, Hash)]
pub struct TlsConfig {
    #[serde(default)]
    pub required: bool,
    pub ca_file: Option<String>,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,      // For PEM keys
    pub cert_password: Option<String>, // For PKCS12 certs in AMQP
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq, Hash)]
pub struct ClientTlsConfig {
    #[serde(default)]
    pub required: bool,
    pub ca_file: Option<String>,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    pub cert_password: Option<String>,
    #[serde(default)]
    pub accept_invalid_certs: bool,
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq, Hash)]
pub struct NatsConfig {
    pub url: String,
    pub username: Option<String>,
    pub password: Option<String>,
    #[serde(default = "default_true")]
    pub await_ack: bool,
    pub token: Option<String>,
    pub default_stream: Option<String>,
    #[serde(flatten, default)]
    pub tls: ClientTlsConfig,
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq, Hash)]
pub struct AmqpConfig {
    pub url: String,
    pub username: Option<String>,
    pub password: Option<String>,
    #[serde(default = "default_true")]
    pub await_ack: bool,
    #[serde(flatten, default)]
    pub tls: ClientTlsConfig,
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq, Hash)]
pub struct MqttConfig {
    pub url: String,
    #[serde(flatten, default)]
    pub tls: ClientTlsConfig,
    pub username: Option<String>,
    pub password: Option<String>,
    // The capacity of the internal message queue for the MQTT client.
    pub queue_capacity: Option<usize>,
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq, Hash)]
pub struct FileConfig {
    pub path: String,
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq, Hash)]
pub struct HttpConfig {
    pub listen_address: Option<String>,
    pub url: Option<String>,
    pub response_sink: Option<String>,
    #[serde(flatten, default)]
    pub tls: TlsConfig, // Server-side TLS does not use accept_invalid_certs
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq, Hash)]
pub struct MemoryConfig {
    pub topic: String,
    pub capacity: Option<usize>,
}

impl TlsConfig {
    /// Checks if client-side mTLS is configured.
    pub fn is_mtls_client_configured(&self) -> bool {
        self.required && self.cert_file.is_some()
    }

    /// Checks if server-side TLS is configured.
    pub fn is_tls_server_configured(&self) -> bool {
        self.required && self.cert_file.is_some() && self.key_file.is_some() // Server needs both cert and key
    }
}

impl ClientTlsConfig {
    pub fn is_mtls_client_configured(&self) -> bool {
        self.required && self.cert_file.is_some()
    }
}
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionType {
    Kafka(KafkaConfig),
    Nats(NatsConfig),
    Amqp(AmqpConfig),
    Mqtt(MqttConfig),
    File(FileConfig),
    Http(HttpConfig),
    Static(StaticEndpoint),
    Memory(MemoryConfig),
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct KafkaEndpoint {
    pub topic: Option<String>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct NatsEndpoint {
    pub subject: Option<String>,
    pub stream: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Hash)]
pub struct AmqpEndpoint {
    pub queue: Option<String>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct MqttEndpoint {
    pub topic: Option<String>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct FileEndpoint {}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct HttpEndpoint {
    pub url: Option<String>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct StaticEndpoint {
    #[serde(default = "default_static_response_content")]
    pub content: String,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct MemoryEndpoint {}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct PublisherEndpoint {
    // pub connection: String,
    #[serde(flatten)]
    pub endpoint_type: PublisherEndpointType,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct ConsumerEndpoint {
    // pub connection: String,
    #[serde(flatten)]
    pub endpoint_type: ConsumerEndpointType,
}

pub fn load_config() -> Result<Config, config::ConfigError> {
    // Attempt to load .env file
    dotenvy::dotenv().ok();
    let config_file = std::env::var("CONFIG_FILE").unwrap_or_else(|_| "config.yml".to_string());

    let settings = config::Config::builder()
        // Start with default values
        .set_default("log_level", "info")?
        .set_default("sled_path", "/tmp/dedup_db")?
        .set_default("dedup_ttl_seconds", 86400)?
        // Load from a configuration file, if it exists.
        .add_source(config::File::from(Path::new(&config_file)).required(false))
        // Load from environment variables, which will override file and defaults.
        .add_source(
            config::Environment::default()
                .prefix("BRIDGE")
                .separator("__")
                .ignore_empty(true)
                .try_parsing(true),
        )
        .build()?;
    settings.try_deserialize()
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum PublisherEndpointType {
    Kafka(KafkaPublisherEndpoint),
    Nats(NatsPublisherEndpoint),
    Amqp(AmqpPublisherEndpoint),
    Mqtt(MqttPublisherEndpoint),
    File(FilePublisherEndpoint),
    Http(HttpPublisherEndpoint),
    Static(StaticEndpoint),
    Memory(MemoryPublisherEndpoint),
}

#[derive(Debug, Deserialize, Clone)]
pub struct Route {
    pub r#in: ConsumerEndpoint,
    pub out: PublisherEndpoint,
    pub dlq: Option<DlqConfig>,
    pub concurrency: Option<usize>,
    #[serde(default = "default_true")]
    pub deduplication_enabled: bool,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum ConsumerEndpointType {
    Kafka(KafkaConsumerEndpoint),
    Nats(NatsConsumerEndpoint),
    Amqp(AmqpConsumerEndpoint),
    Mqtt(MqttConsumerEndpoint),
    File(FileConsumerEndpoint),
    Http(HttpConsumerEndpoint),
    Static(StaticEndpoint),
    Memory(MemoryConsumerEndpoint),
}
fn default_static_response_content() -> String {
    "OK".to_string()
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    #[serde(default)]
    pub logger: String,
    pub log_level: String,
    pub sled_path: String,
    pub dedup_ttl_seconds: u64,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub routes: HashMap<String, Route>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            logger: "plain".to_string(),
            log_level: "info".to_string(),
            sled_path: "/tmp/dedup_db".to_string(),
            dedup_ttl_seconds: 180,
            metrics: MetricsConfig::default(),
            routes: HashMap::new(),
        }
    }
}
#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct KafkaConsumerEndpoint {
    #[serde(flatten)]
    pub config: KafkaConfig,
    #[serde(flatten)]
    pub endpoint: KafkaEndpoint,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct NatsConsumerEndpoint {
    #[serde(flatten)]
    pub config: NatsConfig,
    #[serde(flatten)]
    pub endpoint: NatsEndpoint,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct AmqpConsumerEndpoint {
    #[serde(flatten)]
    pub config: AmqpConfig,
    #[serde(flatten)]
    pub endpoint: AmqpEndpoint,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct MqttConsumerEndpoint {
    #[serde(flatten)]
    pub config: MqttConfig,
    #[serde(flatten)]
    pub endpoint: MqttEndpoint,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct FileConsumerEndpoint {
    #[serde(flatten)]
    pub config: FileConfig,
    #[serde(flatten)]
    pub endpoint: FileEndpoint,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct HttpConsumerEndpoint {
    #[serde(flatten)]
    pub config: HttpConfig,
    #[serde(flatten)]
    pub endpoint: HttpEndpoint,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct MemoryConsumerEndpoint {
    #[serde(flatten)]
    pub config: MemoryConfig,
    #[serde(flatten)]
    pub endpoint: MemoryEndpoint,
}

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaPublisherEndpoint {
    #[serde(flatten)]
    pub config: KafkaConfig,
    #[serde(flatten)]
    pub endpoint: KafkaEndpoint,
}

#[derive(Debug, Deserialize, Clone)]
pub struct NatsPublisherEndpoint {
    #[serde(flatten)]
    pub config: NatsConfig,
    #[serde(flatten)]
    pub endpoint: NatsEndpoint,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AmqpPublisherEndpoint {
    #[serde(flatten)]
    pub config: AmqpConfig,
    #[serde(flatten)]
    pub endpoint: AmqpEndpoint,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MqttPublisherEndpoint {
    #[serde(flatten)]
    pub config: MqttConfig,
    #[serde(flatten)]
    pub endpoint: MqttEndpoint,
}

#[derive(Debug, Deserialize, Clone)]
pub struct FilePublisherEndpoint {
    #[serde(flatten)]
    pub config: FileConfig,
    #[serde(flatten)]
    pub endpoint: FileEndpoint,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HttpPublisherEndpoint {
    #[serde(flatten)]
    pub config: HttpConfig,
    #[serde(flatten)]
    pub endpoint: HttpEndpoint,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MemoryPublisherEndpoint {
    #[serde(flatten)]
    pub config: MemoryConfig,
    #[serde(flatten)]
    pub endpoint: MemoryEndpoint,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub listen_address: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            listen_address: "0.0.0.0:9090".to_string(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct DlqKafkaEndpoint {
    pub topic: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DlqConfig {
    // pub connection: String,
    #[serde(flatten)]
    pub kafka: KafkaPublisherEndpoint,
}

#[allow(unused_imports)]
mod tests {
    use super::*;

    #[test]
    fn test_config_deserialization() {
        let yaml_config = r#"
log_level: "debug"
sled_path: "/tmp/test_db"
dedup_ttl_seconds: 3600

metrics:
  enabled: true
  listen_address: "0.0.0.0:9191"

routes:
  kafka_to_nats:
    in:
      kafka:
        brokers: "kafka:9092"
        group_id: "bridge_group"
        topic: "in_topic"
    out:
      nats:
        url: "nats://nats:4222"
        subject: "out_subject"
    dlq:
      brokers: "kafka:9092"
      group_id: "bridge_group_dlq"
      topic: "my_dlq"
"#;

        let config: Result<Config, _> = serde_yaml::from_str(yaml_config);
        dbg!(&config);
        assert!(config.is_ok());
        let config = config.unwrap();

        assert_eq!(config.log_level, "debug");
        assert_eq!(config.routes.len(), 1);

        let route = &config.routes["kafka_to_nats"];
        assert!(route.dlq.is_some());
        if let ConsumerEndpointType::Kafka(k) = &route.r#in.endpoint_type {
            assert_eq!(k.config.brokers, "kafka:9092");
            assert_eq!(k.endpoint.topic.as_deref(), Some("in_topic"));
        }
    }
    #[test]
    fn test_config_from_env_vars() {
        // Set environment variables
        // Clear the var first to avoid interference from other tests
        std::env::remove_var("BRIDGE__LOG_LEVEL");
        std::env::set_var("BRIDGE__LOG_LEVEL", "trace");
        std::env::set_var("BRIDGE__LOGGER", "json");
        std::env::set_var("BRIDGE__SLED_PATH", "/tmp/env_test_db");
        std::env::set_var("BRIDGE__DEDUP_TTL_SECONDS", "300");

        // Route 0: Kafka to NATS
        std::env::set_var(
            "BRIDGE__ROUTES__KAFKA_TO_NATS_FROM_ENV__IN__KAFKA__BROKERS",
            "env-kafka:9092",
        );
        // Source
        std::env::set_var(
            "BRIDGE__ROUTES__KAFKA_TO_NATS_FROM_ENV__IN__KAFKA__GROUP_ID",
            "env-group",
        );
        std::env::set_var(
            "BRIDGE__ROUTES__KAFKA_TO_NATS_FROM_ENV__IN__KAFKA__TOPIC",
            "env-in-topic",
        );
        // Sink
        std::env::set_var(
            "BRIDGE__ROUTES__KAFKA_TO_NATS_FROM_ENV__OUT__NATS__URL",
            "nats://env-nats:4222",
        );
        std::env::set_var(
            "BRIDGE__ROUTES__KAFKA_TO_NATS_FROM_ENV__OUT__NATS__SUBJECT",
            "env-out-subject",
        );
        // DLQ
        std::env::set_var(
            "BRIDGE__ROUTES__KAFKA_TO_NATS_FROM_ENV__DLQ__BROKERS",
            "env-dlq-kafka:9092",
        );
        std::env::set_var(
            "BRIDGE__ROUTES__KAFKA_TO_NATS_FROM_ENV__DLQ__GROUP_ID",
            "env-dlq-group",
        );
        std::env::set_var(
            "BRIDGE__ROUTES__KAFKA_TO_NATS_FROM_ENV__DLQ__TOPIC",
            "env-dlq-topic",
        );

        // Load config
        let config = load_config().unwrap();

        // Assertions
        assert_eq!(config.log_level, "trace");
        assert_eq!(config.logger, "json");
        assert_eq!(config.sled_path, "/tmp/env_test_db");
        assert_eq!(config.dedup_ttl_seconds, 300);
        assert_eq!(config.routes.len(), 1);

        let (name, route) = config.routes.iter().next().unwrap();
        assert_eq!(name, "kafka_to_nats_from_env");

        // Assert source
        if let ConsumerEndpointType::Kafka(k) = &route.r#in.endpoint_type {
            assert_eq!(k.config.brokers, "env-kafka:9092"); // group_id is now optional
            assert_eq!(k.endpoint.topic.as_deref(), Some("env-in-topic"));
        } else {
            panic!("Expected Kafka source endpoint");
        }
    }
}
