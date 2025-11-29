use std::path::Path;

use anyhow::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq, Hash)]
pub struct KafkaConfig {
    pub brokers: String,
    pub group_id: String,
    #[serde(default)]
    pub tls: TlsConfig,
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq, Hash)]
pub struct TlsConfig {
    #[serde(default)]
    pub required: bool,
    pub ca_file: Option<String>,
    pub cert_file: Option<String>,
    pub key_file: Option<String>, // For PEM keys
    pub cert_password: Option<String>,
    #[serde(default)]
    pub accept_invalid_certs: bool,
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq, Hash)]
pub struct NatsConfig {
    pub url: String,
    pub default_stream: Option<String>,
    #[serde(flatten, default)]
    pub tls: TlsConfig,
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq, Hash)]
pub struct AmqpConfig {
    pub url: String,
    #[serde(flatten, default)]
    pub tls: TlsConfig,
}

#[derive(Debug, Deserialize, Clone, Default, PartialEq, Eq, Hash)]
pub struct MqttConfig {
    pub url: String,
    #[serde(flatten, default)]
    pub tls: TlsConfig,
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
    pub tls: TlsConfig,
}

impl TlsConfig {
    /// Checks if client-side mTLS is configured.
    pub fn is_mtls_client_configured(&self) -> bool {
        self.required && self.cert_file.is_some() && self.key_file.is_some()
    }

    /// Checks if server-side TLS is configured.
    pub fn is_tls_server_configured(&self) -> bool {
        self.required && self.cert_file.is_some() && self.key_file.is_some()
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
    StaticResponse(StaticResponseEndpoint),
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
pub struct StaticResponseEndpoint {
    #[serde(default = "default_static_response_content")]
    pub content: String,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub struct SourceEndpoint {
    // pub connection: String,
    #[serde(flatten)]
    pub endpoint_type: SourceEndpointType,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum SourceEndpointType {
    Kafka(KafkaSourceEndpoint),
    Nats(NatsSourceEndpoint),
    Amqp(AmqpSourceEndpoint),
    Mqtt(MqttSourceEndpoint),
    File(FileSourceEndpoint),
    Http(HttpSourceEndpoint),
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct SinkEndpoint {
    // pub connection: String,
    #[serde(flatten)]
    pub endpoint_type: SinkEndpointType,
}

pub fn load_config() -> Result<Config, config::ConfigError> {
    // Attempt to load .env file
    dotenvy::dotenv().ok();
    let config_file = std::env::var("CONFIG_FILE").unwrap_or_else(|_| "config.yml".to_string());

    let settings = config::Config::builder()
        .add_source(
            // You can add a config file here, e.g., config::File::with_name("config.yaml")
            config::File::from(Path::new(&config_file)).required(false),
        )
        .add_source(
            config::Environment::default()
                .prefix("BRIDGE")
                .separator("__")
                .try_parsing(true),
        )
        .set_default("log_level", "info")?
        .set_default("sled_path", "/tmp/dedup_db")?
        .set_default("dedup_ttl_seconds", 86400)?
        .build()?;

    settings.try_deserialize()
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum SinkEndpointType {
    Kafka(KafkaSinkEndpoint),
    Nats(NatsSinkEndpoint),
    Amqp(AmqpSinkEndpoint),
    Mqtt(MqttSinkEndpoint),
    File(FileSinkEndpoint),
    Http(HttpSinkEndpoint),
    StaticResponse(StaticResponseEndpoint),
}

#[derive(Debug, Deserialize, Clone)]
pub struct Route {
    pub name: String,
    pub source: SourceEndpoint,
    pub sink: SinkEndpoint,
    pub dlq: Option<DlqConfig>,
}
fn default_static_response_content() -> String {
    "OK".to_string()
}


#[derive(Debug, Deserialize, Default, Clone)]
pub struct Config {
    #[serde(default)]
    pub logger: String,
    pub log_level: String,
    pub sled_path: String,
    pub dedup_ttl_seconds: u64,
    #[serde(default)]
    pub metrics: MetricsConfig,
    // #[serde(default)]
    // pub connections: Vec<Connection>,
    #[serde(default)]
    pub routes: Vec<Route>,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct KafkaSourceEndpoint {
    #[serde(flatten)]
    pub config: KafkaConfig,
    #[serde(flatten)]
    pub endpoint: KafkaEndpoint,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct NatsSourceEndpoint {
    #[serde(flatten)]
    pub config: NatsConfig,
    #[serde(flatten)]
    pub endpoint: NatsEndpoint,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct AmqpSourceEndpoint {
    #[serde(flatten)]
    pub config: AmqpConfig,
    #[serde(flatten)]
    pub endpoint: AmqpEndpoint,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct MqttSourceEndpoint {
    #[serde(flatten)]
    pub config: MqttConfig,
    #[serde(flatten)]
    pub endpoint: MqttEndpoint,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct FileSourceEndpoint {
    #[serde(flatten)]
    pub config: FileConfig,
    #[serde(flatten)]
    pub endpoint: FileEndpoint,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct HttpSourceEndpoint {
    #[serde(flatten)]
    pub config: HttpConfig,
    #[serde(flatten)]
    pub endpoint: HttpEndpoint,
}

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaSinkEndpoint {
    #[serde(flatten)]
    pub config: KafkaConfig,
    #[serde(flatten)]
    pub endpoint: KafkaEndpoint,
}

#[derive(Debug, Deserialize, Clone)]
pub struct NatsSinkEndpoint {
    #[serde(flatten)]
    pub config: NatsConfig,
    #[serde(flatten)]
    pub endpoint: NatsEndpoint,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AmqpSinkEndpoint {
    #[serde(flatten)]
    pub config: AmqpConfig,
    #[serde(flatten)]
    pub endpoint: AmqpEndpoint,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MqttSinkEndpoint {
    #[serde(flatten)]
    pub config: MqttConfig,
    #[serde(flatten)]
    pub endpoint: MqttEndpoint,
}

#[derive(Debug, Deserialize, Clone)]
pub struct FileSinkEndpoint {
    #[serde(flatten)]
    pub config: FileConfig,
    #[serde(flatten)]
    pub endpoint: FileEndpoint,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HttpSinkEndpoint {
    #[serde(flatten)]
    pub config: HttpConfig,
    #[serde(flatten)]
    pub endpoint: HttpEndpoint,
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
    pub kafka: KafkaSinkEndpoint,
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
  - name: "kafka_to_nats"
    source:
      kafka:
        brokers: "kafka:9092"
        group_id: "bridge_group"
        topic: "in_topic"
    sink:
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

        let route = &config.routes[0];
        assert_eq!(route.name, "kafka_to_nats");
        assert!(route.dlq.is_some());
        if let SourceEndpointType::Kafka(k) = &route.source.endpoint_type {
            assert_eq!(k.config.brokers, "kafka:9092");
            assert_eq!(k.endpoint.topic.as_deref(), Some("in_topic"));
        }
    }
}
