use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Clone, Default)]
pub struct KafkaConfig {
    pub brokers: String,
    pub group_id: String,
    #[serde(default)]
    pub tls: TlsConfig,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct TlsConfig {
    #[serde(default)]
    pub required: bool,
    pub ca_file: Option<String>,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
    pub cert_password: Option<String>,
    #[serde(default)]
    pub accept_invalid_certs: bool,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct NatsConfig {
    pub url: String,
    pub default_stream: Option<String>,
    #[serde(default)]
    pub tls: NatsTlsConfig,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct NatsTlsConfig {
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub accept_invalid_certs: bool,
    pub ca_file: Option<String>,
    pub cert_file: Option<String>,
    pub key_file: Option<String>,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct AmqpConfig {
    pub url: String,
    #[serde(default)]
    pub tls: TlsConfig,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct MqttConfig {
    pub url: String,
    #[serde(default)]
    pub tls: TlsConfig,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct FileConfig {
    pub path: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct HttpConfig {
    pub listen_address: Option<String>,
    pub url: Option<String>,
    // Optional sink to send the HTTP response to
    pub response_sink: Option<String>,
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
#[serde(rename_all = "lowercase")]
pub struct SourceEndpoint {
    pub connection: String,
    #[serde(flatten)]
    pub endpoint_type: SourceEndpointType,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum SourceEndpointType {
    Kafka(KafkaEndpoint),
    Nats(NatsEndpoint),
    Amqp(AmqpEndpoint),
    Mqtt(MqttEndpoint),
    File(FileEndpoint),
    Http(HttpEndpoint),
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct SinkEndpoint {
    pub connection: String,
    #[serde(flatten)]
    pub endpoint_type: SinkEndpointType,
}

pub fn load_config() -> Result<Config, config::ConfigError> {
    // Attempt to load .env file
    dotenvy::dotenv().ok();

    let settings = config::Config::builder()
        .add_source(
            // You can add a config file here, e.g., config::File::with_name("config.yaml")
            config::File::with_name("config").required(false),
        )
        .add_source(
            config::Environment::default()
                .prefix("MQ_MULTI_BRIDGE")
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
pub struct Connection {
    pub name: String,
    #[serde(flatten)]
    pub connection_type: ConnectionType,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum SinkEndpointType {
    Kafka(KafkaEndpoint),
    Nats(NatsEndpoint),
    Amqp(AmqpEndpoint),
    Mqtt(MqttEndpoint),
    File(FileEndpoint),
    Http(HttpEndpoint),
}

#[derive(Debug, Deserialize, Clone)]
pub struct Route {
    pub name: String,
    pub source: SourceEndpoint,
    pub sink: SinkEndpoint,
}

#[derive(Debug, Deserialize, Default, Clone)]
pub struct Config {
    pub log_level: String,
    pub sled_path: String,
    pub dedup_ttl_seconds: u64,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub connections: Vec<Connection>,
    pub dlq: Option<DlqConfig>,
    #[serde(default)]
    pub routes: Vec<Route>,
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
    pub connection: String,
    pub kafka: DlqKafkaEndpoint,
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

connections:
  - name: "kafka_main"
    kafka:
      brokers: "kafka:9092"
      group_id: "bridge_group"
  - name: "nats_main"
    nats:
      url: "nats://nats:4222"
  - name: "output_log"
    file:
      path: "/tmp/output.log"
  - name: "http_server"
    http:
      listen_address: "0.0.0.0:8080"

dlq:
  connection: "kafka_main"
  # The topic must be nested under the endpoint type, 'kafka' in this case.
  kafka:
    topic: "my_dlq"

routes:
  - name: "kafka_to_nats"
    source:
      connection: "kafka_main"
      kafka:
        topic: "in_topic"
    sink:
      connection: "nats_main"
      nats:
        subject: "out_subject"
"#;

        let config: Result<Config, _> = serde_yaml::from_str(yaml_config);
        assert!(config.is_ok());
        let config = config.unwrap();

        assert_eq!(config.log_level, "debug");
        assert_eq!(config.connections.len(), 4);
        assert_eq!(config.routes.len(), 1);
        assert!(config.dlq.is_some());

        let kafka_conn = config.connections.iter().find(|c| c.name == "kafka_main").unwrap();
        if let ConnectionType::Kafka(kafka_config) = &kafka_conn.connection_type {
            assert_eq!(kafka_config.brokers, "kafka:9092");
        } else {
            panic!("Expected Kafka connection");
        }

        let route = &config.routes[0];
        assert_eq!(route.name, "kafka_to_nats");
        assert_eq!(route.source.connection, "kafka_main");
    }
}

