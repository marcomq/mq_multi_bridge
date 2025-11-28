use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    pub group_id: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct NatsConfig {
    pub url: String,
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct AmqpConfig {
    pub url: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum ConnectionType {
    Kafka(KafkaConfig),
    Nats(NatsConfig),
    Amqp(AmqpConfig),
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct KafkaEndpoint {
    pub topic: String,
}

#[derive(Debug, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct NatsEndpoint {
    pub subject: String,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Hash)]
pub struct AmqpEndpoint {
    pub queue: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub struct SourceEndpoint {
    pub connection: String,
    #[serde(flatten)]
    pub endpoint_type: SourceEndpointType,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
pub enum SourceEndpointType {
    Kafka(KafkaEndpoint),
    Nats(NatsEndpoint),
    Amqp(AmqpEndpoint),
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
}

#[derive(Debug, Deserialize, Clone)]
pub struct Route {
    pub name: String,
    pub source: SourceEndpoint,
    pub sink: SinkEndpoint,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub log_level: String,
    pub sled_path: String,
    pub dedup_ttl_seconds: u64,
    #[serde(default)]
    pub connections: Vec<Connection>,
    pub dlq: Option<DlqConfig>,
    #[serde(default)]
    pub routes: Vec<Route>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DlqConfig {
    pub connection: String,
    #[serde(flatten)]
    pub kafka: KafkaEndpoint,
}
