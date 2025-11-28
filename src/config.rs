use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct KafkaConfig {
    pub brokers: String,
    pub group_id: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct NatsConfig {
    pub url: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AmqpConfig {
    pub url: String,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Hash)]
pub struct KafkaEndpoint {
    pub topic: String,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Hash)]
pub struct NatsEndpoint {
    pub subject: String,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Hash)]
pub struct AmqpEndpoint {
    pub queue: String,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum SourceEndpoint {
    Kafka(KafkaEndpoint),
    Nats(NatsEndpoint),
    Amqp(AmqpEndpoint),
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum SinkEndpoint {
    Kafka(KafkaEndpoint),
    Nats(NatsEndpoint),
    Amqp(AmqpEndpoint), // Assuming AMQP sink publishes to a queue for simplicity
}

#[derive(Debug, Deserialize, Clone)]
pub struct Route {
    pub name: String,
    pub source: SourceEndpoint,
    pub sink: SinkEndpoint,
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
        .set_default("kafka.brokers", "localhost:9092")?
        .set_default("kafka.group_id", "mq-bridge-group")?
        .set_default("nats.url", "nats://localhost:4222")?
        .set_default("amqp.url", "amqp://guest:guest@localhost:5672")?
        .set_default("sled_path", "/tmp/dedup_db")?
        .set_default("dedup_ttl_seconds", 86400)?
        .set_default("dlq.kafka.topic", "events-dlq")?
        .build()?;

    settings.try_deserialize()
}

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub log_level: String,
    pub sled_path: String,
    pub dedup_ttl_seconds: u64,
    pub kafka: KafkaConfig,
    pub nats: NatsConfig,
    pub amqp: AmqpConfig,
    pub dlq: DlqConfig,
    pub routes: Vec<Route>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DlqConfig {
    pub kafka: KafkaEndpoint,
}
