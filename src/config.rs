use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub log_level: String,
    pub kafka_brokers: String,
    pub kafka_group_id: String,
    pub kafka_in_topic: String,
    pub kafka_out_topic: String,
    pub kafka_dlq_topic: String,
    pub nats_url: String,
    pub nats_in_subject: String,
    pub nats_out_subject: String,
    pub amqp_url: String,
    pub amqp_in_queue: String,
    pub sled_path: String,
    pub dedup_ttl_seconds: u64,
}

pub fn load_config() -> Result<Config, config::ConfigError> {
    // Attempt to load .env file
    dotenvy::dotenv().ok();

    let settings = config::Config::builder()
        .add_source(
            config::Environment::default()
                .separator("_")
                .try_parsing(true),
        )
        .set_default("log_level", "info")?
        .set_default("kafka_brokers", "localhost:9092")?
        .set_default("kafka_group_id", "mq-bridge-group")?
        .set_default("kafka_in_topic", "events-in")?
        .set_default("kafka_out_topic", "events-out")?
        .set_default("kafka_dlq_topic", "events-dlq")?
        .set_default("nats_url", "nats://localhost:4222")?
        .set_default("nats_in_subject", "events.in")?
        .set_default("nats_out_subject", "events.out")?
        .set_default("amqp_url", "amqp://guest:guest@localhost:5672")?
        .set_default("amqp_in_queue", "events-in-amqp")?
        .set_default("sled_path", "/tmp/dedup_db")?
        .set_default("dedup_ttl_seconds", 86400)?
        .build()?;

    settings.try_deserialize()
}