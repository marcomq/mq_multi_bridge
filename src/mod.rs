use crate::model::CanonicalMessage;
use async_trait::async_trait;
use std::error::Error;

pub mod kafka;
pub mod nats;

#[async_trait]
pub trait MessageSink {
    async fn send(&self, message: CanonicalMessage) -> Result<(), Box<dyn Error + Send + Sync>>;
}