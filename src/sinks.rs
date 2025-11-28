use crate::model::CanonicalMessage;
use async_trait::async_trait;

#[async_trait]
pub trait MessageSink {
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<()>;
}
