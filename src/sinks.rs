use crate::model::CanonicalMessage;
use async_trait::async_trait;
use std::any::Any;

#[async_trait]
pub trait MessageSink: Send + Sync {
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<()>;
    fn as_any(&self) -> &dyn Any;
}
