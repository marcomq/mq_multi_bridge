use crate::model::CanonicalMessage;
use async_trait::async_trait;
use std::any::Any;

#[async_trait]
pub trait MessagePublisher: Send + Sync + 'static {
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<Option<CanonicalMessage>>;
    async fn flush(&self) -> anyhow::Result<()> {
        Ok(())
    }
    fn as_any(&self) -> &dyn Any;
}
