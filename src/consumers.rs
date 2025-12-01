use crate::model::CanonicalMessage;
use async_trait::async_trait;
pub use futures::future::BoxFuture;
use std::any::Any;

/// A closure that can be called to commit the message.
/// It returns a `BoxFuture` to allow for async commit operations.
pub type BoxedMessageStream =
    Box<dyn FnOnce(Option<CanonicalMessage>) -> BoxFuture<'static, ()> + Send + 'static>;

#[async_trait]
pub trait MessageConsumer: Send + Sync + 'static {
    /// Receives a single message.
    async fn receive(&self) -> anyhow::Result<(CanonicalMessage, BoxedMessageStream)>;

    /// Receives a batch of messages. The default implementation calls `receive` in a loop.
    /// Implementers should override this for more efficient batch retrieval.
    async fn receive_batch(
        &self,
        max_items: usize,
    ) -> anyhow::Result<Vec<(CanonicalMessage, BoxedMessageStream)>> {
        let mut messages = Vec::with_capacity(max_items.min(1)); // At least 1
        let (msg, commit) = self.receive().await?;
        messages.push((msg, commit));
        Ok(messages)
    }
    fn as_any(&self) -> &dyn Any;
}
