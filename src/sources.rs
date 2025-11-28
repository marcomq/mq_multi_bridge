use crate::model::CanonicalMessage;
use async_trait::async_trait;
pub use futures::future::BoxFuture;
use std::any::Any;

/// A closure that can be called to commit the message.
/// It returns a `BoxFuture` to allow for async commit operations.
pub type BoxedMessageStream = Box<dyn FnOnce() -> BoxFuture<'static, ()> + Send + 'static>;

#[async_trait]
pub trait MessageSource: Send + Sync {
    async fn receive(&self) -> anyhow::Result<(CanonicalMessage, BoxedMessageStream)>;
    fn as_any(&self) -> &dyn Any;
}
