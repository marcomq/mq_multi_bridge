use crate::model::CanonicalMessage;
use async_trait::async_trait;
pub use futures::future::BoxFuture;
use std::any::Any;
use tokio::task::JoinHandle;
use async_channel::{bounded, Receiver};
use anyhow::anyhow;

/// A closure that can be called to commit the message.
/// It returns a `BoxFuture` to allow for async commit operations.
pub type CommitFunc =
    Box<dyn FnOnce(Option<CanonicalMessage>) -> BoxFuture<'static, ()> + Send + 'static>;

#[async_trait]
pub trait MessageConsumer: Send + Sync {
    /// Receives a single message.
    async fn receive(&mut self) -> anyhow::Result<(CanonicalMessage, CommitFunc)>;
    fn as_any(&self) -> &dyn Any;
}

/// A `MessageConsumer` that wraps another consumer to buffer messages in an internal channel.
/// This pre-fetches messages in a background task to improve throughput by reducing
/// the latency of the `receive` call.
pub struct BufferedConsumer {
    message_rx: Receiver<(CanonicalMessage, CommitFunc)>,
    background_task: Option<JoinHandle<()>>,
}

impl BufferedConsumer {
    /// Creates a new `BufferedConsumer` that wraps the given `consumer`.
    ///
    /// It spawns a background task that continuously calls `receive()` on the inner
    /// consumer and puts the messages into a channel of `buffer_size`.
    pub fn new(mut consumer: Box<dyn MessageConsumer>, buffer_size: usize) -> Self {
        let (tx, rx) = bounded(buffer_size);

        let background_task = tokio::spawn(async move {
            loop {
                match consumer.receive().await {
                    Ok(message_data) => {
                        if tx.send(message_data).await.is_err() {
                            // The receiver was dropped, which means the BufferedConsumer was dropped.
                            // We can safely exit the background task.
                            tracing::info!("BufferedConsumer channel closed, background task stopping.");
                            break;
                        }
                    }
                    Err(e) => {
                        // The underlying consumer has an error (e.g., EOF, connection lost).
                        // We should stop the background task. The error will be propagated
                        // to the final `receive` call when the channel closes.
                        tracing::info!("Underlying consumer in BufferedConsumer failed: {}. Background task stopping.", e);
                        break;
                    }
                }
            }
        });

        Self {
            message_rx: rx,
            background_task: Some(background_task),
        }
    }

    /// Stops the background read-ahead task gracefully.
    pub async fn stop(&mut self) {
        if let Some(handle) = self.background_task.take() {
            tracing::info!("Stopping BufferedConsumer background task.");
            handle.abort();
            // We can optionally wait for the task to be fully stopped.
            // We'll ignore the result as a cancellation error is expected.
            let _ = handle.await;
        }
    }
}

#[async_trait]
impl MessageConsumer for BufferedConsumer {
    async fn receive(&mut self) -> anyhow::Result<(CanonicalMessage, CommitFunc)> {
        self.message_rx
            .recv()
            .await
            .map_err(|_| anyhow!("BufferedConsumer channel has been closed."))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Drop for BufferedConsumer {
    fn drop(&mut self) {
        // When the BufferedConsumer is dropped, we must abort the background task
        // to prevent it from running forever if `stop` was not called.
        if let Some(handle) = self.background_task.take() {
            handle.abort();
        }
    }
}
