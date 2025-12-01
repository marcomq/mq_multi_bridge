//  mq_multi_bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq_multi_bridge
use crate::config::FileConfig;
use crate::model::CanonicalMessage;
use crate::consumers::{BoxFuture, BoxedMessageStream, MessageConsumer};
use crate::publishers::MessagePublisher;
use anyhow::{anyhow, Context};
use async_trait::async_trait;
use std::any::Any;
use std::path::Path;
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, BufWriter};
use tokio::sync::Mutex;
use tracing::info;

/// A sink that writes messages to a file, one per line.
pub struct FilePublisher {
    writer: Arc<Mutex<BufWriter<File>>>,
}

impl FilePublisher {
    pub async fn new(config: &FileConfig) -> anyhow::Result<Self> {
        let path = Path::new(&config.path);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!("Failed to create parent directory for file: {:?}", parent)
            })?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.path)
            .await
            .with_context(|| {
                format!("Failed to open or create file for writing: {}", config.path)
            })?;

        info!(path = %config.path, "File sink opened for appending");
        Ok(Self {
            writer: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }
}

impl Clone for FilePublisher {
    fn clone(&self) -> Self {
        Self {
            writer: self.writer.clone(),
        }
    }
}

#[async_trait]
impl MessagePublisher for FilePublisher {
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<Option<CanonicalMessage>> {
        let mut payload = serde_json::to_vec(&message)?;
        payload.push(b'\n'); // Add a newline to separate messages

        let mut writer = self.writer.lock().await;
        writer.write_all(&payload).await?;
        writer.flush().await?; // Ensure it's written to disk
        Ok(None)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A source that reads messages from a file, one line at a time.
pub struct FileConsumer {
    reader: Arc<Mutex<BufReader<File>>>,
    path: String,
}

impl FileConsumer {
    pub async fn new(config: &FileConfig) -> anyhow::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .open(&config.path)
            .await
            .with_context(|| format!("Failed to open file for reading: {}", config.path))?;

        info!(path = %config.path, "File source opened for reading");
        Ok(Self {
            reader: Arc::new(Mutex::new(BufReader::new(file))),
            path: config.path.clone(),
        })
    }
}

impl Clone for FileConsumer {
    fn clone(&self) -> Self {
        Self {
            reader: self.reader.clone(),
            path: self.path.clone(),
        }
    }
}

#[async_trait]
impl MessageConsumer for FileConsumer {
    async fn receive(&self) -> anyhow::Result<(CanonicalMessage, BoxedMessageStream)> {
        let mut reader = self.reader.lock().await;
        let mut line = String::new();

        let bytes_read = reader.read_line(&mut line).await?;
        if bytes_read == 0 {
            return Err(anyhow!("End of file reached: {}", self.path));
        }

        let payload: serde_json::Value = serde_json::from_str(&line)?;
        let message = CanonicalMessage::deserialized_new(payload);

        // The commit for a file source is a no-op.
        let commit = Box::new(move |_| Box::pin(async move {}) as BoxFuture<'static, ()>);

        Ok((message, commit))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::CanonicalMessage;
    use serde_json::json;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_file_sink_and_source_integration() {
        // 1. Setup a temporary directory and file path
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.log");
        let file_path_str = file_path.to_str().unwrap().to_string();

        // 2. Create a FileSink
        let sink_config = FileConfig {
            path: file_path_str.clone(),
        };
        let sink = FilePublisher::new(&sink_config).await.unwrap();

        // 3. Send some messages
        let msg1 = CanonicalMessage::new(json!({"hello": "world"}));
        let msg2 = CanonicalMessage::new(json!({"foo": "bar"}));

        sink.send(msg1.clone()).await.unwrap();
        sink.send(msg2.clone()).await.unwrap();

        // 4. Create a FileSource to read from the same file
        let source_config = FileConfig {
            path: file_path_str.clone(),
        };
        let source = FileConsumer::new(&source_config).await.unwrap();

        // 5. Receive the messages and verify them
        let (received_msg1, commit1) = source.receive().await.unwrap();
        commit1(None).await; // Commit is a no-op, but we should call it
        assert_eq!(received_msg1.message_id, msg1.message_id);
        assert_eq!(received_msg1.payload, msg1.payload);

        let (received_msg2, commit2) = source.receive().await.unwrap();
        commit2(None).await;
        assert_eq!(received_msg2.message_id, msg2.message_id);
        assert_eq!(received_msg2.payload, msg2.payload);

        // 6. Verify that reading again results in EOF
        let eof_result = source.receive().await;
        match eof_result {
            Ok(_) => panic!("Expected an error, but got Ok"),
            Err(e) => assert!(e.to_string().contains("End of file reached")),
        }
    }

    #[tokio::test]
    async fn test_file_sink_creates_directory() {
        let dir = tempdir().unwrap();
        let nested_dir_path = dir.path().join("nested");
        let file_path = nested_dir_path.join("test.log");

        // The `nested` directory does not exist yet, FileSink::new should create it.
        let sink_config = FileConfig {
            path: file_path.to_str().unwrap().to_string(),
        };
        let sink_result = FilePublisher::new(&sink_config).await;

        assert!(sink_result.is_ok());
        assert!(nested_dir_path.exists());
        assert!(file_path.exists());
    }
}
