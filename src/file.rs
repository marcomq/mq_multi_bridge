//  mq_multi_bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq_multi_bridge

use crate::config::FileConfig;
use crate::model::CanonicalMessage;
use crate::sinks::MessageSink;
use crate::sources::{BoxFuture, BoxedMessageStream, MessageSource};
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
pub struct FileSink {
    writer: Arc<Mutex<BufWriter<File>>>,
}

impl FileSink {
    pub async fn new(config: &FileConfig) -> anyhow::Result<Self> {
        let path = Path::new(&config.path);
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("Failed to create parent directory for file: {:?}", parent))?;
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&config.path)
            .await
            .with_context(|| format!("Failed to open or create file for writing: {}", config.path))?;

        info!(path = %config.path, "File sink opened for appending");
        Ok(Self {
            writer: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }
}

impl Clone for FileSink {
    fn clone(&self) -> Self {
        Self { writer: self.writer.clone() }
    }
}

#[async_trait]
impl MessageSink for FileSink {
    async fn send(&self, message: CanonicalMessage) -> anyhow::Result<()> {
        let mut payload = serde_json::to_vec(&message)?;
        payload.push(b'\n'); // Add a newline to separate messages

        let mut writer = self.writer.lock().await;
        writer.write_all(&payload).await?;
        writer.flush().await?; // Ensure it's written to disk
        Ok(())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A source that reads messages from a file, one line at a time.
pub struct FileSource {
    reader: Arc<Mutex<BufReader<File>>>,
    path: String,
}

impl FileSource {
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

impl Clone for FileSource {
    fn clone(&self) -> Self {
        Self { reader: self.reader.clone(), path: self.path.clone() }
    }
}

#[async_trait]
impl MessageSource for FileSource {
    async fn receive(&self) -> anyhow::Result<(CanonicalMessage, BoxedMessageStream)> {
        let mut reader = self.reader.lock().await;
        let mut line = String::new();

        let bytes_read = reader.read_line(&mut line).await?;
        if bytes_read == 0 {
            return Err(anyhow!("End of file reached: {}", self.path));
        }

        let message: CanonicalMessage = serde_json::from_str(&line)?;

        // The commit for a file source is a no-op.
        let commit = Box::new(move || Box::pin(async move {}) as BoxFuture<'static, ()>);

        Ok((message, commit))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}