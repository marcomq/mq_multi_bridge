//  mq_multi_bridge
//  © Copyright 2025, by Marco Mengelkoch
//  Licensed under MIT License, see License file for more details
//  git clone https://github.com/marcomq/mq_multi_bridge

use crate::config::StaticResponseEndpoint;
use crate::model::CanonicalMessage;
use crate::sinks::MessageSink;
use async_trait::async_trait;
use serde_json::Value;
use std::any::Any;
use tracing::trace;

/// A sink that responds with a static, pre-configured message.
#[derive(Clone)]
pub struct StaticResponseSink {
    content: String,
}

impl StaticResponseSink {
    pub fn new(config: &StaticResponseEndpoint) -> anyhow::Result<Self> {
        Ok(Self {
            content: config.content.clone(),
        })
    }
}

#[async_trait]
impl MessageSink for StaticResponseSink {
    async fn send(&self, _message: CanonicalMessage) -> anyhow::Result<Option<CanonicalMessage>> {
        trace!(response = %self.content, "Sending static response");
        Ok(Some(CanonicalMessage::new(Value::String(
            self.content.clone(),
        ))))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}