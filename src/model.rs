use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CanonicalMessage {
    pub message_id: Uuid,
    pub schema_version: String,
    pub ts: u64,
    pub payload: serde_json::Value,
}

impl CanonicalMessage {
    pub fn new(payload: serde_json::Value) -> Self {
        Self {
            message_id: Uuid::new_v4(),
            schema_version: "1.0".to_string(),
            ts: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            payload,
        }
    }
}