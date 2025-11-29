use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CanonicalMessage {
    #[serde(default = "Uuid::new_v4")]
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

    /// Creates a new CanonicalMessage from a deserialized payload,
    /// generating a new message_id if one is not present.
    pub fn deserialized_new(payload: serde_json::Value) -> Self {
        let mut msg: CanonicalMessage =
            serde_json::from_value(payload.clone()).unwrap_or_else(|_| {
                // If the payload is not a valid CanonicalMessage, treat the whole thing as the payload
                CanonicalMessage::new(payload)
            });
        // Ensure timestamp is current if it was missing
        msg.ts = msg.ts.max(1); // Ensure it's not 0 from default
        msg
    }
}
