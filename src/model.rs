use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CanonicalMessage {
    pub message_id: u64,
    pub payload: Vec<u8>,
}

impl CanonicalMessage {
    pub fn new(payload: Vec<u8>) -> Self {
        let message_id = seahash::hash(&payload);
        Self {
            message_id,
            payload,
        }
    }

    pub fn from_json(payload: serde_json::Value) -> Result<Self, serde_json::Error> {
        let bytes = serde_json::to_vec(&payload)?;
        Ok(Self::new(bytes))
    }
}
