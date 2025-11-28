use sled::Db;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;
use uuid::Uuid;

#[derive(Clone)]
pub struct DeduplicationStore {
    db: Db,
    ttl_seconds: u64,
}

impl DeduplicationStore {
    pub fn new<P: AsRef<Path>>(path: P, ttl_seconds: u64) -> Result<Self, sled::Error> {
        info!("Opening deduplication database at: {:?}", path.as_ref());
        let db = sled::open(path)?;
        Ok(Self { db, ttl_seconds })
    }

    /// Checks if a message ID has been seen recently. If not, it stores it.
    /// Returns `true` if the message is a duplicate, `false` otherwise.
    pub fn is_duplicate(&self, message_id: &Uuid) -> Result<bool, sled::Error> {
        let key = message_id.as_bytes();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let optional_value = self.db.get(&key)?;

        if let Some(value) = optional_value {
            // Value is the timestamp of insertion
            let ts_bytes: [u8; 8] = value.as_ref().try_into().unwrap();
            let insertion_ts = u64::from_be_bytes(ts_bytes);

            // Check if TTL has expired
            if now > insertion_ts + self.ttl_seconds {
                // TTL expired, treat as not a duplicate and update timestamp
                self.db.insert(&key, &now.to_be_bytes())?;
                return Ok(false);
            }
            // Within TTL, it's a duplicate
            return Ok(true);
        }

        // Not seen before, insert it and return false (not a duplicate)
        self.db.insert(&key, &now.to_be_bytes())?;
        Ok(false)
    }
}