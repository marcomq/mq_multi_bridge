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

        let optional_value = self.db.get(key)?;

        if let Some(value) = optional_value {
            // Value is the timestamp of insertion
            let ts_bytes: [u8; 8] = value.as_ref().try_into().expect("DB value is not a u64");
            let insertion_ts = u64::from_be_bytes(ts_bytes);

            // Check if TTL has expired
            if now > insertion_ts + self.ttl_seconds {
                // TTL expired, treat as not a duplicate and update timestamp
                self.db.insert(key, &now.to_be_bytes())?;
                return Ok(false);
            }
            // Within TTL, it's a duplicate
            return Ok(true);
        }

        // Not seen before, insert it and return false (not a duplicate)
        self.db.insert(key, &now.to_be_bytes())?;
        Ok(false)
    }

    /// Flushes the database to disk asynchronously.
    pub async fn flush_async(&self) -> Result<usize, sled::Error> {
        self.db.flush_async().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn test_is_duplicate_new_and_seen() { // This test is now synchronous
        let dir = tempdir().unwrap();
        let store = DeduplicationStore::new(dir.path(), 60).unwrap();
        let message_id = Uuid::new_v4();

        // First time seeing this ID, should not be a duplicate.
        assert!(!store.is_duplicate(&message_id).unwrap());

        // Second time, should be a duplicate.
        assert!(store.is_duplicate(&message_id).unwrap());
    }

    #[tokio::test]
    async fn test_is_duplicate_ttl_expiration() {
        let dir = tempdir().unwrap();
        let ttl_seconds = 2;
        let store = DeduplicationStore::new(dir.path(), ttl_seconds).unwrap();
        let message_id = Uuid::new_v4();

        // First time, not a duplicate
        assert!(!store.is_duplicate(&message_id).unwrap());

        // Immediately after, it is a duplicate
        assert!(store.is_duplicate(&message_id).unwrap());

        // Wait for the TTL to expire
        tokio::time::sleep(Duration::from_secs(ttl_seconds + 1)).await;

        // After TTL, it should no longer be considered a duplicate
        assert!(!store.is_duplicate(&message_id).unwrap());

        // And now it should be a duplicate again, since it was re-inserted
        assert!(store.is_duplicate(&message_id).unwrap());
    }
}
