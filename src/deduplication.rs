use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::info;

#[derive(Clone)]
pub struct DeduplicationStore {
    db: sled::Db,
    ttl_seconds: u64,
    // We use an atomic u64 for the current time to avoid frequent syscalls to SystemTime::now()
    // This can be updated by a background task for efficiency.
    current_time: Arc<AtomicU64>,
}

impl DeduplicationStore {
    pub fn new<P: AsRef<Path>>(path: P, ttl_seconds: u64) -> Result<Self, sled::Error> {
        info!("Opening deduplication database at: {:?}", path.as_ref());

        const MB: u64 = 1024 * 1024;
        let cache_capacity_bytes = 25 * MB;

        let db = sled::Config::new()
            .path(path)
            .mode(sled::Mode::HighThroughput)
            .cache_capacity(cache_capacity_bytes)
            .use_compression(false) // Disable compression for higher speed on in-memory data
            .flush_every_ms(None) // We will flush manually, reducing I/O
            .open()?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let current_time = Arc::new(AtomicU64::new(now));

        // Spawn a background task to update the time periodically
        let time_updater = current_time.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs();
                time_updater.store(now, Ordering::Relaxed);
            }
        });

        Ok(Self {
            db,
            ttl_seconds,
            current_time,
        })
    }

    /// Checks if a message ID has been seen recently. If not, it stores it.
    /// Returns `true` if the message is a duplicate, `false` otherwise.
    pub fn is_duplicate(&self, message_id: &u64) -> Result<bool, sled::Error> {
        let key = message_id.to_be_bytes();
        let now = self.current_time.load(Ordering::Relaxed);
        let new_value = now.to_be_bytes();

        let result = self
            .db
            .compare_and_swap(key, None as Option<&[u8]>, Some(&new_value))?;

        match result {
            Ok(_) => Ok(false), // Key was not present, CAS succeeded, not a duplicate.
            Err(sled::CompareAndSwapError {
                current: Some(current_value),
                ..
            }) => {
                let insertion_ts = u64::from_be_bytes(current_value.as_ref().try_into().unwrap());
                if now > insertion_ts + self.ttl_seconds {
                    // TTL expired, treat as not a duplicate and update timestamp.
                    self.db.insert(key, &new_value)?;
                    Ok(false)
                } else {
                    // Within TTL, it's a duplicate.
                    Ok(true)
                }
            }
            Err(sled::CompareAndSwapError { current: None, .. }) => {
                // This case should be rare under high contention, but it means another thread
                // swapped our `None` before we could. We can treat it as not a duplicate for this attempt.
                Ok(false)
            }
        }
    }

    /// Flushes the database to disk asynchronously.
    pub async fn flush_async(&self) -> Result<usize, sled::Error> {
        self.db.flush_async().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use seahash::hash;
    use std::time::Duration;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_is_duplicate_new_and_seen() {
        let dir = tempdir().unwrap();
        let store = DeduplicationStore::new(dir.path(), 60).unwrap();
        let payload = b"test_payload";
        let message_id = hash(payload);

        // First time seeing this ID, should not be a duplicate.
        assert!(!store.is_duplicate(&message_id).unwrap());

        // Second time, should be a duplicate.
        assert!(store.is_duplicate(&message_id).unwrap());
    }

    #[tokio::test]
    async fn test_is_duplicate_ttl_expiration() {
        let dir = tempdir().unwrap();
        let ttl_seconds = 1;
        let store = DeduplicationStore::new(dir.path(), ttl_seconds).unwrap();
        let payload = b"another_payload";
        let message_id = hash(payload);

        // First time, not a duplicate
        assert!(!store.is_duplicate(&message_id).unwrap());

        // Immediately after, it is a duplicate
        assert!(store.is_duplicate(&message_id).unwrap());

        // Wait for the TTL to expire
        tokio::time::sleep(Duration::from_secs(ttl_seconds + 2)).await;

        // After TTL, it should no longer be considered a duplicate
        assert!(!store.is_duplicate(&message_id).unwrap());

        // And now it should be a duplicate again, since it was re-inserted
        assert!(store.is_duplicate(&message_id).unwrap());
    }
}
