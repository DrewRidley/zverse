//! ZVerse: High-Performance KV Database with Copy-on-Write and Locality-Aware Memory Mapping
//!
//! ZVerse is a modern key-value storage engine that uses key-timestamp interleaving for natural
//! COW semantics and mmap2-based locality optimization for temporal and graph workloads.

pub mod engine;
pub mod interleaved_key;
pub mod storage;
pub mod query;
pub mod locality;

pub use engine::ZVerseEngine;
pub use interleaved_key::InterleavedKey;

use std::sync::atomic::{AtomicU64, Ordering};

/// Global timestamp generator for COW operations using system time
static GLOBAL_TIMESTAMP: AtomicU64 = AtomicU64::new(0);

/// Generate a new timestamp for COW operations
pub fn next_timestamp() -> u64 {
    GLOBAL_TIMESTAMP.fetch_add(1, Ordering::Relaxed)
}

/// Initialize timestamp from system time
pub fn init_timestamp() {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("System time before Unix epoch")
        .as_micros() as u64;
    GLOBAL_TIMESTAMP.store(now, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_generation() {
        init_timestamp();
        let t1 = next_timestamp();
        let t2 = next_timestamp();
        assert!(t2 > t1);
    }
}