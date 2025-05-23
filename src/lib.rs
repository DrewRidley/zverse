//! ZVerse: Z-ordered Versioned Storage Engine
//!
//! This crate provides a high-performance key-value store designed specifically
//! for versioned data access patterns, using Z-order curves to optimize locality.

#![warn(missing_docs)]

/// Core data structures shared across implementations
pub mod core;
pub mod zcurve_db;

/// In-memory implementation for development and testing
pub mod inmemory;

/// Memory-mapped storage implementation
pub mod mmap_storage;

/// Persistent implementation using memory-mapped storage
pub mod persistent_zverse;

/// Lock-free concurrency framework
pub mod lockfree;

/// Lock-free ZVerse implementation
pub mod lockfree_zverse;

/// Master-class tiered lock-free implementation
pub mod master_class;

/// Stress tests for master-class implementation
#[cfg(test)]
pub mod master_class_stress_tests;

use chrono;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};

// Re-exports
pub use config::ZVerseConfig;
pub use core::{EntryWithData, ZEntry, ZSegmentHeader, ZValueRange};
pub use error::Error;
pub use inmemory::InMemoryZVerse;
pub use mmap_storage::MmapStorageManager;
pub use persistent_zverse::PersistentZVerse;
pub use zcurve_db::{ZCurveDB, Error as ZCurveError, DBStats};
pub use lockfree_zverse::LockFreeZVerseKV;
pub use master_class::{MasterClassZVerse, MasterClassConfig, SyncStrategy};

/// Error types for ZVerse operations
pub mod error {
    use std::error::Error as StdError;
    use std::fmt;
    use std::io;

    /// Error types that can occur in ZVerse operations
    #[derive(Debug)]
    pub enum Error {
        /// An I/O error occurred
        Io(io::Error),
        /// The key was not found
        KeyNotFound,
        /// The version was not found
        VersionNotFound,
        /// Segment file is corrupted
        CorruptSegment,
        /// Configuration error
        ConfigError(String),
        /// Other errors
        Other(String),
    }

    impl fmt::Display for Error {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Error::Io(err) => write!(f, "I/O error: {}", err),
                Error::KeyNotFound => write!(f, "Key not found"),
                Error::VersionNotFound => write!(f, "Version not found"),
                Error::CorruptSegment => write!(f, "Segment file is corrupted"),
                Error::ConfigError(msg) => write!(f, "Configuration error: {}", msg),
                Error::Other(msg) => write!(f, "Error: {}", msg),
            }
        }
    }

    impl StdError for Error {
        fn source(&self) -> Option<&(dyn StdError + 'static)> {
            match self {
                Error::Io(err) => Some(err),
                _ => None,
            }
        }
    }

    impl From<io::Error> for Error {
        fn from(err: io::Error) -> Self {
            Error::Io(err)
        }
    }
}

/// Configuration options for ZVerse
pub mod config {
    /// Configuration for a ZVerse instance
    #[derive(Debug, Clone)]
    pub struct ZVerseConfig {
        /// Path to the data directory
        pub data_path: String,
        /// Size of each segment in bytes
        pub segment_size: usize,
        /// Maximum number of entries per segment
        pub max_entries_per_segment: usize,
        /// Whether to sync writes immediately
        pub sync_writes: bool,
        /// Maximum size of the in-memory cache in bytes
        pub cache_size_bytes: usize,
        /// Number of threads to use for background operations
        pub background_threads: usize,
    }

    impl Default for ZVerseConfig {
        fn default() -> Self {
            Self {
                data_path: "zverse_data".to_string(),
                segment_size: 16 * 1024 * 1024, // 16MB
                max_entries_per_segment: 1_000_000,
                sync_writes: true,
                cache_size_bytes: 1024 * 1024 * 1024, // 1GB
                background_threads: 4,
            }
        }
    }
}

/// Z-order curve utilities
pub mod zorder {
    /// Calculate a Z-order value from a key and version
    pub fn calculate_z_value(key_hash: u32, version: u32) -> u64 {
        // Interleave the bits of key_hash and version
        let mut z = 0u64;
        for i in 0..32 {
            // Get the i-th bit of key_hash and put it at position 2*i
            if (key_hash & (1 << i)) != 0 {
                z |= 1 << (2 * i);
            }

            // Get the i-th bit of version and put it at position 2*i + 1
            if (version & (1 << i)) != 0 {
                z |= 1 << (2 * i + 1);
            }
        }
        z
    }

    /// Extract the key hash from a Z-order value
    pub fn extract_key_hash(z: u64) -> u32 {
        let mut key_hash = 0u32;
        for i in 0..32 {
            if (z & (1 << (2 * i))) != 0 {
                key_hash |= 1 << i;
            }
        }
        key_hash
    }

    /// Extract the version from a Z-order value
    pub fn extract_version(z: u64) -> u32 {
        let mut version = 0u32;
        for i in 0..32 {
            if (z & (1 << (2 * i + 1))) != 0 {
                version |= 1 << i;
            }
        }
        version
    }

    /// Calculate the minimum Z-value that contains a given key hash
    pub fn min_z_for_key(key_hash: u32) -> u64 {
        let mut z = 0u64;
        for i in 0..32 {
            if (key_hash & (1 << i)) != 0 {
                z |= 1 << (2 * i);
            }
        }
        z
    }

    /// Calculate the maximum Z-value that contains a given key hash
    pub fn max_z_for_key(key_hash: u32) -> u64 {
        let min_z = min_z_for_key(key_hash);
        let mut z = min_z;
        for i in 0..32 {
            z |= 1 << (2 * i + 1);
        }
        z
    }
}

/// Main ZVerse structure - currently just a wrapper around InMemoryZVerse
/// for development and testing
pub struct ZVerse {
    /// In-memory implementation
    inner: InMemoryZVerse,
    /// Configuration
    config: ZVerseConfig,
}

impl ZVerse {
    /// Create a new ZVerse instance with the given configuration
    pub fn new(config: ZVerseConfig) -> std::result::Result<Self, Error> {
        // Create directory if it doesn't exist
        if !std::path::Path::new(&config.data_path).exists() {
            std::fs::create_dir_all(&config.data_path)?;
        }

        Ok(Self {
            inner: InMemoryZVerse::new(),
            config,
        })
    }

    /// Get a value for a key at a specific version
    pub fn get<K, V>(&self, key: K, version: Option<u64>) -> std::result::Result<Option<V>, Error>
    where
        K: AsRef<[u8]>,
        V: From<Vec<u8>>,
    {
        self.inner.get(key, version)
    }

    /// Put a value for a key
    pub fn put<K, V>(&self, key: K, value: V) -> std::result::Result<u64, Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.inner.put(key, value)
    }

    /// Delete a key
    pub fn delete<K>(&self, key: K) -> std::result::Result<u64, Error>
    where
        K: AsRef<[u8]>,
    {
        self.inner.delete(key)
    }

    /// Scan a range of keys at a specific version
    pub fn scan<K>(
        &self,
        start: Option<K>,
        end: Option<K>,
        version: Option<u64>,
    ) -> std::result::Result<
        Box<dyn Iterator<Item = std::result::Result<(Vec<u8>, Vec<u8>), Error>> + '_>,
        Error,
    >
    where
        K: AsRef<[u8]>,
    {
        self.inner.scan(start, end, version)
    }

    /// Get the history of a key
    pub fn history<K>(
        &self,
        key: K,
        start_version: Option<u64>,
        end_version: Option<u64>,
    ) -> std::result::Result<
        Box<dyn Iterator<Item = std::result::Result<(u64, Vec<u8>), Error>> + '_>,
        Error,
    >
    where
        K: AsRef<[u8]>,
    {
        self.inner.history(key, start_version, end_version)
    }

    /// Get a snapshot at a specific version
    pub fn snapshot(
        &self,
        version: u64,
    ) -> std::result::Result<
        Box<dyn Iterator<Item = std::result::Result<(Vec<u8>, Vec<u8>), Error>> + '_>,
        Error,
    > {
        self.inner.snapshot(version)
    }

    /// Flush all pending writes
    pub fn flush(&self) -> std::result::Result<(), Error> {
        // No-op for in-memory implementation
        Ok(())
    }

    /// Close the database
    pub fn close(&self) -> std::result::Result<(), Error> {
        // No-op for in-memory implementation
        Ok(())
    }
}

/// Module for benchmarking
#[cfg(test)]
mod bench {

    /// Run a basic benchmark
    pub fn run_basic_benchmark() {
        // Benchmark code would go here
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_z_order_calculation() {
        let key_hash = 0x12345678u32;
        let version = 0xABCDEF01u32;

        let z = zorder::calculate_z_value(key_hash, version);

        let extracted_key = zorder::extract_key_hash(z);
        let extracted_version = zorder::extract_version(z);

        assert_eq!(extracted_key, key_hash);
        assert_eq!(extracted_version, version);
    }
}
