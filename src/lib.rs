//! IMEC-v2: Interval-mapped Morton-Eytzinger CoW Key-Value Engine
//!
//! A high-performance key-value storage engine that combines:
//! - Order-preserving UTF-8 encoding for arbitrary keys
//! - Morton-T interleaving for spatial-temporal locality
//! - Eytzinger layout for cache-optimal search
//! - Copy-on-Write extents for lock-free MVCC
//!
//! # Examples
//!
//! ```
//! use zverse::encoding::{morton_t_encode, current_timestamp_micros};
//!
//! let key = "user:alice";
//! let timestamp = current_timestamp_micros();
//! let morton_code = morton_t_encode(key, timestamp);
//! println!("Morton-T code: {:x}", morton_code.value());
//! ```

pub mod encoding;
pub mod interval;
pub mod storage;

// Re-export core encoding functionality
pub use encoding::{
    BitStream,
    MortonTCode,
    encode,
    encode_limited,
    morton_t_encode,
    current_timestamp_micros,
    morton_t_range_for_prefix,
    morton_t_range_for_time_window,
};

pub use interval::{
    IntervalTable,
    IntervalNode,
    IntervalError,
    IntervalTableStats,
};

pub use storage::{
    StorageEngine,
    StorageConfig,
    StorageError,
    StorageStats,
    Page,
    PageType,
    Extent,
    ExtentError,
};

use std::sync::atomic::{AtomicU64, Ordering};

/// Global timestamp counter for generating unique timestamps
static GLOBAL_TIMESTAMP: AtomicU64 = AtomicU64::new(0);

/// Initialize the global timestamp system
pub fn init_timestamp() {
    let now = current_timestamp_micros();
    GLOBAL_TIMESTAMP.store(now, Ordering::Relaxed);
}

/// Get the next unique timestamp
pub fn next_timestamp() -> u64 {
    GLOBAL_TIMESTAMP.fetch_add(1, Ordering::Relaxed)
}

/// Version information
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const NAME: &str = env!("CARGO_PKG_NAME");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_library_basics() {
        init_timestamp();
        
        let key = "test:key";
        let timestamp = next_timestamp();
        let code = morton_t_encode(key, timestamp);
        
        assert!(code.value() > 0);
        assert!(timestamp > 0);
    }
    
    #[test]
    fn test_version_info() {
        assert!(!VERSION.is_empty());
        assert_eq!(NAME, "zverse");
    }
}