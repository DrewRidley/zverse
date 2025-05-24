//! ZVerse - High-Performance Lock-Free Temporal Database
//!
//! ZVerse is a high-performance, lock-free temporal database using Morton space-time encoding
//! for optimal cache locality and zero-coordination concurrent access.
//!
//! # Features
//!
//! - **Lock-free operations**: Zero coordination overhead between threads
//! - **Temporal data management**: Efficient versioning and time-based queries
//! - **High performance**: 1.3M+ writes/sec and 10M+ reads/sec
//! - **Cache-optimized**: Strategic prefetching and memory layout
//! - **Morton encoding**: Space-time locality for optimal data organization
//! - **Batch operations**: High-throughput bulk data processing
//!
//! # Quick Start
//!
//! ```rust
//! use zverse::MortonTemporalDB;
//!
//! let db = MortonTemporalDB::new();
//!
//! // Insert data
//! db.put("user:123", b"user data".to_vec()).unwrap();
//!
//! // Retrieve data
//! let data = db.get("user:123").unwrap();
//!
//! // Temporal queries
//! db.put_at_time("user:123", b"old data".to_vec(), 500).unwrap();
//! db.put_at_time("user:123", b"updated data".to_vec(), 1000).unwrap();
//! let historical_data = db.get_at_time("user:123", 750).unwrap();
//! assert_eq!(historical_data, b"old data");
//! ```

#![feature(core_intrinsics)]
#![warn(missing_docs)]
#![warn(rust_2018_idioms)]

pub mod encoding;
pub mod morton;

// Re-export main types for convenience
pub use morton::{MortonError, MortonResult, MortonTemporalDB};
