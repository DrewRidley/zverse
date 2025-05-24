//! Morton Temporal Database - High-Performance Lock-Free Temporal Data Storage
//!
//! This module implements a lock-free temporal database using Morton space-time encoding
//! for optimal cache locality and zero-coordination concurrent access.

use std::sync::atomic::{AtomicU64, AtomicPtr, Ordering};
use std::collections::HashMap;
use smallvec::SmallVec;
use crate::encoding::morton::{morton_t_encode, current_timestamp_micros};

const PARTITION_COUNT: usize = 1024;
const PREFETCH_DISTANCE: usize = 4;
const TIMELINE_INLINE_SIZE: usize = 8;
const VALUE_INLINE_SIZE: usize = 512;

// Branch prediction hints for hot paths
macro_rules! likely {
    ($expr:expr) => {
        std::intrinsics::likely($expr)
    };
}

macro_rules! unlikely {
    ($expr:expr) => {
        std::intrinsics::unlikely($expr)
    };
}

/// Error types for Morton temporal database operations
#[derive(Debug, Clone, PartialEq)]
pub enum MortonError {
    /// Partition not found or null pointer
    PartitionNotFound,
    /// Key not found in database
    KeyNotFound,
    /// Invalid timestamp for temporal query
    InvalidTimestamp,
    /// Operation failed due to concurrent modification
    ConcurrentModification,
}

impl std::fmt::Display for MortonError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MortonError::PartitionNotFound => write!(f, "Partition not found"),
            MortonError::KeyNotFound => write!(f, "Key not found"),
            MortonError::InvalidTimestamp => write!(f, "Invalid timestamp"),
            MortonError::ConcurrentModification => write!(f, "Concurrent modification detected"),
        }
    }
}

impl std::error::Error for MortonError {}

/// Result type for Morton database operations
pub type MortonResult<T> = Result<T, MortonError>;

/// A temporal record containing key, timestamp, and value data
#[derive(Clone, Debug)]
struct Record {
    key: String,
    timestamp: u64,
    value: SmallVec<[u8; VALUE_INLINE_SIZE]>,
}

impl Record {
    fn new(key: String, timestamp: u64, value: Vec<u8>) -> Self {
        Self {
            key,
            timestamp,
            value: SmallVec::from_vec(value),
        }
    }
}

/// Optimized timeline for temporal version management
#[derive(Clone, Debug)]
struct Timeline {
    timestamps: SmallVec<[u64; TIMELINE_INLINE_SIZE]>,
    morton_codes: SmallVec<[u64; TIMELINE_INLINE_SIZE]>,
}

impl Timeline {
    fn new() -> Self {
        Self {
            timestamps: SmallVec::new(),
            morton_codes: SmallVec::new(),
        }
    }

    /// Insert a new timestamp-morton pair with optimized monotonic fast path
    #[inline(always)]
    fn insert(&mut self, timestamp: u64, morton_code: u64) {
        // Fast path: monotonic insertion (most common case)
        if likely!(self.timestamps.is_empty() || timestamp >= *self.timestamps.last().unwrap()) {
            self.timestamps.push(timestamp);
            self.morton_codes.push(morton_code);
        } else {
            // Slower insertion for out-of-order timestamps
            let pos = self.timestamps.iter().position(|&t| t > timestamp).unwrap_or(self.timestamps.len());
            self.timestamps.insert(pos, timestamp);
            self.morton_codes.insert(pos, morton_code);
        }
    }

    /// Get the latest Morton code (most recent version)
    #[inline(always)]
    fn latest_morton(&self) -> Option<u64> {
        if likely!(!self.morton_codes.is_empty()) {
            self.morton_codes.last().copied()
        } else {
            None
        }
    }

    /// Optimized predecessor search with branch prediction
    #[inline(always)]
    fn predecessor_search(&self, query_time: u64) -> Option<u64> {
        if unlikely!(self.timestamps.is_empty()) {
            return None;
        }

        // Fast path: query is at or after latest timestamp (common case)
        if likely!(query_time >= *self.timestamps.last().unwrap()) {
            return self.morton_codes.last().copied();
        }

        // Optimized linear search with manual unrolling for small timelines
        let mut best_idx = None;
        let len = self.timestamps.len();
        let mut i = 0;
        
        // Unroll by 4 for better performance
        while i + 3 < len {
            if self.timestamps[i] <= query_time { best_idx = Some(i); }
            if self.timestamps[i + 1] <= query_time { best_idx = Some(i + 1); }
            if self.timestamps[i + 2] <= query_time { best_idx = Some(i + 2); }
            if self.timestamps[i + 3] <= query_time { best_idx = Some(i + 3); }
            i += 4;
        }
        
        // Handle remaining elements
        while i < len {
            if self.timestamps[i] <= query_time { best_idx = Some(i); }
            i += 1;
        }
        
        best_idx.map(|idx| self.morton_codes[idx])
    }


}

/// Lock-free partition containing temporal data
struct Partition {
    timeline_index: HashMap<String, Timeline>,
    morton_storage: HashMap<u64, Record>,
}

impl Partition {
    fn new() -> Self {
        Self {
            timeline_index: HashMap::with_capacity(2000),
            morton_storage: HashMap::with_capacity(4000),
        }
    }

    /// Insert a record with timestamp
    #[inline(always)]
    fn put_with_timestamp(&mut self, key: &str, value: Vec<u8>, timestamp: u64) -> MortonResult<()> {
        let morton_code = morton_t_encode(key, timestamp).value();
        
        let record = Record::new(key.to_string(), timestamp, value);
        self.morton_storage.insert(morton_code, record);

        self.timeline_index.entry(key.to_string())
            .or_insert_with(Timeline::new)
            .insert(timestamp, morton_code);

        Ok(())
    }

    /// Batch insert for better temporal locality
    fn put_batch_with_timestamps(&mut self, items: &[(String, Vec<u8>, u64)]) -> usize {
        let mut success_count = 0;
        
        for (key, value, timestamp) in items {
            if self.put_with_timestamp(key, value.clone(), *timestamp).is_ok() {
                success_count += 1;
            }
        }
        
        success_count
    }

    /// Get the latest version of a key
    #[inline(always)]
    fn get(&self, key: &str) -> MortonResult<Vec<u8>> {
        let timeline = self.timeline_index.get(key).ok_or(MortonError::KeyNotFound)?;
        let morton_code = timeline.latest_morton().ok_or(MortonError::KeyNotFound)?;
        let record = self.morton_storage.get(&morton_code).ok_or(MortonError::KeyNotFound)?;
        Ok(record.value.to_vec())
    }

    /// Get a version at a specific timestamp
    #[inline(always)]
    fn get_at_time(&self, key: &str, query_time: u64) -> MortonResult<Vec<u8>> {
        let timeline = self.timeline_index.get(key).ok_or(MortonError::KeyNotFound)?;
        let morton_code = timeline.predecessor_search(query_time).ok_or(MortonError::KeyNotFound)?;
        let record = self.morton_storage.get(&morton_code).ok_or(MortonError::KeyNotFound)?;
        Ok(record.value.to_vec())
    }

    /// Batch get operations for better cache efficiency
    fn get_batch(&self, keys: &[&str]) -> Vec<MortonResult<Vec<u8>>> {
        keys.iter().map(|&key| self.get(key)).collect()
    }

    /// Batch temporal get operations
    fn get_batch_at_time(&self, keys: &[&str], query_time: u64) -> Vec<MortonResult<Vec<u8>>> {
        keys.iter().map(|&key| self.get_at_time(key, query_time)).collect()
    }


}

/// High-performance lock-free Morton temporal database
pub struct MortonTemporalDB {
    partitions: [AtomicPtr<Partition>; PARTITION_COUNT],
    global_timestamp: AtomicU64,
}

unsafe impl Send for MortonTemporalDB {}
unsafe impl Sync for MortonTemporalDB {}

impl MortonTemporalDB {
    /// Create a new Morton temporal database
    pub fn new() -> Self {
        let partitions = std::array::from_fn(|_| {
            let partition = Box::new(Partition::new());
            AtomicPtr::new(Box::into_raw(partition))
        });

        Self {
            partitions,
            global_timestamp: AtomicU64::new(current_timestamp_micros()),
        }
    }

    /// Get partition ID using key-based hashing for consistent routing
    #[inline(always)]
    fn get_partition_id(&self, key: &str, _timestamp: u64) -> usize {
        // Use FNV hash on key for consistent partition assignment
        let mut hash = 0xcbf29ce484222325u64; // FNV offset basis
        for byte in key.bytes() {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x100000001b3); // FNV prime
        }
        // Fibonacci hash for final distribution
        hash = hash.wrapping_mul(11400714819323198485u64);
        (hash >> (64 - 10)) as usize
    }

    /// Get next timestamp
    #[inline(always)]
    fn get_timestamp(&self) -> u64 {
        self.global_timestamp.fetch_add(1, Ordering::Relaxed)
    }

    /// Get range of timestamps for batch operations
    fn get_timestamp_range(&self, count: usize) -> u64 {
        self.global_timestamp.fetch_add(count as u64, Ordering::Relaxed)
    }

    /// Insert a key-value pair with automatic timestamping
    #[inline(always)]
    pub fn put(&self, key: &str, value: Vec<u8>) -> MortonResult<()> {
        let timestamp = self.get_timestamp();
        self.put_at_time(key, value, timestamp)
    }

    /// Insert a key-value pair at a specific timestamp
    pub fn put_at_time(&self, key: &str, value: Vec<u8>, timestamp: u64) -> MortonResult<()> {
        let partition_id = self.get_partition_id(key, timestamp);
        
        let partition_ptr = self.partitions[partition_id].load(Ordering::Acquire);
        if unlikely!(partition_ptr.is_null()) {
            return Err(MortonError::PartitionNotFound);
        }

        unsafe {
            let partition = &mut *partition_ptr;
            partition.put_with_timestamp(key, value, timestamp)
        }
    }

    /// Batch insert with temporal locality optimization
    pub fn put_temporal_batch(&self, items: &[(String, Vec<u8>)]) -> usize {
        let timestamp_base = self.get_timestamp_range(items.len());
        let mut success_count = 0;
        
        // Group by partition for cache efficiency
        let mut partition_groups: Vec<Vec<(String, Vec<u8>, u64)>> = vec![Vec::new(); PARTITION_COUNT];
        
        for (i, (key, value)) in items.iter().enumerate() {
            let timestamp = timestamp_base + i as u64;
            let partition_id = self.get_partition_id(key, timestamp);
            partition_groups[partition_id].push((key.clone(), value.clone(), timestamp));
        }

        // Process each partition's batch with prefetching
        for (partition_id, group) in partition_groups.iter().enumerate() {
            if likely!(!group.is_empty()) {
                let partition_ptr = self.partitions[partition_id].load(Ordering::Acquire);
                if likely!(!partition_ptr.is_null()) {
                    unsafe {
                        std::intrinsics::prefetch_read_instruction(partition_ptr, 3);
                        let partition = &mut *partition_ptr;
                        success_count += partition.put_batch_with_timestamps(group);
                    }
                }
            }
        }

        success_count
    }

    /// Get the latest version of a key with strategic prefetching
    #[inline(always)]
    pub fn get(&self, key: &str) -> MortonResult<Vec<u8>> {
        let primary_partition_id = self.get_partition_id(key, 0);
        
        // Strategic prefetching of nearby partitions
        for i in 1..=PREFETCH_DISTANCE {
            let prefetch_id = (primary_partition_id + i) % PARTITION_COUNT;
            let prefetch_ptr = self.partitions[prefetch_id].load(Ordering::Relaxed);
            if likely!(!prefetch_ptr.is_null()) {
                unsafe {
                    std::intrinsics::prefetch_read_instruction(prefetch_ptr, 3);
                }
            }
        }

        // Try primary partition first (most likely case)
        let partition_ptr = self.partitions[primary_partition_id].load(Ordering::Acquire);
        if likely!(!partition_ptr.is_null()) {
            unsafe {
                let partition = &*partition_ptr;
                if let Ok(result) = partition.get(key) {
                    return Ok(result);
                }
            }
        }

        // Search nearby partitions with optimized pattern
        for distance in 1..=8 {
            for &direction in &[-1i32, 1i32] {
                let check_partition_id = ((primary_partition_id as i32 + direction * distance) as usize) % PARTITION_COUNT;
                let check_partition_ptr = self.partitions[check_partition_id].load(Ordering::Acquire);
                
                if likely!(!check_partition_ptr.is_null()) {
                    unsafe {
                        let check_partition = &*check_partition_ptr;
                        if let Ok(result) = check_partition.get(key) {
                            return Ok(result);
                        }
                    }
                }
            }
        }

        Err(MortonError::KeyNotFound)
    }

    /// Get a version at a specific timestamp
    pub fn get_at_time(&self, key: &str, query_time: u64) -> MortonResult<Vec<u8>> {
        let partition_id = self.get_partition_id(key, query_time);
        
        let partition_ptr = self.partitions[partition_id].load(Ordering::Acquire);
        if unlikely!(partition_ptr.is_null()) {
            return Err(MortonError::PartitionNotFound);
        }

        unsafe {
            let partition = &*partition_ptr;
            partition.get_at_time(key, query_time)
        }
    }

    /// Batch get operations with partition grouping for cache efficiency
    pub fn get_batch(&self, keys: &[&str]) -> Vec<MortonResult<Vec<u8>>> {
        let mut results = vec![Err(MortonError::KeyNotFound); keys.len()];
        let mut partition_groups: Vec<Vec<(usize, &str)>> = vec![Vec::new(); PARTITION_COUNT];
        
        // Group keys by partition
        for (i, &key) in keys.iter().enumerate() {
            let partition_id = self.get_partition_id(key, 0);
            partition_groups[partition_id].push((i, key));
        }

        // Process each partition's queries in batch
        for (partition_id, group) in partition_groups.iter().enumerate() {
            if likely!(!group.is_empty()) {
                let partition_ptr = self.partitions[partition_id].load(Ordering::Acquire);
                if likely!(!partition_ptr.is_null()) {
                    unsafe {
                        let partition = &*partition_ptr;
                        for &(result_idx, key) in group {
                            results[result_idx] = partition.get(key);
                        }
                    }
                }
            }
        }

        results
    }

    /// Batch temporal get operations
    pub fn get_batch_at_time(&self, keys: &[&str], query_time: u64) -> Vec<MortonResult<Vec<u8>>> {
        let mut results = vec![Err(MortonError::KeyNotFound); keys.len()];
        let mut partition_groups: Vec<Vec<(usize, &str)>> = vec![Vec::new(); PARTITION_COUNT];
        
        // Group keys by partition
        for (i, &key) in keys.iter().enumerate() {
            let partition_id = self.get_partition_id(key, query_time);
            partition_groups[partition_id].push((i, key));
        }

        // Process each partition's queries in batch
        for (partition_id, group) in partition_groups.iter().enumerate() {
            if likely!(!group.is_empty()) {
                let partition_ptr = self.partitions[partition_id].load(Ordering::Acquire);
                if likely!(!partition_ptr.is_null()) {
                    unsafe {
                        let partition = &*partition_ptr;
                        let keys_only: Vec<&str> = group.iter().map(|(_, key)| *key).collect();
                        let partition_results = partition.get_batch_at_time(&keys_only, query_time);
                        
                        for ((result_idx, _), result) in group.iter().zip(partition_results) {
                            results[*result_idx] = result;
                        }
                    }
                }
            }
        }

        results
    }


}

impl Default for MortonTemporalDB {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for MortonTemporalDB {
    fn drop(&mut self) {
        for partition_atomic in &self.partitions {
            let ptr = partition_atomic.load(Ordering::Acquire);
            if !ptr.is_null() {
                unsafe {
                    let _partition = Box::from_raw(ptr);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let db = MortonTemporalDB::new();
        
        // Test put and get
        assert!(db.put("key1", b"value1".to_vec()).is_ok());
        assert_eq!(db.get("key1").unwrap(), b"value1");
        
        // Test key not found
        assert!(matches!(db.get("nonexistent"), Err(MortonError::KeyNotFound)));
    }

    #[test]
    fn test_temporal_operations() {
        let db = MortonTemporalDB::new();
        
        // Insert multiple versions
        assert!(db.put_at_time("key1", b"value1".to_vec(), 100).is_ok());
        assert!(db.put_at_time("key1", b"value2".to_vec(), 200).is_ok());
        assert!(db.put_at_time("key1", b"value3".to_vec(), 300).is_ok());
        
        // Test temporal queries
        assert_eq!(db.get_at_time("key1", 150).unwrap(), b"value1");
        assert_eq!(db.get_at_time("key1", 250).unwrap(), b"value2");
        assert_eq!(db.get_at_time("key1", 350).unwrap(), b"value3");
        
        // Test latest version
        assert_eq!(db.get("key1").unwrap(), b"value3");
    }

    #[test]
    fn test_batch_operations() {
        let db = MortonTemporalDB::new();
        
        // Test batch insert
        let batch = vec![
            ("key1".to_string(), b"value1".to_vec()),
            ("key2".to_string(), b"value2".to_vec()),
            ("key3".to_string(), b"value3".to_vec()),
        ];
        assert_eq!(db.put_temporal_batch(&batch), 3);
        
        // Test batch get
        let keys = vec!["key1", "key2", "key3", "nonexistent"];
        let results = db.get_batch(&keys);
        assert_eq!(results[0].as_ref().unwrap(), b"value1");
        assert_eq!(results[1].as_ref().unwrap(), b"value2");
        assert_eq!(results[2].as_ref().unwrap(), b"value3");
        assert!(results[3].is_err());
    }


}