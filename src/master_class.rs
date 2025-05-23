//! Master-Class Tiered Lock-Free ZVerse Implementation
//!
//! This module implements a three-tier architecture designed for 1M+ writes/sec:
//! - Tier 1: Hot write path with sharded lock-free ring buffers
//! - Tier 2: Background compaction layer
//! - Tier 3: Read-optimized Z-ordered storage

use memmap2::{Mmap, MmapMut};
use parking_lot::RwLock;
use smallvec::SmallVec;
use std::cell::UnsafeCell;
use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;
use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crate::error::Error;
use crate::zorder;

/// Number of CPU cores for sharding
const NUM_CORES: usize = 16;
/// Number of compaction workers
const NUM_COMPACTION_WORKERS: usize = 4;
/// Ring buffer size per shard
const RING_BUFFER_SIZE: usize = 65536;

/// Master-class ZVerse with three-tier architecture
pub struct MasterClassZVerse {
    /// Tier 1: Hot write path
    hot_write_path: Arc<HotWritePath>,
    /// Tier 2: Background compaction
    compaction_layer: Arc<CompactionLayer>,
    /// Tier 3: Read-optimized storage
    read_optimized_storage: Arc<ReadOptimizedStorage>,
    /// Configuration
    config: MasterClassConfig,
}

/// Configuration for master-class implementation
#[derive(Debug, Clone)]
pub struct MasterClassConfig {
    pub data_path: String,
    pub sync_strategy: SyncStrategy,
    pub compaction_threads: usize,
    pub segment_size: usize,
}

/// Synchronization strategy for persistence
#[derive(Debug, Clone)]
pub enum SyncStrategy {
    Immediate,
    Batched(Duration),
    OnThreshold(usize),
}

/// High-performance entry optimized for cache efficiency
#[derive(Debug, Clone)]
pub struct ZEntry {
    /// Pre-calculated Z-order value
    pub z_value: u64,
    /// Hash for fast key comparison
    pub key_hash: u64,
    /// Inline small keys for cache efficiency
    pub key: SmallVec<[u8; 32]>,
    /// Value data
    pub value: Vec<u8>,
    /// Version number
    pub version: u64,
    /// Metadata flags
    pub flags: u8,
}

impl ZEntry {
    pub fn new(key: &[u8], value: &[u8], version: u64) -> Self {
        let key_hash = xxhash64(key);
        let z_value = z_order_interleave(key_hash, version);

        Self {
            z_value,
            key_hash,
            key: SmallVec::from_slice(key),
            value: value.to_vec(),
            version,
            flags: 0,
        }
    }
}

impl PartialEq for ZEntry {
    fn eq(&self, other: &Self) -> bool {
        self.z_value == other.z_value && self.key_hash == other.key_hash
    }
}

impl Eq for ZEntry {}

impl PartialOrd for ZEntry {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for ZEntry {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        self.z_value.cmp(&other.z_value)
    }
}

/// TIER 1: Hot Write Path - Target 1M+ writes/sec
pub struct HotWritePath {
    /// Sharded lock-free ring buffers (one per core)
    write_shards: [LockFreeRingBuffer; NUM_CORES],
    /// Global version counter (only coordination point)
    global_version: AtomicU64,
    /// Memory-mapped write buffers for persistence
    write_segments: MemoryMappedWriteBuffers,
    /// Compaction trigger
    compaction_trigger: Arc<CompactionTrigger>,
}

/// Lock-free ring buffer optimized for single-writer, multiple-reader
pub struct LockFreeRingBuffer {
    /// Lock-free circular buffer
    buffer: UnsafeCell<Box<[MaybeUninit<ZEntry>; RING_BUFFER_SIZE]>>,
    /// Atomic head pointer (reader)
    head: AtomicUsize,
    /// Atomic tail pointer (writer)
    tail: AtomicUsize,
    /// Memory-mapped region for persistence
    mmap_region: Option<MmapMut>,
}

/// Memory-mapped write buffers for zero-copy persistence
pub struct MemoryMappedWriteBuffers {
    /// Ring buffer regions
    buffer_regions: Vec<MmapMut>,
    /// Current active region
    active_region: AtomicUsize,
    /// Sync strategy
    sync_strategy: SyncStrategy,
}

/// Compaction trigger for coordinating background work
pub struct CompactionTrigger {
    /// Pending work flag
    pending_work: AtomicUsize,
    /// Urgency level
    urgency_level: AtomicUsize,
}

/// TIER 2: Background Compaction Layer
pub struct CompactionLayer {
    /// Multiple workers processing different Z-ranges
    workers: [CompactionWorker; NUM_COMPACTION_WORKERS],
    /// Lock-free work distribution
    work_queue: LockFreeQueue<CompactionTask>,
    /// Compaction state tracking
    compaction_state: AtomicCompactionState,
    /// Output to read-optimized storage
    output_writer: Arc<SegmentWriter>,
    /// Worker threads
    worker_handles: Vec<thread::JoinHandle<()>>,
}

/// Individual compaction worker
pub struct CompactionWorker {
    /// Worker ID
    id: usize,
    /// Z-range responsibility
    z_range: ZValueRange,
    /// Merge state
    merge_heap: BinaryHeap<ZEntry>,
    /// Output buffer
    output_buffer: Vec<ZEntry>,
}

/// Compaction task
#[derive(Debug, Clone)]
pub struct CompactionTask {
    pub z_range: ZValueRange,
    pub input_sources: Vec<InputSource>,
    pub priority: u8,
}

/// Input source for compaction
#[derive(Debug, Clone)]
pub enum InputSource {
    RingBuffer(usize),
    Segment(u64),
}

/// Z-value range
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZValueRange {
    pub min: u64,
    pub max: u64,
}

/// Atomic compaction state
pub struct AtomicCompactionState {
    /// Current compaction version
    compaction_version: AtomicU64,
    /// Active workers
    active_workers: AtomicUsize,
    /// Completed work units
    completed_work: AtomicU64,
}

/// Lock-free queue for work distribution
pub struct LockFreeQueue<T> {
    nodes: Box<[AtomicPtr<Node<T>>; 1024]>,
    head: AtomicUsize,
    tail: AtomicUsize,
}

/// Queue node
struct Node<T> {
    item: T,
    next: AtomicPtr<Node<T>>,
}

/// TIER 3: Read-Optimized Z-Ordered Storage
pub struct ReadOptimizedStorage {
    /// Lock-free segment directory
    segment_directory: LockFreeRadixTree,
    /// Memory-mapped segments
    segments: RwLock<Vec<MmapSegment>>,
    /// Segment metadata cache
    segment_cache: LockFreeCache,
}

/// Memory-mapped segment
pub struct MmapSegment {
    /// Memory-mapped file
    mmap: Mmap,
    /// Segment header
    header: *const ZSegmentHeader,
    /// Z-ordered entry array
    entries: *const ZEntry,
    /// Key/value data region
    data_region: *const u8,
    /// Metadata
    z_range: ZValueRange,
    version_range: VersionRange,
}

/// Segment header structure
#[repr(C, align(4096))]
pub struct ZSegmentHeader {
    pub magic: u64,
    pub format_version: u32,
    pub segment_id: u64,
    pub z_value_min: u64,
    pub z_value_max: u64,
    pub entry_count: u64,
    pub data_offset: u64,
    pub data_size: u64,
    pub checksum: u64,
    pub created_timestamp: u64,
    pub last_compaction: u64,
    pub flags: u32,
    pub reserved: [u8; 4000],
}

/// Version range
#[derive(Debug, Clone)]
pub struct VersionRange {
    pub min: u64,
    pub max: u64,
}

/// Lock-free radix tree for segment directory
pub struct LockFreeRadixTree {
    root: AtomicPtr<RadixNode>,
}

/// Radix tree node
struct RadixNode {
    key: u64,
    value: AtomicPtr<SegmentRef>,
    children: [AtomicPtr<RadixNode>; 16],
}

/// Segment reference
#[derive(Debug)]
pub struct SegmentRef {
    pub segment_id: u64,
    pub z_range: ZValueRange,
    pub file_path: String,
}

/// Lock-free cache for segment metadata
pub struct LockFreeCache {
    buckets: Box<[AtomicPtr<CacheEntry>; 1024]>,
}

/// Cache entry
struct CacheEntry {
    key: u64,
    value: SegmentMetadata,
    next: AtomicPtr<CacheEntry>,
}

/// Segment metadata
#[derive(Debug, Clone)]
pub struct SegmentMetadata {
    pub entry_count: usize,
    pub data_size: usize,
    pub last_accessed: Instant,
}

/// Segment writer for compaction output
pub struct SegmentWriter {
    current_segment: AtomicPtr<MmapMut>,
    segment_counter: AtomicU64,
    data_path: String,
}

// Implementation of core operations

impl MasterClassZVerse {
    /// Create new master-class ZVerse instance
    pub fn new(config: MasterClassConfig) -> Result<Self, Error> {
        let hot_write_path = Arc::new(HotWritePath::new(&config)?);
        let compaction_layer = Arc::new(CompactionLayer::new(&config)?);
        let read_optimized_storage = Arc::new(ReadOptimizedStorage::new(&config)?);

        Ok(Self {
            hot_write_path,
            compaction_layer,
            read_optimized_storage,
            config,
        })
    }

    /// High-performance write operation (target: <1µs)
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<u64, Error> {
        // Step 1: Single atomic operation for global ordering
        let version = self
            .hot_write_path
            .global_version
            .fetch_add(1, Ordering::AcqRel);

        // Step 2: Create optimized entry
        let entry = ZEntry::new(key, value, version);

        // Step 3: Shard selection (no coordination needed)
        let shard_id = entry.key_hash as usize % NUM_CORES;

        // Step 4: Lock-free append to ring buffer
        self.hot_write_path.write_shards[shard_id].append(entry)?;

        // Step 5: Return immediately (no durability wait)
        Ok(version)
    }

    /// Lock-free read operation
    pub fn get(&self, key: &[u8], version: Option<u64>) -> Result<Option<Vec<u8>>, Error> {
        let read_version =
            version.unwrap_or_else(|| self.hot_write_path.global_version.load(Ordering::Acquire));

        let key_hash = xxhash64(key);

        // Query all tiers in order (newest to oldest)

        // 1. Hot tier (write buffers)
        if let Some(value) = self.hot_write_path.query(key_hash, read_version)? {
            return Ok(Some(value));
        }

        // 2. Warm tier (compaction layer)
        if let Some(value) = self.compaction_layer.query(key_hash, read_version)? {
            return Ok(Some(value));
        }

        // 3. Cold tier (read-optimized segments)
        self.read_optimized_storage.get(key_hash, read_version)
    }

    /// Range scan across all tiers
    pub fn scan(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        version: Option<u64>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        let read_version =
            version.unwrap_or_else(|| self.hot_write_path.global_version.load(Ordering::Acquire));

        // Calculate Z-value ranges
        let start_hash = xxhash64(start_key);
        let end_hash = xxhash64(end_key);
        let z_ranges = calculate_z_ranges(start_hash, end_hash, read_version);

        // Collect results from all tiers
        let mut results = Vec::new();

        // Query each tier
        results.extend(self.hot_write_path.scan(&z_ranges, read_version)?);
        results.extend(self.compaction_layer.scan(&z_ranges, read_version)?);
        results.extend(self.read_optimized_storage.scan(&z_ranges, read_version)?);

        // Sort by Z-order and deduplicate
        results.sort_by(|a, b| {
            let a_z = z_order_interleave(xxhash64(&a.0), read_version);
            let b_z = z_order_interleave(xxhash64(&b.0), read_version);
            a_z.cmp(&b_z)
        });

        // Remove duplicates (newer versions override older)
        let mut final_results = Vec::new();
        let mut last_key: Option<&[u8]> = None;

        for (key, value) in results.iter().rev() {
            if last_key != Some(key.as_slice()) {
                final_results.push((key.clone(), value.clone()));
                last_key = Some(key);
            }
        }

        final_results.reverse();
        Ok(final_results)
    }

    /// Get current version
    pub fn current_version(&self) -> u64 {
        self.hot_write_path.global_version.load(Ordering::Acquire)
    }

    /// Get performance statistics
    pub fn stats(&self) -> MasterClassStats {
        MasterClassStats {
            total_writes: self.hot_write_path.global_version.load(Ordering::Acquire),
            hot_tier_entries: self.hot_write_path.count_entries(),
            warm_tier_entries: self.compaction_layer.count_entries(),
            cold_tier_entries: self.read_optimized_storage.count_entries(),
            compaction_workers_active: self.compaction_layer.active_workers(),
        }
    }
}

/// Performance statistics
#[derive(Debug)]
pub struct MasterClassStats {
    pub total_writes: u64,
    pub hot_tier_entries: usize,
    pub warm_tier_entries: usize,
    pub cold_tier_entries: usize,
    pub compaction_workers_active: usize,
}

// Implementation helpers

impl HotWritePath {
    fn new(config: &MasterClassConfig) -> Result<Self, Error> {
        let write_shards = std::array::from_fn(|_| LockFreeRingBuffer::new());
        let write_segments = MemoryMappedWriteBuffers::new(config)?;
        let compaction_trigger = Arc::new(CompactionTrigger::new());

        Ok(Self {
            write_shards,
            global_version: AtomicU64::new(1),
            write_segments,
            compaction_trigger,
        })
    }

    fn query(&self, key_hash: u64, version: u64) -> Result<Option<Vec<u8>>, Error> {
        let shard_id = key_hash as usize % NUM_CORES;
        self.write_shards[shard_id].query(key_hash, version)
    }

    fn scan(
        &self,
        z_ranges: &[ZValueRange],
        version: u64,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        let mut results = Vec::new();
        for shard in &self.write_shards {
            results.extend(shard.scan(z_ranges, version)?);
        }
        Ok(results)
    }

    fn count_entries(&self) -> usize {
        self.write_shards.iter().map(|s| s.count()).sum()
    }
}

impl LockFreeRingBuffer {
    fn new() -> Self {
        Self {
            buffer: UnsafeCell::new(Box::new(
                [const { MaybeUninit::uninit() }; RING_BUFFER_SIZE],
            )),
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            mmap_region: None,
        }
    }

    fn append(&self, entry: ZEntry) -> Result<(), Error> {
        loop {
            let tail = self.tail.load(Ordering::Acquire);
            let next_tail = (tail + 1) % RING_BUFFER_SIZE;

            // Check if buffer is full
            if next_tail == self.head.load(Ordering::Acquire) {
                return Err(Error::Other("Ring buffer full".to_string()));
            }

            // Try to claim the slot
            if self
                .tail
                .compare_exchange_weak(tail, next_tail, Ordering::Release, Ordering::Relaxed)
                .is_ok()
            {
                // Write to claimed slot
                unsafe {
                    let buffer = &mut *self.buffer.get();
                    buffer[tail].as_mut_ptr().write(entry);
                }
                return Ok(());
            }
        }
    }

    fn query(&self, key_hash: u64, version: u64) -> Result<Option<Vec<u8>>, Error> {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);

        let mut best_match: Option<&ZEntry> = None;
        let mut best_version = 0;

        let mut current = head;
        while current != tail {
            unsafe {
                let buffer = &*self.buffer.get();
                let entry = &*buffer[current].as_ptr();
                if entry.key_hash == key_hash && entry.version <= version {
                    if entry.version > best_version {
                        best_version = entry.version;
                        best_match = Some(entry);
                    }
                }
            }
            current = (current + 1) % RING_BUFFER_SIZE;
        }

        Ok(best_match.map(|entry| entry.value.clone()))
    }

    fn scan(
        &self,
        z_ranges: &[ZValueRange],
        version: u64,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        let mut results = Vec::new();
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);

        let mut current = head;
        while current != tail {
            unsafe {
                let buffer = &*self.buffer.get();
                let entry = &*buffer[current].as_ptr();
                if entry.version <= version {
                    for range in z_ranges {
                        if entry.z_value >= range.min && entry.z_value <= range.max {
                            results.push((entry.key.to_vec(), entry.value.clone()));
                            break;
                        }
                    }
                }
            }
            current = (current + 1) % RING_BUFFER_SIZE;
        }

        Ok(results)
    }

    fn count(&self) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        if tail >= head {
            tail - head
        } else {
            RING_BUFFER_SIZE - head + tail
        }
    }
}

// Stub implementations for remaining components

impl MemoryMappedWriteBuffers {
    fn new(_config: &MasterClassConfig) -> Result<Self, Error> {
        Ok(Self {
            buffer_regions: Vec::new(),
            active_region: AtomicUsize::new(0),
            sync_strategy: SyncStrategy::Batched(Duration::from_millis(100)),
        })
    }
}

impl CompactionTrigger {
    fn new() -> Self {
        Self {
            pending_work: AtomicUsize::new(0),
            urgency_level: AtomicUsize::new(0),
        }
    }
}

impl CompactionLayer {
    fn new(_config: &MasterClassConfig) -> Result<Self, Error> {
        let workers = std::array::from_fn(|i| CompactionWorker::new(i));
        let work_queue = LockFreeQueue::new();
        let compaction_state = AtomicCompactionState::new();
        let output_writer = Arc::new(SegmentWriter::new());

        Ok(Self {
            workers,
            work_queue,
            compaction_state,
            output_writer,
            worker_handles: Vec::new(),
        })
    }

    fn query(&self, _key_hash: u64, _version: u64) -> Result<Option<Vec<u8>>, Error> {
        // Stub implementation
        Ok(None)
    }

    fn scan(
        &self,
        _z_ranges: &[ZValueRange],
        _version: u64,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        // Stub implementation
        Ok(Vec::new())
    }

    fn count_entries(&self) -> usize {
        0 // Stub implementation
    }

    fn active_workers(&self) -> usize {
        self.compaction_state.active_workers.load(Ordering::Acquire)
    }
}

impl CompactionWorker {
    fn new(id: usize) -> Self {
        Self {
            id,
            z_range: ZValueRange {
                min: 0,
                max: u64::MAX,
            },
            merge_heap: BinaryHeap::new(),
            output_buffer: Vec::new(),
        }
    }
}

impl AtomicCompactionState {
    fn new() -> Self {
        Self {
            compaction_version: AtomicU64::new(0),
            active_workers: AtomicUsize::new(0),
            completed_work: AtomicU64::new(0),
        }
    }
}

impl<T> LockFreeQueue<T> {
    fn new() -> Self {
        Self {
            nodes: Box::new([const { AtomicPtr::new(std::ptr::null_mut()) }; 1024]),
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
        }
    }
}

impl<T> Node<T> {
    fn new(item: T) -> Self {
        Self {
            item,
            next: AtomicPtr::new(std::ptr::null_mut()),
        }
    }
}

impl ReadOptimizedStorage {
    fn new(_config: &MasterClassConfig) -> Result<Self, Error> {
        Ok(Self {
            segment_directory: LockFreeRadixTree::new(),
            segments: RwLock::new(Vec::new()),
            segment_cache: LockFreeCache::new(),
        })
    }

    fn get(&self, _key_hash: u64, _version: u64) -> Result<Option<Vec<u8>>, Error> {
        // Stub implementation
        Ok(None)
    }

    fn scan(
        &self,
        _z_ranges: &[ZValueRange],
        _version: u64,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        // Stub implementation
        Ok(Vec::new())
    }

    fn count_entries(&self) -> usize {
        0 // Stub implementation
    }
}

impl LockFreeRadixTree {
    fn new() -> Self {
        Self {
            root: AtomicPtr::new(std::ptr::null_mut()),
        }
    }
}

impl LockFreeCache {
    fn new() -> Self {
        Self {
            buckets: Box::new([const { AtomicPtr::new(std::ptr::null_mut()) }; 1024]),
        }
    }
}

impl SegmentWriter {
    fn new() -> Self {
        Self {
            current_segment: AtomicPtr::new(std::ptr::null_mut()),
            segment_counter: AtomicU64::new(0),
            data_path: String::new(),
        }
    }
}

// Utility functions

/// Fast hash function for keys
fn xxhash64(data: &[u8]) -> u64 {
    // Simplified hash - in production use xxhash or similar
    let mut hash = 0xcbf29ce484222325u64;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Z-order curve bit interleaving
fn z_order_interleave(key_hash: u64, version: u64) -> u64 {
    zorder::calculate_z_value(key_hash as u32, version as u32)
}

/// Calculate Z-value ranges for range queries
fn calculate_z_ranges(start_hash: u64, end_hash: u64, version: u64) -> Vec<ZValueRange> {
    // Simplified implementation - in production use litmax/bigmin algorithms
    let start_z = z_order_interleave(start_hash, version);
    let end_z = z_order_interleave(end_hash, version);

    vec![ZValueRange {
        min: start_z,
        max: end_z,
    }]
}

impl Default for MasterClassConfig {
    fn default() -> Self {
        Self {
            data_path: "./zverse_data".to_string(),
            sync_strategy: SyncStrategy::Batched(Duration::from_millis(100)),
            compaction_threads: NUM_COMPACTION_WORKERS,
            segment_size: 64 * 1024 * 1024, // 64MB
        }
    }
}

// Thread-safe implementations
unsafe impl Send for MasterClassZVerse {}
unsafe impl Sync for MasterClassZVerse {}
unsafe impl Send for HotWritePath {}
unsafe impl Sync for HotWritePath {}
unsafe impl Send for LockFreeRingBuffer {}
unsafe impl Sync for LockFreeRingBuffer {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Instant;

    #[test]
    fn test_basic_operations() {
        let config = MasterClassConfig::default();
        let db = MasterClassZVerse::new(config).expect("Failed to create database");

        // Test basic put operation
        let version1 = db.put(b"key1", b"value1").expect("Put failed");
        let version2 = db.put(b"key2", b"value2").expect("Put failed");
        let version3 = db.put(b"key1", b"value1-updated").expect("Put failed");

        assert!(version2 > version1);
        assert!(version3 > version2);

        // Test basic get operation
        let result1 = db.get(b"key1", None).expect("Get failed");
        assert_eq!(result1, Some(b"value1-updated".to_vec()));

        let result2 = db.get(b"key2", None).expect("Get failed");
        assert_eq!(result2, Some(b"value2".to_vec()));

        // Test non-existent key
        let result3 = db.get(b"nonexistent", None).expect("Get failed");
        assert_eq!(result3, None);

        // Test versioned access
        let result4 = db.get(b"key1", Some(version1)).expect("Get failed");
        assert_eq!(result4, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_ring_buffer_operations() {
        let ring_buffer = LockFreeRingBuffer::new();

        // Test append operations
        let entry1 = ZEntry::new(b"key1", b"value1", 1);
        let entry2 = ZEntry::new(b"key2", b"value2", 2);

        ring_buffer.append(entry1).expect("Append failed");
        ring_buffer.append(entry2).expect("Append failed");

        // Test query operations
        let key1_hash = xxhash64(b"key1");
        let result1 = ring_buffer.query(key1_hash, 10).expect("Query failed");
        assert_eq!(result1, Some(b"value1".to_vec()));

        let key2_hash = xxhash64(b"key2");
        let result2 = ring_buffer.query(key2_hash, 10).expect("Query failed");
        assert_eq!(result2, Some(b"value2".to_vec()));

        // Test count
        assert_eq!(ring_buffer.count(), 2);
    }

    #[test]
    fn test_concurrent_writes() {
        let config = MasterClassConfig::default();
        let db = Arc::new(MasterClassZVerse::new(config).expect("Failed to create database"));

        let mut handles = vec![];

        // Spawn multiple writers
        for thread_id in 0..4 {
            let db_clone = db.clone();
            let handle = thread::spawn(move || {
                let mut versions = vec![];
                for i in 0..100 {
                    let key = format!("thread{}-key{}", thread_id, i);
                    let value = format!("thread{}-value{}", thread_id, i);
                    let version = db_clone
                        .put(key.as_bytes(), value.as_bytes())
                        .expect("Put failed");
                    versions.push(version);
                }
                versions
            });
            handles.push(handle);
        }

        // Collect all versions
        let mut all_versions = vec![];
        for handle in handles {
            let versions = handle.join().unwrap();
            all_versions.extend(versions);
        }

        // All versions should be unique (writers are properly coordinated)
        all_versions.sort();
        let mut unique_versions = all_versions.clone();
        unique_versions.dedup();
        assert_eq!(all_versions.len(), unique_versions.len());

        // Verify we wrote 400 entries (4 threads * 100 entries)
        assert_eq!(all_versions.len(), 400);
    }

    #[test]
    fn test_write_performance() {
        let config = MasterClassConfig::default();
        let db = MasterClassZVerse::new(config).expect("Failed to create database");

        let start = Instant::now();
        let num_ops = 1000;

        for i in 0..num_ops {
            let key = format!("perf-key-{:06}", i);
            let value = format!("perf-value-{:06}", i);
            db.put(key.as_bytes(), value.as_bytes())
                .expect("Put failed");
        }

        let elapsed = start.elapsed();
        let ops_per_sec = num_ops as f64 / elapsed.as_secs_f64();

        println!("Write performance: {:.0} ops/sec", ops_per_sec);

        // Should be significantly faster than the old lock-free implementation
        // (which was getting ~600 ops/sec)
        assert!(
            ops_per_sec > 1000.0,
            "Expected >1000 ops/sec, got {:.0}",
            ops_per_sec
        );
    }

    #[test]
    fn test_stats() {
        let config = MasterClassConfig::default();
        let db = MasterClassZVerse::new(config).expect("Failed to create database");

        // Initially should have 0 writes
        let initial_stats = db.stats();
        assert_eq!(initial_stats.total_writes, 1); // Version starts at 1

        // After some writes
        for i in 0..10 {
            let key = format!("stats-key-{}", i);
            let value = format!("stats-value-{}", i);
            db.put(key.as_bytes(), value.as_bytes())
                .expect("Put failed");
        }

        let final_stats = db.stats();
        assert_eq!(final_stats.total_writes, 11); // 1 + 10 writes
        assert!(final_stats.hot_tier_entries > 0);
    }

    #[test]
    fn test_z_order_calculation() {
        let entry1 = ZEntry::new(b"key1", b"value1", 100);
        let entry2 = ZEntry::new(b"key2", b"value2", 101);

        // Entries should be ordered by Z-value
        assert_ne!(entry1.z_value, entry2.z_value);

        // Same key, different version should have different Z-values
        let entry3 = ZEntry::new(b"key1", b"value1-new", 102);
        assert_ne!(entry1.z_value, entry3.z_value);
    }

    #[test]
    fn test_configuration() {
        let config = MasterClassConfig {
            data_path: "/tmp/test".to_string(),
            sync_strategy: SyncStrategy::Immediate,
            compaction_threads: 8,
            segment_size: 128 * 1024 * 1024,
        };

        let db = MasterClassZVerse::new(config).expect("Failed to create database");

        // Should be able to perform basic operations
        let version = db.put(b"test", b"value").expect("Put failed");
        assert!(version > 0);

        let result = db.get(b"test", None).expect("Get failed");
        assert_eq!(result, Some(b"value".to_vec()));
    }
}
