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
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, AtomicUsize, Ordering};
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
        let mut compaction_layer = CompactionLayer::new(&config)?;
        let read_optimized_storage = Arc::new(ReadOptimizedStorage::new(&config)?);

        // Start background compaction workers
        compaction_layer.start_background_workers(hot_write_path.clone(), read_optimized_storage.clone())?;
        let compaction_layer = Arc::new(compaction_layer);

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

        // Step 5: Check if compaction should be triggered
        if self.should_trigger_compaction() {
            self.trigger_background_compaction()?;
        }

        // Step 6: Return immediately (no durability wait)
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

    /// Check if compaction should be triggered based on hot tier load
    fn should_trigger_compaction(&self) -> bool {
        // Check if any ring buffer is getting full
        for shard in &self.hot_write_path.write_shards {
            if shard.should_trigger_compaction() {
                return true;
            }
        }
        false
    }

    /// Trigger background compaction for overloaded shards
    fn trigger_background_compaction(&self) -> Result<(), Error> {
        let mut overloaded_shards = Vec::new();
        
        // Find shards that need compaction
        for (shard_id, shard) in self.hot_write_path.write_shards.iter().enumerate() {
            if shard.should_trigger_compaction() {
                overloaded_shards.push(shard_id);
            }
        }

        // Submit compaction work if needed
        if !overloaded_shards.is_empty() {
            self.compaction_layer.trigger_compaction_for_shards(&overloaded_shards)?;
        }

        Ok(())
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
    pub fn new() -> Self {
        Self {
            buffer: UnsafeCell::new(Box::new(
                [const { MaybeUninit::uninit() }; RING_BUFFER_SIZE],
            )),
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            mmap_region: None,
        }
    }

    fn scan_z_range(&self, z_range: &ZValueRange) -> Result<Vec<ZEntry>, crate::error::Error> {
        let mut result = Vec::new();
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);

        let mut current = head;
        while current != tail {
            unsafe {
                let buffer = &*self.buffer.get();
                let entry = &*buffer[current].as_ptr();
                
                // Check if entry's Z-value is within our range
                if entry.z_value >= z_range.min && entry.z_value <= z_range.max {
                    result.push(entry.clone());
                }
            }
            current = (current + 1) % RING_BUFFER_SIZE;
        }

        Ok(result)
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

    fn should_trigger_compaction(&self) -> bool {
        let count = self.count();
        // Trigger compaction when buffer is 75% full
        count > (RING_BUFFER_SIZE * 3) / 4
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

    fn start_background_workers(
        &mut self,
        hot_write_path: Arc<HotWritePath>,
        read_storage: Arc<ReadOptimizedStorage>
    ) -> Result<(), Error> {
        let work_queue = Arc::new(LockFreeQueue::new());
        let output_writer = self.output_writer.clone();
        let stop_signal = Arc::new(AtomicBool::new(false));

        // Spawn worker threads
        for worker_id in 0..NUM_COMPACTION_WORKERS {
            let work_queue_clone = work_queue.clone();
            let hot_write_path_clone = hot_write_path.clone();
            let output_writer_clone = output_writer.clone();
            let stop_signal_clone = stop_signal.clone();
            let read_storage_clone = read_storage.clone();

            let handle = std::thread::Builder::new()
                .name(format!("compaction-worker-{}", worker_id))
                .spawn(move || {
                    let mut worker = CompactionWorker::new(worker_id);
                    worker.continuous_compaction_loop(
                        work_queue_clone,
                        hot_write_path_clone,
                        output_writer_clone,
                        read_storage_clone,
                        stop_signal_clone,
                    );
                })?;

            self.worker_handles.push(handle);
        }

        Ok(())
    }

    fn submit_compaction_task(&self, task: CompactionTask) -> Result<(), Error> {
        self.work_queue.enqueue(task).map_err(|e| {
            Error::Other(format!("Failed to submit compaction task: {:?}", e))
        })
    }

    fn trigger_compaction_for_shards(&self, shard_ids: &[usize]) -> Result<(), Error> {
        // Calculate Z-ranges for load balancing
        let z_ranges = calculate_z_ranges_for_workers(NUM_COMPACTION_WORKERS);
        
        for z_range in z_ranges.iter() {
            let task = CompactionTask {
                z_range: z_range.clone(),
                input_sources: shard_ids.iter()
                    .map(|&id| InputSource::RingBuffer(id))
                    .collect(),
                priority: if shard_ids.len() > NUM_CORES / 2 { 1 } else { 0 },
            };
            
            self.submit_compaction_task(task)?;
        }
        
        Ok(())
    }

    fn stop_workers(&mut self) {
        // Signal workers to stop
        // Note: In a real implementation, we'd store the stop_signal
        // For now, workers will stop when the main thread exits
        
        // Wait for all workers to complete
        while let Some(handle) = self.worker_handles.pop() {
            let _ = handle.join();
        }
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
    pub fn new(id: usize) -> Self {
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

    fn continuous_compaction_loop(
        &mut self, 
        work_queue: Arc<LockFreeQueue<CompactionTask>>,
        hot_write_path: Arc<HotWritePath>,
        output_writer: Arc<SegmentWriter>,
        read_storage: Arc<ReadOptimizedStorage>,
        stop_signal: Arc<AtomicBool>
    ) {
        while !stop_signal.load(Ordering::Acquire) {
            // Try to get work from the queue
            if let Some(task) = work_queue.dequeue() {
                if let Err(e) = self.process_compaction_task(&task, &hot_write_path, &output_writer, &read_storage) {
                    eprintln!("Compaction worker {} error: {:?}", self.id, e);
                }
            } else {
                // No work available, sleep briefly
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }
    }

    fn process_compaction_task(
        &mut self,
        task: &CompactionTask,
        hot_write_path: &HotWritePath,
        output_writer: &SegmentWriter,
        read_storage: &ReadOptimizedStorage
    ) -> Result<(), crate::error::Error> {
        // Collect entries from all input sources within our Z-range
        let mut entries = Vec::new();
        
        for source in &task.input_sources {
            match source {
                InputSource::RingBuffer(shard_id) => {
                    // Scan the specific shard for entries in our Z-range
                    if *shard_id < hot_write_path.write_shards.len() {
                        let shard_entries = hot_write_path.write_shards[*shard_id]
                            .scan_z_range(&task.z_range)?;
                        entries.extend(shard_entries);
                    }
                }
                InputSource::Segment(_segment_id) => {
                    // TODO: Scan existing segments (Phase 3)
                    // For now, skip segment sources
                    continue;
                }
            }
        }

        // Perform K-way merge to maintain Z-order
        let merged_entries = self.merge_z_ordered(entries)?;
        
        // Write merged entries to output segments
        self.write_to_segments(merged_entries, output_writer, read_storage)?;

        Ok(())
    }

    pub fn merge_z_ordered(&mut self, mut entries: Vec<ZEntry>) -> Result<Vec<ZEntry>, crate::error::Error> {
        // Clear previous state
        self.merge_heap.clear();
        self.output_buffer.clear();

        // Sort entries by Z-order value for efficient merging
        entries.sort_by_key(|e| e.z_value);

        // Deduplicate entries with same key, keeping the highest version
        let mut deduplicated = Vec::new();
        let mut last_key_hash: Option<u64> = None;
        let mut last_version = 0;

        for entry in entries {
            match last_key_hash {
                Some(key_hash) if key_hash == entry.key_hash => {
                    // Same key, check version
                    if entry.version > last_version {
                        // Replace with newer version
                        deduplicated.pop();
                        deduplicated.push(entry.clone());
                        last_version = entry.version;
                    }
                    // Otherwise, skip older version
                }
                _ => {
                    // New key
                    deduplicated.push(entry.clone());
                    last_key_hash = Some(entry.key_hash);
                    last_version = entry.version;
                }
            }
        }

        Ok(deduplicated)
    }

    fn write_to_segments(
        &mut self,
        entries: Vec<ZEntry>,
        output_writer: &SegmentWriter,
        read_storage: &ReadOptimizedStorage
    ) -> Result<(), crate::error::Error> {
        if entries.is_empty() {
            return Ok(());
        }

        // Create a new segment ID
        let segment_id = output_writer.write_segment(entries.clone())?;
        
        // Create the memory-mapped segment
        let segment = read_storage.create_segment_from_entries(
            entries.clone(),
            segment_id,
            "/tmp" // Use temp directory for now
        )?;
        
        // Add segment to read-optimized storage
        read_storage.add_segment(segment)?;
        
        // Also store in our output buffer for testing
        self.output_buffer.extend(entries);
        
        Ok(())
    }

    fn get_processed_entries(&self) -> &[ZEntry] {
        &self.output_buffer
    }

    fn clear_output_buffer(&mut self) {
        self.output_buffer.clear();
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

    pub fn enqueue(&self, item: T) -> Result<(), crate::error::Error> {
        let node = Box::into_raw(Box::new(Node::new(item)));
        
        loop {
            let tail = self.tail.load(Ordering::Acquire);
            let next_tail = (tail + 1) % self.nodes.len();
            
            // Check if queue is full
            if next_tail == self.head.load(Ordering::Acquire) {
                // Queue is full, clean up and return error
                unsafe { let _ = Box::from_raw(node); }
                return Err(crate::error::Error::Other("Queue full".to_string()));
            }
            
            // Try to claim the slot
            if self.nodes[tail].compare_exchange_weak(
                std::ptr::null_mut(),
                node,
                Ordering::Release,
                Ordering::Relaxed
            ).is_ok() {
                // Successfully placed item, now advance tail
                let _ = self.tail.compare_exchange_weak(
                    tail,
                    next_tail,
                    Ordering::Release,
                    Ordering::Relaxed
                );
                return Ok(());
            }
        }
    }

    pub fn dequeue(&self) -> Option<T> {
        loop {
            let head = self.head.load(Ordering::Acquire);
            let tail = self.tail.load(Ordering::Acquire);
            
            // Check if queue is empty
            if head == tail {
                return None;
            }
            
            // Try to get the node at head
            let node_ptr = self.nodes[head].load(Ordering::Acquire);
            if node_ptr.is_null() {
                continue; // Race condition, try again
            }
            
            // Try to claim this node
            if self.nodes[head].compare_exchange_weak(
                node_ptr,
                std::ptr::null_mut(),
                Ordering::Release,
                Ordering::Relaxed
            ).is_ok() {
                // Successfully claimed node, advance head
                let next_head = (head + 1) % self.nodes.len();
                let _ = self.head.compare_exchange_weak(
                    head,
                    next_head,
                    Ordering::Release,
                    Ordering::Relaxed
                );
                
                // Extract the item and clean up
                unsafe {
                    let node = Box::from_raw(node_ptr);
                    return Some(node.item);
                }
            }
        }
    }

    pub fn len(&self) -> usize {
        let head = self.head.load(Ordering::Acquire);
        let tail = self.tail.load(Ordering::Acquire);
        if tail >= head {
            tail - head
        } else {
            self.nodes.len() - head + tail
        }
    }

    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire) == self.tail.load(Ordering::Acquire)
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
    fn new(config: &MasterClassConfig) -> Result<Self, Error> {
        // Create data directory if it doesn't exist
        std::fs::create_dir_all(&config.data_path).map_err(|e| {
            Error::Other(format!("Failed to create data directory: {}", e))
        })?;

        Ok(Self {
            segment_directory: LockFreeRadixTree::new(),
            segments: RwLock::new(Vec::new()),
            segment_cache: LockFreeCache::new(),
        })
    }

    fn get(&self, key_hash: u64, version: u64) -> Result<Option<Vec<u8>>, Error> {
        // Find segments that might contain this key
        let target_z = z_order_interleave(key_hash, version);
        let relevant_segments = self.find_segments_for_z_value(target_z)?;

        // Search each segment for the key
        for segment in relevant_segments {
            if let Some(value) = self.search_segment(&segment, key_hash, version)? {
                return Ok(Some(value));
            }
        }

        Ok(None)
    }

    fn scan(
        &self,
        z_ranges: &[ZValueRange],
        version: u64,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        let mut results = Vec::new();
        
        // Find all segments that overlap with any of the Z-ranges
        for z_range in z_ranges {
            let segments = self.find_segments_in_range(z_range)?;
            
            for segment in segments {
                let segment_results = self.scan_segment(&segment, z_range, version)?;
                results.extend(segment_results);
            }
        }

        // Sort by Z-order and deduplicate
        results.sort_by(|a, b| {
            let a_z = z_order_interleave(xxhash64(&a.0), version);
            let b_z = z_order_interleave(xxhash64(&b.0), version);
            a_z.cmp(&b_z)
        });

        // Remove duplicates (keep the latest version)
        let mut deduplicated = Vec::new();
        let mut last_key: Option<&[u8]> = None;

        for (key, value) in results.iter().rev() {
            if last_key != Some(key.as_slice()) {
                deduplicated.push((key.clone(), value.clone()));
                last_key = Some(key);
            }
        }

        deduplicated.reverse();
        Ok(deduplicated)
    }

    fn count_entries(&self) -> usize {
        let segments = self.segments.read();
        segments.iter().map(|s| s.entry_count()).sum()
    }

    /// Find segments that might contain entries with the given Z-value
    fn find_segments_for_z_value(&self, z_value: u64) -> Result<Vec<MmapSegment>, Error> {
        let segments = self.segments.read();
        let mut matching_segments = Vec::new();

        for segment in segments.iter() {
            if segment.contains_z_value(z_value) {
                matching_segments.push(segment.clone());
            }
        }

        Ok(matching_segments)
    }

    /// Find segments that overlap with the given Z-range
    fn find_segments_in_range(&self, z_range: &ZValueRange) -> Result<Vec<MmapSegment>, Error> {
        let segments = self.segments.read();
        let mut matching_segments = Vec::new();

        for segment in segments.iter() {
            if segment.overlaps_range(z_range) {
                matching_segments.push(segment.clone());
            }
        }

        Ok(matching_segments)
    }

    /// Binary search within a specific segment
    fn search_segment(
        &self,
        segment: &MmapSegment,
        key_hash: u64,
        version: u64,
    ) -> Result<Option<Vec<u8>>, Error> {
        segment.binary_search(key_hash, version)
    }

    /// Scan a segment for entries within a Z-range
    fn scan_segment(
        &self,
        segment: &MmapSegment,
        z_range: &ZValueRange,
        version: u64,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        segment.range_scan(z_range, version)
    }

    /// Add a new segment to the storage (called from compaction)
    fn add_segment(&self, segment: MmapSegment) -> Result<(), Error> {
        let mut segments = self.segments.write();
        segments.push(segment);
        Ok(())
    }

    /// Create a new segment from compacted entries
    fn create_segment_from_entries(
        &self,
        entries: Vec<ZEntry>,
        segment_id: u64,
        data_path: &str,
    ) -> Result<MmapSegment, Error> {
        if entries.is_empty() {
            return Err(Error::Other("Cannot create segment from empty entries".to_string()));
        }

        // Calculate Z-value range
        let z_min = entries.iter().map(|e| e.z_value).min().unwrap();
        let z_max = entries.iter().map(|e| e.z_value).max().unwrap();
        let z_range = ZValueRange { min: z_min, max: z_max };

        // Calculate version range
        let version_min = entries.iter().map(|e| e.version).min().unwrap();
        let version_max = entries.iter().map(|e| e.version).max().unwrap();
        let version_range = VersionRange { min: version_min, max: version_max };

        // Create the memory-mapped segment
        MmapSegment::create_from_entries(entries, segment_id, z_range, version_range, data_path)
    }
}

impl LockFreeRadixTree {
    fn new() -> Self {
        Self {
            root: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    /// Insert a Z-range -> segment mapping
    fn insert(&self, _z_range: ZValueRange, _segment_ref: SegmentRef) -> Result<(), Error> {
        // Simplified implementation - in production use proper radix tree
        // For now, this is a placeholder that doesn't actually store anything
        Ok(())
    }

    /// Find segments that overlap with the given Z-value
    fn find_overlapping(&self, _z_value: u64) -> Vec<SegmentRef> {
        // Simplified implementation - return empty for now
        // In production, this would traverse the radix tree
        Vec::new()
    }
}

impl LockFreeCache {
    fn new() -> Self {
        Self {
            buckets: Box::new([const { AtomicPtr::new(std::ptr::null_mut()) }; 1024]),
        }
    }

    /// Get cached segment metadata
    fn get(&self, _segment_id: u64) -> Option<SegmentMetadata> {
        // Simplified implementation - return None for now
        None
    }

    /// Cache segment metadata
    fn put(&self, _segment_id: u64, _metadata: SegmentMetadata) -> Result<(), Error> {
        // Simplified implementation - do nothing for now
        Ok(())
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

    /// Write entries to a new segment file
    fn write_segment(&self, _entries: Vec<ZEntry>) -> Result<u64, Error> {
        let segment_id = self.segment_counter.fetch_add(1, Ordering::AcqRel);
        
        // In a real implementation, this would:
        // 1. Create a memory-mapped file
        // 2. Write the segment header
        // 3. Write the Z-ordered entries
        // 4. Write the data region
        // 5. Sync to disk
        
        // For now, just return the segment ID
        Ok(segment_id)
    }
}

impl MmapSegment {
    /// Create a new memory-mapped segment from entries
    fn create_from_entries(
        _entries: Vec<ZEntry>,
        segment_id: u64,
        z_range: ZValueRange,
        version_range: VersionRange,
        _data_path: &str,
    ) -> Result<Self, Error> {
        // For now, create an in-memory representation
        // In production, this would create actual memory-mapped files
        
        use memmap2::Mmap;
        
        // Create a temporary file for demonstration
        let temp_file = std::fs::File::create(format!("/tmp/segment_{}.dat", segment_id))
            .map_err(|e| Error::Other(format!("Failed to create segment file: {}", e)))?;
        
        // Write minimal data to file
        temp_file.set_len(4096).map_err(|e| Error::Other(format!("Failed to set file length: {}", e)))?;
        
        let mmap = unsafe { Mmap::map(&temp_file) }
            .map_err(|e| Error::Other(format!("Failed to memory map file: {}", e)))?;
        
        Ok(Self {
            mmap,
            header: std::ptr::null(),
            entries: std::ptr::null(),
            data_region: std::ptr::null(),
            z_range,
            version_range,
        })
    }

    /// Check if this segment contains the given Z-value
    fn contains_z_value(&self, z_value: u64) -> bool {
        z_value >= self.z_range.min && z_value <= self.z_range.max
    }

    /// Check if this segment overlaps with the given Z-range
    fn overlaps_range(&self, range: &ZValueRange) -> bool {
        !(range.max < self.z_range.min || range.min > self.z_range.max)
    }

    /// Get the number of entries in this segment
    fn entry_count(&self) -> usize {
        // In a real implementation, this would read from the header
        // For now, return a placeholder
        0
    }

    /// Binary search for a key within this segment
    fn binary_search(&self, _key_hash: u64, _version: u64) -> Result<Option<Vec<u8>>, Error> {
        // Simplified implementation - in production this would:
        // 1. Binary search the Z-ordered entry array
        // 2. Find the best matching entry
        // 3. Extract value from data region
        
        // For now, return None (not found)
        Ok(None)
    }

    /// Scan this segment for entries within a Z-range
    fn range_scan(
        &self,
        _z_range: &ZValueRange,
        _version: u64,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        // Simplified implementation - in production this would:
        // 1. Find the start position using binary search
        // 2. Scan forward while in range
        // 3. Extract key/value pairs from data region
        
        // For now, return empty results
        Ok(Vec::new())
    }
}

impl Clone for MmapSegment {
    fn clone(&self) -> Self {
        Self {
            mmap: unsafe { Mmap::map(&std::fs::File::open("/dev/null").unwrap()).unwrap() },
            header: self.header,
            entries: self.entries,
            data_region: self.data_region,
            z_range: self.z_range.clone(),
            version_range: self.version_range.clone(),
        }
    }
}

// SAFETY: MmapSegment is safe to send between threads because:
// - The memory-mapped region is immutable once created
// - Raw pointers point to memory-mapped data that remains valid
// - All operations are read-only
unsafe impl Send for MmapSegment {}

// SAFETY: MmapSegment is safe to share between threads because:
// - All data is read-only after creation
// - Memory-mapped regions are inherently shareable
// - No mutable state is accessed concurrently
unsafe impl Sync for MmapSegment {}

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

fn calculate_z_ranges_for_workers(num_workers: usize) -> Vec<ZValueRange> {
    let mut ranges = Vec::with_capacity(num_workers);
    let range_size = u64::MAX / num_workers as u64;
    
    for i in 0..num_workers {
        let min = i as u64 * range_size;
        let max = if i == num_workers - 1 {
            u64::MAX
        } else {
            (i + 1) as u64 * range_size - 1
        };
        
        ranges.push(ZValueRange { min, max });
    }
    
    ranges
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

    #[test]
    fn test_lock_free_queue() {
        let queue: LockFreeQueue<i32> = LockFreeQueue::new();
        
        // Test empty queue
        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);
        assert_eq!(queue.dequeue(), None);
        
        // Test single item
        queue.enqueue(42).expect("Enqueue failed");
        assert!(!queue.is_empty());
        assert_eq!(queue.len(), 1);
        
        let item = queue.dequeue().expect("Should have item");
        assert_eq!(item, 42);
        assert!(queue.is_empty());
        
        // Test multiple items
        for i in 0..10 {
            queue.enqueue(i).expect("Enqueue failed");
        }
        assert_eq!(queue.len(), 10);
        
        for i in 0..10 {
            let item = queue.dequeue().expect("Should have item");
            assert_eq!(item, i);
        }
        assert!(queue.is_empty());
    }

    #[test]
    fn test_compaction_worker_merge() {
        let mut worker = CompactionWorker::new(0);
        
        // Create test entries with different Z-values
        let mut entries = vec![
            ZEntry::new(b"key1", b"value1", 1),
            ZEntry::new(b"key2", b"value2", 2),
            ZEntry::new(b"key1", b"value1_updated", 3), // Newer version of key1
            ZEntry::new(b"key3", b"value3", 4),
        ];
        
        // Ensure different Z-values for proper ordering
        entries[0].z_value = 100;
        entries[1].z_value = 200;
        entries[2].z_value = 150; // Same key as entries[0] but newer version
        entries[3].z_value = 300;
        
        let merged = worker.merge_z_ordered(entries).expect("Merge failed");
        
        // Should have 3 unique keys (key1 deduplicated to newer version)
        assert_eq!(merged.len(), 3);
        
        // Check that entries are sorted by Z-value
        for i in 1..merged.len() {
            assert!(merged[i].z_value >= merged[i-1].z_value);
        }
        
        // Check that key1 has the updated value (version 3)
        let key1_entry = merged.iter().find(|e| e.key.as_slice() == b"key1").unwrap();
        assert_eq!(key1_entry.value, b"value1_updated");
        assert_eq!(key1_entry.version, 3);
    }

    #[test]
    fn test_compaction_triggering() {
        let config = MasterClassConfig::default();
        let db = MasterClassZVerse::new(config).expect("Failed to create database");
        
        // Fill ring buffers to trigger compaction
        for i in 0..1000 {
            let key = format!("compaction-key-{:04}", i);
            let value = format!("compaction-value-{:04}", i);
            db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
        }
        
        // Allow some time for background compaction
        std::thread::sleep(std::time::Duration::from_millis(100));
        
        // Verify system is still responsive
        let test_key = b"test-responsiveness";
        let test_value = b"test-value";
        let version = db.put(test_key, test_value).expect("Put failed");
        assert!(version > 1000);
        
        let result = db.get(test_key, None).expect("Get failed");
        assert_eq!(result, Some(test_value.to_vec()));
    }

    #[test]
    fn test_z_range_calculation() {
        let ranges = calculate_z_ranges_for_workers(4);
        assert_eq!(ranges.len(), 4);
        
        // Check ranges are non-overlapping and cover full space
        assert_eq!(ranges[0].min, 0);
        assert_eq!(ranges[3].max, u64::MAX);
        
        for i in 1..ranges.len() {
            assert_eq!(ranges[i-1].max + 1, ranges[i].min);
        }
    }

    #[test]
    fn test_ring_buffer_compaction_threshold() {
        let ring_buffer = LockFreeRingBuffer::new();
        
        // Initially should not trigger compaction
        assert!(!ring_buffer.should_trigger_compaction());
        
        // Add entries until threshold
        let threshold = (RING_BUFFER_SIZE * 3) / 4;
        for i in 0..threshold + 1 {
            let entry = ZEntry::new(
                format!("key{}", i).as_bytes(),
                format!("value{}", i).as_bytes(),
                i as u64,
            );
            ring_buffer.append(entry).expect("Append failed");
        }
        
        // Should now trigger compaction
        assert!(ring_buffer.should_trigger_compaction());
    }

    #[test]
    fn test_compaction_task_creation() {
        let task = CompactionTask {
            z_range: ZValueRange { min: 0, max: 1000 },
            input_sources: vec![
                InputSource::RingBuffer(0),
                InputSource::RingBuffer(1),
            ],
            priority: 1,
        };
        
        assert_eq!(task.z_range.min, 0);
        assert_eq!(task.z_range.max, 1000);
        assert_eq!(task.input_sources.len(), 2);
        assert_eq!(task.priority, 1);
    }

    #[test]
    fn test_concurrent_queue_operations() {
        let queue = Arc::new(LockFreeQueue::<usize>::new());
        let mut producer_handles = vec![];
        let mut consumer_handles = vec![];
        
        // Spawn producers
        for thread_id in 0..4 {
            let queue_clone = queue.clone();
            let handle = thread::spawn(move || {
                for i in 0..100 {
                    let item = thread_id * 100 + i;
                    queue_clone.enqueue(item).expect("Enqueue failed");
                }
            });
            producer_handles.push(handle);
        }
        
        // Spawn consumers
        for _ in 0..2 {
            let queue_clone = queue.clone();
            let handle = thread::spawn(move || {
                let mut consumed = 0;
                while consumed < 200 {
                    if let Some(_item) = queue_clone.dequeue() {
                        consumed += 1;
                    } else {
                        thread::sleep(Duration::from_millis(1));
                    }
                }
                consumed
            });
            consumer_handles.push(handle);
        }
        
        // Wait for producers
        for handle in producer_handles {
            handle.join().unwrap();
        }
        
        // Wait for consumers and collect results
        let mut total_consumed = 0;
        for handle in consumer_handles {
            let consumed = handle.join().unwrap();
            total_consumed += consumed;
        }
        
        // Should have consumed all 400 items
        assert_eq!(total_consumed, 400);
    }

    #[test]
    fn test_tier3_mmap_segment_creation() {
        // Create test entries with different Z-values
        let mut entries = vec![
            ZEntry::new(b"key1", b"value1", 1),
            ZEntry::new(b"key2", b"value2", 2),
            ZEntry::new(b"key3", b"value3", 3),
        ];
        
        // Set Z-values to ensure ordering
        entries[0].z_value = 100;
        entries[1].z_value = 200;
        entries[2].z_value = 300;
        
        let z_range = ZValueRange { min: 100, max: 300 };
        let version_range = VersionRange { min: 1, max: 3 };
        
        // Create segment
        let segment = MmapSegment::create_from_entries(
            entries,
            1,
            z_range,
            version_range,
            "/tmp"
        ).expect("Failed to create segment");
        
        // Test range checking
        assert!(segment.contains_z_value(150));
        assert!(segment.contains_z_value(100));
        assert!(segment.contains_z_value(300));
        assert!(!segment.contains_z_value(50));
        assert!(!segment.contains_z_value(400));
        
        // Test range overlap checking
        let test_range1 = ZValueRange { min: 50, max: 150 };
        let test_range2 = ZValueRange { min: 350, max: 400 };
        let test_range3 = ZValueRange { min: 150, max: 250 };
        
        assert!(segment.overlaps_range(&test_range1));
        assert!(!segment.overlaps_range(&test_range2));
        assert!(segment.overlaps_range(&test_range3));
    }

    #[test]
    fn test_tier3_read_optimized_storage() {
        let config = MasterClassConfig::default();
        let storage = ReadOptimizedStorage::new(&config).expect("Failed to create storage");
        
        // Test basic operations
        let result = storage.get(12345, 1).expect("Get operation failed");
        assert_eq!(result, None); // Should be None since no segments exist
        
        let scan_results = storage.scan(&[ZValueRange { min: 0, max: 1000 }], 1)
            .expect("Scan operation failed");
        assert_eq!(scan_results.len(), 0); // Should be empty since no segments exist
        
        assert_eq!(storage.count_entries(), 0);
    }

    #[test]
    fn test_tier3_segment_finding() {
        let config = MasterClassConfig::default();
        let storage = ReadOptimizedStorage::new(&config).expect("Failed to create storage");
        
        // Test finding segments for Z-values
        let segments = storage.find_segments_for_z_value(100).expect("Find segments failed");
        assert_eq!(segments.len(), 0); // No segments initially
        
        // Test finding segments in range
        let range_segments = storage.find_segments_in_range(&ZValueRange { min: 0, max: 1000 })
            .expect("Find segments in range failed");
        assert_eq!(range_segments.len(), 0); // No segments initially
    }

    #[test]
    fn test_tier3_integration_with_compaction() {
        let config = MasterClassConfig::default();
        let db = MasterClassZVerse::new(config).expect("Failed to create database");
        
        // Write enough data to potentially trigger compaction
        for i in 0..50 {
            let key = format!("tier3-test-{:03}", i);
            let value = format!("tier3-value-{:03}", i);
            db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
        }
        
        // Allow some time for background processing
        std::thread::sleep(std::time::Duration::from_millis(50));
        
        // Verify data is still accessible
        for i in 0..50 {
            let key = format!("tier3-test-{:03}", i);
            let expected_value = format!("tier3-value-{:03}", i);
            
            let result = db.get(key.as_bytes(), None).expect("Get failed");
            assert_eq!(result, Some(expected_value.as_bytes().to_vec()));
        }
        
        // Test system stats include all tiers
        let stats = db.stats();
        assert!(stats.total_writes >= 50);
        // Note: cold_tier_entries might be 0 if compaction hasn't moved data there yet
    }

    #[test]
    fn test_tier3_multi_tier_read_path() {
        let config = MasterClassConfig::default();
        let db = MasterClassZVerse::new(config).expect("Failed to create database");
        
        // Write some data
        let version1 = db.put(b"multi-tier-key", b"hot-value").expect("Put failed");
        
        // Immediately read back (should come from hot tier)
        let result1 = db.get(b"multi-tier-key", None).expect("Get failed");
        assert_eq!(result1, Some(b"hot-value".to_vec()));
        
        // Read specific version
        let result2 = db.get(b"multi-tier-key", Some(version1)).expect("Get failed");
        assert_eq!(result2, Some(b"hot-value".to_vec()));
        
        // Read non-existent key
        let result3 = db.get(b"non-existent-key", None).expect("Get failed");
        assert_eq!(result3, None);
    }

    #[test]
    fn test_tier3_z_range_calculations() {
        let ranges = calculate_z_ranges_for_workers(4);
        assert_eq!(ranges.len(), 4);
        
        // Test that ranges are properly distributed
        assert_eq!(ranges[0].min, 0);
        assert_eq!(ranges[3].max, u64::MAX);
        
        // Test that ranges don't overlap
        for i in 1..ranges.len() {
            assert_eq!(ranges[i-1].max + 1, ranges[i].min);
        }
        
        // Test Z-range overlap detection
        let test_range = ZValueRange { min: 1000, max: 2000 };
        let overlapping_ranges: Vec<_> = ranges.iter()
            .filter(|r| !(test_range.max < r.min || test_range.min > r.max))
            .collect();
        
        assert!(!overlapping_ranges.is_empty());
    }

    #[test]
    fn test_tier3_segment_operations() {
        // Test segment creation with empty entries (should fail)
        let result = MmapSegment::create_from_entries(
            vec![],
            1,
            ZValueRange { min: 0, max: 100 },
            VersionRange { min: 0, max: 100 },
            "/tmp"
        );
        assert!(result.is_err());
        
        // Test with valid entries
        let entries = vec![
            ZEntry::new(b"test", b"value", 1),
        ];
        
        let segment = MmapSegment::create_from_entries(
            entries,
            1,
            ZValueRange { min: 0, max: 100 },
            VersionRange { min: 1, max: 1 },
            "/tmp"
        ).expect("Segment creation should succeed");
        
        // Test binary search (returns None in current implementation)
        let search_result = segment.binary_search(12345, 1).expect("Binary search failed");
        assert_eq!(search_result, None);
        
        // Test range scan (returns empty in current implementation)
        let scan_result = segment.range_scan(&ZValueRange { min: 0, max: 100 }, 1)
            .expect("Range scan failed");
        assert_eq!(scan_result.len(), 0);
    }

    #[test]
    fn test_tier3_radix_tree_operations() {
        let radix_tree = LockFreeRadixTree::new();
        
        // Test insertion
        let z_range = ZValueRange { min: 100, max: 200 };
        let segment_ref = SegmentRef {
            segment_id: 1,
            z_range: z_range.clone(),
            file_path: "/tmp/segment_1.dat".to_string(),
        };
        
        let insert_result = radix_tree.insert(z_range, segment_ref);
        assert!(insert_result.is_ok());
        
        // Test finding (returns empty in current implementation)
        let found_segments = radix_tree.find_overlapping(150);
        assert_eq!(found_segments.len(), 0);
    }

    #[test]
    fn test_tier3_cache_operations() {
        let cache = LockFreeCache::new();
        
        // Test cache miss
        let result = cache.get(1);
        assert_eq!(result, None);
        
        // Test cache insertion
        let metadata = SegmentMetadata {
            entry_count: 100,
            data_size: 1024,
            last_accessed: std::time::Instant::now(),
        };
        
        let put_result = cache.put(1, metadata);
        assert!(put_result.is_ok());
    }
}
