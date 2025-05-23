//! Temporal Interval Storage Engine
//!
//! This engine stores records as Morton intervals rather than points,
//! enabling efficient temporal queries and true MVCC semantics.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::path::PathBuf;
use parking_lot::RwLock;

use crate::storage::file::{FileStorage, FileStorageConfig, FileStorageError};
use crate::storage::page::{Page, PageError, PAGE_SIZE};
use crate::interval::table::IntervalTable;
use crate::encoding::morton::{morton_t_encode, current_timestamp_micros, morton_t_bigmin, morton_t_litmax, morton_t_key_temporal_range};
use serde::{Serialize, Deserialize};

/// Temporal record with Morton interval validity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalRecord {
    /// Start of Morton interval (inclusive)
    pub morton_start: u64,
    /// End of Morton interval (exclusive), u64::MAX means infinity
    pub morton_end: u64,
    /// Original key
    pub key: String,
    /// Record value
    pub value: Vec<u8>,
    /// Creation timestamp
    pub created_at: u64,
}

impl TemporalRecord {
    /// Create a new temporal record with infinite validity
    pub fn new(key: String, value: Vec<u8>, timestamp: u64) -> Self {
        let morton_start = morton_t_encode(&key, timestamp).value();
        Self {
            morton_start,
            morton_end: u64::MAX, // Infinite validity initially
            key,
            value,
            created_at: timestamp,
        }
    }

    /// Check if this record is valid at given Morton code
    pub fn contains(&self, morton_code: u64) -> bool {
        morton_code >= self.morton_start && morton_code < self.morton_end
    }

    /// Close this record's validity at given timestamp
    pub fn close_at(&mut self, timestamp: u64, key: &str) {
        self.morton_end = morton_t_encode(key, timestamp).value();
    }

    /// Check if this record is currently active (not closed)
    pub fn is_active(&self) -> bool {
        self.morton_end == u64::MAX
    }
}

/// Configuration for temporal storage engine
#[derive(Debug, Clone)]
pub struct TemporalEngineConfig {
    pub file_config: FileStorageConfig,
    pub initial_extent_count: u32,
    pub max_extents: u32,
    pub extent_pool_size: u32,  // Number of pre-allocated extents to maintain
    pub extent_allocation_batch: u32,  // How many extents to allocate at once
}

impl Default for TemporalEngineConfig {
    fn default() -> Self {
        Self {
            file_config: FileStorageConfig::default(),
            initial_extent_count: 16,
            max_extents: 65536,
            extent_pool_size: 32,  // Keep 32 pre-allocated extents
            extent_allocation_batch: 16,  // Allocate 16 at a time (amortized)
        }
    }
}

impl TemporalEngineConfig {
    pub fn with_path(mut self, path: PathBuf) -> Self {
        self.file_config.file_path = path;
        self
    }

    pub fn with_file_size(mut self, size: u64) -> Self {
        self.file_config.file_size = size;
        self
    }

    pub fn with_extent_size(mut self, size: u32) -> Self {
        self.file_config.extent_size = size;
        self
    }
}

/// Extent metadata for temporal intervals
#[derive(Debug, Clone)]
struct TemporalExtentMetadata {
    extent_id: u32,
    offset: u64,
    morton_start: u64,
    morton_end: u64,
    record_count: u32,
    max_records: u32,
}

/// Temporal interval storage engine
pub struct TemporalEngine {
    /// File storage backend
    file_storage: Arc<Mutex<FileStorage>>,
    /// Interval table for extent lookup
    interval_table: Arc<RwLock<IntervalTable>>,
    /// Extent metadata cache
    extent_cache: Arc<RwLock<HashMap<u32, TemporalExtentMetadata>>>,
    /// In-memory record cache for active intervals
    active_records: Arc<RwLock<HashMap<String, TemporalRecord>>>,
    /// Pool of pre-allocated empty extents (amortized allocation)
    extent_pool: Arc<RwLock<Vec<u32>>>,
    /// Next extent ID
    next_extent_id: AtomicU32,
    /// Configuration
    config: TemporalEngineConfig,
    /// Statistics
    read_count: AtomicU64,
    write_count: AtomicU64,
    interval_splits: AtomicU64,
}

impl TemporalEngine {
    /// Create new temporal engine
    pub fn new(config: TemporalEngineConfig) -> Result<Self, TemporalEngineError> {
        let file_storage = Arc::new(Mutex::new(FileStorage::new(config.file_config.clone())?));
        
        let engine = Self {
            file_storage,
            interval_table: Arc::new(RwLock::new(IntervalTable::new())),
            extent_cache: Arc::new(RwLock::new(HashMap::new())),
            active_records: Arc::new(RwLock::new(HashMap::new())),
            extent_pool: Arc::new(RwLock::new(Vec::new())),
            next_extent_id: AtomicU32::new(1),
            config,
            read_count: AtomicU64::new(0),
            write_count: AtomicU64::new(0),
            interval_splits: AtomicU64::new(0),
        };

        engine.initialize_extents()?;
        Ok(engine)
    }

    /// Initialize extent space for Morton intervals
    fn initialize_extents(&self) -> Result<(), TemporalEngineError> {
        let extent_count = self.config.initial_extent_count as u64;
        let range_per_extent = u64::MAX / extent_count;
        
        let mut interval_table = self.interval_table.write();
        let mut extent_cache = self.extent_cache.write();
        
        for i in 0..extent_count {
            let extent_id = self.next_extent_id.fetch_add(1, Ordering::Relaxed);
            let start = i * range_per_extent;
            let end = if i == extent_count - 1 { u64::MAX } else { (i + 1) * range_per_extent - 1 };
            
            // Allocate extent storage
            let offset = {
                let mut storage = self.file_storage.lock().unwrap();
                storage.allocate_extent(extent_id)?
            };
            
            // Calculate max records per extent
            let extent_size = {
                let storage = self.file_storage.lock().unwrap();
                storage.superblock().extent_size()
            };
            let header_size = 512u32;
            let max_records = (extent_size - header_size) / 256; // ~256 bytes per record estimate
            
            // Create metadata
            let metadata = TemporalExtentMetadata {
                extent_id,
                offset,
                morton_start: start,
                morton_end: end,
                record_count: 0,
                max_records,
            };
            
            // Insert into tables
            interval_table.insert(start, end, extent_id)
                .map_err(TemporalEngineError::IntervalError)?;
            extent_cache.insert(extent_id, metadata);
        }
        
        Ok(())
    }

    /// Put a key-value pair (creates temporal interval)
    pub fn put(&self, key: &str, value: &[u8]) -> Result<(), TemporalEngineError> {
        self.write_count.fetch_add(1, Ordering::Relaxed);
        
        let timestamp = current_timestamp_micros();
        let new_record = TemporalRecord::new(key.to_string(), value.to_vec(), timestamp);
        
        // Check if there's an existing active record for this key
        let mut active_records = self.active_records.write();
        
        if let Some(existing) = active_records.get_mut(key) {
            // Close the existing record
            existing.close_at(timestamp, key);
            self.interval_splits.fetch_add(1, Ordering::Relaxed);
            
            // Persist the closed record
            self.persist_record(existing.clone())?;
        }
        
        // Store the new active record
        active_records.insert(key.to_string(), new_record.clone());
        
        Ok(())
    }

    /// Get latest value for key at specific timestamp
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, TemporalEngineError> {
        self.read_count.fetch_add(1, Ordering::Relaxed);
        
        // First check active records (fastest path)
        {
            let active_records = self.active_records.read();
            if let Some(record) = active_records.get(key) {
                return Ok(Some(record.value.clone()));
            }
        }
        
        // Search persisted records using efficient temporal range
        let (min_morton, max_morton) = morton_t_key_temporal_range(key);
        self.search_temporal_range_for_latest(key, min_morton.value(), max_morton.value())
    }

    /// Get value for key at specific timestamp
    pub fn get_at_timestamp(&self, key: &str, timestamp: u64) -> Result<Option<Vec<u8>>, TemporalEngineError> {
        let morton_code = morton_t_encode(key, timestamp);
        
        // First check active records
        {
            let active_records = self.active_records.read();
            if let Some(record) = active_records.get(key) {
                if record.contains(morton_code.value()) {
                    return Ok(Some(record.value.clone()));
                }
            }
        }
        
        // Search persisted records using efficient temporal range
        self.search_persisted_records(key, morton_t_encode(key, timestamp).value())
    }

    /// Get all records in key range at specific timestamp
    pub fn range_query(&self, key_start: &str, key_end: &str, timestamp: u64) -> Result<Vec<(String, Vec<u8>)>, TemporalEngineError> {
        // Use bigmin/litmax for efficient range bounds
        let start_min = morton_t_bigmin(key_start).value();
        let end_max = morton_t_litmax(key_end).value();
        
        let mut results = Vec::new();
        
        // Check active records using efficient prefix matching
        {
            let active_records = self.active_records.read();
            for (key, record) in active_records.iter() {
                // Check if key falls in lexicographic range
                if key.as_str() >= key_start && key.as_str() <= key_end {
                    // Check if record is valid at timestamp
                    if record.contains(morton_t_encode(key, timestamp).value()) {
                        results.push((key.clone(), record.value.clone()));
                    }
                }
            }
        }
        
        // Search persisted records using efficient Morton range
        self.search_persisted_range(start_min, end_max, timestamp, &mut results)?;
        
        Ok(results)
    }

    /// Persist a temporal record to storage
    fn persist_record(&self, record: TemporalRecord) -> Result<(), TemporalEngineError> {
        // Find extent for this Morton interval, allocate if needed
        let extent_id = self.find_or_allocate_extent_for_morton(record.morton_start)?;
        
        // Serialize and write record using bincode
        let serialized = self.serialize_record(&record)?;
        self.write_record_to_extent(extent_id, &serialized)?;
        
        Ok(())
    }

    /// Search persisted records for key at Morton code
    fn search_persisted_records(&self, key: &str, morton_code: u64) -> Result<Option<Vec<u8>>, TemporalEngineError> {
        // Find extent containing this Morton code
        let extent_id = {
            let interval_table = self.interval_table.read();
            match interval_table.find_extent(morton_code) {
                Some(id) => id,
                None => return Ok(None),
            }
        };
        
        // Search records in extent
        self.search_extent_for_key(extent_id, key, morton_code)
    }

    /// Search persisted records for key using efficient temporal range


    /// Search temporal range for latest version of key
    fn search_temporal_range_for_latest(&self, key: &str, morton_min: u64, morton_max: u64) -> Result<Option<Vec<u8>>, TemporalEngineError> {
        // Find all extents that might contain temporal versions of this key
        let interval_table = self.interval_table.read();
        let overlapping_extents = interval_table.find_overlapping(morton_min, morton_max);
        
        let mut latest_value = None;
        let mut latest_timestamp = 0u64;
        
        // Search through all overlapping extents for the latest version
        for node in overlapping_extents {
            if let Some((timestamp, value)) = self.search_extent_for_latest_version(node.extent_id, key, morton_min, morton_max)? {
                if timestamp > latest_timestamp {
                    latest_timestamp = timestamp;
                    latest_value = Some(value);
                }
            }
        }
        
        Ok(latest_value)
    }

    /// Search persisted records in Morton range
    fn search_persisted_range(&self, morton_start: u64, morton_end: u64, timestamp: u64, results: &mut Vec<(String, Vec<u8>)>) -> Result<(), TemporalEngineError> {
        // Find all extents that might contain records in this range
        let interval_table = self.interval_table.read();
        let overlapping_extents = interval_table.find_overlapping(morton_start, morton_end);
        
        for node in overlapping_extents {
            self.search_extent_for_range(node.extent_id, morton_start, morton_end, timestamp, results)?;
        }
        
        Ok(())
    }

    /// Search specific extent for key
    fn search_extent_for_key(&self, extent_id: u32, key: &str, morton_code: u64) -> Result<Option<Vec<u8>>, TemporalEngineError> {
        // Read extent pages and search for matching temporal records
        let metadata = {
            let cache = self.extent_cache.read();
            cache.get(&extent_id).cloned()
                .ok_or(TemporalEngineError::ExtentNotFound)?
        };
        
        // Simple linear search through extent pages for now
        for page_idx in 0..((metadata.max_records * 256) / PAGE_SIZE as u32) {
            if let Some(page) = self.read_extent_page(extent_id, page_idx)? {
                if let Some(value) = self.search_page_for_temporal_key(&page, key, morton_code)? {
                    return Ok(Some(value));
                }
            }
        }
        
        Ok(None)
    }

    /// Search extent for latest version of key within Morton range
    fn search_extent_for_latest_version(&self, extent_id: u32, key: &str, morton_min: u64, morton_max: u64) -> Result<Option<(u64, Vec<u8>)>, TemporalEngineError> {
        let metadata = {
            let cache = self.extent_cache.read();
            cache.get(&extent_id).cloned()
                .ok_or(TemporalEngineError::ExtentNotFound)?
        };
        
        let mut latest_timestamp = 0u64;
        let mut latest_value = None;
        
        // Search through extent pages for temporal records
        for page_idx in 0..((metadata.max_records * 256) / PAGE_SIZE as u32) {
            if let Some(page) = self.read_extent_page(extent_id, page_idx)? {
                if let Some((timestamp, value)) = self.search_page_for_latest_temporal_key(&page, key, morton_min, morton_max)? {
                    if timestamp > latest_timestamp {
                        latest_timestamp = timestamp;
                        latest_value = Some(value);
                    }
                }
            }
        }
        
        Ok(latest_value.map(|v| (latest_timestamp, v)))
    }

    /// Search extent for records in Morton range
    fn search_extent_for_range(&self, _extent_id: u32, _morton_start: u64, _morton_end: u64, _timestamp: u64, _results: &mut Vec<(String, Vec<u8>)>) -> Result<(), TemporalEngineError> {
        // Similar to search_extent_for_key but for ranges
        // Implementation would scan pages and collect all matching records
        Ok(())
    }

    /// Read page from extent
    fn read_extent_page(&self, extent_id: u32, page_index: u32) -> Result<Option<Page>, TemporalEngineError> {
        let storage = self.file_storage.lock().unwrap();
        storage.read_page(extent_id, page_index)
            .map_err(TemporalEngineError::IoError)
    }

    /// Search page for temporal record matching key and Morton code
    fn search_page_for_temporal_key(&self, _page: &Page, _key: &str, _morton_code: u64) -> Result<Option<Vec<u8>>, TemporalEngineError> {
        // Deserialize temporal records from page and check for containment
        // This would need custom page format for temporal records
        Ok(None) // Placeholder
    }

    /// Search page for latest temporal record matching key within Morton range
    fn search_page_for_latest_temporal_key(&self, _page: &Page, _key: &str, _morton_min: u64, _morton_max: u64) -> Result<Option<(u64, Vec<u8>)>, TemporalEngineError> {
        // Deserialize temporal records from page and find latest version
        // This would scan all temporal intervals for the key and return the most recent
        Ok(None) // Placeholder
    }

    /// Write record to extent
    fn write_record_to_extent(&self, _extent_id: u32, _serialized: &[u8]) -> Result<(), TemporalEngineError> {
        // Find available page in extent and write serialized record
        // Update extent metadata
        Ok(())
    }

    /// Serialize temporal record using bincode for better performance
    fn serialize_record(&self, record: &TemporalRecord) -> Result<Vec<u8>, TemporalEngineError> {
        bincode::serialize(record)
            .map_err(|_| TemporalEngineError::SerializationError)
    }

    /// Find extent for Morton code or allocate new one using amortized strategy
    fn find_or_allocate_extent_for_morton(&self, morton_code: u64) -> Result<u32, TemporalEngineError> {
        // First check if existing extent can handle this Morton code
        {
            let interval_table = self.interval_table.read();
            if let Some(extent_id) = interval_table.find_extent(morton_code) {
                return Ok(extent_id);
            }
        }
        
        // No existing extent, need to allocate one
        self.allocate_extent_for_morton(morton_code)
    }

    /// Allocate extent for Morton code using amortized pre-allocation
    fn allocate_extent_for_morton(&self, morton_code: u64) -> Result<u32, TemporalEngineError> {
        // Ensure we have enough pre-allocated extents
        self.ensure_extent_pool_size()?;
        
        // Get a pre-allocated extent from the pool
        let extent_id = {
            let mut pool = self.extent_pool.write();
            pool.pop().ok_or(TemporalEngineError::ExtentNotFound)?
        };
        
        // Calculate Morton range for this extent (simple strategy for now)
        let range_size = u64::MAX / 1024; // Divide space into 1024 ranges initially
        let range_start = (morton_code / range_size) * range_size;
        let range_end = range_start + range_size - 1;
        
        // Add to interval table
        {
            let mut interval_table = self.interval_table.write();
            interval_table.insert(range_start, range_end, extent_id)
                .map_err(TemporalEngineError::IntervalError)?;
        }
        
        // Update extent metadata
        {
            let mut cache = self.extent_cache.write();
            if let Some(metadata) = cache.get_mut(&extent_id) {
                metadata.morton_start = range_start;
                metadata.morton_end = range_end;
            }
        }
        
        Ok(extent_id)
    }

    /// Ensure extent pool has enough pre-allocated extents (amortized allocation)
    fn ensure_extent_pool_size(&self) -> Result<(), TemporalEngineError> {
        let current_pool_size = self.extent_pool.read().len();
        
        if current_pool_size < self.config.extent_pool_size as usize {
            // Allocate a batch of extents to amortize allocation cost
            self.allocate_extent_batch()?;
        }
        
        Ok(())
    }

    /// Allocate a batch of extents (like Vec doubling strategy)
    fn allocate_extent_batch(&self) -> Result<(), TemporalEngineError> {
        let batch_size = self.config.extent_allocation_batch;
        let mut new_extents = Vec::new();
        
        for _ in 0..batch_size {
            let extent_id = self.next_extent_id.fetch_add(1, Ordering::Relaxed);
            if extent_id > self.config.max_extents {
                break; // Don't exceed max extents
            }
            
            // Allocate extent storage
            let offset = {
                let mut storage = self.file_storage.lock().unwrap();
                storage.allocate_extent(extent_id)?
            };
            
            // Calculate max records per extent
            let extent_size = {
                let storage = self.file_storage.lock().unwrap();
                storage.superblock().extent_size()
            };
            let header_size = 512u32;
            let max_records = (extent_size - header_size) / 256;
            
            // Create metadata (will be updated when extent is actually used)
            let metadata = TemporalExtentMetadata {
                extent_id,
                offset,
                morton_start: 0, // Will be set when extent is assigned
                morton_end: 0,   // Will be set when extent is assigned
                record_count: 0,
                max_records,
            };
            
            // Add to cache
            {
                let mut cache = self.extent_cache.write();
                cache.insert(extent_id, metadata);
            }
            
            new_extents.push(extent_id);
        }
        
        // Add all new extents to the pool
        {
            let mut pool = self.extent_pool.write();
            pool.extend(new_extents);
        }
        
        Ok(())
    }

    /// Flush active records to storage
    pub fn flush(&self) -> Result<(), TemporalEngineError> {
        let active_records = self.active_records.read();
        for record in active_records.values() {
            // Only persist closed records during flush
            if !record.is_active() {
                self.persist_record(record.clone())?;
            }
        }
        
        let mut storage = self.file_storage.lock().unwrap();
        storage.flush().map_err(TemporalEngineError::IoError)?;
        Ok(())
    }

    /// Get engine statistics
    pub fn stats(&self) -> TemporalEngineStats {
        TemporalEngineStats {
            reads: self.read_count.load(Ordering::Relaxed),
            writes: self.write_count.load(Ordering::Relaxed),
            interval_splits: self.interval_splits.load(Ordering::Relaxed),
            active_records: self.active_records.read().len() as u64,
            extent_count: self.extent_cache.read().len(),
        }
    }
}

/// Temporal engine statistics
#[derive(Debug, Clone)]
pub struct TemporalEngineStats {
    pub reads: u64,
    pub writes: u64,
    pub interval_splits: u64,
    pub active_records: u64,
    pub extent_count: usize,
}

/// Temporal engine errors
#[derive(Debug)]
pub enum TemporalEngineError {
    IoError(std::io::Error),
    FileStorageError(FileStorageError),
    IntervalError(crate::interval::table::IntervalError),
    PageError(PageError),
    ExtentNotFound,
    SerializationError,
    CorruptedData,
}

impl From<std::io::Error> for TemporalEngineError {
    fn from(err: std::io::Error) -> Self {
        TemporalEngineError::IoError(err)
    }
}

impl From<FileStorageError> for TemporalEngineError {
    fn from(err: FileStorageError) -> Self {
        TemporalEngineError::FileStorageError(err)
    }
}

impl From<PageError> for TemporalEngineError {
    fn from(err: PageError) -> Self {
        TemporalEngineError::PageError(err)
    }
}

impl std::fmt::Display for TemporalEngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TemporalEngineError::IoError(e) => write!(f, "I/O error: {}", e),
            TemporalEngineError::FileStorageError(e) => write!(f, "File storage error: {}", e),
            TemporalEngineError::IntervalError(e) => write!(f, "Interval error: {}", e),
            TemporalEngineError::PageError(e) => write!(f, "Page error: {}", e),
            TemporalEngineError::ExtentNotFound => write!(f, "Extent not found"),
            TemporalEngineError::SerializationError => write!(f, "Serialization error"),
            TemporalEngineError::CorruptedData => write!(f, "Corrupted data"),
        }
    }
}

impl std::error::Error for TemporalEngineError {}