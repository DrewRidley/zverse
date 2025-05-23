//! File-backed key-value storage engine for IMEC
//!
//! This module combines the high-level StorageEngine with FileStorage
//! to provide persistent key-value operations with Morton-T encoding,
//! spatial-temporal locality, and mmap-based file persistence.

use crate::encoding::{morton_t_encode, MortonTCode, current_timestamp_micros};
use crate::interval::{IntervalTable, IntervalError};
use crate::storage::file::{FileStorage, FileStorageConfig, FileStorageError, FileStorageStats};
use crate::storage::page::{Page, PageType, Slot, PageError, PAGE_SIZE};
use crate::storage::extent::{ExtentError};

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use std::path::PathBuf;
use parking_lot::RwLock;

/// Configuration for file-backed storage engine
#[derive(Debug, Clone)]
pub struct FileEngineConfig {
    /// File storage configuration
    pub file_config: FileStorageConfig,
    /// Maximum number of extents
    pub max_extents: u32,
    /// Cache size for hot extents (in memory)
    pub cache_size: usize,
    /// Morton range padding for new intervals
    morton_padding: u64,
}

impl Default for FileEngineConfig {
    fn default() -> Self {
        Self {
            file_config: FileStorageConfig::default(),
            max_extents: 10000,
            cache_size: 100,
            morton_padding: 100_000_000, // Much larger padding for better key grouping
        }
    }
}

impl FileEngineConfig {
    /// Create config with custom file path
    pub fn with_path<P: Into<PathBuf>>(path: P) -> Self {
        let mut config = Self::default();
        config.file_config.file_path = path.into();
        config
    }

    /// Set file size
    pub fn with_file_size(mut self, size: u64) -> Self {
        self.file_config.file_size = size;
        self
    }

    /// Set extent size
    pub fn with_extent_size(mut self, size: u32) -> Self {
        self.file_config.extent_size = size;
        self
    }
}

/// File-backed storage engine with key-value operations
pub struct FileEngine {
    /// File storage backend
    file_storage: Arc<Mutex<FileStorage>>,
    /// Interval table mapping Morton ranges to extents  
    interval_table: Arc<RwLock<IntervalTable>>,
    /// Cache of extent metadata
    extent_cache: Arc<RwLock<HashMap<u32, ExtentMetadata>>>,
    /// Next extent ID to allocate
    next_extent_id: AtomicU32,
    /// Configuration
    config: FileEngineConfig,
    /// Read counter
    read_count: AtomicU64,
    /// Write counter
    write_count: AtomicU64,
}

/// Metadata for cached extents
#[derive(Debug, Clone)]
struct ExtentMetadata {
    extent_id: u32,
    offset: u64,
    morton_start: u64,
    morton_end: u64,
    page_count: u32,
    allocated_pages: u32,
}

impl FileEngine {
    /// Create a new file-backed storage engine
    pub fn new(config: FileEngineConfig) -> Result<Self, FileEngineError> {
        let file_storage = Arc::new(Mutex::new(FileStorage::new(config.file_config.clone())?));
        
        let engine = Self {
            file_storage,
            interval_table: Arc::new(RwLock::new(IntervalTable::new())),
            extent_cache: Arc::new(RwLock::new(HashMap::new())),
            next_extent_id: AtomicU32::new(1),
            config,
            read_count: AtomicU64::new(0),
            write_count: AtomicU64::new(0),
        };
        
        // Pre-allocate 4 large extents covering the entire Morton space
        engine.initialize_large_extents()?;
        
        Ok(engine)
    }

    /// Initialize 4 large extents covering the entire Morton space
    fn initialize_large_extents(&self) -> Result<(), FileEngineError> {
        // Divide the full u64 range into 4 large extents
        let ranges = [
            (0u64, u64::MAX / 4),
            (u64::MAX / 4 + 1, u64::MAX / 2),
            (u64::MAX / 2 + 1, u64::MAX / 4 * 3),
            (u64::MAX / 4 * 3 + 1, u64::MAX),
        ];

        for (start, end) in ranges.iter() {
            let extent_id = self.next_extent_id.fetch_add(1, Ordering::Relaxed);
            let extent_offset = {
                let mut storage = self.file_storage.lock().unwrap();
                storage.allocate_extent(extent_id)?
            };
            
            // Insert into interval table
            {
                let mut interval_table = self.interval_table.write();
                interval_table.insert(*start, *end, extent_id)?;
            }
            
            // Cache the extent metadata
            let metadata = ExtentMetadata {
                extent_id,
                offset: extent_offset,
                morton_start: *start,
                morton_end: *end,
                page_count: self.calculate_page_count_for_extent(),
                allocated_pages: 0,
            };
            {
                let mut cache = self.extent_cache.write();
                cache.insert(extent_id, metadata);
            }
        }

        Ok(())
    }

    /// Store a key-value pair
    pub fn put(&self, key: &str, value: &[u8]) -> Result<(), FileEngineError> {
        self.write_count.fetch_add(1, Ordering::Relaxed);
        
        let timestamp = current_timestamp_micros();
        let morton_code = morton_t_encode(key, timestamp);
        
        // Find or create extent for this Morton code
        let extent_id = self.find_or_create_extent_for_morton(morton_code.value())?;
        
        // Find or allocate a page within the extent
        let page_index = self.find_or_allocate_page_in_extent(extent_id, morton_code.value())?;
        
        // Read the page, modify it, and write it back
        let mut page = {
            let storage = self.file_storage.lock().unwrap();
            storage.read_page(extent_id, page_index)?
                .unwrap_or_else(|| Page::new(PageType::Leaf))
        };
        
        self.insert_into_page(&mut page, key, value, morton_code)?;
        
        // Write the modified page back
        {
            let mut storage = self.file_storage.lock().unwrap();
            storage.write_page(extent_id, page_index, &page)?;
        }
        
        Ok(())
    }

    /// Retrieve a value for a key
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, FileEngineError> {
        self.read_count.fetch_add(1, Ordering::Relaxed);
        
        let current_time = current_timestamp_micros();
        
        // Search backwards from current time to find latest version
        // Use a small window and simple linear search for reliability
        let max_offset = 100_000u64; // 100ms window should be enough
        
        for offset in 0..max_offset {
            let search_timestamp = current_time.saturating_sub(offset);
            let morton_code = morton_t_encode(key, search_timestamp);
            
            if let Some(value) = self.get_at_morton(key, morton_code.value())? {
                return Ok(Some(value));
            }
            
            // Skip ahead for efficiency after first few microseconds
            if offset > 1000 && offset % 100 != 0 {
                continue;
            }
        }
        
        Ok(None)
    }

    /// Get value at specific Morton code
    fn get_at_morton(&self, key: &str, morton_code: u64) -> Result<Option<Vec<u8>>, FileEngineError> {
        // Find extent containing this Morton code
        let extent_id = {
            let interval_table = self.interval_table.read();
            match interval_table.find_extent(morton_code) {
                Some(id) => id,
                None => return Ok(None),
            }
        };
        
        // Find page within extent
        let page_index = match self.find_page_for_morton_in_extent(extent_id, morton_code)? {
            Some(idx) => idx,
            None => return Ok(None),
        };
        
        // Read the page
        let page = {
            let storage = self.file_storage.lock().unwrap();
            match storage.read_page(extent_id, page_index)? {
                Some(page) => page,
                None => return Ok(None),
            }
        };
        
        // Search for key in page
        self.search_page_for_key(&page, key, morton_code)
    }

    /// Find or create extent for Morton code
    fn find_or_create_extent_for_morton(&self, morton_code: u64) -> Result<u32, FileEngineError> {
        // Find existing extent that contains this Morton code
        let extent_id = {
            let interval_table = self.interval_table.read();
            match interval_table.find_extent(morton_code) {
                Some(id) => id,
                None => return Err(FileEngineError::ExtentNotFound),
            }
        };
        
        // Check if extent is full and needs splitting
        if self.is_extent_full(extent_id)? {
            // Split the extent and determine which half contains our Morton code
            let _new_extent_id = self.split_extent(extent_id)?;
            
            // Return the extent that contains our Morton code
            let interval_table = self.interval_table.read();
            if let Some(id) = interval_table.find_extent(morton_code) {
                Ok(id)
            } else {
                Err(FileEngineError::ExtentNotFound)
            }
        } else {
            Ok(extent_id)
        }
    }

    /// Find or allocate page in extent for Morton code
    fn find_or_allocate_page_in_extent(&self, extent_id: u32, morton_code: u64) -> Result<u32, FileEngineError> {
        // First try to find existing page
        if let Some(page_index) = self.find_page_for_morton_in_extent(extent_id, morton_code)? {
            return Ok(page_index);
        }
        
        // Allocate new page
        let mut metadata = {
            let cache = self.extent_cache.read();
            cache.get(&extent_id).cloned()
                .ok_or(FileEngineError::ExtentNotFound)?
        };
        
        if metadata.allocated_pages >= metadata.page_count {
            return Err(FileEngineError::ExtentFull);
        }
        
        let page_index = metadata.allocated_pages;
        metadata.allocated_pages += 1;
        
        Ok(page_index)
    }

    /// Find page for Morton code within extent
    fn find_page_for_morton_in_extent(&self, extent_id: u32, _morton_code: u64) -> Result<Option<u32>, FileEngineError> {
        // For now, simple implementation - just check if any pages exist
        let metadata = {
            let cache = self.extent_cache.read();
            cache.get(&extent_id).cloned()
        };
        
        if let Some(metadata) = metadata {
            if metadata.allocated_pages > 0 {
                // Linear search through allocated pages (can be optimized later)
                for page_index in 0..metadata.allocated_pages {
                    let storage = self.file_storage.lock().unwrap();
                    if let Some(_page) = storage.read_page(extent_id, page_index)? {
                        return Ok(Some(page_index));
                    }
                }
            }
        }
        Ok(None)
    }

    /// Insert key-value pair into page
    fn insert_into_page(&self, page: &mut Page, key: &str, value: &[u8], morton_code: MortonTCode) -> Result<(), FileEngineError> {
        // Allocate space for key and value in blob area
        let key_bytes = key.as_bytes();
        let total_blob_size = key_bytes.len() + value.len();
        
        let blob_offset = page.allocate_blob(total_blob_size)
            .map_err(FileEngineError::PageError)?;
        
        // Write key to blob area
        let key_offset = blob_offset;
        page.write_blob(key_offset, key_bytes)
            .map_err(FileEngineError::PageError)?;
        
        // Write value to blob area  
        let value_offset = blob_offset + key_bytes.len() as u32;
        page.write_blob(value_offset, value)
            .map_err(FileEngineError::PageError)?;
        
        // Create slot with key hash as fingerprint (not Morton code)
        let key_hash = self.hash_key(key);
        let slot = Slot::new_leaf(
            key_hash,
            key_offset,
            key_bytes.len() as u16,
            value_offset,
            value.len() as u16,
        );
        
        // Insert slot into page
        page.insert_slot(slot)
            .map_err(FileEngineError::PageError)?;
        
        // Update page checksum
        page.update_checksum();
        
        Ok(())
    }

    /// Search page for key
    fn search_page_for_key(&self, page: &Page, key: &str, morton_code: u64) -> Result<Option<Vec<u8>>, FileEngineError> {
        // Search for slot with matching key hash fingerprint
        let key_hash = self.hash_key(key);
        let slot_index = match page.search_slot(key_hash) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        
        let slot = page.get_slot(slot_index).unwrap();
        
        // Read key from blob area and compare
        let stored_key = page.read_key(slot.key_offset, slot.key_length)
            .ok_or(FileEngineError::CorruptedData)?;
        
        if stored_key == key.as_bytes() {
            // Key matches - read value
            let value = page.read_value(slot.child_or_value_ptr as u32, slot.value_length)
                .ok_or(FileEngineError::CorruptedData)?;
            Ok(Some(value.to_vec()))
        } else {
            // Hash collision - key doesn't match
            Ok(None)
        }
    }

    /// Calculate page count for extent
    fn calculate_page_count_for_extent(&self) -> u32 {
        let extent_size = {
            let storage = self.file_storage.lock().unwrap();
            storage.superblock().extent_size()
        };
        let header_size = 512; // Extent header size
        (extent_size - header_size) / PAGE_SIZE as u32
    }

    /// Flush all changes to disk
    pub fn flush(&self) -> Result<(), FileEngineError> {
        let mut storage = self.file_storage.lock().unwrap();
        storage.flush()?;
        Ok(())
    }

    /// Get storage statistics
    pub fn stats(&self) -> FileEngineStats {
        let file_stats = {
            let storage = self.file_storage.lock().unwrap();
            storage.stats()
        };
        FileEngineStats {
            reads: self.read_count.load(Ordering::Relaxed),
            writes: self.write_count.load(Ordering::Relaxed),
            cache_hits: 0, // TODO: implement cache hit tracking
            cache_misses: 0, // TODO: implement cache miss tracking
            extent_count: self.extent_cache.read().len(),
            interval_count: self.interval_table.read().len(),
            file_stats,
        }
    }

    /// Get number of extents
    pub fn extent_count(&self) -> usize {
        self.extent_cache.read().len()
    }

    /// Get number of intervals
    pub fn interval_count(&self) -> usize {
        self.interval_table.read().len()
    }

    /// Validate storage integrity
    pub fn validate(&self) -> Result<(), FileEngineError> {
        // Validate interval table
        {
            let interval_table = self.interval_table.read();
            interval_table.validate()?;
        }
        
        // Validate superblock
        {
            let storage = self.file_storage.lock().unwrap();
            if !storage.superblock().verify_checksum() {
                return Err(FileEngineError::CorruptedData);
            }
        }
        
        Ok(())
    }

    /// Close the storage engine
    pub fn close(self) -> Result<(), FileEngineError> {
        let storage = Arc::try_unwrap(self.file_storage).map_err(|_| FileEngineError::CorruptedData)?.into_inner().unwrap();
        storage.close()?;
        Ok(())
    }

    /// Get file path
    pub fn file_path(&self) -> &PathBuf {
        &self.config.file_config.file_path
    }

    /// Check if an extent is full and needs splitting
    fn is_extent_full(&self, extent_id: u32) -> Result<bool, FileEngineError> {
        let metadata = {
            let cache = self.extent_cache.read();
            cache.get(&extent_id).cloned()
                .ok_or(FileEngineError::ExtentNotFound)?
        };
        
        // Consider extent full if allocated pages >= page_count
        // This ensures we split before completely filling the extent
        Ok(metadata.allocated_pages >= metadata.page_count)
    }

    /// Split an extent into two halves when it becomes full
    fn split_extent(&self, extent_id: u32) -> Result<u32, FileEngineError> {
        // Get the existing extent metadata
        let existing_metadata = {
            let cache = self.extent_cache.read();
            cache.get(&extent_id)
                .ok_or(FileEngineError::ExtentNotFound)?
                .clone()
        };

        // Calculate the midpoint for splitting
        let morton_range = existing_metadata.morton_end - existing_metadata.morton_start;
        let midpoint = existing_metadata.morton_start + morton_range / 2;

        // Create new extent for the second half
        let new_extent_id = self.next_extent_id.fetch_add(1, Ordering::Relaxed);
        if new_extent_id > self.config.max_extents {
            return Err(FileEngineError::TooManyExtents);
        }

        let new_extent_offset = {
            let mut storage = self.file_storage.lock().unwrap();
            storage.allocate_extent(new_extent_id)?
        };

        // Update the original extent to cover first half
        let updated_metadata = ExtentMetadata {
            extent_id: existing_metadata.extent_id,
            offset: existing_metadata.offset,
            morton_start: existing_metadata.morton_start,
            morton_end: midpoint,
            page_count: existing_metadata.page_count,
            allocated_pages: 0, // Reset after split
        };

        // Create metadata for new extent (second half)
        let new_metadata = ExtentMetadata {
            extent_id: new_extent_id,
            offset: new_extent_offset,
            morton_start: midpoint + 1,
            morton_end: existing_metadata.morton_end,
            page_count: self.calculate_page_count_for_extent(),
            allocated_pages: 0,
        };

        // Update interval table - remove old interval and add two new ones
        // Note: This is a simplified approach. The interval table should support
        // proper splitting, but for now we'll rebuild with new intervals
        
        // Insert the two new intervals
        {
            let mut interval_table = self.interval_table.write();
            interval_table.insert(
                updated_metadata.morton_start,
                updated_metadata.morton_end,
                updated_metadata.extent_id
            )?;
            
            interval_table.insert(
                new_metadata.morton_start,
                new_metadata.morton_end,
                new_metadata.extent_id
            )?;
        }

        // Update the cache
        {
            let mut cache = self.extent_cache.write();
            cache.insert(extent_id, updated_metadata);
            cache.insert(new_extent_id, new_metadata);
        }

        Ok(new_extent_id)
    }

    /// Hash a key to create a consistent fingerprint
    fn hash_key(&self, key: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}

/// Storage engine statistics
#[derive(Debug, Clone)]
pub struct FileEngineStats {
    pub reads: u64,
    pub writes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub extent_count: usize,
    pub interval_count: usize,
    pub file_stats: FileStorageStats,
}

impl Default for FileEngineStats {
    fn default() -> Self {
        Self {
            reads: 0,
            writes: 0,
            cache_hits: 0,
            cache_misses: 0,
            extent_count: 0,
            interval_count: 0,
            file_stats: FileStorageStats {
                file_size: 0,
                extent_count: 0,
                page_size: 0,
                extent_size: 0,
                txid: 0,
                utilization: 0.0,
            },
        }
    }
}

/// File storage engine errors
#[derive(Debug)]
pub enum FileEngineError {
    FileStorageError(FileStorageError),
    ExtentError(ExtentError),
    PageError(PageError),
    IntervalError(IntervalError),
    IoError(std::io::Error),
    ExtentNotFound,
    ExtentFull,
    TooManyExtents,
    CorruptedData,
    InsufficientSpace,
}

impl From<FileStorageError> for FileEngineError {
    fn from(err: FileStorageError) -> Self {
        FileEngineError::FileStorageError(err)
    }
}

impl From<ExtentError> for FileEngineError {
    fn from(err: ExtentError) -> Self {
        FileEngineError::ExtentError(err)
    }
}

impl From<PageError> for FileEngineError {
    fn from(err: PageError) -> Self {
        FileEngineError::PageError(err)
    }
}

impl From<IntervalError> for FileEngineError {
    fn from(err: IntervalError) -> Self {
        FileEngineError::IntervalError(err)
    }
}

impl From<std::io::Error> for FileEngineError {
    fn from(err: std::io::Error) -> Self {
        FileEngineError::IoError(err)
    }
}

impl std::fmt::Display for FileEngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileEngineError::FileStorageError(e) => write!(f, "File storage error: {}", e),
            FileEngineError::ExtentError(e) => write!(f, "Extent error: {}", e),
            FileEngineError::PageError(e) => write!(f, "Page error: {}", e),
            FileEngineError::IntervalError(e) => write!(f, "Interval error: {}", e),
            FileEngineError::IoError(e) => write!(f, "I/O error: {}", e),
            FileEngineError::ExtentNotFound => write!(f, "Extent not found"),
            FileEngineError::ExtentFull => write!(f, "Extent is full"),
            FileEngineError::TooManyExtents => write!(f, "Too many extents"),
            FileEngineError::CorruptedData => write!(f, "Corrupted data"),
            FileEngineError::InsufficientSpace => write!(f, "Insufficient space"),
        }
    }
}

impl std::error::Error for FileEngineError {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_file_engine_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = FileEngineConfig::with_path(temp_dir.path().join("test.db"));
        
        let engine = FileEngine::new(config).unwrap();
        assert_eq!(engine.extent_count(), 0);
        assert_eq!(engine.interval_count(), 0);
    }

    #[test]
    fn test_basic_put_get() {
        let temp_dir = TempDir::new().unwrap();
        let config = FileEngineConfig::with_path(temp_dir.path().join("test.db"));
        
        let mut engine = FileEngine::new(config).unwrap();
        
        engine.put("test_key", b"test_value").unwrap();
        let value = engine.get("test_key").unwrap();
        assert_eq!(value, Some(b"test_value".to_vec()));
    }

    #[test]
    fn test_nonexistent_key() {
        let temp_dir = TempDir::new().unwrap();
        let config = FileEngineConfig::with_path(temp_dir.path().join("test.db"));
        
        let engine = FileEngine::new(config).unwrap();
        let value = engine.get("nonexistent").unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_multiple_keys() {
        let temp_dir = TempDir::new().unwrap();
        let config = FileEngineConfig::with_path(temp_dir.path().join("test.db"));
        
        let mut engine = FileEngine::new(config).unwrap();
        
        engine.put("key1", b"value1").unwrap();
        engine.put("key2", b"value2").unwrap();
        engine.put("key3", b"value3").unwrap();
        
        assert_eq!(engine.get("key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(engine.get("key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(engine.get("key3").unwrap(), Some(b"value3".to_vec()));
    }

    #[test]
    fn test_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("persistence_test.db");
        
        // Create engine and store data
        {
            let config = FileEngineConfig::with_path(db_path.clone());
            let mut engine = FileEngine::new(config).unwrap();
            
            engine.put("persistent_key", b"persistent_value").unwrap();
            engine.flush().unwrap();
            engine.close().unwrap();
        }
        
        // Reopen and verify data
        {
            let mut config = FileEngineConfig::with_path(db_path);
            config.file_config.create_if_missing = false;
            config.file_config.truncate = false;
            
            let engine = FileEngine::new(config).unwrap();
            let value = engine.get("persistent_key").unwrap();
            assert_eq!(value, Some(b"persistent_value".to_vec()));
        }
    }

    #[test]
    fn test_large_values() {
        let temp_dir = TempDir::new().unwrap();
        let config = FileEngineConfig::with_path(temp_dir.path().join("test.db"));
        
        let mut engine = FileEngine::new(config).unwrap();
        
        let large_value = vec![42u8; 1000];
        engine.put("large_key", &large_value).unwrap();
        
        let retrieved = engine.get("large_key").unwrap();
        assert_eq!(retrieved, Some(large_value));
    }

    #[test]
    fn test_unicode_keys() {
        let temp_dir = TempDir::new().unwrap();
        let config = FileEngineConfig::with_path(temp_dir.path().join("test.db"));
        
        let mut engine = FileEngine::new(config).unwrap();
        
        engine.put("café", b"coffee").unwrap();
        engine.put("naïve", b"innocent").unwrap();
        engine.put("🚀", b"rocket").unwrap();
        
        assert_eq!(engine.get("café").unwrap(), Some(b"coffee".to_vec()));
        assert_eq!(engine.get("naïve").unwrap(), Some(b"innocent".to_vec()));
        assert_eq!(engine.get("🚀").unwrap(), Some(b"rocket".to_vec()));
    }

    #[test]
    fn test_storage_validation() {
        let temp_dir = TempDir::new().unwrap();
        let config = FileEngineConfig::with_path(temp_dir.path().join("test.db"));
        
        let mut engine = FileEngine::new(config).unwrap();
        
        engine.put("key1", b"value1").unwrap();
        engine.put("key2", b"value2").unwrap();
        
        assert!(engine.validate().is_ok());
    }
}