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
use std::sync::atomic::{AtomicU32, Ordering};
use std::cell::RefCell;
use std::path::PathBuf;

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
    pub morton_padding: u64,
}

impl Default for FileEngineConfig {
    fn default() -> Self {
        Self {
            file_config: FileStorageConfig::default(),
            max_extents: 10000,
            cache_size: 100,
            morton_padding: 10000,
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
    file_storage: FileStorage,
    /// Interval table mapping Morton ranges to extents  
    interval_table: IntervalTable,
    /// Cache of extent metadata
    extent_cache: HashMap<u32, ExtentMetadata>,
    /// Next extent ID to allocate
    next_extent_id: AtomicU32,
    /// Configuration
    config: FileEngineConfig,
    /// Statistics
    stats: RefCell<FileEngineStats>,
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
        let file_storage = FileStorage::new(config.file_config.clone())?;
        
        Ok(Self {
            file_storage,
            interval_table: IntervalTable::new(),
            extent_cache: HashMap::new(),
            next_extent_id: AtomicU32::new(1),
            config,
            stats: RefCell::new(FileEngineStats::default()),
        })
    }

    /// Store a key-value pair
    pub fn put(&mut self, key: &str, value: &[u8]) -> Result<(), FileEngineError> {
        let timestamp = current_timestamp_micros();
        let morton_code = morton_t_encode(key, timestamp);
        
        // Find or create extent for this Morton code
        let extent_id = self.find_or_create_extent_for_morton(morton_code.value())?;
        
        // Find or allocate a page within the extent
        let page_index = self.find_or_allocate_page_in_extent(extent_id, morton_code.value())?;
        
        // Read the page, modify it, and write it back
        let mut page = self.file_storage.read_page(extent_id, page_index)?
            .unwrap_or_else(|| Page::new(PageType::Leaf));
        
        self.insert_into_page(&mut page, key, value, morton_code)?;
        
        // Write the modified page back
        self.file_storage.write_page(extent_id, page_index, &page)?;
        
        // Update statistics
        self.stats.borrow_mut().writes += 1;
        
        Ok(())
    }

    /// Retrieve a value for a key
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, FileEngineError> {
        // Try recent timestamps first for temporal locality
        let current_time = current_timestamp_micros();
        let search_window = 1_000_000; // 1 second window
        
        for offset in 0..search_window {
            let search_timestamp = current_time.saturating_sub(offset);
            let morton_code = morton_t_encode(key, search_timestamp);
            
            if let Some(value) = self.get_at_morton(key, morton_code.value())? {
                self.stats.borrow_mut().reads += 1;
                return Ok(Some(value));
            }
            
            // Skip ahead for efficiency
            if offset > 1000 && offset % 1000 != 0 {
                continue;
            }
        }
        
        self.stats.borrow_mut().reads += 1;
        Ok(None)
    }

    /// Get value at specific Morton code
    fn get_at_morton(&self, key: &str, morton_code: u64) -> Result<Option<Vec<u8>>, FileEngineError> {
        // Find extent containing this Morton code
        let extent_id = match self.interval_table.find_extent(morton_code) {
            Some(id) => id,
            None => return Ok(None),
        };
        
        // Find page within extent
        let page_index = match self.find_page_for_morton_in_extent(extent_id, morton_code)? {
            Some(idx) => idx,
            None => return Ok(None),
        };
        
        // Read the page
        let page = match self.file_storage.read_page(extent_id, page_index)? {
            Some(page) => page,
            None => return Ok(None),
        };
        
        // Search for key in page
        self.search_page_for_key(&page, key, morton_code)
    }

    /// Find or create extent for Morton code
    fn find_or_create_extent_for_morton(&mut self, morton_code: u64) -> Result<u32, FileEngineError> {
        // Check if existing extent can handle this Morton code
        if let Some(extent_id) = self.interval_table.find_extent(morton_code) {
            return Ok(extent_id);
        }
        
        // Create new extent
        let extent_id = self.next_extent_id.fetch_add(1, Ordering::Relaxed);
        if extent_id > self.config.max_extents {
            return Err(FileEngineError::TooManyExtents);
        }
        
        let extent_offset = self.file_storage.allocate_extent(extent_id)?;
        
        // Create interval with padding around the Morton code
        let interval_start = morton_code.saturating_sub(self.config.morton_padding);
        let interval_end = morton_code.saturating_add(self.config.morton_padding);
        
        // Insert into interval table
        self.interval_table.insert(interval_start, interval_end, extent_id)?;
        
        // Cache the extent metadata
        let metadata = ExtentMetadata {
            extent_id,
            offset: extent_offset,
            morton_start: interval_start,
            morton_end: interval_end,
            page_count: self.calculate_page_count_for_extent(),
            allocated_pages: 0,
        };
        self.extent_cache.insert(extent_id, metadata);
        
        Ok(extent_id)
    }

    /// Find or allocate page in extent for Morton code
    fn find_or_allocate_page_in_extent(&mut self, extent_id: u32, morton_code: u64) -> Result<u32, FileEngineError> {
        // Check if existing page can handle this Morton code
        if let Some(page_index) = self.find_page_for_morton_in_extent(extent_id, morton_code)? {
            return Ok(page_index);
        }
        
        // Allocate new page (for now, simple sequential allocation)
        let metadata = self.extent_cache.get_mut(&extent_id)
            .ok_or(FileEngineError::ExtentNotFound)?;
        
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
        if let Some(metadata) = self.extent_cache.get(&extent_id) {
            if metadata.allocated_pages > 0 {
                // Linear search through allocated pages (can be optimized later)
                for page_index in 0..metadata.allocated_pages {
                    if let Some(_page) = self.file_storage.read_page(extent_id, page_index)? {
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
        
        // Create slot
        let slot = Slot::new_leaf(
            morton_code.value(),
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
        // Search for slot with matching Morton fingerprint
        let slot_index = match page.search_slot(morton_code) {
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
        let extent_size = self.file_storage.superblock().extent_size();
        let header_size = 512; // Extent header size
        (extent_size - header_size) / PAGE_SIZE as u32
    }

    /// Flush all changes to disk
    pub fn flush(&mut self) -> Result<(), FileEngineError> {
        self.file_storage.flush()?;
        Ok(())
    }

    /// Get storage statistics
    pub fn stats(&self) -> FileEngineStats {
        let mut stats = self.stats.borrow().clone();
        let file_stats = self.file_storage.stats();
        stats.file_stats = file_stats;
        stats.extent_count = self.extent_cache.len();
        stats.interval_count = self.interval_table.len();
        stats
    }

    /// Get number of extents
    pub fn extent_count(&self) -> usize {
        self.extent_cache.len()
    }

    /// Get number of intervals
    pub fn interval_count(&self) -> usize {
        self.interval_table.len()
    }

    /// Validate storage integrity
    pub fn validate(&self) -> Result<(), FileEngineError> {
        // Validate interval table
        self.interval_table.validate()?;
        
        // Validate superblock
        if !self.file_storage.superblock().verify_checksum() {
            return Err(FileEngineError::CorruptedData);
        }
        
        Ok(())
    }

    /// Close the storage engine
    pub fn close(self) -> Result<(), FileEngineError> {
        self.file_storage.close()?;
        Ok(())
    }

    /// Get file path
    pub fn file_path(&self) -> &PathBuf {
        &self.config.file_config.file_path
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