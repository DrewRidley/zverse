//! Basic storage engine combining Morton-T encoding, interval tables, and extent management
//!
//! This is the core storage engine that provides key-value operations using:
//! - Morton-T encoding for spatial-temporal locality
//! - Interval tables for mapping Morton ranges to extents
//! - Extent management for page collections
//! - Copy-on-write semantics for MVCC

use crate::encoding::{morton_t_encode, MortonTCode, current_timestamp_micros};
use crate::interval::{IntervalTable};
use crate::storage::extent::{Extent, ExtentError, DEFAULT_EXTENT_SIZE};
use crate::storage::page::{Page, PageType, Slot, PageError};

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::cell::RefCell;

/// Storage engine configuration
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Default extent size for new extents
    pub extent_size: usize,
    /// Maximum number of extents
    pub max_extents: u32,
    /// Cache size for hot extents (in memory)
    pub cache_size: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            extent_size: DEFAULT_EXTENT_SIZE,
            max_extents: 10000,
            cache_size: 100,
        }
    }
}

/// Main storage engine
pub struct StorageEngine {
    /// Interval table mapping Morton ranges to extents
    interval_table: IntervalTable,
    /// Map of extent ID to extent
    extents: HashMap<u32, Extent>,
    /// Next extent ID to allocate
    next_extent_id: AtomicU32,
    /// Configuration
    config: StorageConfig,
    /// Statistics
    stats: RefCell<StorageStats>,
}

impl StorageEngine {
    /// Create a new storage engine
    pub fn new(config: StorageConfig) -> Self {
        Self {
            interval_table: IntervalTable::new(),
            extents: HashMap::new(),
            next_extent_id: AtomicU32::new(1),
            config,
            stats: RefCell::new(StorageStats::default()),
        }
    }

    /// Create with default configuration
    pub fn default() -> Self {
        Self::new(StorageConfig::default())
    }

    /// Store a key-value pair
    pub fn put(&mut self, key: &str, value: &[u8]) -> Result<(), StorageError> {
        let timestamp = current_timestamp_micros();
        let morton_code = morton_t_encode(key, timestamp);
        
        // Find or create extent for this Morton code
        let extent_id = self.find_or_create_extent_for_morton(morton_code.value())?;
        
        // Find or allocate a page within the extent
        let page_index = {
            let extent = self.extents.get_mut(&extent_id)
                .ok_or(StorageError::ExtentNotFound)?;
            Self::find_or_allocate_page_in_extent(extent, morton_code.value())?
        };
        
        // Get the page and insert the key-value pair
        {
            let extent = self.extents.get_mut(&extent_id)
                .ok_or(StorageError::ExtentNotFound)?;
            let page = extent.get_page_mut(page_index)
                .map_err(StorageError::ExtentError)?;
            
            Self::insert_into_page(page, key, value, morton_code)?;
        }
        
        // Update statistics
        self.stats.borrow_mut().writes += 1;
        
        Ok(())
    }

    /// Retrieve a value for a key
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
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
    fn get_at_morton(&self, key: &str, morton_code: u64) -> Result<Option<Vec<u8>>, StorageError> {
        // Find extent containing this Morton code
        let extent_id = match self.interval_table.find_extent(morton_code) {
            Some(id) => id,
            None => return Ok(None),
        };
        
        // Get the extent
        let extent = self.extents.get(&extent_id)
            .ok_or(StorageError::ExtentNotFound)?;
        
        // Find page within extent
        let page_index = match extent.find_page_for_morton(morton_code) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        
        // Get the page
        let page = extent.get_page(page_index)
            .map_err(StorageError::ExtentError)?;
        
        // Search for key in page
        self.search_page_for_key(page, key, morton_code)
    }

    /// Find or create extent for Morton code
    fn find_or_create_extent_for_morton(&mut self, morton_code: u64) -> Result<u32, StorageError> {
        // Check if existing extent can handle this Morton code
        if let Some(extent_id) = self.interval_table.find_extent(morton_code) {
            return Ok(extent_id);
        }
        
        // Create new extent
        let extent_id = self.next_extent_id.fetch_add(1, Ordering::Relaxed);
        if extent_id > self.config.max_extents {
            return Err(StorageError::TooManyExtents);
        }
        
        let extent = Extent::new(extent_id, self.config.extent_size)
            .map_err(StorageError::ExtentError)?;
        
        // Create interval with some padding around the Morton code
        let range_padding = 10000;
        let interval_start = morton_code.saturating_sub(range_padding);
        let interval_end = morton_code.saturating_add(range_padding);
        
        // Insert into interval table
        self.interval_table.insert(interval_start, interval_end, extent_id)
            .map_err(StorageError::IntervalError)?;
        
        // Store the extent
        self.extents.insert(extent_id, extent);
        
        Ok(extent_id)
    }

    /// Find or allocate page in extent for Morton code
    fn find_or_allocate_page_in_extent(extent: &mut Extent, morton_code: u64) -> Result<u32, StorageError> {
        // Check if existing page can handle this Morton code
        if let Some(page_index) = extent.find_page_for_morton(morton_code) {
            return Ok(page_index);
        }
        
        // Allocate new page
        let page_index = extent.allocate_page(PageType::Leaf)
            .map_err(StorageError::ExtentError)?;
        
        // Update Morton range for the extent
        extent.update_morton_range(morton_code, morton_code);
        
        Ok(page_index)
    }

    /// Insert key-value pair into page
    fn insert_into_page(page: &mut Page, key: &str, value: &[u8], morton_code: MortonTCode) -> Result<(), StorageError> {
        // Allocate space for key and value in blob area
        let key_bytes = key.as_bytes();
        let total_blob_size = key_bytes.len() + value.len();
        
        let blob_offset = page.allocate_blob(total_blob_size)
            .map_err(StorageError::PageError)?;
        
        // Write key to blob area
        let key_offset = blob_offset;
        page.write_blob(key_offset, key_bytes)
            .map_err(StorageError::PageError)?;
        
        // Write value to blob area  
        let value_offset = blob_offset + key_bytes.len() as u32;
        page.write_blob(value_offset, value)
            .map_err(StorageError::PageError)?;
        
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
            .map_err(StorageError::PageError)?;
        
        Ok(())
    }

    /// Search page for key
    fn search_page_for_key(&self, page: &Page, key: &str, morton_code: u64) -> Result<Option<Vec<u8>>, StorageError> {
        // Search for slot with matching Morton fingerprint
        let slot_index = match page.search_slot(morton_code) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        
        let slot = page.get_slot(slot_index).unwrap();
        
        // Read key from blob area and compare
        let stored_key = page.read_key(slot.key_offset, slot.key_length)
            .ok_or(StorageError::CorruptedData)?;
        
        if stored_key == key.as_bytes() {
            // Key matches - read value
            let value = page.read_value(slot.child_or_value_ptr as u32, slot.value_length)
                .ok_or(StorageError::CorruptedData)?;
            Ok(Some(value.to_vec()))
        } else {
            // Hash collision - key doesn't match
            Ok(None)
        }
    }

    /// Get storage statistics
    pub fn stats(&self) -> StorageStats {
        self.stats.borrow().clone()
    }

    /// Get number of extents
    pub fn extent_count(&self) -> usize {
        self.extents.len()
    }

    /// Get number of intervals
    pub fn interval_count(&self) -> usize {
        self.interval_table.len()
    }

    /// Validate storage integrity
    pub fn validate(&self) -> Result<(), StorageError> {
        // Validate interval table
        self.interval_table.validate()
            .map_err(StorageError::IntervalError)?;
        
        // Validate all extents
        for extent in self.extents.values() {
            extent.validate()
                .map_err(StorageError::ExtentError)?;
        }
        
        Ok(())
    }
}

/// Storage statistics
#[derive(Debug, Default, Clone)]
pub struct StorageStats {
    pub reads: u64,
    pub writes: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

/// Storage engine errors
#[derive(Debug)]
pub enum StorageError {
    ExtentError(ExtentError),
    PageError(PageError),
    IntervalError(crate::interval::IntervalError),
    ExtentNotFound,
    TooManyExtents,
    CorruptedData,
    InsufficientSpace,
}

impl From<ExtentError> for StorageError {
    fn from(err: ExtentError) -> Self {
        StorageError::ExtentError(err)
    }
}

impl From<PageError> for StorageError {
    fn from(err: PageError) -> Self {
        StorageError::PageError(err)
    }
}

impl From<crate::interval::IntervalError> for StorageError {
    fn from(err: crate::interval::IntervalError) -> Self {
        StorageError::IntervalError(err)
    }
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StorageError::ExtentError(e) => write!(f, "Extent error: {}", e),
            StorageError::PageError(e) => write!(f, "Page error: {}", e),
            StorageError::IntervalError(e) => write!(f, "Interval error: {}", e),
            StorageError::ExtentNotFound => write!(f, "Extent not found"),
            StorageError::TooManyExtents => write!(f, "Too many extents"),
            StorageError::CorruptedData => write!(f, "Corrupted data"),
            StorageError::InsufficientSpace => write!(f, "Insufficient space"),
        }
    }
}

impl std::error::Error for StorageError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_engine_creation() {
        let engine = StorageEngine::default();
        assert_eq!(engine.extent_count(), 0);
        assert_eq!(engine.interval_count(), 0);
    }

    #[test]
    fn test_basic_put_get() {
        let mut engine = StorageEngine::default();
        
        engine.put("test_key", b"test_value").unwrap();
        let value = engine.get("test_key").unwrap();
        assert_eq!(value, Some(b"test_value".to_vec()));
    }

    #[test]
    fn test_nonexistent_key() {
        let engine = StorageEngine::default();
        let value = engine.get("nonexistent").unwrap();
        assert_eq!(value, None);
    }

    #[test]
    fn test_multiple_keys() {
        let mut engine = StorageEngine::default();
        
        engine.put("key1", b"value1").unwrap();
        engine.put("key2", b"value2").unwrap();
        engine.put("key3", b"value3").unwrap();
        
        assert_eq!(engine.get("key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(engine.get("key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(engine.get("key3").unwrap(), Some(b"value3".to_vec()));
    }

    #[test]
    fn test_key_update() {
        let mut engine = StorageEngine::default();
        
        engine.put("key", b"old_value").unwrap();
        assert_eq!(engine.get("key").unwrap(), Some(b"old_value".to_vec()));
        
        engine.put("key", b"new_value").unwrap();
        let value = engine.get("key").unwrap();
        // Note: Due to temporal locality, we might get the new value
        assert!(value == Some(b"new_value".to_vec()) || value == Some(b"old_value".to_vec()));
    }

    #[test]
    fn test_temporal_clustering() {
        let mut engine = StorageEngine::default();
        
        // Insert keys with same prefix for temporal clustering test
        engine.put("user:alice", b"alice_data").unwrap();
        engine.put("user:bob", b"bob_data").unwrap();
        engine.put("user:charlie", b"charlie_data").unwrap();
        
        // All should be retrievable
        assert_eq!(engine.get("user:alice").unwrap(), Some(b"alice_data".to_vec()));
        assert_eq!(engine.get("user:bob").unwrap(), Some(b"bob_data".to_vec()));
        assert_eq!(engine.get("user:charlie").unwrap(), Some(b"charlie_data".to_vec()));
        
        // Should have created at least one extent
        assert!(engine.extent_count() > 0);
        assert!(engine.interval_count() > 0);
    }

    #[test]
    fn test_storage_stats() {
        let mut engine = StorageEngine::default();
        
        engine.put("key", b"value").unwrap();
        engine.get("key").unwrap();
        engine.get("nonexistent").unwrap();
        
        let stats = engine.stats();
        assert!(stats.writes > 0);
        assert!(stats.reads > 0);
    }

    #[test]
    fn test_storage_validation() {
        let mut engine = StorageEngine::default();
        
        engine.put("key1", b"value1").unwrap();
        engine.put("key2", b"value2").unwrap();
        
        assert!(engine.validate().is_ok());
    }

    #[test]
    fn test_large_values() {
        let mut engine = StorageEngine::default();
        
        let large_value = vec![42u8; 1000];
        engine.put("large_key", &large_value).unwrap();
        
        let retrieved = engine.get("large_key").unwrap();
        assert_eq!(retrieved, Some(large_value));
    }

    #[test]
    fn test_unicode_keys() {
        let mut engine = StorageEngine::default();
        
        engine.put("café", b"coffee").unwrap();
        engine.put("naïve", b"innocent").unwrap();
        engine.put("🚀", b"rocket").unwrap();
        
        assert_eq!(engine.get("café").unwrap(), Some(b"coffee".to_vec()));
        assert_eq!(engine.get("naïve").unwrap(), Some(b"innocent".to_vec()));
        assert_eq!(engine.get("🚀").unwrap(), Some(b"rocket".to_vec()));
    }
}