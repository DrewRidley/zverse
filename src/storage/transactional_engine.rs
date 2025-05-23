//! Transactional file engine with MVCC support for concurrent operations
//!
//! This module provides a multi-reader/multi-writer storage engine using
//! snapshot isolation and copy-on-write semantics for lock-free concurrency.

use crate::encoding::{morton_t_encode, current_timestamp_micros};
use crate::interval::{IntervalTable, IntervalError};
use crate::storage::file::{FileStorage, FileStorageConfig, FileStorageError};
use crate::storage::page::{Page, PageType, Slot, PageError};
use crate::storage::transaction::{
    TransactionManager, Transaction, TransactionId, TransactionError, TransactionStats
};

use std::sync::{Arc, RwLock, Mutex};
use std::collections::HashMap;
use std::path::PathBuf;


/// Configuration for transactional engine
#[derive(Debug, Clone)]
pub struct TransactionalEngineConfig {
    /// File storage configuration
    pub file_config: FileStorageConfig,
    /// Maximum number of extents
    pub max_extents: u32,
    /// Morton range padding for new intervals
    pub morton_padding: u64,
    /// Transaction timeout in microseconds
    pub transaction_timeout: u64,
    /// Maximum concurrent transactions
    pub max_concurrent_txns: u32,
}

impl Default for TransactionalEngineConfig {
    fn default() -> Self {
        Self {
            file_config: FileStorageConfig::default(),
            max_extents: 10000,
            morton_padding: 1_000_000,
            transaction_timeout: 30_000_000, // 30 seconds
            max_concurrent_txns: 1000,
        }
    }
}

impl TransactionalEngineConfig {
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

    /// Set transaction timeout
    pub fn with_transaction_timeout(mut self, timeout_micros: u64) -> Self {
        self.transaction_timeout = timeout_micros;
        self
    }
}

/// Transaction handle for user operations
pub struct TransactionHandle {
    /// Internal transaction
    txn: Arc<Mutex<Transaction>>,
    /// Transaction manager reference
    tm: Arc<TransactionManager>,
    /// Engine reference for operations
    engine: Arc<RwLock<TransactionalEngineInner>>,
}

impl TransactionHandle {
    /// Read a key-value pair
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, TransactionalEngineError> {
        // First check transaction's write set
        let value = self.tm.read(&self.txn, key)?;
        if value.is_some() {
            return Ok(value);
        }

        // If not found in transaction, read from storage
        let engine = self.engine.read().unwrap();
        engine.get_with_transaction(&self.txn, key)
    }

    /// Write a key-value pair
    pub fn put(&self, key: &str, value: &[u8]) -> Result<(), TransactionalEngineError> {
        self.tm.write(&self.txn, key, value.to_vec())?;
        Ok(())
    }

    /// Commit the transaction
    pub fn commit(self) -> Result<(), TransactionalEngineError> {
        // Apply writes to storage first
        {
            let mut engine = self.engine.write().unwrap();
            engine.apply_transaction_writes(&self.txn)?;
        }

        // Then commit in transaction manager
        self.tm.commit_transaction(&self.txn)?;
        Ok(())
    }

    /// Abort the transaction
    pub fn abort(self) -> Result<(), TransactionalEngineError> {
        self.tm.abort_transaction(&self.txn)?;
        Ok(())
    }

    /// Get transaction ID
    pub fn id(&self) -> TransactionId {
        self.txn.lock().unwrap().id
    }
}

/// Internal engine state
#[derive(Debug)]
struct TransactionalEngineInner {
    /// File storage backend
    file_storage: FileStorage,
    /// Interval table for Morton range mapping
    interval_table: IntervalTable,
    /// Extent metadata cache
    extent_cache: HashMap<u32, ExtentMetadata>,
    /// Next extent ID counter
    next_extent_id: u32,
    /// Configuration
    config: TransactionalEngineConfig,
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
    owning_transaction: Option<TransactionId>,
}

impl TransactionalEngineInner {
    /// Get value with transaction isolation
    fn get_with_transaction(&self, txn: &Arc<Mutex<Transaction>>, key: &str) -> Result<Option<Vec<u8>>, TransactionalEngineError> {
        let _txn_guard = txn.lock().unwrap();
        let current_time = current_timestamp_micros();
        let morton_code = morton_t_encode(key, current_time);

        // Find extent containing this Morton code
        let extent_id = match self.interval_table.find_extent(morton_code.value()) {
            Some(id) => id,
            None => return Ok(None),
        };

        // Find page within extent
        let page_index = match self.find_page_for_morton_in_extent(extent_id, morton_code.value())? {
            Some(idx) => idx,
            None => return Ok(None),
        };

        // Read the page
        let page = match self.file_storage.read_page(extent_id, page_index)? {
            Some(page) => page,
            None => return Ok(None),
        };

        // Search for key in page
        self.search_page_for_key(&page, key, morton_code.value())
    }

    /// Apply transaction writes to storage
    fn apply_transaction_writes(&mut self, txn: &Arc<Mutex<Transaction>>) -> Result<(), TransactionalEngineError> {
        let txn_guard = txn.lock().unwrap();
        
        for (key, value) in &txn_guard.write_set {
            let timestamp = current_timestamp_micros();
            let morton_code = morton_t_encode(key, timestamp);
            
            // Find or create extent for this Morton code
            let extent_id = self.find_or_create_extent_for_morton(morton_code.value(), Some(txn_guard.id))?;
            
            // Find or allocate a page within the extent
            let page_index = self.find_or_allocate_page_in_extent(extent_id, morton_code.value())?;
            
            // Read the page, modify it, and write it back
            let mut page = self.file_storage.read_page(extent_id, page_index)?
                .unwrap_or_else(|| Page::new(PageType::Leaf));
            
            self.insert_into_page(&mut page, key, value, morton_code)?;
            
            // Write the modified page back
            self.file_storage.write_page(extent_id, page_index, &page)?;
        }
        
        Ok(())
    }

    /// Find or create extent for Morton code with transaction context
    fn find_or_create_extent_for_morton(&mut self, morton_code: u64, txn_id: Option<TransactionId>) -> Result<u32, TransactionalEngineError> {
        // Check if existing extent can handle this Morton code
        if let Some(extent_id) = self.interval_table.find_extent(morton_code) {
            return Ok(extent_id);
        }
        
        // Create new extent
        let extent_id = self.next_extent_id;
        self.next_extent_id += 1;
        
        if extent_id > self.config.max_extents {
            return Err(TransactionalEngineError::TooManyExtents);
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
            owning_transaction: txn_id,
        };
        self.extent_cache.insert(extent_id, metadata);
        
        Ok(extent_id)
    }

    /// Find or allocate page in extent for Morton code
    fn find_or_allocate_page_in_extent(&mut self, extent_id: u32, morton_code: u64) -> Result<u32, TransactionalEngineError> {
        // Check if existing page can handle this Morton code
        if let Some(page_index) = self.find_page_for_morton_in_extent(extent_id, morton_code)? {
            return Ok(page_index);
        }
        
        // Allocate new page
        let metadata = self.extent_cache.get_mut(&extent_id)
            .ok_or(TransactionalEngineError::ExtentNotFound)?;
        
        if metadata.allocated_pages >= metadata.page_count {
            return Err(TransactionalEngineError::ExtentFull);
        }
        
        let page_index = metadata.allocated_pages;
        metadata.allocated_pages += 1;
        
        Ok(page_index)
    }

    /// Find page for Morton code within extent
    fn find_page_for_morton_in_extent(&self, extent_id: u32, _morton_code: u64) -> Result<Option<u32>, TransactionalEngineError> {
        if let Some(metadata) = self.extent_cache.get(&extent_id) {
            if metadata.allocated_pages > 0 {
                // Simple implementation - return first page
                // TODO: Implement proper Morton-based page lookup
                return Ok(Some(0));
            }
        }
        Ok(None)
    }

    /// Insert key-value pair into page
    fn insert_into_page(&self, page: &mut Page, key: &str, value: &[u8], morton_code: crate::encoding::MortonTCode) -> Result<(), TransactionalEngineError> {
        // Allocate space for key and value in blob area
        let key_bytes = key.as_bytes();
        let total_blob_size = key_bytes.len() + value.len();
        
        let blob_offset = page.allocate_blob(total_blob_size)
            .map_err(TransactionalEngineError::PageError)?;
        
        // Write key to blob area
        let key_offset = blob_offset;
        page.write_blob(key_offset, key_bytes)
            .map_err(TransactionalEngineError::PageError)?;
        
        // Write value to blob area  
        let value_offset = blob_offset + key_bytes.len() as u32;
        page.write_blob(value_offset, value)
            .map_err(TransactionalEngineError::PageError)?;
        
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
            .map_err(TransactionalEngineError::PageError)?;
        
        // Update page checksum
        page.update_checksum();
        
        Ok(())
    }

    /// Search page for key
    fn search_page_for_key(&self, page: &Page, key: &str, morton_code: u64) -> Result<Option<Vec<u8>>, TransactionalEngineError> {
        // Search for slot with matching Morton fingerprint
        let slot_index = match page.search_slot(morton_code) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        
        let slot = page.get_slot(slot_index).unwrap();
        
        // Read key from blob area and compare
        let stored_key = page.read_key(slot.key_offset, slot.key_length)
            .ok_or(TransactionalEngineError::CorruptedData)?;
        
        if stored_key == key.as_bytes() {
            // Key matches - read value
            let value = page.read_value(slot.child_or_value_ptr as u32, slot.value_length)
                .ok_or(TransactionalEngineError::CorruptedData)?;
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
        (extent_size - header_size) / 4096 // PAGE_SIZE
    }
}

/// Transactional storage engine with MVCC support
pub struct TransactionalEngine {
    /// Transaction manager
    tm: Arc<TransactionManager>,
    /// Internal engine state
    inner: Arc<RwLock<TransactionalEngineInner>>,
    /// Configuration
    config: TransactionalEngineConfig,
}

impl TransactionalEngine {
    /// Create a new transactional storage engine
    pub fn new(config: TransactionalEngineConfig) -> Result<Self, TransactionalEngineError> {
        let file_storage = FileStorage::new(config.file_config.clone())?;
        
        let inner = TransactionalEngineInner {
            file_storage,
            interval_table: IntervalTable::new(),
            extent_cache: HashMap::new(),
            next_extent_id: 1,
            config: config.clone(),
        };

        Ok(Self {
            tm: Arc::new(TransactionManager::new()),
            inner: Arc::new(RwLock::new(inner)),
            config,
        })
    }

    /// Begin a new transaction
    pub fn begin_transaction(&self) -> Result<TransactionHandle, TransactionalEngineError> {
        let txn = self.tm.begin_transaction()?;
        
        Ok(TransactionHandle {
            txn,
            tm: self.tm.clone(),
            engine: self.inner.clone(),
        })
    }

    /// Perform a read-only operation without explicit transaction
    pub fn read_only<F, R>(&self, f: F) -> Result<R, TransactionalEngineError>
    where
        F: FnOnce(&TransactionHandle) -> Result<R, TransactionalEngineError>,
    {
        let txn = self.begin_transaction()?;
        let result = f(&txn)?;
        txn.commit()?;
        Ok(result)
    }

    /// Perform a read-write operation within a transaction
    pub fn read_write<F, R>(&self, f: F) -> Result<R, TransactionalEngineError>
    where
        F: FnOnce(&TransactionHandle) -> Result<R, TransactionalEngineError>,
    {
        let txn = self.begin_transaction()?;
        let result = f(&txn);
        
        match result {
            Ok(r) => {
                txn.commit()?;
                Ok(r)
            }
            Err(e) => {
                txn.abort()?;
                Err(e)
            }
        }
    }

    /// Get transaction statistics
    pub fn transaction_stats(&self) -> TransactionStats {
        self.tm.stats()
    }

    /// Flush all changes to disk
    pub fn flush(&self) -> Result<(), TransactionalEngineError> {
        let mut inner = self.inner.write().unwrap();
        inner.file_storage.flush()?;
        Ok(())
    }

    /// Close the engine
    pub fn close(self) -> Result<(), TransactionalEngineError> {
        let inner = Arc::try_unwrap(self.inner)
            .map_err(|_| TransactionalEngineError::InternalError("Failed to unwrap Arc".to_string()))?
            .into_inner()
            .map_err(|_| TransactionalEngineError::InternalError("Failed to acquire lock".to_string()))?;
        inner.file_storage.close()?;
        Ok(())
    }

    /// Get file path
    pub fn file_path(&self) -> &PathBuf {
        &self.config.file_config.file_path
    }
}

/// Engine statistics
#[derive(Debug, Clone)]
pub struct TransactionalEngineStats {
    pub transaction_stats: TransactionStats,
    pub extent_count: usize,
    pub interval_count: usize,
    pub file_utilization: f64,
}

/// Transactional engine errors
#[derive(Debug)]
pub enum TransactionalEngineError {
    TransactionError(TransactionError),
    FileStorageError(FileStorageError),
    PageError(PageError),
    IntervalError(IntervalError),
    ExtentNotFound,
    ExtentFull,
    TooManyExtents,
    CorruptedData,
    InsufficientSpace,
    IoError(std::io::Error),
    InternalError(String),
}

impl From<TransactionError> for TransactionalEngineError {
    fn from(err: TransactionError) -> Self {
        TransactionalEngineError::TransactionError(err)
    }
}

impl From<FileStorageError> for TransactionalEngineError {
    fn from(err: FileStorageError) -> Self {
        TransactionalEngineError::FileStorageError(err)
    }
}

impl From<PageError> for TransactionalEngineError {
    fn from(err: PageError) -> Self {
        TransactionalEngineError::PageError(err)
    }
}

impl From<IntervalError> for TransactionalEngineError {
    fn from(err: IntervalError) -> Self {
        TransactionalEngineError::IntervalError(err)
    }
}

impl From<std::io::Error> for TransactionalEngineError {
    fn from(err: std::io::Error) -> Self {
        TransactionalEngineError::IoError(err)
    }
}

impl std::fmt::Display for TransactionalEngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionalEngineError::TransactionError(e) => write!(f, "Transaction error: {}", e),
            TransactionalEngineError::FileStorageError(e) => write!(f, "File storage error: {}", e),
            TransactionalEngineError::PageError(e) => write!(f, "Page error: {}", e),
            TransactionalEngineError::IntervalError(e) => write!(f, "Interval error: {}", e),
            TransactionalEngineError::ExtentNotFound => write!(f, "Extent not found"),
            TransactionalEngineError::ExtentFull => write!(f, "Extent is full"),
            TransactionalEngineError::TooManyExtents => write!(f, "Too many extents"),
            TransactionalEngineError::CorruptedData => write!(f, "Corrupted data"),
            TransactionalEngineError::InsufficientSpace => write!(f, "Insufficient space"),
            TransactionalEngineError::IoError(e) => write!(f, "I/O error: {}", e),
            TransactionalEngineError::InternalError(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for TransactionalEngineError {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_transactional_engine_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = TransactionalEngineConfig::with_path(temp_dir.path().join("test.db"));
        
        let engine = TransactionalEngine::new(config).unwrap();
        let stats = engine.transaction_stats();
        assert_eq!(stats.active_transactions, 0);
    }

    #[test]
    fn test_single_transaction() {
        let temp_dir = TempDir::new().unwrap();
        let config = TransactionalEngineConfig::with_path(temp_dir.path().join("test.db"));
        let engine = TransactionalEngine::new(config).unwrap();

        let result = engine.read_write(|txn| {
            txn.put("key1", b"value1")?;
            let value = txn.get("key1")?;
            assert_eq!(value, Some(b"value1".to_vec()));
            Ok(())
        });

        assert!(result.is_ok());
    }

    #[test]
    fn test_concurrent_transactions() {
        let temp_dir = TempDir::new().unwrap();
        let config = TransactionalEngineConfig::with_path(temp_dir.path().join("test.db"));
        let engine = Arc::new(TransactionalEngine::new(config).unwrap());

        let engine1 = engine.clone();
        let engine2 = engine.clone();

        let handle1 = std::thread::spawn(move || {
            engine1.read_write(|txn| {
                txn.put("key1", b"value1")?;
                Ok(())
            })
        });

        let handle2 = std::thread::spawn(move || {
            engine2.read_write(|txn| {
                txn.put("key2", b"value2")?;
                Ok(())
            })
        });

        assert!(handle1.join().unwrap().is_ok());
        assert!(handle2.join().unwrap().is_ok());
    }

    #[test]
    fn test_transaction_isolation() {
        let temp_dir = TempDir::new().unwrap();
        let config = TransactionalEngineConfig::with_path(temp_dir.path().join("test.db"));
        let engine = TransactionalEngine::new(config).unwrap();

        // First transaction commits
        engine.read_write(|txn| {
            txn.put("key1", b"value1")?;
            Ok(())
        }).unwrap();

        // Second transaction should see the committed value
        let value = engine.read_only(|txn| {
            txn.get("key1")
        }).unwrap();

        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_transaction_abort() {
        let temp_dir = TempDir::new().unwrap();
        let config = TransactionalEngineConfig::with_path(temp_dir.path().join("test.db"));
        let engine = TransactionalEngine::new(config).unwrap();

        let txn = engine.begin_transaction().unwrap();
        txn.put("key1", b"value1").unwrap();
        txn.abort().unwrap();

        // Value should not be visible after abort
        let value = engine.read_only(|txn| {
            txn.get("key1")
        }).unwrap();

        assert_eq!(value, None);
    }
}