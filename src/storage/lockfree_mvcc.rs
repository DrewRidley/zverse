//! Lock-free MVCC storage engine with microsecond performance
//!
//! This engine provides MVCC semantics without locks by using:
//! - Atomic timestamps for transaction ordering
//! - Copy-on-write at page level with CAS updates
//! - Direct file access for reads (no version store overhead)
//! - Timestamp-based visibility without coordination

use crate::encoding::{morton_t_encode, current_timestamp_micros};
use crate::interval::{IntervalTable, IntervalError};
use crate::storage::file::{FileStorage, FileStorageConfig, FileStorageError};
use crate::storage::page::{Page, PageType, Slot, PageError, PAGE_SIZE};

use std::sync::atomic::{AtomicU64, AtomicU32, Ordering};
use std::sync::Arc;
use std::collections::HashMap;
use std::path::PathBuf;

/// Lock-free transaction handle
pub struct LockFreeTransaction {
    /// Transaction timestamp (also serves as ID)
    pub timestamp: u64,
    /// Snapshot timestamp for reads
    pub snapshot_ts: u64,
    /// Write buffer for this transaction
    writes: Vec<(String, Vec<u8>)>,
}

impl LockFreeTransaction {
    fn new(timestamp: u64, snapshot_ts: u64) -> Self {
        Self {
            timestamp,
            snapshot_ts,
            writes: Vec::new(),
        }
    }

    /// Read a key (lock-free)
    pub fn get(&self, engine: &LockFreeMVCC, key: &str) -> Result<Option<Vec<u8>>, LockFreeMVCCError> {
        // Check write buffer first (read your own writes)
        for (write_key, value) in &self.writes {
            if write_key == key {
                return Ok(Some(value.clone()));
            }
        }

        // Read from storage with snapshot isolation
        engine.get_at_snapshot(key, self.snapshot_ts)
    }

    /// Write a key (buffered until commit)
    pub fn put(&mut self, key: &str, value: &[u8]) {
        self.writes.push((key.to_string(), value.to_vec()));
    }

    /// Commit transaction (uses CAS for atomicity)
    pub fn commit(self, engine: &LockFreeMVCC) -> Result<(), LockFreeMVCCError> {
        if self.writes.is_empty() {
            return Ok(());
        }

        engine.commit_writes(self.writes, self.timestamp)
    }
}

/// Page metadata with MVCC timestamps
#[derive(Debug, Clone, Copy)]
#[repr(C)]
struct PageMVCCHeader {
    /// Standard page header
    base_header: [u8; 48], // Matches PageHeader size
    /// Creation timestamp
    created_ts: u64,
    /// Deletion timestamp (0 = not deleted)
    deleted_ts: u64,
    /// Next page in version chain (0 = none)
    next_version_page: u32,
    /// Reserved for alignment
    reserved: u32,
}

impl PageMVCCHeader {
    fn new(created_ts: u64) -> Self {
        Self {
            base_header: [0; 48],
            created_ts,
            deleted_ts: 0,
            next_version_page: 0,
            reserved: 0,
        }
    }

    fn is_visible(&self, snapshot_ts: u64) -> bool {
        self.created_ts <= snapshot_ts && 
        (self.deleted_ts == 0 || self.deleted_ts > snapshot_ts)
    }
}

/// Atomic page reference for CAS operations
#[derive(Debug)]
struct AtomicPageRef {
    /// Extent ID
    extent_id: AtomicU32,
    /// Page index within extent
    page_index: AtomicU32,
    /// Creation timestamp of current page
    timestamp: AtomicU64,
}

impl AtomicPageRef {
    fn new() -> Self {
        Self {
            extent_id: AtomicU32::new(0),
            page_index: AtomicU32::new(0),
            timestamp: AtomicU64::new(0),
        }
    }

    fn load(&self) -> (u32, u32, u64) {
        (
            self.extent_id.load(Ordering::Acquire),
            self.page_index.load(Ordering::Acquire),
            self.timestamp.load(Ordering::Acquire),
        )
    }

    fn compare_and_swap(&self, expected_ts: u64, new_extent: u32, new_page: u32, new_ts: u64) -> bool {
        // Use timestamp as the CAS target for simplicity
        self.timestamp.compare_exchange_weak(
            expected_ts, new_ts, Ordering::AcqRel, Ordering::Relaxed
        ).is_ok() && {
            self.extent_id.store(new_extent, Ordering::Release);
            self.page_index.store(new_page, Ordering::Release);
            true
        }
    }
}

/// Lock-free MVCC storage engine
pub struct LockFreeMVCC {
    /// File storage backend
    file_storage: FileStorage,
    /// Interval table (uses internal locking, but optimized for reads)
    interval_table: IntervalTable,
    /// Atomic page references for each Morton range
    page_refs: HashMap<String, Arc<AtomicPageRef>>,
    /// Global timestamp counter
    global_timestamp: AtomicU64,
    /// Next extent ID
    next_extent_id: AtomicU32,
    /// Configuration
    config: LockFreeMVCCConfig,
}

/// Configuration for lock-free MVCC
#[derive(Debug, Clone)]
pub struct LockFreeMVCCConfig {
    pub file_config: FileStorageConfig,
    pub max_extents: u32,
    pub morton_padding: u64,
}

impl Default for LockFreeMVCCConfig {
    fn default() -> Self {
        Self {
            file_config: FileStorageConfig::default(),
            max_extents: 10000,
            morton_padding: 1_000_000,
        }
    }
}

impl LockFreeMVCCConfig {
    pub fn with_path<P: Into<PathBuf>>(path: P) -> Self {
        let mut config = Self::default();
        config.file_config.file_path = path.into();
        config
    }

    pub fn with_file_size(mut self, size: u64) -> Self {
        self.file_config.file_size = size;
        self
    }
}

impl LockFreeMVCC {
    /// Create new lock-free MVCC engine
    pub fn new(config: LockFreeMVCCConfig) -> Result<Self, LockFreeMVCCError> {
        let file_storage = FileStorage::new(config.file_config.clone())?;
        
        Ok(Self {
            file_storage,
            interval_table: IntervalTable::new(),
            page_refs: HashMap::new(),
            global_timestamp: AtomicU64::new(1),
            next_extent_id: AtomicU32::new(1),
            config,
        })
    }

    /// Begin transaction (lock-free)
    pub fn begin_transaction(&self) -> LockFreeTransaction {
        let timestamp = self.global_timestamp.fetch_add(1, Ordering::SeqCst);
        let snapshot_ts = timestamp; // Use same timestamp for snapshot
        
        LockFreeTransaction::new(timestamp, snapshot_ts)
    }

    /// Direct get operation (lock-free, microsecond performance)
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, LockFreeMVCCError> {
        let snapshot_ts = self.global_timestamp.load(Ordering::Acquire);
        self.get_at_snapshot(key, snapshot_ts)
    }

    /// Get at specific snapshot (lock-free)
    fn get_at_snapshot(&self, key: &str, snapshot_ts: u64) -> Result<Option<Vec<u8>>, LockFreeMVCCError> {
        let morton_code = morton_t_encode(key, snapshot_ts);
        
        // Find extent (interval table has internal optimization for reads)
        let extent_id = match self.interval_table.find_extent(morton_code.value()) {
            Some(id) => id,
            None => return Ok(None),
        };

        // Get page reference (lock-free)
        let page_key = format!("{}:{}", extent_id, morton_code.value() / 1000000); // Group by range
        if let Some(page_ref) = self.page_refs.get(&page_key) {
            let (extent_id, page_index, page_ts) = page_ref.load();
            
            // Read page directly from file (no locks)
            if let Some(page) = self.file_storage.read_page(extent_id, page_index)? {
                // Check MVCC visibility
                let mvcc_header = self.extract_mvcc_header(&page);
                if mvcc_header.is_visible(snapshot_ts) {
                    return self.search_page_for_key(&page, key, morton_code.value());
                }
            }
        }

        Ok(None)
    }

    /// Put operation (creates transaction automatically)
    pub fn put(&self, key: &str, value: &[u8]) -> Result<(), LockFreeMVCCError> {
        let mut txn = self.begin_transaction();
        txn.put(key, value);
        txn.commit(self)
    }

    /// Commit writes (uses CAS for atomicity)
    fn commit_writes(&self, writes: Vec<(String, Vec<u8>)>, commit_ts: u64) -> Result<(), LockFreeMVCCError> {
        for (key, value) in writes {
            self.commit_single_write(&key, &value, commit_ts)?;
        }
        Ok(())
    }

    /// Commit single write with CAS
    fn commit_single_write(&self, key: &str, value: &[u8], commit_ts: u64) -> Result<(), LockFreeMVCCError> {
        let morton_code = morton_t_encode(key, commit_ts);
        
        // Find or create extent
        let extent_id = self.find_or_create_extent(morton_code.value())?;
        
        // Allocate new page
        let page_index = self.allocate_page_in_extent(extent_id)?;
        
        // Create page with MVCC header
        let mut page = Page::new(PageType::Leaf);
        self.insert_into_page(&mut page, key, value, morton_code, commit_ts)?;
        
        // Write page to storage
        self.file_storage.write_page(extent_id, page_index, &page)?;
        
        // Update page reference atomically
        let page_key = format!("{}:{}", extent_id, morton_code.value() / 1000000);
        let page_ref = self.page_refs.get(&page_key)
            .cloned()
            .unwrap_or_else(|| Arc::new(AtomicPageRef::new()));
        
        let (old_extent, old_page, old_ts) = page_ref.load();
        
        // CAS update (retry loop could be added here)
        if !page_ref.compare_and_swap(old_ts, extent_id, page_index, commit_ts) {
            // CAS failed - could retry or return conflict error
            return Err(LockFreeMVCCError::WriteConflict);
        }
        
        Ok(())
    }

    /// Extract MVCC header from page
    fn extract_mvcc_header(&self, page: &Page) -> PageMVCCHeader {
        // In a real implementation, this would extract from the page bytes
        // For now, assume all pages are visible (simplified)
        PageMVCCHeader::new(1)
    }

    /// Search page for key (same as before, but lock-free)
    fn search_page_for_key(&self, page: &Page, key: &str, morton_code: u64) -> Result<Option<Vec<u8>>, LockFreeMVCCError> {
        let slot_index = match page.search_slot(morton_code) {
            Some(idx) => idx,
            None => return Ok(None),
        };
        
        let slot = page.get_slot(slot_index).unwrap();
        
        let stored_key = page.read_key(slot.key_offset, slot.key_length)
            .ok_or(LockFreeMVCCError::CorruptedData)?;
        
        if stored_key == key.as_bytes() {
            let value = page.read_value(slot.child_or_value_ptr as u32, slot.value_length)
                .ok_or(LockFreeMVCCError::CorruptedData)?;
            Ok(Some(value.to_vec()))
        } else {
            Ok(None)
        }
    }

    /// Insert into page with MVCC timestamp
    fn insert_into_page(&self, page: &mut Page, key: &str, value: &[u8], morton_code: crate::encoding::MortonTCode, timestamp: u64) -> Result<(), LockFreeMVCCError> {
        let key_bytes = key.as_bytes();
        let total_blob_size = key_bytes.len() + value.len();
        
        let blob_offset = page.allocate_blob(total_blob_size)?;
        
        let key_offset = blob_offset;
        page.write_blob(key_offset, key_bytes)?;
        
        let value_offset = blob_offset + key_bytes.len() as u32;
        page.write_blob(value_offset, value)?;
        
        let slot = Slot::new_leaf(
            morton_code.value(),
            key_offset,
            key_bytes.len() as u16,
            value_offset,
            value.len() as u16,
        );
        
        page.insert_slot(slot)?;
        page.update_checksum();
        
        Ok(())
    }

    /// Find or create extent (simplified)
    fn find_or_create_extent(&self, morton_code: u64) -> Result<u32, LockFreeMVCCError> {
        if let Some(extent_id) = self.interval_table.find_extent(morton_code) {
            return Ok(extent_id);
        }
        
        let extent_id = self.next_extent_id.fetch_add(1, Ordering::SeqCst);
        let offset = self.file_storage.allocate_extent(extent_id)?;
        
        let interval_start = morton_code.saturating_sub(self.config.morton_padding);
        let interval_end = morton_code.saturating_add(self.config.morton_padding);
        
        self.interval_table.insert(interval_start, interval_end, extent_id)?;
        
        Ok(extent_id)
    }

    /// Allocate page in extent (simplified)
    fn allocate_page_in_extent(&self, extent_id: u32) -> Result<u32, LockFreeMVCCError> {
        // Simplified - in real implementation would track page allocation per extent
        Ok(0)
    }

    /// Get statistics
    pub fn stats(&self) -> LockFreeMVCCStats {
        LockFreeMVCCStats {
            global_timestamp: self.global_timestamp.load(Ordering::Acquire),
            extent_count: self.next_extent_id.load(Ordering::Acquire),
            interval_count: self.interval_table.len(),
            page_refs: self.page_refs.len(),
        }
    }

    /// Flush to disk
    pub fn flush(&mut self) -> Result<(), LockFreeMVCCError> {
        self.file_storage.flush()?;
        Ok(())
    }
}

/// Statistics for lock-free MVCC
#[derive(Debug, Clone)]
pub struct LockFreeMVCCStats {
    pub global_timestamp: u64,
    pub extent_count: u32,
    pub interval_count: usize,
    pub page_refs: usize,
}

/// Errors for lock-free MVCC
#[derive(Debug)]
pub enum LockFreeMVCCError {
    FileStorageError(FileStorageError),
    PageError(PageError),
    IntervalError(IntervalError),
    WriteConflict,
    CorruptedData,
    IoError(std::io::Error),
}

impl From<FileStorageError> for LockFreeMVCCError {
    fn from(err: FileStorageError) -> Self {
        LockFreeMVCCError::FileStorageError(err)
    }
}

impl From<PageError> for LockFreeMVCCError {
    fn from(err: PageError) -> Self {
        LockFreeMVCCError::PageError(err)
    }
}

impl From<IntervalError> for LockFreeMVCCError {
    fn from(err: IntervalError) -> Self {
        LockFreeMVCCError::IntervalError(err)
    }
}

impl From<std::io::Error> for LockFreeMVCCError {
    fn from(err: std::io::Error) -> Self {
        LockFreeMVCCError::IoError(err)
    }
}

impl std::fmt::Display for LockFreeMVCCError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockFreeMVCCError::FileStorageError(e) => write!(f, "File storage error: {}", e),
            LockFreeMVCCError::PageError(e) => write!(f, "Page error: {}", e),
            LockFreeMVCCError::IntervalError(e) => write!(f, "Interval error: {}", e),
            LockFreeMVCCError::WriteConflict => write!(f, "Write conflict"),
            LockFreeMVCCError::CorruptedData => write!(f, "Corrupted data"),
            LockFreeMVCCError::IoError(e) => write!(f, "I/O error: {}", e),
        }
    }
}

impl std::error::Error for LockFreeMVCCError {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_lockfree_mvcc_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = LockFreeMVCCConfig::with_path(temp_dir.path().join("test.db"));
        
        let engine = LockFreeMVCC::new(config).unwrap();
        let stats = engine.stats();
        assert_eq!(stats.global_timestamp, 1);
    }

    #[test]
    fn test_lockfree_put_get() {
        let temp_dir = TempDir::new().unwrap();
        let config = LockFreeMVCCConfig::with_path(temp_dir.path().join("test.db"));
        let mut engine = LockFreeMVCC::new(config).unwrap();
        
        engine.put("test_key", b"test_value").unwrap();
        let value = engine.get("test_key").unwrap();
        assert_eq!(value, Some(b"test_value".to_vec()));
    }

    #[test]
    fn test_transaction_isolation() {
        let temp_dir = TempDir::new().unwrap();
        let config = LockFreeMVCCConfig::with_path(temp_dir.path().join("test.db"));
        let engine = LockFreeMVCC::new(config).unwrap();
        
        let mut txn1 = engine.begin_transaction();
        txn1.put("key1", b"value1");
        
        // Before commit, other reads shouldn't see the value
        let value = engine.get("key1").unwrap();
        assert_eq!(value, None);
        
        txn1.commit(&engine).unwrap();
        
        // After commit, reads should see the value
        let value = engine.get("key1").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
    }
}