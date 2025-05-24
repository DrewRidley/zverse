//! Memory-mapped partition implementation using memmap2
//!
//! This module provides a memory-mapped wrapper around partition files,
//! enabling zero-copy access to temporal data stored on disk.

use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock, Mutex};
use std::cell::RefCell;
use std::collections::HashMap;
use memmap2::{MmapMut, MmapOptions};
use crate::disk::format::{PartitionHeader, RecordHeader, FormatError, HEADER_SIZE};
use crate::disk::config::DiskConfig;


/// Errors that can occur with memory-mapped partitions
#[derive(Debug, Clone)]
pub enum MmapError {
    /// Failed to create or open file
    FileError(String),
    /// Memory mapping failed
    MappingFailed(String),
    /// File growth operation failed
    GrowthFailed(String),
    /// Format validation failed
    FormatError(FormatError),
    /// Insufficient space for operation
    InsufficientSpace(usize),
    /// Concurrent access violation
    ConcurrentAccess,
}

impl std::fmt::Display for MmapError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MmapError::FileError(msg) => write!(f, "File error: {}", msg),
            MmapError::MappingFailed(msg) => write!(f, "Memory mapping failed: {}", msg),
            MmapError::GrowthFailed(msg) => write!(f, "File growth failed: {}", msg),
            MmapError::FormatError(err) => write!(f, "Format error: {}", err),
            MmapError::InsufficientSpace(needed) => write!(f, "Insufficient space: {} bytes needed", needed),
            MmapError::ConcurrentAccess => write!(f, "Concurrent access violation"),
        }
    }
}

impl std::error::Error for MmapError {}

impl From<io::Error> for MmapError {
    fn from(err: io::Error) -> Self {
        MmapError::FileError(err.to_string())
    }
}

impl From<FormatError> for MmapError {
    fn from(err: FormatError) -> Self {
        MmapError::FormatError(err)
    }
}

/// A memory-mapped partition that provides zero-copy access to temporal data
#[derive(Debug)]
pub struct MmapPartition {
    /// Partition ID (Fibonacci hash)
    partition_id: u32,
    
    /// Path to the partition file
    file_path: PathBuf,
    
    /// File handle for growth operations
    file: RefCell<File>,
    
    /// Memory-mapped view of the file
    mmap: RwLock<MmapMut>,
    
    /// Current file size in bytes
    file_size: AtomicU64,
    
    /// Whether the partition has been modified
    dirty: AtomicBool,
    
    /// Configuration
    config: Arc<DiskConfig>,
    
    /// Last sync timestamp
    last_sync: AtomicU64,
    
    /// In-memory index: Morton key -> file offset for O(1) lookups
    index: Mutex<HashMap<u64, u64>>,
    
    /// In-memory tracking of next record offset to avoid frequent header updates
    next_record_offset: AtomicU64,
}

impl MmapPartition {
    /// Create a new memory-mapped partition
    pub fn create(
        partition_id: u32,
        file_path: PathBuf,
        config: Arc<DiskConfig>,
    ) -> Result<Self, MmapError> {
        // Create the file
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&file_path)?;
        
        // Initialize with header
        let header = PartitionHeader::new(partition_id);
        let header_bytes = header.to_bytes();
        file.write_all(&header_bytes)?;
        file.sync_all()?;
        
        // Memory map the file
        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };
        
        Ok(Self {
            partition_id,
            file_path,
            file: RefCell::new(file),
            mmap: RwLock::new(mmap),
            file_size: AtomicU64::new(HEADER_SIZE as u64),
            dirty: AtomicBool::new(false),
            config,
            last_sync: AtomicU64::new(0),
            index: Mutex::new(HashMap::new()),
            next_record_offset: AtomicU64::new(HEADER_SIZE as u64),
        })
    }
    
    /// Open an existing memory-mapped partition
    pub fn open(
        partition_id: u32,
        file_path: PathBuf,
        config: Arc<DiskConfig>,
    ) -> Result<Self, MmapError> {
        // Open existing file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&file_path)?;
        
        let file_size = file.metadata()?.len();
        
        // Memory map the file
        let mmap = unsafe { 
            if config.memory.use_populate {
                MmapOptions::new().populate().map_mut(&file)?
            } else {
                MmapOptions::new().map_mut(&file)?
            }
        };
        
        let partition = Self {
            partition_id,
            file_path,
            file: RefCell::new(file),
            mmap: RwLock::new(mmap),
            file_size: AtomicU64::new(file_size),
            dirty: AtomicBool::new(false),
            config,
            last_sync: AtomicU64::new(0),
            index: Mutex::new(HashMap::new()),
            next_record_offset: AtomicU64::new(HEADER_SIZE as u64),
        };
        
        // Validate header
        partition.validate_header()?;
        
        // Rebuild index from existing records
        partition.rebuild_index()?;
        
        // Set next record offset from header
        let header = partition.header()?;
        partition.next_record_offset.store(header.index_offset, Ordering::Release);
        
        Ok(partition)
    }
    
    /// Get the partition header
    pub fn header(&self) -> Result<PartitionHeader, MmapError> {
        let mmap = self.mmap.read().unwrap();
        if mmap.len() < HEADER_SIZE {
            return Err(MmapError::FormatError(FormatError::InsufficientData));
        }
        
        let mut header_bytes = [0u8; HEADER_SIZE];
        header_bytes.copy_from_slice(&mmap[0..HEADER_SIZE]);
        
        let header = PartitionHeader::from_bytes(&header_bytes);
        header.validate()?;
        
        Ok(header)
    }
    
    /// Update the partition header
    pub fn update_header<F>(&self, update_fn: F) -> Result<(), MmapError>
    where
        F: FnOnce(&mut PartitionHeader),
    {
        let mut mmap = self.mmap.write().unwrap();
        if mmap.len() < HEADER_SIZE {
            return Err(MmapError::FormatError(FormatError::InsufficientData));
        }
        
        // Read current header
        let mut header_bytes = [0u8; HEADER_SIZE];
        header_bytes.copy_from_slice(&mmap[0..HEADER_SIZE]);
        let mut header = PartitionHeader::from_bytes(&header_bytes);
        
        // Apply update
        update_fn(&mut header);
        header.update_checksum();
        
        // Write back
        let updated_bytes = header.to_bytes();
        mmap[0..HEADER_SIZE].copy_from_slice(&updated_bytes);
        
        self.dirty.store(true, Ordering::Release);
        Ok(())
    }
    
    /// Insert a record into the partition
    pub fn insert_record(
        &self,
        morton_key: u64,
        key: &[u8],
        value: &[u8],
    ) -> Result<u64, MmapError> {
        let record_header = RecordHeader::new(morton_key, key.len() as u16, value.len() as u32);
        let total_size = record_header.total_size();
        
        // Get insertion point from in-memory tracking (no disk access)
        let insert_offset = self.next_record_offset.load(Ordering::Acquire);
        
        // Check if we need to grow the file - grow more aggressively to reduce remapping
        let current_size = self.file_size.load(Ordering::Acquire);
        let needed_size = insert_offset + total_size as u64;
        
        if needed_size > current_size {
            // Grow by at least 64MB at a time to reduce frequent remapping
            let min_growth = 64 * 1024 * 1024;
            let grow_to = (needed_size + min_growth).max(current_size * 2);
            self.grow_file(grow_to)?;
        }
        
        // Write the record with bounds checking
        {
            let mut mmap = self.mmap.write().unwrap();
            let offset = insert_offset as usize;
            
            // Verify we have enough space before writing
            if offset + total_size > mmap.len() {
                return Err(MmapError::InsufficientSpace(total_size));
            }
            
            // Write record header
            let header_bytes = record_header.to_bytes();
            mmap[offset..offset + RecordHeader::SIZE].copy_from_slice(&header_bytes);
            
            // Write key
            let key_offset = offset + RecordHeader::SIZE;
            mmap[key_offset..key_offset + key.len()].copy_from_slice(key);
            
            // Write value
            let value_offset = key_offset + key.len();
            mmap[value_offset..value_offset + value.len()].copy_from_slice(value);
        }
        
        // Update in-memory tracking (no disk I/O)
        let new_next_offset = insert_offset + total_size as u64;
        self.next_record_offset.store(new_next_offset, Ordering::Release);
        
        // Add to index for O(1) lookups
        {
            let mut index = self.index.lock().unwrap();
            index.insert(morton_key, insert_offset);
        }
        
        // Only update header every 10K records to minimize disk I/O
        let index_size = {
            let index = self.index.lock().unwrap();
            index.len()
        };
        
        if index_size % 10000 == 0 {
            self.update_header(|h| {
                h.record_count = index_size as u32;
                h.index_offset = new_next_offset;
                h.min_timestamp = h.min_timestamp.min(self.extract_timestamp(morton_key));
                h.max_timestamp = h.max_timestamp.max(self.extract_timestamp(morton_key));
            })?;
            self.dirty.store(true, Ordering::Release);
        }
        
        Ok(insert_offset)
    }
    
    /// Get a record by Morton key using O(1) index lookup
    pub fn get_record(&self, morton_key: u64) -> Result<Option<Vec<u8>>, MmapError> {
        // Fast O(1) index lookup
        let file_offset = {
            let index = self.index.lock().unwrap();
            match index.get(&morton_key) {
                Some(&offset) => offset,
                None => return Ok(None), // Key not found
            }
        };
        
        let mmap = self.mmap.read().unwrap();
        let offset = file_offset as usize;
        
        // Strict bounds checking before any memory access
        if offset >= mmap.len() || offset + RecordHeader::SIZE > mmap.len() {
            return Ok(None); // Offset is out of bounds
        }
        
        // Direct access to record at known offset
        let record_header = RecordHeader::from_bytes(&mmap[offset..])?;
        
        // Verify this is the correct record and not deleted
        if record_header.morton_key == morton_key && !record_header.is_deleted() {
            let value_offset = offset + RecordHeader::SIZE + record_header.key_len as usize;
            let value_end = value_offset + record_header.value_len as usize;
            
            // Additional bounds checking for value access
            if value_offset < mmap.len() && value_end <= mmap.len() && value_end > value_offset {
                return Ok(Some(mmap[value_offset..value_end].to_vec()));
            }
        }
        
        Ok(None)
    }
    
    /// Sync memory-mapped data to disk
    pub fn sync(&self, force_sync: bool) -> Result<(), MmapError> {
        // Always flush header state before sync
        let next_offset = self.next_record_offset.load(Ordering::Acquire);
        let index_size = {
            let index = self.index.lock().unwrap();
            index.len()
        };
        
        self.update_header(|h| {
            h.record_count = index_size as u32;
            h.index_offset = next_offset;
        })?;
        
        if !self.dirty.load(Ordering::Acquire) && !force_sync {
            return Ok(()); // Nothing to sync
        }
        
        let mmap = self.mmap.read().unwrap();
        
        if self.config.durability.use_async_sync && !force_sync {
            mmap.flush_async().map_err(|e| MmapError::MappingFailed(e.to_string()))?;
        } else {
            mmap.flush().map_err(|e| MmapError::MappingFailed(e.to_string()))?;
        }
        
        self.dirty.store(false, Ordering::Release);
        self.last_sync.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            Ordering::Release,
        );
        
        Ok(())
    }
    
    /// Check if partition needs syncing
    pub fn needs_sync(&self) -> bool {
        if !self.dirty.load(Ordering::Acquire) {
            return false;
        }
        
        let last_sync = self.last_sync.load(Ordering::Acquire);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        (now - last_sync) >= self.config.durability.sync_interval.as_secs()
    }
    
    /// Get partition statistics
    pub fn stats(&self) -> PartitionStats {
        let header = self.header().unwrap_or_else(|_| PartitionHeader::new(self.partition_id));
        let file_size = self.file_size.load(Ordering::Acquire);
        let is_dirty = self.dirty.load(Ordering::Acquire);
        
        PartitionStats {
            partition_id: self.partition_id,
            record_count: header.record_count,
            file_size_bytes: file_size,
            min_timestamp: header.min_timestamp,
            max_timestamp: header.max_timestamp,
            is_dirty,
            last_sync: self.last_sync.load(Ordering::Acquire),
        }
    }
    
    /// Validate the partition header
    fn validate_header(&self) -> Result<(), MmapError> {
        let header = self.header()?;
        
        if header.partition_id != self.partition_id {
            return Err(MmapError::FormatError(FormatError::CorruptedRecord));
        }
        
        if !header.verify_checksum() {
            return Err(MmapError::FormatError(FormatError::ChecksumMismatch));
        }
        
        Ok(())
    }
    
    /// Grow the file to accommodate more data
    fn grow_file(&self, min_size: u64) -> Result<(), MmapError> {
        let current_size = self.file_size.load(Ordering::Acquire);
        if min_size <= current_size {
            return Ok(()); // Already large enough
        }
        
        // Calculate new size with growth factor - grow in larger chunks to reduce remapping
        let growth_factor = self.config.partition_growth_factor.max(2.0); // At least 2x growth
        let mut new_size = (current_size as f64 * growth_factor) as u64;
        new_size = new_size.max(min_size);
        new_size = new_size.min(self.config.max_partition_size as u64);
        
        // Extend the file using existing file handle
        {
            let mut file = self.file.borrow_mut();
            file.seek(SeekFrom::Start(new_size - 1))?;
            file.write_all(&[0])?;
            file.sync_all()?;
        }
        
        // Remap the memory using the existing file handle
        let file = self.file.borrow();
        let new_mmap = unsafe { 
            if self.config.memory.use_populate {
                MmapOptions::new().populate().map_mut(&*file)?
            } else {
                MmapOptions::new().map_mut(&*file)?
            }
        };
        
        // Replace the old mapping - index offsets remain valid since file layout doesn't change
        {
            let mut mmap = self.mmap.write().unwrap();
            *mmap = new_mmap;
        }
        
        self.file_size.store(new_size, Ordering::Release);
        Ok(())
    }
    
    /// Extract timestamp from Morton key (simplified)
    fn extract_timestamp(&self, morton_key: u64) -> u64 {
        // This is a simplified extraction - in reality you'd use the proper Morton decoding
        morton_key >> 32
    }
    
    /// Rebuild the in-memory index from existing records on disk
    fn rebuild_index(&self) -> Result<(), MmapError> {
        let mmap = self.mmap.read().unwrap();
        let header = self.header()?;
        
        let mut index = self.index.lock().unwrap();
        index.clear();
        
        // Scan through all existing records and rebuild index
        let mut offset = header.records_offset as usize;
        let end_offset = header.index_offset.min(mmap.len() as u64) as usize;
        
        while offset + RecordHeader::SIZE <= end_offset {
            if let Ok(record_header) = RecordHeader::from_bytes(&mmap[offset..]) {
                if !record_header.is_deleted() {
                    index.insert(record_header.morton_key, offset as u64);
                }
                offset += record_header.total_size();
                if offset >= end_offset { break; }
            } else {
                break;
            }
        }
        
        Ok(())
    }
}

/// Statistics for a partition
#[derive(Debug, Clone)]
pub struct PartitionStats {
    pub partition_id: u32,
    pub record_count: u32,
    pub file_size_bytes: u64,
    pub min_timestamp: u64,
    pub max_timestamp: u64,
    pub is_dirty: bool,
    pub last_sync: u64,
}

impl Drop for MmapPartition {
    fn drop(&mut self) {
        // Sync on drop if configured
        if self.config.durability.sync_on_shutdown {
            let _ = self.sync(true);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::fs;
    
    fn test_config() -> Arc<DiskConfig> {
        Arc::new(DiskConfig::default())
    }
    
    fn setup_test_dir() -> PathBuf {
        let test_dir = PathBuf::from("./test_data");
        let _ = fs::create_dir_all(&test_dir);
        test_dir
    }
    
    fn cleanup_test_file(file_path: &PathBuf) {
        let _ = fs::remove_file(file_path);
        let _ = fs::remove_dir("./test_data");
    }
    
    #[test]
    fn test_create_partition() {
        let test_dir = setup_test_dir();
        let file_path = test_dir.join("test_partition_create.dat");
        let config = test_config();
        
        let partition = MmapPartition::create(42, file_path.clone(), config).unwrap();
        let header = partition.header().unwrap();
        
        assert_eq!(header.partition_id, 42);
        assert_eq!(header.record_count, 0);
        
        drop(partition);
        cleanup_test_file(&file_path);
    }
    
    #[test]
    fn test_insert_and_get_record() {
        let test_dir = setup_test_dir();
        let file_path = test_dir.join("test_partition_insert.dat");
        let config = test_config();
        
        let partition = MmapPartition::create(42, file_path.clone(), config).unwrap();
        
        let morton_key = 0x123456789ABCDEF0;
        let key = b"test_key";
        let value = b"test_value";
        
        partition.insert_record(morton_key, key, value).unwrap();
        
        let retrieved = partition.get_record(morton_key).unwrap();
        assert_eq!(retrieved, Some(value.to_vec()));
        
        let header = partition.header().unwrap();
        assert_eq!(header.record_count, 1);
        
        drop(partition);
        cleanup_test_file(&file_path);
    }
}