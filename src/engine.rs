//! Core ZVerse storage engine with mmap2 integration
//!
//! This module implements the main storage engine that provides COW semantics
//! through key-timestamp interleaving and mmap2-based file access.

use crate::interleaved_key::InterleavedKey;
use crate::storage::{Entry, EntryIterator, FileOffset, binary_search_entries};
use crate::next_timestamp;

use memmap2::{Mmap, MmapOptions};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

/// Errors that can occur during engine operations
#[derive(Debug)]
pub enum EngineError {
    Io(io::Error),
    InvalidKey,
    KeyNotFound,
    CorruptedData,
    FileLocked,
}

impl From<io::Error> for EngineError {
    fn from(err: io::Error) -> Self {
        EngineError::Io(err)
    }
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::Io(err) => write!(f, "I/O error: {}", err),
            EngineError::InvalidKey => write!(f, "Invalid key"),
            EngineError::KeyNotFound => write!(f, "Key not found"),
            EngineError::CorruptedData => write!(f, "Corrupted data"),
            EngineError::FileLocked => write!(f, "File locked"),
        }
    }
}

impl std::error::Error for EngineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            EngineError::Io(err) => Some(err),
            _ => None,
        }
    }
}

/// Configuration for the ZVerse engine
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// File path for the database
    pub file_path: String,
    /// Initial file size for new databases
    pub initial_size: u64,
    /// Whether to create the file if it doesn't exist
    pub create_if_missing: bool,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            file_path: "zverse.db".to_string(),
            initial_size: 1024 * 1024, // 1MB
            create_if_missing: true,
        }
    }
}

/// Main ZVerse storage engine
pub struct ZVerseEngine {
    /// Memory-mapped file handle
    mmap: Arc<RwLock<Mmap>>,
    /// Write handle for appending new entries
    write_file: Arc<RwLock<File>>,
    /// Index mapping original keys to latest entry offsets
    index: Arc<RwLock<BTreeMap<Vec<u8>, FileOffset>>>,
    /// Current file size for append operations
    file_size: Arc<RwLock<u64>>,
    /// Configuration
    config: EngineConfig,
}

impl ZVerseEngine {
    /// Open or create a new ZVerse database
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, EngineError> {
        let config = EngineConfig {
            file_path: path.as_ref().to_string_lossy().to_string(),
            ..Default::default()
        };
        Self::open_with_config(config)
    }

    /// Open database with custom configuration
    pub fn open_with_config(config: EngineConfig) -> Result<Self, EngineError> {
        let path = Path::new(&config.file_path);
        let file_exists = path.exists();

        // Open or create the file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(config.create_if_missing)
            .open(path)?;

        // Initialize file size if it's a new file
        if !file_exists && config.create_if_missing {
            file.set_len(config.initial_size)?;
        }

        // Get current file size
        let file_size = file.metadata()?.len();

        // Create memory mapping
        let mmap = unsafe {
            MmapOptions::new()
                .map(&file)?
        };

        // Build index by scanning existing entries
        let index = Self::build_index(&mmap)?;

        Ok(Self {
            mmap: Arc::new(RwLock::new(mmap)),
            write_file: Arc::new(RwLock::new(file)),
            index: Arc::new(RwLock::new(index)),
            file_size: Arc::new(RwLock::new(file_size)),
            config,
        })
    }

    /// Build index by scanning all entries in the file
    fn build_index(mmap: &Mmap) -> Result<BTreeMap<Vec<u8>, FileOffset>, EngineError> {
        let mut index = BTreeMap::new();
        let mut iter = EntryIterator::new(&mmap[..]);
        let mut consecutive_errors = 0;

        while let Some(result) = iter.next() {
            match result {
                Ok((offset, entry)) => {
                    consecutive_errors = 0; // Reset error counter on successful read
                    
                    // Keep only the latest version of each key
                    let key = entry.original_key.clone();
                    match index.get(&key) {
                        Some(&existing_offset) => {
                            // Compare timestamps to keep the latest
                            if let Some(existing_entry) = Self::read_entry_at_offset(&mmap[..], existing_offset)? {
                                if entry.cow_key.timestamp() > existing_entry.cow_key.timestamp() {
                                    index.insert(key, offset);
                                }
                            }
                        }
                        None => {
                            index.insert(key, offset);
                        }
                    }
                }
                Err(_) => {
                    consecutive_errors += 1;
                    // If we hit many consecutive errors, we've probably reached uninitialized data
                    if consecutive_errors > 3 {
                        break;
                    }
                }
            }
        }

        Ok(index)
    }

    /// Read an entry at a specific offset
    fn read_entry_at_offset(data: &[u8], offset: FileOffset) -> Result<Option<Entry>, EngineError> {
        if offset as usize >= data.len() {
            return Ok(None);
        }

        let mut iter = EntryIterator::at_offset(data, offset as usize);
        match iter.next() {
            Some(Ok((_, entry))) => Ok(Some(entry)),
            Some(Err(e)) => Err(EngineError::from(e)),
            None => Ok(None),
        }
    }

    /// Get the latest value for a key
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, EngineError> {
        let index = self.index.read();
        let mmap = self.mmap.read();

        if let Some(&offset) = index.get(key) {
            if let Some(entry) = Self::read_entry_at_offset(&mmap[..], offset)? {
                let value = entry.value_bytes();
                if value.is_empty() {
                    // Empty value indicates tombstone (deleted)
                    Ok(None)
                } else {
                    Ok(Some(value.to_vec()))
                }
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Get value for a key at a specific timestamp
    pub fn get_at_timestamp(&self, key: &[u8], timestamp: u64) -> Result<Option<Vec<u8>>, EngineError> {
        let target_key = InterleavedKey::new(key, timestamp);
        let mmap = self.mmap.read();

        // Binary search for the exact version or the latest version before the timestamp
        match binary_search_entries(&mmap[..], &target_key)? {
            Some((_, entry)) => {
                if entry.original_key == key {
                    Ok(Some(entry.value_bytes().to_vec()))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    /// Store a key-value pair (COW operation)
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), EngineError> {
        let timestamp = next_timestamp();
        let cow_key = InterleavedKey::new(key, timestamp);
        let entry = Entry::new(cow_key, key.to_vec(), value.to_vec());

        // Serialize the entry
        let serialized = entry.serialize()?;

        // Append to file
        let mut write_file = self.write_file.write();
        let mut file_size = self.file_size.write();

        // Seek to end of file
        write_file.seek(SeekFrom::End(0))?;
        let offset = write_file.stream_position()?;

        // Write the entry
        write_file.write_all(&serialized)?;
        write_file.flush()?;

        // Update file size
        *file_size = write_file.stream_position()?;

        // Remap the file to include new data
        drop(write_file);
        drop(file_size);
        self.remap_file()?;

        // Update index
        let mut index = self.index.write();
        index.insert(key.to_vec(), offset);

        Ok(())
    }

    /// Delete a key (COW operation - adds tombstone)
    pub fn delete(&self, key: &[u8]) -> Result<bool, EngineError> {
        // Check if key exists
        let exists = {
            let index = self.index.read();
            index.contains_key(key)
        };

        if exists {
            // Store empty value as tombstone
            self.put(key, &[])?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Iterate over all latest key-value pairs
    pub fn scan(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>, EngineError> {
        let index = self.index.read();
        let mmap = self.mmap.read();
        let mut results = Vec::new();

        for (key, &offset) in index.iter() {
            if let Some(entry) = Self::read_entry_at_offset(&mmap[..], offset)? {
                // Skip tombstones (empty values)
                if !entry.value_bytes().is_empty() {
                    results.push((key.clone(), entry.value_bytes().to_vec()));
                }
            }
        }

        Ok(results)
    }

    /// Range scan over keys with optional prefix
    pub fn range(&self, start_key: &[u8], end_key: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, EngineError> {
        let index = self.index.read();
        let mmap = self.mmap.read();
        let mut results = Vec::new();

        for (key, &offset) in index.range(start_key.to_vec()..end_key.to_vec()) {
            if let Some(entry) = Self::read_entry_at_offset(&mmap[..], offset)? {
                // Skip tombstones (empty values)
                if !entry.value_bytes().is_empty() {
                    results.push((key.clone(), entry.value_bytes().to_vec()));
                }
            }
        }

        Ok(results)
    }

    /// Get all versions of a key
    pub fn get_versions(&self, key: &[u8]) -> Result<Vec<(u64, Vec<u8>)>, EngineError> {
        let mmap = self.mmap.read();
        let mut versions = Vec::new();
        let mut iter = EntryIterator::new(&mmap[..]);

        while let Some(result) = iter.next() {
            match result {
                Ok((_, entry)) => {
                    if entry.original_key == key {
                        versions.push((entry.cow_key.timestamp(), entry.value_bytes().to_vec()));
                    }
                }
                Err(_) => break, // Stop on errors (likely reached uninitialized data)
            }
        }

        // Sort by timestamp
        versions.sort_by_key(|&(timestamp, _)| timestamp);
        Ok(versions)
    }

    /// Remap the file after writes
    fn remap_file(&self) -> Result<(), EngineError> {
        let write_file = self.write_file.read();
        let mut mmap = self.mmap.write();

        // Create new mapping
        let new_mmap = unsafe {
            MmapOptions::new()
                .map(&*write_file)?
        };

        *mmap = new_mmap;
        Ok(())
    }

    /// Get database statistics
    pub fn stats(&self) -> EngineStats {
        let index = self.index.read();
        let file_size = *self.file_size.read();

        EngineStats {
            total_keys: index.len(),
            file_size_bytes: file_size,
        }
    }

    /// Flush any pending writes
    pub fn flush(&self) -> Result<(), EngineError> {
        let mut write_file = self.write_file.write();
        write_file.flush()?;
        Ok(())
    }
}

/// Database statistics
#[derive(Debug, Clone)]
pub struct EngineStats {
    pub total_keys: usize,
    pub file_size_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_basic_operations() {
        let temp_file = "./test_basic_ops.db";
        let engine = ZVerseEngine::open(temp_file).unwrap();

        // Test put and get
        engine.put(b"key1", b"value1").unwrap();
        assert_eq!(engine.get(b"key1").unwrap(), Some(b"value1".to_vec()));

        // Test update
        engine.put(b"key1", b"value2").unwrap();
        assert_eq!(engine.get(b"key1").unwrap(), Some(b"value2".to_vec()));

        // Test non-existent key
        assert_eq!(engine.get(b"nonexistent").unwrap(), None);
        
        // Cleanup
        fs::remove_file(temp_file).ok();
    }

    #[test]
    fn test_versions() {
        let temp_file = "./test_versions.db";
        let engine = ZVerseEngine::open(temp_file).unwrap();

        engine.put(b"key", b"v1").unwrap();
        engine.put(b"key", b"v2").unwrap();
        engine.put(b"key", b"v3").unwrap();

        let versions = engine.get_versions(b"key").unwrap();
        assert_eq!(versions.len(), 3);
        assert_eq!(versions[2].1, b"v3");
        
        // Cleanup
        fs::remove_file(temp_file).ok();
    }

    #[test]
    fn test_range_scan() {
        let temp_file = "./test_range_scan.db";
        let engine = ZVerseEngine::open(temp_file).unwrap();

        engine.put(b"key1", b"value1").unwrap();
        engine.put(b"key2", b"value2").unwrap();
        engine.put(b"key3", b"value3").unwrap();

        let results = engine.range(b"key1", b"key3").unwrap();
        assert_eq!(results.len(), 2); // key1 and key2 (range is exclusive of end)
        
        // Cleanup
        fs::remove_file(temp_file).ok();
    }

    #[test]
    fn test_delete() {
        let temp_file = "./test_delete.db";
        let engine = ZVerseEngine::open(temp_file).unwrap();

        engine.put(b"key", b"value").unwrap();
        assert!(engine.delete(b"key").unwrap());
        assert_eq!(engine.get(b"key").unwrap(), None);
        assert!(!engine.delete(b"nonexistent").unwrap());
        
        // Cleanup
        fs::remove_file(temp_file).ok();
    }
}