//! Persistent ZVerse implementation using memory-mapped storage
//!
//! This module provides a persistent implementation of ZVerse using memory-mapped
//! files for zero-copy access and efficient persistence.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use crate::core::ZEntry;
use crate::error::Error;
use crate::mmap_storage::MmapStorageManager;
use crate::zorder;
use crate::ZVerseConfig;

/// Persistent ZVerse implementation using memory-mapped storage
pub struct PersistentZVerse {
    /// Storage manager
    storage: MmapStorageManager,
    
    /// Latest commit version
    latest_commit_version: AtomicU64,
    
    /// Version index cache (version -> entries)
    version_index: RwLock<HashMap<u64, Vec<ZEntry>>>,
    
    /// Key hash index cache (key_hash -> entries)
    key_index: RwLock<HashMap<u32, Vec<ZEntry>>>,
    
    /// Configuration
    config: ZVerseConfig,
    
    /// Data directory path
    data_path: PathBuf,
}

impl PersistentZVerse {
    /// Create a new persistent ZVerse instance
    pub fn new(config: ZVerseConfig) -> std::result::Result<Self, Error> {
        let data_path = Path::new(&config.data_path).to_path_buf();
        
        // Create storage manager
        let storage = MmapStorageManager::new(
            &data_path.join("segments"),
            Some(config.segment_size),
            Some(config.max_entries_per_segment),
        )?;
        
        // Find the highest version number in existing data
        let mut max_version = 0;
        
        // Scan all entries to build indexes and find max version
        match storage.find_entries_in_range(0, u64::MAX) {
            Ok(entries) => {
                for entry in entries {
                    max_version = max_version.max(entry.version);
                }
            }
            Err(_) => {
                // If we can't read existing data, start fresh
                max_version = 0;
            }
        }
        
        Ok(Self {
            storage,
            latest_commit_version: AtomicU64::new(max_version.max(1)),
            version_index: RwLock::new(HashMap::new()),
            key_index: RwLock::new(HashMap::new()),
            config,
            data_path,
        })
    }
    
    /// Get the next version number
    fn next_version(&self) -> u64 {
        self.latest_commit_version.fetch_add(1, Ordering::SeqCst)
    }
    
    /// Get the latest version number
    fn latest_version(&self) -> u64 {
        self.latest_commit_version.load(Ordering::SeqCst)
    }
    
    /// Calculate key hash
    fn calculate_key_hash<K: AsRef<[u8]>>(&self, key: K) -> u32 {
        let key_bytes = key.as_ref();
        
        // Use the first 4 bytes of key as hash, or hash the key if it's shorter
        if key_bytes.len() >= 4 {
            u32::from_le_bytes([key_bytes[0], key_bytes[1], key_bytes[2], key_bytes[3]])
        } else {
            // Simple hash function for short keys
            let mut hash = 0u32;
            for (i, &b) in key_bytes.iter().enumerate() {
                hash = hash.wrapping_add(b as u32).wrapping_mul(31u32.wrapping_add(i as u32));
            }
            hash
        }
    }
    
    /// Calculate Z-value for key and version
    fn calculate_z_value<K: AsRef<[u8]>>(&self, key: K, version: u64) -> u64 {
        let key_hash = self.calculate_key_hash(key);
        let version_u32 = (version & 0xFFFFFFFF) as u32;
        zorder::calculate_z_value(key_hash, version_u32)
    }
    
    /// Get a value for a key at a specific version
    pub fn get<K, V>(&self, key: K, version: Option<u64>) -> std::result::Result<Option<V>, Error>
    where
        K: AsRef<[u8]>,
        V: From<Vec<u8>>,
    {
        let key_bytes = key.as_ref();
        let version = version.unwrap_or_else(|| self.latest_version());
        let key_hash = self.calculate_key_hash(&key_bytes);
        
        // Calculate min/max Z-values for this key across all versions
        let z_min = zorder::min_z_for_key(key_hash);
        let z_max = zorder::max_z_for_key(key_hash);
        
        // Find all entries for this key
        let entries = self.storage.find_entries_in_range(z_min, z_max)?;
        
        // Find the entry with the largest version <= requested version
        let mut result = None;
        let mut latest_matching_version = 0;
        
        for entry in entries {
            // Extract the key hash from the entry's Z-value
            let entry_key_hash = zorder::extract_key_hash(entry.z_value);
            
            // If this entry is for our key and its version is <= requested version
            if entry_key_hash == key_hash && entry.version <= version && entry.version > latest_matching_version {
                // Get the actual key bytes to verify (since hash collisions are possible)
                let entry_key_bytes = self.storage.get_key_bytes(&entry)?;
                
                if entry_key_bytes == key_bytes {
                    latest_matching_version = entry.version;
                    
                    if entry.is_deleted() {
                        result = None; // Key was deleted at this version
                    } else {
                        // Get the value bytes
                        let value_bytes = self.storage.get_value_bytes(&entry)?;
                        result = Some(value_bytes);
                    }
                }
            }
        }
        
        Ok(result.map(V::from))
    }
    
    /// Put a value for a key
    pub fn put<K, V>(&self, key: K, value: V) -> std::result::Result<u64, Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key_bytes = key.as_ref().to_vec();
        let value_bytes = value.as_ref().to_vec();
        let version = self.next_version();
        
        // Calculate Z-value
        let z_value = self.calculate_z_value(&key_bytes, version);
        
        // Create entry with key and value offsets to be filled by the storage layer
        let entry = ZEntry {
            z_value,
            key_offset: 0, // Will be set by storage
            key_length: key_bytes.len() as u16,
            value_offset: 0, // Will be set by storage
            value_length: value_bytes.len() as u32,
            version,
            flags: 0,
        };
        
        // Append entry to storage
        self.storage.append_entry(entry, &key_bytes, &value_bytes)?;
        
        // Update indexes
        self.update_indexes(entry, &key_bytes);
        
        Ok(version)
    }
    
    /// Delete a key
    pub fn delete<K>(&self, key: K) -> std::result::Result<u64, Error>
    where
        K: AsRef<[u8]>,
    {
        let key_bytes = key.as_ref().to_vec();
        let version = self.next_version();
        
        // Calculate Z-value
        let z_value = self.calculate_z_value(&key_bytes, version);
        
        // Create tombstone entry
        let mut entry = ZEntry {
            z_value,
            key_offset: 0, // Will be set by storage
            key_length: key_bytes.len() as u16,
            value_offset: 0, // Will be set by storage
            value_length: 0,
            version,
            flags: 0,
        };
        
        // Mark as deleted
        entry.set_deleted(true);
        
        // Append entry to storage
        self.storage.append_entry(entry, &key_bytes, &Vec::new())?;
        
        // Update indexes
        self.update_indexes(entry, &key_bytes);
        
        Ok(version)
    }
    
    /// Scan a range of keys at a specific version
    pub fn scan<K>(
        &self,
        start: Option<K>,
        end: Option<K>,
        version: Option<u64>,
    ) -> std::result::Result<Box<dyn Iterator<Item = std::result::Result<(Vec<u8>, Vec<u8>), Error>> + '_>, Error>
    where
        K: AsRef<[u8]>,
    {
        let version = version.unwrap_or_else(|| self.latest_version());
        
        // For simplicity, we'll scan all entries and filter
        // In a real implementation, we'd use Z-curve properties to optimize this
        
        // Get all segments
        let all_entries = self.get_all_entries()?;
        
        // Build a map of key -> (version, value) for the latest version of each key
        let mut result_map: HashMap<Vec<u8>, (u64, Vec<u8>)> = HashMap::new();
        
        for entry in all_entries {
            if entry.version <= version {
                let key_bytes = self.storage.get_key_bytes(&entry)?;
                
                // Check range bounds
                let start_in_range = start.as_ref().map_or(true, |s| key_bytes >= s.as_ref().to_vec());
                let end_in_range = end.as_ref().map_or(true, |e| key_bytes < e.as_ref().to_vec());
                
                if start_in_range && end_in_range {
                    let existing = result_map.get(&key_bytes).cloned();
                    
                    let should_update = match existing {
                        None => true,
                        Some((existing_version, _)) => entry.version > existing_version,
                    };
                    
                    if should_update {
                        if entry.is_deleted() {
                            result_map.remove(&key_bytes);
                        } else {
                            let value_bytes = self.storage.get_value_bytes(&entry)?;
                            result_map.insert(key_bytes.clone(), (entry.version, value_bytes));
                        }
                    }
                }
            }
        }
        
        // Convert the map to a vector of results
        let results: Vec<_> = result_map.into_iter()
            .map(|(k, (_, v))| Ok((k, v)))
            .collect();
        
        Ok(Box::new(results.into_iter()))
    }
    
    /// Get the history of a key
    pub fn history<K>(
        &self,
        key: K,
        start_version: Option<u64>,
        end_version: Option<u64>,
    ) -> std::result::Result<Box<dyn Iterator<Item = std::result::Result<(u64, Vec<u8>), Error>> + '_>, Error>
    where
        K: AsRef<[u8]>,
    {
        let key_bytes = key.as_ref();
        let start_version = start_version.unwrap_or(0);
        let end_version = end_version.unwrap_or_else(|| self.latest_version());
        
        let key_hash = self.calculate_key_hash(key_bytes);
        
        // Calculate min/max Z-values for this key across all versions
        let z_min = zorder::min_z_for_key(key_hash);
        let z_max = zorder::max_z_for_key(key_hash);
        
        // Find all entries for this key
        let entries = self.storage.find_entries_in_range(z_min, z_max)?;
        
        // Filter entries by key and version range
        let mut history_entries = Vec::new();
        
        for entry in entries {
            let entry_key_hash = zorder::extract_key_hash(entry.z_value);
            
            if entry_key_hash == key_hash && entry.version >= start_version && entry.version <= end_version {
                // Verify the key (since hash collisions are possible)
                let entry_key_bytes = self.storage.get_key_bytes(&entry)?;
                
                if entry_key_bytes == key_bytes {
                    if entry.is_deleted() {
                        history_entries.push(Ok((entry.version, Vec::new())));
                    } else {
                        let value_bytes = self.storage.get_value_bytes(&entry)?;
                        history_entries.push(Ok((entry.version, value_bytes)));
                    }
                }
            }
        }
        
        // Sort by version
        history_entries.sort_by(|a, b| {
            let a_version = a.as_ref().map(|(v, _)| *v).unwrap_or(0);
            let b_version = b.as_ref().map(|(v, _)| *v).unwrap_or(0);
            a_version.cmp(&b_version)
        });
        
        Ok(Box::new(history_entries.into_iter()))
    }
    
    /// Get a snapshot at a specific version
    pub fn snapshot(
        &self,
        version: u64,
    ) -> std::result::Result<Box<dyn Iterator<Item = std::result::Result<(Vec<u8>, Vec<u8>), Error>> + '_>, Error> {
        // Similar to scan, but for all keys
        self.scan::<&[u8]>(None, None, Some(version))
    }
    
    /// Flush all pending writes
    pub fn flush(&self) -> std::result::Result<(), Error> {
        self.storage.flush()
    }
    
    /// Close the database
    pub fn close(&self) -> std::result::Result<(), Error> {
        self.storage.close()
    }
    
    /// Update internal indexes
    fn update_indexes(&self, entry: ZEntry, key_bytes: &[u8]) {
        let key_hash = self.calculate_key_hash(key_bytes);
        
        // Update version index
        let mut version_index = self.version_index.write().unwrap();
        version_index.entry(entry.version)
            .or_insert_with(Vec::new)
            .push(entry);
        
        // Update key index
        let mut key_index = self.key_index.write().unwrap();
        key_index.entry(key_hash)
            .or_insert_with(Vec::new)
            .push(entry);
    }
    
    /// Get all entries from all segments
    fn get_all_entries(&self) -> std::result::Result<Vec<ZEntry>, Error> {
        // This is a simplification - in a real implementation we'd have a more efficient way
        // to iterate all entries
        
        // Find all segments by scanning the entire Z-value range
        let entries = self.storage.find_entries_in_range(0, u64::MAX)?;
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    
    #[test]
    fn test_persistent_basic_operations() {
        let dir = tempdir().unwrap();
        
        let config = ZVerseConfig {
            data_path: dir.path().to_string_lossy().to_string(),
            segment_size: 1024 * 1024, // 1MB
            max_entries_per_segment: 1000,
            sync_writes: true,
            cache_size_bytes: 10 * 1024 * 1024, // 10MB
            background_threads: 1,
        };
        
        let db = PersistentZVerse::new(config).unwrap();
        
        // Put a value
        let v1 = db.put("key1", "value1").unwrap();
        
        // Get the value
        let result1: Option<String> = db.get("key1", None)
            .unwrap()
            .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
        
        assert_eq!(result1, Some("value1".to_string()));
        
        // Update the value
        let _v2 = db.put("key1", "value1-updated").unwrap();
        
        // Get the updated value
        let result2: Option<String> = db.get("key1", None)
            .unwrap()
            .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
        
        assert_eq!(result2, Some("value1-updated".to_string()));
        
        // Get the original version
        let result1_v1: Option<String> = db.get("key1", Some(v1))
            .unwrap()
            .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
        
        assert_eq!(result1_v1, Some("value1".to_string()));
    }
}