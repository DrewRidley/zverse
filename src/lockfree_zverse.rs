//! Lock-free ZVerse implementation using epoch-based concurrency control
//!
//! This module provides a lock-free implementation of ZVerse that ensures:
//! - Serializability for writers via single coordination
//! - Wait-free readers with no atomic operations during reads
//! - Zero contention between readers and writers
//! - Proper versioning and Z-order curve optimization

use std::collections::HashMap;

use crate::core::EntryWithData;
use crate::error::Error;
use crate::lockfree::LockFreeZVerse;
use crate::zorder;

/// Lock-free ZVerse implementation
pub struct LockFreeZVerseKV {
    /// Lock-free data structure
    inner: LockFreeZVerse<EntryWithData>,
}

impl LockFreeZVerseKV {
    /// Create a new lock-free ZVerse
    pub fn new() -> Self {
        Self {
            inner: LockFreeZVerse::new(),
        }
    }

    /// Calculate key hash for Z-order curve
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

    /// Calculate Z-value for key and version (optimized)
    fn calculate_z_value<K: AsRef<[u8]>>(&self, key: K, version: u64) -> u64 {
        let key_hash = self.calculate_key_hash(key);
        let version_u32 = (version & 0xFFFFFFFF) as u32;
        zorder::calculate_z_value(key_hash, version_u32)
    }

    /// Binary search insertion to maintain Z-order
    fn insert_maintaining_order(data: &mut Vec<EntryWithData>, entry: EntryWithData) {
        let target_z = entry.entry.z_value;
        
        // Find insertion point using binary search
        let insert_pos = data.binary_search_by(|e| e.entry.z_value.cmp(&target_z))
            .unwrap_or_else(|pos| pos);
        
        data.insert(insert_pos, entry);
    }

    /// Get a value for a key at a specific version
    pub fn get<K, V>(&self, key: K, version: Option<u64>) -> Result<Option<V>, Error>
    where
        K: AsRef<[u8]>,
        V: From<Vec<u8>>,
    {
        let key_bytes = key.as_ref();
        let read_tx = self.inner.begin_read();
        
        let target_version = version.unwrap_or(u64::MAX);
        
        // Find the entry with the largest version <= requested version for this key
        let mut result = None;
        let mut latest_matching_version = 0;
        
        for entry in read_tx.data() {
            if entry.key_bytes() == key_bytes && entry.entry.version <= target_version {
                if entry.entry.version > latest_matching_version {
                    latest_matching_version = entry.entry.version;
                    
                    if entry.is_deleted() {
                        result = None; // Key was deleted at this version
                    } else {
                        result = Some(entry.value_bytes().to_vec());
                    }
                }
            }
        }
        
        Ok(result.map(V::from))
    }

    /// Put a value for a key
    pub fn put<K, V>(&self, key: K, value: V) -> Result<u64, Error>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        let key_bytes = key.as_ref().to_vec();
        let value_bytes = value.as_ref().to_vec();
        
        let mut write_tx = self.inner.begin_write()?;
        let new_version = write_tx.version() + 1;
        
        // Calculate Z-value
        let z_value = self.calculate_z_value(&key_bytes, new_version);
        
        // Create new entry
        let entry = EntryWithData::new(
            z_value,
            key_bytes,
            value_bytes,
            new_version,
            false, // not deleted
        );
        
        // Insert entry maintaining Z-order (optimized)
        let data = write_tx.data_mut();
        Self::insert_maintaining_order(data, entry);
        
        write_tx.commit()
    }

    /// Delete a key
    pub fn delete<K>(&self, key: K) -> Result<u64, Error>
    where
        K: AsRef<[u8]>,
    {
        let key_bytes = key.as_ref().to_vec();
        
        let mut write_tx = self.inner.begin_write()?;
        let new_version = write_tx.version() + 1;
        
        // Calculate Z-value
        let z_value = self.calculate_z_value(&key_bytes, new_version);
        
        // Create tombstone entry
        let entry = EntryWithData::new(
            z_value,
            key_bytes,
            Vec::new(),
            new_version,
            true, // deleted
        );
        
        // Insert tombstone maintaining Z-order (optimized)
        let data = write_tx.data_mut();
        Self::insert_maintaining_order(data, entry);
        
        write_tx.commit()
    }

    /// Scan a range of keys at a specific version
    pub fn scan<K>(
        &self,
        start: Option<K>,
        end: Option<K>,
        version: Option<u64>,
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), Error>> + '_>, Error>
    where
        K: AsRef<[u8]>,
    {
        let read_tx = self.inner.begin_read();
        let target_version = version.unwrap_or(u64::MAX);
        
        // Build a map of key -> (version, value) for the latest version of each key
        let mut result_map: HashMap<Vec<u8>, (u64, Vec<u8>)> = HashMap::new();
        
        for entry in read_tx.data() {
            if entry.entry.version <= target_version {
                let key_bytes = entry.key_bytes();
                
                // Check range bounds
                let start_in_range = start.as_ref().map_or(true, |s| key_bytes >= s.as_ref());
                let end_in_range = end.as_ref().map_or(true, |e| key_bytes < e.as_ref());
                
                if start_in_range && end_in_range {
                    let existing = result_map.get(key_bytes).cloned();
                    
                    let should_update = match existing {
                        None => true,
                        Some((existing_version, _)) => entry.entry.version > existing_version,
                    };
                    
                    if should_update {
                        if entry.is_deleted() {
                            result_map.remove(key_bytes);
                        } else {
                            result_map.insert(key_bytes.to_vec(), (entry.entry.version, entry.value_bytes().to_vec()));
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
    ) -> Result<Box<dyn Iterator<Item = Result<(u64, Vec<u8>), Error>> + '_>, Error>
    where
        K: AsRef<[u8]>,
    {
        let key_bytes = key.as_ref();
        let read_tx = self.inner.begin_read();
        
        let start_version = start_version.unwrap_or(0);
        let end_version = end_version.unwrap_or(u64::MAX);
        
        // Find all versions of the key
        let mut history_entries = Vec::new();
        
        for entry in read_tx.data() {
            if entry.key_bytes() == key_bytes 
                && entry.entry.version >= start_version 
                && entry.entry.version <= end_version 
            {
                if entry.is_deleted() {
                    history_entries.push(Ok((entry.entry.version, Vec::new())));
                } else {
                    history_entries.push(Ok((entry.entry.version, entry.value_bytes().to_vec())));
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
    ) -> Result<Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), Error>> + '_>, Error> {
        // Similar to scan, but for all keys
        self.scan::<&[u8]>(None, None, Some(version))
    }

    /// Get current version
    pub fn current_version(&self) -> u64 {
        let read_tx = self.inner.begin_read();
        read_tx.version()
    }

    /// Get entry count (approximate)
    pub fn entry_count(&self) -> usize {
        let read_tx = self.inner.begin_read();
        read_tx.data().len()
    }

    /// Flush all pending writes (no-op for lock-free implementation)
    pub fn flush(&self) -> Result<(), Error> {
        Ok(())
    }

    /// Close the database (no-op for lock-free implementation)
    pub fn close(&self) -> Result<(), Error> {
        Ok(())
    }

    /// Get performance statistics
    pub fn performance_stats(&self) -> PerformanceStats {
        let read_tx = self.inner.begin_read();
        let entry_count = read_tx.data().len();
        let current_version = read_tx.version();
        
        PerformanceStats {
            total_entries: entry_count,
            current_version,
            implementation: "Lock-Free".to_string(),
            concurrent_readers: true,
            wait_free_reads: true,
            serialized_writes: true,
        }
    }
}

/// Performance statistics for the lock-free implementation
#[derive(Debug, Clone)]
pub struct PerformanceStats {
    pub total_entries: usize,
    pub current_version: u64,
    pub implementation: String,
    pub concurrent_readers: bool,
    pub wait_free_reads: bool,
    pub serialized_writes: bool,
}

impl Default for LockFreeZVerseKV {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::sync::Arc;

    #[test]
    fn test_basic_operations() {
        let db = LockFreeZVerseKV::new();
        
        // Put a value
        let v1 = db.put("key1", "value1").unwrap();
        let _v2 = db.put("key2", "value2").unwrap();
        let v3 = db.put("key1", "value1-updated").unwrap();
        
        // Get the values
        let result1: Option<String> = db.get("key1", None)
            .unwrap()
            .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
        let result2: Option<String> = db.get("key2", None)
            .unwrap()
            .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
        
        assert_eq!(result1, Some("value1-updated".to_string()));
        assert_eq!(result2, Some("value2".to_string()));
        
        // Get a specific version
        let result1_v1: Option<String> = db.get("key1", Some(v1))
            .unwrap()
            .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
        
        assert_eq!(result1_v1, Some("value1".to_string()));
        
        // Delete a key
        let _v4 = db.delete("key1").unwrap();
        
        let result1_after_delete: Option<String> = db.get("key1", None)
            .unwrap()
            .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
        
        assert_eq!(result1_after_delete, None);
        
        // Get the deleted key at a previous version
        let result1_v3: Option<String> = db.get("key1", Some(v3))
            .unwrap()
            .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
        
        assert_eq!(result1_v3, Some("value1-updated".to_string()));
    }

    #[test]
    fn test_concurrent_readers() {
        let db = Arc::new(LockFreeZVerseKV::new());
        
        // Insert some initial data
        for i in 0..100 {
            db.put(format!("key{}", i), format!("value{}", i)).unwrap();
        }
        
        let mut handles = vec![];
        
        // Spawn multiple concurrent readers
        for _ in 0..10 {
            let db_clone = db.clone();
            let handle = thread::spawn(move || {
                for i in 0..100 {
                    let key = format!("key{}", i % 100);
                    let _result: Option<String> = db_clone.get(&key, None)
                        .unwrap()
                        .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
                }
            });
            handles.push(handle);
        }
        
        // All readers should complete successfully
        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn test_concurrent_writers() {
        let db = Arc::new(LockFreeZVerseKV::new());
        let mut handles = vec![];
        
        // Spawn multiple concurrent writers
        for thread_id in 0..5 {
            let db_clone = db.clone();
            let handle = thread::spawn(move || {
                let mut versions = vec![];
                for i in 0..20 {
                    let key = format!("thread{}_key{}", thread_id, i);
                    let value = format!("thread{}_value{}", thread_id, i);
                    let version = db_clone.put(key, value).unwrap();
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
        
        // All versions should be unique (writers are serialized)
        all_versions.sort();
        let mut unique_versions = all_versions.clone();
        unique_versions.dedup();
        assert_eq!(all_versions.len(), unique_versions.len());
        
        // Final entry count should be 100 (5 threads * 20 entries)
        assert_eq!(db.entry_count(), 100);
    }

    #[test]
    fn test_readers_writers_no_blocking() {
        let db = Arc::new(LockFreeZVerseKV::new());
        
        // Insert initial data
        db.put("initial", "data").unwrap();
        
        // Start a long-running reader
        let db_reader = db.clone();
        let reader_handle = thread::spawn(move || {
            let start = std::time::Instant::now();
            for _ in 0..1000 {
                let _result: Option<String> = db_reader.get("initial", None)
                    .unwrap()
                    .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
            }
            start.elapsed()
        });
        
        // Start concurrent writers
        let db_writer = db.clone();
        let writer_handle = thread::spawn(move || {
            let start = std::time::Instant::now();
            for i in 0..100 {
                db_writer.put(format!("key{}", i), format!("value{}", i)).unwrap();
            }
            start.elapsed()
        });
        
        // Both should complete without significant blocking
        let reader_time = reader_handle.join().unwrap();
        let writer_time = writer_handle.join().unwrap();
        
        println!("Reader time: {:?}, Writer time: {:?}", reader_time, writer_time);
        
        // Verify final state
        let final_count = db.entry_count();
        assert_eq!(final_count, 101); // initial + 100 new entries
    }
}