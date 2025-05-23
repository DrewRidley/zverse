//! Simplified in-memory implementation of ZVerse
//! 
//! This module provides a basic in-memory implementation of the ZVerse
//! architecture to validate the Z-order curve concepts without the complexity
//! of memory-mapping and disk persistence.

use std::cmp::Ordering;

use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::RwLock;

use crate::zorder;
use crate::Error;

/// Entry stored in memory
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct InMemoryEntry {
    /// Z-value (interleaved key_hash + version)
    pub z_value: u64,
    /// Original key
    pub key: Vec<u8>,
    /// Value data
    pub value: Vec<u8>,
    /// Version number
    pub version: u64,
    /// Whether this entry is a tombstone (deleted)
    pub deleted: bool,
}

impl InMemoryEntry {
    /// Create a new entry
    pub fn new(key: Vec<u8>, value: Vec<u8>, version: u64, deleted: bool) -> Self {
        // Use the first 4 bytes of key as hash, or hash the key if it's shorter
        let key_hash = if key.len() >= 4 {
            u32::from_le_bytes([key[0], key[1], key[2], key[3]])
        } else {
            // Simple hash function for short keys
            let mut hash = 0u32;
            for (i, &b) in key.iter().enumerate() {
                hash = hash.wrapping_add(b as u32).wrapping_mul(31u32.wrapping_add(i as u32));
            }
            hash
        };
        
        let version_u32 = (version & 0xFFFFFFFF) as u32;
        let z_value = zorder::calculate_z_value(key_hash, version_u32);
        
        Self {
            z_value,
            key,
            value,
            version,
            deleted,
        }
    }
}

// For sorting entries by z_value
impl Ord for InMemoryEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.z_value.cmp(&other.z_value)
    }
}

impl PartialOrd for InMemoryEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// In-memory ZVerse implementation
pub struct InMemoryZVerse {
    /// Entries sorted by z_value
    entries: RwLock<Vec<InMemoryEntry>>,
    /// Latest commit version
    latest_commit_version: AtomicU64,
}

impl InMemoryZVerse {
    /// Create a new in-memory ZVerse
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(Vec::new()),
            latest_commit_version: AtomicU64::new(1),
        }
    }
    
    /// Get the next version number
    pub fn next_version(&self) -> u64 {
        self.latest_commit_version.fetch_add(1, AtomicOrdering::SeqCst)
    }
    
    /// Get the latest version number
    pub fn latest_version(&self) -> u64 {
        self.latest_commit_version.load(AtomicOrdering::SeqCst)
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
        
        let entry = InMemoryEntry::new(key_bytes, value_bytes, version, false);
        
        let mut entries = self.entries.write().unwrap();
        entries.push(entry);
        
        // Sort entries by z_value
        entries.sort_by_key(|e| e.z_value);
        
        Ok(version)
    }
    
    /// Get a value for a key at a specific version
    pub fn get<K, V>(&self, key: K, version: Option<u64>) -> std::result::Result<Option<V>, Error>
    where
        K: AsRef<[u8]>,
        V: From<Vec<u8>>,
    {
        let key_bytes = key.as_ref();
        let version = version.unwrap_or_else(|| self.latest_version());
        
        let entries = self.entries.read().unwrap();
        
        // Find the entry with the largest version <= requested version
        let mut result = None;
        let mut latest_matching_version = 0;
        
        for entry in entries.iter() {
            if entry.key == key_bytes && entry.version <= version && entry.version > latest_matching_version {
                latest_matching_version = entry.version;
                if !entry.deleted {
                    result = Some(entry.value.clone());
                } else {
                    result = None; // Key was deleted at this version
                }
            }
        }
        
        Ok(result.map(V::from))
    }
    
    /// Delete a key
    pub fn delete<K>(&self, key: K) -> std::result::Result<u64, Error>
    where
        K: AsRef<[u8]>,
    {
        let key_bytes = key.as_ref().to_vec();
        let version = self.next_version();
        
        let entry = InMemoryEntry::new(key_bytes, Vec::new(), version, true);
        
        let mut entries = self.entries.write().unwrap();
        entries.push(entry);
        
        // Sort entries by z_value
        entries.sort_by_key(|e| e.z_value);
        
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
        let entries = self.entries.read().unwrap();
        
        // For each key, find the entry with the largest version <= requested version
        let mut result_map = std::collections::HashMap::new();
        
        for entry in entries.iter() {
            if entry.version <= version {
                let start_in_range = start.as_ref().map_or(true, |s| entry.key >= s.as_ref().to_vec());
                let end_in_range = end.as_ref().map_or(true, |e| entry.key < e.as_ref().to_vec());
                
                if start_in_range && end_in_range {
                    let existing = result_map.get(&entry.key).cloned();
                    
                    let should_update = match existing {
                        None => true,
                        Some((existing_version, _)) => entry.version > existing_version,
                    };
                    
                    if should_update {
                        if entry.deleted {
                            result_map.remove(&entry.key);
                        } else {
                            result_map.insert(entry.key.clone(), (entry.version, entry.value.clone()));
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
        
        let entries = self.entries.read().unwrap();
        
        // Find all versions of the key
        let mut results = Vec::new();
        
        for entry in entries.iter() {
            if entry.key == key_bytes && entry.version >= start_version && entry.version <= end_version {
                if entry.deleted {
                    results.push(Ok((entry.version, Vec::new())));
                } else {
                    results.push(Ok((entry.version, entry.value.clone())));
                }
            }
        }
        
        // Sort by version
        results.sort_by(|a, b| {
            let a_version = a.as_ref().map(|(v, _)| *v).unwrap_or(0);
            let b_version = b.as_ref().map(|(v, _)| *v).unwrap_or(0);
            a_version.cmp(&b_version)
        });
        
        Ok(Box::new(results.into_iter()))
    }
    
    /// Get a snapshot of all keys at a specific version
    pub fn snapshot(
        &self,
        version: u64,
    ) -> std::result::Result<Box<dyn Iterator<Item = std::result::Result<(Vec<u8>, Vec<u8>), Error>> + '_>, Error> {
        // Similar to scan, but for all keys
        self.scan::<&[u8]>(None, None, Some(version))
    }
    
    /// Count the number of entries
    pub fn count_entries(&self) -> usize {
        let entries = self.entries.read().unwrap();
        entries.len()
    }
}

impl Default for InMemoryZVerse {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_basic_operations() {
        let db = InMemoryZVerse::new();
        
        // Put a value
        let v1 = db.put("key1", "value1").unwrap();
        let v2 = db.put("key2", "value2").unwrap();
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
    fn test_scan() {
        let db = InMemoryZVerse::new();
        
        // Put some values
        db.put("key1", "value1").unwrap();
        db.put("key2", "value2").unwrap();
        db.put("key3", "value3").unwrap();
        
        // Scan all keys
        let results: Vec<(String, String)> = db.scan::<&[u8]>(None, None, None)
            .unwrap()
            .map(|r| {
                let (k, v) = r.unwrap();
                (String::from_utf8(k).unwrap(), String::from_utf8(v).unwrap())
            })
            .collect();
        
        assert_eq!(results.len(), 3);
        assert!(results.contains(&("key1".to_string(), "value1".to_string())));
        assert!(results.contains(&("key2".to_string(), "value2".to_string())));
        assert!(results.contains(&("key3".to_string(), "value3".to_string())));
        
        // Scan a range
        let results: Vec<(String, String)> = db.scan(Some("key1"), Some("key3"), None)
            .unwrap()
            .map(|r| {
                let (k, v) = r.unwrap();
                (String::from_utf8(k).unwrap(), String::from_utf8(v).unwrap())
            })
            .collect();
        
        assert_eq!(results.len(), 2);
        assert!(results.contains(&("key1".to_string(), "value1".to_string())));
        assert!(results.contains(&("key2".to_string(), "value2".to_string())));
    }
    
    #[test]
    fn test_history() {
        let db = InMemoryZVerse::new();
        
        // Put multiple versions of a key
        let v1 = db.put("key1", "value1-v1").unwrap();
        let v2 = db.put("key1", "value1-v2").unwrap();
        let v3 = db.put("key1", "value1-v3").unwrap();
        
        // Get history
        let history: Vec<(u64, String)> = db.history("key1", None, None)
            .unwrap()
            .map(|r| {
                let (v, value) = r.unwrap();
                (v, String::from_utf8(value).unwrap())
            })
            .collect();
        
        assert_eq!(history.len(), 3);
        assert_eq!(history[0], (v1, "value1-v1".to_string()));
        assert_eq!(history[1], (v2, "value1-v2".to_string()));
        assert_eq!(history[2], (v3, "value1-v3".to_string()));
    }
}