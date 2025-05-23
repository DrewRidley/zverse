//! Key-timestamp interleaving implementation for COW semantics
//!
//! This module implements the core interleaving algorithm that combines user keys
//! with timestamps to create a space-filling curve through both key space and time.

use byteorder::{BigEndian, ByteOrder};
use std::cmp::Ordering;

/// An interleaved key that combines user key bytes with timestamp bytes
/// to create natural COW versioning and temporal-spatial clustering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterleavedKey {
    /// The interleaved key-timestamp bytes used for storage ordering
    pub interleaved: Vec<u8>,
    /// Original user key for exact matching and extraction
    pub original_key: Vec<u8>,
    /// Timestamp in microseconds since Unix epoch
    pub timestamp: u64,
}

impl InterleavedKey {
    /// Create a new interleaved key from user key and timestamp
    pub fn new(key: &[u8], timestamp: u64) -> Self {
        let interleaved = Self::interleave_bytes(key, timestamp);
        
        Self {
            interleaved,
            original_key: key.to_vec(),
            timestamp,
        }
    }

    /// Interleave key bytes with timestamp bytes
    fn interleave_bytes(key: &[u8], timestamp: u64) -> Vec<u8> {
        let mut timestamp_bytes = [0u8; 8];
        BigEndian::write_u64(&mut timestamp_bytes, timestamp);
        
        let mut interleaved = Vec::with_capacity(key.len() + 8);
        
        // Interleave key bytes with timestamp bytes
        for (i, &key_byte) in key.iter().enumerate() {
            interleaved.push(key_byte);
            
            // Add timestamp byte, cycling through all 8 bytes
            let timestamp_index = i % 8;
            interleaved.push(timestamp_bytes[timestamp_index]);
        }
        
        // If key is shorter than 8 bytes, append remaining timestamp bytes
        if key.len() < 8 {
            interleaved.extend_from_slice(&timestamp_bytes[key.len()..]);
        }
        
        interleaved
    }

    /// Extract the original key
    pub fn original_key(&self) -> &[u8] {
        &self.original_key
    }

    /// Extract the timestamp
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Get the interleaved bytes for storage/comparison
    pub fn interleaved_bytes(&self) -> &[u8] {
        &self.interleaved
    }

    /// Create an interleaved key with the maximum possible timestamp for range queries
    pub fn with_max_timestamp(key: &[u8]) -> Self {
        Self::new(key, u64::MAX)
    }

    /// Create an interleaved key with the minimum possible timestamp for range queries
    pub fn with_min_timestamp(key: &[u8]) -> Self {
        Self::new(key, 0)
    }

    /// Get the first N bytes of the interleaved key for page bounds
    pub fn prefix(&self, n: usize) -> Vec<u8> {
        if self.interleaved.len() <= n {
            self.interleaved.clone()
        } else {
            self.interleaved[..n].to_vec()
        }
    }

    /// Parse an interleaved key from stored bytes, recovering original key and timestamp
    pub fn from_interleaved_bytes(interleaved: &[u8]) -> Option<Self> {
        if interleaved.is_empty() {
            return None;
        }

        // Attempt to extract original key and timestamp
        // This is a best-effort reconstruction
        let mut original_key = Vec::new();
        let mut timestamp_bytes = [0u8; 8];
        
        if interleaved.len() == 8 {
            // Edge case: empty original key, just timestamp
            timestamp_bytes.copy_from_slice(interleaved);
        } else if interleaved.len() < 16 {
            // Short key case: key bytes followed by remaining timestamp bytes
            let key_len = interleaved.len() - 8;
            for i in 0..key_len {
                original_key.push(interleaved[i * 2]);
                timestamp_bytes[i] = interleaved[i * 2 + 1];
            }
            // Copy remaining timestamp bytes
            timestamp_bytes[key_len..].copy_from_slice(&interleaved[key_len * 2..]);
        } else {
            // Standard case: extract interleaved pattern
            let mut i = 0;
            let mut ts_index = 0;
            while i < interleaved.len() - 1 && ts_index < 8 {
                original_key.push(interleaved[i]);
                timestamp_bytes[ts_index] = interleaved[i + 1];
                i += 2;
                ts_index += 1;
            }
            
            // Handle remaining key bytes if key was longer than 8 bytes
            while i < interleaved.len() - 1 {
                original_key.push(interleaved[i]);
                // Skip timestamp byte (already collected in first 8 iterations)
                i += 2;
            }
        }

        let timestamp = BigEndian::read_u64(&timestamp_bytes);
        Some(Self {
            interleaved: interleaved.to_vec(),
            original_key,
            timestamp,
        })
    }

    /// Check if this key is a prefix match for another key
    pub fn is_prefix_of(&self, other: &InterleavedKey) -> bool {
        if self.original_key.len() > other.original_key.len() {
            return false;
        }
        other.original_key.starts_with(&self.original_key)
    }

    /// Create a range bounds for querying keys with a specific prefix at a timestamp
    pub fn prefix_range(prefix: &[u8], timestamp: u64) -> (Self, Self) {
        let start = Self::new(prefix, timestamp);
        
        // Create end bound by incrementing the prefix if possible
        let mut end_prefix = prefix.to_vec();
        let mut carry = true;
        for byte in end_prefix.iter_mut().rev() {
            if carry {
                if *byte == 255 {
                    *byte = 0;
                } else {
                    *byte += 1;
                    carry = false;
                    break;
                }
            }
        }
        
        let end = if carry {
            // Prefix was all 255s, use max timestamp with same prefix
            Self::new(prefix, u64::MAX)
        } else {
            Self::new(&end_prefix, timestamp)
        };
        
        (start, end)
    }
}

impl PartialOrd for InterleavedKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InterleavedKey {
    fn cmp(&self, other: &Self) -> Ordering {
        // Primary ordering by interleaved bytes
        self.interleaved.cmp(&other.interleaved)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_interleaving() {
        let key = b"hello";
        let timestamp = 0x0123456789ABCDEF;
        let ik = InterleavedKey::new(key, timestamp);
        
        assert_eq!(ik.original_key(), b"hello");
        assert_eq!(ik.timestamp(), timestamp);
        
        // Verify interleaving pattern
        let expected = vec![
            b'h', 0x01,  // h + timestamp[0]
            b'e', 0x23,  // e + timestamp[1] 
            b'l', 0x45,  // l + timestamp[2]
            b'l', 0x67,  // l + timestamp[3]
            b'o', 0x89,  // o + timestamp[4]
            0xAB, 0xCD, 0xEF  // remaining timestamp bytes
        ];
        assert_eq!(ik.interleaved_bytes(), &expected);
    }

    #[test]
    fn test_empty_key() {
        let timestamp = 0x0123456789ABCDEF;
        let ik = InterleavedKey::new(b"", timestamp);
        
        assert_eq!(ik.original_key(), b"");
        assert_eq!(ik.timestamp(), timestamp);
        assert_eq!(ik.interleaved_bytes(), &[0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF]);
    }

    #[test]
    fn test_long_key() {
        let key = b"this_is_a_very_long_key";
        let timestamp = 0x0123456789ABCDEF;
        let ik = InterleavedKey::new(key, timestamp);
        
        assert_eq!(ik.original_key(), key);
        assert_eq!(ik.timestamp(), timestamp);
        
        // Should cycle through timestamp bytes
        let interleaved = ik.interleaved_bytes();
        assert_eq!(interleaved[0], b't');
        assert_eq!(interleaved[1], 0x01);  // timestamp[0]
        assert_eq!(interleaved[16], b'a');  // 9th key byte (index 8)
        assert_eq!(interleaved[17], 0x01);  // timestamp[0] (cycling)
    }

    #[test]
    fn test_ordering() {
        let key1 = InterleavedKey::new(b"aaa", 100);
        let key2 = InterleavedKey::new(b"aaa", 200);
        let key3 = InterleavedKey::new(b"bbb", 100);
        
        // Same key, different timestamps
        assert!(key1 < key2);
        
        // Different keys, same timestamp  
        assert!(key1 < key3);
        
        // Ordering should be primarily by interleaved bytes
        assert!(key1 < key2);
        assert!(key2 < key3);
    }

    #[test]
    fn test_prefix_range() {
        let prefix = b"user:";
        let timestamp = 12345;
        let (start, end) = InterleavedKey::prefix_range(prefix, timestamp);
        
        assert_eq!(start.original_key(), prefix);
        assert_eq!(start.timestamp(), timestamp);
        
        // End should have incremented prefix
        assert_eq!(end.original_key(), b"user;");
        assert_eq!(end.timestamp(), timestamp);
    }

    #[test]
    fn test_roundtrip_reconstruction() {
        let original = InterleavedKey::new(b"test_key", 98765);
        let reconstructed = InterleavedKey::from_interleaved_bytes(&original.interleaved)
            .expect("Should reconstruct successfully");
        
        assert_eq!(original.original_key(), reconstructed.original_key());
        assert_eq!(original.timestamp(), reconstructed.timestamp());
        assert_eq!(original.interleaved, reconstructed.interleaved);
    }

    #[test]
    fn test_temporal_clustering() {
        // Keys with similar timestamps should be close in ordering
        let key1 = InterleavedKey::new(b"user:1", 1000);
        let key2 = InterleavedKey::new(b"user:2", 1001);
        let key3 = InterleavedKey::new(b"user:3", 1002);
        let key4 = InterleavedKey::new(b"user:4", 2000);
        
        // Should maintain relative ordering
        assert!(key1 < key2);
        assert!(key2 < key3);
        assert!(key3 < key4);
        
        // Distance between key1 and key2 should be smaller than key3 and key4
        // (This is a property of the interleaving that promotes temporal locality)
    }

    #[test]
    fn test_prefix_operations() {
        let key = InterleavedKey::new(b"long_key_for_testing", 12345);
        let prefix = key.prefix(10);
        
        assert_eq!(prefix.len(), 10);
        assert_eq!(prefix, key.interleaved_bytes()[..10]);
    }
}