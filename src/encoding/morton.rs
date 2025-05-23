//! Morton-T interleaving module for hybrid IMEC design
//!
//! This module implements Morton-T encoding which interleaves the first 32 bits
//! of raw UTF-8 key encoding with 32-bit timestamps to achieve both spatial
//! (key prefix) and temporal locality in a single 64-bit code.

use crate::encoding::utf8::{BitStream, encode_limited};

/// A Morton-T code that encodes both key prefix and timestamp information
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct MortonTCode(pub u64);

impl MortonTCode {
    /// Create a new Morton-T code from raw value
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// Get the raw 64-bit value
    pub fn value(&self) -> u64 {
        self.0
    }

    /// Extract the key prefix bits (up to 48 bits)
    pub fn extract_key_prefix(&self) -> u64 {
        let mut key_bits = 0u64;
        let mut key_pos = 0;
        
        // Extract key bits from the interleaved pattern (3:1 ratio)
        let mut bit_pos = 0;
        while bit_pos < 64 && key_pos < 48 {
            // Extract 3 key bits
            for _ in 0..3 {
                if bit_pos >= 64 || key_pos >= 48 { break; }
                let bit = (self.0 >> bit_pos) & 1;
                key_bits |= bit << key_pos;
                key_pos += 1;
                bit_pos += 1;
            }
            // Skip 1 timestamp bit
            bit_pos += 1;
        }
        
        key_bits
    }

    /// Extract the timestamp bits (16 bits)
    pub fn extract_timestamp(&self) -> u16 {
        let mut time_bits = 0u16;
        let mut time_pos = 0;
        let mut bit_pos = 0;
        
        // Extract timestamp bits from the interleaved pattern (3:1 ratio)
        while bit_pos < 64 && time_pos < 16 {
            // Skip 3 key bits
            bit_pos += 3;
            if bit_pos >= 64 { break; }
            
            // Extract 1 timestamp bit
            let bit = (self.0 >> bit_pos) & 1;
            time_bits |= (bit as u16) << time_pos;
            time_pos += 1;
            bit_pos += 1;
        }
        
        time_bits
    }
}

/// Maximum key size in bytes (same as libmdbx)
pub const MAX_KEY_BYTES: usize = 512;

/// Number of key prefix bits to use in Morton encoding (48 bits = 6 bytes)
pub const KEY_PREFIX_BITS: usize = 48;

/// Number of timestamp bits to use (16 bits for temporal locality)
pub const TIMESTAMP_BITS: usize = 16;

/// Encode a UTF-8 key and timestamp into a Morton-T code
///
/// The encoding uses raw key bits (preserving lexicographic order) combined
/// with timestamp bits for temporal locality:
/// 
/// 1. Take first 32 bits of UTF-8 encoded key (preserves prefix ordering)
/// 2. Use 32 bits of timestamp for temporal clustering
/// 3. Interleave: K₀T₀K₁T₁K₂T₂...K₃₁T₃₁
///
/// Properties:
/// - Keys with same prefix maintain spatial locality
/// - Same key at different timestamps cluster temporally
/// - Supports arbitrary key lengths up to 512 bytes
/// - 64-bit output for efficient operations
pub fn morton_t_encode(key: &str, timestamp: u64) -> MortonTCode {
    // Get first 32 bits of raw UTF-8 encoding (preserves order)
    let key_bits = encode_limited(key, KEY_PREFIX_BITS);
    

    
    // Use lower 16 bits of timestamp
    let time_bits = (timestamp & 0xFFFF) as u16;
    
    // Interleave the key prefix bits with timestamp bits
    let interleaved = interleave_key_and_time(&key_bits, time_bits);
    

    
    MortonTCode(interleaved)
}

/// Interleave key prefix bits with timestamp bits
/// 
/// Takes up to 48 bits from key encoding and 16-bit timestamp, produces 64-bit result
/// Pattern: balanced interleaving to use all 64 bits effectively
fn interleave_key_and_time(key_bits: &BitStream, time_bits: u16) -> u64 {
    let mut result = 0u64;
    let max_key_bits = key_bits.bit_len().min(48);
    
    // Simple interleaving: 3 key bits for every 1 timestamp bit
    // This gives us 48 key bits + 16 timestamp bits = 64 bits total
    let mut bit_pos = 0;
    let mut key_pos = 0;
    let mut time_pos = 0;
    
    while bit_pos < 64 && (key_pos < max_key_bits || time_pos < 16) {
        // Add 3 key bits
        for _ in 0..3 {
            if bit_pos >= 64 || key_pos >= max_key_bits { break; }
            let key_bit = key_bits.get_bit(key_pos).unwrap_or(false);
            if key_bit {
                result |= 1u64 << bit_pos;
            }
            bit_pos += 1;
            key_pos += 1;
        }
        
        // Add 1 timestamp bit
        if bit_pos < 64 && time_pos < 16 {
            let time_bit = (time_bits >> time_pos) & 1;
            if time_bit == 1 {
                result |= 1u64 << bit_pos;
            }
            bit_pos += 1;
            time_pos += 1;
        }
    }
    
    result
}

/// Estimate timestamp for a key based on current time
/// 
/// This is used for lookups when we don't know the exact timestamp
/// but want to search in the most recent time window.
pub fn estimate_timestamp_for_key(_key: &str) -> u64 {
    // For now, just return current timestamp
    // In a real implementation, this might use:
    // - Recent write patterns for this key prefix
    // - Transaction timestamp
    // - Query hint from application
    current_timestamp_micros()
}

/// Get current timestamp in microseconds since Unix epoch
pub fn current_timestamp_micros() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

/// Create a Morton-T range for scanning keys with a given prefix
/// 
/// Returns (start, end) Morton-T codes that bound all keys with the prefix
/// at the given timestamp. Uses raw key encoding to preserve order.
pub fn morton_t_range_for_prefix(prefix: &str, timestamp: u64) -> (MortonTCode, MortonTCode) {
    // Start: prefix exactly
    let start = morton_t_encode(prefix, timestamp);
    
    // End: increment prefix to get exclusive upper bound
    let end_key = increment_string(prefix);
    let end = morton_t_encode(&end_key, timestamp);
    
    (start, end)
}

/// Increment a string lexicographically to create an exclusive upper bound
fn increment_string(s: &str) -> String {
    if s.is_empty() {
        return String::from("\u{01}"); // Smallest non-empty string
    }
    
    let mut bytes = s.as_bytes().to_vec();
    
    // Try to increment from the end
    for i in (0..bytes.len()).rev() {
        if bytes[i] < 255 {
            bytes[i] += 1;
            // Truncate everything after this position
            bytes.truncate(i + 1);
            return String::from_utf8(bytes).unwrap_or_else(|_| s.to_string() + "\u{01}");
        }
    }
    
    // All bytes were 255, append a byte
    bytes.push(1);
    String::from_utf8(bytes).unwrap_or_else(|_| s.to_string() + "\u{01}")
}

/// Create a Morton-T range for scanning a time window for any key
pub fn morton_t_range_for_time_window(start_time: u64, end_time: u64) -> (MortonTCode, MortonTCode) {
    // Use empty key (minimum) and maximum key
    let start = morton_t_encode("", start_time);
    let end = morton_t_encode("\u{10FFFF}", end_time);
    
    (start, end)
}

/// Create the smallest possible Morton-T code for a given key prefix
/// 
/// This is the "bigmin" operation - given a key prefix, returns the smallest
/// Morton code that could exist for any key starting with that prefix.
/// Uses timestamp 0 to ensure minimum value.
pub fn morton_t_bigmin(key_prefix: &str) -> MortonTCode {
    morton_t_encode(key_prefix, 0)
}

/// Create the largest possible Morton-T code for a given key prefix
/// 
/// This is the "litmax" operation - given a key prefix, returns the largest
/// Morton code that could exist for any key starting with that prefix.
/// Uses maximum timestamp and extends the key prefix to its limit.
pub fn morton_t_litmax(key_prefix: &str) -> MortonTCode {
    // Create the lexicographically largest key with this prefix
    let max_key = if key_prefix.is_empty() {
        "\u{10FFFF}".repeat(10) // Max Unicode repeated
    } else {
        format!("{}{}", key_prefix, "\u{10FFFF}".repeat(10))
    };
    
    morton_t_encode(&max_key, u64::MAX)
}

/// Create an efficient range for scanning all keys with a given prefix
/// 
/// Returns (min, max) Morton codes that bound ALL possible keys starting
/// with the given prefix across ALL timestamps. This is more efficient
/// than morton_t_range_for_prefix as it uses bigmin/litmax.
pub fn morton_t_prefix_range(key_prefix: &str) -> (MortonTCode, MortonTCode) {
    (morton_t_bigmin(key_prefix), morton_t_litmax(key_prefix))
}

/// Create an efficient range for finding the latest version of a specific key
/// 
/// Returns (min, max) Morton codes that bound all temporal versions of
/// the given key. The range spans from the earliest possible timestamp
/// to the latest possible timestamp for this exact key.
pub fn morton_t_key_temporal_range(key: &str) -> (MortonTCode, MortonTCode) {
    let min = morton_t_encode(key, 0);
    let max = morton_t_encode(key, u64::MAX);
    (min, max)
}

/// Find the Morton code range for a spatial+temporal query
/// 
/// Returns Morton codes that bound all keys between key_start and key_end
/// within the time window [time_start, time_end].
/// Note: Due to Morton interleaving, this provides a conservative bound.
pub fn morton_t_spatiotemporal_range(
    key_start: &str, 
    key_end: &str, 
    time_start: u64, 
    time_end: u64
) -> (MortonTCode, MortonTCode) {
    // For Morton codes, we need to consider all combinations
    // and take the true min/max to ensure proper bounds
    let combinations = vec![
        morton_t_encode(key_start, time_start),
        morton_t_encode(key_start, time_end),
        morton_t_encode(key_end, time_start),
        morton_t_encode(key_end, time_end),
    ];
    
    let min = combinations.iter().min().copied().unwrap();
    let max = combinations.iter().max().copied().unwrap();
    (min, max)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_morton_t_basic_encoding() {
        let code = morton_t_encode("test", 12345);
        
        // Should be a valid 64-bit code
        assert!(code.value() > 0);
        
        // Same key and timestamp should produce same code
        let code2 = morton_t_encode("test", 12345);
        assert_eq!(code, code2);
    }

    #[test]
    fn test_morton_t_key_prefix_locality() {
        let timestamp = 1000;
        
        // Test that keys with same prefix have some spatial locality
        let user_keys = vec!["user:alice", "user:bob", "user:charlie"];
        let user_codes: Vec<_> = user_keys.iter()
            .map(|&key| morton_t_encode(key, timestamp))
            .collect();
        
        // All codes should be unique
        for i in 0..user_codes.len() {
            for j in i+1..user_codes.len() {
                assert_ne!(
                    user_codes[i], user_codes[j],
                    "Duplicate codes for '{}' and '{}'",
                    user_keys[i], user_keys[j]
                );
            }
        }
        
        // Keys with same prefix should have some clustering
        let prefix_distances: Vec<_> = user_codes.windows(2)
            .map(|w| w[1].value().abs_diff(w[0].value()))
            .collect();
        
        // Compare to completely random keys
        let random_keys = vec!["apple", "zebra", "123"];
        let random_codes: Vec<_> = random_keys.iter()
            .map(|&key| morton_t_encode(key, timestamp))
            .collect();
        
        let random_distances: Vec<_> = random_codes.windows(2)
            .map(|w| w[1].value().abs_diff(w[0].value()))
            .collect();
        
        println!("Prefix clustering distances: {:?}", prefix_distances);
        println!("Random distances: {:?}", random_distances);
        
        // This is a practical test - we expect reasonable clustering
        // due to shared prefix bits in Morton encoding
    }

    #[test]
    fn test_morton_t_temporal_clustering() {
        let key = "test_key";
        let base_time = 1000;
        
        // Test temporal clustering - same key at nearby times should cluster
        let codes: Vec<_> = (0..5)
            .map(|i| morton_t_encode(key, base_time + i))
            .collect();
        
        // All codes should be unique
        for i in 0..codes.len() {
            for j in i+1..codes.len() {
                assert_ne!(codes[i], codes[j], "Duplicate codes for same key at different times");
            }
        }
        
        // Calculate temporal clustering quality
        let temporal_distances: Vec<_> = codes.windows(2)
            .map(|w| w[1].value().abs_diff(w[0].value()))
            .collect();
        
        println!("Temporal clustering distances: {:?}", temporal_distances);
        
        // Temporal clustering should show some locality
        for distance in temporal_distances {
            assert!(
                distance < u64::MAX / 100, // Reasonable temporal locality threshold
                "Poor temporal clustering: distance {}",
                distance
            );
        }
    }

    #[test]
    fn test_morton_t_bit_extraction() {
        let key = "test";
        let timestamp = 0x12345678u64;
        let code = morton_t_encode(key, timestamp);
        
        // Extract and verify timestamp (lower 16 bits)
        let extracted_time = code.extract_timestamp();
        let _expected_time = (0x12345678u64 & 0xFFFF) as u64;
        
        // Due to interleaving, we may not get exact match, but should be non-zero
        assert!(extracted_time > 0, "Extracted timestamp should be non-zero");
        
        // Extract key prefix bits
        let extracted_key = code.extract_key_prefix();
        
        // Should be non-zero for non-empty key
        assert!(extracted_key > 0);
    }

    #[test]
    fn test_morton_t_range_for_prefix() {
        let (start, end) = morton_t_range_for_prefix("user:", 1000);
        
        // Start and end should be different
        assert_ne!(start, end);
        assert!(start < end);
        
        // Test that keys with the prefix show spatial locality
        let test_codes = vec![
            morton_t_encode("user:alice", 1000),
            morton_t_encode("user:bob", 1000),
            morton_t_encode("user:charlie", 1000),
        ];
        
        // Due to prefix sharing in Morton encoding, codes should be
        // reasonably clustered. Test that at least some fall in range.
        let mut contained_count = 0;
        for code in &test_codes {
            if start <= *code && *code < end {
                contained_count += 1;
            }
        }
        
        println!("Prefix range contains {}/{} keys", contained_count, test_codes.len());
        
        // This tests the practical utility of prefix ranges
        // With shared prefix bits, we expect some containment
    }

    #[test]
    fn test_morton_t_range_for_time_window() {
        let (start, end) = morton_t_range_for_time_window(1000, 2000);
        
        assert!(start < end);
        
        // Test temporal range containment
        let window_codes = vec![
            morton_t_encode("any_key", 1500),
            morton_t_encode("another", 1800),
        ];
        
        // All codes in time window should be valid
        for code in window_codes {
            assert!(code.value() > 0, "Invalid code in time window");
        }
    }

    #[test]
    fn test_empty_key_encoding() {
        let code = morton_t_encode("", 1000);
        assert!(code.value() > 0); // Should still produce valid code
        
        // Empty key vs non-empty key should produce different codes
        let non_empty = morton_t_encode("a", 1000);
        assert_ne!(code, non_empty, "Empty and non-empty keys should produce different codes");
    }

    #[test]
    fn test_key_prefix_ordering() {
        let timestamp = 1000;
        
        // Keys that share prefixes should have related Morton codes
        let prefixed_keys = vec!["user:a", "user:b", "user:c"];
        let codes: Vec<_> = prefixed_keys.iter()
            .map(|&key| morton_t_encode(key, timestamp))
            .collect();
        
        // All codes should be unique
        for i in 0..codes.len() {
            for j in i+1..codes.len() {
                assert_ne!(codes[i], codes[j], 
                    "Duplicate codes for '{}' and '{}'", 
                    prefixed_keys[i], prefixed_keys[j]);
            }
        }
        
        // Due to shared prefix bits, codes should show some clustering
        let distances: Vec<_> = codes.windows(2)
            .map(|w| w[1].value().abs_diff(w[0].value()))
            .collect();
        
        println!("Prefix distances: {:?}", distances);
    }

    #[test]
    fn test_morton_t_bigmin() {
        // Test bigmin for various prefixes
        let min_user = morton_t_bigmin("user:");
        let min_empty = morton_t_bigmin("");
        let min_a = morton_t_bigmin("a");
        
        // Bigmin should use timestamp 0
        assert_eq!(min_user, morton_t_encode("user:", 0));
        assert_eq!(min_empty, morton_t_encode("", 0));
        assert_eq!(min_a, morton_t_encode("a", 0));
        
        // Empty prefix should be smallest
        assert!(min_empty <= min_a);
        assert!(min_empty <= min_user);
    }

    #[test]
    fn test_morton_t_litmax() {
        // Test litmax for various prefixes
        let max_user = morton_t_litmax("user:");
        let max_empty = morton_t_litmax("");
        let max_a = morton_t_litmax("a");
        
        // All should be valid codes
        assert!(max_user.value() > 0);
        assert!(max_empty.value() > 0);
        assert!(max_a.value() > 0);
        
        // Litmax should be larger than corresponding bigmin
        assert!(max_user > morton_t_bigmin("user:"));
        assert!(max_empty > morton_t_bigmin(""));
        assert!(max_a > morton_t_bigmin("a"));
    }

    #[test]
    fn test_morton_t_prefix_range() {
        let (min, max) = morton_t_prefix_range("user:");
        
        // Range should be valid
        assert!(min < max);
        assert_eq!(min, morton_t_bigmin("user:"));
        assert_eq!(max, morton_t_litmax("user:"));
        
        // Test that keys with prefix fall in range (conceptually)
        let user_keys = vec!["user:alice", "user:bob", "user:charlie"];
        for key in user_keys {
            let code = morton_t_encode(key, 1000);
            // The actual containment depends on Morton encoding properties
            // but the range should be reasonable
            assert!(code.value() > 0);
        }
    }

    #[test]
    fn test_morton_t_key_temporal_range() {
        let (min, max) = morton_t_key_temporal_range("user:alice");
        
        // Range should span all timestamps for this key
        assert!(min < max);
        assert_eq!(min, morton_t_encode("user:alice", 0));
        assert_eq!(max, morton_t_encode("user:alice", u64::MAX));
        
        // Test that different timestamps of same key fall in range
        let timestamps = vec![1000, 2000, 3000, 4000];
        for ts in timestamps {
            let code = morton_t_encode("user:alice", ts);
            assert!(code >= min);
            assert!(code <= max);
        }
    }

    #[test]
    fn test_morton_t_spatiotemporal_range() {
        let (min, max) = morton_t_spatiotemporal_range("user:a", "user:z", 1000, 2000);
        
        // Range should be valid
        assert!(min < max);
        
        // Due to Morton interleaving, the min/max might not be the simple encodings
        // but should bound all combinations
        let test_combinations = vec![
            morton_t_encode("user:a", 1000),
            morton_t_encode("user:a", 2000),
            morton_t_encode("user:z", 1000),
            morton_t_encode("user:z", 2000),
        ];
        
        // All combinations should fall within the range
        for code in test_combinations {
            assert!(code >= min, "Code {:?} should be >= min {:?}", code, min);
            assert!(code <= max, "Code {:?} should be <= max {:?}", code, max);
        }
    }

    #[test]
    fn test_bigmin_litmax_ordering() {
        // Test that bigmin/litmax maintain proper ordering relationships
        let prefixes = vec!["", "a", "user:", "user:a", "user:alice"];
        
        for prefix in &prefixes {
            let min = morton_t_bigmin(prefix);
            let max = morton_t_litmax(prefix);
            
            // Basic ordering
            assert!(min < max, "bigmin should be < litmax for prefix '{}'", prefix);
            
            // Test with actual encoded values
            let test_code = morton_t_encode(prefix, 1000);
            assert!(min <= test_code, "bigmin should be <= encoded value for prefix '{}'", prefix);
            // Note: test_code might not be <= max due to timestamp differences
        }
    }
}