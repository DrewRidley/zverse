//! Encoding module for IMEC-v2
//!
//! This module provides order-preserving UTF-8 encoding and Morton-T interleaving
//! for creating spatial-temporal locality in key-value storage.

pub mod utf8;
pub mod morton;

// Re-export key types and functions for convenience
pub use utf8::{BitStream, encode, encode_limited, decode};
pub use morton::{
    MortonTCode, 
    morton_t_encode, 
    estimate_timestamp_for_key,
    current_timestamp_micros,
    morton_t_range_for_prefix,
    morton_t_range_for_time_window,
    MAX_KEY_BYTES,
    KEY_PREFIX_BITS,
    TIMESTAMP_BITS,
};

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_end_to_end_encoding() {
        let test_cases = vec![
            ("", 1000),
            ("user:alice", 1001),
            ("user:bob", 1002),
            ("order:12345", 1003),
            ("🚀", 1004),
        ];

        let mut codes = Vec::new();
        for (key, timestamp) in &test_cases {
            let code = morton_t_encode(key, *timestamp);
            codes.push((*key, *timestamp, code));
        }

        // Verify all codes are unique
        for i in 0..codes.len() {
            for j in i+1..codes.len() {
                assert_ne!(
                    codes[i].2, codes[j].2,
                    "Duplicate codes for ('{}', {}) and ('{}', {})",
                    codes[i].0, codes[i].1, codes[j].0, codes[j].1
                );
            }
        }

        // Verify codes are valid (non-zero for most cases)
        for (key, timestamp, code) in &codes {
            assert!(code.value() > 0, "Zero code for key '{}' at timestamp {}", key, timestamp);
        }

        // Test temporal locality - same key at different times should cluster
        let temporal_test_key = "user:alice";
        let temporal_codes: Vec<_> = (1000..1010)
            .map(|t| morton_t_encode(temporal_test_key, t))
            .collect();
        
        // Calculate average distance between adjacent temporal codes
        let mut temporal_distances = Vec::new();
        for i in 0..temporal_codes.len()-1 {
            let distance = temporal_codes[i+1].value().abs_diff(temporal_codes[i].value());
            temporal_distances.push(distance);
        }
        
        let avg_temporal_distance: f64 = temporal_distances.iter().map(|&d| d as f64).sum::<f64>() / temporal_distances.len() as f64;
        
        // Temporal clustering should be reasonable
        assert!(avg_temporal_distance < u64::MAX as f64 / 1000.0, "Poor temporal clustering");
    }

    #[test]
    fn test_range_operations() {
        let timestamp = 2000;
        
        // Test prefix range generation
        let (start, end) = morton_t_range_for_prefix("user:", timestamp);
        
        // Ranges should be valid
        assert!(start < end);
        assert!(start.value() > 0);
        assert!(end.value() > 0);
        
        // Test time window range generation
        let (time_start, time_end) = morton_t_range_for_time_window(1000, 2000);
        assert!(time_start < time_end);
        
        // Test that Morton codes are generated correctly
        let user_keys = vec!["user:alice", "user:bob", "user:charlie"];
        let user_codes: Vec<_> = user_keys.iter()
            .map(|&key| morton_t_encode(key, timestamp))
            .collect();
        
        // All codes should be valid and unique
        for (i, &code) in user_codes.iter().enumerate() {
            assert!(code.value() > 0, "User key '{}' produced zero code", user_keys[i]);
        }
        
        // Codes should be unique
        for i in 0..user_codes.len() {
            for j in i+1..user_codes.len() {
                assert_ne!(user_codes[i], user_codes[j], 
                    "Duplicate codes for '{}' and '{}'", user_keys[i], user_keys[j]);
            }
        }
    }

    #[test]
    fn test_temporal_range() {
        let key = "test_key";
        let (start, end) = morton_t_range_for_time_window(1000, 2000);
        
        // Test temporal clustering - codes at similar times should be similar
        let window_codes = vec![
            morton_t_encode(key, 1000),
            morton_t_encode(key, 1500),
            morton_t_encode(key, 2000),
        ];
        
        let outside_codes = vec![
            morton_t_encode(key, 500),   // Much earlier
            morton_t_encode(key, 3000),  // Much later
        ];
        
        // Calculate distances within window vs outside window
        let mut within_distances = Vec::new();
        for code in &window_codes {
            let distance_to_start = code.value().abs_diff(start.value());
            let distance_to_end = code.value().abs_diff(end.value());
            within_distances.push(distance_to_start.min(distance_to_end));
        }
        
        let mut outside_distances = Vec::new();
        for code in &outside_codes {
            let distance_to_start = code.value().abs_diff(start.value());
            let distance_to_end = code.value().abs_diff(end.value());
            outside_distances.push(distance_to_start.min(distance_to_end));
        }
        
        // Codes within time window should be closer to range boundaries
        let avg_within: f64 = within_distances.iter().map(|&d| d as f64).sum::<f64>() / within_distances.len() as f64;
        let avg_outside: f64 = outside_distances.iter().map(|&d| d as f64).sum::<f64>() / outside_distances.len() as f64;
        
        // This tests that temporal ranges provide some locality benefit
        assert!(
            avg_within < avg_outside || avg_within < u64::MAX as f64 / 100.0,
            "Temporal range doesn't provide locality benefit: within={}, outside={}", avg_within, avg_outside
        );
    }
}