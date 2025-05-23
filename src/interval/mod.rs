//! Interval module for IMEC-v2
//!
//! This module provides the interval table data structure that maps Morton-T
//! code ranges to storage extents using cache-optimal Eytzinger layout.

pub mod table;

// Re-export key types and functions
pub use table::{
    IntervalTable,
    IntervalNode, 
    IntervalError,
    IntervalTableStats,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::morton_t_encode;

    #[test]
    fn test_interval_integration() {
        let mut table = IntervalTable::new();
        
        // Test with real Morton-T codes
        let code1 = morton_t_encode("user:alice", 1000);
        let code2 = morton_t_encode("user:bob", 1000);
        let code3 = morton_t_encode("order:123", 1000);
        
        // Create intervals that would contain these codes
        table.insert(code1.value() - 1000, code1.value() + 1000, 1).unwrap();
        table.insert(code2.value() - 1000, code2.value() + 1000, 2).unwrap();
        table.insert(code3.value() - 1000, code3.value() + 1000, 3).unwrap();
        
        // Test lookups
        assert_eq!(table.find_extent(code1.value()), Some(1));
        assert_eq!(table.find_extent(code2.value()), Some(2));
        assert_eq!(table.find_extent(code3.value()), Some(3));
    }
    
    #[test]
    fn test_range_scan_integration() {
        let mut table = IntervalTable::new();
        
        // Test that user keys fall in the interval
        let alice_code = morton_t_encode("user:alice", 1000);
        let bob_code = morton_t_encode("user:bob", 1000);
        let charlie_code = morton_t_encode("user:charlie", 1000);
        
        // Create a range that encompasses all user keys
        let min_code = alice_code.value().min(bob_code.value()).min(charlie_code.value());
        let max_code = alice_code.value().max(bob_code.value()).max(charlie_code.value());
        
        // Create interval with some padding
        let range_start = min_code.saturating_sub(1000);
        let range_end = max_code.saturating_add(1000);
        
        table.insert(range_start, range_end, 1).unwrap();
        
        // Test that all user keys are found in the interval
        assert_eq!(table.find_extent(alice_code.value()), Some(1));
        assert_eq!(table.find_extent(bob_code.value()), Some(1));
        assert_eq!(table.find_extent(charlie_code.value()), Some(1));
        
        // Test overlapping search for range scans
        let overlapping = table.find_overlapping(range_start, range_end);
        assert_eq!(overlapping.len(), 1);
        assert_eq!(overlapping[0].extent_id, 1);
    }
}