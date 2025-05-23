//! Simple locality-aware caching for ZVerse
//!
//! This module provides boundary-aware caching and intelligent prefetching
//! based on spatial proximity in the Morton code space.

use crate::interleaved_key::InterleavedKey;
use std::collections::{HashMap, VecDeque};
use std::ops::Range;
use std::time::{Duration, Instant};

/// Hot region tracking for memory management
#[derive(Debug, Clone)]
pub struct HotRegion {
    pub prefix: Vec<u8>,
    pub last_access: Instant,
    pub access_count: u32,
    pub size_estimate: usize,
}

impl HotRegion {
    fn new(prefix: Vec<u8>, size_estimate: usize) -> Self {
        Self {
            prefix,
            last_access: Instant::now(),
            access_count: 1,
            size_estimate,
        }
    }

    fn touch(&mut self) {
        self.last_access = Instant::now();
        self.access_count += 1;
    }

    fn is_hot(&self) -> bool {
        self.access_count > 3 && self.last_access.elapsed() < Duration::from_secs(60)
    }

    fn heat_score(&self) -> f64 {
        let age_factor = 1.0 / (1.0 + self.last_access.elapsed().as_secs_f64() / 60.0);
        (self.access_count as f64) * age_factor
    }
}

/// Simple locality manager for boundary-aware caching
pub struct LocalityManager {
    /// Recent accesses for proximity detection
    recent_accesses: VecDeque<(Vec<u8>, Instant)>,
    /// Hot regions by prefix
    hot_regions: HashMap<Vec<u8>, HotRegion>,
    /// Memory budget for caching
    memory_budget: usize,
    /// Current estimated memory usage
    current_usage: usize,
    /// Prefix length for region tracking
    prefix_len: usize,
}

impl LocalityManager {
    pub fn new() -> Self {
        Self::with_budget(64 * 1024 * 1024) // 64MB default
    }

    pub fn with_budget(memory_budget: usize) -> Self {
        Self {
            recent_accesses: VecDeque::new(),
            hot_regions: HashMap::new(),
            memory_budget,
            current_usage: 0,
            prefix_len: 8, // 8-byte prefixes for region tracking
        }
    }

    /// Record a key access
    pub fn record_access(&mut self, key: &InterleavedKey) {
        let prefix = self.get_prefix(key);
        
        // Update recent accesses
        self.recent_accesses.push_back((prefix.clone(), Instant::now()));
        if self.recent_accesses.len() > 1000 {
            self.recent_accesses.pop_front();
        }

        // Update hot regions
        match self.hot_regions.get_mut(&prefix) {
            Some(region) => region.touch(),
            None => {
                let region = HotRegion::new(prefix.clone(), 4096); // Estimate 4KB per region
                self.current_usage += region.size_estimate;
                self.hot_regions.insert(prefix, region);
            }
        }

        // Cleanup old regions if over budget
        if self.current_usage > self.memory_budget {
            self.evict_cold_regions();
        }
    }

    /// Get key prefix for region tracking
    fn get_prefix(&self, key: &InterleavedKey) -> Vec<u8> {
        let bytes = key.interleaved_bytes();
        if bytes.len() <= self.prefix_len {
            bytes.to_vec()
        } else {
            bytes[..self.prefix_len].to_vec()
        }
    }

    /// Suggest ranges to prefetch based on boundary proximity
    pub fn suggest_prefetch(&self, current_key: &InterleavedKey) -> Vec<Range<Vec<u8>>> {
        let mut suggestions = Vec::new();
        let current_prefix = self.get_prefix(current_key);

        // Suggest nearby boundary regions
        suggestions.extend(self.get_boundary_ranges(&current_prefix));

        // Suggest hot regions if they're spatially close
        for region in self.hot_regions.values() {
            if region.is_hot() && self.is_spatially_close(&current_prefix, &region.prefix) {
                let start = region.prefix.clone();
                let mut end = start.clone();
                self.increment_prefix(&mut end);
                suggestions.push(start..end);
            }
        }

        suggestions
    }

    /// Get boundary ranges around a prefix
    fn get_boundary_ranges(&self, prefix: &[u8]) -> Vec<Range<Vec<u8>>> {
        let mut ranges = Vec::new();

        // Previous boundary
        if let Some(prev) = self.get_previous_prefix(prefix) {
            let start = prev.clone();
            let end = prefix.to_vec();
            ranges.push(start..end);
        }

        // Next boundary  
        let mut next = prefix.to_vec();
        self.increment_prefix(&mut next);
        let start = prefix.to_vec();
        ranges.push(start..next);

        ranges
    }

    /// Get the previous prefix in lexicographic order
    fn get_previous_prefix(&self, prefix: &[u8]) -> Option<Vec<u8>> {
        let mut prev = prefix.to_vec();
        for byte in prev.iter_mut().rev() {
            if *byte > 0 {
                *byte -= 1;
                return Some(prev);
            }
            *byte = 255;
        }
        None
    }

    /// Increment a prefix to get the next boundary
    fn increment_prefix(&self, prefix: &mut Vec<u8>) {
        for byte in prefix.iter_mut().rev() {
            if *byte < 255 {
                *byte += 1;
                return;
            }
            *byte = 0;
        }
        // All bytes were 255, add a new byte
        prefix.push(1);
    }

    /// Check if two prefixes are spatially close
    fn is_spatially_close(&self, a: &[u8], b: &[u8]) -> bool {
        let distance = self.calculate_distance(a, b);
        distance < 256 * 4 // Within 4 "steps" in the prefix space
    }

    /// Calculate distance between two prefixes
    fn calculate_distance(&self, a: &[u8], b: &[u8]) -> u64 {
        let min_len = std::cmp::min(a.len(), b.len());
        let mut distance = 0u64;

        for i in 0..min_len {
            distance += (a[i] as i16 - b[i] as i16).abs() as u64;
        }

        // Add penalty for length difference
        distance += (a.len() as i64 - b.len() as i64).abs() as u64 * 256;
        distance
    }

    /// Evict cold regions to stay within memory budget
    fn evict_cold_regions(&mut self) {
        let mut regions: Vec<_> = self.hot_regions.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        regions.sort_by(|a, b| a.1.heat_score().partial_cmp(&b.1.heat_score()).unwrap());

        while self.current_usage > self.memory_budget && !regions.is_empty() {
            let (prefix, region) = regions.remove(0);
            self.current_usage -= region.size_estimate;
            self.hot_regions.remove(&prefix);
        }
    }

    /// Get current hot regions for memory advice
    pub fn get_hot_prefixes(&self) -> Vec<Vec<u8>> {
        self.hot_regions
            .values()
            .filter(|region| region.is_hot())
            .map(|region| region.prefix.clone())
            .collect()
    }

    /// Check if a prefix is currently hot
    pub fn is_hot_prefix(&self, prefix: &[u8]) -> bool {
        let region_prefix = if prefix.len() <= self.prefix_len {
            prefix.to_vec()
        } else {
            prefix[..self.prefix_len].to_vec()
        };

        self.hot_regions
            .get(&region_prefix)
            .map(|region| region.is_hot())
            .unwrap_or(false)
    }

    /// Cleanup old access history
    pub fn cleanup(&mut self) {
        let cutoff = Instant::now() - Duration::from_secs(300);
        
        // Clean recent accesses
        while let Some((_, time)) = self.recent_accesses.front() {
            if *time < cutoff {
                self.recent_accesses.pop_front();
            } else {
                break;
            }
        }

        // Remove very old regions
        let old_cutoff = Instant::now() - Duration::from_secs(600);
        let old_prefixes: Vec<_> = self.hot_regions
            .iter()
            .filter(|(_, region)| region.last_access < old_cutoff)
            .map(|(prefix, _)| prefix.clone())
            .collect();

        for prefix in old_prefixes {
            if let Some(region) = self.hot_regions.remove(&prefix) {
                self.current_usage -= region.size_estimate;
            }
        }
    }

    /// Get current memory usage stats
    pub fn memory_stats(&self) -> (usize, usize, usize) {
        (self.current_usage, self.memory_budget, self.hot_regions.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_locality_manager() {
        let mut manager = LocalityManager::new();
        let key = InterleavedKey::new(b"test", 12345);
        
        // Record multiple accesses to make the region hot (needs > 3 accesses)
        for _ in 0..5 {
            manager.record_access(&key);
        }
        
        let hot_prefixes = manager.get_hot_prefixes();
        assert!(!hot_prefixes.is_empty());
    }

    #[test]
    fn test_boundary_ranges() {
        let manager = LocalityManager::new();
        let prefix = b"test".to_vec();
        let ranges = manager.get_boundary_ranges(&prefix);
        
        assert!(!ranges.is_empty());
    }

    #[test]
    fn test_spatial_distance() {
        let manager = LocalityManager::new();
        let a = b"test";
        let b = b"test";
        let c = b"very_different_key";
        
        assert!(manager.is_spatially_close(a, b));
        assert!(!manager.is_spatially_close(a, c));
    }

    #[test]
    fn test_memory_budget() {
        let mut manager = LocalityManager::with_budget(1024);
        
        // Add enough regions to exceed budget
        for i in 0..100 {
            let key = InterleavedKey::new(&format!("key{}", i).into_bytes(), 12345 + i);
            manager.record_access(&key);
        }
        
        let (usage, budget, _) = manager.memory_stats();
        assert!(usage <= budget);
    }
}