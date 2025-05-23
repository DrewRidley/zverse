//! Morton-VART Hybrid Engine - Coordination-Free Insertions with Spatial-Temporal Locality
//!
//! This engine combines the best of both worlds:
//! - Morton-T encoding for perfect spatial-temporal locality
//! - VART (Versioned Adaptive Radix Tree) for coordination-free insertions
//! - Zero locks, pure atomic pointer operations
//! - Perfect cache locality for range scans within nodes

use crate::encoding::morton::{morton_t_encode, morton_t_bigmin, morton_t_litmax, current_timestamp_micros};
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};

use std::path::PathBuf;
use crossbeam_utils::CachePadded;
use memmap2::{MmapMut, MmapOptions};
use std::fs::OpenOptions;

/// Morton-encoded temporal record
#[derive(Debug, Clone)]
pub struct MortonRecord {
    pub morton_code: u64,
    pub key: String,
    pub value: Vec<u8>,
    pub timestamp: u64,
    pub version: u64,
}

impl MortonRecord {
    pub fn new(key: String, value: Vec<u8>, timestamp: u64) -> Self {
        let morton_code = morton_t_encode(&key, timestamp).value();
        Self {
            morton_code,
            key,
            value,
            timestamp,
            version: 0,
        }
    }
}

/// VART Node types optimized for Morton codes
#[derive(Debug)]
enum VartNode {
    /// Leaf node containing sorted Morton records (cache-friendly)
    Leaf {
        records: Vec<MortonRecord>,
        capacity: usize,
    },
    /// Internal node with Morton prefix compression
    Internal4 {
        prefix: u64,           // Common Morton prefix
        prefix_len: u8,        // Length of prefix in bits
        keys: [u8; 4],         // Next 8 bits after prefix
        children: [AtomicPtr<VartNode>; 4],
        count: AtomicU64,
    },
    Internal16 {
        prefix: u64,
        prefix_len: u8,
        keys: [u8; 16],
        children: [AtomicPtr<VartNode>; 16],
        count: AtomicU64,
    },
    Internal256 {
        prefix: u64,
        prefix_len: u8,
        children: [AtomicPtr<VartNode>; 256],
        count: AtomicU64,
    },
}

impl VartNode {
    fn new_leaf(capacity: usize) -> Self {
        VartNode::Leaf {
            records: Vec::with_capacity(capacity),
            capacity,
        }
    }

    fn new_internal4(prefix: u64, prefix_len: u8) -> Self {
        VartNode::Internal4 {
            prefix,
            prefix_len,
            keys: [0; 4],
            children: Default::default(),
            count: AtomicU64::new(0),
        }
    }

    /// Insert record using coordination-free atomic pointer updates
    fn insert_atomic(&self, record: MortonRecord) -> Result<*mut VartNode, MortonVartError> {
        match self {
            VartNode::Leaf { records, capacity } => {
                // Leaf insertion - check if we need to split
                if records.len() >= *capacity {
                    // Split leaf into internal node
                    self.split_leaf(record)
                } else {
                    // Find insertion point using binary search on Morton codes
                    let insert_pos = records.binary_search_by_key(&record.morton_code, |r| r.morton_code)
                        .unwrap_or_else(|e| e);
                    
                    // Create new leaf with inserted record
                    let mut new_records = records.clone();
                    new_records.insert(insert_pos, record);
                    
                    let new_leaf = Box::into_raw(Box::new(VartNode::Leaf {
                        records: new_records,
                        capacity: *capacity,
                    }));
                    
                    Ok(new_leaf)
                }
            },
            VartNode::Internal4 { prefix, prefix_len, keys, children, count } => {
                // Check prefix match
                let common_prefix = self.longest_common_prefix(*prefix, *prefix_len, record.morton_code);
                
                if common_prefix < *prefix_len {
                    // For simplicity, reject split for now
                    Err(MortonVartError::InsertionFailed)
                } else {
                    // Find child to descend into
                    let key_byte = self.extract_key_byte(record.morton_code, *prefix_len);
                    let child_index = self.find_child_index(key_byte, keys);
                    
                    if child_index < 4 && !children[child_index].load(Ordering::Acquire).is_null() {
                        // Recursively insert into child
                        let child_ptr = children[child_index].load(Ordering::Acquire);
                        let child = unsafe { &*child_ptr };
                        let new_child = child.insert_atomic(record)?;
                        
                        // Atomically update child pointer
                        children[child_index].store(new_child, Ordering::Release);
                        count.fetch_add(1, Ordering::Relaxed);
                        
                        Ok(self as *const VartNode as *mut VartNode)
                    } else {
                        // Create new child
                        let new_leaf = Box::into_raw(Box::new(VartNode::new_leaf(64)));
                        let updated_leaf = unsafe { &*new_leaf }.insert_atomic(record)?;
                        
                        // Add child to this internal node
                        self.add_child(key_byte, updated_leaf)
                    }
                }
            },
            VartNode::Internal16 { prefix: _, prefix_len, keys, children, count } => {
                // Simplified implementation - find empty slot
                let current_count = count.load(Ordering::Acquire);
                if current_count >= 16 {
                    return Err(MortonVartError::InsertionFailed);
                }
                
                let key_byte = self.extract_key_byte(record.morton_code, *prefix_len);
                let child_index = self.find_child_index(key_byte, keys);
                
                if child_index < 16 && !children[child_index].load(Ordering::Acquire).is_null() {
                    // Recursively insert into child
                    let child_ptr = children[child_index].load(Ordering::Acquire);
                    let child = unsafe { &*child_ptr };
                    child.insert_atomic(record)
                } else {
                    // Create new child leaf
                    let new_leaf = Box::into_raw(Box::new(VartNode::new_leaf(64)));
                    unsafe { &*new_leaf }.insert_atomic(record)
                }
            },
            VartNode::Internal256 { .. } => {
                // Simplified - not implemented for benchmark
                Err(MortonVartError::InsertionFailed)
            },
        }
    }

    /// Split leaf when it becomes too large
    fn split_leaf(&self, new_record: MortonRecord) -> Result<*mut VartNode, MortonVartError> {
        if let VartNode::Leaf { records, .. } = self {
            let mut all_records = records.clone();
            all_records.push(new_record);
            all_records.sort_by_key(|r| r.morton_code);
            
            // Find optimal split point using Morton code properties
            let split_point = self.find_optimal_split(&all_records);
            
            // Create new internal node
            let (left_records, right_records) = all_records.split_at(split_point);
            
            let left_leaf = Box::into_raw(Box::new(VartNode::Leaf {
                records: left_records.to_vec(),
                capacity: 64,
            }));
            
            let right_leaf = Box::into_raw(Box::new(VartNode::Leaf {
                records: right_records.to_vec(),
                capacity: 64,
            }));
            
            // Determine common prefix for internal node
            let left_morton = left_records[0].morton_code;
            let right_morton = right_records[0].morton_code;
            let (prefix, prefix_len) = self.compute_common_prefix(left_morton, right_morton);
            
            let mut internal = VartNode::new_internal4(prefix, prefix_len);
            if let VartNode::Internal4 { keys, children, .. } = &mut internal {
                let left_key = self.extract_key_byte(left_morton, prefix_len);
                let right_key = self.extract_key_byte(right_morton, prefix_len);
                
                keys[0] = left_key;
                keys[1] = right_key;
                children[0].store(left_leaf, Ordering::Release);
                children[1].store(right_leaf, Ordering::Release);
            }
            
            Ok(Box::into_raw(Box::new(internal)))
        } else {
            Err(MortonVartError::InvalidNodeType)
        }
    }

    /// Find optimal split point using Morton code clustering
    fn find_optimal_split(&self, records: &[MortonRecord]) -> usize {
        let len = records.len();
        if len <= 2 { return len / 2; }
        
        let mut best_split = len / 2;
        let mut min_entropy = f64::MAX;
        
        // Try different split points and find one that maximizes Morton locality
        for i in 1..len-1 {
            let left_span = records[i-1].morton_code ^ records[0].morton_code;
            let right_span = records[len-1].morton_code ^ records[i].morton_code;
            
            // Minimize the sum of spans (prefer clustered Morton codes)
            let entropy = (left_span + right_span) as f64;
            if entropy < min_entropy {
                min_entropy = entropy;
                best_split = i;
            }
        }
        
        best_split
    }

    /// Compute longest common prefix for Morton codes
    fn longest_common_prefix(&self, prefix: u64, prefix_len: u8, morton: u64) -> u8 {
        let mask = !((1u64 << (64 - prefix_len)) - 1);
        let common = !(prefix ^ morton);
        (common | !mask).leading_ones() as u8
    }

    /// Extract key byte at specific bit position
    fn extract_key_byte(&self, morton: u64, prefix_len: u8) -> u8 {
        let shift = 64 - prefix_len - 8;
        ((morton >> shift) & 0xFF) as u8
    }

    /// Find child index for key byte
    fn find_child_index(&self, key_byte: u8, keys: &[u8]) -> usize {
        keys.iter().position(|&k| k == key_byte).unwrap_or(keys.len())
    }

    /// Add child to internal node
    fn add_child(&self, key_byte: u8, child_ptr: *mut VartNode) -> Result<*mut VartNode, MortonVartError> {
        match self {
            VartNode::Internal4 { prefix, prefix_len, keys, children, count } => {
                let current_count = count.load(Ordering::Acquire);
                if current_count >= 4 {
                    // Need to grow to Internal16
                    // For simplicity, just reject when full for now
                    Err(MortonVartError::InsertionFailed)
                } else {
                    // Create new Internal4 with added child
                    let new_node = VartNode::Internal4 {
                        prefix: *prefix,
                        prefix_len: *prefix_len,
                        keys: {
                            let mut new_keys = *keys;
                            new_keys[current_count as usize] = key_byte;
                            new_keys
                        },
                        children: {
                            let new_children: [AtomicPtr<VartNode>; 4] = Default::default();
                            for i in 0..current_count as usize {
                                let old_ptr = children[i].load(Ordering::Acquire);
                                new_children[i].store(old_ptr, Ordering::Release);
                            }
                            new_children[current_count as usize].store(child_ptr, Ordering::Release);
                            new_children
                        },
                        count: AtomicU64::new(current_count + 1),
                    };
                    Ok(Box::into_raw(Box::new(new_node)))
                }
            },
            _ => Err(MortonVartError::InvalidNodeType),
        }
    }

    /// Compute common prefix between two Morton codes
    fn compute_common_prefix(&self, morton1: u64, morton2: u64) -> (u64, u8) {
        let xor = morton1 ^ morton2;
        let common_bits = xor.leading_zeros() as u8;
        let prefix = morton1 & (!((1u64 << (64 - common_bits)) - 1));
        (prefix, common_bits)
    }

    /// Range query using Morton properties
    fn range_query(&self, start_morton: u64, end_morton: u64, results: &mut Vec<MortonRecord>) {
        match self {
            VartNode::Leaf { records, .. } => {
                // Binary search for range bounds in sorted leaf
                let start_idx = records.binary_search_by_key(&start_morton, |r| r.morton_code)
                    .unwrap_or_else(|e| e);
                let end_idx = records.binary_search_by_key(&end_morton, |r| r.morton_code)
                    .unwrap_or_else(|e| e);
                
                results.extend_from_slice(&records[start_idx..end_idx]);
            },
            VartNode::Internal4 { prefix, prefix_len, children, .. } => {
                // Check if this subtree overlaps with query range
                if self.subtree_overlaps(*prefix, *prefix_len, start_morton, end_morton) {
                    // Recursively query overlapping children
                    for i in 0..4 {
                        let child_ptr = children[i].load(Ordering::Acquire);
                        if !child_ptr.is_null() {
                            let child = unsafe { &*child_ptr };
                            child.range_query(start_morton, end_morton, results);
                        }
                    }
                }
            },
            VartNode::Internal16 { prefix, prefix_len, children, .. } => {
                if self.subtree_overlaps(*prefix, *prefix_len, start_morton, end_morton) {
                    for i in 0..16 {
                        let child_ptr = children[i].load(Ordering::Acquire);
                        if !child_ptr.is_null() {
                            let child = unsafe { &*child_ptr };
                            child.range_query(start_morton, end_morton, results);
                        }
                    }
                }
            },
            VartNode::Internal256 { prefix, prefix_len, children, .. } => {
                if self.subtree_overlaps(*prefix, *prefix_len, start_morton, end_morton) {
                    for i in 0..256 {
                        let child_ptr = children[i].load(Ordering::Acquire);
                        if !child_ptr.is_null() {
                            let child = unsafe { &*child_ptr };
                            child.range_query(start_morton, end_morton, results);
                        }
                    }
                }
            },
        }
    }

    /// Check if subtree overlaps with query range
    fn subtree_overlaps(&self, prefix: u64, prefix_len: u8, start: u64, end: u64) -> bool {
        let mask = !((1u64 << (64 - prefix_len)) - 1);
        let subtree_start = prefix;
        let subtree_end = prefix | !mask;
        
        !(subtree_end < start || subtree_start > end)
    }
}

/// Main Morton-VART hybrid engine
pub struct MortonVartEngine {
    /// Root of the VART tree (atomic for coordination-free updates)
    root: AtomicPtr<VartNode>,
    
    /// Global timestamp counter
    global_timestamp: CachePadded<AtomicU64>,
    
    /// Memory-mapped file for persistence
    mmap: Option<MmapMut>,
    
    /// Performance counters
    read_count: CachePadded<AtomicU64>,
    write_count: CachePadded<AtomicU64>,
    node_count: CachePadded<AtomicU64>,
    split_count: CachePadded<AtomicU64>,
}

impl MortonVartEngine {
    /// Create new Morton-VART engine
    pub fn new(file_path: Option<PathBuf>) -> Result<Self, MortonVartError> {
        let initial_root = Box::into_raw(Box::new(VartNode::new_leaf(64)));
        
        let mmap = if let Some(path) = file_path {
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(path)?;
            file.set_len(1024 * 1024 * 1024)?; // 1GB
            
            Some(unsafe {
                MmapOptions::new()
                    .len(1024 * 1024 * 1024)
                    .map_mut(&file)?
            })
        } else {
            None
        };
        
        Ok(Self {
            root: AtomicPtr::new(initial_root),
            global_timestamp: CachePadded::new(AtomicU64::new(current_timestamp_micros())),
            mmap,
            read_count: CachePadded::new(AtomicU64::new(0)),
            write_count: CachePadded::new(AtomicU64::new(0)),
            node_count: CachePadded::new(AtomicU64::new(1)),
            split_count: CachePadded::new(AtomicU64::new(0)),
        })
    }

    /// Insert key-value pair (completely coordination-free)
    pub fn put(&self, key: &str, value: &[u8]) -> Result<(), MortonVartError> {
        let timestamp = self.global_timestamp.fetch_add(1, Ordering::SeqCst);
        let record = MortonRecord::new(key.to_string(), value.to_vec(), timestamp);
        
        self.write_count.fetch_add(1, Ordering::Relaxed);
        
        // Retry loop for coordination-free insertion
        loop {
            let root_ptr = self.root.load(Ordering::Acquire);
            let root = unsafe { &*root_ptr };
            
            match root.insert_atomic(record.clone()) {
                Ok(new_root) => {
                    // Atomically update root if it changed
                    if new_root != root_ptr {
                        match self.root.compare_exchange_weak(
                            root_ptr,
                            new_root,
                            Ordering::AcqRel,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => {
                                self.split_count.fetch_add(1, Ordering::Relaxed);
                                break;
                            },
                            Err(_) => {
                                // Root changed, retry
                                unsafe { Box::from_raw(new_root) }; // Clean up
                                continue;
                            }
                        }
                    } else {
                        break;
                    }
                },
                Err(e) => return Err(e),
            }
        }
        
        Ok(())
    }

    /// Get value for key at specific timestamp
    pub fn get(&self, key: &str, timestamp: Option<u64>) -> Result<Option<Vec<u8>>, MortonVartError> {
        let timestamp = timestamp.unwrap_or_else(|| self.global_timestamp.load(Ordering::Acquire));
        let morton_code = morton_t_encode(key, timestamp).value();
        
        self.read_count.fetch_add(1, Ordering::Relaxed);
        
        let root_ptr = self.root.load(Ordering::Acquire);
        let root = unsafe { &*root_ptr };
        
        self.search_node(root, morton_code, key)
    }

    /// Range query using Morton properties
    pub fn range_query(&self, key_start: &str, key_end: &str, timestamp: Option<u64>) -> Result<Vec<(String, Vec<u8>)>, MortonVartError> {
        let timestamp = timestamp.unwrap_or_else(|| self.global_timestamp.load(Ordering::Acquire));
        
        let start_morton = morton_t_bigmin(key_start).value();
        let end_morton = morton_t_litmax(key_end).value();
        
        let mut results = Vec::new();
        
        let root_ptr = self.root.load(Ordering::Acquire);
        let root = unsafe { &*root_ptr };
        
        root.range_query(start_morton, end_morton, &mut results);
        
        // Filter by lexicographic key range and timestamp
        let filtered: Vec<_> = results.into_iter()
            .filter(|r| r.key.as_str() >= key_start && r.key.as_str() <= key_end && r.timestamp <= timestamp)
            .map(|r| (r.key, r.value))
            .collect();
        
        Ok(filtered)
    }

    /// Search for exact Morton code match
    fn search_node(&self, node: &VartNode, morton_code: u64, key: &str) -> Result<Option<Vec<u8>>, MortonVartError> {
        match node {
            VartNode::Leaf { records, .. } => {
                // Binary search in sorted leaf
                if let Ok(index) = records.binary_search_by_key(&morton_code, |r| r.morton_code) {
                    // Handle hash collisions by checking actual key
                    for i in index.. {
                        if i >= records.len() || records[i].morton_code != morton_code {
                            break;
                        }
                        if records[i].key == key {
                            return Ok(Some(records[i].value.clone()));
                        }
                    }
                    // Check backwards for collisions
                    for i in (0..index).rev() {
                        if records[i].morton_code != morton_code {
                            break;
                        }
                        if records[i].key == key {
                            return Ok(Some(records[i].value.clone()));
                        }
                    }
                }
                Ok(None)
            },
            VartNode::Internal4 { prefix, prefix_len, keys, children, .. } => {
                // Check prefix match
                let common = node.longest_common_prefix(*prefix, *prefix_len, morton_code);
                if common >= *prefix_len {
                    let key_byte = node.extract_key_byte(morton_code, *prefix_len);
                    let child_index = node.find_child_index(key_byte, keys);
                    
                    if child_index < 4 {
                        let child_ptr = children[child_index].load(Ordering::Acquire);
                        if !child_ptr.is_null() {
                            let child = unsafe { &*child_ptr };
                            return self.search_node(child, morton_code, key);
                        }
                    }
                }
                Ok(None)
            },
            VartNode::Internal16 { prefix, prefix_len, keys, children, .. } => {
                let common = node.longest_common_prefix(*prefix, *prefix_len, morton_code);
                if common >= *prefix_len {
                    let key_byte = node.extract_key_byte(morton_code, *prefix_len);
                    let child_index = node.find_child_index(key_byte, keys);
                    
                    if child_index < 16 {
                        let child_ptr = children[child_index].load(Ordering::Acquire);
                        if !child_ptr.is_null() {
                            let child = unsafe { &*child_ptr };
                            return self.search_node(child, morton_code, key);
                        }
                    }
                }
                Ok(None)
            },
            VartNode::Internal256 { prefix, prefix_len, children, .. } => {
                let common = node.longest_common_prefix(*prefix, *prefix_len, morton_code);
                if common >= *prefix_len {
                    let key_byte = node.extract_key_byte(morton_code, *prefix_len);
                    let child_ptr = children[key_byte as usize].load(Ordering::Acquire);
                    if !child_ptr.is_null() {
                        let child = unsafe { &*child_ptr };
                        return self.search_node(child, morton_code, key);
                    }
                }
                Ok(None)
            },
        }
    }

    /// Get performance statistics
    pub fn stats(&self) -> MortonVartStats {
        MortonVartStats {
            reads: self.read_count.load(Ordering::Relaxed),
            writes: self.write_count.load(Ordering::Relaxed),
            node_count: self.node_count.load(Ordering::Relaxed),
            split_count: self.split_count.load(Ordering::Relaxed),
            current_timestamp: self.global_timestamp.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MortonVartStats {
    pub reads: u64,
    pub writes: u64,
    pub node_count: u64,
    pub split_count: u64,
    pub current_timestamp: u64,
}

#[derive(Debug)]
pub enum MortonVartError {
    IoError(std::io::Error),
    InvalidNodeType,
    InsertionFailed,
    MemoryAllocation,
}

impl From<std::io::Error> for MortonVartError {
    fn from(err: std::io::Error) -> Self {
        MortonVartError::IoError(err)
    }
}

impl std::fmt::Display for MortonVartError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MortonVartError::IoError(e) => write!(f, "I/O error: {}", e),
            MortonVartError::InvalidNodeType => write!(f, "Invalid node type"),
            MortonVartError::InsertionFailed => write!(f, "Insertion failed"),
            MortonVartError::MemoryAllocation => write!(f, "Memory allocation failed"),
        }
    }
}

impl std::error::Error for MortonVartError {}