//! Core data structures for ZVerse
//!
//! This module contains the fundamental data structures used throughout the ZVerse
//! implementation, including memory layouts, segment definitions, and entry formats.

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

/// Z-value range for a segment
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ZValueRange {
    /// Minimum Z-value in this range
    pub min: u64,
    /// Maximum Z-value in this range
    pub max: u64,
}

impl Hash for ZValueRange {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.min.hash(state);
        self.max.hash(state);
    }
}

/// Segment header stored at the beginning of each segment file
#[repr(C, align(4096))]
pub struct ZSegmentHeader {
    /// Magic number for validation (0x5A56455253450001)
    pub magic: u64,
    /// Format version for compatibility
    pub format_version: u32,
    /// Segment identifier
    pub segment_id: u64,
    /// Minimum Z-value in this segment
    pub z_value_min: u64,
    /// Maximum Z-value in this segment
    pub z_value_max: u64,
    /// Number of entries in the segment
    pub entry_count: u64,
    /// Offset to free space
    pub free_space_offset: u64,
    /// CRC64 of segment data for integrity
    pub checksum: u64,
    /// Creation timestamp
    pub created_timestamp: u64,
    /// Last modification timestamp
    pub last_modified_timestamp: u64,
    /// Reserved space for future use (pad to 4096 bytes)
    pub reserved: [u8; 4024],
}

/// Entry within a segment
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ZEntry {
    /// Z-value (interleaved key + version bits)
    pub z_value: u64,
    /// Offset to key within segment
    pub key_offset: u32,
    /// Length of key in bytes
    pub key_length: u16,
    /// Offset to value within segment
    pub value_offset: u32,
    /// Length of value in bytes
    pub value_length: u32,
    /// Original version
    pub version: u64,
    /// Bit flags (deleted, compressed, etc.)
    pub flags: u16,
}

impl ZEntry {
    /// Check if this entry is a tombstone (deleted)
    pub fn is_deleted(&self) -> bool {
        (self.flags & 0x0001) != 0
    }
    
    /// Set the deleted flag
    pub fn set_deleted(&mut self, deleted: bool) {
        if deleted {
            self.flags |= 0x0001;
        } else {
            self.flags &= !0x0001;
        }
    }
    
    /// Check if this entry is compressed
    pub fn is_compressed(&self) -> bool {
        (self.flags & 0x0002) != 0
    }
    
    /// Set the compressed flag
    pub fn set_compressed(&mut self, compressed: bool) {
        if compressed {
            self.flags |= 0x0002;
        } else {
            self.flags &= !0x0002;
        }
    }
}

impl PartialEq for ZEntry {
    fn eq(&self, other: &Self) -> bool {
        self.z_value == other.z_value
    }
}

impl Eq for ZEntry {}

impl Ord for ZEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.z_value.cmp(&other.z_value)
    }
}

impl PartialOrd for ZEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Reference to a memory-mapped segment
pub struct SegmentRef {
    /// Segment ID
    pub id: u64,
    /// Segment path
    pub path: PathBuf,
    /// Z-value range
    pub z_range: ZValueRange,
    /// Whether this segment is active (in memory)
    pub active: bool,
}

/// Entry with key and value data
#[derive(Debug, Clone)]
pub struct EntryWithData {
    /// Entry metadata
    pub entry: ZEntry,
    /// Key data
    pub key: Vec<u8>,
    /// Value data
    pub value: Vec<u8>,
}

impl EntryWithData {
    /// Create a new entry with data
    pub fn new(
        z_value: u64,
        key: Vec<u8>,
        value: Vec<u8>,
        version: u64,
        deleted: bool,
    ) -> Self {
        let mut entry = ZEntry {
            z_value,
            key_offset: 0,
            key_length: key.len() as u16,
            value_offset: 0,
            value_length: value.len() as u32,
            version,
            flags: 0,
        };
        
        if deleted {
            entry.set_deleted(true);
        }
        
        Self {
            entry,
            key,
            value,
        }
    }
    
    /// Check if this entry is a tombstone (deleted)
    pub fn is_deleted(&self) -> bool {
        self.entry.is_deleted()
    }
    
    /// Get the key as a byte slice
    pub fn key_bytes(&self) -> &[u8] {
        &self.key
    }
    
    /// Get the value as a byte slice
    pub fn value_bytes(&self) -> &[u8] {
        &self.value
    }
}

/// Tier type for segment temperature
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TierType {
    /// Hot tier - frequently accessed data
    Hot,
    /// Warm tier - occasionally accessed data
    Warm,
    /// Cold tier - rarely accessed data
    Cold,
    /// Archive tier - historical data
    Archive,
}