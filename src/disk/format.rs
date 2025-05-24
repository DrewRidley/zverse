//! Binary file format definitions for memory-mapped temporal partitions
//!
//! This module defines the on-disk format for partition files that can be
//! directly memory-mapped for zero-copy access.

use std::mem;
use byteorder::{LittleEndian, ByteOrder};

/// Magic number identifying ZVerse partition files
pub const PARTITION_MAGIC: &[u8; 8] = b"ZVERSEDB";

/// Current format version
pub const FORMAT_VERSION: u32 = 1;

/// Header size in bytes (aligned to cache line boundary)
pub const HEADER_SIZE: usize = 128;

/// Cache line size for alignment optimization
pub const CACHE_LINE_SIZE: usize = 64;

/// Partition file header (128 bytes, cache-line aligned)
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct PartitionHeader {
    /// Magic number for file identification
    pub magic: [u8; 8],
    
    /// Format version number
    pub version: u32,
    
    /// Partition ID (Fibonacci hash partition)
    pub partition_id: u32,
    
    /// Number of records in this partition
    pub record_count: u32,
    
    /// Earliest timestamp in partition (for temporal locality)
    pub min_timestamp: u64,
    
    /// Latest timestamp in partition
    pub max_timestamp: u64,
    
    /// Offset to records section from file start
    pub records_offset: u64,
    
    /// Offset to index section from file start
    pub index_offset: u64,
    
    /// Total file size in bytes
    pub file_size: u64,
    
    /// CRC32 checksum of header (excluding this field)
    pub header_checksum: u32,
    
    /// CRC32 checksum of records section
    pub records_checksum: u32,
    
    /// CRC32 checksum of index section
    pub index_checksum: u32,
    
    /// Flags for partition properties
    pub flags: u32,
    
    /// Reserved space for future extensions
    pub reserved: [u8; 40],
}

/// Record format for temporal data (variable length)
#[repr(C)]
#[derive(Debug, Clone)]
pub struct RecordHeader {
    /// Morton-encoded key (space-time locality)
    pub morton_key: u64,
    
    /// Key length in bytes
    pub key_len: u16,
    
    /// Value length in bytes
    pub value_len: u32,
    
    /// Record flags (deleted, compressed, etc.)
    pub flags: u16,
}

/// Index entry for binary search on Morton codes
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct IndexEntry {
    /// Morton-encoded key
    pub morton_key: u64,
    
    /// Offset to record in file
    pub record_offset: u64,
}

/// Partition flags
pub mod flags {
    /// Partition contains compressed data
    pub const COMPRESSED: u32 = 0x0001;
    
    /// Partition is read-only
    pub const READ_ONLY: u32 = 0x0002;
    
    /// Partition uses temporal optimization
    pub const TEMPORAL_OPTIMIZED: u32 = 0x0004;
    
    /// Partition has been synced to disk
    pub const SYNCED: u32 = 0x0008;
}

/// Record flags
pub mod record_flags {
    /// Record has been deleted (tombstone)
    pub const DELETED: u16 = 0x0001;
    
    /// Record value is compressed
    pub const COMPRESSED: u16 = 0x0002;
    
    /// Record has extended metadata
    pub const EXTENDED: u16 = 0x0004;
}

impl PartitionHeader {
    /// Create a new partition header
    pub fn new(partition_id: u32) -> Self {
        Self {
            magic: *PARTITION_MAGIC,
            version: FORMAT_VERSION,
            partition_id,
            record_count: 0,
            min_timestamp: u64::MAX,
            max_timestamp: 0,
            records_offset: HEADER_SIZE as u64,
            index_offset: HEADER_SIZE as u64,
            file_size: HEADER_SIZE as u64,
            header_checksum: 0,
            records_checksum: 0,
            index_checksum: 0,
            flags: flags::TEMPORAL_OPTIMIZED,
            reserved: [0; 40],
        }
    }
    
    /// Validate header magic and version
    pub fn validate(&self) -> Result<(), FormatError> {
        if &self.magic != PARTITION_MAGIC {
            return Err(FormatError::InvalidMagic);
        }
        
        if self.version != FORMAT_VERSION {
            return Err(FormatError::UnsupportedVersion(self.version));
        }
        
        if self.records_offset < HEADER_SIZE as u64 {
            return Err(FormatError::InvalidOffset);
        }
        
        Ok(())
    }
    
    /// Calculate and update header checksum
    pub fn update_checksum(&mut self) {
        self.header_checksum = 0; // Zero out before calculating
        let header_bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const _ as *const u8,
                mem::size_of::<PartitionHeader>()
            )
        };
        self.header_checksum = crc32fast::hash(&header_bytes[..mem::size_of::<PartitionHeader>() - 4]);
    }
    
    /// Verify header checksum
    pub fn verify_checksum(&self) -> bool {
        let stored_checksum = self.header_checksum;
        let mut temp_header = *self;
        temp_header.update_checksum();
        temp_header.header_checksum == stored_checksum
    }
    
    /// Serialize header to bytes (little-endian)
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut bytes = [0u8; HEADER_SIZE];
        
        bytes[0..8].copy_from_slice(&self.magic);
        LittleEndian::write_u32(&mut bytes[8..12], self.version);
        LittleEndian::write_u32(&mut bytes[12..16], self.partition_id);
        LittleEndian::write_u32(&mut bytes[16..20], self.record_count);
        LittleEndian::write_u64(&mut bytes[20..28], self.min_timestamp);
        LittleEndian::write_u64(&mut bytes[28..36], self.max_timestamp);
        LittleEndian::write_u64(&mut bytes[36..44], self.records_offset);
        LittleEndian::write_u64(&mut bytes[44..52], self.index_offset);
        LittleEndian::write_u64(&mut bytes[52..60], self.file_size);
        LittleEndian::write_u32(&mut bytes[60..64], self.header_checksum);
        LittleEndian::write_u32(&mut bytes[64..68], self.records_checksum);
        LittleEndian::write_u32(&mut bytes[68..72], self.index_checksum);
        LittleEndian::write_u32(&mut bytes[72..76], self.flags);
        bytes[76..116].copy_from_slice(&self.reserved);
        
        bytes
    }
    
    /// Deserialize header from bytes (little-endian)
    pub fn from_bytes(bytes: &[u8; HEADER_SIZE]) -> Self {
        let mut magic = [0u8; 8];
        magic.copy_from_slice(&bytes[0..8]);
        
        let mut reserved = [0u8; 40];
        reserved.copy_from_slice(&bytes[76..116]);
        
        Self {
            magic,
            version: LittleEndian::read_u32(&bytes[8..12]),
            partition_id: LittleEndian::read_u32(&bytes[12..16]),
            record_count: LittleEndian::read_u32(&bytes[16..20]),
            min_timestamp: LittleEndian::read_u64(&bytes[20..28]),
            max_timestamp: LittleEndian::read_u64(&bytes[28..36]),
            records_offset: LittleEndian::read_u64(&bytes[36..44]),
            index_offset: LittleEndian::read_u64(&bytes[44..52]),
            file_size: LittleEndian::read_u64(&bytes[52..60]),
            header_checksum: LittleEndian::read_u32(&bytes[60..64]),
            records_checksum: LittleEndian::read_u32(&bytes[64..68]),
            index_checksum: LittleEndian::read_u32(&bytes[68..72]),
            flags: LittleEndian::read_u32(&bytes[72..76]),
            reserved,
        }
    }
}

impl RecordHeader {
    /// Size of record header in bytes
    pub const SIZE: usize = mem::size_of::<RecordHeader>();
    
    /// Create a new record header
    pub fn new(morton_key: u64, key_len: u16, value_len: u32) -> Self {
        Self {
            morton_key,
            key_len,
            value_len,
            flags: 0,
        }
    }
    
    /// Total size of this record including header, key, and value
    pub fn total_size(&self) -> usize {
        Self::SIZE + self.key_len as usize + self.value_len as usize
    }
    
    /// Check if record is deleted
    pub fn is_deleted(&self) -> bool {
        (self.flags & record_flags::DELETED) != 0
    }
    
    /// Mark record as deleted
    pub fn mark_deleted(&mut self) {
        self.flags |= record_flags::DELETED;
    }
    
    /// Serialize record header to bytes
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        LittleEndian::write_u64(&mut bytes[0..8], self.morton_key);
        LittleEndian::write_u16(&mut bytes[8..10], self.key_len);
        LittleEndian::write_u32(&mut bytes[10..14], self.value_len);
        LittleEndian::write_u16(&mut bytes[14..16], self.flags);
        bytes
    }
    
    /// Deserialize record header from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, FormatError> {
        if bytes.len() < Self::SIZE {
            return Err(FormatError::InsufficientData);
        }
        
        Ok(Self {
            morton_key: LittleEndian::read_u64(&bytes[0..8]),
            key_len: LittleEndian::read_u16(&bytes[8..10]),
            value_len: LittleEndian::read_u32(&bytes[10..14]),
            flags: LittleEndian::read_u16(&bytes[14..16]),
        })
    }
}

impl IndexEntry {
    /// Size of index entry in bytes
    pub const SIZE: usize = mem::size_of::<IndexEntry>();
    
    /// Create a new index entry
    pub fn new(morton_key: u64, record_offset: u64) -> Self {
        Self {
            morton_key,
            record_offset,
        }
    }
    
    /// Serialize index entry to bytes
    pub fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        LittleEndian::write_u64(&mut bytes[0..8], self.morton_key);
        LittleEndian::write_u64(&mut bytes[8..16], self.record_offset);
        bytes
    }
    
    /// Deserialize index entry from bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, FormatError> {
        if bytes.len() < Self::SIZE {
            return Err(FormatError::InsufficientData);
        }
        
        Ok(Self {
            morton_key: LittleEndian::read_u64(&bytes[0..8]),
            record_offset: LittleEndian::read_u64(&bytes[8..16]),
        })
    }
}

/// Format-related errors
#[derive(Debug, Clone, PartialEq)]
pub enum FormatError {
    /// Invalid magic number in header
    InvalidMagic,
    /// Unsupported format version
    UnsupportedVersion(u32),
    /// Invalid offset in header
    InvalidOffset,
    /// Checksum mismatch
    ChecksumMismatch,
    /// Insufficient data for parsing
    InsufficientData,
    /// Record is corrupted
    CorruptedRecord,
}

impl std::fmt::Display for FormatError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FormatError::InvalidMagic => write!(f, "Invalid magic number"),
            FormatError::UnsupportedVersion(v) => write!(f, "Unsupported format version: {}", v),
            FormatError::InvalidOffset => write!(f, "Invalid offset in header"),
            FormatError::ChecksumMismatch => write!(f, "Checksum verification failed"),
            FormatError::InsufficientData => write!(f, "Insufficient data for parsing"),
            FormatError::CorruptedRecord => write!(f, "Record data is corrupted"),
        }
    }
}

impl std::error::Error for FormatError {}

/// Utility functions for working with the file format
pub mod utils {
    use super::*;
    
    /// Align size to cache line boundary
    pub fn align_to_cache_line(size: usize) -> usize {
        (size + CACHE_LINE_SIZE - 1) & !(CACHE_LINE_SIZE - 1)
    }
    
    /// Calculate padding needed for cache line alignment
    pub fn cache_line_padding(size: usize) -> usize {
        align_to_cache_line(size) - size
    }
    
    /// Verify data integrity using CRC32
    pub fn verify_checksum(data: &[u8], expected: u32) -> bool {
        crc32fast::hash(data) == expected
    }
    
    /// Calculate CRC32 checksum for data
    pub fn calculate_checksum(data: &[u8]) -> u32 {
        crc32fast::hash(data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_header_serialization() {
        let mut header = PartitionHeader::new(42);
        header.record_count = 100;
        header.min_timestamp = 1000;
        header.max_timestamp = 2000;
        header.update_checksum();
        
        let bytes = header.to_bytes();
        let deserialized = PartitionHeader::from_bytes(&bytes);
        
        assert_eq!(deserialized.partition_id, 42);
        assert_eq!(deserialized.record_count, 100);
        assert_eq!(deserialized.min_timestamp, 1000);
        assert_eq!(deserialized.max_timestamp, 2000);
        assert!(deserialized.verify_checksum());
    }
    
    #[test]
    fn test_record_header_serialization() {
        let header = RecordHeader::new(0x123456789ABCDEF0, 32, 1024);
        let bytes = header.to_bytes();
        let deserialized = RecordHeader::from_bytes(&bytes).unwrap();
        
        assert_eq!(deserialized.morton_key, 0x123456789ABCDEF0);
        assert_eq!(deserialized.key_len, 32);
        assert_eq!(deserialized.value_len, 1024);
    }
    
    #[test]
    fn test_index_entry_serialization() {
        let entry = IndexEntry::new(0xFEDCBA9876543210, 0x1000);
        let bytes = entry.to_bytes();
        let deserialized = IndexEntry::from_bytes(&bytes).unwrap();
        
        assert_eq!(deserialized.morton_key, 0xFEDCBA9876543210);
        assert_eq!(deserialized.record_offset, 0x1000);
    }
}