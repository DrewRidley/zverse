//! Storage format and serialization for ZVerse entries
//!
//! This module defines the on-disk format for entries and provides serialization
//! utilities for COW operations with mmap2 integration.

use crate::interleaved_key::InterleavedKey;
use byteorder::{BigEndian, ByteOrder, ReadBytesExt, WriteBytesExt};
use std::io::{self, Cursor, Read, Write};

/// File offset for locating entries in the mmap'd file
pub type FileOffset = u64;

/// Threshold for storing values inline vs overflow area
pub const INLINE_VALUE_THRESHOLD: u32 = 4096; // 4KB

/// Entry stored in the database file
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub cow_key: InterleavedKey,
    pub original_key: Vec<u8>,
    pub value: EntryValue,
}

/// Value storage strategy - inline or overflow
#[derive(Debug, Clone, PartialEq)]
pub enum EntryValue {
    /// Small values stored directly in the entry
    Inline(Vec<u8>),
    /// Large values stored in overflow area with reference
    Overflow { size: u64, offset: FileOffset },
}

impl Entry {
    /// Create a new entry with automatic value storage decision
    pub fn new(cow_key: InterleavedKey, original_key: Vec<u8>, value: Vec<u8>) -> Self {
        let entry_value = if value.len() <= INLINE_VALUE_THRESHOLD as usize {
            EntryValue::Inline(value)
        } else {
            // For now, treat large values as inline until we implement overflow
            // TODO: Implement overflow storage in Phase 4
            EntryValue::Inline(value)
        };

        Self {
            cow_key,
            original_key,
            value: entry_value,
        }
    }

    /// Get the value bytes regardless of storage type
    pub fn value_bytes(&self) -> &[u8] {
        match &self.value {
            EntryValue::Inline(bytes) => bytes,
            EntryValue::Overflow { .. } => {
                // TODO: Implement overflow reading in Phase 4
                panic!("Overflow values not yet implemented");
            }
        }
    }

    /// Check if this entry matches the given original key
    pub fn matches_key(&self, key: &[u8]) -> bool {
        self.original_key == key
    }

    /// Get the total serialized size of this entry
    pub fn serialized_size(&self) -> usize {
        let mut size = 0;
        
        // cow_key_length (4 bytes) + cow_key bytes
        size += 4 + self.cow_key.interleaved_bytes().len();
        
        // original_key_length (4 bytes) + original_key bytes
        size += 4 + self.original_key.len();
        
        // timestamp (8 bytes)
        size += 8;
        
        // value_type (1 byte) + value_length (4 bytes)
        size += 1 + 4;
        
        match &self.value {
            EntryValue::Inline(bytes) => size + bytes.len(),
            EntryValue::Overflow { .. } => size + 8, // overflow_offset
        }
    }

    /// Serialize entry to bytes for storage
    pub fn serialize(&self) -> io::Result<Vec<u8>> {
        let mut buffer = Vec::with_capacity(self.serialized_size());
        self.write_to(&mut buffer)?;
        Ok(buffer)
    }

    /// Write entry to a writer
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Write cow_key_length and cow_key
        let cow_key_bytes = self.cow_key.interleaved_bytes();
        writer.write_u32::<BigEndian>(cow_key_bytes.len() as u32)?;
        writer.write_all(cow_key_bytes)?;

        // Write original_key_length and original_key
        writer.write_u32::<BigEndian>(self.original_key.len() as u32)?;
        writer.write_all(&self.original_key)?;

        // Write timestamp
        writer.write_u64::<BigEndian>(self.cow_key.timestamp())?;

        // Write value type and data
        match &self.value {
            EntryValue::Inline(bytes) => {
                writer.write_u8(0)?; // Inline type marker
                writer.write_u32::<BigEndian>(bytes.len() as u32)?;
                writer.write_all(bytes)?;
            }
            EntryValue::Overflow { size, offset } => {
                writer.write_u8(1)?; // Overflow type marker
                writer.write_u32::<BigEndian>(*size as u32)?;
                writer.write_u64::<BigEndian>(*offset)?;
            }
        }

        Ok(())
    }

    /// Deserialize entry from bytes
    pub fn deserialize(bytes: &[u8]) -> io::Result<Self> {
        let mut cursor = Cursor::new(bytes);
        Self::read_from(&mut cursor)
    }

    /// Read entry from a reader
    pub fn read_from<R: Read>(reader: &mut R) -> io::Result<Self> {
        // Read cow_key
        let cow_key_len = reader.read_u32::<BigEndian>()? as usize;
        let mut cow_key_bytes = vec![0u8; cow_key_len];
        reader.read_exact(&mut cow_key_bytes)?;

        // Read original_key
        let original_key_len = reader.read_u32::<BigEndian>()? as usize;
        let mut original_key = vec![0u8; original_key_len];
        reader.read_exact(&mut original_key)?;

        // Read timestamp
        let timestamp = reader.read_u64::<BigEndian>()?;

        // Reconstruct InterleavedKey
        let cow_key = InterleavedKey::from_interleaved_bytes(&cow_key_bytes)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Invalid interleaved key"))?;

        // Verify timestamp consistency
        if cow_key.timestamp() != timestamp {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Timestamp mismatch in entry",
            ));
        }

        // Read value
        let value_type = reader.read_u8()?;
        let value = match value_type {
            0 => {
                // Inline value
                let value_len = reader.read_u32::<BigEndian>()? as usize;
                let mut value_bytes = vec![0u8; value_len];
                reader.read_exact(&mut value_bytes)?;
                EntryValue::Inline(value_bytes)
            }
            1 => {
                // Overflow value
                let size = reader.read_u32::<BigEndian>()? as u64;
                let offset = reader.read_u64::<BigEndian>()?;
                EntryValue::Overflow { size, offset }
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Unknown value type",
                ));
            }
        };

        Ok(Self {
            cow_key,
            original_key,
            value,
        })
    }
}

/// Iterator over entries in a byte slice (for mmap scanning)
pub struct EntryIterator<'a> {
    data: &'a [u8],
    position: usize,
}

impl<'a> EntryIterator<'a> {
    /// Create new iterator over entry data
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, position: 0 }
    }

    /// Create iterator starting at a specific offset
    pub fn at_offset(data: &'a [u8], offset: usize) -> Self {
        Self {
            data,
            position: offset,
        }
    }

    /// Get current position in the data
    pub fn position(&self) -> usize {
        self.position
    }
}

impl<'a> Iterator for EntryIterator<'a> {
    type Item = io::Result<(FileOffset, Entry)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.position >= self.data.len() {
            return None;
        }

        let offset = self.position as FileOffset;
        let mut cursor = Cursor::new(&self.data[self.position..]);
        
        match Entry::read_from(&mut cursor) {
            Ok(entry) => {
                self.position += cursor.position() as usize;
                Some(Ok((offset, entry)))
            }
            Err(e) => Some(Err(e)),
        }
    }
}

/// Binary search for entries in sorted mmap data
pub fn binary_search_entries(
    data: &[u8],
    target_key: &InterleavedKey,
) -> Result<Option<(FileOffset, Entry)>, io::Error> {
    let mut left = 0;
    let mut right = data.len();
    let mut best_match: Option<(FileOffset, Entry)> = None;

    while left < right {
        let mid = left + (right - left) / 2;
        
        // Find entry boundary by scanning from mid
        let entry_start = find_entry_start_from(data, mid)?;
        if entry_start >= data.len() {
            right = mid;
            continue;
        }

        let mut cursor = Cursor::new(&data[entry_start..]);
        let entry = Entry::read_from(&mut cursor)?;
        
        match entry.cow_key.cmp(target_key) {
            std::cmp::Ordering::Equal => {
                return Ok(Some((entry_start as FileOffset, entry)));
            }
            std::cmp::Ordering::Less => {
                // This entry is smaller, check if it's the best match for the original key
                if entry.original_key == target_key.original_key() {
                    best_match = Some((entry_start as FileOffset, entry));
                }
                left = entry_start + cursor.position() as usize;
            }
            std::cmp::Ordering::Greater => {
                right = entry_start;
            }
        }
    }

    Ok(best_match)
}

/// Find the start of an entry boundary from any position in the data
fn find_entry_start_from(data: &[u8], start_pos: usize) -> io::Result<usize> {
    // For now, scan backwards to find a valid entry start
    // This is a simplified implementation - in practice we'd want
    // entry boundaries marked or use a more sophisticated approach
    
    for pos in (0..=start_pos).rev() {
        if pos + 4 > data.len() {
            continue;
        }
        
        // Try to read a length field
        let mut cursor = Cursor::new(&data[pos..]);
        if let Ok(cow_key_len) = cursor.read_u32::<BigEndian>() {
            if cow_key_len > 0 && cow_key_len < 1024 && pos + 4 + cow_key_len as usize <= data.len() {
                // This looks like a valid entry start
                return Ok(pos);
            }
        }
    }
    
    Ok(start_pos)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::next_timestamp;

    #[test]
    fn test_entry_serialization() {
        let timestamp = next_timestamp();
        let cow_key = InterleavedKey::new(b"test_key", timestamp);
        let value = b"test_value".to_vec();
        
        let entry = Entry::new(cow_key.clone(), b"test_key".to_vec(), value.clone());
        
        let serialized = entry.serialize().unwrap();
        let deserialized = Entry::deserialize(&serialized).unwrap();
        
        assert_eq!(entry.cow_key.interleaved_bytes(), deserialized.cow_key.interleaved_bytes());
        assert_eq!(entry.original_key, deserialized.original_key);
        assert_eq!(entry.value_bytes(), deserialized.value_bytes());
    }

    #[test]
    fn test_entry_iterator() {
        let timestamp1 = next_timestamp();
        let timestamp2 = next_timestamp();
        
        let entry1 = Entry::new(
            InterleavedKey::new(b"key1", timestamp1),
            b"key1".to_vec(),
            b"value1".to_vec(),
        );
        
        let entry2 = Entry::new(
            InterleavedKey::new(b"key2", timestamp2),
            b"key2".to_vec(),
            b"value2".to_vec(),
        );
        
        let mut data = Vec::new();
        entry1.write_to(&mut data).unwrap();
        entry2.write_to(&mut data).unwrap();
        
        let mut iter = EntryIterator::new(&data);
        
        let (offset1, read_entry1) = iter.next().unwrap().unwrap();
        assert_eq!(offset1, 0);
        assert_eq!(read_entry1.original_key, b"key1");
        
        let (offset2, read_entry2) = iter.next().unwrap().unwrap();
        assert!(offset2 > 0);
        assert_eq!(read_entry2.original_key, b"key2");
        
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_value_threshold() {
        let timestamp = next_timestamp();
        let cow_key = InterleavedKey::new(b"key", timestamp);
        
        // Small value should be inline
        let small_value = vec![0u8; 100];
        let entry = Entry::new(cow_key.clone(), b"key".to_vec(), small_value);
        assert!(matches!(entry.value, EntryValue::Inline(_)));
        
        // Large value should still be inline for now (until Phase 4)
        let large_value = vec![0u8; 10000];
        let entry = Entry::new(cow_key, b"key".to_vec(), large_value);
        assert!(matches!(entry.value, EntryValue::Inline(_)));
    }

    #[test]
    fn test_entry_ordering() {
        let timestamp1 = 1000;
        let timestamp2 = 2000;
        
        let entry1 = Entry::new(
            InterleavedKey::new(b"aaa", timestamp1),
            b"aaa".to_vec(),
            b"value1".to_vec(),
        );
        
        let entry2 = Entry::new(
            InterleavedKey::new(b"aaa", timestamp2),
            b"aaa".to_vec(),
            b"value2".to_vec(),
        );
        
        // Later timestamp should sort after earlier timestamp for same key
        assert!(entry1.cow_key < entry2.cow_key);
    }
}