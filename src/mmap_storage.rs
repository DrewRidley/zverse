//! Memory-mapped storage implementation for ZVerse
//!
//! This module provides a persistent storage implementation using memory-mapped files
//! organized into segments based on Z-value ranges. It supports zero-copy access to data
//! and efficient persistence without traditional WAL.

use crate::core::{ZEntry, ZSegmentHeader, ZValueRange};
use crate::error::Error;
use crate::zorder;

use memmap2::{MmapMut, MmapOptions};
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Magic number for segment files (ZVERSE00)
const SEGMENT_MAGIC: u64 = 0x5A56455253453030;

/// Current format version
const FORMAT_VERSION: u32 = 1;

/// Default segment size (16MB)
const DEFAULT_SEGMENT_SIZE: usize = 16 * 1024 * 1024;

/// Minimum segment size (1MB)
const MIN_SEGMENT_SIZE: usize = 1024 * 1024;

/// Maximum entries per segment
const MAX_ENTRIES_PER_SEGMENT: usize = 1_000_000;

/// Memory-mapped segment file
pub struct MmapSegment {
    /// Path to segment file
    path: PathBuf,

    /// Memory-mapped file
    mmap: RwLock<MmapMut>,

    /// Header reference
    header: *mut ZSegmentHeader,

    /// Z-value range contained in this segment
    z_range: ZValueRange,

    /// Segment ID
    id: u64,

    /// Lock for appending entries
    append_lock: Mutex<()>,
}

// Safe to share across threads
unsafe impl Send for MmapSegment {}
unsafe impl Sync for MmapSegment {}

impl MmapSegment {
    /// Create a new segment file
    pub fn create<P: AsRef<Path>>(
        path: P,
        id: u64,
        z_min: u64,
        z_max: u64,
        size: usize,
    ) -> Result<Self, Error> {
        let path = path.as_ref().to_path_buf();

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Create segment file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;

        // Set file size
        let segment_size = size.max(MIN_SEGMENT_SIZE);
        file.set_len(segment_size as u64)?;

        // Create memory map
        let mut mmap = unsafe { MmapOptions::new().map_mut(&file)? };

        // Initialize header
        let header = mmap.as_mut_ptr() as *mut ZSegmentHeader;
        unsafe {
            (*header).magic = SEGMENT_MAGIC;
            (*header).format_version = FORMAT_VERSION;
            (*header).segment_id = id;
            (*header).z_value_min = z_min;
            (*header).z_value_max = z_max;
            (*header).entry_count = 0;
            (*header).free_space_offset = std::mem::size_of::<ZSegmentHeader>() as u64;
            (*header).checksum = 0;
            (*header).created_timestamp = chrono::Utc::now().timestamp() as u64;
            (*header).last_modified_timestamp = (*header).created_timestamp;

            // Zero out reserved space
            (*header).reserved = [0; 4024];
        }

        // Sync to disk
        mmap.flush()?;

        Ok(Self {
            path,
            mmap: RwLock::new(mmap),
            header,
            z_range: ZValueRange {
                min: z_min,
                max: z_max,
            },
            id,
            append_lock: Mutex::new(()),
        })
    }

    /// Open an existing segment file
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let path = path.as_ref().to_path_buf();

        // Open existing file
        let file = OpenOptions::new().read(true).write(true).open(&path)?;

        // Create memory map
        let mmap = unsafe { MmapOptions::new().map_mut(&file)? };

        // Validate header
        let header = mmap.as_ptr() as *const ZSegmentHeader;
        unsafe {
            if (*header).magic != SEGMENT_MAGIC {
                return Err(Error::CorruptSegment);
            }

            if (*header).format_version != FORMAT_VERSION {
                return Err(Error::ConfigError(format!(
                    "Unsupported format version: {}",
                    (*header).format_version
                )));
            }
        }

        // Extract segment info
        let id = unsafe { (*header).segment_id };
        let z_min = unsafe { (*header).z_value_min };
        let z_max = unsafe { (*header).z_value_max };

        Ok(Self {
            path,
            mmap: RwLock::new(mmap),
            header: header as *mut ZSegmentHeader,
            z_range: ZValueRange {
                min: z_min,
                max: z_max,
            },
            id,
            append_lock: Mutex::new(()),
        })
    }

    /// Get segment ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Get Z-value range
    pub fn z_range(&self) -> &ZValueRange {
        &self.z_range
    }

    /// Check if the segment contains a specific Z-value
    pub fn contains_z(&self, z_value: u64) -> bool {
        z_value >= self.z_range.min && z_value <= self.z_range.max
    }

    /// Get entry count
    pub fn entry_count(&self) -> u64 {
        unsafe { (*self.header).entry_count }
    }

    /// Append an entry to the segment
    pub fn append_entry(
        &self,
        entry: &ZEntry,
        key_data: &[u8],
        value_data: &[u8],
    ) -> Result<(), Error> {
        // Acquire append lock to ensure thread safety
        let _lock = self.append_lock.lock();

        // Calculate sizes
        let key_len = key_data.len();
        let value_len = value_data.len();
        let entry_size = std::mem::size_of::<ZEntry>() + key_len + value_len;

        // Ensure 8-byte alignment for the entry
        let entry_size_aligned = (entry_size + 7) & !7;

        let mut mmap = self.mmap.write();

        // Check if there's enough space
        let free_offset = unsafe { (*self.header).free_space_offset } as usize;

        // Ensure the free_offset is 8-byte aligned
        let aligned_offset = (free_offset + 7) & !7;

        if aligned_offset + entry_size_aligned > mmap.len() {
            return Err(Error::Other("Segment is full".to_string()));
        }

        // Calculate offsets for key and value within the segment
        let key_offset = aligned_offset + std::mem::size_of::<ZEntry>();
        let value_offset = key_offset + key_len;

        // Create entry with correct offsets
        let mut new_entry = *entry;
        new_entry.key_offset = key_offset as u32;
        new_entry.value_offset = value_offset as u32;
        new_entry.key_length = key_len as u16;
        new_entry.value_length = value_len as u32;

        // Write entry at aligned offset
        let entry_ptr = unsafe { mmap.as_mut_ptr().add(aligned_offset) as *mut ZEntry };
        unsafe {
            *entry_ptr = new_entry;
        }

        // Write key data
        if !key_data.is_empty() {
            let key_ptr = unsafe { mmap.as_mut_ptr().add(key_offset) };
            unsafe {
                std::ptr::copy_nonoverlapping(key_data.as_ptr(), key_ptr, key_len);
            }
        }

        // Write value data
        if !value_data.is_empty() {
            let value_ptr = unsafe { mmap.as_mut_ptr().add(value_offset) };
            unsafe {
                std::ptr::copy_nonoverlapping(value_data.as_ptr(), value_ptr, value_len);
            }
        }

        // Update header
        unsafe {
            (*self.header).entry_count += 1;
            (*self.header).free_space_offset = (aligned_offset + entry_size_aligned) as u64;
            (*self.header).last_modified_timestamp = chrono::Utc::now().timestamp() as u64;
        }

        // Sync to disk
        mmap.flush_range(aligned_offset, entry_size_aligned)?;
        mmap.flush_range(0, std::mem::size_of::<ZSegmentHeader>())?;

        Ok(())
    }

    /// Iterate over all entries in the segment
    pub fn iter_entries(&self) -> Result<Vec<ZEntry>, Error> {
        let mmap = self.mmap.read();
        let entry_count = unsafe { (*self.header).entry_count } as usize;
        let mut entries = Vec::with_capacity(entry_count);

        // Start after the header, aligned to 8 bytes
        let mut offset = (std::mem::size_of::<ZSegmentHeader>() + 7) & !7;

        for _ in 0..entry_count {
            // Ensure offset is 8-byte aligned
            offset = (offset + 7) & !7;

            if offset + std::mem::size_of::<ZEntry>() > mmap.len() {
                break;
            }

            let entry = unsafe { *(mmap.as_ptr().add(offset) as *const ZEntry) };
            entries.push(entry);

            // Move to next entry (entry + key + value, then align)
            let entry_total_size = std::mem::size_of::<ZEntry>()
                + entry.key_length as usize
                + entry.value_length as usize;
            offset += (entry_total_size + 7) & !7;
        }

        Ok(entries.into_iter().collect())
    }

    /// Get key bytes for an entry
    pub fn get_key_bytes(&self, entry: &ZEntry) -> &[u8] {
        let mmap = self.mmap.read();
        let key_offset = entry.key_offset as usize;
        let key_len = entry.key_length as usize;

        unsafe { std::slice::from_raw_parts(mmap.as_ptr().add(key_offset), key_len) }
    }

    /// Get value bytes for an entry
    pub fn get_value_bytes(&self, entry: &ZEntry) -> &[u8] {
        let mmap = self.mmap.read();
        let value_offset = entry.value_offset as usize;
        let value_len = entry.value_length as usize;

        unsafe { std::slice::from_raw_parts(mmap.as_ptr().add(value_offset), value_len) }
    }

    /// Flush changes to disk
    pub fn flush(&self) -> Result<(), Error> {
        let mmap = self.mmap.read();
        mmap.flush()?;
        Ok(())
    }
}

/// Segment directory for managing multiple segments
pub struct SegmentDirectory {
    /// Base path for segments
    base_path: PathBuf,

    /// Active segments by ID
    segments: RwLock<HashMap<u64, Arc<MmapSegment>>>,

    /// Z-value ranges to segment mappings
    z_ranges: RwLock<Vec<(ZValueRange, u64)>>,

    /// Next segment ID
    next_segment_id: AtomicU64,

    /// Segment size
    segment_size: usize,
}

impl SegmentDirectory {
    /// Create a new segment directory
    pub fn new<P: AsRef<Path>>(base_path: P, segment_size: Option<usize>) -> Result<Self, Error> {
        let base_path = base_path.as_ref().to_path_buf();

        // Create directory if it doesn't exist
        fs::create_dir_all(&base_path)?;

        let segment_size = segment_size.unwrap_or(DEFAULT_SEGMENT_SIZE);

        let mut directory = Self {
            base_path,
            segments: RwLock::new(HashMap::new()),
            z_ranges: RwLock::new(Vec::new()),
            next_segment_id: AtomicU64::new(1),
            segment_size,
        };

        // Load existing segments if any
        directory.load_existing_segments()?;

        Ok(directory)
    }

    /// Load existing segments
    fn load_existing_segments(&mut self) -> Result<(), Error> {
        let segment_paths = fs::read_dir(&self.base_path)?;
        let mut max_id = 0;

        for entry in segment_paths {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() && path.extension().map_or(false, |ext| ext == "segment") {
                match MmapSegment::open(&path) {
                    Ok(segment) => {
                        let id = segment.id();
                        let z_range = segment.z_range().clone();

                        max_id = max_id.max(id);

                        let segment = Arc::new(segment);
                        self.segments.write().insert(id, segment.clone());
                        self.z_ranges.write().push((z_range, id));
                    }
                    Err(e) => {
                        // Log error but continue loading other segments
                        eprintln!("Failed to load segment {:?}: {}", path, e);
                    }
                }
            }
        }

        // Update next segment ID
        if max_id > 0 {
            self.next_segment_id.store(max_id + 1, Ordering::SeqCst);
        }

        Ok(())
    }

    /// Create a new segment for a Z-value range
    pub fn create_segment(&self, z_min: u64, z_max: u64) -> Result<Arc<MmapSegment>, Error> {
        let id = self.next_segment_id.fetch_add(1, Ordering::SeqCst);
        let path = self.segment_path(id);

        let segment = MmapSegment::create(path, id, z_min, z_max, self.segment_size)?;

        let z_range = segment.z_range().clone();
        let segment = Arc::new(segment);

        self.segments.write().insert(id, segment.clone());
        self.z_ranges.write().push((z_range, id));

        Ok(segment)
    }

    /// Get segment path
    fn segment_path(&self, id: u64) -> PathBuf {
        self.base_path.join(format!("segment_{:016x}.segment", id))
    }

    /// Find segment containing a Z-value
    pub fn find_segment_for_z(&self, z_value: u64) -> Option<Arc<MmapSegment>> {
        let z_ranges = self.z_ranges.read();

        for (range, id) in z_ranges.iter() {
            if z_value >= range.min && z_value <= range.max {
                if let Some(segment) = self.segments.read().get(id) {
                    return Some(segment.clone());
                }
            }
        }

        None
    }

    /// Get all segments
    pub fn all_segments(&self) -> Vec<Arc<MmapSegment>> {
        self.segments.read().values().cloned().collect()
    }

    /// Find segments in a Z-value range
    pub fn find_segments_in_range(&self, z_min: u64, z_max: u64) -> Vec<Arc<MmapSegment>> {
        let z_ranges = self.z_ranges.read();
        let mut result = Vec::new();

        for (range, id) in z_ranges.iter() {
            // Check if ranges overlap
            if !(range.max < z_min || range.min > z_max) {
                if let Some(segment) = self.segments.read().get(id) {
                    result.push(segment.clone());
                }
            }
        }

        result
    }

    /// Close all segments
    pub fn close(&self) -> Result<(), Error> {
        for segment in self.segments.read().values() {
            segment.flush()?;
        }

        Ok(())
    }
}

/// Memory-mapped storage manager
pub struct MmapStorageManager {
    /// Segment directory
    segment_directory: SegmentDirectory,

    /// Configuration
    segment_size: usize,
    max_entries_per_segment: usize,
}

impl MmapStorageManager {
    /// Create a new storage manager
    pub fn new<P: AsRef<Path>>(
        base_path: P,
        segment_size: Option<usize>,
        max_entries_per_segment: Option<usize>,
    ) -> Result<Self, Error> {
        let segment_directory = SegmentDirectory::new(base_path, segment_size)?;

        Ok(Self {
            segment_directory,
            segment_size: segment_size.unwrap_or(DEFAULT_SEGMENT_SIZE),
            max_entries_per_segment: max_entries_per_segment.unwrap_or(MAX_ENTRIES_PER_SEGMENT),
        })
    }

    /// Append an entry to the appropriate segment
    pub fn append_entry(
        &self,
        entry: ZEntry,
        key_data: &[u8],
        value_data: &[u8],
    ) -> Result<(), Error> {
        let z_value = entry.z_value;

        // Find or create segment for this Z-value
        let segment = match self.segment_directory.find_segment_for_z(z_value) {
            Some(segment) => {
                // Check if segment is full
                if segment.entry_count() >= self.max_entries_per_segment as u64 {
                    // Create a new segment with a narrower range
                    let mid = (segment.z_range().min + segment.z_range().max) / 2;

                    if z_value <= mid {
                        self.segment_directory
                            .create_segment(segment.z_range().min, mid)?
                    } else {
                        self.segment_directory
                            .create_segment(mid + 1, segment.z_range().max)?
                    }
                } else {
                    segment
                }
            }
            None => {
                // Create a new segment
                // For simplicity, we'll create a segment that covers the entire range initially
                self.segment_directory.create_segment(0, u64::MAX)?
            }
        };

        // Append entry to segment
        segment.append_entry(&entry, key_data, value_data)?;

        Ok(())
    }

    /// Find entries by Z-value
    pub fn find_entries_by_z(&self, z_value: u64) -> Result<Vec<ZEntry>, Error> {
        let mut result = Vec::new();

        if let Some(segment) = self.segment_directory.find_segment_for_z(z_value) {
            let entries = segment.iter_entries()?;
            for entry in entries {
                if entry.z_value == z_value {
                    result.push(entry);
                }
            }
        }

        Ok(result)
    }

    /// Find entries in a Z-value range
    pub fn find_entries_in_range(&self, z_min: u64, z_max: u64) -> Result<Vec<ZEntry>, Error> {
        let mut result = Vec::new();

        for segment in self.segment_directory.find_segments_in_range(z_min, z_max) {
            let entries = segment.iter_entries()?;
            for entry in entries {
                if entry.z_value >= z_min && entry.z_value <= z_max {
                    result.push(entry);
                }
            }
        }

        Ok(result)
    }

    /// Get key bytes for an entry
    pub fn get_key_bytes(&self, entry: &ZEntry) -> Result<Vec<u8>, Error> {
        let z_value = entry.z_value;

        if let Some(segment) = self.segment_directory.find_segment_for_z(z_value) {
            // This is not ideal for zero-copy, but for simplicity we're copying here
            // In a real implementation, we'd use reference-counting and lifetimes
            Ok(segment.get_key_bytes(entry).to_vec())
        } else {
            Err(Error::KeyNotFound)
        }
    }

    /// Get value bytes for an entry
    pub fn get_value_bytes(&self, entry: &ZEntry) -> Result<Vec<u8>, Error> {
        let z_value = entry.z_value;

        if let Some(segment) = self.segment_directory.find_segment_for_z(z_value) {
            // This is not ideal for zero-copy, but for simplicity we're copying here
            Ok(segment.get_value_bytes(entry).to_vec())
        } else {
            Err(Error::KeyNotFound)
        }
    }

    /// Flush all segments to disk
    pub fn flush(&self) -> Result<(), Error> {
        self.segment_directory.close()
    }

    /// Close the storage manager
    pub fn close(&self) -> Result<(), Error> {
        self.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_segment_create_open() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_segment.segment");

        // Create segment
        let segment = MmapSegment::create(&path, 1, 0, 1000, 1024 * 1024).unwrap();
        assert_eq!(segment.id(), 1);
        assert_eq!(segment.z_range().min, 0);
        assert_eq!(segment.z_range().max, 1000);
        assert_eq!(segment.entry_count(), 0);

        // Flush and drop the segment
        segment.flush().unwrap();
        drop(segment);

        // Re-open segment
        let segment = MmapSegment::open(&path).unwrap();
        assert_eq!(segment.id(), 1);
        assert_eq!(segment.z_range().min, 0);
        assert_eq!(segment.z_range().max, 1000);
        assert_eq!(segment.entry_count(), 0);
    }

    #[test]
    fn test_segment_directory() {
        let dir = tempdir().unwrap();

        // Create directory
        let directory = SegmentDirectory::new(dir.path(), Some(1024 * 1024)).unwrap();

        // Create segments
        let segment1 = directory.create_segment(0, 1000).unwrap();
        let segment2 = directory.create_segment(1001, 2000).unwrap();

        // Find segments
        let found = directory.find_segment_for_z(500);
        assert!(found.is_some());
        assert_eq!(found.unwrap().id(), segment1.id());

        let found = directory.find_segment_for_z(1500);
        assert!(found.is_some());
        assert_eq!(found.unwrap().id(), segment2.id());

        // Find segments in range
        let segments = directory.find_segments_in_range(900, 1100);
        assert_eq!(segments.len(), 2);

        // Close directory
        directory.close().unwrap();
    }
}
