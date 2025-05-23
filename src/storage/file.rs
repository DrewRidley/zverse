//! File-based storage layer with mmap support for IMEC
//!
//! This module implements the disk-based storage layer that uses memory-mapped files
//! for efficient I/O operations. It provides the foundation for persistent storage
//! with copy-on-write semantics and MVCC support.

use crate::storage::extent::{ExtentError, ExtentHeader, EXTENT_HEADER_SIZE};
use crate::storage::page::{Page, PageType, PAGE_SIZE};
use crate::interval::IntervalTable;

use memmap2::{MmapMut, MmapOptions};
use std::fs::{File, OpenOptions};
use std::io::{self, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::collections::HashMap;

/// Size of the superblock (4KB)
pub const SUPERBLOCK_SIZE: usize = 4096;

/// Magic number for IMEC files
pub const IMEC_MAGIC: u64 = 0x494D4543_76320001; // "IMECv2\0\1"

/// Default file size (1GB)
pub const DEFAULT_FILE_SIZE: u64 = 1024 * 1024 * 1024;

/// Minimum file size (16MB)
pub const MIN_FILE_SIZE: u64 = 16 * 1024 * 1024;

/// Maximum file size (1TB)
pub const MAX_FILE_SIZE: u64 = 1024 * 1024 * 1024 * 1024;

/// File superblock containing metadata
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct Superblock {
    /// Magic number for file format identification
    pub magic: u64,
    /// Version of the file format
    pub version: u32,
    /// Reserved for alignment
    pub reserved: u32,
    /// Pointer to interval table root
    pub root_ptr: u64,
    /// Head of freelist (LIFO stack of free extents)
    pub freelist_head: u64,
    /// Monotonic transaction ID
    pub txid: u64,
    /// Geometry configuration (extent/page sizes)
    pub geometry: u32,
    /// Padding for alignment
    pub padding: u32,
    /// File size in bytes
    pub file_size: u64,
    /// Number of allocated extents
    pub extent_count: u64,
    /// CRC64 checksum for crash consistency
    pub checksum: u64,
    /// Reserved space for future use
    pub reserved_space: [u8; SUPERBLOCK_SIZE - 88],
}

impl Superblock {
    /// Create a new superblock
    pub fn new(file_size: u64) -> Self {
        Self {
            magic: IMEC_MAGIC,
            version: 1,
            reserved: 0,
            root_ptr: 0,
            freelist_head: 0,
            txid: 0,
            geometry: (PAGE_SIZE as u32) | (16 << 16), // 4KB pages, 16MB extents (stored as 16 * 1MB units)
            padding: 0,
            file_size,
            extent_count: 0,
            checksum: 0,
            reserved_space: [0; SUPERBLOCK_SIZE - 88],
        }
    }

    /// Validate the superblock
    pub fn is_valid(&self) -> bool {
        self.magic == IMEC_MAGIC && 
        self.version == 1 &&
        self.file_size >= MIN_FILE_SIZE &&
        self.file_size <= MAX_FILE_SIZE
    }

    /// Get page size from geometry
    pub fn page_size(&self) -> u32 {
        self.geometry & 0xFFFF
    }

    /// Get extent size from geometry
    pub fn extent_size(&self) -> u32 {
        (self.geometry >> 16) * 1024 * 1024
    }

    /// Update checksum
    pub fn update_checksum(&mut self) {
        self.checksum = 0;
        let bytes = self.to_bytes();
        self.checksum = Self::calculate_crc64(&bytes);
    }

    /// Verify checksum
    pub fn verify_checksum(&self) -> bool {
        let stored_checksum = self.checksum;
        let mut temp = self.clone();
        temp.checksum = 0;
        let bytes = temp.to_bytes();
        stored_checksum == Self::calculate_crc64(&bytes)
    }

    /// Simple CRC64 implementation
    fn calculate_crc64(data: &[u8]) -> u64 {
        let mut crc = 0u64;
        for &byte in data {
            crc = crc.wrapping_mul(31).wrapping_add(byte as u64);
        }
        crc
    }

    /// Serialize to bytes
    pub fn to_bytes(&self) -> [u8; SUPERBLOCK_SIZE] {
        let mut bytes = [0u8; SUPERBLOCK_SIZE];
        bytes[0..8].copy_from_slice(&self.magic.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.version.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.reserved.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.root_ptr.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.freelist_head.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.txid.to_le_bytes());
        bytes[40..44].copy_from_slice(&self.geometry.to_le_bytes());
        bytes[44..48].copy_from_slice(&self.padding.to_le_bytes());
        bytes[48..56].copy_from_slice(&self.file_size.to_le_bytes());
        bytes[56..64].copy_from_slice(&self.extent_count.to_le_bytes());
        bytes[64..72].copy_from_slice(&self.checksum.to_le_bytes());
        bytes[72..72+self.reserved_space.len()].copy_from_slice(&self.reserved_space);
        bytes
    }

    /// Deserialize from bytes
    pub fn from_bytes(bytes: &[u8; SUPERBLOCK_SIZE]) -> Self {
        Self {
            magic: u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5], bytes[6], bytes[7],
            ]),
            version: u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]),
            reserved: u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]),
            root_ptr: u64::from_le_bytes([
                bytes[16], bytes[17], bytes[18], bytes[19],
                bytes[20], bytes[21], bytes[22], bytes[23],
            ]),
            freelist_head: u64::from_le_bytes([
                bytes[24], bytes[25], bytes[26], bytes[27],
                bytes[28], bytes[29], bytes[30], bytes[31],
            ]),
            txid: u64::from_le_bytes([
                bytes[32], bytes[33], bytes[34], bytes[35],
                bytes[36], bytes[37], bytes[38], bytes[39],
            ]),
            geometry: u32::from_le_bytes([bytes[40], bytes[41], bytes[42], bytes[43]]),
            padding: u32::from_le_bytes([bytes[44], bytes[45], bytes[46], bytes[47]]),
            file_size: u64::from_le_bytes([
                bytes[48], bytes[49], bytes[50], bytes[51],
                bytes[52], bytes[53], bytes[54], bytes[55],
            ]),
            extent_count: u64::from_le_bytes([
                bytes[56], bytes[57], bytes[58], bytes[59],
                bytes[60], bytes[61], bytes[62], bytes[63],
            ]),
            checksum: u64::from_le_bytes([
                bytes[64], bytes[65], bytes[66], bytes[67],
                bytes[68], bytes[69], bytes[70], bytes[71],
            ]),
            reserved_space: {
                let mut reserved = [0u8; SUPERBLOCK_SIZE - 88];
                let reserved_len = SUPERBLOCK_SIZE - 88;
                reserved.copy_from_slice(&bytes[72..72+reserved_len]);
                reserved
            },
        }
    }
}

/// Configuration for file-based storage
#[derive(Debug, Clone)]
pub struct FileStorageConfig {
    /// Path to the database file
    pub file_path: PathBuf,
    /// Initial file size
    pub file_size: u64,
    /// Page size (must be power of 2)
    pub page_size: u32,
    /// Extent size (must be multiple of page size)
    pub extent_size: u32,
    /// Whether to create the file if it doesn't exist
    pub create_if_missing: bool,
    /// Whether to truncate existing file
    pub truncate: bool,
}

impl Default for FileStorageConfig {
    fn default() -> Self {
        Self {
            file_path: PathBuf::from("./imec_storage.db"),
            file_size: DEFAULT_FILE_SIZE,
            page_size: PAGE_SIZE as u32,
            extent_size: 16 * 1024 * 1024, // 16MB
            create_if_missing: true,
            truncate: false,
        }
    }
}

/// File-based storage engine using memory-mapped I/O
#[derive(Debug)]
pub struct FileStorage {
    /// Memory-mapped file
    mmap: MmapMut,
    /// File handle
    _file: File,
    /// Superblock
    superblock: Superblock,
    /// Configuration
    config: FileStorageConfig,
    /// Transaction ID counter
    txid_counter: AtomicU64,
    /// Interval table (in-memory cache)
    interval_table: IntervalTable,
    /// Extent cache (extent_id -> offset)
    extent_cache: HashMap<u32, u64>,
}

impl FileStorage {
    /// Create or open a file-based storage
    pub fn new(config: FileStorageConfig) -> io::Result<Self> {
        // Validate configuration
        if config.file_size < MIN_FILE_SIZE || config.file_size > MAX_FILE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Invalid file size: {} (must be between {} and {})", 
                       config.file_size, MIN_FILE_SIZE, MAX_FILE_SIZE)
            ));
        }

        // Avoid /tmp directory on macOS as requested
        if config.file_path.starts_with("/tmp") {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Cannot use /tmp directory - use a persistent location instead"
            ));
        }

        // Create parent directories if needed
        if let Some(parent) = config.file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Open or create file
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(config.create_if_missing)
            .truncate(config.truncate)
            .open(&config.file_path)?;

        // Check if file exists and has content
        let file_exists = file.metadata()?.len() > 0;
        
        let superblock = if file_exists && !config.truncate {
            // Read existing superblock
            let mut superblock_bytes = [0u8; SUPERBLOCK_SIZE];
            use std::io::Read;
            file.read_exact(&mut superblock_bytes)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, 
                    format!("Failed to read superblock: {}", e)))?;
            
            let superblock = Superblock::from_bytes(&superblock_bytes);
            if !superblock.is_valid() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Invalid or corrupted superblock"
                ));
            }
            
            if !superblock.verify_checksum() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Superblock checksum mismatch - file may be corrupted"
                ));
            }
            
            superblock
        } else {
            // Create new superblock
            let mut superblock = Superblock::new(config.file_size);
            superblock.update_checksum();
            
            // Resize file to desired size
            file.seek(SeekFrom::Start(config.file_size - 1))?;
            file.write_all(&[0])?;
            file.seek(SeekFrom::Start(0))?;
            
            // Write superblock
            file.write_all(&superblock.to_bytes())?;
            file.flush()?;
            
            superblock
        };

        // Memory map the file
        let mmap = unsafe {
            MmapOptions::new()
                .len(config.file_size as usize)
                .map_mut(&file)?
        };

        Ok(Self {
            mmap,
            _file: file,
            txid_counter: AtomicU64::new(superblock.txid),
            superblock,
            interval_table: IntervalTable::new(),
            extent_cache: HashMap::new(),
            config,
        })
    }

    /// Get the superblock
    pub fn superblock(&self) -> &Superblock {
        &self.superblock
    }

    /// Allocate a new extent
    pub fn allocate_extent(&mut self, extent_id: u32) -> io::Result<u64> {
        // For now, simple allocation strategy: place extents sequentially after superblock
        let extent_size = self.superblock.extent_size() as u64;
        let offset = SUPERBLOCK_SIZE as u64 + (self.superblock.extent_count * extent_size);
        
        if offset + extent_size > self.config.file_size {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "File is full - cannot allocate new extent"
            ));
        }

        // Initialize extent header
        let extent_header = ExtentHeader::new(extent_id, extent_size as usize);
        let extent_bytes = unsafe {
            std::slice::from_raw_parts(
                &extent_header as *const _ as *const u8,
                EXTENT_HEADER_SIZE
            )
        };

        // Write extent header to mmap
        let mmap_slice = &mut self.mmap[offset as usize..(offset + EXTENT_HEADER_SIZE as u64) as usize];
        mmap_slice.copy_from_slice(extent_bytes);

        // Update superblock
        self.superblock.extent_count += 1;
        self.update_superblock()?;

        // Cache the extent location
        self.extent_cache.insert(extent_id, offset);

        Ok(offset)
    }

    /// Get extent at offset
    pub fn get_extent(&self, extent_id: u32) -> io::Result<Option<&[u8]>> {
        if let Some(&offset) = self.extent_cache.get(&extent_id) {
            let extent_size = self.superblock.extent_size() as usize;
            let extent_slice = &self.mmap[offset as usize..(offset as usize + extent_size)];
            Ok(Some(extent_slice))
        } else {
            Ok(None)
        }
    }

    /// Get mutable extent at offset
    pub fn get_extent_mut(&mut self, extent_id: u32) -> io::Result<Option<&mut [u8]>> {
        if let Some(&offset) = self.extent_cache.get(&extent_id) {
            let extent_size = self.superblock.extent_size() as usize;
            let extent_slice = &mut self.mmap[offset as usize..(offset as usize + extent_size)];
            Ok(Some(extent_slice))
        } else {
            Ok(None)
        }
    }

    /// Write page to extent
    pub fn write_page(&mut self, extent_id: u32, page_index: u32, page: &Page) -> io::Result<()> {
        if let Some(&extent_offset) = self.extent_cache.get(&extent_id) {
            let page_size = self.superblock.page_size() as usize;
            let page_offset = extent_offset + EXTENT_HEADER_SIZE as u64 + (page_index as u64 * page_size as u64);
            
            if page_offset + page_size as u64 > self.config.file_size {
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "Page write would exceed file size"
                ));
            }

            let page_bytes = page.as_bytes();
            let mmap_slice = &mut self.mmap[page_offset as usize..(page_offset as usize + page_size)];
            mmap_slice.copy_from_slice(page_bytes);

            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Extent {} not found", extent_id)
            ))
        }
    }

    /// Read page from extent
    pub fn read_page(&self, extent_id: u32, page_index: u32) -> io::Result<Option<Page>> {
        if let Some(&extent_offset) = self.extent_cache.get(&extent_id) {
            let page_size = self.superblock.page_size() as usize;
            let page_offset = extent_offset + EXTENT_HEADER_SIZE as u64 + (page_index as u64 * page_size as u64);
            
            if page_offset + page_size as u64 > self.config.file_size {
                return Ok(None);
            }

            let page_slice = &self.mmap[page_offset as usize..(page_offset as usize + page_size)];
            let mut page_data = [0u8; PAGE_SIZE];
            page_data.copy_from_slice(page_slice);
            
            let page = Page::from_bytes(page_data);
            Ok(Some(page))
        } else {
            Ok(None)
        }
    }

    /// Flush changes to disk
    pub fn flush(&mut self) -> io::Result<()> {
        self.mmap.flush()?;
        Ok(())
    }

    /// Update superblock in memory and on disk
    fn update_superblock(&mut self) -> io::Result<()> {
        self.superblock.txid = self.txid_counter.fetch_add(1, Ordering::SeqCst);
        self.superblock.update_checksum();
        
        let superblock_bytes = self.superblock.to_bytes();
        self.mmap[0..SUPERBLOCK_SIZE].copy_from_slice(&superblock_bytes);
        
        Ok(())
    }

    /// Get file statistics
    pub fn stats(&self) -> FileStorageStats {
        FileStorageStats {
            file_size: self.config.file_size,
            extent_count: self.superblock.extent_count,
            page_size: self.superblock.page_size(),
            extent_size: self.superblock.extent_size(),
            txid: self.superblock.txid,
            utilization: (self.superblock.extent_count * self.superblock.extent_size() as u64) as f64 
                        / self.config.file_size as f64,
        }
    }

    /// Get next transaction ID
    pub fn next_txid(&self) -> u64 {
        self.txid_counter.load(Ordering::SeqCst)
    }

    /// Close and sync the storage
    pub fn close(mut self) -> io::Result<()> {
        self.flush()?;
        drop(self.mmap);
        Ok(())
    }
}

/// Statistics for file storage
#[derive(Debug, Clone)]
pub struct FileStorageStats {
    pub file_size: u64,
    pub extent_count: u64,
    pub page_size: u32,
    pub extent_size: u32,
    pub txid: u64,
    pub utilization: f64,
}



/// File storage errors
#[derive(Debug)]
pub enum FileStorageError {
    IoError(io::Error),
    ExtentError(ExtentError),
    CorruptedFile,
    FileFull,
    InvalidConfiguration,
}

impl From<io::Error> for FileStorageError {
    fn from(err: io::Error) -> Self {
        FileStorageError::IoError(err)
    }
}

impl From<ExtentError> for FileStorageError {
    fn from(err: ExtentError) -> Self {
        FileStorageError::ExtentError(err)
    }
}

impl std::fmt::Display for FileStorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileStorageError::IoError(e) => write!(f, "I/O error: {}", e),
            FileStorageError::ExtentError(e) => write!(f, "Extent error: {}", e),
            FileStorageError::CorruptedFile => write!(f, "File is corrupted"),
            FileStorageError::FileFull => write!(f, "File is full"),
            FileStorageError::InvalidConfiguration => write!(f, "Invalid configuration"),
        }
    }
}

impl std::error::Error for FileStorageError {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_superblock_creation() {
        let sb = Superblock::new(DEFAULT_FILE_SIZE);
        assert!(sb.is_valid());
        assert_eq!(sb.magic, IMEC_MAGIC);
        assert_eq!(sb.file_size, DEFAULT_FILE_SIZE);
    }

    #[test]
    fn test_superblock_checksum() {
        let mut sb = Superblock::new(DEFAULT_FILE_SIZE);
        sb.update_checksum();
        assert!(sb.verify_checksum());
        
        sb.txid = 12345;
        assert!(!sb.verify_checksum());
        
        sb.update_checksum();
        assert!(sb.verify_checksum());
    }

    #[test]
    fn test_file_storage_creation() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test.db");
        
        let config = FileStorageConfig {
            file_path: file_path.clone(),
            file_size: 64 * 1024 * 1024, // 64MB
            create_if_missing: true,
            ..Default::default()
        };

        let storage = FileStorage::new(config).unwrap();
        assert!(storage.superblock().is_valid());
        assert_eq!(storage.superblock().file_size, 64 * 1024 * 1024);

        // Close and reopen
        storage.close().unwrap();

        let config2 = FileStorageConfig {
            file_path,
            file_size: 64 * 1024 * 1024,
            create_if_missing: false,
            ..Default::default()
        };

        let storage2 = FileStorage::new(config2).unwrap();
        assert!(storage2.superblock().is_valid());
    }

    #[test]
    fn test_extent_allocation() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_extents.db");
        
        let config = FileStorageConfig {
            file_path,
            file_size: 128 * 1024 * 1024, // 128MB
            create_if_missing: true,
            ..Default::default()
        };

        let mut storage = FileStorage::new(config).unwrap();
        
        // Allocate some extents
        let extent1_offset = storage.allocate_extent(1).unwrap();
        let extent2_offset = storage.allocate_extent(2).unwrap();
        
        assert_ne!(extent1_offset, extent2_offset);
        assert!(extent1_offset >= SUPERBLOCK_SIZE as u64);
        assert!(extent2_offset > extent1_offset);
        
        // Check extent data can be accessed
        assert!(storage.get_extent(1).unwrap().is_some());
        assert!(storage.get_extent(2).unwrap().is_some());
        assert!(storage.get_extent(99).unwrap().is_none());
    }

    #[test]
    fn test_page_read_write() {
        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("test_pages.db");
        
        let config = FileStorageConfig {
            file_path,
            file_size: 64 * 1024 * 1024,
            create_if_missing: true,
            ..Default::default()
        };

        let mut storage = FileStorage::new(config).unwrap();
        
        // Allocate extent
        storage.allocate_extent(1).unwrap();
        
        // Create and write a page
        let page = Page::new(PageType::Leaf);
        storage.write_page(1, 0, &page).unwrap();
        
        // Read it back
        let read_page = storage.read_page(1, 0).unwrap().unwrap();
        assert_eq!(page.header().magic, read_page.header().magic);
        assert_eq!(page.header().page_type, read_page.header().page_type);
    }

    #[test]
    fn test_tmp_directory_rejection() {
        let config = FileStorageConfig {
            file_path: PathBuf::from("/tmp/should_fail.db"),
            ..Default::default()
        };

        let result = FileStorage::new(config);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("/tmp"));
    }
}