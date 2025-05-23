//! Extent management for IMEC storage engine
//!
//! Extents are contiguous collections of pages that serve as the fundamental
//! storage unit in IMEC. They support Copy-on-Write semantics for MVCC and
//! are memory-mapped for efficient access.

use crate::storage::page::{Page, PageType, PAGE_SIZE};
use std::collections::HashMap;
use std::mem;

/// Default extent size (16MB)
pub const DEFAULT_EXTENT_SIZE: usize = 16 * 1024 * 1024;

/// Minimum extent size (1MB)
pub const MIN_EXTENT_SIZE: usize = 1024 * 1024;

/// Maximum extent size (64MB)
pub const MAX_EXTENT_SIZE: usize = 64 * 1024 * 1024;

/// Extent header size
pub const EXTENT_HEADER_SIZE: usize = mem::size_of::<ExtentHeader>();

/// Magic number for extent validation
pub const EXTENT_MAGIC: u32 = 0xBEEFCAFE;

/// Extent header containing metadata about the extent
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct ExtentHeader {
    /// Magic number for validation
    pub magic: u32,
    /// Unique extent identifier
    pub extent_id: u32,
    /// Total size of the extent in bytes
    pub extent_size: u32,
    /// Number of pages in this extent
    pub page_count: u32,
    /// Number of pages currently allocated
    pub allocated_pages: u32,
    /// Size of each page in bytes
    pub page_size: u32,
    /// Morton-T range start for this extent
    pub morton_range_start: u64,
    /// Morton-T range end for this extent
    pub morton_range_end: u64,
    /// CoW generation for MVCC
    pub generation: u64,
    /// Creation timestamp
    pub created_at: u64,
    /// Last modification timestamp
    pub modified_at: u64,
    /// Checksum for integrity
    pub checksum: u32,
    /// Reserved space for future use
    pub reserved: [u8; 32],
}

impl ExtentHeader {
    pub fn new(extent_id: u32, extent_size: usize) -> Self {
        let page_count = if extent_size > EXTENT_HEADER_SIZE {
            (extent_size - EXTENT_HEADER_SIZE) / PAGE_SIZE
        } else {
            0
        };
        
        Self {
            magic: EXTENT_MAGIC,
            extent_id,
            extent_size: extent_size as u32,
            page_count: page_count as u32,
            allocated_pages: 0,
            page_size: PAGE_SIZE as u32,
            morton_range_start: u64::MAX,
            morton_range_end: 0,
            generation: 0,
            created_at: 0, // Should be set by caller
            modified_at: 0,
            checksum: 0,
            reserved: [0; 32],
        }
    }

    pub fn is_valid(&self) -> bool {
        self.magic == EXTENT_MAGIC &&
        self.extent_size >= MIN_EXTENT_SIZE as u32 &&
        self.extent_size <= MAX_EXTENT_SIZE as u32 &&
        self.page_size == PAGE_SIZE as u32 &&
        self.allocated_pages <= self.page_count
    }

    pub fn pages_available(&self) -> u32 {
        self.page_count - self.allocated_pages
    }

    pub fn usage_ratio(&self) -> f64 {
        if self.page_count == 0 {
            0.0
        } else {
            self.allocated_pages as f64 / self.page_count as f64
        }
    }
}

/// Extent managing a collection of pages
#[derive(Debug)]
pub struct Extent {
    /// Raw extent data (header + pages)
    data: Vec<u8>,
    /// Map of page index to allocation status
    page_allocation: HashMap<u32, bool>,
    /// Free page list for efficient allocation
    free_pages: Vec<u32>,
}

impl Extent {
    /// Create a new extent with specified size
    pub fn new(extent_id: u32, extent_size: usize) -> Result<Self, ExtentError> {
        if extent_size < MIN_EXTENT_SIZE || extent_size > MAX_EXTENT_SIZE {
            return Err(ExtentError::InvalidSize);
        }

        let mut data = vec![0u8; extent_size];
        let header = ExtentHeader::new(extent_id, extent_size);
        
        // Write header
        unsafe {
            let header_ptr = data.as_mut_ptr() as *mut ExtentHeader;
            *header_ptr = header;
        }

        let page_count = header.page_count;
        let mut free_pages = Vec::with_capacity(page_count as usize);
        for i in (0..page_count).rev() {
            free_pages.push(i);
        }

        Ok(Self {
            data,
            page_allocation: HashMap::new(),
            free_pages,
        })
    }

    /// Get extent header
    pub fn header(&self) -> &ExtentHeader {
        unsafe {
            &*(self.data.as_ptr() as *const ExtentHeader)
        }
    }

    /// Get mutable extent header
    pub fn header_mut(&mut self) -> &mut ExtentHeader {
        unsafe {
            &mut *(self.data.as_mut_ptr() as *mut ExtentHeader)
        }
    }

    /// Allocate a new page within the extent
    pub fn allocate_page(&mut self, page_type: PageType) -> Result<u32, ExtentError> {
        if self.free_pages.is_empty() {
            return Err(ExtentError::ExtentFull);
        }

        let page_index = self.free_pages.pop().unwrap();
        self.page_allocation.insert(page_index, true);

        // Initialize the page
        let page_offset = self.page_offset(page_index);
        let page_data = &mut self.data[page_offset..page_offset + PAGE_SIZE];
        let page = Page::new(page_type);
        page_data.copy_from_slice(page.as_bytes());

        // Update header
        let header = self.header_mut();
        header.allocated_pages += 1;
        header.modified_at = current_timestamp();

        Ok(page_index)
    }

    /// Get page at specified index
    pub fn get_page(&self, page_index: u32) -> Result<&Page, ExtentError> {
        if !self.is_page_allocated(page_index) {
            return Err(ExtentError::PageNotAllocated);
        }

        let page_offset = self.page_offset(page_index);
        if page_offset + PAGE_SIZE > self.data.len() {
            return Err(ExtentError::InvalidPageIndex);
        }

        unsafe {
            let page_ptr = self.data.as_ptr().add(page_offset) as *const Page;
            Ok(&*page_ptr)
        }
    }

    /// Get mutable page at specified index
    pub fn get_page_mut(&mut self, page_index: u32) -> Result<&mut Page, ExtentError> {
        if !self.is_page_allocated(page_index) {
            return Err(ExtentError::PageNotAllocated);
        }

        let page_offset = self.page_offset(page_index);
        if page_offset + PAGE_SIZE > self.data.len() {
            return Err(ExtentError::InvalidPageIndex);
        }

        // Update modification time
        self.header_mut().modified_at = current_timestamp();

        unsafe {
            let page_ptr = self.data.as_mut_ptr().add(page_offset) as *mut Page;
            Ok(&mut *page_ptr)
        }
    }

    /// Find page containing the Morton-T code
    pub fn find_page_for_morton(&self, morton_code: u64) -> Option<u32> {
        let header = self.header();
        
        // Check if Morton code is within this extent's range
        if morton_code < header.morton_range_start || morton_code > header.morton_range_end {
            return None;
        }

        // Search allocated pages for one containing this Morton code
        for (page_index, _) in &self.page_allocation {
            if let Ok(page) = self.get_page(*page_index) {
                let page_header = page.header();
                if morton_code >= page_header.morton_range_start && 
                   morton_code <= page_header.morton_range_end {
                    return Some(*page_index);
                }
            }
        }

        None
    }

    /// Deallocate a page
    pub fn deallocate_page(&mut self, page_index: u32) -> Result<(), ExtentError> {
        if !self.is_page_allocated(page_index) {
            return Err(ExtentError::PageNotAllocated);
        }

        self.page_allocation.remove(&page_index);
        self.free_pages.push(page_index);

        // Update header
        let header = self.header_mut();
        header.allocated_pages -= 1;
        header.modified_at = current_timestamp();

        Ok(())
    }

    /// Check if page is allocated
    pub fn is_page_allocated(&self, page_index: u32) -> bool {
        self.page_allocation.contains_key(&page_index)
    }

    /// Get page offset within extent data
    fn page_offset(&self, page_index: u32) -> usize {
        EXTENT_HEADER_SIZE + (page_index as usize * PAGE_SIZE)
    }

    /// Update Morton range for this extent
    pub fn update_morton_range(&mut self, morton_start: u64, morton_end: u64) {
        let header = self.header_mut();
        header.morton_range_start = header.morton_range_start.min(morton_start);
        header.morton_range_end = header.morton_range_end.max(morton_end);
        header.modified_at = current_timestamp();
    }

    /// Get all allocated page indices
    pub fn allocated_pages(&self) -> Vec<u32> {
        self.page_allocation.keys().cloned().collect()
    }

    /// Get extent statistics
    pub fn stats(&self) -> ExtentStats {
        let header = self.header();
        ExtentStats {
            extent_id: header.extent_id,
            total_pages: header.page_count,
            allocated_pages: header.allocated_pages,
            free_pages: header.pages_available(),
            usage_ratio: header.usage_ratio(),
            morton_range_start: header.morton_range_start,
            morton_range_end: header.morton_range_end,
            generation: header.generation,
        }
    }

    /// Create a copy-on-write copy of this extent
    pub fn cow_copy(&self, new_extent_id: u32) -> Result<Self, ExtentError> {
        let header = self.header();
        let mut new_extent = Self::new(new_extent_id, header.extent_size as usize)?;
        
        // Copy all data
        new_extent.data.copy_from_slice(&self.data);
        new_extent.page_allocation = self.page_allocation.clone();
        new_extent.free_pages = self.free_pages.clone();

        // Update header for new extent
        let new_header = new_extent.header_mut();
        new_header.extent_id = new_extent_id;
        new_header.generation = header.generation + 1;
        new_header.created_at = current_timestamp();
        new_header.modified_at = current_timestamp();

        Ok(new_extent)
    }

    /// Validate extent integrity
    pub fn validate(&self) -> Result<(), ExtentError> {
        let header = self.header();
        
        if !header.is_valid() {
            return Err(ExtentError::CorruptedExtent);
        }

        // Validate allocated pages
        for (page_index, _) in &self.page_allocation {
            if *page_index >= header.page_count {
                return Err(ExtentError::InvalidPageIndex);
            }

            if let Ok(page) = self.get_page(*page_index) {
                page.validate().map_err(|_| ExtentError::CorruptedExtent)?;
            }
        }

        Ok(())
    }

    /// Get raw extent data
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Get mutable raw extent data
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

/// Statistics about an extent
#[derive(Debug, Clone)]
pub struct ExtentStats {
    pub extent_id: u32,
    pub total_pages: u32,
    pub allocated_pages: u32,
    pub free_pages: u32,
    pub usage_ratio: f64,
    pub morton_range_start: u64,
    pub morton_range_end: u64,
    pub generation: u64,
}

/// Errors that can occur during extent operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExtentError {
    InvalidSize,
    ExtentFull,
    PageNotAllocated,
    InvalidPageIndex,
    CorruptedExtent,
    AllocationFailed,
}

impl std::fmt::Display for ExtentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExtentError::InvalidSize => write!(f, "Invalid extent size"),
            ExtentError::ExtentFull => write!(f, "Extent is full"),
            ExtentError::PageNotAllocated => write!(f, "Page is not allocated"),
            ExtentError::InvalidPageIndex => write!(f, "Invalid page index"),
            ExtentError::CorruptedExtent => write!(f, "Extent is corrupted"),
            ExtentError::AllocationFailed => write!(f, "Page allocation failed"),
        }
    }
}

impl std::error::Error for ExtentError {}

/// Get current timestamp (placeholder implementation)
fn current_timestamp() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extent_creation() {
        let extent = Extent::new(1, DEFAULT_EXTENT_SIZE).unwrap();
        let header = extent.header();
        
        assert_eq!(header.magic, EXTENT_MAGIC);
        assert_eq!(header.extent_id, 1);
        assert_eq!(header.extent_size, DEFAULT_EXTENT_SIZE as u32);
        assert!(header.is_valid());
    }

    #[test]
    fn test_page_allocation() {
        let mut extent = Extent::new(1, DEFAULT_EXTENT_SIZE).unwrap();
        
        let page_index = extent.allocate_page(PageType::Leaf).unwrap();
        assert_eq!(page_index, 0);
        assert!(extent.is_page_allocated(page_index));
        assert_eq!(extent.header().allocated_pages, 1);
    }

    #[test]
    fn test_page_access() {
        let mut extent = Extent::new(1, DEFAULT_EXTENT_SIZE).unwrap();
        let page_index = extent.allocate_page(PageType::Internal).unwrap();
        
        let page = extent.get_page(page_index).unwrap();
        assert_eq!(page.header().page_type(), PageType::Internal);
        
        // Test mutable access
        let page_mut = extent.get_page_mut(page_index).unwrap();
        assert_eq!(page_mut.header().page_type(), PageType::Internal);
    }

    #[test]
    fn test_page_deallocation() {
        let mut extent = Extent::new(1, DEFAULT_EXTENT_SIZE).unwrap();
        let page_index = extent.allocate_page(PageType::Leaf).unwrap();
        
        assert!(extent.is_page_allocated(page_index));
        extent.deallocate_page(page_index).unwrap();
        assert!(!extent.is_page_allocated(page_index));
        assert_eq!(extent.header().allocated_pages, 0);
    }

    #[test]
    fn test_morton_range_update() {
        let mut extent = Extent::new(1, DEFAULT_EXTENT_SIZE).unwrap();
        
        extent.update_morton_range(1000, 2000);
        let header = extent.header();
        assert_eq!(header.morton_range_start, 1000);
        assert_eq!(header.morton_range_end, 2000);
        
        extent.update_morton_range(500, 1500);
        let header = extent.header();
        assert_eq!(header.morton_range_start, 500);
        assert_eq!(header.morton_range_end, 2000);
    }

    #[test]
    fn test_cow_copy() {
        let mut extent = Extent::new(1, DEFAULT_EXTENT_SIZE).unwrap();
        extent.allocate_page(PageType::Leaf).unwrap();
        extent.update_morton_range(100, 200);
        
        let copied_extent = extent.cow_copy(2).unwrap();
        let copied_header = copied_extent.header();
        
        assert_eq!(copied_header.extent_id, 2);
        assert_eq!(copied_header.generation, extent.header().generation + 1);
        assert_eq!(copied_header.allocated_pages, extent.header().allocated_pages);
        assert_eq!(copied_header.morton_range_start, extent.header().morton_range_start);
    }

    #[test]
    fn test_extent_full() {
        let mut extent = Extent::new(1, MIN_EXTENT_SIZE).unwrap();
        let header = extent.header();
        let page_count = header.page_count;
        
        // Allocate all pages
        for _ in 0..page_count {
            extent.allocate_page(PageType::Leaf).unwrap();
        }
        
        // Try to allocate one more - should fail
        assert_eq!(extent.allocate_page(PageType::Leaf), Err(ExtentError::ExtentFull));
    }

    #[test]
    fn test_extent_validation() {
        let extent = Extent::new(1, DEFAULT_EXTENT_SIZE).unwrap();
        assert!(extent.validate().is_ok());
    }

    #[test]
    fn test_extent_stats() {
        let mut extent = Extent::new(1, DEFAULT_EXTENT_SIZE).unwrap();
        extent.allocate_page(PageType::Leaf).unwrap();
        extent.allocate_page(PageType::Internal).unwrap();
        
        let stats = extent.stats();
        assert_eq!(stats.extent_id, 1);
        assert_eq!(stats.allocated_pages, 2);
        assert!(stats.usage_ratio > 0.0);
    }

    #[test]
    fn test_invalid_extent_size() {
        assert!(matches!(Extent::new(1, 100), Err(ExtentError::InvalidSize)));
        assert!(matches!(Extent::new(1, MAX_EXTENT_SIZE + 1), Err(ExtentError::InvalidSize)));
    }
}