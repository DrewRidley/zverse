//! Page structure with hybrid Eytzinger/sequential layout for IMEC storage engine
//!
//! Pages are the fundamental storage unit containing key-value data. They use:
//! - Eytzinger layout for internal pages (cache-optimal tree navigation)
//! - Sequential layout for leaf pages (optimal for range scans)
//! - 32-byte slots with Morton-T fingerprints
//! - Bottom-up blob storage for variable-length keys/values


use std::cmp::Ordering;
use std::mem;

/// Standard page size (4KB) to match OS page size
pub const PAGE_SIZE: usize = 4096;

/// Size of each slot entry (32 bytes)
pub const SLOT_SIZE: usize = 32;

/// Maximum number of slots per page
pub const MAX_SLOTS_PER_PAGE: usize = (PAGE_SIZE - PAGE_HEADER_SIZE) / SLOT_SIZE;

/// Page header size
pub const PAGE_HEADER_SIZE: usize = mem::size_of::<PageHeader>();

/// Magic number for page validation
pub const PAGE_MAGIC: u32 = 0xDEADBEEF;

/// Page type determines slot array layout
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageType {
    /// Internal page - uses Eytzinger layout for cache-optimal navigation
    Internal = 0,
    /// Leaf page - uses sequential layout for range scan efficiency
    Leaf = 1,
}

impl From<u8> for PageType {
    fn from(value: u8) -> Self {
        match value {
            0 => PageType::Internal,
            1 => PageType::Leaf,
            _ => PageType::Leaf, // Default to leaf for safety
        }
    }
}

/// Page header containing metadata and layout information
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct PageHeader {
    /// Magic number for validation
    pub magic: u32,
    /// Page type (Internal or Leaf)
    pub page_type: u8,
    /// Reserved for alignment
    pub reserved: [u8; 3],
    /// Number of active slots
    pub slot_count: u16,
    /// Offset to start of free space
    pub free_space_offset: u16,
    /// Bytes used in blob area
    pub blob_space_used: u16,
    /// Padding for alignment
    pub padding: [u8; 2],
    /// Minimum Morton-T code in this page
    pub morton_range_start: u64,
    /// Maximum Morton-T code in this page
    pub morton_range_end: u64,
    /// CoW generation for MVCC
    pub generation: u64,
    /// Page checksum for integrity
    pub checksum: u32,
    /// Reserved space for future use
    pub reserved2: u32,
}

impl PageHeader {
    pub fn new(page_type: PageType) -> Self {
        Self {
            magic: PAGE_MAGIC,
            page_type: page_type as u8,
            reserved: [0; 3],
            slot_count: 0,
            free_space_offset: PAGE_HEADER_SIZE as u16,
            blob_space_used: 0,
            padding: [0; 2],
            morton_range_start: u64::MAX,
            morton_range_end: u64::MIN,
            generation: 0,
            checksum: 0,
            reserved2: 0,
        }
    }

    pub fn page_type(&self) -> PageType {
        PageType::from(self.page_type)
    }

    pub fn is_valid(&self) -> bool {
        self.magic == PAGE_MAGIC
    }

    /// Calculate available space for new slots and blob data
    pub fn available_space(&self) -> usize {
        let slots_end = PAGE_HEADER_SIZE + (self.slot_count as usize * SLOT_SIZE);
        let blob_start = PAGE_SIZE - self.blob_space_used as usize;
        
        if blob_start <= slots_end {
            0
        } else {
            blob_start - slots_end
        }
    }

    /// Serialize header to bytes
    pub fn to_bytes(&self) -> [u8; PAGE_HEADER_SIZE] {
        let mut bytes = [0u8; PAGE_HEADER_SIZE];
        bytes[0..4].copy_from_slice(&self.magic.to_le_bytes());
        bytes[4] = self.page_type;
        bytes[5..8].copy_from_slice(&self.reserved);
        bytes[8..10].copy_from_slice(&self.slot_count.to_le_bytes());
        bytes[10..12].copy_from_slice(&self.free_space_offset.to_le_bytes());
        bytes[12..14].copy_from_slice(&self.blob_space_used.to_le_bytes());
        bytes[14..16].copy_from_slice(&self.padding);
        bytes[16..24].copy_from_slice(&self.morton_range_start.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.morton_range_end.to_le_bytes());
        bytes[32..40].copy_from_slice(&self.generation.to_le_bytes());
        bytes[40..44].copy_from_slice(&self.checksum.to_le_bytes());
        bytes[44..48].copy_from_slice(&self.reserved2.to_le_bytes());
        bytes
    }

    /// Deserialize header from bytes
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            magic: u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
            page_type: bytes[4],
            reserved: [bytes[5], bytes[6], bytes[7]],
            slot_count: u16::from_le_bytes([bytes[8], bytes[9]]),
            free_space_offset: u16::from_le_bytes([bytes[10], bytes[11]]),
            blob_space_used: u16::from_le_bytes([bytes[12], bytes[13]]),
            padding: [bytes[14], bytes[15]],
            morton_range_start: u64::from_le_bytes([
                bytes[16], bytes[17], bytes[18], bytes[19],
                bytes[20], bytes[21], bytes[22], bytes[23],
            ]),
            morton_range_end: u64::from_le_bytes([
                bytes[24], bytes[25], bytes[26], bytes[27],
                bytes[28], bytes[29], bytes[30], bytes[31],
            ]),
            generation: u64::from_le_bytes([
                bytes[32], bytes[33], bytes[34], bytes[35],
                bytes[36], bytes[37], bytes[38], bytes[39],
            ]),
            checksum: u32::from_le_bytes([bytes[40], bytes[41], bytes[42], bytes[43]]),
            reserved2: u32::from_le_bytes([bytes[44], bytes[45], bytes[46], bytes[47]]),
        }
    }
}

/// Slot entry containing key metadata and pointers
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct Slot {
    /// Morton-T fingerprint or key hash for fast comparison
    pub fingerprint: u64,
    /// Pointer to child page (internal) or value blob (leaf)
    pub child_or_value_ptr: u64,
    /// Offset to key in blob area
    pub key_offset: u32,
    /// Length of key in bytes
    pub key_length: u16,
    /// Length of value in bytes (leaf pages only)
    pub value_length: u16,
    /// Flags: tombstone, overflow, etc.
    pub flags: u8,
    /// Reserved for future use
    pub reserved: [u8; 7],
}

impl Slot {
    pub fn new_internal(fingerprint: u64, child_ptr: u64, key_offset: u32, key_length: u16) -> Self {
        Self {
            fingerprint,
            child_or_value_ptr: child_ptr,
            key_offset,
            key_length,
            value_length: 0,
            flags: 0,
            reserved: [0; 7],
        }
    }

    pub fn new_leaf(fingerprint: u64, key_offset: u32, key_length: u16, value_offset: u32, value_length: u16) -> Self {
        Self {
            fingerprint,
            child_or_value_ptr: value_offset as u64,
            key_offset,
            key_length,
            value_length,
            flags: 0,
            reserved: [0; 7],
        }
    }

    pub fn is_tombstone(&self) -> bool {
        (self.flags & 0x01) != 0
    }

    pub fn set_tombstone(&mut self, is_tombstone: bool) {
        if is_tombstone {
            self.flags |= 0x01;
        } else {
            self.flags &= !0x01;
        }
    }

    pub fn is_overflow(&self) -> bool {
        (self.flags & 0x02) != 0
    }

    pub fn set_overflow(&mut self, is_overflow: bool) {
        if is_overflow {
            self.flags |= 0x02;
        } else {
            self.flags &= !0x02;
        }
    }

    /// Serialize slot to bytes
    pub fn to_bytes(&self) -> [u8; SLOT_SIZE] {
        let mut bytes = [0u8; SLOT_SIZE];
        bytes[0..8].copy_from_slice(&self.fingerprint.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.child_or_value_ptr.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.key_offset.to_le_bytes());
        bytes[20..22].copy_from_slice(&self.key_length.to_le_bytes());
        bytes[22..24].copy_from_slice(&self.value_length.to_le_bytes());
        bytes[24] = self.flags;
        bytes[25..32].copy_from_slice(&self.reserved);
        bytes
    }

    /// Deserialize slot from bytes
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            fingerprint: u64::from_le_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3],
                bytes[4], bytes[5], bytes[6], bytes[7],
            ]),
            child_or_value_ptr: u64::from_le_bytes([
                bytes[8], bytes[9], bytes[10], bytes[11],
                bytes[12], bytes[13], bytes[14], bytes[15],
            ]),
            key_offset: u32::from_le_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]),
            key_length: u16::from_le_bytes([bytes[20], bytes[21]]),
            value_length: u16::from_le_bytes([bytes[22], bytes[23]]),
            flags: bytes[24],
            reserved: [bytes[25], bytes[26], bytes[27], bytes[28], bytes[29], bytes[30], bytes[31]],
        }
    }
}

/// Page containing slots and blob data with hybrid layout support
pub struct Page {
    /// Raw page data (4KB)
    data: [u8; PAGE_SIZE],
}

impl Page {
    /// Create a new empty page
    pub fn new(page_type: PageType) -> Self {
        let mut page = Self {
            data: [0; PAGE_SIZE],
        };
        
        let header = PageHeader::new(page_type);
        page.write_header(&header);
        page.update_checksum();
        page
    }

    /// Create a page from byte array
    pub fn from_bytes(data: [u8; PAGE_SIZE]) -> Self {
        Self { data }
    }

    /// Get page header
    pub fn header(&self) -> PageHeader {
        PageHeader::from_bytes(&self.data[0..PAGE_HEADER_SIZE])
    }

    /// Write header to page
    pub fn write_header(&mut self, header: &PageHeader) {
        let header_bytes = header.to_bytes();
        self.data[0..PAGE_HEADER_SIZE].copy_from_slice(&header_bytes);
    }

    /// Get slot at index
    pub fn get_slot(&self, index: usize) -> Option<Slot> {
        let header = self.header();
        if index >= header.slot_count as usize {
            return None;
        }

        let slot_offset = PAGE_HEADER_SIZE + (index * SLOT_SIZE);
        if slot_offset + SLOT_SIZE > PAGE_SIZE {
            return None;
        }

        Some(Slot::from_bytes(&self.data[slot_offset..slot_offset + SLOT_SIZE]))
    }

    /// Write slot at index
    pub fn write_slot(&mut self, index: usize, slot: &Slot) -> Result<(), PageError> {
        if index >= MAX_SLOTS_PER_PAGE {
            return Err(PageError::InvalidSlotIndex);
        }

        let slot_offset = PAGE_HEADER_SIZE + (index * SLOT_SIZE);
        if slot_offset + SLOT_SIZE > PAGE_SIZE {
            return Err(PageError::InvalidSlotIndex);
        }

        let slot_bytes = slot.to_bytes();
        self.data[slot_offset..slot_offset + SLOT_SIZE].copy_from_slice(&slot_bytes);
        Ok(())
    }

    /// Search for a slot using Morton-T fingerprint
    pub fn search_slot(&self, fingerprint: u64) -> Option<usize> {
        let header = self.header();
        if header.slot_count == 0 {
            return None;
        }

        match header.page_type() {
            PageType::Internal => self.eytzinger_search(fingerprint),
            PageType::Leaf => self.sequential_search(fingerprint),
        }
    }

    /// Eytzinger binary search for internal pages
    fn eytzinger_search(&self, fingerprint: u64) -> Option<usize> {
        let header = self.header();
        let mut idx = 1; // Eytzinger arrays are 1-indexed

        while idx <= header.slot_count as usize {
            if let Some(slot) = self.get_slot(idx - 1) { // Convert to 0-indexed for array access
                match fingerprint.cmp(&slot.fingerprint) {
                    Ordering::Equal => return Some(idx - 1),
                    Ordering::Less => idx = 2 * idx,         // Left child
                    Ordering::Greater => idx = 2 * idx + 1,  // Right child
                }
            } else {
                break;
            }
        }

        None
    }

    /// Sequential binary search for leaf pages
    fn sequential_search(&self, fingerprint: u64) -> Option<usize> {
        let header = self.header();
        let mut left = 0;
        let mut right = header.slot_count as usize;

        while left < right {
            let mid = left + (right - left) / 2;
            if let Some(slot) = self.get_slot(mid) {
                match fingerprint.cmp(&slot.fingerprint) {
                    Ordering::Equal => return Some(mid),
                    Ordering::Less => right = mid,
                    Ordering::Greater => left = mid + 1,
                }
            } else {
                break;
            }
        }

        None
    }

    /// Insert a new slot (used for leaf pages primarily)
    pub fn insert_slot(&mut self, slot: Slot) -> Result<usize, PageError> {
        let mut header = self.header();
        
        if header.slot_count as usize >= MAX_SLOTS_PER_PAGE {
            return Err(PageError::PageFull);
        }

        // For now, simple append insertion
        // TODO: Implement proper sorted insertion for both layouts
        let slot_index = header.slot_count as usize;
        let slot_offset = PAGE_HEADER_SIZE + (slot_index * SLOT_SIZE);

        if slot_offset + SLOT_SIZE > PAGE_SIZE {
            return Err(PageError::PageFull);
        }

        // Write the slot
        self.write_slot(slot_index, &slot)?;

        // Update header
        header.slot_count += 1;
        header.morton_range_start = header.morton_range_start.min(slot.fingerprint);
        header.morton_range_end = header.morton_range_end.max(slot.fingerprint);
        self.write_header(&header);

        Ok(slot_index)
    }

    /// Allocate space in blob area for data
    pub fn allocate_blob(&mut self, size: usize) -> Result<u32, PageError> {
        let mut header = self.header();
        
        if header.available_space() < size {
            return Err(PageError::InsufficientSpace);
        }
        
        let offset = PAGE_SIZE - header.blob_space_used as usize - size;
        header.blob_space_used += size as u16;
        self.write_header(&header);
        
        Ok(offset as u32)
    }

    /// Read key from blob area
    pub fn read_key(&self, offset: u32, length: u16) -> Option<&[u8]> {
        let start = offset as usize;
        let end = start + (length as usize);
        
        if end > PAGE_SIZE {
            return None;
        }

        Some(&self.data[start..end])
    }

    /// Read value from blob area
    pub fn read_value(&self, offset: u32, length: u16) -> Option<&[u8]> {
        let start = offset as usize;
        let end = start + (length as usize);
        
        if end > PAGE_SIZE {
            return None;
        }

        Some(&self.data[start..end])
    }

    /// Write data to blob area
    pub fn write_blob(&mut self, offset: u32, data: &[u8]) -> Result<(), PageError> {
        let start = offset as usize;
        let end = start + data.len();
        
        if end > PAGE_SIZE {
            return Err(PageError::InsufficientSpace);
        }

        self.data[start..end].copy_from_slice(data);
        Ok(())
    }

    /// Get raw page data
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// Get mutable raw page data
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Validate page integrity
    pub fn validate(&self) -> Result<(), PageError> {
        let header = self.header();
        
        if !header.is_valid() {
            return Err(PageError::CorruptedPage);
        }

        // Validate slot count and offsets
        for i in 0..header.slot_count as usize {
            if let Some(slot) = self.get_slot(i) {
                // Validate key offset and length
                let key_end = slot.key_offset as usize + slot.key_length as usize;
                if key_end > PAGE_SIZE {
                    return Err(PageError::CorruptedPage);
                }

                // Validate value offset and length for leaf pages
                if header.page_type() == PageType::Leaf {
                    let value_end = slot.child_or_value_ptr as usize + slot.value_length as usize;
                    if value_end > PAGE_SIZE {
                        return Err(PageError::CorruptedPage);
                    }
                }
            }
        }

        Ok(())
    }

    /// Calculate and update checksum
    pub fn update_checksum(&mut self) {
        let mut header = self.header();
        header.checksum = 0; // Zero out checksum for calculation
        self.write_header(&header);

        // Simple checksum calculation (could be improved)
        let mut checksum = 0u32;
        for chunk in self.data.chunks(4) {
            let mut bytes = [0u8; 4];
            bytes[..chunk.len()].copy_from_slice(chunk);
            checksum = checksum.wrapping_add(u32::from_le_bytes(bytes));
        }

        header.checksum = checksum;
        self.write_header(&header);
    }

    /// Verify checksum
    pub fn verify_checksum(&self) -> bool {
        let header = self.header();
        let stored_checksum = header.checksum;

        // Create temporary page for checksum calculation
        let mut temp_data = self.data;
        let mut temp_header = header;
        temp_header.checksum = 0;
        let temp_header_bytes = temp_header.to_bytes();
        temp_data[0..PAGE_HEADER_SIZE].copy_from_slice(&temp_header_bytes);

        let mut calculated_checksum = 0u32;
        for chunk in temp_data.chunks(4) {
            let mut bytes = [0u8; 4];
            bytes[..chunk.len()].copy_from_slice(chunk);
            calculated_checksum = calculated_checksum.wrapping_add(u32::from_le_bytes(bytes));
        }

        stored_checksum == calculated_checksum
    }
}

impl Default for Page {
    fn default() -> Self {
        Self::new(PageType::Leaf)
    }
}

/// Errors that can occur during page operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PageError {
    PageFull,
    InsufficientSpace,
    CorruptedPage,
    InvalidOffset,
    InvalidSlotIndex,
}

impl std::fmt::Display for PageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PageError::PageFull => write!(f, "Page is full"),
            PageError::InsufficientSpace => write!(f, "Insufficient space in page"),
            PageError::CorruptedPage => write!(f, "Page is corrupted"),
            PageError::InvalidOffset => write!(f, "Invalid offset"),
            PageError::InvalidSlotIndex => write!(f, "Invalid slot index"),
        }
    }
}

impl std::error::Error for PageError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_creation() {
        let page = Page::new(PageType::Leaf);
        let header = page.header();
        
        assert_eq!(header.magic, PAGE_MAGIC);
        assert_eq!(header.page_type(), PageType::Leaf);
        assert_eq!(header.slot_count, 0);
        assert!(header.is_valid());
    }

    #[test]
    fn test_slot_insertion() {
        let mut page = Page::new(PageType::Leaf);
        
        let slot = Slot::new_leaf(12345, 100, 10, 200, 20);
        let index = page.insert_slot(slot).unwrap();
        
        assert_eq!(index, 0);
        assert_eq!(page.header().slot_count, 1);
        
        let retrieved_slot = page.get_slot(0).unwrap();
        assert_eq!(retrieved_slot.fingerprint, 12345);
        assert_eq!(retrieved_slot.key_length, 10);
        assert_eq!(retrieved_slot.value_length, 20);
    }

    #[test]
    fn test_blob_allocation() {
        let mut page = Page::new(PageType::Leaf);
        
        let offset = page.allocate_blob(100).unwrap();
        assert!(offset > 0);
        
        let data = b"test data";
        page.write_blob(offset, data).unwrap();
        
        let read_data = page.read_value(offset, data.len() as u16).unwrap();
        assert_eq!(read_data, data);
    }

    #[test]
    fn test_page_validation() {
        let page = Page::new(PageType::Internal);
        assert!(page.validate().is_ok());
        assert!(page.verify_checksum()); // Should pass with zero checksum initially
    }

    #[test]
    fn test_sequential_search() {
        let mut page = Page::new(PageType::Leaf);
        
        // Insert sorted slots
        let fingerprints = [100, 200, 300, 400, 500];
        for &fp in &fingerprints {
            let slot = Slot::new_leaf(fp, 0, 10, 0, 20);
            page.insert_slot(slot).unwrap();
        }
        
        // Test search
        assert_eq!(page.search_slot(300), Some(2));
        assert_eq!(page.search_slot(100), Some(0));
        assert_eq!(page.search_slot(500), Some(4));
        assert_eq!(page.search_slot(999), None);
    }

    #[test]
    fn test_page_capacity() {
        let mut page = Page::new(PageType::Leaf);
        let mut inserted = 0;
        
        // Try to fill the page
        loop {
            let slot = Slot::new_leaf(inserted as u64, 0, 10, 0, 20);
            match page.insert_slot(slot) {
                Ok(_) => inserted += 1,
                Err(PageError::PageFull) => break,
                Err(e) => panic!("Unexpected error: {}", e),
            }
        }
        
        assert!(inserted > 0);
        assert!(inserted <= MAX_SLOTS_PER_PAGE);
    }

    #[test]
    fn test_checksum() {
        let mut page = Page::new(PageType::Leaf);
        
        // Add some data
        let slot = Slot::new_leaf(42, 100, 10, 200, 20);
        page.insert_slot(slot).unwrap();
        
        // Update checksum
        page.update_checksum();
        assert!(page.verify_checksum());
        
        // Corrupt the page
        page.as_bytes_mut()[100] = 255;
        assert!(!page.verify_checksum());
    }
}