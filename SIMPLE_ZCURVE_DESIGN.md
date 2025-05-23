# Simple Z-Curve Memory-Mapped Storage Engine (ZCurve-DB)

## Design Philosophy

**Simple. Fast. Correct.**

Inspired by Firewood's "compaction-less" approach and libmdbx's memory-mapped simplicity, ZCurve-DB is a ground-up storage engine that uses Z-order curves for spatial locality and memory mapping for automatic disk paging.

**Core Principle**: Let the OS handle memory management. We focus on data organization.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                        ZCurve-DB                            │
├─────────────────────────────────────────────────────────────┤
│  API Layer: get(key), put(key, val), scan(start, end)      │
├─────────────────────────────────────────────────────────────┤
│  Z-Index: Morton-encoded (key_hash, version) → block_id    │
├─────────────────────────────────────────────────────────────┤
│  Block Manager: Fixed-size blocks, memory-mapped           │
├─────────────────────────────────────────────────────────────┤
│  OS Memory Mapping: Automatic paging, no manual cache      │
├─────────────────────────────────────────────────────────────┤
│  File System: Single append-only file per database         │
└─────────────────────────────────────────────────────────────┘
```

## Key Design Decisions

### 1. No Compaction
- **Inspiration**: Firewood's compaction-less approach
- **Implementation**: Copy-on-write blocks, old versions naturally age out
- **Benefit**: Eliminates complex background processes that cause the failures we saw

### 2. Single Memory-Mapped File
- **Inspiration**: libmdbx's simplicity
- **Implementation**: One `.zcdb` file per database, grows as needed
- **Benefit**: OS handles all memory pressure, paging, and caching automatically

### 3. Z-Curve Spatial Index
- **Innovation**: Use Morton codes to encode (key_hash, version) pairs
- **Benefit**: Related data clustered together, excellent scan performance
- **Implementation**: Simple arithmetic, no complex tree structures

### 4. Fixed-Size Blocks
- **Size**: 4KB blocks (matches OS page size)
- **Layout**: Each block contains multiple Z-ordered entries
- **Benefit**: Predictable memory usage, efficient OS paging

## Data Structures

### Z-Value Encoding
```rust
// Convert (key, version) to Z-order value
fn encode_z_value(key_hash: u64, version: u64) -> u64 {
    morton_encode_2d(key_hash, version)
}

// Morton encoding: interleave bits of two 32-bit values
fn morton_encode_2d(x: u64, y: u64) -> u64 {
    let x32 = (x & 0xFFFFFFFF) as u32;
    let y32 = (y & 0xFFFFFFFF) as u32;
    
    let mut result = 0u64;
    for i in 0..32 {
        result |= ((x32 >> i) & 1) << (2 * i);
        result |= ((y32 >> i) & 1) << (2 * i + 1);
    }
    result
}
```

### Block Structure
```rust
// 4KB block = 4096 bytes
const BLOCK_SIZE: usize = 4096;
const ENTRY_SIZE: usize = 64; // Fixed size entries
const ENTRIES_PER_BLOCK: usize = BLOCK_SIZE / ENTRY_SIZE; // 64 entries

#[repr(C)]
struct Block {
    header: BlockHeader,           // 64 bytes
    entries: [Entry; 63],         // 63 × 64 bytes = 4032 bytes
}

#[repr(C)]
struct BlockHeader {
    magic: u32,                   // 0xZCDB
    block_id: u32,               // Block identifier
    entry_count: u16,            // Number of valid entries
    z_min: u64,                  // Minimum Z-value in block
    z_max: u64,                  // Maximum Z-value in block
    next_block: u32,             // Next block in Z-order (0 = none)
    checksum: u32,               // CRC32 of block data
    reserved: [u8; 20],          // Future use
}

#[repr(C)]
struct Entry {
    z_value: u64,                // Morton-encoded (key_hash, version)
    key_len: u16,                // Key length
    value_len: u16,              // Value length
    key_hash: u32,               // Hash of key for quick comparison
    key_offset: u32,             // Offset to key data within block
    value_offset: u32,           // Offset to value data within block
    flags: u16,                  // Deleted, compressed, etc.
    reserved: [u8; 6],           // Alignment
}
```

### Database File Structure
```
File: database.zcdb

┌─────────────────┐  Offset 0
│   File Header   │  4KB - Database metadata
├─────────────────┤  Offset 4KB
│   Block Index   │  4KB - Index mapping Z-ranges to block IDs
├─────────────────┤  Offset 8KB
│     Block 0     │  4KB - Data block
├─────────────────┤  Offset 12KB
│     Block 1     │  4KB - Data block
├─────────────────┤  ...
│      ...        │
├─────────────────┤
│   Block N       │  4KB - Data block
└─────────────────┘

Variable data (keys/values) stored after fixed entries within each block
```

### File Header
```rust
#[repr(C)]
struct FileHeader {
    magic: [u8; 8],              // b"ZCURVEDB"
    version: u32,                // File format version
    page_size: u32,              // Always 4096
    block_count: u32,            // Total number of blocks
    root_version: u64,           // Latest committed version
    free_blocks: u32,            // Head of free block list
    total_entries: u64,          // Total entries in database
    created_time: u64,           // Unix timestamp
    last_sync: u64,              // Last sync timestamp
    reserved: [u8; 4056],        // Pad to 4096 bytes
}
```

## Core Operations

### Put Operation
```rust
impl ZCurveDB {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        // 1. Generate next version
        let version = self.next_version();
        
        // 2. Compute Z-value
        let key_hash = hash64(key);
        let z_value = encode_z_value(key_hash, version);
        
        // 3. Find appropriate block (binary search on Z-ranges)
        let block_id = self.find_block_for_z_value(z_value)?;
        
        // 4. Insert entry (may trigger block split)
        self.insert_entry(block_id, z_value, key, value)?;
        
        // 5. Update index if block split occurred
        self.update_index_if_needed()?;
        
        Ok(())
    }
}
```

### Get Operation
```rust
impl ZCurveDB {
    fn get(&self, key: &[u8], version: Option<u64>) -> Result<Option<Vec<u8>>, Error> {
        let key_hash = hash64(key);
        let target_version = version.unwrap_or(self.latest_version());
        
        // Search for exact match or latest version ≤ target
        let z_value = encode_z_value(key_hash, target_version);
        
        // Binary search in appropriate blocks
        let block_id = self.find_block_for_z_value(z_value)?;
        
        // Search within block
        self.search_block(block_id, key_hash, key, target_version)
    }
}
```

### Scan Operation
```rust
impl ZCurveDB {
    fn scan(&self, start_key: &[u8], end_key: &[u8], version: u64) 
           -> Result<impl Iterator<Item = (Vec<u8>, Vec<u8>)>, Error> {
        
        // Calculate Z-value range using litmax/bigmin algorithms
        let start_hash = hash64(start_key);
        let end_hash = hash64(end_key);
        
        let z_ranges = calculate_z_ranges(start_hash, end_hash, version);
        
        // Scan blocks that overlap with Z-ranges
        let blocks = self.find_blocks_for_ranges(&z_ranges)?;
        
        Ok(ZCurveScanIterator::new(blocks, start_key, end_key, version))
    }
}
```

## Memory Management Strategy

### Automatic OS Paging
```rust
impl ZCurveDB {
    fn new(path: &str) -> Result<Self, Error> {
        // Memory-map entire file
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
            
        // Let OS handle all paging decisions
        let mmap = unsafe {
            MmapOptions::new()
                .map_mut(&file)?
        };
        
        Ok(ZCurveDB {
            mmap,
            file,
            header: parse_header(&mmap)?,
        })
    }
}
```

### Memory Usage Characteristics
- **Hot Data**: Recently accessed Z-values stay in OS page cache
- **Cold Data**: Old versions automatically paged out by OS
- **Working Set**: Adapts automatically to available system memory
- **No Manual Tuning**: OS memory manager handles everything

## Block Management

### Block Allocation
```rust
impl ZCurveDB {
    fn allocate_block(&mut self) -> Result<u32, Error> {
        if let Some(free_block) = self.pop_free_block() {
            Ok(free_block)
        } else {
            // Grow file by one block
            let new_block_id = self.header().block_count;
            self.grow_file(BLOCK_SIZE)?;
            self.header_mut().block_count += 1;
            Ok(new_block_id)
        }
    }
    
    fn free_block(&mut self, block_id: u32) {
        // Add to free list (copy-on-write versioning means
        // old blocks become free when no longer referenced)
        self.push_free_block(block_id);
    }
}
```

### Block Splitting
```rust
impl ZCurveDB {
    fn split_block(&mut self, block_id: u32) -> Result<u32, Error> {
        let block = self.get_block_mut(block_id);
        
        // Find split point (median Z-value)
        let split_point = block.entry_count / 2;
        
        // Allocate new block
        let new_block_id = self.allocate_block()?;
        let new_block = self.get_block_mut(new_block_id);
        
        // Move upper half of entries to new block
        new_block.entries[0..split_point].copy_from_slice(
            &block.entries[split_point..block.entry_count]);
            
        // Update headers
        block.entry_count = split_point as u16;
        new_block.entry_count = split_point as u16;
        
        // Update Z-ranges
        block.z_max = block.entries[split_point - 1].z_value;
        new_block.z_min = new_block.entries[0].z_value;
        
        Ok(new_block_id)
    }
}
```

## Index Structure

### Simple B+-tree Index
```rust
// Maps Z-value ranges to block IDs
// Stored in dedicated index blocks at beginning of file
struct IndexEntry {
    z_min: u64,                  // Minimum Z-value in range
    z_max: u64,                  // Maximum Z-value in range
    block_id: u32,               // Block containing this range
    reserved: u32,               // Alignment
}

impl ZCurveDB {
    fn find_block_for_z_value(&self, z_value: u64) -> Result<u32, Error> {
        // Binary search in index
        let index = self.get_index();
        
        let pos = index.binary_search_by(|entry| {
            if z_value < entry.z_min {
                std::cmp::Ordering::Greater
            } else if z_value > entry.z_max {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        });
        
        match pos {
            Ok(idx) => Ok(index[idx].block_id),
            Err(_) => Err(Error::BlockNotFound),
        }
    }
}
```

## Versioning Strategy

### Copy-on-Write Blocks
```rust
impl ZCurveDB {
    fn update_entry(&mut self, block_id: u32, entry_idx: usize, 
                   new_value: &[u8]) -> Result<(), Error> {
        
        // Always create new version - never modify in place
        let version = self.next_version();
        let key_hash = self.get_block(block_id).entries[entry_idx].key_hash;
        
        // Insert new entry with higher version
        self.put_with_version(key_hash, new_value, version)
    }
    
    fn cleanup_old_versions(&mut self, cutoff_version: u64) {
        // Mark blocks containing only old versions as free
        // This happens naturally as new blocks are allocated
        // and old blocks are no longer referenced
    }
}
```

### Version Resolution
```rust
impl ZCurveDB {
    fn find_latest_version(&self, key_hash: u64, max_version: u64) 
                          -> Result<Option<Entry>, Error> {
        
        // Z-curve naturally orders by version within same key_hash
        // So we can scan backwards from max_version to find latest
        
        let z_value = encode_z_value(key_hash, max_version);
        let block_id = self.find_block_for_z_value(z_value)?;
        
        // Binary search within block for highest version ≤ max_version
        self.search_block_for_version(block_id, key_hash, max_version)
    }
}
```

## Performance Characteristics

### Time Complexity
- **Put**: O(log B) where B = number of blocks
- **Get**: O(log B + log E) where E = entries per block
- **Scan**: O(log B + R) where R = results returned
- **Version Lookup**: O(log B + log E)

### Space Complexity
- **Index Size**: O(B) - one entry per block
- **Block Overhead**: ~1.5% (64 bytes header per 4KB block)
- **Memory Usage**: Working set fits in RAM, rest paged by OS

### Cache Behavior
- **Spatial Locality**: Z-curve clustering improves cache hits
- **Temporal Locality**: Recent versions clustered together
- **OS Page Cache**: Automatic caching of hot blocks
- **No Cache Tuning**: OS handles all memory pressure

## Implementation Phases

### Phase 1: Core Engine (2 weeks)
- [x] File format and basic structures
- [x] Memory mapping setup
- [ ] Basic put/get operations
- [ ] Simple index structure
- [ ] Block allocation/splitting

### Phase 2: Advanced Operations (1 week)
- [ ] Range scanning with Z-curves
- [ ] Version resolution
- [ ] Copy-on-write versioning
- [ ] Basic error handling

### Phase 3: Optimization (1 week)
- [ ] Efficient Z-range calculations (litmax/bigmin)
- [ ] Block splitting optimizations
- [ ] Index caching
- [ ] Performance benchmarking

### Phase 4: Production Features (1 week)
- [ ] Crash recovery
- [ ] Checksums and data integrity
- [ ] Configuration options
- [ ] Comprehensive testing

## Error Handling

### Recovery Strategy
```rust
impl ZCurveDB {
    fn recover() -> Result<Self, Error> {
        // 1. Validate file header
        // 2. Check block checksums
        // 3. Rebuild index if corrupted
        // 4. Mark invalid blocks as free
        
        // No complex WAL or transaction log needed
        // Worst case: lose most recent uncommitted data
    }
}
```

### Consistency Guarantees
- **Atomic Writes**: Each block write is atomic (4KB aligned)
- **Crash Safety**: File always in valid state after block allocation
- **No Corruption**: Checksums detect and isolate bad blocks
- **Simple Recovery**: Rebuild index from block headers if needed

## API Design

### Simple, Synchronous Interface
```rust
pub struct ZCurveDB {
    // Private implementation
}

impl ZCurveDB {
    // Core operations
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error>;
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error>;
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error>;
    pub fn delete(&mut self, key: &[u8]) -> Result<bool, Error>;
    
    // Versioned operations
    pub fn get_version(&self, key: &[u8], version: u64) -> Result<Option<Vec<u8>>, Error>;
    pub fn put_version(&mut self, key: &[u8], value: &[u8]) -> Result<u64, Error>;
    
    // Range operations
    pub fn scan(&self, start: &[u8], end: &[u8]) -> Result<ScanIterator, Error>;
    pub fn scan_version(&self, start: &[u8], end: &[u8], version: u64) 
                       -> Result<ScanIterator, Error>;
    
    // Maintenance
    pub fn sync(&mut self) -> Result<(), Error>;
    pub fn compact(&mut self) -> Result<(), Error>; // Optional, for space reclaim
    pub fn stats(&self) -> DBStats;
}
```

## Why This Will Work

### 1. Proven Foundations
- **Memory Mapping**: Proven by libmdbx, LMDB
- **Z-Curves**: Proven spatial indexing technique
- **No Compaction**: Proven by Firewood
- **Fixed Blocks**: Proven by most filesystems

### 2. Simplicity
- **Single File**: Easy to manage, backup, copy
- **Fixed Structures**: Predictable performance
- **OS Memory Management**: No complex cache logic
- **Minimal State**: Easy to reason about

### 3. Performance
- **Z-Curve Locality**: Related data clustered on disk
- **Memory Mapping**: Zero-copy access to data
- **Block Alignment**: Optimal for OS page cache
- **No Locks**: Single-threaded simplicity

### 4. Scalability
- **Large Files**: Memory mapping handles TB+ databases
- **Memory Pressure**: OS automatically pages cold data
- **Version History**: Old versions naturally age out
- **Growing Workloads**: Index scales logarithmically

This design avoids all the complexity that broke the original ZVerse while keeping the benefits of Z-curves and memory mapping. It can be implemented incrementally and tested at each step.