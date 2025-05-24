# ZVerse Disk Storage Design

## Overview

Design for memory-mapped disk storage that leverages temporal locality to handle datasets larger than main memory without significant performance degradation. Built on lessons from ODHT with intelligent timestamp-based flushing.

## Core Design Principles

### 1. Temporal Locality Preservation
- Morton space-time encoding naturally clusters related temporal data
- Memory mapping exploits this locality for efficient disk access
- Hot temporal regions stay in memory, cold regions remain on disk

### 2. Zero-Copy Architecture
- Direct memory mapping of disk structures (inspired by ODHT)
- Platform-independent binary format
- No serialization/deserialization overhead

### 3. OS-Managed Persistence
- Use `memmap2` crate for memory mapping with OS-managed flushing
- Leverage OS page cache for optimal memory management
- Periodic `msync()` calls for durability guarantees

## File Structure Layout

```
data/
├── partitions/
│   ├── partition_0000.dat    # Memory-mapped partition files
│   ├── partition_0001.dat
│   └── ...
├── metadata/
│   ├── index.dat            # Partition index mapping
│   └── timestamps.dat       # Flush boundary tracking
└── config.toml              # Database configuration
```

### Partition File Format

Each partition file is designed for direct memory mapping:

```
[Header: 64 bytes]
- Magic number (8 bytes): "ZVERSEDB"
- Version (4 bytes): Format version
- Partition ID (4 bytes): Fibonacci hash partition
- Record count (4 bytes): Number of records
- Timestamp range (16 bytes): Min/max timestamps
- Checksum (4 bytes): Header integrity
- Reserved (24 bytes): Future expansion

[Records Section: Variable]
- Records sorted by Morton codes for temporal locality
- Each record: [Morton code (8) | Key len (2) | Value len (4) | Key data | Value data]

[Index Section: Variable]
- Morton code index for binary search
- Memory-mapped for O(log n) lookups
```

## Memory Mapping Strategy

### 1. Tiered Memory Management

```rust
pub struct TieredMemoryManager {
    hot_partitions: LRU<PartitionId, MmapPartition>,    // Recently accessed
    warm_partitions: HashMap<PartitionId, WeakRef>,     // Mapped but not cached
    cold_partitions: HashSet<PartitionId>,              // On disk only
}
```

### 2. Temporal Access Patterns
- **Hot**: Recent timestamps (last hour/day)
- **Warm**: Medium-age timestamps (last week/month) 
- **Cold**: Old timestamps (older than month)

### 3. Adaptive Loading
- Load partitions on-demand based on timestamp queries
- Prefetch adjacent temporal partitions
- Evict based on temporal access patterns, not just LRU

## OS-Managed Durability with memmap2

### 1. Memory Mapping Strategy
- Use `memmap2::MmapMut` for read-write access to partition files
- OS handles page cache management and dirty page flushing
- Periodic `msync(MS_ASYNC)` for durability without blocking writes

### 2. Crash Safety
- Memory-mapped files provide durability through OS page cache
- `msync(MS_SYNC)` at configurable intervals for explicit durability
- File-level integrity through atomic operations and checksums

### 3. Simple Configuration
```rust
pub struct DurabilityConfig {
    sync_interval: Duration,          // How often to call msync()
    sync_on_shutdown: bool,           // Sync before clean shutdown
    use_async_sync: bool,             // Use MS_ASYNC vs MS_SYNC
}
```

## Implementation Strategy

### Phase 1: Core Memory Mapping (Week 1-2)
1. **Partition File Format**
   - Design binary layout compatible with memory mapping
   - Implement header and record serialization
   - Add integrity checking with checksums

2. **Memory Mapping Infrastructure**
   - Create `MmapPartition` wrapper around existing partitions
   - Implement safe memory mapping with proper error handling
   - Add partition file creation and growth management

3. **Basic Integration**
   - Modify existing partition system to use memory-mapped files
   - Maintain API compatibility with current implementation
   - Add configuration for disk vs in-memory mode

### Phase 2: Temporal Optimization (Week 3)
1. **Temporal Access Patterns**
   - Implement hot/warm/cold partition classification
   - Add temporal-aware LRU eviction policy
   - Optimize Morton code layout for disk access

2. **Prefetching Strategy**
   - Implement temporal locality prefetching
   - Add metrics for cache hit rates by temporal distance
   - Tune prefetch window based on access patterns

### Phase 3: Durability & OS Integration (Week 4)
1. **Memory Mapping Integration**
   - Integrate `memmap2::MmapMut` with existing partition system
   - Implement periodic `msync()` calls for durability
   - Add configuration for sync frequency and strategy

2. **File Growth Management**
   - Handle dynamic file growth for memory-mapped regions
   - Implement safe remapping when files grow
   - Add proper error handling for mapping failures

### Phase 4: Testing & Benchmarking (Week 5)
1. **Durability Testing**
   - Test data integrity after system crashes
   - Verify memory-mapped file consistency
   - Test with different sync strategies and intervals

2. **Performance Benchmarking**
   - Compare memory-mapped vs in-memory performance
   - Test with datasets larger than available RAM
   - Measure OS page cache effectiveness

## Key Features

### 1. Morton Locality Preservation
- Partition files maintain Morton order for optimal temporal access
- Memory mapping respects cache line boundaries
- Prefetching follows temporal curves

### 2. Graceful Degradation
- Performance degrades gradually as working set exceeds RAM
- Most recent data stays hot in memory
- Cold data access is still reasonably fast due to SSD optimization

### 4. Crash Safety Through OS
- Memory-mapped files provide crash safety through OS page cache
- No complex WAL needed - OS handles dirty page management
- Simple recovery through file integrity verification

### 4. Scalability
- Linear scaling with disk space rather than RAM
- Efficient handling of time-series workloads
- Natural archival of old data

## Configuration Options

```rust
pub struct DiskConfig {
    // File management
    max_partition_size: usize,           // 256MB default
    partition_growth_factor: f64,        // 1.5x growth
    
    // Memory management  
    hot_partition_limit: usize,          // Max hot partitions in RAM
    warm_partition_limit: usize,         // Max warm partitions mapped
    
    // Durability management
    sync_interval: Duration,             // 30 seconds default msync()
    sync_on_shutdown: bool,              // true - sync before shutdown
    use_async_sync: bool,                // true - use MS_ASYNC
    
    // Performance tuning
    prefetch_window: Duration,           // 1 hour temporal window
    cache_line_alignment: bool,          // Align to 64-byte boundaries
    mmap_populate: bool,                 // MAP_POPULATE for prefault
}
```

## Success Metrics

1. **Performance**: <20% degradation when working set > RAM
2. **Scalability**: Handle 10x larger datasets than available RAM  
3. **Durability**: Data persisted through OS page cache with configurable sync
4. **Recovery**: <1 second startup time with integrity verification
5. **Space Efficiency**: <10% overhead vs raw data size
6. **Simplicity**: Leverage OS memory management instead of complex custom logic

## Risk Mitigation

1. **Memory Mapping Failures**: Fallback to read/write operations
2. **Disk Space**: Configurable data retention and archival
3. **Performance Regression**: Comprehensive benchmarking at each phase
4. **Data Corruption**: Checksums and integrity verification
5. **Platform Issues**: Extensive testing on target platforms

This design leverages the natural temporal locality of Morton encoding while providing the scalability benefits of disk storage and the crash safety of timestamp-based consistency boundaries.