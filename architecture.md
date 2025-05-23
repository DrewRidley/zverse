# ZVerse: Z-Order Curve Optimized Versioned KV Store

## Executive Summary

ZVerse is a high-performance key-value store designed specifically for versioned data access patterns common in database systems like SurrealDB. It uses Z-order curves (Morton codes) to transform the 2D problem space of (key, version) into a 1D space, dramatically improving locality for common access patterns while reducing memory fragmentation and allocation overhead.

## Problem Statement

The current VART (Versioned Adaptive Radix Trie) implementation in SurrealDB suffers from several critical performance issues:

1. **Excessive Arc Operations**: Profiling reveals that most CPU time is spent on reference counting operations (clone/drop), not on actual data comparisons or business logic.

2. **Poor Cache Locality**: Range scans and versioned queries have poor performance due to data being scattered across memory.

3. **Memory Fragmentation**: The tree structure leads to excessive allocations and deallocations, causing memory fragmentation.

4. **Suboptimal Data Access Patterns**: Common operations like "get all records at version X" or "get record history for key Y" require traversing a complex tree structure.

5. **High Allocation Overhead**: Even simple operations require multiple allocations, introducing significant overhead.

## Z-Order Curve Solution

Our solution fundamentally reimagines how versioned data is stored by using Z-order curves to map the 2D space of (key, version) into a 1D space. This approach:

1. **Eliminates Tree Structure**: Replaces the complex tree with a flat, sorted array structure.

2. **Removes Arc Overhead**: Uses arena allocation instead of reference counting.

3. **Optimizes Cache Locality**: Similar keys and versions are stored near each other in memory.

4. **Accelerates Common Queries**: Makes "get all records at version X" and "get record at key Y, version Z" dramatically faster.

5. **Reduces Memory Fragmentation**: Uses bulk allocations instead of per-node allocations.

### How Z-Order Curves Work

A Z-order curve (or Morton code) interleaves the bits of multiple dimensions to create a single value that preserves locality in all dimensions. For a 2D space of (key, version):

1. Convert the key and version to binary
2. Interleave their bits (key bits in even positions, version bits in odd positions)
3. The resulting value is the Z-order value

Example:
```
Key: 5 (binary: 101)
Version: 3 (binary: 011)
Z-value: 10011 (interleaved)
```

This creates a space-filling curve that visits points in an order that preserves locality in both dimensions.

## Architecture

### Core Data Structures

```
// Memory-mapped segment header (fixed size)
struct ZSegmentHeader {
    magic: u64,                      // Magic number for validation
    format_version: u32,             // Format version for compatibility
    segment_id: u64,                 // Unique segment identifier
    z_value_min: u64,                // Minimum z-value in this segment
    z_value_max: u64,                // Maximum z-value in this segment
    entry_count: AtomicU64,          // Number of entries in segment
    free_space_offset: AtomicU64,    // Offset to free space
    checksum: u64,                   // CRC64 of segment data for integrity
    created_timestamp: u64,          // Creation timestamp
    last_modified_timestamp: u64,    // Last modification timestamp
}

// Zero-copy entry within memory-mapped segment
struct ZEntry {
    z_value: u64,                    // Interleaved key + version bits
    key_offset: u32,                 // Offset to key within segment
    key_length: u16,                 // Length of key in bytes
    value_offset: u32,               // Offset to value within segment
    value_length: u32,               // Length of value in bytes
    version: u64,                    // Original version
    flags: u16,                      // Bit flags (deleted, compressed, etc.)
}

// Primary data structure
struct ZVerse {
    // Segment directory (lock-free)
    segment_directory: LockFreeRadixTree<ZValueRange, SegmentRef>,
    
    // Memory-mapped segments
    active_segments: RwLock<Vec<MmapSegment>>,
    
    // Tier management
    hot_tier: Vec<SegmentId>,        // Segments to keep in memory
    warm_tier: Vec<SegmentId>,       // Segments to allow paging
    cold_tier: Vec<SegmentId>,       // Segments to explicitly page out
    
    // Concurrency control
    latest_commit_version: AtomicU64, // Latest committed version
    
    // Configuration
    config: ZVerseConfig,
    
    // Statistics
    stats: ZVerseStats,
}

// Thread-local write buffer for batched commits
thread_local! {
    static WRITE_BUFFER: RefCell<Vec<(ZValue, Key, Value)>> = RefCell::new(Vec::with_capacity(1024));
}
```

### Tiered Storage and Zero-Copy Architecture

ZVerse implements a tiered storage architecture that completely eliminates traditional WAL while maintaining ACID guarantees:

1. **Memory-Mapped Files as Primary Storage**: 
   - All data lives in memory-mapped files
   - Zero-copy reads directly from mapped memory
   - Direct page-aligned writes that avoid double-buffering
   - Leverages OS page cache for intelligent caching

2. **Z-Order Informed Tiering**:
   - Z-values naturally cluster "hot" vs "cold" data
   - Higher z-values (newer versions, frequently accessed keys) stay in memory
   - Lower z-values (older versions, rarely accessed keys) swapped to disk
   - Automatic temperature-aware migration between tiers

3. **Segmented Storage Files**:
   - Data divided into fixed-size segments based on z-value ranges
   - Each segment is a separate memory-mapped file
   - Allows targeted fsync() operations only on modified segments
   - Enables parallel I/O across multiple segments

4. **Zero Allocation Design**:
   - Values stored directly in memory-mapped regions
   - Keys stored once and referenced by offset
   - No copying of data between memory regions
   - Zero heap allocations during normal operation

### Key Operations

#### Insert(key, value)

1. Acquire next version number via atomic increment of `latest_commit_version`
2. Compute Z-value from key and version
3. Append the new entry to a thread-local buffer
4. Periodically merge thread-local buffers into the main entries array using a lock-free merge algorithm
5. Update indexes atomically using Compare-and-Swap (CAS) operations

This approach ensures:
- Writers never block readers
- Multiple writers can operate concurrently
- No locks are required for the common case

#### Get(key, version)

1. Compute Z-value from key and version
2. Binary search in the entries array
3. Return the matching entry, or the entry with the largest version less than the requested version

#### Range(start_key, end_key, version)

1. Compute Z-values for (start_key, version) and (end_key, version)
2. Use litmax/bigmin algorithms to identify Z-value ranges
3. Scan entries within these ranges

#### History(key, start_version, end_version)

1. Use key_index to find the range of entries for the key
2. Filter by version range
3. Return matching entries

#### Snapshot(version)

1. Use version_index to find the range of entries for the version
2. Return all entries in that range

### Optimizations

#### SIMD Operations

While our initial attempts at SIMD for byte comparisons weren't effective in the VART structure, SIMD can be effectively used in ZVerse for:

1. **Bulk Z-value Computations**: Computing multiple Z-values in parallel
2. **Range Filtering**: Using SIMD to filter multiple entries at once
3. **Batch Operations**: Processing multiple entries simultaneously

#### Compression

Since similar keys and versions are stored near each other, compression can be very effective:

1. **Delta Encoding**: Store differences between consecutive keys and values
2. **Dictionary Compression**: Common patterns in keys and values can be dictionary-compressed
3. **Bitmap Indexes**: For version ranges, bitmap indexes can efficiently represent which entries belong to which versions

### Lock-Free and WAL-Free Concurrency

The Z-order curve layout combined with memory mapping enables a truly lock-free and WAL-free architecture:

1. **Direct Persistence Without WAL**:
   - Each commit directly modifies the z-ordered memory-mapped segments
   - Atomic msync() operations replace traditional WAL commits
   - Page-aligned writes eliminate traditional write amplification
   - No separate log structures or replay mechanisms needed

2. **Multi-Version Z-Order Concurrency Control**:
   - Readers access data at their chosen version without any synchronization
   - Writers append entries to higher version regions without blocking readers
   - Perfect isolation through z-curve separation of version dimensions
   - Entire transactions become visible atomically through a single atomic pointer update

3. **Lock-Free Directory Structure**:
   - Lock-free concurrent radix tree maps z-value ranges to storage segments
   - Readers never block on directory structure changes
   - Multiple writers can update different segments concurrently
   - Compare-and-swap operations used only for directory structure updates

4. **Z-Aware Thread Affinity**:
   - Threads assigned to specific z-value ranges
   - Natural sharding with minimal cross-thread coordination
   - Eliminates false sharing across CPU cores
   - Optimal cache utilization by maintaining thread-local z-value locality

## Implementation Plan

### Phase 1: Core Implementation

1. ✅ Project setup and architecture document
2. ⬜ Basic data structures implementation (ZEntry, ZVerse)
3. ⬜ Z-order calculation implementation
4. ⬜ Basic operations: Insert, Get
5. ⬜ Arena allocator implementation
6. ⬜ Basic benchmarking harness

### Phase 2: Advanced Operations

1. ⬜ Range queries implementation
2. ⬜ History queries implementation
3. ⬜ Snapshot operations
4. ⬜ Index structures for faster access
5. ⬜ Advanced benchmarking and comparison with VART

### Phase 3: Optimizations

1. ⬜ SIMD optimizations
2. ⬜ Compression implementation
3. ⬜ Memory-mapping for large datasets
4. ⬜ Concurrency support
5. ⬜ Performance tuning based on benchmarks

### Phase 4: Concurrency and Robustness

1. ⬜ Lock-free concurrency implementation
2. ⬜ MVCC (Multi-Version Concurrency Control) implementation
3. ⬜ Garbage collection for old versions
4. ⬜ Recovery and durability mechanisms
5. ⬜ Stress testing with concurrent workloads

### Phase 5: Integration

1. ⬜ SurrealDB integration interfaces
2. ⬜ Compatibility layer for VART
3. ⬜ Migration tools
4. ⬜ Documentation and examples
5. ⬜ Final performance testing and validation

### Expected Performance Improvements

Based on the combined benefits of Z-order curves, zero-copy design, and direct memory mapping:

1. **Single Key-Version Lookup**: 5-10x faster due to direct memory access, zero indirection, and perfect cache locality
2. **Range Scans**: 10-20x faster due to contiguous memory layout and direct access to memory-mapped regions
3. **Version Snapshots**: 20-50x faster as all data for a version is contiguous and directly accessible
4. **Memory Usage**: 50-70% reduction due to elimination of tree structure overhead and zero-copy design
5. **Write Performance**: 5-10x faster due to elimination of WAL, zero allocation, and direct memory mapping
6. **Concurrent Operations**: 
   - Perfect read scalability (linear scaling with reader threads, zero overhead)
   - Near-linear write scalability (up to 95% efficiency with multiple writer threads)
   - Zero contention between readers and writers under all workloads
7. **I/O Efficiency**:
   - 80-90% reduction in write amplification compared to LSM or B-tree based systems
   - Elimination of read amplification through direct memory access
   - Dramatically reduced fsync() overhead through targeted segment syncing
   - Zero-copy persistence with no serialization/deserialization overhead

## Advanced System Design

The combination of Z-order curves and memory-mapped segments enables a revolutionary approach to database storage:

### Direct Persistence Without WAL

1. **Elimination of Write-Ahead Logging**:
   - Z-ordered segments are the database - no separate log structures
   - Atomic segment updates replace traditional log-then-apply approach
   - Zero recovery time - memory mapping makes data immediately available
   - Perfect crash consistency through careful ordering of msync operations

2. **Intelligent Data Placement**:
   - Z-values naturally separate hot/cold data
   - Higher z-values (newer versions) stay in memory
   - Lower z-values (older versions) naturally page to disk
   - Automatic migration between temperature tiers based on access patterns

3. **Zero-Copy End-to-End**:
   - Values accessed directly from memory-mapped regions
   - No serialization/deserialization overhead
   - Direct DMA transfers from storage to CPU
   - Memory-aligned structures optimized for SIMD operations

4. **Optimized I/O Patterns**:
   - Perfectly sequential writes for new data
   - Predictable read patterns following z-order curve
   - Multi-version data naturally separates hot/cold regions
   - Perfect alignment with storage device characteristics

### Adaptive Tiering Based on Z-Values

1. **Z-Value Temperature Mapping**:
   - Newer versions (higher in the version dimension) are typically hotter
   - More frequently accessed keys are kept in memory
   - Z-values perfectly encode this multi-dimensional locality
   - Natural stratification into access frequency tiers

2. **Dynamic Tier Migration**:
   - Segments automatically promoted/demoted based on access patterns
   - Zero-copy migration through memory mapping manipulation
   - Background thread manages tier placement optimization
   - Explicit temperature hints for known access patterns

3. **Tier-Specific Optimizations**:
   - Hot tier: Fully cached, potential NUMA optimization
   - Warm tier: Memory mapped but allowed to page
   - Cold tier: Explicitly paged out, compressed storage
   - Archive tier: Deduplicated, heavily compressed, potentially on slower media

## Conclusion

The ZVerse architecture represents a revolutionary approach to database storage that fundamentally reimagines how versioned key-value data is managed. By combining Z-order curves with memory-mapped zero-copy design, we create a system that dramatically outperforms traditional approaches for all common database operations.

This design completely eliminates the traditional WAL while maintaining ACID guarantees through careful orchestration of memory-mapped segments. The natural properties of Z-order curves enable an unprecedented combination of benefits:

1. **Extreme Performance**: Direct memory access with perfect locality for both point and range operations
2. **Perfect Concurrency**: True lock-free design with zero contention between readers and writers
3. **Optimal Resource Usage**: Dramatic reduction in CPU, memory, and I/O requirements
4. **Natural Hot/Cold Separation**: Z-values inherently encode access temperature
5. **Zero-Copy End-to-End**: No serialization, deserialization or data copying anywhere in the pipeline
6. **Perfect Crash Recovery**: Instant recovery with zero replay needed

For SurrealDB, this architecture eliminates all the bottlenecks identified in the current VART implementation while providing a foundation for future optimizations in areas like SIMD processing, compression, and advanced concurrency. The result will be a database capable of throughput and latency characteristics previously thought impossible with ACID guarantees.

By embracing the inherent properties of Z-order curves and combining them with modern systems programming techniques like memory mapping and zero-copy design, ZVerse establishes a new paradigm for high-performance database storage engines.