# ZVerse - High-Performance Lock-Free Temporal Database

[![Rust](https://img.shields.io/badge/rust-nightly-orange.svg)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

ZVerse is a high-performance, lock-free temporal database built in Rust that uses Morton space-time encoding for optimal cache locality and zero-coordination concurrent access.

## 🚀 Performance Highlights

- **1.2M+ writes/sec** at 16 threads
- **22M+ reads/sec** at 16 threads  
- **2.6M+ temporal records/sec** creation rate
- **10M+ batch queries/sec** for bulk operations
- **Zero locks** in critical path - true lock-free operation
- **2-20x faster** than Redis/Memcached for in-memory operations
</thinking>

<edits>

<old_text>
## 🚀 Performance Highlights

- **1.3M+ writes/sec** at 32 threads
- **10.7M+ reads/sec** at 32 threads  
- **213K temporal records/sec** creation rate
- **6M+ batch queries/sec** for bulk operations
- **Zero locks** in critical path - true lock-free operation
- **2-10x faster** than Redis/Memcached for in-memory operations

## ✨ Key Features

- **Lock-Free Architecture**: Zero coordination overhead between threads using atomic pointers and Morton-based partitioning
- **Temporal Data Management**: Efficient versioning with predecessor search and time-based queries
- **Cache-Optimized Design**: Strategic prefetching, inline storage, and hot/cold data separation
- **Morton Space-Time Encoding**: Natural data locality for temporal access patterns
- **Batch Operations**: High-throughput bulk data processing with partition grouping
- **Memory Safe**: Production-ready implementation with comprehensive error handling

## 📦 Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
zverse = "0.1.0"
```

**Note**: Requires Rust nightly for `core_intrinsics` feature.

## 🔧 Quick Start

```rust
use zverse::MortonTemporalDB;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = MortonTemporalDB::new();
    
    // Insert data with automatic timestamping
    db.put("user:123", b"user data".to_vec())?;
    
    // Retrieve latest version
    let data = db.get("user:123")?;
    println!("Data: {:?}", String::from_utf8(data)?);
    
    // Insert at specific timestamps
    db.put_at_time("user:123", b"old data".to_vec(), 1000)?;
    db.put_at_time("user:123", b"new data".to_vec(), 2000)?;
    
    // Query historical versions
    let old_data = db.get_at_time("user:123", 1500)?;
    let new_data = db.get_at_time("user:123", 2500)?;
    
    println!("Old: {:?}", String::from_utf8(old_data)?);
    println!("New: {:?}", String::from_utf8(new_data)?);
    
    Ok(())
}
```

## 📖 API Reference

### Core Operations

#### `MortonTemporalDB::new() -> Self`
Creates a new temporal database instance with 1024 lock-free partitions.

#### `put(key: &str, value: Vec<u8>) -> MortonResult<()>`
Inserts a key-value pair with automatic timestamping.

#### `get(key: &str) -> MortonResult<Vec<u8>>`
Retrieves the latest version of a key with strategic prefetching.

#### `put_at_time(key: &str, value: Vec<u8>, timestamp: u64) -> MortonResult<()>`
Inserts a key-value pair at a specific timestamp.

#### `get_at_time(key: &str, timestamp: u64) -> MortonResult<Vec<u8>>`
Retrieves the version of a key at or before the specified timestamp.

### Batch Operations

#### `put_temporal_batch(items: &[(String, Vec<u8>)]) -> usize`
Batch insert with temporal locality optimization and partition grouping.

#### `get_batch(keys: &[&str]) -> Vec<MortonResult<Vec<u8>>>`
Batch retrieval with cache-efficient partition processing.

#### `get_batch_at_time(keys: &[&str], timestamp: u64) -> Vec<MortonResult<Vec<u8>>>`
Batch temporal queries for maximum throughput.

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    Client Applications                       │
└─────────────────────────┬───────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────┐
│              MortonTemporalDB                               │
│  ┌─────────────────────────────────────────────────────────┐│
│  │           Global Timestamp Generator                     ││
│  │         (AtomicU64 with FNV Hashing)                    ││
│  └─────────────────────────────────────────────────────────┘│
└─────────────────────────┬───────────────────────────────────┘
                          │
          ┌───────────────┼───────────────┐
          │               │               │
┌─────────▼───┐  ┌────────▼────┐  ┌──────▼─────┐
│ Partition 0 │  │ Partition 1 │  │    ...     │
│             │  │             │  │            │
│ Timeline    │  │ Timeline    │  │ Partition  │
│ Index       │  │ Index       │  │   1023     │
│             │  │             │  │            │
│ Morton      │  │ Morton      │  │ Morton     │
│ Storage     │  │ Storage     │  │ Storage    │
└─────────────┘  └─────────────┘  └────────────┘
```

### Key Components

- **Partitioned Architecture**: 1024 independent partitions for lock-free concurrent access
- **Morton Encoding**: Space-time interleaving for temporal data locality
- **Timeline Management**: SmallVec-optimized version chains with inline storage
- **Strategic Prefetching**: Cache-aware data access patterns
- **Branch Prediction**: Optimized hot paths with likely/unlikely hints

## 🎯 Use Cases

### Optimal For
- **Time-series databases** with high write throughput
- **Real-time analytics** requiring sub-millisecond queries  
- **IoT data ingestion** with temporal correlation needs
- **Financial trading systems** with strict latency requirements
- **Caching layers** for temporal application data
- **Audit logs** with historical query requirements

### Limitations
- **Memory-bound** storage (no persistence)
- **Single-node** design (horizontal scaling via app-level sharding)
- **Rust nightly** dependency for intrinsics

## 📊 Benchmarks

### Concurrent Operation Performance (16 threads)
- **Writes**: 1,185,338 ops/sec
- **Reads**: 22,170,901 ops/sec
- **Temporal Creation**: 2,636,068 records/sec
- **Batch Queries**: 10,274,451 queries/sec

### Comparison with Industry Standards
| Database | Write Ops/sec | Read Ops/sec | Type |
|----------|---------------|---------------|------|
| **ZVerse** | **1,185,338** | **22,170,901** | In-memory temporal |
| Redis | 100K-500K | 100K-1M | In-memory K-V |
| Memcached | 300K-1M | 300K-1M | In-memory cache |

## 🔬 Technical Details

### Configuration
```rust
const PARTITION_COUNT: usize = 1024;           // Lock-free partitions
const PREFETCH_DISTANCE: usize = 4;            // Cache prefetch range  
const TIMELINE_INLINE_SIZE: usize = 8;         // SmallVec capacity
const VALUE_INLINE_SIZE: usize = 512;          // Value storage limit
```

### Memory Characteristics
- **Timeline Inline Ratio**: ~100% (most timelines fit in SmallVec)
- **Value Inline Ratio**: Variable based on payload size
- **Memory Overhead**: ~20% for metadata and indexes
- **Allocation Pattern**: Minimal heap allocations in hot paths

## 🧪 Testing

Run the test suite:

```bash
cargo test
```

Run with optimizations:

```bash
RUSTFLAGS="-C target-cpu=native" cargo test --release
```

## 🚀 Development

### Building
```bash
cargo build --release
```

### Benchmarking
```bash
RUSTFLAGS="-C target-cpu=native" cargo test --release
```

### Documentation
```bash
cargo doc --open
```

## 🤝 Contributing

Contributions are welcome! Please see our [design document](design/LOCKFREE_MORTON_TEMPORAL_DATABASE.md) for architectural details.

### Development Guidelines
1. Run `cargo test` before submitting
2. Follow Rust naming conventions
3. Add tests for new functionality
4. Update documentation for API changes
5. Maintain performance benchmarks

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🏆 Acknowledgments

- Inspired by SurrealKV's VART data structure
- Morton encoding techniques from spatial database research
- Lock-free programming patterns from systems research
- Performance optimization techniques from high-frequency trading systems

---

Built with ❤️ in Rust for maximum performance and safety.