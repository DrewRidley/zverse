# ZVerse Temporal Database Benchmarks

This directory contains comprehensive benchmarks for the ZVerse temporal database, designed to evaluate performance across various temporal database operations and real-world usage patterns.

## Benchmark Suites

### 1. `temporal_db_bench.rs` - Comprehensive Database Operations

This benchmark suite covers the full range of database operations with comparisons to standard data structures:

#### Basic Operations
- **Sequential Insert**: Tests insertion performance with automatic timestamping
- **Sequential Temporal Insert**: Tests insertion at specific timestamps  
- **Random Insert**: Tests random key insertion patterns

#### Temporal Operations
- **Versioning Workload**: Creates multiple versions of the same keys
- **Temporal Queries**: Retrieves latest and historical versions of data

#### Batch Operations
- **Batch Insert**: Tests bulk insertion performance (100-5000 items)
- **Batch Get**: Tests bulk retrieval performance

#### Mixed Workloads
- **Read Heavy (80/20)**: Simulates read-heavy workloads
- **Temporal Mixed**: Combines latest reads, historical reads, current writes, and temporal writes

#### Database Scenarios
- **User Balance Updates**: Simulates financial account balance tracking
- **Session Tracking**: Simulates user session management

#### Performance Scaling
- **Large Dataset Read**: Tests performance with datasets from 1K to 1M records

### 2. `temporal_versioning.rs` - Focused Temporal Features

This benchmark suite specifically targets temporal database capabilities:

#### Key Versioning
- **Create Versions**: Tests creating 5-50 versions of the same key
- **Historical Queries**: 
  - Get latest version
  - Get random historical version
  - Get slightly older version (common pattern)

#### Temporal Range Queries
- **Recent Window**: Query within recent time windows (time series pattern)

#### Application Patterns
- **Bank Transfer**: Simulates financial transaction processing
- **Event Sourcing**: Tests event append patterns
- **Audit Trail**: Document versioning and historical lookup
- **Concurrent Versioning**: High-frequency updates to same key

## Key Temporal Database Scenarios

### Creating Multiple Versions
```rust
// Create versions of a user's balance over time
for version in 0..versions {
    let value = format!("balance:{}", version * 100).into_bytes();
    let timestamp = base_time + (version as u64 * 1000);
    db.put_at_time(key, value, timestamp);
}
```

### Fetching Older Versions
```rust
// Get balance as it was at a specific point in time
let historical_balance = db.get_at_time("user:alice", past_timestamp);
```

### Audit Trail Queries
```rust
// Get document state at any point in history
let document_state = db.get_at_time("document:123", audit_timestamp);
```

## Running Benchmarks

### Run All Benchmarks
```bash
cargo bench
```

### Run Specific Benchmark Suite
```bash
# Comprehensive database operations
cargo bench --bench temporal_db_bench

# Focused temporal features
cargo bench --bench temporal_versioning
```

### Run Specific Benchmark Group
```bash
# Only versioning benchmarks
cargo bench --bench temporal_versioning versioning_benches

# Only application pattern benchmarks  
cargo bench --bench temporal_versioning application_benches

# Only basic operations
cargo bench --bench temporal_db_bench basic_ops
```

### Run with HTML Reports
```bash
cargo bench --bench temporal_versioning -- --output-format html
```

## Benchmark Metrics

The benchmarks measure:

- **Throughput**: Operations per second
- **Latency**: Time per operation
- **Scalability**: Performance with varying dataset sizes
- **Memory Efficiency**: Through various data sizes and access patterns

## Expected Performance Characteristics

Based on the optimized Morton encoding implementation:

- **Writes**: 1.2M+ ops/sec
- **Reads**: 10M+ ops/sec  
- **Batch Reads**: 10M+ ops/sec
- **Temporal Queries**: Efficient due to Morton space-time encoding

## Understanding Results

### Temporal vs Non-Temporal Operations
- Current time operations (`put`, `get`) should be fastest
- Historical queries (`get_at_time`) test temporal indexing efficiency
- Multiple versions of same key test Morton encoding effectiveness

### Batch vs Single Operations
- Batch operations should show higher throughput due to amortized costs
- Single operations show pure per-operation overhead

### Memory Access Patterns
- Sequential access should outperform random access
- Morton encoding should provide good cache locality for temporal access

## Benchmark Configuration

- **Seeded RNG**: Reproducible results across runs
- **Black Box**: Prevents compiler optimizations from skewing results
- **Iteration Counts**: Automatically determined by Criterion
- **Warmup**: Built into Criterion framework

## Adding New Benchmarks

To add temporal database-specific benchmarks:

1. Focus on temporal access patterns (time-based queries)
2. Test version management scenarios
3. Include realistic application workloads
4. Use appropriate data sizes for your use case
5. Consider concurrent access patterns

The benchmark suite is designed to reflect real-world temporal database usage patterns while providing comprehensive performance insights.