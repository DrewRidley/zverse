# ZVerse Large Scale Memory Testing Guide

## Overview

The ZVerse Large Scale Memory Test is designed to evaluate the performance and stability of the ZVerse storage engine when handling datasets that exceed available system RAM. This test suite pushes the system into memory pressure conditions, forcing the use of swap space and testing the engine's behavior under extreme resource constraints.

## Purpose

This test suite validates:

1. **Out-of-Memory Performance**: How ZVerse performs when datasets exceed available RAM
2. **Memory Pressure Handling**: System behavior under swap conditions
3. **Data Integrity**: Ensuring no data corruption under memory stress
4. **Concurrent Operations**: Multi-threaded performance under memory pressure
5. **System Stability**: No crashes or memory leaks under extreme conditions

## System Requirements

### Target System
- **RAM**: 18GB (M3 Mac)
- **Storage**: At least 50GB free space for test data and swap
- **CPU**: Multi-core processor for concurrent testing
- **OS**: macOS with sufficient swap space configured

### Test Data Sizes
- **Test 1**: ~25GB dataset (exceeds 18GB RAM)
- **Test 2**: ~5GB dataset for read testing
- **Test 3**: ~10GB concurrent operations
- **Test 4**: ~6GB integrity verification
- **Test 5**: ~30GB extreme stress test

## Running the Tests

### Method 1: Direct Execution

```bash
# Navigate to the zverse directory
cd zverse

# Run the large scale memory test
cargo run --bin large_scale_memory_test
```

### Method 2: With Resource Monitoring

```bash
# Run with comprehensive system monitoring
./monitor_memory_test.sh
```

### Method 3: Monitor Only (if test already running)

```bash
# Monitor existing test process
./monitor_memory_test.sh --monitor-only
```

## Test Suite Components

### Test 1: Out-of-Memory Write Performance
- **Dataset**: 25,000 entries × 1MB = ~25GB
- **Purpose**: Test write performance when exceeding RAM
- **Expected**: Performance degradation as system enters swap
- **Success Criteria**: 
  - Complete all writes without crashes
  - Maintain >50 ops/sec under memory pressure
  - Data integrity preserved

### Test 2: Out-of-Memory Read Performance
- **Dataset**: 10,000 entries × 512KB = ~5GB
- **Purpose**: Test read performance under memory pressure
- **Operations**: 5,000 random reads with latency tracking
- **Success Criteria**:
  - >1000 reads/sec baseline performance
  - >95% read success rate
  - P99 latency <10ms

### Test 3: Concurrent Memory Stress
- **Threads**: 8 writers + 16 readers
- **Duration**: 30 seconds
- **Write Size**: 512KB per operation
- **Success Criteria**:
  - >50 writes/sec sustained
  - >1000 reads/sec sustained
  - >95% write success rate
  - >80% read success rate

### Test 4: Large Dataset Integrity
- **Dataset**: 8,000 entries × 768KB = ~6GB
- **Verification**: Checksum validation for all entries
- **Success Criteria**:
  - 100% data integrity (no corruption)
  - All written data successfully retrieved
  - Checksums match original values

### Test 5: Extreme Memory Mixed Workload
- **Target**: 30GB dataset (extreme pressure)
- **Value Size**: 2MB per entry
- **Duration**: 60 seconds
- **Success Criteria**:
  - Write >20GB before hitting system limits
  - Maintain >70% read success rate
  - System remains stable throughout

## Expected Performance Characteristics

### Normal Operations (No Memory Pressure)
- **Writes**: 1,000+ ops/sec
- **Reads**: 10,000+ ops/sec
- **Latency**: <1ms P99

### Under Memory Pressure (>18GB dataset)
- **Writes**: 50-500 ops/sec (degraded due to swap)
- **Reads**: 100-5,000 ops/sec (depending on cache hits)
- **Latency**: 10-100ms P99 (swap overhead)

### Extreme Pressure (>25GB dataset)
- **Writes**: 10-100 ops/sec (severe swap pressure)
- **Reads**: 50-1,000 ops/sec (cache misses frequent)
- **Latency**: 100ms+ P99 (extensive swapping)

## Interpreting Results

### Success Indicators
- ✅ **EXCELLENT**: Performance exceeds expectations
- ✅ **GOOD**: Performance meets minimum requirements  
- ⚠️ **OK/WARNING**: Performance degraded but functional
- ❌ **CRITICAL/POOR**: Significant issues detected

### Key Metrics to Monitor

#### Memory Usage
- **Free Memory**: Should decrease to <1GB during tests
- **Swap Usage**: Should increase significantly (>5GB)
- **Memory Pressure**: System pressure indicator

#### Performance Metrics
- **Throughput**: Operations per second
- **Latency**: Response time percentiles
- **Success Rate**: Percentage of successful operations
- **Data Integrity**: Checksum validation results

#### System Health
- **CPU Usage**: Should remain reasonable (<90% sustained)
- **Load Average**: May spike during memory pressure
- **I/O Activity**: Heavy disk activity expected during swap

## Monitoring and Analysis

### Real-Time Monitoring
The monitoring script provides real-time updates:
- Memory usage and swap activity
- CPU load and process statistics
- Test progress and performance metrics

### Log Files
Generated in `./test_logs/` directory:
- `memory_usage_*.log`: Memory and swap statistics
- `cpu_usage_*.log`: CPU utilization and load averages
- `io_stats_*.log`: Disk I/O activity
- `process_stats_*.log`: Test process resource usage
- `system_overview_*.log`: System configuration and state

### Analysis Commands
```bash
# Find peak memory pressure
tail -n +2 test_logs/memory_usage_*.log | awk -F',' '{print $8}' | sort -n | tail -1

# Find peak CPU usage
tail -n +2 test_logs/cpu_usage_*.log | awk -F',' '{print $2+$3}' | sort -n | tail -1

# Find peak process memory usage
tail -n +2 test_logs/process_stats_*.log | awk -F',' '{print $5}' | sort -n | tail -1
```

## Troubleshooting

### Common Issues

#### Test Fails to Start
- **Cause**: Insufficient disk space
- **Solution**: Ensure 50GB+ free space available
- **Check**: `df -h` to verify available space

#### System Becomes Unresponsive
- **Cause**: Excessive swap usage overwhelming system
- **Solution**: Reduce test dataset size or increase swap space
- **Prevention**: Monitor memory usage closely

#### Poor Performance
- **Cause**: Background processes consuming resources
- **Solution**: Close unnecessary applications before testing
- **Check**: `top` or Activity Monitor for resource usage

#### Memory Allocation Errors
- **Cause**: System memory limits exceeded
- **Solution**: Restart system to clear memory state
- **Check**: `vm_stat` for memory pressure indicators

### Performance Optimization

#### For Better Test Performance
1. **Close Background Apps**: Minimize memory usage
2. **Increase Swap Space**: Configure larger swap file
3. **Use SSD Storage**: Faster swap performance
4. **Reduce Dataset Size**: For systems with <16GB RAM

#### For System Stability
1. **Monitor Progress**: Watch for system warning signs
2. **Gradual Testing**: Start with smaller datasets
3. **Resource Limits**: Set reasonable test durations
4. **Recovery Plan**: Be prepared to force-quit if needed

## Safety Considerations

### System Protection
- Tests are designed to stress but not crash the system
- Automatic cleanup on process termination
- Monitoring includes system health checks
- Built-in limits to prevent system damage

### Data Safety
- All test data is temporary and can be safely deleted
- No modification of existing user data
- Test databases created in isolated directories
- Comprehensive integrity checking built-in

### Recovery Procedures
If system becomes unresponsive:
1. Force-quit the test process: `pkill -f large_scale_memory_test`
2. Clear test data: `rm -rf ./zverse_data`
3. Monitor memory recovery: `vm_stat`
4. Restart if necessary to clear memory state

## Interpreting Success

### Exceptional Performance
- Handles >25GB dataset with minimal degradation
- Maintains >100 ops/sec under extreme memory pressure
- Zero data integrity issues
- System remains responsive throughout

### Good Performance
- Completes all tests without crashes
- Graceful performance degradation under pressure
- >95% data integrity maintained
- System recovers quickly after test completion

### Acceptable Performance
- Tests complete with some performance impact
- System remains stable under memory pressure
- Minor performance degradation acceptable
- No data corruption detected

### Areas for Improvement
- Significant performance degradation under memory pressure
- System stability issues during peak usage
- Data integrity concerns
- Poor recovery characteristics

This comprehensive test suite provides thorough validation of ZVerse's behavior under extreme memory conditions, ensuring reliable operation even when system resources are severely constrained.