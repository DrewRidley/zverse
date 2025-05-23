# ZVerse Large Scale Memory Test

## Quick Start

```bash
# Run the comprehensive test with monitoring
./run_large_scale_test.sh

# Or run without monitoring
./run_large_scale_test.sh --no-monitor

# Check if your system can run the test
./run_large_scale_test.sh --check
```

## What This Test Does

This test validates ZVerse's performance and stability when handling datasets that exceed available system RAM. It specifically:

- **Generates 25-30GB of data** on an 18GB system
- **Forces memory pressure** and swap usage
- **Tests concurrent operations** under extreme conditions
- **Verifies data integrity** throughout the process
- **Monitors system resources** in real-time

## Test Components

| Test | Dataset Size | Purpose | Expected Result |
|------|-------------|---------|-----------------|
| Out-of-Memory Writes | 25GB | Write performance under swap | >50 ops/sec maintained |
| Out-of-Memory Reads | 5GB | Read performance under pressure | >1000 reads/sec, >95% success |
| Concurrent Stress | 10GB | Multi-threaded under pressure | Stable concurrent operations |
| Data Integrity | 6GB | Verify no corruption | 100% data integrity |
| Extreme Pressure | 30GB | System limits testing | Graceful degradation |

## System Requirements

- **Memory**: 18GB (M3 Mac)
- **Storage**: 50GB+ free space
- **Time**: 15-30 minutes
- **Network**: None required

## Key Success Metrics

### ✅ Excellent Performance
- Writes >25GB without crashes
- Maintains >100 ops/sec under memory pressure
- Zero data corruption
- System remains responsive

### ✅ Good Performance  
- Completes all tests successfully
- Graceful performance degradation
- >95% data integrity
- Quick recovery after test

### ⚠️ Acceptable Performance
- Tests complete with some impact
- System stable under pressure
- Minor performance degradation
- No data corruption

## What to Watch

### Memory Usage
- **Free Memory**: Should drop to <1GB
- **Swap Usage**: Should increase to 5-15GB
- **Memory Pressure**: System pressure indicators

### Performance
- **Write Rate**: Starts high, degrades under swap
- **Read Rate**: Depends on cache hit ratio
- **Latency**: Increases significantly under swap

### System Health
- **CPU Usage**: Should remain <90%
- **Load Average**: May spike during pressure
- **Disk I/O**: Heavy activity during swap

## Monitoring Output

The test provides real-time updates:
```
[12:34:56] Free: 2048MB | Wired: 8192MB | Load: 2.5 | Test PID: 12345 (1024MB) | Swap: used = 8.2G
```

## Expected Timeline

1. **Minutes 0-5**: Normal performance, building dataset
2. **Minutes 5-15**: Memory pressure begins, performance degrades
3. **Minutes 15-25**: Heavy swap usage, significant latency
4. **Minutes 25-30**: Test completion, system recovery

## Troubleshooting

### Test Fails to Start
```bash
# Check disk space
df -h

# Check system memory
system_profiler SPHardwareDataType | grep Memory

# Verify compilation
cargo check --bin large_scale_memory_test
```

### System Becomes Slow
- **Normal behavior** during memory pressure
- Monitor swap usage: `sysctl vm.swapusage`
- Force quit if needed: `pkill -f large_scale_memory_test`

### Out of Disk Space
- Clean up test data: `rm -rf ./zverse_data`
- Clean up logs: `rm -rf ./test_logs`
- Check available space: `df -h`

## Results Analysis

### Log Files (in `./test_logs/`)
- `memory_usage_*.log`: Memory and swap statistics
- `cpu_usage_*.log`: CPU utilization data
- `process_stats_*.log`: Test process resource usage
- `system_overview_*.log`: System configuration

### Quick Analysis Commands
```bash
# Peak memory pressure
tail -n +2 test_logs/memory_usage_*.log | awk -F',' '{print $8}' | sort -n | tail -1

# Peak CPU usage
tail -n +2 test_logs/cpu_usage_*.log | awk -F',' '{print $2+$3}' | sort -n | tail -1

# Peak process memory
tail -n +2 test_logs/process_stats_*.log | awk -F',' '{print $5}' | sort -n | tail -1
```

## Safety Notes

- **No user data is modified** - all test data is temporary
- **System won't crash** - built-in safety limits
- **Can be interrupted** - Ctrl+C stops safely
- **Auto-cleanup** - temporary files removed on completion

## Performance Expectations

### Normal Operation (No Memory Pressure)
- Writes: 1,000+ ops/sec
- Reads: 10,000+ ops/sec  
- Latency: <1ms P99

### Under Memory Pressure (>18GB dataset)
- Writes: 50-500 ops/sec
- Reads: 100-5,000 ops/sec
- Latency: 10-100ms P99

### Extreme Pressure (>25GB dataset)  
- Writes: 10-100 ops/sec
- Reads: 50-1,000 ops/sec
- Latency: 100ms+ P99

This test demonstrates ZVerse's ability to handle datasets far exceeding available RAM while maintaining data integrity and reasonable performance under extreme memory pressure conditions.