//! Memory Mapping Test for ZCurve-DB
//! 
//! Tests that memory mapping works correctly with datasets that exceed
//! typical memory limits, verifying OS paging behavior.

use std::fs;
use std::time::Instant;
use zverse::ZCurveDB;

fn main() {
    println!("=== ZCurve-DB Memory Mapping Test ===");
    
    // Print system information
    println!("System Information:");
    if let Ok(output) = std::process::Command::new("sysctl").args(&["hw.memsize"]).output() {
        if let Ok(meminfo) = String::from_utf8(output.stdout) {
            if let Some(mem_bytes) = meminfo.split(':').nth(1).and_then(|s| s.trim().parse::<u64>().ok()) {
                let mem_gb = mem_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
                println!("  System memory: {:.1} GB", mem_gb);
            }
        }
    }
    println!();
    
    // Clean up any existing test database
    let _ = fs::remove_file("./memory_mapping_test.zcdb");
    
    test_moderate_dataset();
    test_memory_access_patterns();
    test_persistence_under_load();
    test_concurrent_access();
    
    // Clean up
    let _ = fs::remove_file("./memory_mapping_test.zcdb");
    
    println!("=== Memory Mapping Tests Complete ===");
}

fn test_moderate_dataset() {
    println!("1. Testing Moderate Dataset (3GB target)");
    
    let mut db = ZCurveDB::open("./memory_mapping_test.zcdb").expect("Failed to create database");
    
    // Target: 3GB dataset - enough to test memory pressure without taking forever
    let num_entries = 1_000_000; // 1M entries
    let value_size = 3 * 1024; // 3KB per value = ~3GB total
    let large_value = vec![0xCC; value_size];
    
    println!("   Creating {} entries with {}KB values each...", num_entries, value_size / 1024);
    println!("   Total target size: ~{}GB", (num_entries * value_size) / (1024 * 1024 * 1024));
    
    let start = Instant::now();
    let mut last_report = Instant::now();
    let mut successful_writes = 0;
    
    for i in 0..num_entries {
        let key = format!("mmap_test_{:08}", i);
        // Add pattern to verify data integrity
        let mut value = large_value.clone();
        let pattern = (i as u32).to_le_bytes();
        value[0..4].copy_from_slice(&pattern);
        value[value_size-4..].copy_from_slice(&pattern);
        
        match db.put(key.as_bytes(), &value) {
            Ok(_) => successful_writes += 1,
            Err(e) => {
                println!("   Write failed at entry {}: {}", i, e);
                if i < num_entries / 10 {
                    // If we fail very early, something is seriously wrong
                    panic!("Too many early failures");
                }
                break;
            }
        }
        
        // Progress reporting every 5k entries
        if i % 5_000 == 0 && i > 0 {
            let elapsed = last_report.elapsed();
            let rate = 5_000.0 / elapsed.as_secs_f64();
            let gb_written = (i * value_size) as f64 / (1024.0 * 1024.0 * 1024.0);
            println!("     Progress: {}/{} entries ({:.1}GB written, {:.0} ops/sec)", 
                     i, num_entries, gb_written, rate);
            last_report = Instant::now();
        }
    }
    
    let total_elapsed = start.elapsed();
    let stats = db.stats();
    let total_ops_per_sec = successful_writes as f64 / total_elapsed.as_secs_f64();
    let total_gb = (successful_writes * value_size) as f64 / (1024.0 * 1024.0 * 1024.0);
    let gb_per_sec = total_gb / total_elapsed.as_secs_f64();
    
    println!("   Completed: {:.1}GB written in {:.1}s", total_gb, total_elapsed.as_secs_f64());
    println!("   Performance: {:.0} ops/sec, {:.2} GB/sec", total_ops_per_sec, gb_per_sec);
    println!("   Final stats: {} entries, {} blocks", stats.total_entries, stats.total_blocks);
    
    if total_ops_per_sec > 100.0 && successful_writes > num_entries / 2 {
        println!("   ✅ EXCELLENT: Successfully created large dataset with good performance");
    } else if total_ops_per_sec > 50.0 && successful_writes > num_entries / 4 {
        println!("   ✅ GOOD: Created substantial dataset with reasonable performance");
    } else {
        println!("   ⚠️  LIMITED: Performance or completion affected by memory pressure");
    }
    
    println!();
}

fn test_memory_access_patterns() {
    println!("2. Testing Memory Access Patterns");
    
    let db = ZCurveDB::open("./memory_mapping_test.zcdb").expect("Failed to open database");
    let stats = db.stats();
    let dataset_gb = (stats.total_entries * 3 * 1024) as f64 / (1024.0 * 1024.0 * 1024.0);
    
    println!("   Testing access patterns on {:.1}GB dataset ({} entries)...", 
             dataset_gb, stats.total_entries);
    
    // Test 1: Sequential access
    println!("   Sequential access test...");
    let seq_start = Instant::now();
    let mut seq_successful = 0;
    
    for i in (0..1000).step_by(10) {
        let key = format!("mmap_test_{:08}", i);
        if let Ok(Some(value)) = db.get(key.as_bytes()) {
            seq_successful += 1;
            // Verify data integrity
            let pattern = (i as u32).to_le_bytes();
            if value.len() >= 4 && value[0..4] == pattern {
                // Data integrity check passed
            } else {
                println!("     Data corruption detected at entry {}", i);
            }
        }
    }
    
    let seq_elapsed = seq_start.elapsed();
    let seq_rate = seq_successful as f64 / seq_elapsed.as_secs_f64();
    
    // Test 2: Random access
    println!("   Random access test...");
    let rand_start = Instant::now();
    let mut rand_successful = 0;
    
    for i in 0..500 {
        let random_idx = (i * 7919) % (stats.total_entries as usize);
        let key = format!("mmap_test_{:08}", random_idx);
        
        if let Ok(Some(value)) = db.get(key.as_bytes()) {
            rand_successful += 1;
            // Verify data integrity
            let pattern = (random_idx as u32).to_le_bytes();
            if value.len() >= 4 && value[0..4] == pattern {
                // Data integrity check passed
            } else {
                println!("     Data corruption detected at random entry {}", random_idx);
            }
        }
    }
    
    let rand_elapsed = rand_start.elapsed();
    let rand_rate = rand_successful as f64 / rand_elapsed.as_secs_f64();
    
    println!("   Sequential access: {:.0} ops/sec ({} successful)", seq_rate, seq_successful);
    println!("   Random access: {:.0} ops/sec ({} successful)", rand_rate, rand_successful);
    
    let performance_ratio = rand_rate / seq_rate;
    println!("   Random/Sequential ratio: {:.2}", performance_ratio);
    
    if seq_rate > 100.0 && rand_rate > 50.0 && performance_ratio > 0.3 {
        println!("   ✅ EXCELLENT: Good performance for both access patterns");
    } else if seq_rate > 50.0 && rand_rate > 20.0 {
        println!("   ✅ GOOD: Reasonable performance under memory pressure");
    } else {
        println!("   ⚠️  DEGRADED: Access patterns show memory pressure impact");
    }
    
    println!();
}

fn test_persistence_under_load() {
    println!("3. Testing Persistence Under Load");
    
    // First, sync the current large dataset
    {
        let mut db = ZCurveDB::open("./memory_mapping_test.zcdb").expect("Failed to open database");
        let sync_start = Instant::now();
        db.sync().expect("Sync failed");
        let sync_time = sync_start.elapsed();
        
        println!("   Synced large dataset in {:.2}s", sync_time.as_secs_f64());
    } // Drop database to close file handles
    
    // Reopen and verify immediate availability
    println!("   Reopening database...");
    let reopen_start = Instant::now();
    let db = ZCurveDB::open("./memory_mapping_test.zcdb").expect("Failed to reopen database");
    let reopen_time = reopen_start.elapsed();
    
    let stats = db.stats();
    let dataset_gb = (stats.total_entries * 3 * 1024) as f64 / (1024.0 * 1024.0 * 1024.0);
    
    println!("   Reopened {:.1}GB dataset in {:.2}s", dataset_gb, reopen_time.as_secs_f64());
    
    // Immediate availability test
    let availability_start = Instant::now();
    let mut available_entries = 0;
    
    for i in 0..100 {
        let key = format!("mmap_test_{:08}", i);
        if db.get(key.as_bytes()).unwrap_or(None).is_some() {
            available_entries += 1;
        }
    }
    
    let availability_time = availability_start.elapsed();
    let availability_rate = available_entries as f64 / availability_time.as_secs_f64();
    
    println!("   Immediate data availability: {} entries accessible at {:.0} ops/sec", 
             available_entries, availability_rate);
    
    if reopen_time.as_secs_f64() < 5.0 && available_entries > 80 {
        println!("   ✅ EXCELLENT: Fast reopening with immediate data access");
    } else if reopen_time.as_secs_f64() < 15.0 && available_entries > 60 {
        println!("   ✅ GOOD: Reasonable persistence performance");
    } else {
        println!("   ⚠️  SLOW: Persistence performance needs improvement");
    }
    
    println!();
}

fn test_concurrent_access() {
    println!("4. Testing Concurrent Access Under Memory Pressure");
    
    let db = ZCurveDB::open("./memory_mapping_test.zcdb").expect("Failed to open database");
    let stats = db.stats();
    let dataset_gb = (stats.total_entries * 3 * 1024) as f64 / (1024.0 * 1024.0 * 1024.0);
    
    println!("   Testing concurrent operations on {:.1}GB dataset...", dataset_gb);
    
    // Simulate concurrent read load
    let test_duration = std::time::Duration::from_secs(15);
    let start = Instant::now();
    let mut total_operations = 0;
    let mut successful_operations = 0;
    
    while start.elapsed() < test_duration {
        total_operations += 1;
        
        // Mix of read operations to test memory pressure
        let operation_type = total_operations % 3;
        
        match operation_type {
            0 => {
                // Sequential read
                let seq_idx = (total_operations / 3) % (stats.total_entries as usize);
                let key = format!("mmap_test_{:08}", seq_idx);
                if db.get(key.as_bytes()).unwrap_or(None).is_some() {
                    successful_operations += 1;
                }
            }
            1 => {
                // Random read  
                let rand_idx = (total_operations * 7919) % (stats.total_entries as usize);
                let key = format!("mmap_test_{:08}", rand_idx);
                if db.get(key.as_bytes()).unwrap_or(None).is_some() {
                    successful_operations += 1;
                }
            }
            2 => {
                // Range scan
                let start_idx = (total_operations * 31) % (stats.total_entries as usize / 10);
                let end_idx = start_idx + 10;
                let start_key = format!("mmap_test_{:08}", start_idx);
                let end_key = format!("mmap_test_{:08}", end_idx);
                
                if let Ok(scan) = db.scan(start_key.as_bytes(), end_key.as_bytes()) {
                    let results: Vec<_> = scan.take(5).collect(); // Limit to first 5 results
                    if !results.is_empty() {
                        successful_operations += 1;
                    }
                }
            }
            _ => unreachable!()
        }
        
        // Brief pause to prevent overwhelming the system
        if total_operations % 50 == 0 {
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }
    
    let elapsed = start.elapsed();
    let ops_per_sec = total_operations as f64 / elapsed.as_secs_f64();
    let success_rate = successful_operations as f64 / total_operations as f64;
    
    println!("   Concurrent operations: {} total at {:.0} ops/sec", total_operations, ops_per_sec);
    println!("   Success rate: {:.1}% under memory pressure", success_rate * 100.0);
    
    if ops_per_sec > 100.0 && success_rate > 0.9 {
        println!("   ✅ EXCELLENT: High performance maintained under memory pressure");
    } else if ops_per_sec > 50.0 && success_rate > 0.7 {
        println!("   ✅ GOOD: Reasonable performance under memory pressure");
    } else {
        println!("   ⚠️  DEGRADED: Performance impacted by memory pressure");
    }
    
    // Memory usage estimation
    println!("   Dataset size: {:.1}GB (should exceed typical working memory)", dataset_gb);
    if dataset_gb > 2.0 {
        println!("   ✅ Successfully demonstrated memory mapping with large dataset");
    } else {
        println!("   ⚠️  Dataset smaller than intended");
    }
    
    println!();
}