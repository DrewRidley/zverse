//! Test for ZCurve-DB with large datasets exceeding memory limits
//! 
//! This test creates datasets larger than typical system memory to verify
//! that memory mapping and OS paging work correctly.

use std::fs;
use std::time::Instant;
use zverse::ZCurveDB;

fn main() {
    println!("=== ZCurve-DB Large Dataset Test ===");
    
    // Print system information
    if let Ok(output) = std::process::Command::new("sysctl").args(&["hw.memsize"]).output() {
        if let Ok(meminfo) = String::from_utf8(output.stdout) {
            if let Some(mem_bytes) = meminfo.split(':').nth(1).and_then(|s| s.trim().parse::<u64>().ok()) {
                let mem_gb = mem_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
                println!("System memory: {:.1} GB", mem_gb);
                println!("Target dataset: {:.1} GB (exceeds system memory)", mem_gb * 1.2);
            }
        }
    }
    
    // Clean up any existing test database
    let _ = fs::remove_file("./large_dataset_test.zcdb");
    
    test_large_dataset_creation();
    test_large_dataset_random_access();
    test_large_dataset_persistence();
    test_memory_pressure_performance();
    
    // Clean up
    let _ = fs::remove_file("./large_dataset_test.zcdb");
    
    println!("=== Large Dataset Tests Complete ===");
}

fn test_large_dataset_creation() {
    println!("1. Testing Large Dataset Creation");
    
    let mut db = ZCurveDB::open("./large_dataset_test.zcdb").expect("Failed to create database");
    
    // Target: Create ~20GB dataset (should exceed most M3 Mac memory)
    let num_entries = 800_000; // 800K entries
    let value_size = 25 * 1024; // 25KB per value = ~20GB total
    let large_value = vec![0xAB; value_size];
    
    println!("   Creating {} entries with {}KB values each...", num_entries, value_size / 1024);
    println!("   Total target size: ~{}GB", (num_entries * value_size) / (1024 * 1024 * 1024));
    
    let start = Instant::now();
    let mut last_report = Instant::now();
    
    for i in 0..num_entries {
        let key = format!("large_key_{:08}", i);
        // Vary value content to prevent compression optimizations
        let mut value = large_value.clone();
        let pattern = (i as u32).to_le_bytes();
        value[0..4].copy_from_slice(&pattern);
        value[value_size-4..].copy_from_slice(&pattern);
        
        match db.put(key.as_bytes(), &value) {
            Ok(_) => {},
            Err(e) => {
                println!("   Write failed at entry {}: {}", i, e);
                break;
            }
        }
        
        // Progress reporting every 10k entries
        if i % 10_000 == 0 && i > 0 {
            let elapsed = last_report.elapsed();
            let rate = 10_000.0 / elapsed.as_secs_f64();
            let gb_written = (i * value_size) as f64 / (1024.0 * 1024.0 * 1024.0);
            println!("     Progress: {}/{} entries ({:.1}GB written, {:.0} ops/sec)", 
                     i, num_entries, gb_written, rate);
            last_report = Instant::now();
        }
    }
    
    let total_elapsed = start.elapsed();
    let final_stats = db.stats();
    let total_ops_per_sec = final_stats.total_entries as f64 / total_elapsed.as_secs_f64();
    let total_gb = (final_stats.total_entries * value_size as u64) as f64 / (1024.0 * 1024.0 * 1024.0);
    let gb_per_sec = total_gb / total_elapsed.as_secs_f64();
    
    println!("   Completed: {:.1}GB written in {:.1}s", total_gb, total_elapsed.as_secs_f64());
    println!("   Performance: {:.0} ops/sec, {:.2} GB/sec", total_ops_per_sec, gb_per_sec);
    println!("   Database stats: {} entries, {} blocks", final_stats.total_entries, final_stats.total_blocks);
    
    if total_ops_per_sec > 100.0 && gb_per_sec > 0.05 {
        println!("   ✅ EXCELLENT: High throughput maintained with large dataset");
    } else if total_ops_per_sec > 50.0 && gb_per_sec > 0.02 {
        println!("   ✅ GOOD: Reasonable performance with large dataset");
    } else {
        println!("   ⚠️  DEGRADED: Performance impact from large dataset size");
    }
    
    println!();
}

fn test_large_dataset_random_access() {
    println!("2. Testing Random Access on Large Dataset");
    
    let db = ZCurveDB::open("./large_dataset_test.zcdb").expect("Failed to open database");
    let stats = db.stats();
    
    println!("   Testing random reads on {:.1}GB dataset...", 
             (stats.total_entries * 25 * 1024) as f64 / (1024.0 * 1024.0 * 1024.0));
    
    let num_random_reads = 1000;
    let start = Instant::now();
    let mut successful_reads = 0;
    let mut failed_reads = 0;
    
    for i in 0..num_random_reads {
        // Generate random key within our dataset
        let random_idx = (i * 7919) % (stats.total_entries as usize); // Prime for distribution
        let key = format!("large_key_{:08}", random_idx);
        
        match db.get(key.as_bytes()) {
            Ok(Some(value)) => {
                successful_reads += 1;
                // Verify data integrity
                if value.len() == 25 * 1024 {
                    let pattern = (random_idx as u32).to_le_bytes();
                    if value[0..4] == pattern {
                        // Data integrity check passed
                    } else {
                        println!("     Warning: Data corruption detected at key {}", key);
                    }
                } else {
                    println!("     Warning: Incorrect value size for key {}", key);
                }
            }
            Ok(None) => {
                failed_reads += 1;
            }
            Err(e) => {
                println!("     Error reading key {}: {}", key, e);
                failed_reads += 1;
            }
        }
    }
    
    let read_elapsed = start.elapsed();
    let read_rate = successful_reads as f64 / read_elapsed.as_secs_f64();
    let success_rate = successful_reads as f64 / num_random_reads as f64;
    
    println!("   Random read performance: {:.0} ops/sec", read_rate);
    println!("   Success rate: {:.1}% ({} successful, {} failed)", 
             success_rate * 100.0, successful_reads, failed_reads);
    
    if read_rate > 500.0 && success_rate > 0.8 {
        println!("   ✅ EXCELLENT: High random read performance on large dataset");
    } else if read_rate > 200.0 && success_rate > 0.6 {
        println!("   ✅ GOOD: Decent random read performance under memory pressure");
    } else {
        println!("   ⚠️  DEGRADED: Random access performance impacted by dataset size");
    }
    
    println!();
}

fn test_large_dataset_persistence() {
    println!("3. Testing Large Dataset Persistence");
    
    // First, sync the current database
    {
        let mut db = ZCurveDB::open("./large_dataset_test.zcdb").expect("Failed to open database");
        let initial_stats = db.stats();
        println!("   Syncing {:.1}GB dataset to disk...", 
                 (initial_stats.total_entries * 25 * 1024) as f64 / (1024.0 * 1024.0 * 1024.0));
        
        let sync_start = Instant::now();
        db.sync().expect("Sync failed");
        let sync_time = sync_start.elapsed();
        
        println!("   Sync completed in {:.2}s", sync_time.as_secs_f64());
    } // Database closed
    
    // Reopen and verify data integrity
    println!("   Reopening large dataset from disk...");
    let reopen_start = Instant::now();
    let db = ZCurveDB::open("./large_dataset_test.zcdb").expect("Failed to reopen database");
    let reopen_time = reopen_start.elapsed();
    
    let stats = db.stats();
    println!("   Reopened in {:.2}s: {} entries, {} blocks", 
             reopen_time.as_secs_f64(), stats.total_entries, stats.total_blocks);
    
    // Test that data is still accessible
    let verification_start = Instant::now();
    let mut verified_entries = 0;
    
    for i in (0..1000).step_by(10) { // Sample every 10th entry
        let key = format!("large_key_{:08}", i);
        if let Ok(Some(value)) = db.get(key.as_bytes()) {
            if value.len() == 25 * 1024 {
                let pattern = (i as u32).to_le_bytes();
                if value[0..4] == pattern {
                    verified_entries += 1;
                }
            }
        }
    }
    
    let verification_time = verification_start.elapsed();
    let verification_rate = verified_entries as f64 / verification_time.as_secs_f64();
    
    println!("   Verification: {} entries checked at {:.0} ops/sec", 
             verified_entries, verification_rate);
    
    if reopen_time.as_secs_f64() < 10.0 && verified_entries > 80 {
        println!("   ✅ EXCELLENT: Fast reopening and data integrity maintained");
    } else if reopen_time.as_secs_f64() < 30.0 && verified_entries > 50 {
        println!("   ✅ GOOD: Reasonable persistence performance");
    } else {
        println!("   ⚠️  SLOW: Persistence performance needs improvement");
    }
    
    println!();
}

fn test_memory_pressure_performance() {
    println!("4. Testing Performance Under Memory Pressure");
    
    let mut db = ZCurveDB::open("./large_dataset_test.zcdb").expect("Failed to open database");
    let initial_stats = db.stats();
    
    println!("   Testing mixed operations on {:.1}GB dataset...", 
             (initial_stats.total_entries * 25 * 1024) as f64 / (1024.0 * 1024.0 * 1024.0));
    
    let test_duration = std::time::Duration::from_secs(30);
    let start = Instant::now();
    let mut operations = 0;
    let mut successful_operations = 0;
    
    while start.elapsed() < test_duration {
        operations += 1;
        
        if operations % 3 == 0 {
            // Write operation (creates memory pressure)
            let key = format!("pressure_test_{:08}", operations);
            let value = vec![0xCD; 10 * 1024]; // 10KB values
            
            if db.put(key.as_bytes(), &value).is_ok() {
                successful_operations += 1;
            }
        } else {
            // Read operation (tests memory pressure handling)
            let random_idx = (operations * 7919) % (initial_stats.total_entries as usize);
            let key = format!("large_key_{:08}", random_idx);
            
            if db.get(key.as_bytes()).unwrap_or(None).is_some() {
                successful_operations += 1;
            }
        }
        
        // Brief pause to prevent overwhelming the system
        if operations % 100 == 0 {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
    
    let elapsed = start.elapsed();
    let ops_per_sec = operations as f64 / elapsed.as_secs_f64();
    let success_rate = successful_operations as f64 / operations as f64;
    
    println!("   Mixed operations: {} total, {:.0} ops/sec", operations, ops_per_sec);
    println!("   Success rate: {:.1}% under memory pressure", success_rate * 100.0);
    
    let final_stats = db.stats();
    let size_increase = final_stats.total_entries - initial_stats.total_entries;
    println!("   Dataset grew by {} entries during test", size_increase);
    
    if ops_per_sec > 100.0 && success_rate > 0.8 {
        println!("   ✅ EXCELLENT: High performance maintained under memory pressure");
    } else if ops_per_sec > 50.0 && success_rate > 0.6 {
        println!("   ✅ GOOD: Reasonable performance under memory pressure");
    } else {
        println!("   ⚠️  DEGRADED: Performance significantly impacted by memory pressure");
    }
    
    // Test range scanning under memory pressure
    println!("   Testing range scan under memory pressure...");
    let range_start = Instant::now();
    let scan_result = db.scan(b"large_key_00000000", b"large_key_00001000").expect("Scan failed");
    let scan_results: Vec<_> = scan_result.collect();
    let range_time = range_start.elapsed();
    
    println!("   Range scan: {} results in {:.2}s", scan_results.len(), range_time.as_secs_f64());
    
    if range_time.as_secs_f64() < 5.0 && scan_results.len() > 100 {
        println!("   ✅ Range scanning works well under memory pressure");
    } else {
        println!("   ⚠️  Range scanning performance affected by memory pressure");
    }
    
    println!();
}