//! Diagnostic Test for ZVerse Compaction and Memory Mapping Issues
//!
//! This test investigates why ZVerse fails at exactly system memory limits
//! rather than paging data to disk as designed.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use std::fs;
use zverse::master_class::{MasterClassZVerse, MasterClassConfig, SyncStrategy};

fn main() {
    println!("=== ZVerse Compaction and Memory Mapping Diagnostic ===\n");
    
    // Clean test environment
    let _ = fs::remove_dir_all("./zverse_debug");
    fs::create_dir_all("./zverse_debug").expect("Failed to create test directory");
    
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
    
    // Test 1: Check if compaction workers are actually running
    test_compaction_workers_active();
    
    // Test 2: Monitor ring buffer states
    test_ring_buffer_monitoring();
    
    // Test 3: Check actual file system usage vs memory usage
    test_memory_vs_disk_usage();
    
    // Test 4: Test tier migration behavior
    test_tier_data_flow();
    
    // Test 5: Stress test compaction under load
    test_compaction_under_load();
    
    println!("=== Diagnostic Complete ===");
}

fn test_compaction_workers_active() {
    println!("1. Testing Compaction Worker Activity");
    
    let config = MasterClassConfig {
        data_path: "./zverse_debug/compaction_test".to_string(),
        sync_strategy: SyncStrategy::OnThreshold(1000),
        compaction_threads: 4,
        segment_size: 64 * 1024 * 1024,
    };
    
    let db = Arc::new(MasterClassZVerse::new(config).expect("Failed to create database"));
    
    // Add data to trigger compaction
    println!("   Adding 10,000 entries to trigger compaction...");
    let start = Instant::now();
    
    for i in 0..10_000 {
        let key = format!("compaction-test-{:06}", i);
        let value = vec![0xAB; 1024]; // 1KB values
        
        match db.put(key.as_bytes(), &value) {
            Ok(_) => {},
            Err(e) => {
                println!("   ❌ Write failed at entry {}: {}", i, e);
                break;
            }
        }
        
        // Check for early compaction failures
        if i % 1000 == 0 && i > 0 {
            println!("   Progress: {} entries written", i);
        }
    }
    
    let write_time = start.elapsed();
    println!("   Write phase completed in {:.2}s", write_time.as_secs_f64());
    
    // Give compaction time to work
    println!("   Waiting 5 seconds for compaction to process...");
    thread::sleep(Duration::from_secs(5));
    
    // Check if any files were created (indicating compaction activity)
    match fs::read_dir("./zverse_debug/compaction_test") {
        Ok(entries) => {
            let file_count = entries.count();
            if file_count > 0 {
                println!("   ✅ Found {} files in data directory - compaction may be working", file_count);
            } else {
                println!("   ❌ No files found in data directory - compaction not creating output");
            }
        }
        Err(_) => {
            println!("   ❌ Data directory doesn't exist - compaction not working");
        }
    }
    
    // Test read-back to see if data is accessible
    println!("   Testing data accessibility...");
    let mut successful_reads = 0;
    for i in 0..100 {
        let key = format!("compaction-test-{:06}", i);
        if db.get(key.as_bytes(), None).unwrap_or(None).is_some() {
            successful_reads += 1;
        }
    }
    println!("   Read success rate: {}/100 entries", successful_reads);
    println!();
}

fn test_ring_buffer_monitoring() {
    println!("2. Testing Ring Buffer State Monitoring");
    
    let config = MasterClassConfig {
        data_path: "./zverse_debug/ring_buffer_test".to_string(),
        sync_strategy: SyncStrategy::OnThreshold(500), // Lower threshold
        compaction_threads: 2,
        segment_size: 32 * 1024 * 1024,
    };
    
    let db = Arc::new(MasterClassZVerse::new(config).expect("Failed to create database"));
    
    println!("   Monitoring ring buffer fill rate...");
    
    let mut entries_written = 0;
    let start = Instant::now();
    
    // Write until we hit an error or limit
    for i in 0..100_000 {
        let key = format!("ring-buffer-{:08}", i);
        let value = vec![0xCD; 2048]; // 2KB values
        
        match db.put(key.as_bytes(), &value) {
            Ok(_) => {
                entries_written += 1;
                
                if i % 1000 == 0 && i > 0 {
                    let elapsed = start.elapsed();
                    let rate = i as f64 / elapsed.as_secs_f64();
                    println!("   Entries: {}, Rate: {:.0} ops/sec", i, rate);
                }
            }
            Err(e) => {
                println!("   ❌ Ring buffer error at entry {}: {}", i, e);
                if e.to_string().contains("Queue full") || e.to_string().contains("full") {
                    println!("   📊 Ring buffer capacity reached at {} entries", i);
                    break;
                }
            }
        }
    }
    
    println!("   Total entries written: {}", entries_written);
    println!("   Ring buffer capacity appears to be: ~{} entries", entries_written);
    println!();
}

fn test_memory_vs_disk_usage() {
    println!("3. Testing Memory vs Disk Usage");
    
    let config = MasterClassConfig {
        data_path: "./zverse_debug/memory_disk_test".to_string(),
        sync_strategy: SyncStrategy::Immediate,
        compaction_threads: 4,
        segment_size: 128 * 1024 * 1024,
    };
    
    let db = MasterClassZVerse::new(config).expect("Failed to create database");
    
    // Get initial memory usage
    let initial_memory = get_process_memory_mb();
    println!("   Initial process memory: {:.1} MB", initial_memory);
    
    // Write data in chunks and monitor memory vs disk
    let chunk_size = 1000;
    let value_size = 10 * 1024; // 10KB values
    
    for chunk in 0..20 {
        println!("   Writing chunk {}...", chunk + 1);
        
        for i in 0..chunk_size {
            let key = format!("memory-disk-{:04}-{:06}", chunk, i);
            let value = vec![0xEF; value_size];
            
            if let Err(e) = db.put(key.as_bytes(), &value) {
                println!("   ❌ Failed at chunk {}, entry {}: {}", chunk, i, e);
                break;
            }
        }
        
        // Check memory and disk usage
        let current_memory = get_process_memory_mb();
        let disk_usage = get_directory_size_mb("./zverse_debug/memory_disk_test");
        let data_written_mb = ((chunk + 1) * chunk_size * value_size) as f64 / (1024.0 * 1024.0);
        
        println!("   Memory: {:.1} MB (+{:.1}), Disk: {:.1} MB, Data written: {:.1} MB", 
                 current_memory, current_memory - initial_memory, disk_usage, data_written_mb);
        
        // If memory growth equals data written, we're not paging to disk
        let memory_growth = current_memory - initial_memory;
        if memory_growth > data_written_mb * 0.8 {
            println!("   ⚠️  Memory growth ({:.1} MB) suggests data staying in memory, not paging to disk", memory_growth);
        }
        
        thread::sleep(Duration::from_millis(500)); // Let compaction catch up
    }
    
    println!();
}

fn test_tier_data_flow() {
    println!("4. Testing Three-Tier Data Flow");
    
    let config = MasterClassConfig {
        data_path: "./zverse_debug/tier_test".to_string(),
        sync_strategy: SyncStrategy::OnThreshold(100),
        compaction_threads: 6,
        segment_size: 64 * 1024 * 1024,
    };
    
    let db = MasterClassZVerse::new(config).expect("Failed to create database");
    
    // Phase 1: Fill hot tier
    println!("   Phase 1: Writing to hot tier...");
    for i in 0..500 {
        let key = format!("tier-hot-{:04}", i);
        let value = vec![0x01; 1024];
        if let Err(e) = db.put(key.as_bytes(), &value) {
            println!("   ❌ Hot tier write failed at {}: {}", i, e);
            break;
        }
    }
    
    // Check immediate availability (should be in hot tier)
    let mut hot_reads = 0;
    for i in 0..100 {
        let key = format!("tier-hot-{:04}", i);
        if db.get(key.as_bytes(), None).unwrap_or(None).is_some() {
            hot_reads += 1;
        }
    }
    println!("   Hot tier read success: {}/100", hot_reads);
    
    // Phase 2: Wait for compaction (move to warm tier)
    println!("   Phase 2: Waiting for compaction to warm tier...");
    thread::sleep(Duration::from_secs(3));
    
    // Phase 3: Write more data to trigger further compaction
    println!("   Phase 3: Writing additional data to trigger cold tier...");
    for i in 500..1000 {
        let key = format!("tier-cold-{:04}", i);
        let value = vec![0x02; 1024];
        if let Err(e) = db.put(key.as_bytes(), &value) {
            println!("   ❌ Cold tier write failed at {}: {}", i, e);
            break;
        }
    }
    
    // Test data availability across tiers
    thread::sleep(Duration::from_secs(2));
    
    let mut old_data_reads = 0;
    let mut new_data_reads = 0;
    
    for i in 0..50 {
        let hot_key = format!("tier-hot-{:04}", i);
        let cold_key = format!("tier-cold-{:04}", i + 500);
        
        if db.get(hot_key.as_bytes(), None).unwrap_or(None).is_some() {
            old_data_reads += 1;
        }
        if db.get(cold_key.as_bytes(), None).unwrap_or(None).is_some() {
            new_data_reads += 1;
        }
    }
    
    println!("   Old data (should be in cold tier): {}/50", old_data_reads);
    println!("   New data (should be in hot tier): {}/50", new_data_reads);
    
    if old_data_reads < 25 {
        println!("   ❌ Significant data loss - tier migration not working properly");
    } else if old_data_reads == 50 && new_data_reads == 50 {
        println!("   ✅ All data accessible - tier system functioning");
    } else {
        println!("   ⚠️  Partial data loss - tier system has issues");
    }
    
    println!();
}

fn test_compaction_under_load() {
    println!("5. Testing Compaction Under Heavy Load");
    
    let config = MasterClassConfig {
        data_path: "./zverse_debug/load_test".to_string(),
        sync_strategy: SyncStrategy::OnThreshold(50), // Very aggressive compaction
        compaction_threads: 8,
        segment_size: 32 * 1024 * 1024,
    };
    
    let db = Arc::new(MasterClassZVerse::new(config).expect("Failed to create database"));
    
    let stop_flag = Arc::new(AtomicBool::new(false));
    let writes_completed = Arc::new(AtomicUsize::new(0));
    let errors_encountered = Arc::new(AtomicUsize::new(0));
    
    // Spawn multiple writer threads
    let mut handles = vec![];
    
    for thread_id in 0..4 {
        let db_clone = db.clone();
        let stop_flag_clone = stop_flag.clone();
        let writes_clone = writes_completed.clone();
        let errors_clone = errors_encountered.clone();
        
        let handle = thread::spawn(move || {
            let mut local_writes = 0;
            let mut local_errors = 0;
            let mut counter = 0;
            
            while !stop_flag_clone.load(Ordering::Relaxed) {
                let key = format!("load-test-{:02}-{:08}", thread_id, counter);
                let value = vec![(counter % 256) as u8; 4096]; // 4KB values
                
                match db_clone.put(key.as_bytes(), &value) {
                    Ok(_) => {
                        local_writes += 1;
                        writes_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(e) => {
                        local_errors += 1;
                        errors_clone.fetch_add(1, Ordering::Relaxed);
                        
                        if e.to_string().contains("Queue full") || e.to_string().contains("full") {
                            // This is the key error we're investigating
                            println!("   📊 Thread {} hit queue full at write {}", thread_id, counter);
                            break;
                        }
                    }
                }
                
                counter += 1;
                
                // Brief pause to prevent overwhelming the system
                if counter % 100 == 0 {
                    thread::sleep(Duration::from_micros(100));
                }
            }
            
            (local_writes, local_errors)
        });
        
        handles.push(handle);
    }
    
    // Monitor progress for 30 seconds
    let start = Instant::now();
    let monitor_duration = Duration::from_secs(30);
    
    while start.elapsed() < monitor_duration {
        thread::sleep(Duration::from_secs(2));
        
        let total_writes = writes_completed.load(Ordering::Relaxed);
        let total_errors = errors_encountered.load(Ordering::Relaxed);
        let elapsed = start.elapsed().as_secs_f64();
        
        println!("   Progress: {} writes, {} errors, {:.0} writes/sec", 
                 total_writes, total_errors, total_writes as f64 / elapsed);
        
        if total_errors > total_writes / 10 {
            println!("   ❌ High error rate detected - stopping test early");
            break;
        }
    }
    
    stop_flag.store(true, Ordering::Relaxed);
    
    // Collect results
    let mut total_writes = 0;
    let mut total_errors = 0;
    
    for handle in handles {
        let (writes, errors) = handle.join().unwrap();
        total_writes += writes;
        total_errors += errors;
    }
    
    println!("   Final results: {} writes, {} errors", total_writes, total_errors);
    
    if total_errors == 0 {
        println!("   ✅ Compaction handled load perfectly");
    } else if total_errors < total_writes / 20 {
        println!("   ⚠️  Some compaction pressure but mostly functional");
    } else {
        println!("   ❌ Compaction cannot keep up with write load");
    }
    
    // Check final disk usage
    let disk_usage = get_directory_size_mb("./zverse_debug/load_test");
    println!("   Final disk usage: {:.1} MB", disk_usage);
    
    if disk_usage < 10.0 {
        println!("   ❌ Very low disk usage suggests compaction not writing to disk");
    }
    
    println!();
}

fn get_process_memory_mb() -> f64 {
    // Simple memory estimation - in a real implementation, use a proper memory monitoring library
    if let Ok(output) = std::process::Command::new("ps")
        .args(&["-o", "rss=", "-p", &std::process::id().to_string()])
        .output() {
        if let Ok(memory_str) = String::from_utf8(output.stdout) {
            if let Ok(memory_kb) = memory_str.trim().parse::<f64>() {
                return memory_kb / 1024.0; // Convert KB to MB
            }
        }
    }
    0.0
}

fn get_directory_size_mb(path: &str) -> f64 {
    if let Ok(metadata) = fs::metadata(path) {
        if metadata.is_dir() {
            let mut total_size = 0u64;
            if let Ok(entries) = fs::read_dir(path) {
                for entry in entries.flatten() {
                    if let Ok(metadata) = entry.metadata() {
                        total_size += metadata.len();
                    }
                }
            }
            return total_size as f64 / (1024.0 * 1024.0);
        }
    }
    0.0
}