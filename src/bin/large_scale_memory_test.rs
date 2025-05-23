use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use zverse::master_class::{MasterClassZVerse, MasterClassConfig};

fn main() {
    println!("=== ZVerse Large-Scale Memory Stress Test ===");
    println!("Target: Generate dataset exceeding available RAM (18GB system)");
    println!("Testing performance under memory pressure and swap conditions\n");
    
    // Test 1: Large dataset generation exceeding RAM
    test_out_of_memory_writes();
    
    // Test 2: Read performance under memory pressure
    test_out_of_memory_reads();
    
    // Test 3: Concurrent operations under memory pressure
    test_concurrent_memory_stress();
    
    // Test 4: Data integrity verification with large dataset
    test_large_dataset_integrity();
    
    // Test 5: Mixed workload under extreme memory pressure
    test_extreme_memory_mixed_workload();
    
    println!("=== Large-Scale Memory Stress Test Completed ===");
}

fn test_out_of_memory_writes() {
    println!("1. Out-of-Memory Write Performance Test");
    println!("   Generating 25GB+ dataset to exceed 18GB system RAM...");
    
    let mut config = MasterClassConfig::default();
    config.segment_size = 256 * 1024 * 1024; // 256MB segments for better memory mapping
    let db = MasterClassZVerse::new(config).expect("Failed to create database");
    
    // Target: 25GB of data (key + value)
    // Using 1MB values, need ~25,000 entries
    let large_value_size = 1024 * 1024; // 1MB per value
    let target_entries = 25_000; // ~25GB total
    let checkpoint_interval = 1_000;
    
    println!("   Target: {} entries × {}MB = ~{}GB dataset", 
             target_entries, large_value_size / (1024 * 1024), 
             target_entries * large_value_size / (1024 * 1024 * 1024));
    
    let overall_start = Instant::now();
    let mut checkpoint_times = vec![];
    let mut total_bytes_written = 0u64;
    
    for i in 0..target_entries {
        let checkpoint_start = if i % checkpoint_interval == 0 { Some(Instant::now()) } else { None };
        
        let key = format!("large-scale-{:08}", i);
        let value = generate_large_value(large_value_size, i);
        
        let write_start = Instant::now();
        db.put(key.as_bytes(), &value).expect("Put failed");
        let write_elapsed = write_start.elapsed();
        
        total_bytes_written += key.len() as u64 + value.len() as u64;
        
        if let Some(checkpoint_start) = checkpoint_start {
            let checkpoint_elapsed = checkpoint_start.elapsed();
            checkpoint_times.push((i, checkpoint_elapsed, total_bytes_written));
            
            let gb_written = total_bytes_written as f64 / (1024.0 * 1024.0 * 1024.0);
            let throughput_mbps = (checkpoint_interval * large_value_size) as f64 
                / (1024.0 * 1024.0) / checkpoint_elapsed.as_secs_f64();
            
            println!("   Checkpoint {}: {:.2}GB written, {:.1}MB/s, latency {:.2}µs", 
                     i / checkpoint_interval, gb_written, throughput_mbps, 
                     write_elapsed.as_micros());
            
            // Memory pressure indicator
            if i > 18_000 { // After ~18GB, should be hitting swap
                println!("     └─ Memory pressure zone: system likely using swap");
            }
        }
        
        // Brief pause every 100 operations to allow background processes
        if i % 100 == 0 {
            thread::sleep(Duration::from_micros(10));
        }
    }
    
    let overall_elapsed = overall_start.elapsed();
    let final_gb = total_bytes_written as f64 / (1024.0 * 1024.0 * 1024.0);
    let avg_throughput = total_bytes_written as f64 / (1024.0 * 1024.0) / overall_elapsed.as_secs_f64();
    let avg_ops_per_sec = target_entries as f64 / overall_elapsed.as_secs_f64();
    
    println!("   Final Results:");
    println!("   Total data written: {:.2}GB", final_gb);
    println!("   Total time: {:.1} seconds", overall_elapsed.as_secs_f64());
    println!("   Average throughput: {:.1}MB/s", avg_throughput);
    println!("   Average ops/sec: {:.0}", avg_ops_per_sec);
    
    if final_gb > 18.0 {
        println!("   ✅ SUCCESS: Exceeded system RAM capacity (18GB)");
    } else {
        println!("   ⚠️  WARNING: Did not exceed system RAM capacity");
    }
    
    if avg_ops_per_sec > 50.0 {
        println!("   ✅ EXCELLENT: Maintained good performance under memory pressure");
    } else if avg_ops_per_sec > 10.0 {
        println!("   ✅ GOOD: Reasonable performance under memory pressure");
    } else {
        println!("   ⚠️  OK: Performance degraded under memory pressure");
    }
    println!();
}

fn test_out_of_memory_reads() {
    println!("2. Out-of-Memory Read Performance Test");
    println!("   Reading from large dataset under memory pressure...");
    
    let mut config = MasterClassConfig::default();
    config.segment_size = 256 * 1024 * 1024;
    let db = Arc::new(MasterClassZVerse::new(config).expect("Failed to create database"));
    
    // First, ensure we have a large dataset (smaller than write test for time)
    let value_size = 512 * 1024; // 512KB values
    let num_entries = 10_000; // ~5GB dataset
    
    println!("   Pre-populating {}GB dataset...", 
             (num_entries * value_size) / (1024 * 1024 * 1024));
    
    for i in 0..num_entries {
        let key = format!("read-large-{:08}", i);
        let value = generate_large_value(value_size, i);
        db.put(key.as_bytes(), &value).expect("Put failed");
        
        if i % 1000 == 0 {
            print!(".");
            std::io::Write::flush(&mut std::io::stdout()).unwrap();
        }
    }
    println!(" Done!");
    
    // Now test random read performance
    println!("   Testing random read performance...");
    
    let num_reads = 5_000;
    let start = Instant::now();
    let mut successful_reads = 0;
    let mut total_bytes_read = 0u64;
    let mut read_times = vec![];
    
    for i in 0..num_reads {
        let key_idx = (i * 7919) % num_entries; // Prime number for good distribution
        let key = format!("read-large-{:08}", key_idx);
        
        let read_start = Instant::now();
        if let Ok(Some(value)) = db.get(key.as_bytes(), None) {
            let read_elapsed = read_start.elapsed();
            successful_reads += 1;
            total_bytes_read += value.len() as u64;
            read_times.push(read_elapsed.as_micros() as f64);
        }
        
        if i % 500 == 0 {
            let interim_elapsed = start.elapsed();
            let interim_rate = successful_reads as f64 / interim_elapsed.as_secs_f64();
            println!("   Progress: {}/{} reads, {:.0} reads/sec", i, num_reads, interim_rate);
        }
    }
    
    let elapsed = start.elapsed();
    let reads_per_sec = successful_reads as f64 / elapsed.as_secs_f64();
    let mb_per_sec = total_bytes_read as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
    let success_rate = successful_reads as f64 / num_reads as f64;
    
    // Calculate latency statistics
    read_times.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let p50 = read_times[read_times.len() / 2];
    let p95 = read_times[read_times.len() * 95 / 100];
    let p99 = read_times[read_times.len() * 99 / 100];
    
    println!("   Read Performance Results:");
    println!("   Successful reads: {}/{} ({:.1}%)", successful_reads, num_reads, success_rate * 100.0);
    println!("   Read rate: {:.0} reads/sec", reads_per_sec);
    println!("   Data throughput: {:.1}MB/s", mb_per_sec);
    println!("   Latency P50: {:.0}µs, P95: {:.0}µs, P99: {:.0}µs", p50, p95, p99);
    
    if reads_per_sec > 1000.0 && success_rate > 0.95 {
        println!("   ✅ EXCELLENT: High read performance under memory pressure");
    } else if reads_per_sec > 100.0 && success_rate > 0.90 {
        println!("   ✅ GOOD: Acceptable read performance under memory pressure");
    } else {
        println!("   ⚠️  NEEDS IMPROVEMENT: Read performance degraded significantly");
    }
    println!();
}

fn test_concurrent_memory_stress() {
    println!("3. Concurrent Operations Under Memory Pressure");
    println!("   Testing 8 writers + 16 readers with large dataset...");
    
    let mut config = MasterClassConfig::default();
    config.segment_size = 128 * 1024 * 1024;
    let db = Arc::new(MasterClassZVerse::new(config).expect("Failed to create database"));
    
    // Pre-populate with medium dataset
    let initial_entries = 5_000;
    let initial_value_size = 256 * 1024; // 256KB
    
    println!("   Pre-populating with {}MB initial dataset...", 
             (initial_entries * initial_value_size) / (1024 * 1024));
    
    for i in 0..initial_entries {
        let key = format!("concurrent-init-{:08}", i);
        let value = generate_large_value(initial_value_size, i);
        db.put(key.as_bytes(), &value).expect("Put failed");
    }
    
    let test_duration = Duration::from_secs(30);
    let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    
    let mut writer_handles = vec![];
    let mut reader_handles = vec![];
    
    // Start 8 concurrent writers
    for writer_id in 0..8 {
        let db_clone = db.clone();
        let stop_flag_clone = stop_flag.clone();
        
        let handle = thread::spawn(move || {
            let mut writes = 0;
            let mut counter = 0;
            let value_size = 512 * 1024; // 512KB values
            
            while !stop_flag_clone.load(std::sync::atomic::Ordering::Relaxed) {
                let key = format!("concurrent-writer-{}-{:08}", writer_id, counter);
                let value = generate_large_value(value_size, counter);
                
                if db_clone.put(key.as_bytes(), &value).is_ok() {
                    writes += 1;
                }
                counter += 1;
                
                // Small pause to prevent overwhelming the system
                if counter % 10 == 0 {
                    thread::sleep(Duration::from_millis(10));
                }
            }
            (writes, counter)
        });
        writer_handles.push(handle);
    }
    
    // Start 16 concurrent readers
    for reader_id in 0..16 {
        let db_clone = db.clone();
        let stop_flag_clone = stop_flag.clone();
        
        let handle = thread::spawn(move || {
            let mut reads = 0;
            let mut successful = 0;
            
            while !stop_flag_clone.load(std::sync::atomic::Ordering::Relaxed) {
                for source in 0..3 {
                    let key = match source {
                        0 => format!("concurrent-init-{:08}", (reads * 7) % initial_entries),
                        1 => format!("concurrent-writer-{}-{:08}", reader_id % 8, (reads * 13) % 100),
                        _ => format!("concurrent-writer-{}-{:08}", (reader_id + 4) % 8, (reads * 17) % 50),
                    };
                    
                    if let Ok(Some(_)) = db_clone.get(key.as_bytes(), None) {
                        successful += 1;
                    }
                    reads += 1;
                }
                
                // Small pause between read bursts
                thread::sleep(Duration::from_micros(100));
            }
            (reads, successful)
        });
        reader_handles.push(handle);
    }
    
    println!("   Running concurrent test for {} seconds...", test_duration.as_secs());
    let test_start = Instant::now();
    
    // Progress reporting
    let progress_handle = {
        let stop_flag_clone = stop_flag.clone();
        thread::spawn(move || {
            let mut last_report = Instant::now();
            while !stop_flag_clone.load(std::sync::atomic::Ordering::Relaxed) {
                thread::sleep(Duration::from_secs(5));
                if last_report.elapsed() >= Duration::from_secs(5) {
                    let elapsed = test_start.elapsed();
                    println!("   Progress: {:.0}s elapsed, memory pressure building...", elapsed.as_secs_f64());
                    last_report = Instant::now();
                }
            }
        })
    };
    
    thread::sleep(test_duration);
    stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);
    
    let mut total_writes = 0;
    let mut total_write_attempts = 0;
    let mut total_reads = 0;
    let mut successful_reads = 0;
    
    for handle in writer_handles {
        let (writes, attempts) = handle.join().unwrap();
        total_writes += writes;
        total_write_attempts += attempts;
    }
    
    for handle in reader_handles {
        let (reads, successful) = handle.join().unwrap();
        total_reads += reads;
        successful_reads += successful;
    }
    
    progress_handle.join().unwrap();
    
    let actual_duration = test_start.elapsed();
    let write_rate = total_writes as f64 / actual_duration.as_secs_f64();
    let read_rate = total_reads as f64 / actual_duration.as_secs_f64();
    let write_success_rate = total_writes as f64 / total_write_attempts as f64;
    let read_success_rate = successful_reads as f64 / total_reads as f64;
    
    println!("   Concurrent Performance Results:");
    println!("   Write rate: {:.0} ops/sec (success rate: {:.1}%)", write_rate, write_success_rate * 100.0);
    println!("   Read rate: {:.0} ops/sec (success rate: {:.1}%)", read_rate, read_success_rate * 100.0);
    println!("   Total data written: ~{}MB", (total_writes * 512) / 1024);
    
    if write_rate > 50.0 && read_rate > 1000.0 && write_success_rate > 0.95 && read_success_rate > 0.80 {
        println!("   ✅ EXCELLENT: Maintained high concurrent performance under memory pressure");
    } else if write_rate > 10.0 && read_rate > 100.0 && write_success_rate > 0.80 && read_success_rate > 0.60 {
        println!("   ✅ GOOD: Acceptable concurrent performance under memory pressure");
    } else {
        println!("   ⚠️  CHALLENGING: Performance significantly impacted by memory pressure");
    }
    println!();
}

fn test_large_dataset_integrity() {
    println!("4. Large Dataset Integrity Verification");
    println!("   Creating dataset and verifying data integrity under memory stress...");
    
    let mut config = MasterClassConfig::default();
    config.segment_size = 64 * 1024 * 1024;
    let db = MasterClassZVerse::new(config).expect("Failed to create database");
    
    let num_entries = 8_000;
    let value_size = 768 * 1024; // 768KB values for ~6GB dataset
    let mut integrity_map = HashMap::new();
    
    println!("   Writing {} entries with verification checksums...", num_entries);
    
    // Write phase with integrity tracking
    let write_start = Instant::now();
    for i in 0..num_entries {
        let key = format!("integrity-{:08}", i);
        let value = generate_checksummed_value(value_size, i);
        let checksum = calculate_simple_checksum(&value);
        
        db.put(key.as_bytes(), &value).expect("Put failed");
        integrity_map.insert(key, checksum);
        
        if i % 1000 == 0 {
            let elapsed = write_start.elapsed();
            let rate = i as f64 / elapsed.as_secs_f64();
            println!("   Write progress: {}/{} ({:.0} ops/sec)", i, num_entries, rate);
        }
    }
    
    let write_elapsed = write_start.elapsed();
    println!("   Write phase completed in {:.1}s", write_elapsed.as_secs_f64());
    
    // Verification phase
    println!("   Verifying data integrity...");
    let verify_start = Instant::now();
    let mut verified_count = 0;
    let mut corruption_count = 0;
    let mut missing_count = 0;
    
    for (key, expected_checksum) in &integrity_map {
        match db.get(key.as_bytes(), None) {
            Ok(Some(value)) => {
                let actual_checksum = calculate_simple_checksum(&value);
                if actual_checksum == *expected_checksum {
                    verified_count += 1;
                } else {
                    corruption_count += 1;
                    if corruption_count <= 5 {
                        println!("   CORRUPTION: Key {} checksum mismatch", key);
                    }
                }
            },
            Ok(None) => {
                missing_count += 1;
                if missing_count <= 5 {
                    println!("   MISSING: Key {} not found", key);
                }
            },
            Err(e) => {
                println!("   ERROR: Failed to read key {}: {:?}", key, e);
            }
        }
        
        if (verified_count + corruption_count + missing_count) % 1000 == 0 {
            let progress = verified_count + corruption_count + missing_count;
            println!("   Verification progress: {}/{}", progress, num_entries);
        }
    }
    
    let verify_elapsed = verify_start.elapsed();
    
    println!("   Integrity Verification Results:");
    println!("   Verified: {} entries", verified_count);
    println!("   Corrupted: {} entries", corruption_count);
    println!("   Missing: {} entries", missing_count);
    println!("   Verification time: {:.1}s", verify_elapsed.as_secs_f64());
    
    let integrity_rate = verified_count as f64 / num_entries as f64;
    
    if integrity_rate >= 1.0 {
        println!("   ✅ PERFECT: 100% data integrity maintained under memory pressure");
    } else if integrity_rate >= 0.99 {
        println!("   ✅ EXCELLENT: >99% data integrity maintained");
    } else if integrity_rate >= 0.95 {
        println!("   ✅ GOOD: >95% data integrity maintained");
    } else {
        println!("   ❌ CRITICAL: Significant data integrity issues detected");
    }
    println!();
}

fn test_extreme_memory_mixed_workload() {
    println!("5. Extreme Memory Pressure Mixed Workload");
    println!("   Testing system limits with aggressive memory usage...");
    
    let mut config = MasterClassConfig::default();
    config.segment_size = 512 * 1024 * 1024; // Large segments
    let db = Arc::new(MasterClassZVerse::new(config).expect("Failed to create database"));
    
    let extreme_value_size = 2 * 1024 * 1024; // 2MB values
    let target_memory_gb = 30; // Target 30GB to really stress the system
    let entries_needed = (target_memory_gb * 1024) / (extreme_value_size / (1024 * 1024));
    
    println!("   Target: {}GB dataset with {}MB values", target_memory_gb, extreme_value_size / (1024 * 1024));
    println!("   This will severely stress the {}GB system", 18);
    
    let test_duration = Duration::from_secs(60);
    let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    
    // Aggressive writer
    let writer_handle = {
        let db_clone = db.clone();
        let stop_flag_clone = stop_flag.clone();
        
        thread::spawn(move || {
            let mut writes = 0;
            let mut counter = 0;
            
            while !stop_flag_clone.load(std::sync::atomic::Ordering::Relaxed) && counter < entries_needed {
                let key = format!("extreme-{:010}", counter);
                let value = generate_large_value(extreme_value_size, counter);
                
                let write_start = Instant::now();
                match db_clone.put(key.as_bytes(), &value) {
                    Ok(_) => {
                        writes += 1;
                        let write_time = write_start.elapsed();
                        
                        if writes % 100 == 0 {
                            let gb_written = (writes * extreme_value_size) as f64 / (1024.0 * 1024.0 * 1024.0);
                            println!("   Extreme writer: {:.2}GB written, last write: {:.1}ms", 
                                     gb_written, write_time.as_millis());
                        }
                    },
                    Err(e) => {
                        println!("   Write error at {}GB: {:?}", 
                                 (writes * extreme_value_size) as f64 / (1024.0 * 1024.0 * 1024.0), e);
                        break;
                    }
                }
                counter += 1;
                
                // Dynamic backoff based on write time
                if write_start.elapsed() > Duration::from_millis(100) {
                    thread::sleep(Duration::from_millis(50));
                }
            }
            writes
        })
    };
    
    // Stress readers
    let mut reader_handles = vec![];
    for reader_id in 0..4 {
        let db_clone = db.clone();
        let stop_flag_clone = stop_flag.clone();
        
        let handle = thread::spawn(move || {
            let mut reads = 0;
            let mut successful = 0;
            let mut slow_reads = 0;
            
            thread::sleep(Duration::from_secs(5)); // Let writer get ahead
            
            while !stop_flag_clone.load(std::sync::atomic::Ordering::Relaxed) {
                let key_idx = (reads * 37 + reader_id * 1000) % 1000; // Read from first 1000 entries
                let key = format!("extreme-{:010}", key_idx);
                
                let read_start = Instant::now();
                if let Ok(Some(_)) = db_clone.get(key.as_bytes(), None) {
                    let read_time = read_start.elapsed();
                    successful += 1;
                    
                    if read_time > Duration::from_millis(100) {
                        slow_reads += 1;
                    }
                }
                reads += 1;
                
                if reads % 500 == 0 {
                    println!("   Reader {}: {}/{} successful, {} slow reads", 
                             reader_id, successful, reads, slow_reads);
                }
                
                thread::sleep(Duration::from_millis(50));
            }
            (reads, successful, slow_reads)
        });
        reader_handles.push(handle);
    }
    
    println!("   Running extreme test for {} seconds...", test_duration.as_secs());
    let start_time = Instant::now();
    
    thread::sleep(test_duration);
    stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);
    
    let total_writes = writer_handle.join().unwrap();
    let mut total_reads = 0;
    let mut total_successful = 0;
    let mut total_slow = 0;
    
    for handle in reader_handles {
        let (reads, successful, slow) = handle.join().unwrap();
        total_reads += reads;
        total_successful += successful;
        total_slow += slow;
    }
    
    let actual_duration = start_time.elapsed();
    let gb_written = (total_writes * extreme_value_size) as f64 / (1024.0 * 1024.0 * 1024.0);
    let write_rate = total_writes as f64 / actual_duration.as_secs_f64();
    let read_rate = total_reads as f64 / actual_duration.as_secs_f64();
    let read_success_rate = total_successful as f64 / total_reads as f64;
    let slow_read_rate = total_slow as f64 / total_successful as f64;
    
    println!("   Extreme Memory Pressure Results:");
    println!("   Data written: {:.2}GB", gb_written);
    println!("   Write rate: {:.1} ops/sec", write_rate);
    println!("   Read rate: {:.1} ops/sec (success: {:.1}%)", read_rate, read_success_rate * 100.0);
    println!("   Slow reads: {:.1}% (>100ms)", slow_read_rate * 100.0);
    
    if gb_written > 20.0 && read_success_rate > 0.70 {
        println!("   ✅ EXCEPTIONAL: System handled extreme memory pressure remarkably well");
    } else if gb_written > 15.0 && read_success_rate > 0.50 {
        println!("   ✅ EXCELLENT: System survived extreme memory pressure");
    } else if gb_written > 10.0 && read_success_rate > 0.30 {
        println!("   ✅ GOOD: System functional under extreme memory pressure");
    } else {
        println!("   ⚠️  SYSTEM LIMITS: Reached practical memory pressure limits");
    }
    
    if gb_written > 18.0 {
        println!("   🎯 ACHIEVEMENT: Successfully exceeded system RAM capacity!");
    }
    
    println!();
}

fn generate_large_value(size: usize, seed: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(size);
    let pattern = [
        (seed as u8).wrapping_add(1),
        (seed as u8).wrapping_add(2),
        (seed as u8).wrapping_add(3),
        (seed as u8).wrapping_add(4),
    ];
    
    for i in 0..size {
        value.push(pattern[i % 4]);
    }
    value
}

fn generate_checksummed_value(size: usize, seed: usize) -> Vec<u8> {
    let mut value = Vec::with_capacity(size);
    
    // Header with seed for verification
    value.extend_from_slice(&(seed as u64).to_le_bytes());
    value.extend_from_slice(&(size as u64).to_le_bytes());
    
    // Fill with predictable pattern
    let base_pattern = [
        (seed as u8).wrapping_mul(3).wrapping_add(7),
        (seed as u8).wrapping_mul(5).wrapping_add(11),
        (seed as u8).wrapping_mul(7).wrapping_add(13),
        (seed as u8).wrapping_mul(11).wrapping_add(17),
    ];
    
    for i in 16..size {
        value.push(base_pattern[(i - 16) % 4]);
    }
    
    value
}

fn calculate_simple_checksum(data: &[u8]) -> u64 {
    let mut checksum = 0u64;
    for (i, &byte) in data.iter().enumerate() {
        checksum = checksum.wrapping_add((byte as u64).wrapping_mul(i as u64 + 1));
    }
    checksum
}