use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use zverse::master_class::{MasterClassZVerse, MasterClassConfig};

fn main() {
    println!("=== ZVerse Master Class Performance Verification ===\n");
    
    // Test 1: Single-threaded write performance
    test_single_threaded_writes();
    
    // Test 2: Concurrent write performance
    test_concurrent_writes();
    
    // Test 3: Read performance
    test_read_performance();
    
    // Test 4: Mixed workload
    test_mixed_workload();
    
    // Test 5: Race condition verification
    test_race_conditions();
    
    // Test 6: Memory stress test
    test_memory_stress();
    
    println!("=== All Performance Tests Completed ===");
}

fn test_single_threaded_writes() {
    println!("1. Single-threaded Write Performance Test");
    println!("   Testing 10,000 sequential writes...");
    
    let config = MasterClassConfig::default();
    let db = MasterClassZVerse::new(config).expect("Failed to create database");
    
    let num_ops = 10_000;
    let start = Instant::now();
    
    for i in 0..num_ops {
        let key = format!("single-key-{:06}", i);
        let value = format!("single-value-{:06}", i);
        db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
    }
    
    let elapsed = start.elapsed();
    let ops_per_sec = num_ops as f64 / elapsed.as_secs_f64();
    
    println!("   Result: {:.0} ops/sec", ops_per_sec);
    println!("   Latency: {:.2} µs per operation", elapsed.as_micros() as f64 / num_ops as f64);
    
    if ops_per_sec > 1_000_000.0 {
        println!("   ✅ EXCELLENT: Exceeds 1M ops/sec target!");
    } else if ops_per_sec > 100_000.0 {
        println!("   ✅ GOOD: Exceeds 100K ops/sec");
    } else if ops_per_sec > 10_000.0 {
        println!("   ⚠️  OK: Exceeds 10K ops/sec");
    } else {
        println!("   ❌ POOR: Below 10K ops/sec");
    }
    println!();
}

fn test_concurrent_writes() {
    println!("2. Concurrent Write Performance Test");
    println!("   Testing 16 threads × 1,000 writes each...");
    
    let config = MasterClassConfig::default();
    let db = Arc::new(MasterClassZVerse::new(config).expect("Failed to create database"));
    
    let num_threads = 16;
    let writes_per_thread = 1_000;
    let barrier = Arc::new(Barrier::new(num_threads));
    
    let mut handles = vec![];
    
    for thread_id in 0..num_threads {
        let db_clone = db.clone();
        let barrier_clone = barrier.clone();
        
        let handle = thread::spawn(move || {
            barrier_clone.wait(); // Synchronize start
            
            let start = Instant::now();
            let mut versions = vec![];
            
            for i in 0..writes_per_thread {
                let key = format!("concurrent-{:02}-{:04}", thread_id, i);
                let value = format!("value-{:02}-{:04}", thread_id, i);
                
                let version = db_clone.put(key.as_bytes(), value.as_bytes())
                    .expect("Put failed");
                versions.push(version);
            }
            
            let elapsed = start.elapsed();
            (versions, elapsed)
        });
        handles.push(handle);
    }
    
    let overall_start = Instant::now();
    let mut all_versions = vec![];
    let mut total_time = Duration::new(0, 0);
    
    for handle in handles {
        let (versions, elapsed) = handle.join().unwrap();
        all_versions.extend(versions);
        total_time = total_time.max(elapsed);
    }
    let overall_elapsed = overall_start.elapsed();
    
    // Verify no version collisions
    all_versions.sort();
    let mut unique_versions = all_versions.clone();
    unique_versions.dedup();
    
    let total_ops = all_versions.len();
    let throughput = total_ops as f64 / overall_elapsed.as_secs_f64();
    
    println!("   Result: {:.0} ops/sec total throughput", throughput);
    println!("   Parallel execution time: {:.2} ms", overall_elapsed.as_millis());
    println!("   Version uniqueness: {}/{} ({})", unique_versions.len(), total_ops,
             if unique_versions.len() == total_ops { "✅ PASS" } else { "❌ FAIL" });
    
    if throughput > 500_000.0 {
        println!("   ✅ EXCELLENT: High concurrent throughput!");
    } else if throughput > 100_000.0 {
        println!("   ✅ GOOD: Good concurrent performance");
    } else if throughput > 10_000.0 {
        println!("   ⚠️  OK: Acceptable concurrent performance");
    } else {
        println!("   ❌ POOR: Low concurrent performance");
    }
    println!();
}

fn test_read_performance() {
    println!("3. Read Performance Test");
    println!("   Pre-populating 5,000 entries...");
    
    let config = MasterClassConfig::default();
    let db = Arc::new(MasterClassZVerse::new(config).expect("Failed to create database"));
    
    // Pre-populate data
    let num_entries = 5_000;
    for i in 0..num_entries {
        let key = format!("read-key-{:05}", i);
        let value = format!("read-value-{:05}-{}", i, "x".repeat(100)); // 100+ byte values
        db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
    }
    
    println!("   Testing single-threaded reads...");
    
    // Single-threaded read test
    let num_reads = 50_000;
    let start = Instant::now();
    let mut successful_reads = 0;
    
    for i in 0..num_reads {
        let key_idx = i % num_entries;
        let key = format!("read-key-{:05}", key_idx);
        
        if let Ok(Some(_)) = db.get(key.as_bytes(), None) {
            successful_reads += 1;
        }
    }
    
    let elapsed = start.elapsed();
    let reads_per_sec = successful_reads as f64 / elapsed.as_secs_f64();
    
    println!("   Single-threaded: {:.0} reads/sec", reads_per_sec);
    println!("   Success rate: {:.1}%", successful_reads as f64 / num_reads as f64 * 100.0);
    
    // Multi-threaded read test
    println!("   Testing concurrent reads (8 threads)...");
    
    let num_reader_threads = 8;
    let reads_per_thread = 10_000;
    let barrier = Arc::new(Barrier::new(num_reader_threads));
    
    let mut handles = vec![];
    
    for _thread_id in 0..num_reader_threads {
        let db_clone = db.clone();
        let barrier_clone = barrier.clone();
        
        let handle = thread::spawn(move || {
            barrier_clone.wait();
            
            let start = Instant::now();
            let mut successful = 0;
            
            for i in 0..reads_per_thread {
                let key_idx = i % num_entries;
                let key = format!("read-key-{:05}", key_idx);
                
                if let Ok(Some(_)) = db_clone.get(key.as_bytes(), None) {
                    successful += 1;
                }
            }
            
            let elapsed = start.elapsed();
            (successful, elapsed)
        });
        handles.push(handle);
    }
    
    let mut total_successful = 0;
    let mut max_time = Duration::new(0, 0);
    
    for handle in handles {
        let (successful, elapsed) = handle.join().unwrap();
        total_successful += successful;
        max_time = max_time.max(elapsed);
    }
    
    let concurrent_reads_per_sec = total_successful as f64 / max_time.as_secs_f64();
    
    println!("   Concurrent: {:.0} reads/sec total", concurrent_reads_per_sec);
    
    if concurrent_reads_per_sec > 1_000_000.0 {
        println!("   ✅ EXCELLENT: >1M reads/sec!");
    } else if concurrent_reads_per_sec > 500_000.0 {
        println!("   ✅ GOOD: >500K reads/sec");
    } else if concurrent_reads_per_sec > 100_000.0 {
        println!("   ⚠️  OK: >100K reads/sec");
    } else {
        println!("   ❌ POOR: <100K reads/sec");
    }
    println!();
}

fn test_mixed_workload() {
    println!("4. Mixed Workload Test");
    println!("   Testing 4 writers + 8 readers for 3 seconds...");
    
    let config = MasterClassConfig::default();
    let db = Arc::new(MasterClassZVerse::new(config).expect("Failed to create database"));
    
    // Pre-populate some data
    for i in 0..1000 {
        let key = format!("mixed-init-{:04}", i);
        let value = format!("mixed-value-{:04}", i);
        db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
    }
    
    let duration = Duration::from_secs(3);
    let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
    
    let mut writer_handles = vec![];
    let mut reader_handles = vec![];
    
    // Writers
    for writer_id in 0..4 {
        let db_clone = db.clone();
        let stop_flag_clone = stop_flag.clone();
        
        let handle = thread::spawn(move || {
            let mut writes = 0;
            let mut counter = 0;
            
            while !stop_flag_clone.load(std::sync::atomic::Ordering::Relaxed) {
                let key = format!("mixed-writer-{}-{:06}", writer_id, counter);
                let value = format!("mixed-value-{}-{:06}", writer_id, counter);
                
                if db_clone.put(key.as_bytes(), value.as_bytes()).is_ok() {
                    writes += 1;
                }
                counter += 1;
                
                if counter % 100 == 0 {
                    thread::sleep(Duration::from_micros(100)); // Brief pause
                }
            }
            writes
        });
        writer_handles.push(handle);
    }
    
    // Readers
    for reader_id in 0..8 {
        let db_clone = db.clone();
        let stop_flag_clone = stop_flag.clone();
        
        let handle = thread::spawn(move || {
            let mut reads = 0;
            let mut successful = 0;
            
            while !stop_flag_clone.load(std::sync::atomic::Ordering::Relaxed) {
                // Read from various sources
                for source in 0..4 {
                    let key = if reads % 3 == 0 {
                        format!("mixed-init-{:04}", (reads / 3) % 1000)
                    } else {
                        format!("mixed-writer-{}-{:06}", source, (reads / 10) % 100)
                    };
                    
                    if let Ok(Some(_)) = db_clone.get(key.as_bytes(), None) {
                        successful += 1;
                    }
                    reads += 1;
                }
            }
            (reads, successful)
        });
        reader_handles.push(handle);
    }
    
    thread::sleep(duration);
    stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);
    
    let mut total_writes = 0;
    let mut total_reads = 0;
    let mut successful_reads = 0;
    
    for handle in writer_handles {
        total_writes += handle.join().unwrap() as usize;
    }
    
    for handle in reader_handles {
        let (reads, successful) = handle.join().unwrap();
        total_reads += reads;
        successful_reads += successful;
    }
    
    let write_rate = total_writes as f64 / duration.as_secs_f64();
    let read_rate = total_reads as f64 / duration.as_secs_f64();
    let success_rate = successful_reads as f64 / total_reads as f64;
    
    println!("   Write rate: {:.0} ops/sec", write_rate);
    println!("   Read rate: {:.0} ops/sec", read_rate);
    println!("   Read success rate: {:.1}%", success_rate * 100.0);
    
    if write_rate > 10_000.0 && read_rate > 100_000.0 && success_rate > 0.5 {
        println!("   ✅ EXCELLENT: High mixed workload performance!");
    } else if write_rate > 1_000.0 && read_rate > 10_000.0 && success_rate > 0.3 {
        println!("   ✅ GOOD: Good mixed workload performance");
    } else {
        println!("   ⚠️  NEEDS IMPROVEMENT: Mixed workload performance could be better");
    }
    println!();
}

fn test_race_conditions() {
    println!("5. Race Condition Verification Test");
    println!("   Testing version ordering with 32 threads...");
    
    let config = MasterClassConfig::default();
    let db = Arc::new(MasterClassZVerse::new(config).expect("Failed to create database"));
    
    let num_threads = 32;
    let writes_per_thread = 100;
    let versions = Arc::new(std::sync::Mutex::new(Vec::new()));
    
    let mut handles = vec![];
    
    for thread_id in 0..num_threads {
        let db_clone = db.clone();
        let versions_clone = versions.clone();
        
        let handle = thread::spawn(move || {
            for i in 0..writes_per_thread {
                let key = format!("race-{:02}-{:03}", thread_id, i);
                let value = format!("race-value-{:02}-{:03}", thread_id, i);
                
                let version = db_clone.put(key.as_bytes(), value.as_bytes())
                    .expect("Put failed");
                
                versions_clone.lock().unwrap().push((thread_id, i, version));
            }
        });
        handles.push(handle);
    }
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let mut all_versions = versions.lock().unwrap();
    all_versions.sort_by_key(|&(_, _, version)| version);
    
    // Check for version ordering violations
    let mut violations = 0;
    for i in 1..all_versions.len() {
        if all_versions[i].2 <= all_versions[i-1].2 {
            violations += 1;
        }
    }
    
    // Check for duplicate versions
    let mut version_counts = HashMap::new();
    for &(_, _, version) in all_versions.iter() {
        *version_counts.entry(version).or_insert(0) += 1;
    }
    
    let duplicates = version_counts.values().filter(|&&count| count > 1).count();
    
    println!("   Total operations: {}", all_versions.len());
    println!("   Version ordering violations: {}", violations);
    println!("   Duplicate versions: {}", duplicates);
    
    if violations == 0 && duplicates == 0 {
        println!("   ✅ EXCELLENT: No race conditions detected!");
    } else if violations < 10 && duplicates < 5 {
        println!("   ⚠️  WARNING: Minor race condition issues detected");
    } else {
        println!("   ❌ CRITICAL: Serious race condition problems!");
    }
    println!();
}

fn test_memory_stress() {
    println!("6. Memory Stress Test");
    println!("   Testing with large values and rapid allocation...");
    
    let config = MasterClassConfig::default();
    let db = MasterClassZVerse::new(config).expect("Failed to create database");
    
    let large_value_size = 64 * 1024; // 64KB values
    let num_operations = 1000;
    
    let start = Instant::now();
    
    for i in 0..num_operations {
        let key = format!("memory-stress-{:06}", i);
        let value = vec![((i % 256) as u8); large_value_size];
        
        db.put(key.as_bytes(), &value).expect("Put failed");
        
        // Occasionally read back to verify integrity
        if i % 100 == 0 {
            let result = db.get(key.as_bytes(), None).expect("Get failed");
            if let Some(retrieved_value) = result {
                if retrieved_value.len() != large_value_size {
                    panic!("Value corruption detected: wrong size");
                }
                if retrieved_value[0] != ((i % 256) as u8) {
                    panic!("Value corruption detected: wrong content");
                }
            } else {
                panic!("Value missing after write");
            }
        }
    }
    
    let elapsed = start.elapsed();
    let ops_per_sec = num_operations as f64 / elapsed.as_secs_f64();
    let mb_per_sec = (num_operations * large_value_size) as f64 / (1024.0 * 1024.0) / elapsed.as_secs_f64();
    
    println!("   Large value performance: {:.0} ops/sec", ops_per_sec);
    println!("   Throughput: {:.1} MB/sec", mb_per_sec);
    
    if ops_per_sec > 1000.0 {
        println!("   ✅ EXCELLENT: Handles large values efficiently!");
    } else if ops_per_sec > 100.0 {
        println!("   ✅ GOOD: Reasonable large value performance");
    } else {
        println!("   ⚠️  OK: Large value performance could be improved");
    }
    
    println!("   ✅ Memory stress test completed without crashes");
    println!();
}