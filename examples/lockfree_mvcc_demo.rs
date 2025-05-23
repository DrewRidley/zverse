//! Lock-Free MVCC Demo - High Performance Concurrent Storage
//!
//! This demo showcases true lock-free concurrent access targeting
//! millions of operations per second with MVCC isolation.

use zverse::storage::lockfree_engine::{LockFreeEngine, LockFreeEngineConfig};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 IMEC Lock-Free MVCC Demo - Million Ops/Second Target");
    println!("=======================================================");

    // Create database file
    let db_path = PathBuf::from("./lockfree_mvcc_demo.db");
    if db_path.exists() {
        std::fs::remove_file(&db_path)?;
    }

    let config = LockFreeEngineConfig::default()
        .with_path(db_path.clone())
        .with_file_size(1024 * 1024 * 1024) // 1GB for high throughput
        .with_extent_size(64 * 1024); // 64KB extents

    println!("\n📁 Database: {}", db_path.display());

    // Create lock-free engine
    let start = Instant::now();
    let engine = Arc::new(LockFreeEngine::new(config)?);
    let creation_time = start.elapsed();
    
    println!("  Creation: {:?}", creation_time);
    println!("  File size: {} MB", std::fs::metadata(&db_path)?.len() / 1024 / 1024);
    println!("  Initial extents: {}", engine.extent_count());

    // Global counters for tracking
    let global_ops = Arc::new(AtomicU64::new(0));
    let global_errors = Arc::new(AtomicU64::new(0));

    println!("\n🔥 Lock-Free Performance Tests");

    // Test 1: Single-threaded baseline
    println!("\n1️⃣ Single-Threaded Baseline:");
    
    let baseline_start = Instant::now();
    let put_result = engine.put("baseline_key", b"baseline_value");
    let put_time = baseline_start.elapsed();
    
    match put_result {
        Ok(_) => println!("  PUT: {:?} ({:.1}μs)", put_time, put_time.as_nanos() as f64 / 1000.0),
        Err(e) => println!("  PUT failed: {}", e),
    }

    let get_start = Instant::now();
    let get_result = engine.get("baseline_key");
    let get_time = get_start.elapsed();
    
    match get_result {
        Ok(Some(value)) => {
            println!("  GET: {:?} ({:.1}μs)", get_time, get_time.as_nanos() as f64 / 1000.0);
            println!("  Value: {:?}", std::str::from_utf8(&value).unwrap());
        }
        Ok(None) => println!("  GET: {:?} (not found)", get_time),
        Err(e) => println!("  GET failed: {}", e),
    }

    // Test 2: Concurrent readers (true lock-free)
    println!("\n2️⃣ Massive Concurrent Readers (Lock-Free):");
    
    // Pre-populate data
    for i in 0..1000 {
        let key = format!("read_key_{}", i);
        let value = format!("read_value_{}", i);
        let _ = engine.put(&key, value.as_bytes());
    }

    let reader_count = 100;
    let reads_per_thread = 10000;
    let mut reader_handles = Vec::new();
    let reader_start = Instant::now();
    
    for thread_id in 0..reader_count {
        let engine_clone = engine.clone();
        let ops_counter = global_ops.clone();
        let error_counter = global_errors.clone();
        
        let handle = thread::spawn(move || {
            let start = Instant::now();
            let mut successful_reads = 0;
            let mut failed_reads = 0;
            
            for i in 0..reads_per_thread {
                let key = format!("read_key_{}", i % 1000);
                match engine_clone.get(&key) {
                    Ok(Some(_)) => {
                        successful_reads += 1;
                        ops_counter.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(None) => failed_reads += 1,
                    Err(_) => {
                        failed_reads += 1;
                        error_counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            
            (thread_id, start.elapsed(), successful_reads, failed_reads)
        });
        reader_handles.push(handle);
    }
    
    let mut total_successful_reads = 0;
    let mut total_failed_reads = 0;
    let mut min_thread_time = Duration::from_secs(1000);
    let mut max_thread_time = Duration::from_nanos(0);
    
    for handle in reader_handles {
        let (thread_id, thread_time, successful, failed) = handle.join().unwrap();
        total_successful_reads += successful;
        total_failed_reads += failed;
        min_thread_time = min_thread_time.min(thread_time);
        max_thread_time = max_thread_time.max(thread_time);
        
        if thread_id < 5 {
            println!("  Reader {} completed in {:?} ({} successful, {} failed)", 
                     thread_id, thread_time, successful, failed);
        }
    }
    
    let reader_total_time = reader_start.elapsed();
    let total_read_ops = reader_count * reads_per_thread;
    let read_throughput = total_read_ops as f64 / reader_total_time.as_secs_f64();
    
    println!("  Total operations: {}", total_read_ops);
    println!("  Successful reads: {}", total_successful_reads);
    println!("  Failed reads: {}", total_failed_reads);
    println!("  Total time: {:?}", reader_total_time);
    println!("  Thread time range: {:?} - {:?}", min_thread_time, max_thread_time);
    println!("  READ THROUGHPUT: {:.0} ops/second", read_throughput);
    println!("  READ THROUGHPUT: {:.1} Mops/second", read_throughput / 1_000_000.0);

    // Test 3: Concurrent writers (minimally-blocking)
    println!("\n3️⃣ Concurrent Writers (Atomic Allocation):");
    
    let writer_count = 50;
    let writes_per_thread = 1000;
    let mut writer_handles = Vec::new();
    let writer_start = Instant::now();
    
    for thread_id in 0..writer_count {
        let engine_clone = engine.clone();
        let ops_counter = global_ops.clone();
        let error_counter = global_errors.clone();
        
        let handle = thread::spawn(move || {
            let start = Instant::now();
            let mut successful_writes = 0;
            let mut failed_writes = 0;
            
            for i in 0..writes_per_thread {
                let key = format!("write_key_{}_{}", thread_id, i);
                let value = format!("write_value_{}_{}", thread_id, i);
                
                match engine_clone.put(&key, value.as_bytes()) {
                    Ok(_) => {
                        successful_writes += 1;
                        ops_counter.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        failed_writes += 1;
                        error_counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            
            (thread_id, start.elapsed(), successful_writes, failed_writes)
        });
        writer_handles.push(handle);
    }
    
    let mut total_successful_writes = 0;
    let mut total_failed_writes = 0;
    let mut min_write_time = Duration::from_secs(1000);
    let mut max_write_time = Duration::from_nanos(0);
    
    for handle in writer_handles {
        let (thread_id, thread_time, successful, failed) = handle.join().unwrap();
        total_successful_writes += successful;
        total_failed_writes += failed;
        min_write_time = min_write_time.min(thread_time);
        max_write_time = max_write_time.max(thread_time);
        
        if thread_id < 5 {
            println!("  Writer {} completed in {:?} ({} successful, {} failed)", 
                     thread_id, thread_time, successful, failed);
        }
    }
    
    let writer_total_time = writer_start.elapsed();
    let total_write_ops = writer_count * writes_per_thread;
    let write_throughput = total_write_ops as f64 / writer_total_time.as_secs_f64();
    
    println!("  Total operations: {}", total_write_ops);
    println!("  Successful writes: {}", total_successful_writes);
    println!("  Failed writes: {}", total_failed_writes);
    println!("  Total time: {:?}", writer_total_time);
    println!("  Thread time range: {:?} - {:?}", min_write_time, max_write_time);
    println!("  WRITE THROUGHPUT: {:.0} ops/second", write_throughput);
    println!("  WRITE THROUGHPUT: {:.1} Mops/second", write_throughput / 1_000_000.0);

    // Test 4: Mixed workload (readers + writers)
    println!("\n4️⃣ Mixed Workload (Million Ops Target):");
    
    let mixed_reader_count = 80;
    let mixed_writer_count = 20;
    let mixed_ops_per_thread = 50000;
    let mut mixed_handles = Vec::new();
    let mixed_start = Instant::now();
    
    // Reset counters
    global_ops.store(0, Ordering::Relaxed);
    global_errors.store(0, Ordering::Relaxed);
    
    // Launch readers
    for thread_id in 0..mixed_reader_count {
        let engine_clone = engine.clone();
        let ops_counter = global_ops.clone();
        let error_counter = global_errors.clone();
        
        let handle = thread::spawn(move || {
            let mut successful = 0;
            let mut failed = 0;
            
            for i in 0..mixed_ops_per_thread {
                let key = format!("read_key_{}", i % 1000);
                match engine_clone.get(&key) {
                    Ok(Some(_)) => {
                        successful += 1;
                        ops_counter.fetch_add(1, Ordering::Relaxed);
                    }
                    Ok(None) => failed += 1,
                    Err(_) => {
                        failed += 1;
                        error_counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            
            ("reader", thread_id, successful, failed)
        });
        mixed_handles.push(handle);
    }
    
    // Launch writers
    for thread_id in 0..mixed_writer_count {
        let engine_clone = engine.clone();
        let ops_counter = global_ops.clone();
        let error_counter = global_errors.clone();
        
        let handle = thread::spawn(move || {
            let mut successful = 0;
            let mut failed = 0;
            
            for i in 0..mixed_ops_per_thread {
                let key = format!("mixed_write_{}_{}", thread_id, i);
                let value = format!("mixed_value_{}_{}", thread_id, i);
                
                match engine_clone.put(&key, value.as_bytes()) {
                    Ok(_) => {
                        successful += 1;
                        ops_counter.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        failed += 1;
                        error_counter.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            
            ("writer", thread_id, successful, failed)
        });
        mixed_handles.push(handle);
    }
    
    let mut mixed_read_ops = 0;
    let mut mixed_write_ops = 0;
    let mut mixed_failed = 0;
    
    for handle in mixed_handles {
        let (op_type, thread_id, successful, failed) = handle.join().unwrap();
        
        if op_type == "reader" {
            mixed_read_ops += successful;
        } else {
            mixed_write_ops += successful;
        }
        mixed_failed += failed;
        
        if thread_id < 3 {
            println!("  {} {} completed: {} successful, {} failed", 
                     op_type, thread_id, successful, failed);
        }
    }
    
    let mixed_total_time = mixed_start.elapsed();
    let total_mixed_ops = mixed_read_ops + mixed_write_ops;
    let mixed_throughput = total_mixed_ops as f64 / mixed_total_time.as_secs_f64();
    
    println!("  Mixed workload results:");
    println!("    Read operations: {}", mixed_read_ops);
    println!("    Write operations: {}", mixed_write_ops);
    println!("    Failed operations: {}", mixed_failed);
    println!("    Total successful: {}", total_mixed_ops);
    println!("    Total time: {:?}", mixed_total_time);
    println!("    MIXED THROUGHPUT: {:.0} ops/second", mixed_throughput);
    println!("    MIXED THROUGHPUT: {:.1} Mops/second", mixed_throughput / 1_000_000.0);

    // Test 5: Ultra-high throughput stress test
    println!("\n5️⃣ Ultra-High Throughput Stress Test:");
    
    let stress_reader_count = 200;
    let stress_ops_per_thread = 100000;
    let mut stress_handles = Vec::new();
    let stress_start = Instant::now();
    
    global_ops.store(0, Ordering::Relaxed);
    
    for thread_id in 0..stress_reader_count {
        let engine_clone = engine.clone();
        let ops_counter = global_ops.clone();
        
        let handle = thread::spawn(move || {
            let start = Instant::now();
            let mut ops = 0;
            
            for i in 0..stress_ops_per_thread {
                let key = format!("read_key_{}", i % 1000);
                if engine_clone.get(&key).is_ok() {
                    ops += 1;
                    ops_counter.fetch_add(1, Ordering::Relaxed);
                }
            }
            
            (thread_id, start.elapsed(), ops)
        });
        stress_handles.push(handle);
    }
    
    // Monitor progress
    let monitor_handle = thread::spawn({
        let ops_counter = global_ops.clone();
        move || {
            let start = Instant::now();
            loop {
                thread::sleep(Duration::from_millis(1000));
                let ops = ops_counter.load(Ordering::Relaxed);
                let elapsed = start.elapsed().as_secs_f64();
                let throughput = ops as f64 / elapsed;
                println!("    Progress: {} ops in {:.1}s = {:.0} ops/s ({:.1} Mops/s)", 
                         ops, elapsed, throughput, throughput / 1_000_000.0);
                
                if ops >= (stress_reader_count * stress_ops_per_thread) as u64 {
                    break;
                }
            }
        }
    });
    
    let mut stress_total_ops = 0;
    for handle in stress_handles {
        let (_thread_id, _thread_time, ops) = handle.join().unwrap();
        stress_total_ops += ops;
    }
    
    let _ = monitor_handle.join();
    let stress_total_time = stress_start.elapsed();
    let stress_throughput = stress_total_ops as f64 / stress_total_time.as_secs_f64();
    
    println!("  Stress test completed:");
    println!("    Total operations: {}", stress_total_ops);
    println!("    Total time: {:?}", stress_total_time);
    println!("    STRESS THROUGHPUT: {:.0} ops/second", stress_throughput);
    println!("    STRESS THROUGHPUT: {:.1} Mops/second", stress_throughput / 1_000_000.0);

    // Get final statistics
    let stats = engine.stats();
    let _ = engine.flush();

    println!("\n📊 Final Performance Summary:");
    println!("  ┌─────────────────────────────────────────────────────┐");
    println!("  │ Lock-Free MVCC Performance Results                 │");
    println!("  ├─────────────────────────────────────────────────────┤");
    println!("  │ Single GET:             {:>15.1} μs     │", get_time.as_nanos() as f64 / 1000.0);
    println!("  │ Single PUT:             {:>15.1} μs     │", put_time.as_nanos() as f64 / 1000.0);
    println!("  │ Concurrent reads:       {:>15.1} Mops/s │", read_throughput / 1_000_000.0);
    println!("  │ Concurrent writes:      {:>15.1} Mops/s │", write_throughput / 1_000_000.0);
    println!("  │ Mixed workload:         {:>15.1} Mops/s │", mixed_throughput / 1_000_000.0);
    println!("  │ Stress test:            {:>15.1} Mops/s │", stress_throughput / 1_000_000.0);
    println!("  │ Total extents:          {:>15} │", engine.extent_count());
    println!("  └─────────────────────────────────────────────────────┘");

    println!("\n🎯 Million Ops/Second Target Achievement:");
    let achieved_million_ops = stress_throughput >= 1_000_000.0;
    println!("  Target: 1.0+ Mops/second");
    println!("  Actual: {:.1} Mops/second {}", 
             stress_throughput / 1_000_000.0,
             if achieved_million_ops { "✅" } else { "❌" });

    if achieved_million_ops {
        println!("  🏆 MILLION OPS/SECOND TARGET ACHIEVED!");
        println!("  Performance multiplier: {:.1}x million ops/second", stress_throughput / 1_000_000.0);
    }

    println!("\n📊 Engine Statistics:");
    println!("  Total reads: {}", stats.reads);
    println!("  Total writes: {}", stats.writes);
    println!("  Cache hits: {}", stats.cache_hits);
    println!("  Cache misses: {}", stats.cache_misses);
    println!("  Cache hit ratio: {:.1}%", 
             if stats.cache_hits + stats.cache_misses > 0 {
                 (stats.cache_hits as f64 / (stats.cache_hits + stats.cache_misses) as f64) * 100.0
             } else { 0.0 });

    println!("\n✅ Lock-Free MVCC Demo Completed Successfully!");
    println!("IMEC achieves ultra-high performance through:");
    println!("  ✓ True lock-free concurrent reads with memory mapping");
    println!("  ✓ Atomic page allocation for minimal write coordination");
    println!("  ✓ Cache-friendly data structures with padding");
    println!("  ✓ Morton Z-curve ordering for spatial locality");
    println!("  ✓ Optimized extent allocation strategy");
    
    if achieved_million_ops {
        println!("  🚀 MILLION+ OPERATIONS PER SECOND ACHIEVED!");
    }

    println!("\nDatabase: {} ({} MB)", 
             db_path.display(), 
             std::fs::metadata(&db_path)?.len() / 1024 / 1024);

    Ok(())
}