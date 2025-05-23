//! Temporal Interval Storage Demo
//!
//! This demo showcases the new temporal interval architecture that stores
//! records as Morton intervals rather than points, enabling efficient
//! temporal queries and true MVCC semantics.

use zverse::storage::temporal_engine::{TemporalEngine, TemporalEngineConfig};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 IMEC Temporal Interval Demo - Morton Interval Architecture");
    println!("=============================================================");

    // Create database file
    let db_path = PathBuf::from("./temporal_demo.db");
    if db_path.exists() {
        std::fs::remove_file(&db_path)?;
    }

    let mut config = TemporalEngineConfig::default()
        .with_path(db_path.clone())
        .with_file_size(256 * 1024 * 1024) // 256MB
        .with_extent_size(64 * 1024); // 64KB extents
    config.initial_extent_count = 4; // Use fewer extents to avoid allocation issues

    println!("\n📁 Database: {}", db_path.display());

    // Create temporal engine
    let start = Instant::now();
    let engine = Arc::new(TemporalEngine::new(config)?);
    let creation_time = start.elapsed();
    
    println!("  Creation: {:?}", creation_time);
    println!("  File size: {} MB", std::fs::metadata(&db_path)?.len() / 1024 / 1024);

    println!("\n🔥 Temporal Interval Architecture Tests");

    // Test 1: Basic temporal operations
    println!("\n1️⃣ Basic Temporal Operations:");
    
    let t1 = Instant::now();
    engine.put("user:alice", b"alice_data_v1")?;
    let put1_time = t1.elapsed();
    println!("  PUT user:alice v1: {:?} ({:.1}μs)", put1_time, put1_time.as_nanos() as f64 / 1000.0);
    
    // Wait a moment then read
    thread::sleep(Duration::from_micros(100));
    
    let t2 = Instant::now();
    let value1 = engine.get("user:alice")?;
    let get1_time = t2.elapsed();
    println!("  GET user:alice: {:?} ({:.1}μs) -> {:?}", 
             get1_time, get1_time.as_nanos() as f64 / 1000.0,
             value1.as_ref().map(|v| std::str::from_utf8(v).unwrap()));

    // Test 2: Temporal versioning (interval splitting)
    println!("\n2️⃣ Temporal Versioning (Interval Splitting):");
    
    // Update the same key - should split the interval
    thread::sleep(Duration::from_micros(100));
    let t3 = Instant::now();
    engine.put("user:alice", b"alice_data_v2")?;
    let put2_time = t3.elapsed();
    println!("  PUT user:alice v2: {:?} ({:.1}μs)", put2_time, put2_time.as_nanos() as f64 / 1000.0);
    
    // Read latest version
    let t4 = Instant::now();
    let value2 = engine.get("user:alice")?;
    let get2_time = t4.elapsed();
    println!("  GET user:alice (latest): {:?} ({:.1}μs) -> {:?}", 
             get2_time, get2_time.as_nanos() as f64 / 1000.0,
             value2.as_ref().map(|v| std::str::from_utf8(v).unwrap()));

    // Test 3: Multiple users for range testing
    println!("\n3️⃣ Multiple Users Setup:");
    
    let users = ["user:bob", "user:charlie", "user:diana", "user:eve"];
    let mut total_put_time = Duration::new(0, 0);
    
    for (i, user) in users.iter().enumerate() {
        let value = format!("{}_data_initial", user.split(':').nth(1).unwrap());
        let t = Instant::now();
        engine.put(user, value.as_bytes())?;
        let put_time = t.elapsed();
        total_put_time += put_time;
        
        println!("  PUT {}: {:?} ({:.1}μs)", user, put_time, put_time.as_nanos() as f64 / 1000.0);
    }
    
    let avg_put_time = total_put_time / users.len() as u32;
    println!("  Average PUT time: {:?} ({:.1}μs)", avg_put_time, avg_put_time.as_nanos() as f64 / 1000.0);

    // Test 4: Range queries (the key benefit of Morton intervals!)
    println!("\n4️⃣ Range Queries (Morton Interval Power):");
    
    thread::sleep(Duration::from_micros(100));
    let range_start = Instant::now();
    let range_results = engine.range_query("user:a", "user:z", 
        zverse::encoding::morton::current_timestamp_micros())?;
    let range_time = range_start.elapsed();
    
    println!("  Range query user:a to user:z: {:?} ({:.1}μs)", 
             range_time, range_time.as_nanos() as f64 / 1000.0);
    println!("  Found {} records:", range_results.len());
    for (key, value) in range_results.iter().take(5) {
        println!("    {}: {:?}", key, std::str::from_utf8(value).unwrap());
    }

    // Test 5: Concurrent operations
    println!("\n5️⃣ Concurrent Temporal Operations:");
    
    let reader_count = 10;
    let writer_count = 5;
    let mut handles = Vec::new();
    let concurrent_start = Instant::now();
    
    // Launch concurrent readers
    for i in 0..reader_count {
        let engine_clone = engine.clone();
        let handle = thread::spawn(move || {
            let start = Instant::now();
            let mut successful = 0;
            let mut failed = 0;
            
            for j in 0..100 {
                let key = format!("user:{}", ["alice", "bob", "charlie", "diana", "eve"][j % 5]);
                match engine_clone.get(&key) {
                    Ok(Some(_)) => successful += 1,
                    Ok(None) => failed += 1,
                    Err(_) => failed += 1,
                }
            }
            
            ("reader", i, start.elapsed(), successful, failed)
        });
        handles.push(handle);
    }
    
    // Launch concurrent writers
    for i in 0..writer_count {
        let engine_clone = engine.clone();
        let handle = thread::spawn(move || {
            let start = Instant::now();
            let mut successful = 0;
            let mut failed = 0;
            
            for j in 0..50 {
                let key = format!("concurrent_key_{}_{}", i, j);
                let value = format!("concurrent_value_{}_{}", i, j);
                match engine_clone.put(&key, value.as_bytes()) {
                    Ok(_) => successful += 1,
                    Err(_) => failed += 1,
                }
            }
            
            ("writer", i, start.elapsed(), successful, failed)
        });
        handles.push(handle);
    }
    
    let mut total_reads = 0;
    let mut total_writes = 0;
    let mut total_read_time = Duration::new(0, 0);
    let mut total_write_time = Duration::new(0, 0);
    
    for handle in handles {
        let (op_type, thread_id, thread_time, successful, failed) = handle.join().unwrap();
        
        if op_type == "reader" {
            total_reads += successful;
            total_read_time += thread_time;
            if thread_id < 3 {
                println!("  Reader {}: {:?} ({} successful, {} failed)", 
                         thread_id, thread_time, successful, failed);
            }
        } else {
            total_writes += successful;
            total_write_time += thread_time;
            if thread_id < 3 {
                println!("  Writer {}: {:?} ({} successful, {} failed)", 
                         thread_id, thread_time, successful, failed);
            }
        }
    }
    
    let concurrent_total_time = concurrent_start.elapsed();
    let total_ops = total_reads + total_writes;
    let throughput = total_ops as f64 / concurrent_total_time.as_secs_f64();
    
    println!("  Concurrent results:");
    println!("    Total reads: {}", total_reads);
    println!("    Total writes: {}", total_writes);
    println!("    Total operations: {}", total_ops);
    println!("    Total time: {:?}", concurrent_total_time);
    println!("    Throughput: {:.0} ops/second", throughput);

    // Test 6: Performance comparison
    println!("\n6️⃣ Performance Benchmark:");
    
    let benchmark_ops = 1000;
    let bench_start = Instant::now();
    
    for i in 0..benchmark_ops {
        let key = format!("benchmark_key_{}", i);
        let value = format!("benchmark_value_{}", i);
        engine.put(&key, value.as_bytes())?;
        
        // Every 10th operation, do a read
        if i % 10 == 0 {
            let _ = engine.get(&key)?;
        }
    }
    
    let bench_time = bench_start.elapsed();
    let bench_throughput = (benchmark_ops as f64 * 1.1) / bench_time.as_secs_f64(); // 1.1 for the extra reads
    
    println!("  Benchmark: {} ops in {:?}", benchmark_ops, bench_time);
    println!("  Average op time: {:?} ({:.1}μs)", 
             bench_time / benchmark_ops, 
             (bench_time.as_nanos() as f64 / benchmark_ops as f64) / 1000.0);
    println!("  Benchmark throughput: {:.0} ops/second", bench_throughput);

    // Get final statistics
    let stats = engine.stats();
    let _ = engine.flush();

    println!("\n📊 Temporal Engine Statistics:");
    println!("  Total reads: {}", stats.reads);
    println!("  Total writes: {}", stats.writes);
    println!("  Interval splits: {}", stats.interval_splits);
    println!("  Active records: {}", stats.active_records);
    println!("  Extent count: {}", stats.extent_count);

    println!("\n📊 Performance Summary:");
    println!("  ┌─────────────────────────────────────────────────────┐");
    println!("  │ Temporal Interval Architecture Results             │");
    println!("  ├─────────────────────────────────────────────────────┤");
    println!("  │ Single PUT:             {:>15.1} μs     │", put1_time.as_nanos() as f64 / 1000.0);
    println!("  │ Single GET:             {:>15.1} μs     │", get1_time.as_nanos() as f64 / 1000.0);
    println!("  │ Interval split PUT:     {:>15.1} μs     │", put2_time.as_nanos() as f64 / 1000.0);
    println!("  │ Range query:            {:>15.1} μs     │", range_time.as_nanos() as f64 / 1000.0);
    println!("  │ Average benchmark op:   {:>15.1} μs     │", (bench_time.as_nanos() as f64 / benchmark_ops as f64) / 1000.0);
    println!("  │ Concurrent throughput:  {:>15.1} ops/s │", throughput);
    println!("  │ Benchmark throughput:   {:>15.1} ops/s │", bench_throughput);
    println!("  └─────────────────────────────────────────────────────┘");

    println!("\n🎯 Temporal Interval Architecture Benefits:");
    println!("  ✓ True temporal versioning with interval splitting");
    println!("  ✓ Efficient latest-version queries (no timestamp guessing)");
    println!("  ✓ Morton spatial locality preserved");
    println!("  ✓ Range queries across space AND time");
    println!("  ✓ MVCC semantics with explicit validity periods");

    if stats.interval_splits > 0 {
        println!("  ✓ Interval splitting working ({} splits performed)", stats.interval_splits);
    }

    if stats.active_records > 0 {
        println!("  ✓ Active record caching ({} records in memory)", stats.active_records);
    }

    let target_achieved = get1_time.as_micros() <= 10 && put1_time.as_micros() <= 50;
    if target_achieved {
        println!("  🏆 PERFORMANCE TARGETS ACHIEVED!");
    }

    println!("\n✅ Temporal Interval Demo Completed Successfully!");
    println!("Database: {} ({} MB)", 
             db_path.display(), 
             std::fs::metadata(&db_path)?.len() / 1024 / 1024);

    Ok(())
}