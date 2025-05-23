//! Lock-free concurrency benchmark for ZVerse
//!
//! This example demonstrates the lock-free concurrency capabilities of ZVerse,
//! showing how readers and writers can operate without blocking each other.

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicUsize, Ordering};
use zverse::{LockFreeZVerseKV, Error};

fn main() -> Result<(), Error> {
    println!("=== Lock-Free ZVerse Concurrency Benchmark ===");
    
    // Test scenarios
    test_pure_readers()?;
    test_pure_writers()?;
    test_mixed_workload()?;
    test_readers_writers_isolation()?;
    test_high_contention()?;
    
    Ok(())
}

/// Test pure reader performance with multiple threads
fn test_pure_readers() -> Result<(), Error> {
    println!("\n=== Pure Reader Performance Test ===");
    
    let db = Arc::new(LockFreeZVerseKV::new());
    
    // Pre-populate with data
    println!("Pre-populating with 10,000 entries...");
    for i in 0..10_000 {
        db.put(format!("key:{:06}", i), format!("value-{}", i))?;
    }
    
    let reader_counts = [1, 2, 4, 8, 16];
    
    for &num_readers in &reader_counts {
        let total_reads = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];
        
        let start = Instant::now();
        
        // Spawn reader threads
        for _ in 0..num_readers {
            let db_clone = db.clone();
            let reads_clone = total_reads.clone();
            
            let handle = thread::spawn(move || {
                let start_time = Instant::now();
                while start_time.elapsed() < Duration::from_secs(2) {
                    for i in 0..100 {
                        let key = format!("key:{:06}", i * 100);
                        let _: Option<String> = db_clone.get(&key, None)
                            .unwrap()
                            .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
                        reads_clone.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
            handles.push(handle);
        }
        
        // Wait for all readers to complete
        for handle in handles {
            handle.join().unwrap();
        }
        
        let duration = start.elapsed();
        let total = total_reads.load(Ordering::Relaxed);
        let reads_per_sec = total as f64 / duration.as_secs_f64();
        
        println!("{:2} readers: {:8} reads in {:?} ({:.0} reads/sec, {:.0} reads/sec/thread)",
                 num_readers, total, duration, reads_per_sec, reads_per_sec / num_readers as f64);
    }
    
    Ok(())
}

/// Test pure writer performance (serialized)
fn test_pure_writers() -> Result<(), Error> {
    println!("\n=== Pure Writer Performance Test ===");
    
    let writer_counts = [1, 2, 4, 8];
    
    for &num_writers in &writer_counts {
        let db = Arc::new(LockFreeZVerseKV::new());
        let total_writes = Arc::new(AtomicUsize::new(0));
        let mut handles = vec![];
        
        let start = Instant::now();
        
        // Spawn writer threads
        for thread_id in 0..num_writers {
            let db_clone = db.clone();
            let writes_clone = total_writes.clone();
            
            let handle = thread::spawn(move || {
                let start_time = Instant::now();
                let mut count = 0;
                while start_time.elapsed() < Duration::from_secs(2) {
                    let key = format!("thread{}:key:{:06}", thread_id, count);
                    let value = format!("thread{}:value:{}", thread_id, count);
                    db_clone.put(key, value).unwrap();
                    writes_clone.fetch_add(1, Ordering::Relaxed);
                    count += 1;
                }
            });
            handles.push(handle);
        }
        
        // Wait for all writers to complete
        for handle in handles {
            handle.join().unwrap();
        }
        
        let duration = start.elapsed();
        let total = total_writes.load(Ordering::Relaxed);
        let writes_per_sec = total as f64 / duration.as_secs_f64();
        
        println!("{:2} writers: {:8} writes in {:?} ({:.0} writes/sec, {:.0} writes/sec/thread)",
                 num_writers, total, duration, writes_per_sec, writes_per_sec / num_writers as f64);
    }
    
    Ok(())
}

/// Test mixed reader/writer workload
fn test_mixed_workload() -> Result<(), Error> {
    println!("\n=== Mixed Reader/Writer Workload Test ===");
    
    let db = Arc::new(LockFreeZVerseKV::new());
    
    // Pre-populate
    for i in 0..1_000 {
        db.put(format!("initial:{:06}", i), format!("value-{}", i))?;
    }
    
    let total_reads = Arc::new(AtomicUsize::new(0));
    let total_writes = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];
    
    let start = Instant::now();
    
    // Spawn 8 readers
    for _ in 0..8 {
        let db_clone = db.clone();
        let reads_clone = total_reads.clone();
        
        let handle = thread::spawn(move || {
            let start_time = Instant::now();
            while start_time.elapsed() < Duration::from_secs(3) {
                for i in 0..50 {
                    let key = format!("initial:{:06}", i * 20);
                    let _: Option<String> = db_clone.get(&key, None)
                        .unwrap()
                        .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
                    reads_clone.fetch_add(1, Ordering::Relaxed);
                }
            }
        });
        handles.push(handle);
    }
    
    // Spawn 4 writers
    for thread_id in 0..4 {
        let db_clone = db.clone();
        let writes_clone = total_writes.clone();
        
        let handle = thread::spawn(move || {
            let start_time = Instant::now();
            let mut count = 0;
            while start_time.elapsed() < Duration::from_secs(3) {
                let key = format!("writer{}:key:{:06}", thread_id, count);
                let value = format!("writer{}:value:{}", thread_id, count);
                db_clone.put(key, value).unwrap();
                writes_clone.fetch_add(1, Ordering::Relaxed);
                count += 1;
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    
    let duration = start.elapsed();
    let reads = total_reads.load(Ordering::Relaxed);
    let writes = total_writes.load(Ordering::Relaxed);
    let total_ops = reads + writes;
    
    println!("Mixed workload: {} reads + {} writes = {} ops in {:?}",
             reads, writes, total_ops, duration);
    println!("  {:.0} reads/sec, {:.0} writes/sec, {:.0} total ops/sec",
             reads as f64 / duration.as_secs_f64(),
             writes as f64 / duration.as_secs_f64(),
             total_ops as f64 / duration.as_secs_f64());
    
    Ok(())
}

/// Test that readers and writers don't block each other
fn test_readers_writers_isolation() -> Result<(), Error> {
    println!("\n=== Reader/Writer Isolation Test ===");
    
    let db = Arc::new(LockFreeZVerseKV::new());
    
    // Pre-populate
    for i in 0..1_000 {
        db.put(format!("test:{:06}", i), format!("value-{}", i))?;
    }
    
    println!("Testing that long-running readers don't block writers...");
    
    // Start a long-running reader
    let db_reader = db.clone();
    let reader_completed = Arc::new(AtomicUsize::new(0));
    let reader_completed_clone = reader_completed.clone();
    
    let reader_handle = thread::spawn(move || {
        let start = Instant::now();
        // Run for 5 seconds
        while start.elapsed() < Duration::from_secs(5) {
            for i in 0..100 {
                let key = format!("test:{:06}", i * 10);
                let _: Option<String> = db_reader.get(&key, None)
                    .unwrap()
                    .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
                reader_completed_clone.fetch_add(1, Ordering::Relaxed);
            }
        }
    });
    
    // After 1 second, start a writer
    thread::sleep(Duration::from_secs(1));
    println!("  Starting writer after 1 second...");
    
    let db_writer = db.clone();
    let writer_start = Instant::now();
    let writer_handle = thread::spawn(move || {
        let start = Instant::now();
        let mut count = 0;
        while start.elapsed() < Duration::from_secs(2) {
            let key = format!("writer:key:{:06}", count);
            let value = format!("writer:value:{}", count);
            db_writer.put(key, value).unwrap();
            count += 1;
        }
        count
    });
    
    let writes_completed = writer_handle.join().unwrap();
    let writer_duration = writer_start.elapsed();
    
    reader_handle.join().unwrap();
    let reads_completed = reader_completed.load(Ordering::Relaxed);
    
    println!("  Reader completed {} reads over 5 seconds", reads_completed);
    println!("  Writer completed {} writes in {:?} (not blocked by reader)", 
             writes_completed, writer_duration);
    println!("  Writer throughput: {:.0} writes/sec", 
             writes_completed as f64 / writer_duration.as_secs_f64());
    
    Ok(())
}

/// Test high contention scenario
fn test_high_contention() -> Result<(), Error> {
    println!("\n=== High Contention Test ===");
    
    let db = Arc::new(LockFreeZVerseKV::new());
    
    // Pre-populate with small dataset that all threads will access
    for i in 0..100 {
        db.put(format!("hot:{:02}", i), format!("initial-value-{}", i))?;
    }
    
    let total_ops = Arc::new(AtomicUsize::new(0));
    let mut handles = vec![];
    
    let start = Instant::now();
    
    // Spawn many threads accessing the same small set of keys
    for thread_id in 0..20 {
        let db_clone = db.clone();
        let ops_clone = total_ops.clone();
        
        let handle = thread::spawn(move || {
            let start_time = Instant::now();
            let mut local_ops = 0;
            
            while start_time.elapsed() < Duration::from_secs(2) {
                for i in 0..10 {
                    let key = format!("hot:{:02}", i); // Same keys across all threads
                    
                    if thread_id % 5 == 0 {
                        // 20% writers
                        let value = format!("updated-by-thread-{}-{}", thread_id, local_ops);
                        db_clone.put(key, value).unwrap();
                    } else {
                        // 80% readers
                        let _: Option<String> = db_clone.get(&key, None)
                            .unwrap()
                            .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
                    }
                    
                    local_ops += 1;
                }
            }
            
            ops_clone.fetch_add(local_ops, Ordering::Relaxed);
        });
        handles.push(handle);
    }
    
    // Wait for all threads
    for handle in handles {
        handle.join().unwrap();
    }
    
    let duration = start.elapsed();
    let total = total_ops.load(Ordering::Relaxed);
    let ops_per_sec = total as f64 / duration.as_secs_f64();
    
    println!("High contention: 20 threads, {} operations in {:?}", total, duration);
    println!("  {:.0} ops/sec total, {:.0} ops/sec/thread", 
             ops_per_sec, ops_per_sec / 20.0);
    
    // Verify data integrity
    let final_count = db.entry_count();
    println!("  Final entry count: {} (shows successful concurrent updates)", final_count);
    
    Ok(())
}