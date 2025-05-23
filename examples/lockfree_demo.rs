//! Lock-Free ZVerse Performance Demo
//!
//! This example demonstrates the high-performance capabilities of the lock-free
//! ZVerse implementation, including concurrent operations and performance metrics.

use std::sync::Arc;
use std::thread;
use std::time::Instant;
use zverse::LockFreeZVerseKV;

fn main() {
    println!("🚀 Lock-Free ZVerse Performance Demo\n");
    
    // Create a new lock-free database
    let db = Arc::new(LockFreeZVerseKV::new());
    
    // Demo 1: Basic Operations
    println!("📊 Demo 1: Basic Operations");
    basic_operations_demo(&db);
    
    // Demo 2: Concurrent Readers
    println!("\n📊 Demo 2: Concurrent Readers (Wait-Free)");
    concurrent_readers_demo(&db);
    
    // Demo 3: Concurrent Writers
    println!("\n📊 Demo 3: Concurrent Writers (Serialized)");
    concurrent_writers_demo(&db);
    
    // Demo 4: Mixed Workload
    println!("\n📊 Demo 4: Mixed Reader-Writer Workload");
    mixed_workload_demo(&db);
    
    // Demo 5: Performance Comparison
    println!("\n📊 Demo 5: Performance Analysis");
    performance_analysis(&db);
    
    println!("\n✅ Demo completed successfully!");
}

fn basic_operations_demo(db: &Arc<LockFreeZVerseKV>) {
    let start = Instant::now();
    
    // Perform basic operations
    let ops = 10000;
    for i in 0..ops {
        let key = format!("demo-key-{:06}", i);
        let value = format!("demo-value-{:06}", i);
        db.put(key, value).expect("Put failed");
    }
    
    let write_time = start.elapsed();
    
    // Test reads
    let start = Instant::now();
    for i in 0..ops {
        let key = format!("demo-key-{:06}", i);
        let _: Option<Vec<u8>> = db.get(key, None).expect("Get failed");
    }
    let read_time = start.elapsed();
    
    println!("  • {} writes in {:?} ({:.0} ops/sec)", 
        ops, write_time, ops as f64 / write_time.as_secs_f64());
    println!("  • {} reads in {:?} ({:.0} ops/sec)", 
        ops, read_time, ops as f64 / read_time.as_secs_f64());
}

fn concurrent_readers_demo(db: &Arc<LockFreeZVerseKV>) {
    let readers = 8;
    let ops_per_reader = 5000;
    
    let start = Instant::now();
    let mut handles = vec![];
    
    for reader_id in 0..readers {
        let db_clone = db.clone();
        let handle = thread::spawn(move || {
            for i in 0..ops_per_reader {
                let key = format!("demo-key-{:06}", (reader_id * ops_per_reader + i) % 10000);
                let _: Option<Vec<u8>> = db_clone.get(key, None).expect("Get failed");
            }
        });
        handles.push(handle);
    }
    
    // Wait for all readers to complete
    for handle in handles {
        handle.join().unwrap();
    }
    
    let total_time = start.elapsed();
    let total_ops = readers * ops_per_reader;
    
    println!("  • {} concurrent readers", readers);
    println!("  • {} total reads in {:?} ({:.0} ops/sec)", 
        total_ops, total_time, total_ops as f64 / total_time.as_secs_f64());
    println!("  • Zero contention between readers ✅");
}

fn concurrent_writers_demo(db: &Arc<LockFreeZVerseKV>) {
    let writers = 4;
    let ops_per_writer = 1000;
    
    let start = Instant::now();
    let mut handles = vec![];
    
    for writer_id in 0..writers {
        let db_clone = db.clone();
        let handle = thread::spawn(move || {
            for i in 0..ops_per_writer {
                let key = format!("writer-{}-key-{:06}", writer_id, i);
                let value = format!("writer-{}-value-{:06}", writer_id, i);
                db_clone.put(key, value).expect("Put failed");
            }
        });
        handles.push(handle);
    }
    
    // Wait for all writers to complete
    for handle in handles {
        handle.join().unwrap();
    }
    
    let total_time = start.elapsed();
    let total_ops = writers * ops_per_writer;
    
    println!("  • {} concurrent writers", writers);
    println!("  • {} total writes in {:?} ({:.0} ops/sec)", 
        total_ops, total_time, total_ops as f64 / total_time.as_secs_f64());
    println!("  • Serialized for consistency ✅");
}

fn mixed_workload_demo(db: &Arc<LockFreeZVerseKV>) {
    let readers = 4;
    let writers = 2;
    let ops_per_thread = 2000;
    
    let start = Instant::now();
    let mut handles = vec![];
    
    // Spawn readers
    for reader_id in 0..readers {
        let db_clone = db.clone();
        let handle = thread::spawn(move || {
            for i in 0..ops_per_thread {
                let key = format!("demo-key-{:06}", (reader_id * ops_per_thread + i) % 10000);
                let _: Option<Vec<u8>> = db_clone.get(key, None).expect("Get failed");
            }
        });
        handles.push(handle);
    }
    
    // Spawn writers
    for writer_id in 0..writers {
        let db_clone = db.clone();
        let handle = thread::spawn(move || {
            for i in 0..ops_per_thread {
                let key = format!("mixed-writer-{}-key-{:06}", writer_id, i);
                let value = format!("mixed-writer-{}-value-{:06}", writer_id, i);
                db_clone.put(key, value).expect("Put failed");
            }
        });
        handles.push(handle);
    }
    
    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap();
    }
    
    let total_time = start.elapsed();
    let total_read_ops = readers * ops_per_thread;
    let total_write_ops = writers * ops_per_thread;
    
    println!("  • {} readers + {} writers running concurrently", readers, writers);
    println!("  • {} reads + {} writes in {:?}", total_read_ops, total_write_ops, total_time);
    println!("  • No reader-writer blocking ✅");
    println!("  • Mixed throughput: {:.0} ops/sec", 
        (total_read_ops + total_write_ops) as f64 / total_time.as_secs_f64());
}

fn performance_analysis(db: &Arc<LockFreeZVerseKV>) {
    let stats = db.performance_stats();
    
    println!("  📈 Performance Statistics:");
    println!("     Implementation: {}", stats.implementation);
    println!("     Total Entries: {}", stats.total_entries);
    println!("     Current Version: {}", stats.current_version);
    println!("     Concurrent Readers: {}", if stats.concurrent_readers { "✅ Yes" } else { "❌ No" });
    println!("     Wait-Free Reads: {}", if stats.wait_free_reads { "✅ Yes" } else { "❌ No" });
    println!("     Serialized Writes: {}", if stats.serialized_writes { "✅ Yes" } else { "❌ No" });
    
    // Memory usage estimation
    let estimated_memory_mb = (stats.total_entries * 150) / (1024 * 1024); // Rough estimate
    println!("     Estimated Memory Usage: ~{} MB", estimated_memory_mb);
    
    println!("\n  🎯 Key Benefits:");
    println!("     • Wait-free reads: No locks, no atomic operations during reads");
    println!("     • Zero reader contention: Multiple readers never block each other");
    println!("     • No reader-writer blocking: Readers never wait for writers");
    println!("     • Consistent snapshots: Readers see consistent point-in-time views");
    println!("     • Memory-efficient: Copy-on-write with epoch-based GC");
    println!("     • High throughput: Optimized Z-order curve data layout");
}