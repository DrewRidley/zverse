//! Multithreaded performance demonstration of ZVerse Morton Temporal Database
//!
//! This example demonstrates the high-performance concurrent capabilities
//! using 16 threads to show the lock-free scaling benefits.

use zverse::{MortonTemporalDB, MortonResult};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use std::sync::atomic::{AtomicUsize, Ordering};

const THREAD_COUNT: usize = 16;
const OPS_PER_THREAD: usize = 50_000;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 ZVerse Multithreaded Performance Demo");
    println!("========================================");
    println!("Testing with {} threads, {} ops per thread", THREAD_COUNT, OPS_PER_THREAD);
    
    let db = Arc::new(MortonTemporalDB::new());
    
    // Concurrent write test
    println!("\n✍️  Concurrent Write Performance Test");
    test_concurrent_writes(&db)?;
    
    // Concurrent read test
    println!("\n📖 Concurrent Read Performance Test");
    test_concurrent_reads(&db)?;
    
    // Mixed workload test
    println!("\n🔄 Mixed Workload Test (70% reads, 30% writes)");
    test_mixed_workload(&db)?;
    
    // Temporal batch test
    println!("\n⏰ Temporal Batch Operations Test");
    test_temporal_batch_operations(&db)?;
    

    
    println!("\n🏆 Multithreaded demo completed successfully!");
    Ok(())
}

fn test_concurrent_writes(db: &Arc<MortonTemporalDB>) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let success_counter = Arc::new(AtomicUsize::new(0));
    
    let handles: Vec<_> = (0..THREAD_COUNT).map(|thread_id| {
        let db_clone = Arc::clone(db);
        let counter_clone = Arc::clone(&success_counter);
        
        thread::spawn(move || {
            let mut local_success = 0;
            for i in 0..OPS_PER_THREAD {
                let key = format!("write_test:{}:{}", thread_id, i);
                let value = format!("Thread {} value {}", thread_id, i).into_bytes();
                
                if db_clone.put(&key, value).is_ok() {
                    local_success += 1;
                }
            }
            counter_clone.fetch_add(local_success, Ordering::Relaxed);
        })
    }).collect();
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let duration = start.elapsed();
    let total_ops = success_counter.load(Ordering::Relaxed);
    let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
    
    println!("  Total operations: {}", total_ops);
    println!("  Duration: {:?}", duration);
    println!("  Throughput: {:.0} writes/sec", ops_per_sec);
    println!("  Per-thread average: {:.0} writes/sec", ops_per_sec / THREAD_COUNT as f64);
    
    Ok(())
}

fn test_concurrent_reads(db: &Arc<MortonTemporalDB>) -> Result<(), Box<dyn std::error::Error>> {
    // First, populate database with test data
    println!("  Populating database for read test...");
    for i in 0..10_000 {
        let key = format!("read_test:{}", i);
        let value = format!("Read test value {}", i).into_bytes();
        db.put(&key, value)?;
    }
    
    let start = Instant::now();
    let success_counter = Arc::new(AtomicUsize::new(0));
    
    let handles: Vec<_> = (0..THREAD_COUNT).map(|thread_id| {
        let db_clone = Arc::clone(db);
        let counter_clone = Arc::clone(&success_counter);
        
        thread::spawn(move || {
            let mut rng_state = thread_id * 12345; // Simple LCG for deterministic randomness
            let mut local_success = 0;
            
            for _ in 0..OPS_PER_THREAD {
                // Simple LCG random number generator
                rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);
                let key_id = (rng_state % 10_000) as usize;
                let key = format!("read_test:{}", key_id);
                
                if db_clone.get(&key).is_ok() {
                    local_success += 1;
                }
            }
            counter_clone.fetch_add(local_success, Ordering::Relaxed);
        })
    }).collect();
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let duration = start.elapsed();
    let total_ops = success_counter.load(Ordering::Relaxed);
    let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
    let hit_rate = (total_ops as f64 / (THREAD_COUNT * OPS_PER_THREAD) as f64) * 100.0;
    
    println!("  Total operations: {}", total_ops);
    println!("  Duration: {:?}", duration);
    println!("  Throughput: {:.0} reads/sec", ops_per_sec);
    println!("  Hit rate: {:.1}%", hit_rate);
    println!("  Per-thread average: {:.0} reads/sec", ops_per_sec / THREAD_COUNT as f64);
    
    Ok(())
}

fn test_mixed_workload(db: &Arc<MortonTemporalDB>) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let read_counter = Arc::new(AtomicUsize::new(0));
    let write_counter = Arc::new(AtomicUsize::new(0));
    
    let handles: Vec<_> = (0..THREAD_COUNT).map(|thread_id| {
        let db_clone = Arc::clone(db);
        let read_counter_clone = Arc::clone(&read_counter);
        let write_counter_clone = Arc::clone(&write_counter);
        
        thread::spawn(move || {
            let mut rng_state = thread_id * 54321;
            let mut local_reads = 0;
            let mut local_writes = 0;
            
            for i in 0..OPS_PER_THREAD {
                rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);
                let is_write = (rng_state % 100) < 30; // 30% writes, 70% reads
                
                if is_write {
                    let key = format!("mixed:{}:{}", thread_id, i);
                    let value = format!("Mixed workload value {}", i).into_bytes();
                    if db_clone.put(&key, value).is_ok() {
                        local_writes += 1;
                    }
                } else {
                    let key_id = (rng_state % 50_000) as usize;
                    let key = format!("write_test:{}:{}", key_id % THREAD_COUNT, key_id / THREAD_COUNT);
                    if db_clone.get(&key).is_ok() {
                        local_reads += 1;
                    }
                }
            }
            
            read_counter_clone.fetch_add(local_reads, Ordering::Relaxed);
            write_counter_clone.fetch_add(local_writes, Ordering::Relaxed);
        })
    }).collect();
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let duration = start.elapsed();
    let total_reads = read_counter.load(Ordering::Relaxed);
    let total_writes = write_counter.load(Ordering::Relaxed);
    let total_ops = total_reads + total_writes;
    let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
    
    println!("  Total operations: {} ({} reads, {} writes)", total_ops, total_reads, total_writes);
    println!("  Duration: {:?}", duration);
    println!("  Combined throughput: {:.0} ops/sec", ops_per_sec);
    println!("  Read throughput: {:.0} reads/sec", total_reads as f64 / duration.as_secs_f64());
    println!("  Write throughput: {:.0} writes/sec", total_writes as f64 / duration.as_secs_f64());
    
    Ok(())
}

fn test_temporal_batch_operations(db: &Arc<MortonTemporalDB>) -> Result<(), Box<dyn std::error::Error>> {
    let start = Instant::now();
    let success_counter = Arc::new(AtomicUsize::new(0));
    
    let handles: Vec<_> = (0..THREAD_COUNT).map(|thread_id| {
        let db_clone = Arc::clone(db);
        let counter_clone = Arc::clone(&success_counter);
        
        thread::spawn(move || {
            let batch_size = 100;
            let batches = OPS_PER_THREAD / batch_size;
            let mut total_success = 0;
            
            for batch_id in 0..batches {
                let mut batch_data = Vec::with_capacity(batch_size);
                
                for i in 0..batch_size {
                    let key = format!("batch:{}:{}:{}", thread_id, batch_id, i);
                    let value = format!("Batch value thread {} batch {} item {}", thread_id, batch_id, i).into_bytes();
                    batch_data.push((key, value));
                }
                
                let inserted = db_clone.put_temporal_batch(&batch_data);
                total_success += inserted;
            }
            
            counter_clone.fetch_add(total_success, Ordering::Relaxed);
        })
    }).collect();
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let duration = start.elapsed();
    let total_ops = success_counter.load(Ordering::Relaxed);
    let ops_per_sec = total_ops as f64 / duration.as_secs_f64();
    
    println!("  Total batch operations: {}", total_ops);
    println!("  Duration: {:?}", duration);
    println!("  Batch throughput: {:.0} records/sec", ops_per_sec);
    println!("  Per-thread average: {:.0} records/sec", ops_per_sec / THREAD_COUNT as f64);
    
    // Test batch reads
    let batch_read_start = Instant::now();
    let read_success_counter = Arc::new(AtomicUsize::new(0));
    
    let read_handles: Vec<_> = (0..THREAD_COUNT).map(|thread_id| {
        let db_clone = Arc::clone(db);
        let counter_clone = Arc::clone(&read_success_counter);
        
        thread::spawn(move || {
            let batch_size = 50;
            let batches = 1000;
            let mut total_success = 0;
            
            for batch_id in 0..batches {
                let keys: Vec<String> = (0..batch_size)
                    .map(|i| format!("batch:{}:{}:{}", thread_id, batch_id % 500, i))
                    .collect();
                let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
                
                let results = db_clone.get_batch(&key_refs);
                total_success += results.iter().filter(|r| r.is_ok()).count();
            }
            
            counter_clone.fetch_add(total_success, Ordering::Relaxed);
        })
    }).collect();
    
    for handle in read_handles {
        handle.join().unwrap();
    }
    
    let read_duration = batch_read_start.elapsed();
    let total_read_ops = read_success_counter.load(Ordering::Relaxed);
    let read_ops_per_sec = total_read_ops as f64 / read_duration.as_secs_f64();
    
    println!("  Batch read operations: {}", total_read_ops);
    println!("  Batch read throughput: {:.0} reads/sec", read_ops_per_sec);
    
    Ok(())
}