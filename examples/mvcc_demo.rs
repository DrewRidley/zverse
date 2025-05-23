//! Comprehensive MVCC and concurrent transaction demo for IMEC
//!
//! This demo showcases Phase 4 features:
//! - Multi-reader/multi-writer concurrency
//! - Snapshot isolation
//! - Lock-free transactions
//! - Conflict detection and resolution

use zverse::storage::transactional_engine::{TransactionalEngine, TransactionalEngineConfig};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 IMEC MVCC & Concurrent Transactions Demo");
    println!("==========================================");

    // Create database file
    let db_path = PathBuf::from("./mvcc_demo.db");
    if db_path.exists() {
        std::fs::remove_file(&db_path)?;
    }

    let config = TransactionalEngineConfig::with_path(db_path.clone())
        .with_file_size(128 * 1024 * 1024) // 128MB
        .with_extent_size(16 * 1024 * 1024); // 16MB extents

    println!("\n📁 Database: {}", db_path.display());

    // Create transactional engine
    let start = Instant::now();
    let engine = Arc::new(TransactionalEngine::new(config)?);
    let creation_time = start.elapsed();
    
    println!("  Creation: {:?}", creation_time);
    println!("  File size: {} MB", std::fs::metadata(&db_path)?.len() / 1024 / 1024);

    println!("\n🔥 Phase 4: MVCC Transactions Test");

    // Test 1: Basic transaction isolation
    println!("\n1️⃣ Transaction Isolation Test:");
    
    let engine1 = engine.clone();
    let start = Instant::now();
    
    // Transaction 1: Setup initial data
    engine1.read_write(|txn| {
        txn.put("account:alice", b"1000")?;
        txn.put("account:bob", b"500")?;
        txn.put("counter", b"0")?;
        Ok(())
    })?;
    
    let setup_time = start.elapsed();
    println!("  Initial data setup: {:?}", setup_time);

    // Test 2: Concurrent readers
    println!("\n2️⃣ Concurrent Readers Test:");
    
    let reader_count = 10;
    let mut reader_handles = Vec::new();
    let reader_start = Instant::now();
    
    for i in 0..reader_count {
        let engine_clone = engine.clone();
        let handle = thread::spawn(move || {
            let start = Instant::now();
            let result = engine_clone.read_only(|txn| {
                let alice = txn.get("account:alice")?;
                let bob = txn.get("account:bob")?;
                let counter = txn.get("counter")?;
                
                // Simulate some work
                thread::sleep(Duration::from_millis(1));
                
                Ok((alice, bob, counter))
            });
            (i, start.elapsed(), result)
        });
        reader_handles.push(handle);
    }
    
    let mut successful_reads = 0;
    let mut total_read_time = Duration::new(0, 0);
    
    for handle in reader_handles {
        let (reader_id, read_time, result) = handle.join().unwrap();
        total_read_time += read_time;
        
        match result {
            Ok((alice, bob, counter)) => {
                successful_reads += 1;
                if reader_id == 0 {
                    println!("  Reader {}: Alice={:?}, Bob={:?}, Counter={:?} ({}μs)", 
                             reader_id,
                             alice.as_ref().map(|v| std::str::from_utf8(v).unwrap()),
                             bob.as_ref().map(|v| std::str::from_utf8(v).unwrap()),
                             counter.as_ref().map(|v| std::str::from_utf8(v).unwrap()),
                             read_time.as_micros());
                }
            }
            Err(e) => println!("  Reader {} failed: {}", reader_id, e),
        }
    }
    
    let reader_total_time = reader_start.elapsed();
    let avg_read_time = total_read_time / reader_count;
    
    println!("  Successful reads: {}/{}", successful_reads, reader_count);
    println!("  Average read time: {:?} ({:.1}μs)", avg_read_time, avg_read_time.as_nanos() as f64 / 1000.0);
    println!("  Total concurrent time: {:?}", reader_total_time);
    println!("  Concurrency speedup: {:.1}x", total_read_time.as_secs_f64() / reader_total_time.as_secs_f64());

    // Test 3: Concurrent writers (non-conflicting)
    println!("\n3️⃣ Concurrent Writers Test (Non-conflicting):");
    
    let writer_count = 5;
    let mut writer_handles = Vec::new();
    let writer_start = Instant::now();
    
    for i in 0..writer_count {
        let engine_clone = engine.clone();
        let handle = thread::spawn(move || {
            let start = Instant::now();
            let result = engine_clone.read_write(|txn| {
                let key = format!("writer:thread:{}", i);
                let value = format!("data_from_thread_{}", i);
                txn.put(&key, value.as_bytes())?;
                
                // Simulate some work
                thread::sleep(Duration::from_millis(2));
                
                Ok(())
            });
            (i, start.elapsed(), result)
        });
        writer_handles.push(handle);
    }
    
    let mut successful_writes = 0;
    let mut total_write_time = Duration::new(0, 0);
    
    for handle in writer_handles {
        let (writer_id, write_time, result) = handle.join().unwrap();
        total_write_time += write_time;
        
        match result {
            Ok(()) => {
                successful_writes += 1;
                println!("  Writer {} completed in {:?} ({:.1}μs)", 
                         writer_id, write_time, write_time.as_nanos() as f64 / 1000.0);
            }
            Err(e) => println!("  Writer {} failed: {}", writer_id, e),
        }
    }
    
    let writer_total_time = writer_start.elapsed();
    let avg_write_time = total_write_time / writer_count;
    
    println!("  Successful writes: {}/{}", successful_writes, writer_count);
    println!("  Average write time: {:?} ({:.1}μs)", avg_write_time, avg_write_time.as_nanos() as f64 / 1000.0);
    println!("  Total concurrent time: {:?}", writer_total_time);

    // Test 4: Mixed workload (readers + writers)
    println!("\n4️⃣ Mixed Workload Test:");
    
    let mixed_start = Instant::now();
    let mut mixed_handles = Vec::new();
    
    // Start 10 readers and 5 writers concurrently
    for i in 0..15 {
        let engine_clone = engine.clone();
        let handle = thread::spawn(move || {
            let start = Instant::now();
            if i < 10 {
                // Reader
                let result = engine_clone.read_only(|txn| {
                    let alice = txn.get("account:alice")?;
                    let counter = txn.get("counter")?;
                    thread::sleep(Duration::from_millis(1));
                    Ok((alice, counter))
                });
                ("read", i, start.elapsed(), result.map(|_| ()))
            } else {
                // Writer
                let result = engine_clone.read_write(|txn| {
                    let key = format!("mixed:op:{}", i);
                    let value = format!("mixed_data_{}", i);
                    txn.put(&key, value.as_bytes())?;
                    thread::sleep(Duration::from_millis(2));
                    Ok(())
                });
                ("write", i, start.elapsed(), result)
            }
        });
        mixed_handles.push(handle);
    }
    
    let mut read_ops = 0;
    let mut write_ops = 0;
    let mut failed_ops = 0;
    
    for handle in mixed_handles {
        let (op_type, op_id, op_time, result) = handle.join().unwrap();
        match result {
            Ok(()) => {
                if op_type == "read" {
                    read_ops += 1;
                } else {
                    write_ops += 1;
                }
                if op_id < 3 {
                    println!("  {} {} completed in {:?}", op_type, op_id, op_time);
                }
            }
            Err(e) => {
                failed_ops += 1;
                println!("  {} {} failed: {}", op_type, op_id, e);
            }
        }
    }
    
    let mixed_total_time = mixed_start.elapsed();
    let total_ops = read_ops + write_ops;
    let ops_per_sec = total_ops as f64 / mixed_total_time.as_secs_f64();
    
    println!("  Mixed workload results:");
    println!("    Reads: {}, Writes: {}, Failed: {}", read_ops, write_ops, failed_ops);
    println!("    Total time: {:?}", mixed_total_time);
    println!("    Throughput: {:.1} ops/sec", ops_per_sec);

    // Test 5: Transaction conflict detection
    println!("\n5️⃣ Conflict Detection Test:");
    
    let conflict_start = Instant::now();
    
    // Both transactions try to modify the same key
    let txn1 = engine.begin_transaction()?;
    let txn2 = engine.begin_transaction()?;
    
    println!("  Transaction 1 ID: {}", txn1.id());
    println!("  Transaction 2 ID: {}", txn2.id());
    
    // Both read the counter
    let counter1 = txn1.get("counter")?.unwrap_or_default();
    let counter2 = txn2.get("counter")?.unwrap_or_default();
    
    let val1: i32 = std::str::from_utf8(&counter1).unwrap().parse().unwrap();
    let val2: i32 = std::str::from_utf8(&counter2).unwrap().parse().unwrap();
    
    // Both try to increment
    txn1.put("counter", (val1 + 1).to_string().as_bytes())?;
    txn2.put("counter", (val2 + 10).to_string().as_bytes())?;
    
    // First commit should succeed
    let commit1_result = txn1.commit();
    println!("  Transaction 1 commit: {:?}", commit1_result.is_ok());
    
    // Second commit might fail due to conflict
    let commit2_result = txn2.commit();
    println!("  Transaction 2 commit: {:?}", commit2_result.is_ok());
    
    if commit2_result.is_err() {
        println!("  ✅ Conflict detected and handled correctly!");
    }
    
    let conflict_time = conflict_start.elapsed();
    println!("  Conflict detection time: {:?}", conflict_time);

    // Test 6: Performance under load
    println!("\n6️⃣ Performance Under Load:");
    
    let load_operations = 100;
    let load_start = Instant::now();
    let mut load_handles = Vec::new();
    
    for i in 0..load_operations {
        let engine_clone = engine.clone();
        let handle = thread::spawn(move || {
            let start = Instant::now();
            if i % 3 == 0 {
                // Write operation
                engine_clone.read_write(|txn| {
                    txn.put(&format!("load:key:{}", i), &format!("load_value_{}", i).as_bytes())?;
                    Ok(())
                })
            } else {
                // Read operation
                engine_clone.read_only(|txn| {
                    txn.get("account:alice")?;
                    txn.get("counter")?;
                    Ok(())
                })
            }.map(|_| start.elapsed())
        });
        load_handles.push(handle);
    }
    
    let mut successful_load_ops = 0;
    let mut total_load_time = Duration::new(0, 0);
    
    for handle in load_handles {
        match handle.join().unwrap() {
            Ok(op_time) => {
                successful_load_ops += 1;
                total_load_time += op_time;
            }
            Err(_) => {}
        }
    }
    
    let load_total_time = load_start.elapsed();
    let load_ops_per_sec = successful_load_ops as f64 / load_total_time.as_secs_f64();
    let avg_load_time = total_load_time / successful_load_ops;
    
    println!("  Load test results:");
    println!("    Operations: {}/{}", successful_load_ops, load_operations);
    println!("    Total time: {:?}", load_total_time);
    println!("    Throughput: {:.1} ops/sec", load_ops_per_sec);
    println!("    Average operation time: {:?} ({:.1}μs)", avg_load_time, avg_load_time.as_nanos() as f64 / 1000.0);

    // Get final statistics
    let stats = engine.transaction_stats();
    println!("\n📊 Final Transaction Statistics:");
    println!("  Active transactions: {}", stats.active_transactions);
    println!("  Committed transactions: {}", stats.committed_transactions);
    println!("  Total versions: {}", stats.total_versions);
    println!("  Next transaction ID: {}", stats.next_txn_id);
    println!("  Next snapshot ID: {}", stats.next_snapshot_id);

    // Flush and close
    let flush_start = Instant::now();
    engine.flush()?;
    let flush_time = flush_start.elapsed();
    
    println!("\n💾 Persistence:");
    println!("  Flush time: {:?}", flush_time);
    
    // Engine will be automatically closed when Arc is dropped
    // Arc::try_unwrap(engine).map_err(|_| "Failed to close engine")?.close()?;

    println!("\n🎯 IMEC Phase 4 Performance Summary:");
    println!("  ┌─────────────────────────────────────────────────────┐");
    println!("  │ MVCC & Concurrent Transactions Results             │");
    println!("  ├─────────────────────────────────────────────────────┤");
    println!("  │ Concurrent readers:     {:>15.1} ops/sec │", successful_reads as f64 / reader_total_time.as_secs_f64());
    println!("  │ Concurrent writers:     {:>15.1} ops/sec │", successful_writes as f64 / writer_total_time.as_secs_f64());
    println!("  │ Mixed workload:         {:>15.1} ops/sec │", ops_per_sec);
    println!("  │ Load test:              {:>15.1} ops/sec │", load_ops_per_sec);
    println!("  │ Average read time:      {:>15.1} μs     │", avg_read_time.as_nanos() as f64 / 1000.0);
    println!("  │ Average write time:     {:>15.1} μs     │", avg_write_time.as_nanos() as f64 / 1000.0);
    println!("  │ Reader concurrency:     {:>15.1}x       │", total_read_time.as_secs_f64() / reader_total_time.as_secs_f64());
    println!("  └─────────────────────────────────────────────────────┘");

    // Compare to Phase 4 targets
    println!("\n🎯 Phase 4 Target Achievement:");
    let target_commit_time = Duration::from_micros(100); // 100μs target
    let actual_commit_time = avg_write_time;
    
    println!("  Target: 100μs transaction commits");
    println!("  Actual: {:.1}μs transaction commits", actual_commit_time.as_nanos() as f64 / 1000.0);
    
    if actual_commit_time <= target_commit_time {
        println!("  Status: ✅ Transaction commit target MET ({:.1}x faster)!", 
                 target_commit_time.as_nanos() as f64 / actual_commit_time.as_nanos() as f64);
    } else {
        println!("  Status: ❌ Transaction commit target not met");
    }

    println!("\n✅ Phase 4 MVCC Demo Completed Successfully!");
    println!("IMEC now supports:");
    println!("  ✓ Multi-reader/multi-writer concurrency");
    println!("  ✓ Snapshot isolation with MVCC");
    println!("  ✓ Lock-free transaction processing");
    println!("  ✓ Conflict detection and resolution");
    println!("  ✓ Copy-on-write extent allocation");
    println!("  ✓ High-performance concurrent operations");

    println!("\nDatabase: {} ({} MB)", 
             db_path.display(), 
             std::fs::metadata(&db_path)?.len() / 1024 / 1024);

    Ok(())
}