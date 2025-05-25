use std::sync::Arc;
use std::thread;
use std::time::Duration;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

use rand::SeedableRng;
use rand::{rngs::StdRng, Rng};
use zverse::revolutionary::RevolutionaryDB;

fn seeded_rng(seed: u64) -> impl Rng {
    StdRng::seed_from_u64(seed)
}

// Basic MVCC operations
pub fn mvcc_basic_ops(c: &mut Criterion) {
    let mut group = c.benchmark_group("mvcc_basic_ops");
    group.throughput(Throughput::Elements(1));
    
    // Non-transactional baseline
    group.bench_function("non_transactional_put", |b| {
        let db = RevolutionaryDB::new();
        let mut counter = 0u64;
        b.iter(|| {
            let key = format!("key:{}", counter);
            let value = counter.to_le_bytes().to_vec();
            let _ = black_box(db.put(&key, &value));
            counter += 1;
        })
    });

    // Transactional operations
    group.bench_function("transactional_put", |b| {
        let db = RevolutionaryDB::new();
        let mut counter = 0u64;
        b.iter(|| {
            let (mut txn, slot_id) = db.begin_transaction(false);
            let key = format!("key:{}", counter);
            let value = counter.to_le_bytes().to_vec();
            let _ = black_box(db.put_txn(&mut txn, &key, &value));
            let _ = db.commit_transaction(txn, slot_id);
            counter += 1;
        })
    });

    // Batch operations in single transaction
    for batch_size in [10, 100, 1000].iter() {
        group.bench_with_input(
            BenchmarkId::new("batch_transactional_put", batch_size),
            batch_size,
            |b, &batch_size| {
                let db = RevolutionaryDB::new();
                let mut counter = 0u64;
                b.iter(|| {
                    let (mut txn, slot_id) = db.begin_transaction(false);
                    for _ in 0..batch_size {
                        let key = format!("key:{}", counter);
                        let value = counter.to_le_bytes().to_vec();
                        let _ = db.put_txn(&mut txn, &key, &value);
                        counter += 1;
                    }
                    let _ = black_box(db.commit_transaction(txn, slot_id));
                })
            }
        );
    }

    group.finish();
}

// Read performance with MVCC
pub fn mvcc_read_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("mvcc_read_performance");
    group.throughput(Throughput::Elements(1));

    // Setup: pre-populate database
    let db = Arc::new(RevolutionaryDB::new());
    let num_records = 10000;
    for i in 0..num_records {
        let (mut txn, slot_id) = db.begin_transaction(false);
        let key = format!("user:{}", i);
        let value = format!("data_{}", i).into_bytes();
        db.put_txn(&mut txn, &key, &value).unwrap();
        db.commit_transaction(txn, slot_id).unwrap();
    }

    // Transactional reads
    group.bench_function("transactional_read", |b| {
        let mut rng = seeded_rng(0x123456789ABCDEF0);
        b.iter(|| {
            let (txn, slot_id) = db.begin_transaction(true);
            let key_id = rng.gen_range(0..num_records);
            let key = format!("user:{}", key_id);
            let _ = black_box(db.get_txn(&txn, &key));
            db.rollback_transaction(txn, slot_id);
        })
    });

    // Temporal reads
    group.bench_function("temporal_read", |b| {
        let mut rng = seeded_rng(0x123456789ABCDEF0);
        let base_timestamp = 1000000u64;
        b.iter(|| {
            let key_id = rng.gen_range(0..num_records);
            let timestamp = base_timestamp + rng.gen_range(0..100000);
            let key = format!("user:{}", key_id);
            let _ = black_box(db.get_at_time(&key, timestamp));
        })
    });

    group.finish();
}

// Concurrent transaction performance
pub fn mvcc_concurrent_transactions(c: &mut Criterion) {
    let mut group = c.benchmark_group("mvcc_concurrent_transactions");
    
    for num_threads in [1, 2, 4, 8, 16].iter() {
        group.bench_with_input(
            BenchmarkId::new("concurrent_writes", num_threads),
            num_threads,
            |b, &num_threads| {
                b.iter_custom(|iters| {
                    let db = Arc::new(RevolutionaryDB::new());
                    let operations_per_thread = (iters / num_threads as u64).max(1);
                    
                    let start = std::time::Instant::now();
                    
                    let handles: Vec<_> = (0..num_threads).map(|thread_id| {
                        let db_clone = db.clone();
                        thread::spawn(move || {
                            for i in 0..operations_per_thread {
                                let (mut txn, slot_id) = db_clone.begin_transaction(false);
                                let key = format!("thread:{}:key:{}", thread_id, i);
                                let value = (thread_id as u64 * 1000000 + i).to_le_bytes().to_vec();
                                db_clone.put_txn(&mut txn, &key, &value).unwrap();
                                db_clone.commit_transaction(txn, slot_id).unwrap();
                            }
                        })
                    }).collect();
                    
                    for handle in handles {
                        handle.join().unwrap();
                    }
                    
                    start.elapsed()
                })
            }
        );
    }

    group.finish();
}

// Long-running vs short transactions
pub fn mvcc_transaction_duration(c: &mut Criterion) {
    let mut group = c.benchmark_group("mvcc_transaction_duration");
    group.throughput(Throughput::Elements(1));

    // Short transactions (single operation)
    group.bench_function("short_transaction", |b| {
        let db = RevolutionaryDB::new();
        let mut counter = 0u64;
        b.iter(|| {
            let (mut txn, slot_id) = db.begin_transaction(false);
            let key = format!("key:{}", counter);
            let value = counter.to_le_bytes().to_vec();
            db.put_txn(&mut txn, &key, &value).unwrap();
            let _ = black_box(db.commit_transaction(txn, slot_id));
            counter += 1;
        })
    });

    // Medium transactions (10 operations)
    group.bench_function("medium_transaction", |b| {
        let db = RevolutionaryDB::new();
        let mut counter = 0u64;
        b.iter(|| {
            let (mut txn, slot_id) = db.begin_transaction(false);
            for _ in 0..10 {
                let key = format!("key:{}", counter);
                let value = counter.to_le_bytes().to_vec();
                db.put_txn(&mut txn, &key, &value).unwrap();
                counter += 1;
            }
            let _ = black_box(db.commit_transaction(txn, slot_id));
        })
    });

    // Long transactions (100 operations)
    group.bench_function("long_transaction", |b| {
        let db = RevolutionaryDB::new();
        let mut counter = 0u64;
        b.iter(|| {
            let (mut txn, slot_id) = db.begin_transaction(false);
            for _ in 0..100 {
                let key = format!("key:{}", counter);
                let value = counter.to_le_bytes().to_vec();
                db.put_txn(&mut txn, &key, &value).unwrap();
                counter += 1;
            }
            let _ = black_box(db.commit_transaction(txn, slot_id));
        })
    });

    group.finish();
}

// Mixed workload performance
pub fn mvcc_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mvcc_mixed_workload");
    group.throughput(Throughput::Elements(1));

    // Setup: pre-populate database
    let db = Arc::new(RevolutionaryDB::new());
    let num_records = 1000;
    for i in 0..num_records {
        let (mut txn, slot_id) = db.begin_transaction(false);
        let key = format!("user:{}", i);
        let value = format!("initial_data_{}", i).into_bytes();
        db.put_txn(&mut txn, &key, &value).unwrap();
        db.commit_transaction(txn, slot_id).unwrap();
    }

    // Read-heavy workload (80% reads, 20% writes)
    group.bench_function("read_heavy_80_20", |b| {
        let mut rng = seeded_rng(0xDEADBEEF);
        b.iter(|| {
            if rng.gen_ratio(8, 10) {
                // Read operation
                let (txn, slot_id) = db.begin_transaction(true);
                let key_id = rng.gen_range(0..num_records);
                let key = format!("user:{}", key_id);
                let _ = black_box(db.get_txn(&txn, &key));
                db.rollback_transaction(txn, slot_id);
            } else {
                // Write operation
                let (mut txn, slot_id) = db.begin_transaction(false);
                let key_id = rng.gen_range(0..num_records);
                let key = format!("user:{}", key_id);
                let value = rng.r#gen::<u64>().to_le_bytes().to_vec();
                let _ = db.put_txn(&mut txn, &key, &value);
                let _ = black_box(db.commit_transaction(txn, slot_id));
            }
        })
    });

    // Write-heavy workload (20% reads, 80% writes)
    group.bench_function("write_heavy_20_80", |b| {
        let mut rng = seeded_rng(0xCAFEBABE);
        b.iter(|| {
            if rng.gen_ratio(2, 10) {
                // Read operation
                let (txn, slot_id) = db.begin_transaction(true);
                let key_id = rng.gen_range(0..num_records);
                let key = format!("user:{}", key_id);
                let _ = black_box(db.get_txn(&txn, &key));
                db.rollback_transaction(txn, slot_id);
            } else {
                // Write operation
                let (mut txn, slot_id) = db.begin_transaction(false);
                let key_id = rng.gen_range(0..num_records);
                let key = format!("user:{}", key_id);
                let value = rng.r#gen::<u64>().to_le_bytes().to_vec();
                let _ = db.put_txn(&mut txn, &key, &value);
                let _ = black_box(db.commit_transaction(txn, slot_id));
            }
        })
    });

    group.finish();
}

// Rollback performance
pub fn mvcc_rollback_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("mvcc_rollback_performance");
    group.throughput(Throughput::Elements(1));

    // Rollback after single operation
    group.bench_function("rollback_single_op", |b| {
        let db = RevolutionaryDB::new();
        let mut counter = 0u64;
        b.iter(|| {
            let (mut txn, slot_id) = db.begin_transaction(false);
            let key = format!("key:{}", counter);
            let value = counter.to_le_bytes().to_vec();
            db.put_txn(&mut txn, &key, &value).unwrap();
            black_box(db.rollback_transaction(txn, slot_id));
            counter += 1;
        })
    });

    // Rollback after multiple operations
    group.bench_function("rollback_multiple_ops", |b| {
        let db = RevolutionaryDB::new();
        let mut counter = 0u64;
        b.iter(|| {
            let (mut txn, slot_id) = db.begin_transaction(false);
            for _ in 0..10 {
                let key = format!("key:{}", counter);
                let value = counter.to_le_bytes().to_vec();
                db.put_txn(&mut txn, &key, &value).unwrap();
                counter += 1;
            }
            black_box(db.rollback_transaction(txn, slot_id));
        })
    });

    group.finish();
}

// Scalability tests
pub fn mvcc_scalability(c: &mut Criterion) {
    let mut group = c.benchmark_group("mvcc_scalability");

    // Scale by number of records in transaction
    for num_ops in [1, 10, 100, 1000, 10000].iter() {
        group.throughput(Throughput::Elements(*num_ops as u64));
        group.bench_with_input(
            BenchmarkId::new("transaction_size", num_ops),
            num_ops,
            |b, &num_ops| {
                let db = RevolutionaryDB::new();
                let mut counter = 0u64;
                b.iter(|| {
                    let (mut txn, slot_id) = db.begin_transaction(false);
                    for _ in 0..num_ops {
                        let key = format!("key:{}", counter);
                        let value = counter.to_le_bytes().to_vec();
                        db.put_txn(&mut txn, &key, &value).unwrap();
                        counter += 1;
                    }
                    let _ = black_box(db.commit_transaction(txn, slot_id));
                })
            }
        );
    }

    group.finish();
}

// Snapshot isolation validation
pub fn mvcc_snapshot_isolation(c: &mut Criterion) {
    let mut group = c.benchmark_group("mvcc_snapshot_isolation");
    group.throughput(Throughput::Elements(1));

    group.bench_function("concurrent_snapshot_reads", |b| {
        let db = Arc::new(RevolutionaryDB::new());
        
        // Pre-populate
        let (mut txn, slot_id) = db.begin_transaction(false);
        for i in 0..100 {
            let key = format!("shared:{}", i);
            let value = format!("initial_{}", i).into_bytes();
            db.put_txn(&mut txn, &key, &value).unwrap();
        }
        db.commit_transaction(txn, slot_id).unwrap();

        b.iter_custom(|iters| {
            let start = std::time::Instant::now();
            
            let handles: Vec<_> = (0..4).map(|_| {
                let db_clone = db.clone();
                thread::spawn(move || {
                    for _ in 0..iters/4 {
                        let (txn, slot_id) = db_clone.begin_transaction(true);
                        for i in 0..10 {
                            let key = format!("shared:{}", i);
                            let _ = black_box(db_clone.get_txn(&txn, &key));
                        }
                        db_clone.rollback_transaction(txn, slot_id);
                    }
                })
            }).collect();
            
            for handle in handles {
                handle.join().unwrap();
            }
            
            start.elapsed()
        })
    });

    group.finish();
}

// Flush boundary interaction
pub fn mvcc_flush_interaction(c: &mut Criterion) {
    let mut group = c.benchmark_group("mvcc_flush_interaction");
    group.throughput(Throughput::Elements(1));

    group.bench_function("transactions_with_flush_pressure", |b| {
        let db = Arc::new(RevolutionaryDB::new());
        
        // Create background "flush pressure" by advancing flush boundary
        let db_clone = db.clone();
        let _flush_thread = thread::spawn(move || {
            loop {
                let current_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as u64;
                db_clone.advance_flush_boundary(current_time.saturating_sub(1000000)); // 1 second lag
                thread::sleep(Duration::from_millis(10));
            }
        });

        let mut counter = 0u64;
        b.iter(|| {
            let (mut txn, slot_id) = db.begin_transaction(false);
            for _ in 0..5 {
                let key = format!("key:{}", counter);
                let value = counter.to_le_bytes().to_vec();
                db.put_txn(&mut txn, &key, &value).unwrap();
                counter += 1;
            }
            let _ = black_box(db.commit_transaction(txn, slot_id));
        })
    });

    group.finish();
}

criterion_group!(
    mvcc_benches,
    mvcc_basic_ops,
    mvcc_read_performance,
    mvcc_concurrent_transactions,
    mvcc_transaction_duration,
    mvcc_mixed_workload,
    mvcc_rollback_performance,
    mvcc_scalability,
    mvcc_snapshot_isolation,
    mvcc_flush_interaction
);

criterion_main!(mvcc_benches);