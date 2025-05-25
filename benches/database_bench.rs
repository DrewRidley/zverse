use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use std::thread;
use zverse::Database;

fn basic_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("basic_operations");
    group.throughput(Throughput::Elements(1));

    group.bench_function("put", |b| {
        let db = Database::new();
        let mut counter = 0u64;
        b.iter(|| {
            let key = format!("key:{}", counter);
            let value = counter.to_le_bytes().to_vec();
            let _ = black_box(db.put(&key, &value));
            counter += 1;
        })
    });

    group.bench_function("get", |b| {
        let db = Database::new();
        // Pre-populate
        for i in 0u64..1000 {
            let key = format!("key:{}", i);
            let value = i.to_le_bytes().to_vec();
            let _ = db.put(&key, &value);
        }
        
        let mut counter = 0u64;
        b.iter(|| {
            let key = format!("key:{}", counter % 1000);
            let _ = black_box(db.get(&key));
            counter += 1;
        })
    });

    group.finish();
}

fn transactional_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("transactional_operations");
    group.throughput(Throughput::Elements(1));

    group.bench_function("transactional_put", |b| {
        let db = Database::new();
        let mut counter = 0u64;
        b.iter(|| {
            let (mut txn, slot_id) = db.begin_transaction();
            let key = format!("key:{}", counter);
            let value = counter.to_le_bytes().to_vec();
            let _ = db.put_txn(&mut txn, &key, &value);
            let _ = black_box(db.commit_transaction(txn, slot_id));
            counter += 1;
        })
    });

    group.bench_function("transactional_get", |b| {
        let db = Database::new();
        // Pre-populate
        for i in 0u64..1000 {
            let key = format!("key:{}", i);
            let value = i.to_le_bytes().to_vec();
            let _ = db.put(&key, &value);
        }
        
        let mut counter = 0u64;
        b.iter(|| {
            let (txn, slot_id) = db.begin_read_transaction();
            let key = format!("key:{}", counter % 1000);
            let _ = black_box(db.get_txn(&txn, &key));
            db.rollback_transaction(txn, slot_id);
            counter += 1;
        })
    });

    group.finish();
}

fn batch_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_operations");

    for batch_size in [10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("batch_put", batch_size),
            batch_size,
            |b, &batch_size| {
                let db = Database::new();
                let mut counter = 0u64;
                b.iter(|| {
                    let (mut txn, slot_id) = db.begin_transaction();
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

fn concurrent_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_operations");

    for num_threads in [1, 2, 4, 8].iter() {
        group.bench_with_input(
            BenchmarkId::new("concurrent_writes", num_threads),
            num_threads,
            |b, &num_threads| {
                let db = Arc::new(Database::new());
                
                b.iter_custom(|iters| {
                    let operations_per_thread = (iters / num_threads as u64).max(1);

                    let start = std::time::Instant::now();

                    let handles: Vec<_> = (0..num_threads).map(|thread_id| {
                        let db_clone = db.clone();
                        thread::spawn(move || {
                            for i in 0..operations_per_thread {
                                let key = format!("thread:{}:key:{}", thread_id, i);
                                let value = (thread_id as u64 * 1000000 + i).to_le_bytes().to_vec();
                                let _ = db_clone.put(&key, &value);
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

    for num_threads in [1, 2, 4, 8].iter() {
        group.bench_with_input(
            BenchmarkId::new("concurrent_reads", num_threads),
            num_threads,
            |b, &num_threads| {
                let db = Arc::new(Database::new());
                
                // Pre-populate
                for i in 0u64..1000 {
                    let key = format!("key:{}", i);
                    let value = i.to_le_bytes().to_vec();
                    let _ = db.put(&key, &value);
                }

                b.iter_custom(|iters| {
                    let operations_per_thread = (iters / num_threads as u64).max(1);

                    let start = std::time::Instant::now();

                    let handles: Vec<_> = (0..num_threads).map(|_| {
                        let db_clone = db.clone();
                        thread::spawn(move || {
                            for i in 0..operations_per_thread {
                                let key = format!("key:{}", i % 1000);
                                let _ = black_box(db_clone.get(&key));
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

fn mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    group.throughput(Throughput::Elements(1));

    group.bench_function("read_heavy_80_20", |b| {
        let db = Database::new();
        
        // Pre-populate
        for i in 0u64..1000 {
            let key = format!("key:{}", i);
            let value = i.to_le_bytes().to_vec();
            let _ = db.put(&key, &value);
        }

        let mut counter = 0u64;
        b.iter(|| {
            if counter % 10 < 8 {
                // Read operation (80%)
                let key = format!("key:{}", counter % 1000);
                let _ = black_box(db.get(&key));
            } else {
                // Write operation (20%)
                let key = format!("key:{}", counter);
                let value = counter.to_le_bytes().to_vec();
                let _ = black_box(db.put(&key, &value));
            }
            counter += 1;
        })
    });

    group.bench_function("write_heavy_20_80", |b| {
        let db = Database::new();
        
        // Pre-populate
        for i in 0u64..1000 {
            let key = format!("key:{}", i);
            let value = i.to_le_bytes().to_vec();
            let _ = db.put(&key, &value);
        }

        let mut counter = 0u64;
        b.iter(|| {
            if counter % 10 < 2 {
                // Read operation (20%)
                let key = format!("key:{}", counter % 1000);
                let _ = black_box(db.get(&key));
            } else {
                // Write operation (80%)
                let key = format!("key:{}", counter);
                let value = counter.to_le_bytes().to_vec();
                let _ = black_box(db.put(&key, &value));
            }
            counter += 1;
        })
    });

    group.finish();
}

fn temporal_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("temporal_operations");
    group.throughput(Throughput::Elements(1));

    group.bench_function("temporal_put", |b| {
        let db = Database::new();
        let mut counter = 0u64;
        b.iter(|| {
            let key = format!("key:{}", counter % 100);
            let value = counter.to_le_bytes().to_vec();
            let timestamp = 1000000 + counter;
            let _ = black_box(db.put_at_time(&key, &value, timestamp));
            counter += 1;
        })
    });

    group.bench_function("temporal_get", |b| {
        let db = Database::new();
        
        // Pre-populate with versioned data
        for i in 0u64..100 {
            for version in 0u64..10 {
                let key = format!("key:{}", i);
                let value = (i * 1000 + version).to_le_bytes().to_vec();
                let timestamp = 1000000 + version;
                let _ = db.put_at_time(&key, &value, timestamp);
            }
        }

        let mut counter = 0u64;
        b.iter(|| {
            let key = format!("key:{}", counter % 100);
            let timestamp = 1000000 + (counter % 10);
            let _ = black_box(db.get_at_time(&key, timestamp));
            counter += 1;
        })
    });

    group.finish();
}

criterion_group!(
    benches,
    basic_operations,
    transactional_operations,
    batch_operations,
    concurrent_operations,
    mixed_workload,
    temporal_operations
);

criterion_main!(benches);