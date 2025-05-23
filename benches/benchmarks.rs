use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use rand::{Rng, SeedableRng};
use rand::distributions::Alphanumeric;
use rand::rngs::StdRng;
use std::time::Duration;
use std::time::Instant;
use zverse::{ZVerse, ZVerseConfig, LockFreeZVerseKV};
use std::sync::Arc;
use std::thread;

// Helper functions for generating test data
fn generate_random_key(rng: &mut StdRng, len: usize) -> Vec<u8> {
    rng.sample_iter(&Alphanumeric)
        .take(len)
        .map(|c| c as u8)
        .collect()
}

fn generate_random_value(rng: &mut StdRng, len: usize) -> Vec<u8> {
    rng.sample_iter(&Alphanumeric)
        .take(len)
        .map(|c| c as u8)
        .collect()
}

// Benchmark Z-order curve operations
fn bench_zorder(c: &mut Criterion) {
    let mut group = c.benchmark_group("zorder");
    group.measurement_time(Duration::from_secs(5));
    
    group.bench_function("calculate_z_value", |b| {
        b.iter(|| {
            let key_hash = black_box(0x12345678u32);
            let version = black_box(0xABCDEF01u32);
            black_box(zverse::zorder::calculate_z_value(key_hash, version))
        })
    });
    
    group.bench_function("extract_key_hash", |b| {
        b.iter(|| {
            let z = black_box(0x1234567890ABCDEFu64);
            black_box(zverse::zorder::extract_key_hash(z))
        })
    });
    
    group.bench_function("extract_version", |b| {
        b.iter(|| {
            let z = black_box(0x1234567890ABCDEFu64);
            black_box(zverse::zorder::extract_version(z))
        })
    });
    
    group.finish();
}

// Benchmark basic KV operations
fn bench_kv_operations(c: &mut Criterion) {
    // Create temporary directory for benchmark data
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let config = ZVerseConfig {
        data_path: temp_dir.path().to_string_lossy().to_string(),
        segment_size: 4 * 1024 * 1024, // 4MB
        max_entries_per_segment: 100_000,
        sync_writes: false,  // For benchmarking speed
        cache_size_bytes: 64 * 1024 * 1024, // 64MB
        background_threads: 1,
    };
    
    let db = ZVerse::new(config).expect("Failed to create ZVerse instance");
    
    // Generate test data
    let mut rng = StdRng::seed_from_u64(42); // Use fixed seed for reproducibility
    let key_sizes = [8, 16, 64, 256];
    let value_sizes = [8, 64, 1024, 4096];
    
    let mut group = c.benchmark_group("kv_operations");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(50);
    
    // Benchmark put operations with various key/value sizes
    for &key_size in &key_sizes {
        for &value_size in &value_sizes {
            group.bench_with_input(
                BenchmarkId::new("put", format!("k{}v{}", key_size, value_size)),
                &(key_size, value_size),
                |b, &(key_size, value_size)| {
                    let key = generate_random_key(&mut rng, key_size);
                    let value = generate_random_value(&mut rng, value_size);
                    
                    b.iter(|| {
                        black_box(db.put(black_box(&key), black_box(&value)))
                            .expect("Put operation failed")
                    })
                },
            );
        }
    }
    
    // Pre-populate database for get and scan benchmarks
    let mut keys = Vec::new();
    for i in 0..10000 {
        let key = format!("key-{:08}", i).into_bytes();
        let value = format!("value-for-key-{:08}", i).into_bytes();
        db.put(&key, &value).expect("Failed to put value");
        keys.push(key);
    }
    
    // Benchmark get operations
    group.bench_function("get_existing", |b| {
        let key_index = rng.gen_range(0..keys.len());
        let key = &keys[key_index];
        
        b.iter(|| {
            black_box(db.get::<_, Vec<u8>>(black_box(key), None))
                .expect("Get operation failed")
        })
    });
    
    group.bench_function("get_nonexistent", |b| {
        let key = "nonexistent-key".as_bytes();
        
        b.iter(|| {
            black_box(db.get::<_, Vec<u8>>(black_box(key), None))
                .expect("Get operation failed")
        })
    });
    
    // Benchmark scan operations
    group.bench_function("scan_all", |b| {
        b.iter(|| {
            let iter = black_box(db.scan::<&[u8]>(None, None, None))
                .expect("Scan operation failed");
            
            // Consume iterator
            let mut count = 0;
            for result in iter {
                result.expect("Failed to get scan result");
                count += 1;
            }
            black_box(count)
        })
    });
    
    group.bench_function("scan_range", |b| {
        let start_key = "key-00001000".as_bytes();
        let end_key = "key-00002000".as_bytes();
        
        b.iter(|| {
            let iter = black_box(db.scan(Some(start_key), Some(end_key), None))
                .expect("Scan operation failed");
            
            // Consume iterator
            let mut count = 0;
            for result in iter {
                result.expect("Failed to get scan result");
                count += 1;
            }
            black_box(count)
        })
    });
    
    // Benchmark history operations
    group.bench_function("history", |b| {
        // First, create multiple versions of the same key
        let history_key = "history-test-key".as_bytes();
        for i in 0..10 {
            let value_string = format!("history-value-{}", i);
            let value = value_string.as_bytes();
            db.put(history_key, value).expect("Failed to put history value");
        }
        
        b.iter(|| {
            let iter = black_box(db.history(history_key, None, None))
                .expect("History operation failed");
            
            // Consume iterator
            let mut count = 0;
            for result in iter {
                result.expect("Failed to get history result");
                count += 1;
            }
            black_box(count)
        })
    });
    
    group.finish();
}

// Benchmark batch operations
fn bench_batch_operations(c: &mut Criterion) {
    // Create temporary directory for benchmark data
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let config = ZVerseConfig {
        data_path: temp_dir.path().to_string_lossy().to_string(),
        segment_size: 4 * 1024 * 1024, // 4MB
        max_entries_per_segment: 100_000,
        sync_writes: false,  // For benchmarking speed
        cache_size_bytes: 64 * 1024 * 1024, // 64MB
        background_threads: 1,
    };
    
    let db = ZVerse::new(config).expect("Failed to create ZVerse instance");
    let mut rng = StdRng::seed_from_u64(42);
    
    let mut group = c.benchmark_group("batch_operations");
    group.measurement_time(Duration::from_secs(20));
    group.sample_size(10);
    
    // Benchmark batch puts
    for &batch_size in &[10, 100, 1000, 10000] {
        group.bench_with_input(
            BenchmarkId::new("batch_put", batch_size),
            &batch_size,
            |b, &batch_size| {
                let mut keys = Vec::with_capacity(batch_size);
                let mut values = Vec::with_capacity(batch_size);
                
                for _ in 0..batch_size {
                    keys.push(generate_random_key(&mut rng, 16));
                    values.push(generate_random_value(&mut rng, 100));
                }
                
                b.iter(|| {
                    for i in 0..batch_size {
                        db.put(&keys[i], &values[i]).expect("Put operation failed");
                    }
                    db.flush().expect("Flush failed");
                })
            },
        );
    }
    
    group.finish();
}

// Benchmark mixed workloads
fn bench_mixed_workload(c: &mut Criterion) {
    // Create temporary directory for benchmark data
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let config = ZVerseConfig {
        data_path: temp_dir.path().to_string_lossy().to_string(),
        segment_size: 4 * 1024 * 1024, // 4MB
        max_entries_per_segment: 100_000,
        sync_writes: false,  // For benchmarking speed
        cache_size_bytes: 64 * 1024 * 1024, // 64MB
        background_threads: 1,
    };
    
    let db = ZVerse::new(config).expect("Failed to create ZVerse instance");
    let mut rng = StdRng::seed_from_u64(42);
    
    // Pre-populate database
    let mut keys = Vec::new();
    for i in 0..10000 {
        let key = format!("key-{:08}", i).into_bytes();
        let value = format!("value-for-key-{:08}", i).into_bytes();
        db.put(&key, &value).expect("Failed to put value");
        keys.push(key);
    }
    
    let mut group = c.benchmark_group("mixed_workload");
    group.measurement_time(Duration::from_secs(30));
    group.sample_size(10);
    
    // Read-heavy workload (80% reads, 20% writes)
    group.bench_function("read_heavy", |b| {
        b.iter(|| {
            for _ in 0..100 {
                let op_type = rng.gen_range(0..100);
                
                if op_type < 80 {
                    // Read operation
                    let key_index = rng.gen_range(0..keys.len());
                    db.get::<_, Vec<u8>>(&keys[key_index], None)
                        .expect("Get operation failed");
                } else {
                    // Write operation
                    let key = generate_random_key(&mut rng, 16);
                    let value = generate_random_value(&mut rng, 100);
                    db.put(&key, &value).expect("Put operation failed");
                }
            }
            db.flush().expect("Flush failed");
        })
    });
    
    // Write-heavy workload (20% reads, 80% writes)
    group.bench_function("write_heavy", |b| {
        b.iter(|| {
            for _ in 0..100 {
                let op_type = rng.gen_range(0..100);
                
                if op_type < 20 {
                    // Read operation
                    let key_index = rng.gen_range(0..keys.len());
                    db.get::<_, Vec<u8>>(&keys[key_index], None)
                        .expect("Get operation failed");
                } else {
                    // Write operation
                    let key = generate_random_key(&mut rng, 16);
                    let value = generate_random_value(&mut rng, 100);
                    db.put(&key, &value).expect("Put operation failed");
                }
            }
            db.flush().expect("Flush failed");
        })
    });
    
    // Balanced workload (50% reads, 50% writes)
    group.bench_function("balanced", |b| {
        b.iter(|| {
            for _ in 0..100 {
                let op_type = rng.gen_range(0..100);
                
                if op_type < 50 {
                    // Read operation
                    let key_index = rng.gen_range(0..keys.len());
                    db.get::<_, Vec<u8>>(&keys[key_index], None)
                        .expect("Get operation failed");
                } else {
                    // Write operation
                    let key = generate_random_key(&mut rng, 16);
                    let value = generate_random_value(&mut rng, 100);
                    db.put(&key, &value).expect("Put operation failed");
                }
            }
            db.flush().expect("Flush failed");
        })
    });
    
    group.finish();
}

// Benchmark lock-free KV operations
fn bench_lockfree_operations(c: &mut Criterion) {
    let db = LockFreeZVerseKV::new();
    let mut rng = StdRng::seed_from_u64(42);
    
    let mut group = c.benchmark_group("lockfree_operations");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(100);
    
    // Benchmark put operations
    group.bench_function("put", |b| {
        b.iter(|| {
            let key = generate_random_key(&mut rng, 16);
            let value = generate_random_value(&mut rng, 100);
            black_box(db.put(black_box(key), black_box(value)))
                .expect("Put operation failed")
        })
    });
    
    // Pre-populate for get benchmarks
    for i in 0..10000 {
        let key = format!("key-{:08}", i);
        let value = format!("value-for-key-{:08}", i);
        db.put(key, value).expect("Failed to put value");
    }
    
    // Benchmark get operations
    group.bench_function("get_existing", |b| {
        b.iter(|| {
            let key = format!("key-{:08}", rng.gen_range(0..10000));
            black_box(db.get::<_, Vec<u8>>(black_box(key), None))
                .expect("Get operation failed")
        })
    });
    
    group.bench_function("get_nonexistent", |b| {
        b.iter(|| {
            let key = "nonexistent-key";
            black_box(db.get::<_, Vec<u8>>(black_box(key), None))
                .expect("Get operation failed")
        })
    });
    
    // Benchmark delete operations
    group.bench_function("delete", |b| {
        b.iter(|| {
            let key = format!("delete-key-{}", rng.gen_range(0..1000000));
            // Put then delete
            db.put(&key, "temp-value").expect("Put failed");
            black_box(db.delete(black_box(key)))
                .expect("Delete operation failed")
        })
    });
    
    group.finish();
}

// Benchmark lock-free concurrent operations
fn bench_lockfree_concurrent(c: &mut Criterion) {
    let mut group = c.benchmark_group("lockfree_concurrent");
    group.measurement_time(Duration::from_secs(15));
    group.sample_size(10);
    
    // Benchmark concurrent readers
    group.bench_function("concurrent_readers", |b| {
        let db = Arc::new(LockFreeZVerseKV::new());
        
        // Pre-populate
        for i in 0..1000 {
            let key = format!("key-{:08}", i);
            let value = format!("value-for-key-{:08}", i);
            db.put(key, value).expect("Failed to put value");
        }
        
        b.iter(|| {
            let mut handles = vec![];
            
            // Spawn 4 concurrent readers
            for _ in 0..4 {
                let db_clone = db.clone();
                let handle = thread::spawn(move || {
                    let mut rng = StdRng::seed_from_u64(42);
                    for _ in 0..250 { // 250 * 4 = 1000 total operations
                        let key = format!("key-{:08}", rng.gen_range(0..1000));
                        let _: Option<Vec<u8>> = db_clone.get(key, None).expect("Get failed");
                    }
                });
                handles.push(handle);
            }
            
            // Wait for all to complete
            for handle in handles {
                handle.join().unwrap();
            }
        })
    });
    
    // Benchmark concurrent writers
    group.bench_function("concurrent_writers", |b| {
        let db = Arc::new(LockFreeZVerseKV::new());
        
        b.iter(|| {
            let mut handles = vec![];
            
            // Spawn 4 concurrent writers
            for thread_id in 0..4 {
                let db_clone = db.clone();
                let handle = thread::spawn(move || {
                    for i in 0..250 { // 250 * 4 = 1000 total operations
                        let key = format!("thread{}-key-{:08}", thread_id, i);
                        let value = format!("thread{}-value-{:08}", thread_id, i);
                        db_clone.put(key, value).expect("Put failed");
                    }
                });
                handles.push(handle);
            }
            
            // Wait for all to complete
            for handle in handles {
                handle.join().unwrap();
            }
        })
    });
    
    // Benchmark mixed concurrent operations
    group.bench_function("mixed_concurrent", |b| {
        let db = Arc::new(LockFreeZVerseKV::new());
        
        // Pre-populate
        for i in 0..1000 {
            let key = format!("key-{:08}", i);
            let value = format!("value-for-key-{:08}", i);
            db.put(key, value).expect("Failed to put value");
        }
        
        b.iter(|| {
            let mut handles = vec![];
            
            // Spawn 2 readers and 2 writers
            for thread_id in 0..2 {
                // Reader thread
                let db_clone = db.clone();
                let reader_handle = thread::spawn(move || {
                    let mut rng = StdRng::seed_from_u64(42 + thread_id as u64);
                    for _ in 0..250 {
                        let key = format!("key-{:08}", rng.gen_range(0..1000));
                        let _: Option<Vec<u8>> = db_clone.get(key, None).expect("Get failed");
                    }
                });
                handles.push(reader_handle);
                
                // Writer thread
                let db_clone = db.clone();
                let writer_handle = thread::spawn(move || {
                    for i in 0..250 {
                        let key = format!("writer{}-key-{:08}", thread_id, i);
                        let value = format!("writer{}-value-{:08}", thread_id, i);
                        db_clone.put(key, value).expect("Put failed");
                    }
                });
                handles.push(writer_handle);
            }
            
            // Wait for all to complete
            for handle in handles {
                handle.join().unwrap();
            }
        })
    });
    
    group.finish();
}

// Benchmark lock-free vs persistent comparison
fn bench_lockfree_vs_persistent(c: &mut Criterion) {
    // Create temporary directory for persistent benchmark
    let temp_dir = tempfile::tempdir().expect("Failed to create temporary directory");
    let config = ZVerseConfig {
        data_path: temp_dir.path().to_string_lossy().to_string(),
        segment_size: 4 * 1024 * 1024,
        max_entries_per_segment: 100_000,
        sync_writes: false,
        cache_size_bytes: 64 * 1024 * 1024,
        background_threads: 1,
    };
    
    let persistent_db = ZVerse::new(config).expect("Failed to create persistent ZVerse");
    let lockfree_db = LockFreeZVerseKV::new();
    let mut rng = StdRng::seed_from_u64(42);
    
    let mut group = c.benchmark_group("lockfree_vs_persistent");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(20);
    
    // Compare write performance
    group.bench_function("persistent_writes", |b| {
        b.iter(|| {
            let key = generate_random_key(&mut rng, 16);
            let value = generate_random_value(&mut rng, 100);
            black_box(persistent_db.put(black_box(&key), black_box(&value)))
                .expect("Persistent put failed")
        })
    });
    
    group.bench_function("lockfree_writes", |b| {
        b.iter(|| {
            let key = generate_random_key(&mut rng, 16);
            let value = generate_random_value(&mut rng, 100);
            black_box(lockfree_db.put(black_box(key), black_box(value)))
                .expect("Lock-free put failed")
        })
    });
    
    // Pre-populate both databases for read comparison
    for i in 0..1000 {
        let key = format!("key-{:08}", i);
        let value = format!("value-for-key-{:08}", i);
        persistent_db.put(&key, &value).expect("Failed to populate persistent");
        lockfree_db.put(&key, &value).expect("Failed to populate lock-free");
    }
    
    // Compare read performance
    group.bench_function("persistent_reads", |b| {
        b.iter(|| {
            let key = format!("key-{:08}", rng.gen_range(0..1000));
            black_box(persistent_db.get::<_, Vec<u8>>(black_box(&key), None))
                .expect("Persistent get failed")
        })
    });
    
    group.bench_function("lockfree_reads", |b| {
        b.iter(|| {
            let key = format!("key-{:08}", rng.gen_range(0..1000));
            black_box(lockfree_db.get::<_, Vec<u8>>(black_box(key), None))
                .expect("Lock-free get failed")
        })
    });
    
    group.finish();
}

criterion_group!(
    benches,
    bench_zorder,
    bench_kv_operations,
    bench_batch_operations,
    bench_mixed_workload,
    bench_lockfree_operations,
    bench_lockfree_concurrent,
    bench_lockfree_vs_persistent
);
criterion_main!(benches);