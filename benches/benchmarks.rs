use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use rand::{Rng, SeedableRng};
use rand::distributions::Alphanumeric;
use rand::rngs::StdRng;
use std::time::Duration;
use std::time::Instant;
use zverse::{ZVerse, ZVerseConfig};

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

criterion_group!(
    benches,
    bench_zorder,
    bench_kv_operations,
    bench_batch_operations,
    bench_mixed_workload
);
criterion_main!(benches);