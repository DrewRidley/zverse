use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::prelude::SliceRandom;
use rand::SeedableRng;
use rand::{rngs::StdRng, Rng};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use zverse::Database;

fn seeded_rng(seed: u64) -> StdRng {
    StdRng::seed_from_u64(seed)
}

fn generate_keys(count: usize, key_size: usize, seed: u64) -> Vec<String> {
    let mut rng = seeded_rng(seed);
    let mut keys = Vec::with_capacity(count);
    
    for i in 0..count {
        let mut key = format!("key_{:016x}", i);
        if key_size > key.len() {
            let padding: String = (0..(key_size - key.len()))
                .map(|_| char::from(b'a' + (rng.gen::<u8>() % 26)))
                .collect();
            key.push_str(&padding);
        } else if key_size < key.len() {
            key.truncate(key_size);
        }
        keys.push(key);
    }
    
    keys.shuffle(&mut rng);
    keys
}

fn generate_values(count: usize, value_size: usize, seed: u64) -> Vec<Vec<u8>> {
    let mut rng = seeded_rng(seed);
    let mut values = Vec::with_capacity(count);
    
    for _ in 0..count {
        let value: Vec<u8> = (0..value_size).map(|_| rng.gen()).collect();
        values.push(value);
    }
    
    values
}

// Large scale sequential insert benchmark
fn large_scale_sequential_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_scale_sequential_insert");
    
    let dataset_sizes = vec![100_000, 500_000, 1_000_000];
    let key_sizes = vec![16, 32, 64, 128];
    let value_sizes = vec![32, 128, 512, 1024];
    
    for &dataset_size in &dataset_sizes {
        for &key_size in &key_sizes {
            for &value_size in &value_sizes {
                let benchmark_id = BenchmarkId::new(
                    format!("k{}_v{}", key_size, value_size), 
                    dataset_size
                );
                
                group.throughput(Throughput::Elements(dataset_size as u64));
                group.bench_with_input(benchmark_id, &dataset_size, |b, &dataset_size| {
                    let keys = generate_keys(dataset_size, key_size, 0x1111);
                    let values = generate_values(dataset_size, value_size, 0x2222);
                    
                    b.iter(|| {
                        let db = Database::new();
                        for (key, value) in keys.iter().zip(values.iter()) {
                            let _ = black_box(db.put(key, value));
                        }
                        black_box(db)
                    });
                });
            }
        }
    }
    
    group.finish();
}

// Large scale random insert benchmark
fn large_scale_random_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_scale_random_insert");
    
    let dataset_sizes = vec![100_000, 500_000, 1_000_000];
    let key_sizes = vec![16, 64, 256];
    let value_sizes = vec![128, 1024, 4096];
    
    for &dataset_size in &dataset_sizes {
        for &key_size in &key_sizes {
            for &value_size in &value_sizes {
                let benchmark_id = BenchmarkId::new(
                    format!("k{}_v{}", key_size, value_size), 
                    dataset_size
                );
                
                group.throughput(Throughput::Elements(1));
                group.bench_with_input(benchmark_id, &dataset_size, |b, &dataset_size| {
                    let keys = generate_keys(dataset_size, key_size, 0x3333);
                    let values = generate_values(dataset_size, value_size, 0x4444);
                    let db = Database::new();
                    
                    // Pre-populate with half the data
                    for i in 0..dataset_size / 2 {
                        let _ = db.put(&keys[i], &values[i]);
                    }
                    
                    let mut rng = seeded_rng(0x5555);
                    b.iter(|| {
                        let idx = rng.gen_range(0..dataset_size);
                        let _ = black_box(db.put(&keys[idx], &values[idx]));
                    });
                });
            }
        }
    }
    
    group.finish();
}

// Large scale read benchmark
fn large_scale_random_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_scale_random_read");
    
    let dataset_sizes = vec![100_000, 500_000, 1_000_000];
    let key_sizes = vec![16, 64, 256];
    let value_sizes = vec![128, 1024, 4096];
    
    for &dataset_size in &dataset_sizes {
        for &key_size in &key_sizes {
            for &value_size in &value_sizes {
                let benchmark_id = BenchmarkId::new(
                    format!("k{}_v{}", key_size, value_size), 
                    dataset_size
                );
                
                group.throughput(Throughput::Elements(1));
                group.bench_with_input(benchmark_id, &dataset_size, |b, &dataset_size| {
                    let keys = generate_keys(dataset_size, key_size, 0x6666);
                    let values = generate_values(dataset_size, value_size, 0x7777);
                    let db = Database::new();
                    
                    // Pre-populate
                    for (key, value) in keys.iter().zip(values.iter()) {
                        let _ = db.put(key, value);
                    }
                    
                    let mut rng = seeded_rng(0x8888);
                    b.iter(|| {
                        let idx = rng.gen_range(0..dataset_size);
                        let _ = black_box(db.get(&keys[idx]));
                    });
                });
            }
        }
    }
    
    group.finish();
}

// Mixed workload benchmark
fn large_scale_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_scale_mixed_workload");
    
    let dataset_sizes = vec![100_000, 500_000, 1_000_000];
    let read_ratios = vec![50, 80, 95]; // percentage reads
    
    for &dataset_size in &dataset_sizes {
        for &read_ratio in &read_ratios {
            let benchmark_id = BenchmarkId::new(
                format!("read_{}%", read_ratio), 
                dataset_size
            );
            
            group.throughput(Throughput::Elements(1));
            group.bench_with_input(benchmark_id, &dataset_size, |b, &dataset_size| {
                let keys = generate_keys(dataset_size, 64, 0x9999);
                let values = generate_values(dataset_size, 512, 0xAAAA);
                let db = Database::new();
                
                // Pre-populate
                for (key, value) in keys.iter().zip(values.iter()) {
                    let _ = db.put(key, value);
                }
                
                let mut rng = seeded_rng(0xBBBB);
                let mut counter = 0;
                b.iter(|| {
                    let idx = rng.gen_range(0..dataset_size);
                    if (counter % 100) < read_ratio {
                        // Read operation
                        let _ = black_box(db.get(&keys[idx]));
                    } else {
                        // Write operation
                        let _ = black_box(db.put(&keys[idx], &values[idx]));
                    }
                    counter += 1;
                });
            });
        }
    }
    
    group.finish();
}

// Temporal operations benchmark
fn large_scale_temporal_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_scale_temporal_operations");
    
    let dataset_sizes = vec![50_000, 100_000, 250_000];
    let versions_per_key = vec![5, 10, 20];
    
    for &dataset_size in &dataset_sizes {
        for &versions in &versions_per_key {
            let benchmark_id = BenchmarkId::new(
                format!("v{}", versions), 
                dataset_size
            );
            
            group.throughput(Throughput::Elements(1));
            group.bench_with_input(benchmark_id, &dataset_size, |b, &dataset_size| {
                let keys = generate_keys(dataset_size, 32, 0xCCCC);
                let values = generate_values(dataset_size * versions, 256, 0xDDDD);
                let db = Database::new();
                
                // Pre-populate with versioned data
                for (i, key) in keys.iter().enumerate() {
                    for version in 0..versions {
                        let timestamp = 1000000 + (version as u64 * 1000);
                        let value_idx = i * versions + version;
                        let _ = db.put_at_time(key, &values[value_idx], timestamp);
                    }
                }
                
                let mut rng = seeded_rng(0xEEEE);
                b.iter(|| {
                    let key_idx = rng.gen_range(0..dataset_size);
                    let version = rng.gen_range(0..versions);
                    let timestamp = 1000000 + (version as u64 * 1000) + 500; // Query between versions
                    let _ = black_box(db.get_at_time(&keys[key_idx], timestamp));
                });
            });
        }
    }
    
    group.finish();
}

// Concurrent operations benchmark
fn large_scale_concurrent_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_scale_concurrent_operations");
    
    let dataset_sizes = vec![100_000, 500_000];
    let thread_counts = vec![2, 4, 8, 16];
    
    for &dataset_size in &dataset_sizes {
        for &num_threads in &thread_counts {
            let benchmark_id = BenchmarkId::new(
                format!("t{}", num_threads), 
                dataset_size
            );
            
            group.bench_with_input(benchmark_id, &dataset_size, |b, &dataset_size| {
                b.iter_custom(|iters| {
                    let db = Arc::new(Database::new());
                    let keys = generate_keys(dataset_size, 32, 0xFFFF);
                    let values = generate_values(dataset_size, 128, 0x0000);
                    
                    // Pre-populate
                    for (key, value) in keys.iter().zip(values.iter()) {
                        let _ = db.put(key, value);
                    }
                    
                    let operations_per_thread = (iters / num_threads as u64).max(1);
                    
                    let start = Instant::now();
                    
                    let handles: Vec<_> = (0..num_threads).map(|thread_id| {
                        let db_clone = db.clone();
                        let keys_clone = keys.clone();
                        let values_clone = values.clone();
                        
                        thread::spawn(move || {
                            let mut rng = seeded_rng(0x1111 + thread_id as u64);
                            for _ in 0..operations_per_thread {
                                let idx = rng.gen_range(0..dataset_size);
                                if rng.gen_bool(0.7) {
                                    // Read operation (70%)
                                    let _ = db_clone.get(&keys_clone[idx]);
                                } else {
                                    // Write operation (30%)
                                    let _ = db_clone.put(&keys_clone[idx], &values_clone[idx]);
                                }
                            }
                        })
                    }).collect();
                    
                    for handle in handles {
                        handle.join().unwrap();
                    }
                    
                    start.elapsed()
                })
            });
        }
    }
    
    group.finish();
}

// Batch operations benchmark
fn large_scale_batch_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_scale_batch_operations");
    
    let batch_sizes = vec![100, 1000, 10000, 50000];
    let key_sizes = vec![32, 128];
    let value_sizes = vec![256, 2048];
    
    for &batch_size in &batch_sizes {
        for &key_size in &key_sizes {
            for &value_size in &value_sizes {
                let benchmark_id = BenchmarkId::new(
                    format!("k{}_v{}", key_size, value_size), 
                    batch_size
                );
                
                group.throughput(Throughput::Elements(batch_size as u64));
                group.bench_with_input(benchmark_id, &batch_size, |b, &batch_size| {
                    let keys = generate_keys(batch_size, key_size, 0x2222);
                    let values = generate_values(batch_size, value_size, 0x3333);
                    
                    b.iter(|| {
                        let db = Database::new();
                        for (key, value) in keys.iter().zip(values.iter()) {
                            let _ = black_box(db.put(key, value));
                        }
                        black_box(db)
                    });
                });
            }
        }
    }
    
    group.finish();
}

// Variable key length benchmark
fn variable_key_length_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("variable_key_length");
    
    let dataset_size = 100_000;
    let key_length_ranges = vec![
        (8, 16),    // Short keys
        (16, 64),   // Medium keys  
        (64, 256),  // Long keys
        (8, 256),   // Mixed length keys
    ];
    
    for (min_len, max_len) in key_length_ranges {
        let benchmark_id = BenchmarkId::new(
            format!("key_len_{}_to_{}", min_len, max_len), 
            dataset_size
        );
        
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(benchmark_id, &dataset_size, |b, &dataset_size| {
            let mut rng = seeded_rng(0x4444);
            let mut keys = Vec::with_capacity(dataset_size);
            let values = generate_values(dataset_size, 512, 0x5555);
            
            // Generate variable length keys
            for i in 0..dataset_size {
                let key_len = rng.gen_range(min_len..=max_len);
                let mut key = format!("key_{:08x}", i);
                if key_len > key.len() {
                    let padding: String = (0..(key_len - key.len()))
                        .map(|_| char::from(b'a' + (rng.gen::<u8>() % 26)))
                        .collect();
                    key.push_str(&padding);
                } else if key_len < key.len() {
                    key.truncate(key_len);
                }
                keys.push(key);
            }
            
            let db = Database::new();
            
            // Pre-populate
            for (key, value) in keys.iter().zip(values.iter()) {
                let _ = db.put(key, value);
            }
            
            let mut rng = seeded_rng(0x6666);
            b.iter(|| {
                let idx = rng.gen_range(0..dataset_size);
                let _ = black_box(db.get(&keys[idx]));
            });
        });
    }
    
    group.finish();
}

// Memory pressure benchmark
fn memory_pressure_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_pressure");
    
    let large_datasets = vec![1_000_000, 2_000_000, 5_000_000];
    let large_value_sizes = vec![1024, 4096, 16384];
    
    for &dataset_size in &large_datasets {
        for &value_size in &large_value_sizes {
            let benchmark_id = BenchmarkId::new(
                format!("v{}", value_size), 
                dataset_size
            );
            
            group.sample_size(10); // Fewer samples for large datasets
            group.bench_with_input(benchmark_id, &dataset_size, |b, &dataset_size| {
                let keys = generate_keys(dataset_size, 64, 0x7777);
                let values = generate_values(dataset_size, value_size, 0x8888);
                
                b.iter(|| {
                    let db = Database::new();
                    
                    // Insert all data
                    for (key, value) in keys.iter().zip(values.iter()) {
                        let _ = db.put(key, value);
                    }
                    
                    // Perform some read operations
                    let mut rng = seeded_rng(0x9999);
                    for _ in 0..1000 {
                        let idx = rng.gen_range(0..dataset_size);
                        let _ = black_box(db.get(&keys[idx]));
                    }
                    
                    black_box(db)
                });
            });
        }
    }
    
    group.finish();
}

criterion_group!(
    insert_benches,
    large_scale_sequential_insert,
    large_scale_random_insert,
    large_scale_batch_operations
);

criterion_group!(
    read_benches,
    large_scale_random_read,
    variable_key_length_benchmark
);

criterion_group!(
    mixed_benches,
    large_scale_mixed_workload,
    large_scale_concurrent_operations
);

criterion_group!(
    temporal_benches,
    large_scale_temporal_operations
);

criterion_group!(
    stress_benches,
    memory_pressure_benchmark
);

criterion_main!(
    insert_benches,
    read_benches,
    mixed_benches,
    temporal_benches,
    stress_benches
);