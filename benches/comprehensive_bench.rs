use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::Rng;
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use zverse::Database;

mod benchmark_config;
use benchmark_config::{BenchmarkConfig, DataGenerator, KeyPattern, ValuePattern, WorkloadPattern};

fn comprehensive_mixed_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("comprehensive_mixed_insert");
    let config = BenchmarkConfig::production();
    let generator = DataGenerator::new(0x1234567890ABCDEF);
    
    // Test combinations of different key/value sizes with large datasets
    let test_combinations = vec![
        (16, 64),     // Small key, small value
        (32, 256),    // Small key, medium value  
        (64, 1024),   // Medium key, large value
        (128, 4096),  // Large key, very large value
        (256, 16384), // Very large key, huge value
    ];
    
    for &dataset_size in &config.dataset_sizes {
        for (key_size, value_size) in &test_combinations {
            let benchmark_id = BenchmarkId::new(
                format!("k{}_v{}", key_size, value_size),
                dataset_size
            );
            
            group.throughput(Throughput::Elements(dataset_size as u64));
            group.bench_with_input(benchmark_id, &dataset_size, |b, &dataset_size| {
                let keys = generator.generate_keys(dataset_size, *key_size, KeyPattern::Random);
                let values = generator.generate_values(dataset_size, *value_size, ValuePattern::Random);
                
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
    
    group.finish();
}

fn comprehensive_mixed_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("comprehensive_mixed_read");
    let config = BenchmarkConfig::production();
    let generator = DataGenerator::new(0xFEDCBA0987654321);
    
    let test_combinations = vec![
        (32, 128),
        (64, 512), 
        (128, 2048),
        (256, 8192),
    ];
    
    for &dataset_size in &config.dataset_sizes {
        for (key_size, value_size) in &test_combinations {
            let benchmark_id = BenchmarkId::new(
                format!("k{}_v{}", key_size, value_size),
                dataset_size
            );
            
            group.throughput(Throughput::Elements(1));
            group.bench_with_input(benchmark_id, &dataset_size, |b, &dataset_size| {
                let keys = generator.generate_keys(dataset_size, *key_size, KeyPattern::Random);
                let values = generator.generate_values(dataset_size, *value_size, ValuePattern::Random);
                let db = Database::new();
                
                // Pre-populate database
                for (key, value) in keys.iter().zip(values.iter()) {
                    let _ = db.put(key, value);
                }
                
                let mut rng = generator.seeded_rng(0x1111);
                b.iter(|| {
                    let idx = rng.gen_range(0..dataset_size);
                    let _ = black_box(db.get(&keys[idx]));
                });
            });
        }
    }
    
    group.finish();
}

fn variable_size_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("variable_size_workload");
    let config = BenchmarkConfig::production();
    let generator = DataGenerator::new(0x9999999999999999);
    
    // Test variable key and value sizes
    let size_ranges = vec![
        ((8, 32), (64, 256)),      // Small variable
        ((16, 128), (256, 2048)),  // Medium variable
        ((32, 512), (1024, 8192)), // Large variable
        ((8, 1024), (64, 16384)),  // Full range variable
    ];
    
    for &dataset_size in &[100_000, 1_000_000] {
        for ((min_key, max_key), (min_val, max_val)) in &size_ranges {
            let benchmark_id = BenchmarkId::new(
                format!("k{}-{}_v{}-{}", min_key, max_key, min_val, max_val),
                dataset_size
            );
            
            group.throughput(Throughput::Elements(1));
            group.bench_with_input(benchmark_id, &dataset_size, |b, &dataset_size| {
                let keys = generator.generate_variable_keys(dataset_size, *min_key, *max_key);
                
                // Generate variable size values
                let mut rng = generator.seeded_rng(0x3333);
                let mut values = Vec::with_capacity(dataset_size);
                for _ in 0..dataset_size {
                    let value_size = rng.gen_range(*min_val..=*max_val);
                    let value: Vec<u8> = (0..value_size).map(|_| rng.gen()).collect();
                    values.push(value);
                }
                
                let db = Database::new();
                
                // Pre-populate
                for (key, value) in keys.iter().zip(values.iter()) {
                    let _ = db.put(key, value);
                }
                
                let mut rng = generator.seeded_rng(0x4444);
                b.iter(|| {
                    let idx = rng.gen_range(0..dataset_size);
                    if rng.gen_bool(0.7) {
                        let _ = black_box(db.get(&keys[idx]));
                    } else {
                        let _ = black_box(db.put(&keys[idx], &values[idx]));
                    }
                });
            });
        }
    }
    
    group.finish();
}

fn mixed_workload_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload_patterns");
    let generator = DataGenerator::new(0xAAAAAAAAAAAAAAAA);
    
    let patterns = vec![
        ("read_heavy", WorkloadPattern::read_heavy()),
        ("write_heavy", WorkloadPattern::write_heavy()),
        ("balanced", WorkloadPattern::balanced()),
        ("temporal_heavy", WorkloadPattern::temporal_heavy()),
    ];
    
    let dataset_sizes = vec![100_000, 500_000, 1_000_000];
    
    for &dataset_size in &dataset_sizes {
        for (pattern_name, pattern) in &patterns {
            let benchmark_id = BenchmarkId::new(*pattern_name, dataset_size);
            
            group.throughput(Throughput::Elements(1));
            group.bench_with_input(benchmark_id, &dataset_size, |b, &dataset_size| {
                let keys = generator.generate_keys(dataset_size, 64, KeyPattern::Random);
                let values = generator.generate_values(dataset_size, 512, ValuePattern::Random);
                let temporal_data = generator.generate_temporal_data(&keys[..dataset_size/10], 5, 512);
                let db = Database::new();
                
                // Pre-populate
                for (key, value) in keys.iter().zip(values.iter()) {
                    let _ = db.put(key, value);
                }
                
                // Add temporal data
                for (key, value, timestamp) in &temporal_data {
                    let _ = db.put_at_time(key, value, *timestamp);
                }
                
                let mut rng = generator.seeded_rng(0x5555);
                let mut counter = 0;
                b.iter(|| {
                    let operation = counter % 100;
                    let idx = rng.gen_range(0..dataset_size);
                    
                    if operation < pattern.read_percentage {
                        let _ = black_box(db.get(&keys[idx]));
                    } else if operation < pattern.read_percentage + pattern.write_percentage {
                        let _ = black_box(db.put(&keys[idx], &values[idx]));
                    } else if operation < pattern.read_percentage + pattern.write_percentage + pattern.temporal_read_percentage {
                        let temporal_idx = idx % temporal_data.len();
                        let timestamp = temporal_data[temporal_idx].2 + 500;
                        let _ = black_box(db.get_at_time(&temporal_data[temporal_idx].0, timestamp));
                    }
                    
                    counter += 1;
                });
            });
        }
    }
    
    group.finish();
}

fn large_scale_concurrent_mixed(c: &mut Criterion) {
    let mut group = c.benchmark_group("large_scale_concurrent_mixed");
    let generator = DataGenerator::new(0xBBBBBBBBBBBBBBBB);
    
    let thread_counts = vec![2, 4, 8, 16, 32];
    let dataset_sizes = vec![500_000, 2_000_000];
    
    for &dataset_size in &dataset_sizes {
        for &num_threads in &thread_counts {
            let benchmark_id = BenchmarkId::new(
                format!("threads_{}", num_threads),
                dataset_size
            );
            
            group.bench_with_input(benchmark_id, &dataset_size, |b, &dataset_size| {
                b.iter_custom(|iters| {
                    let db = Arc::new(Database::new());
                    let keys = generator.generate_keys(dataset_size, 64, KeyPattern::Random);
                    let values = generator.generate_values(dataset_size, 1024, ValuePattern::Random);
                    
                    // Pre-populate with mixed sizes
                    for (i, (key, value)) in keys.iter().zip(values.iter()).enumerate() {
                        // Use different value sizes for variety
                        let adjusted_value = if i % 4 == 0 {
                            &value[..value.len().min(256)]
                        } else if i % 4 == 1 {
                            &value[..value.len().min(512)]
                        } else {
                            value
                        };
                        let _ = db.put(key, adjusted_value);
                    }
                    
                    let operations_per_thread = (iters / num_threads as u64).max(1);
                    
                    let start = Instant::now();
                    
                    let handles: Vec<_> = (0..num_threads).map(|thread_id| {
                        let db_clone = db.clone();
                        let keys_clone = keys.clone();
                        let values_clone = values.clone();
                        
                        thread::spawn(move || {
                            let mut rng = generator.seeded_rng(0x6666 + thread_id as u64);
                            for _ in 0..operations_per_thread {
                                let idx = rng.gen_range(0..dataset_size);
                                let operation = rng.gen_range(0..100);
                                
                                if operation < 70 {
                                    // Read operation
                                    let _ = db_clone.get(&keys_clone[idx]);
                                } else if operation < 85 {
                                    // Write with original size
                                    let _ = db_clone.put(&keys_clone[idx], &values_clone[idx]);
                                } else if operation < 95 {
                                    // Write with smaller value
                                    let small_value = &values_clone[idx][..values_clone[idx].len().min(256)];
                                    let _ = db_clone.put(&keys_clone[idx], small_value);
                                } else {
                                    // Temporal read
                                    let timestamp = 1000000 + (idx as u64 * 1000);
                                    let _ = db_clone.get_at_time(&keys_clone[idx], timestamp);
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

fn memory_stress_mixed_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_stress_mixed_sizes");
    let generator = DataGenerator::new(0xCCCCCCCCCCCCCCCC);
    
    let stress_configs = vec![
        (2_000_000, "2M_records"),
        (5_000_000, "5M_records"),
        (10_000_000, "10M_records"),
    ];
    
    for (dataset_size, config_name) in stress_configs {
        let benchmark_id = BenchmarkId::new(config_name, dataset_size);
        
        group.sample_size(10);
        group.bench_with_input(benchmark_id, &dataset_size, |b, &dataset_size| {
            b.iter(|| {
                let db = Database::new();
                
                // Generate mixed size data
                let mut rng = generator.seeded_rng(0x7777);
                
                for i in 0..dataset_size {
                    // Variable key sizes
                    let key_size = match i % 4 {
                        0 => 32,
                        1 => 64,
                        2 => 128,
                        3 => 256,
                        _ => unreachable!(),
                    };
                    
                    // Variable value sizes
                    let value_size = match i % 5 {
                        0 => 64,
                        1 => 256,
                        2 => 1024,
                        3 => 4096,
                        4 => 16384,
                        _ => unreachable!(),
                    };
                    
                    let key = format!("stress_key_{:016x}", i);
                    let key = if key.len() > key_size {
                        key[..key_size].to_string()
                    } else {
                        format!("{}{}", key, "x".repeat(key_size - key.len()))
                    };
                    
                    let value: Vec<u8> = (0..value_size).map(|_| rng.gen()).collect();
                    
                    let _ = db.put(&key, &value);
                    
                    // Perform some reads every 1000 inserts
                    if i % 1000 == 0 && i > 0 {
                        for _ in 0..10 {
                            let read_idx = rng.gen_range(0..i);
                            let read_key = format!("stress_key_{:016x}", read_idx);
                            let _ = black_box(db.get(&read_key));
                        }
                    }
                }
                
                black_box(db)
            });
        });
    }
    
    group.finish();
}

fn temporal_mixed_versions(c: &mut Criterion) {
    let mut group = c.benchmark_group("temporal_mixed_versions");
    let generator = DataGenerator::new(0xDDDDDDDDDDDDDDDD);
    
    let dataset_sizes = vec![50_000, 200_000, 500_000];
    let version_configs = vec![
        (5, "few_versions"),
        (20, "many_versions"),
        (50, "lots_versions"),
    ];
    
    for &dataset_size in &dataset_sizes {
        for (versions_per_key, version_name) in &version_configs {
            let benchmark_id = BenchmarkId::new(*version_name, dataset_size);
            
            group.throughput(Throughput::Elements(1));
            group.bench_with_input(benchmark_id, &dataset_size, |b, &dataset_size| {
                let keys = generator.generate_keys(dataset_size, 48, KeyPattern::Hierarchical);
                let db = Database::new();
                
                // Create temporal data with mixed value sizes
                let mut rng = generator.seeded_rng(0x8888);
                for (key_idx, key) in keys.iter().enumerate() {
                    for version in 0..*versions_per_key {
                        let timestamp = 1_000_000 + (version as u64 * 1000) + (key_idx as u64);
                        
                        // Variable value size per version
                        let value_size = match version % 4 {
                            0 => 128,
                            1 => 512,
                            2 => 2048,
                            3 => 8192,
                            _ => unreachable!(),
                        };
                        
                        let value: Vec<u8> = (0..value_size).map(|_| rng.gen()).collect();
                        let _ = db.put_at_time(key, &value, timestamp);
                    }
                }
                
                let mut rng = generator.seeded_rng(0x9999);
                b.iter(|| {
                    let key_idx = rng.gen_range(0..dataset_size);
                    let version = rng.gen_range(0..*versions_per_key);
                    let timestamp = 1_000_000 + (version as u64 * 1000) + 500;
                    let _ = black_box(db.get_at_time(&keys[key_idx], timestamp));
                });
            });
        }
    }
    
    group.finish();
}

criterion_group!(
    comprehensive_benches,
    comprehensive_mixed_insert,
    comprehensive_mixed_read,
    variable_size_workload
);

criterion_group!(
    workload_benches,
    mixed_workload_patterns,
    large_scale_concurrent_mixed
);

criterion_group!(
    stress_benches,
    memory_stress_mixed_sizes,
    temporal_mixed_versions
);

criterion_main!(
    comprehensive_benches,
    workload_benches,
    stress_benches
);