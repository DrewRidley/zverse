use std::collections::BTreeMap;
use std::time::Instant;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::prelude::SliceRandom;
use rand::SeedableRng;
use rand::{rngs::StdRng, Rng};

use zverse::MortonTemporalDB;

fn seeded_rng(alter: u64) -> impl Rng {
    StdRng::seed_from_u64(0xEA3C47920F94A980 ^ alter)
}

// Sequential insert with automatic timestamping
pub fn seq_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("seq_insert");
    group.throughput(Throughput::Elements(1));
    
    group.bench_function("morton_temporal", |b| {
        let db = MortonTemporalDB::new();
        let mut key_counter = 0u64;
        b.iter(|| {
            let key = format!("key:{}", key_counter);
            let value = key_counter.to_le_bytes().to_vec();
            let _ = db.put(&key, value);
            key_counter += 1;
        })
    });

    // Compare with BTreeMap baseline
    group.bench_function("btreemap", |b| {
        let mut btree = BTreeMap::new();
        let mut key_counter = 0u64;
        b.iter(|| {
            let key = format!("key:{}", key_counter);
            let value = key_counter.to_le_bytes().to_vec();
            btree.insert(key, value);
            key_counter += 1;
        })
    });

    group.finish();
}

// Sequential insert at specific timestamps
pub fn seq_temporal_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("seq_temporal_insert");
    group.throughput(Throughput::Elements(1));
    
    group.bench_function("morton_temporal", |b| {
        let db = MortonTemporalDB::new();
        let mut key_counter = 0u64;
        let mut timestamp = 1000000u64;
        b.iter(|| {
            let key = format!("key:{}", key_counter);
            let value = key_counter.to_le_bytes().to_vec();
            let _ = db.put_at_time(&key, value, timestamp);
            key_counter += 1;
            timestamp += 1000; // 1ms increments
        })
    });

    group.finish();
}

// Random insert operations
pub fn rand_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("rand_insert");
    group.throughput(Throughput::Elements(1));

    let keys = generate_keys(10000);

    group.bench_function("morton_temporal", |b| {
        let db = MortonTemporalDB::new();
        let mut rng = seeded_rng(0xE080D1A42C207DAF);
        b.iter(|| {
            let key = &keys[rng.gen_range(0..keys.len())];
            let value = rng.r#gen::<u64>().to_le_bytes().to_vec();
            let _ = db.put(key, value);
        })
    });

    group.finish();
}

// Versioning benchmark - multiple versions of same key
pub fn versioning_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("versioning_workload");
    group.throughput(Throughput::Elements(1));

    let num_keys = 1000;
    let versions_per_key = 10;

    group.bench_function("create_versions", |b| {
        b.iter_custom(|iters| {
            let db = MortonTemporalDB::new();
            let start = Instant::now();
            
            for iter in 0..iters {
                for key_id in 0..num_keys {
                    for version in 0..versions_per_key {
                        let key = format!("user:{}", key_id);
                        let value = format!("data_v{}_{}", version, iter).into_bytes();
                        let timestamp = 1000000 + (version * 1000) + iter;
                        let _ = black_box(db.put_at_time(&key, value, timestamp));
                    }
                }
            }
            
            start.elapsed()
        })
    });

    group.finish();
}

// Temporal queries - fetching older versions
pub fn temporal_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("temporal_queries");
    group.throughput(Throughput::Elements(1));

    // Setup: create database with multiple versions
    let db = MortonTemporalDB::new();
    let num_keys = 1000;
    let versions_per_key = 20;
    
    for key_id in 0..num_keys {
        for version in 0..versions_per_key {
            let key = format!("user:{}", key_id);
            let value = format!("balance:{}", version * 100).into_bytes();
            let timestamp = 1000000 + (version * 5000); // 5ms between versions
            let _ = db.put_at_time(&key, value, timestamp);
        }
    }

    group.bench_function("get_latest", |b| {
        let mut rng = seeded_rng(0x123456789ABCDEF0);
        b.iter(|| {
            let key_id = rng.gen_range(0..num_keys);
            let key = format!("user:{}", key_id);
            let _ = black_box(db.get(&key));
        })
    });

    group.bench_function("get_historical", |b| {
        let mut rng = seeded_rng(0x123456789ABCDEF0);
        b.iter(|| {
            let key_id = rng.gen_range(0..num_keys);
            let version = rng.gen_range(0..versions_per_key);
            let key = format!("user:{}", key_id);
            let timestamp = 1000000 + (version * 5000) + 2500; // Between versions
            let _ = black_box(db.get_at_time(&key, timestamp));
        })
    });

    group.finish();
}

// Batch operations benchmark
pub fn batch_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_operations");
    
    for batch_size in [100, 500, 1000, 5000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));
        
        group.bench_with_input(
            BenchmarkId::new("batch_insert", batch_size),
            batch_size,
            |b, &batch_size| {
                b.iter(|| {
                    let db = MortonTemporalDB::new();
                    let items: Vec<(String, Vec<u8>)> = (0..batch_size)
                        .map(|i| {
                            let key = format!("batch_key:{}", i);
                            let value = (i as u64).to_le_bytes().to_vec();
                            (key, value)
                        })
                        .collect();
                    
                    let _ = black_box(db.put_temporal_batch(&items));
                })
            }
        );

        // Setup database for batch reads
        let db = MortonTemporalDB::new();
        let keys: Vec<String> = (0..*batch_size)
            .map(|i| {
                let key = format!("batch_key:{}", i);
                let value = (i as u64).to_le_bytes().to_vec();
                let _ = db.put(&key, value);
                key
            })
            .collect();

        group.bench_with_input(
            BenchmarkId::new("batch_get", batch_size),
            batch_size,
            |b, &batch_size| {
                let key_refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
                b.iter(|| {
                    let _ = black_box(db.get_batch(&key_refs));
                })
            }
        );
    }

    group.finish();
}

// Mixed workload simulation
pub fn mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    group.throughput(Throughput::Elements(1));

    let db = MortonTemporalDB::new();
    let keys = generate_keys(1000);
    
    // Pre-populate with some data
    for (i, key) in keys.iter().take(500).enumerate() {
        let value = format!("initial_data_{}", i).into_bytes();
        let _ = db.put(key, value);
    }

    group.bench_function("read_heavy_80_20", |b| {
        let mut rng = seeded_rng(0xDEADBEEF);
        b.iter(|| {
            if rng.gen_ratio(8, 10) {
                // 80% reads
                let key = &keys[rng.gen_range(0..500)];
                let _ = black_box(db.get(key));
            } else {
                // 20% writes
                let key = &keys[rng.gen_range(0..keys.len())];
                let value = rng.r#gen::<u64>().to_le_bytes().to_vec();
                let _ = black_box(db.put(key, value));
            }
        })
    });

    group.bench_function("temporal_mixed", |b| {
        let mut rng = seeded_rng(0xCAFEBABE);
        b.iter(|| {
            let operation = rng.gen_range(0..4);
            match operation {
                0 => {
                    // Latest read (40%)
                    let key = &keys[rng.gen_range(0..500)];
                    let _ = black_box(db.get(key));
                },
                1 => {
                    // Historical read (30%)
                    let key = &keys[rng.gen_range(0..500)];
                    let timestamp = 1000000 + rng.gen_range(0..100000);
                    let _ = black_box(db.get_at_time(key, timestamp));
                },
                2 => {
                    // Current write (20%)
                    let key = &keys[rng.gen_range(0..keys.len())];
                    let value = rng.r#gen::<u64>().to_le_bytes().to_vec();
                    let _ = black_box(db.put(key, value));
                },
                3 => {
                    // Temporal write (10%)
                    let key = &keys[rng.gen_range(0..keys.len())];
                    let value = rng.r#gen::<u64>().to_le_bytes().to_vec();
                    let timestamp = 1000000 + rng.gen_range(0..200000);
                    let _ = black_box(db.put_at_time(key, value, timestamp));
                },
                _ => unreachable!(),
            }
        })
    });

    group.finish();
}

// Database use case simulations
pub fn database_scenarios(c: &mut Criterion) {
    let mut group = c.benchmark_group("database_scenarios");
    group.throughput(Throughput::Elements(1));

    // User balance updates scenario
    group.bench_function("user_balance_updates", |b| {
        let db = MortonTemporalDB::new();
        let mut rng = seeded_rng(0x987654321);
        let num_users = 10000;
        
        // Initialize users with balance
        for user_id in 0..num_users {
            let key = format!("user:{}", user_id);
            let balance = 1000u64; // Starting balance
            let _ = db.put(&key, balance.to_le_bytes().to_vec());
        }

        b.iter(|| {
            let user_id = rng.gen_range(0..num_users);
            let key = format!("user:{}", user_id);
            
            // Read current balance
            if let Ok(current_data) = db.get(&key) {
                let current_balance = u64::from_le_bytes(
                    current_data.as_slice().try_into().unwrap_or([0; 8])
                );
                
                // Update balance
                let change = rng.gen_range(-100i64..=100i64);
                let new_balance = (current_balance as i64 + change).max(0) as u64;
                let _ = black_box(db.put(&key, new_balance.to_le_bytes().to_vec()));
            }
        })
    });

    // Session tracking scenario
    group.bench_function("session_tracking", |b| {
        let db = MortonTemporalDB::new();
        let mut rng = seeded_rng(0xABCDEF123);
        let num_sessions = 5000;

        b.iter(|| {
            let session_id = rng.gen_range(0..num_sessions);
            let key = format!("session:{}", session_id);
            
            if rng.gen_ratio(7, 10) {
                // 70% - Update existing session
                let activity_data = format!("activity_{}", rng.r#gen::<u32>()).into_bytes();
                let _ = black_box(db.put(&key, activity_data));
            } else {
                // 30% - Read session data
                let _ = black_box(db.get(&key));
            }
        })
    });

    group.finish();
}

// Performance scaling tests
pub fn scaling_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("scaling_benchmark");

    for dataset_size in [1_000, 10_000, 100_000, 1_000_000].iter() {
        group.throughput(Throughput::Elements(1));
        
        group.bench_with_input(
            BenchmarkId::new("large_dataset_read", dataset_size),
            dataset_size,
            |b, &dataset_size| {
                let db = MortonTemporalDB::new();
                
                // Pre-populate database
                for i in 0..dataset_size {
                    let key = format!("large_key:{}", i);
                    let value = format!("value_{}", i).into_bytes();
                    let _ = db.put(&key, value);
                }

                let mut rng = seeded_rng(0x555666777);
                b.iter(|| {
                    let key_id = rng.gen_range(0..dataset_size);
                    let key = format!("large_key:{}", key_id);
                    let _ = black_box(db.get(&key));
                })
            }
        );
    }

    group.finish();
}

// Helper function to generate test keys
fn generate_keys(count: usize) -> Vec<String> {
    let mut keys = Vec::with_capacity(count);
    let mut rng = seeded_rng(0x123456789ABCDEF0);
    
    for i in 0..count {
        // Mix of different key patterns
        let key = match i % 4 {
            0 => format!("user:{}", rng.r#gen::<u32>()),
            1 => format!("session:{}", rng.r#gen::<u32>()),
            2 => format!("order:{}", rng.r#gen::<u32>()),
            3 => format!("product:{}", rng.r#gen::<u32>()),
            _ => unreachable!(),
        };
        keys.push(key);
    }
    
    keys.shuffle(&mut rng);
    keys
}

criterion_group!(
    basic_ops,
    seq_insert,
    seq_temporal_insert,
    rand_insert
);

criterion_group!(
    temporal_ops,
    versioning_workload,
    temporal_queries
);

criterion_group!(
    batch_ops,
    batch_operations
);

criterion_group!(
    workload_simulation,
    mixed_workload,
    database_scenarios
);

criterion_group!(
    performance_scaling,
    scaling_benchmark
);

criterion_main!(
    basic_ops,
    temporal_ops,
    batch_ops,
    workload_simulation,
    performance_scaling
);