use std::collections::BTreeMap;
use std::time::Instant;

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::prelude::SliceRandom;
use rand::SeedableRng;
use rand::{rngs::StdRng, Rng};

use zverse::master_class::{MasterClassZVerse, MasterClassConfig};

fn seeded_rng(alter: u64) -> impl Rng {
    StdRng::seed_from_u64(0xEA3C47920F94A980 ^ alter)
}

pub fn seq_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("seq_insert");
    group.throughput(Throughput::Elements(1));
    group.bench_function("master_class", |b| {
        let config = MasterClassConfig::default();
        let db = MasterClassZVerse::new(config).expect("Failed to create database");
        let mut key = 0u64;
        b.iter(|| {
            let key_bytes = key.to_le_bytes();
            let value_bytes = key.to_le_bytes();
            let _ = db.put(&key_bytes, &value_bytes);
            key += 1;
        })
    });

    group.finish();
}

pub fn seq_insert_mut(c: &mut Criterion) {
    let mut group = c.benchmark_group("seq_insert_mut");
    group.throughput(Throughput::Elements(1));
    group.bench_function("master_class", |b| {
        let config = MasterClassConfig::default();
        let db = MasterClassZVerse::new(config).expect("Failed to create database");
        let mut key = 0u64;
        b.iter(|| {
            let key_bytes = key.to_le_bytes();
            let value_bytes = key.to_le_bytes();
            let _ = db.put(&key_bytes, &value_bytes);
            key += 1;
        })
    });

    // Benchmark for BTreeMap
    group.bench_function("btreemap", |b| {
        let mut btree = BTreeMap::new();
        let mut key = 0u64;
        b.iter(|| {
            btree.insert(key, key);
            key += 1;
        })
    });

    group.finish();
}

pub fn rand_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("rand_insert");
    group.throughput(Throughput::Elements(1));

    let keys = gen_keys(3, 2, 3);

    group.bench_function("master_class", |b| {
        let config = MasterClassConfig::default();
        let db = MasterClassZVerse::new(config).expect("Failed to create database");
        let mut rng = seeded_rng(0xE080D1A42C207DAF);
        b.iter(|| {
            let key = &keys[rng.gen_range(0..keys.len())];
            let _ = db.put(key.as_bytes(), key.as_bytes());
        })
    });

    group.finish();
}

pub fn rand_insert_mut(c: &mut Criterion) {
    let mut group = c.benchmark_group("rand_insert_mut");
    group.throughput(Throughput::Elements(1));

    let keys = gen_keys(3, 2, 3);

    group.bench_function("master_class", |b| {
        let config = MasterClassConfig::default();
        let db = MasterClassZVerse::new(config).expect("Failed to create database");
        let mut rng = seeded_rng(0xE080D1A42C207DAF);
        b.iter(|| {
            let key = &keys[rng.gen_range(0..keys.len())];
            let _ = db.put(key.as_bytes(), key.as_bytes());
        })
    });

    // Benchmark for BTreeMap
    group.bench_function("btreemap", |b| {
        let mut btree = BTreeMap::new();
        let mut rng = seeded_rng(0xE080D1A42C207DAF);
        b.iter(|| {
            let key = &keys[rng.gen_range(0..keys.len())];
            btree.insert(key.clone(), key.clone());
        })
    });

    group.finish();
}

pub fn seq_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("seq_delete");
    group.throughput(Throughput::Elements(1));
    group.bench_function("master_class", |b| {
        let config = MasterClassConfig::default();
        let db = MasterClassZVerse::new(config).expect("Failed to create database");
        b.iter_custom(|iters| {
            // Pre-populate data
            for i in 0..iters {
                let key_bytes = (i as u64).to_le_bytes();
                let value_bytes = (i as u64).to_le_bytes();
                let _ = db.put(&key_bytes, &value_bytes);
            }
            
            // Measure deletion time (simulate by overwriting with tombstone)
            let start = Instant::now();
            for i in 0..iters {
                let key_bytes = (i as u64).to_le_bytes();
                let tombstone = b"__DELETED__";
                let _ = db.put(&key_bytes, tombstone);
            }
            start.elapsed()
        })
    });

    group.finish();
}

pub fn rand_delete(c: &mut Criterion) {
    let mut group = c.benchmark_group("rand_delete");
    let keys = gen_keys(3, 2, 3);

    group.throughput(Throughput::Elements(1));
    group.bench_function("master_class", |b| {
        let config = MasterClassConfig::default();
        let db = MasterClassZVerse::new(config).expect("Failed to create database");
        let mut rng = seeded_rng(0xE080D1A42C207DAF);
        
        // Pre-populate data
        for key in &keys {
            let _ = db.put(key.as_bytes(), key.as_bytes());
        }
        
        b.iter(|| {
            let key = &keys[rng.gen_range(0..keys.len())];
            let tombstone = b"__DELETED__";
            let _ = criterion::black_box(db.put(key.as_bytes(), tombstone));
        })
    });

    group.finish();
}

pub fn rand_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_get");

    group.throughput(Throughput::Elements(1));
    {
        let size = 1_000_000;
        let config = MasterClassConfig::default();
        let db = MasterClassZVerse::new(config).expect("Failed to create database");
        
        // Pre-populate data
        for i in 0..size {
            let key_bytes = (i as u64).to_le_bytes();
            let value_bytes = (i as u64).to_le_bytes();
            db.put(&key_bytes, &value_bytes).unwrap();
        }
        
        group.bench_with_input(BenchmarkId::new("master_class", size), &size, |b, size| {
            let mut rng = seeded_rng(0xE080D1A42C207DAF);
            b.iter(|| {
                let key: u64 = rng.gen_range(0..*size);
                let key_bytes = key.to_le_bytes();
                let _ = criterion::black_box(db.get(&key_bytes, None));
            })
        });
    }

    group.finish();
}

pub fn rand_get_str(c: &mut Criterion) {
    let mut group = c.benchmark_group("random_get_str");
    let keys = gen_keys(3, 2, 3);

    {
        let size = 1_000_000;
        let config = MasterClassConfig::default();
        let db = MasterClassZVerse::new(config).expect("Failed to create database");
        
        // Pre-populate data
        for (i, key) in keys.iter().enumerate() {
            let value = i.to_string();
            db.put(key.as_bytes(), value.as_bytes()).unwrap();
        }
        
        group.bench_with_input(BenchmarkId::new("master_class", size), &size, |b, _size| {
            let mut rng = seeded_rng(0xE080D1A42C207DAF);
            b.iter(|| {
                let key = &keys[rng.gen_range(0..keys.len())];
                let _ = criterion::black_box(db.get(key.as_bytes(), None));
            })
        });
    }

    group.finish();
}

pub fn seq_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("seq_get");

    group.throughput(Throughput::Elements(1));
    {
        let size = 1_000_000;
        let config = MasterClassConfig::default();
        let db = MasterClassZVerse::new(config).expect("Failed to create database");
        
        // Pre-populate data
        for i in 0..size {
            let key_bytes = (i as u64).to_le_bytes();
            let value_bytes = (i as u64).to_le_bytes();
            db.put(&key_bytes, &value_bytes).unwrap();
        }
        
        group.bench_with_input(BenchmarkId::new("master_class", size), &size, |b, _size| {
            let mut key = 0u64;
            b.iter(|| {
                let key_bytes = key.to_le_bytes();
                let _ = criterion::black_box(db.get(&key_bytes, None));
                key += 1;
            })
        });
    }

    group.finish();
}

pub fn iter_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("iter_benchmark");

    group.throughput(Throughput::Elements(1));
    {
        let size = 1_000_000;
        let config = MasterClassConfig::default();
        let db = MasterClassZVerse::new(config).expect("Failed to create database");
        
        // Pre-populate data
        for i in 0..size {
            let key_bytes = (i as u64).to_le_bytes();
            let value_bytes = (i as u64).to_le_bytes();
            db.put(&key_bytes, &value_bytes).unwrap();
        }
        
        group.bench_with_input(BenchmarkId::new("master_class", size), &size, |b, _size| {
            b.iter(|| {
                // Simulate iteration by counting stats
                let stats = db.stats();
                let count = criterion::black_box(stats.hot_tier_entries + stats.warm_tier_entries + stats.cold_tier_entries);
                // Note: This is not a true iteration, but simulates the work
                assert!(count > 0, "No items found in database");
            })
        });
    }

    group.finish();
}

pub fn range_benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_benchmark");

    group.throughput(Throughput::Elements(1));
    {
        let size = 1_000_000;
        let config = MasterClassConfig::default();
        let db = MasterClassZVerse::new(config).expect("Failed to create database");
        
        // Pre-populate data
        for i in 0..size {
            let key_bytes = (i as u64).to_le_bytes();
            let value_bytes = (i as u64).to_le_bytes();
            db.put(&key_bytes, &value_bytes).unwrap();
        }
        
        group.bench_with_input(BenchmarkId::new("master_class", size), &size, |b, _size| {
            b.iter(|| {
                // Simulate range scan by scanning a subset of sequential keys
                let mut count = 0;
                for i in 0..1000u64 {  // Scan first 1000 keys as a range
                    let key_bytes = i.to_le_bytes();
                    if let Ok(Some(_)) = db.get(&key_bytes, None) {
                        count += 1;
                    }
                }
                let _ = criterion::black_box(count);
                assert!(count > 0, "No items found in range");
            })
        });
    }

    group.finish();
}

fn gen_keys(l1_prefix: usize, l2_prefix: usize, suffix: usize) -> Vec<String> {
    let mut keys = Vec::new();
    let chars: Vec<char> = ('a'..='z').collect();
    let mut rng = seeded_rng(0x740A11E72FDC215D);
    for i in 0..chars.len() {
        let level1_prefix = chars[i].to_string().repeat(l1_prefix);
        for i in 0..chars.len() {
            let level2_prefix = chars[i].to_string().repeat(l2_prefix);
            let key_prefix = level1_prefix.clone() + &level2_prefix;
            for _ in 0..=u8::MAX {
                let suffix: String = (0..suffix)
                    .map(|_| chars[rng.gen_range(0..chars.len())])
                    .collect();
                let k = key_prefix.clone() + &suffix;
                keys.push(k);
            }
        }
    }

    keys.shuffle(&mut rng);
    keys
}

fn generate_data(
    num_keys: usize,
    key_size: usize,
    value_size: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut data = Vec::with_capacity(num_keys);

    for i in 0..num_keys {
        // Generate key
        let mut key = vec![0xFF; key_size];
        key[0..8.min(key_size)].copy_from_slice(&i.to_le_bytes()[0..8.min(key_size)]);

        // Generate value
        let value = vec![0x42; value_size];

        data.push((key, value));
    }

    data
}

fn variable_size_bulk_insert_mut(c: &mut Criterion) {
    let mut group = c.benchmark_group("master_class_insert");

    // Test different combinations of key sizes, value sizes, and number of keys
    let key_sizes = vec![16, 32, 64, 128];
    let num_keys_list = vec![100_000, 500_000, 1_000_000];

    for &num_keys in &num_keys_list {
        for &key_size in &key_sizes {
            let benchmark_id = BenchmarkId::new(format!("k{}_v{}", key_size, key_size), num_keys);

            group.bench_with_input(benchmark_id, &num_keys, |b, &num_keys| {
                // Generate test data outside the benchmark loop
                let data = generate_data(num_keys, key_size, key_size);

                b.iter(|| {
                    let config = MasterClassConfig::default();
                    let db = MasterClassZVerse::new(config).expect("Failed to create database");
                    for (key, value) in data.iter() {
                        db.put(key, value).expect("insert should succeed");
                    }
                    black_box(db)
                });
            });
        }
    }

    group.finish();
}

criterion_group!(delete_benches, seq_delete, rand_delete);
criterion_group!(
    insert_benches,
    seq_insert,
    seq_insert_mut,
    rand_insert,
    rand_insert_mut,
    variable_size_bulk_insert_mut
);
criterion_group!(read_benches, seq_get, rand_get, rand_get_str);
criterion_group!(iter_benches, iter_benchmark);
criterion_group!(range_benches, range_benchmark);
criterion_main!(
    insert_benches,
    read_benches,
    delete_benches,
    iter_benches,
    range_benches
);