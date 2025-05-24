use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rand::SeedableRng;
use rand::{rngs::StdRng, Rng};
use zverse::MortonTemporalDB;

fn seeded_rng(alter: u64) -> impl Rng {
    StdRng::seed_from_u64(0xEA3C47920F94A980 ^ alter)
}

// Create multiple versions of same key over time
pub fn key_versioning(c: &mut Criterion) {
    let mut group = c.benchmark_group("key_versioning");
    group.throughput(Throughput::Elements(1));

    for versions in [5, 10, 20, 50].iter() {
        group.bench_with_input(
            BenchmarkId::new("create_versions", versions),
            versions,
            |b, &versions| {
                b.iter(|| {
                    let db = MortonTemporalDB::new();
                    let key = "user:alice";
                    let base_time = 1000000u64;
                    
                    for version in 0..versions {
                        let value = format!("balance:{}", version * 100).into_bytes();
                        let timestamp = base_time + (version as u64 * 1000);
                        let _ = black_box(db.put_at_time(key, value, timestamp));
                    }
                })
            }
        );
    }

    group.finish();
}

// Fetch older versions of keys
pub fn historical_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("historical_queries");
    group.throughput(Throughput::Elements(1));

    // Setup: Create database with versioned data
    let db = MortonTemporalDB::new();
    let num_keys = 1000;
    let versions_per_key = 20;
    let base_time = 1000000u64;
    let time_interval = 5000u64; // 5ms between versions

    for key_id in 0..num_keys {
        for version in 0..versions_per_key {
            let key = format!("account:{}", key_id);
            let balance: u64 = 1000 + (version * 50); // Increasing balance
            let value = balance.to_le_bytes().to_vec();
            let timestamp = base_time + (version as u64 * time_interval);
            let _ = db.put_at_time(&key, value, timestamp);
        }
    }

    group.bench_function("get_latest_version", |b| {
        let mut rng = seeded_rng(0x123456789ABCDEF0);
        b.iter(|| {
            let key_id = rng.gen_range(0..num_keys);
            let key = format!("account:{}", key_id);
            let _ = black_box(db.get(&key));
        })
    });

    group.bench_function("get_random_historical", |b| {
        let mut rng = seeded_rng(0x987654321FEDCBA0);
        b.iter(|| {
            let key_id = rng.gen_range(0..num_keys);
            let version = rng.gen_range(0..versions_per_key);
            let key = format!("account:{}", key_id);
            let query_time = base_time + (version as u64 * time_interval) + 1000; // Slightly after version
            let _ = black_box(db.get_at_time(&key, query_time));
        })
    });

    group.bench_function("get_slightly_older", |b| {
        let mut rng = seeded_rng(0xABCDEF0123456789);
        b.iter(|| {
            let key_id = rng.gen_range(0..num_keys);
            let key = format!("account:{}", key_id);
            // Query for second-to-last version
            let query_time = base_time + ((versions_per_key - 2) as u64 * time_interval) + 1000;
            let _ = black_box(db.get_at_time(&key, query_time));
        })
    });

    group.finish();
}

// Temporal range queries
pub fn temporal_range_queries(c: &mut Criterion) {
    let mut group = c.benchmark_group("temporal_range_queries");
    
    let db = MortonTemporalDB::new();
    let key = "sensor:temperature";
    let measurements = 1000;
    let base_time = 1000000u64;
    let interval = 1000u64; // 1ms intervals
    
    // Setup: Create time series data
    for i in 0..measurements {
        let temperature = 20.0 + (i as f64 * 0.1);
        let value = temperature.to_le_bytes().to_vec();
        let timestamp = base_time + (i as u64 * interval);
        let _ = db.put_at_time(key, value, timestamp);
    }

    group.bench_function("query_recent_window", |b| {
        let mut rng = seeded_rng(0xCAFEBABE);
        b.iter(|| {
            // Query within last 100 measurements
            let end_measurement = rng.gen_range(100..measurements);
            let query_time = base_time + (end_measurement as u64 * interval) - 500;
            let _ = black_box(db.get_at_time(key, query_time));
        })
    });

    group.finish();
}

// Database transaction simulation
pub fn transaction_simulation(c: &mut Criterion) {
    let mut group = c.benchmark_group("transaction_simulation");
    group.throughput(Throughput::Elements(1));

    group.bench_function("bank_transfer", |b| {
        let db = MortonTemporalDB::new();
        let mut rng = seeded_rng(0xDEADBEEF);
        
        // Initialize account balances
        for account_id in 0..1000 {
            let key = format!("account:{}", account_id);
            let balance = 10000u64; // $100.00 in cents
            let _ = db.put(&key, balance.to_le_bytes().to_vec());
        }

        b.iter(|| {
            let from_account = rng.gen_range(0..1000);
            let to_account = rng.gen_range(0..1000);
            let amount = rng.gen_range(1..1000); // $0.01 to $9.99
            
            let from_key = format!("account:{}", from_account);
            let to_key = format!("account:{}", to_account);
            
            // Read balances
            if let (Ok(from_data), Ok(to_data)) = (db.get(&from_key), db.get(&to_key)) {
                let from_balance = u64::from_le_bytes(
                    from_data.as_slice().try_into().unwrap_or([0; 8])
                );
                let to_balance = u64::from_le_bytes(
                    to_data.as_slice().try_into().unwrap_or([0; 8])
                );
                
                if from_balance >= amount {
                    // Perform transfer
                    let new_from_balance = from_balance - amount;
                    let new_to_balance = to_balance + amount;
                    
                    let _ = black_box(db.put(&from_key, new_from_balance.to_le_bytes().to_vec()));
                    let _ = black_box(db.put(&to_key, new_to_balance.to_le_bytes().to_vec()));
                }
            }
        })
    });

    group.finish();
}

// Event sourcing pattern
pub fn event_sourcing(c: &mut Criterion) {
    let mut group = c.benchmark_group("event_sourcing");
    group.throughput(Throughput::Elements(1));

    group.bench_function("append_events", |b| {
        let db = MortonTemporalDB::new();
        let mut event_counter = 0u64;
        let mut rng = seeded_rng(0x123ABC456DEF789);
        
        b.iter(|| {
            let user_id = rng.gen_range(0..1000);
            let event_type = ["login", "purchase", "logout", "update_profile"]
                [rng.gen_range(0..4)];
            
            let key = format!("user:{}:events", user_id);
            let event_data = format!("{{\"type\":\"{}\",\"id\":{}}}", event_type, event_counter);
            let value = event_data.into_bytes();
            
            let _ = black_box(db.put(&key, value));
            event_counter += 1;
        })
    });

    group.finish();
}

// Audit trail queries
pub fn audit_trail(c: &mut Criterion) {
    let mut group = c.benchmark_group("audit_trail");
    
    let db = MortonTemporalDB::new();
    let base_time = 1000000u64;
    let num_records = 10000;
    
    // Setup: Create audit trail
    for i in 0..num_records {
        let key = format!("document:{}", i % 100); // 100 documents with multiple edits
        let value = format!("edit_v{}", i).into_bytes();
        let timestamp = base_time + (i as u64 * 2000); // 2ms between edits
        let _ = db.put_at_time(&key, value, timestamp);
    }

    group.bench_function("latest_document_state", |b| {
        let mut rng = seeded_rng(0x567890ABCDEF123);
        b.iter(|| {
            let doc_id = rng.gen_range(0..100);
            let key = format!("document:{}", doc_id);
            let _ = black_box(db.get(&key));
        })
    });

    group.bench_function("document_at_time", |b| {
        let mut rng = seeded_rng(0xFEDCBA0987654321);
        b.iter(|| {
            let doc_id = rng.gen_range(0..100);
            let time_offset = rng.gen_range(0..num_records as u64);
            let key = format!("document:{}", doc_id);
            let query_time = base_time + (time_offset * 2000) + 1000;
            let _ = black_box(db.get_at_time(&key, query_time));
        })
    });

    group.finish();
}

// Concurrent version creation
pub fn concurrent_versioning(c: &mut Criterion) {
    let mut group = c.benchmark_group("concurrent_versioning");
    group.throughput(Throughput::Elements(1));

    group.bench_function("rapid_updates", |b| {
        let db = MortonTemporalDB::new();
        let mut counter = 0u64;
        
        b.iter(|| {
            let key = "high_frequency_key";
            let value = counter.to_le_bytes().to_vec();
            let _ = black_box(db.put(key, value));
            counter += 1;
        })
    });

    group.finish();
}

criterion_group!(
    versioning_benches,
    key_versioning,
    historical_queries,
    temporal_range_queries
);

criterion_group!(
    application_benches,
    transaction_simulation,
    event_sourcing,
    audit_trail,
    concurrent_versioning
);

criterion_main!(
    versioning_benches,
    application_benches
);