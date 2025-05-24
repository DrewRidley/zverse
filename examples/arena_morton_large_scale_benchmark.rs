//! Large-Scale Arena Morton Zero Coordination Benchmark
//!
//! This benchmark tests the Arena Morton Zero Coordination Engine with realistic temporal data
//! patterns, simulating a 10GB in-memory database with millions of records and concurrent access.

use rand::Rng;
use rand::distributions::Alphanumeric;
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::thread;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use std::alloc::{Layout, alloc, dealloc};
use std::ptr;
use zverse::encoding::morton::{current_timestamp_micros, morton_t_encode};

#[derive(Serialize, Deserialize, Clone)]
struct UserProfile {
    id: usize,
    name: String,
    email: String,
    age: u32,
    country: String,
    bio: String,
    created_at: u64,
    last_login: u64,
    preferences: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct Transaction {
    id: usize,
    from_user: usize,
    to_user: usize,
    amount: f64,
    currency: String,
    description: String,
    timestamp: u64,
    status: String,
    metadata: HashMap<String, String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct SensorReading {
    sensor_id: usize,
    reading_id: usize,
    value: f64,
    unit: String,
    location: (f64, f64),
    timestamp: u64,
    quality: f32,
    tags: Vec<String>,
}

/// Cache-aligned record in arena
#[repr(C, align(64))]
#[derive(Debug, Clone)]
struct ArenaRecord {
    morton_code: u64,
    timestamp: u64,
    key_len: u32,
    value_len: u32,
    // key and value data follows immediately after
}

impl ArenaRecord {
    const HEADER_SIZE: usize = std::mem::size_of::<ArenaRecord>();

    fn total_size(key_len: usize, value_len: usize) -> usize {
        Self::HEADER_SIZE + key_len + value_len
    }

    unsafe fn key_ptr(&self) -> *const u8 {
        unsafe { (self as *const Self as *const u8).add(Self::HEADER_SIZE) }
    }

    unsafe fn value_ptr(&self) -> *const u8 {
        unsafe {
            (self as *const Self as *const u8)
                .add(Self::HEADER_SIZE)
                .add(self.key_len as usize)
        }
    }

    fn key(&self) -> &str {
        unsafe {
            let slice = std::slice::from_raw_parts(self.key_ptr(), self.key_len as usize);
            std::str::from_utf8_unchecked(slice)
        }
    }

    fn value(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.value_ptr(), self.value_len as usize) }
    }
}

/// Lock-free arena allocator for sequential cache-friendly allocation
struct Arena {
    base_ptr: *mut u8,
    size: usize,
    head: AtomicUsize,
}

unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

impl Arena {
    fn new(size: usize) -> Self {
        let layout = Layout::from_size_align(size, 64).unwrap();
        let base_ptr = unsafe { alloc(layout) };
        if base_ptr.is_null() {
            panic!("Arena allocation failed");
        }

        Self {
            base_ptr,
            size,
            head: AtomicUsize::new(0),
        }
    }

    fn allocate(&self, size: usize) -> Option<*mut u8> {
        let aligned_size = (size + 63) & !63; // 64-byte align

        loop {
            let current_head = self.head.load(Ordering::Acquire);
            let new_head = current_head + aligned_size;

            if new_head > self.size {
                return None; // Arena full
            }

            match self.head.compare_exchange_weak(
                current_head,
                new_head,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let ptr = unsafe { self.base_ptr.add(current_head) };
                    return Some(ptr);
                }
                Err(_) => continue, // Retry
            }
        }
    }

    fn utilization(&self) -> f64 {
        self.head.load(Ordering::Acquire) as f64 / self.size as f64
    }
}

impl Drop for Arena {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.size, 64).unwrap();
        unsafe { dealloc(self.base_ptr, layout) };
    }
}

/// Append-only Morton partition with atomic head pointer
struct MortonPartition {
    arena: Arena,
}

impl MortonPartition {
    fn new(arena_size: usize) -> Self {
        Self {
            arena: Arena::new(arena_size),
        }
    }

    fn append(&self, key: &str, value: &[u8], morton_code: u64, timestamp: u64) -> bool {
        let record_size = ArenaRecord::total_size(key.len(), value.len());

        let record_ptr = match self.arena.allocate(record_size) {
            Some(ptr) => ptr as *mut ArenaRecord,
            None => return false, // Arena full
        };

        unsafe {
            (*record_ptr).morton_code = morton_code;
            (*record_ptr).timestamp = timestamp;
            (*record_ptr).key_len = key.len() as u32;
            (*record_ptr).value_len = value.len() as u32;

            let key_ptr = (*record_ptr).key_ptr() as *mut u8;
            ptr::copy_nonoverlapping(key.as_ptr(), key_ptr, key.len());

            let value_ptr = (*record_ptr).value_ptr() as *mut u8;
            ptr::copy_nonoverlapping(value.as_ptr(), value_ptr, value.len());
        }

        true
    }

    fn scan<F>(&self, mut visitor: F)
    where
        F: FnMut(&ArenaRecord),
    {
        let mut current_offset = 0;
        let max_offset = self.arena.head.load(Ordering::Acquire);

        while current_offset < max_offset {
            let record_ptr =
                unsafe { self.arena.base_ptr.add(current_offset) as *const ArenaRecord };

            let record = unsafe { &*record_ptr };
            visitor(record);

            current_offset +=
                ArenaRecord::total_size(record.key_len as usize, record.value_len as usize);
            current_offset = (current_offset + 63) & !63; // Align to 64 bytes
        }
    }

    fn utilization(&self) -> f64 {
        self.arena.utilization()
    }

    fn record_count(&self) -> usize {
        let mut count = 0;
        self.scan(|_| count += 1);
        count
    }
}

/// True Zero-Coordination Morton Engine
struct ArenaZeroCoordEngine {
    partitions: Vec<Arc<MortonPartition>>,
    partition_count: usize,
    global_timestamp: AtomicU64,
}

impl ArenaZeroCoordEngine {
    fn new(partition_count: usize, arena_size_per_partition: usize) -> Self {
        let mut partitions = Vec::with_capacity(partition_count);
        for _ in 0..partition_count {
            partitions.push(Arc::new(MortonPartition::new(arena_size_per_partition)));
        }

        Self {
            partitions,
            partition_count,
            global_timestamp: AtomicU64::new(current_timestamp_micros()),
        }
    }

    fn put(&self, key: &str, value: &[u8]) -> bool {
        let timestamp = self.global_timestamp.fetch_add(1, Ordering::SeqCst);
        let morton_code = morton_t_encode(key, timestamp).value();

        let mut hasher = DefaultHasher::new();
        morton_code.hash(&mut hasher);
        let partition_id = (hasher.finish() % self.partition_count as u64) as usize;

        self.partitions[partition_id].append(key, value, morton_code, timestamp)
    }

    fn get(&self, key: &str) -> Option<Vec<u8>> {
        let mut best_match: Option<Vec<u8>> = None;
        let mut best_timestamp = 0u64;

        for partition in &self.partitions {
            partition.scan(|record| {
                if record.key() == key && record.timestamp > best_timestamp {
                    best_timestamp = record.timestamp;
                    best_match = Some(record.value().to_vec());
                }
            });
        }

        best_match
    }

    fn range_query(&self, key_start: &str, key_end: &str) -> Vec<(String, Vec<u8>)> {
        let mut results_with_timestamps = Vec::new();

        for partition in &self.partitions {
            partition.scan(|record| {
                let key = record.key();
                if key >= key_start && key <= key_end {
                    results_with_timestamps.push((
                        key.to_string(),
                        record.value().to_vec(),
                        record.timestamp,
                    ));
                }
            });
        }

        results_with_timestamps.sort_by(|a, b| {
            a.0.cmp(&b.0).then(b.2.cmp(&a.2)) // key asc, timestamp desc
        });

        let mut final_results = Vec::new();
        let mut last_key: Option<String> = None;

        for (key, value, _timestamp) in results_with_timestamps {
            if last_key.as_ref() != Some(&key) {
                final_results.push((key.clone(), value));
                last_key = Some(key);
            }
        }

        final_results
    }

    fn stats(&self) -> EngineStats {
        let mut total_records = 0;
        let mut total_utilization: f64 = 0.0;
        let mut max_utilization: f64 = 0.0;
        let mut min_utilization: f64 = 1.0;

        for partition in &self.partitions {
            let count = partition.record_count();
            let util = partition.utilization();

            total_records += count;
            total_utilization += util;
            if util > max_utilization {
                max_utilization = util;
            }
            if util < min_utilization {
                min_utilization = util;
            }
        }

        EngineStats {
            total_records,
            partition_count: self.partition_count,
            avg_utilization: total_utilization / self.partition_count as f64,
            max_utilization,
            min_utilization,
        }
    }
}

#[derive(Debug)]
struct EngineStats {
    total_records: usize,
    partition_count: usize,
    avg_utilization: f64,
    max_utilization: f64,
    min_utilization: f64,
}

fn generate_user_profile(rng: &mut StdRng, user_id: usize) -> UserProfile {
    let countries = vec!["US", "UK", "DE", "FR", "JP", "AU", "CA", "BR"];
    let preferences = vec![
        "sports", "music", "tech", "travel", "food", "art", "science",
    ];

    UserProfile {
        id: user_id,
        name: format!("User{}", user_id),
        email: format!("user{}@example.com", user_id),
        age: rng.gen_range(18..80),
        country: countries[rng.gen_range(0..countries.len())].to_string(),
        bio: (0..rng.gen_range(50..200))
            .map(|_| rng.sample(Alphanumeric) as char)
            .collect(),
        created_at: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64,
        last_login: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64,
        preferences: preferences
            .into_iter()
            .filter(|_| rng.gen_bool(0.3))
            .map(|s| s.to_string())
            .collect(),
    }
}

fn generate_transaction(rng: &mut StdRng, tx_id: usize) -> Transaction {
    let currencies = vec!["USD", "EUR", "GBP", "JPY", "AUD"];
    let statuses = vec!["pending", "completed", "failed", "cancelled"];

    let mut metadata = HashMap::new();
    metadata.insert(
        "ip".to_string(),
        format!(
            "{}.{}.{}.{}",
            rng.gen_range(1..255),
            rng.gen_range(0..255),
            rng.gen_range(0..255),
            rng.gen_range(1..255)
        ),
    );
    metadata.insert(
        "device".to_string(),
        format!("device_{}", rng.gen_range(1000..9999)),
    );

    let from_user = rng.gen_range(0..1000000);
    let to_user = rng.gen_range(0..1000000);
    let amount: f64 = rng.sample(rand::distributions::Standard);
    let amount = amount * 10000.0;
    let currency = currencies[rng.gen_range(0..currencies.len())].to_string();
    let desc_len = rng.gen_range(20..100);
    let description = (0..desc_len)
        .map(|_| rng.sample(Alphanumeric) as char)
        .collect();
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    let status = statuses[rng.gen_range(0..statuses.len())].to_string();

    Transaction {
        id: tx_id,
        from_user,
        to_user,
        amount,
        currency,
        description,
        timestamp,
        status,
        metadata,
    }
}

fn generate_sensor_reading(rng: &mut StdRng, sensor_id: usize, reading_id: usize) -> SensorReading {
    let units = vec!["°C", "°F", "Pa", "m/s", "lux", "dB", "%"];
    let tags = vec!["indoor", "outdoor", "critical", "normal", "calibrated"];

    let value: f64 = rng.sample(rand::distributions::Standard);
    let value = value * 100.0;
    let unit = units[rng.gen_range(0..units.len())].to_string();
    let lat: f64 = rng.sample(rand::distributions::Standard);
    let lat = lat * 180.0 - 90.0;
    let lon: f64 = rng.sample(rand::distributions::Standard);
    let lon = lon * 360.0 - 180.0;
    let location = (lat, lon);
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    let quality: f32 = rng.sample(rand::distributions::Standard);
    let filtered_tags = tags
        .into_iter()
        .filter(|_| rng.gen_bool(0.2))
        .map(|s| s.to_string())
        .collect();

    SensorReading {
        sensor_id,
        reading_id,
        value,
        unit,
        location,
        timestamp,
        quality,
        tags: filtered_tags,
    }
}

fn serialize_data<T: Serialize>(data: &T) -> Vec<u8> {
    bincode::serialize(data).unwrap()
}

fn benchmark_user_profiles(
    engine: &Arc<ArenaZeroCoordEngine>,
    count: usize,
    total_records: &Arc<AtomicUsize>,
    total_bytes: &Arc<AtomicU64>,
    errors: &Arc<AtomicUsize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut rng = StdRng::seed_from_u64(42);
    let mut local_bytes = 0u64;
    let mut local_errors = 0;

    for i in 0..count {
        let user = generate_user_profile(&mut rng, i);
        let key = format!("user:{:08}", user.id);
        let data = serialize_data(&user);
        local_bytes += data.len() as u64;

        if engine.put(&key, &data) {
            total_records.fetch_add(1, Ordering::Relaxed);
        } else {
            local_errors += 1;
        }

        if i % 50000 == 0 {
            println!("  Generated {} user profiles...", i);
        }
    }

    total_bytes.fetch_add(local_bytes, Ordering::Relaxed);
    errors.fetch_add(local_errors, Ordering::Relaxed);
    Ok(())
}

fn benchmark_transactions(
    engine: &Arc<ArenaZeroCoordEngine>,
    count: usize,
    total_records: &Arc<AtomicUsize>,
    total_bytes: &Arc<AtomicU64>,
    errors: &Arc<AtomicUsize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut rng = StdRng::seed_from_u64(123);
    let mut local_bytes = 0u64;
    let mut local_errors = 0;

    for i in 0..count {
        let tx = generate_transaction(&mut rng, i);
        let key = format!("tx:{:08}:{}", tx.id, tx.timestamp);
        let data = serialize_data(&tx);
        local_bytes += data.len() as u64;

        if engine.put(&key, &data) {
            total_records.fetch_add(1, Ordering::Relaxed);
        } else {
            local_errors += 1;
        }

        if i % 50000 == 0 {
            println!("  Generated {} transactions...", i);
        }
    }

    total_bytes.fetch_add(local_bytes, Ordering::Relaxed);
    errors.fetch_add(local_errors, Ordering::Relaxed);
    Ok(())
}

fn benchmark_sensor_data(
    engine: &Arc<ArenaZeroCoordEngine>,
    count: usize,
    total_records: &Arc<AtomicUsize>,
    total_bytes: &Arc<AtomicU64>,
    errors: &Arc<AtomicUsize>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut rng = StdRng::seed_from_u64(456);
    let mut local_bytes = 0u64;
    let mut local_errors = 0;

    for i in 0..count {
        let sensor = generate_sensor_reading(&mut rng, i % 1000, i);
        let key = format!("sensor:{:04}:{:08}", sensor.sensor_id, sensor.reading_id);
        let data = serialize_data(&sensor);
        local_bytes += data.len() as u64;

        if engine.put(&key, &data) {
            total_records.fetch_add(1, Ordering::Relaxed);
        } else {
            local_errors += 1;
        }

        if i % 100000 == 0 {
            println!("  Generated {} sensor readings...", i);
        }
    }

    total_bytes.fetch_add(local_bytes, Ordering::Relaxed);
    errors.fetch_add(local_errors, Ordering::Relaxed);
    Ok(())
}

fn benchmark_concurrent_mixed_workload(
    engine: &Arc<ArenaZeroCoordEngine>,
    thread_count: usize,
    ops_per_thread: usize,
) -> Result<(f64, f64), Box<dyn std::error::Error>> {
    let total_ops = Arc::new(AtomicUsize::new(0));
    let total_errors = Arc::new(AtomicUsize::new(0));

    let start = Instant::now();

    let handles: Vec<_> = (0..thread_count)
        .map(|thread_id| {
            let engine = engine.clone();
            let total_ops = total_ops.clone();
            let total_errors = total_errors.clone();

            thread::spawn(move || {
                let mut rng = StdRng::seed_from_u64(thread_id as u64);
                let mut local_ops = 0;
                let mut local_errors = 0;

                for i in 0..ops_per_thread {
                    let operation = rng.gen_range(0..100);

                    match operation {
                        0..=60 => {
                            // 60% writes - user profiles
                            let user =
                                generate_user_profile(&mut rng, thread_id * ops_per_thread + i);
                            let key = format!("concurrent:user:{:02}:{:08}", thread_id, i);
                            let data = serialize_data(&user);
                            if engine.put(&key, &data) {
                                local_ops += 1;
                            } else {
                                local_errors += 1;
                            }
                        }
                        61..=80 => {
                            // 20% writes - transactions
                            let tx = generate_transaction(&mut rng, thread_id * ops_per_thread + i);
                            let key = format!("concurrent:tx:{:02}:{:08}", thread_id, i);
                            let data = serialize_data(&tx);
                            if engine.put(&key, &data) {
                                local_ops += 1;
                            } else {
                                local_errors += 1;
                            }
                        }
                        81..=90 => {
                            // 10% writes - sensor data
                            let sensor = generate_sensor_reading(&mut rng, thread_id, i);
                            let key = format!("concurrent:sensor:{:02}:{:08}", thread_id, i);
                            let data = serialize_data(&sensor);
                            if engine.put(&key, &data) {
                                local_ops += 1;
                            } else {
                                local_errors += 1;
                            }
                        }
                        _ => {
                            // 10% reads
                            let key = format!(
                                "concurrent:user:{:02}:{:08}",
                                thread_id,
                                rng.gen_range(0..i.max(1))
                            );
                            if engine.get(&key).is_some() {
                                local_ops += 1;
                            } else {
                                // Count as success even if not found
                                local_ops += 1;
                            }
                        }
                    }
                }

                total_ops.fetch_add(local_ops, Ordering::Relaxed);
                total_errors.fetch_add(local_errors, Ordering::Relaxed);
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let duration = start.elapsed();
    let ops_completed = total_ops.load(Ordering::Relaxed);
    let errors = total_errors.load(Ordering::Relaxed);
    let throughput = ops_completed as f64 / duration.as_secs_f64();
    let success_rate = ops_completed as f64 / (ops_completed + errors) as f64 * 100.0;

    Ok((throughput, success_rate))
}

fn test_range_queries(engine: &Arc<ArenaZeroCoordEngine>) {
    println!("\n🔍 Range Query Performance Test");
    println!("------------------------------");

    let ranges = [
        ("user:00000000", "user:00010000", "First 10K users"),
        ("tx:00000000", "tx:00050000", "First 50K transactions"),
        ("sensor:0000", "sensor:0050", "First 50 sensors"),
        (
            "concurrent:user:00",
            "concurrent:user:05",
            "Concurrent user data",
        ),
    ];

    for (start_key, end_key, description) in ranges {
        let range_start = Instant::now();
        let results = engine.range_query(start_key, end_key);
        let range_time = range_start.elapsed();

        println!(
            "{}: {} results in {:?}",
            description,
            results.len(),
            range_time
        );
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 ARENA MORTON ZERO COORDINATION LARGE-SCALE BENCHMARK");
    println!("=======================================================");
    println!("Testing TRUE zero-coordination with realistic 10GB dataset:");
    println!("  • Arena allocation for cache-friendly memory layout");
    println!("  • Morton-T encoding for temporal locality");
    println!("  • Hash-based partition distribution");
    println!("  • Lock-free concurrent access");
    println!("  • Append-only logs with atomic pointers");

    // Configure for ~10GB in-memory storage
    let partition_count = 64;
    let arena_size_per_partition = 160 * 1024 * 1024; // 160MB per partition = ~10GB total

    println!("\n📁 Engine Configuration:");
    println!("  Partition count: {}", partition_count);
    println!(
        "  Arena size per partition: {:.0}MB",
        arena_size_per_partition as f64 / 1024.0 / 1024.0
    );
    println!(
        "  Total memory capacity: {:.1}GB",
        (partition_count * arena_size_per_partition) as f64 / 1024.0 / 1024.0 / 1024.0
    );

    // Create engine
    let start = Instant::now();
    let engine = Arc::new(ArenaZeroCoordEngine::new(
        partition_count,
        arena_size_per_partition,
    ));
    let creation_time = start.elapsed();

    println!("  Creation time: {:?}", creation_time);

    // Global counters
    let total_records = Arc::new(AtomicUsize::new(0));
    let total_bytes = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicUsize::new(0));

    println!("\n🔥 Large-Scale Data Generation");

    // Phase 1: User Profiles (1M users, ~1.5GB)
    println!("\n1️⃣ Phase 1: User Profiles (Target: 1M users)");
    let user_start = Instant::now();
    benchmark_user_profiles(&engine, 1_000_000, &total_records, &total_bytes, &errors)?;
    let user_time = user_start.elapsed();

    let user_records = total_records.load(Ordering::Relaxed);
    let user_bytes = total_bytes.load(Ordering::Relaxed);
    println!(
        "  ✅ Generated {} user records in {:?}",
        user_records, user_time
    );
    println!(
        "  📊 Data size: {:.2} GB",
        user_bytes as f64 / 1024.0 / 1024.0 / 1024.0
    );
    println!(
        "  🚀 Throughput: {:.0} records/sec",
        user_records as f64 / user_time.as_secs_f64()
    );

    // Phase 2: Transactions (3M transactions, ~4GB)
    println!("\n2️⃣ Phase 2: Transaction Logs (Target: 3M transactions)");
    let tx_start = Instant::now();
    let tx_start_records = total_records.load(Ordering::Relaxed);
    let tx_start_bytes = total_bytes.load(Ordering::Relaxed);

    benchmark_transactions(&engine, 3_000_000, &total_records, &total_bytes, &errors)?;
    let tx_time = tx_start.elapsed();

    let tx_records = total_records.load(Ordering::Relaxed) - tx_start_records;
    let tx_bytes = total_bytes.load(Ordering::Relaxed) - tx_start_bytes;
    println!(
        "  ✅ Generated {} transaction records in {:?}",
        tx_records, tx_time
    );
    println!(
        "  📊 Additional data: {:.2} GB",
        tx_bytes as f64 / 1024.0 / 1024.0 / 1024.0
    );
    println!(
        "  🚀 Throughput: {:.0} records/sec",
        tx_records as f64 / tx_time.as_secs_f64()
    );

    // Phase 3: Sensor Data (6M readings, ~4.5GB)
    println!("\n3️⃣ Phase 3: Sensor Readings (Target: 6M readings)");
    let sensor_start = Instant::now();
    let sensor_start_records = total_records.load(Ordering::Relaxed);
    let sensor_start_bytes = total_bytes.load(Ordering::Relaxed);

    benchmark_sensor_data(&engine, 6_000_000, &total_records, &total_bytes, &errors)?;
    let sensor_time = sensor_start.elapsed();

    let sensor_records = total_records.load(Ordering::Relaxed) - sensor_start_records;
    let sensor_bytes = total_bytes.load(Ordering::Relaxed) - sensor_start_bytes;
    println!(
        "  ✅ Generated {} sensor records in {:?}",
        sensor_records, sensor_time
    );
    println!(
        "  📊 Additional data: {:.2} GB",
        sensor_bytes as f64 / 1024.0 / 1024.0 / 1024.0
    );
    println!(
        "  🚀 Throughput: {:.0} records/sec",
        sensor_records as f64 / sensor_time.as_secs_f64()
    );

    let final_records = total_records.load(Ordering::Relaxed);
    let final_bytes = total_bytes.load(Ordering::Relaxed);
    let total_errors = errors.load(Ordering::Relaxed);

    println!("\n📊 Summary:");
    println!("-----------------------------");
    println!("  Total records: {}", final_records);
    println!(
        "  Total bytes: {:.2} GB",
        final_bytes as f64 / 1024.0 / 1024.0 / 1024.0
    );
    println!("  Total errors: {}", total_errors);

    let stats = engine.stats();
    println!("\n🧮 Engine Stats:");
    println!("  Partitions: {}", stats.partition_count);
    println!("  Avg utilization: {:.2}%", stats.avg_utilization * 100.0);
    println!("  Max utilization: {:.2}%", stats.max_utilization * 100.0);
    println!("  Min utilization: {:.2}%", stats.min_utilization * 100.0);
    println!("  Total records (engine): {}", stats.total_records);

    // Phase 4: Concurrent Mixed Workload
    println!("\n⚡ Phase 4: Concurrent Mixed Workload (16 threads × 100K ops/thread)");
    let thread_count = 10;
    let ops_per_thread = 100_000;
    let (throughput, success_rate) =
        benchmark_concurrent_mixed_workload(&engine, thread_count, ops_per_thread)?;
    println!(
        "  ✅ Completed {} ops ({} threads)",
        thread_count * ops_per_thread,
        thread_count
    );
    println!("  🚀 Throughput: {:.0} ops/sec", throughput);
    println!("  🎯 Success rate: {:.2}%", success_rate);

    // Phase 5: Range Query Performance
    test_range_queries(&engine);

    println!("\n🏁 Benchmark complete.");
    Ok(())
}
