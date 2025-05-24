//! Lock-Free Versioned Morton Engine Benchmark
//!
//! Simple approach: Morton encode (key, version) directly into u64 for O(1) lookup

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use dashmap::DashMap;
use rand::prelude::*;
use rand::distributions::Alphanumeric;
use serde::{Serialize, Deserialize};
use zverse::encoding::morton::{morton_t_encode, current_timestamp_micros};

#[derive(Clone, Debug)]
struct Record {
    key: String,
    timestamp: u64,
    value: Vec<u8>,
}

struct SimpleMortonEngine {
    // Morton code -> Record
    storage: DashMap<u64, Record>,
    // Key -> current version number
    versions: DashMap<String, u8>,
    global_timestamp: AtomicU64,
}

impl SimpleMortonEngine {
    fn new() -> Self {
        Self {
            storage: DashMap::new(),
            versions: DashMap::new(),
            global_timestamp: AtomicU64::new(current_timestamp_micros()),
        }
    }

    fn put(&self, key: &str, value: Vec<u8>) -> bool {
        let timestamp = self.global_timestamp.fetch_add(1, Ordering::Relaxed);
        
        // Always insert as version 0 (latest)
        let morton_0 = morton_t_encode(key, 0).value();
        
        // Check if there's an existing version 0
        if let Some(old_record) = self.storage.get(&morton_0) {
            // Move existing version 0 -> version 1
            let morton_1 = morton_t_encode(key, 1).value();
            let moved_record = Record {
                key: old_record.key.clone(),
                timestamp: old_record.timestamp,
                value: old_record.value.clone(),
            };
            self.storage.insert(morton_1, moved_record);
        }
        
        // Insert new record as version 0
        let new_record = Record {
            key: key.to_string(),
            timestamp,
            value,
        };
        self.storage.insert(morton_0, new_record);
        
        // Update version counter
        let mut version_entry = self.versions.entry(key.to_string()).or_insert(0);
        if *version_entry < 254 {
            *version_entry += 1;
        }
        
        true
    }

    fn get(&self, key: &str) -> Option<Vec<u8>> {
        let morton_0 = morton_t_encode(key, 0).value();
        self.storage.get(&morton_0).map(|record| record.value.clone())
    }

    fn get_version(&self, key: &str, version: u8) -> Option<Vec<u8>> {
        let morton = morton_t_encode(key, version as u64).value();
        self.storage.get(&morton).map(|record| record.value.clone())
    }

    fn range_scan(&self, start_key: &str, end_key: &str) -> Vec<(String, Vec<u8>)> {
        let mut results = Vec::new();
        
        // Collect all version 0 records in range
        for entry in self.storage.iter() {
            let morton_code = *entry.key();
            let record = entry.value();
            
            // Check if this is a version 0 record (latest)
            let expected_morton_0 = morton_t_encode(&record.key, 0).value();
            if morton_code == expected_morton_0 {
                if record.key.as_str() >= start_key && record.key.as_str() <= end_key {
                    results.push((record.key.clone(), record.value.clone()));
                }
            }
        }
        
        results.sort_by(|a, b| a.0.cmp(&b.0));
        results
    }

    fn stats(&self) -> (usize, usize) {
        let total_records = self.storage.len();
        
        // Count version 0 records (latest versions)
        let mut unique_keys = 0;
        for entry in self.storage.iter() {
            let morton_code = *entry.key();
            let record = entry.value();
            let expected_morton_0 = morton_t_encode(&record.key, 0).value();
            if morton_code == expected_morton_0 {
                unique_keys += 1;
            }
        }
        
        (total_records, unique_keys)
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct UserProfile {
    id: String,
    name: String,
    email: String,
    age: u32,
    data: Vec<u8>,
}

fn generate_user_profile(rng: &mut StdRng, user_id: &str) -> UserProfile {
    let name_len = rng.gen_range(5..20);
    let name: String = (0..name_len)
        .map(|_| rng.sample(Alphanumeric) as char)
        .collect();
    
    let data_size = rng.gen_range(100..500);
    let data: Vec<u8> = (0..data_size)
        .map(|_| rng.sample(rand::distributions::Standard))
        .collect();

    UserProfile {
        id: user_id.to_string(),
        name,
        email: format!("{}@example.com", user_id),
        age: rng.gen_range(18..80),
        data,
    }
}

fn serialize_profile(profile: &UserProfile) -> Vec<u8> {
    bincode::serialize(profile).unwrap()
}

fn benchmark_single_threaded(engine: &SimpleMortonEngine, operations: usize) {
    println!("\n🔄 Single-Threaded Benchmark ({} operations)", operations);
    println!("--------------------------------------------");
    
    let mut rng = StdRng::seed_from_u64(42);
    
    // Write phase
    println!("  Starting writes...");
    let write_start = Instant::now();
    for i in 0..operations {
        let user_id = format!("user:{:08}", i);
        let profile = generate_user_profile(&mut rng, &user_id);
        let data = serialize_profile(&profile);
        engine.put(&user_id, data);
        
        if i % 10000 == 0 {
            println!("    Written {} records...", i);
        }
    }
    let write_time = write_start.elapsed();
    
    // Read phase
    println!("  Starting reads...");
    let read_start = Instant::now();
    let mut hits = 0;
    for i in 0..operations {
        let user_id = format!("user:{:08}", i);
        if engine.get(&user_id).is_some() {
            hits += 1;
        }
    }
    let read_time = read_start.elapsed();
    
    println!("  ✅ Writes: {} ops in {:?} ({:.0} ops/sec)", 
             operations, write_time, operations as f64 / write_time.as_secs_f64());
    println!("  ✅ Reads:  {}/{} hits in {:?} ({:.0} ops/sec)", 
             hits, operations, read_time, operations as f64 / read_time.as_secs_f64());
}

fn benchmark_concurrent(engine: &Arc<SimpleMortonEngine>, thread_count: usize, ops_per_thread: usize) {
    println!("\n⚡ Concurrent Benchmark ({} threads × {} ops)", thread_count, ops_per_thread);
    println!("--------------------------------------------");
    
    let start = Instant::now();
    
    let handles: Vec<_> = (0..thread_count).map(|thread_id| {
        let engine = engine.clone();
        thread::spawn(move || {
            let mut rng = StdRng::seed_from_u64(thread_id as u64);
            
            for i in 0..ops_per_thread {
                let user_id = format!("user:{:02}:{:08}", thread_id, i);
                
                if i % 10 < 8 {
                    // 80% writes
                    let profile = generate_user_profile(&mut rng, &user_id);
                    let data = serialize_profile(&profile);
                    engine.put(&user_id, data);
                } else {
                    // 20% reads
                    engine.get(&user_id);
                }
            }
        })
    }).collect();
    
    for handle in handles {
        handle.join().unwrap();
    }
    
    let duration = start.elapsed();
    let total_ops = thread_count * ops_per_thread;
    let throughput = total_ops as f64 / duration.as_secs_f64();
    
    println!("  ✅ Completed {} ops in {:?}", total_ops, duration);
    println!("  🚀 Throughput: {:.0} ops/sec", throughput);
}

fn benchmark_range_scans(engine: &SimpleMortonEngine) {
    println!("\n🔍 Range Scan Performance");
    println!("-------------------------");
    
    let test_cases = [
        ("user:00000000", "user:00010000", "Small range (10K)"),
        ("user:00000000", "user:00100000", "Medium range (100K)"),
        ("user:00000000", "user:99999999", "Full range"),
    ];
    
    for (start_key, end_key, description) in test_cases {
        let scan_start = Instant::now();
        let results = engine.range_scan(start_key, end_key);
        let scan_time = scan_start.elapsed();
        
        println!("  {}: {} results in {:?}", description, results.len(), scan_time);
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 SIMPLE LOCK-FREE MORTON BENCHMARK");
    println!("====================================");
    println!("Testing simplified approach:");
    println!("  • Morton encoding: (key, version) -> u64");
    println!("  • Version 0 = always latest");
    println!("  • Simple DashMap storage");
    println!("  • No complex version management");
    
    let engine = Arc::new(SimpleMortonEngine::new());
    
    // Start small to verify it works
    benchmark_single_threaded(&engine, 10000);
    
    let (total, unique) = engine.stats();
    println!("\n📊 Stats: {} total records, {} unique keys", total, unique);
    
    // Test concurrent access
    benchmark_concurrent(&engine, 4, 5000);
    
    let (total, unique) = engine.stats();
    println!("\n📊 Stats: {} total records, {} unique keys", total, unique);
    
    // Test range scans
    benchmark_range_scans(&engine);
    
    // Version test
    println!("\n📅 Version Test");
    println!("---------------");
    let test_key = "user:00:00000100";
    
    // Update same key multiple times
    for i in 0..5 {
        let mut rng = StdRng::seed_from_u64(i);
        let profile = generate_user_profile(&mut rng, test_key);
        let data = serialize_profile(&profile);
        engine.put(test_key, data);
        println!("  Updated {} (version {})", test_key, i);
    }
    
    // Read different versions
    for version in 0..3 {
        if let Some(_data) = engine.get_version(test_key, version) {
            println!("  Found version {} for {}", version, test_key);
        }
    }
    
    println!("\n🏁 Benchmark complete!");
    
    Ok(())
}