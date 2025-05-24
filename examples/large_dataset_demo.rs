//! Large Dataset Demo - 30GB+ Temporal Database Storage
//!
//! This example demonstrates ZVerse's ability to handle datasets much larger than available RAM
//! using memory-mapped storage with temporal locality optimization.
//!
//! Usage:
//!   cargo run --example large_dataset_demo --release
//!
//! This will create approximately 30GB of temporal data across multiple partitions,
//! then demonstrate retrieval of historical records.

use std::env;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::path::PathBuf;
use std::fs;

use zverse::disk::{DiskConfig, MmapPartition};
use zverse::encoding::morton::morton_t_encode;

const DEFAULT_TARGET_SIZE_GB: usize = 30;
const RECORDS_PER_PARTITION: usize = 1_000_000;
const PARTITIONS_COUNT: usize = 128;
const PROGRESS_INTERVAL: usize = 50_000;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 ZVerse Large Dataset Demo - Temporal Database at Scale");
    println!("=========================================================");
    
    let args: Vec<String> = env::args().collect();
    let target_gb = if args.len() > 1 {
        args[1].parse().unwrap_or(DEFAULT_TARGET_SIZE_GB)
    } else {
        DEFAULT_TARGET_SIZE_GB
    };
    
    println!("📊 Target dataset size: ~{}GB", target_gb);
    println!("📁 Partitions: {}", PARTITIONS_COUNT);
    println!("📝 Records per partition: {}", RECORDS_PER_PARTITION);
    
    // Setup data directory
    let data_dir = PathBuf::from("./large_dataset_demo");
    let _ = fs::remove_dir_all(&data_dir); // Clean start
    fs::create_dir_all(&data_dir)?;
    
    println!("🏗️  Created data directory: {}", data_dir.display());
    
    // Configure for large dataset handling
    let config = Arc::new(DiskConfig::high_performance());
    println!("⚙️  Using high-performance configuration");
    
    // Fixed timestamp base to ensure consistent key generation
    let base_timestamp = get_current_timestamp() - (365 * 24 * 3600 * 1_000_000); // 1 year ago
    let time_range = 365 * 24 * 3600 * 1_000_000; // 1 year in microseconds
    
    // Phase 1: Data Generation and Insertion
    println!("\n📥 Phase 1: Generating and inserting temporal data...");
    let insert_start = Instant::now();
    
    let mut partitions = Vec::new();
    let mut total_records = 0;
    let mut total_bytes = 0;
    
    for partition_id in 0..PARTITIONS_COUNT {
        let partition_path = data_dir.join(format!("partition_{:04}.dat", partition_id));
        let partition = MmapPartition::create(partition_id as u32, partition_path, config.clone())?;
        
        // Generate temporal data for this partition using fixed timestamps
        
        for record_id in 0..RECORDS_PER_PARTITION {
            // Create realistic temporal patterns
            let timestamp = base_timestamp + (record_id as u64 * time_range / RECORDS_PER_PARTITION as u64);
            
            // Generate realistic keys (user IDs, session IDs, etc.)
            let key_type = record_id % 4;
            let key = match key_type {
                0 => format!("user:{}", record_id % 100_000),
                1 => format!("session:{}:{}", partition_id, record_id % 10_000),
                2 => format!("order:{}:{}", (base_timestamp + (record_id as u64 * time_range / RECORDS_PER_PARTITION as u64)) / 1_000_000, record_id),
                3 => format!("event:{}:{}", partition_id, record_id),
                _ => unreachable!(),
            };
            
            // Generate realistic value data (JSON-like structures)
            let value = match key_type {
                0 => format!(
                    "{{\"user_id\":{},\"balance\":{},\"last_login\":{},\"tier\":\"{}\"}}",
                    record_id % 100_000,
                    (record_id % 10000) + 1000,
                    timestamp,
                    if record_id % 10 == 0 { "premium" } else { "standard" }
                ),
                1 => format!(
                    "{{\"session_id\":\"{}\",\"duration\":{},\"pages_viewed\":{},\"ip\":\"192.168.{}.{}\"}}",
                    record_id,
                    (record_id % 3600) + 60,
                    (record_id % 50) + 1,
                    (record_id % 255) + 1,
                    (record_id % 255) + 1
                ),
                2 => format!(
                    "{{\"order_id\":{},\"amount\":{:.2},\"items\":{},\"status\":\"{}\"}}",
                    record_id,
                    ((record_id % 10000) as f64) / 100.0 + 10.0,
                    (record_id % 20) + 1,
                    if record_id % 5 == 0 { "completed" } else { "pending" }
                ),
                3 => format!(
                    "{{\"event_type\":\"{}\",\"severity\":\"{}\",\"message\":\"Event {} in partition {}\"}}",
                    match record_id % 3 { 0 => "info", 1 => "warning", 2 => "error", _ => "info" },
                    match record_id % 4 { 0 => "low", 1 => "medium", 2 => "high", 3 => "critical", _ => "low" },
                    record_id,
                    partition_id
                ),
                _ => unreachable!(),
            };
            
            // Encode using Morton space-time encoding for temporal locality
            let morton_code = morton_t_encode(&key, timestamp);
            let morton_key = morton_code.value();
            
            // Insert the record
            partition.insert_record(morton_key, key.as_bytes(), value.as_bytes())?;
            
            total_records += 1;
            total_bytes += key.len() + value.len() + 16; // Approximate overhead
            
            // Progress reporting
            if record_id % PROGRESS_INTERVAL == 0 && record_id > 0 {
                let elapsed = insert_start.elapsed();
                let records_per_sec = total_records as f64 / elapsed.as_secs_f64();
                let gb_written = total_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
                
                print!("\r📝 Partition {}/{}: {} records, {:.2}GB, {:.0} records/sec", 
                       partition_id + 1, PARTITIONS_COUNT, total_records, gb_written, records_per_sec);
                std::io::Write::flush(&mut std::io::stdout()).unwrap();
            }
        }
        
        // Sync partition to disk
        partition.sync(false)?;
        partitions.push(partition);
        
        let elapsed = insert_start.elapsed();
        let gb_written = total_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
        println!("\n✅ Partition {} complete: {:.2}GB written in {:.2}s", 
                 partition_id, gb_written, elapsed.as_secs_f64());
    }
    
    let insert_duration = insert_start.elapsed();
    let final_gb = total_bytes as f64 / (1024.0 * 1024.0 * 1024.0);
    
    println!("\n🎉 Phase 1 Complete!");
    println!("📊 Total records inserted: {}", total_records);
    println!("💾 Total data size: {:.2}GB", final_gb);
    println!("⏱️  Total time: {:.2}s", insert_duration.as_secs_f64());
    println!("🚀 Average throughput: {:.0} records/sec", total_records as f64 / insert_duration.as_secs_f64());
    println!("📈 Average write speed: {:.2}MB/s", 
             (total_bytes as f64 / (1024.0 * 1024.0)) / insert_duration.as_secs_f64());
    
    // Phase 2: Random Historical Queries
    println!("\n🔍 Phase 2: Testing random historical queries...");
    let query_start = Instant::now();
    
    let num_queries = 10_000;
    let mut successful_queries = 0;
    let mut query_times = Vec::new();
    
    for query_id in 0..num_queries {
        // Pick a random partition and record
        let partition_idx = query_id % partitions.len();
        let record_id = (query_id * 7) % RECORDS_PER_PARTITION; // Pseudo-random
        
        // Reconstruct the key we're looking for (using SAME timestamp logic as write phase)
        let key_type = record_id % 4;
        let timestamp = base_timestamp + (record_id as u64 * time_range / RECORDS_PER_PARTITION as u64);
        let key = match key_type {
            0 => format!("user:{}", record_id % 100_000),
            1 => format!("session:{}:{}", partition_idx, record_id % 10_000),
            2 => format!("order:{}:{}", timestamp / 1_000_000, record_id),
            3 => format!("event:{}:{}", partition_idx, record_id),
            _ => unreachable!(),
        };
        let morton_code = morton_t_encode(&key, timestamp);
        let morton_key = morton_code.value();
        
        // Perform the query
        let query_start_time = Instant::now();
        let result = partitions[partition_idx].get_record(morton_key);
        let query_duration = query_start_time.elapsed();
        
        query_times.push(query_duration);
        
        if result.is_ok() && result.unwrap().is_some() {
            successful_queries += 1;
        }
        
        // Progress reporting
        if query_id % 1000 == 0 && query_id > 0 {
            let avg_query_time = query_times.iter().sum::<Duration>() / query_times.len() as u32;
            print!("\r🔍 Queries: {}/{}, Success: {:.1}%, Avg: {:.2}ms", 
                   query_id, num_queries, 
                   (successful_queries as f64 / query_id as f64) * 100.0,
                   avg_query_time.as_secs_f64() * 1000.0);
            std::io::Write::flush(&mut std::io::stdout()).unwrap();
        }
    }
    
    let query_duration = query_start.elapsed();
    let avg_query_time = query_times.iter().sum::<Duration>() / query_times.len() as u32;
    query_times.sort();
    let p95_query_time = query_times[(query_times.len() as f64 * 0.95) as usize];
    let p99_query_time = query_times[(query_times.len() as f64 * 0.99) as usize];
    
    println!("\n🎯 Phase 2 Complete!");
    println!("🔍 Total queries: {}", num_queries);
    println!("✅ Successful queries: {} ({:.1}%)", successful_queries, 
             (successful_queries as f64 / num_queries as f64) * 100.0);
    println!("⏱️  Total query time: {:.2}s", query_duration.as_secs_f64());
    println!("🚀 Average throughput: {:.0} queries/sec", num_queries as f64 / query_duration.as_secs_f64());
    println!("📊 Query latency stats:");
    println!("   Average: {:.2}ms", avg_query_time.as_secs_f64() * 1000.0);
    println!("   P95: {:.2}ms", p95_query_time.as_secs_f64() * 1000.0);
    println!("   P99: {:.2}ms", p99_query_time.as_secs_f64() * 1000.0);
    
    // Phase 3: Memory Usage Analysis
    println!("\n💾 Phase 3: Memory usage analysis...");
    
    // Get disk usage
    let mut total_disk_usage = 0;
    for entry in fs::read_dir(&data_dir)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        total_disk_usage += metadata.len();
    }
    
    println!("📁 Disk usage: {:.2}GB", total_disk_usage as f64 / (1024.0 * 1024.0 * 1024.0));
    println!("🗂️  Files created: {}", fs::read_dir(&data_dir)?.count());
    
    // Cleanup option
    println!("\n🧹 Cleanup: Delete test data? (y/N)");
    let mut input = String::new();
    std::io::stdin().read_line(&mut input)?;
    
    if input.trim().to_lowercase() == "y" {
        fs::remove_dir_all(&data_dir)?;
        println!("✅ Test data cleaned up");
    } else {
        println!("📁 Test data preserved in: {}", data_dir.display());
    }
    
    println!("\n🎉 Large Dataset Demo Complete!");
    println!("📊 Successfully demonstrated handling {:.2}GB of temporal data", final_gb);
    println!("🚀 Peak performance: {:.0} records/sec", total_records as f64 / insert_duration.as_secs_f64());
    println!("🎯 Query success rate: {:.1}%", (successful_queries as f64 / num_queries as f64) * 100.0);
    
    Ok(())
}

fn get_current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

fn calculate_simple_hash(key: &str) -> u32 {
    // Simple FNV-1a hash for demonstration
    let mut hash = 2166136261u32;
    for byte in key.bytes() {
        hash ^= byte as u32;
        hash = hash.wrapping_mul(16777619);
    }
    hash
}