//! Simple demonstration of ZVerse Morton Temporal Database
//!
//! This example shows basic usage of the high-performance temporal database
//! including single operations, temporal queries, and batch processing.

use zverse::{MortonTemporalDB, MortonResult};
use std::time::Instant;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 ZVerse Morton Temporal Database Demo");
    println!("=======================================");
    
    // Create a new temporal database
    let db = MortonTemporalDB::new();
    
    // Basic operations demo
    println!("\n📝 Basic Operations");
    demo_basic_operations(&db)?;
    
    // Temporal operations demo
    println!("\n⏰ Temporal Operations");
    demo_temporal_operations(&db)?;
    
    // Batch operations demo
    println!("\n📦 Batch Operations");
    demo_batch_operations(&db)?;
    
    // Performance demo
    println!("\n🚀 Performance Demo");
    demo_performance(&db)?;
    

    
    println!("\n✅ Demo completed successfully!");
    Ok(())
}

fn demo_basic_operations(db: &MortonTemporalDB) -> MortonResult<()> {
    // Insert some data
    db.put("user:alice", b"Alice Johnson".to_vec())?;
    db.put("user:bob", b"Bob Smith".to_vec())?;
    db.put("user:charlie", b"Charlie Brown".to_vec())?;
    
    // Retrieve data
    let alice_data = db.get("user:alice")?;
    let bob_data = db.get("user:bob")?;
    
    println!("  Stored users:");
    println!("    user:alice -> {}", String::from_utf8_lossy(&alice_data));
    println!("    user:bob -> {}", String::from_utf8_lossy(&bob_data));
    
    // Update data
    db.put("user:alice", b"Alice Johnson-Smith".to_vec())?;
    let updated_alice = db.get("user:alice")?;
    println!("    user:alice (updated) -> {}", String::from_utf8_lossy(&updated_alice));
    
    Ok(())
}

fn demo_temporal_operations(db: &MortonTemporalDB) -> MortonResult<()> {
    let key = "sensor:temperature";
    
    // Insert temperature readings at different times
    db.put_at_time(key, b"20.5C".to_vec(), 1000)?;
    db.put_at_time(key, b"21.2C".to_vec(), 2000)?;
    db.put_at_time(key, b"22.1C".to_vec(), 3000)?;
    db.put_at_time(key, b"23.4C".to_vec(), 4000)?;
    db.put_at_time(key, b"24.0C".to_vec(), 5000)?;
    
    println!("  Temperature readings over time:");
    
    // Query at different time points
    for query_time in [500, 1500, 2500, 3500, 4500, 5500] {
        match db.get_at_time(key, query_time) {
            Ok(temp) => {
                println!("    At time {}: {}", query_time, String::from_utf8_lossy(&temp));
            }
            Err(_) => {
                println!("    At time {}: No data", query_time);
            }
        }
    }
    
    // Get latest value
    let latest = db.get(key)?;
    println!("    Latest temperature: {}", String::from_utf8_lossy(&latest));
    
    Ok(())
}

fn demo_batch_operations(db: &MortonTemporalDB) -> MortonResult<()> {
    // Prepare batch data
    let batch_data = vec![
        ("product:1001".to_string(), b"Laptop Computer".to_vec()),
        ("product:1002".to_string(), b"Wireless Mouse".to_vec()),
        ("product:1003".to_string(), b"Mechanical Keyboard".to_vec()),
        ("product:1004".to_string(), b"4K Monitor".to_vec()),
        ("product:1005".to_string(), b"USB-C Hub".to_vec()),
    ];
    
    // Batch insert
    let inserted_count = db.put_temporal_batch(&batch_data);
    println!("  Batch inserted {} products", inserted_count);
    
    // Batch retrieve
    let keys: Vec<&str> = vec!["product:1001", "product:1003", "product:1005", "product:9999"];
    let results = db.get_batch(&keys);
    
    println!("  Batch retrieval results:");
    for (i, result) in results.iter().enumerate() {
        match result {
            Ok(data) => {
                println!("    {} -> {}", keys[i], String::from_utf8_lossy(data));
            }
            Err(_) => {
                println!("    {} -> Not found", keys[i]);
            }
        }
    }
    
    Ok(())
}

fn demo_performance(db: &MortonTemporalDB) -> MortonResult<()> {
    let num_operations = 10_000;
    
    // Write performance test
    let start = Instant::now();
    for i in 0..num_operations {
        let key = format!("perf:key:{}", i);
        let value = format!("Performance test value {}", i).into_bytes();
        db.put(&key, value)?;
    }
    let write_duration = start.elapsed();
    let write_ops_per_sec = num_operations as f64 / write_duration.as_secs_f64();
    
    println!("  Write performance: {:.0} ops/sec ({} ops in {:?})", 
             write_ops_per_sec, num_operations, write_duration);
    
    // Read performance test
    let start = Instant::now();
    for i in 0..num_operations {
        let key = format!("perf:key:{}", i);
        let _ = db.get(&key)?;
    }
    let read_duration = start.elapsed();
    let read_ops_per_sec = num_operations as f64 / read_duration.as_secs_f64();
    
    println!("  Read performance: {:.0} ops/sec ({} ops in {:?})", 
             read_ops_per_sec, num_operations, read_duration);
    
    // Batch performance test
    let batch_keys: Vec<String> = (0..1000).map(|i| format!("perf:key:{}", i)).collect();
    let batch_key_refs: Vec<&str> = batch_keys.iter().map(|s| s.as_str()).collect();
    
    let start = Instant::now();
    let batch_iterations = 100;
    for _ in 0..batch_iterations {
        let _ = db.get_batch(&batch_key_refs);
    }
    let batch_duration = start.elapsed();
    let batch_ops_per_sec = (batch_iterations * 1000) as f64 / batch_duration.as_secs_f64();
    
    println!("  Batch performance: {:.0} ops/sec ({} batch ops in {:?})", 
             batch_ops_per_sec, batch_iterations, batch_duration);
    
    Ok(())
}

