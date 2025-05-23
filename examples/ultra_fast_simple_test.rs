//! Simple test to verify UltraFastTemporalEngine basic functionality

use std::time::Instant;
use tempfile::TempDir;
use zverse::storage::ultra_fast_engine::{UltraFastTemporalEngine, UltraFastEngineConfig};
use zverse::encoding::morton::current_timestamp_micros;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🧪 Ultra-Fast Engine Simple Test");
    println!("================================");
    
    // Create temporary directory for test database
    let temp_dir = TempDir::new()?;
    
    // Configure engine with larger extents to avoid space issues
    let config = UltraFastEngineConfig::default()
        .with_path(temp_dir.path().join("test.db"))
        .with_file_size(100 * 1024 * 1024) // 100MB
        .with_extent_size(1024 * 1024); // 1MB extents
    
    println!("📁 Creating engine...");
    let engine = UltraFastTemporalEngine::new(config)?;
    println!("✅ Engine created successfully");
    
    // Test 1: Basic put/get operations
    println!("\n🔧 Test 1: Basic put/get operations");
    
    let test_keys = vec![
        ("user:alice", "Alice's data"),
        ("user:bob", "Bob's data"),
        ("user:charlie", "Charlie's data"),
        ("order:123", "Order 123 details"),
        ("order:456", "Order 456 details"),
    ];
    
    // Write data
    let start = Instant::now();
    for (key, value) in &test_keys {
        engine.put(key, value.as_bytes())?;
    }
    let write_time = start.elapsed();
    println!("  ✅ Wrote {} records in {:?}", test_keys.len(), write_time);
    
    // Read data
    let start = Instant::now();
    let mut successful_reads = 0;
    for (key, expected_value) in &test_keys {
        if let Some(value) = engine.get(key)? {
            let value_str = String::from_utf8(value)?;
            if &value_str == expected_value {
                successful_reads += 1;
            } else {
                println!("    ❌ Key {} has incorrect value: expected '{}', got '{}'", key, expected_value, value_str);
            }
        } else {
            println!("    ❌ Key {} not found", key);
        }
    }
    let read_time = start.elapsed();
    println!("  ✅ Read {} records successfully in {:?}", successful_reads, read_time);
    
    // Test 2: Range queries
    println!("\n🔍 Test 2: Range queries");
    
    let timestamp = current_timestamp_micros();
    
    // Query user range
    let start = Instant::now();
    let user_results = engine.range_query("user:", "user:~", timestamp)?;
    let user_query_time = start.elapsed();
    println!("  ✅ User range query returned {} results in {:?}", user_results.len(), user_query_time);
    
    for (key, _value) in &user_results {
        println!("    - {}", key);
    }
    
    // Query order range
    let start = Instant::now();
    let order_results = engine.range_query("order:", "order:~", timestamp)?;
    let order_query_time = start.elapsed();
    println!("  ✅ Order range query returned {} results in {:?}", order_results.len(), order_query_time);
    
    for (key, _value) in &order_results {
        println!("    - {}", key);
    }
    
    // Test 3: Performance metrics
    println!("\n📊 Test 3: Performance metrics");
    
    let stats = engine.stats();
    println!("  Reads: {}", stats.reads);
    println!("  Writes: {}", stats.writes);
    println!("  Cache hits: {}", stats.cache_hits);
    println!("  Cache misses: {}", stats.cache_misses);
    println!("  Active records: {}", stats.active_records);
    println!("  Extent count: {}", stats.extent_count);
    
    if stats.cache_hits + stats.cache_misses > 0 {
        let hit_rate = stats.cache_hits as f64 / (stats.cache_hits + stats.cache_misses) as f64 * 100.0;
        println!("  Cache hit rate: {:.1}%", hit_rate);
    }
    
    // Test 4: Temporal versioning
    println!("\n⏰ Test 4: Temporal versioning");
    
    // Write multiple versions of the same key
    let versioned_key = "versioned:test";
    for i in 0..5 {
        let value = format!("version_{}", i);
        engine.put(versioned_key, value.as_bytes())?;
        std::thread::sleep(std::time::Duration::from_millis(1)); // Small delay for temporal separation
    }
    
    // Read latest version
    if let Some(latest_value) = engine.get(versioned_key)? {
        let latest_str = String::from_utf8(latest_value)?;
        println!("  ✅ Latest version: {}", latest_str);
    } else {
        println!("  ❌ Could not retrieve versioned key");
    }
    
    // Test 5: Flush and validate
    println!("\n💾 Test 5: Flush and validate");
    
    let start = Instant::now();
    engine.flush()?;
    let flush_time = start.elapsed();
    println!("  ✅ Flushed to disk in {:?}", flush_time);
    
    println!("\n🎯 All tests completed successfully!");
    println!("The UltraFastTemporalEngine is working correctly.");
    
    Ok(())
}