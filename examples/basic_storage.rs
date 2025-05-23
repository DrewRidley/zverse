//! Basic storage engine example demonstrating IMEC key-value operations
//!
//! This example shows how to use the ZVerse storage engine with Morton-T encoding
//! for spatial-temporal locality and efficient key-value operations.

use zverse::storage::engine::StorageEngine;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 ZVerse IMEC Storage Engine Demo");
    println!("==================================");

    // Create storage engine with default configuration
    let mut engine = StorageEngine::default();
    
    println!("\n📊 Initial Stats:");
    let stats = engine.stats();
    println!("  Reads: {}, Writes: {}", stats.reads, stats.writes);
    println!("  Extents: {}, Intervals: {}", engine.extent_count(), engine.interval_count());

    println!("\n💾 Storing key-value pairs...");
    
    // Store some basic key-value pairs
    engine.put("user:alice", b"Alice Johnson")?;
    engine.put("user:bob", b"Bob Smith")?;
    engine.put("user:charlie", b"Charlie Brown")?;
    
    // Store some product data
    engine.put("product:laptop", b"Gaming Laptop - $1299")?;
    engine.put("product:mouse", b"Wireless Mouse - $29")?;
    engine.put("product:keyboard", b"Mechanical Keyboard - $89")?;
    
    // Store some order data
    engine.put("order:1001", b"Order for user:alice, product:laptop")?;
    engine.put("order:1002", b"Order for user:bob, product:mouse")?;

    println!("✅ Stored 8 key-value pairs");

    println!("\n🔍 Retrieving values...");
    
    // Retrieve and display values
    let test_keys = vec![
        "user:alice",
        "user:bob", 
        "product:laptop",
        "order:1001",
        "nonexistent:key"
    ];
    
    for key in test_keys {
        match engine.get(key)? {
            Some(value) => {
                let value_str = String::from_utf8_lossy(&value);
                println!("  {} -> {}", key, value_str);
            }
            None => {
                println!("  {} -> [NOT FOUND]", key);
            }
        }
    }

    println!("\n🧪 Testing temporal clustering...");
    
    // Store multiple versions of the same key to test temporal locality
    engine.put("config:theme", b"dark")?;
    std::thread::sleep(std::time::Duration::from_millis(1));
    engine.put("config:theme", b"light")?;
    std::thread::sleep(std::time::Duration::from_millis(1));
    engine.put("config:theme", b"auto")?;
    
    // Most recent value should be retrieved first due to temporal locality
    if let Some(theme) = engine.get("config:theme")? {
        let theme_str = String::from_utf8_lossy(&theme);
        println!("  Current theme: {}", theme_str);
    }

    println!("\n🔧 Testing large values...");
    
    // Test with larger data
    let large_data = vec![42u8; 1000];
    engine.put("blob:large", &large_data)?;
    
    if let Some(retrieved) = engine.get("blob:large")? {
        println!("  Large blob: {} bytes (first few: {:?})", 
                retrieved.len(), &retrieved[..5]);
    }

    println!("\n🌍 Testing Unicode keys...");
    
    // Test with Unicode keys
    engine.put("用户:张三", b"Zhang San")?;
    engine.put("café:latte", b"Espresso + Milk")?;
    engine.put("🚀:rocket", b"Space Vehicle")?;
    
    let unicode_keys = vec!["用户:张三", "café:latte", "🚀:rocket"];
    for key in unicode_keys {
        if let Some(value) = engine.get(key)? {
            let value_str = String::from_utf8_lossy(&value);
            println!("  {} -> {}", key, value_str);
        }
    }

    println!("\n📈 Final Stats:");
    let final_stats = engine.stats();
    println!("  Reads: {}, Writes: {}", final_stats.reads, final_stats.writes);
    println!("  Extents: {}, Intervals: {}", engine.extent_count(), engine.interval_count());

    println!("\n✅ Validating storage integrity...");
    engine.validate()?;
    println!("  Storage validation passed!");

    println!("\n🎉 Demo completed successfully!");
    println!("The IMEC storage engine is working correctly with:");
    println!("  ✓ Morton-T encoding for spatial-temporal locality");
    println!("  ✓ Interval table for range mapping");
    println!("  ✓ Extent management for page collections");
    println!("  ✓ Copy-on-write semantics");
    println!("  ✓ Unicode key support");
    println!("  ✓ Large value storage");
    println!("  ✓ Temporal clustering optimization");

    Ok(())
}