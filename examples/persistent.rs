//! In-memory ZVerse example with persistence simulation
//!
//! This example demonstrates the key concepts of ZVerse using the in-memory implementation.
//! While not truly persistent, it shows how the API would work with persistence.

use std::time::Instant;
use zverse::{ZVerseConfig, ZVerse, Error};

fn main() -> Result<(), Error> {
    // Create configuration
    let config = ZVerseConfig {
        data_path: "zverse_example".to_string(),
        segment_size: 1 * 1024 * 1024, // 1MB segments for demonstration
        max_entries_per_segment: 10000,
        sync_writes: true,             // Ensure durability
        cache_size_bytes: 10 * 1024 * 1024, // 10MB cache
        background_threads: 2,
    };
    
    println!("=== Creating Initial Database ===");
    
    // Create a new database
    let db = ZVerse::new(config.clone())?;
    
    // Insert some data
    println!("Inserting initial data...");
    
    let v1 = db.put("user:1", "Alice")?;
    let v2 = db.put("user:2", "Bob")?;
    let v3 = db.put("user:3", "Charlie")?;
    
    println!("Inserted users at versions: {}, {}, {}", v1, v2, v3);
    
    // Update a value
    let v4 = db.put("user:1", "Alice Smith")?;
    println!("Updated user:1 at version {}", v4);
    
    // Add some more data
    db.put("product:1", "Laptop")?;
    db.put("product:2", "Phone")?;
    db.put("product:3", "Tablet")?;
    
    // Flush to ensure all data is persisted
    db.flush()?;
    
    // Flush any pending changes (simulating persistence)
    println!("Flushing database...");
    db.flush()?;
    
    println!("\n=== Using Same Database (Persistence Simulation) ===");
    
    // Verify data persisted
    let alice: Option<String> = db.get("user:1", None)?
        .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
    println!("Retrieved user:1: {:?}", alice);
    
    // Get original version
    let alice_original: Option<String> = db.get("user:1", Some(v1))?
        .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
    println!("Original user:1 (v{}): {:?}", v1, alice_original);
    
    println!("\n=== Scan Operations ===");
    
    // Scan all users
    println!("All users:");
    let users = db.scan(Some("user:".as_bytes()), Some("user:".as_bytes()), None)?;
    for result in users {
        let (key, value) = result?;
        println!("  {} = {}", 
                 String::from_utf8_lossy(&key),
                 String::from_utf8_lossy(&value));
    }
    
    // Scan all products
    println!("\nAll products:");
    let products = db.scan(Some("product:".as_bytes()), Some("product;".as_bytes()), None)?;
    for result in products {
        let (key, value) = result?;
        println!("  {} = {}", 
                 String::from_utf8_lossy(&key),
                 String::from_utf8_lossy(&value));
    }
    
    println!("\n=== History and Versioning ===");
    
    // Get history of user:1
    println!("History of user:1:");
    let history = db.history("user:1", None, None)?;
    for result in history {
        let (version, value) = result?;
        println!("  v{} = {}", version, String::from_utf8_lossy(&value));
    }
    
    // Get snapshot at version v2
    println!("\nSnapshot at version {}:", v2);
    let snapshot = db.snapshot(v2)?;
    for result in snapshot {
        let (key, value) = result?;
        println!("  {} = {}", 
                 String::from_utf8_lossy(&key),
                 String::from_utf8_lossy(&value));
    }
    
    println!("\n=== Write Performance ===");
    
    // Insert 1,000 items and measure time
    let start = Instant::now();
    for i in 0..1_000 {
        let key = format!("perf:key:{}", i);
        let value = format!("value-for-key-{}", i);
        db.put(key, value)?;
    }
    let insert_duration = start.elapsed();
    println!("Inserted 1,000 items in {:?} ({:.2} items/sec)",
             insert_duration,
             1_000.0 / insert_duration.as_secs_f64());
    
    // Random access
    let start = Instant::now();
    for i in (0..1_000).step_by(10) {
        let key = format!("perf:key:{}", i);
        let _: Option<String> = db.get(&key, None)?
            .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
    }
    let get_duration = start.elapsed();
    println!("Random access of 100 items in {:?} ({:.2} items/sec)",
             get_duration,
             100.0 / get_duration.as_secs_f64());
    
    println!("\n=== Cleanup ===");
    
    // Close the database
    db.close()?;
    
    println!("In-memory database closed. In a real persistent implementation,");
    println!("the data would be saved to disk and could be reopened later.");
    
    Ok(())
}