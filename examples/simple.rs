//! Simple example demonstrating ZVerse functionality
//!
//! This example shows basic operations like put, get, scan, and history.

use std::time::Instant;
use zverse::{ZVerse, ZVerseConfig, Error};

fn main() -> Result<(), Error> {
    // Create a new ZVerse instance with default configuration
    let config = ZVerseConfig::default();
    let db = ZVerse::new(config)?;
    
    println!("=== Basic Operations ===");
    
    // Put some values
    let v1 = db.put("user:1", "Alice")?;
    let v2 = db.put("user:2", "Bob")?;
    let v3 = db.put("user:3", "Charlie")?;
    
    println!("Inserted 3 users at versions: {}, {}, {}", v1, v2, v3);
    
    // Update a value
    let v4 = db.put("user:1", "Alice Smith")?;
    println!("Updated user:1 at version {}", v4);
    
    // Get latest value
    let alice: Option<String> = db.get("user:1", None)?
        .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
    println!("Latest value for user:1: {:?}", alice);
    
    // Get specific version
    let alice_original: Option<String> = db.get("user:1", Some(v1))?
        .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
    println!("Original value for user:1 (v{}): {:?}", v1, alice_original);
    
    println!("\n=== Scan Operations ===");
    
    // Scan all keys at latest version
    println!("All users at latest version:");
    let users = db.scan::<&[u8]>(None, None, None)?;
    for result in users {
        let (key, value) = result?;
        println!("  {} = {}", 
                 String::from_utf8_lossy(&key),
                 String::from_utf8_lossy(&value));
    }
    
    // Scan a range
    println!("\nUsers 1-2 at latest version:");
    let users = db.scan(Some("user:1"), Some("user:3"), None)?;
    for result in users {
        let (key, value) = result?;
        println!("  {} = {}", 
                 String::from_utf8_lossy(&key),
                 String::from_utf8_lossy(&value));
    }
    
    println!("\n=== History Operations ===");
    
    // Get history of a key
    println!("History of user:1:");
    let history = db.history("user:1", None, None)?;
    for result in history {
        let (version, value) = result?;
        println!("  v{} = {}", version, String::from_utf8_lossy(&value));
    }
    
    println!("\n=== Snapshot Operations ===");
    
    // Insert more data to demonstrate snapshots
    db.put("post:1", "First post")?;
    let v_post2 = db.put("post:2", "Second post")?;
    db.put("post:3", "Third post")?;
    
    // Delete and update some data
    db.delete("post:2")?;
    db.put("user:2", "Robert")?;
    
    // Get a snapshot at an earlier version
    println!("Snapshot at version {}:", v_post2);
    let snapshot = db.snapshot(v_post2)?;
    for result in snapshot {
        let (key, value) = result?;
        println!("  {} = {}", 
                 String::from_utf8_lossy(&key),
                 String::from_utf8_lossy(&value));
    }
    
    println!("\n=== Performance Test ===");
    
    // Insert 10,000 items and measure time
    let start = Instant::now();
    for i in 0..10_000 {
        let key = format!("perf:key:{}", i);
        let value = format!("value-for-key-{}", i);
        db.put(key, value)?;
    }
    let insert_duration = start.elapsed();
    println!("Inserted 10,000 items in {:?} ({:.2} items/sec)",
             insert_duration,
             10_000.0 / insert_duration.as_secs_f64());
    
    // Random access
    let start = Instant::now();
    for i in (0..10_000).step_by(100) {
        let key = format!("perf:key:{}", i);
        let _: Option<String> = db.get(&key, None)?
            .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
    }
    let get_duration = start.elapsed();
    println!("Random access of 100 items in {:?} ({:.2} items/sec)",
             get_duration,
             100.0 / get_duration.as_secs_f64());
    
    Ok(())
}