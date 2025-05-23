//! Memory-mapped persistent ZVerse test
//!
//! This example tests the persistent storage capabilities of ZVerse
//! using memory-mapped files.

use std::fs;
use std::path::Path;
use std::time::Instant;
use tempfile::tempdir;
use zverse::{ZVerseConfig, PersistentZVerse, Error};

fn main() -> Result<(), Error> {
    // Create temporary directory for test
    let temp_dir = tempdir().expect("Failed to create temp directory");
    let data_path = temp_dir.path().join("mmap_test");
    
    println!("=== Testing Memory-Mapped Persistent ZVerse ===");
    println!("Using directory: {:?}", data_path);
    
    // Create configuration
    let config = ZVerseConfig {
        data_path: data_path.to_string_lossy().to_string(),
        segment_size: 1 * 1024 * 1024, // 1MB segments
        max_entries_per_segment: 1000,
        sync_writes: true,
        cache_size_bytes: 10 * 1024 * 1024, // 10MB cache
        background_threads: 2,
    };
    
    println!("\n=== Creating Initial Database ===");
    
    // Create a new database
    let db = PersistentZVerse::new(config.clone())?;
    
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
    
    // Test retrieval
    let alice: Option<String> = db.get("user:1", None)?
        .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
    println!("Retrieved user:1: {:?}", alice);
    
    // Get original version
    let alice_original: Option<String> = db.get("user:1", Some(v1))?
        .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
    println!("Original user:1 (v{}): {:?}", v1, alice_original);
    
    // Test history
    println!("\nHistory of user:1:");
    let history = db.history("user:1", None, None)?;
    for result in history {
        let (version, value) = result?;
        println!("  v{} = {}", version, String::from_utf8_lossy(&value));
    }
    
    // Test scan
    println!("\nScanning all data:");
    let scan_results = db.scan::<&[u8]>(None, None, None)?;
    let mut count = 0;
    for result in scan_results {
        let (key, value) = result?;
        println!("  {} = {}", 
                 String::from_utf8_lossy(&key),
                 String::from_utf8_lossy(&value));
        count += 1;
    }
    println!("Total entries: {}", count);
    
    // Flush and close
    println!("\nFlushing and closing database...");
    db.flush()?;
    db.close()?;
    
    println!("\n=== Reopening Database to Test Persistence ===");
    
    // Reopen the database
    let db2 = PersistentZVerse::new(config.clone())?;
    
    // Verify data persisted
    let alice_reopened: Option<String> = db2.get("user:1", None)?
        .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
    println!("After reopening, user:1: {:?}", alice_reopened);
    
    // Test that we can still query historical versions
    let alice_v1_reopened: Option<String> = db2.get("user:1", Some(v1))?
        .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
    println!("After reopening, user:1 v{}: {:?}", v1, alice_v1_reopened);
    
    // Add more data to verify writes still work
    let v5 = db2.put("user:4", "David")?;
    println!("Added user:4 at version {}", v5);
    
    println!("\n=== Performance Test ===");
    
    // Insert performance test
    let start = Instant::now();
    for i in 0..100 {
        let key = format!("perf:key:{}", i);
        let value = format!("value-for-key-{}", i);
        db2.put(key, value)?;
    }
    let insert_duration = start.elapsed();
    println!("Inserted 100 items in {:?} ({:.2} items/sec)",
             insert_duration,
             100.0 / insert_duration.as_secs_f64());
    
    // Read performance test
    let start = Instant::now();
    for i in 0..100 {
        let key = format!("perf:key:{}", i);
        let _: Option<String> = db2.get(&key, None)?
            .map(|v: Vec<u8>| String::from_utf8(v).unwrap());
    }
    let read_duration = start.elapsed();
    println!("Read 100 items in {:?} ({:.2} items/sec)",
             read_duration,
             100.0 / read_duration.as_secs_f64());
    
    // Close the second database
    db2.close()?;
    
    println!("\n=== Test Complete ===");
    println!("All operations completed successfully!");
    println!("Data was persisted and successfully reopened.");
    
    Ok(())
}