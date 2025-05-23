//! Simple test for ZCurve-DB core functionality

use std::fs;
use zverse::ZCurveDB;

fn main() {
    println!("=== ZCurve-DB Simple Test ===");
    
    // Clean up any existing test database
    let _ = fs::remove_file("./simple_test.zcdb");
    
    // Create database
    let mut db = ZCurveDB::open("./simple_test.zcdb").expect("Failed to create database");
    
    // Test 1: Put and get a single value
    println!("1. Testing single put/get");
    db.put(b"test_key", b"test_value").expect("Put failed");
    let result = db.get(b"test_key").expect("Get failed").expect("Key not found");
    assert_eq!(result, b"test_value");
    println!("   ✅ Single put/get works");
    
    // Test 2: Put multiple values
    println!("2. Testing multiple puts");
    for i in 0..10 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
    }
    println!("   ✅ Multiple puts completed");
    
    // Test 3: Get all values back
    println!("3. Testing retrieval");
    for i in 0..10 {
        let key = format!("key_{}", i);
        let expected_value = format!("value_{}", i);
        let result = db.get(key.as_bytes()).expect("Get failed").expect("Key not found");
        assert_eq!(result, expected_value.as_bytes());
    }
    println!("   ✅ All values retrieved correctly");
    
    // Test 4: Check stats
    let stats = db.stats();
    println!("4. Database stats: {} entries, {} blocks", stats.total_entries, stats.total_blocks);
    assert!(stats.total_entries >= 11); // At least 11 entries (1 + 10)
    println!("   ✅ Stats look reasonable");
    
    // Test 5: Sync to disk
    db.sync().expect("Sync failed");
    println!("5. ✅ Sync to disk completed");
    
    // Clean up
    let _ = fs::remove_file("./simple_test.zcdb");
    
    println!("=== All Simple Tests Passed! ===");
}