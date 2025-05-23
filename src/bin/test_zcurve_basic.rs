//! Basic test for ZCurve-DB implementation
//! 
//! Tests core functionality: put, get, versioning, and basic error handling

use std::fs;
use std::time::Instant;
use zverse::{ZCurveDB, ZCurveError};

fn main() {
    println!("=== ZCurve-DB Basic Functionality Test ===\n");
    
    // Clean up any existing test database
    let _ = fs::remove_file("./test_basic.zcdb");
    
    // Test 1: Create database and basic operations
    test_basic_operations();
    
    // Test 2: Test versioning
    test_versioning();
    
    // Test 3: Test persistence (reopen database)
    test_persistence();
    
    // Test 4: Test performance with many entries
    test_performance();
    
    // Test 5: Test error conditions
    test_error_conditions();
    
    // Clean up
    let _ = fs::remove_file("./test_basic.zcdb");
    
    println!("=== All ZCurve-DB Tests Completed Successfully! ===");
}

fn test_basic_operations() {
    println!("1. Testing Basic Operations");
    
    let mut db = ZCurveDB::open("./test_basic.zcdb").expect("Failed to create database");
    
    // Test initial state
    let stats = db.stats();
    println!("   Initial stats: {} entries, {} blocks", stats.total_entries, stats.total_blocks);
    assert_eq!(stats.total_entries, 0);
    
    // Test putting data
    db.put(b"key1", b"value1").expect("Put failed");
    db.put(b"key2", b"value2").expect("Put failed");
    db.put(b"key3", b"value3").expect("Put failed");
    
    println!("   Inserted 3 key-value pairs");
    
    // Test getting data
    let value1 = db.get(b"key1").expect("Get failed").expect("Key not found");
    let value2 = db.get(b"key2").expect("Get failed").expect("Key not found");
    let value3 = db.get(b"key3").expect("Get failed").expect("Key not found");
    
    assert_eq!(value1, b"value1");
    assert_eq!(value2, b"value2");
    assert_eq!(value3, b"value3");
    
    println!("   ✅ Retrieved all values correctly");
    
    // Test non-existent key
    let missing = db.get(b"missing").expect("Get failed");
    assert!(missing.is_none());
    println!("   ✅ Non-existent key returns None");
    
    // Check stats after operations
    let stats = db.stats();
    println!("   Final stats: {} entries, {} blocks", stats.total_entries, stats.total_blocks);
    assert_eq!(stats.total_entries, 3);
    
    println!("   ✅ Basic operations test passed\n");
}

fn test_versioning() {
    println!("2. Testing Versioning");
    
    let mut db = ZCurveDB::open("./test_versioning.zcdb").expect("Failed to create database");
    
    // Insert initial value
    db.put(b"versioned_key", b"version1").expect("Put failed");
    let version1 = db.stats().current_version;
    
    // Update the same key
    db.put(b"versioned_key", b"version2").expect("Put failed");
    let version2 = db.stats().current_version;
    
    // Update again
    db.put(b"versioned_key", b"version3").expect("Put failed");
    let version3 = db.stats().current_version;
    
    println!("   Created 3 versions: {}, {}, {}", version1, version2, version3);
    
    // Test getting specific versions
    let val_v1 = db.get_version(b"versioned_key", version1).expect("Get failed").expect("Version not found");
    let val_v2 = db.get_version(b"versioned_key", version2).expect("Get failed").expect("Version not found");
    let val_v3 = db.get_version(b"versioned_key", version3).expect("Get failed").expect("Version not found");
    
    assert_eq!(val_v1, b"version1");
    assert_eq!(val_v2, b"version2");
    assert_eq!(val_v3, b"version3");
    
    println!("   ✅ All versions retrieved correctly");
    
    // Test getting latest version
    let latest = db.get(b"versioned_key").expect("Get failed").expect("Key not found");
    assert_eq!(latest, b"version3");
    
    println!("   ✅ Latest version retrieved correctly");
    
    // Clean up
    let _ = fs::remove_file("./test_versioning.zcdb");
    
    println!("   ✅ Versioning test passed\n");
}

fn test_persistence() {
    println!("3. Testing Persistence");
    
    // Create database and add data
    {
        let mut db = ZCurveDB::open("./test_persistence.zcdb").expect("Failed to create database");
        
        db.put(b"persistent1", b"data1").expect("Put failed");
        db.put(b"persistent2", b"data2").expect("Put failed");
        db.put(b"persistent3", b"data3").expect("Put failed");
        
        db.sync().expect("Sync failed");
        println!("   Wrote 3 entries and synced to disk");
    } // Database goes out of scope and closes
    
    // Reopen database and verify data
    {
        let db = ZCurveDB::open("./test_persistence.zcdb").expect("Failed to reopen database");
        
        let stats = db.stats();
        println!("   Reopened database: {} entries, {} blocks", stats.total_entries, stats.total_blocks);
        
        let value1 = db.get(b"persistent1").expect("Get failed").expect("Key not found");
        let value2 = db.get(b"persistent2").expect("Get failed").expect("Key not found");
        let value3 = db.get(b"persistent3").expect("Get failed").expect("Key not found");
        
        assert_eq!(value1, b"data1");
        assert_eq!(value2, b"data2");
        assert_eq!(value3, b"data3");
        
        println!("   ✅ All data persisted correctly");
    }
    
    // Clean up
    let _ = fs::remove_file("./test_persistence.zcdb");
    
    println!("   ✅ Persistence test passed\n");
}

fn test_performance() {
    println!("4. Testing Performance");
    
    let mut db = ZCurveDB::open("./test_performance.zcdb").expect("Failed to create database");
    
    let num_entries = 1000;
    let value_size = 100; // 100-byte values
    let test_value = vec![0xAB; value_size];
    
    // Test write performance
    println!("   Inserting {} entries with {}B values...", num_entries, value_size);
    let start = Instant::now();
    
    for i in 0..num_entries {
        let key = format!("perf_key_{:06}", i);
        db.put(key.as_bytes(), &test_value).expect("Put failed");
        
        if i % 100 == 0 && i > 0 {
            let elapsed = start.elapsed();
            let rate = i as f64 / elapsed.as_secs_f64();
            println!("     Progress: {}/{} entries ({:.0} ops/sec)", i, num_entries, rate);
        }
    }
    
    let write_elapsed = start.elapsed();
    let write_rate = num_entries as f64 / write_elapsed.as_secs_f64();
    println!("   Write performance: {:.0} ops/sec", write_rate);
    
    // Test read performance
    println!("   Reading {} entries...", num_entries);
    let start = Instant::now();
    let mut successful_reads = 0;
    
    for i in 0..num_entries {
        let key = format!("perf_key_{:06}", i);
        if let Ok(Some(value)) = db.get(key.as_bytes()) {
            successful_reads += 1;
            assert_eq!(value.len(), value_size);
        }
    }
    
    let read_elapsed = start.elapsed();
    let read_rate = successful_reads as f64 / read_elapsed.as_secs_f64();
    println!("   Read performance: {:.0} ops/sec ({} successful)", read_rate, successful_reads);
    
    // Check final stats
    let stats = db.stats();
    println!("   Final stats: {} entries, {} blocks, {:.1}MB file size", 
             stats.total_entries, stats.total_blocks, stats.file_size as f64 / (1024.0 * 1024.0));
    
    if write_rate > 1000.0 && read_rate > 5000.0 {
        println!("   ✅ Good performance achieved");
    } else {
        println!("   ⚠️  Performance could be improved");
    }
    
    // Clean up
    let _ = fs::remove_file("./test_performance.zcdb");
    
    println!("   ✅ Performance test completed\n");
}

fn test_error_conditions() {
    println!("5. Testing Error Conditions");
    
    let mut db = ZCurveDB::open("./test_errors.zcdb").expect("Failed to create database");
    
    // Test very large key (should fail)
    let large_key = vec![0; 70000]; // Larger than u16::MAX
    match db.put(&large_key, b"value") {
        Err(ZCurveError::KeyTooLarge) => {
            println!("   ✅ Large key properly rejected");
        }
        _ => {
            panic!("Expected KeyTooLarge error");
        }
    }
    
    // Test very large value (should fail)
    let large_value = vec![0; 70000]; // Larger than u16::MAX
    match db.put(b"key", &large_value) {
        Err(ZCurveError::ValueTooLarge) => {
            println!("   ✅ Large value properly rejected");
        }
        _ => {
            panic!("Expected ValueTooLarge error");
        }
    }
    
    // Test opening invalid file
    {
        use std::fs::File;
        use std::io::Write;
        
        let mut invalid_file = File::create("./invalid.zcdb").expect("Failed to create file");
        invalid_file.write_all(b"invalid data").expect("Write failed");
    }
    
    match ZCurveDB::open("./invalid.zcdb") {
        Err(ZCurveError::InvalidMagic) => {
            println!("   ✅ Invalid file properly rejected");
        }
        _ => {
            panic!("Expected InvalidMagic error");
        }
    }
    
    // Clean up
    let _ = fs::remove_file("./test_errors.zcdb");
    let _ = fs::remove_file("./invalid.zcdb");
    
    println!("   ✅ Error handling test passed\n");
}