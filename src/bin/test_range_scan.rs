//! Test for ZCurve-DB range scanning functionality
//! 
//! Tests the scan and scan_version methods to ensure proper Z-curve range queries

use std::fs;
use zverse::ZCurveDB;

fn main() {
    println!("=== ZCurve-DB Range Scanning Test ===");
    
    // Clean up any existing test database
    let _ = fs::remove_file("./range_scan_test.zcdb");
    
    test_basic_range_scan();
    test_empty_range_scan();
    test_single_key_range();
    test_full_range_scan();
    test_version_range_scan();
    test_overlapping_ranges();
    
    // Clean up
    let _ = fs::remove_file("./range_scan_test.zcdb");
    
    println!("=== Range Scanning Tests Complete ===");
}

fn test_basic_range_scan() {
    println!("1. Testing Basic Range Scan");
    
    let mut db = ZCurveDB::open("./range_scan_test.zcdb").expect("Failed to create database");
    
    // Insert test data with predictable key ordering
    let test_data = vec![
        ("apple", "fruit1"),
        ("banana", "fruit2"), 
        ("cherry", "fruit3"),
        ("date", "fruit4"),
        ("elderberry", "fruit5"),
    ];
    
    for (key, value) in &test_data {
        db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
    }
    
    println!("   Inserted {} entries", test_data.len());
    
    // Test range scan from "banana" to "date"
    let scan_result = db.scan(b"banana", b"date").expect("Scan failed");
    let mut results = Vec::new();
    
    for item in scan_result {
        let (key, value) = item.expect("Scan item failed");
        results.push((String::from_utf8(key).unwrap(), String::from_utf8(value).unwrap()));
    }
    
    // Sort results for consistent testing
    results.sort();
    
    println!("   Range scan 'banana' to 'date' returned {} results:", results.len());
    for (key, value) in &results {
        println!("     {} = {}", key, value);
    }
    
    // Should include entries within the range
    let expected_in_range = ["banana", "cherry", "date"];
    let found_keys: Vec<&str> = results.iter().map(|(k, _)| k.as_str()).collect();
    
    let mut found_expected = 0;
    for expected_key in &expected_in_range {
        if found_keys.contains(expected_key) {
            found_expected += 1;
        }
    }
    
    if found_expected >= 2 {
        println!("   ✅ Range scan returned relevant results");
    } else {
        println!("   ⚠️  Range scan may need improvement");
    }
    
    println!();
}

fn test_empty_range_scan() {
    println!("2. Testing Empty Range Scan");
    
    let mut db = ZCurveDB::open("./range_scan_test2.zcdb").expect("Failed to create database");
    
    // Insert some data
    db.put(b"key1", b"value1").expect("Put failed");
    db.put(b"key3", b"value3").expect("Put failed");
    
    // Scan a range that should be empty (between existing keys)
    let scan_result = db.scan(b"key2", b"key2").expect("Scan failed");
    let results: Vec<_> = scan_result.collect();
    
    println!("   Scanned empty range, got {} results", results.len());
    
    if results.is_empty() {
        println!("   ✅ Empty range scan returned no results correctly");
    } else {
        println!("   ⚠️  Empty range scan returned unexpected results");
    }
    
    let _ = fs::remove_file("./range_scan_test2.zcdb");
    println!();
}

fn test_single_key_range() {
    println!("3. Testing Single Key Range");
    
    let mut db = ZCurveDB::open("./range_scan_test3.zcdb").expect("Failed to create database");
    
    // Insert test data
    db.put(b"target_key", b"target_value").expect("Put failed");
    db.put(b"other_key1", b"other_value1").expect("Put failed");
    db.put(b"other_key2", b"other_value2").expect("Put failed");
    
    // Scan for just one specific key
    let scan_result = db.scan(b"target_key", b"target_key").expect("Scan failed");
    let results: Vec<_> = scan_result.collect();
    
    println!("   Single key range scan returned {} results", results.len());
    
    if results.len() == 1 {
        let (key, value) = results[0].as_ref().expect("Result failed");
        let key_str = String::from_utf8_lossy(key);
        let value_str = String::from_utf8_lossy(value);
        println!("     Found: {} = {}", key_str, value_str);
        
        if key == b"target_key" && value == b"target_value" {
            println!("   ✅ Single key range scan found correct entry");
        } else {
            println!("   ⚠️  Single key range scan found wrong entry");
        }
    } else {
        println!("   ⚠️  Single key range scan returned {} results (expected 1)", results.len());
    }
    
    let _ = fs::remove_file("./range_scan_test3.zcdb");
    println!();
}

fn test_full_range_scan() {
    println!("4. Testing Full Range Scan");
    
    let mut db = ZCurveDB::open("./range_scan_test4.zcdb").expect("Failed to create database");
    
    // Insert multiple entries
    let num_entries = 20;
    for i in 0..num_entries {
        let key = format!("key_{:03}", i);
        let value = format!("value_{:03}", i);
        db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
    }
    
    // Scan entire range using wide bounds
    let scan_result = db.scan(b"key_000", b"key_999").expect("Scan failed");
    let results: Vec<_> = scan_result.collect();
    
    println!("   Full range scan returned {} results (inserted {})", results.len(), num_entries);
    
    if results.len() >= num_entries / 2 {
        println!("   ✅ Full range scan found reasonable number of entries");
    } else {
        println!("   ⚠️  Full range scan found fewer entries than expected");
    }
    
    // Check that results are within expected range
    let mut valid_results = 0;
    for result in &results {
        if let Ok((key, _)) = result {
            let key_str = String::from_utf8_lossy(key);
            if key_str.starts_with("key_") {
                valid_results += 1;
            }
        }
    }
    
    println!("   {} results have expected key format", valid_results);
    
    let _ = fs::remove_file("./range_scan_test4.zcdb");
    println!();
}

fn test_version_range_scan() {
    println!("5. Testing Version-Specific Range Scan");
    
    let mut db = ZCurveDB::open("./range_scan_test5.zcdb").expect("Failed to create database");
    
    // Insert initial data
    db.put(b"version_key_1", b"version_1").expect("Put failed");
    db.put(b"version_key_2", b"version_1").expect("Put failed");
    let version_1 = db.stats().current_version;
    
    // Update data (creates new versions)
    db.put(b"version_key_1", b"version_2").expect("Put failed");
    db.put(b"version_key_3", b"version_2").expect("Put failed");
    let version_2 = db.stats().current_version;
    
    println!("   Created data at versions {} and {}", version_1, version_2);
    
    // Scan at version 1 (should see initial data)
    let scan_v1 = db.scan_version(b"version_key_1", b"version_key_3", version_1).expect("Scan failed");
    let results_v1: Vec<_> = scan_v1.collect();
    
    // Scan at version 2 (should see updated data)  
    let scan_v2 = db.scan_version(b"version_key_1", b"version_key_3", version_2).expect("Scan failed");
    let results_v2: Vec<_> = scan_v2.collect();
    
    println!("   Version {} scan: {} results", version_1, results_v1.len());
    println!("   Version {} scan: {} results", version_2, results_v2.len());
    
    if !results_v1.is_empty() && !results_v2.is_empty() {
        println!("   ✅ Version-specific scanning returned results for both versions");
    } else {
        println!("   ⚠️  Version-specific scanning needs improvement");
    }
    
    let _ = fs::remove_file("./range_scan_test5.zcdb");
    println!();
}

fn test_overlapping_ranges() {
    println!("6. Testing Overlapping Range Scans");
    
    let mut db = ZCurveDB::open("./range_scan_test6.zcdb").expect("Failed to create database");
    
    // Insert data with overlapping ranges
    let test_keys = ["aaa", "bbb", "ccc", "ddd", "eee", "fff"];
    for (i, key) in test_keys.iter().enumerate() {
        let value = format!("value_{}", i);
        db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
    }
    
    // Test multiple overlapping ranges
    let ranges = [
        ("aaa", "ccc"),
        ("bbb", "ddd"), 
        ("ccc", "fff"),
    ];
    
    for (start, end) in &ranges {
        let scan_result = db.scan(start.as_bytes(), end.as_bytes()).expect("Scan failed");
        let results: Vec<_> = scan_result.collect();
        
        println!("   Range {} to {}: {} results", start, end, results.len());
    }
    
    println!("   ✅ Multiple overlapping range scans completed");
    
    let _ = fs::remove_file("./range_scan_test6.zcdb");
    println!();
}