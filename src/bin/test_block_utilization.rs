//! Simplified test for ZCurve-DB block utilization
//! 
//! Tests basic block packing functionality

use std::fs;
use zverse::ZCurveDB;

fn main() {
    println!("=== ZCurve-DB Block Utilization Test ===");
    
    // Clean up any existing test database
    let _ = fs::remove_file("./block_util_test.zcdb");
    
    test_basic_block_utilization();
    
    // Clean up
    let _ = fs::remove_file("./block_util_test.zcdb");
    
    println!("=== Block Utilization Tests Complete ===");
}

fn test_basic_block_utilization() {
    println!("1. Testing Basic Block Utilization");
    
    let mut db = ZCurveDB::open("./block_util_test.zcdb").expect("Failed to create database");
    
    // Add a few entries with small values
    for i in 0..10 {
        let key = format!("test_{:02}", i);
        let value = format!("val_{:02}", i);
        db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
    }
    
    let stats = db.stats();
    println!("   Added 10 entries: {} total blocks", stats.total_blocks);
    
    let entries_per_block = stats.total_entries as f64 / stats.total_blocks as f64;
    println!("   Average entries per block: {:.2}", entries_per_block);
    
    if entries_per_block > 2.0 {
        println!("   ✅ GOOD: Multiple entries per block");
    } else {
        println!("   ⚠️  BASIC: One entry per block (room for optimization)");
    }
    
    // Verify all entries are retrievable
    let mut retrieved = 0;
    for i in 0..10 {
        let key = format!("test_{:02}", i);
        if db.get(key.as_bytes()).unwrap().is_some() {
            retrieved += 1;
        }
    }
    
    println!("   Data integrity: {}/10 entries retrievable", retrieved);
    assert_eq!(retrieved, 10);
    println!("   ✅ All entries retrievable correctly");
    println!();
}