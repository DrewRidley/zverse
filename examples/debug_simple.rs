use zverse::revolutionary::RevolutionaryDB;
use std::time::{SystemTime, UNIX_EPOCH};

fn current_timestamp_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

fn calculate_morton_key(key: &str, timestamp: u64) -> u64 {
    // Simple hash for demo - in production use proper Morton encoding
    let mut hash = 2166136261u64;
    for byte in key.bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(16777619);
    }
    hash ^ timestamp
}

fn main() {
    println!("=== Debug Simple MVCC Test ===\n");
    
    let db = RevolutionaryDB::new();
    
    // Test 1: Put and get immediately
    println!("Test 1: Basic put/get");
    let put_timestamp = current_timestamp_micros();
    println!("Put timestamp: {}", put_timestamp);
    let put_morton = calculate_morton_key("test_key", put_timestamp);
    println!("Put Morton key: {}", put_morton);
    
    let result = db.put_at_time("test_key", b"test_value", put_timestamp);
    println!("Put result: {:?}", result);
    
    // Try to get with same timestamp
    let get_result = db.get_at_time("test_key", put_timestamp);
    let get_result_str = get_result.as_ref().map(|v| String::from_utf8_lossy(v).to_string());
    println!("Get with same timestamp: {:?}", get_result_str);
    
    // Try regular get (current timestamp)
    let get_timestamp = current_timestamp_micros();
    println!("Get timestamp: {}", get_timestamp);
    let get_morton = calculate_morton_key("test_key", get_timestamp);
    println!("Get Morton key: {}", get_morton);
    
    let regular_get = db.get("test_key");
    let regular_get_str = regular_get.as_ref().map(|v| String::from_utf8_lossy(v).to_string());
    println!("Regular get result: {:?}", regular_get_str);
    
    println!("\n--- Analysis ---");
    println!("Timestamp difference: {}", get_timestamp.saturating_sub(put_timestamp));
    println!("Morton keys match: {}", put_morton == get_morton);
    
    // Test 2: Put with current timestamp, get with current timestamp
    println!("\nTest 2: Both operations use current time");
    
    let shared_timestamp = current_timestamp_micros();
    println!("Shared timestamp: {}", shared_timestamp);
    
    db.put_at_time("test_key2", b"test_value2", shared_timestamp).unwrap();
    let result2 = db.get_at_time("test_key2", shared_timestamp);
    let result2_str = result2.as_ref().map(|v| String::from_utf8_lossy(v).to_string());
    println!("Result: {:?}", result2_str);
    
    // Test 3: Transaction operations with timestamp consistency
    println!("\nTest 3: Transaction operations");
    let mut txn = db.begin_transaction(false);
    let txn_timestamp = txn.read_timestamp;
    println!("Transaction timestamp: {}", txn_timestamp);
    
    // All operations in transaction should use the same timestamp
    db.put_txn(&mut txn, "txn_key1", b"txn_value1").unwrap();
    db.put_txn(&mut txn, "txn_key2", b"txn_value2").unwrap();
    
    // Verify we can read our own writes within transaction
    let txn_result1 = db.get_txn(&txn, "txn_key1");
    let txn_result1_str = txn_result1.as_ref().map(|v| String::from_utf8_lossy(v).to_string());
    println!("Transaction read own write 1: {:?}", txn_result1_str);
    
    let txn_result2 = db.get_txn(&txn, "txn_key2");
    let txn_result2_str = txn_result2.as_ref().map(|v| String::from_utf8_lossy(v).to_string());
    println!("Transaction read own write 2: {:?}", txn_result2_str);
    
    // Data should NOT be visible outside transaction before commit
    let outside_read1 = db.get("txn_key1");
    let outside_read2 = db.get("txn_key2");
    println!("Outside transaction before commit: key1={:?}, key2={:?}", 
             outside_read1.is_some(), outside_read2.is_some());
    
    // Commit transaction
    println!("Committing transaction...");
    db.commit_transaction(txn).unwrap();
    
    // Now data should be visible outside using the transaction's timestamp
    let after_commit1 = db.get_at_time("txn_key1", txn_timestamp);
    let after_commit2 = db.get_at_time("txn_key2", txn_timestamp);
    let after_commit1_str = after_commit1.as_ref().map(|v| String::from_utf8_lossy(v).to_string());
    let after_commit2_str = after_commit2.as_ref().map(|v| String::from_utf8_lossy(v).to_string());
    println!("After commit with transaction timestamp: key1={:?}, key2={:?}",
             after_commit1_str, after_commit2_str);
}