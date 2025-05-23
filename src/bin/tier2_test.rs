use std::sync::Arc;
use zverse::master_class::{
    MasterClassZVerse, MasterClassConfig, CompactionWorker, LockFreeQueue, 
    ZEntry, CompactionTask, InputSource, ZValueRange
};

fn main() {
    println!("=== Tier 2 Background Compaction Verification ===\n");
    
    test_lock_free_queue();
    test_compaction_worker_merge();
    test_integration_with_background_workers();
    
    println!("=== All Tier 2 Tests Passed! ===");
}

fn test_lock_free_queue() {
    println!("1. Testing Lock-Free Work Queue...");
    
    let queue: LockFreeQueue<i32> = LockFreeQueue::new();
    
    // Test basic operations
    assert!(queue.is_empty());
    
    // Enqueue items
    for i in 0..10 {
        queue.enqueue(i).expect("Enqueue failed");
    }
    
    // Dequeue items
    for expected in 0..10 {
        let item = queue.dequeue().expect("Should have item");
        assert_eq!(item, expected);
    }
    
    assert!(queue.is_empty());
    println!("   ✅ Lock-free queue operations work correctly");
}

fn test_compaction_worker_merge() {
    println!("2. Testing CompactionWorker K-way Merge...");
    
    let mut worker = CompactionWorker::new(0);
    
    // Create test entries with overlapping keys
    let mut entries = vec![
        ZEntry::new(b"key1", b"value1_v1", 1),
        ZEntry::new(b"key2", b"value2_v1", 2), 
        ZEntry::new(b"key1", b"value1_v2", 3), // Newer version of key1
        ZEntry::new(b"key3", b"value3_v1", 4),
        ZEntry::new(b"key2", b"value2_v2", 5), // Newer version of key2
    ];
    
    // Set different Z-values for proper ordering
    entries[0].z_value = 100;
    entries[1].z_value = 200;
    entries[2].z_value = 150; // Same key as entry[0] but newer version
    entries[3].z_value = 300;
    entries[4].z_value = 250; // Same key as entry[1] but newer version
    
    let merged = worker.merge_z_ordered(entries).expect("Merge failed");
    
    // Should have 3 unique keys after deduplication
    assert_eq!(merged.len(), 3);
    
    // Verify Z-order sorting
    for i in 1..merged.len() {
        assert!(merged[i].z_value >= merged[i-1].z_value);
    }
    
    // Verify newest versions are kept
    let key1_entry = merged.iter().find(|e| e.key.as_slice() == b"key1").unwrap();
    assert_eq!(key1_entry.version, 3);
    assert_eq!(key1_entry.value, b"value1_v2");
    
    let key2_entry = merged.iter().find(|e| e.key.as_slice() == b"key2").unwrap();
    assert_eq!(key2_entry.version, 5);
    assert_eq!(key2_entry.value, b"value2_v2");
    
    println!("   ✅ K-way merge with deduplication works correctly");
}

fn test_integration_with_background_workers() {
    println!("3. Testing Integration with Background Workers...");
    
    let config = MasterClassConfig::default();
    let db = MasterClassZVerse::new(config).expect("Failed to create database");
    
    // Write data to trigger compaction logic
    let mut versions = Vec::new();
    for i in 0..100 {
        let key = format!("integration-key-{:03}", i);
        let value = format!("integration-value-{:03}", i);
        let version = db.put(key.as_bytes(), value.as_bytes()).expect("Put failed");
        versions.push(version);
    }
    
    // Verify all writes succeeded and have unique versions
    assert_eq!(versions.len(), 100);
    versions.sort();
    for i in 1..versions.len() {
        assert!(versions[i] > versions[i-1], "Versions should be strictly increasing");
    }
    
    // Test read-back to verify data integrity
    for i in 0..100 {
        let key = format!("integration-key-{:03}", i);
        let expected_value = format!("integration-value-{:03}", i);
        
        let result = db.get(key.as_bytes(), None).expect("Get failed");
        assert_eq!(result, Some(expected_value.as_bytes().to_vec()));
    }
    
    // Test system responsiveness during load
    let stats = db.stats();
    assert!(stats.total_writes >= 100);
    assert!(stats.hot_tier_entries > 0);
    
    println!("   ✅ Integration with background workers successful");
    println!("   📊 Stats: {} total writes, {} hot tier entries", 
             stats.total_writes, stats.hot_tier_entries);
}