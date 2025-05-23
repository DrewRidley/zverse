use std::time::{Duration, Instant};
use zverse::{ZVerseEngine, init_timestamp};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the timestamp system
    init_timestamp();

    // Create a database file on disk for demonstration (not in memory-backed /tmp)
    let db_file = std::path::Path::new("./zverse_perf_test.db");

    // Remove existing file for clean test
    std::fs::remove_file(db_file).ok();

    let engine = ZVerseEngine::open(db_file)?;

    println!("ZVerse Performance Test");
    println!("======================");

    // Test 1: Bulk Insert Performance
    println!("\n1. Bulk Insert Performance Test");
    let insert_count = 500_000;
    let start = Instant::now();

    for i in 0..insert_count {
        let key = format!("user:{:08}", i);
        let value = format!(
            "User data for user {}, created at timestamp {}",
            i,
            i * 1000
        );
        engine.put(key.as_bytes(), value.as_bytes())?;

        if i % 10_000 == 0 && i > 0 {
            let elapsed = start.elapsed();
            let ops_per_sec = i as f64 / elapsed.as_secs_f64();
            println!("  {} records inserted, {:.0} ops/sec", i, ops_per_sec);
        }
    }

    let insert_duration = start.elapsed();
    let insert_ops_per_sec = insert_count as f64 / insert_duration.as_secs_f64();

    println!(
        "  ✅ Inserted {} records in {:?}",
        insert_count, insert_duration
    );
    println!("  📊 Insert rate: {:.0} ops/sec", insert_ops_per_sec);
    println!(
        "  📊 Average insert latency: {:.2}μs",
        insert_duration.as_micros() as f64 / insert_count as f64
    );

    // Test 2: Point Lookup Performance
    println!("\n2. Point Lookup Performance Test");
    let lookup_count = 10_000;
    let mut found_count = 0;

    let start = Instant::now();
    for i in 0..lookup_count {
        let key = format!("user:{:08}", i * 10); // Sample every 10th user
        if engine.get(key.as_bytes())?.is_some() {
            found_count += 1;
        }
    }
    let lookup_duration = start.elapsed();
    let lookup_ops_per_sec = lookup_count as f64 / lookup_duration.as_secs_f64();

    println!(
        "  ✅ Performed {} lookups in {:?}",
        lookup_count, lookup_duration
    );
    println!("  📊 Found {} records", found_count);
    println!("  📊 Lookup rate: {:.0} ops/sec", lookup_ops_per_sec);
    println!(
        "  📊 Average lookup latency: {:.2}μs",
        lookup_duration.as_micros() as f64 / lookup_count as f64
    );

    // Test 3: Range Scan Performance
    println!("\n3. Range Scan Performance Test");
    let start = Instant::now();

    let range_results = engine.range(b"user:00010000", b"user:00020000")?;
    let range_duration = start.elapsed();

    println!("  ✅ Range scan completed in {:?}", range_duration);
    println!("  📊 Found {} records in range", range_results.len());
    println!(
        "  📊 Scan rate: {:.0} records/sec",
        range_results.len() as f64 / range_duration.as_secs_f64()
    );

    // Test 4: Update Performance (COW semantics)
    println!("\n4. Update Performance Test (COW)");
    let update_count = 1_000;
    let start = Instant::now();

    for i in 0..update_count {
        let key = format!("user:{:08}", i);
        let value = format!(
            "UPDATED: User data for user {}, updated at {}",
            i,
            start.elapsed().as_micros()
        );
        engine.put(key.as_bytes(), value.as_bytes())?;
    }

    let update_duration = start.elapsed();
    let update_ops_per_sec = update_count as f64 / update_duration.as_secs_f64();

    println!(
        "  ✅ Updated {} records in {:?}",
        update_count, update_duration
    );
    println!("  📊 Update rate: {:.0} ops/sec", update_ops_per_sec);

    // Test 5: Version History Performance
    println!("\n5. Version History Test");
    let start = Instant::now();

    let versions = engine.get_versions(b"user:00000001")?;
    let version_duration = start.elapsed();

    println!("  ✅ Retrieved version history in {:?}", version_duration);
    println!("  📊 Found {} versions for user:00000001", versions.len());
    for (i, (timestamp, _)) in versions.iter().enumerate() {
        if i < 3 {
            println!("    Version {}: timestamp {}", i + 1, timestamp);
        }
    }
    if versions.len() > 3 {
        println!("    ... and {} more versions", versions.len() - 3);
    }

    // Test 6: Mixed Workload Performance
    println!("\n6. Mixed Workload Test");
    let mixed_ops = 5_000;
    let start = Instant::now();

    for i in 0..mixed_ops {
        match i % 3 {
            0 => {
                // Write operation (33%)
                let key = format!("mixed:{:08}", i);
                let value = format!("Mixed workload data {}", i);
                engine.put(key.as_bytes(), value.as_bytes())?;
            }
            1 => {
                // Read operation (33%)
                let key = format!("user:{:08}", i % 10000);
                engine.get(key.as_bytes())?;
            }
            2 => {
                // Range operation (33%)
                let start_key = format!("user:{:08}", i % 1000);
                let end_key = format!("user:{:08}", (i % 1000) + 100);
                engine.range(start_key.as_bytes(), end_key.as_bytes())?;
            }
            _ => unreachable!(),
        }
    }

    let mixed_duration = start.elapsed();
    let mixed_ops_per_sec = mixed_ops as f64 / mixed_duration.as_secs_f64();

    println!(
        "  ✅ Completed {} mixed operations in {:?}",
        mixed_ops, mixed_duration
    );
    println!("  📊 Mixed workload rate: {:.0} ops/sec", mixed_ops_per_sec);

    // Test 7: Temporal Locality Test
    println!("\n7. Temporal Locality Test");
    let start = Instant::now();

    // Insert data with clustered timestamps
    for batch in 0..10 {
        let batch_start = Instant::now();
        for i in 0..100 {
            let key = format!("temporal:{}:{:04}", batch, i);
            let value = format!("Batch {} item {}", batch, i);
            engine.put(key.as_bytes(), value.as_bytes())?;
        }

        // Small delay to create temporal clustering
        std::thread::sleep(Duration::from_millis(1));
    }

    let temporal_duration = start.elapsed();
    println!("  ✅ Created temporal clusters in {:?}", temporal_duration);

    // Now test sequential access of one batch
    let batch_start = Instant::now();
    let mut batch_results = Vec::new();
    for i in 0..100 {
        let key = format!("temporal:5:{:04}", i);
        if let Some(value) = engine.get(key.as_bytes())? {
            batch_results.push(value);
        }
    }
    let batch_duration = batch_start.elapsed();

    println!(
        "  📊 Sequential batch access: {} items in {:?}",
        batch_results.len(),
        batch_duration
    );
    println!(
        "  📊 Batch access rate: {:.0} items/sec",
        batch_results.len() as f64 / batch_duration.as_secs_f64()
    );

    // Database Statistics
    println!("\n8. Database Statistics");
    let stats = engine.stats();
    println!("  📊 Total unique keys: {}", stats.total_keys);
    println!(
        "  📊 File size: {:.2} MB",
        stats.file_size_bytes as f64 / 1_048_576.0
    );
    println!(
        "  📊 Average bytes per key: {:.1}",
        stats.file_size_bytes as f64 / stats.total_keys as f64
    );

    // Cleanup and final timing
    engine.flush()?;

    println!("\n🎉 Performance Test Completed Successfully!");
    println!("\n📈 Summary:");
    println!("  • Bulk Insert: {:.0} ops/sec", insert_ops_per_sec);
    println!("  • Point Lookup: {:.0} ops/sec", lookup_ops_per_sec);
    println!("  • Mixed Workload: {:.0} ops/sec", mixed_ops_per_sec);
    println!(
        "  • Database Size: {:.2} MB",
        stats.file_size_bytes as f64 / 1_048_576.0
    );

    // Keep the test file for inspection
    println!("\n💾 Database file saved as: {}", db_file.display());
    println!("   You can inspect it or run the test again to see performance on existing data.");

    Ok(())
}
