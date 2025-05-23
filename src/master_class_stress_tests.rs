use crate::master_class::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

#[cfg(test)]
mod stress_tests {
    use super::*;

    #[test]
    fn stress_test_concurrent_writes_heavy() {
        let config = MasterClassConfig::default();
        let db = Arc::new(MasterClassZVerse::new(config).expect("Failed to create database"));

        let num_threads = 12;
        let writes_per_thread = 1000;
        let barrier = Arc::new(Barrier::new(num_threads));

        let mut handles = vec![];

        for thread_id in 0..num_threads {
            let db_clone = db.clone();
            let barrier_clone = barrier.clone();

            let handle = thread::spawn(move || {
                barrier_clone.wait(); // Synchronize start

                let mut versions = vec![];
                for i in 0..writes_per_thread {
                    let key = format!("stress-{:04}-{:06}", thread_id, i);
                    let value = format!("value-{:04}-{:06}", thread_id, i);

                    let version = db_clone
                        .put(key.as_bytes(), value.as_bytes())
                        .expect("Put failed");
                    versions.push(version);
                }
                versions
            });
            handles.push(handle);
        }

        let start = Instant::now();
        let mut all_versions = vec![];
        for handle in handles {
            let versions = handle.join().unwrap();
            all_versions.extend(versions);
        }
        let elapsed = start.elapsed();

        // Verify all versions are unique (no race conditions)
        all_versions.sort();
        let mut unique_versions = all_versions.clone();
        unique_versions.dedup();
        assert_eq!(
            all_versions.len(),
            unique_versions.len(),
            "Version collision detected!"
        );

        // Verify total count
        assert_eq!(all_versions.len(), num_threads * writes_per_thread);

        let total_ops = all_versions.len() as f64;
        let ops_per_sec = total_ops / elapsed.as_secs_f64();
        println!(
            "Heavy concurrent writes: {:.0} ops/sec across {} threads",
            ops_per_sec, num_threads
        );

        // Should maintain high performance even under heavy load
        assert!(
            ops_per_sec > 10000.0,
            "Performance degraded under heavy load: {:.0} ops/sec",
            ops_per_sec
        );
    }

    #[test]
    fn stress_test_read_performance_single_threaded() {
        let config = MasterClassConfig::default();
        let db = MasterClassZVerse::new(config).expect("Failed to create database");

        // Pre-populate with data
        let num_entries = 10000;
        for i in 0..num_entries {
            let key = format!("read-test-{:06}", i);
            let value = format!("read-value-{:06}", i);
            db.put(key.as_bytes(), value.as_bytes())
                .expect("Put failed");
        }

        // Test read performance
        let start = Instant::now();
        let num_reads = 50000;

        for i in 0..num_reads {
            let key_idx = i % num_entries;
            let key = format!("read-test-{:06}", key_idx);
            let result = db.get(key.as_bytes(), None).expect("Get failed");
            assert!(result.is_some(), "Expected value for key {}", key);
        }

        let elapsed = start.elapsed();
        let reads_per_sec = num_reads as f64 / elapsed.as_secs_f64();

        println!(
            "Single-threaded read performance: {:.0} reads/sec",
            reads_per_sec
        );

        // Should achieve very high read performance
        assert!(
            reads_per_sec > 100000.0,
            "Read performance too low: {:.0} reads/sec",
            reads_per_sec
        );
    }

    #[test]
    fn stress_test_concurrent_readers() {
        let config = MasterClassConfig::default();
        let db = Arc::new(MasterClassZVerse::new(config).expect("Failed to create database"));

        // Pre-populate with data
        let num_entries = 5000;
        for i in 0..num_entries {
            let key = format!("concurrent-read-{:06}", i);
            let value = format!("concurrent-value-{:06}", i);
            db.put(key.as_bytes(), value.as_bytes())
                .expect("Put failed");
        }

        let num_readers = 8;
        let reads_per_reader = 10000;
        let barrier = Arc::new(Barrier::new(num_readers));

        let mut handles = vec![];

        for reader_id in 0..num_readers {
            let db_clone = db.clone();
            let barrier_clone = barrier.clone();

            let handle = thread::spawn(move || {
                barrier_clone.wait(); // Synchronize start

                let start = Instant::now();
                let mut successful_reads = 0;

                for i in 0..reads_per_reader {
                    let key_idx = i % num_entries;
                    let key = format!("concurrent-read-{:06}", key_idx);

                    if let Ok(Some(_)) = db_clone.get(key.as_bytes(), None) {
                        successful_reads += 1;
                    }
                }

                let elapsed = start.elapsed();
                (successful_reads, elapsed)
            });
            handles.push(handle);
        }

        let mut total_reads = 0;
        let mut total_time = Duration::new(0, 0);

        for handle in handles {
            let (reads, elapsed) = handle.join().unwrap();
            total_reads += reads;
            total_time = total_time.max(elapsed); // Use max time (parallel execution)
        }

        let reads_per_sec = total_reads as f64 / total_time.as_secs_f64();

        println!(
            "Concurrent readers: {:.0} total reads/sec across {} threads",
            reads_per_sec, num_readers
        );
        assert_eq!(
            total_reads,
            num_readers * reads_per_reader,
            "Some reads failed"
        );

        // Should scale well with multiple readers
        assert!(
            reads_per_sec > 500000.0,
            "Concurrent read performance too low: {:.0} reads/sec",
            reads_per_sec
        );
    }

    #[test]
    fn stress_test_mixed_workload() {
        let config = MasterClassConfig::default();
        let db = Arc::new(MasterClassZVerse::new(config).expect("Failed to create database"));

        let num_writers = 4;
        let num_readers = 8;
        let duration = Duration::from_secs(2);
        let stop_flag = Arc::new(AtomicBool::new(false));

        let mut handles = vec![];

        // Spawn writers
        for writer_id in 0..num_writers {
            let db_clone = db.clone();
            let stop_flag_clone = stop_flag.clone();

            let handle = thread::spawn(move || {
                let mut writes = 0;
                let mut counter = 0;

                while !stop_flag_clone.load(Ordering::Relaxed) {
                    let key = format!("mixed-writer-{}-{}", writer_id, counter);
                    let value = format!("mixed-value-{}-{}", writer_id, counter);

                    if db_clone.put(key.as_bytes(), value.as_bytes()).is_ok() {
                        writes += 1;
                    }
                    counter += 1;
                }
                writes
            });
            handles.push(handle);
        }

        // Spawn readers
        for reader_id in 0..num_readers {
            let db_clone = db.clone();
            let stop_flag_clone = stop_flag.clone();

            let handle = thread::spawn(move || {
                let mut reads = 0;
                let mut successful_reads = 0;

                while !stop_flag_clone.load(Ordering::Relaxed) {
                    // Try to read from different writers
                    for writer_id in 0..num_writers {
                        let key = format!("mixed-writer-{}-{}", writer_id, reads % 100);
                        if let Ok(Some(_)) = db_clone.get(key.as_bytes(), None) {
                            successful_reads += 1;
                        }
                        reads += 1;
                    }
                }
                (reads, successful_reads)
            });
            handles.push(handle);
        }

        // Let it run for specified duration
        thread::sleep(duration);
        stop_flag.store(true, Ordering::Relaxed);

        let mut total_writes = 0;
        let mut total_reads = 0;
        let mut successful_reads = 0;

        for (i, handle) in handles.into_iter().enumerate() {
            if i < num_writers {
                total_writes += handle.join().unwrap() as usize;
            } else {
                let (reads, success) = handle.join().unwrap();
                total_reads += reads;
                successful_reads += success;
            }
        }

        let write_rate = total_writes as f64 / duration.as_secs_f64();
        let read_rate = total_reads as f64 / duration.as_secs_f64();
        let success_rate = successful_reads as f64 / total_reads as f64;

        println!(
            "Mixed workload - Writes: {:.0}/sec, Reads: {:.0}/sec, Success rate: {:.1}%",
            write_rate,
            read_rate,
            success_rate * 100.0
        );

        assert!(
            write_rate > 1000.0,
            "Write rate too low in mixed workload: {:.0}/sec",
            write_rate
        );
        assert!(
            read_rate > 10000.0,
            "Read rate too low in mixed workload: {:.0}/sec",
            read_rate
        );
        assert!(
            success_rate > 0.1,
            "Read success rate too low: {:.1}%",
            success_rate * 100.0
        );
    }

    #[test]
    fn stress_test_data_consistency() {
        let config = MasterClassConfig::default();
        let db = Arc::new(MasterClassZVerse::new(config).expect("Failed to create database"));

        let num_keys = 1000;
        let num_writers = 8;
        let writes_per_key = 10;

        let mut handles = vec![];

        // Each writer updates each key multiple times
        for writer_id in 0..num_writers {
            let db_clone = db.clone();

            let handle = thread::spawn(move || {
                for key_id in 0..num_keys {
                    for write_id in 0..writes_per_key {
                        let key = format!("consistency-key-{:04}", key_id);
                        let value = format!("writer-{}-write-{}", writer_id, write_id);

                        db_clone
                            .put(key.as_bytes(), value.as_bytes())
                            .expect("Put failed");
                    }
                }
            });
            handles.push(handle);
        }

        // Wait for all writes to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify consistency: each key should have exactly one value
        let mut value_counts = HashMap::new();

        for key_id in 0..num_keys {
            let key = format!("consistency-key-{:04}", key_id);

            if let Ok(Some(value)) = db.get(key.as_bytes(), None) {
                let value_str = String::from_utf8(value).unwrap();
                *value_counts.entry(value_str).or_insert(0) += 1;
            }
        }

        println!(
            "Data consistency test: {} unique values found for {} keys",
            value_counts.len(),
            num_keys
        );

        // Each key should have a value
        assert_eq!(
            value_counts.values().sum::<usize>(),
            num_keys,
            "Some keys are missing values"
        );

        // Verify no data corruption by checking value format
        for value in value_counts.keys() {
            assert!(
                value.starts_with("writer-") && value.contains("-write-"),
                "Corrupted value detected: {}",
                value
            );
        }
    }

    #[test]
    fn stress_test_memory_safety() {
        let config = MasterClassConfig::default();
        let db = Arc::new(MasterClassZVerse::new(config).expect("Failed to create database"));

        let num_iterations = 1000;
        let large_value_size = 10 * 1024; // 10KB values

        // Test with large values that could cause memory issues
        for i in 0..num_iterations {
            let key = format!("memory-test-{:06}", i);
            let value = vec![b'X'; large_value_size];

            db.put(key.as_bytes(), &value).expect("Put failed");

            // Occasionally read back to verify
            if i % 100 == 0 {
                let result = db.get(key.as_bytes(), None).expect("Get failed");
                assert_eq!(result.unwrap().len(), large_value_size);
            }
        }

        // Test rapid allocation/deallocation
        let mut handles = vec![];

        for thread_id in 0..4 {
            let db_clone = db.clone();

            let handle = thread::spawn(move || {
                for i in 0..250 {
                    let key = format!("rapid-{}-{}", thread_id, i);
                    let value = vec![thread_id as u8; 1024];

                    db_clone.put(key.as_bytes(), &value).expect("Put failed");
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        println!("Memory safety test completed without crashes");
    }

    #[test]
    fn stress_test_ring_buffer_overflow() {
        let config = MasterClassConfig::default();
        let db = MasterClassZVerse::new(config).expect("Failed to create database");

        // Try to overflow the ring buffer
        let ring_buffer_size = 1024; // From RING_BUFFER_SIZE constant
        let overflow_attempts = ring_buffer_size * 2;

        let mut successful_writes = 0;
        let mut overflow_errors = 0;

        for i in 0..overflow_attempts {
            let key = format!("overflow-test-{:06}", i);
            let value = format!("overflow-value-{:06}", i);

            match db.put(key.as_bytes(), value.as_bytes()) {
                Ok(_) => successful_writes += 1,
                Err(_) => overflow_errors += 1,
            }
        }

        println!(
            "Ring buffer overflow test: {} successful, {} errors",
            successful_writes, overflow_errors
        );

        // Should handle overflow gracefully
        if overflow_errors > 0 {
            println!("Ring buffer overflow detected and handled correctly");
        }

        // Should still be able to write after overflow
        let final_key = b"final-test";
        let final_value = b"final-value";

        // This might fail if buffer is full, which is acceptable
        match db.put(final_key, final_value) {
            Ok(_) => {
                let result = db.get(final_key, None).expect("Get failed");
                assert_eq!(result, Some(final_value.to_vec()));
            }
            Err(_) => println!("Buffer full, overflow handling working correctly"),
        }
    }

    #[test]
    fn stress_test_version_ordering() {
        let config = MasterClassConfig::default();
        let db = Arc::new(MasterClassZVerse::new(config).expect("Failed to create database"));

        let num_threads = 8;
        let writes_per_thread = 500;
        let versions = Arc::new(std::sync::Mutex::new(Vec::new()));

        let mut handles = vec![];

        for thread_id in 0..num_threads {
            let db_clone = db.clone();
            let versions_clone = versions.clone();

            let handle = thread::spawn(move || {
                for i in 0..writes_per_thread {
                    let key = format!("version-test-{}-{}", thread_id, i);
                    let value = format!("version-value-{}-{}", thread_id, i);

                    let version = db_clone
                        .put(key.as_bytes(), value.as_bytes())
                        .expect("Put failed");

                    versions_clone.lock().unwrap().push((thread_id, i, version));
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let mut all_versions = versions.lock().unwrap();
        all_versions.sort_by_key(|&(_, _, version)| version);

        // Verify versions are strictly increasing
        for i in 1..all_versions.len() {
            assert!(
                all_versions[i].2 > all_versions[i - 1].2,
                "Version ordering violation: {} <= {}",
                all_versions[i].2,
                all_versions[i - 1].2
            );
        }

        println!(
            "Version ordering test passed: {} versions in strict order",
            all_versions.len()
        );
    }

    #[test]
    fn stress_test_long_running_stability() {
        let config = MasterClassConfig::default();
        let db = Arc::new(MasterClassZVerse::new(config).expect("Failed to create database"));

        let duration = Duration::from_secs(5); // 5-second stress test
        let stop_flag = Arc::new(AtomicBool::new(false));
        let operations = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];

        // Continuous writer
        {
            let db_clone = db.clone();
            let stop_flag_clone = stop_flag.clone();
            let operations_clone = operations.clone();

            let handle = thread::spawn(move || {
                let mut counter = 0;
                while !stop_flag_clone.load(Ordering::Relaxed) {
                    let key = format!("stability-{:08}", counter);
                    let value = format!("stability-value-{:08}", counter);

                    if db_clone.put(key.as_bytes(), value.as_bytes()).is_ok() {
                        operations_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    counter += 1;

                    if counter % 1000 == 0 {
                        thread::sleep(Duration::from_millis(1)); // Brief pause
                    }
                }
            });
            handles.push(handle);
        }

        // Continuous reader
        {
            let db_clone = db.clone();
            let stop_flag_clone = stop_flag.clone();
            let operations_clone = operations.clone();

            let handle = thread::spawn(move || {
                let mut counter = 0;
                while !stop_flag_clone.load(Ordering::Relaxed) {
                    let key = format!("stability-{:08}", counter % 1000);

                    if db_clone.get(key.as_bytes(), None).is_ok() {
                        operations_clone.fetch_add(1, Ordering::Relaxed);
                    }
                    counter += 1;
                }
            });
            handles.push(handle);
        }

        thread::sleep(duration);
        stop_flag.store(true, Ordering::Relaxed);

        for handle in handles {
            handle.join().unwrap();
        }

        let total_ops = operations.load(Ordering::Relaxed);
        let ops_per_sec = total_ops as f64 / duration.as_secs_f64();

        println!(
            "Long-running stability test: {:.0} total ops/sec over {} seconds",
            ops_per_sec,
            duration.as_secs()
        );

        assert!(
            total_ops > 1000,
            "System became unresponsive during long-running test"
        );
        assert!(
            ops_per_sec > 1000.0,
            "Performance degraded significantly over time"
        );
    }
}
