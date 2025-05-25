//! MVCC Demonstration with Selective Flushing
//!
//! This example demonstrates Multi-Version Concurrency Control (MVCC) capabilities
//! of the Revolutionary Database, including:
//! - Snapshot isolation between transactions
//! - Read-your-own-writes within transactions
//! - Atomic commit/rollback operations
//! - Selective flushing that respects active transaction boundaries

use std::sync::Arc;
use std::thread;
use std::time::Duration;
use zverse::revolutionary::RevolutionaryDB;

fn main() {
    println!("=== Revolutionary DB MVCC Demo ===\n");
    
    demo_basic_transactions();
    demo_snapshot_isolation();
    demo_read_own_writes();
    demo_rollback_behavior();
    demo_concurrent_transactions();
    demo_selective_flushing();
    
    println!("=== Demo Complete ===");
}

/// Basic transaction operations
fn demo_basic_transactions() {
    println!("1. Basic Transaction Operations");
    println!("------------------------------");
    
    let db = RevolutionaryDB::new();
    
    // Set up initial state
    db.put("account:alice", b"1000").unwrap();
    db.put("account:bob", b"500").unwrap();
    
    println!("Initial state:");
    println!("  Alice: {:?}", String::from_utf8_lossy(&db.get("account:alice").unwrap()));
    println!("  Bob: {:?}", String::from_utf8_lossy(&db.get("account:bob").unwrap()));
    
    // Transfer money in a transaction
    let mut txn = db.begin_transaction(false);
    
    // Read current balances
    let alice_data = db.get_txn(&txn, "account:alice").unwrap();
    let alice_balance: u32 = String::from_utf8_lossy(&alice_data).parse().unwrap();
    let bob_data = db.get_txn(&txn, "account:bob").unwrap();
    let bob_balance: u32 = String::from_utf8_lossy(&bob_data).parse().unwrap();
    
    println!("\nTransfer $100 from Alice to Bob...");
    
    // Update balances
    let new_alice = alice_balance - 100;
    let new_bob = bob_balance + 100;
    
    db.put_txn(&mut txn, "account:alice", new_alice.to_string().as_bytes()).unwrap();
    db.put_txn(&mut txn, "account:bob", new_bob.to_string().as_bytes()).unwrap();
    
    // Commit transaction
    db.commit_transaction(txn).unwrap();
    
    println!("After transaction:");
    println!("  Alice: {:?}", String::from_utf8_lossy(&db.get("account:alice").unwrap()));
    println!("  Bob: {:?}", String::from_utf8_lossy(&db.get("account:bob").unwrap()));
    
    println!();
}

/// Demonstrate snapshot isolation between transactions
fn demo_snapshot_isolation() {
    println!("2. Snapshot Isolation");
    println!("--------------------");
    
    let db = RevolutionaryDB::new();
    
    // Initial state
    db.put("counter", b"0").unwrap();
    
    // Start two transactions at the same time
    let mut txn1 = db.begin_transaction(false);
    let txn2 = db.begin_transaction(false);
    
    println!("Both transactions start, counter = 0");
    
    // Both read initial value
    let data1 = db.get_txn(&txn1, "counter").unwrap();
    let value1 = String::from_utf8_lossy(&data1);
    let data2 = db.get_txn(&txn2, "counter").unwrap();
    let value2 = String::from_utf8_lossy(&data2);
    
    println!("T1 reads: {}", value1);
    println!("T2 reads: {}", value2);
    
    // T1 increments
    db.put_txn(&mut txn1, "counter", b"1").unwrap();
    println!("T1 increments counter to 1");
    
    // T2 should still see old value (snapshot isolation)
    let data2_again = db.get_txn(&txn2, "counter").unwrap();
    let value2_again = String::from_utf8_lossy(&data2_again);
    println!("T2 still reads: {} (snapshot isolation)", value2_again);
    
    // Commit T1
    db.commit_transaction(txn1).unwrap();
    println!("T1 commits");
    
    // T2 still sees its snapshot
    let data2_after_commit = db.get_txn(&txn2, "counter").unwrap();
    let value2_after_commit = String::from_utf8_lossy(&data2_after_commit);
    println!("T2 still reads: {} (consistent snapshot)", value2_after_commit);
    
    // Clean up T2
    db.rollback_transaction(txn2);
    
    // New transaction sees committed value
    let txn3 = db.begin_transaction(true);
    let final_data = db.get_txn(&txn3, "counter").unwrap();
    let final_value = String::from_utf8_lossy(&final_data);
    println!("New transaction reads: {}", final_value);
    db.rollback_transaction(txn3);
    
    println!();
}

/// Demonstrate read-your-own-writes within transactions
fn demo_read_own_writes() {
    println!("3. Read Your Own Writes");
    println!("----------------------");
    
    let db = RevolutionaryDB::new();
    
    let mut txn = db.begin_transaction(false);
    
    // Write a value
    db.put_txn(&mut txn, "temp_key", b"my_value").unwrap();
    println!("Transaction writes: temp_key = 'my_value'");
    
    // Should be able to read our own write
    let value = db.get_txn(&txn, "temp_key").unwrap();
    println!("Transaction reads back: {:?}", String::from_utf8_lossy(&value));
    
    // But it's not visible outside the transaction
    let outside_read = db.get("temp_key");
    println!("Outside transaction sees: {:?}", outside_read);
    
    // Update the value within transaction
    db.put_txn(&mut txn, "temp_key", b"updated_value").unwrap();
    println!("Transaction updates: temp_key = 'updated_value'");
    
    // Should see the updated value
    let updated_value = db.get_txn(&txn, "temp_key").unwrap();
    println!("Transaction reads updated: {:?}", String::from_utf8_lossy(&updated_value));
    
    // Commit to make it visible
    db.commit_transaction(txn).unwrap();
    
    let committed_value = db.get("temp_key").unwrap();
    println!("After commit, everyone sees: {:?}", String::from_utf8_lossy(&committed_value));
    
    println!();
}

/// Demonstrate rollback behavior
fn demo_rollback_behavior() {
    println!("4. Rollback Behavior");
    println!("-------------------");
    
    let db = RevolutionaryDB::new();
    
    // Initial state
    db.put("important_data", b"original").unwrap();
    println!("Initial value: {:?}", String::from_utf8_lossy(&db.get("important_data").unwrap()));
    
    let mut txn = db.begin_transaction(false);
    
    // Make changes
    db.put_txn(&mut txn, "important_data", b"modified").unwrap();
    db.put_txn(&mut txn, "new_key", b"new_data").unwrap();
    
    println!("Transaction modifies data...");
    println!("  important_data = {:?}", String::from_utf8_lossy(&db.get_txn(&txn, "important_data").unwrap()));
    println!("  new_key = {:?}", String::from_utf8_lossy(&db.get_txn(&txn, "new_key").unwrap()));
    
    // Rollback instead of commit
    println!("Rolling back transaction...");
    db.rollback_transaction(txn);
    
    // Check that changes were not applied
    println!("After rollback:");
    println!("  important_data = {:?}", String::from_utf8_lossy(&db.get("important_data").unwrap()));
    println!("  new_key = {:?}", db.get("new_key")); // Should be None
    
    println!();
}

/// Demonstrate concurrent transactions
fn demo_concurrent_transactions() {
    println!("5. Concurrent Transactions");
    println!("-------------------------");
    
    let db = Arc::new(RevolutionaryDB::new());
    
    // Initial bank accounts
    db.put("account:x", b"1000").unwrap();
    db.put("account:y", b"1000").unwrap();
    db.put("account:z", b"1000").unwrap();
    
    println!("Initial balances: X=1000, Y=1000, Z=1000");
    
    // Simulate concurrent transfers
    let mut handles = vec![];
    
    for i in 0..3 {
        let db_clone = db.clone();
        let handle = thread::spawn(move || {
            let mut txn = db_clone.begin_transaction(false);
            
            // Each thread transfers money between different accounts
            let (from, to, amount) = match i {
                0 => ("account:x", "account:y", 100),
                1 => ("account:y", "account:z", 50),
                2 => ("account:z", "account:x", 25),
                _ => unreachable!(),
            };
            
            // Simulate some processing time
            thread::sleep(Duration::from_millis(10));
            
            // Read balances
            let from_data = db_clone.get_txn(&txn, from).unwrap();
            let from_balance: u32 = String::from_utf8_lossy(&from_data).parse().unwrap();
            let to_data = db_clone.get_txn(&txn, to).unwrap();
            let to_balance: u32 = String::from_utf8_lossy(&to_data).parse().unwrap();
            
            // Transfer
            let new_from = from_balance - amount;
            let new_to = to_balance + amount;
            
            db_clone.put_txn(&mut txn, from, new_from.to_string().as_bytes()).unwrap();
            db_clone.put_txn(&mut txn, to, new_to.to_string().as_bytes()).unwrap();
            
            println!("Transaction {} transfers ${} from {} to {}", i, amount, from, to);
            
            // Commit
            db_clone.commit_transaction(txn).unwrap();
        });
        
        handles.push(handle);
    }
    
    // Wait for all transactions to complete
    for handle in handles {
        handle.join().unwrap();
    }
    
    // Check final balances
    let x_final: u32 = String::from_utf8_lossy(&db.get("account:x").unwrap()).parse().unwrap();
    let y_final: u32 = String::from_utf8_lossy(&db.get("account:y").unwrap()).parse().unwrap();
    let z_final: u32 = String::from_utf8_lossy(&db.get("account:z").unwrap()).parse().unwrap();
    let total = x_final + y_final + z_final;
    
    println!("Final balances: X={}, Y={}, Z={}", x_final, y_final, z_final);
    println!("Total preserved: {} (should be 3000)", total);
    
    println!();
}

/// Demonstrate selective flushing with active transactions
fn demo_selective_flushing() {
    println!("6. Selective Flushing Demo");
    println!("-------------------------");
    
    let db = RevolutionaryDB::new();
    
    // Create some committed data (eligible for flushing)
    for i in 0..10 {
        db.put(&format!("old_data:{}", i), format!("value_{}", i).as_bytes()).unwrap();
    }
    println!("Created 10 committed records (eligible for flushing)");
    
    // Start a long-running transaction
    let mut long_txn = db.begin_transaction(false);
    
    // Write some data in the transaction (should be protected from flushing)
    for i in 0..5 {
        db.put_txn(&mut long_txn, &format!("pending:{}", i), b"uncommitted").unwrap();
    }
    println!("Started long-running transaction with 5 pending writes");
    println!("These timestamps should be protected from flushing");
    
    // Simulate flush boundary calculation
    // The background flusher would exclude timestamps from the active transaction
    println!("Background flusher would:");
    println!("  - Flush old committed data");
    println!("  - Skip timestamps from active transaction");
    println!("  - Wait for transaction to commit/rollback");
    
    // Simulate transaction completing after some time
    thread::sleep(Duration::from_millis(50));
    
    println!("Transaction commits after processing...");
    db.commit_transaction(long_txn).unwrap();
    
    println!("Now all data becomes eligible for flushing");
    
    // Verify we can read the committed data
    for i in 0..5 {
        let value = db.get(&format!("pending:{}", i)).unwrap();
        println!("  pending:{} = {:?}", i, String::from_utf8_lossy(&value));
    }
    
    println!();
}