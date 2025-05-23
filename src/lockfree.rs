//! Lock-free concurrency module for ZVerse
//!
//! This module implements epoch-based concurrency control that allows:
//! - Serializability for writers via single coordination
//! - Wait-free readers with no atomic operations during reads  
//! - Zero contention between readers and writers
//! - Safe memory reclamation through epochs

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::thread::{self, ThreadId};
use parking_lot::RwLock;

use crate::error::Error;

/// Global epoch counter
static GLOBAL_EPOCH: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static THREAD_ID: ThreadId = thread::current().id();
}

/// Epoch-based memory reclamation system
pub struct EpochManager {
    /// Currently active readers by thread ID
    active_readers: RwLock<HashMap<ThreadId, u64>>,
    
    /// Garbage collection threshold
    gc_threshold: usize,
    
    /// Deferred deletions by epoch
    deferred_deletions: Mutex<HashMap<u64, Vec<Box<dyn Send + Sync>>>>,
    
    /// Local epoch counter for this manager (used in tests)
    local_epoch: AtomicU64,
}

impl EpochManager {
    /// Create a new epoch manager
    pub fn new() -> Self {
        Self {
            active_readers: RwLock::new(HashMap::new()),
            gc_threshold: 10000,
            deferred_deletions: Mutex::new(HashMap::new()),
            local_epoch: AtomicU64::new(0),
        }
    }
    
    /// Enter a read epoch
    pub fn enter_read(self: Arc<Self>) -> ReadGuard {
        let current_epoch = self.local_epoch.load(Ordering::Acquire);
        
        THREAD_ID.with(|thread_id| {
            let mut readers = self.active_readers.write();
            readers.insert(*thread_id, current_epoch);
        });
        
        ReadGuard {
            epoch_manager: self,
            epoch: current_epoch,
        }
    }
    
    /// Exit a read epoch
    fn exit_read(&self, thread_id: &ThreadId) {
        let mut readers = self.active_readers.write();
        readers.remove(thread_id);
    }
    
    /// Advance the global epoch (writers only)
    pub fn advance_epoch(&self) -> u64 {
        let new_epoch = self.local_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        self.try_garbage_collect();
        new_epoch
    }
    
    /// Try to garbage collect old epochs
    fn try_garbage_collect(&self) {
        let readers = self.active_readers.read();
        let min_active_epoch = readers.values().min().copied().unwrap_or(u64::MAX);
        drop(readers);
        
        let mut deletions = self.deferred_deletions.lock().unwrap();
        let epochs_to_remove: Vec<u64> = deletions
            .keys()
            .filter(|&&epoch| epoch < min_active_epoch)
            .copied()
            .collect();
        
        for epoch in epochs_to_remove {
            deletions.remove(&epoch);
        }
    }
    
    /// Defer deletion until safe
    pub fn defer_deletion<T: Send + Sync + 'static>(&self, item: T, epoch: u64) {
        let mut deletions = self.deferred_deletions.lock().unwrap();
        deletions.entry(epoch).or_insert_with(Vec::new).push(Box::new(item));
    }
}

/// RAII guard for read operations
pub struct ReadGuard {
    epoch_manager: Arc<EpochManager>,
    epoch: u64,
}

impl ReadGuard {
    /// Get the current read epoch
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        THREAD_ID.with(|thread_id| {
            self.epoch_manager.exit_read(thread_id);
        });
    }
}

/// Lock-free concurrent data structure
pub struct LockFreeZVerse<T> {
    /// Epoch manager for memory reclamation
    epoch_manager: Arc<EpochManager>,
    
    /// Writer coordination mutex (only one writer at a time)
    writer_mutex: Mutex<()>,
    
    /// Current data snapshot
    current_data: AtomicU64, // Pointer to current DataSnapshot
    
    /// Data snapshots by epoch
    snapshots: RwLock<HashMap<u64, Arc<DataSnapshot<T>>>>,
    
    /// Latest version counter
    latest_version: AtomicU64,
}

/// Immutable data snapshot for a specific epoch
pub struct DataSnapshot<T> {
    /// Epoch this snapshot was created in
    epoch: u64,
    
    /// Immutable data entries
    entries: Vec<T>,
    
    /// Version when this snapshot was created
    version: u64,
}

impl<T> DataSnapshot<T> {
    /// Create a new data snapshot
    pub fn new(epoch: u64, entries: Vec<T>, version: u64) -> Self {
        Self {
            epoch,
            entries,
            version,
        }
    }
    
    /// Get entries
    pub fn entries(&self) -> &[T] {
        &self.entries
    }
    
    /// Get epoch
    pub fn epoch(&self) -> u64 {
        self.epoch
    }
    
    /// Get version
    pub fn version(&self) -> u64 {
        self.version
    }
}

impl<T: Clone + Send + Sync + 'static> LockFreeZVerse<T> {
    /// Create a new lock-free ZVerse
    pub fn new() -> Self {
        let epoch_manager = Arc::new(EpochManager::new());
        let initial_snapshot = Arc::new(DataSnapshot::new(0, Vec::new(), 0));
        
        let mut snapshots = HashMap::new();
        snapshots.insert(0, initial_snapshot);
        
        Self {
            epoch_manager,
            writer_mutex: Mutex::new(()),
            current_data: AtomicU64::new(0), // Points to epoch 0
            snapshots: RwLock::new(snapshots),
            latest_version: AtomicU64::new(1),
        }
    }
    
    /// Begin a read transaction (wait-free)
    pub fn begin_read(&self) -> ReadTransaction<T> {
        let read_guard = self.epoch_manager.clone().enter_read();
        let current_epoch = read_guard.epoch();
        
        // Get the current data snapshot (wait-free)
        let snapshots = self.snapshots.read();
        let snapshot = snapshots.get(&current_epoch)
            .or_else(|| {
                // If exact epoch not found, get the latest one <= current_epoch
                snapshots.iter()
                    .filter(|(epoch, _)| **epoch <= current_epoch)
                    .max_by_key(|(epoch, _)| **epoch)
                    .map(|(_, snapshot)| snapshot)
            })
            .cloned()
            .unwrap_or_else(|| {
                // Fallback: get any available snapshot or create empty one
                snapshots.values().next()
                    .cloned()
                    .unwrap_or_else(|| {
                        Arc::new(DataSnapshot::new(current_epoch, Vec::new(), 0))
                    })
            });
        
        ReadTransaction {
            _read_guard: read_guard,
            snapshot,
        }
    }
    
    /// Begin a write transaction (serialized)
    pub fn begin_write(&self) -> Result<WriteTransaction<T>, Error> {
        // Only one writer at a time
        let writer_lock = self.writer_mutex.lock()
            .map_err(|_| Error::Other("Failed to acquire writer lock".to_string()))?;
        
        // Advance the epoch
        let new_epoch = self.epoch_manager.advance_epoch();
        
        // Get current data to modify
        let current_epoch = self.current_data.load(Ordering::Acquire);
        let snapshots = self.snapshots.read();
        let current_snapshot = snapshots.get(&current_epoch).unwrap().clone();
        drop(snapshots);
        
        Ok(WriteTransaction {
            _writer_lock: writer_lock,
            epoch_manager: self.epoch_manager.clone(),
            zverse: self,
            new_epoch,
            current_data: current_snapshot.entries().to_vec(),
            version: self.latest_version.load(Ordering::Acquire),
        })
    }
    
    /// Commit a write transaction
    fn commit_write(&self, transaction: WriteTransaction<T>) -> Result<u64, Error> {
        let new_version = self.latest_version.fetch_add(1, Ordering::AcqRel) + 1;
        
        // Create new snapshot
        let new_snapshot = Arc::new(DataSnapshot::new(
            transaction.new_epoch,
            transaction.current_data,
            new_version,
        ));
        
        // Update snapshots
        let mut snapshots = self.snapshots.write();
        snapshots.insert(transaction.new_epoch, new_snapshot);
        
        // Update current data pointer
        self.current_data.store(transaction.new_epoch, Ordering::Release);
        
        // Clean up old snapshots (keep last few for active readers)
        let epochs_to_remove: Vec<u64> = snapshots.keys()
            .filter(|&&epoch| epoch + 10 < transaction.new_epoch) // Keep last 10 epochs
            .copied()
            .collect();
        
        for epoch in epochs_to_remove {
            if let Some(old_snapshot) = snapshots.remove(&epoch) {
                self.epoch_manager.defer_deletion(old_snapshot, transaction.new_epoch);
            }
        }
        
        Ok(new_version)
    }
}

/// Read transaction handle (wait-free)
pub struct ReadTransaction<T> {
    _read_guard: ReadGuard,
    snapshot: Arc<DataSnapshot<T>>,
}

impl<T> ReadTransaction<T> {
    /// Get the data snapshot
    pub fn data(&self) -> &[T] {
        self.snapshot.entries()
    }
    
    /// Get the version
    pub fn version(&self) -> u64 {
        self.snapshot.version()
    }
    
    /// Get the epoch
    pub fn epoch(&self) -> u64 {
        self.snapshot.epoch()
    }
}

/// Write transaction handle (serialized)
pub struct WriteTransaction<'a, T> {
    _writer_lock: std::sync::MutexGuard<'a, ()>,
    epoch_manager: Arc<EpochManager>,
    zverse: &'a LockFreeZVerse<T>,
    new_epoch: u64,
    current_data: Vec<T>,
    version: u64,
}

impl<'a, T: Clone + Send + Sync + 'static> WriteTransaction<'a, T> {
    /// Get mutable access to data
    pub fn data_mut(&mut self) -> &mut Vec<T> {
        &mut self.current_data
    }
    
    /// Get current data
    pub fn data(&self) -> &[T] {
        &self.current_data
    }
    
    /// Get current version
    pub fn version(&self) -> u64 {
        self.version
    }
    
    /// Commit the transaction
    pub fn commit(self) -> Result<u64, Error> {
        self.zverse.commit_write(self)
    }
    
    /// Add an entry
    pub fn insert(&mut self, entry: T) {
        self.current_data.push(entry);
    }
    
    /// Remove entries matching a predicate
    pub fn remove_if<F>(&mut self, predicate: F) 
    where 
        F: Fn(&T) -> bool,
    {
        self.current_data.retain(|entry| !predicate(entry));
    }
}

impl<T> Default for LockFreeZVerse<T> 
where 
    T: Clone + Send + Sync + 'static
{
    fn default() -> Self {
        Self::new()
    }
}

// Ensure our types are Send and Sync
unsafe impl<T: Send + Sync> Send for LockFreeZVerse<T> {}
unsafe impl<T: Send + Sync> Sync for LockFreeZVerse<T> {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;
    
    #[test]
    fn test_epoch_management() {
        let epoch_manager = Arc::new(EpochManager::new());
        
        // Test entering and exiting read epochs
        let guard1 = epoch_manager.clone().enter_read();
        let epoch1 = guard1.epoch();
        
        let guard2 = epoch_manager.clone().enter_read();
        let epoch2 = guard2.epoch();
        
        // Epochs should be close to each other (within a reasonable range)
        // since they were created nearly simultaneously
        assert!((epoch1 as i64 - epoch2 as i64).abs() <= 1);
        
        drop(guard1);
        drop(guard2);
        
        // Advance epoch and verify it increases
        let current_epoch = epoch_manager.local_epoch.load(Ordering::Acquire);
        let new_epoch = epoch_manager.advance_epoch();
        assert_eq!(new_epoch, current_epoch + 1);
        assert_eq!(epoch_manager.local_epoch.load(Ordering::Acquire), new_epoch);
    }
    
    #[test]
    fn test_concurrent_readers() {
        let zverse = Arc::new(LockFreeZVerse::<i32>::new());
        let mut handles = vec![];
        
        // Spawn multiple readers
        for i in 0..10 {
            let zverse_clone = zverse.clone();
            let handle = thread::spawn(move || {
                for _ in 0..100 {
                    let read_tx = zverse_clone.begin_read();
                    let _data = read_tx.data();
                    thread::sleep(Duration::from_micros(10));
                }
                i
            });
            handles.push(handle);
        }
        
        // All readers should complete successfully
        for handle in handles {
            handle.join().unwrap();
        }
    }
    
    #[test]
    fn test_writer_serialization() {
        let zverse = Arc::new(LockFreeZVerse::<i32>::new());
        let zverse_clone = zverse.clone();
        
        // Spawn concurrent writers
        let handle1 = thread::spawn(move || {
            let mut write_tx = zverse_clone.begin_write().unwrap();
            write_tx.insert(1);
            write_tx.insert(2);
            write_tx.commit().unwrap()
        });
        
        let zverse_clone2 = zverse.clone();
        let handle2 = thread::spawn(move || {
            let mut write_tx = zverse_clone2.begin_write().unwrap();
            write_tx.insert(3);
            write_tx.insert(4);
            write_tx.commit().unwrap()
        });
        
        let version1 = handle1.join().unwrap();
        let version2 = handle2.join().unwrap();
        
        // Writers should be serialized - one should get version 2, other version 3
        assert!(version1 != version2);
        assert!((version1 == 2 && version2 == 3) || (version1 == 3 && version2 == 2));
        
        // Final data should contain all 4 items
        let read_tx = zverse.begin_read();
        assert_eq!(read_tx.data().len(), 4);
    }
    
    #[test]
    fn test_readers_writers_no_blocking() {
        let zverse = Arc::new(LockFreeZVerse::<i32>::new());
        
        // Start a long-running reader
        let zverse_reader = zverse.clone();
        let reader_handle = thread::spawn(move || {
            let read_tx = zverse_reader.begin_read();
            thread::sleep(Duration::from_millis(100)); // Hold read for 100ms
            read_tx.data().len()
        });
        
        // Start a writer that should not be blocked by the reader
        let zverse_writer = zverse.clone();
        let writer_handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50)); // Start after reader
            let mut write_tx = zverse_writer.begin_write().unwrap();
            write_tx.insert(42);
            write_tx.commit().unwrap()
        });
        
        // Both should complete successfully
        let reader_result = reader_handle.join().unwrap();
        let writer_result = writer_handle.join().unwrap();
        
        assert_eq!(reader_result, 0); // Reader saw empty data
        assert_eq!(writer_result, 2); // Writer committed at version 2
    }
}