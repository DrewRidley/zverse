//! Transaction system with MVCC support for IMEC storage engine
//!
//! This module implements multi-version concurrency control (MVCC) using copy-on-write
//! semantics for lock-free read/write operations with snapshot isolation.

use crate::storage::extent::ExtentError;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, Mutex};
use std::collections::{HashMap, HashSet};
use std::time::{SystemTime, UNIX_EPOCH};

/// Unique transaction identifier
pub type TransactionId = u64;

/// Snapshot timestamp for isolation
pub type SnapshotId = u64;

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    Preparing,
    Committed,
    Aborted,
}

/// Transaction metadata
#[derive(Debug, Clone)]
pub struct Transaction {
    /// Unique transaction ID
    pub id: TransactionId,
    /// Snapshot timestamp for reads
    pub snapshot_id: SnapshotId,
    /// Transaction start time
    pub start_time: u64,
    /// Current state
    pub state: TransactionState,
    /// Read set - keys read by this transaction
    pub read_set: HashSet<String>,
    /// Write set - keys modified by this transaction
    pub write_set: HashMap<String, Vec<u8>>,
    /// Copy-on-write extents allocated by this transaction
    pub cow_extents: HashSet<u32>,
    /// Dependencies - transactions this depends on
    pub dependencies: HashSet<TransactionId>,
}

impl Transaction {
    /// Create a new transaction
    pub fn new(id: TransactionId, snapshot_id: SnapshotId) -> Self {
        Self {
            id,
            snapshot_id,
            start_time: current_timestamp_micros(),
            state: TransactionState::Active,
            read_set: HashSet::new(),
            write_set: HashMap::new(),
            cow_extents: HashSet::new(),
            dependencies: HashSet::new(),
        }
    }

    /// Add a key to the read set
    pub fn add_read(&mut self, key: &str) {
        self.read_set.insert(key.to_string());
    }

    /// Add a key-value pair to the write set
    pub fn add_write(&mut self, key: &str, value: Vec<u8>) {
        self.write_set.insert(key.to_string(), value);
    }

    /// Add a copy-on-write extent
    pub fn add_cow_extent(&mut self, extent_id: u32) {
        self.cow_extents.insert(extent_id);
    }

    /// Check if transaction has writes
    pub fn has_writes(&self) -> bool {
        !self.write_set.is_empty()
    }

    /// Check if transaction reads a key
    pub fn reads_key(&self, key: &str) -> bool {
        self.read_set.contains(key)
    }

    /// Check if transaction writes a key
    pub fn writes_key(&self, key: &str) -> bool {
        self.write_set.contains_key(key)
    }
}

/// MVCC version of a key-value pair
#[derive(Debug, Clone)]
pub struct Version {
    /// Transaction ID that created this version
    pub created_by: TransactionId,
    /// Transaction ID that deleted this version (if any)
    pub deleted_by: Option<TransactionId>,
    /// Creation timestamp
    pub timestamp: u64,
    /// Value data
    pub value: Vec<u8>,
    /// Morton code for spatial locality
    pub morton_code: u64,
}

impl Version {
    /// Create a new version
    pub fn new(created_by: TransactionId, value: Vec<u8>, morton_code: u64) -> Self {
        Self {
            created_by,
            deleted_by: None,
            timestamp: current_timestamp_micros(),
            value,
            morton_code,
        }
    }

    /// Check if version is visible to a snapshot
    pub fn is_visible(&self, snapshot_id: SnapshotId, committed_txns: &HashSet<TransactionId>) -> bool {
        // Version is visible if:
        // 1. Created by a committed transaction before the snapshot
        // 2. Not deleted by a committed transaction before the snapshot
        
        if !committed_txns.contains(&self.created_by) {
            return false;
        }
        
        if self.created_by > snapshot_id {
            return false;
        }
        
        if let Some(deleted_by) = self.deleted_by {
            if committed_txns.contains(&deleted_by) && deleted_by <= snapshot_id {
                return false;
            }
        }
        
        true
    }

    /// Mark version as deleted
    pub fn delete(&mut self, deleted_by: TransactionId) {
        self.deleted_by = Some(deleted_by);
    }
}

/// Multi-version store for a single key
#[derive(Debug, Clone)]
pub struct VersionChain {
    /// Key name
    pub key: String,
    /// All versions of this key, ordered by timestamp
    pub versions: Vec<Version>,
}

impl VersionChain {
    /// Create new version chain
    pub fn new(key: String) -> Self {
        Self {
            key,
            versions: Vec::new(),
        }
    }

    /// Add a new version
    pub fn add_version(&mut self, version: Version) {
        // Insert in timestamp order (most recent first)
        let pos = self.versions
            .binary_search_by(|v| v.timestamp.cmp(&version.timestamp).reverse())
            .unwrap_or_else(|e| e);
        self.versions.insert(pos, version);
    }

    /// Get visible version for a snapshot
    pub fn get_visible_version(&self, snapshot_id: SnapshotId, committed_txns: &HashSet<TransactionId>) -> Option<&Version> {
        self.versions
            .iter()
            .find(|v| v.is_visible(snapshot_id, committed_txns))
    }

    /// Get latest version (for writers)
    pub fn get_latest_version(&self) -> Option<&Version> {
        self.versions.first()
    }

    /// Mark latest version as deleted
    pub fn delete_latest(&mut self, deleted_by: TransactionId) {
        if let Some(version) = self.versions.first_mut() {
            version.delete(deleted_by);
        }
    }
}

/// Transaction manager coordinating MVCC operations
pub struct TransactionManager {
    /// Next transaction ID
    next_txn_id: AtomicU64,
    /// Global snapshot counter
    next_snapshot_id: AtomicU64,
    /// Active transactions
    active_txns: RwLock<HashMap<TransactionId, Arc<Mutex<Transaction>>>>,
    /// Committed transaction IDs
    committed_txns: RwLock<HashSet<TransactionId>>,
    /// Multi-version key-value store
    version_store: RwLock<HashMap<String, VersionChain>>,
    /// Copy-on-write extent tracking
    cow_extents: RwLock<HashMap<u32, TransactionId>>,
    /// Global commit counter for atomic operations
    commit_counter: AtomicU64,
}

impl TransactionManager {
    /// Create new transaction manager
    pub fn new() -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
            next_snapshot_id: AtomicU64::new(1),
            active_txns: RwLock::new(HashMap::new()),
            committed_txns: RwLock::new(HashSet::new()),
            version_store: RwLock::new(HashMap::new()),
            cow_extents: RwLock::new(HashMap::new()),
            commit_counter: AtomicU64::new(0),
        }
    }

    /// Begin a new transaction
    pub fn begin_transaction(&self) -> Result<Arc<Mutex<Transaction>>, TransactionError> {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let snapshot_id = self.next_snapshot_id.load(Ordering::SeqCst);
        
        let txn = Arc::new(Mutex::new(Transaction::new(txn_id, snapshot_id)));
        
        {
            let mut active = self.active_txns.write().unwrap();
            active.insert(txn_id, txn.clone());
        }
        
        Ok(txn)
    }

    /// Read a key within a transaction
    pub fn read(&self, txn: &Arc<Mutex<Transaction>>, key: &str) -> Result<Option<Vec<u8>>, TransactionError> {
        let mut txn_guard = txn.lock().unwrap();
        
        if txn_guard.state != TransactionState::Active {
            return Err(TransactionError::TransactionNotActive);
        }
        
        // Check write set first (read your own writes)
        if let Some(value) = txn_guard.write_set.get(key) {
            let result = value.clone();
            txn_guard.add_read(key);
            return Ok(Some(result));
        }
        
        // Read from version store
        let snapshot_id = txn_guard.snapshot_id;
        txn_guard.add_read(key);
        drop(txn_guard);
        
        let version_store = self.version_store.read().unwrap();
        let committed = self.committed_txns.read().unwrap();
        
        if let Some(chain) = version_store.get(key) {
            if let Some(version) = chain.get_visible_version(snapshot_id, &committed) {
                Ok(Some(version.value.clone()))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }

    /// Write a key-value pair within a transaction
    pub fn write(&self, txn: &Arc<Mutex<Transaction>>, key: &str, value: Vec<u8>) -> Result<(), TransactionError> {
        let mut txn_guard = txn.lock().unwrap();
        
        if txn_guard.state != TransactionState::Active {
            return Err(TransactionError::TransactionNotActive);
        }
        
        txn_guard.add_write(key, value);
        Ok(())
    }

    /// Commit a transaction
    pub fn commit_transaction(&self, txn: &Arc<Mutex<Transaction>>) -> Result<(), TransactionError> {
        let mut txn_guard = txn.lock().unwrap();
        let txn_id = txn_guard.id;
        
        if txn_guard.state != TransactionState::Active {
            return Err(TransactionError::TransactionNotActive);
        }
        
        // Phase 1: Validation and preparation
        txn_guard.state = TransactionState::Preparing;
        
        // Validate no conflicts with committed transactions
        self.validate_transaction(&txn_guard)?;
        
        // Phase 2: Commit
        if txn_guard.has_writes() {
            self.apply_writes(&txn_guard)?;
        }
        
        txn_guard.state = TransactionState::Committed;
        let commit_time = self.commit_counter.fetch_add(1, Ordering::SeqCst);
        
        // Update global state
        {
            let mut committed = self.committed_txns.write().unwrap();
            committed.insert(txn_id);
        }
        
        {
            let mut active = self.active_txns.write().unwrap();
            active.remove(&txn_id);
        }
        
        // Advance snapshot counter for new readers
        self.next_snapshot_id.store(commit_time + 1, Ordering::SeqCst);
        
        Ok(())
    }

    /// Abort a transaction
    pub fn abort_transaction(&self, txn: &Arc<Mutex<Transaction>>) -> Result<(), TransactionError> {
        let mut txn_guard = txn.lock().unwrap();
        let txn_id = txn_guard.id;
        
        txn_guard.state = TransactionState::Aborted;
        
        // Clean up copy-on-write extents
        {
            let mut cow_extents = self.cow_extents.write().unwrap();
            for extent_id in &txn_guard.cow_extents {
                cow_extents.remove(extent_id);
            }
        }
        
        // Remove from active transactions
        {
            let mut active = self.active_txns.write().unwrap();
            active.remove(&txn_id);
        }
        
        Ok(())
    }

    /// Validate transaction for conflicts
    fn validate_transaction(&self, txn: &Transaction) -> Result<(), TransactionError> {
        let version_store = self.version_store.read().unwrap();
        let committed = self.committed_txns.read().unwrap();
        
        // Check for write-write conflicts
        for key in txn.write_set.keys() {
            if let Some(chain) = version_store.get(key) {
                if let Some(latest) = chain.get_latest_version() {
                    if committed.contains(&latest.created_by) && 
                       latest.created_by > txn.snapshot_id {
                        return Err(TransactionError::WriteConflict);
                    }
                }
            }
        }
        
        // Check for read-write conflicts (anti-dependency)
        for key in &txn.read_set {
            if let Some(chain) = version_store.get(key) {
                if let Some(latest) = chain.get_latest_version() {
                    if committed.contains(&latest.created_by) && 
                       latest.created_by > txn.snapshot_id {
                        return Err(TransactionError::ReadConflict);
                    }
                }
            }
        }
        
        Ok(())
    }

    /// Apply transaction writes to version store
    fn apply_writes(&self, txn: &Transaction) -> Result<(), TransactionError> {
        let mut version_store = self.version_store.write().unwrap();
        
        for (key, value) in &txn.write_set {
            let morton_code = compute_morton_code(key, txn.start_time);
            let version = Version::new(txn.id, value.clone(), morton_code);
            
            let chain = version_store
                .entry(key.clone())
                .or_insert_with(|| VersionChain::new(key.clone()));
            
            chain.add_version(version);
        }
        
        Ok(())
    }

    /// Get statistics
    pub fn stats(&self) -> TransactionStats {
        let active = self.active_txns.read().unwrap();
        let committed = self.committed_txns.read().unwrap();
        let versions = self.version_store.read().unwrap();
        
        TransactionStats {
            active_transactions: active.len(),
            committed_transactions: committed.len(),
            total_versions: versions.values().map(|c| c.versions.len()).sum(),
            next_txn_id: self.next_txn_id.load(Ordering::SeqCst),
            next_snapshot_id: self.next_snapshot_id.load(Ordering::SeqCst),
        }
    }
}

/// Transaction statistics
#[derive(Debug, Clone)]
pub struct TransactionStats {
    pub active_transactions: usize,
    pub committed_transactions: usize,
    pub total_versions: usize,
    pub next_txn_id: u64,
    pub next_snapshot_id: u64,
}

/// Transaction errors
#[derive(Debug)]
pub enum TransactionError {
    TransactionNotActive,
    WriteConflict,
    ReadConflict,
    ExtentError(ExtentError),
    InternalError(String),
}

impl From<ExtentError> for TransactionError {
    fn from(err: ExtentError) -> Self {
        TransactionError::ExtentError(err)
    }
}

impl std::fmt::Display for TransactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionError::TransactionNotActive => write!(f, "Transaction is not active"),
            TransactionError::WriteConflict => write!(f, "Write conflict detected"),
            TransactionError::ReadConflict => write!(f, "Read conflict detected"),
            TransactionError::ExtentError(e) => write!(f, "Extent error: {}", e),
            TransactionError::InternalError(msg) => write!(f, "Internal error: {}", msg),
        }
    }
}

impl std::error::Error for TransactionError {}

/// Helper function to compute Morton code for a key
fn compute_morton_code(key: &str, timestamp: u64) -> u64 {
    use crate::encoding::morton_t_encode;
    morton_t_encode(key, timestamp).value()
}

/// Get current timestamp in microseconds
fn current_timestamp_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_creation() {
        let tm = TransactionManager::new();
        let txn = tm.begin_transaction().unwrap();
        
        let guard = txn.lock().unwrap();
        assert_eq!(guard.state, TransactionState::Active);
        assert_eq!(guard.id, 1);
    }

    #[test]
    fn test_read_write_transaction() {
        let tm = TransactionManager::new();
        let txn = tm.begin_transaction().unwrap();
        
        // Write a value
        tm.write(&txn, "key1", b"value1".to_vec()).unwrap();
        
        // Read it back (read your own writes)
        let value = tm.read(&txn, "key1").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
        
        // Commit
        tm.commit_transaction(&txn).unwrap();
    }

    #[test]
    fn test_transaction_isolation() {
        let tm = TransactionManager::new();
        
        // Transaction 1 writes
        let txn1 = tm.begin_transaction().unwrap();
        tm.write(&txn1, "key1", b"value1".to_vec()).unwrap();
        tm.commit_transaction(&txn1).unwrap();
        
        // Transaction 2 starts after txn1 commits
        let txn2 = tm.begin_transaction().unwrap();
        let value = tm.read(&txn2, "key1").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_write_conflict_detection() {
        let tm = TransactionManager::new();
        
        // Both transactions start at same time
        let txn1 = tm.begin_transaction().unwrap();
        let txn2 = tm.begin_transaction().unwrap();
        
        // Both write to same key
        tm.write(&txn1, "key1", b"value1".to_vec()).unwrap();
        tm.write(&txn2, "key1", b"value2".to_vec()).unwrap();
        
        // First commit succeeds
        tm.commit_transaction(&txn1).unwrap();
        
        // Second commit should fail due to write conflict
        let result = tm.commit_transaction(&txn2);
        assert!(matches!(result, Err(TransactionError::WriteConflict)));
    }

    #[test]
    fn test_transaction_abort() {
        let tm = TransactionManager::new();
        let txn = tm.begin_transaction().unwrap();
        
        tm.write(&txn, "key1", b"value1".to_vec()).unwrap();
        tm.abort_transaction(&txn).unwrap();
        
        let guard = txn.lock().unwrap();
        assert_eq!(guard.state, TransactionState::Aborted);
    }

    #[test]
    fn test_version_visibility() {
        let committed_txns = [1, 2, 3].iter().cloned().collect();
        
        let version = Version::new(2, b"test".to_vec(), 12345);
        
        // Visible to snapshot after creation
        assert!(version.is_visible(3, &committed_txns));
        
        // Not visible to snapshot before creation
        assert!(!version.is_visible(1, &committed_txns));
        
        // Not visible if creator not committed
        let uncommitted_txns = [1, 3].iter().cloned().collect();
        assert!(!version.is_visible(3, &uncommitted_txns));
    }
}