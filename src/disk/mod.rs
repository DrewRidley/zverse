//! Disk storage module for ZVerse temporal database
//!
//! This module provides memory-mapped disk storage capabilities that leverage
//! temporal locality for efficient access to datasets larger than available RAM.

pub mod config;
pub mod format;
pub mod mmap_partition;
pub mod file_manager;
pub mod temporal_cache;
pub mod durability;
pub mod integrity;

// Public API exports
pub use config::{DiskConfig, MemoryConfig, DurabilityConfig, TemporalConfig, PerformanceConfig};
pub use format::{PartitionHeader, RecordHeader, IndexEntry, FormatError};
pub use mmap_partition::{MmapPartition, MmapError};
pub use file_manager::{FileManager, FileError};
pub use temporal_cache::{TemporalCache, TemporalTiers};
pub use durability::{DurabilityManager, SyncStrategy};
pub use integrity::{IntegrityChecker, IntegrityError};

use std::path::PathBuf;
use crate::morton::MortonError;

/// Error types specific to disk storage operations
#[derive(Debug, Clone, PartialEq)]
pub enum DiskError {
    /// Memory mapping operation failed
    MemoryMappingFailed(String),
    /// File I/O operation failed
    FileOperation(String),
    /// Data format is corrupted or invalid
    CorruptedData(String),
    /// Insufficient disk space
    InsufficientSpace(u64),
    /// Recovery operation failed
    RecoveryFailed(String),
    /// Configuration is invalid
    InvalidConfiguration(String),
    /// Platform-specific error
    PlatformError(String),
}

impl std::fmt::Display for DiskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiskError::MemoryMappingFailed(msg) => write!(f, "Memory mapping failed: {}", msg),
            DiskError::FileOperation(msg) => write!(f, "File operation failed: {}", msg),
            DiskError::CorruptedData(msg) => write!(f, "Corrupted data: {}", msg),
            DiskError::InsufficientSpace(needed) => write!(f, "Insufficient disk space: {} bytes needed", needed),
            DiskError::RecoveryFailed(msg) => write!(f, "Recovery failed: {}", msg),
            DiskError::InvalidConfiguration(msg) => write!(f, "Invalid configuration: {}", msg),
            DiskError::PlatformError(msg) => write!(f, "Platform error: {}", msg),
        }
    }
}

impl std::error::Error for DiskError {}

impl From<DiskError> for MortonError {
    fn from(_err: DiskError) -> Self {
        MortonError::ConcurrentModification // Map to closest existing error type
    }
}

/// Result type for disk storage operations
pub type DiskResult<T> = Result<T, DiskError>;

/// Main disk storage manager that coordinates memory-mapped partition access
pub struct DiskStorageManager {
    config: DiskConfig,
    file_manager: FileManager,
    temporal_cache: TemporalCache,
    durability_manager: durability::DurabilityManager,
}

impl DiskStorageManager {
    /// Create a new disk storage manager with the given configuration
    pub fn new(config: DiskConfig) -> DiskResult<Self> {
        let file_manager = FileManager::new(&config)?;
        let temporal_cache = TemporalCache::new(&config);
        let durability_manager = durability::DurabilityManager::new(&config);

        Ok(Self {
            config,
            file_manager,
            temporal_cache,
            durability_manager,
        })
    }

    /// Initialize disk storage for a new database
    pub fn initialize(&mut self, data_path: &PathBuf) -> DiskResult<()> {
        self.file_manager.initialize(data_path)?;
        Ok(())
    }

    /// Open existing disk storage
    pub fn open(&mut self, data_path: &PathBuf) -> DiskResult<()> {
        self.file_manager.open(data_path)?;
        // Verify file integrity on startup if configured
        if self.config.durability.verify_on_startup {
            self.verify_integrity()?;
        }
        Ok(())
    }

    /// Get a memory-mapped partition for the given partition ID
    pub fn get_partition(&mut self, partition_id: u32) -> DiskResult<&mut MmapPartition> {
        self.temporal_cache.get_partition(partition_id, &mut self.file_manager)
    }

    /// Sync all memory-mapped partitions to disk
    pub fn sync(&mut self) -> DiskResult<()> {
        self.durability_manager.sync_all(&mut self.temporal_cache)
    }

    /// Verify integrity of all partition files
    pub fn verify_integrity(&self) -> DiskResult<()> {
        self.file_manager.verify_all_partitions()
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> CacheStats {
        self.temporal_cache.stats()
    }

    /// Shutdown disk storage gracefully
    pub fn shutdown(&mut self) -> DiskResult<()> {
        if self.config.durability.sync_on_shutdown {
            self.sync()?;
        }
        self.temporal_cache.clear();
        self.file_manager.shutdown()?;
        Ok(())
    }
}

/// Cache performance statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub hit_rate: f64,
    pub miss_rate: f64,
    pub hot_partitions: usize,
    pub warm_partitions: usize,
    pub cold_partitions: usize,
    pub memory_usage_bytes: usize,
    pub evictions_total: u64,
    pub prefetch_hits: u64,
    pub prefetch_misses: u64,
}

/// Memory mapping statistics
#[derive(Debug, Clone)]
pub struct MmapStats {
    pub mapped_partitions: usize,
    pub total_mapped_bytes: usize,
    pub sync_count: u64,
    pub last_sync_duration: std::time::Duration,
}

/// Constants for disk storage
pub mod constants {
    use std::time::Duration;

    /// Magic number for partition files
    pub const PARTITION_MAGIC: &[u8; 8] = b"ZVERSEDB";
    
    /// Current format version
    pub const FORMAT_VERSION: u32 = 1;
    
    /// Default partition size (256 MB)
    pub const DEFAULT_PARTITION_SIZE: usize = 256 * 1024 * 1024;
    
    /// Cache line size for alignment
    pub const CACHE_LINE_SIZE: usize = 64;
    
    /// Default flush interval (5 minutes)
    pub const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_secs(300);
    
    /// Default grace period for late data (30 seconds)
    pub const DEFAULT_GRACE_PERIOD: Duration = Duration::from_secs(30);
    
    /// Maximum unflushed records before forced flush
    pub const DEFAULT_MAX_UNFLUSHED: usize = 1_000_000;
}