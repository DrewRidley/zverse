//! Configuration for disk storage using memory-mapped files

use std::path::PathBuf;
use std::time::Duration;

/// Configuration for disk-based storage using memory mapping
#[derive(Debug, Clone)]
pub struct DiskConfig {
    /// Base directory for storing partition files
    pub data_path: PathBuf,
    
    /// Maximum size of each partition file in bytes (default: 256MB)
    pub max_partition_size: usize,
    
    /// Growth factor when expanding partition files (default: 1.5)
    pub partition_growth_factor: f64,
    
    /// Memory management configuration
    pub memory: MemoryConfig,
    
    /// Durability and sync configuration
    pub durability: DurabilityConfig,
    
    /// Temporal cache configuration
    pub temporal: TemporalConfig,
    
    /// Performance tuning options
    pub performance: PerformanceConfig,
}

/// Memory management configuration
#[derive(Debug, Clone)]
pub struct MemoryConfig {
    /// Maximum number of hot partitions to keep in memory
    pub hot_partition_limit: usize,
    
    /// Maximum number of warm partitions to keep mapped
    pub warm_partition_limit: usize,
    
    /// Total memory budget for cached partitions in bytes
    pub memory_budget_bytes: usize,
    
    /// Use MAP_POPULATE to prefault pages during mapping
    pub use_populate: bool,
    
    /// Use huge pages if available (Linux only)
    pub use_huge_pages: bool,
}

/// Durability and synchronization configuration
#[derive(Debug, Clone)]
pub struct DurabilityConfig {
    /// How often to call msync() for durability (default: 30 seconds)
    pub sync_interval: Duration,
    
    /// Perform synchronous msync() on clean shutdown
    pub sync_on_shutdown: bool,
    
    /// Use MS_ASYNC instead of MS_SYNC for better performance
    pub use_async_sync: bool,
    
    /// Force sync after this many writes (0 = disabled)
    pub force_sync_after_writes: usize,
    
    /// Verify file integrity on startup
    pub verify_on_startup: bool,
}

/// Temporal access pattern configuration
#[derive(Debug, Clone)]
pub struct TemporalConfig {
    /// Time window for hot data classification
    pub hot_data_window: Duration,
    
    /// Time window for warm data classification  
    pub warm_data_window: Duration,
    
    /// Prefetch window for temporal locality optimization
    pub prefetch_window: Duration,
    
    /// Enable temporal-aware eviction policy
    pub temporal_eviction: bool,
}

/// Performance tuning configuration
#[derive(Debug, Clone)]
pub struct PerformanceConfig {
    /// Align records to cache line boundaries
    pub cache_line_alignment: bool,
    
    /// Read-ahead size for sequential access patterns
    pub readahead_size: usize,
    
    /// Enable background prefetching threads
    pub background_prefetch: bool,
    
    /// Number of background I/O threads
    pub background_threads: usize,
    
    /// Use memory advise hints (MADV_SEQUENTIAL, etc.)
    pub use_memory_advise: bool,
}

/// Temporal region classification
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TemporalTiers {
    Hot,    // Recently accessed, keep in memory
    Warm,   // Medium age, keep mapped but may evict
    Cold,   // Old data, load on demand
}

impl Default for DiskConfig {
    fn default() -> Self {
        Self {
            data_path: PathBuf::from("zverse_data"),
            max_partition_size: 256 * 1024 * 1024, // 256MB
            partition_growth_factor: 1.5,
            memory: MemoryConfig::default(),
            durability: DurabilityConfig::default(),
            temporal: TemporalConfig::default(),
            performance: PerformanceConfig::default(),
        }
    }
}

impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            hot_partition_limit: 64,                    // 64 hot partitions
            warm_partition_limit: 256,                  // 256 warm partitions  
            memory_budget_bytes: 1024 * 1024 * 1024,   // 1GB budget
            use_populate: false,                        // Don't prefault by default
            use_huge_pages: false,                      // Conservative default
        }
    }
}

impl Default for DurabilityConfig {
    fn default() -> Self {
        Self {
            sync_interval: Duration::from_secs(30),     // Sync every 30 seconds
            sync_on_shutdown: true,                     // Always sync on shutdown
            use_async_sync: true,                       // Use async for performance
            force_sync_after_writes: 0,                 // Disabled by default
            verify_on_startup: true,                    // Always verify integrity
        }
    }
}

impl Default for TemporalConfig {
    fn default() -> Self {
        Self {
            hot_data_window: Duration::from_secs(3600),     // 1 hour hot
            warm_data_window: Duration::from_secs(86400),   // 1 day warm
            prefetch_window: Duration::from_secs(1800),     // 30 min prefetch
            temporal_eviction: true,                        // Enable temporal eviction
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            cache_line_alignment: true,                 // Align for better cache performance
            readahead_size: 128 * 1024,                // 128KB readahead
            background_prefetch: true,                  // Enable background prefetch
            background_threads: 2,                      // 2 background threads
            use_memory_advise: true,                    // Use memory hints
        }
    }
}

impl DiskConfig {
    /// Create a configuration optimized for high-performance temporal workloads
    pub fn high_performance() -> Self {
        Self {
            memory: MemoryConfig {
                hot_partition_limit: 128,
                warm_partition_limit: 512,
                memory_budget_bytes: 4 * 1024 * 1024 * 1024, // 4GB
                use_populate: true,
                use_huge_pages: true,
                ..Default::default()
            },
            durability: DurabilityConfig {
                sync_interval: Duration::from_secs(60),  // Less frequent sync
                use_async_sync: true,
                ..Default::default()
            },
            performance: PerformanceConfig {
                readahead_size: 512 * 1024,             // 512KB readahead
                background_threads: 4,                   // More background threads
                ..Default::default()
            },
            ..Default::default()
        }
    }
    
    /// Create a configuration optimized for durability over performance
    pub fn high_durability() -> Self {
        Self {
            durability: DurabilityConfig {
                sync_interval: Duration::from_secs(5),   // Frequent sync
                use_async_sync: false,                   // Synchronous sync
                force_sync_after_writes: 10000,         // Force sync often
                ..Default::default()
            },
            ..Default::default()
        }
    }
    
    /// Create a configuration for memory-constrained environments
    pub fn low_memory() -> Self {
        Self {
            memory: MemoryConfig {
                hot_partition_limit: 16,
                warm_partition_limit: 64,
                memory_budget_bytes: 256 * 1024 * 1024, // 256MB
                use_populate: false,
                ..Default::default()
            },
            max_partition_size: 64 * 1024 * 1024,       // 64MB partitions
            ..Default::default()
        }
    }
    
    /// Validate configuration parameters
    pub fn validate(&self) -> Result<(), String> {
        if self.max_partition_size < 1024 * 1024 {
            return Err("Partition size must be at least 1MB".to_string());
        }
        
        if self.partition_growth_factor < 1.0 || self.partition_growth_factor > 10.0 {
            return Err("Growth factor must be between 1.0 and 10.0".to_string());
        }
        
        if self.memory.hot_partition_limit > self.memory.warm_partition_limit {
            return Err("Hot partition limit cannot exceed warm partition limit".to_string());
        }
        
        if self.memory.memory_budget_bytes < 64 * 1024 * 1024 {
            return Err("Memory budget must be at least 64MB".to_string());
        }
        
        Ok(())
    }
}