//! Storage module for IMEC-v2
//!
//! This module provides the storage layer combining pages, extents, and the storage engine
//! with Morton-T encoding and interval table integration.

pub mod page;
pub mod extent;
pub mod engine;
pub mod file;
pub mod file_engine;

// Re-export key types and functions
pub use page::{
    Page,
    PageType,
    PageHeader,
    Slot,
    PageError,
    PAGE_SIZE,
    SLOT_SIZE,
    MAX_SLOTS_PER_PAGE,
};

pub use extent::{
    Extent,
    ExtentHeader,
    ExtentError,
    ExtentStats,
    DEFAULT_EXTENT_SIZE,
    MIN_EXTENT_SIZE,
    MAX_EXTENT_SIZE,
};

pub use engine::{
    StorageEngine,
    StorageConfig,
    StorageStats,
    StorageError,
};

pub use file::{
    FileStorage,
    FileStorageConfig,
    FileStorageStats,
    Superblock,
    FileStorageError,
};

pub use file_engine::{
    FileEngine,
    FileEngineConfig,
    FileEngineStats,
    FileEngineError,
};

#[cfg(test)]
mod tests {
    use super::*;


    #[test]
    fn test_storage_integration() {
        let mut engine = StorageEngine::default();
        
        // Test basic operations
        engine.put("integration_test", b"test_data").unwrap();
        let value = engine.get("integration_test").unwrap();
        assert_eq!(value, Some(b"test_data".to_vec()));
    }

    #[test] 
    fn test_morton_code_storage() {
        let mut engine = StorageEngine::default();
        
        // Test with keys that will produce different Morton codes
        let keys = vec!["user:alice", "user:bob", "order:123", "product:xyz"];
        
        for (i, key) in keys.iter().enumerate() {
            let value = format!("data_{}", i);
            engine.put(key, value.as_bytes()).unwrap();
        }
        
        // Verify all keys can be retrieved
        for (i, key) in keys.iter().enumerate() {
            let expected_value = format!("data_{}", i);
            let retrieved = engine.get(key).unwrap();
            assert_eq!(retrieved, Some(expected_value.into_bytes()));
        }
        
        // Should have created storage structures
        assert!(engine.extent_count() > 0);
        assert!(engine.interval_count() > 0);
    }

    #[test]
    fn test_page_extent_integration() {
        let mut extent = Extent::new(1, DEFAULT_EXTENT_SIZE).unwrap();
        let page_index = extent.allocate_page(PageType::Leaf).unwrap();
        
        let page = extent.get_page_mut(page_index).unwrap();
        
        // Test slot insertion
        let slot = Slot::new_leaf(12345, 100, 10, 200, 20);
        page.insert_slot(slot).unwrap();
        
        // Verify slot can be found
        assert_eq!(page.search_slot(12345), Some(0));
        assert_eq!(page.search_slot(99999), None);
    }

    #[test]
    fn test_temporal_locality_in_storage() {
        let mut engine = StorageEngine::default();
        
        // Insert multiple versions of the same key rapidly
        for i in 0..5 {
            let value = format!("version_{}", i);
            engine.put("versioned_key", value.as_bytes()).unwrap();
            
            // Small delay to create temporal separation
            std::thread::sleep(std::time::Duration::from_micros(10));
        }
        
        // Should be able to retrieve the key (likely the most recent version)
        let result = engine.get("versioned_key").unwrap();
        assert!(result.is_some());
        
        // The exact version we get depends on temporal locality search
        let value = String::from_utf8(result.unwrap()).unwrap();
        assert!(value.starts_with("version_"));
    }

    #[test]
    fn test_error_propagation() {
        let engine = StorageEngine::default();
        
        // Test that errors propagate correctly through the stack
        let result = engine.get("test");
        assert!(result.is_ok()); // Should return Ok(None), not an error
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_storage_validation() {
        let mut engine = StorageEngine::default();
        
        // Add some data
        engine.put("validation_test", b"validation_data").unwrap();
        
        // Validate entire storage
        assert!(engine.validate().is_ok());
    }
}