// standalone_searcher/mod.rs - Standalone searcher for direct Quickwit split searching
//
// This module provides a standalone searcher that can search Quickwit split files
// without requiring the full SplitCacheManager infrastructure.

pub mod jni;
pub mod searcher;

// Re-export main types for convenience
pub use searcher::{
    StandaloneSearchConfig, StandaloneSearcher, CacheConfig, ResourceConfig,
    TimeoutConfig, WarmupConfig, SplitSearchMetadata, SearchResult,
    resolve_storage_for_split,
};
