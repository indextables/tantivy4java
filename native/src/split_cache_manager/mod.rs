//! Split Cache Manager module - Global cache management for split files
//!
//! This module has been refactored from a single file into submodules:
//! - manager.rs: Core GlobalSplitCacheManager struct and implementation
//! - jni_lifecycle.rs: JNI functions for create/close operations
//! - jni_cache_ops.rs: JNI functions for cache operations (stats, eviction, preload, search)
//! - jni_metrics.rs: JNI functions for all metrics (batch, searcher, L1, L2, storage)

mod jni_cache_ops;
mod jni_lifecycle;
mod jni_metrics;
mod manager;

// Re-export public items
pub use jni_cache_ops::*;
pub use jni_lifecycle::*;
pub use jni_metrics::*;
pub use manager::{
    record_batch_metrics, GlobalCacheStats, GlobalSplitCacheManager, BATCH_METRICS,
    CACHE_MANAGERS, GLOBAL_BATCH_METRICS,
};
