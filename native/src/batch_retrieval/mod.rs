//! Batch document retrieval module for tantivy4java
//!
//! This module provides two implementations for batch document retrieval:
//! - `simple`: Lightweight optimization reducing S3 requests via range consolidation
//! - `optimized`: Full-featured retrieval with persistent cache and async parallel processing

mod optimized;
pub mod simple;

// Re-export from optimized module
pub use optimized::{
    AsyncParallelRangeFetcher, BatchRetrievalError, BatchRetrievalMetrics, BatchRetrievalResult,
    CacheOperationResult, DirectCacheConfig, DirectRangeCacheManager, DocAddress, DocumentRange,
    OptimizedBatchConfig, OptimizedBatchRetriever, QuickwitCacheConfig,
    QuickwitPersistentCacheManager, SmartRangeConfig, SmartRangeConsolidator,
};

// Re-export from simple module
pub use simple::{
    prefetch_ranges_with_cache, BatchOptimizationMetrics, PrefetchRange, PrefetchStats,
    SimpleBatchConfig, SimpleBatchOptimizer,
};
