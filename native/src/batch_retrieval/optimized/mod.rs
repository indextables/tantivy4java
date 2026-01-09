//! Optimized batch document retrieval with persistent cache
//!
//! This module provides full-featured batch retrieval with:
//! - Multi-tier caching (memory + Quickwit storage)
//! - Smart range consolidation
//! - Async parallel processing
//! - Performance metrics

mod cache;
mod direct;
mod errors;
mod parallel;
mod range_consolidator;
mod retriever;
mod types;

// Re-export all public types
pub use cache::{QuickwitCacheConfig, QuickwitPersistentCacheManager};
pub use direct::{DirectCacheConfig, DirectRangeCacheManager};
pub use errors::{
    BatchRetrievalError, BatchRetrievalMetrics, BatchRetrievalResult, CacheOperationResult,
};
pub use parallel::AsyncParallelRangeFetcher;
pub use range_consolidator::SmartRangeConsolidator;
pub use retriever::{OptimizedBatchConfig, OptimizedBatchRetriever};
pub use types::{DocAddress, DocumentRange, SmartRangeConfig};
