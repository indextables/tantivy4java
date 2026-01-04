// split_searcher - Replacement JNI methods that use StandaloneSearcher internally
// This replaces the old convoluted SplitSearcher implementation with clean StandaloneSearcher calls

// Submodules
pub mod cache_config;
pub mod byterange_cache;
pub mod searcher_cache;
pub mod types;
pub mod query_utils;
pub mod document_retrieval;
pub mod schema_creation;
pub mod aggregation;
pub mod jni_lifecycle;
pub mod jni_search;
pub mod jni_prewarm;
pub mod jni_utils;
pub mod async_impl;

// Re-exports - items used by other modules in the crate
pub(crate) use types::CachedSearcherContext;
pub use types::EnhancedSearchResult;
pub use searcher_cache::{
    clear_searcher_cache,
    SEARCHER_CACHE_HITS, SEARCHER_CACHE_MISSES, SEARCHER_CACHE_EVICTIONS,
};
pub use async_impl::{
    perform_search_async_impl_leaf_response, perform_schema_retrieval_async_impl_thread_safe,
    perform_real_quickwit_search_with_aggregations, perform_doc_retrieval_async_impl_thread_safe,
};

// Cache configuration is now in cache_config.rs submodule

// ByteRange cache merging is now in byterange_cache.rs submodule

// Searcher cache and types are now in searcher_cache.rs and types.rs submodules

// JNI lifecycle functions (createNativeWithSharedCache, closeNative, validateSplitNative, getCacheStatsNative)
// are now in jni_lifecycle.rs submodule

// Search JNI functions (searchWithQueryAst, searchWithSplitQuery, searchWithAggregations, getSchemaFromNative)
// are now in jni_search.rs submodule

// Prewarm JNI functions (preloadComponentsNative, preloadFieldsNative)
// are now in jni_prewarm.rs submodule


// JNI utility functions (metadata, tokenize, cache status, etc.)
// are now in jni_utils.rs submodule

// Aggregation functions moved to aggregation.rs submodule

// Async implementation functions (search, doc retrieval, schema retrieval, Quickwit search)
// are now in async_impl.rs submodule

