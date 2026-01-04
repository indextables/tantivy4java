// types.rs - Shared data structures for split_searcher module

use std::sync::Arc;
use std::collections::HashMap;
use std::path::PathBuf;
use std::ops::Range;
use quickwit_storage::{Storage, ByteRangeCache};
use crate::standalone_searcher::StandaloneSearcher;

/// Simple data structure to hold search results for JNI integration
#[derive(Debug)]
pub struct SearchResultData {
    pub hits: Vec<SearchHit>,
    pub total_hits: u64,
}

/// Individual search hit data
#[derive(Debug)]
pub struct SearchHit {
    pub score: f32,
    pub segment_ord: u32,
    pub doc_id: u32,
}

/// Enhanced SearchResult data structure that includes both hits and aggregations
#[derive(Debug)]
pub struct EnhancedSearchResult {
    pub hits: Vec<(f32, tantivy::DocAddress)>,
    pub aggregation_results: Option<Vec<u8>>, // Postcard-serialized aggregation results
    pub aggregation_json: Option<String>, // Original aggregation request JSON
}

/// Cached searcher context for efficient single document retrieval
/// Contains pre-resolved storage, index, and searcher to avoid repeated resolution
pub(crate) struct CachedSearcherContext {
    pub(crate) standalone_searcher: Arc<StandaloneSearcher>,
    // ✅ CRITICAL FIX: Removed runtime field - using shared global runtime instead
    pub(crate) split_uri: String,
    pub(crate) aws_config: HashMap<String, String>,
    pub(crate) footer_start: u64,
    pub(crate) footer_end: u64,
    pub(crate) doc_mapping_json: Option<String>,
    pub(crate) cached_storage: Arc<dyn Storage>,
    pub(crate) cached_index: Arc<tantivy::Index>,
    pub(crate) cached_searcher: Arc<tantivy::Searcher>,
    // 🚀 BATCH OPTIMIZATION FIX: Store ByteRangeCache and bundle file offsets
    // This allows prefetch to populate the same cache that doc_async uses
    pub(crate) byte_range_cache: Option<ByteRangeCache>,
    pub(crate) bundle_file_offsets: HashMap<PathBuf, Range<u64>>,
}

impl CachedSearcherContext {
    /// Clear the L1 ByteRangeCache to free memory.
    ///
    /// This should be called after prewarm operations to prevent unbounded memory growth.
    /// The prewarmed data is safely persisted in L2 disk cache, so clearing L1 is safe.
    /// Subsequent queries will populate L1 on-demand from L2.
    pub fn clear_l1_cache(&self) {
        if let Some(cache) = &self.byte_range_cache {
            cache.clear();
        }
    }
}
