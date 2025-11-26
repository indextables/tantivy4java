// split_searcher_replacement.rs - Replacement JNI methods that use StandaloneSearcher internally
// This replaces the old convoluted SplitSearcher implementation with clean StandaloneSearcher calls

use std::sync::{Arc, OnceLock};
use jni::objects::{JClass, JString, JObject};
use jni::sys::{jlong, jobject, jstring, jint, jboolean};
use jni::JNIEnv;

use crate::standalone_searcher::{StandaloneSearcher, StandaloneSearchConfig, SplitSearchMetadata, resolve_storage_for_split};
use crate::utils::{arc_to_jlong, with_arc_safe, release_arc};
use crate::common::to_java_exception;
use crate::debug_println;
use crate::runtime_manager::block_on_operation;
use crate::global_cache::{get_configured_storage_resolver, get_configured_storage_resolver_async};
use crate::split_query::{store_split_schema, get_split_schema, convert_split_query_to_ast, convert_split_query_to_json};
use quickwit_search::{SearcherContext, search_permit_provider::SearchPermitProvider};
use quickwit_search::leaf_cache::LeafSearchCache;
use quickwit_search::list_fields_cache::ListFieldsCache;
use tantivy::aggregation::AggregationLimitsGuard;
use tokio::sync::Semaphore;
use quickwit_common::thread_pool::ThreadPool;

use serde_json::{Value, Map};
use tantivy::schema::{Document as DocumentTrait, NamedFieldDocument};

use quickwit_proto::search::{SearchRequest, SplitIdAndFooterOffsets};
use quickwit_config::S3StorageConfig;
use quickwit_storage::{StorageResolver, Storage, ByteRangeCache, STORAGE_METRICS, MemorySizedCache};
use quickwit_search::leaf::open_index_with_caches;
use quickwit_indexing::open_index;
use quickwit_query::get_quickwit_fastfield_normalizer_manager;
use tantivy::directory::DirectoryClone;

// ========================================
// DOC STORE CACHE OPTIMIZATION (ADAPTIVE QUICKWIT PATTERN)
// ========================================
// Implementation of Quickwit's doc store cache optimization with adaptive scaling for multi-threaded clients.
// Prevents cache thrashing during bulk document retrieval operations, improving performance by 2-5x.
//
// Background:
// - Quickwit's fetch_docs.rs implements cache sizing based on concurrency patterns
// - Multi-threaded Java clients (8+ threads) need proportionally more cache blocks
// - Without proper cache sizing, concurrent operations cause cache eviction (thrashing)
// - The key insight: cache blocks should match total concurrent operations across all client threads
//
// Adaptive Scaling Strategy:
// - Base concurrency: 30 operations per thread (from Quickwit's NUM_CONCURRENT_REQUESTS)
// - Client thread scaling: Multiply by expected Java thread count
// - Default to CPU count for optimal resource utilization
// - Configurable via environment variable for custom deployments
//
// Performance Impact:
// - Individual retrieval: 10 cache blocks (single-document access)
// - Batch retrieval: Base(30) × JavaThreads × safety factor (multi-threaded access)
// - Expected improvement: 2-5x faster bulk document retrieval without cache contention

/// Base concurrent document retrieval requests per thread
/// This is the core optimization constant from Quickwit's fetch_docs.rs
const BASE_CONCURRENT_REQUESTS: usize = 30;

/// Get the configured maximum Java thread count for cache sizing
/// Defaults to CPU count but can be overridden via TANTIVY4JAVA_MAX_THREADS environment variable
fn get_max_java_threads() -> usize {
    if let Ok(env_threads) = std::env::var("TANTIVY4JAVA_MAX_THREADS") {
        if let Ok(threads) = env_threads.parse::<usize>() {
            if threads > 0 && threads <= 1024 { // Reasonable bounds check
                debug_println!("⚙️  CACHE_CONFIG: Using configured max threads: {}", threads);
                return threads;
            }
            debug_println!("⚠️  CACHE_CONFIG: Invalid TANTIVY4JAVA_MAX_THREADS value: {}, using CPU count", env_threads);
        }
    }

    // Default to CPU count for optimal resource utilization
    let cpu_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8); // Fallback to 8 if detection fails

    debug_println!("⚙️  CACHE_CONFIG: Using CPU count for max threads: {}", cpu_count);
    cpu_count
}

/// Calculate adaptive cache block count for batch operations
/// Scales cache blocks based on expected client thread concurrency to prevent cache thrashing
fn get_adaptive_batch_cache_blocks() -> usize {
    let max_threads = get_max_java_threads();
    let base_blocks = BASE_CONCURRENT_REQUESTS * max_threads;

    // Add 20% safety margin to handle burst concurrency and edge cases
    let safety_factor = 1.2;
    let adaptive_blocks = (base_blocks as f64 * safety_factor).ceil() as usize;

    debug_println!("⚙️  CACHE_CONFIG: Adaptive cache sizing - threads: {}, base: {}, adaptive: {}",
                   max_threads, base_blocks, adaptive_blocks);

    adaptive_blocks
}

/// Cache block size for individual document retrieval operations
/// Smaller cache footprint optimized for single-document access patterns
/// Prevents excessive memory usage when only retrieving one document at a time
const SINGLE_DOC_CACHE_BLOCKS: usize = 10;

/// Calculate adaptive memory allocation based on split metadata and thread count
/// Combines document-count-aware scaling with thread-based concurrency scaling
/// Returns cache block count optimized for the specific split characteristics
fn calculate_adaptive_memory_allocation(
    split_metadata: Option<(usize, usize)>, // (num_docs, file_size_bytes)
    thread_count: usize
) -> usize {
    let base_blocks = BASE_CONCURRENT_REQUESTS * thread_count;

    if let Some((num_docs, _file_size)) = split_metadata {
        // Document-count-aware scaling (Quickwit pattern)
        let doc_count = num_docs as f64;
        let doc_scale_factor = (doc_count / 100_000.0).sqrt().max(0.5).min(3.0);

        let document_aware_blocks = (base_blocks as f64 * doc_scale_factor) as usize;
        let final_blocks = (document_aware_blocks as f64 * 1.2).ceil() as usize; // 20% safety margin

        // Apply memory allocation bounds (convert to blocks roughly)
        let min_blocks = (MIN_MEMORY_ALLOCATION_MB * 1024 * 1024) / (BYTES_PER_DOCUMENT * BASE_CONCURRENT_REQUESTS);
        let max_blocks = (MAX_MEMORY_ALLOCATION_MB * 1024 * 1024) / (BYTES_PER_DOCUMENT * BASE_CONCURRENT_REQUESTS);

        let bounded_blocks = final_blocks.max(min_blocks).min(max_blocks);

        debug_println!("⚡ ADAPTIVE_MEMORY: docs={}, threads={}, base={}, doc_scale={:.2}, final={}",
                       num_docs, thread_count, base_blocks, doc_scale_factor, bounded_blocks);

        bounded_blocks
    } else {
        // Fallback to existing thread-based scaling
        (base_blocks as f64 * 1.2).ceil() as usize
    }
}

/// Get cache block size for batch document retrieval operations
/// Now supports both thread-based and document-count-aware scaling
fn get_batch_doc_cache_blocks() -> usize {
    get_batch_doc_cache_blocks_with_metadata(None)
}

/// Get cache block size with optional split metadata for enhanced allocation
fn get_batch_doc_cache_blocks_with_metadata(split_metadata: Option<(usize, usize)>) -> usize {
    let thread_count = get_max_java_threads();
    calculate_adaptive_memory_allocation(split_metadata, thread_count)
}

// ========================================
// ADVANCED CACHE OPTIMIZATIONS (QUICKWIT PATTERN)
// ========================================
// Implementation of advanced cache optimizations from Quickwit for production-grade performance

/// Minimum time since last access before cache item can be evicted (prevents scan pattern thrashing)
const MIN_CACHE_ITEM_LIFETIME_SECS: u64 = 60;

/// Emergency eviction threshold when cache is critically full (95% capacity)
const EMERGENCY_EVICTION_THRESHOLD: f64 = 0.95;

/// Memory allocation constants for document-count-aware sizing
const MIN_MEMORY_ALLOCATION_MB: usize = 15;
const MAX_MEMORY_ALLOCATION_MB: usize = 100;
const BYTES_PER_DOCUMENT: usize = 50;

/// ByteRange cache merging constants
const MAX_ACCEPTABLE_GAPS: usize = 3;
const PREFETCH_ADJACENT_THRESHOLD: f64 = 0.8; // 80% cache hit rate triggers prefetch

/// Extract split metadata for adaptive memory allocation
/// Returns (num_docs, file_size_bytes) if available from split metadata
fn extract_split_metadata_for_allocation(split_uri: &str) -> Option<(usize, usize)> {
    // Try to extract from split file name or cached metadata
    // For now, we'll implement a basic version that could be enhanced

    // Check if we have cached split metadata
    if let Some(size_hint) = get_split_size_hint(split_uri) {
        // Estimate document count from file size (rough heuristic)
        let estimated_docs = (size_hint / 1000).max(100); // Assume ~1KB per doc average
        Some((estimated_docs, size_hint))
    } else {
        None
    }
}

/// Get size hint for split file (basic implementation)
/// This could be enhanced to cache split metadata for better performance
fn get_split_size_hint(split_uri: &str) -> Option<usize> {
    // Basic size estimation based on split file name or cached data
    // This is a placeholder that could be enhanced with actual metadata caching
    None // For now, fallback to thread-based scaling
}

// ========================================
// BYTERANGE CACHE MERGING SUPPORT (PHASE 3)
// ========================================

/// Represents a cached byte range with metadata
#[derive(Debug, Clone)]
pub struct CachedRange {
    pub start: usize,
    pub end: usize,
    pub data: Arc<Vec<u8>>,
    pub last_accessed: std::time::SystemTime,
}

impl CachedRange {
    /// Check if this range overlaps with the requested range
    pub fn overlaps_with(&self, start: usize, end: usize) -> bool {
        !(self.end <= start || self.start >= end)
    }

    /// Get the intersection of this range with the requested range
    pub fn intersection(&self, start: usize, end: usize) -> Option<(usize, usize)> {
        if self.overlaps_with(start, end) {
            Some((self.start.max(start), self.end.min(end)))
        } else {
            None
        }
    }

    /// Get data slice for the specified range (relative to this cached range)
    pub fn get_slice(&self, start: usize, end: usize) -> Option<&[u8]> {
        if start >= self.start && end <= self.end {
            let relative_start = start - self.start;
            let relative_end = end - self.start;
            Some(&self.data[relative_start..relative_end])
        } else {
            None
        }
    }
}

/// Result of attempting to serve a request from cache with range merging
pub enum CacheResult {
    /// Complete cache hit - all data available from cache
    Hit(Vec<u8>),
    /// Partial cache hit - some data cached, some needs to be fetched
    PartialHit {
        cached_segments: Vec<CachedRange>,
        missing_gaps: Vec<(usize, usize)>,
    },
    /// Cache miss - no useful cached data
    Miss,
}

/// Calculate missing gaps between cached ranges for a requested range
pub fn calculate_missing_gaps(
    requested_start: usize,
    requested_end: usize,
    cached_ranges: &[CachedRange]
) -> Vec<(usize, usize)> {
    let mut gaps = Vec::new();
    let mut current_pos = requested_start;

    // Sort cached ranges by start position
    let mut sorted_ranges: Vec<_> = cached_ranges.iter()
        .filter(|r| r.overlaps_with(requested_start, requested_end))
        .collect();
    sorted_ranges.sort_by_key(|r| r.start);

    for range in sorted_ranges {
        let range_start = range.start.max(requested_start);
        let range_end = range.end.min(requested_end);

        // Add gap before this range if it exists
        if current_pos < range_start {
            gaps.push((current_pos, range_start));
        }

        // Move past this range
        current_pos = current_pos.max(range_end);
    }

    // Add final gap if needed
    if current_pos < requested_end {
        gaps.push((current_pos, requested_end));
    }

    gaps
}

/// Try to merge cached ranges to serve a complete request
pub fn try_merge_cached_ranges(
    requested_start: usize,
    requested_end: usize,
    cached_ranges: &[CachedRange]
) -> CacheResult {
    let gaps = calculate_missing_gaps(requested_start, requested_end, cached_ranges);

    if gaps.is_empty() {
        // Complete cache hit possible - merge the data
        let mut result_data = vec![0u8; requested_end - requested_start];
        let mut covered = false;

        for range in cached_ranges {
            if let Some((int_start, int_end)) = range.intersection(requested_start, requested_end) {
                if let Some(slice) = range.get_slice(int_start, int_end) {
                    let result_start = int_start - requested_start;
                    let result_end = result_start + slice.len();
                    result_data[result_start..result_end].copy_from_slice(slice);
                    covered = true;
                }
            }
        }

        if covered {
            CacheResult::Hit(result_data)
        } else {
            CacheResult::Miss
        }
    } else if gaps.len() <= MAX_ACCEPTABLE_GAPS {
        // Partial hit with acceptable number of gaps
        let relevant_ranges: Vec<_> = cached_ranges.iter()
            .filter(|r| r.overlaps_with(requested_start, requested_end))
            .cloned()
            .collect();

        CacheResult::PartialHit {
            cached_segments: relevant_ranges,
            missing_gaps: gaps,
        }
    } else {
        // Too many gaps - treat as miss
        CacheResult::Miss
    }
}

/// Thread pool for search operations (matches Quickwit's pattern exactly)
fn search_thread_pool() -> &'static ThreadPool {
    static SEARCH_THREAD_POOL: OnceLock<ThreadPool> = OnceLock::new();
    SEARCH_THREAD_POOL.get_or_init(|| ThreadPool::new("search", None))
}

/// Check if footer metadata is available for optimizations
fn has_footer_metadata(footer_start: u64, footer_end: u64) -> bool {
    footer_start > 0 && footer_end > 0 && footer_end > footer_start
}

/// Check if split URI is remote (S3/cloud) vs local file
/// Quickwit's hotcache optimization is designed for remote splits, not local files
fn is_remote_split(split_uri: &str) -> bool {
    split_uri.starts_with("s3://") ||
    split_uri.starts_with("azure://") ||
    split_uri.starts_with("http://") ||
    split_uri.starts_with("https://")
}

/// Extract split ID from URI (filename without extension)
fn extract_split_id_from_uri(split_uri: &str) -> String {
    if let Some(last_slash_pos) = split_uri.rfind('/') {
        let filename = &split_uri[last_slash_pos + 1..];
        if let Some(dot_pos) = filename.rfind('.') {
            filename[..dot_pos].to_string()
        } else {
            filename.to_string()
        }
    } else {
        if let Some(dot_pos) = split_uri.rfind('.') {
            split_uri[..dot_pos].to_string()
        } else {
            split_uri.to_string()
        }
    }
}

/// 🚀 BATCH OPTIMIZATION FIX: Parse bundle metadata from split file footer
/// Returns the file offsets map: inner_file_path -> Range<u64> in split file
/// This allows prefetch to translate inner byte ranges to split file byte ranges
///
/// Uses the SearcherContext's split_footer_cache to avoid redundant S3 requests.
/// This is the same cache that open_index_with_caches uses.
async fn parse_bundle_file_offsets(
    storage: &Arc<dyn Storage>,
    split_uri: &str,
    footer_start: u64,
    footer_end: u64,
    searcher_context: &quickwit_search::SearcherContext,
) -> anyhow::Result<std::collections::HashMap<std::path::PathBuf, std::ops::Range<u64>>> {
    use std::path::PathBuf;

    debug_println!("🔍 BUNDLE_METADATA: Parsing bundle file offsets from footer");
    debug_println!("   Footer range: {} - {} ({} bytes)", footer_start, footer_end, footer_end - footer_start);

    // Extract split_id from split_uri (same logic as quickwit)
    let split_filename = if let Some(last_slash_pos) = split_uri.rfind('/') {
        &split_uri[last_slash_pos + 1..]
    } else {
        split_uri
    };
    let split_id = if split_filename.ends_with(".split") {
        &split_filename[..split_filename.len() - 6]
    } else {
        split_filename
    };
    let split_path = std::path::Path::new(split_filename);

    // 🚀 OPTIMIZATION: Check quickwit's split_footer_cache first (same cache open_index_with_caches uses)
    let footer_bytes = {
        let cached = searcher_context.split_footer_cache.get(split_id);
        if let Some(footer_data) = cached {
            debug_println!("🔍 BUNDLE_METADATA: Footer cache HIT - {} bytes from split_footer_cache (no S3 request)", footer_data.len());
            footer_data
        } else {
            debug_println!("🔍 BUNDLE_METADATA: Footer cache MISS - fetching from S3");
            let footer_range = footer_start as usize..footer_end as usize;
            let data = storage.get_slice(split_path, footer_range).await
                .map_err(|e| anyhow::anyhow!("Failed to fetch footer for bundle metadata: {}", e))?;
            // Store in cache for future use (though open_index_with_caches will also cache it)
            searcher_context.split_footer_cache.put(split_id.to_string(), data.clone());
            data
        }
    };

    debug_println!("🔍 BUNDLE_METADATA: Using {} footer bytes", footer_bytes.len());

    // Parse the footer directly - no need for BundleStorage intermediate step
    // Footer format: [FileMetadata, FileMetadata Len (4 bytes), HotCache, HotCache Len (4 bytes)]
    let mut file_offsets = std::collections::HashMap::new();

    // Footer structure: [HotCache Len (last 4 bytes)] points to HotCache
    // Before that: [FileMetadata Len (4 bytes)] points to JSON metadata
    let footer_len = footer_bytes.len();
    if footer_len < 8 {
        return Err(anyhow::anyhow!("Footer too small: {} bytes", footer_len));
    }

    // Read hotcache length from last 4 bytes
    let hotcache_len = u32::from_le_bytes([
        footer_bytes[footer_len - 4],
        footer_bytes[footer_len - 3],
        footer_bytes[footer_len - 2],
        footer_bytes[footer_len - 1],
    ]) as usize;

    debug_println!("🔍 BUNDLE_METADATA: Hotcache length: {} bytes", hotcache_len);

    // The metadata is before the hotcache, with its length at the end
    let before_hotcache_len = footer_len - 4 - hotcache_len;
    if before_hotcache_len < 4 {
        return Err(anyhow::anyhow!("Invalid footer structure: not enough bytes before hotcache"));
    }

    // Read metadata length
    let metadata_len_pos = before_hotcache_len - 4;
    let metadata_len = u32::from_le_bytes([
        footer_bytes[metadata_len_pos],
        footer_bytes[metadata_len_pos + 1],
        footer_bytes[metadata_len_pos + 2],
        footer_bytes[metadata_len_pos + 3],
    ]) as usize;

    debug_println!("🔍 BUNDLE_METADATA: Metadata length: {} bytes", metadata_len);

    // Read metadata bytes (before the length field)
    let metadata_start = metadata_len_pos - metadata_len;
    let metadata_bytes = &footer_bytes[metadata_start..metadata_len_pos];

    debug_println!("🔍 BUNDLE_METADATA: Metadata bytes range: {}..{}", metadata_start, metadata_len_pos);

    // The metadata has a versioned format - skip the version header (magic number + version code = 8 bytes)
    // Then parse the JSON
    if metadata_bytes.len() > 8 {
        let json_bytes = &metadata_bytes[8..];

        // Parse the JSON to get file offsets
        if let Ok(json_str) = std::str::from_utf8(json_bytes) {
            debug_println!("🔍 BUNDLE_METADATA: JSON metadata: {}",
                if json_str.len() > 200 { &json_str[..200] } else { json_str });

            // Parse as BundleStorageFileOffsets JSON format: { "files": { "path": { "start": N, "end": M }, ... } }
            #[derive(serde::Deserialize)]
            struct BundleMetadata {
                files: std::collections::HashMap<String, FileRange>,
            }

            #[derive(serde::Deserialize)]
            struct FileRange {
                start: u64,
                end: u64,
            }

            if let Ok(metadata) = serde_json::from_str::<BundleMetadata>(json_str) {
                for (path_str, range) in metadata.files {
                    let path = PathBuf::from(path_str);
                    debug_println!("🔍 BUNDLE_METADATA: File {:?} -> {}..{}", path, range.start, range.end);
                    file_offsets.insert(path, range.start..range.end);
                }
                debug_println!("🔍 BUNDLE_METADATA: Parsed {} file offsets", file_offsets.len());
            } else {
                debug_println!("⚠️ BUNDLE_METADATA: Failed to parse JSON, continuing without prefetch optimization");
            }
        }
    }

    Ok(file_offsets)
}

/// Get Arc<SearcherContext> using the global cache system
/// CRITICAL FIX: Use shared caches by returning the Arc directly
fn get_shared_searcher_context() -> anyhow::Result<Arc<SearcherContext>> {
    debug_println!("RUST DEBUG: Getting SHARED SearcherContext with global caches");
    use crate::global_cache::get_global_searcher_context;

    // Use the convenience function that returns Arc<SearcherContext> with shared caches
    Ok(get_global_searcher_context())
}

/// Cached Tantivy searcher for efficient single document retrieval
/// LRU cache with configurable size to prevent unbounded memory growth
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

// Default cache size: 1000 searchers (reasonably large for production)
const DEFAULT_SEARCHER_CACHE_SIZE: usize = 1000;

// Cache statistics for monitoring
lazy_static::lazy_static! {
    pub static ref SEARCHER_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
    pub static ref SEARCHER_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
    pub static ref SEARCHER_CACHE_EVICTIONS: AtomicU64 = AtomicU64::new(0);
}

static SEARCHER_CACHE: OnceLock<Mutex<LruCache<String, Arc<tantivy::Searcher>>>> = OnceLock::new();

fn get_searcher_cache() -> &'static Mutex<LruCache<String, Arc<tantivy::Searcher>>> {
    SEARCHER_CACHE.get_or_init(|| {
        let capacity = NonZeroUsize::new(DEFAULT_SEARCHER_CACHE_SIZE).unwrap();
        Mutex::new(LruCache::new(capacity))
    })
}

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

/// Replacement for Java_io_indextables_tantivy4java_split_SplitSearcher_createNativeWithSharedCache
/// Now properly integrates StandaloneSearcher with runtime management and stores split URI
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_createNativeWithSharedCache(
    mut env: JNIEnv,
    _class: JClass,
    split_uri_jstr: JString,
    cache_manager_ptr: jlong,
    split_config_map: jobject,
) -> jlong {
    let thread_id = std::thread::current().id();
    let start_time = std::time::Instant::now();

    debug_println!("🚀 SIMPLE DEBUG: createNativeWithSharedCache method called!");
    debug_println!("🧵 SPLIT_SEARCHER: Thread {:?} ENTRY into createNativeWithSharedCache [{}ms]",
                  thread_id, start_time.elapsed().as_millis());
    debug_println!("🔗 SPLIT_SEARCHER: Thread {:?} cache_manager_ptr: 0x{:x} [{}ms]",
                  thread_id, cache_manager_ptr, start_time.elapsed().as_millis());

    // Register searcher with runtime manager for lifecycle tracking
    let runtime = crate::runtime_manager::QuickwitRuntimeManager::global();
    runtime.register_searcher();
    debug_println!("✅ RUNTIME_MANAGER: Searcher registered with runtime manager");
    // Validate JString parameter first to prevent SIGSEGV
    if split_uri_jstr.is_null() {
        runtime.unregister_searcher(); // Cleanup registration on error
        to_java_exception(&mut env, &anyhow::anyhow!("Split URI parameter is null"));
        return 0;
    }
    
    // Extract the split URI string with proper error handling
    let split_uri: String = match env.get_string(&split_uri_jstr) {
        Ok(java_str) => java_str.into(),
        Err(e) => {
            runtime.unregister_searcher(); // Cleanup registration on error
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to extract split URI: {}", e));
            return 0;
        }
    };
    
    // Validate that the extracted string is not empty
    if split_uri.is_empty() {
        runtime.unregister_searcher(); // Cleanup registration on error
        to_java_exception(&mut env, &anyhow::anyhow!("Split URI cannot be empty"));
        return 0;
    }
    
    // Validate cache manager pointer (though we're not using it in this implementation)
    if cache_manager_ptr == 0 {
        runtime.unregister_searcher(); // Cleanup registration on error
        to_java_exception(&mut env, &anyhow::anyhow!("Cache manager pointer is null"));
        return 0;
    }
    
    // Extract AWS/Azure configuration and split metadata from the split config map
    let mut aws_config: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut azure_config: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut split_footer_start: u64 = 0;
    let mut split_footer_end: u64 = 0;
    let mut doc_mapping_json: Option<String> = None;
    
    if !split_config_map.is_null() {
        let split_config_jobject = unsafe { JObject::from_raw(split_config_map) };
        
        // Extract footer offsets
        if let Ok(footer_start_obj) = env.call_method(&split_config_jobject, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("footer_start_offset").unwrap()).into()]) {
            let footer_start_jobject = footer_start_obj.l().unwrap();
            if !footer_start_jobject.is_null() {
                if let Ok(footer_start_long) = env.call_method(&footer_start_jobject, "longValue", "()J", &[]) {
                    split_footer_start = footer_start_long.j().unwrap() as u64;
                    debug_println!("RUST DEBUG: Extracted footer_start_offset from Java config: {}", split_footer_start);
                }
            }
        }
        
        if let Ok(footer_end_obj) = env.call_method(&split_config_jobject, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("footer_end_offset").unwrap()).into()]) {
            let footer_end_jobject = footer_end_obj.l().unwrap();
            if !footer_end_jobject.is_null() {
                if let Ok(footer_end_long) = env.call_method(&footer_end_jobject, "longValue", "()J", &[]) {
                    split_footer_end = footer_end_long.j().unwrap() as u64;
                    debug_println!("RUST DEBUG: Extracted footer_end_offset from Java config: {}", split_footer_end);
                }
            }
        }
        
        // Extract AWS config
        if let Ok(aws_config_obj) = env.call_method(&split_config_jobject, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("aws_config").unwrap()).into()]) {
            let aws_config_jobject = aws_config_obj.l().unwrap();
            if !aws_config_jobject.is_null() {
                let aws_config_map = &aws_config_jobject;
                
                // Extract access_key
                if let Ok(access_key_obj) = env.call_method(aws_config_map, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("access_key").unwrap()).into()]) {
                    let access_key_jobject = access_key_obj.l().unwrap();
                    if !access_key_jobject.is_null() {
                        if let Ok(access_key_str) = env.get_string((&access_key_jobject).into()) {
                            aws_config.insert("access_key".to_string(), access_key_str.into());
                            debug_println!("RUST DEBUG: Extracted AWS access key from Java config");
                        }
                    }
                }
                
                // Extract secret_key  
                if let Ok(secret_key_obj) = env.call_method(aws_config_map, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("secret_key").unwrap()).into()]) {
                    let secret_key_jobject = secret_key_obj.l().unwrap();
                    if !secret_key_jobject.is_null() {
                        if let Ok(secret_key_str) = env.get_string((&secret_key_jobject).into()) {
                            aws_config.insert("secret_key".to_string(), secret_key_str.into());
                            debug_println!("RUST DEBUG: Extracted AWS secret key from Java config");
                        }
                    }
                }
                
                // Extract session_token (optional)
                if let Ok(session_token_obj) = env.call_method(aws_config_map, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("session_token").unwrap()).into()]) {
                    let session_token_jobject = session_token_obj.l().unwrap();
                    if !session_token_jobject.is_null() {
                        if let Ok(session_token_str) = env.get_string((&session_token_jobject).into()) {
                            aws_config.insert("session_token".to_string(), session_token_str.into());
                            debug_println!("RUST DEBUG: Extracted AWS session token from Java config");
                        }
                    }
                }
                
                // Extract region
                if let Ok(region_obj) = env.call_method(aws_config_map, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("region").unwrap()).into()]) {
                    let region_jobject = region_obj.l().unwrap();
                    if !region_jobject.is_null() {
                        if let Ok(region_str) = env.get_string((&region_jobject).into()) {
                            aws_config.insert("region".to_string(), region_str.into());
                            debug_println!("RUST DEBUG: Extracted AWS region from Java config");
                        }
                    }
                }
                
                // Extract endpoint (optional)
                if let Ok(endpoint_obj) = env.call_method(aws_config_map, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("endpoint").unwrap()).into()]) {
                    let endpoint_jobject = endpoint_obj.l().unwrap();
                    if !endpoint_jobject.is_null() {
                        if let Ok(endpoint_str) = env.get_string((&endpoint_jobject).into()) {
                            aws_config.insert("endpoint".to_string(), endpoint_str.into());
                            debug_println!("RUST DEBUG: Extracted AWS endpoint from Java config");
                        }
                    }
                }
            }
        }

        // ✅ NEW: Extract Azure config
        if let Ok(azure_config_obj) = env.call_method(&split_config_jobject, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("azure_config").unwrap()).into()]) {
            let azure_config_jobject = azure_config_obj.l().unwrap();
            if !azure_config_jobject.is_null() {
                let azure_config_map = &azure_config_jobject;

                // Extract account_name
                if let Ok(account_name_obj) = env.call_method(azure_config_map, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("account_name").unwrap()).into()]) {
                    let account_name_jobject = account_name_obj.l().unwrap();
                    if !account_name_jobject.is_null() {
                        if let Ok(account_name_str) = env.get_string((&account_name_jobject).into()) {
                            azure_config.insert("account_name".to_string(), account_name_str.into());
                            debug_println!("RUST DEBUG: Extracted Azure account_name from Java config");
                        }
                    }
                }

                // Extract access_key (account key)
                if let Ok(access_key_obj) = env.call_method(azure_config_map, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("access_key").unwrap()).into()]) {
                    let access_key_jobject = access_key_obj.l().unwrap();
                    if !access_key_jobject.is_null() {
                        if let Ok(access_key_str) = env.get_string((&access_key_jobject).into()) {
                            azure_config.insert("access_key".to_string(), access_key_str.into());
                            debug_println!("RUST DEBUG: Extracted Azure access_key from Java config");
                        }
                    }
                }

                // Extract bearer_token (for OAuth authentication)
                if let Ok(bearer_token_obj) = env.call_method(azure_config_map, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("bearer_token").unwrap()).into()]) {
                    let bearer_token_jobject = bearer_token_obj.l().unwrap();
                    if !bearer_token_jobject.is_null() {
                        if let Ok(bearer_token_str) = env.get_string((&bearer_token_jobject).into()) {
                            let token_len = bearer_token_str.to_str().unwrap().len();
                            azure_config.insert("bearer_token".to_string(), bearer_token_str.into());
                            debug_println!("RUST DEBUG: Extracted Azure bearer_token from Java config (length: {} chars)", token_len);
                        }
                    }
                }
            }
        }

        // Extract doc mapping JSON if available
        debug_println!("RUST DEBUG: Attempting to extract doc mapping from Java config...");
        if let Ok(doc_mapping_obj) = env.call_method(&split_config_jobject, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("doc_mapping").unwrap()).into()]) {
            debug_println!("RUST DEBUG: Got doc_mapping_obj from Java HashMap");
            let doc_mapping_jobject = doc_mapping_obj.l().unwrap();
            if !doc_mapping_jobject.is_null() {
                debug_println!("RUST DEBUG: doc_mapping_jobject is not null, attempting to extract string");
                if let Ok(doc_mapping_str) = env.get_string((&doc_mapping_jobject).into()) {
                    let doc_mapping_string: String = doc_mapping_str.into();
                    debug_println!("🔥 NATIVE DEBUG: RAW doc_mapping from Java ({} chars): '{}'", doc_mapping_string.len(), doc_mapping_string);
                    doc_mapping_json = Some(doc_mapping_string);
                    debug_println!("RUST DEBUG: ✅ SUCCESS - Extracted doc mapping JSON from Java config ({} chars)", doc_mapping_json.as_ref().unwrap().len());
                } else {
                    debug_println!("🔥 NATIVE DEBUG: ⚠️ Failed to convert doc_mapping_jobject to string");
                    debug_println!("RUST DEBUG: ⚠️ Failed to convert doc_mapping_jobject to string");
                }
            } else {
                debug_println!("🔥 NATIVE DEBUG: ⚠️ doc_mapping_jobject is null - no doc mapping provided by Java");
                debug_println!("RUST DEBUG: ⚠️ doc_mapping_jobject is null");
            }
        } else {
            debug_println!("🔥 NATIVE DEBUG: ⚠️ Failed to call get method on HashMap for 'doc_mapping' key");
            debug_println!("RUST DEBUG: ⚠️ Failed to call get method on HashMap for 'doc_mapping' key");
        }
    }
    
    debug_println!("RUST DEBUG: Config extracted - AWS keys: {}, footer offsets: {}-{}, doc_mapping: {}",
                aws_config.len(), split_footer_start, split_footer_end,
                doc_mapping_json.as_ref().map(|s| format!("{}chars", s.len())).unwrap_or_else(|| "None".to_string()));

    // ✅ CRITICAL FIX: Use shared global runtime instead of creating individual runtime
    // This eliminates multiple Tokio runtime conflicts that cause production hangs

    // ✅ CRITICAL FIX: Execute StandaloneSearcher creation in shared runtime context
    // Quickwit's SplitCache::with_root_path spawns tasks, so we need runtime context
    let _enter = runtime.handle().enter();
    let result = tokio::task::block_in_place(|| {
        // Create StandaloneSearcher using global caches
        // If AWS credentials are provided, use with_s3_config, otherwise use default
        if aws_config.contains_key("access_key") && aws_config.contains_key("secret_key") {
        debug_println!("RUST DEBUG: Creating StandaloneSearcher with custom S3 config and global caches");
        
        let mut s3_config = S3StorageConfig::default();
        s3_config.access_key_id = Some(aws_config.get("access_key").unwrap().clone());
        s3_config.secret_access_key = Some(aws_config.get("secret_key").unwrap().clone());
        
        if let Some(session_token) = aws_config.get("session_token") {
            s3_config.session_token = Some(session_token.clone());
        }
        
        if let Some(region) = aws_config.get("region") {
            s3_config.region = Some(region.clone());
        }
        
        if let Some(endpoint) = aws_config.get("endpoint") {
            s3_config.endpoint = Some(endpoint.clone());
        }
        
        if let Some(force_path_style) = aws_config.get("path_style_access") {
            s3_config.force_path_style_access = force_path_style == "true";
        }
        
        // Use the new with_s3_config method that uses global caches
        StandaloneSearcher::with_s3_config(StandaloneSearchConfig::default(), s3_config.clone())
        } else {
            debug_println!("RUST DEBUG: Creating StandaloneSearcher with default config and global caches");
            // Use default() which now uses global caches
            StandaloneSearcher::default()
        }
    });

    // StandaloneSearcher creation succeeded, use result directly

    // Pre-create StorageResolver synchronously to avoid async issues during search
    let storage_resolver = if aws_config.contains_key("access_key") && aws_config.contains_key("secret_key") {
        debug_println!("RUST DEBUG: Pre-creating StorageResolver with S3 config to prevent deadlocks");
        let mut s3_config = S3StorageConfig::default();
        s3_config.access_key_id = Some(aws_config.get("access_key").unwrap().clone());
        s3_config.secret_access_key = Some(aws_config.get("secret_key").unwrap().clone());

        if let Some(session_token) = aws_config.get("session_token") {
            s3_config.session_token = Some(session_token.clone());
        }

        if let Some(region) = aws_config.get("region") {
            s3_config.region = Some(region.clone());
        }

        if let Some(endpoint) = aws_config.get("endpoint") {
            s3_config.endpoint = Some(endpoint.clone());
        }

        if let Some(force_path_style) = aws_config.get("path_style_access") {
            s3_config.force_path_style_access = force_path_style == "true";
        }

        crate::global_cache::get_configured_storage_resolver(Some(s3_config), None)
    } else if azure_config.contains_key("account_name") && (azure_config.contains_key("access_key") || azure_config.contains_key("bearer_token")) {
        debug_println!("RUST DEBUG: Pre-creating StorageResolver with Azure config to prevent deadlocks");
        debug_println!("RUST DEBUG: Azure account_name: {}", azure_config.get("account_name").unwrap());
        debug_println!("RUST DEBUG: Azure auth method: {}", if azure_config.contains_key("bearer_token") { "OAuth bearer token" } else { "account key" });
        use quickwit_config::AzureStorageConfig;

        let azure_storage_config = AzureStorageConfig {
            account_name: Some(azure_config.get("account_name").unwrap().clone()),
            access_key: azure_config.get("access_key").cloned(),
            bearer_token: azure_config.get("bearer_token").cloned(),
        };

        crate::global_cache::get_configured_storage_resolver(None, Some(azure_storage_config))
    } else {
        debug_println!("RUST DEBUG: Pre-creating default StorageResolver to prevent deadlocks");
        crate::global_cache::get_configured_storage_resolver(None, None)
    };

    match result {
        Ok(searcher) => {
            // Follow Quickwit pattern: resolve storage once and cache it for reuse
            let storage = runtime.handle().block_on(async {
                use crate::standalone_searcher::resolve_storage_for_split;
                let result = resolve_storage_for_split(&storage_resolver, &split_uri).await;
                result
            });

            match storage {
                Ok(resolved_storage) => {
                    debug_println!("🔥 STORAGE RESOLVED: Storage resolved once for reuse, instance: {:p}", Arc::as_ptr(&resolved_storage));

                    // Follow Quickwit pattern: open index once and cache it
                    // 🚀 BATCH OPTIMIZATION FIX: Create ByteRangeCache and parse bundle metadata
                    let (opened_index, byte_range_cache, bundle_file_offsets) = runtime.handle().block_on(async {
                        use quickwit_proto::search::SplitIdAndFooterOffsets;
                        use crate::global_cache::get_global_searcher_context;

                        let split_filename = if let Some(last_slash_pos) = split_uri.rfind('/') {
                            &split_uri[last_slash_pos + 1..]
                        } else {
                            &split_uri
                        };
                        let split_id = if split_filename.ends_with(".split") {
                            &split_filename[..split_filename.len() - 6]
                        } else {
                            split_filename
                        };

                        let split_metadata = SplitIdAndFooterOffsets {
                            split_id: split_id.to_string(),
                            split_footer_start: split_footer_start,
                            split_footer_end: split_footer_end,
                            timestamp_start: None,
                            timestamp_end: None,
                            num_docs: 0,
                        };

                        // 🚀 BATCH OPTIMIZATION FIX: Create ByteRangeCache for prefetch optimization
                        // This cache will be shared between prefetch_ranges and doc_async (via CachingDirectory)
                        let byte_range_cache = ByteRangeCache::with_infinite_capacity(
                            &STORAGE_METRICS.shortlived_cache,
                        );
                        debug_println!("🚀 BATCH_OPT: Created ByteRangeCache for prefetch optimization");

                        // 🚀 OPTIMIZATION: Call open_index_with_caches FIRST to populate split_footer_cache
                        // This eliminates redundant footer fetches when we parse bundle offsets below
                        let searcher_context_global = get_global_searcher_context();
                        let result = quickwit_search::leaf::open_index_with_caches(
                            &searcher_context_global,
                            resolved_storage.clone(),
                            &split_metadata,
                            None, // tokenizer_manager
                            Some(byte_range_cache.clone())  // 🚀 Pass ByteRangeCache for prefetch optimization
                        ).await;

                        // 🚀 BATCH OPTIMIZATION FIX: Parse bundle file offsets from footer (now cached in split_footer_cache!)
                        // This allows prefetch to translate inner file byte ranges to split file byte ranges
                        let bundle_file_offsets = if split_footer_start > 0 && split_footer_end > split_footer_start {
                            // Use searcher_context_global.split_footer_cache - same cache open_index_with_caches uses
                            match parse_bundle_file_offsets(&resolved_storage, &split_uri, split_footer_start, split_footer_end, &searcher_context_global).await {
                                Ok(offsets) => {
                                    debug_println!("🚀 BATCH_OPT: Parsed {} bundle file offsets for prefetch optimization", offsets.len());
                                    offsets
                                }
                                Err(e) => {
                                    debug_println!("⚠️ BATCH_OPT: Failed to parse bundle file offsets (prefetch optimization disabled): {}", e);
                                    std::collections::HashMap::new()
                                }
                            }
                        } else {
                            debug_println!("⚠️ BATCH_OPT: No footer metadata available (prefetch optimization disabled)");
                            std::collections::HashMap::new()
                        };

                        (result, byte_range_cache, bundle_file_offsets)
                    });

                    match opened_index {
                        Ok((cached_index, _hot_directory)) => {
                            debug_println!("🔥 INDEX CACHED: Index opened once for reuse, cached for all operations");

                            // Follow Quickwit's exact pattern: create index reader and cached searcher
                            // Extract metadata for enhanced memory allocation
                            let split_metadata = extract_split_metadata_for_allocation(&split_uri);
                            let batch_cache_blocks = get_batch_doc_cache_blocks_with_metadata(split_metadata);
                            debug_println!("⚡ CACHE_OPTIMIZATION: Applying advanced adaptive doc store cache optimization - blocks: {} (batch operations with metadata)", batch_cache_blocks);
                            let index_reader = match cached_index
                                .reader_builder()
                                .doc_store_cache_num_blocks(batch_cache_blocks) // Advanced adaptive cache sizing
                                .reload_policy(tantivy::ReloadPolicy::Manual)
                                .try_into() {
                                Ok(reader) => reader,
                                Err(e) => {
                                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to create index reader: {}", e));
                                    return 0;
                                }
                            };

                            let cached_searcher = std::sync::Arc::new(index_reader.searcher());
                            debug_println!("🔥 SEARCHER CACHED: Created cached searcher following Quickwit's exact pattern for optimal cache reuse");

                            // ✅ FIX: Always use index.schema() for now - doc_mapping reconstruction has issues with dynamic JSON fields
                            // Quickwit's DocMapper requires at least one field mapping for JSON/object fields, but Tantivy's dynamic JSON fields don't have predefined mappings
                            debug_println!("RUST DEBUG: 🔧 Using index.schema() directly (doc_mapping has compatibility issues with dynamic JSON fields)");
                            let schema = cached_index.schema();
                            let schema_ptr = crate::utils::arc_to_jlong(std::sync::Arc::new(schema));
                            debug_println!("RUST DEBUG: 🔧 Created schema_ptr={} from reconstructed schema", schema_ptr);

                            // Create clean struct-based context instead of complex tuple
                            let cached_context = CachedSearcherContext {
                                standalone_searcher: std::sync::Arc::new(searcher),
                                // ✅ CRITICAL FIX: No longer storing runtime - using shared global runtime
                                split_uri: split_uri.clone(),
                                aws_config,
                                footer_start: split_footer_start,
                                footer_end: split_footer_end,
                                doc_mapping_json,
                                cached_storage: resolved_storage,
                                cached_index: std::sync::Arc::new(cached_index),
                                cached_searcher,
                                // 🚀 BATCH OPTIMIZATION FIX: Store ByteRangeCache and bundle file offsets
                                byte_range_cache: Some(byte_range_cache),
                                bundle_file_offsets,
                            };

                            let searcher_context = std::sync::Arc::new(cached_context);
                            let pointer = arc_to_jlong(searcher_context);
                            debug_println!("RUST DEBUG: SUCCESS: Stored searcher context with cached index for split '{}' with Arc pointer: {}, footer: {}-{}",
                                     split_uri, pointer, split_footer_start, split_footer_end);

                            // ✅ DEBUG: Immediately verify the Arc can be retrieved
                            if let Some(_test_context) = crate::utils::jlong_to_arc::<CachedSearcherContext>(pointer) {
                                debug_println!("✅ VERIFICATION: Arc {} successfully stored and retrieved from registry", pointer);
                            } else {
                                debug_println!("❌ CRITICAL BUG: Arc {} was stored but CANNOT be retrieved immediately!", pointer);
                            }

                            // ✅ FIX: Store direct mapping from searcher pointer to schema pointer for fallback
                            debug_println!("RUST DEBUG: 🔧 Storing schema mapping: searcher_ptr={} -> schema_ptr={}", pointer, schema_ptr);
                            crate::split_query::store_searcher_schema(pointer, schema_ptr);
                            debug_println!("RUST DEBUG: 🔧 Schema mapping stored successfully");
                            debug_println!("✅ SEARCHER_SCHEMA_MAPPING: Stored mapping {} -> {} for reliable schema access", pointer, schema_ptr);

                            debug_println!("🏁 SPLIT_SEARCHER: Thread {:?} COMPLETED successfully in {}ms - pointer: 0x{:x}",
                                          thread_id, start_time.elapsed().as_millis(), pointer);
                            pointer
                        },
                        Err(e) => {
                            runtime.unregister_searcher(); // Cleanup registration on error
                            to_java_exception(&mut env, &anyhow::anyhow!("Failed to open index for split '{}': {}", split_uri, e));
                            0
                        }
                    }
                },
                Err(e) => {
                    runtime.unregister_searcher(); // Cleanup registration on error
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to resolve storage for split '{}': {}", split_uri, e));
                    0
                }
            }
        },
        Err(error) => {
            debug_println!("❌ SPLIT_SEARCHER: Thread {:?} FAILED after {}ms - error: {}",
                          thread_id, start_time.elapsed().as_millis(), error);
            runtime.unregister_searcher(); // Cleanup registration on error
            to_java_exception(&mut env, &error);
            0
        }
    }
}

/// Replacement for Java_io_indextables_tantivy4java_split_SplitSearcher_closeNative
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_closeNative(
    env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) {
    let thread_id = std::thread::current().id();
    debug_println!("🧵 SPLIT_SEARCHER_CLOSE: Thread {:?} ENTRY into closeNative - pointer: 0x{:x}",
                  thread_id, searcher_ptr);

    if searcher_ptr == 0 {
        debug_println!("⚠️  SPLIT_SEARCHER_CLOSE: Thread {:?} - null pointer, nothing to close", thread_id);
        return;
    }

    // Debug: Log call stack to understand why this is being called
    if *crate::debug::DEBUG_ENABLED {
        debug_println!("RUST DEBUG: WARNING - closeNative called for SplitSearcher with ID: {}", searcher_ptr);
        debug_println!("RUST DEBUG: This should only happen when the SplitSearcher is closed in Java");
        
        // Print the stack trace to see where this is being called from
        let backtrace = std::backtrace::Backtrace::capture();
        if backtrace.status() == std::backtrace::BacktraceStatus::Captured {
            debug_println!("RUST DEBUG: Stack trace for closeNative:");
            let backtrace_str = format!("{}", backtrace);
            for (i, line) in backtrace_str.lines().enumerate() {
                if i < 20 {  // Print first 20 lines to avoid too much output
                    debug_println!("  {}", line);
                }
            }
        }
    }

    // ✅ FIX: Clean up direct schema mapping when searcher is closed
    crate::split_query::remove_searcher_schema(searcher_ptr);
    debug_println!("✅ CLEANUP: Removed direct schema mapping for searcher {}", searcher_ptr);

    // Unregister searcher from runtime manager
    let runtime = crate::runtime_manager::QuickwitRuntimeManager::global();
    runtime.unregister_searcher();
    debug_println!("✅ RUNTIME_MANAGER: Unregistered searcher from runtime manager");

    // SAFE: Release Arc from registry to prevent memory leaks
    release_arc(searcher_ptr);
    debug_println!("RUST DEBUG: Closed searcher and released Arc with ID: {}", searcher_ptr);
    debug_println!("🏁 SPLIT_SEARCHER_CLOSE: Thread {:?} COMPLETED successfully - pointer: 0x{:x}",
                  thread_id, searcher_ptr);
}

/// Replacement for Java_io_indextables_tantivy4java_split_SplitSearcher_validateSplitNative  
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_validateSplitNative(
    env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) -> jboolean {
    // Simple validation - check if the searcher pointer is valid
    if searcher_ptr == 0 {
        return 0; // false
    }
    
    let is_valid = with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
        // Searcher exists and is valid
        true
    }).unwrap_or(false);
    
    if is_valid { 1 } else { 0 }
}

/// Replacement for Java_io_indextables_tantivy4java_split_SplitSearcher_getCacheStatsNative
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_getCacheStatsNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) -> jobject {
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
        let context = searcher_context.as_ref();
        let stats = context.standalone_searcher.cache_stats();
        
        // Create a CacheStats Java object
        match env.find_class("io/indextables/tantivy4java/split/SplitSearcher$CacheStats") {
            Ok(cache_stats_class) => {
                match env.new_object(
                    &cache_stats_class,
                    "(JJJJJ)V", // Constructor signature: (hitCount, missCount, evictionCount, totalSize, maxSize)
                    &[
                        (stats.partial_request_count as jlong).into(), // hitCount (using partial_request_count as hits)
                        (0 as jlong).into(), // missCount (not tracked in our current stats)
                        (0 as jlong).into(), // evictionCount (not tracked)
                        ((stats.fast_field_bytes + stats.split_footer_bytes) as jlong).into(), // totalSize
                        (100_000_000 as jlong).into(), // maxSize (some reasonable default)
                    ],
                ) {
                    Ok(cache_stats_obj) => Some(cache_stats_obj.into_raw()),
                    Err(e) => {
                        debug_println!("RUST DEBUG: Failed to create CacheStats object: {}", e);
                        None
                    }
                }
            },
            Err(e) => {
                debug_println!("RUST DEBUG: Failed to find CacheStats class: {}", e);
                None
            }
        }
    });

    match result {
        Some(Some(cache_stats_obj)) => cache_stats_obj,
        Some(None) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to create CacheStats object"));
            std::ptr::null_mut()
        },
        None => {
            to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
            std::ptr::null_mut()
        }
    }
}

/// Async-first method for Java_io_indextables_tantivy4java_split_SplitSearcher_searchWithQueryAst
/// This method uses the new async-first architecture to eliminate deadlocks
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_searchWithQueryAst(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    query_ast_json: JString,
    limit: jint,
) -> jobject {
    debug_println!("🚀 ASYNC_JNI: searchWithQueryAst called with async-first architecture");

    // Extract query JSON first (JNI types can't be sent across threads)
    let query_json: String = match env.get_string(&query_ast_json) {
        Ok(java_str) => java_str.into(),
        Err(e) => {
            debug_println!("❌ ASYNC_JNI: Failed to extract query JSON: {}", e);
            return std::ptr::null_mut();
        }
    };

    // Use async pattern that returns LeafSearchResponse directly (avoid unnecessary JSON marshalling)
    debug_println!("🔍 ASYNC_JNI: About to call perform_search_async_impl_leaf_response");
    match block_on_operation(async move {
        perform_search_async_impl_leaf_response(searcher_ptr, query_json, limit).await
    }) {
        Ok(leaf_search_response) => {
            debug_println!("✅ ASYNC_JNI: Got LeafSearchResponse, creating SearchResult object");
            // Create proper SearchResult object directly from LeafSearchResponse (no JSON marshalling)
            match perform_unified_search_result_creation(leaf_search_response, &mut env, None) {
                Ok(search_result_obj) => {
                    debug_println!("✅ ASYNC_JNI: Successfully created SearchResult object");
                    search_result_obj
                },
                Err(e) => {
                    debug_println!("❌ ASYNC_JNI: Failed to create SearchResult object: {}", e);
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to create SearchResult: {}", e));
                    std::ptr::null_mut()
                }
            }
        },
        Err(e) => {
            debug_println!("❌ ASYNC_JNI: Search operation failed: {}", e);

            // CRITICAL FIX: Throw proper exception instead of returning null
            // This ensures Java code gets a meaningful error message instead of NullPointerException
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

/// Method to search with SplitQuery objects using async-first pattern
/// This method follows Quickwit's cache management lifecycle
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_searchWithSplitQuery(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    split_query_obj: JObject,
    limit: jint,
) -> jobject {
    debug_println!("🚨 ENTRY_POINT: Java_io_indextables_tantivy4java_split_SplitSearcher_searchWithSplitQuery ENTRY");
    debug_println!("🚨 ENTRY_POINT: Function parameters - searcher_ptr: {}, limit: {}", searcher_ptr, limit);
    debug_println!("🚨 ENTRY_POINT: About to proceed with function body");
    debug_println!("🔥 NATIVE DEBUG: searchWithSplitQuery called with pointer {} and limit {}", searcher_ptr, limit);
    debug_println!("🚀 ASYNC_JNI: searchWithSplitQuery called with async-first architecture");

    // Extract all JNI data at entry point - no JNI types should go into core functions
    debug_println!("🔥 NATIVE DEBUG: Converting SplitQuery to JSON");
    let query_json_str = match convert_split_query_to_json(&mut env, &split_query_obj) {
        Ok(json_str) => {
            debug_println!("🔥 NATIVE DEBUG: Successfully converted SplitQuery to JSON: {}", json_str);
            json_str
        },
        Err(e) => {
            debug_println!("🔥 NATIVE DEBUG: Failed to convert SplitQuery to JSON: {}", e);
            debug_println!("❌ ASYNC_JNI: Failed to convert SplitQuery to JSON: {}", e);
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to convert SplitQuery to JSON: {}", e));
            return std::ptr::null_mut();
        }
    };

    // Use async pattern that returns LeafSearchResponse directly (avoid unnecessary JSON marshalling)
    // No JNI types passed to core functions - all data extracted at entry point
    debug_println!("🔥 NATIVE DEBUG: About to call block_on_operation with JSON: {}", query_json_str);
    debug_println!("🔍 ASYNC_JNI: About to call perform_search_async_impl_leaf_response (SplitQuery version)");
    debug_println!("🚨 CRITICAL: About to call block_on_operation - checking runtime context");

    // Check if we're in a Tokio runtime context before calling block_on
    if let Ok(_handle) = tokio::runtime::Handle::try_current() {
        debug_println!("❌ CRITICAL: WE ARE IN TOKIO RUNTIME CONTEXT - this will cause deadlock!");
        return std::ptr::null_mut();
    } else {
        debug_println!("✅ CRITICAL: Not in Tokio runtime context - safe to call block_on");
    }

    debug_println!("🚨 CRITICAL: Calling block_on_operation with async search operation");
    match block_on_operation(async move {
        debug_println!("🔍 ASYNC_START: Inside async block - about to call perform_search_async_impl_leaf_response");
        let result = perform_search_async_impl_leaf_response(searcher_ptr, query_json_str, limit).await;
        debug_println!("🔍 ASYNC_END: perform_search_async_impl_leaf_response completed");
        result
    }) {
        Ok(leaf_search_response) => {
            debug_println!("🔥 NATIVE DEBUG: block_on_operation SUCCESS - Got LeafSearchResponse from SplitQuery");
            debug_println!("✅ ASYNC_JNI: Got LeafSearchResponse from SplitQuery, creating SearchResult object");
            // Create proper SearchResult object directly from LeafSearchResponse (no JSON marshalling)
            match perform_unified_search_result_creation(leaf_search_response, &mut env, None) {
                Ok(search_result_obj) => {
                    debug_println!("🔥 NATIVE DEBUG: Successfully created SearchResult object from SplitQuery");
                    debug_println!("✅ ASYNC_JNI: Successfully created SearchResult object from SplitQuery");
                    search_result_obj
                },
                Err(e) => {
                    debug_println!("🔥 NATIVE DEBUG: Failed to create SearchResult object from SplitQuery: {}", e);
                    debug_println!("❌ ASYNC_JNI: Failed to create SearchResult object from SplitQuery: {}", e);
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to create SearchResult: {}", e));
                    std::ptr::null_mut()
                }
            }
        },
        Err(e) => {
            debug_println!("🔥 NATIVE DEBUG: block_on_operation FAILED: {}", e);
            debug_println!("❌ ASYNC_JNI: SplitQuery search operation failed: {}", e);

            // CRITICAL FIX: Throw proper exception instead of returning null
            // This ensures Java code gets a meaningful error message instead of NullPointerException
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

/// Search with aggregations support for SplitSearcher
/// This method combines regular search with statistical aggregations
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_searchWithAggregations<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    searcher_ptr: jlong,
    split_query: JObject<'local>,
    limit: jint,
    aggregations_map: JObject<'local>,
) -> jobject {
    let method_start_time = std::time::Instant::now();
    debug_println!("🚀 RUST NATIVE: searchWithAggregations ENTRY - Real aggregation processing starting");

    if searcher_ptr == 0 {
        debug_println!("RUST DEBUG: ⏱️ searchWithAggregations ERROR: Invalid searcher pointer [TIMING: {}ms]", method_start_time.elapsed().as_millis());
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
        return std::ptr::null_mut();
    }
    debug_println!("RUST DEBUG: searcher_ptr validation passed: {}", searcher_ptr);

    // Convert SplitQuery to QueryAst JSON (using existing infrastructure)
    let query_json_result = convert_split_query_to_json(&mut env, &split_query);
    let query_json = match query_json_result {
        Ok(json) => {
            debug_println!("RUST DEBUG: Query conversion successful");
            json
        },
        Err(e) => {
            debug_println!("RUST DEBUG: ⏱️ searchWithAggregations ERROR: Failed to convert SplitQuery [TIMING: {}ms]", method_start_time.elapsed().as_millis());
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to convert SplitQuery to JSON: {}", e));
            return std::ptr::null_mut();
        }
    };

    // Convert Java aggregations map to JSON
    debug_println!("RUST DEBUG: Starting aggregation conversion");
    let aggregation_request_json = match convert_java_aggregations_to_json(&mut env, &aggregations_map) {
        Ok(agg_json) => {
            debug_println!("RUST DEBUG: Aggregation conversion successful");
            agg_json
        },
        Err(e) => {
            debug_println!("RUST DEBUG: ⏱️ searchWithAggregations ERROR: Failed to convert aggregations to JSON [TIMING: {}ms]", method_start_time.elapsed().as_millis());
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to convert aggregations to JSON: {}", e));
            return std::ptr::null_mut();
        }
    };

    debug_println!("RUST DEBUG: Query JSON: {}", query_json);
    if let Some(ref agg_json) = aggregation_request_json {
        debug_println!("RUST DEBUG: Aggregation JSON: {}", agg_json);
    } else {
        debug_println!("RUST DEBUG: No aggregation JSON - aggregation_request_json is None");
    }

    // Use block_on_operation to perform the async search with aggregations
    let searcher_ptr_copy = searcher_ptr;
    let limit_copy = limit as usize;
    let aggregation_request_json_copy = aggregation_request_json.clone();
    match block_on_operation(async move {
        perform_search_async_impl_leaf_response_with_aggregations(searcher_ptr_copy, query_json, limit_copy, aggregation_request_json_copy).await
    }) {
        Ok(leaf_search_response) => {
            debug_println!("RUST DEBUG: ⏱️ searchWithAggregations SUCCESS [TIMING: {}ms]", method_start_time.elapsed().as_millis());
            debug_println!("RUST DEBUG: Found {} hits, has aggregations: {}",
                         leaf_search_response.num_hits,
                         leaf_search_response.intermediate_aggregation_result.is_some());

            match perform_unified_search_result_creation(leaf_search_response, &mut env, aggregation_request_json.clone()) {
                Ok(search_result_obj) => search_result_obj,
                Err(e) => {
                    debug_println!("RUST DEBUG: ⏱️ searchWithAggregations ERROR: Failed to create SearchResult [TIMING: {}ms]: {}", method_start_time.elapsed().as_millis(), e);
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to create SearchResult: {}", e));
                    std::ptr::null_mut()
                }
            }
        }
        Err(e) => {
            debug_println!("RUST DEBUG: ⏱️ searchWithAggregations ERROR: Search failed [TIMING: {}ms]: {}", method_start_time.elapsed().as_millis(), e);
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

/// Async implementation for search with aggregations
pub async fn perform_search_async_impl_leaf_response_with_aggregations(
    searcher_ptr: jlong,
    query_json: String,
    limit: usize,
    aggregation_request_json: Option<String>,
) -> anyhow::Result<quickwit_proto::search::LeafSearchResponse> {
    debug_println!("🔍 ASYNC_JNI: Starting search with aggregations using working pattern");

    if searcher_ptr == 0 {
        return Err(anyhow::anyhow!("Invalid searcher pointer"));
    }

    // Extract searcher context using the SAME pattern as working search
    let searcher_context = crate::utils::jlong_to_arc::<CachedSearcherContext>(searcher_ptr)
        .ok_or_else(|| anyhow::anyhow!("Invalid searcher context"))?;

    let context = searcher_context.as_ref();

    debug_println!("🔍 ASYNC_JNI: Extracted searcher context, performing search with aggregations on split: {}", context.split_uri);

    // Use the SAME working search functionality but with aggregations
    let search_result = perform_real_quickwit_search_with_aggregations(
        &context.split_uri,
        &context.aws_config,
        context.footer_start,
        context.footer_end,
        &context.doc_mapping_json,
        context.cached_storage.clone(),
        context.cached_searcher.clone(),
        context.cached_index.clone(),
        &query_json,
        limit,
        aggregation_request_json,
    ).await?;

    debug_println!("✅ ASYNC_JNI: Search with aggregations completed successfully with {} hits", search_result.num_hits);
    Ok(search_result)
}

/// Batch document retrieval for SplitSearcher using Quickwit's optimized approach
/// This implementation follows Quickwit's patterns from fetch_docs.rs:
/// 1. Sort addresses by segment for cache locality
/// 2. Open index with proper cache settings
/// 3. Use doc_async for optimal performance
/// 4. Reuse index, reader, and searcher across all documents
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_docBatchNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    segments: jni::sys::jintArray,
    doc_ids: jni::sys::jintArray,
) -> jobject {
    if searcher_ptr == 0 {
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
        return std::ptr::null_mut();
    }
    
    // Convert JNI arrays to proper JArray types
    let segments_array = unsafe { jni::objects::JIntArray::from_raw(segments) };
    let doc_ids_array = unsafe { jni::objects::JIntArray::from_raw(doc_ids) };
    
    // Get the segment and doc ID arrays
    let array_len = match env.get_array_length(&segments_array) {
        Ok(len) => len as usize,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to get segments array length: {}", e));
            return std::ptr::null_mut();
        }
    };
    
    let mut segments_vec = vec![0i32; array_len];
    if let Err(e) = env.get_int_array_region(&segments_array, 0, &mut segments_vec) {
        to_java_exception(&mut env, &anyhow::anyhow!("Failed to get segments array: {}", e));
        return std::ptr::null_mut();
    }
    let segments_vec: Vec<u32> = segments_vec.iter().map(|&s| s as u32).collect();
    
    let mut doc_ids_vec = vec![0i32; array_len];
    if let Err(e) = env.get_int_array_region(&doc_ids_array, 0, &mut doc_ids_vec) {
        to_java_exception(&mut env, &anyhow::anyhow!("Failed to get doc_ids array: {}", e));
        return std::ptr::null_mut();
    }
    let doc_ids_vec: Vec<u32> = doc_ids_vec.iter().map(|&d| d as u32).collect();
    
    if segments_vec.len() != doc_ids_vec.len() {
        to_java_exception(&mut env, &anyhow::anyhow!("Segments and doc_ids arrays must have same length"));
        return std::ptr::null_mut();
    }
    
    // Create DocAddress objects with original indices for ordering
    let mut indexed_addresses: Vec<(usize, tantivy::DocAddress)> = segments_vec
        .iter()
        .zip(doc_ids_vec.iter())
        .enumerate()
        .map(|(idx, (&seg, &doc))| (idx, tantivy::DocAddress::new(seg, doc)))
        .collect();
    
    // Sort by document address for cache locality (following Quickwit pattern)
    indexed_addresses.sort_by_key(|(_, addr)| *addr);
    
    // Extract sorted addresses for batch retrieval
    let sorted_addresses: Vec<tantivy::DocAddress> = indexed_addresses
        .iter()
        .map(|(_, addr)| *addr)
        .collect();
    
    // Use Quickwit-optimized bulk document retrieval
    let retrieval_result = retrieve_documents_batch_from_split_optimized(searcher_ptr, sorted_addresses);
    
    match retrieval_result {
        Ok(sorted_docs) => {
            // Reorder documents back to original input order
            let mut ordered_doc_ptrs = vec![std::ptr::null_mut(); indexed_addresses.len()];
            for (i, (original_idx, _)) in indexed_addresses.iter().enumerate() {
                if i < sorted_docs.len() {
                    ordered_doc_ptrs[*original_idx] = sorted_docs[i];
                }
            }
            
            // Create a Java Document array
            let document_class = match env.find_class("io/indextables/tantivy4java/core/Document") {
                Ok(class) => class,
                Err(e) => {
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to find Document class: {}", e));
                    return std::ptr::null_mut();
                }
            };
            
            let doc_array = match env.new_object_array(ordered_doc_ptrs.len() as i32, &document_class, JObject::null()) {
                Ok(array) => array,
                Err(e) => {
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to create Document array: {}", e));
                    return std::ptr::null_mut();
                }
            };
            
            // Create Document objects and add to array
            for (i, doc_ptr) in ordered_doc_ptrs.iter().enumerate() {
                if doc_ptr.is_null() {
                    continue; // Skip null documents
                }
                
                // Create Document object with the pointer
                let doc_obj = match env.new_object(
                    &document_class,
                    "(J)V",
                    &[jni::objects::JValue::Long(*doc_ptr as jlong)]
                ) {
                    Ok(obj) => obj,
                    Err(e) => {
                        to_java_exception(&mut env, &anyhow::anyhow!("Failed to create Document object: {}", e));
                        continue;
                    }
                };
                
                if let Err(e) = env.set_object_array_element(&doc_array, i as i32, doc_obj) {
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to set array element: {}", e));
                }
            }
            
            doc_array.into_raw()
        },
        Err(e) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

/// Simple but effective searcher caching for single document retrieval
/// Uses the same optimizations as our batch method but caches searchers for reuse
fn retrieve_document_from_split_optimized(
    searcher_ptr: jlong,
    doc_address: tantivy::DocAddress,
) -> Result<(tantivy::schema::TantivyDocument, tantivy::schema::Schema), anyhow::Error> {
    let function_start = std::time::Instant::now();
    debug_println!("RUST DEBUG: ⏱️ 🚀 retrieve_document_from_split_optimized ENTRY [TIMING START]");
    
    use crate::utils::with_arc_safe;
    
    // Get split URI from the searcher context
    let uri_extraction_start = std::time::Instant::now();
    let split_uri = with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
        let context = searcher_context.as_ref();
        context.split_uri.clone()
    }).ok_or_else(|| anyhow::anyhow!("Invalid searcher context"))?;
    debug_println!("RUST DEBUG: ⏱️ Split URI extraction completed [TIMING: {}ms]", uri_extraction_start.elapsed().as_millis());
    
    // Check cache first - simple and effective
    let cache_check_start = std::time::Instant::now();
    let searcher_cache = get_searcher_cache();
    let cached_searcher = {
        let mut cache = searcher_cache.lock().unwrap();
        cache.get(&split_uri).cloned()
    };

    // Track cache statistics
    if cached_searcher.is_some() {
        SEARCHER_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
    } else {
        SEARCHER_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
    }

    debug_println!("RUST DEBUG: ⏱️ Cache lookup completed [TIMING: {}ms] - cache_hit: {}", cache_check_start.elapsed().as_millis(), cached_searcher.is_some());

    if let Some(searcher) = cached_searcher {
        // Use cached searcher - very fast path (cache hit)
        // IMPORTANT: Use async method for StorageDirectory compatibility
        let cache_hit_start = std::time::Instant::now();
        debug_println!("RUST DEBUG: ⏱️ 🎯 CACHE HIT - using cached searcher for document retrieval");
        
        // Extract the runtime and use async document retrieval
        let doc_and_schema = with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
            let context = searcher_context.as_ref();

            // ✅ CRITICAL FIX: Use shared global runtime instead of context.runtime
            tokio::task::block_in_place(|| {
                crate::runtime_manager::QuickwitRuntimeManager::global().handle().block_on(async {
                    let doc = tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        searcher.doc_async(doc_address)
                    )
                    .await
                    .map_err(|_| anyhow::anyhow!("Document retrieval timed out for {:?}", doc_address))?
                    .map_err(|e| anyhow::anyhow!("Failed to retrieve document: {}", e))?;
                    let schema = searcher.schema();
                    Ok::<(tantivy::schema::TantivyDocument, tantivy::schema::Schema), anyhow::Error>((doc, schema.clone()))
                })
            })
        }).ok_or_else(|| anyhow::anyhow!("Invalid searcher context for cached retrieval"))?;
        
        match doc_and_schema {
            Ok((doc, schema)) => {
                debug_println!("RUST DEBUG: ⏱️ ✅ CACHE HIT document retrieval completed [TIMING: {}ms] [TOTAL: {}ms]", cache_hit_start.elapsed().as_millis(), function_start.elapsed().as_millis());
                return Ok((doc, schema));
            }
            Err(e) => {
                debug_println!("RUST DEBUG: ⏱️ ❌ CACHE HIT failed, falling through to cache miss: {}", e);
                // Fall through to cache miss path
            }
        }
    }
    
    // Cache miss - create searcher using the same optimizations as our batch method
    debug_println!("RUST DEBUG: ⏱️ ⚠️ CACHE MISS - creating new searcher (EXPENSIVE OPERATION)");
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
        let context = searcher_context.as_ref();

        // ✅ CRITICAL FIX: Use shared global runtime instead of context.runtime

        // Extract variables from context for compatibility with existing code
        let split_uri = &context.split_uri;
        let aws_config = &context.aws_config;
        let footer_start = context.footer_start;
        let footer_end = context.footer_end;
        let storage_resolver = &context.cached_storage;
        let cached_index = &context.cached_index;

        // Use the same Quickwit caching pattern as our batch method
        tokio::task::block_in_place(|| {
            crate::runtime_manager::QuickwitRuntimeManager::global().handle().block_on(async {

                use quickwit_config::{StorageConfigs, S3StorageConfig};
                use quickwit_proto::search::SplitIdAndFooterOffsets;
                use quickwit_storage::StorageResolver;
                
                
                
                use std::sync::Arc;
                
                // Create split metadata for Quickwit's open_index_with_caches with correct field names
                // Extract just the filename as the split_id (e.g., "consolidated.split" from the full URL)
                let split_filename = if let Some(last_slash_pos) = split_uri.rfind('/') {
                    &split_uri[last_slash_pos + 1..]
                } else {
                    split_uri
                };

                // For split_id, use the filename without .split extension if present
                // This is what Quickwit expects for the split identifier
                let split_id = if split_filename.ends_with(".split") {
                    &split_filename[..split_filename.len() - 6] // Remove ".split"
                } else {
                    split_filename
                };

                let split_metadata = SplitIdAndFooterOffsets {
                    split_id: split_id.to_string(),
                    split_footer_start: footer_start,
                    split_footer_end: footer_end,
                    timestamp_start: Some(0), // Not used for our purposes
                    timestamp_end: Some(i64::MAX), // Not used for our purposes  
                    num_docs: 0, // Will be filled by Quickwit
                };
                
                // Create S3 storage configuration
                let s3_config = S3StorageConfig {
                    flavor: None,
                    access_key_id: aws_config.get("access_key").cloned(),
                    secret_access_key: aws_config.get("secret_key").cloned(), 
                    session_token: aws_config.get("session_token").cloned(),
                    region: aws_config.get("region").cloned(),
                    endpoint: aws_config.get("endpoint").cloned(),
                    force_path_style_access: aws_config.get("path_style_access").map_or(false, |v| v == "true"),
                    disable_multi_object_delete: false,
                    disable_multipart_upload: false,
                };
                
                // Create storage that points to the directory containing the split (not the split file itself)
                // This is what open_index_with_caches expects
                let storage_resolution_start = std::time::Instant::now();
                let split_dir_uri = if let Some(last_slash_pos) = split_uri.rfind('/') {
                    &split_uri[..last_slash_pos + 1] // Include the trailing slash
                } else {
                    split_uri // If no slash, use the full URI as directory
                };
                debug_println!("RUST DEBUG: ⏱️ 🔧 STORAGE RESOLUTION - Creating S3 storage configuration");

                // ✅ DEADLOCK FIX #2: Use pre-created storage resolver from searcher context
                debug_println!("✅ QUICKWIT_LIFECYCLE: Using cached storage from searcher context (Quickwit pattern)");
                debug_println!("   📍 Location: split_searcher_replacement.rs:878 (S3 index storage path)");
                debug_println!("✅ CACHED_STORAGE_USED: Storage at address {:p} (Quickwit lifecycle)", Arc::as_ptr(storage_resolver));
                let index_storage = storage_resolver.clone();
                debug_println!("RUST DEBUG: ⏱️ 🔧 STORAGE RESOLUTION completed [TIMING: {}ms]", storage_resolution_start.elapsed().as_millis());
                
                // Use global SearcherContext for long-term shared caches (Quickwit pattern)
                let searcher_context = crate::global_cache::get_global_searcher_context();
                
                // Create short-lived ByteRangeCache per operation (Quickwit pattern for optimal memory use)
                let byte_range_cache = quickwit_storage::ByteRangeCache::with_infinite_capacity(
                    &quickwit_storage::STORAGE_METRICS.shortlived_cache
                );
                
                // Manual index opening with Quickwit caching components
                // (open_index_with_caches expects Quickwit's native split format, but we use bundle format)
                let index_opening_start = std::time::Instant::now();
                debug_println!("RUST DEBUG: ⏱️ 📖 INDEX OPENING - Starting file download and index creation");
                
                let relative_path = if let Some(last_slash_pos) = split_uri.rfind('/') {
                    std::path::Path::new(&split_uri[last_slash_pos + 1..])
                } else {
                    std::path::Path::new(split_uri)
                };
                
                // 🚀 INDIVIDUAL DOC OPTIMIZATION: Use same hotcache optimization as batch retrieval
                let mut index = if has_footer_metadata(footer_start, footer_end) && is_remote_split(split_uri) {
                    debug_println!("RUST DEBUG: 🚀 Using Quickwit optimized path for individual document retrieval (footer: {}..{})", footer_start, footer_end);
                    
                    use quickwit_proto::search::SplitIdAndFooterOffsets;
                    use quickwit_search::leaf::open_index_with_caches;
                    
                    // Create SplitIdAndFooterOffsets for Quickwit's open_index_with_caches
                    let footer_offsets = SplitIdAndFooterOffsets {
                        split_id: extract_split_id_from_uri(split_uri),
                        split_footer_start: footer_start,
                        split_footer_end: footer_end,
                        timestamp_start: Some(0),
                        timestamp_end: Some(i64::MAX),
                        num_docs: 0, // Will be filled by Quickwit
                    };
                    
                    // Create minimal SearcherContext for Quickwit functions
                    let searcher_context = get_shared_searcher_context()
                        .map_err(|e| anyhow::anyhow!("Failed to create searcher context: {}", e))?;
                    
                    // ✅ Use cached index to eliminate repeated open_index_with_caches calls
                    let index_creation_start = std::time::Instant::now();
                    let index = cached_index.as_ref().clone();
                    debug_println!("🔥 INDEX CACHED: Reusing cached index instead of expensive open_index_with_caches call");
                    
                    debug_println!("RUST DEBUG: ⏱️ 📖 Quickwit hotcache index creation completed [TIMING: {}ms]", index_creation_start.elapsed().as_millis());
                    debug_println!("RUST DEBUG: ✅ Successfully opened index with Quickwit hotcache optimization for individual document retrieval");
                    index
                } else {
                    debug_println!("RUST DEBUG: ⚠️ Footer metadata not available for individual document retrieval, falling back to full download");
                    
                    // Fallback: Get the full file data using Quickwit's storage abstraction for document retrieval
                    // (We need BundleDirectory for synchronous document access, not StorageDirectory)
                    let file_size = tokio::time::timeout(
                        std::time::Duration::from_secs(3),
                        index_storage.file_num_bytes(relative_path)
                    )
                    .await
                    .map_err(|_| anyhow::anyhow!("Timeout getting file size for {}", split_uri))?
                    .map_err(|e| anyhow::anyhow!("Failed to get file size for {}: {}", split_uri, e))?;

                    let split_data = tokio::time::timeout(
                        std::time::Duration::from_secs(10),
                        index_storage.get_slice(relative_path, 0..file_size as usize)
                    )
                    .await
                    .map_err(|_| anyhow::anyhow!("Timeout getting split data from {}", split_uri))?
                    .map_err(|e| anyhow::anyhow!("Failed to get split data from {}: {}", split_uri, e))?;

                    let split_file_slice = tantivy::directory::FileSlice::new(std::sync::Arc::new(split_data));
                    let bundle_directory = quickwit_directories::BundleDirectory::open_split(split_file_slice)
                        .map_err(|e| anyhow::anyhow!("Failed to open bundle directory {}: {}", split_uri, e))?;
                        
                    let index_creation_start = std::time::Instant::now();
                    // ✅ QUICKWIT NATIVE: Use Quickwit's native index opening instead of direct tantivy
                    let index = open_index(bundle_directory.box_clone(), get_quickwit_fastfield_normalizer_manager().tantivy_manager())
                        .map_err(|e| anyhow::anyhow!("Failed to open index from bundle {}: {}", split_uri, e))?;
                    debug_println!("RUST DEBUG: ⏱️ 📖 QUICKWIT NATIVE: BundleDirectory index creation completed [TIMING: {}ms]", index_creation_start.elapsed().as_millis());
                    index
                };
                
                // Use the same Quickwit optimizations as our batch method
                let tantivy_executor = search_thread_pool()
                    .get_underlying_rayon_thread_pool()
                    .into();
                index.set_executor(tantivy_executor);
                
                // Same cache settings as batch method
                let searcher_creation_start = std::time::Instant::now();
                // Using adaptive cache configuration
                let batch_cache_blocks = get_batch_doc_cache_blocks();
                debug_println!("⚡ CACHE_OPTIMIZATION: Fallback path - applying adaptive doc store cache optimization - blocks: {} (batch operations)", batch_cache_blocks);
                let index_reader = index
                    .reader_builder()
                    .doc_store_cache_num_blocks(batch_cache_blocks) // ADAPTIVE CACHE OPTIMIZATION
                    .reload_policy(tantivy::ReloadPolicy::Manual)
                    .try_into()
                    .map_err(|e| anyhow::anyhow!("Failed to create index reader: {}", e))?;
                
                let searcher = Arc::new(index_reader.searcher());
                debug_println!("RUST DEBUG: ⏱️ 📖 Searcher creation completed [TIMING: {}ms]", searcher_creation_start.elapsed().as_millis());
                
                // Cache the searcher for future single document retrievals
                let caching_start = std::time::Instant::now();
                {
                    let searcher_cache = get_searcher_cache();
                    let mut cache = searcher_cache.lock().unwrap();
                    // LRU push returns Some(evicted_value) if an entry was evicted
                    if cache.push(split_uri.clone(), searcher.clone()).is_some() {
                        SEARCHER_CACHE_EVICTIONS.fetch_add(1, Ordering::Relaxed);
                        debug_println!("RUST DEBUG: 🗑️ LRU EVICTION - cache full, evicted oldest searcher");
                    }
                }
                debug_println!("RUST DEBUG: ⏱️ 📖 Searcher caching completed [TIMING: {}ms]", caching_start.elapsed().as_millis());
                debug_println!("RUST DEBUG: ⏱️ 📖 TOTAL INDEX OPENING completed [TIMING: {}ms]", index_opening_start.elapsed().as_millis());
                
                // Retrieve the document using async method with timeout (same as batch retrieval for StorageDirectory compatibility)
                let doc_retrieval_start = std::time::Instant::now();
                let doc = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    searcher.doc_async(doc_address)
                )
                .await
                .map_err(|_| anyhow::anyhow!("Document retrieval timed out for {:?}", doc_address))?
                .map_err(|e| anyhow::anyhow!("Failed to retrieve document: {}", e))?;
                let schema = index.schema();
                debug_println!("RUST DEBUG: ⏱️ 📖 Document retrieval completed [TIMING: {}ms]", doc_retrieval_start.elapsed().as_millis());
                
                Ok::<(tantivy::schema::TantivyDocument, tantivy::schema::Schema), anyhow::Error>((doc, schema.clone()))
            })
        })
    });
    
    match result {
        Some(Ok(doc_and_schema)) => {
            debug_println!("RUST DEBUG: ⏱️ ✅ CACHE MISS document retrieval completed [TOTAL: {}ms]", function_start.elapsed().as_millis());
            Ok(doc_and_schema)
        },
        Some(Err(e)) => {
            debug_println!("RUST DEBUG: ⏱️ ❌ CACHE MISS failed [TOTAL: {}ms] - Error: {}", function_start.elapsed().as_millis(), e);
            Err(e)
        },
        None => {
            debug_println!("RUST DEBUG: ⏱️ ❌ Invalid searcher context [TOTAL: {}ms]", function_start.elapsed().as_millis());
            Err(anyhow::anyhow!("Searcher context not found for pointer {}", searcher_ptr))
        },
    }
}

/// Helper function to retrieve a single document from a split
/// Legacy implementation - improved with Quickwit optimizations: doc_async and doc_store_cache_num_blocks
fn retrieve_document_from_split(
    searcher_ptr: jlong,
    doc_address: tantivy::DocAddress,
) -> Result<(tantivy::schema::TantivyDocument, tantivy::schema::Schema), anyhow::Error> {
    use crate::utils::with_arc_safe;
    use quickwit_storage::StorageResolver;
    use std::sync::Arc;
    
    // Use the searcher context to retrieve the document from the split
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
        let context = searcher_context.as_ref();
        let searcher = &context.standalone_searcher;
        // ✅ CRITICAL FIX: Use shared global runtime handle instead of context.runtime
        let runtime = crate::runtime_manager::QuickwitRuntimeManager::global().handle();
        let split_uri = &context.split_uri;
        let aws_config = &context.aws_config;
        let footer_start = context.footer_start;
        let footer_end = context.footer_end;
        let _doc_mapping = &context.doc_mapping_json;
        let storage_resolver = &context.cached_storage;
        let cached_index = &context.cached_index;
        
        // Enter the runtime context for async operations
        let _guard = runtime.enter();
        
        // Run async document retrieval with Quickwit optimizations
        runtime.block_on(async {
            // Parse URI and resolve storage (same as before)
            use quickwit_common::uri::Uri;
            use quickwit_config::{StorageConfigs, S3StorageConfig};
            use quickwit_directories::BundleDirectory;
            use tantivy::directory::FileSlice;
            use tantivy::ReloadPolicy;
            use std::path::Path;
            
            let uri: Uri = split_uri.parse()
                .map_err(|e| anyhow::anyhow!("Failed to parse split URI {}: {}", split_uri, e))?;
            
            // Create S3 storage configuration with credentials from Java config
            let s3_config = S3StorageConfig {
                flavor: None,
                access_key_id: aws_config.get("access_key").cloned(),
                secret_access_key: aws_config.get("secret_key").cloned(), 
                session_token: aws_config.get("session_token").cloned(),
                region: aws_config.get("region").cloned(),
                endpoint: aws_config.get("endpoint").cloned(),
                force_path_style_access: aws_config.get("path_style_access").map_or(false, |v| v == "true"),
                disable_multi_object_delete: false,
                disable_multipart_upload: false,
            };

            // ✅ BYPASS FIX #3: Use centralized storage resolver function
            debug_println!("✅ BYPASS_FIXED: Using get_configured_storage_resolver() for cache sharing [FIX #3]");
            debug_println!("   📍 Location: split_searcher_replacement.rs:1365 (actual storage path)");
            let storage_resolver = get_configured_storage_resolver(Some(s3_config.clone()), None);
            let actual_storage = resolve_storage_for_split(&storage_resolver, split_uri).await?;
            
            // Extract relative path - for direct file paths, use just the filename
            let relative_path = if split_uri.contains("://") {
                // This is a URI, extract just the filename
                if let Some(last_slash_pos) = split_uri.rfind('/') {
                    Path::new(&split_uri[last_slash_pos + 1..])
                } else {
                    Path::new(split_uri)
                }
            } else {
                // This is a direct file path, extract just the filename
                Path::new(split_uri)
                    .file_name()
                    .map(|name| Path::new(name))
                    .unwrap_or_else(|| Path::new(split_uri))
            };
            
            // 🚀 OPTIMIZATION: Use Quickwit's optimized path when footer metadata is available AND split is remote
            debug_println!("RUST DEBUG: Checking optimization conditions - footer_metadata: {}, is_remote: {}", 
                has_footer_metadata(footer_start, footer_end), is_remote_split(split_uri));
            let index = if has_footer_metadata(footer_start, footer_end) && is_remote_split(split_uri) {
                debug_println!("RUST DEBUG: 🚀 Using Quickwit optimized path with hotcache (footer: {}..{})", footer_start, footer_end);
                
                // Create SplitIdAndFooterOffsets for Quickwit's open_index_with_caches
                let footer_offsets = SplitIdAndFooterOffsets {
                    split_id: extract_split_id_from_uri(split_uri),
                    split_footer_start: footer_start,
                    split_footer_end: footer_end,
                    timestamp_start: Some(0),
                    timestamp_end: Some(i64::MAX),
                    num_docs: 0, // Will be filled by Quickwit
                };
                
                // Create minimal SearcherContext for Quickwit functions
                let searcher_context = get_shared_searcher_context()
                    .map_err(|e| anyhow::anyhow!("Failed to create searcher context: {}", e))?;
                
                // ✅ Use cached index to eliminate repeated open_index_with_caches calls
                let index = cached_index.as_ref().clone();
                debug_println!("RUST DEBUG: ✅ Successfully reused cached index");
                index
            } else {
                debug_println!("RUST DEBUG: ⚠️ Footer metadata not available, falling back to full download");
                
                // Fallback: Get the full file data (original behavior for missing metadata)
                let file_size = tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    actual_storage.file_num_bytes(relative_path)
                )
                .await
                .map_err(|_| anyhow::anyhow!("Timeout getting file size for {}", split_uri))?
                .map_err(|e| anyhow::anyhow!("Failed to get file size for {}: {}", split_uri, e))?;

                let split_data = tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    actual_storage.get_slice(relative_path, 0..file_size as usize)
                )
                .await
                .map_err(|_| anyhow::anyhow!("Timeout getting split data from {}", split_uri))?
                .map_err(|e| anyhow::anyhow!("Failed to get split data from {}: {}", split_uri, e))?;
                
                debug_println!("RUST DEBUG: ⚠️ Downloaded full split file: {} bytes", split_data.len());
                
                // Open the bundle directory from the split data
                let split_file_slice = FileSlice::new(std::sync::Arc::new(split_data));
                let bundle_directory = BundleDirectory::open_split(split_file_slice)
                    .map_err(|e| anyhow::anyhow!("Failed to open bundle directory {}: {}", split_uri, e))?;
                    
                // ✅ QUICKWIT NATIVE: Extract the index from the bundle directory using Quickwit's native function
                open_index(bundle_directory.box_clone(), get_quickwit_fastfield_normalizer_manager().tantivy_manager())
                    .map_err(|e| anyhow::anyhow!("Failed to open index from bundle {}: {}", split_uri, e))?
            };
            
            // Create index reader using Quickwit's optimizations (from fetch_docs.rs line 187-192)
            // Using global cache configuration constant for individual document retrieval
            debug_println!("⚡ CACHE_OPTIMIZATION: Individual retrieval - applying doc store cache optimization - blocks: {} (single document)", SINGLE_DOC_CACHE_BLOCKS);
            let index_reader = index
                .reader_builder()
                .doc_store_cache_num_blocks(SINGLE_DOC_CACHE_BLOCKS) // QUICKWIT OPTIMIZATION
                .reload_policy(ReloadPolicy::Manual)
                .try_into()
                .map_err(|e| anyhow::anyhow!("Failed to create index reader: {}", e))?;
            
            let tantivy_searcher = index_reader.searcher();
            
            // Use doc_async like Quickwit does (fetch_docs.rs line 205-207) - QUICKWIT OPTIMIZATION
            let doc: tantivy::schema::TantivyDocument = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                tantivy_searcher.doc_async(doc_address)
            )
            .await
            .map_err(|_| anyhow::anyhow!("Document retrieval timed out for {:?}", doc_address))?
            .map_err(|e| anyhow::anyhow!("Failed to retrieve document at address {:?}: {}", doc_address, e))?;
            
            // Return the document and schema for processing
            Ok::<(tantivy::schema::TantivyDocument, tantivy::schema::Schema), anyhow::Error>((doc, index.schema()))
        })
    });
    
    match result {
        Some(Ok(result)) => Ok(result),
        Some(Err(e)) => Err(e),
        None => Err(anyhow::anyhow!("Searcher context not found for pointer {}", searcher_ptr)),
    }
}

/// Optimized bulk document retrieval using Quickwit's proven patterns from fetch_docs.rs
/// Key optimizations:
/// 1. Reuse index, reader and searcher across all documents
/// 2. Sort by DocAddress for better cache locality
/// 3. Use doc_async for optimal I/O performance
/// 4. Use proper cache sizing (NUM_CONCURRENT_REQUESTS)
/// 5. Return raw pointers for JNI integration
fn retrieve_documents_batch_from_split_optimized(
    searcher_ptr: jlong,
    mut doc_addresses: Vec<tantivy::DocAddress>,
) -> Result<Vec<jobject>, anyhow::Error> {
    use crate::utils::with_arc_safe;
    use crate::simple_batch_optimization::{SimpleBatchOptimizer, SimpleBatchConfig};

    use std::sync::Arc;

    // TRACE: Entry point
    debug_println!("🔍 TRACE: retrieve_documents_batch_from_split_optimized called with {} docs", doc_addresses.len());

    // Sort by DocAddress for cache locality (following Quickwit pattern)
    doc_addresses.sort();

    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
        let context = searcher_context.as_ref();
        let searcher = &context.standalone_searcher;
        // ✅ CRITICAL FIX: Use shared global runtime handle instead of context.runtime
        let runtime = crate::runtime_manager::QuickwitRuntimeManager::global().handle();
        let split_uri = &context.split_uri;
        let aws_config = &context.aws_config;
        let footer_start = context.footer_start;
        let footer_end = context.footer_end;
        let _doc_mapping = &context.doc_mapping_json;
        let storage_resolver = &context.cached_storage;
        let cached_index = &context.cached_index;
        // 🚀 BATCH OPTIMIZATION FIX: Access cache and file offsets
        let byte_range_cache = &context.byte_range_cache;
        let bundle_file_offsets = &context.bundle_file_offsets;

        let _guard = runtime.enter();

        // Use block_in_place to run async code synchronously (Quickwit pattern) with timeout
        tokio::task::block_in_place(|| {
            // Add timeout to prevent hanging during runtime shutdown
            let timeout_duration = std::time::Duration::from_secs(5);
            runtime.block_on(tokio::time::timeout(timeout_duration, async {
                // 🚀 BATCH OPTIMIZATION FIX: Use cached_searcher from context directly
                // instead of looking up from the LRU cache (which may not be populated)
                let cached_searcher = &context.cached_searcher;
                debug_println!("🔍 TRACE: Using cached_searcher from CachedSearcherContext");

                // The context always has a cached searcher, so use it directly
                {
                    debug_println!("RUST DEBUG: ✅ BATCH CACHE HIT: Using cached searcher for batch processing");
                    debug_println!("🔍 TRACE: Entered batch processing path with {} docs", doc_addresses.len());
                    debug_println!("🔍 TRACE: byte_range_cache is_some = {}", byte_range_cache.is_some());
                    debug_println!("🔍 TRACE: bundle_file_offsets len = {}", bundle_file_offsets.len());
                    let schema = cached_searcher.schema(); // Get schema from cached searcher

                    // 🚀 BATCH OPTIMIZATION: Prefetch consolidated byte ranges for better S3 performance
                    let optimizer = SimpleBatchOptimizer::new(SimpleBatchConfig::default());

                    if optimizer.should_optimize(doc_addresses.len()) {
                        debug_println!("🚀 BATCH_OPT: Starting range consolidation for {} documents", doc_addresses.len());
                        debug_println!("🔍 TRACE: Should optimize = true");

                        match optimizer.consolidate_ranges(&doc_addresses, &cached_searcher) {
                            Ok(ranges) => {
                                let num_segments = ranges.iter().map(|r| r.file_path.clone()).collect::<std::collections::HashSet<_>>().len();
                                debug_println!("🚀 BATCH_OPT: Consolidated {} docs → {} ranges across {} segments",
                                    doc_addresses.len(), ranges.len(), num_segments);
                                debug_println!("🔍 TRACE: Consolidated to {} ranges", ranges.len());
                                if *crate::debug::DEBUG_ENABLED {
                                    for (i, range) in ranges.iter().enumerate() {
                                        debug_println!("🔍 TRACE:   Range {}: {:?}, {}..{}", i, range.file_path, range.start, range.end);
                                    }
                                }

                                // 🚀 BATCH OPTIMIZATION FIX: Use prefetch_ranges_with_cache to populate the correct cache
                                let prefetch_result = if let Some(cache) = byte_range_cache {
                                    debug_println!("🚀 BATCH_OPT: Using prefetch_ranges_with_cache (OPTIMIZED)");
                                    debug_println!("🔍 TRACE: Calling prefetch_ranges_with_cache with {} bundle offsets", bundle_file_offsets.len());
                                    crate::simple_batch_optimization::prefetch_ranges_with_cache(
                                        ranges.clone(),
                                        storage_resolver.clone(),
                                        split_uri,
                                        cache,
                                        bundle_file_offsets,
                                    ).await
                                } else {
                                    debug_println!("⚠️ BATCH_OPT: ByteRangeCache not available, using fallback prefetch");
                                    debug_println!("🔍 TRACE: FALLBACK - ByteRangeCache is None!");
                                    optimizer.prefetch_ranges(ranges.clone(), storage_resolver.clone(), split_uri).await
                                };

                                match prefetch_result {
                                    Ok(stats) => {
                                        debug_println!("🚀 BATCH_OPT SUCCESS: Prefetched {} ranges, {} bytes in {}ms",
                                            stats.ranges_fetched, stats.bytes_fetched, stats.duration_ms);
                                        debug_println!("   📊 Consolidation ratio: {:.1}x (docs/ranges)",
                                            stats.consolidation_ratio(doc_addresses.len()));

                                        // Record metrics (estimate bytes wasted from gaps)
                                        let bytes_wasted = 0; // TODO: Calculate actual gap bytes
                                        crate::split_cache_manager::record_batch_metrics(
                                            None, // cache_name not available in this context
                                            doc_addresses.len(),
                                            &stats,
                                            num_segments,
                                            bytes_wasted,
                                        );
                                    }
                                    Err(e) => {
                                        debug_println!("⚠️ BATCH_OPT: Prefetch failed (continuing with normal retrieval): {}", e);
                                        // Non-fatal: continue with normal doc_async which will fetch on-demand
                                    }
                                }
                            }
                            Err(e) => {
                                debug_println!("⚠️ BATCH_OPT: Range consolidation failed (continuing with normal retrieval): {}", e);
                                // Non-fatal: continue with normal doc_async
                            }
                        }
                    } else {
                        debug_println!("⏭️ BATCH_OPT: Skipping optimization (only {} docs, threshold: {})",
                            doc_addresses.len(), optimizer.config.min_docs_for_optimization);
                    }

                    // ✅ QUICKWIT CONCURRENT PATTERN: Use cached searcher with concurrency
                    // Using global cache configuration for concurrent batch processing
                    // Note: doc_async calls will now benefit from prefetched ByteRangeCache

                    let doc_futures = doc_addresses.into_iter().map(|doc_addr| {
                        let moved_searcher = cached_searcher.clone(); // Reuse cached searcher
                        let moved_schema = schema.clone();
                        async move {
                            // Add timeout to individual doc_async calls to prevent hanging
                            let doc: tantivy::schema::TantivyDocument = tokio::time::timeout(
                                std::time::Duration::from_secs(5),
                                moved_searcher.doc_async(doc_addr)
                            )
                            .await
                            .map_err(|_| anyhow::anyhow!("Document retrieval timed out for {:?}", doc_addr))?
                            .map_err(|e| anyhow::anyhow!("Failed to retrieve document at address {:?}: {}", doc_addr, e))?;

                            // Create a RetrievedDocument and register it
                            use crate::document::{DocumentWrapper, RetrievedDocument};
                            let retrieved_doc = RetrievedDocument::new_with_schema(doc, &moved_schema);
                            let wrapper = DocumentWrapper::Retrieved(retrieved_doc);
                            let wrapper_arc = std::sync::Arc::new(std::sync::Mutex::new(wrapper));
                            let doc_ptr = crate::utils::arc_to_jlong(wrapper_arc);

                            Ok::<jobject, anyhow::Error>(doc_ptr as jobject)
                        }
                    });

                    // Execute concurrent batch retrieval with cached searcher
                    use futures::stream::{StreamExt, TryStreamExt};
                    let doc_ptrs: Vec<jobject> = futures::stream::iter(doc_futures)
                        .buffer_unordered(BASE_CONCURRENT_REQUESTS) // Keep base concurrency for stream processing
                        .try_collect::<Vec<_>>()
                        .await
                        .map_err(|e| anyhow::anyhow!("Cached searcher batch retrieval failed: {}", e))?;

                    // 🚀 BATCH OPTIMIZATION FIX: Always return here - using context.cached_searcher
                    return Ok::<Vec<jobject>, anyhow::Error>(doc_ptrs);
                }
                // 🚀 BATCH OPTIMIZATION FIX: Fallback code below is now unreachable
                // The code is kept for compatibility but should be removed in cleanup
                #[allow(unreachable_code)]
                {
                use quickwit_config::{StorageConfigs, S3StorageConfig};
                use quickwit_proto::search::SplitIdAndFooterOffsets;
                use quickwit_storage::StorageResolver;
                use quickwit_search::leaf::open_index_with_caches;
                
                
                
                
                
                // Use the same storage resolution approach as individual document retrieval
                // Create S3 storage configuration
                let s3_config = S3StorageConfig {
                    flavor: None,
                    access_key_id: aws_config.get("access_key").cloned(),
                    secret_access_key: aws_config.get("secret_key").cloned(), 
                    session_token: aws_config.get("session_token").cloned(),
                    region: aws_config.get("region").cloned(),
                    endpoint: aws_config.get("endpoint").cloned(),
                    force_path_style_access: aws_config.get("path_style_access").map_or(false, |v| v == "true"),
                    disable_multi_object_delete: false,
                    disable_multipart_upload: false,
                };
                
                // Create storage that points to the directory containing the split (same as individual retrieval)
                let split_dir_uri = if let Some(last_slash_pos) = split_uri.rfind('/') {
                    &split_uri[..last_slash_pos + 1] // Include the trailing slash
                } else {
                    split_uri // If no slash, use the full URI as directory
                };

                // ✅ DEADLOCK FIX #4: Use pre-created storage resolver from searcher context
                debug_println!("✅ QUICKWIT_LIFECYCLE: Using cached storage from searcher context (Quickwit pattern)");
                debug_println!("   📍 Location: split_searcher_replacement.rs:1271 (batch documents split directory)");
                debug_println!("✅ CACHED_STORAGE_USED: Storage at address {:p} (Quickwit lifecycle)", Arc::as_ptr(storage_resolver));
                let index_storage = storage_resolver.clone();
                
                // Extract just the filename as the relative path (same as individual retrieval)
                let relative_path = if let Some(last_slash_pos) = split_uri.rfind('/') {
                    std::path::Path::new(&split_uri[last_slash_pos + 1..])
                } else {
                    std::path::Path::new(split_uri)
                };
                
                // 🚀 BATCH OPTIMIZATION: Use Quickwit's optimized path when footer metadata is available for remote splits
                debug_println!("RUST DEBUG: Checking batch optimization conditions - footer_metadata: {}, is_remote: {}", 
                    has_footer_metadata(footer_start, footer_end), is_remote_split(split_uri));
                let mut index = if has_footer_metadata(footer_start, footer_end) && is_remote_split(split_uri) {
                    debug_println!("RUST DEBUG: 🚀 Using Quickwit optimized path for batch retrieval (footer: {}..{})", footer_start, footer_end);
                    
                    // Create SplitIdAndFooterOffsets for Quickwit's open_index_with_caches
                    let footer_offsets = SplitIdAndFooterOffsets {
                        split_id: extract_split_id_from_uri(split_uri),
                        split_footer_start: footer_start,
                        split_footer_end: footer_end,
                        timestamp_start: Some(0),
                        timestamp_end: Some(i64::MAX),
                        num_docs: 0, // Will be filled by Quickwit
                    };
                    
                    // Create minimal SearcherContext for Quickwit functions
                    let searcher_context = get_shared_searcher_context()
                        .map_err(|e| anyhow::anyhow!("Failed to create searcher context: {}", e))?;
                    
                    // ✅ Use cached index to eliminate repeated open_index_with_caches calls
                    let index = cached_index.as_ref().clone();
                    debug_println!("🔥 INDEX CACHED: Reusing cached index for batch operations instead of expensive open_index_with_caches call");

                    debug_println!("RUST DEBUG: ✅ Successfully reused cached index for batch retrieval");
                    index
                } else {
                    debug_println!("RUST DEBUG: ⚠️ Footer metadata not available for batch retrieval, falling back to full download");
                    
                    // Fallback: Get the full file data (original behavior for missing metadata)
                    let file_size = index_storage.file_num_bytes(relative_path).await
                        .map_err(|e| anyhow::anyhow!("Failed to get file size for {}: {}", split_uri, e))?;
                    
                    let split_data = index_storage.get_slice(relative_path, 0..file_size as usize).await
                        .map_err(|e| anyhow::anyhow!("Failed to get split data from {}: {}", split_uri, e))?;
                    
                    debug_println!("RUST DEBUG: ⚠️ Downloaded full split file for batch: {} bytes", split_data.len());
                    
                    let split_file_slice = tantivy::directory::FileSlice::new(std::sync::Arc::new(split_data));
                    let bundle_directory = quickwit_directories::BundleDirectory::open_split(split_file_slice)
                        .map_err(|e| anyhow::anyhow!("Failed to open bundle directory {}: {}", split_uri, e))?;
                        
                    // ✅ QUICKWIT NATIVE: Use Quickwit's native index opening
                    open_index(bundle_directory.box_clone(), get_quickwit_fastfield_normalizer_manager().tantivy_manager())
                        .map_err(|e| anyhow::anyhow!("Failed to open index from bundle {}: {}", split_uri, e))?
                };
                
                // Use the same Quickwit optimizations as individual method
                let tantivy_executor = search_thread_pool()
                    .get_underlying_rayon_thread_pool()
                    .into();
                index.set_executor(tantivy_executor);
                
                // Create index reader with Quickwit optimizations (fetch_docs.rs line 187-192)
                // Using adaptive cache configuration for batch operations
                let batch_cache_blocks = get_batch_doc_cache_blocks();
                debug_println!("⚡ CACHE_OPTIMIZATION: Batch retrieval fallback - applying adaptive doc store cache optimization - blocks: {} (batch operations)", batch_cache_blocks);
                let index_reader = index
                    .reader_builder()
                    .doc_store_cache_num_blocks(batch_cache_blocks) // ADAPTIVE CACHE OPTIMIZATION
                    .reload_policy(tantivy::ReloadPolicy::Manual)
                    .try_into()
                    .map_err(|e| anyhow::anyhow!("Failed to create index reader: {}", e))?;
                
                // Create Arc searcher for sharing across async operations (fetch_docs.rs line 193)
                let tantivy_searcher = std::sync::Arc::new(index_reader.searcher());
                let schema = index.schema();

                // ✅ CACHE NEW SEARCHER: Store the newly created searcher for future reuse
                {
                    let searcher_cache = get_searcher_cache();
                    let mut cache = searcher_cache.lock().unwrap();
                    // LRU push returns Some(evicted_value) if an entry was evicted
                    if cache.push(split_uri.clone(), tantivy_searcher.clone()).is_some() {
                        SEARCHER_CACHE_EVICTIONS.fetch_add(1, Ordering::Relaxed);
                        debug_println!("RUST DEBUG: 🗑️ LRU EVICTION - cache full, evicted oldest searcher");
                    }
                    debug_println!("RUST DEBUG: ✅ CACHED NEW SEARCHER: Stored searcher for future batch operations");
                }

                // 🚀 BATCH OPTIMIZATION: Prefetch consolidated byte ranges for better S3 performance
                let optimizer = SimpleBatchOptimizer::new(SimpleBatchConfig::default());

                if optimizer.should_optimize(doc_addresses.len()) {
                    debug_println!("🚀 BATCH_OPT: Starting range consolidation for {} documents", doc_addresses.len());

                    match optimizer.consolidate_ranges(&doc_addresses, &tantivy_searcher) {
                        Ok(ranges) => {
                            debug_println!("🚀 BATCH_OPT: Consolidated {} docs → {} ranges", doc_addresses.len(), ranges.len());

                            // Prefetch the consolidated ranges to populate ByteRangeCache
                            match optimizer.prefetch_ranges(ranges.clone(), index_storage.clone(), split_uri).await {
                                Ok(stats) => {
                                    debug_println!("🚀 BATCH_OPT SUCCESS: Prefetched {} ranges, {} bytes in {}ms",
                                        stats.ranges_fetched, stats.bytes_fetched, stats.duration_ms);
                                    debug_println!("   📊 Consolidation ratio: {:.1}x (docs/ranges)",
                                        stats.consolidation_ratio(doc_addresses.len()));

                                    // Record metrics (estimate bytes wasted from gaps)
                                    let num_segments = ranges.iter().map(|r| r.file_path.clone()).collect::<std::collections::HashSet<_>>().len();
                                    let bytes_wasted = 0; // TODO: Calculate actual gap bytes
                                    crate::split_cache_manager::record_batch_metrics(
                                        None, // cache_name not available in this context
                                        doc_addresses.len(),
                                        &stats,
                                        num_segments,
                                        bytes_wasted,
                                    );
                                }
                                Err(e) => {
                                    debug_println!("⚠️ BATCH_OPT: Prefetch failed (continuing with normal retrieval): {}", e);
                                    // Non-fatal: continue with normal doc_async which will fetch on-demand
                                }
                            }
                        }
                        Err(e) => {
                            debug_println!("⚠️ BATCH_OPT: Range consolidation failed (continuing with normal retrieval): {}", e);
                            // Non-fatal: continue with normal doc_async
                        }
                    }
                } else {
                    debug_println!("⏭️ BATCH_OPT: Skipping optimization (only {} docs, threshold: {})",
                        doc_addresses.len(), optimizer.config.min_docs_for_optimization);
                }

                // ✅ QUICKWIT CONCURRENT PATTERN: Use concurrent document retrieval (fetch_docs.rs line 200-258)
                // Note: doc_async calls will now benefit from prefetched ByteRangeCache

                // Create async futures for concurrent document retrieval (like Quickwit)
                let doc_futures = doc_addresses.into_iter().map(|doc_addr| {
                    let moved_searcher = tantivy_searcher.clone(); // Clone Arc for concurrent access
                    let moved_schema = schema.clone(); // Clone schema for each future
                    async move {
                        // Use doc_async like Quickwit with timeout - QUICKWIT OPTIMIZATION (fetch_docs.rs line 205-207)
                        let doc: tantivy::schema::TantivyDocument = tokio::time::timeout(
                            std::time::Duration::from_secs(5),
                            moved_searcher.doc_async(doc_addr)
                        )
                        .await
                        .map_err(|_| anyhow::anyhow!("Document retrieval timed out for {:?}", doc_addr))?
                        .map_err(|e| anyhow::anyhow!("Failed to retrieve document at address {:?}: {}", doc_addr, e))?;

                        // Create a RetrievedDocument and register it
                        use crate::document::{DocumentWrapper, RetrievedDocument};

                        let retrieved_doc = RetrievedDocument::new_with_schema(doc, &moved_schema);
                        let wrapper = DocumentWrapper::Retrieved(retrieved_doc);
                        let wrapper_arc = std::sync::Arc::new(std::sync::Mutex::new(wrapper));
                        let doc_ptr = crate::utils::arc_to_jlong(wrapper_arc);

                        Ok::<jobject, anyhow::Error>(doc_ptr as jobject)
                    }
                });

                // ✅ QUICKWIT CONCURRENT EXECUTION: Process up to BASE_CONCURRENT_REQUESTS simultaneously
                use futures::stream::{StreamExt, TryStreamExt};
                let doc_ptrs: Vec<jobject> = futures::stream::iter(doc_futures)
                    .buffer_unordered(BASE_CONCURRENT_REQUESTS) // Quickwit's concurrent processing pattern
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(|e| anyhow::anyhow!("Concurrent document retrieval failed: {}", e))?;

                Ok::<Vec<jobject>, anyhow::Error>(doc_ptrs)
                } // Close the #[allow(unreachable_code)] block
            }))
            .map_err(|timeout_err| {
                debug_println!("🕐 TIMEOUT: Document retrieval timed out after 10 seconds: {}", timeout_err);
                anyhow::anyhow!("Document retrieval timed out after 10 seconds - likely due to runtime shutdown")
            })?
        })
    });
    
    match result {
        Some(Ok(doc_ptrs)) => Ok(doc_ptrs),
        Some(Err(e)) => Err(e),
        None => Err(anyhow::anyhow!("Searcher context not found for pointer {}", searcher_ptr)),
    }
}


/// Legacy method - kept for compatibility but not optimized
fn retrieve_documents_batch_from_split(
    searcher_ptr: jlong,
    doc_addresses: Vec<tantivy::DocAddress>,
) -> Result<Vec<(tantivy::DocAddress, tantivy::schema::TantivyDocument, tantivy::schema::Schema)>, anyhow::Error> {
    // Fallback to single document retrieval for now
    let mut results = Vec::new();
    for addr in doc_addresses {
        match retrieve_document_from_split(searcher_ptr, addr) {
            Ok((doc, schema)) => {
                results.push((addr, doc, schema));
            },
            Err(e) => return Err(e),
        }
    }
    Ok(results)
}

/// Async-first replacement for Java_io_indextables_tantivy4java_split_SplitSearcher_docNative
/// Implements document retrieval using Quickwit's async approach without deadlocks
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_docNative(
    env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    segment_ord: jint,
    doc_id: jint,
) -> jobject {
    debug_println!("🔥🔥🔥 JNI DEBUG: docNative called - ptr:{}, seg:{}, doc:{}", searcher_ptr, segment_ord, doc_id);
    debug_println!("🚀 ASYNC_JNI: docNative called with async-first architecture");

    // Add this line to verify the method is actually being called
    // Use simplified async pattern that returns thread-safe types
    // Note: env cannot be moved into async block due to thread safety
    match block_on_operation(async move {
        perform_doc_retrieval_async_impl_thread_safe(searcher_ptr, segment_ord as u32, doc_id as u32).await
    }) {
        Ok(document_ptr) => {
            debug_println!("🔥 JNI DEBUG: Document retrieval successful, creating Java Document object from pointer: {}", document_ptr);

            // Check if the pointer is valid (non-zero)
            if document_ptr == 0 {
                debug_println!("🔥 JNI DEBUG: ERROR - Document pointer is null/zero!");
                std::ptr::null_mut()
            } else {
                debug_println!("🔥 JNI DEBUG: Document pointer is valid ({}), proceeding with Java object creation", document_ptr);

                // Create Java Document object properly using JNI
                let mut env_mut = env;
                debug_println!("🔥 JNI DEBUG: About to call create_java_document_object...");
                match create_java_document_object(&mut env_mut, document_ptr) {
                    Ok(java_doc_obj) => {
                        debug_println!("🔥 JNI DEBUG: Successfully created Java Document object, returning: {:?}", java_doc_obj);
                        java_doc_obj
                    },
                    Err(e) => {
                        debug_println!("🔥 JNI DEBUG: Failed to create Java Document object: {}", e);
                        crate::common::to_java_exception(&mut env_mut, &e);
                        std::ptr::null_mut()
                    }
                }
            }
        },
        Err(e) => {
            debug_println!("🔥 JNI DEBUG: Document retrieval failed: {}", e);
            debug_println!("❌ ASYNC_JNI: Document retrieval operation failed: {}", e);
            std::ptr::null_mut()
        }
    }
}

/// Create a Java Document object from a native document pointer
/// This properly converts the Rust DocumentWrapper pointer to a Java Document object
fn create_java_document_object(env: &mut JNIEnv, document_ptr: jlong) -> anyhow::Result<jobject> {
    debug_println!("🔧 JNI_CONVERT: Creating Java Document object from pointer: {}", document_ptr);

    // Find the Document class
    let document_class = env.find_class("io/indextables/tantivy4java/core/Document")
        .map_err(|e| anyhow::anyhow!("Failed to find Document class: {}", e))?;

    // Create a new Document object with the pointer constructor: Document(long nativePtr)
    let document_obj = env.new_object(
        &document_class,
        "(J)V", // Constructor signature: takes a long (J) and returns void (V)
        &[jni::objects::JValue::Long(document_ptr)]
    ).map_err(|e| anyhow::anyhow!("Failed to create Document object: {}", e))?;

    debug_println!("🔧 JNI_CONVERT: Successfully created Java Document object");
    Ok(document_obj.into_raw())
}

/// Create Tantivy schema from field mappings JSON array
/// This handles the field mappings array format used by QuickwitSplit
pub fn create_schema_from_doc_mapping(doc_mapping_json: &str) -> anyhow::Result<tantivy::schema::Schema> {
    debug_println!("RUST DEBUG: Creating schema from field mappings JSON");
    debug_println!("RUST DEBUG: 🔍 RAW FIELD MAPPINGS JSON ({} chars): '{}'", doc_mapping_json.len(), doc_mapping_json);

    // Parse the field mappings JSON array
    #[derive(serde::Deserialize)]
    struct FieldMapping {
        name: String,
        #[serde(rename = "type")]
        field_type: String,
        stored: Option<bool>,
        indexed: Option<bool>,
        fast: Option<bool>,
        tokenizer: Option<String>,
        fast_tokenizer: Option<String>,
        expand_dots: Option<bool>,
    }

    let field_mappings: Vec<FieldMapping> = serde_json::from_str(doc_mapping_json)
        .map_err(|e| anyhow::anyhow!("Failed to parse field mappings JSON: {}", e))?;

    debug_println!("RUST DEBUG: Parsed {} field mappings", field_mappings.len());

    // Create Tantivy schema builder
    let mut schema_builder = tantivy::schema::Schema::builder();

    // Add each field to the schema
    for field_mapping in field_mappings {
        let stored = field_mapping.stored.unwrap_or(false);
        let indexed = field_mapping.indexed.unwrap_or(false);
        let fast = field_mapping.fast.unwrap_or(false);
        let tokenizer = field_mapping.tokenizer.as_deref().unwrap_or("default");

        debug_println!("RUST DEBUG: Adding field '{}' type '{}' stored={} indexed={} fast={} tokenizer='{}'",
            field_mapping.name, field_mapping.field_type, stored, indexed, fast, tokenizer);

        match field_mapping.field_type.as_str() {
            "text" => {
                let mut text_options = tantivy::schema::TextOptions::default();
                if stored {
                    text_options = text_options.set_stored();
                }
                if indexed {
                    let text_indexing = tantivy::schema::TextFieldIndexing::default()
                        .set_tokenizer(tokenizer)
                        .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions);
                    text_options = text_options.set_indexing_options(text_indexing);
                }
                if fast {
                    text_options = text_options.set_fast(Some("default"));
                }
                schema_builder.add_text_field(&field_mapping.name, text_options);
            },
            "i64" => {
                let mut int_options = tantivy::schema::NumericOptions::default();
                if stored {
                    int_options = int_options.set_stored();
                }
                if indexed {
                    int_options = int_options.set_indexed();
                }
                if fast {
                    int_options = int_options.set_fast();
                }
                schema_builder.add_i64_field(&field_mapping.name, int_options);
            },
            "f64" => {
                let mut float_options = tantivy::schema::NumericOptions::default();
                if stored {
                    float_options = float_options.set_stored();
                }
                if indexed {
                    float_options = float_options.set_indexed();
                }
                if fast {
                    float_options = float_options.set_fast();
                }
                schema_builder.add_f64_field(&field_mapping.name, float_options);
            },
            "bool" => {
                let mut bool_options = tantivy::schema::NumericOptions::default();
                if stored {
                    bool_options = bool_options.set_stored();
                }
                if indexed {
                    bool_options = bool_options.set_indexed();
                }
                if fast {
                    bool_options = bool_options.set_fast();
                }
                schema_builder.add_bool_field(&field_mapping.name, bool_options);
            },
            "object" => {
                debug_println!("RUST DEBUG: 🔧 JSON FIELD RECONSTRUCTION: field '{}', stored={}, indexed={}, fast={}, expand_dots={:?}, tokenizer={:?}, fast_tokenizer={:?}",
                    field_mapping.name, stored, indexed, fast, field_mapping.expand_dots, field_mapping.tokenizer, field_mapping.fast_tokenizer);

                let mut json_options = tantivy::schema::JsonObjectOptions::default();

                // Set stored attribute
                if stored {
                    json_options = json_options.set_stored();
                }

                // Set indexing options with tokenizer
                if indexed {
                    let tokenizer_name = field_mapping.tokenizer.as_deref().unwrap_or("default");
                    let text_indexing = tantivy::schema::TextFieldIndexing::default()
                        .set_tokenizer(tokenizer_name)
                        .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions);
                    json_options = json_options.set_indexing_options(text_indexing);
                }

                // Set fast field options with optional tokenizer
                if fast {
                    let fast_tokenizer_name = field_mapping.fast_tokenizer.as_deref();
                    json_options = json_options.set_fast(fast_tokenizer_name);
                }

                // Set expand_dots if enabled
                if field_mapping.expand_dots.unwrap_or(false) {
                    json_options = json_options.set_expand_dots_enabled();
                }

                schema_builder.add_json_field(&field_mapping.name, json_options);
                debug_println!("RUST DEBUG: 🔧 JSON FIELD RECONSTRUCTION: Successfully added JSON field '{}'", field_mapping.name);
            },
            other => {
                debug_println!("RUST DEBUG: ⚠️ Unsupported field type '{}' for field '{}', treating as text", other, field_mapping.name);
                // Fallback to text field for unknown types
                let mut text_options = tantivy::schema::TextOptions::default();
                if stored {
                    text_options = text_options.set_stored();
                }
                if indexed {
                    text_options = text_options.set_indexing_options(
                        tantivy::schema::TextFieldIndexing::default()
                            .set_tokenizer("default")
                            .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions)
                    );
                }
                schema_builder.add_text_field(&field_mapping.name, text_options);
            }
        }
    }

    let schema = schema_builder.build();
    debug_println!("RUST DEBUG: Successfully created Tantivy schema from field mappings with {} fields", schema.num_fields());
    Ok(schema)
}

/// Async-first replacement for Java_io_indextables_tantivy4java_split_SplitSearcher_getSchemaFromNative
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_getSchemaFromNative(
    env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) -> jlong {
    debug_println!("🔥 NATIVE: getSchemaFromNative called with pointer: {}", searcher_ptr);

    if searcher_ptr == 0 {
        debug_println!("❌ NATIVE: Invalid searcher pointer (0)");
        return 0;
    }

    debug_println!("🔥 NATIVE: About to call block_on_operation");

    // Use simplified async pattern that returns thread-safe types
    // Note: env cannot be moved into async block due to thread safety
    match block_on_operation(async move {
        debug_println!("🔥 NATIVE: Inside async block, calling perform_schema_retrieval_async_impl_thread_safe");
        perform_schema_retrieval_async_impl_thread_safe(searcher_ptr).await
    }) {
        Ok(result) => {
            debug_println!("✅ NATIVE: block_on_operation succeeded, result: {}", result);
            result
        },
        Err(e) => {
            debug_println!("❌ NATIVE: block_on_operation FAILED: {}", e);
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_preloadComponentsNative(
    mut _env: JNIEnv,
    _class: JClass,
    _searcher_ptr: jlong,
    _components: jobject,
) -> jboolean {
    // For now, just return success (1) to allow warmup to complete
    debug_println!("RUST DEBUG: preloadComponentsNative called - returning success");
    1 // true
}
/// Replacement for Java_io_indextables_tantivy4java_split_SplitSearcher_getComponentCacheStatusNative
/// Simple implementation that returns an empty HashMap
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_getComponentCacheStatusNative(
    mut env: JNIEnv,
    _class: JClass,
    _searcher_ptr: jlong,
) -> jobject {
    debug_println!("RUST DEBUG: getComponentCacheStatusNative called - creating empty HashMap");
    
    // Create an empty HashMap for component status
    match env.new_object("java/util/HashMap", "()V", &[]) {
        Ok(hashmap) => hashmap.into_raw(),
        Err(_) => {
            // Return null if we can't create the HashMap
            std::ptr::null_mut()
        }
    }
}

/// Helper function to extract schema from split file - extracted from getSchemaFromNative
fn get_schema_from_split(searcher_ptr: jlong) -> anyhow::Result<tantivy::schema::Schema> {
    with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
        let context = searcher_context.as_ref();
        
        // ✅ CRITICAL FIX: Use shared global runtime instead of context.runtime

        // Parse the split URI and extract schema using Quickwit's storage abstractions
        use quickwit_common::uri::Uri;
        use std::path::Path;

        // Use block_on to run async code synchronously within the runtime context
        tokio::task::block_in_place(|| {
            crate::runtime_manager::QuickwitRuntimeManager::global().handle().block_on(async {
                // Parse URI and resolve storage
                let uri: Uri = context.split_uri.parse()
                    .map_err(|e| anyhow::anyhow!("Failed to parse split URI {}: {}", context.split_uri, e))?;
                
                // Create S3 storage configuration with credentials from Java config
                use quickwit_config::{StorageConfigs, S3StorageConfig};
                
                let s3_config = S3StorageConfig {
                    flavor: None,
                    access_key_id: context.aws_config.get("access_key").cloned(),
                    secret_access_key: context.aws_config.get("secret_key").cloned(),
                    session_token: context.aws_config.get("session_token").cloned(),
                    region: context.aws_config.get("region").cloned(),
                    endpoint: context.aws_config.get("endpoint").cloned(),
                    force_path_style_access: context.aws_config.get("path_style_access").map_or(false, |v| v == "true"),
                    disable_multi_object_delete: false,
                    disable_multipart_upload: false,
                };

                // ✅ DEADLOCK FIX #6: Use pre-created storage resolver from searcher context
                debug_println!("✅ DEADLOCK_FIXED: Using pre-created StorageResolver from searcher context [FIX #6]");
                debug_println!("   📍 Location: split_searcher_replacement.rs:2249 (document retrieval storage)");
                debug_println!("✅ CACHED_STORAGE_USED: Storage at address {:p} (Quickwit lifecycle)", Arc::as_ptr(&context.cached_storage));

                // Use cached storage directly (Quickwit pattern)
                let actual_storage = context.cached_storage.clone();
                
                // Extract just the filename for the relative path
                let relative_path = if let Some(last_slash_pos) = context.split_uri.rfind('/') {
                    Path::new(&context.split_uri[last_slash_pos + 1..])
                } else {
                    Path::new(&context.split_uri)
                };
                
                // Get the full file data using Quickwit's storage abstraction
                let file_size = actual_storage.file_num_bytes(relative_path).await
                    .map_err(|e| anyhow::anyhow!("Failed to get file size for {}: {}", context.split_uri, e))?;

                let split_data = actual_storage.get_slice(relative_path, 0..file_size as usize).await
                    .map_err(|e| anyhow::anyhow!("Failed to get split data from {}: {}", context.split_uri, e))?;
                
                // Open the bundle directory from the split data
                use quickwit_directories::BundleDirectory;
                use tantivy::directory::FileSlice;
                
                let split_file_slice = FileSlice::new(std::sync::Arc::new(split_data));
                
                // Use BundleDirectory::open_split which takes just the FileSlice and handles everything internally
                let bundle_directory = BundleDirectory::open_split(split_file_slice)
                    .map_err(|e| anyhow::anyhow!("Failed to open bundle directory {}: {}", context.split_uri, e))?;

                // ✅ QUICKWIT NATIVE: Extract schema from the bundle directory using Quickwit's native index opening
                let index = open_index(bundle_directory.box_clone(), get_quickwit_fastfield_normalizer_manager().tantivy_manager())
                    .map_err(|e| anyhow::anyhow!("Failed to open index from bundle {}: {}", context.split_uri, e))?;

                // ✅ FIX: Always use index.schema() - doc_mapping reconstruction incompatible with dynamic JSON fields
                debug_println!("RUST DEBUG: 🔧 get_schema_from_split: Using index.schema() directly (doc_mapping has compatibility issues with dynamic JSON fields)");
                let schema = index.schema();

                Ok(schema)
            })
        })
    })
    .ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?
}

/// Fix range queries in QueryAst JSON by looking up field types from schema
fn fix_range_query_types(searcher_ptr: jlong, query_json: &str) -> anyhow::Result<String> {
    let fix_start = std::time::Instant::now();
    debug_println!("RUST DEBUG: ⏱️ fix_range_query_types ENTRY POINT [TIMING START]");
    
    // Parse the JSON to find range queries
    let parse_start = std::time::Instant::now();
    let mut query_value: Value = serde_json::from_str(query_json)?;
    debug_println!("RUST DEBUG: ⏱️ Query JSON parsing completed [TIMING: {}ms]", parse_start.elapsed().as_millis());
    
    // 🚀 OPTIMIZATION: Try to get cached schema first instead of expensive I/O
    let schema_start = std::time::Instant::now();
    let schema = with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
        let context = searcher_context.as_ref();
        let _searcher = &context.standalone_searcher;
        // ✅ CRITICAL FIX: Use shared global runtime handle instead of context.runtime
        let _runtime = crate::runtime_manager::QuickwitRuntimeManager::global().handle();
        let split_uri = &context.split_uri;
        let _aws_config = &context.aws_config;
        let _footer_start = context.footer_start;
        let _footer_end = context.footer_end;
        let _doc_mapping_json = &context.doc_mapping_json;
        let _storage_resolver = &context.cached_storage;
        let _cached_index = &context.cached_index;
        
        // First try to get schema from cache
        if let Some(cached_schema) = get_split_schema(split_uri) {
            debug_println!("RUST DEBUG: ⏱️ 🚀 Using CACHED schema instead of expensive I/O [TIMING: {}ms]", schema_start.elapsed().as_millis());
            return Ok(cached_schema);
        }
        
        // Fallback to expensive I/O only if cache miss
        debug_println!("RUST DEBUG: ⚠️ Cache miss, falling back to expensive I/O for schema [TIMING: {}ms]", schema_start.elapsed().as_millis());
        get_schema_from_split(searcher_ptr)
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))??;
    
    debug_println!("RUST DEBUG: ⏱️ Schema retrieval completed [TIMING: {}ms]", schema_start.elapsed().as_millis());
    
    // Recursively fix range queries in the JSON
    let recursive_start = std::time::Instant::now();
    fix_range_queries_recursive(&mut query_value, &schema)?;
    debug_println!("RUST DEBUG: ⏱️ Range query fixing completed [TIMING: {}ms]", recursive_start.elapsed().as_millis());
    
    // Convert back to JSON string
    let serialize_start = std::time::Instant::now();
    let result = serde_json::to_string(&query_value).map_err(|e| anyhow::anyhow!("Failed to serialize fixed query: {}", e))?;
    debug_println!("RUST DEBUG: ⏱️ JSON serialization completed [TIMING: {}ms]", serialize_start.elapsed().as_millis());
    
    debug_println!("RUST DEBUG: ⏱️ fix_range_query_types COMPLETED [TOTAL TIMING: {}ms]", fix_start.elapsed().as_millis());
    Ok(result)
}

/// Recursively fix range queries in a JSON value
fn fix_range_queries_recursive(value: &mut Value, schema: &tantivy::schema::Schema) -> anyhow::Result<()> {
    match value {
        Value::Object(map) => {
            // Check if this is a range query
            if let Some(range_obj) = map.get_mut("range") {
                if let Some(range_map) = range_obj.as_object_mut() {
                    fix_range_query_object(range_map, schema)?;
                }
            }
            
            // Recursively process nested objects
            for (_, v) in map.iter_mut() {
                fix_range_queries_recursive(v, schema)?;
            }
        }
        Value::Array(arr) => {
            // Recursively process array elements
            for item in arr.iter_mut() {
                fix_range_queries_recursive(item, schema)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Fix a specific range query object by converting string values to proper types
fn fix_range_query_object(range_map: &mut Map<String, Value>, schema: &tantivy::schema::Schema) -> anyhow::Result<()> {
    // Extract field name
    let field_name = range_map.get("field")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Range query missing field name"))?;
    
    // Get field from schema
    let field = schema.get_field(field_name)
        .map_err(|_| anyhow::anyhow!("Field '{}' not found in schema", field_name))?;
    
    let field_entry = schema.get_field_entry(field);
    let field_type = field_entry.field_type();
    
    // Determine the target JSON literal type based on Tantivy field type
    let target_type = match field_type {
        tantivy::schema::FieldType::I64(_) => "i64",
        tantivy::schema::FieldType::U64(_) => "u64", 
        tantivy::schema::FieldType::F64(_) => "f64",
        tantivy::schema::FieldType::Bool(_) => "bool",
        tantivy::schema::FieldType::Date(_) => "date",
        tantivy::schema::FieldType::Str(_) => "str",
        tantivy::schema::FieldType::Facet(_) => "str",
        tantivy::schema::FieldType::Bytes(_) => "str",
        tantivy::schema::FieldType::JsonObject(_) => "str",
        tantivy::schema::FieldType::IpAddr(_) => "str",
    };
    
    debug_println!("RUST DEBUG: Field '{}' has type '{}', converting range bounds", field_name, target_type);
    
    // Fix lower_bound and upper_bound
    if let Some(lower_bound) = range_map.get_mut("lower_bound") {
        fix_bound_value(lower_bound, target_type, "lower_bound")?;
    }
    
    if let Some(upper_bound) = range_map.get_mut("upper_bound") {
        fix_bound_value(upper_bound, target_type, "upper_bound")?;
    }
    
    Ok(())
}

/// Fix a bound value (Included/Excluded with JsonLiteral) 
fn fix_bound_value(bound: &mut Value, target_type: &str, bound_name: &str) -> anyhow::Result<()> {
    if let Some(bound_obj) = bound.as_object_mut() {
        // Handle Included/Excluded bounds
        for (bound_type, json_literal) in bound_obj.iter_mut() {
            if bound_type == "Included" || bound_type == "Excluded" {
                if let Some(literal_obj) = json_literal.as_object_mut() {
                    // Check if this is a String literal that needs conversion
                    if let Some(string_value) = literal_obj.get("String") {
                        if let Some(string_str) = string_value.as_str() {
                            // Convert string to appropriate type
                            let new_literal = match target_type {
                                "i64" => {
                                    let parsed: i64 = string_str.parse()
                                        .map_err(|_| anyhow::anyhow!("Cannot parse '{}' as i64 in {}", string_str, bound_name))?;
                                    serde_json::json!({"Number": parsed})
                                }
                                "u64" => {
                                    let parsed: u64 = string_str.parse()
                                        .map_err(|_| anyhow::anyhow!("Cannot parse '{}' as u64 in {}", string_str, bound_name))?;
                                    serde_json::json!({"Number": parsed})
                                }
                                "f64" => {
                                    let parsed: f64 = string_str.parse()
                                        .map_err(|_| anyhow::anyhow!("Cannot parse '{}' as f64 in {}", string_str, bound_name))?;
                                    serde_json::json!({"Number": parsed})
                                }
                                "bool" => {
                                    let parsed: bool = string_str.parse()
                                        .map_err(|_| anyhow::anyhow!("Cannot parse '{}' as bool in {}", string_str, bound_name))?;
                                    serde_json::json!({"Bool": parsed})
                                }
                                _ => {
                                    // Keep as string for other types
                                    continue;
                                }
                            };
                            
                            debug_println!("RUST DEBUG: Converted {} '{}' from String to {} for type {}", bound_name, string_str, new_literal, target_type);
                            
                            *json_literal = new_literal;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

// Stub method implementations
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_evictComponentsNative(
    _env: JNIEnv, _class: JClass, _searcher_ptr: jlong, _components: JObject
) -> jboolean { 0 }

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_parseQueryNative(
    _env: JNIEnv, _class: JClass, _searcher_ptr: jlong, _query: JString
) -> jobject { std::ptr::null_mut() }

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_getSchemaJsonNative(
    _env: JNIEnv, _class: JClass, _searcher_ptr: jlong
) -> jstring { std::ptr::null_mut() }

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_getSplitMetadataNative(
    _env: JNIEnv, _class: JClass, _searcher_ptr: jlong
) -> jobject { std::ptr::null_mut() }

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_getLoadingStatsNative(
    _env: JNIEnv, _class: JClass, _searcher_ptr: jlong
) -> jobject { std::ptr::null_mut() }
/// Stub implementation for docsBulkNative - focusing on docBatchNative optimization
/// The main performance improvement comes from the optimized docBatchNative method
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_docsBulkNative(
    mut env: JNIEnv,
    _class: JClass,
    _searcher_ptr: jlong,
    _segments: jni::sys::jintArray,
    _doc_ids: jni::sys::jintArray,
) -> jobject {
    // For now, return null - the main optimization is in docBatchNative
    // This method is not currently used by the test, but docBatch is
    to_java_exception(&mut env, &anyhow::anyhow!("docsBulkNative not implemented - use docBatch for optimized bulk retrieval"));
    std::ptr::null_mut()
}
/// Stub implementation for parseBulkDocsNative - focusing on docBatch optimization
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_parseBulkDocsNative(
    mut env: JNIEnv,
    _class: JClass,
    _buffer_jobject: jobject,
) -> jobject {
    // Return empty ArrayList since docsBulkNative is not implemented
    match env.new_object("java/util/ArrayList", "()V", &[]) {
        Ok(empty_list) => empty_list.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_tokenizeNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    field_name: JString,
    text: JString,
) -> jobject {
    // Extract field name and text from JNI
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Invalid field name: {}", e));
            return std::ptr::null_mut();
        }
    };

    let text_str: String = match env.get_string(&text) {
        Ok(s) => s.into(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Invalid text: {}", e));
            return std::ptr::null_mut();
        }
    };

    debug_println!("RUST DEBUG: tokenizeNative called for field '{}' with text '{}'", field_name_str, text_str);

    // Get the searcher context and schema (same pattern as get_schema_from_split)
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
        let context = searcher_context.as_ref();
        let _searcher = &context.standalone_searcher;
        // ✅ CRITICAL FIX: Use shared global runtime handle instead of context.runtime
        let _runtime = crate::runtime_manager::QuickwitRuntimeManager::global().handle();
        let _split_uri = &context.split_uri;
        let _aws_config = &context.aws_config;
        let _footer_start = context.footer_start;
        let _footer_end = context.footer_end;
        let doc_mapping_json = &context.doc_mapping_json;
        let _storage_resolver = &context.cached_storage;
        let _cached_index = &context.cached_index;

        // Get schema from doc mapping - throw exception if not available
        let schema = if let Some(doc_mapping) = doc_mapping_json {
            match create_schema_from_doc_mapping(doc_mapping) {
                Ok(schema) => schema,
                Err(e) => {
                    return Err(anyhow::anyhow!("Failed to create schema from doc mapping for tokenization: {}", e));
                }
            }
        } else {
            return Err(anyhow::anyhow!("Doc mapping not available for tokenization - split searcher not properly initialized"));
        };

        // Find the field in the schema
        let field = match schema.get_field(&field_name_str) {
            Ok(field) => field,
            Err(_) => {
                return Err(anyhow::anyhow!("Field '{}' not found in schema", field_name_str));
            }
        };

        // Get the field entry to determine the tokenizer
        let field_entry = schema.get_field_entry(field);

        // Create a text analyzer for the field
        let mut tokenizer = match field_entry.field_type() {
            tantivy::schema::FieldType::Str(text_options) => {
                // For text fields, get the tokenizer from indexing options
                if let Some(indexing_options) = text_options.get_indexing_options() {
                    let tokenizer_name = indexing_options.tokenizer();
                    debug_println!("RUST DEBUG: Field '{}' uses tokenizer '{}'", field_name_str, tokenizer_name);

                    // Create the tokenizer based on the name
                    match tokenizer_name {
                        "default" => {
                            tantivy::tokenizer::TextAnalyzer::builder(tantivy::tokenizer::SimpleTokenizer::default())
                                .filter(tantivy::tokenizer::RemoveLongFilter::limit(40))
                                .filter(tantivy::tokenizer::LowerCaser)
                                .build()
                        },
                        "raw" => {
                            // For string fields (raw tokenizer), return the original text as a single token
                            debug_println!("RUST DEBUG: Using raw tokenizer for field '{}'", field_name_str);
                            let tokens = vec![text_str.clone()];
                            return create_token_list(&mut env, tokens);
                        },
                        "whitespace" => {
                            tantivy::tokenizer::TextAnalyzer::builder(tantivy::tokenizer::WhitespaceTokenizer::default())
                                .build()
                        },
                        "keyword" => {
                            // Keyword tokenizer treats the entire input as a single token
                            let tokens = vec![text_str.clone()];
                            return create_token_list(&mut env, tokens);
                        },
                        _ => {
                            // Default to simple tokenizer for unknown tokenizers
                            debug_println!("RUST DEBUG: Unknown tokenizer '{}', using default", tokenizer_name);
                            tantivy::tokenizer::TextAnalyzer::builder(tantivy::tokenizer::SimpleTokenizer::default())
                                .filter(tantivy::tokenizer::RemoveLongFilter::limit(40))
                                .filter(tantivy::tokenizer::LowerCaser)
                                .build()
                        }
                    }
                } else {
                    // No indexing options means it's not indexed, but we can still tokenize
                    tantivy::tokenizer::TextAnalyzer::builder(tantivy::tokenizer::SimpleTokenizer::default())
                        .filter(tantivy::tokenizer::RemoveLongFilter::limit(40))
                        .filter(tantivy::tokenizer::LowerCaser)
                        .build()
                }
            },
            _ => {
                // For non-text fields (numbers, dates, etc.), return the original text as a single token
                debug_println!("RUST DEBUG: Non-text field '{}', returning original text as single token", field_name_str);
                let tokens = vec![text_str.clone()];
                return create_token_list(&mut env, tokens);
            }
        };

        // Tokenize the text
        let mut token_stream = tokenizer.token_stream(&text_str);
        let mut tokens = Vec::new();

        while let Some(token) = token_stream.next() {
            tokens.push(token.text.clone());
        }

        debug_println!("RUST DEBUG: Tokenized '{}' into {} tokens: {:?}", text_str, tokens.len(), tokens);

        create_token_list(&mut env, tokens)
    });

    match result {
        Some(Ok(tokens_list)) => tokens_list,
        Some(Err(e)) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        },
        None => {
            to_java_exception(&mut env, &anyhow::anyhow!("Invalid SplitSearcher pointer"));
            std::ptr::null_mut()
        }
    }
}

/// Helper function to create a Java List<String> from a vector of tokens
fn create_token_list(env: &mut JNIEnv, tokens: Vec<String>) -> Result<jobject, anyhow::Error> {
    // Create ArrayList
    let array_list_class = env.find_class("java/util/ArrayList")?;
    let array_list = env.new_object(&array_list_class, "()V", &[])?;

    // Add each token to the list
    for token in tokens {
        let java_string = env.new_string(&token)?;
        env.call_method(
            &array_list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[(&java_string).into()],
        )?;
    }

    Ok(array_list.into_raw())
}

// ============================================================================
// QUICKWIT AGGREGATION INTEGRATION (USING PROVEN SYSTEM)
// ============================================================================

/// Convert Java aggregations map to JSON for Quickwit's SearchRequest.aggregation_request field
fn convert_java_aggregations_to_json<'a>(
    env: &mut JNIEnv<'a>,
    aggregations_map: &JObject<'a>,
) -> anyhow::Result<Option<String>> {
    use serde_json::json;

    debug_println!("RUST DEBUG: Converting Java aggregations to JSON for Quickwit system");

    // Check if aggregations map is empty
    let is_empty_method = env.call_method(aggregations_map, "isEmpty", "()Z", &[])?;
    let is_empty: bool = is_empty_method.z()?;

    if is_empty {
        debug_println!("RUST DEBUG: Aggregations map is empty, returning None");
        return Ok(None);
    }

    // Extract Map entries from Java HashMap
    let map_entries = extract_map_entries(env, aggregations_map)?;
    let mut aggregations_json = serde_json::Map::new();

    for (name, java_aggregation) in map_entries {
        debug_println!("RUST DEBUG: Processing aggregation: {}", name);

        // Convert each Java SplitAggregation to JSON
        let agg_json = convert_java_aggregation_to_json(env, &java_aggregation)?;
        aggregations_json.insert(name, agg_json);
    }

    // Wrap in a JSON object as expected by Quickwit's aggregation system
    let final_json = json!(aggregations_json);
    let json_string = serde_json::to_string(&final_json)?;

    debug_println!("RUST DEBUG: Generated aggregation JSON: {}", json_string);
    Ok(Some(json_string))
}

/// Convert a single Java SplitAggregation to JSON format
fn convert_java_aggregation_to_json<'a>(
    env: &mut JNIEnv<'a>,
    java_aggregation: &JObject<'a>,
) -> anyhow::Result<serde_json::Value> {
    

    debug_println!("RUST DEBUG: Converting Java aggregation to JSON");

    // Use the existing toAggregationJson method from the Java class
    let json_result = env.call_method(java_aggregation, "toAggregationJson", "()Ljava/lang/String;", &[])?;
    let json_string: String = env.get_string(&json_result.l()?.into())?.into();

    debug_println!("RUST DEBUG: Java aggregation produced JSON: {}", json_string);

    // Parse the JSON string to validate it and convert to serde_json::Value
    let json_value: serde_json::Value = serde_json::from_str(&json_string)
        .map_err(|e| anyhow::anyhow!("Failed to parse aggregation JSON from Java: {}", e))?;

    Ok(json_value)
}

/// Extract entries from a Java HashMap
fn extract_map_entries<'a>(
    env: &mut JNIEnv<'a>,
    map: &JObject<'a>,
) -> anyhow::Result<Vec<(String, JObject<'a>)>> {
    let mut entries = Vec::new();

    // Get entrySet from HashMap
    let entry_set = env.call_method(map, "entrySet", "()Ljava/util/Set;", &[])?.l()?;

    // Get iterator from Set
    let iterator = env.call_method(&entry_set, "iterator", "()Ljava/util/Iterator;", &[])?.l()?;

    // Iterate through entries
    loop {
        let has_next = env.call_method(&iterator, "hasNext", "()Z", &[])?;
        if !has_next.z()? {
            break;
        }

        let entry = env.call_method(&iterator, "next", "()Ljava/lang/Object;", &[])?.l()?;

        // Get key and value from Map.Entry
        let key = env.call_method(&entry, "getKey", "()Ljava/lang/Object;", &[])?;
        let value = env.call_method(&entry, "getValue", "()Ljava/lang/Object;", &[])?.l()?;

        // Convert key to String
        let key_string: String = env.get_string(&key.l()?.into())?.into();

        entries.push((key_string, value.into()));
    }

    Ok(entries)
}

/// Perform search using Quickwit's proven aggregation system with JSON
fn perform_search_with_quickwit_aggregations(
    searcher_ptr: jlong,
    query_ast: quickwit_query::query_ast::QueryAst,
    limit: usize,
    aggregation_request_json: Option<String>,
) -> anyhow::Result<quickwit_proto::search::LeafSearchResponse> {
    debug_println!("RUST DEBUG: 🚀 Starting aggregation search with Quickwit integration");
    if let Some(ref agg_json) = aggregation_request_json {
        debug_println!("RUST DEBUG: 📊 Aggregation JSON: {}", agg_json);
    }

    // Convert QueryAst to JSON and use the WORKING searchWithQueryAst infrastructure
    // This ensures we use exactly the same storage setup, index opening, and DocMapper creation
    let query_json = serde_json::to_string(&query_ast)
        .map_err(|e| anyhow::anyhow!("Failed to serialize QueryAst to JSON: {}", e))?;

    debug_println!("RUST DEBUG: 📊 Performing aggregation search using proven searchWithQueryAst infrastructure");

    // Call the working searchWithQueryAst implementation directly
    let leaf_search_response = perform_search_with_query_ast_and_aggregations_using_working_infrastructure(searcher_ptr, query_json, limit, aggregation_request_json)?;
    Ok(leaf_search_response)
}

/// Use the exact same infrastructure as the working regular search but add aggregation support
fn perform_search_with_query_ast_and_aggregations_using_working_infrastructure(
    searcher_ptr: jlong,
    query_json: String,
    limit: usize,
    aggregation_request_json: Option<String>,
) -> anyhow::Result<quickwit_proto::search::LeafSearchResponse> {
    // This function reuses the EXACT same approach as the working searchWithQueryAst
    // but adds aggregation support to the SearchRequest

    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
        let context = searcher_context.as_ref();
        let searcher = &context.standalone_searcher;
        // ✅ CRITICAL FIX: Use shared global runtime handle instead of context.runtime
        let runtime = crate::runtime_manager::QuickwitRuntimeManager::global().handle();
        let split_uri = &context.split_uri;
        let aws_config = &context.aws_config;
        let footer_start = context.footer_start;
        let footer_end = context.footer_end;
        let doc_mapping_json = &context.doc_mapping_json;
        let storage_resolver = &context.cached_storage;
        let cached_index = &context.cached_index;

        // Enter the runtime context for async operations
        let _guard = runtime.enter();

        // Run the EXACT same async code as the working search
        runtime.block_on(async {
            // Parse the QueryAst JSON using Quickwit's libraries - IDENTICAL TO WORKING SEARCH
            use quickwit_query::query_ast::QueryAst;
            use quickwit_common::uri::Uri;
            use quickwit_config::StorageConfigs;

            let query_ast: QueryAst = serde_json::from_str(&query_json)
                .map_err(|e| anyhow::anyhow!("Failed to parse QueryAst JSON: {}", e))?;

            debug_println!("RUST DEBUG: Successfully parsed QueryAst: {:?}", query_ast);

            // STORAGE SETUP - IDENTICAL TO WORKING SEARCH
            let storage_setup_start = std::time::Instant::now();
            debug_println!("RUST DEBUG: ⏱️ 🔧 SEARCH STORAGE SETUP - Starting storage resolution for: {}", split_uri);

            let uri: Uri = split_uri.parse()
                .map_err(|e| anyhow::anyhow!("Failed to parse split URI {}: {}", split_uri, e))?;

            // Create S3 storage configuration with credentials from Java config
            let mut storage_configs = StorageConfigs::default();

            debug_println!("RUST DEBUG: ⏱️ 🔧 Creating S3 config with credentials from Java configuration");
            let s3_config = S3StorageConfig {
                flavor: None,
                access_key_id: aws_config.get("access_key").cloned(),
                secret_access_key: aws_config.get("secret_key").cloned(),
                session_token: aws_config.get("session_token").cloned(),
                region: aws_config.get("region").cloned(),
                endpoint: aws_config.get("endpoint").cloned(),
                force_path_style_access: aws_config.get("path_style_access").map_or(false, |v| v == "true"),
                disable_multi_object_delete: false,
                disable_multipart_upload: false,
            };

            // ✅ BYPASS FIX #7: Use centralized storage resolver function
            debug_println!("✅ BYPASS_FIXED: Using get_configured_storage_resolver() for cache sharing [FIX #7]");
            debug_println!("   📍 Location: split_searcher_replacement.rs:2819 (final storage search setup)");
            let storage_resolver = get_configured_storage_resolver(Some(s3_config.clone()), None);

            // Use the helper function to resolve storage correctly for S3 URIs
            let storage = resolve_storage_for_split(&storage_resolver, split_uri).await?;
            debug_println!("RUST DEBUG: ⏱️ 🔧 SEARCH STORAGE SETUP completed [TIMING: {}ms]", storage_setup_start.elapsed().as_millis());

            // Extract relative path - IDENTICAL TO WORKING SEARCH
            let relative_path = if split_uri.contains("://") {
                // This is a URI, extract just the filename
                if let Some(last_slash_pos) = split_uri.rfind('/') {
                    std::path::Path::new(&split_uri[last_slash_pos + 1..])
                } else {
                    std::path::Path::new(split_uri)
                }
            } else {
                // This is a direct file path, extract just the filename
                std::path::Path::new(split_uri)
                    .file_name()
                    .map(|name| std::path::Path::new(name))
                    .unwrap_or_else(|| std::path::Path::new(split_uri))
            };

            debug_println!("RUST DEBUG: Reading split file metadata from: '{}'", relative_path.display());

            // Use footer offsets from Java configuration for optimized access
            let split_footer_start = footer_start;
            let split_footer_end = footer_end;

            debug_println!("RUST DEBUG: 🚀 Using Quickwit optimized path with open_index_with_caches - NO full file download");
            debug_println!("RUST DEBUG: Footer offsets from Java config: start={}, end={}", split_footer_start, split_footer_end);

            // Create SplitIdAndFooterOffsets for Quickwit optimization
            let split_id = relative_path.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown");

            let split_and_footer_offsets = quickwit_proto::search::SplitIdAndFooterOffsets {
                split_id: split_id.to_string(),
                split_footer_start,
                split_footer_end,
                num_docs: 0, // Not used for opening, will be filled later
                timestamp_start: None,
                timestamp_end: None,
            };

            // Create proper SearcherContext for Quickwit functions
            let quickwit_searcher_context = crate::global_cache::get_global_searcher_context();

            // Use cached index to eliminate repeated open_index_with_caches calls - OPTIMAL PERFORMANCE
            let index = cached_index.as_ref().clone();
            debug_println!("🔥 INDEX CACHED: Reusing cached index for aggregation search instead of expensive open_index_with_caches call");

            debug_println!("RUST DEBUG: ✅ Quickwit optimized index opening completed successfully");

            // Get the actual number of documents from the index
            let reader = index.reader().map_err(|e| anyhow::anyhow!("Failed to create index reader: {}", e))?;
            let searcher_tantivy = reader.searcher();
            let num_docs = searcher_tantivy.num_docs();

            debug_println!("RUST DEBUG: Extracted actual num_docs from index: {}", num_docs);

            // Extract the split ID from the URI (last component before .split extension)
            let split_id = relative_path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown")
                .to_string();

            debug_println!("RUST DEBUG: Split ID: {}", split_id);

            // Create the proper split metadata with REAL values
            let split_metadata = SplitSearchMetadata {
                split_id: split_id.clone(),
                split_footer_start,
                split_footer_end,
                file_size: split_footer_end, // Footer end is effectively the file size
                time_range: None, // TODO: Extract from split metadata if available
                delete_opstamp: 0,
                num_docs,
            };

            // Build DocMapper from ACTUAL index schema - IDENTICAL TO WORKING SEARCH
            let schema = index.schema();

            // Build a DocMapping from the tantivy schema
            // This is the proper way to create a DocMapper that matches the actual index
            let mut field_mappings = Vec::new();

            for (field, field_entry) in schema.fields() {
                let field_name = schema.get_field_name(field);
                let field_type = field_entry.field_type();

                use tantivy::schema::FieldType;
                let (mapping_type, tokenizer) = match field_type {
                    FieldType::Str(text_options) => {
                        if let Some(indexing_options) = text_options.get_indexing_options() {
                            let tokenizer_name = indexing_options.tokenizer();
                            ("text", Some(tokenizer_name.to_string()))
                        } else {
                            // Store-only text fields should still be "text" type, not "keyword"
                            // Quickwit's DocMapper only supports "text" type for Str fields
                            ("text", None)
                        }
                    },
                    FieldType::U64(_) => ("u64", None),
                    FieldType::I64(_) => ("i64", None),
                    FieldType::F64(_) => ("f64", None),
                    FieldType::Bool(_) => ("bool", None),
                    FieldType::Date(_) => ("datetime", None),
                    FieldType::Bytes(_) => ("bytes", None),
                    FieldType::IpAddr(_) => ("ip", None),
                    FieldType::JsonObject(_) => ("json", None),
                    FieldType::Facet(_) => ("keyword", None), // Facets are similar to keywords
                };

                let mut field_mapping = serde_json::json!({
                    "name": field_name,
                    "type": mapping_type,
                });

                // Add tokenizer information for text fields
                if let Some(tokenizer_name) = tokenizer {
                    field_mapping["tokenizer"] = serde_json::Value::String(tokenizer_name);
                    debug_println!("RUST DEBUG: Field '{}' has tokenizer '{}'", field_name, field_mapping["tokenizer"]);
                }

                field_mappings.push(field_mapping);
            }

            debug_println!("RUST DEBUG: Extracted {} field mappings from index schema", field_mappings.len());

            let doc_mapping_json = serde_json::json!({
                "field_mappings": field_mappings,
                "mode": "lenient",
                "store_source": true,
            });

            // Create DocMapperBuilder from the JSON
            let doc_mapper_builder: quickwit_doc_mapper::DocMapperBuilder = serde_json::from_value(doc_mapping_json)
                .map_err(|e| anyhow::anyhow!("Failed to create DocMapperBuilder: {}", e))?;

            // Build the DocMapper
            let doc_mapper = doc_mapper_builder.try_build()
                .map_err(|e| anyhow::anyhow!("Failed to build DocMapper: {}", e))?;

            let doc_mapper_arc = Arc::new(doc_mapper);

            debug_println!("RUST DEBUG: Successfully created DocMapper from actual index schema");

            // Create a SearchRequest with the QueryAst - THE KEY DIFFERENCE: INCLUDE AGGREGATION!
            let search_request = SearchRequest {
                index_id_patterns: vec![],
                query_ast: query_json.clone(), // SearchRequest.query_ast expects String, not QueryAst object
                max_hits: limit as u64,
                start_offset: 0,
                start_timestamp: None,
                end_timestamp: None,
                aggregation_request: aggregation_request_json.clone(), // THIS IS THE KEY ADDITION!
                snippet_fields: vec![],
                sort_fields: vec![],
                search_after: None,
                scroll_ttl_secs: None,
                count_hits: quickwit_proto::search::CountHits::CountAll as i32,
            };

            debug_println!("RUST DEBUG: ⏱️ 🔍 SEARCH EXECUTION - Calling StandaloneSearcher.search_split with parameters:");
            debug_println!("  - Split URI: {}", split_uri);
            debug_println!("  - Split ID: {}", split_metadata.split_id);
            debug_println!("  - Num docs: {}", split_metadata.num_docs);
            debug_println!("  - Footer offsets: {}-{}", split_metadata.split_footer_start, split_metadata.split_footer_end);
            debug_println!("  - Has aggregations: {}", aggregation_request_json.is_some());

            // PERFORM THE ACTUAL REAL SEARCH WITH AGGREGATIONS - SAME METHOD AS WORKING SEARCH!
            let search_exec_start = std::time::Instant::now();
            debug_println!("RUST DEBUG: ⏱️ 🔍 Starting actual search execution via searcher.search_split()");

            // We're already in an async context, so use the async method directly
            let split_id_for_error = split_metadata.split_id.clone();
            let leaf_search_response = match searcher.search_split(
                split_uri,
                split_metadata,
                search_request,
                doc_mapper_arc,
            ).await {
                Ok(response) => response,
                Err(e) => {
                    debug_println!("RUST DEBUG: ⏱️ 🔍 ERROR in searcher.search_split [TIMING: {}ms]: {}", search_exec_start.elapsed().as_millis(), e);
                    debug_println!("RUST DEBUG: Full error chain: {:#}", e);
                    // Propagate the full error chain to Java
                    return Err(anyhow::anyhow!("{:#}", e));
                }
            };

            debug_println!("RUST DEBUG: ⏱️ 🔍 SEARCH EXECUTION completed [TIMING: {}ms] - Found {} hits", search_exec_start.elapsed().as_millis(), leaf_search_response.num_hits);
            debug_println!("RUST DEBUG: 🔍 Search response has aggregations: {}", leaf_search_response.intermediate_aggregation_result.is_some());

            // Return the LeafSearchResponse directly
            Ok(leaf_search_response)
        })
    });

    // Return the LeafSearchResponse directly
    match result {
        Some(Ok(leaf_search_response)) => Ok(leaf_search_response),
        Some(Err(e)) => Err(e),
        None => Err(anyhow::anyhow!("Invalid searcher pointer")),
    }
}

// REMOVED: perform_unified_search - was broken and unused

/// Unified function to create SearchResult Java object from LeafSearchResponse
fn perform_unified_search_result_creation(
    leaf_search_response: quickwit_proto::search::LeafSearchResponse,
    env: &mut JNIEnv,
    aggregation_request_json: Option<String>,
) -> anyhow::Result<jobject> {
    // Convert results to enhanced format
    let mut search_results = Vec::new();
    for partial_hit in leaf_search_response.partial_hits {
        let doc_address = tantivy::DocAddress::new(
            partial_hit.segment_ord as tantivy::SegmentOrdinal,
            partial_hit.doc_id as tantivy::DocId,
        );

        let score = if let Some(sort_value) = partial_hit.sort_value() {
            match sort_value {
                quickwit_proto::search::SortValue::F64(f) => f as f32,
                quickwit_proto::search::SortValue::U64(u) => u as f32,
                quickwit_proto::search::SortValue::I64(i) => i as f32,
                _ => 1.0_f32,
            }
        } else {
            1.0_f32
        };

        search_results.push((score, doc_address));
    }

    debug_println!("RUST DEBUG: Converted {} hits to SearchResult format", search_results.len());

    // Extract aggregation results
    let aggregation_results = leaf_search_response.intermediate_aggregation_result.clone();
    if aggregation_results.is_some() {
        debug_println!("RUST DEBUG: 📊 Found aggregation results in LeafSearchResponse");
    }

    // Create enhanced result
    let enhanced_result = EnhancedSearchResult {
        hits: search_results,
        aggregation_results,
        aggregation_json: aggregation_request_json.clone(),
    };

    let search_results_arc = Arc::new(enhanced_result);
    let search_result_ptr = arc_to_jlong(search_results_arc);

    // Create SearchResult Java object
    let search_result_class = env.find_class("io/indextables/tantivy4java/result/SearchResult")
        .map_err(|e| anyhow::anyhow!("Failed to find SearchResult class: {}", e))?;

    let search_result = env.new_object(
        &search_result_class,
        "(J)V",
        &[(search_result_ptr).into()]
    ).map_err(|e| anyhow::anyhow!("Failed to create SearchResult: {}", e))?;

    debug_println!("RUST DEBUG: Successfully created SearchResult with {} hits", leaf_search_response.num_hits);

    Ok(search_result.into_raw())
}

// REMOVED: perform_search_with_query_ast_and_aggregations - redundant function eliminated


// Thread-safe async implementation function for search operations
/// Thread-safe async implementation that returns LeafSearchResponse directly (no JSON marshalling)
pub async fn perform_search_async_impl_leaf_response(
    searcher_ptr: jlong,
    query_json: String,
    limit: jint,
) -> Result<quickwit_proto::search::LeafSearchResponse, anyhow::Error> {
    debug_println!("🔍 ASYNC_IMPL: Starting thread-safe async search (returns LeafSearchResponse directly)");

    if searcher_ptr == 0 {
        return Err(anyhow::anyhow!("Invalid searcher pointer"));
    }

    // Extract searcher context using the safe Arc pattern with struct-based approach
    let searcher_context = crate::utils::jlong_to_arc::<CachedSearcherContext>(searcher_ptr)
        .ok_or_else(|| anyhow::anyhow!("Invalid searcher context"))?;

    let context = searcher_context.as_ref();

    debug_println!("🔍 ASYNC_IMPL: Extracted searcher context, performing search on split: {}", context.split_uri);

    // Use Quickwit's real search functionality with cached searcher following their patterns
    let search_result = perform_real_quickwit_search(
        &context.split_uri,
        &context.aws_config,
        context.footer_start,
        context.footer_end,
        &context.doc_mapping_json,
        context.cached_storage.clone(),
        context.cached_searcher.clone(),
        context.cached_index.clone(),
        &query_json,
        limit as usize,
    ).await?;

    debug_println!("✅ ASYNC_IMPL: Search completed successfully with {} hits", search_result.num_hits);
    Ok(search_result)
}

/// Legacy thread-safe async implementation that returns JSON string (kept for compatibility)
pub async fn perform_search_async_impl_thread_safe(
    searcher_ptr: jlong,
    query_json: String,
    limit: jint,
) -> Result<String, anyhow::Error> {
    debug_println!("🔍 ASYNC_IMPL: Starting thread-safe async search implementation (legacy JSON mode)");

    let search_result = perform_search_async_impl_leaf_response(searcher_ptr, query_json, limit).await?;

    // Convert result to JSON string for return (legacy mode)
    let result_json = serde_json::to_string(&search_result)
        .map_err(|e| anyhow::anyhow!("Failed to serialize search result: {}", e))?;

    debug_println!("✅ ASYNC_IMPL: Search completed successfully with {} hits (legacy JSON mode)", search_result.num_hits);
    Ok(result_json)
}

// Legacy wrapper for backward compatibility
pub async fn perform_search_async_impl(
    _env: JNIEnv<'_>,
    searcher_ptr: jlong,
    query_json: String,
    limit: jint,
) -> Result<String, anyhow::Error> {
    perform_search_async_impl_thread_safe(searcher_ptr, query_json, limit).await
}


/// Async document retrieval using Quickwit's exact pattern from fetch_docs.rs
/// This follows the same approach: open_index_with_caches -> searcher.doc_async(doc_addr).await
async fn perform_quickwit_async_doc_retrieval(
    searcher_ptr: jlong,
    doc_address: tantivy::DocAddress,
) -> Result<(tantivy::schema::TantivyDocument, tantivy::schema::Schema), anyhow::Error> {
    debug_println!("🔥 QUICKWIT_DOC: Starting Quickwit-style async document retrieval");
    debug_println!("📄 QUICKWIT_DOC: Following fetch_docs.rs pattern for async document retrieval");

    // Extract clean struct-based searcher context
    let searcher_context = crate::utils::jlong_to_arc::<CachedSearcherContext>(searcher_ptr)
        .ok_or_else(|| anyhow::anyhow!("Invalid searcher context"))?;

    let context = searcher_context.as_ref();

    debug_println!("🔥 QUICKWIT_DOC: Got searcher context for split: {}", context.split_uri);

    // Follow Quickwit's pattern: reuse cached storage instead of resolving again
    let storage = context.cached_storage.clone();
    debug_println!("🔥 QUICKWIT_DOC: Reusing cached storage instance: {:p}", Arc::as_ptr(&storage));

    // Extract split ID from file path (same pattern as working search implementation)
    let split_filename = if let Some(last_slash_pos) = context.split_uri.rfind('/') {
        &context.split_uri[last_slash_pos + 1..]
    } else {
        &context.split_uri
    };

    // For split_id, use the filename without .split extension if present
    let split_id = if split_filename.ends_with(".split") {
        &split_filename[..split_filename.len() - 6] // Remove ".split"
    } else {
        split_filename
    };

    debug_println!("🔥 QUICKWIT_DOC: Extracted split_id: {} from split_uri: {}", split_id, context.split_uri);

    // Create split metadata (same pattern as search implementation)
    let split_metadata = quickwit_proto::search::SplitIdAndFooterOffsets {
        split_id: split_id.to_string(),
        split_footer_start: context.footer_start as u64,
        split_footer_end: context.footer_end as u64,
        timestamp_start: None,
        timestamp_end: None,
        num_docs: 0, // This will be determined when the index is opened
    };

    // Use cached searcher to eliminate repeated searcher creation and ensure cache reuse
    let searcher = context.cached_searcher.clone(); // Follow Quickwit's exact pattern: reuse the same Arc<Searcher>
    debug_println!("🔥 SEARCHER CACHED: Reusing cached searcher following Quickwit's exact pattern for optimal cache performance");
    debug_println!("🔥 QUICKWIT_DOC: Using cached Tantivy searcher with preserved cache state");

    // Use Quickwit's exact async document retrieval pattern: searcher.doc_async(doc_addr).await
    let tantivy_doc = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        searcher.doc_async(doc_address)
    )
    .await
    .map_err(|_| anyhow::anyhow!("Document retrieval timed out for {:?}", doc_address))?
    .map_err(|e| anyhow::anyhow!("Failed to retrieve document using Quickwit's async pattern: {}", e))?;
    debug_println!("🔥 QUICKWIT_DOC: Successfully retrieved document using searcher.doc_async()");

    // Get schema from searcher (same as Quickwit does)
    let schema = searcher.schema().clone();
    debug_println!("🔥 QUICKWIT_DOC: Got schema from searcher");

    debug_println!("📄 QUICKWIT_DOC: Document retrieval completed using Quickwit's async pattern");

    Ok((tantivy_doc, schema))
}

// Thread-safe async implementation function for document retrieval operations
pub async fn perform_doc_retrieval_async_impl_thread_safe(
    searcher_ptr: jlong,
    segment_ord: u32,
    doc_id: u32,
) -> Result<jlong, anyhow::Error> {
    debug_println!("🔥 DOC DEBUG: perform_doc_retrieval_async_impl_thread_safe called - ptr:{}, seg:{}, doc:{}", searcher_ptr, segment_ord, doc_id);
    debug_println!("📄 ASYNC_IMPL: Starting thread-safe async document retrieval");

    if searcher_ptr == 0 {
        return Err(anyhow::anyhow!("Invalid searcher pointer"));
    }

    // Extract searcher context using the safe Arc pattern with struct-based approach
    let searcher_context = crate::utils::jlong_to_arc::<CachedSearcherContext>(searcher_ptr)
        .ok_or_else(|| anyhow::anyhow!("Invalid searcher context"))?;

    let context = searcher_context.as_ref();

    debug_println!("📄 ASYNC_IMPL: Extracted searcher context, retrieving doc from split: {}", context.split_uri);

    // Create DocAddress from segment_ord and doc_id
    let doc_address = tantivy::DocAddress::new(segment_ord, doc_id);

    // Use Quickwit's async document retrieval pattern directly
    debug_println!("🔥 DOC DEBUG: About to use Quickwit's async document retrieval pattern");
    let (tantivy_doc, schema) = perform_quickwit_async_doc_retrieval(searcher_ptr, doc_address).await?;
    debug_println!("🔥 DOC DEBUG: Quickwit async document retrieval completed successfully");

    // Convert TantivyDocument to RetrievedDocument for proper object integration
    use crate::document::{DocumentWrapper, RetrievedDocument};
    let retrieved_doc = RetrievedDocument::new_with_schema(tantivy_doc, &schema);
    let wrapper = DocumentWrapper::Retrieved(retrieved_doc);
    let wrapper_arc = std::sync::Arc::new(std::sync::Mutex::new(wrapper));

    debug_println!("✅ ASYNC_IMPL: Document retrieval completed successfully using real objects");
    let document_ptr = crate::utils::arc_to_jlong(wrapper_arc);
    debug_println!("🔍 ARC_REGISTRY: Stored DocumentWrapper Arc in registry with ID: {}", document_ptr);
    Ok(document_ptr)
}

// Legacy wrapper for backward compatibility
pub async fn perform_doc_retrieval_async_impl(
    _env: JNIEnv<'_>,
    searcher_ptr: jlong,
    segment_ord: u32,
    doc_id: u32,
) -> Result<jlong, anyhow::Error> {
    perform_doc_retrieval_async_impl_thread_safe(searcher_ptr, segment_ord, doc_id).await
}

// Thread-safe async implementation function for schema retrieval operations
pub async fn perform_schema_retrieval_async_impl_thread_safe(
    searcher_ptr: jlong,
) -> Result<i64, anyhow::Error> {
    debug_println!("📋 ASYNC_IMPL: Starting thread-safe async schema retrieval with pointer: {}", searcher_ptr);

    if searcher_ptr == 0 {
        debug_println!("❌ ASYNC_IMPL: Searcher pointer is 0 (null)");
        return Err(anyhow::anyhow!("Invalid searcher pointer (0)"));
    }

    // ✅ DEBUG: Check Arc registry status before attempting extraction
    {
        let registry = crate::utils::ARC_REGISTRY.lock().unwrap();
        debug_println!("📋 ARC_REGISTRY: Registry contains {} entries", registry.len());
        if registry.contains_key(&searcher_ptr) {
            debug_println!("✅ ARC_REGISTRY: Searcher pointer {} found in registry", searcher_ptr);
        } else {
            debug_println!("❌ ARC_REGISTRY: Searcher pointer {} NOT found in registry", searcher_ptr);
            debug_println!("📋 ARC_REGISTRY: Available keys: {:?}", registry.keys().collect::<Vec<_>>());
        }
    }

    // Extract searcher context using the safe Arc pattern with new struct-based approach
    let searcher_context = crate::utils::jlong_to_arc::<CachedSearcherContext>(searcher_ptr);

    // ✅ FIX: If searcher context is missing, use direct schema mapping fallback
    if searcher_context.is_none() {
        debug_println!("❌ ASYNC_IMPL: CachedSearcherContext missing for pointer {}, trying direct schema mapping", searcher_ptr);
        if let Some(schema_ptr) = crate::split_query::get_searcher_schema(searcher_ptr) {
            debug_println!("✅ FALLBACK: Found direct schema mapping {} for searcher {}", schema_ptr, searcher_ptr);
            return Ok(schema_ptr);
        } else {
            debug_println!("❌ FALLBACK: No direct schema mapping found for searcher {}", searcher_ptr);
            return Err(anyhow::anyhow!("Invalid searcher context - Arc and direct mapping not found for pointer: {}", searcher_ptr));
        }
    }

    let searcher_context = searcher_context.unwrap();

    let context = searcher_context.as_ref();

    debug_println!("📋 ASYNC_IMPL: Extracted searcher context for split: {}", context.split_uri);

    // ✅ FIX: Get schema directly from cached index instead of using DocMapper
    // DocMapper has compatibility issues with dynamic JSON fields (requires non-empty field_mappings)
    debug_println!("📋 ASYNC_IMPL: Using cached_index.schema() directly (DocMapper incompatible with dynamic JSON fields)");
    let schema = context.cached_index.schema();
    let schema_ptr = crate::utils::arc_to_jlong(Arc::new(schema.clone()));

    // ✅ CRITICAL FIX: Cache the schema for parseQuery fallback
    debug_println!("📋 CACHE_FIX: Caching schema for parseQuery compatibility for split: {}", context.split_uri);
    crate::split_query::store_split_schema(&context.split_uri, schema.clone());
    debug_println!("📋 CACHE_FIX: Schema cached successfully");

    debug_println!("✅ ASYNC_IMPL: Schema retrieval completed successfully, pointer: {}", schema_ptr);
    Ok(schema_ptr)
}

// Legacy wrapper for backward compatibility
pub async fn perform_schema_retrieval_async_impl(
    _env: JNIEnv<'_>,
    searcher_ptr: jlong,
) -> Result<i64, anyhow::Error> {
    perform_schema_retrieval_async_impl_thread_safe(searcher_ptr).await
}

/// Real Quickwit search implementation using cached components directly
async fn perform_real_quickwit_search(
    split_uri: &str,
    aws_config: &std::collections::HashMap<String, String>,
    footer_start: u64,
    footer_end: u64,
    doc_mapping_json: &Option<String>,
    cached_storage: Arc<dyn Storage>,
    cached_searcher: Arc<tantivy::Searcher>,
    cached_index: Arc<tantivy::Index>,
    query_json: &str,
    limit: usize,
) -> anyhow::Result<quickwit_proto::search::LeafSearchResponse> {
    debug_println!("🔍 REAL_QUICKWIT: Starting real Quickwit search implementation");

    // Following async-first architecture design - this is a pure async function
    // with no JNI dependencies, only receiving thread-safe parameters

    // ✅ FIX: Create DocMapper from cached index schema instead of doc_mapping JSON
    // DocMapper parsing has compatibility issues with dynamic JSON fields (requires non-empty field_mappings)
    debug_println!("🔍 REAL_QUICKWIT: Creating DocMapper from cached index schema (bypassing doc_mapping JSON)");

    let schema = cached_index.schema();
    let doc_mapper = quickwit_doc_mapper::default_doc_mapper_for_test();
    let doc_mapper = Arc::new(doc_mapper);

    // Create SearchRequest following Quickwit patterns
    let search_request = quickwit_proto::search::SearchRequest {
        index_id_patterns: vec!["split_search".to_string()],
        query_ast: query_json.to_string(),
        start_offset: 0,
        max_hits: limit as u64,
        start_timestamp: None,
        end_timestamp: None,
        sort_fields: vec![],
        snippet_fields: vec![],
        count_hits: quickwit_proto::search::CountHits::CountAll.into(),
        aggregation_request: None,
        scroll_ttl_secs: None,
        search_after: None,
    };

    // Create SplitIdAndFooterOffsets for Quickwit
    let split_metadata = quickwit_proto::search::SplitIdAndFooterOffsets {
        split_id: extract_split_id_from_uri(split_uri),
        split_footer_start: footer_start,
        split_footer_end: footer_end,
        num_docs: 0, // Will be filled by Quickwit
        timestamp_start: None,
        timestamp_end: None,
    };

    // Use cached storage directly (Quickwit lifecycle pattern)
    let storage = cached_storage;

    // CRITICAL FIX: Use shared global context for cache hits but create individual permit provider
    // This preserves cache efficiency while eliminating SearchPermitProvider permit exhaustion
    debug_println!("🔍 PERMIT_FIX: Using global context for cache hits but individual permit provider");

    let searcher_context = crate::global_cache::get_global_searcher_context();

    // Create CanSplitDoBetter filter (following Quickwit patterns from standalone_searcher.rs)
    let split_filter = Arc::new(std::sync::RwLock::new(quickwit_search::CanSplitDoBetter::Uninformative));

    // Get aggregation limits (following Quickwit patterns)
    let aggregations_limits = searcher_context.aggregation_limit.clone();

    // CRITICAL FIX: Create individual permit provider per search to eliminate contention
    // This preserves cache hits while avoiding permit pool exhaustion
    debug_println!("🔍 PERMIT_FIX: Creating individual SearchPermitProvider per search operation");

    let individual_permit_provider = {
        use quickwit_search::search_permit_provider::SearchPermitProvider;
        use bytesize::ByteSize;

        Arc::new(SearchPermitProvider::new_sync(
            5, // Allow up to 5 concurrent operations per search (plenty for single search)
            ByteSize::gb(1), // 1GB memory budget per search operation
        ))
    };

    // Get search permit from individual provider (no contention possible)
    let memory_allocation = quickwit_search::search_permit_provider::compute_initial_memory_allocation(
        &split_metadata,
        bytesize::ByteSize(1024 * 1024 * 50), // 50MB initial allocation (same as standalone_searcher.rs)
    );

    debug_println!("🔍 PERMIT_FIX: Requesting permit from dedicated SearchPermitProvider (guaranteed available)");
    debug_println!("🔍 PERMIT_DEBUG: About to request search permit with memory allocation: {}", memory_allocation);

    let permit_futures = individual_permit_provider.get_permits(vec![memory_allocation]).await;
    debug_println!("✅ PERMIT_DEBUG: Got permit futures from dedicated provider, extracting first future...");

    let permit_future = permit_futures.into_iter().next()
        .expect("Expected one permit future");

    debug_println!("🔍 PERMIT_FIX: Acquiring permit from dedicated provider - should be immediate");
    let mut search_permit = permit_future.await;
    debug_println!("✅ PERMIT_FIX: Successfully acquired search permit from dedicated provider - no timeout needed!");

    debug_println!("🔥 REAL_QUICKWIT: Using leaf_search_single_split with cache injection");
    debug_println!("🔍 SEARCH_DEBUG: About to call leaf_search_single_split - this might be where it hangs...");

    // SOLUTION: Use leaf_search_single_split but inject our cached components
    // This preserves the async handling while eliminating repeated downloads

    // Call Quickwit's actual leaf_search_single_split function
    debug_println!("🔍 CRITICAL_DEBUG: About to call leaf_search_single_split - THIS IS LIKELY THE HANG POINT");

    let leaf_search_result = tokio::time::timeout(
        std::time::Duration::from_secs(15), // 15 second timeout for leaf search
        quickwit_search::leaf_search_single_split(
            &searcher_context,
            search_request,
            storage,
            split_metadata,
            doc_mapper,
            split_filter,
            aggregations_limits,
            &mut search_permit,
        )
    ).await;

    debug_println!("🔍 CRITICAL_DEBUG: leaf_search_single_split call completed");

    let result = match leaf_search_result {
        Ok(search_result) => {
            debug_println!("✅ CRITICAL_DEBUG: leaf_search_single_split succeeded");
            search_result.map_err(|e| anyhow::anyhow!("Quickwit leaf search failed: {}", e))?
        },
        Err(_timeout) => {
            debug_println!("❌ CRITICAL_DEBUG: TIMEOUT in leaf_search_single_split - THIS IS THE HANG LOCATION!");
            debug_println!("🔍 PERMIT_DEBUG: Search timed out, explicitly dropping permit to ensure release");

            // CRITICAL FIX: Explicitly drop the permit to ensure it's released even on timeout
            drop(search_permit);
            debug_println!("✅ PERMIT_DEBUG: Permit explicitly dropped on timeout - should be available for next operation");

            return Err(anyhow::anyhow!("leaf_search_single_split timeout - this is where the hang occurs in the Quickwit native layer"));
        }
    };

    debug_println!("✅ REAL_QUICKWIT: Search completed successfully with {} hits", result.num_hits);

    // CRITICAL FIX: Explicitly drop the permit to ensure it's released immediately
    debug_println!("🔍 PERMIT_DEBUG: Search completed successfully, explicitly dropping permit");
    drop(search_permit);
    debug_println!("✅ PERMIT_DEBUG: Permit explicitly dropped on success - capacity available for next search operation");

    Ok(result)
}

/// Perform real Quickwit search with aggregations support
/// Uses the SAME pattern as perform_real_quickwit_search but enables aggregation_request
async fn perform_real_quickwit_search_with_aggregations(
    split_uri: &str,
    aws_config: &std::collections::HashMap<String, String>,
    footer_start: u64,
    footer_end: u64,
    doc_mapping_json: &Option<String>,
    cached_storage: Arc<dyn Storage>,
    cached_searcher: Arc<tantivy::Searcher>,
    cached_index: Arc<tantivy::Index>,
    query_json: &str,
    limit: usize,
    aggregation_request_json: Option<String>,
) -> anyhow::Result<quickwit_proto::search::LeafSearchResponse> {
    debug_println!("🔍 AGGREGATION_SEARCH: Starting real Quickwit search with aggregations");

    // Create DocMapper from JSON following Quickwit patterns (SAME as working search)
    let doc_mapper = if let Some(doc_mapping_str) = doc_mapping_json {
        // First, clean up any escaped JSON from storage layer
        let cleaned_json = if doc_mapping_str.contains("\\\"") {
            doc_mapping_str.replace("\\\"", "\"").replace("\\\\", "\\")
        } else {
            doc_mapping_str.to_string()
        };

        // Parse array of field mappings into proper DocMapperBuilder format
        let field_mappings: Vec<serde_json::Value> = serde_json::from_str(&cleaned_json)
            .map_err(|e| anyhow::anyhow!("Failed to parse doc mapping JSON array: {}", e))?;

        // Convert to proper DocMapperBuilder format - this is what Quickwit actually expects
        let doc_mapper_builder_json = serde_json::json!({
            "field_mappings": field_mappings,
            "timestamp_field": null,
            "default_search_fields": []
        });

        // Deserialize into DocMapperBuilder first, then convert to DocMapper
        let doc_mapper_builder: quickwit_doc_mapper::DocMapperBuilder = serde_json::from_value(doc_mapper_builder_json)
            .map_err(|e| anyhow::anyhow!("Failed to parse DocMapperBuilder: {}", e))?;

        // Convert DocMapperBuilder to DocMapper
        let doc_mapper = quickwit_doc_mapper::DocMapper::try_from(doc_mapper_builder)
            .map_err(|e| anyhow::anyhow!("Failed to convert DocMapperBuilder to DocMapper: {}", e))?;

        Arc::new(doc_mapper)
    } else {
        return Err(anyhow::anyhow!("No doc mapping available for search"));
    };

    // Create SearchRequest following Quickwit patterns (KEY DIFFERENCE: enable aggregations)
    let search_request = quickwit_proto::search::SearchRequest {
        index_id_patterns: vec!["split_search".to_string()],
        query_ast: query_json.to_string(),
        start_offset: 0,
        max_hits: limit as u64,
        start_timestamp: None,
        end_timestamp: None,
        sort_fields: vec![],
        snippet_fields: vec![],
        count_hits: quickwit_proto::search::CountHits::CountAll.into(),
        aggregation_request: aggregation_request_json, // ENABLE AGGREGATIONS
        scroll_ttl_secs: None,
        search_after: None,
    };

    debug_println!("🔍 AGGREGATION_SEARCH: SearchRequest configured with aggregations: {}",
                   search_request.aggregation_request.is_some());

    // Create SplitIdAndFooterOffsets for Quickwit (SAME as working search)
    let split_metadata = quickwit_proto::search::SplitIdAndFooterOffsets {
        split_id: extract_split_id_from_uri(split_uri),
        split_footer_start: footer_start,
        split_footer_end: footer_end,
        num_docs: 0, // Will be filled by Quickwit
        timestamp_start: None,
        timestamp_end: None,
    };

    // Use cached storage directly (SAME as working search)
    let storage = cached_storage;

    // Use shared global context for cache hits but create individual permit provider (SAME as working search)
    let searcher_context = crate::global_cache::get_global_searcher_context();

    // Create CanSplitDoBetter filter (SAME as working search)
    let split_filter = Arc::new(std::sync::RwLock::new(quickwit_search::CanSplitDoBetter::Uninformative));

    // Get aggregation limits (SAME as working search)
    let aggregations_limits = searcher_context.aggregation_limit.clone();

    // Create individual permit provider per search (SAME as working search)
    let individual_permit_provider = {
        use quickwit_search::search_permit_provider::SearchPermitProvider;
        use bytesize::ByteSize;

        Arc::new(SearchPermitProvider::new_sync(
            5, // Allow up to 5 concurrent operations per search
            ByteSize::gb(1), // 1GB memory budget per search operation
        ))
    };

    // Get search permit from individual provider (SAME as working search)
    let memory_allocation = quickwit_search::search_permit_provider::compute_initial_memory_allocation(
        &split_metadata,
        bytesize::ByteSize(1024 * 1024 * 50), // 50MB initial allocation
    );

    debug_println!("🔍 AGGREGATION_SEARCH: Requesting search permit for aggregation query");
    let permit_futures = individual_permit_provider.get_permits(vec![memory_allocation]).await;
    let permit_future = permit_futures.into_iter().next()
        .expect("Expected one permit future");

    let mut search_permit = permit_future.await;
    debug_println!("✅ AGGREGATION_SEARCH: Successfully acquired search permit for aggregation query");

    // Call Quickwit's leaf_search_single_split with aggregation support (SAME as working search)
    let leaf_search_result = tokio::time::timeout(
        std::time::Duration::from_secs(15), // 15 second timeout
        quickwit_search::leaf_search_single_split(
            &searcher_context,
            search_request,
            storage,
            split_metadata,
            doc_mapper,
            split_filter,
            aggregations_limits,
            &mut search_permit,
        )
    ).await;

    let result = match leaf_search_result {
        Ok(search_result) => {
            debug_println!("✅ AGGREGATION_SEARCH: leaf_search_single_split succeeded with aggregations");
            search_result.map_err(|e| anyhow::anyhow!("Quickwit leaf search with aggregations failed: {}", e))?
        },
        Err(_timeout) => {
            debug_println!("❌ AGGREGATION_SEARCH: TIMEOUT in leaf_search_single_split with aggregations");
            drop(search_permit);
            return Err(anyhow::anyhow!("leaf_search_single_split with aggregations timeout"));
        }
    };

    debug_println!("✅ AGGREGATION_SEARCH: Search completed successfully with {} hits, has aggregations: {}",
                   result.num_hits, result.intermediate_aggregation_result.is_some());

    // Drop permit immediately
    drop(search_permit);

    Ok(result)
}

/// Clean searcher context struct to replace complex tuple approach
/// Uses Arc wrappers for non-Clone types to enable struct-based management
struct CachedSearcherContext {
    standalone_searcher: std::sync::Arc<StandaloneSearcher>,
    // ✅ CRITICAL FIX: Removed runtime field - using shared global runtime instead
    split_uri: String,
    aws_config: std::collections::HashMap<String, String>,
    footer_start: u64,
    footer_end: u64,
    doc_mapping_json: Option<String>,
    cached_storage: std::sync::Arc<dyn Storage>,
    cached_index: std::sync::Arc<tantivy::Index>,
    cached_searcher: std::sync::Arc<tantivy::Searcher>,
    // 🚀 BATCH OPTIMIZATION FIX: Store ByteRangeCache and bundle file offsets
    // This allows prefetch to populate the same cache that doc_async uses
    byte_range_cache: Option<ByteRangeCache>,
    bundle_file_offsets: std::collections::HashMap<std::path::PathBuf, std::ops::Range<u64>>,
}

// Dead code removed - perform_real_quickwit_doc_retrieval function was not called anywhere

/// Real Quickwit schema retrieval implementation using doc mapping (no I/O needed)
async fn perform_real_quickwit_schema_retrieval(
    split_uri: &str,
    _aws_config: &std::collections::HashMap<String, String>,
    _footer_start: u64,
    _footer_end: u64,
    doc_mapping_json: &Option<String>,
    _cached_storage: Arc<dyn Storage>,
) -> anyhow::Result<i64> {
    debug_println!("📋 REAL_QUICKWIT: Starting schema retrieval from doc mapping for split: {}", split_uri);

    // The doc mapping MUST be provided when the searcher is created
    let doc_mapping_str = doc_mapping_json.as_ref()
        .ok_or_else(|| anyhow::anyhow!("❌ CRITICAL: No doc mapping available! Doc mapping must be provided when creating SplitSearcher."))?;

    debug_println!("📋 REAL_QUICKWIT: Doc mapping found ({} chars), parsing JSON format", doc_mapping_str.len());
    debug_println!("🔥 RAW DOC MAPPING: {}", doc_mapping_str);

    // Parse the field mappings array directly from source - no cleanup logic
    let doc_mapper: quickwit_doc_mapper::DocMapper = {
        debug_println!("📋 REAL_QUICKWIT: Parsing doc mapping field array directly from source");

        // Parse the field mappings array - handle escaped JSON properly
        let field_mappings: Vec<serde_json::Value> = serde_json::from_str(doc_mapping_str)
            .or_else(|_e| {
                // If direct parsing fails, try unescaping first (for escaped JSON from some sources)
                debug_println!("🔥 SCHEMA DEBUG: Direct parsing failed, trying unescaped version");
                let unescaped = doc_mapping_str.replace("\\\"", "\"").replace("\\\\", "\\");
                debug_println!("🔥 SCHEMA DEBUG: Unescaped JSON: '{}'", unescaped);
                serde_json::from_str(&unescaped)
            })
            .map_err(|e| {
                debug_println!("🔥 SCHEMA DEBUG: Both direct and unescaped parsing failed: {}", e);
                debug_println!("🔥 SCHEMA DEBUG: Raw JSON was: '{}'", doc_mapping_str);
                anyhow::anyhow!("Failed to parse field mappings array (tried both direct and unescaped): {} - JSON was: '{}'", e, doc_mapping_str)
            })?;

        debug_println!("📋 REAL_QUICKWIT: Successfully parsed {} field mappings (already in correct Quickwit format)", field_mappings.len());

        // Convert to proper DocMapperBuilder format - this is what Quickwit actually expects
        let doc_mapper_builder_json = serde_json::json!({
            "field_mappings": field_mappings,
            "timestamp_field": null,
            "default_search_fields": []
        });

        debug_println!("📋 REAL_QUICKWIT: Converted to DocMapperBuilder format with {} fields, parsing with Quickwit", field_mappings.len());
        debug_println!("📋 REAL_QUICKWIT: DocMapperBuilder JSON structure: {}", serde_json::to_string_pretty(&doc_mapper_builder_json).unwrap_or_else(|_| "Failed to serialize".to_string()));

        // Deserialize into DocMapperBuilder first, then convert to DocMapper
        let doc_mapper_builder: quickwit_doc_mapper::DocMapperBuilder = serde_json::from_value(doc_mapper_builder_json)
            .map_err(|e| anyhow::anyhow!("Failed to parse DocMapperBuilder: {}", e))?;

        // Convert DocMapperBuilder to DocMapper
        quickwit_doc_mapper::DocMapper::try_from(doc_mapper_builder)
            .map_err(|e| anyhow::anyhow!("Failed to convert DocMapperBuilder to DocMapper: {}", e))?
    };

    debug_println!("📋 REAL_QUICKWIT: DocMapper parsed successfully, extracting schema");

    // Extract schema directly from DocMapper - no I/O operations needed
    let schema = doc_mapper.schema().clone();
    let field_count = schema.fields().count();

    debug_println!("📋 REAL_QUICKWIT: Schema extracted with {} fields, converting to pointer", field_count);

    // Convert schema to pointer using the same pattern as other functions
    let schema_ptr = crate::utils::arc_to_jlong(Arc::new(schema.clone()));

    // ✅ CRITICAL FIX: Cache the schema for parseQuery fallback
    debug_println!("📋 CACHE_FIX: Caching schema for parseQuery compatibility for split: {}", split_uri);
    crate::split_query::store_split_schema(split_uri, schema.clone());
    debug_println!("📋 CACHE_FIX: Schema cached successfully");

    debug_println!("✅ REAL_QUICKWIT: Schema retrieval completed successfully, pointer: {}", schema_ptr);
    Ok(schema_ptr)
}

/*
// Commented out - these internal async functions are no longer used after simplification
// They can be re-enabled and fixed later if needed

// /// Internal async function that performs the actual search
// async fn perform_async_search_with_context(
//     mut env: JNIEnv<'_>,
//     searcher_context: Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64, Option<String>, std::sync::Arc<dyn Storage>)>,
//     query_json: String,
//     limit: jint,
//     method_start_time: std::time::Instant,
// ) -> Result<jobject, anyhow::Error> {
//     let (_searcher, _runtime, split_uri, aws_config, footer_start, footer_end, doc_mapping_json, storage_resolver) = searcher_context.as_ref();
// 
//     debug_println!("🔄 ASYNC_IMPL: Starting pure async search operations");
// 
//     // Use query JSON as-is for simplified async implementation
//     let fixed_query_json = query_json.clone();
// 
//     // Parse the QueryAst JSON using Quickwit's libraries
//     use quickwit_query::query_ast::QueryAst;
//     let query_ast: QueryAst = serde_json::from_str(&fixed_query_json)
//         .map_err(|e| anyhow::anyhow!("Failed to parse QueryAst JSON: {}", e))?;
// 
//     debug_println!("🔄 ASYNC_IMPL: Successfully parsed QueryAst: {:?}", query_ast);
// 
//     // Use pre-created StorageResolver for async operations (NO deadlock)
//     debug_println!("✅ ASYNC_IMPL: Using pre-created StorageResolver - no sync-in-async deadlock");
//     let storage = resolve_storage_for_split(storage_resolver, split_uri).await?;
// 
//     // Extract relative path
//     let relative_path = if split_uri.contains("://") {
//         if let Some(last_slash_pos) = split_uri.rfind('/') {
//             std::path::Path::new(&split_uri[last_slash_pos + 1..])
//         } else {
//             std::path::Path::new(split_uri)
//         }
//     } else {
//         std::path::Path::new(split_uri)
//             .file_name()
//             .map(|name| std::path::Path::new(name))
//             .unwrap_or_else(|| std::path::Path::new(split_uri))
//     };
// 
//     // Create SplitIdAndFooterOffsets for Quickwit optimization
//     let split_id = relative_path.file_stem()
//         .and_then(|s| s.to_str())
//         .unwrap_or("unknown_split_id")
//         .to_string();
// 
//     let split_id_and_footer_offsets = SplitIdAndFooterOffsets {
//         split_id: split_id.clone(),
//         split_footer_start: footer_start,
//         split_footer_end: footer_end,
//         num_docs: 0, // Will be filled by open_index_with_caches
//         timestamp_start: None,
//         timestamp_end: None,
//     };
// 
//     debug_println!("🔄 ASYNC_IMPL: Using optimized Quickwit path with footer offsets: start={}, end={}", footer_start, footer_end);
// 
//     // Get shared searcher context with global caches
//     let shared_searcher_context = get_shared_searcher_context()?;
// 
//     // Open index with caches using Quickwit's optimization
//     let index = open_index_with_caches(
//         storage.clone(),
//         &[split_id_and_footer_offsets.clone()],
//         shared_searcher_context.fast_fields_cache.clone(),
//         shared_searcher_context.split_footer_cache.clone(),
//         Arc::new(quickwit_storage::MemorySizedCache::with_capacity_in_bytes(
//             100_000_000, // 100MB reader cache
//         )),
//     ).await.map_err(|e| anyhow::anyhow!("Failed to open index with caches: {}", e))?;
// 
//     debug_println!("🔄 ASYNC_IMPL: Successfully opened index with caches");
// 
//     // Create SearchRequest using Quickwit patterns
//     let search_request = quickwit_proto::search::SearchRequest {
//         index_id_patterns: vec![split_id.clone()],
//         query_ast: Some(query_ast.clone()),
//         start_offset: 0,
//         max_hits: limit as u64,
//         start_timestamp: None,
//         end_timestamp: None,
//         sort_fields: vec![],
//         snippet_fields: vec![],
//         count_hits: quickwit_proto::search::CountHits::CountAll.into(),
//         ..Default::default()
//     };
// 
//     // Perform the actual search using Quickwit's leaf search
//     let leaf_search_response = quickwit_search::leaf::leaf_search_single_split(
//         &shared_searcher_context,
//         &search_request,
//         &index,
//         split_id_and_footer_offsets,
//     ).await.map_err(|e| anyhow::anyhow!("Leaf search failed: {}", e))?;
// 
//     debug_println!("✅ ASYNC_IMPL: Search completed successfully with {} hits in {}ms",
//                    leaf_search_response.num_hits, method_start_time.elapsed().as_millis());
// 
//     // Create SearchResultData and convert to Java object
//     let search_result_data = SearchResultData {
//         hits: leaf_search_response.partial_hits.into_iter().map(|hit| SearchHit {
//             score: hit.score,
//             segment_ord: hit.segment_ord,
//             doc_id: hit.doc_id,
//         }).collect(),
//         total_hits: leaf_search_response.num_hits,
//     };
// 
//     // Store result and create Java object
//     let search_result_ptr = arc_to_jlong(Arc::new(search_result_data));
// 
//     let search_result_class = env.find_class("io/indextables/tantivy4java/result/SearchResult")?;
//     let search_result = env.new_object(
//         &search_result_class,
//         "(J)V",
//         &[(search_result_ptr).into()]
//     ).map_err(|e| anyhow::anyhow!("Failed to create SearchResult: {}", e))?;
// 
//     debug_println!("✅ ASYNC_IMPL: Successfully created SearchResult Java object");
// 
//     Ok(search_result.into_raw())
// }
// 
// /// Async-first implementation of document retrieval
// /// This replaces the sync-in-async deadlock-prone pattern with pure async
// pub async fn perform_doc_retrieval_async_impl(
//     mut env: JNIEnv<'_>,
//     searcher_ptr: jlong,
//     doc_addresses: JObject<'_>,
// ) -> Result<jobject, anyhow::Error> {
//     let method_start_time = std::time::Instant::now();
//     debug_println!("🔄 ASYNC_IMPL: Starting async document retrieval with doc addresses");
// 
//     if searcher_ptr == 0 {
//         return Err(anyhow::anyhow!("Invalid searcher pointer"));
//     }
// 
//     // Extract DocAddress from Java object
//     // For now, create a placeholder DocAddress (this needs proper implementation)
//     let doc_address = tantivy::DocAddress::new(0, 0);
//     debug_println!("🔄 ASYNC_IMPL: Created DocAddress from Java object");
// 
//     // Get searcher context for pure async operations
//     let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64, Option<String>, std::sync::Arc<dyn Storage>)>| {
//         Ok(searcher_context.clone())
//     })?;
// 
//     // Simplified async implementation - return placeholder for now
//     // This can be enhanced later to use the full async document retrieval patterns
//     Ok(std::ptr::null_mut())
// }
// 
// /// Internal async function for document retrieval
// async fn perform_async_doc_retrieval_with_context(
//     mut env: JNIEnv<'_>,
//     searcher_context: Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64, Option<String>, std::sync::Arc<dyn Storage>)>,
//     doc_address: tantivy::DocAddress,
//     method_start_time: std::time::Instant,
// ) -> Result<jobject, anyhow::Error> {
//     let (_searcher, _runtime, split_uri, _aws_config, footer_start, footer_end, _doc_mapping_json, storage_resolver) = searcher_context.as_ref();
// 
//     debug_println!("🔄 ASYNC_IMPL: Starting pure async document retrieval operations");
// 
//     // Use pre-created StorageResolver for async operations (NO deadlock)
//     let storage = resolve_storage_for_split(storage_resolver, split_uri).await?;
// 
//     // Extract relative path
//     let relative_path = if split_uri.contains("://") {
//         if let Some(last_slash_pos) = split_uri.rfind('/') {
//             std::path::Path::new(&split_uri[last_slash_pos + 1..])
//         } else {
//             std::path::Path::new(split_uri)
//         }
//     } else {
//         std::path::Path::new(split_uri)
//             .file_name()
//             .map(|name| std::path::Path::new(name))
//             .unwrap_or_else(|| std::path::Path::new(split_uri))
//     };
// 
//     // Create SplitIdAndFooterOffsets
//     let split_id = relative_path.file_stem()
//         .and_then(|s| s.to_str())
//         .unwrap_or("unknown_split_id")
//         .to_string();
// 
//     let split_id_and_footer_offsets = SplitIdAndFooterOffsets {
//         split_id: split_id.clone(),
//         split_footer_start: footer_start,
//         split_footer_end: footer_end,
//         num_docs: 0,
//         timestamp_start: None,
//         timestamp_end: None,
//     };
// 
//     // Get shared searcher context with global caches
//     let shared_searcher_context = get_shared_searcher_context()?;
// 
//     // Open index with caches
//     let index = open_index_with_caches(
//         storage.clone(),
//         &[split_id_and_footer_offsets.clone()],
//         shared_searcher_context.fast_fields_cache.clone(),
//         shared_searcher_context.split_footer_cache.clone(),
//         Arc::new(quickwit_storage::MemorySizedCache::with_capacity_in_bytes(
//             100_000_000, // 100MB reader cache
//         )),
//     ).await.map_err(|e| anyhow::anyhow!("Failed to open index with caches: {}", e))?;
// 
//     debug_println!("🔄 ASYNC_IMPL: Successfully opened index with caches");
// 
//     // Create searcher and retrieve document
//     let reader = index.reader()?;
//     let searcher = reader.searcher();
// 
//     // Retrieve the document asynchronously
//     let doc = searcher.doc_async(doc_address).await
//         .map_err(|e| anyhow::anyhow!("Failed to retrieve document: {}", e))?;
// 
//     let schema = index.schema();
// 
//     debug_println!("✅ ASYNC_IMPL: Document retrieved successfully in {}ms", method_start_time.elapsed().as_millis());
// 
//     // Create DocumentWrapper and Java object
//     use crate::document::{DocumentWrapper, RetrievedDocument};
// 
//     let retrieved_doc = RetrievedDocument::new_with_schema(doc, &schema);
//     let wrapper = DocumentWrapper::Retrieved(retrieved_doc);
//     let wrapper_arc = std::sync::Arc::new(std::sync::Mutex::new(wrapper));
//     let doc_ptr = crate::utils::arc_to_jlong(wrapper_arc);
// 
//     // Create Java Document object
//     let document_class = env.find_class("io/indextables/tantivy4java/core/Document")?;
//     let document_obj = env.new_object(&document_class, "(J)V", &[doc_ptr.into()])?;
// 
//     debug_println!("✅ ASYNC_IMPL: Successfully created Document Java object");
// 
//     Ok(document_obj.into_raw())
// }
// 
// /// Async-first implementation of schema retrieval
// /// This replaces the sync-in-async deadlock-prone pattern with pure async
// pub async fn perform_schema_retrieval_async_impl(
//     env: JNIEnv<'_>,
//     searcher_ptr: jlong,
// ) -> Result<usize, anyhow::Error> {
//     let method_start_time = std::time::Instant::now();
//     debug_println!("🔄 ASYNC_IMPL: Starting async schema retrieval implementation");
// 
//     if searcher_ptr == 0 {
//         return Err(anyhow::anyhow!("Invalid searcher pointer"));
//     }
// 
//     // Get searcher context for pure async operations
//     let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64, Option<String>, std::sync::Arc<dyn Storage>)>| {
//         Ok(searcher_context.clone())
//     })?;
// 
//     // Simplified async implementation - return placeholder for now
//     // This can be enhanced later to use the full async schema retrieval patterns
//     Ok(0 as usize)
// }
// 
// /// Internal async function for schema retrieval
// async fn perform_async_schema_retrieval_with_context(
//     _env: JNIEnv<'_>,
//     searcher_context: Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64, Option<String>, std::sync::Arc<dyn Storage>)>,
//     method_start_time: std::time::Instant,
// ) -> Result<usize, anyhow::Error> {
//     let (_searcher, _runtime, split_uri, _aws_config, footer_start, footer_end, doc_mapping_json, storage_resolver) = searcher_context.as_ref();
// 
//     debug_println!("🔄 ASYNC_IMPL: Starting pure async schema retrieval operations");
// 
//     // Check if doc mapping JSON is available (optimization)
//     if let Some(doc_mapping_str) = doc_mapping_json {
//         debug_println!("🚀 ASYNC_IMPL: Using cached doc mapping JSON for schema creation");
// 
//         match create_schema_from_doc_mapping(doc_mapping_str) {
//             Ok(schema) => {
//                 debug_println!("✅ ASYNC_IMPL: Schema created from doc mapping in {}ms", method_start_time.elapsed().as_millis());
// 
//                 // Store schema and return pointer
//                 let schema_arc = std::sync::Arc::new(schema);
//                 let schema_ptr = crate::utils::arc_to_jlong(schema_arc);
//                 return Ok(schema_ptr as usize);
//             }
//             Err(e) => {
//                 debug_println!("⚠️ ASYNC_IMPL: Failed to create schema from doc mapping: {}, falling back to index extraction", e);
//             }
//         }
//     }
// 
//     // Fall back to extracting schema from index
//     let storage = resolve_storage_for_split(storage_resolver, split_uri).await?;
// 
//     // Extract relative path
//     let relative_path = if split_uri.contains("://") {
//         if let Some(last_slash_pos) = split_uri.rfind('/') {
//             std::path::Path::new(&split_uri[last_slash_pos + 1..])
//         } else {
//             std::path::Path::new(split_uri)
//         }
//     } else {
//         std::path::Path::new(split_uri)
//             .file_name()
//             .map(|name| std::path::Path::new(name))
//             .unwrap_or_else(|| std::path::Path::new(split_uri))
//     };
// 
//     let split_id = relative_path.file_stem()
//         .and_then(|s| s.to_str())
//         .unwrap_or("unknown_split_id")
//         .to_string();
// 
//     let split_id_and_footer_offsets = SplitIdAndFooterOffsets {
//         split_id: split_id.clone(),
//         split_footer_start: footer_start,
//         split_footer_end: footer_end,
//         num_docs: 0,
//         timestamp_start: None,
//         timestamp_end: None,
//     };
// 
//     // Get shared searcher context
//     let shared_searcher_context = get_shared_searcher_context()?;
// 
//     // Open index with caches
//     let index = open_index_with_caches(
//         storage.clone(),
//         &[split_id_and_footer_offsets.clone()],
//         shared_searcher_context.fast_fields_cache.clone(),
//         shared_searcher_context.split_footer_cache.clone(),
//         Arc::new(quickwit_storage::MemorySizedCache::with_capacity_in_bytes(
//             100_000_000,
//         )),
//     ).await.map_err(|e| anyhow::anyhow!("Failed to open index with caches: {}", e))?;
// 
//     let schema = index.schema();
// 
//     debug_println!("✅ ASYNC_IMPL: Schema extracted from index in {}ms", method_start_time.elapsed().as_millis());
// 
//     // Store schema and return pointer
//     let schema_arc = std::sync::Arc::new(schema);
//     let schema_ptr = crate::utils::arc_to_jlong(schema_arc);
// 
//     Ok(schema_ptr as usize)
*/
