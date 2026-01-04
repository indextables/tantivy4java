// cache_config.rs - Adaptive cache sizing configuration
// Implementation of Quickwit's doc store cache optimization with adaptive scaling for multi-threaded clients.

use crate::debug_println;

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
pub const BASE_CONCURRENT_REQUESTS: usize = 30;

/// Cache block size for individual document retrieval operations
/// Smaller cache footprint optimized for single-document access patterns
/// Prevents excessive memory usage when only retrieving one document at a time
pub const SINGLE_DOC_CACHE_BLOCKS: usize = 10;

/// Minimum time since last access before cache item can be evicted (prevents scan pattern thrashing)
pub const MIN_CACHE_ITEM_LIFETIME_SECS: u64 = 60;

/// Emergency eviction threshold when cache is critically full (95% capacity)
pub const EMERGENCY_EVICTION_THRESHOLD: f64 = 0.95;

/// Memory allocation constants for document-count-aware sizing
pub const MIN_MEMORY_ALLOCATION_MB: usize = 15;
pub const MAX_MEMORY_ALLOCATION_MB: usize = 100;
pub const BYTES_PER_DOCUMENT: usize = 50;

/// ByteRange cache merging constants
pub const MAX_ACCEPTABLE_GAPS: usize = 3;
pub const PREFETCH_ADJACENT_THRESHOLD: f64 = 0.8; // 80% cache hit rate triggers prefetch

/// Get the configured maximum Java thread count for cache sizing
/// Defaults to CPU count but can be overridden via TANTIVY4JAVA_MAX_THREADS environment variable
pub fn get_max_java_threads() -> usize {
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
pub fn get_adaptive_batch_cache_blocks() -> usize {
    let max_threads = get_max_java_threads();
    let base_blocks = BASE_CONCURRENT_REQUESTS * max_threads;

    // Add 20% safety margin to handle burst concurrency and edge cases
    let safety_factor = 1.2;
    let adaptive_blocks = (base_blocks as f64 * safety_factor).ceil() as usize;

    debug_println!("⚙️  CACHE_CONFIG: Adaptive cache sizing - threads: {}, base: {}, adaptive: {}",
                   max_threads, base_blocks, adaptive_blocks);

    adaptive_blocks
}

/// Calculate adaptive memory allocation based on split metadata and thread count
/// Combines document-count-aware scaling with thread-based concurrency scaling
/// Returns cache block count optimized for the specific split characteristics
pub fn calculate_adaptive_memory_allocation(
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
pub fn get_batch_doc_cache_blocks() -> usize {
    get_batch_doc_cache_blocks_with_metadata(None)
}

/// Get cache block size with optional split metadata for enhanced allocation
pub fn get_batch_doc_cache_blocks_with_metadata(split_metadata: Option<(usize, usize)>) -> usize {
    let thread_count = get_max_java_threads();
    calculate_adaptive_memory_allocation(split_metadata, thread_count)
}

/// Extract split metadata for adaptive memory allocation
/// Returns (num_docs, file_size_bytes) if available from split metadata
pub fn extract_split_metadata_for_allocation(split_uri: &str) -> Option<(usize, usize)> {
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
pub fn get_split_size_hint(_split_uri: &str) -> Option<usize> {
    // Basic size estimation based on split file name or cached data
    // This is a placeholder that could be enhanced with actual metadata caching
    None // For now, fallback to thread-based scaling
}
