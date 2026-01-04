// searcher_cache.rs - LRU searcher cache management
// Manages Tantivy searcher instances with statistics tracking

use std::sync::{Arc, OnceLock, Mutex};
use std::sync::atomic::AtomicU64;
use std::num::NonZeroUsize;
use lru::LruCache;
use quickwit_common::thread_pool::ThreadPool;
use quickwit_search::SearcherContext;
use crate::debug_println;

// Default cache size: 1000 searchers (reasonably large for production)
pub const DEFAULT_SEARCHER_CACHE_SIZE: usize = 1000;

// Cache statistics for monitoring
lazy_static::lazy_static! {
    pub static ref SEARCHER_CACHE_HITS: AtomicU64 = AtomicU64::new(0);
    pub static ref SEARCHER_CACHE_MISSES: AtomicU64 = AtomicU64::new(0);
    pub static ref SEARCHER_CACHE_EVICTIONS: AtomicU64 = AtomicU64::new(0);
}

static SEARCHER_CACHE: OnceLock<Mutex<LruCache<String, Arc<tantivy::Searcher>>>> = OnceLock::new();

/// Get the global searcher cache
pub fn get_searcher_cache() -> &'static Mutex<LruCache<String, Arc<tantivy::Searcher>>> {
    SEARCHER_CACHE.get_or_init(|| {
        let capacity = NonZeroUsize::new(DEFAULT_SEARCHER_CACHE_SIZE).unwrap();
        Mutex::new(LruCache::new(capacity))
    })
}

/// Clear the entire searcher cache
/// This releases all Arc<tantivy::Searcher> references, which in turn releases
/// Storage/L2DiskCache references, allowing proper cleanup of disk cache files.
pub fn clear_searcher_cache() {
    if let Some(cache) = SEARCHER_CACHE.get() {
        if let Ok(mut guard) = cache.lock() {
            let count = guard.len();
            guard.clear();
            debug_println!("RUST DEBUG: 🧹 Cleared searcher cache ({} entries removed)", count);
        }
    }
}

/// Thread pool for search operations (matches Quickwit's pattern exactly)
pub fn search_thread_pool() -> &'static ThreadPool {
    static SEARCH_THREAD_POOL: OnceLock<ThreadPool> = OnceLock::new();
    SEARCH_THREAD_POOL.get_or_init(|| ThreadPool::new("search", None))
}

/// Check if footer metadata is available for optimizations
pub fn has_footer_metadata(footer_start: u64, footer_end: u64) -> bool {
    footer_start > 0 && footer_end > 0 && footer_end > footer_start
}

/// Check if split URI is remote (S3/cloud) vs local file
/// Quickwit's hotcache optimization is designed for remote splits, not local files
pub fn is_remote_split(split_uri: &str) -> bool {
    split_uri.starts_with("s3://") ||
    split_uri.starts_with("azure://") ||
    split_uri.starts_with("http://") ||
    split_uri.starts_with("https://")
}

/// Extract split ID from URI (filename without extension)
pub fn extract_split_id_from_uri(split_uri: &str) -> String {
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

/// Get Arc<SearcherContext> using the global cache system
/// CRITICAL FIX: Use shared caches by returning the Arc directly
pub fn get_shared_searcher_context() -> anyhow::Result<Arc<SearcherContext>> {
    debug_println!("RUST DEBUG: Getting SHARED SearcherContext with global caches");
    use crate::global_cache::get_global_searcher_context;

    // Use the convenience function that returns Arc<SearcherContext> with shared caches
    Ok(get_global_searcher_context())
}

/// 🚀 BATCH OPTIMIZATION FIX: Parse bundle metadata from split file footer
/// Returns the file offsets map: inner_file_path -> Range<u64> in split file
/// This allows prefetch to translate inner byte ranges to split file byte ranges
///
/// Uses the SearcherContext's split_footer_cache to avoid redundant S3 requests.
/// This is the same cache that open_index_with_caches uses.
pub async fn parse_bundle_file_offsets(
    storage: &Arc<dyn quickwit_storage::Storage>,
    split_uri: &str,
    footer_start: u64,
    footer_end: u64,
    searcher_context: &SearcherContext,
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
