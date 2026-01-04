// cache_extension.rs - Core file extension caching logic
// Extracted from mod.rs during refactoring

use std::collections::HashMap;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use quickwit_storage::Storage;

use crate::debug_println;
use crate::disk_cache::L2DiskCache;

use super::helpers::parse_split_uri;

/// Helper function to cache all files with a given extension from the bundle.
///
/// This implements DISK-ONLY caching as the primary prewarm strategy.
/// Data is written ONLY to L2DiskCache - bypassing L1 memory cache.
///
/// # Memory-Safe Prewarm Design
///
/// The L1 ByteRangeCache now uses bounded capacity with automatic eviction when full.
/// However, for large prewarm operations, we still prefer writing directly to L2DiskCache:
/// 1. Populate persistent disk cache for fast reads across JVM restarts
/// 2. Avoid L1 churn during bulk prewarm operations
/// 3. Allow subsequent searches to find data in L2 and populate L1 on-demand
///
/// Note: L1 is now bounded (default 256MB, configurable via TANTIVY4JAVA_L1_CACHE_MB)
/// and will auto-evict when full, so OOM is no longer a risk. However, direct L2
/// writes are still preferred for prewarm efficiency.
///
/// **IMPORTANT**: If no disk cache is configured, prewarm is a NO-OP.
/// Configure TieredCacheConfig with a disk cache path to enable prewarm functionality.
///
/// # Arguments
/// * `extension` - File extension to match (e.g., "term", "store", "fast")
/// * `storage` - Storage backend for reading files
/// * `split_uri` - Full URI of the split file (e.g., "s3://bucket/path/split.split")
/// * `bundle_offsets` - Map of inner paths to bundle byte ranges
/// * `disk_cache` - L2 disk cache to populate (if None, prewarm is skipped)
///
/// # Returns
/// Tuple of (success_count, failure_count)
pub(crate) async fn cache_files_by_extension(
    extension: &str,
    storage: Arc<dyn Storage>,
    split_uri: &str,
    bundle_offsets: &HashMap<PathBuf, Range<u64>>,
    disk_cache: Option<Arc<L2DiskCache>>,
) -> (usize, usize) {
    // MEMORY SAFETY: If no disk cache, skip prewarm entirely to prevent OOM
    let disk_cache = match disk_cache {
        Some(dc) => dc,
        None => {
            debug_println!(
                "⚠️ PREWARM_SKIP: No disk cache configured - skipping .{} prewarm to prevent OOM",
                extension
            );
            debug_println!("   Configure TieredCacheConfig.withDiskCachePath() to enable prewarm");
            return (0, 0);
        }
    };
    // Find all files with the given extension
    let files: Vec<_> = bundle_offsets
        .iter()
        .filter(|(path, _)| {
            path.extension()
                .map(|ext| ext == extension)
                .unwrap_or(false)
        })
        .collect();

    if files.is_empty() {
        debug_println!("⚠️ PREWARM_CACHE: No .{} files found in bundle", extension);
        return (0, 0);
    }

    // Parse split_uri into storage_loc and split_id for L2 disk cache
    let (storage_loc, split_id) = parse_split_uri(split_uri);

    // Extract just the filename for storage operations
    let split_filename = if let Some(last_slash_pos) = split_uri.rfind('/') {
        &split_uri[last_slash_pos + 1..]
    } else {
        split_uri
    };
    let split_path = PathBuf::from(split_filename);

    debug_println!(
        "🔥 PREWARM_DISK: Found {} .{} files to cache (L2 disk-only)",
        files.len(),
        extension
    );

    let mut warm_up_futures = Vec::new();
    let mut already_cached_count = 0;

    // Extract component name from split_path for cache key consistency
    // StorageWithPersistentCache uses extract_component(path) which returns the filename
    let storage_component = split_path
        .file_name()
        .and_then(|n| n.to_str())
        .map(|s| {
            if s.starts_with('.') {
                s[1..].to_string()
            } else {
                s.to_string()
            }
        })
        .unwrap_or_else(|| "unknown".to_string());

    for (inner_path, bundle_range) in files {
        let bundle_start = bundle_range.start as usize;
        let bundle_end = bundle_range.end as usize;
        let file_length = bundle_end - bundle_start;

        // CRITICAL FIX: Use the SAME cache key format as StorageWithPersistentCache!
        // StorageWithPersistentCache caches with:
        //   - component = split filename (e.g., "mysplit.split")
        //   - range = bundle_start..bundle_end (absolute offset in split file)
        // NOT with inner path and relative range, which was causing cache key mismatches!
        let cache_range = bundle_start as u64..bundle_end as u64;

        debug_println!(
            "🔑 PREWARM_CACHE_KEY: storage_loc='{}', split_id='{}', component='{}', range={:?}",
            storage_loc,
            split_id,
            storage_component,
            cache_range
        );

        // Check if data is already in L2 disk cache - skip download if so
        // Uses the same key format as StorageWithPersistentCache
        if disk_cache
            .get(
                &storage_loc,
                &split_id,
                &storage_component,
                Some(cache_range.clone()),
            )
            .is_some()
        {
            debug_println!(
                "⏭️ PREWARM_DISK: Skipping '{}' ({} bytes) - already in disk cache (range {:?})",
                inner_path.display(),
                file_length,
                cache_range
            );
            already_cached_count += 1;
            continue;
        }

        let storage = storage.clone();
        let split_path = split_path.clone();
        let inner_path = inner_path.clone();

        debug_println!(
            "🔥 PREWARM_DISK: Queuing '{}' ({} bytes) for download (range {:?})",
            inner_path.display(),
            file_length,
            cache_range
        );

        warm_up_futures.push(async move {
            match storage.get_slice(&split_path, bundle_start..bundle_end).await {
                Ok(_bytes) => {
                    // NOTE: Download recording AND caching is done by StorageWithPersistentCache::get_slice()
                    // We don't cache here because StorageWithPersistentCache already handles it with
                    // the correct cache key format (component=split_filename, range=bundle_range)
                    debug_println!(
                        "✅ PREWARM_DISK: Downloaded '{}' ({} bytes) - cached by storage layer",
                        inner_path.display(),
                        _bytes.len()
                    );
                    Ok(())
                }
                Err(e) => {
                    debug_println!(
                        "⚠️ PREWARM_DISK: Failed '{}': {}",
                        inner_path.display(),
                        e
                    );
                    Err(e)
                }
            }
        });
    }

    let results = futures::future::join_all(warm_up_futures).await;
    let downloaded_count = results.iter().filter(|r| r.is_ok()).count();
    let failure_count = results.len() - downloaded_count;

    // Success = already cached + newly downloaded
    let success_count = already_cached_count + downloaded_count;

    // CRITICAL: Flush disk cache to ensure all async writes complete before returning.
    // This prevents race conditions where subsequent cache lookups miss data that's
    // still being written by the background writer thread.
    if downloaded_count > 0 {
        debug_println!(
            "🔄 PREWARM_FLUSH: Flushing disk cache to ensure {} downloads are persisted",
            downloaded_count
        );
        disk_cache.flush_async().await;
        debug_println!("✅ PREWARM_FLUSH: Disk cache flush complete");
    }

    debug_println!(
        "🔥 PREWARM_CACHE: .{} files - {} already cached, {} downloaded, {} failed",
        extension,
        already_cached_count,
        downloaded_count,
        failure_count
    );

    (success_count, failure_count)
}
