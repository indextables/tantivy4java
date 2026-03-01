// cache_extension.rs - Core file extension caching logic
// Extracted from mod.rs during refactoring

use std::collections::HashMap;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use quickwit_storage::Storage;

use crate::debug_println;
use crate::global_cache::get_global_disk_cache;

use super::helpers::parse_split_uri;

/// Cache all files with a given extension from the bundle into L2 disk cache.
///
/// Reads through `storage` which should be a prewarm-mode `StorageWithPersistentCache`
/// when L2 disk cache is configured. The storage layer handles:
/// - L2 cache check (returns cached data without hitting object storage)
/// - L3 fetch on miss (downloads from S3/Azure)
/// - L2 write with blocking `put()` (guaranteed write, never dropped)
/// - Prewarm metric recording via `record_prewarm_download()`
///
/// **IMPORTANT**: If no disk cache is configured, prewarm is a NO-OP.
/// Configure TieredCacheConfig with a disk cache path to enable prewarm functionality.
///
/// Uses O(1) manifest `exists()` checks to skip already-cached files, avoiding
/// unnecessary disk I/O from reading cached data only to discard it.
pub(crate) async fn cache_files_by_extension(
    extension: &str,
    storage: Arc<dyn Storage>,
    split_uri: &str,
    bundle_offsets: &HashMap<PathBuf, Range<u64>>,
) -> (usize, usize) {
    // If no disk cache is configured, prewarm is a NO-OP — avoid downloading
    // data from S3/Azure just to discard it.
    let disk_cache = match get_global_disk_cache() {
        Some(dc) => Some(dc),
        None => {
            debug_println!(
                "⚠️ PREWARM_SKIP: No disk cache configured - skipping .{} prewarm",
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

    // Extract just the filename for storage operations
    let split_filename = if let Some(last_slash_pos) = split_uri.rfind('/') {
        &split_uri[last_slash_pos + 1..]
    } else {
        split_uri
    };
    let split_path = PathBuf::from(split_filename);

    // Parse split_uri into storage_loc and split_id for L2 disk cache exists() checks.
    // These MUST match the keys used by StorageWithPersistentCache to ensure
    // prewarm and query cache lookups are consistent.
    let (storage_loc, split_id) = parse_split_uri(split_uri);

    // Extract component name for cache key consistency with StorageWithPersistentCache
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

    debug_println!(
        "🔥 PREWARM_DISK: Found {} .{} files to cache via prewarm-mode storage",
        files.len(),
        extension
    );

    let mut warm_up_futures = Vec::new();
    let mut already_cached_count = 0;

    for (inner_path, bundle_range) in files {
        let bundle_start = bundle_range.start as usize;
        let bundle_end = bundle_range.end as usize;
        let file_length = bundle_end - bundle_start;

        // O(1) manifest check: skip already-cached files to avoid unnecessary disk reads.
        // exists() is a lightweight manifest lookup (~1μs) vs get_slice() which would
        // read the entire file from disk via mmap just to discard the data.
        let cache_range = bundle_start as u64..bundle_end as u64;
        if let Some(ref dc) = disk_cache {
            if dc.exists(
                &storage_loc,
                &split_id,
                &storage_component,
                Some(cache_range),
            ) {
                debug_println!(
                    "⏭️ PREWARM_DISK: Skipping '{}' ({} bytes) - already in disk cache",
                    inner_path.display(),
                    file_length,
                );
                already_cached_count += 1;
                continue;
            }
        }

        let storage = storage.clone();
        let split_path = split_path.clone();
        let inner_path = inner_path.clone();

        debug_println!(
            "🔥 PREWARM_DISK: Queuing '{}' ({} bytes) for prewarm (range {}..{})",
            inner_path.display(),
            file_length,
            bundle_start,
            bundle_end
        );

        warm_up_futures.push(async move {
            // Read through prewarm-mode StorageWithPersistentCache:
            // - Checks L2 cache first (returns immediately on hit)
            // - On miss: fetches from L3, records prewarm metrics, writes to L2 with blocking put()
            // We discard the returned data — the purpose is to populate L2.
            match storage.get_slice(&split_path, bundle_start..bundle_end).await {
                Ok(bytes) => {
                    debug_println!(
                        "✅ PREWARM_DISK: Prewarmed '{}' ({} bytes)",
                        inner_path.display(),
                        bytes.len()
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

    debug_println!(
        "🔥 PREWARM_CACHE: .{} files - {} already cached, {} downloaded, {} failed",
        extension,
        already_cached_count,
        downloaded_count,
        failure_count
    );

    (success_count, failure_count)
}
