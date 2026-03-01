// cache_extension.rs - Core file extension caching logic
// Extracted from mod.rs during refactoring

use std::collections::HashMap;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;

use quickwit_storage::Storage;

use crate::debug_println;

/// Cache all files with a given extension from the bundle into L2 disk cache.
///
/// Reads through `storage` which should be a prewarm-mode `StorageWithPersistentCache`
/// when L2 disk cache is configured. The storage layer handles:
/// - L2 cache check (returns cached data without hitting object storage)
/// - L3 fetch on miss (downloads from S3/Azure)
/// - L2 write with blocking `put()` (guaranteed write, never dropped)
/// - Prewarm metric recording via `record_prewarm_download()`
///
/// When no disk cache is configured (storage is raw), reads go directly to object storage
/// and data is discarded (no caching). This is a no-op in practice since without a cache
/// there's nothing to prewarm into.
pub(crate) async fn cache_files_by_extension(
    extension: &str,
    storage: Arc<dyn Storage>,
    split_uri: &str,
    bundle_offsets: &HashMap<PathBuf, Range<u64>>,
) -> (usize, usize) {
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

    debug_println!(
        "🔥 PREWARM_DISK: Found {} .{} files to cache via prewarm-mode storage",
        files.len(),
        extension
    );

    let mut warm_up_futures = Vec::new();

    for (inner_path, bundle_range) in files {
        let bundle_start = bundle_range.start as usize;
        let bundle_end = bundle_range.end as usize;
        let file_length = bundle_end - bundle_start;

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
    let success_count = results.iter().filter(|r| r.is_ok()).count();
    let failure_count = results.len() - success_count;

    debug_println!(
        "🔥 PREWARM_CACHE: .{} files - {} success, {} failed",
        extension,
        success_count,
        failure_count
    );

    (success_count, failure_count)
}
