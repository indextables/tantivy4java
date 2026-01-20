// all_fields.rs - All-fields prewarm implementations
// Extracted from mod.rs during refactoring

use std::sync::Arc;

use jni::sys::jlong;

use crate::debug_println;
use crate::global_cache::get_global_disk_cache;
use crate::split_searcher::CachedSearcherContext;
use crate::utils::with_arc_safe;

use super::cache_extension::cache_files_by_extension;
use super::helpers::parse_split_uri;

/// Async implementation of term dictionary prewarming
///
/// This uses DISK-ONLY caching to prevent memory exhaustion:
/// 1. Read entire .term files from the bundle into L2 disk cache
/// 2. Cache with component path and byte range (0..file_length)
/// 3. L2DiskCache serves sub-range queries via mmap for efficiency
///
/// **IMPORTANT**: If no disk cache is configured, prewarm is a NO-OP.
/// Configure TieredCacheConfig with a disk cache path to enable prewarm.
pub async fn prewarm_term_dictionaries_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_TERM: Starting term dictionary warmup (disk-only)");

    // Get storage, bundle file offsets, and split_uri from the context
    let (storage, bundle_offsets, split_uri) =
        with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
            (
                ctx.cached_storage.clone(),
                ctx.bundle_file_offsets.clone(),
                ctx.split_uri.clone(),
            )
        })
        .ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Get L2 disk cache - if None, prewarm will be skipped to prevent OOM
    let disk_cache = get_global_disk_cache();

    // Cache all .term files (disk-only, no L1)
    let (success_count, failure_count) =
        cache_files_by_extension("term", storage, &split_uri, &bundle_offsets, disk_cache).await;

    debug_println!(
        "✅ PREWARM_TERM: Term dictionary warmup complete - {} success, {} failures",
        success_count,
        failure_count
    );

    if success_count > 0 || failure_count == 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!(
            "All term dictionary warmup operations failed"
        ))
    }
}

/// Async implementation of postings prewarming
///
/// This uses DISK-ONLY caching to prevent memory exhaustion:
/// 1. Read entire .idx files (term-to-posting list index) into L2 disk cache
/// 2. Read entire .pos files (positions data) into L2 disk cache
/// 3. Cache with component path and byte range (0..file_length)
/// 4. L2DiskCache serves sub-range queries via mmap for efficiency
///
/// **IMPORTANT**: If no disk cache is configured, prewarm is a NO-OP.
/// Configure TieredCacheConfig with a disk cache path to enable prewarm.
pub async fn prewarm_postings_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_POSTINGS: Starting postings warmup (disk-only)");

    // Get storage, bundle file offsets, and split_uri from the context
    let (storage, bundle_offsets, split_uri) =
        with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
            (
                ctx.cached_storage.clone(),
                ctx.bundle_file_offsets.clone(),
                ctx.split_uri.clone(),
            )
        })
        .ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Get L2 disk cache - if None, prewarm will be skipped to prevent OOM
    let disk_cache = get_global_disk_cache();

    // Cache all .idx files (term-to-posting list index) - disk-only
    let (idx_success, idx_failure) = cache_files_by_extension(
        "idx",
        storage.clone(),
        &split_uri,
        &bundle_offsets,
        disk_cache.clone(),
    )
    .await;

    // Cache all .pos files (positions data) - disk-only
    let (pos_success, pos_failure) =
        cache_files_by_extension("pos", storage, &split_uri, &bundle_offsets, disk_cache).await;

    let total_success = idx_success + pos_success;
    let total_failure = idx_failure + pos_failure;

    debug_println!(
        "✅ PREWARM_POSTINGS: Completed - {} success ({} idx, {} pos), {} failures",
        total_success,
        idx_success,
        pos_success,
        total_failure
    );

    if total_success > 0 || total_failure == 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!("All postings warmup operations failed"))
    }
}

/// Async implementation of field norms prewarming
///
/// This uses DISK-ONLY caching to prevent memory exhaustion:
/// 1. Read entire .fieldnorm files into L2 disk cache
/// 2. Cache with component path and byte range (0..file_length)
/// 3. L2DiskCache serves sub-range queries via mmap for efficiency
///
/// **IMPORTANT**: If no disk cache is configured, prewarm is a NO-OP.
/// Configure TieredCacheConfig with a disk cache path to enable prewarm.
pub async fn prewarm_fieldnorms_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_FIELDNORMS: Starting fieldnorms warmup (disk-only)");

    // Get storage, bundle file offsets, and split_uri from the context
    let (storage, bundle_offsets, split_uri) =
        with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
            (
                ctx.cached_storage.clone(),
                ctx.bundle_file_offsets.clone(),
                ctx.split_uri.clone(),
            )
        })
        .ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Get L2 disk cache - if None, prewarm will be skipped to prevent OOM
    let disk_cache = get_global_disk_cache();

    // Cache all .fieldnorm files - disk-only
    let (success_count, failure_count) = cache_files_by_extension(
        "fieldnorm",
        storage,
        &split_uri,
        &bundle_offsets,
        disk_cache,
    )
    .await;

    debug_println!(
        "✅ PREWARM_FIELDNORMS: Completed - {} success, {} failures",
        success_count,
        failure_count
    );

    // Fieldnorms are optional, so don't fail if none found
    Ok(())
}

/// Async implementation of fast fields prewarming
///
/// This uses DISK-ONLY caching to prevent memory exhaustion:
/// 1. Read entire .fast files into L2 disk cache
/// 2. Cache with component path and byte range (0..file_length)
/// 3. L2DiskCache serves sub-range queries via mmap for efficiency
///
/// **IMPORTANT**: If no disk cache is configured, prewarm is a NO-OP.
/// Configure TieredCacheConfig with a disk cache path to enable prewarm.
pub async fn prewarm_fastfields_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_FASTFIELDS: Starting fast fields warmup (disk-only)");

    // Get storage, bundle file offsets, and split_uri from the context
    let (storage, bundle_offsets, split_uri) =
        with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
            (
                ctx.cached_storage.clone(),
                ctx.bundle_file_offsets.clone(),
                ctx.split_uri.clone(),
            )
        })
        .ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Get L2 disk cache - if None, prewarm will be skipped to prevent OOM
    let disk_cache = get_global_disk_cache();

    // Cache all .fast files - disk-only
    let (success_count, failure_count) =
        cache_files_by_extension("fast", storage, &split_uri, &bundle_offsets, disk_cache).await;

    debug_println!(
        "✅ PREWARM_FASTFIELDS: Completed - {} success, {} failures",
        success_count,
        failure_count
    );

    if success_count > 0 || failure_count == 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!("All fast field warmup operations failed"))
    }
}

/// Async implementation of store (document storage) prewarming
///
/// This uses DISK-ONLY caching as the primary prewarm strategy:
/// 1. Read entire .store files from the bundle into L2 disk cache
/// 2. Cache with component path and byte range (0..file_length)
/// 3. L2DiskCache serves sub-range queries via mmap for efficiency
///
/// # Memory-Safe Prewarm Design
///
/// The L1 ByteRangeCache is now bounded (256MB default, configurable) with auto-eviction.
/// However, for large prewarm operations, we still prefer writing directly to L2DiskCache:
/// 1. Populate persistent disk cache for fast document retrieval across JVM restarts
/// 2. Avoid L1 churn during bulk prewarm operations
/// 3. Allow subsequent document fetches to find data in L2 and populate L1 on-demand
pub async fn prewarm_store_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_STORE: Starting document store warmup (disk-only caching)");

    // MEMORY SAFETY: Only use L2 disk cache for prewarm to prevent OOM during bulk operations
    // If no disk cache is configured, prewarm is a NO-OP
    let disk_cache = match get_global_disk_cache() {
        Some(dc) => dc,
        None => {
            debug_println!(
                "⚠️ PREWARM_STORE_SKIP: No disk cache configured - skipping .store prewarm to prevent OOM"
            );
            debug_println!("   Configure TieredCacheConfig.withDiskCachePath() to enable prewarm");
            return Ok(());
        }
    };

    // Get storage, bundle file offsets, and split_uri from the context (no L1 byte_range_cache needed)
    let (storage, bundle_offsets, split_uri) =
        with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
            (
                ctx.cached_storage.clone(),
                ctx.bundle_file_offsets.clone(),
                ctx.split_uri.clone(),
            )
        })
        .ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Parse split_uri into storage_loc and split_id for L2 disk cache
    let (storage_loc, split_id) = parse_split_uri(&split_uri);

    // Extract split filename for storage operations
    let split_filename = if let Some(last_slash_pos) = split_uri.rfind('/') {
        &split_uri[last_slash_pos + 1..]
    } else {
        &split_uri
    };
    let split_path = std::path::PathBuf::from(split_filename);

    debug_println!(
        "🔥 PREWARM_STORE: Split path: {:?} (disk-only=true)",
        split_path
    );

    // Find all .store files in the bundle
    let store_files: Vec<_> = bundle_offsets
        .iter()
        .filter(|(path, _)| path.extension().map(|ext| ext == "store").unwrap_or(false))
        .collect();

    if store_files.is_empty() {
        debug_println!("⚠️ PREWARM_STORE: No .store files found in bundle");
        return Ok(());
    }

    debug_println!(
        "🔥 PREWARM_STORE: Found {} .store files to warm",
        store_files.len()
    );

    let mut warm_up_futures = Vec::new();
    let mut already_cached_count = 0;

    for (inner_path, bundle_range) in store_files {
        // Bundle range is the absolute byte range within the split file
        let bundle_start = bundle_range.start as usize;
        let bundle_end = bundle_range.end as usize;
        let file_length = bundle_end - bundle_start;

        // CRITICAL: Use bundle filename as component (not inner path) to match how
        // StorageWithPersistentCache stores data during queries. Both prewarm and
        // queries must use the same cache key format: (storage_loc, split_id, bundle_filename, bundle_range)
        let component = split_filename.to_string();
        let cache_range = bundle_range.start..bundle_range.end;

        // Check if data is already cached in L2 disk cache - skip download if so
        // IMPORTANT: Use exists() instead of get().is_some() to avoid expensive file I/O!
        // get() copies the entire file contents just to check existence, while
        // exists() only checks the manifest (O(1) vs O(file_size))
        let already_cached = disk_cache.exists(
            &storage_loc,
            &split_id,
            &component,
            Some(cache_range.clone()),
        );

        if already_cached {
            debug_println!(
                "⏭️ PREWARM_STORE: Skipping '{}' ({} bytes) - already cached",
                inner_path.display(),
                file_length
            );
            already_cached_count += 1;
            continue;
        }

        let storage = storage.clone();
        let split_path = split_path.clone();
        let inner_path = inner_path.clone();

        debug_println!(
            "🔥 PREWARM_STORE: Queuing warmup for '{}' ({} bytes from split at {}..{})",
            inner_path.display(),
            file_length,
            bundle_start,
            bundle_end
        );

        warm_up_futures.push(async move {
            // Read from the split file at the bundle byte range
            // StorageWithPersistentCache.get_slice() handles:
            // 1. Checking disk cache (cache key = bundle filename + bundle range)
            // 2. Fetching from S3 on miss
            // 3. Writing to disk cache on miss
            // No direct disk_cache.put() needed - avoid double writes
            match storage.get_slice(&split_path, bundle_start..bundle_end).await {
                Ok(bytes) => {
                    debug_println!(
                        "✅ PREWARM_STORE: Cached '{}' via StorageWithPersistentCache ({} bytes)",
                        inner_path.display(),
                        bytes.len()
                    );
                    Ok(())
                }
                Err(e) => {
                    debug_println!(
                        "⚠️ PREWARM_STORE: Failed to read from split for '{}': {}",
                        inner_path.display(),
                        e
                    );
                    Err(anyhow::anyhow!("Failed to cache store file: {}", e))
                }
            }
        });
    }

    debug_println!(
        "🔥 PREWARM_STORE: Executing {} warmup operations in parallel",
        warm_up_futures.len()
    );

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
            "🔄 PREWARM_STORE_FLUSH: Flushing disk cache to ensure {} downloads are persisted",
            downloaded_count
        );
        disk_cache.flush_async().await;
        debug_println!("✅ PREWARM_STORE_FLUSH: Disk cache flush complete");
    }

    debug_println!(
        "✅ PREWARM_STORE: Completed - {} already cached, {} downloaded, {} failures",
        already_cached_count,
        downloaded_count,
        failure_count
    );

    if success_count > 0 || failure_count == 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!("All store warmup operations failed"))
    }
}
