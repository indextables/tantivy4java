// all_fields.rs - All-fields prewarm implementations
// Extracted from mod.rs during refactoring

use std::sync::Arc;

use jni::sys::jlong;

use crate::debug_println;
use crate::split_searcher::CachedSearcherContext;
use crate::utils::with_arc_safe;

use super::cache_extension::cache_files_by_extension;

/// Async implementation of term dictionary prewarming
///
/// This uses DISK-ONLY caching to prevent memory exhaustion:
/// 1. Read entire .term files from the bundle via prewarm-mode storage
/// 2. StorageWithPersistentCache handles L2 cache check/write and prewarm metrics
/// 3. L2DiskCache serves sub-range queries via mmap for efficiency
///
/// **IMPORTANT**: If no disk cache is configured, reads go to object storage
/// but data is not persisted. Configure TieredCacheConfig with a disk cache path.
pub async fn prewarm_term_dictionaries_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_TERM: Starting term dictionary warmup (disk-only)");

    // Use prewarm_storage if available (prewarm-mode StorageWithPersistentCache),
    // fall back to cached_storage (query-mode)
    let (storage, bundle_offsets, split_uri) =
        with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
            (
                ctx.prewarm_storage.clone().unwrap_or_else(|| ctx.cached_storage.clone()),
                ctx.bundle_file_offsets.clone(),
                ctx.split_uri.clone(),
            )
        })
        .ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Cache all .term files via prewarm-mode storage
    let (success_count, failure_count) =
        cache_files_by_extension("term", storage, &split_uri, &bundle_offsets).await;

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
/// 1. Read entire .idx files (term-to-posting list index) via prewarm-mode storage
/// 2. Read entire .pos files (positions data) via prewarm-mode storage
/// 3. StorageWithPersistentCache handles L2 cache check/write and prewarm metrics
/// 4. L2DiskCache serves sub-range queries via mmap for efficiency
pub async fn prewarm_postings_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_POSTINGS: Starting postings warmup (disk-only)");

    let (storage, bundle_offsets, split_uri) =
        with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
            (
                ctx.prewarm_storage.clone().unwrap_or_else(|| ctx.cached_storage.clone()),
                ctx.bundle_file_offsets.clone(),
                ctx.split_uri.clone(),
            )
        })
        .ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Cache all .idx files (term-to-posting list index)
    let (idx_success, idx_failure) = cache_files_by_extension(
        "idx",
        storage.clone(),
        &split_uri,
        &bundle_offsets,
    )
    .await;

    // Cache all .pos files (positions data)
    let (pos_success, pos_failure) =
        cache_files_by_extension("pos", storage, &split_uri, &bundle_offsets).await;

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
/// 1. Read entire .fieldnorm files via prewarm-mode storage
/// 2. StorageWithPersistentCache handles L2 cache check/write and prewarm metrics
/// 3. L2DiskCache serves sub-range queries via mmap for efficiency
pub async fn prewarm_fieldnorms_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_FIELDNORMS: Starting fieldnorms warmup (disk-only)");

    let (storage, bundle_offsets, split_uri) =
        with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
            (
                ctx.prewarm_storage.clone().unwrap_or_else(|| ctx.cached_storage.clone()),
                ctx.bundle_file_offsets.clone(),
                ctx.split_uri.clone(),
            )
        })
        .ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Cache all .fieldnorm files
    let (success_count, failure_count) = cache_files_by_extension(
        "fieldnorm",
        storage,
        &split_uri,
        &bundle_offsets,
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
/// 1. Read entire .fast files via prewarm-mode storage
/// 2. StorageWithPersistentCache handles L2 cache check/write and prewarm metrics
/// 3. L2DiskCache serves sub-range queries via mmap for efficiency
pub async fn prewarm_fastfields_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_FASTFIELDS: Starting fast fields warmup (disk-only)");

    let (storage, bundle_offsets, split_uri) =
        with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
            (
                ctx.prewarm_storage.clone().unwrap_or_else(|| ctx.cached_storage.clone()),
                ctx.bundle_file_offsets.clone(),
                ctx.split_uri.clone(),
            )
        })
        .ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Cache all .fast files
    let (success_count, failure_count) =
        cache_files_by_extension("fast", storage, &split_uri, &bundle_offsets).await;

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
/// This uses DISK-ONLY caching via prewarm-mode StorageWithPersistentCache:
/// 1. Read entire .store files from the bundle via prewarm-mode storage
/// 2. StorageWithPersistentCache handles L2 cache check/write and prewarm metrics
/// 3. L2DiskCache serves sub-range queries via mmap for efficiency
///
/// # Memory-Safe Prewarm Design
///
/// The prewarm-mode storage writes directly to L2 disk cache, not L1 memory.
/// Subsequent document fetches find data in L2 and populate L1 on-demand.
pub async fn prewarm_store_impl(searcher_ptr: jlong) -> anyhow::Result<()> {
    debug_println!("🔥 PREWARM_STORE: Starting document store warmup (disk-only caching)");

    let (storage, bundle_offsets, split_uri) =
        with_arc_safe(searcher_ptr, |ctx: &Arc<CachedSearcherContext>| {
            (
                ctx.prewarm_storage.clone().unwrap_or_else(|| ctx.cached_storage.clone()),
                ctx.bundle_file_offsets.clone(),
                ctx.split_uri.clone(),
            )
        })
        .ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?;

    // Cache all .store files via prewarm-mode storage
    let (success_count, failure_count) =
        cache_files_by_extension("store", storage, &split_uri, &bundle_offsets).await;

    debug_println!(
        "✅ PREWARM_STORE: Completed - {} success, {} failures",
        success_count,
        failure_count
    );

    if success_count > 0 || failure_count == 0 {
        Ok(())
    } else {
        Err(anyhow::anyhow!("All store warmup operations failed"))
    }
}
