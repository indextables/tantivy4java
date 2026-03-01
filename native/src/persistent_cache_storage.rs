// persistent_cache_storage.rs - Tiered cache layer for Storage
//
// This module provides a Storage wrapper that adds a tiered cache layer:
// - L1: Memory cache (MemorySizedCache) - fast, limited size
// - L2: Disk cache (L2DiskCache) - persistent, larger, with smart compression + COALESCING
// - L3: Remote storage (S3, Azure, file) - original data source
//
// KEY FEATURE: Range coalescing - if we have partial cache coverage, we only fetch
// the missing gaps from S3, then combine cached + fetched data.

use std::fmt;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_storage::{
    BulkDeleteError, OwnedBytes, PutPayload, SendableAsync, Storage, StorageResult
};
use tokio::io::AsyncRead;

use crate::debug_println;
use crate::disk_cache::{L2DiskCache, CoalesceResult, CachedSegment};
use crate::global_cache::{record_query_download, record_prewarm_download};

/// Statistics for L2 disk cache with coalescing
#[derive(Debug, Default)]
pub struct TieredCacheStats {
    pub l2_hits: AtomicU64,           // Full cache hits
    pub l2_partial_hits: AtomicU64,   // Coalesced partial hits
    pub l3_fetches: AtomicU64,        // Complete cache misses
    pub l3_bytes_saved: AtomicU64,    // Bytes NOT fetched due to coalescing
}

impl TieredCacheStats {
    pub fn summary(&self) -> String {
        let l2 = self.l2_hits.load(Ordering::Relaxed);
        let l2_partial = self.l2_partial_hits.load(Ordering::Relaxed);
        let l3 = self.l3_fetches.load(Ordering::Relaxed);
        let bytes_saved = self.l3_bytes_saved.load(Ordering::Relaxed);
        let total = l2 + l2_partial + l3;
        if total == 0 {
            "No cache activity".to_string()
        } else {
            format!(
                "L2: {} ({:.1}%), L2-partial: {} ({:.1}%), L3: {} ({:.1}%), saved: {} bytes",
                l2, (l2 as f64 / total as f64) * 100.0,
                l2_partial, (l2_partial as f64 / total as f64) * 100.0,
                l3, (l3 as f64 / total as f64) * 100.0,
                bytes_saved
            )
        }
    }
}

/// Storage wrapper that provides L2 disk caching with range coalescing
///
/// Architecture:
/// ```text
/// Search Request
///     ↓
/// L2: Disk Cache (L2DiskCache) - persistent, with smart compression + coalescing
///     ↓ (miss or partial)
/// L3: Remote Storage (S3/Azure/File)
/// ```
///
/// Quickwit's internal caches (ByteRangeCache, fast_fields_cache) handle memory
/// caching at a higher layer. This wrapper adds persistent L2 disk caching.
///
/// KEY FEATURE: Range coalescing - partial cache hits fetch only gaps from L3.
pub struct StorageWithPersistentCache {
    /// The underlying storage backend (S3, file system, etc.)
    pub storage: Arc<dyn Storage>,
    /// L2: Disk cache (persistent, with smart compression + coalescing)
    pub disk_cache: Arc<L2DiskCache>,
    /// Storage location for disk cache key prefix
    storage_loc: String,
    /// Split ID for disk cache key
    split_id: String,
    /// Statistics
    pub stats: Arc<TieredCacheStats>,
    /// When true, uses blocking `put()` for L2 writes (guaranteed) and records
    /// prewarm metrics. When false (default), uses `put_query_path()` which may
    /// drop writes when the queue is full if `drop_writes_when_full` is enabled.
    prewarm_mode: bool,
}

impl StorageWithPersistentCache {
    /// Create a new storage wrapper with L2 disk cache
    ///
    /// Quickwit's internal caches handle memory-level caching, so we only
    /// add persistent L2 disk caching with range coalescing.
    pub fn with_disk_cache_only(
        storage: Arc<dyn Storage>,
        disk_cache: Arc<L2DiskCache>,
        storage_loc: String,
        split_id: String,
    ) -> Self {
        debug_println!("🔄 TIERED_CACHE: Creating StorageWithPersistentCache (L2 disk cache only)");
        debug_println!("   L2 disk cache: {} bytes", disk_cache.stats().total_bytes);
        Self {
            storage,
            disk_cache,
            storage_loc,
            split_id,
            stats: Arc::new(TieredCacheStats::default()),
            prewarm_mode: false,
        }
    }

    /// Create a prewarm-mode clone of this storage wrapper.
    ///
    /// The prewarm variant shares the same underlying storage, disk cache, cache keys,
    /// and stats, but differs in two ways:
    /// - L2 writes use blocking `put()` (guaranteed write, never dropped)
    /// - Downloads are recorded via `record_prewarm_download()` instead of `record_query_download()`
    pub fn for_prewarm(self: &Arc<Self>) -> Arc<Self> {
        Arc::new(Self {
            storage: self.storage.clone(),
            disk_cache: self.disk_cache.clone(),
            storage_loc: self.storage_loc.clone(),
            split_id: self.split_id.clone(),
            stats: self.stats.clone(),
            prewarm_mode: true,
        })
    }

    /// Extract component name from path (e.g., ".term" -> "term")
    fn extract_component(path: &Path) -> String {
        path.file_name()
            .and_then(|n| n.to_str())
            .map(|s| if s.starts_with('.') { s[1..].to_string() } else { s.to_string() })
            .unwrap_or_else(|| "unknown".to_string())
    }

    /// Combine cached segments into a single contiguous buffer
    ///
    /// The segments may not cover the full requested range if there are gaps,
    /// but when called from fully_cached path, they should be contiguous.
    fn combine_segments(segments: &[CachedSegment], requested: &Range<u64>) -> OwnedBytes {
        if segments.len() == 1 {
            // Single segment - just return it
            return segments[0].data.clone();
        }

        let total_len = (requested.end - requested.start) as usize;
        let mut result = vec![0u8; total_len];

        for seg in segments {
            let offset_in_result = (seg.range.start - requested.start) as usize;
            let len = seg.data.len();
            if offset_in_result + len <= result.len() {
                result[offset_in_result..offset_in_result + len].copy_from_slice(seg.data.as_slice());
            }
        }

        OwnedBytes::new(result)
    }

    /// Record a download metric and write data to L2 disk cache.
    ///
    /// In prewarm mode: uses blocking `put()` (guaranteed write) and `record_prewarm_download()`.
    /// In query mode: uses `put_query_path()` (may drop if full) and `record_query_download()`.
    fn record_and_cache(
        &self,
        component: &str,
        byte_range: Option<Range<u64>>,
        data: &[u8],
    ) {
        if self.prewarm_mode {
            record_prewarm_download(data.len() as u64);
            self.disk_cache.put(
                &self.storage_loc, &self.split_id, component,
                byte_range, data,
            );
        } else {
            record_query_download(data.len() as u64);
            self.disk_cache.put_query_path(
                &self.storage_loc, &self.split_id, component,
                byte_range, data,
            );
        }
    }

    /// Fetch gaps from S3 and combine with cached segments
    ///
    /// This is the key coalescing operation:
    /// 1. Fetch each gap from remote storage
    /// 2. Cache each gap in L2
    /// 3. Merge cached segments + fetched gaps into final result
    async fn fetch_and_combine(
        &self,
        path: &Path,
        requested: &Range<u64>,
        coalesce_result: &CoalesceResult,
    ) -> StorageResult<OwnedBytes> {
        let component = Self::extract_component(path);
        let total_len = (requested.end - requested.start) as usize;
        let mut result = vec![0u8; total_len];

        // Copy cached segments into result buffer
        for seg in &coalesce_result.cached_segments {
            let offset_in_result = (seg.range.start - requested.start) as usize;
            let len = seg.data.len();
            if offset_in_result + len <= result.len() {
                result[offset_in_result..offset_in_result + len].copy_from_slice(seg.data.as_slice());
            }
        }

        // Fetch and insert each gap
        for gap in &coalesce_result.gaps {
            debug_println!("🌐 L3_FETCH_GAP: Fetching gap {:?} from S3", gap);

            let gap_usize = gap.start as usize..gap.end as usize;
            let gap_data = self.storage.get_slice(path, gap_usize).await?;

            self.record_and_cache(&component, Some(gap.clone()), gap_data.as_slice());

            // Insert gap data into result buffer
            let offset_in_result = (gap.start - requested.start) as usize;
            let len = gap_data.len();
            if offset_in_result + len <= result.len() {
                result[offset_in_result..offset_in_result + len].copy_from_slice(gap_data.as_slice());
            }
            debug_println!("💾 L2_STORE_GAP: Cached gap {:?} ({} bytes)", gap, len);
        }

        Ok(OwnedBytes::new(result))
    }
}

impl fmt::Debug for StorageWithPersistentCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageWithPersistentCache")
            .field("storage_loc", &self.storage_loc)
            .field("split_id", &self.split_id)
            .finish()
    }
}

#[async_trait]
impl Storage for StorageWithPersistentCache {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.storage.check_connectivity().await
    }

    async fn put(&self, path: &Path, payload: Box<dyn PutPayload>) -> StorageResult<()> {
        // Forward puts to underlying storage (we don't cache writes)
        self.storage.put(path, payload).await
    }

    async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()> {
        // Delegate to underlying storage
        self.storage.copy_to(path, output).await
    }

    async fn get_slice(&self, path: &Path, byte_range: Range<usize>) -> StorageResult<OwnedBytes> {
        let component = Self::extract_component(path);
        let requested_range = byte_range.start as u64..byte_range.end as u64;

        // DETAILED DEBUG: Show exact cache key components being used for queries
        // Compare these with PREWARM_CACHE_KEY output to find mismatches
        debug_println!("🔑 QUERY_CACHE_KEY: storage_loc='{}', split_id='{}', component='{}', range={:?}",
                 self.storage_loc, self.split_id, component, requested_range);
        debug_println!("   📁 Path file_name: {:?}", path.file_name());
        debug_println!("   📂 Full path: {:?}", path);

        // Check L2 disk cache with range COALESCING
        let coalesce_result = self.disk_cache.get_coalesced(
            &self.storage_loc,
            &self.split_id,
            &component,
            requested_range.clone()
        );

        if coalesce_result.fully_cached {
            // Complete cache hit - combine cached segments
            debug_println!("✅ L2_HIT: Fully cached ({} bytes)", coalesce_result.cached_bytes);
            self.stats.l2_hits.fetch_add(1, Ordering::Relaxed);

            return Ok(Self::combine_segments(&coalesce_result.cached_segments, &requested_range));
        } else if !coalesce_result.gaps.is_empty() && coalesce_result.cached_bytes > 0 {
            // Partial cache hit - fetch only the gaps from S3
            debug_println!("🔶 L2_PARTIAL: {} bytes cached, {} bytes to fetch ({} gaps)",
                     coalesce_result.cached_bytes, coalesce_result.gap_bytes, coalesce_result.gaps.len());
            self.stats.l2_partial_hits.fetch_add(1, Ordering::Relaxed);
            self.stats.l3_bytes_saved.fetch_add(coalesce_result.cached_bytes, Ordering::Relaxed);

            // Fetch each gap from S3 and combine with cached segments
            return self.fetch_and_combine(
                path,
                &requested_range,
                &coalesce_result,
            ).await;
        }

        // Complete miss - fetch full range from S3
        debug_println!("❌ L2_MISS: Fetching full range from storage (L3) for component='{}', range={:?}", component, requested_range);
        self.stats.l3_fetches.fetch_add(1, Ordering::Relaxed);

        let bytes = self.storage.get_slice(path, byte_range.clone()).await?;

        // Record download metrics and write to L2 cache based on mode
        let disk_range = byte_range.start as u64..byte_range.end as u64;
        debug_println!("💾 L2_STORE: Writing to disk cache - storage_loc={}, split_id={}, component={}, range={:?}, size={}, prewarm={}",
                 self.storage_loc, self.split_id, component, disk_range, bytes.len(), self.prewarm_mode);
        self.record_and_cache(&component, Some(disk_range), bytes.as_slice());

        Ok(bytes)
    }

    async fn get_slice_stream(
        &self,
        path: &Path,
        range: Range<usize>,
    ) -> StorageResult<Box<dyn AsyncRead + Send + Unpin>> {
        // For streaming, bypass cache and go directly to storage
        self.storage.get_slice_stream(path, range).await
    }

    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes> {
        debug_println!("🔍 TIERED_CACHE: get_all for {:?}", path);
        // For get_all, we could cache but the range is unknown
        // Just forward to storage for now
        self.storage.get_all(path).await
    }

    async fn delete(&self, path: &Path) -> StorageResult<()> {
        self.storage.delete(path).await
    }

    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError> {
        self.storage.bulk_delete(paths).await
    }

    async fn exists(&self, path: &Path) -> StorageResult<bool> {
        self.storage.exists(path).await
    }

    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64> {
        self.storage.file_num_bytes(path).await
    }

    fn uri(&self) -> &Uri {
        self.storage.uri()
    }
}
