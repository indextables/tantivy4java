// cached_reader.rs - AsyncFileReader implementation backed by Storage
//
// Bridges the parquet crate's AsyncFileReader trait to Quickwit's Storage trait,
// allowing parquet reads to go through the existing L1/L2/L3 cache stack.
//
// Includes a shared byte-range cache so that dictionary pages and data pages
// fetched for one doc retrieval are reused by subsequent retrievals without
// additional S3/Azure round-trips.

use std::ops::Range;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::future::{BoxFuture, FutureExt};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::page_index::offset_index::{OffsetIndexMetaData, PageLocation};

use quickwit_storage::Storage;

use crate::debug_println;
use crate::perf_println;

/// Cache key: (file_path, start_byte, end_byte)
type ByteRangeCacheKey = (std::path::PathBuf, u64, u64);

/// Max entries in the LRU byte-range cache. Each entry is typically a dictionary
/// page (~800KB-1MB) or data page. 256 entries ≈ 256MB worst case.
const MAX_BYTE_CACHE_ENTRIES: usize = 256;

/// Configuration for byte-range coalescing during parquet reads.
///
/// Controls how nearby byte ranges are merged into single fetch requests
/// to reduce storage round-trips. Tuning matters especially for cloud
/// storage (S3/Azure) where each GET request adds 50-100ms of latency.
#[derive(Debug, Clone, Copy)]
pub struct CoalesceConfig {
    /// Maximum gap (in bytes) between ranges that will be merged into a
    /// single fetch. Ranges separated by more than this are fetched
    /// independently.
    ///
    /// For cloud storage (S3/Azure): 512KB is a good default — each GET has
    /// ~50-100ms latency, so downloading 512KB of gap bytes to save a
    /// round-trip is almost always worthwhile.
    ///
    /// For local storage: 64KB is sufficient since read latency is <0.1ms.
    pub max_gap: u64,
    /// Maximum total size (in bytes) of a single coalesced fetch request.
    /// Prevents over-fetching when many small ranges span a very large
    /// byte region. Once a coalesced group would exceed this size, a new
    /// group is started.
    pub max_total: u64,
}

impl Default for CoalesceConfig {
    fn default() -> Self {
        Self {
            max_gap: 512 * 1024,       // 512 KB — good for cloud storage
            max_total: 8 * 1024 * 1024, // 8 MB — prevents over-fetching
        }
    }
}

impl CoalesceConfig {
    /// Config tuned for local/NVMe storage (smaller gap, lower latency).
    pub fn local() -> Self {
        Self {
            max_gap: 64 * 1024,         // 64 KB
            max_total: 8 * 1024 * 1024, // 8 MB
        }
    }
}

/// Shared byte-range cache across multiple CachedParquetReader instances.
/// Caches fetched byte ranges (e.g. dictionary pages, data pages) to avoid
/// redundant S3/Azure downloads when retrieving multiple docs from the same file.
/// Uses LRU eviction to bound memory — least-recently-used entries are evicted
/// when the cache exceeds MAX_BYTE_CACHE_ENTRIES.
pub type ByteRangeCache = Arc<Mutex<lru::LruCache<ByteRangeCacheKey, Bytes>>>;

/// Create a new empty byte-range cache.
pub fn new_byte_range_cache() -> ByteRangeCache {
    Arc::new(Mutex::new(lru::LruCache::new(
        std::num::NonZeroUsize::new(MAX_BYTE_CACHE_ENTRIES).unwrap(),
    )))
}

/// An AsyncFileReader that delegates to Quickwit's Storage trait.
/// This ensures all parquet reads go through the L2 disk cache.
pub struct CachedParquetReader {
    storage: Arc<dyn Storage>,
    path: std::path::PathBuf,
    file_size: u64,
    metadata: Option<Arc<ParquetMetaData>>,
    /// Optional shared byte-range cache for dictionary/data page reuse
    byte_cache: Option<ByteRangeCache>,
    /// Coalescing parameters for byte-range merging
    coalesce_config: CoalesceConfig,
    /// Manifest-sourced page locations to inject when metadata lacks an offset index.
    /// Outer vec = row groups, inner vec = columns per row group, innermost = page locations.
    manifest_page_locations: Option<Vec<Vec<Vec<super::manifest::PageLocationEntry>>>>,
}

impl CachedParquetReader {
    /// Create a new reader for a parquet file at the given path
    pub fn new(
        storage: Arc<dyn Storage>,
        path: impl Into<std::path::PathBuf>,
        file_size: u64,
    ) -> Self {
        Self {
            storage,
            path: path.into(),
            file_size,
            metadata: None,
            byte_cache: None,
            coalesce_config: CoalesceConfig::default(),
            manifest_page_locations: None,
        }
    }

    /// Create a reader with pre-loaded metadata
    pub fn with_metadata(
        storage: Arc<dyn Storage>,
        path: impl Into<std::path::PathBuf>,
        file_size: u64,
        metadata: Arc<ParquetMetaData>,
    ) -> Self {
        Self {
            storage,
            path: path.into(),
            file_size,
            metadata: Some(metadata),
            byte_cache: None,
            coalesce_config: CoalesceConfig::default(),
            manifest_page_locations: None,
        }
    }

    /// Attach a shared byte-range cache for dictionary/data page reuse.
    /// When set, fetched byte ranges are cached and served from cache on
    /// subsequent reads of the same range, avoiding redundant S3/Azure calls.
    pub fn with_byte_cache(mut self, cache: ByteRangeCache) -> Self {
        self.byte_cache = Some(cache);
        self
    }

    /// Set coalescing parameters for byte-range merging.
    pub fn with_coalesce_config(mut self, config: CoalesceConfig) -> Self {
        self.coalesce_config = config;
        self
    }

    /// Attach manifest-sourced page locations for read-time injection.
    /// If the loaded metadata lacks an offset index, these will be
    /// injected to enable page-level byte range reads.
    pub fn with_manifest_page_locations(
        mut self,
        locations: Vec<Vec<Vec<super::manifest::PageLocationEntry>>>,
    ) -> Self {
        self.manifest_page_locations = Some(locations);
        self
    }

    /// Helper: convert Range<u64> to Range<usize> for Storage trait
    fn to_usize_range(range: &Range<u64>) -> Range<usize> {
        range.start as usize..range.end as usize
    }

    /// Try to get bytes from the cache
    fn cache_get(&self, range: &Range<u64>) -> Option<Bytes> {
        let cache = self.byte_cache.as_ref()?;
        let key = (self.path.clone(), range.start, range.end);
        cache.lock().ok()?.get(&key).cloned()
    }

    /// Store bytes in the cache
    fn cache_put(&self, range: &Range<u64>, bytes: &Bytes) {
        if let Some(ref cache) = self.byte_cache {
            let key = (self.path.clone(), range.start, range.end);
            if let Ok(mut guard) = cache.lock() {
                guard.put(key, bytes.clone());
            }
        }
    }
}

/// A group of byte ranges that have been coalesced into a single fetch
struct CoalescedGroup {
    /// The merged byte range to fetch
    fetch_range: Range<u64>,
    /// Original (index, range) pairs contained within this fetch
    members: Vec<(usize, Range<u64>)>,
}

impl AsyncFileReader for CachedParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        let storage = self.storage.clone();
        let path = self.path.clone();
        let byte_size = range.end - range.start;
        perf_println!(
            "⏱️ PROJ_DIAG: get_bytes path={:?} range={}..{} ({} bytes)",
            path.file_name().unwrap_or_default(), range.start, range.end, byte_size
        );

        // Check cache first
        if let Some(cached) = self.cache_get(&range) {
            perf_println!("⏱️ PROJ_DIAG: get_bytes CACHE HIT {} bytes", cached.len());
            return async move { Ok(cached) }.boxed();
        }

        perf_println!("⏱️ PROJ_DIAG: get_bytes CACHE MISS — fetching {} bytes from storage", byte_size);

        let byte_cache = self.byte_cache.clone();
        let path_for_cache = self.path.clone();

        async move {
            let t = std::time::Instant::now();
            let usize_range = Self::to_usize_range(&range);
            let owned_bytes = storage
                .get_slice(&path, usize_range)
                .await
                .map_err(|e| {
                    parquet::errors::ParquetError::External(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("Storage read failed for {:?} range {:?}: {}", path, range, e),
                    )))
                })?;
            perf_println!(
                "⏱️ PROJ_DIAG: get_bytes storage.get_slice took {}ms for {} bytes",
                t.elapsed().as_millis(), byte_size
            );

            // Zero-copy conversion: OwnedBytes → Bytes via from_owner()
            // avoids the allocation+copy of .to_vec()
            let bytes = Bytes::from_owner(owned_bytes);

            // Cache the result
            if let Some(cache) = byte_cache {
                let key = (path_for_cache, range.start, range.end);
                if let Ok(mut guard) = cache.lock() {
                    guard.put(key, bytes.clone());
                }
            }

            Ok(bytes)
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, parquet::errors::Result<Vec<Bytes>>> {
        let storage = self.storage.clone();
        let path = self.path.clone();
        let total_requested: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        perf_println!(
            "⏱️ PROJ_DIAG: get_byte_ranges path={:?} count={} ranges, total_requested={} bytes",
            path.file_name().unwrap_or_default(), ranges.len(), total_requested
        );

        // Partition ranges into cached and uncached
        let mut results: Vec<Option<Bytes>> = vec![None; ranges.len()];
        let mut uncached_indices: Vec<usize> = Vec::new();
        let mut uncached_ranges: Vec<Range<u64>> = Vec::new();

        for (i, range) in ranges.iter().enumerate() {
            if let Some(cached) = self.cache_get(range) {
                results[i] = Some(cached);
            } else {
                uncached_indices.push(i);
                uncached_ranges.push(range.clone());
            }
        }

        let cache_hits = ranges.len() - uncached_ranges.len();
        let uncached_bytes: u64 = uncached_ranges.iter().map(|r| r.end - r.start).sum();
        perf_println!(
            "⏱️ PROJ_DIAG: get_byte_ranges: {} cache hits, {} misses ({} bytes to fetch from storage)",
            cache_hits, uncached_ranges.len(), uncached_bytes
        );

        // If all cached, return immediately
        if uncached_ranges.is_empty() {
            perf_println!("⏱️ PROJ_DIAG: get_byte_ranges ALL CACHED — returning immediately");
            return async move {
                Ok(results.into_iter().map(|opt| opt.unwrap()).collect())
            }
            .boxed();
        }

        let byte_cache = self.byte_cache.clone();
        let path_for_cache = self.path.clone();
        let coalesce_config = self.coalesce_config;

        async move {
            let t = std::time::Instant::now();
            // Fetch only the uncached ranges with configured coalescing
            let fetched = fetch_ranges_with_coalescing(
                &storage,
                &path,
                uncached_ranges.clone(),
                coalesce_config,
            )
            .await?;
            perf_println!(
                "⏱️ PROJ_DIAG: get_byte_ranges fetch_ranges_with_coalescing took {}ms for {} uncached bytes",
                t.elapsed().as_millis(), uncached_bytes
            );

            // Cache and fill results
            for (fetch_idx, orig_idx) in uncached_indices.into_iter().enumerate() {
                let bytes = fetched[fetch_idx].clone();

                // Cache the fetched bytes
                if let Some(ref cache) = byte_cache {
                    let key = (
                        path_for_cache.clone(),
                        uncached_ranges[fetch_idx].start,
                        uncached_ranges[fetch_idx].end,
                    );
                    if let Ok(mut guard) = cache.lock() {
                        guard.put(key, bytes.clone());
                    }
                }

                results[orig_idx] = Some(bytes);
            }

            Ok(results
                .into_iter()
                .map(|opt| opt.unwrap_or_else(|| Bytes::new()))
                .collect())
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        if let Some(ref metadata) = self.metadata {
            perf_println!("⏱️ PROJ_DIAG: get_metadata CACHED — returning immediately");
            let metadata = metadata.clone();
            return async move { Ok(metadata) }.boxed();
        }

        let file_size = self.file_size;
        perf_println!(
            "⏱️ PROJ_DIAG: get_metadata NOT CACHED — loading footer+offset_index for file_size={}",
            file_size
        );

        async move {
            let t = std::time::Instant::now();
            use parquet::file::metadata::PageIndexPolicy;
            let metadata = parquet::file::metadata::ParquetMetaDataReader::new()
                .with_offset_index_policy(PageIndexPolicy::Optional)
                .with_prefetch_hint(Some(64 * 1024)) // prefetch 64KB footer
                .load_and_finish(&mut *self, file_size)
                .await?;

            // Inject manifest-sourced page locations if the metadata lacks an offset index
            let metadata = if metadata.offset_index().is_none() {
                if let Some(ref manifest_locs) = self.manifest_page_locations {
                    let offset_index: Vec<Vec<OffsetIndexMetaData>> = manifest_locs
                        .iter()
                        .map(|rg_cols| {
                            rg_cols
                                .iter()
                                .map(|col_pages| {
                                    let page_locations: Vec<PageLocation> = col_pages
                                        .iter()
                                        .map(|pl| PageLocation {
                                            offset: pl.offset,
                                            compressed_page_size: pl.compressed_page_size,
                                            first_row_index: pl.first_row_index,
                                        })
                                        .collect();
                                    OffsetIndexMetaData {
                                        page_locations,
                                        unencoded_byte_array_data_bytes: None,
                                    }
                                })
                                .collect()
                        })
                        .collect();
                    perf_println!(
                        "⏱️ PROJ_DIAG: injecting manifest offset_index: {} row_groups",
                        offset_index.len()
                    );
                    metadata
                        .into_builder()
                        .set_offset_index(Some(offset_index))
                        .build()
                } else {
                    metadata
                }
            } else {
                metadata
            };

            if *crate::debug::PERFLOG_ENABLED {
                let has_offset_index = metadata.offset_index().is_some();
                let num_row_groups = metadata.num_row_groups();
                let num_columns = if num_row_groups > 0 {
                    metadata.row_group(0).num_columns()
                } else {
                    0
                };
                perf_println!(
                    "⏱️ PROJ_DIAG: get_metadata loaded in {}ms — {} row_groups, {} columns, has_offset_index={}",
                    t.elapsed().as_millis(), num_row_groups, num_columns, has_offset_index
                );
            }

            let metadata = Arc::new(metadata);
            self.metadata = Some(metadata.clone());
            Ok(metadata)
        }
        .boxed()
    }
}

/// Fetch multiple byte ranges from storage with coalescing of nearby ranges.
/// This reduces the number of S3/Azure round-trips by merging ranges that are
/// separated by at most `config.max_gap` bytes into single fetches, capped at
/// `config.max_total` bytes per coalesced group.
async fn fetch_ranges_with_coalescing(
    storage: &Arc<dyn Storage>,
    path: &std::path::Path,
    ranges: Vec<Range<u64>>,
    config: CoalesceConfig,
) -> Result<Vec<Bytes>, parquet::errors::ParquetError> {
    if ranges.len() <= 1 {
        let futs: Vec<_> = ranges
            .into_iter()
            .map(|range| {
                let storage = storage.clone();
                let path = path.to_path_buf();
                async move {
                    let usize_range = range.start as usize..range.end as usize;
                    let owned_bytes = storage
                        .get_slice(&path, usize_range)
                        .await
                        .map_err(|e| {
                            parquet::errors::ParquetError::External(Box::new(
                                std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    format!(
                                        "Storage read failed for {:?} range {:?}: {}",
                                        path, range, e
                                    ),
                                ),
                            ))
                        })?;
                    // Zero-copy conversion: OwnedBytes → Bytes via from_owner()
                    Ok::<Bytes, parquet::errors::ParquetError>(Bytes::from_owner(
                        owned_bytes,
                    ))
                }
            })
            .collect();
        return futures::future::try_join_all(futs).await;
    }

    // Build sorted index of ranges for coalescing
    let mut indexed_ranges: Vec<(usize, Range<u64>)> = ranges
        .iter()
        .enumerate()
        .map(|(i, r)| (i, r.clone()))
        .collect();
    indexed_ranges.sort_by_key(|(_, r)| r.start);

    // Coalesce nearby ranges into groups, respecting both max_gap and max_total
    let mut groups: Vec<CoalescedGroup> = Vec::new();
    let mut group_start = indexed_ranges[0].1.start;
    let mut group_end = indexed_ranges[0].1.end;
    let mut group_members = vec![indexed_ranges[0].clone()];

    for &(idx, ref range) in &indexed_ranges[1..] {
        let new_end = group_end.max(range.end);
        let would_exceed_gap = range.start > group_end + config.max_gap;
        let would_exceed_total = (new_end - group_start) > config.max_total;

        if would_exceed_gap || would_exceed_total {
            // Start a new group
            groups.push(CoalescedGroup {
                fetch_range: group_start..group_end,
                members: std::mem::take(&mut group_members),
            });
            group_start = range.start;
            group_end = range.end;
            group_members = vec![(idx, range.clone())];
        } else {
            group_end = new_end;
            group_members.push((idx, range.clone()));
        }
    }
    groups.push(CoalescedGroup {
        fetch_range: group_start..group_end,
        members: group_members,
    });

    if *crate::debug::PERFLOG_ENABLED {
        let total_useful: u64 = ranges.iter().map(|r| r.end - r.start).sum();
        let total_fetched: u64 = groups.iter().map(|g| g.fetch_range.end - g.fetch_range.start).sum();
        let waste_pct = if total_fetched > 0 {
            ((total_fetched - total_useful) as f64 / total_fetched as f64 * 100.0) as u32
        } else { 0 };
        perf_println!(
            "⏱️ PROJ_DIAG: coalesced {} ranges into {} groups for {:?} (gap={}KB, max={}MB) — useful={}B, fetched={}B, waste={}%",
            ranges.len(), groups.len(),
            path.file_name().unwrap_or_default(),
            config.max_gap / 1024, config.max_total / (1024 * 1024),
            total_useful, total_fetched, waste_pct
        );
        for (i, group) in groups.iter().enumerate() {
            let group_size = group.fetch_range.end - group.fetch_range.start;
            let member_bytes: u64 = group.members.iter().map(|(_, r)| r.end - r.start).sum();
            perf_println!(
                "⏱️ PROJ_DIAG:   group[{}]: fetch={}..{} ({}B), {} members ({}B useful, {}B gap)",
                i, group.fetch_range.start, group.fetch_range.end, group_size,
                group.members.len(), member_bytes, group_size - member_bytes
            );
        }
    }

    // Fetch coalesced ranges in parallel
    let group_futs: Vec<_> = groups
        .into_iter()
        .map(|group| {
            let storage = storage.clone();
            let path = path.to_path_buf();
            async move {
                let usize_range =
                    group.fetch_range.start as usize..group.fetch_range.end as usize;
                let coalesced_bytes = storage
                    .get_slice(&path, usize_range)
                    .await
                    .map_err(|e| {
                        parquet::errors::ParquetError::External(Box::new(
                            std::io::Error::new(
                                std::io::ErrorKind::Other,
                                format!(
                                    "Storage read failed for {:?} range {:?}: {}",
                                    path, group.fetch_range, e
                                ),
                            ),
                        ))
                    })?;

                // Zero-copy conversion: OwnedBytes → Bytes via from_owner()
                let coalesced_shared = Bytes::from_owner(coalesced_bytes);
                let results: Vec<(usize, Bytes)> = group
                    .members
                    .iter()
                    .map(|(original_idx, range)| {
                        let local_start =
                            (range.start - group.fetch_range.start) as usize;
                        let local_end =
                            (range.end - group.fetch_range.start) as usize;
                        // Zero-copy slice — shares the same backing buffer
                        (*original_idx, coalesced_shared.slice(local_start..local_end))
                    })
                    .collect();

                Ok::<Vec<(usize, Bytes)>, parquet::errors::ParquetError>(results)
            }
        })
        .collect();

    let group_results = futures::future::try_join_all(group_futs).await?;

    // Reassemble results in original order
    let mut ordered: Vec<Option<Bytes>> = vec![None; ranges.len()];
    for results in group_results {
        for (idx, bytes) in results {
            ordered[idx] = Some(bytes);
        }
    }

    Ok(ordered
        .into_iter()
        .map(|opt| opt.unwrap_or_else(|| Bytes::new()))
        .collect())
}
