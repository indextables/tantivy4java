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

use quickwit_storage::Storage;

use crate::debug_println;

/// Cache key: (file_path, start_byte, end_byte)
type ByteRangeCacheKey = (std::path::PathBuf, u64, u64);

/// Max entries in the LRU byte-range cache. Each entry is typically a dictionary
/// page (~800KB-1MB) or data page. 256 entries ≈ 256MB worst case.
const MAX_BYTE_CACHE_ENTRIES: usize = 256;

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
        }
    }

    /// Attach a shared byte-range cache for dictionary/data page reuse.
    /// When set, fetched byte ranges are cached and served from cache on
    /// subsequent reads of the same range, avoiding redundant S3/Azure calls.
    pub fn with_byte_cache(mut self, cache: ByteRangeCache) -> Self {
        self.byte_cache = Some(cache);
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
        debug_println!(
            "📖 PARQUET_READ: get_bytes path={:?} range={}..{} ({} bytes)",
            path, range.start, range.end, range.end - range.start
        );

        // Check cache first
        if let Some(cached) = self.cache_get(&range) {
            debug_println!("📖 PARQUET_READ: cache HIT for {}..{}", range.start, range.end);
            return async move { Ok(cached) }.boxed();
        }

        let byte_cache = self.byte_cache.clone();
        let path_for_cache = self.path.clone();

        async move {
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

            let bytes = Bytes::from(owned_bytes.to_vec());

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
        debug_println!(
            "📖 PARQUET_READ: get_byte_ranges path={:?} count={} ranges",
            path, ranges.len()
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
        if cache_hits > 0 {
            debug_println!(
                "📖 PARQUET_READ: byte_range cache: {} hits, {} misses",
                cache_hits, uncached_ranges.len()
            );
        }

        // If all cached, return immediately
        if uncached_ranges.is_empty() {
            return async move {
                Ok(results.into_iter().map(|opt| opt.unwrap()).collect())
            }
            .boxed();
        }

        let byte_cache = self.byte_cache.clone();
        let path_for_cache = self.path.clone();

        async move {
            // Fetch only the uncached ranges
            let fetched = fetch_ranges_with_coalescing(
                &storage,
                &path,
                uncached_ranges.clone(),
            )
            .await?;

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
            let metadata = metadata.clone();
            return async move { Ok(metadata) }.boxed();
        }

        let file_size = self.file_size;

        async move {
            // Use the parquet crate's async metadata loader which:
            // 1. Reads the footer via get_bytes() on this reader
            // 2. Reads the offset index (page locations) via get_bytes()
            //
            // The offset index is CRITICAL for page-level byte-range reads.
            // Without it, the reader downloads entire column chunks (~30MB for 1M rows).
            // With it, the reader downloads only the specific pages containing
            // the target rows (~few KB per page).
            use parquet::file::metadata::PageIndexPolicy;
            let metadata = parquet::file::metadata::ParquetMetaDataReader::new()
                .with_offset_index_policy(PageIndexPolicy::Optional)
                .with_prefetch_hint(Some(64 * 1024)) // prefetch 64KB footer
                .load_and_finish(self, file_size)
                .await?;

            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

/// Fetch multiple byte ranges from storage with coalescing of nearby ranges.
/// This reduces the number of S3/Azure round-trips by merging ranges that are
/// separated by at most MAX_COALESCE_GAP bytes into single fetches.
async fn fetch_ranges_with_coalescing(
    storage: &Arc<dyn Storage>,
    path: &std::path::Path,
    ranges: Vec<Range<u64>>,
) -> Result<Vec<Bytes>, parquet::errors::ParquetError> {
    const MAX_COALESCE_GAP: u64 = 64 * 1024; // 64 KB

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
                    Ok::<Bytes, parquet::errors::ParquetError>(Bytes::from(
                        owned_bytes.to_vec(),
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

    // Coalesce nearby ranges into groups
    let mut groups: Vec<CoalescedGroup> = Vec::new();
    let mut group_start = indexed_ranges[0].1.start;
    let mut group_end = indexed_ranges[0].1.end;
    let mut group_members = vec![indexed_ranges[0].clone()];

    for &(idx, ref range) in &indexed_ranges[1..] {
        if range.start <= group_end + MAX_COALESCE_GAP {
            group_end = group_end.max(range.end);
            group_members.push((idx, range.clone()));
        } else {
            groups.push(CoalescedGroup {
                fetch_range: group_start..group_end,
                members: std::mem::take(&mut group_members),
            });
            group_start = range.start;
            group_end = range.end;
            group_members = vec![(idx, range.clone())];
        }
    }
    groups.push(CoalescedGroup {
        fetch_range: group_start..group_end,
        members: group_members,
    });

    debug_println!(
        "📖 PARQUET_READ: coalesced {} ranges into {} fetches for {:?}",
        ranges.len(),
        groups.len(),
        path
    );

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

                let coalesced_shared = Bytes::from(coalesced_bytes.to_vec());
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
