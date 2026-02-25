// types.rs - Shared data structures for split_searcher module

use std::sync::{Arc, Mutex, RwLock};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::ops::Range;
use quickwit_storage::{Storage, ByteRangeCache};
use crate::perf_println;
use crate::debug_println;
use crate::standalone_searcher::StandaloneSearcher;
use crate::parquet_companion::manifest::ParquetManifest;

/// Simple data structure to hold search results for JNI integration
#[derive(Debug)]
pub struct SearchResultData {
    pub hits: Vec<SearchHit>,
    pub total_hits: u64,
}

/// Individual search hit data
#[derive(Debug)]
pub struct SearchHit {
    pub score: f32,
    pub segment_ord: u32,
    pub doc_id: u32,
}

/// Enhanced SearchResult data structure that includes both hits and aggregations
#[derive(Debug)]
pub struct EnhancedSearchResult {
    pub hits: Vec<(f32, tantivy::DocAddress)>,
    pub aggregation_results: Option<Vec<u8>>, // Postcard-serialized aggregation results
    pub aggregation_json: Option<String>, // Aggregation request JSON (may be rewritten for hash fields)
    /// Names of aggregations whose field was redirected to a `_phash_*` fast field (Phase 2).
    /// Used in Phase 3 touchup to identify which terms buckets need hash → string resolution.
    pub redirected_hash_agg_names: Option<std::collections::HashSet<String>>,
    /// Hash value → original string mapping built during Phase 3 touchup.
    /// Stored here so `create_terms_result_object` can replace U64 bucket keys with strings.
    pub hash_resolution_map: Option<std::collections::HashMap<u64, String>>,
    /// Per-aggregation include/exclude string filters saved from Phase 2 rewriting.
    /// Keyed by aggregation name. Used in result creation to post-filter buckets since
    /// Tantivy ignores numeric include arrays on U64 fields.
    pub hash_agg_touchup_infos: Option<Vec<crate::parquet_companion::hash_field_rewriter::HashFieldTouchupInfo>>,
}

/// Cached searcher context for efficient single document retrieval
/// Contains pre-resolved storage, index, and searcher to avoid repeated resolution
pub(crate) struct CachedSearcherContext {
    pub(crate) standalone_searcher: Arc<StandaloneSearcher>,
    // ✅ CRITICAL FIX: Removed runtime field - using shared global runtime instead
    pub(crate) split_uri: String,
    pub(crate) aws_config: HashMap<String, String>,
    pub(crate) footer_start: u64,
    pub(crate) footer_end: u64,
    pub(crate) doc_mapping_json: Option<String>,
    pub(crate) cached_storage: Arc<dyn Storage>,
    pub(crate) cached_index: Arc<tantivy::Index>,
    pub(crate) cached_searcher: Arc<tantivy::Searcher>,
    // 🚀 BATCH OPTIMIZATION FIX: Store ByteRangeCache and bundle file offsets
    // This allows prefetch to populate the same cache that doc_async uses
    pub(crate) byte_range_cache: Option<ByteRangeCache>,
    pub(crate) bundle_file_offsets: HashMap<PathBuf, Range<u64>>,
    // Parquet companion mode: optional manifest for parquet-backed document retrieval
    pub(crate) parquet_manifest: Option<Arc<ParquetManifest>>,
    // Parquet companion mode: effective table_root for resolving parquet file paths (provided at read time)
    pub(crate) parquet_table_root: Option<String>,
    // Parquet companion mode: optional storage for accessing parquet files (used in Phase 2+)
    #[allow(dead_code)]
    pub(crate) parquet_storage: Option<Arc<dyn Storage>>,
    // Phase 2: Optional augmented directory for fast field transcoding from parquet
    pub(crate) augmented_directory: Option<Arc<crate::parquet_companion::augmented_directory::ParquetAugmentedDirectory>>,
    // Parquet companion mode: split overrides (meta.json + fast field data)
    // Used by aggregation/search path to shadow the split's files via UnionDirectory.
    // The fast_field_data is populated lazily — only the columns needed by each query
    // are transcoded on demand, not all columns upfront.
    pub(crate) split_overrides: Option<std::sync::Arc<quickwit_search::SplitOverrides>>,
    // Parquet companion mode: meta.json override bytes (all fields promoted to fast).
    // Constant once set at searcher creation. Used to build fresh SplitOverrides per query.
    pub(crate) parquet_meta_json: Option<Vec<u8>>,
    // Parquet companion mode: tracks which fast field columns have been transcoded so far.
    // Grows monotonically as new queries touch new columns. Protected by Mutex for
    // thread-safe updates when concurrent queries need new columns.
    pub(crate) transcoded_fast_columns: Arc<Mutex<HashSet<String>>>,
    // Parquet companion mode: segment .fast file paths discovered at creation time.
    // Used by lazy transcoding to know which segments need updated .fast files.
    pub(crate) segment_fast_paths: Vec<PathBuf>,
    // Parquet companion mode: cached parquet file metadata (footer) per file path.
    // Avoids re-reading the footer from S3/Azure for every single doc retrieval.
    pub(crate) parquet_metadata_cache: Arc<Mutex<HashMap<PathBuf, Arc<parquet::file::metadata::ParquetMetaData>>>>,
    // Parquet companion mode: shared byte-range cache for dictionary/data page reuse.
    // Dictionary pages are large (800KB-1MB) and must be fetched for every doc retrieval.
    // This cache ensures they're fetched from S3/Azure only once and reused across calls.
    pub(crate) parquet_byte_range_cache: crate::parquet_companion::cached_reader::ByteRangeCache,
    // Parquet companion mode: optional coalesce configuration for byte-range merging.
    // Controls max_gap between ranges that get merged into single fetch requests.
    // None = use default (512KB gap). Smaller values reduce over-fetch for projected reads.
    pub(crate) parquet_coalesce_config: Option<crate::parquet_companion::cached_reader::CoalesceConfig>,
    // Parquet companion mode: pre-built lookup from file path hash → file index.
    // Built once from manifest.parquet_files, used for O(1) fast-field-based doc resolution.
    pub(crate) parquet_file_hash_index: HashMap<u64, usize>,
    // Parquet companion mode: true if __pq_file_hash and __pq_row_in_file fast fields exist.
    // When true, doc retrieval uses merge-safe fast-field resolution.
    // When false, falls back to legacy segment-based positional resolution.
    pub(crate) has_merge_safe_tracking: bool,
    // Parquet companion mode: lazily-loaded __pq_file_hash and __pq_row_in_file Column handles.
    // Indexed as pq_columns[segment_ord] = Some((file_hash_col, row_in_file_col))
    // Column<u64> supports O(1) random access via values_for_doc(doc_id) — no need to
    // materialize all values into a Vec. Populated on first doc retrieval (not during init)
    // via async storage reads, because the HotDirectory hotcache doesn't include these columns.
    pub(crate) pq_columns: Arc<RwLock<Vec<Option<(tantivy::columnar::Column<u64>, tantivy::columnar::Column<u64>)>>>>,
}

impl CachedSearcherContext {
    /// Clear the L1 ByteRangeCache to free memory.
    ///
    /// This should be called after prewarm operations to prevent unbounded memory growth.
    /// The prewarmed data is safely persisted in L2 disk cache, so clearing L1 is safe.
    /// Subsequent queries will populate L1 on-demand from L2.
    pub fn clear_l1_cache(&self) {
        if let Some(cache) = &self.byte_range_cache {
            cache.clear();
        }
    }

    /// Ensure __pq Column<u64> handles are loaded for the given segment.
    /// Fetches only the SSTable index + 2 column byte ranges via async reads,
    /// then opens Column<u64> handles for O(1) random access by doc_id.
    ///
    /// This is a no-op if the segment columns are already cached.
    pub(crate) async fn ensure_pq_segment_loaded(&self, seg_ord: u32) -> anyhow::Result<()> {
        let t0 = std::time::Instant::now();
        // Quick check under read lock — return immediately if already loaded
        {
            let cache = self.pq_columns.read().unwrap();
            let cache_len = cache.len();
            if cache.get(seg_ord as usize).and_then(|o| o.as_ref()).is_some() {
                perf_println!("⏱️ PROJ_DIAG: ensure_pq_segment_loaded seg={} CACHE HIT (cache has {} segments, self={:p}) took {}ms",
                    seg_ord, cache_len, self, t0.elapsed().as_millis());
                return Ok(());
            }
            perf_println!("⏱️ PROJ_DIAG: ensure_pq_segment_loaded seg={} CACHE MISS (cache has {} segments, self={:p}) — loading via searcher fast fields",
                seg_ord, cache_len, self);
        }
        // Lock released — do column reads without holding it

        // Use the existing cached_searcher → SegmentReader → FastFieldReaders path.
        // This goes through HotDirectory/StorageDirectory which does byte-range level reads,
        // fetching only the SSTable index + the 2 __pq column byte ranges — NOT the entire .fast file.
        //
        // Must use async path: HotDirectory delegates to StorageDirectory for bytes not in
        // the hotcache, and StorageDirectory only supports async reads.
        // Pattern: list_dynamic_column_handles (async) → read_bytes_async → open_u64_lenient (sync, now cached)
        let t_cols = std::time::Instant::now();
        let segment_reader = self.cached_searcher.segment_reader(seg_ord);
        let fast_fields = segment_reader.fast_fields();

        let file_hash_field = crate::parquet_companion::indexing::PARQUET_FILE_HASH_FIELD;
        let row_field = crate::parquet_companion::indexing::PARQUET_ROW_IN_FILE_FIELD;

        // Step 1: Get column handles asynchronously (reads SSTable index only)
        let file_hash_handles = fast_fields.list_dynamic_column_handles(file_hash_field).await
            .map_err(|e| anyhow::anyhow!("Failed to list {} column handles: {}", file_hash_field, e))?;
        let row_handles = fast_fields.list_dynamic_column_handles(row_field).await
            .map_err(|e| anyhow::anyhow!("Failed to list {} column handles: {}", row_field, e))?;

        let file_hash_handle = file_hash_handles.into_iter().next()
            .ok_or_else(|| anyhow::anyhow!("No column handle found for {}", file_hash_field))?;
        let row_handle = row_handles.into_iter().next()
            .ok_or_else(|| anyhow::anyhow!("No column handle found for {}", row_field))?;

        // Step 2: Read column bytes asynchronously (fetches only these columns' byte ranges)
        let (file_hash_bytes, row_bytes) = tokio::try_join!(
            file_hash_handle.file_slice().read_bytes_async(),
            row_handle.file_slice().read_bytes_async(),
        ).map_err(|e| anyhow::anyhow!("Failed to read __pq column bytes: {}", e))?;

        perf_println!("⏱️ PROJ_DIAG: ensure_pq_segment_loaded seg={} async column reads in {}ms ({}B + {}B)",
            seg_ord, t_cols.elapsed().as_millis(), file_hash_bytes.len(), row_bytes.len());

        // Step 3: Open Column<u64> handles (sync — bytes already in memory from step 2)
        // These are lightweight handles backed by Arc<OwnedBytes>, supporting O(1) random access.
        let file_hash_col: tantivy::columnar::Column<u64> = file_hash_handle.open_u64_lenient()
            .map_err(|e| anyhow::anyhow!("Failed to open {} column: {}", file_hash_field, e))?
            .ok_or_else(|| anyhow::anyhow!("{} column is not u64-compatible", file_hash_field))?;
        let row_col: tantivy::columnar::Column<u64> = row_handle.open_u64_lenient()
            .map_err(|e| anyhow::anyhow!("Failed to open {} column: {}", row_field, e))?
            .ok_or_else(|| anyhow::anyhow!("{} column is not u64-compatible", row_field))?;

        let num_docs = segment_reader.num_docs() + segment_reader.num_deleted_docs();
        crate::debug_println!(
            "📦 PARQUET_COMPANION: Cached __pq column handles for segment {} ({} docs, O(1) random access)",
            seg_ord, num_docs
        );

        // Store column handles under write lock (double-check to avoid overwriting a concurrent load)
        let mut cache = self.pq_columns.write().unwrap();
        while cache.len() <= seg_ord as usize {
            cache.push(None);
        }
        if cache[seg_ord as usize].is_none() {
            cache[seg_ord as usize] = Some((file_hash_col, row_col));
        }

        perf_println!("⏱️ PROJ_DIAG: ensure_pq_segment_loaded seg={} TOTAL {}ms (self={:p})",
            seg_ord, t0.elapsed().as_millis(), self);
        Ok(())
    }

    /// Warm all native fast field columns into L1 CachingDirectory cache.
    /// The component-level FASTFIELD prewarm writes .fast files to L2 disk cache,
    /// but the CachingDirectory's L1 ByteRangeCache needs individual column byte ranges
    /// to be read through the async path. Without this, the first aggregation that touches
    /// a native fast field column (e.g. _phash_*) triggers a storage download.
    pub(crate) async fn warm_native_fast_fields_l1(&self) -> anyhow::Result<()> {
        let schema = self.cached_searcher.index().schema();
        let field_names: Vec<String> = schema.fields()
            .map(|(_, entry)| entry.name().to_string())
            .collect();
        self.warm_native_fast_fields_l1_for_fields(&field_names).await
    }

    /// Warm specific native fast field columns into L1 CachingDirectory cache.
    /// Only reads byte ranges for the given field names (and silently skips any
    /// that don't have fast field data in the segment).
    pub(crate) async fn warm_native_fast_fields_l1_for_fields(&self, field_names: &[String]) -> anyhow::Result<()> {
        let num_segments = self.cached_searcher.segment_readers().len();
        let mut total_bytes = 0usize;
        let mut total_columns = 0usize;

        for seg_ord in 0..num_segments {
            let segment_reader = self.cached_searcher.segment_reader(seg_ord as u32);
            let fast_fields = segment_reader.fast_fields();

            for field_name in field_names {
                let handles = match fast_fields.list_dynamic_column_handles(field_name).await {
                    Ok(h) => h,
                    Err(_) => continue, // field may not have fast field data
                };
                for handle in handles {
                    let file_slice = handle.file_slice();
                    match file_slice.read_bytes_async().await {
                        Ok(bytes) => {
                            total_bytes += bytes.len();
                            total_columns += 1;
                        }
                        Err(_) => {} // skip on error
                    }
                }
            }
        }

        debug_println!("✅ PREWARM: Warmed {} native fast field columns ({} bytes) into L1 across {} segments",
            total_columns, total_bytes, num_segments);
        Ok(())
    }

    /// Look up a __pq location via cached Column<u64> handles. O(1) random access.
    /// Must call ensure_pq_segment_loaded first.
    pub(crate) fn get_pq_location(&self, seg_ord: u32, doc_id: u32) -> anyhow::Result<(u64, u64)> {
        let cache = self.pq_columns.read().unwrap();
        let (fh_col, row_col) = cache.get(seg_ord as usize)
            .and_then(|opt| opt.as_ref())
            .ok_or_else(|| anyhow::anyhow!(
                "No __pq columns for seg={} (segments={})",
                seg_ord, cache.len()
            ))?;
        let file_hash = fh_col.values_for_doc(doc_id).next().unwrap_or(0);
        let row_in_file = row_col.values_for_doc(doc_id).next().unwrap_or(0);
        Ok((file_hash, row_in_file))
    }
}
