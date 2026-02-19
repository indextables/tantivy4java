// types.rs - Shared data structures for split_searcher module

use std::sync::{Arc, Mutex};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::ops::Range;
use quickwit_storage::{Storage, ByteRangeCache};
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
    pub aggregation_json: Option<String>, // Original aggregation request JSON
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
    // Parquet companion mode: pre-built lookup from file path hash → file index.
    // Built once from manifest.parquet_files, used for O(1) fast-field-based doc resolution.
    pub(crate) parquet_file_hash_index: HashMap<u64, usize>,
    // Parquet companion mode: true if __pq_file_hash and __pq_row_in_file fast fields exist.
    // When true, doc retrieval uses merge-safe fast-field resolution.
    // When false, falls back to legacy segment-based positional resolution.
    pub(crate) has_merge_safe_tracking: bool,
    // Parquet companion mode: lazily-loaded __pq_file_hash and __pq_row_in_file values.
    // Indexed as pq_doc_locations[segment_ord] = Some(vec![(file_hash, row_in_file), ...])
    // Populated on first doc retrieval (not during init) via async storage reads,
    // because the HotDirectory hotcache doesn't include these columns.
    pub(crate) pq_doc_locations: Arc<Mutex<Vec<Option<Vec<(u64, u64)>>>>>,
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

    /// Ensure __pq fast field data is loaded for the given segment.
    /// Reads .fast bytes from the split bundle via async storage, strips the
    /// tantivy footer, opens a ColumnarReader, and caches the (file_hash, row_in_file)
    /// pairs for all docs in that segment.
    ///
    /// This is a no-op if the segment data is already cached.
    pub(crate) async fn ensure_pq_segment_loaded(&self, seg_ord: u32) -> anyhow::Result<()> {
        // Quick check under lock — return immediately if already loaded
        {
            let cache = self.pq_doc_locations.lock().unwrap();
            if cache.get(seg_ord as usize).and_then(|o| o.as_ref()).is_some() {
                return Ok(());
            }
        }
        // Lock released — do async IO without holding it

        let seg_metas = self.cached_index.searchable_segment_metas().unwrap_or_default();
        let seg_meta = seg_metas.get(seg_ord as usize)
            .ok_or_else(|| anyhow::anyhow!("Segment ordinal {} out of range (have {})", seg_ord, seg_metas.len()))?;
        let fast_path = seg_meta.relative_path(tantivy::index::SegmentComponent::FastFields);

        let range = self.bundle_file_offsets.get(&fast_path)
            .ok_or_else(|| anyhow::anyhow!(".fast file {} not found in bundle offsets", fast_path.display()))?;

        let split_path = std::path::Path::new(&self.split_uri).file_name()
            .map(|f| PathBuf::from(f))
            .unwrap_or_else(|| PathBuf::from(&self.split_uri));

        let fast_bytes = self.cached_storage
            .get_slice(&split_path, range.start as usize..range.end as usize)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to read .fast for {}: {}", fast_path.display(), e))?;

        let body = crate::parquet_companion::transcode::strip_tantivy_footer(&fast_bytes)
            .map_err(|e| anyhow::anyhow!("Failed to strip footer from {}: {}", fast_path.display(), e))?;

        let columnar = tantivy::columnar::ColumnarReader::open(body.to_vec())
            .map_err(|e| anyhow::anyhow!("Failed to open columnar for {}: {}", fast_path.display(), e))?;

        let num_docs = columnar.num_rows();

        // Find the __pq columns
        let mut file_hash_col = None;
        let mut row_col = None;
        for (name, handle) in columnar.list_columns()
            .map_err(|e| anyhow::anyhow!("Failed to list columns: {}", e))?
        {
            if name == crate::parquet_companion::indexing::PARQUET_FILE_HASH_FIELD {
                if let tantivy::columnar::DynamicColumn::U64(col) = handle.open()
                    .map_err(|e| anyhow::anyhow!("Failed to open {} column: {}", name, e))? {
                    file_hash_col = Some(col);
                }
            } else if name == crate::parquet_companion::indexing::PARQUET_ROW_IN_FILE_FIELD {
                if let tantivy::columnar::DynamicColumn::U64(col) = handle.open()
                    .map_err(|e| anyhow::anyhow!("Failed to open {} column: {}", name, e))? {
                    row_col = Some(col);
                }
            }
        }

        let file_hash_col = file_hash_col
            .ok_or_else(|| anyhow::anyhow!("Column {} not found in .fast",
                crate::parquet_companion::indexing::PARQUET_FILE_HASH_FIELD))?;
        let row_col = row_col
            .ok_or_else(|| anyhow::anyhow!("Column {} not found in .fast",
                crate::parquet_companion::indexing::PARQUET_ROW_IN_FILE_FIELD))?;

        let mut seg_locations = Vec::with_capacity(num_docs as usize);
        for doc_id in 0..num_docs {
            let file_hash = file_hash_col.values_for_doc(doc_id).next().unwrap_or(0);
            let row_in_file = row_col.values_for_doc(doc_id).next().unwrap_or(0);
            seg_locations.push((file_hash, row_in_file));
        }

        crate::debug_println!(
            "📦 PARQUET_COMPANION: Lazy-loaded {} __pq locations for segment {}",
            seg_locations.len(), fast_path.display()
        );

        // Store under lock (double-check to avoid overwriting a concurrent load)
        let mut cache = self.pq_doc_locations.lock().unwrap();
        while cache.len() <= seg_ord as usize {
            cache.push(None);
        }
        if cache[seg_ord as usize].is_none() {
            cache[seg_ord as usize] = Some(seg_locations);
        }

        Ok(())
    }

    /// Look up a pre-loaded __pq location. Must call ensure_pq_segment_loaded first.
    pub(crate) fn get_pq_location(&self, seg_ord: u32, doc_id: u32) -> anyhow::Result<(u64, u64)> {
        let cache = self.pq_doc_locations.lock().unwrap();
        cache.get(seg_ord as usize)
            .and_then(|opt| opt.as_ref())
            .and_then(|seg| seg.get(doc_id as usize))
            .copied()
            .ok_or_else(|| anyhow::anyhow!(
                "No __pq location for seg={} doc={} (segments={}, docs={})",
                seg_ord, doc_id, cache.len(),
                cache.get(seg_ord as usize).and_then(|o| o.as_ref()).map(|s| s.len()).unwrap_or(0)
            ))
    }
}
