// ffi_profiler.rs — Near-zero-overhead global profiler for FFI read paths.
//
// Design:
//   - Static arrays of AtomicU64 for count, total_nanos, min_nanos, max_nanos per section.
//   - Global AtomicBool enable flag: Acquire load (~1ns), Release store on enable/disable.
//   - When enabled: 2× Instant::now() + atomic updates per section (~60ns).
//   - Separate cache hit/miss counters (single fetch_add each).
//   - All counters readable and resettable from Java via JNI.
//
// IMPORTANT: No section should fire more than O(num_files) times per query.
// Instrument at batch/file level, NEVER per-row. Per-row instrumentation at 1M rows
// would add ~64ms overhead — unacceptable. Batch-level keeps it under 3µs total.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

// ═══════════════════════════════════════════════════════════════════════
// Section enum — one entry per instrumented code section
// ═══════════════════════════════════════════════════════════════════════

/// Instrumented code sections across the FFI read path.
/// The discriminant is the array index — keep them contiguous.
#[repr(usize)]
#[derive(Clone, Copy, Debug)]
pub enum Section {
    // ── Search flow (10) ─────────────────────────────────────────────
    JniStringExtract        = 0,
    QueryConvert            = 1,
    WildcardAnalysis        = 2,
    QueryRewriteHash        = 3,
    QueryRewriteStringIdx   = 4,
    EnsureFastFields        = 5,
    DocMapperCreate         = 6,
    PermitAcquire           = 7,
    LeafSearch              = 8,
    ResultCreation          = 9,

    // ── Aggregation flow (6) ─────────────────────────────────────────
    AggJavaToJson           = 10,
    AggRewriteHash          = 11,
    AggEnsureFastFields     = 12,
    AggLeafSearch           = 13,
    AggHashTouchup          = 14,
    AggResultCreation       = 15,

    // ── Tantivy store doc retrieval (7) ──────────────────────────────
    DocBatchJniExtract      = 16,
    DocBatchSort            = 17,
    DocBatchRangeConsolidate = 18,
    DocBatchPrefetch        = 19,
    DocBatchDocAsync        = 20,
    DocBatchReorder         = 21,
    DocBatchCreateJavaObjects = 22,

    // ── Parquet companion doc retrieval — outer (6) ──────────────────
    PqDocJniExtract         = 23,
    PqDocEnsurePqFields     = 24,
    PqDocResolveLocations   = 25,
    PqDocGroupByFile        = 26,
    PqDocParquetRead        = 27,
    PqDocJniCopy            = 28,

    // ── Single doc retrieval (3) ─────────────────────────────────────
    SingleDocCacheLookup    = 29,
    SingleDocFetch          = 30,
    SingleDocCacheMissOpen  = 31,

    // ── Prewarm (1) ──────────────────────────────────────────────────
    PrewarmComponents       = 32,

    // ── Parquet companion transcode pipeline (7) ─────────────────────
    PcFieldExtraction       = 33,
    PcL2CacheCheck          = 34,
    PcTranscodeStr          = 35,
    PcTranscodeNonStr       = 36,
    PcMergeColumnars        = 37,
    PcNativeFastRead        = 38,
    PcL2CacheWrite          = 39,

    // ── Parquet companion hash touchup (3) ───────────────────────────
    PcHashScanFastField     = 40,
    PcHashLoadPqColumns     = 41,
    PcHashParquetRead       = 42,

    // ── Parquet doc retrieval internals (5) ──────────────────────────
    PcDocCreateReader       = 43,
    PcDocRowGroupFilter     = 44,
    PcDocRowSelection       = 45,
    PcDocStreamBatches      = 46,
    PcDocSerializeTant      = 47,

    // ── Streaming retrieval (3) ──────────────────────────────────────
    StreamSetup             = 48,
    StreamBatchRead         = 49,
    StreamBatchNext         = 50,

    // ── Arrow FFI export (3) ─────────────────────────────────────────
    ArrowFfiResolve         = 51,
    ArrowFfiBatchRead       = 52,
    ArrowFfiExport          = 53,

    // -- TxLog list files pipeline (FR1) (6) --
    TxLogSnapshot           = 54,
    TxLogManifestPrune      = 55,
    TxLogManifestRead       = 56,
    TxLogPartitionFilter    = 57,
    TxLogDataSkip           = 58,
    TxLogArrowExport        = 59,

    // -- FR4 range filter elimination (1) --
    RangeFilterElimination  = 60,

    // -- TxLog parallel improvements (#153) (2) --
    TxLogCheckpointVerify   = 61,
    TxLogBackwardProbe      = 62,
}

pub const NUM_SECTIONS: usize = 63;

impl Section {
    pub const fn name(self) -> &'static str {
        match self {
            Self::JniStringExtract        => "jni_string_extract",
            Self::QueryConvert            => "query_convert",
            Self::WildcardAnalysis        => "wildcard_analysis",
            Self::QueryRewriteHash        => "query_rewrite_hash",
            Self::QueryRewriteStringIdx   => "query_rewrite_string_idx",
            Self::EnsureFastFields        => "ensure_fast_fields",
            Self::DocMapperCreate         => "doc_mapper_create",
            Self::PermitAcquire           => "permit_acquire",
            Self::LeafSearch              => "leaf_search",
            Self::ResultCreation          => "result_creation",
            Self::AggJavaToJson           => "agg_java_to_json",
            Self::AggRewriteHash          => "agg_rewrite_hash",
            Self::AggEnsureFastFields     => "agg_ensure_fast_fields",
            Self::AggLeafSearch           => "agg_leaf_search",
            Self::AggHashTouchup          => "agg_hash_touchup",
            Self::AggResultCreation       => "agg_result_creation",
            Self::DocBatchJniExtract      => "doc_batch_jni_extract",
            Self::DocBatchSort            => "doc_batch_sort",
            Self::DocBatchRangeConsolidate => "doc_batch_range_consolidate",
            Self::DocBatchPrefetch        => "doc_batch_prefetch",
            Self::DocBatchDocAsync        => "doc_batch_doc_async",
            Self::DocBatchReorder         => "doc_batch_reorder",
            Self::DocBatchCreateJavaObjects => "doc_batch_create_java_objects",
            Self::PqDocJniExtract         => "pq_doc_jni_extract",
            Self::PqDocEnsurePqFields     => "pq_doc_ensure_pq_fields",
            Self::PqDocResolveLocations   => "pq_doc_resolve_locations",
            Self::PqDocGroupByFile        => "pq_doc_group_by_file",
            Self::PqDocParquetRead        => "pq_doc_parquet_read",
            Self::PqDocJniCopy            => "pq_doc_jni_copy",
            Self::SingleDocCacheLookup    => "single_doc_cache_lookup",
            Self::SingleDocFetch          => "single_doc_fetch",
            Self::SingleDocCacheMissOpen  => "single_doc_cache_miss_open",
            Self::PrewarmComponents       => "prewarm_components",
            Self::PcFieldExtraction       => "pc_field_extraction",
            Self::PcL2CacheCheck          => "pc_l2_cache_check",
            Self::PcTranscodeStr          => "pc_transcode_str",
            Self::PcTranscodeNonStr       => "pc_transcode_non_str",
            Self::PcMergeColumnars        => "pc_merge_columnars",
            Self::PcNativeFastRead        => "pc_native_fast_read",
            Self::PcL2CacheWrite          => "pc_l2_cache_write",
            Self::PcHashScanFastField     => "pc_hash_scan_fast_field",
            Self::PcHashLoadPqColumns     => "pc_hash_load_pq_columns",
            Self::PcHashParquetRead       => "pc_hash_parquet_read",
            Self::PcDocCreateReader       => "pc_doc_create_reader",
            Self::PcDocRowGroupFilter     => "pc_doc_row_group_filter",
            Self::PcDocRowSelection       => "pc_doc_row_selection",
            Self::PcDocStreamBatches      => "pc_doc_stream_batches",
            Self::PcDocSerializeTant      => "pc_doc_serialize_tant",
            Self::StreamSetup             => "stream_setup",
            Self::StreamBatchRead         => "stream_batch_read",
            Self::StreamBatchNext         => "stream_batch_next",
            Self::ArrowFfiResolve         => "arrow_ffi_resolve",
            Self::ArrowFfiBatchRead       => "arrow_ffi_batch_read",
            Self::ArrowFfiExport          => "arrow_ffi_export",
            Self::TxLogSnapshot          => "txlog_snapshot",
            Self::TxLogManifestPrune     => "txlog_manifest_prune",
            Self::TxLogManifestRead      => "txlog_manifest_read",
            Self::TxLogPartitionFilter   => "txlog_partition_filter",
            Self::TxLogDataSkip          => "txlog_data_skip",
            Self::TxLogArrowExport       => "txlog_arrow_export",
            Self::RangeFilterElimination => "range_filter_elimination",
            Self::TxLogCheckpointVerify => "txlog_checkpoint_verify",
            Self::TxLogBackwardProbe    => "txlog_backward_probe",
        }
    }

    pub const ALL: [Section; NUM_SECTIONS] = [
        Self::JniStringExtract, Self::QueryConvert, Self::WildcardAnalysis,
        Self::QueryRewriteHash, Self::QueryRewriteStringIdx, Self::EnsureFastFields,
        Self::DocMapperCreate, Self::PermitAcquire, Self::LeafSearch, Self::ResultCreation,
        Self::AggJavaToJson, Self::AggRewriteHash, Self::AggEnsureFastFields,
        Self::AggLeafSearch, Self::AggHashTouchup, Self::AggResultCreation,
        Self::DocBatchJniExtract, Self::DocBatchSort, Self::DocBatchRangeConsolidate,
        Self::DocBatchPrefetch, Self::DocBatchDocAsync, Self::DocBatchReorder,
        Self::DocBatchCreateJavaObjects,
        Self::PqDocJniExtract, Self::PqDocEnsurePqFields, Self::PqDocResolveLocations,
        Self::PqDocGroupByFile, Self::PqDocParquetRead, Self::PqDocJniCopy,
        Self::SingleDocCacheLookup, Self::SingleDocFetch, Self::SingleDocCacheMissOpen,
        Self::PrewarmComponents,
        Self::PcFieldExtraction, Self::PcL2CacheCheck, Self::PcTranscodeStr,
        Self::PcTranscodeNonStr, Self::PcMergeColumnars, Self::PcNativeFastRead,
        Self::PcL2CacheWrite,
        Self::PcHashScanFastField, Self::PcHashLoadPqColumns, Self::PcHashParquetRead,
        Self::PcDocCreateReader, Self::PcDocRowGroupFilter, Self::PcDocRowSelection,
        Self::PcDocStreamBatches, Self::PcDocSerializeTant,
        Self::StreamSetup, Self::StreamBatchRead, Self::StreamBatchNext,
        Self::ArrowFfiResolve, Self::ArrowFfiBatchRead, Self::ArrowFfiExport,
        Self::TxLogSnapshot, Self::TxLogManifestPrune, Self::TxLogManifestRead,
        Self::TxLogPartitionFilter, Self::TxLogDataSkip, Self::TxLogArrowExport,
        Self::RangeFilterElimination,
        Self::TxLogCheckpointVerify, Self::TxLogBackwardProbe,
    ];
}

// ═══════════════════════════════════════════════════════════════════════
// Cache counters — lightweight hit/miss tracking
// ═══════════════════════════════════════════════════════════════════════

#[repr(usize)]
#[derive(Clone, Copy, Debug)]
pub enum CacheCounter {
    ByteRangeHit        = 0,
    ByteRangeMiss       = 1,
    L2DiskHit           = 2,
    L2DiskMiss          = 3,
    SearcherHit         = 4,
    SearcherMiss        = 5,
    ParquetMetadataHit  = 6,
    ParquetMetadataMiss = 7,
    PqColumnHit         = 8,
    PqColumnMiss        = 9,
}

pub const NUM_CACHE_COUNTERS: usize = 10;

impl CacheCounter {
    pub const fn name(self) -> &'static str {
        match self {
            Self::ByteRangeHit        => "byte_range_hit",
            Self::ByteRangeMiss       => "byte_range_miss",
            Self::L2DiskHit           => "l2_disk_hit",
            Self::L2DiskMiss          => "l2_disk_miss",
            Self::SearcherHit         => "searcher_hit",
            Self::SearcherMiss        => "searcher_miss",
            Self::ParquetMetadataHit  => "parquet_metadata_hit",
            Self::ParquetMetadataMiss => "parquet_metadata_miss",
            Self::PqColumnHit         => "pq_column_hit",
            Self::PqColumnMiss        => "pq_column_miss",
        }
    }

    pub const ALL: [CacheCounter; NUM_CACHE_COUNTERS] = [
        Self::ByteRangeHit, Self::ByteRangeMiss,
        Self::L2DiskHit, Self::L2DiskMiss,
        Self::SearcherHit, Self::SearcherMiss,
        Self::ParquetMetadataHit, Self::ParquetMetadataMiss,
        Self::PqColumnHit, Self::PqColumnMiss,
    ];
}

// ═══════════════════════════════════════════════════════════════════════
// Compile-time safety: ensure ALL array and enum stay in sync
// ═══════════════════════════════════════════════════════════════════════

const _: () = assert!(Section::TxLogBackwardProbe as usize == NUM_SECTIONS - 1,
    "Section enum last variant must equal NUM_SECTIONS - 1");
const _: () = assert!(CacheCounter::PqColumnMiss as usize == NUM_CACHE_COUNTERS - 1,
    "CacheCounter enum last variant must equal NUM_CACHE_COUNTERS - 1");

// ═══════════════════════════════════════════════════════════════════════
// Static storage — all zero-initialized, no heap allocation
// ═══════════════════════════════════════════════════════════════════════

static ENABLED: AtomicBool = AtomicBool::new(false);

// Per-section: count, total_nanos, min_nanos, max_nanos
const INIT_ZERO: AtomicU64 = AtomicU64::new(0);
const INIT_MAX: AtomicU64 = AtomicU64::new(u64::MAX); // sentinel for min

static COUNTS: [AtomicU64; NUM_SECTIONS] = [INIT_ZERO; NUM_SECTIONS];
static NANOS:  [AtomicU64; NUM_SECTIONS] = [INIT_ZERO; NUM_SECTIONS];
static MINS:   [AtomicU64; NUM_SECTIONS] = [INIT_MAX; NUM_SECTIONS];
static MAXS:   [AtomicU64; NUM_SECTIONS] = [INIT_ZERO; NUM_SECTIONS];

// Cache counters
static CACHE_COUNTERS: [AtomicU64; NUM_CACHE_COUNTERS] = [INIT_ZERO; NUM_CACHE_COUNTERS];

// ═══════════════════════════════════════════════════════════════════════
// Public API
// ═══════════════════════════════════════════════════════════════════════

/// Check if profiling is enabled.
/// Uses Acquire ordering so that when enabled==true, all reset stores are visible.
/// Cost: ~1ns on x86 (identical to Ordering::Relaxed), ~1ns on ARM (load-acquire).
#[inline(always)]
pub fn is_enabled() -> bool {
    ENABLED.load(Ordering::Acquire)
}

/// Enable profiling and reset all counters to start fresh.
/// Uses Release ordering so other threads see zeroed counters when they observe enabled==true.
pub fn enable() {
    reset_all_internal();
    ENABLED.store(true, Ordering::Release);
}

/// Disable profiling.
pub fn disable() {
    ENABLED.store(false, Ordering::Release);
}

/// Record one invocation of `section` that took `nanos` nanoseconds.
/// Only called when profiling is enabled (the macro gates on is_enabled()).
#[inline(always)]
pub fn record(section: Section, nanos: u64) {
    let idx = section as usize;
    COUNTS[idx].fetch_add(1, Ordering::Relaxed);
    NANOS[idx].fetch_add(nanos, Ordering::Relaxed);

    // Update min — CAS loop (typically 1 iteration).
    // Load inside loop so the condition always uses the freshest value.
    loop {
        let current_min = MINS[idx].load(Ordering::Relaxed);
        if nanos >= current_min { break; }
        if MINS[idx].compare_exchange_weak(current_min, nanos, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
            break;
        }
    }

    // Update max — CAS loop (typically 1 iteration)
    loop {
        let current_max = MAXS[idx].load(Ordering::Relaxed);
        if nanos <= current_max { break; }
        if MAXS[idx].compare_exchange_weak(current_max, nanos, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
            break;
        }
    }
}

/// Increment a cache counter by 1.
#[inline(always)]
pub fn cache_inc(counter: CacheCounter) {
    if is_enabled() {
        CACHE_COUNTERS[counter as usize].fetch_add(1, Ordering::Relaxed);
    }
}

/// Snapshot section data as a flat array: [count0, nanos0, min0, max0, count1, nanos1, ...].
/// 4 values per section × NUM_SECTIONS entries.
/// min is reported as 0 if count is 0 (sentinel replaced).
///
/// **Consistency note:** Snapshots use Relaxed loads per-field, so values are approximate
/// under concurrent recording. A section's count and nanos may reflect different numbers
/// of recordings. This is acceptable for profiling — values converge over many samples.
pub fn snapshot_flat() -> Vec<i64> {
    let mut flat = Vec::with_capacity(NUM_SECTIONS * 4);
    for &s in Section::ALL.iter() {
        let idx = s as usize;
        let count = COUNTS[idx].load(Ordering::Relaxed);
        let nanos = NANOS[idx].load(Ordering::Relaxed);
        let min = MINS[idx].load(Ordering::Relaxed);
        let max = MAXS[idx].load(Ordering::Relaxed);
        flat.push(count as i64);
        flat.push(nanos as i64);
        flat.push(if count == 0 { 0 } else { min as i64 });
        flat.push(max as i64);
    }
    flat
}

/// Reset section counters and return pre-reset flat array.
pub fn reset_sections_flat() -> Vec<i64> {
    let mut flat = Vec::with_capacity(NUM_SECTIONS * 4);
    for &s in Section::ALL.iter() {
        let idx = s as usize;
        let count = COUNTS[idx].swap(0, Ordering::Relaxed);
        let nanos = NANOS[idx].swap(0, Ordering::Relaxed);
        let min = MINS[idx].swap(u64::MAX, Ordering::Relaxed);
        let max = MAXS[idx].swap(0, Ordering::Relaxed);
        flat.push(count as i64);
        flat.push(nanos as i64);
        flat.push(if count == 0 { 0 } else { min as i64 });
        flat.push(max as i64);
    }
    flat
}

/// Snapshot cache counters as flat array: [counter0, counter1, ...].
pub fn cache_snapshot_flat() -> Vec<i64> {
    CacheCounter::ALL.iter().map(|c| CACHE_COUNTERS[*c as usize].load(Ordering::Relaxed) as i64).collect()
}

/// Reset cache counters and return pre-reset flat array.
pub fn cache_reset_flat() -> Vec<i64> {
    CacheCounter::ALL.iter().map(|c| CACHE_COUNTERS[*c as usize].swap(0, Ordering::Relaxed) as i64).collect()
}

/// Internal: zero everything.
fn reset_all_internal() {
    for i in 0..NUM_SECTIONS {
        COUNTS[i].store(0, Ordering::Relaxed);
        NANOS[i].store(0, Ordering::Relaxed);
        MINS[i].store(u64::MAX, Ordering::Relaxed);
        MAXS[i].store(0, Ordering::Relaxed);
    }
    for i in 0..NUM_CACHE_COUNTERS {
        CACHE_COUNTERS[i].store(0, Ordering::Relaxed);
    }
}

// ═══════════════════════════════════════════════════════════════════════
// Macros — the instrumentation interface
// ═══════════════════════════════════════════════════════════════════════

/// Profile a synchronous code block. ~1ns when disabled, ~64ns when enabled.
///
/// Usage:
/// ```ignore
/// let result = profile_section!(Section::LeafSearch, {
///     do_expensive_work()
/// });
/// ```
#[macro_export]
macro_rules! profile_section {
    ($section:expr, $block:expr) => {{
        if $crate::ffi_profiler::is_enabled() {
            let __pf_t0 = std::time::Instant::now();
            let __pf_result = $block;
            $crate::ffi_profiler::record($section, __pf_t0.elapsed().as_nanos() as u64);
            __pf_result
        } else {
            $block
        }
    }};
}

// NOTE: profile_section! works for both sync and async code. When the block contains
// .await, wall-clock time including suspension is measured. No separate async macro needed.

#[cfg(test)]
mod tests {
    use super::*;

    // All profiler tests must run sequentially since they share global statics.
    // We use a single test function to guarantee ordering.

    #[test]
    fn test_profiler_comprehensive() {
        // ── Phase 1: enable/disable ──
        disable(); // ensure clean state
        assert!(!is_enabled());
        enable();
        assert!(is_enabled());
        disable();
        assert!(!is_enabled());

        // ── Phase 2: record and snapshot ──
        enable(); // resets all counters
        record(Section::LeafSearch, 1000);
        record(Section::LeafSearch, 3000);
        record(Section::LeafSearch, 2000);

        let flat = snapshot_flat();
        let idx = Section::LeafSearch as usize;
        assert_eq!(flat[idx * 4], 3, "count");
        assert_eq!(flat[idx * 4 + 1], 6000, "total nanos");
        assert_eq!(flat[idx * 4 + 2], 1000, "min");
        assert_eq!(flat[idx * 4 + 3], 3000, "max");

        // Reset should return same values then zero
        let pre = reset_sections_flat();
        assert_eq!(pre[idx * 4], 3, "pre-reset count");
        let post = snapshot_flat();
        assert_eq!(post[idx * 4], 0, "post-reset count");

        // ── Phase 3: cache counters ──
        enable(); // resets again
        cache_inc(CacheCounter::ByteRangeHit);
        cache_inc(CacheCounter::ByteRangeHit);
        cache_inc(CacheCounter::ByteRangeMiss);

        let snap = cache_snapshot_flat();
        assert_eq!(snap[CacheCounter::ByteRangeHit as usize], 2, "cache hits");
        assert_eq!(snap[CacheCounter::ByteRangeMiss as usize], 1, "cache misses");

        let pre = cache_reset_flat();
        assert_eq!(pre[CacheCounter::ByteRangeHit as usize], 2, "pre-reset cache hits");

        let post = cache_snapshot_flat();
        assert_eq!(post[CacheCounter::ByteRangeHit as usize], 0, "post-reset cache hits");

        // ── Phase 4: zero count min sentinel ──
        enable(); // resets again
        let flat = snapshot_flat();
        let idx = Section::JniStringExtract as usize;
        assert_eq!(flat[idx * 4 + 2], 0, "min should be 0 when count=0");

        // ── Phase 5: cache_inc is no-op when disabled ──
        disable();
        cache_inc(CacheCounter::L2DiskHit);
        enable(); // resets
        let snap = cache_snapshot_flat();
        assert_eq!(snap[CacheCounter::L2DiskHit as usize], 0, "disabled cache_inc should be no-op");

        disable();
    }

    #[test]
    fn test_section_names_all_unique() {
        let mut names = std::collections::HashSet::new();
        for s in Section::ALL.iter() {
            assert!(names.insert(s.name()), "Duplicate section name: {}", s.name());
        }
        assert_eq!(names.len(), NUM_SECTIONS);
    }

    #[test]
    fn test_all_array_matches_enum() {
        for (i, s) in Section::ALL.iter().enumerate() {
            assert_eq!(*s as usize, i, "Section::ALL[{}] has discriminant {}", i, *s as usize);
        }
    }
}
