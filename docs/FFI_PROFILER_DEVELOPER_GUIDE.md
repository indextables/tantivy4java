# FFI Read-Path Profiler — Developer Guide

## Overview

The FFI profiler is a near-zero-overhead instrumentation system for the native Rust read path. It tracks invocation count, total time, min time, and max time for 54 code sections across all FFI read operations, plus hit/miss counters for 5 cache layers.

**Design goals:**
- **~1ns overhead when disabled** (single `Acquire` atomic load per section — identical cost to `Relaxed` on x86, ~1ns on ARM)
- **~60-64ns overhead when enabled** (2x `Instant::now()` + atomic updates)
- **Zero heap allocation** on the hot path
- **Global read/reset** via flat `long[]` JNI transfer

## Quick Start

```java
import io.indextables.tantivy4java.split.FfiProfiler;

// Enable profiling (auto-resets all counters)
FfiProfiler.enable();

// Run your workload
for (int i = 0; i < 1000; i++) {
    searcher.search(query, 10);
}

// Read results as a Map
Map<String, FfiProfiler.ProfileEntry> profile = FfiProfiler.reset();
profile.values().forEach(System.out::println);

// Read cache stats
Map<String, Long> cache = FfiProfiler.resetCacheCounters();
cache.forEach((k, v) -> System.out.println(k + ": " + v));

// Disable (back to ~0 overhead)
FfiProfiler.disable();
```

**Example output:**
```
query_convert                   calls=1000      total=   21.50ms  avg=   21.50µs  min=   18.20µs  max=   45.00µs
query_rewrite_hash              calls=1000      total=    3.30ms  avg=    3.30µs  min=    2.80µs  max=    8.10µs
ensure_fast_fields              calls=1000      total=    0.13ms  avg=    0.13µs  min=    0.10µs  max=    0.50µs
doc_mapper_create               calls=1000      total=   42.30ms  avg=   42.30µs  min=   38.00µs  max=   95.00µs
permit_acquire                  calls=1000      total=    0.50ms  avg=    0.50µs  min=    0.40µs  max=    1.20µs
leaf_search                     calls=1000      total= 2841.00ms  avg= 2841.00µs  min= 2100.00µs  max= 5200.00µs
result_creation                 calls=1000      total=    7.70ms  avg=    7.70µs  min=    6.00µs  max=   15.00µs
```

## Java API

### `FfiProfiler` Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `enable()` | void | Enable profiling. **Auto-resets** all counters for a clean start. |
| `disable()` | void | Disable profiling. Counters are preserved and still readable. |
| `isEnabled()` | boolean | Check if profiling is currently enabled. |
| `snapshot()` | `Map<String, ProfileEntry>` | Read current section counters. Zero-count sections omitted. |
| `reset()` | `Map<String, ProfileEntry>` | Reset section counters, returning pre-reset values. |
| `cacheCounters()` | `Map<String, Long>` | Read current cache hit/miss counters. Zero-count omitted. |
| `resetCacheCounters()` | `Map<String, Long>` | Reset cache counters, returning pre-reset values. |

### `ProfileEntry` Fields

| Method | Type | Description |
|--------|------|-------------|
| `getSection()` | String | Section name (e.g., `"leaf_search"`) |
| `getCount()` | long | Number of invocations |
| `getTotalNanos()` | long | Cumulative wall-clock nanoseconds |
| `getMinNanos()` | long | Fastest single invocation (nanos) |
| `getMaxNanos()` | long | Slowest single invocation (nanos) |
| `totalMillis()` | double | `totalNanos / 1_000_000.0` |
| `avgMicros()` | double | `totalNanos / count / 1_000.0` |
| `minMicros()` | double | `minNanos / 1_000.0` |
| `maxMicros()` | double | `maxNanos / 1_000.0` |

The map returned by `snapshot()` and `reset()` uses `LinkedHashMap` and preserves logical flow order (search → aggregation → doc retrieval → parquet companion → streaming → Arrow FFI).

## Section Inventory

### Search Flow (10 sections)

| Section | Location | What it measures |
|---------|----------|-----------------|
| `jni_string_extract` | jni_search.rs | JNI string parameter extraction |
| `query_convert` | jni_search.rs | SplitQuery → QueryAst JSON conversion |
| `wildcard_analysis` | async_impl.rs | Smart wildcard AST analysis + cheap filter |
| `query_rewrite_hash` | async_impl.rs | FieldPresence → hash field rewriting + string indexing rewriting |
| `query_rewrite_string_idx` | async_impl.rs | (Reserved for finer-grained string indexing rewrite timing) |
| `ensure_fast_fields` | async_impl.rs | Lazy parquet → tantivy fast field transcoding |
| `doc_mapper_create` | async_impl.rs | DocMapper JSON parse + build |
| `permit_acquire` | async_impl.rs | SearchPermitProvider permit wait |
| `leaf_search` | async_impl.rs | `leaf_search_single_split()` execution |
| `result_creation` | jni_search.rs | LeafSearchResponse → Java SearchResult |

### Aggregation Flow (6 sections)

| Section | Location | What it measures |
|---------|----------|-----------------|
| `agg_java_to_json` | jni_search.rs | Java aggregation HashMap → JSON |
| `agg_rewrite_hash` | jni_search.rs | Phase 2a/2b: query + aggregation hash field rewriting |
| `agg_ensure_fast_fields` | jni_search.rs | Fast field transcoding for aggregation fields |
| `agg_leaf_search` | jni_search.rs | leaf_search with aggregation_request |
| `agg_hash_touchup` | jni_search.rs | Phase 3: hash → string resolution map build |
| `agg_result_creation` | jni_search.rs | Result + aggregation JSON → Java objects |

### Tantivy Store Document Retrieval (7 sections)

| Section | Location | What it measures |
|---------|----------|-----------------|
| `doc_batch_jni_extract` | doc_retrieval_jni.rs | JNI int array extraction |
| `doc_batch_sort` | doc_retrieval_jni.rs | Address sorting for cache locality |
| `doc_batch_range_consolidate` | batch_doc_retrieval.rs | Merge adjacent byte ranges for prefetch |
| `doc_batch_prefetch` | batch_doc_retrieval.rs | S3/Azure batch byte range fetch into ByteRangeCache |
| `doc_batch_doc_async` | batch_doc_retrieval.rs | Buffered concurrent `doc_async()` calls |
| `doc_batch_reorder` | doc_retrieval_jni.rs | Reorder docs to original input order |
| `doc_batch_create_java_objects` | doc_retrieval_jni.rs | Create Java Document[] objects |

### Parquet Companion Document Retrieval (6 sections)

| Section | Location | What it measures |
|---------|----------|-----------------|
| `pq_doc_jni_extract` | doc_retrieval_jni.rs | JNI array + field name extraction |
| `pq_doc_ensure_pq_fields` | doc_retrieval_jni.rs | Lazy-load `__pq` fast field columns per segment |
| `pq_doc_resolve_locations` | doc_retrieval_jni.rs | O(1) fast field lookup: (seg, doc) → (file_hash, row) |
| `pq_doc_group_by_file` | doc_retrieval_jni.rs | Cluster rows by parquet file |
| `pq_doc_parquet_read` | doc_retrieval_jni.rs | Parquet row group decode + TANT serialization |
| `pq_doc_jni_copy` | doc_retrieval_jni.rs | Copy TANT bytes into JNI byte array |

### Single Document Retrieval (3 sections)

| Section | Location | What it measures |
|---------|----------|-----------------|
| `single_doc_cache_lookup` | single_doc_retrieval.rs | Searcher LRU cache check |
| `single_doc_fetch` | single_doc_retrieval.rs | `doc_async()` on cache hit |
| `single_doc_cache_miss_open` | single_doc_retrieval.rs | Open index + create searcher on cache miss |

### Prewarm (1 section)

| Section | Location | What it measures |
|---------|----------|-----------------|
| `prewarm_components` | jni_prewarm.rs | Component preloading execution |

### Parquet Companion Transcode Pipeline (7 sections)

| Section | Location | What it measures |
|---------|----------|-----------------|
| `pc_field_extraction` | field_extraction.rs | Parse query + agg JSON to find needed columns |
| `pc_l2_cache_check` | augmented_directory.rs | L2 disk cache lookup for transcoded .fast bytes |
| `pc_transcode_str` | transcode.rs | Zero-copy string column transcoding |
| `pc_transcode_non_str` | transcode.rs | Numeric/bool/date/ip transcoding via ColumnarWriter |
| `pc_merge_columnars` | augmented_directory.rs | Merge native + parquet .fast bytes (HYBRID mode) |
| `pc_native_fast_read` | augmented_directory.rs | Read native .fast bytes from split bundle |
| `pc_l2_cache_write` | augmented_directory.rs | Write transcoded bytes to L2 disk cache |

### Parquet Companion Hash Touchup (3 sections)

| Section | Location | What it measures |
|---------|----------|-----------------|
| `pc_hash_scan_fast_field` | hash_touchup.rs | Scan Column\<u64\> to find representative doc per hash |
| `pc_hash_load_pq_columns` | hash_touchup.rs | Load __pq file_hash + row_in_file columns |
| `pc_hash_parquet_read` | hash_touchup.rs | Batch string read from parquet files |

### Parquet Document Retrieval Internals (5 sections)

| Section | Location | What it measures |
|---------|----------|-----------------|
| `pc_doc_create_reader` | arrow_to_tant.rs | CachedParquetReader setup |
| `pc_doc_row_group_filter` | arrow_to_tant.rs | Determine which row groups contain target rows |
| `pc_doc_row_selection` | arrow_to_tant.rs | Build row selection within row groups |
| `pc_doc_stream_batches` | arrow_to_tant.rs | Parquet page decode + decompression |
| `pc_doc_serialize_tant` | arrow_to_tant.rs | Arrow rows → TANT binary format |

### Streaming Retrieval (3 sections)

| Section | Location | What it measures |
|---------|----------|-----------------|
| `stream_setup` | streaming_ffi.rs | Schema build + channel creation + spawn producer |
| `stream_batch_read` | streaming_ffi.rs | Per-file parquet read with adaptive coalesce |
| `stream_batch_next` | streaming_ffi.rs | Consumer wait for next batch from channel |

### Arrow FFI Export (3 sections)

| Section | Location | What it measures |
|---------|----------|-----------------|
| `arrow_ffi_resolve` | doc_retrieval_jni.rs | resolve_doc_addresses_to_groups |
| `arrow_ffi_batch_read` | doc_retrieval_jni.rs | Parquet batch read for Arrow export |
| `arrow_ffi_export` | arrow_ffi_export.rs | Write to FFI_ArrowArray/Schema structs |

## Cache Counters

| Counter | What it tracks |
|---------|---------------|
| `byte_range_hit` / `byte_range_miss` | Storage byte range cache (page/chunk reuse) |
| `l2_disk_hit` / `l2_disk_miss` | L2 disk cache for transcoded fast field bytes |
| `searcher_hit` / `searcher_miss` | Tantivy searcher LRU cache |
| `parquet_metadata_hit` / `parquet_metadata_miss` | Parquet footer metadata reuse |
| `pq_column_hit` / `pq_column_miss` | `__pq` fast field column cache |

## Overhead Analysis

### Per-Section Cost

| State | Cost per section | Components |
|-------|-----------------|------------|
| **Disabled** | ~1ns | 1x `AtomicBool::load(Acquire)` (same cost as Relaxed on x86) |
| **Enabled** | ~64ns | 2x `Instant::now()` (~22ns each) + `fetch_add` count + `fetch_add` nanos + CAS min + CAS max |

### For a Typical Workload (1M doc retrieval, 3 parquet files)

| | Timed sections | Cache increments | Total overhead |
|--|---------------|-----------------|----------------|
| **Disabled** | 37 x 1ns = 37ns | 0 | **~37ns** |
| **Enabled** | 37 x 64ns = 2.4µs | ~70 x 5ns = 0.35µs | **~2.7µs** |

For a 500ms query, the profiler adds 0.0005% overhead when enabled.

### Critical Granularity Rule

**No section should fire more than O(num_files) times per query.**

Instrument at batch/file level, **never per-row**:

```
BAD:  1M rows x 64ns = 64ms overhead (10-25% of query time)
GOOD: 3 files x 64ns = 192ns overhead (unmeasurable)
```

Cache counters fire at O(num_row_groups) which is acceptable since they're a single `fetch_add` (~5ns).

## Adding New Sections

### Step 1: Add to the Rust enum

In `native/src/ffi_profiler.rs`:

```rust
#[repr(usize)]
pub enum Section {
    // ... existing sections ...
    MyNewSection = 54,  // next available index
}

const NUM_SECTIONS: usize = 55;  // bump this
```

Update `Section::name()`, `Section::ALL`, and tests.

### Step 2: Add to the Java name table

In `src/main/java/io/indextables/tantivy4java/split/FfiProfiler.java`:

```java
private static final String[] SECTION_NAMES = {
    // ... existing names ...
    "my_new_section",  // must match Rust name() exactly, same index
};
```

### Step 3: Instrument the code

Use the boundary-recording pattern (preferred for async code):

```rust
let t_start = std::time::Instant::now();
// ... work ...
if crate::ffi_profiler::is_enabled() {
    crate::ffi_profiler::record(
        crate::ffi_profiler::Section::MyNewSection,
        t_start.elapsed().as_nanos() as u64,
    );
}
```

Or the macro pattern (for synchronous code):

```rust
use crate::profile_section;
use crate::ffi_profiler::Section;

let result = profile_section!(Section::MyNewSection, {
    do_work()
});
```

### Step 4: Add cache counters (if applicable)

```rust
// In the cache lookup site:
crate::ffi_profiler::cache_inc(crate::ffi_profiler::CacheCounter::MyNewHit);
// or
crate::ffi_profiler::cache_inc(crate::ffi_profiler::CacheCounter::MyNewMiss);
```

## Important Caveats

### Async Wall-Time

Sections measured across `.await` points include Tokio scheduling time. Under concurrent load, a section's time may include time spent running *other* tasks on the same Tokio worker. For single-query benchmarks this is accurate; under load, interpret as "wall-clock time from the caller's perspective."

### Atomic Ordering

The enable flag uses `Acquire`/`Release` ordering — when a thread sees `enabled==true`, all reset-to-zero stores are guaranteed visible. Counter reads and writes use `Relaxed` ordering for maximum performance. This means:
- Counters may be slightly stale across CPU cores (acceptable for profiling)
- Snapshot across sections is non-atomic (section A and section B may be from different moments)
- Count and nanos for the *same* section may be slightly inconsistent during concurrent updates

### Hierarchical Sections

Some sections are intentionally nested/overlapping:
- `LeafSearch` includes `DocMapperCreate` + `PermitAcquire` + the actual `leaf_search_single_split` call
- `QueryRewriteHash` includes `QueryRewriteStringIdx` time (total rewrite vs sub-phase)
- `ArrowFfiBatchRead` includes `ArrowFfiExport` time

To get the "pure" inner time, subtract the sub-section from the parent. For example: actual leaf search time = `LeafSearch` - `DocMapperCreate` - `PermitAcquire`.

### Test Isolation

The profiler uses global statics. Unit tests that call `enable()` will reset counters set by other tests. In Rust, the unit tests use a single combined test function to avoid parallel interference. In Java, each test calls `enable()` at the start to get a clean slate.

## Architecture

```
Java: FfiProfiler.snapshot()
  → JNI: nativeProfilerSnapshot()
    → Rust: snapshot_flat()
      → Read 54 x (AtomicU64 x 4) → pack into i64[]
    ← flat long[216]
  ← decode into LinkedHashMap<String, ProfileEntry>
```

**Rust statics** (zero-initialized, no heap):
```
ENABLED: AtomicBool
COUNTS:  [AtomicU64; 54]
NANOS:   [AtomicU64; 54]
MINS:    [AtomicU64; 54]  (initialized to u64::MAX)
MAXS:    [AtomicU64; 54]
CACHE_COUNTERS: [AtomicU64; 10]
```

**JNI transfer format** (flat `long[]`):
```
Sections: [count0, nanos0, min0, max0, count1, nanos1, min1, max1, ...]
           ← 4 values per section × 54 sections = 216 longs →

Cache:    [counter0, counter1, ...]
           ← 1 value per counter × 10 counters = 10 longs →
```

**Macros** (`#[macro_export]` at crate root):
- `profile_section!(Section::Xxx, { block })` — works for both sync and async code (measures wall time including `.await`)

Note: `return` statements inside the macro block will exit the enclosing function and skip recording. This means error paths are not profiled. This is by design — profiling only captures successful executions.

Both check `is_enabled()` first and skip all timing when disabled.
