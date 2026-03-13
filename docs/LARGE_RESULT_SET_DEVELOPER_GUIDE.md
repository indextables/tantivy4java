# Large Result Set Retrieval — Developer Guide

Bulk retrieval for returning thousands to millions of rows from Quickwit
splits via Arrow FFI, with bounded memory.

## Overview

Two retrieval tiers are available:

| Tier | Use Case | API | Memory | Latency |
|------|----------|-----|--------|---------|
| **Streaming** | Bulk unscored | `startStreamingRetrieval()` / `nextBatch()` | ~24MB fixed | Pipelined |
| **Scored** | Top-K ranked | `search()` + `docBatchArrowFfi()` | Proportional to K | Two calls |

**Streaming works for all split types:**
- **Companion splits** (with parquet manifest) stream from parquet files via columnar reads
- **Regular splits** (tantivy doc store only) stream from the tantivy document store via `doc_async()`

---

## Architecture

```
Java caller
    │
    ├─ Streaming path ──────────────────────────────────────────┐
    │   startStreamingRetrieval()                               │
    │       → JNI: nativeStartStreamingRetrieval                │
    │           1. DocIdCollector search (no BM25)              │
    │           ┌── companion split? ──────────────────────┐    │
    │           │ YES: resolve → parquet streaming         │    │
    │           │   2. Fast-field resolution to file+row   │    │
    │           │   3. start_streaming_retrieval()         │    │
    │           │      (columnar parquet reads)            │    │
    │           ├──────────────────────────────────────────┤    │
    │           │ NO:  tantivy doc store streaming         │    │
    │           │   2. start_tantivy_streaming_retrieval() │    │
    │           │      (row-oriented doc_async reads)      │    │
    │           └──────────────────────────────────────────┘    │
    │               → spawns tokio producer task                │
    │               → returns session handle                   │
    │                                                           │
    │   nextBatch() (called repeatedly)                         │
    │       → JNI: nativeNextBatch                              │
    │           → session.blocking_next()                       │
    │           → write_batch_to_ffi()                          │
    │       ← row count (>0 = data, 0 = end, -1 = error)       │
    │                                                           │
    │   session.close()  (AutoCloseable)                         │
    │       → JNI: nativeCloseStreamingSession                  │
    │           → release_arc (ARC_REGISTRY)                    │
    └───────────────────────────────────────────────────────────┘
```

---

## Rust Module Layout

```
native/src/
├── split_searcher/
│   ├── docid_collector.rs          # Stage 1: No-score tantivy Collector
│   ├── bulk_retrieval.rs           # Stage 1: Search + fast-field resolution
│   └── streaming_doc_retrieval.rs  # Non-companion: tantivy doc store → Arrow
│
└── parquet_companion/
    ├── streaming_ffi.rs            # Session type + companion streaming pipeline
    ├── read_strategy.rs            # Adaptive I/O strategy (companion only)
    └── arrow_ffi_export.rs         # Shared: batch FFI export (pre-existing)
```

---

## Stage 1: DocIdCollector + Bulk Search

### DocIdCollector (`docid_collector.rs`)

A custom tantivy `Collector` that returns `Vec<(segment_ord, doc_id)>` pairs
without computing BM25 scores.

```rust
pub struct DocIdCollector;

// Key trait implementations:
impl Collector for DocIdCollector {
    type Fruit = Vec<(u32, u32)>;
    fn requires_scoring(&self) -> bool { false }  // Skips term-frequency decompression
}

impl SegmentCollector for DocIdSegmentCollector {
    fn collect_block(&mut self, docs: &[DocId]) { ... }  // Batch of up to 64 docs
}
```

**Why not Quickwit's `leaf_search_single_split`?**

The standard Quickwit path allocates a `PartialHit` protobuf per document
(split_id string + sort values + score). For 2M results, that's 2M protobuf
allocations we don't need. `DocIdCollector` stores only 8 bytes per hit
(two `u32`s), and by returning `requires_scoring() = false`, tantivy uses
the batch `collect_block()` path (up to 64 docs at a time) and never
decompresses term frequencies.

**Memory**: 8 bytes/hit. 2M hits = 16MB.

### Bulk Retrieval (`bulk_retrieval.rs`)

Three public functions used by the streaming path:

```rust
/// No-score search: QueryAst JSON → Vec<(segment_ord, doc_id)>
pub fn perform_bulk_search(
    ctx: &CachedSearcherContext,
    query_ast_json: &str,
) -> Result<Vec<(u32, u32)>>

/// Resolve doc addresses → parquet file groups (entirely in Rust, no JNI)
pub async fn resolve_to_parquet_locations(
    ctx: &Arc<CachedSearcherContext>,
    doc_ids: &[(u32, u32)],
) -> Result<HashMap<usize, Vec<(usize, u64)>>>
//  Returns: file_idx → [(original_index, row_in_file)], sorted by row_in_file

/// Get parquet storage from context with diagnostic error messages
pub fn get_parquet_storage(
    ctx: &CachedSearcherContext,
) -> Result<Arc<dyn Storage>>
```

**Resolution modes:**

| Mode | Condition | Mechanism |
|------|-----------|-----------|
| **Fast-field** | `ctx.has_merge_safe_tracking == true` | O(1) lookup via `__pq_file_hash` / `__pq_row_in_file` columns |
| **Legacy** | `ctx.has_merge_safe_tracking == false` | Manifest-based positional resolution via `group_doc_addresses_by_file()` |

---

## Stage 2: Streaming Pipeline

### Session Model (`streaming_ffi.rs`)

For result sets too large to materialize in one call, the streaming path
uses a producer/consumer model with bounded memory:

```
 ┌─────────────┐     mpsc::channel(2)     ┌──────────────┐
 │   Producer   │ ──── RecordBatch ──────► │   Consumer   │
 │  (tokio task)│                          │  (JNI thread)│
 │              │    ~12MB per batch       │              │
 │  Reads files │    max 2 in flight       │  Writes FFI  │
 │  sequentially│    = ~24MB peak          │  per batch   │
 └─────────────┘                          └──────────────┘
```

```rust
pub struct StreamingRetrievalSession {
    receiver: mpsc::Receiver<Result<RecordBatch>>,
    schema: Arc<Schema>,
    _producer_handle: Option<tokio::task::JoinHandle<()>>,  // None for empty sessions
}

pub fn start_streaming_retrieval(
    groups: HashMap<usize, Vec<(usize, u64)>>,
    projected_fields: Option<Vec<String>>,
    manifest: Arc<ParquetManifest>,
    storage: Arc<dyn Storage>,
    metadata_cache: Option<MetadataCache>,
    byte_cache: Option<ByteRangeCache>,
    coalesce_config: Option<CoalesceConfig>,
) -> Result<StreamingRetrievalSession>
```

**Constants:**
```rust
const TARGET_BATCH_SIZE: usize = 128 * 1024;  // 128K rows ≈ 12MB at ~100 bytes/row
```

### Producer Algorithm

1. Sort file groups by `file_idx` for deterministic output order
2. For each file:
   - Compute selectivity and select adaptive I/O strategy (see Stage 3)
   - Read file's rows via `read_parquet_batches_for_file()`
   - Feed batches through `BatchAccumulator`
   - Rename columns (parquet → tantivy names)
   - Normalize timestamps to microseconds (Spark compatibility)
   - Send through channel (blocks if consumer is behind)
3. Flush remaining accumulated rows as final batch

### BatchAccumulator

Prevents emitting tiny batches from small files while avoiding unbounded memory:

```rust
struct BatchAccumulator {
    target_size: usize,       // TARGET_BATCH_SIZE
    pending: Vec<RecordBatch>,
    pending_rows: usize,
}

// push() returns vec![] until pending_rows >= target_size,
// then drains all pending into one concatenated batch.
// flush() returns any remaining rows at end of stream.
```

Example: 10 files each contributing 20K rows → accumulator concatenates
batches from ~7 files into one 128K-row output batch, then emits a second
batch with the remaining ~72K rows.

### Arrow Type Hints (Non-Companion Splits)

When streaming from non-companion (regular tantivy) splits, the Arrow output
types default to tantivy's internal storage types: all integers as `Int64`,
all floats as `Float64`. This can cause problems when the caller expects
narrower types (e.g., Spark's `IntegerType` → `Int32`, `FloatType` → `Float32`).

The `startStreamingRetrieval()` overload with type hints lets the caller
specify the desired Arrow output type per column. Values are narrowed during
Arrow array construction — no extra cast pass needed.

```java
// Type hints: alternating [fieldName, arrowType, fieldName, arrowType, ...]
String[] typeHints = new String[] {
    "id",    "i32",    // tantivy stores as i64, output as Int32
    "score", "f32",    // tantivy stores as f64, output as Float32
    "rank",  "i16",    // tantivy stores as i64, output as Int16
};

try (SplitSearcher.StreamingSession session =
        searcher.startStreamingRetrieval(queryJson,
            new String[]{"id", "score", "rank", "name"},
            typeHints)) {
    // id column → Int32Array, score → Float32Array, rank → Int16Array, name → Utf8 (default)
}
```

**Supported type hint strings:**

| Hint String | Arrow DataType | Use Case |
|-------------|---------------|----------|
| `"i8"` / `"byte"` | `Int8` | Spark `ByteType` |
| `"i16"` / `"short"` | `Int16` | Spark `ShortType` |
| `"i32"` / `"int"` | `Int32` | Spark `IntegerType` |
| `"i64"` / `"long"` | `Int64` | Default for tantivy integers |
| `"u64"` | `UInt64` | Unsigned integers |
| `"f32"` / `"float"` | `Float32` | Spark `FloatType` |
| `"f64"` / `"double"` | `Float64` | Default for tantivy floats |
| `"bool"` / `"boolean"` | `Boolean` | Boolean fields |
| `"utf8"` / `"string"` | `Utf8` | String fields |
| `"date"` / `"timestamp"` | `Timestamp(Microsecond)` | Date/timestamp fields |
| `"binary"` / `"bytes"` | `Binary` | Binary fields |

**Why companion splits don't need type hints:** Companion splits read from
parquet files which preserve the original Arrow types from the write path.
Type hints only affect the non-companion (tantivy doc store) streaming path.

**Rust implementation:** `tantivy_schema_to_arrow()` in `streaming_doc_retrieval.rs`
accepts an optional `HashMap<String, DataType>` which overrides the default
tantivy→Arrow type mapping per field name. `build_arrow_column()` has dedicated
builder branches for `Int32`, `Int16`, `Int8`, and `Float32` that narrow the
internal `i64`/`f64` values during construction.

### Document Limit (`maxDocs`)

By default, `startStreamingRetrieval()` retrieves **all** matching documents.
When the caller only needs a subset (e.g., Spark `LIMIT 100`), pass `maxDocs`
to truncate the result set **before** any I/O work:

```java
// Without maxDocs: searches 10K matches → prefetches 10K store ranges → retrieves 10K docs
try (var session = searcher.startStreamingRetrieval(queryJson, fields)) { ... }

// With maxDocs=100: searches 10K matches → truncates to 100 → prefetches 100 ranges → retrieves 100 docs
try (var session = searcher.startStreamingRetrieval(queryJson, fields, typeHints, 100)) { ... }
```

**Why this matters:** The bulk search (`DocIdCollector`) always finds all matches — it
has no limit concept. Without `maxDocs`, the streaming pipeline prefetches store-file
byte ranges and retrieves documents for the entire result set, even if the consumer
stops reading after a few rows. For `LIMIT 100` on a 10K-row split, this wastes
~99% of S3 requests.

The truncation happens after `perform_bulk_search()` returns and before both:
- `prefetch_store_ranges()` — range consolidation + S3 prefetch (non-companion)
- `resolve_to_parquet_locations()` — fast-field resolution (companion)

So both companion and non-companion paths benefit from the limit.

**API overloads:**

| Signature | maxDocs | typeHints |
|-----------|---------|-----------|
| `startStreamingRetrieval(query, fields...)` | unlimited | none |
| `startStreamingRetrieval(query, fields, typeHints)` | unlimited | yes |
| `startStreamingRetrieval(query, fields, typeHints, maxDocs)` | capped | yes |

Pass `maxDocs = -1` for unlimited. Pass `typeHints = null` to use default Arrow types.

### Store-File Range Prefetch (Non-Companion)

Before the streaming producer starts retrieving documents, the pipeline runs the
same range consolidation + prefetch that `docBatchProjected` uses. This prevents
each `doc_async()` call from becoming a separate S3 request.

```
doc_ids → DocAddress[] → SimpleBatchOptimizer.consolidate_ranges()
    → merge nearby .store byte ranges
    → prefetch_ranges_with_cache() → bulk S3 GET → ByteRangeCache
    → subsequent doc_async() calls hit cache
```

The prefetch runs synchronously before spawning the producer task (in
`prefetch_store_ranges()`), so all cache entries are populated before
any `doc_async()` call. This uses the same `SimpleBatchOptimizer` with
`SimpleBatchConfig::default()` — the 50-doc optimization threshold applies.

### Java API — `StreamingSession` (AutoCloseable)

`startStreamingRetrieval()` returns a `StreamingSession` wrapper that implements
`AutoCloseable`, providing double-close safety and synchronized access:

```java
// Start session — returns AutoCloseable StreamingSession
try (SplitSearcher.StreamingSession session =
        searcher.startStreamingRetrieval(queryJson, "field1", "field2")) {

    // Discover output schema width
    int numCols = session.getColumnCount();

    // Poll batches until stream ends
    while (true) {
        long[] arrayAddrs = new long[numCols];   // allocate FFI structs
        long[] schemaAddrs = new long[numCols];
        // ... initialize FFI struct memory ...

        int rows = session.nextBatch(arrayAddrs, schemaAddrs);
        if (rows == 0) break;   // end of stream
        if (rows < 0) throw new RuntimeException("streaming error");

        // Import Arrow arrays from FFI addresses
        // Process batch (e.g., pass to Spark ColumnarBatch)
    }
}  // session.close() called automatically — safe to call multiple times
```

**Return values for `nextBatch()`:**

| Value | Meaning |
|-------|---------|
| `> 0` | Number of rows in this batch |
| `0` | End of stream — no more batches |
| `-1` | Error (Java exception thrown) |

### Spark Integration Pattern

```scala
// CompanionColumnarPartitionReader.scala
class CompanionColumnarPartitionReader extends PartitionReader[ColumnarBatch] {
  private var session: SplitSearcher.StreamingSession = null

  // Start session with type hints (for non-companion type narrowing) and limit
  def open(searcher: SplitSearcher, queryJson: String,
           fields: Array[String], typeHints: Array[String], limit: Int): Unit = {
    session = searcher.startStreamingRetrieval(queryJson, fields, typeHints, limit)
  }

  override def next(): Boolean = {
    val rows = session.nextBatch(arrayAddrs, schemaAddrs)
    if (rows == 0) return false
    importBatchFromFfi(rows)
    true
  }

  override def close(): Unit = {
    if (session != null) session.close()  // safe to call multiple times
  }
}
```

---

## Stage 3: Adaptive I/O Strategy

### ReadStrategy (`read_strategy.rs`)

Selects the optimal parquet read approach per file based on selectivity
(fraction of rows needed). Minimizes S3 GET request count — the dominant
cost when data transfer is free (in-region).

```rust
pub enum ReadStrategy {
    PageLevel,           // < 5% selectivity
    CoalescedPageLevel,  // 5-25% selectivity
    FullColumnChunk,     // 25-50% selectivity
    FullRowGroup,        // > 50% selectivity
}
```

**Decision diagram:**

```
Selectivity:  0%        5%           25%           50%          100%
              ├──────────┼────────────┼─────────────┼─────────────┤
              │ PageLevel│ Coalesced  │ FullColumn  │ FullRowGroup│
              │ (surgical│  PageLevel │  Chunk      │ (bulk read) │
              │  per-page│  (1MB gap) │  (4MB gap)  │ (no row     │
              │  reads)  │            │             │  selection) │
              └──────────┴────────────┴─────────────┴─────────────┘
```

**Coalesce configuration per strategy:**

| Strategy | Max Gap | Max Total | Page Index | Row Selection |
|----------|---------|-----------|------------|---------------|
| PageLevel | 512KB (base) | 8MB (base) | Yes | Yes |
| CoalescedPageLevel | 1MB | 16MB | Yes | Yes |
| FullColumnChunk | 4MB | 32MB | No | Yes (post-decode) |
| FullRowGroup | 4MB | 64MB | No | No (filter via `take()`) |

**Public API:**

```rust
pub fn compute_selectivity(selected_rows: usize, total_rows: usize) -> f64;

impl ReadStrategy {
    pub fn for_selectivity(selectivity: f64) -> Self;
    pub fn coalesce_config(&self, base: CoalesceConfig) -> CoalesceConfig;
    pub fn use_row_selection(&self) -> bool;
    pub fn use_page_index(&self) -> bool;
}

// Per-row-group refinement for mixed-selectivity files
pub fn strategy_for_row_group(rows_in_rg: usize, rg_total_rows: usize) -> ReadStrategy;
```

**Why adaptive I/O matters:**

At low selectivity (1% of rows), page-level reads avoid downloading 99%
of the data. But at high selectivity (80% of rows), page-level reads
generate many small S3 GETs that cost more in latency and request fees
than simply reading entire column chunks. The strategy selection
automatically transitions from surgical reads to bulk reads as
selectivity increases.

**S3 request reduction by strategy** (5 columns, 10 row groups):

| Strategy | Requests per File | When |
|----------|-------------------|------|
| PageLevel | 50-100 | < 5% selectivity |
| CoalescedPageLevel | 15-30 | 5-25% |
| FullColumnChunk | 5-10 | 25-50% |
| FullRowGroup | 1-3 | > 50% |

### Integration

The streaming producer automatically applies adaptive I/O per file:

```rust
// In produce_batches():
let selectivity = compute_selectivity(num_rows_in_file, file_entry.num_rows);
let strategy = ReadStrategy::for_selectivity(selectivity);
let effective_coalesce = strategy.coalesce_config(base);
// Pass effective_coalesce to read_parquet_batches_for_file()
```

---

## Performance Debugging

All pipeline stages emit timing via `perf_println!()`, controlled by
the `TANTIVY4JAVA_DEBUG=1` environment variable:

```
⏱️ BULK_SEARCH: query parse + build took 2ms
⏱️ BULK_SEARCH: search returned 1500000 docs in 45ms (no scoring)
⏱️ BULK_RESOLVE: loaded __pq columns for 3 segments in 8ms
⏱️ BULK_RESOLVE: resolved 1500000 docs via fast fields in 12ms
⏱️ BULK_RESOLVE: grouped into 10 files in 3ms
⏱️ STREAMING: producer start — 10 files, 1500000 total docs
⏱️ STREAMING: file[0] selectivity=15.2% strategy=CoalescedPageLevel
⏱️ STREAMING: file[0] read 152000 rows in 4 batches, took 230ms
⏱️ STREAMING_JNI: nextBatch returned 131072 rows
⏱️ STREAMING: producer complete — 10 files, 1500000 rows emitted, took 1840ms
```

---

## Memory Budget

| Component | Size | Lifetime |
|-----------|------|----------|
| DocIdCollector results | 8 bytes/hit | Until resolution completes |
| Resolution groups (HashMap) | ~24 bytes/hit | Until streaming starts |
| Streaming channel | 2 × ~12MB | Duration of session |
| Per-batch FFI export | ~12MB | Until Java imports batch |
| **Peak (streaming)** | **~50MB** | **Regardless of total rows** |

For comparison, materializing 2M rows × 100 bytes with the old path would
require ~200MB of `PartialHit` protobufs plus ~200MB of document data.

---

## Memory Safety

### Session Handle Management (ARC_REGISTRY)

Streaming session handles use the `ARC_REGISTRY` pattern instead of raw
`Box::into_raw`/`Box::from_raw`. This provides:

- **No use-after-free**: Handles are registry IDs, not raw pointers. A stale
  handle returns `None` instead of dereferencing freed memory.
- **Safe double-close**: `release_arc()` is idempotent — calling it twice on
  the same handle is a no-op (the registry entry is already removed).
- **Thread safety**: The registry uses `RwLock` for concurrent access.

```rust
// Creating a session:
let handle = arc_to_jlong(Arc::new(Mutex::new(session)));

// Using a session:
let arc = jlong_to_arc::<Mutex<StreamingRetrievalSession>>(handle)
    .ok_or_else(|| anyhow!("Session not found or already closed"))?;

// Closing a session (idempotent):
release_arc(handle);
```

### Arrow FFI Buffer Lifecycle

`write_batch_to_ffi()` drops previous FFI contents before writing new ones,
preventing Arrow buffer leaks when FFI addresses are reused across batches:

```rust
unsafe {
    // Drop previous FFI contents to release Arrow buffers
    let prev_array = std::ptr::read_unaligned(array_ptr);
    drop(prev_array);
    let prev_schema = std::ptr::read_unaligned(schema_ptr);
    drop(prev_schema);
    // Write new contents
    std::ptr::write_unaligned(array_ptr, FFI_ArrowArray::new(&data));
    std::ptr::write_unaligned(schema_ptr, FFI_ArrowSchema::try_from(&field)?);
}
```

### `StreamingSession` Java Wrapper

The `StreamingSession` class wraps the raw native handle with:

- **`synchronized` methods**: Prevents concurrent `nextBatch`/`close` races
- **`volatile boolean closed`**: Double-close protection without synchronization overhead on reads
- **`AutoCloseable`**: Works with try-with-resources for automatic cleanup

---

## Fast Field Warmup

### Selective Warmup in Bulk Search

`perform_bulk_search()` warms fast fields **only for fields that need them** —
specifically range queries and field_presence/exists queries. Term, phrase, and
wildcard queries do NOT trigger fast field warmup.

```rust
// Extract only fields involved in range or field_presence queries
let fast_field_names = extract_range_query_fields(query_ast_json);

// Warm only those specific fields (not all fast fields)
if !fast_field_names.is_empty() {
    let field_names: Vec<String> = fast_field_names.into_iter().collect();
    let _ = ctx.warm_native_fast_fields_l1_for_fields(&field_names).await;
}
```

**Why selective?** Fast field warmup triggers async reads from storage (S3/Azure)
into the HotDirectory cache. Warming all fast fields is expensive and wastes
bandwidth. The `extract_range_query_fields()` function (from `field_extraction.rs`)
parses the query AST JSON for `"type": "range"` and `"type": "field_presence"`
nodes, returning only the field names that actually need fast field access.

| Query Type | Needs Fast Field Warmup? | Reason |
|------------|--------------------------|--------|
| Term | No | Uses term dictionary + postings |
| Phrase | No | Uses term dictionary + positions |
| Wildcard | No | Uses term dictionary FST |
| Range | **Yes** | Reads fast field column data |
| Exists/FieldPresence | **Yes** | Checks fast field presence |

---

## Error Handling

| Error | Cause | Recovery |
|-------|-------|----------|
| `startStreamingRetrieval` throws `IllegalStateException` | Not companion, or query parse error | Check split type / query JSON |
| `session.nextBatch()` returns `-1` | Parquet read failure, storage error | Check exception message, retry |
| `session.nextBatch()` returns `0` | Normal end of stream | Stop polling |
| `session.nextBatch()` after `close()` | Session already closed | Throws `IllegalStateException` |
| `session.close()` called twice | Already closed | No-op (safe) |
| Producer error mid-stream | S3 timeout, corrupt parquet | Error surfaced via next `nextBatch()` call |

The streaming producer communicates errors through the channel — if a file
read fails, the error is sent as `Err(...)` on the channel and surfaces
as a `-1` return (with Java exception) on the next `nextBatch()` call.

The `StreamingSession` wrapper is `AutoCloseable` and safe to call `close()`
multiple times. If the session is dropped without calling `close()` (e.g.,
exception during processing), the native `Arc` in the registry will be cleaned
up when the registry entry is eventually released. The producer detects the
closed channel on its next `tx.send()` and stops gracefully.

---

## Test Coverage

| Module | Tests | What's covered |
|--------|-------|----------------|
| `docid_collector` | 4 | Collector trait, batch collection, merge_fruits, scoring disabled |
| `streaming_doc_retrieval` | 6 | Schema mapping, projection, type hints (Int32/Float32/Int16/Int8), batch conversion |
| `streaming_ffi` | 6 | BatchAccumulator push/flush/empty, timestamp normalization, type mapping |
| `read_strategy` | 7 | Selectivity thresholds, boundary values, coalesce scaling, flags |
| **Rust Total** | **23** | All unit-testable logic |

| Java Test Class | Tests | What's covered |
|-----------------|-------|----------------|
| `LargeResultSetRetrievalTest` | ~20 | Error handling, query types (term/boolean/range/exists/wildcard), edge cases (empty results, single row), concurrency, field projection, cross-path consistency, scale, StreamingSession lifecycle |
| `ParquetCompanionTest` | 22 | End-to-end companion mode with real splits |
| `ParquetCompanionAggregationTest` | 43 | Aggregations on companion splits |

---

## Adding New Features

### Adding a new ReadStrategy tier

1. Add variant to `ReadStrategy` enum in `read_strategy.rs`
2. Update `for_selectivity()` thresholds
3. Set `coalesce_config()`, `use_row_selection()`, `use_page_index()` for the new tier
4. Add tests for the new boundary values

### Supporting per-row-group strategy selection

The `strategy_for_row_group()` function is implemented but not yet wired
into the read pipeline. To enable it:

1. In `read_parquet_batches_for_file()`, compute selectivity per row group
   instead of per file
2. Apply different coalesce configs per row group fetch
3. For `FullRowGroup` row groups, skip `RowSelection` construction

### Adding new projected field types

Column type mapping is in `streaming_ffi::parquet_type_to_arrow_type()`.
Add new `tantivy_type` or `parquet_type` matches there. The actual data
types come from parquet file metadata at read time — this mapping is only
for schema construction.
