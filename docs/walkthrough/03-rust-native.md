# 03 — Rust Native Walkthrough

Every module under `native/src/`. Grouped by responsibility rather than alphabetized — modules in the same group cooperate closely. For each module: **Purpose / Key types / Relationships**.

The Rust crate is structured around a few hard rules:

- **JNI bridge files are thin.** Any file or submodule named `jni_*` only translates Java types ↔ Rust types and looks up `Arc`s from the registry. Real logic lives in sibling files.
- **Object lifetime goes through `utils.rs`.** No raw `Box::from_raw`. Every Java handle is an `Arc` registered in `ARC_REGISTRY`, looked up via `with_arc_safe`.
- **Async work goes through `runtime_manager.rs`.** A single global Tokio runtime — no per-call runtimes, no `block_on` from inside Tokio context.
- **Caching is layered, not feature-flagged.** L1 (`global_cache/`), L2 (`disk_cache/`), and L3 (remote) all sit behind one `Storage` interface (`persistent_cache_storage.rs`).

## Crate root and foundations

### `lib.rs`
**Purpose:** Crate root. Declares all submodules and exports the few `#[no_mangle]` functions that don't belong to any specific class (e.g., global config initialization).
**Key items:** module declarations; `Java_..._initializeGlobalCache`-style entry points.
**Relationships:** depends on every other module; nothing depends on it.

### `utils.rs`
**Purpose:** Foundational JNI safety. Maintains `ARC_REGISTRY` (a global map of `jlong` → `Arc<dyn Any + Send + Sync>`) so that Java handles can never become dangling. Holds `GLOBAL_JVM` for callbacks. Provides `arc_to_jlong`, `jlong_to_arc`, `with_arc_safe`, `release_arc`, `with_object_mut` (using safe `downcast_mut` — the original raw cast caused SIGSEGV during commit).
**Key items:** `ARC_REGISTRY`, `GLOBAL_JVM`, `arc_to_jlong`, `with_arc_safe`, `release_arc`.
**Relationships:** used by every JNI module. Nothing in here is module-specific — it's the contract every bridge file follows.

### `runtime_manager.rs`
**Purpose:** Singleton Tokio runtime for all async Quickwit operations. Configurable worker/download/upload concurrency. Centralizing the runtime eliminates "sync inside async" deadlocks that occur when Java threads call into Quickwit code that internally spawns tasks.
**Key types:** `QuickwitRuntimeManager`, `RuntimeConfig`.
**Relationships:** used by `quickwit_split/`, `standalone_searcher/`, `split_searcher/`, `txlog/`, `delta_reader/`, `iceberg_reader/`, `parquet_reader/`.

### `debug.rs`
**Purpose:** Conditional debug + perf logging controlled by `TANTIVY4JAVA_DEBUG` and `TANTIVY4JAVA_PERFLOG` env vars.
**Key items:** `DEBUG_ENABLED`, `PERFLOG_ENABLED`, `debug_println!` macro.
**Relationships:** used everywhere; compiles to a cheap flag check when disabled.

### `extract_helpers.rs`
**Purpose:** Shared helpers for extracting typed values from JSON / serde objects. Used by document field handling, query parsing, schema inference.
**Relationships:** foundational utility; used by `document/`, `searcher/`, `split_query/`.

### `common.rs`
**Purpose:** Shared helpers for the external table readers (Delta, Iceberg, Parquet). Consolidates string extraction, byte array conversion, HashMap traversal, and storage config building so the three reader modules don't duplicate logic.
**Key types:** `DeltaStorageConfig` (the common AWS/Azure/file storage config).
**Relationships:** used by `delta_reader/`, `iceberg_reader/`, `parquet_reader/`.

## Core index, schema, and document

### `index.rs`
**Purpose:** Tantivy `Index` and `IndexWriter` JNI bindings. Wraps in-memory and on-disk index creation, writer creation with memory limits, schema attachment. Tracks per-writer memory reservations in `WRITER_RESERVATIONS` so they can be released when the writer is dropped.
**Key types:** `TantivyIndex`, `TantivyIndexWriter`.
**Relationships:** depends on `schema/`, `memory_pool/`. Used by `searcher/` and `document/`.

### `schema/`
**Purpose:** Schema building and runtime introspection.
**Submodules:**
- `jni_builder.rs` — `SchemaBuilder` JNI: `addTextField`, `addIntegerField`, `addJsonField`, etc.
- `jni_schema.rs` — `Schema` introspection: field names, types, capability filters.

**Key types:** Tantivy's `Schema`, `FieldEntry`.
**Relationships:** foundational. Used by `index.rs`, `document/`, `searcher/`, `query/`.

### `document/`
**Purpose:** Document construction and retrieval JNI.
**Submodules:**
- `types.rs` — `DocumentBuilder`, `DocumentWrapper`, `RetrievedDocument`.
- `jni_add_fields.rs` — Adding fields to a document.
- `jni_getters.rs` — Reading fields from a retrieved document.
- `helpers.rs` — Date conversions, Java↔Rust value bridging.

**Relationships:** depends on `schema/`. Used by `searcher/`, `batch_retrieval/`.

### `text_analyzer.rs`
**Purpose:** Tokenizer JNI. Wraps `SimpleTokenizer`, `WhitespaceTokenizer`, `RawTokenizer` plus optional lowercase and length filtering. Constants `DEFAULT_MAX_TOKEN_LENGTH=255` and `LEGACY_MAX_TOKEN_LENGTH=40`.
**Key types:** `TantivyAnalyzer`.
**Relationships:** used by `schema/` field builders for fast-field tokenization.

## Query path

### `query/`
**Purpose:** JNI for the core Tantivy query types (used by `core.Searcher`, not by `SplitSearcher`).
**Submodules:**
- `jni_core.rs` — Term, boolean, range queries.
- `jni_advanced.rs` — Phrase, fuzzy, regex, wildcard, boost, const_score.
- `json_query.rs` — JSON field queries (term, range, exists with dot-notation paths).
- `snippet.rs` — `Snippet` and `SnippetGenerator`.
- `exists_query.rs` — Field-presence queries.
- `wildcard_*` helpers — Multi-segment wildcard expansion (`*Wild*Joe*Hick*`).

**Relationships:** depends on `schema/`. Sibling to `split_query/` (which handles split-specific queries).

### `split_query/`
**Purpose:** Native conversion and optimization of split queries — the part of the system that takes Java `SplitQuery` objects and turns them into Quickwit `QueryAst` for execution. Includes cost analysis and rewriting.
**Submodules:**
- `parse_query.rs` — String → AST.
- `query_converters.rs` — Java ↔ Quickwit AST.
- `ip_rewriter.rs` — CIDR / IP wildcard expansion (uses `ip_expansion.rs`).
- `schema_cache.rs` — Caches per-split schemas to avoid re-fetching.
- `wildcard_analysis.rs` — Detects expensive wildcard patterns.
- `query_optimizer.rs` — Cost analysis, smart wildcard stats.

**Key types:** `QueryAst`, `QueryAnalysis`, `QueryCost`.
**Relationships:** depends on Quickwit query libraries. Used by `split_searcher/` and `standalone_searcher/`.

### `ip_expansion.rs`
**Purpose:** Expands CIDR ranges and IP wildcards into disjunctive term queries (e.g., `10.0.0.0/8` → list of terms). Standalone so it can be unit-tested without query infrastructure.
**Key types:** `IpExpander`.
**Relationships:** used by `split_query/ip_rewriter.rs`.

### `test_query_parser.rs`
**Purpose:** Query parser test fixtures. Not part of the runtime path.

## Searcher layer

### `searcher/`
**Purpose:** In-memory search orchestration and the entire aggregation pipeline.
**Submodules:**
- `batch_parsing.rs` — Parses search request payloads.
- `jni_searcher.rs` — JNI entry points for `core.Searcher`.
- `jni_index_writer.rs` — JNI entry points for `core.IndexWriter` operations that need a searcher (e.g., merge metadata).
- `aggregation/` — One submodule per aggregation type (sum, avg, min, max, count, stats, cardinality, terms, range, histogram, date_histogram, multi_terms).

**Key types:** `SearchResult`, `AggregationResult`.
**Relationships:** depends on `query/`, `document/`, `standalone_searcher/`. Bridges to `split_searcher/` for split-specific workloads.

### `standalone_searcher/`
**Purpose:** A clean Quickwit split searcher that does not require a `SplitCacheManager`. Used directly for one-off searches and as the inner engine of `split_searcher/`.
**Submodules:**
- `searcher.rs` — Main logic: warmup, timeout, resource limits.
- `jni.rs` — JNI bridge.

**Key types:** `StandaloneSearcher`, `SearchResult`.
**Relationships:** depends on `global_cache/`, `persistent_cache_storage.rs`, `disk_cache/`. Used by `split_searcher/`, `split_cache_manager/`.

### `split_searcher/`
**Purpose:** The main workhorse for distributed search. Wraps `StandaloneSearcher` with cache integration, batch operations, metrics, prewarming, and FFI profiling. ~24 files, ~10K lines — the largest single module.
**Notable submodules:** `jni_search.rs`, `jni_lifecycle.rs`, `jni_metadata.rs`, `jni_aggregation.rs`, `jni_batch.rs`, `jni_prewarm.rs`, `jni_cache.rs`, plus pure-Rust supporting files.
**Key types:** `EnhancedSearchResult`, `SplitSearchMetadata`.
**Relationships:** depends on `standalone_searcher/`, `split_query/`, `split_cache_manager/`, `batch_retrieval/`, `prewarm/`, `parquet_companion/`.

### `split_cache_manager/`
**Purpose:** Java-facing cache lifecycle. Tracks `GlobalSplitCacheManager` instances by name, validates that duplicate names have consistent configs, exposes per-manager metrics.
**Submodules:**
- `manager.rs` — `GlobalSplitCacheManager` state.
- `jni_lifecycle.rs` — Create/close cache managers.
- `jni_cache_ops.rs` — Stats, preload, eviction commands.
- `jni_metrics.rs` — All metrics export.

**Key types:** `GlobalSplitCacheManager`, `GlobalCacheStats`, `BATCH_METRICS`.
**Relationships:** depends on `standalone_searcher/`, `global_cache/`, `disk_cache/`.

## Caching and storage

### `global_cache/`
**Purpose:** L1 (in-memory) cache and Quickwit search component integration.
**Submodules:**
- `components.rs` — `GlobalSearcherComponents` wrapping a Quickwit searcher with bounded LRU cache for `Searcher` objects (default 1000; replaced an unbounded HashMap that caused OOMs in long-running deployments).
- `config.rs` — `GlobalCacheConfig` (cache sizes, concurrency, AWS/Azure credentials, region).
- `l1_cache.rs` — In-memory hot byte ranges and decoded fast fields.
- `metrics.rs` — Storage download tracking.
- `storage_resolver.rs` — Caches URI → Storage instance mapping so we don't reconstruct the S3 client per call.

**Key types:** `GlobalSearcherComponents`, `GlobalCacheConfig`.
**Relationships:** depends on `disk_cache/`, `persistent_cache_storage.rs`, `runtime_manager.rs`. Used by `standalone_searcher/`, `split_searcher/`, `split_cache_manager/`.

### `disk_cache/`
**Purpose:** L2 persistent disk cache. Survives JVM restarts; LZ4/Zstd compression; LRU eviction at ~95% capacity; manifest with crash recovery; async background writer so searches are never blocked on disk.
**Submodules:**
- `types.rs` — Config, compression algorithms.
- `range_index.rs` — Overlap queries (find cached subranges of a requested range).
- `manifest.rs` — Persistent manifest with split-level granularity.
- `lru.rs` — Eviction policy.
- `mmap_cache.rs` — File memory mapping.
- `background.rs` — Async writer thread.
- `compression.rs` — LZ4/Zstd codecs.
- `get_ops.rs`, `write_ops.rs`, `path_helpers.rs` — Read/write/path utilities.

**Key types:** `L2DiskCache`, `CompressionAlgorithm`, `CacheManifest`.
**Relationships:** depends on `memory_pool/` (for budget tracking), `global_cache/`. Used by `persistent_cache_storage.rs`, `split_cache_manager/`.

### `persistent_cache_storage.rs`
**Purpose:** The tiered storage wrapper. Implements Quickwit's `Storage` trait by chaining L1 (in-memory) → L2 (disk) → L3 (remote). Adds **range coalescing** to combine nearby reads into one network request.
**Key types:** `TieredCacheStats`.
**Relationships:** depends on `disk_cache/`, `global_cache/`. Used by `standalone_searcher/`, `split_searcher/`. Wraps Quickwit `Storage` backends.

### `batch_retrieval/`
**Purpose:** Bulk document retrieval optimized for S3 cost. Two strategies — pick whichever the workload prefers.
**Submodules:**
- `simple.rs` — `SimpleBatchOptimizer`: range consolidation only.
- `optimized.rs` — `OptimizedBatchRetriever`: persistent cache + async parallel fetching.

**Key types:** `BatchRetrievalMetrics`.
**Relationships:** depends on `persistent_cache_storage.rs`, `document/`, `memory_pool/`. Used by `searcher/`, `split_searcher/`.

### `prewarm/`
**Purpose:** Proactively load index components into the disk cache before queries run, eliminating first-query cache misses.
**Submodules:**
- `all_fields.rs` — Loads all fields for a component (TERM, POSTINGS, FIELDNORM, FASTFIELD, STORE).
- `field_specific.rs` — Loads one field of one component.
- `component_sizes.rs` — Reports per-field sizes (used for capacity planning).
- `cache_extension.rs` — Helpers for extending the L2 cache during prewarm.
- `helpers.rs` — Async coordination helpers; `parse_split_uri` is used elsewhere for cache-key consistency.

**Relationships:** used by `split_searcher/jni_prewarm.rs`. See `docs/TERM_PREWARM_DEVELOPER_GUIDE.md`.

## Memory and profiling

### `memory_pool/`
**Purpose:** Unified memory accounting that coordinates Rust allocations with the JVM (e.g., Spark `TaskMemoryManager`). Watermark batching avoids per-allocation JNI callbacks.
**Submodules:**
- `pool.rs` — `MemoryPool` trait + `UnlimitedMemoryPool`.
- `reservation.rs` — `MemoryReservation` RAII guard.
- `jvm_pool.rs` — `JvmMemoryPool` (calls back to Java for limit enforcement).
- `jni_bridge.rs` — Java callbacks.
- `disk_cache_budget.rs` — L2 disk cache allocation tracking.

**Key types:** `MemoryPool`, `JvmMemoryPool`, `MemoryReservation`, `UnlimitedMemoryPool`.
**Relationships:** foundational. Used by `index.rs` (writer heap), `global_cache/`, `disk_cache/`, `batch_retrieval/`. See `docs/UNIFIED_MEMORY_MANAGEMENT_DESIGN.md`.

### `ffi_profiler.rs`
**Purpose:** Near-zero-overhead FFI read-path profiler. Records timings without inflating latency.
**Key types:** `FfiProfiler`.
**Relationships:** standalone instrumentation; used wherever performance diagnostics matter.

### `ffi_profiler_jni.rs`
**Purpose:** JNI bridge for `split.FfiProfiler`. Exposes the profiler controls and snapshots to Java.

## Split merge and parquet companion

### `quickwit_split/`
**Purpose:** Quickwit split merge operations — the heaviest single subsystem (15 submodules, ~3.5K lines). Handles split downloading, merging, uploading, JSON discovery, merge registries, temp directory management, resilient operation under failure. Generates split metadata and handles hot-cache optimization. Used in process-isolated mode for parallel merges (each merge runs in a separate Rust binary process to escape Tokio runtime contention).
**Key types:** `SplitMetadata`, `MergeSplitConfig`, `MergeAwsConfig`.
**Relationships:** depends on `runtime_manager/`, `utils/`, `global_cache/`. Bridges Quickwit merge APIs.

### `parquet_companion/`
**Purpose:** Parquet Companion mode. Splits reference external Parquet files for stored fields and fast fields instead of duplicating the data inside the split — reducing split size by 80–90%. Includes transcode pipelines, Arrow FFI import/export, field extraction, string indexing, docid mapping, and L2 caching of transcoded fast fields.
**Key types:** `ParquetManifest`, `FastFieldMode`.
**Relationships:** depends on `parquet_reader/` and `disk_cache/`. Used by `split_searcher/` when a split is in companion mode. See `docs/PARQUET_COMPANION_DEVELOPER_GUIDE.md`.

## External table readers (parallel subsystem)

These three modules don't touch the search path. They exist so JVM applications can list files and extract schemas from external tables without pulling in JVM Delta/Iceberg clients.

### `delta_reader/`
**Purpose:** Delta Lake table file listing via `delta-kernel-rs`.
**Submodules:** `engine` (`DeltaStorageConfig`), `scan` (file listing, schema reading), `serialization` (binary protocol to Java), `jni`, `distributed` (snapshot info aggregation).
**Key types:** `DeltaFileEntry`, `DeltaSchemaField`, `DeltaStorageConfig`.
**Relationships:** depends on `common.rs`. Independent of search path.

### `iceberg_reader/`
**Purpose:** Apache Iceberg table reading via `iceberg-rust`.
**Submodules:** `catalog` (REST/Glue/HMS), `scan` (file listing, snapshots), `serialization`, `jni`, `distributed` (manifest aggregation).
**Key types:** `IcebergFileEntry`, `IcebergSnapshot`.
**Relationships:** depends on `common.rs`. Independent of search path.

### `parquet_reader/`
**Purpose:** Hive-style partitioned Parquet directory listing.
**Submodules:** `distributed` (file listing), `serialization`, `jni`.
**Key types:** `ParquetTableInfo`, `ParquetFileEntry`.
**Relationships:** depends on `common.rs`. Used by `parquet_companion/` for the underlying file enumeration when companion mode is active.

### `parquet_schema_reader.rs`
**Purpose:** Standalone schema extraction from a single Parquet file. Reads footer metadata to derive a schema.
**Relationships:** used for schema inference (companion mode and standalone Parquet listing).

## Transaction log

### `txlog/`
**Purpose:** Indextables transaction log v4 (Avro-based). Thirty submodules covering actions, Avro serialization, caching, compression, distribution, garbage collection, JNI bridge, file listing, log replay, metrics, partition pruning, purge, schema dedup, storage, streaming, tombstone distribution, version files. Backward-compatible with prior log versions.
**Key types:** `LogAction`, `TransactionLogEntry`.
**Relationships:** independent subsystem. Uses `runtime_manager/` for async I/O.

## Quick lookup: "where is the Rust for…"

| Concern                                  | Module                                            |
| ---------------------------------------- | ------------------------------------------------- |
| `Index` / `IndexWriter` JNI              | `index.rs`                                        |
| Schema build + introspection             | `schema/`                                         |
| Document JNI                             | `document/`                                       |
| Tantivy query types                      | `query/`                                          |
| Split query AST + optimization           | `split_query/`                                    |
| Aggregations                             | `searcher/aggregation/`                           |
| In-memory search                         | `searcher/`                                       |
| Split search (production)                | `split_searcher/`                                 |
| Cache lifecycle (Java-facing)            | `split_cache_manager/`                            |
| L1 in-memory cache                       | `global_cache/`                                   |
| L2 disk cache                            | `disk_cache/`                                     |
| Tiered storage wrapper                   | `persistent_cache_storage.rs`                     |
| Bulk doc fetching                        | `batch_retrieval/`                                |
| Component prewarm                        | `prewarm/`                                        |
| Memory accounting                        | `memory_pool/`                                    |
| Async runtime                            | `runtime_manager.rs`                              |
| JNI safety / object lifetime             | `utils.rs`                                        |
| Split merge                              | `quickwit_split/`                                 |
| Parquet companion mode                   | `parquet_companion/`                              |
| Delta / Iceberg / Parquet table listing  | `delta_reader/` / `iceberg_reader/` / `parquet_reader/` |
| Transaction log                          | `txlog/`                                          |
| FFI profiler                             | `ffi_profiler.rs` + `ffi_profiler_jni.rs`         |
| CIDR / IP wildcard expansion             | `ip_expansion.rs`                                 |
