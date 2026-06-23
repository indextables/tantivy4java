# 02 — Java API Walkthrough

Every package under `src/main/java/io/indextables/tantivy4java/`, in alphabetical order. Each entry covers what the package is for, its key types, and which native Rust module it bridges to.

The Java side is intentionally thin: it owns no real data. Each Java object holds a `long` native handle and forwards method calls to JNI functions implemented in `native/src/`. When a Java object is closed (or garbage collected — most types implement `AutoCloseable`), the matching native `Arc` is released through `utils.rs`.

## User-facing packages

These are the packages that consumers of the library import directly.

### `core/` — Primary API

The classic Tantivy API. If you're building an in-memory or local-disk index, you only need `core` + `query` + `result`.

| Class                | Purpose                                                                 |
| -------------------- | ----------------------------------------------------------------------- |
| `Tantivy`            | Static initializer; loads the native library and reports version.       |
| `Index`              | Index lifecycle: create in-memory or open on disk; produce IndexWriter and Searcher. Holds the `Index.Memory.*` constants (MIN/DEFAULT/LARGE/XL heap sizes). |
| `IndexWriter`        | Bulk document ingestion. Heap-bounded; commits/rollbacks; segment merge API. |
| `Searcher`           | Query execution, document retrieval, segment listing, aggregations.     |
| `Schema`             | Built schema; field introspection (`getFieldNames`, `hasField`, capability filters). |
| `SchemaBuilder`      | Builder for `Schema` — `addTextField`, `addIntegerField`, `addJsonField`, etc. |
| `Field`, `FieldType`, `FieldInfo`, `TextFieldIndexing` | Field metadata and configuration objects. |
| `Document`, `MapBackedDocument`, `DocumentView` | Document construction and field access. |
| `DocAddress`         | Opaque (segment_ord, doc_id) reference returned by hits.                |

**Bridges to:** `native/src/index.rs`, `native/src/schema/`, `native/src/document/`, `native/src/searcher/`.

### `query/` — Query builders

Type-safe constructors for every Tantivy query type. The `Query` base class is `AutoCloseable` — each query holds a native handle.

Includes term, phrase, boolean (MUST/SHOULD/MUST_NOT via `Occur`), range, fuzzy, regex, wildcard, exists, JSON, match-all, boost, and const-score queries. `Snippet` and `SnippetGenerator` produce highlighted excerpts; `Explanation` describes how a score was computed; `Range` and `Order` are filter/sort helpers.

**Bridges to:** `native/src/query/`. The split-specific equivalents live in the `split` package and bridge to `native/src/split_query/`.

### `result/` — Result containers

`SearchResult` is what `Searcher.search()` and `SplitSearcher.search()` return. It holds the scored hits (each pointing at a `DocAddress`), aggregation results, and batch retrieval handles. Aggregation results are not deserialized eagerly — you ask `SearchResult` for the named aggregation and the matching `aggregation.*Result` class lazily decodes the native payload.

### `aggregation/` — Aggregations

Mirrors the Elasticsearch metric/bucket model. Each aggregation type comes as a pair: a builder (`SumAggregation`) and a result (`SumAggregationResult`).

- **Metric:** `SumAggregation`, `AverageAggregation`, `MinAggregation`, `MaxAggregation`, `CountAggregation`, `StatsAggregation`, `CardinalityAggregation`.
- **Bucket:** `TermsAggregation`, `RangeAggregation`, `HistogramAggregation`, `DateHistogramAggregation`, `MultiTermsAggregation`. Bucket aggregations support sub-aggregations.

`AggregationResult` is the common base. Note: `DateHistogramAggregation` requires `setFixedInterval(...)` after construction; the constructor only takes name/field.

**Bridges to:** `native/src/searcher/aggregation/`.

### `split/` — Distributed split search

This is the production API. It is bigger and more configurable than `core` because it deals with remote storage, shared caches, and per-query optimization.

| Class                                                | Purpose                                                                 |
| ---------------------------------------------------- | ----------------------------------------------------------------------- |
| `SplitCacheManager`                                  | Global per-name cache instance. Holds AWS/Azure credentials, L1+L2 sizes, tiered disk cache config. Use `getInstance(CacheConfig)` to obtain — duplicate names with conflicting configs are rejected. Creates `SplitSearcher`s that share its caches. |
| `SplitCacheManager.CacheConfig`                      | Builder for the manager: max cache size, AWS/Azure credentials, region, custom endpoint, tiered (disk) cache settings, parquet companion table root. |
| `SplitCacheManager.TieredCacheConfig`                | L2 disk cache config: path, max size, compression algorithm.            |
| `SplitCacheManager.SearcherCacheStats`               | Statistics for the bounded LRU searcher cache (hits/misses/evictions).  |
| `SplitSearcher`                                      | Searcher for one split URL (file://, s3://, azure://). Provides search, batch search, document retrieval, prewarming, schema introspection, per-component cache stats. |
| `SplitSearcher.IndexComponent` (enum)                | TERM, POSTINGS, FIELDNORM, FASTFIELD, STORE — passed to `preloadComponents` and `preloadFields`. |
| `SplitSearcher.CacheStats`                           | Per-component hit/miss/eviction counts.                                 |
| `SplitQuery` (base) + `SplitTermQuery`, `SplitBooleanQuery`, `SplitPhraseQuery`, `SplitRangeQuery`, `SplitWildcardQuery`, `SplitExistsQuery`, `SplitMatchAllQuery` | Split-optimized query types. These convert to Quickwit `QueryAst` natively, allowing optimizations (CIDR expansion, wildcard cost analysis) that the Tantivy `query/` types don't get. |
| `SplitParsedQuery`                                   | A pre-parsed query handle, reusable across searches.                    |
| `SplitAggregation`                                   | Split-specific aggregation request wrapper.                             |
| `AdaptiveTuning`, `AdaptiveTuningConfig`, `AdaptiveTuningStats` | Cost-based optimizer that adjusts batch sizes and prefetch behavior at runtime. |
| `BatchOptimizationConfig`, `BatchOptimizationMetrics` | Tuning knobs for `batch_retrieval/` and metrics from it.                |
| `ParquetCompanionConfig`                             | Configuration for parquet-companion-mode splits (external storage refs). |
| `S3CostAnalyzer`                                     | Estimates S3 GET/LIST costs for a planned operation.                    |
| `ColumnStatistics`                                   | Per-column metadata exposed via `SplitMetadata`.                        |
| `FfiProfiler`                                        | Java-side controls for the native FFI profiler.                         |

**Bridges to:** `native/src/split_searcher/`, `native/src/split_query/`, `native/src/split_cache_manager/`, `native/src/parquet_companion/`.

### `batch/` — Bulk document retrieval

| Class                  | Purpose                                                       |
| ---------------------- | ------------------------------------------------------------- |
| `BatchDocumentReader`  | Decodes a marshaled byte buffer of multiple documents.        |
| `BatchDocumentBuilder` | Constructs a batch on the Java side.                          |
| `BatchDocument`        | One document inside a batch.                                  |

These exist so that retrieving N documents costs one JNI call instead of N. Used by `SplitSearcher` and `Searcher` when you ask for many documents at once.

**Bridges to:** `native/src/batch_retrieval/`.

### `delta/` — Delta Lake table discovery

Listing files and reading schemas from a Delta Lake table without pulling in delta-spark or any JVM Delta client.

| Class                                          | Purpose                                       |
| ---------------------------------------------- | --------------------------------------------- |
| `DeltaTableReader`                             | Entry point. Lists files at a snapshot.       |
| `DeltaTableSchema`, `DeltaSchemaField`         | Extracted Delta schema.                       |
| `DeltaFileEntry`                               | A file in the snapshot (path, size, partition values). |
| `DeltaSnapshotInfo`, `DeltaLogChanges`         | Snapshot history and log diffs.               |

**Bridges to:** `native/src/delta_reader/`, which uses `delta-kernel-rs`.

### `iceberg/` — Iceberg table discovery

Same shape as `delta/` but for Apache Iceberg.

| Class                                          | Purpose                                       |
| ---------------------------------------------- | --------------------------------------------- |
| `IcebergTableReader`                           | Entry point. Lists files via REST/Glue/HMS catalogs. |
| `IcebergTableSchema`, `IcebergSchemaField`     | Extracted Iceberg schema.                     |
| `IcebergFileEntry`                             | A data file in a snapshot.                    |
| `IcebergSnapshot`, `IcebergSnapshotInfo`       | Snapshot metadata.                            |

**Bridges to:** `native/src/iceberg_reader/`, which uses `iceberg-rust`.

### `parquet/` — Standalone Parquet discovery

For Hive-style partitioned Parquet directories that aren't backed by a Delta or Iceberg metadata layer.

| Class                  | Purpose                                                |
| ---------------------- | ------------------------------------------------------ |
| `ParquetTableReader`   | Lists Parquet files under a partitioned root.          |
| `ParquetTableInfo`     | Discovered table layout.                               |
| `ParquetSchemaReader`  | Reads schema from a single Parquet file footer.        |

**Bridges to:** `native/src/parquet_reader/` and `native/src/parquet_schema_reader.rs`.

### `util/` — Helpers

| Class          | Purpose                                                              |
| -------------- | -------------------------------------------------------------------- |
| `TextAnalyzer` | Tokenization wrapper (SimpleTokenizer, WhitespaceTokenizer, RawTokenizer + lowercase/length filters). |
| `Facet`        | Hierarchical field values (Lucene-style facets).                     |

**Bridges to:** `native/src/text_analyzer.rs`.

### `filter/` — Partition filtering

| Class             | Purpose                                                          |
| ----------------- | ---------------------------------------------------------------- |
| `PartitionFilter` | Builds a partition pruning predicate used when scanning splits or external tables. |

### `examples/` — Reference programs

`BasicExample`, `QuickwitIndexExample`, `QuickwitSplitExample`, `QuickwitSplitFromPathTest`, `FileSystemRootExample`. Working programs that demonstrate the major APIs. Useful as a "how do I…" reference.

## Internal / configuration packages

These are still public Java types but are typically configured once at startup rather than used in the hot path.

### `config/` — Global configuration

| Class                 | Purpose                                                              |
| --------------------- | -------------------------------------------------------------------- |
| `GlobalCacheConfig`   | Builder for the process-global cache: max sizes, concurrency, warmup memory. Applied via `Tantivy.initialize` or implicitly on first use. |
| `RuntimeManager`      | Configures the Tokio runtime: worker threads, download/upload concurrency. |
| `FileSystemConfig`    | Resolves base paths for index/cache directories.                     |

**Bridges to:** `native/src/global_cache/config.rs`, `native/src/runtime_manager.rs`.

### `memory/` — Memory accounting

| Class                                    | Purpose                                                                  |
| ---------------------------------------- | ------------------------------------------------------------------------ |
| `NativeMemoryManager`                    | Singleton entry point for the native memory pool.                        |
| `NativeMemoryAccountant` (interface)     | Strategy for limiting/recording native allocations. Spark integrations implement this against `TaskMemoryManager`. |
| `UnlimitedMemoryAccountant`              | Default no-op implementation.                                            |
| `NativeMemoryStats`                      | Per-category breakdown (writer heap, L1 cache, L2 disk, batch retrieval, …). |

**Bridges to:** `native/src/memory_pool/`. See `docs/UNIFIED_MEMORY_MANAGEMENT_DESIGN.md` for the design.

## Quick lookup: "where is the Java for…"

| Feature                                 | Package      |
| --------------------------------------- | ------------ |
| Build a schema                          | `core`       |
| Index documents                         | `core`       |
| Search a local index                    | `core`       |
| Search a Quickwit split                 | `split`      |
| Configure shared caching                | `split` (`SplitCacheManager.CacheConfig`) + `config` |
| Build a query                           | `query` or `split` |
| Run an aggregation                      | `aggregation` |
| Bulk-retrieve documents                 | `batch`      |
| List files in a Delta table             | `delta`      |
| List files in an Iceberg table          | `iceberg`    |
| List files in a Parquet table           | `parquet`    |
| Constrain native memory                 | `memory`     |
| Configure the global runtime            | `config`     |
