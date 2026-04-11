# 01 — Architecture

Tantivy4java is a JNI binding that exposes [Tantivy](https://github.com/quickwit-oss/tantivy) and [Quickwit](https://github.com/quickwit-oss/quickwit) to Java/JVM applications. It is built around two complementary use cases:

1. **In-memory or local-disk indexing and search** (the classic Tantivy API), used by tests and embedded scenarios.
2. **Distributed search over Quickwit "splits"** stored on S3, Azure, or local disk, with multi-tier caching, parallel merge, and external table integrations (Delta, Iceberg, Parquet).

The codebase is divided cleanly between **Java** (the public API and a thin JNI shim) and **Rust** (the heavy lifting: Tantivy/Quickwit integration, caching, async I/O, memory accounting). The Java side stores no real state — all index handles, searchers, and caches live in Rust as `Arc`-managed objects, referenced from Java by `jlong` pointers through a registry that prevents use-after-free.

## The layered model

```
┌─────────────────────────────────────────────────────────────────┐
│  Layer 1 — User API (Java)                                       │
│  core.* · query.* · result.* · aggregation.* · split.*           │
│  delta.* · iceberg.* · parquet.* · batch.*                       │
└──────────────────────────────┬──────────────────────────────────┘
                               │ JNI calls (jlong handles)
┌──────────────────────────────▼──────────────────────────────────┐
│  Layer 2 — JNI Bridge (Rust)                                     │
│  index.rs · schema/ · document/ · query/ · searcher/jni_*        │
│  split_searcher/jni_* · split_cache_manager/jni_*                │
│  delta_reader/jni · iceberg_reader/jni · parquet_reader/jni      │
└──────────────────────────────┬──────────────────────────────────┘
                               │ Pure Rust calls
┌──────────────────────────────▼──────────────────────────────────┐
│  Layer 3 — Search Engine (Rust)                                  │
│  searcher/ aggregations · standalone_searcher/ · split_query/    │
│  prewarm/ · batch_retrieval/                                     │
└──────────────────────────────┬──────────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────────┐
│  Layer 4 — Caching & Storage (Rust)                              │
│  persistent_cache_storage.rs (tiered wrapper)                    │
│  global_cache/ (L1 + Quickwit components)                        │
│  disk_cache/ (L2, LZ4/Zstd, range coalescing)                    │
│  parquet_companion/ (external storage refs)                      │
└──────────────────────────────┬──────────────────────────────────┘
                               │ Quickwit Storage trait
┌──────────────────────────────▼──────────────────────────────────┐
│  Layer 5 — Remote storage (S3 / Azure / file://)                 │
└─────────────────────────────────────────────────────────────────┘

       ┌────────────────────────────┐    ┌────────────────────────┐
       │  Memory & Runtime           │    │  External tables        │
       │  memory_pool/ · utils.rs    │    │  delta_reader/          │
       │  runtime_manager.rs         │    │  iceberg_reader/        │
       │  ffi_profiler*              │    │  parquet_reader/        │
       │                             │    │  txlog/                 │
       │  cross-cutting; touch every │    │  parallel to search;    │
       │  layer                      │    │  share common.rs        │
       └────────────────────────────┘    └────────────────────────┘
```

### Layer 1 — User API (Java)

Everything under `src/main/java/io/indextables/tantivy4java/`. Two distinct entry points:

- `core.Index` + `core.Searcher` — local Tantivy index, in-memory or on disk. Used by simple programs and tests.
- `split.SplitCacheManager` + `split.SplitSearcher` — distributed search over Quickwit splits with shared multi-tier caching. The production path.

Both share `query.*`, `result.*`, `aggregation.*`, and `core.Schema/Document/Field`. Configuration (`config.GlobalCacheConfig`, `memory.NativeMemoryManager`) is one-time setup applied before any searcher is created.

### Layer 2 — JNI Bridge (Rust)

Each Java class with native methods has a corresponding Rust module that contains `#[no_mangle] extern "C"` functions. These bridge functions are intentionally thin: they validate arguments, look up the relevant `Arc<...>` from the registry in `utils.rs`, call into pure Rust code, and translate results back. The split-specific code follows a `jni_*.rs` naming convention (e.g., `split_searcher/jni_search.rs`, `split_cache_manager/jni_lifecycle.rs`).

### Layer 3 — Search Engine

Pure Rust logic that wraps Tantivy and Quickwit:

- `searcher/` handles in-memory searching and the entire aggregation pipeline (sum, avg, min, max, count, stats, cardinality, terms, range, histogram, date_histogram, multi-terms).
- `standalone_searcher/` provides a clean Quickwit split searcher decoupled from any cache manager — used directly for one-off searches and as the inner engine of `split_searcher/`.
- `split_searcher/` is the main workhorse for distributed search. Adds caching, batch retrieval, metrics, prewarming, profiling.
- `split_query/` parses, converts, optimizes, and rewrites queries before they reach the searcher (CIDR expansion, wildcard cost analysis, schema caching).
- `prewarm/` and `batch_retrieval/` are explicit performance modules that load data ahead of demand and consolidate document fetches respectively.

### Layer 4 — Caching & Storage

Three cache tiers wrap the underlying Quickwit `Storage` trait:

- **L1 — In-memory** (`global_cache/`): hot byte ranges, decoded fast fields, Quickwit `Searcher` objects (LRU-bounded).
- **L2 — On-disk** (`disk_cache/`): persistent across JVM restarts, LZ4/Zstd compression, LRU eviction, manifest with crash recovery, async background writer.
- **L3 — Remote** (S3, Azure, file://): the actual Quickwit storage backends.

`persistent_cache_storage.rs` is the tiered wrapper that exposes a single `Storage` interface and handles L1→L2→L3 fall-through with **range coalescing** (combining nearby reads to reduce request count).

`parquet_companion/` is a special case: instead of duplicating doc data into the split, it stores references to external Parquet files for stored fields and fast fields. This reduces split size by 80–90% at the cost of an extra layer of fetches.

### Layer 5 — Remote storage

Provided by Quickwit's storage abstraction. Configured through `config.GlobalCacheConfig` (AWS keys, Azure auth, custom endpoints) and the per-call options on `SplitCacheManager.CacheConfig`.

## Cross-cutting concerns

### Memory accounting (`memory_pool/`)

Native allocations are reported back to the JVM through `NativeMemoryAccountant` so that Spark `TaskMemoryManager` (or any other JVM memory governor) sees a consistent picture. The pool uses **watermark batching** — small allocations don't generate JNI callbacks; only crossing a threshold does. RAII `MemoryReservation` guards ensure release on drop. The IndexWriter heap (`Index.Memory.*` constants) is reserved through this pool.

### Async runtime (`runtime_manager.rs`)

A single global Tokio runtime owned by `QuickwitRuntimeManager`, configurable for worker, download, and upload concurrency. All Quickwit operations are async; this runtime is what executes them. Having one runtime avoids the "sync inside async" deadlocks that occur when Java threads block on Quickwit calls that internally spawn tasks.

### Object lifetime (`utils.rs`)

JNI requires that Java's `jlong` handles never become dangling. `utils.rs` maintains a global registry of `Arc<dyn Any + Send + Sync>` keyed by pointer value. `arc_to_jlong` clones into the registry, `with_arc_safe` looks up and downcasts safely, `release_arc` removes. This replaces the original `Box::from_raw` pattern that caused double-frees during AWS SDK shutdown.

### Profiling (`ffi_profiler.rs`)

A near-zero-overhead instrument that records FFI read-path timings without inflating latency. Toggled via `TANTIVY4JAVA_PERFLOG=1`. Java-side controls live in `split.FfiProfiler`.

### Debug logging (`debug.rs`)

`debug_println!` macro and `DEBUG_ENABLED` flag, controlled by `TANTIVY4JAVA_DEBUG=1`. Used everywhere — search, cache, JNI bridge — for diagnostic output. Disabled by default; the macro compiles to a no-op check so production builds pay nothing.

## Two parallel subsystems

Two large bodies of code live alongside the search path but are mostly independent of it:

- **External table readers** (`delta_reader/`, `iceberg_reader/`, `parquet_reader/`, `parquet_schema_reader.rs`) — these discover files and schemas from Delta, Iceberg, and Hive-partitioned Parquet tables. They share helpers in `common.rs` (string extraction, storage config building) but otherwise don't touch the Tantivy/Quickwit search path. They exist so that Java integrations (e.g., Spark connectors) can list table files and extract schemas without going through a JVM Delta/Iceberg client.

- **Transaction log** (`txlog/`) — a 30-submodule implementation of the Indextables transaction log v4 (Avro-based, schema dedup, garbage collection, partition pruning, distributed). Independent of search; used to track table state.

- **Quickwit split merge** (`quickwit_split/`) — operations to merge multiple splits into one. Lives apart from the read path because merges have very different concurrency and memory characteristics. Run as a separate Rust binary in some workloads (process-based parallel merge) to escape Tokio runtime contention.

## Where to look for what

| If you're working on…                          | Start here                                              |
| ---------------------------------------------- | ------------------------------------------------------- |
| A new query type                               | `query/` (Java) + `query/` and `split_query/` (Rust)    |
| A new aggregation                              | `aggregation/` (Java) + `searcher/aggregation/` (Rust)  |
| Changing what gets cached                      | `global_cache/` and `disk_cache/`                       |
| A new storage backend                          | Quickwit upstream + `persistent_cache_storage.rs`       |
| Document retrieval performance                 | `batch_retrieval/` and `prewarm/`                       |
| JVM ↔ native memory                            | `memory_pool/` and `memory/`                            |
| Listing files in an external table             | `delta_reader/` / `iceberg_reader/` / `parquet_reader/` |
| Merging splits                                 | `quickwit_split/`                                       |
| The transaction log                            | `txlog/`                                                |
| Adding a JNI method                            | The matching `jni_*.rs` file in the relevant Rust dir   |

The next two documents (`02-java-api.md`, `03-rust-native.md`) walk through every package and module in detail.
