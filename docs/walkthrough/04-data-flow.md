# 04 — Data Flow

How requests actually travel through the layers described in `01-architecture.md`. Each scenario traces a single user-visible operation through both Java and Rust modules so you can see how the pieces from `02` and `03` connect in practice.

Notation: `java:foo.Bar` means the Java class, `rust:foo/bar.rs` means the Rust module.

## Scenario 1 — Searching a Quickwit split (the production hot path)

The most important path. A Spark task or service receives a query and runs it against a split file on S3.

```
java:split.SplitSearcher.search(query, limit)
        │
        ▼  JNI
rust:split_searcher/jni_search.rs           ◀── translates Java → Rust handles
        │
        ▼  pure Rust
rust:split_searcher/  (search orchestration)
        │
        ├──► rust:split_query/  ◀── converts Java SplitQuery to Quickwit QueryAst
        │       ├── parse_query.rs
        │       ├── query_converters.rs
        │       ├── ip_rewriter.rs        (CIDR / wildcard IP expansion)
        │       ├── wildcard_analysis.rs  (cost analysis)
        │       └── schema_cache.rs       (cached split schema lookup)
        │
        ▼
rust:standalone_searcher/searcher.rs        ◀── invokes Quickwit search API
        │
        ▼  Quickwit asks the Storage trait for byte ranges
rust:persistent_cache_storage.rs            ◀── tiered Storage wrapper
        │
        ├── L1 hit ──► return immediately from rust:global_cache/l1_cache.rs
        ├── L2 hit ──► return from rust:disk_cache/  (decompress LZ4/Zstd)
        └── L3 miss ─► fetch via Quickwit Storage backend (S3/Azure/file://)
                       │
                       └── on success, write to L2 (background) and L1
```

**What each module contributes:**

- `java:split.SplitSearcher` enforces `try-with-resources`, then dispatches to native via a `jlong` handle.
- `rust:split_searcher/jni_search.rs` is pure marshaling — it pulls the searcher `Arc` out of the registry in `utils.rs`, hands off to the search orchestrator.
- `rust:split_query/` is where queries get **rewritten and optimized** before they hit the engine. CIDR like `10.0.0.0/8` becomes a disjunction of term queries; complex wildcards are scored for cost.
- `rust:standalone_searcher/` runs the actual Quickwit search. It is unaware of cache managers — it just asks for a `Searcher` and a `Storage` and runs.
- `rust:persistent_cache_storage.rs` is what makes the search fast. Every byte range Quickwit asks for is checked against L1, then L2, then fetched. **Range coalescing** here is what turns "5 separate 4KB reads" into "1 combined 32KB read".
- The cache manager (`rust:split_cache_manager/`) doesn't appear in the hot path — it set everything up earlier when `java:split.SplitCacheManager.getInstance(config)` was called.

**Async handling:** all Quickwit calls are async. `rust:runtime_manager.rs` provides the singleton Tokio runtime that runs them, so the Java thread blocks on a `oneshot` channel rather than spinning up its own runtime.

## Scenario 2 — Retrieving documents after a search

Once you have hits, you need the document fields. There are two paths depending on volume.

### Single document

```
java:split.SplitSearcher.doc(docAddress)
        ▼  JNI
rust:split_searcher/jni_search.rs (single-doc retrieval)
        ▼
rust:document/jni_getters.rs
        ▼
rust:persistent_cache_storage.rs   (fetches the STORE component byte range)
```

Field decoding happens in `rust:document/` which knows the schema. The result is marshaled back as a `RetrievedDocument`.

### Many documents

```
java:split.SplitSearcher.docBatch(docAddresses)
        ▼  JNI
rust:split_searcher/jni_batch.rs
        ▼
rust:batch_retrieval/optimized.rs  ◀── async parallel fetch with persistent cache
        │
        ├── coalesces nearby doc byte ranges
        ├── parallelizes fetches across the runtime
        ├── reuses cached ranges from rust:persistent_cache_storage.rs
        ▼
rust:document/  (decode each document)
        ▼
java:batch.BatchDocumentReader  ◀── decodes the marshaled byte buffer
```

Why two paths: `batch_retrieval/optimized.rs` is much more efficient at scale (N requests → ~1 round trip) but pays a small fixed cost. For one document the simple path is faster.

**Companion mode wrinkle:** if the split is in parquet companion mode, the STORE component isn't in the split — it's a reference to a Parquet file. `rust:parquet_companion/` resolves the doc ID to a Parquet row and reads it from the companion file (which itself goes through the same tiered cache).

## Scenario 3 — Aggregations

```
java:aggregation.TermsAggregation("city", "city")
java:aggregation.SumAggregation("revenue", "amount")
        │
        ▼  attached to the search request
java:split.SplitSearcher.search(query, limit, aggregations)
        ▼  JNI
rust:split_searcher/jni_aggregation.rs
        ▼
rust:searcher/aggregation/  ◀── one submodule per aggregation type
        │     terms.rs, sum.rs, avg.rs, min.rs, max.rs, count.rs,
        │     stats.rs, cardinality.rs, range.rs, histogram.rs,
        │     date_histogram.rs, multi_terms.rs
        │
        ▼  reads fast fields via the storage layer
rust:persistent_cache_storage.rs   (FASTFIELD component)
        │
        ▼  results marshaled back
java:result.SearchResult.getAggregation("city")
        ▼
java:aggregation.TermsAggregationResult
```

The aggregation classes are pure data — the work is in `rust:searcher/aggregation/`. `BucketResult::Terms` is a struct variant (use `BucketResult::Terms { buckets, .. }`).

## Scenario 4 — Indexing documents into a local index

The classic Tantivy path. Used in tests and embedded scenarios.

```
java:core.SchemaBuilder → addTextField, addIntegerField, …
        ▼  JNI
rust:schema/jni_builder.rs
        ▼
java:core.Schema  (handle to a Tantivy Schema)
        ▼
java:core.Index(schema, path, exists)
        ▼  JNI
rust:index.rs   ◀── creates the Tantivy index
        │
        ▼
java:core.Index.writer(heapSize, numThreads)
        ▼  JNI
rust:index.rs (writer creation)
        │
        ├──► rust:memory_pool/reservation.rs
        │       (reserves heapSize via the active MemoryPool;
        │        records in WRITER_RESERVATIONS for cleanup)
        ▼
java:core.IndexWriter
        ▼
java:core.Document → addText, addInteger, …
java:core.IndexWriter.addDocument(doc)
        ▼  JNI
rust:document/jni_add_fields.rs → rust:index.rs (add to writer)
        ▼
java:core.IndexWriter.commit()
        ▼  JNI
rust:index.rs (commit; flushes segments)
```

**Memory enforcement:** if you pass a heap below `Index.Memory.MIN_HEAP_SIZE` (15 MB), the Java side rejects it with a helpful error pointing at the constants. This is the validation layer that replaced the old "memory arena needs to be at least 15000000" cryptic native error.

**Reservation lifetime:** the writer's `MemoryReservation` is registered in `WRITER_RESERVATIONS` and released when the writer is dropped (either explicitly via `close()` or when the Java object is collected).

## Scenario 5 — Merging splits

A maintenance / compaction operation. Merges N splits into 1.

```
java:split.QuickwitSplit.mergeSplits(splitUrls, outputPath, mergeConfig)
        ▼  JNI
rust:quickwit_split/  (entry point)
        │
        ├── reads metadata for each input split (via tiered cache)
        ├── downloads needed segments
        │     ▼
        │     rust:persistent_cache_storage.rs  (S3/Azure/file://)
        │
        ├── runs a Tantivy MergeExecutor with controlled memory
        │     ▼
        │     rust:memory_pool/  (15 MB heap matching Quickwit's design)
        │
        ├── combines parquet companion manifests if applicable
        │     ▼
        │     rust:parquet_companion/  combine_parquet_manifests
        │
        ├── writes the new split bundle
        ├── persists _doc_mapping.json inside the bundle
        │     (for merge-time JSON sub-field recovery)
        │
        ▼
java:split.QuickwitSplit.SplitMetadata  (returned to caller)
```

**Process isolation:** for high-parallelism merges, this whole path is invoked from a separate Rust binary (the `tantivy4java-merge` standalone executable) so each concurrent merge gets its own Tokio runtime, heap, and address space. The Java side uses `MergeBinaryExtractor` to spawn and coordinate processes. This is what gets the system to 99.5–100% parallel efficiency on N-way merges.

## Scenario 6 — Listing files in an external table (Delta example)

A separate path that doesn't involve the search engine at all.

```
java:delta.DeltaTableReader.listFiles(tableUri, options)
        ▼  JNI
rust:delta_reader/jni.rs
        ▼
rust:delta_reader/scan.rs
        ▼
delta-kernel-rs   (snapshot construction, log replay)
        ▼
rust:delta_reader/serialization.rs   (binary protocol → Java byte buffer)
        ▼
java:delta.DeltaTableReader  (decodes into DeltaFileEntry list)
```

`rust:common.rs` provides the storage config building (AWS/Azure credentials, region, endpoints) shared with `iceberg_reader/` and `parquet_reader/`. The Iceberg and Parquet paths look identical — same shape, different upstream library.

## Scenario 7 — Configuring the system at startup

This is the only path that doesn't return data. It happens once.

```
java:config.GlobalCacheConfig.builder()
        .maxCacheSize(...).awsCredentials(...).region(...)
        .build()
        ▼
java:Tantivy.initialize(globalCacheConfig)   (or implicit on first use)
        ▼  JNI
rust:lib.rs   (Java_..._initializeGlobalCache)
        ▼
rust:global_cache/config.rs   (apply settings)
rust:runtime_manager.rs       (configure Tokio worker counts)
rust:memory_pool/             (set up the JvmMemoryPool if accountant provided)
```

After this, `SplitCacheManager.getInstance(name, cacheConfig)` will create per-cache managers that share the global components. Duplicate names with conflicting configs are rejected here so you don't accidentally fragment caches.

## Cross-cutting flow: how an Arc lives and dies

Every long-lived Java object (Index, Searcher, SplitSearcher, IndexWriter, Schema, Document, …) holds a `long` that points into `ARC_REGISTRY` in `rust:utils.rs`. The lifecycle is the same for all of them:

1. **Creation:** Rust constructs the object as `Arc<MyType>`, calls `arc_to_jlong(arc)` which inserts into the registry and returns the pointer.
2. **Use:** Each JNI method calls `with_arc_safe::<MyType, _>(jlong, |arc| { ... })` which looks up the registry, downcasts via `Any::downcast_ref` (no raw pointer casts), and invokes the closure.
3. **Destruction:** Java's `close()` calls `release_arc(jlong)`, which removes the entry. When the last `Arc` clone goes out of scope, the destructor runs.

This pattern is what eliminated the SIGSEGV crashes during commit and AWS SDK shutdown documented in `CLAUDE.md`. There are no `Box::from_raw` calls anywhere in the production hot path.

## Where memory comes from in each scenario

| Scenario              | Major allocations                                            | Tracked by                         |
| --------------------- | ------------------------------------------------------------ | ---------------------------------- |
| Indexing              | Writer heap (`Index.Memory.*`)                               | `memory_pool/` (`MemoryReservation` per writer) |
| Split search          | L1 cache, decoded fast fields, query workspace               | `global_cache/` budget             |
| Document retrieval    | L2 cache disk space, in-flight ranges                        | `disk_cache/disk_cache_budget.rs`  |
| Aggregations          | Bucket maps, hash tables                                     | `searcher/aggregation/` (transient) |
| Merging               | 15 MB writer heap per process; sequential I/O buffers        | `memory_pool/` + per-process isolation |
| Table listing         | Snapshot decode (delta-kernel) — usually small               | not tracked (short-lived)          |

The `java:memory.NativeMemoryAccountant` interface is the hook that lets Spark see all of these allocations as one number, so a Spark task with a 4 GB executor doesn't accidentally allocate 4 GB of native cache on top.
