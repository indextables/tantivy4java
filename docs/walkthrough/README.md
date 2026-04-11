# Tantivy4Java Code Walkthrough

A guided tour of the tantivy4java codebase: how the modules are organized, what each one is responsible for, and how they fit together. Written for developers who need to find their way around the code, not for end users of the library.

## Reading order

1. **[01-architecture.md](01-architecture.md)** — The big picture. Layered architecture, the JVM ↔ JNI ↔ Rust ↔ Quickwit ↔ remote storage stack, and the cross-cutting concerns (caching, memory, async runtime) that touch every layer.
2. **[02-java-api.md](02-java-api.md)** — Walkthrough of every Java package under `src/main/java/io/indextables/tantivy4java/`. Identifies user-facing API vs. internal plumbing and notes which native module each package bridges to.
3. **[03-rust-native.md](03-rust-native.md)** — Walkthrough of every module under `native/src/`. Separates pure JNI bridges from pure Rust logic, and groups modules by responsibility (core index, searcher, cache, storage, etc).
4. **[04-data-flow.md](04-data-flow.md)** — How requests actually flow through the layers. Traces a search query, a document retrieval, an index write, and a split merge end-to-end so you can see how the modules from docs 02 and 03 connect in practice.
5. **[05-java-design.md](05-java-design.md)** — Design deep dive for the Java side. The thin-shim contract, native handle ownership, builder patterns, `SplitCacheManager` singleton lifecycle, query tree semantics, aggregation request/result pairing, threading model, and the error surface.
6. **[06-rust-design.md](06-rust-design.md)** — Design deep dive for the Rust crate. The Arc registry, JNI bridge conventions, the two async runtime strategies, tiered storage with range coalescing, L2 disk cache internals, the `MemoryPool` trait and RAII reservations, query optimization, panic propagation, and the invariants a Rust contributor needs to hold.

## Source tree at a glance

```
tantivy4java/
├── src/main/java/io/indextables/tantivy4java/   ← Java API + JNI shim
│   ├── core/         Index, Schema, Searcher, Document — primary user API
│   ├── query/        Query builders (Term, Boolean, Range, …)
│   ├── result/       SearchResult container
│   ├── aggregation/  Metric + bucket aggregations
│   ├── split/        SplitSearcher, SplitCacheManager — distributed search
│   ├── batch/        Bulk document retrieval
│   ├── delta/        Delta Lake table discovery
│   ├── iceberg/      Iceberg table discovery
│   ├── parquet/      Hive-partitioned Parquet discovery
│   ├── config/       Global cache + runtime configuration
│   ├── memory/       JVM-coordinated memory accounting
│   ├── filter/       Partition filters
│   ├── util/         TextAnalyzer, Facet
│   └── examples/     Reference programs
│
└── native/src/                                   ← Rust + JNI implementation
    ├── lib.rs                  Crate root, JNI exports
    ├── utils.rs                Arc registry, JavaVM handle
    ├── runtime_manager.rs      Singleton Tokio runtime
    ├── debug.rs                Conditional debug logging
    │
    ├── index.rs                Tantivy Index/IndexWriter JNI
    ├── schema/                 Schema builder + introspection
    ├── document/               Document build/retrieve JNI
    ├── query/                  Core query JNI (Term, Bool, Range, …)
    ├── text_analyzer.rs        Tokenizer JNI
    │
    ├── searcher/               In-memory search orchestration + aggregations
    ├── standalone_searcher/    Cache-manager-free split searcher
    ├── split_searcher/         Split-specific searcher (the workhorse)
    ├── split_query/            Split query AST conversion + optimization
    ├── split_cache_manager/    Java-facing cache lifecycle
    │
    ├── global_cache/           L1 in-memory cache + Quickwit components
    ├── disk_cache/             L2 persistent disk cache (LZ4/Zstd)
    ├── persistent_cache_storage.rs   Tiered storage wrapper (L1→L2→L3)
    ├── batch_retrieval/        Bulk document fetching
    ├── prewarm/                Component preloading
    │
    ├── memory_pool/            JVM-coordinated memory accounting
    ├── ffi_profiler.rs         Low-overhead FFI profiler
    ├── ffi_profiler_jni.rs     Profiler JNI bridge
    │
    ├── quickwit_split/         Split merge operations
    ├── parquet_companion/      Parquet companion mode (external storage refs)
    ├── parquet_reader/         Hive-partitioned Parquet listing
    ├── parquet_schema_reader.rs   Parquet footer schema extraction
    ├── delta_reader/           Delta Lake file listing
    ├── iceberg_reader/         Iceberg table listing
    ├── txlog/                  Indextables transaction log v4
    │
    ├── ip_expansion.rs         CIDR / IP wildcard expansion
    ├── extract_helpers.rs      JSON value extraction helpers
    ├── common.rs               Shared helpers for table readers
    └── test_query_parser.rs    Query parser tests
```

## How to use these docs

- If you're **new to the codebase**, read all six in order. Docs 01–04 are orientation; 05–06 are the design rationale behind what you saw.
- If you're **debugging a query**, jump to `04-data-flow.md` to see the path, then drill into the relevant module in `02` or `03`.
- If you're **adding a new feature**, start with `01-architecture.md` to find the right layer, then read the matching deep dive (`05` for Java, `06` for Rust) to learn the conventions neighbors follow, then use `02`/`03` as a module reference while you work.
- If you're **writing new Rust code**, the invariants list at the end of `06-rust-design.md` is the short version of what not to break.
- If you're **looking for a specific module**, the file trees in `02` and `03` are alphabetized within each section.
