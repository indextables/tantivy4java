# 05 — Java Package Design Deep Dive

The Java side of tantivy4java is deliberately thin. It owns no real state: every long-lived object (Index, Searcher, SplitSearcher, IndexWriter, Schema, Document, Query, …) is a small Java wrapper around a `long` that addresses a Rust-side `Arc` through the registry in `native/src/utils.rs`. This doc explains the conventions that follow from that design and the places where the thin-shim illusion leaks.

## The thin-shim contract

Every native-backed Java class has the same three-field shape:

```java
public class SomeThing implements AutoCloseable {
    private long nativePtr;          // handle into rust:utils.rs ARC_REGISTRY
    private boolean closed = false;  // double-close guard
    // plus whatever pure-Java metadata is cheap to cache (path, name, …)
}
```

The implications are strict:

- **No finalizer, no `Cleaner`.** If you forget `close()`, the Rust `Arc` stays in the registry and its referent stays alive. There is no GC-driven cleanup. The tests rely on `try-with-resources` and explicit `close()`; production code must too.
- **`close()` is idempotent but not thread-safe.** The `closed` flag is plain, not `volatile` or synchronized. Concurrent close from two threads is undefined — don't.
- **The handle is opaque to Java.** Java never dereferences `nativePtr`. It only passes it into native methods. All type checking happens in Rust via `with_arc_safe::<T, _>` downcasts.
- **Pure-Java metadata (like `indexPath`) is advisory.** It's cached for error messages and test assertions, but the source of truth is the Rust object.

### Example: `core.Index`

```java
// src/main/java/io/indextables/tantivy4java/core/Index.java
public class Index implements AutoCloseable {
    private long nativePtr;
    private boolean closed = false;
    private String indexPath;

    public Index(Schema schema, String path, boolean reuse) {
        String resolvedPath = FileSystemConfig.hasGlobalRoot()
            ? FileSystemConfig.resolvePath(path) : path;
        this.nativePtr = nativeNew(schema.getNativePtr(), resolvedPath, reuse);
        this.indexPath = resolvedPath;
    }

    @Override
    public void close() {
        if (!closed) {
            nativeClose(nativePtr);
            closed = true;
        }
    }
}
```

`nativeNew` returns a `jlong` from `arc_to_jlong(Arc::new(TantivyIndex { ... }))`. `nativeClose` calls `release_arc(nativePtr)` which removes the entry from the registry, letting the last `Arc` clone drop. There is no `finalize()` fallback.

## Builder patterns: no `build()`

Builders in tantivy4java do not have an explicit `build()` step. The builder is finalized implicitly by being handed to a consumer:

```java
SchemaBuilder builder = new SchemaBuilder();
builder.addTextField("title", true, false, "default", "position");
builder.addIntegerField("count", true, true, false);
Schema schema = builder.build();   // <-- this IS the sink
```

`SchemaBuilder.build()` exists, but `SplitCacheManager.CacheConfig` and the aggregation builders don't follow that pattern. They're objects you mutate until you pass them to something:

```java
SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("main")
    .withMaxCacheSize(200_000_000)
    .withAwsCredentials(accessKey, secretKey)
    .withAwsRegion("us-east-1");

SplitCacheManager manager = SplitCacheManager.getInstance(config);  // sink
```

The fluent methods all `return this;` and the validation is done eagerly inside each setter so you fail fast:

```java
// SchemaBuilder.java
public SchemaBuilder addTextField(String name, boolean stored, boolean fast,
                                  String tokenizerName, String indexOption,
                                  int maxTokenLength) {
    if (closed) {
        throw new IllegalStateException("SchemaBuilder has been closed");
    }
    TokenLength.validate(maxTokenLength);
    nativeAddTextField(nativePtr, name, stored, fast,
                       tokenizerName, indexOption, maxTokenLength);
    return this;
}
```

The `closed` flag on a builder is set when it's consumed. Calls after consumption throw `IllegalStateException`. This is the only form of linear-type discipline the API tries to enforce.

## `SplitCacheManager`: singleton per name

`SplitCacheManager` is the entry point for all distributed search, and it has the most interesting lifecycle in the Java API. The design goals are:

1. **One cache per logical workload.** A cache has a name; two callers asking for the same name get the same manager.
2. **No duplicate caches from typos.** Asking for the same name with a *different* config is a bug — typically it means two components in the same process think they own the cache. The native layer rejects this.
3. **Clean JVM shutdown.** A shutdown hook walks the registry and calls `close()` on each manager so disk cache manifests are flushed.

The implementation:

```java
public class SplitCacheManager implements AutoCloseable {
    private static final Map<String, SplitCacheManager> instances =
        new ConcurrentHashMap<>();
    private static final ReentrantReadWriteLock instancesLock =
        new ReentrantReadWriteLock();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                NativeMemoryManager.shutdown();
            } catch (UnsatisfiedLinkError | NoClassDefFoundError e) {
                // memory pool may not be configured in all deployments
            }
            synchronized (instances) {
                for (SplitCacheManager manager : instances.values()) {
                    try { manager.close(); }
                    catch (Exception e) {
                        System.err.println("Warning: " + e.getMessage());
                    }
                }
                instances.clear();
            }
        }));
    }
    // ...
}
```

A few things worth noting:

- The config *validation* isn't visible in the Java class. It happens when the manager first asks Rust to construct its `GlobalSplitCacheManager` — `split_cache_manager/manager.rs` compares the requested config with the one stored for that name and fails if they disagree.
- The shutdown hook uses `synchronized(instances)` while `getInstance` is using `ConcurrentHashMap` + `ReentrantReadWriteLock` — the shutdown path is intentionally coarse-grained because by then the JVM is single-threaded-ish anyway. Don't rely on `getInstance` succeeding after shutdown has started.
- Closing the manager closes all searchers it created. Searchers share the manager's cache; closing the manager invalidates them all.

## Query trees: value semantics, no cascading close

`Query` and `SplitQuery` are both `AutoCloseable`, and they both wrap a native handle. But query tree composition uses *value* semantics — child queries are not owned by their parent:

```java
public class SplitBooleanQuery extends SplitQuery {
    private final List<SplitQuery> mustQueries = new ArrayList<>();

    public SplitBooleanQuery addMust(SplitQuery query) {
        if (query == null) throw new IllegalArgumentException("Query cannot be null");
        mustQueries.add(query);
        return this;
    }

    public List<SplitQuery> getMustQueries() {
        return new ArrayList<>(mustQueries);  // defensive copy
    }
}
```

Consequences:

- Closing a `SplitBooleanQuery` does **not** close its children. You are responsible for closing every query you construct.
- Children can be added to multiple parents. There's no shared-ownership tracking.
- `getMustQueries()` returns a copy, so external mutation of the returned list is harmless.

If you're generating queries in a loop, the clean pattern is to close each child as soon as the parent has been used, or to hold them all in a `try-with-resources` chain and close everything after the search.

## Search results: lazy aggregation decoding

`result.SearchResult` is what comes back from `Searcher.search()` and `SplitSearcher.search()`. It contains:

- A list of scored hits, each pointing at a `DocAddress`.
- An opaque byte buffer (or handle) for aggregation results.
- A batch-retrieval handle.

The aggregation payload is **not** decoded eagerly. When you ask `SearchResult.getAggregation("revenue_by_city")`, it hands the bytes to the matching `aggregation.*Result` class, which decodes into buckets/values on demand. This matters because a search that requests ten aggregations but whose caller only reads one doesn't pay the decode cost of the other nine.

## Aggregation request/result pairing

Every aggregation type is a *pair*: a request class extending `SplitAggregation` and a result class implementing `AggregationResult`. They share a name:

```java
TermsAggregation agg = new TermsAggregation("by_city", "city", 10, 100);
// search with agg ...
TermsResult result = (TermsResult) searchResult.getAggregation("by_city");
```

The request classes validate in their constructors (empty name, non-positive size, etc.) so you fail before the JNI call. The result classes are plain POJOs returned from the native decoder:

```java
public class TermsResult implements AggregationResult {
    private final String name;
    private final List<TermsBucket> buckets;
    private final long docCountErrorUpperBound;
    private final long sumOtherDocCount;

    public TermsResult(String name, List<TermsBucket> buckets,
                       long docCountErrorUpperBound, long sumOtherDocCount) {
        this.name = name;
        this.buckets = buckets;
        // ...
    }
}
```

Sub-aggregations are supported: a bucket aggregation can hold a `Map<String, SplitAggregation>` of children that are computed per-bucket natively. The result type then exposes each bucket's sub-aggregation results as another `AggregationResult`.

## The memory API: `Index.Memory` constants

`core.Index.Memory` is a static holder for validated heap sizes:

```java
public static final class Memory {
    public static final int MIN_HEAP_SIZE     = 15_000_000;   // Tantivy's absolute minimum
    public static final int DEFAULT_HEAP_SIZE = 50_000_000;
    public static final int LARGE_HEAP_SIZE   = 128_000_000;
    public static final int XL_HEAP_SIZE      = 256_000_000;
}
```

`IndexWriter` creation validates the passed heap against `MIN_HEAP_SIZE` and throws a descriptive `IllegalArgumentException` pointing at these constants. This was added after users repeatedly hit the Tantivy native error "memory arena needs to be at least 15000000" and had nothing to grep for. The constants exist so that error messages can *name* a suggested fix.

Behind the scenes, `IndexWriter` creation calls through to `rust:index.rs`, which reserves the heap from the active `MemoryPool` (see `06-rust-design.md`) and stores the reservation in `WRITER_RESERVATIONS` keyed by writer pointer. Closing the writer drops the reservation.

## Memory accountant: JVM ↔ native bridge

For Spark (and any other framework with `TaskMemoryManager`-style governance), `memory.NativeMemoryAccountant` is the hook:

```java
public interface NativeMemoryAccountant {
    long acquireMemory(long bytes, String category);   // return bytes actually granted
    void releaseMemory(long bytes, String category);
}
```

Users register their accountant once at startup via `NativeMemoryManager`. The Rust side's `JvmMemoryPool` calls back into this Java interface through JNI when it crosses a watermark — not on every allocation. `UnlimitedMemoryAccountant` is the default; it accepts every request. This is what lets Spark executors see native cache usage as part of their task memory budget without slowing down the hot path.

See `docs/UNIFIED_MEMORY_MANAGEMENT_DESIGN.md` for the background on why watermark batching matters.

## External table readers: differently shaped

The `delta/`, `iceberg/`, and `parquet/` packages don't follow the native-handle-wrapper pattern as strictly. They're more request/response: you call `DeltaTableReader.listFiles(uri, options)` and get back a `List<DeltaFileEntry>`, and there's no long-lived Java object to close. This is because the underlying Rust modules (`delta_reader/`, `iceberg_reader/`, `parquet_reader/`) do their work synchronously via per-operation Tokio runtimes and return everything in one go — there's nothing to keep alive.

The trade-off: if you list a huge table the whole file list comes back at once. For most Delta/Iceberg workloads this is fine; table snapshots are typically tens of thousands of files, not millions.

## Threading model

The Java side makes very few thread-safety guarantees:

| Object                       | Thread-safe?                                                           |
| ---------------------------- | ---------------------------------------------------------------------- |
| `Index`                      | Yes for `searcher()`; not for `writer()` concurrent calls              |
| `IndexWriter`                | No — single writer thread, callers must serialize                      |
| `Searcher`                   | Yes — read-only                                                        |
| `SplitCacheManager`          | Yes                                                                    |
| `SplitSearcher`              | Yes for `search()`; results are short-lived                            |
| `SchemaBuilder`              | No — use from one thread                                               |
| `Query` / `SplitQuery`       | Not for mutation (adding children); yes once built                     |
| `Document` (while building)  | No                                                                     |

The Rust side is more conservative: every native-handle type is `Arc<Mutex<T>>` or `Arc<T>` where `T: Sync`. Java's thread-safety rules are therefore the *minimum* — Rust will be at least this safe, but Java callers shouldn't lean on it.

## Error surface

Exceptions that come back from native code are all `RuntimeException`. The message is composed in `rust:utils.rs::handle_error` (or `convert_throwable` for JNI functions wrapped in panic-catching) — see `06-rust-design.md` for the Rust side. From Java's perspective:

- **Bad input validation** (missing field name, invalid heap size) throws `IllegalArgumentException` or `IllegalStateException` *before* the JNI call, from Java-side guards.
- **Native errors** (bad S3 credentials, corrupt split, missing file) come back as `RuntimeException` with a message describing the Rust-side failure.
- **Rust panics** (shouldn't happen in production) are caught by `convert_throwable`, converted to `"Rust panic: <message>"`, and thrown as `RuntimeException`. The process does not abort.

There is no checked-exception hierarchy. This is a deliberate choice — the callers are mostly JVM-only (Spark, services) and prefer unchecked exceptions.

## Summary of invariants

If you're writing Java code against tantivy4java, these are the rules you can rely on:

1. Every `AutoCloseable` holds a native handle. Forget `close()` and you leak into the Rust registry.
2. Builders validate eagerly; expect `IllegalArgumentException`/`IllegalStateException` before the JNI call.
3. `SplitCacheManager` is one-per-name. Using the same name twice with different configs is an error.
4. Query trees have value semantics — close children yourself.
5. Aggregation request/result classes share a name; decoding is lazy.
6. Memory constants are your documentation for the tuning knobs.
7. Native errors all come back as `RuntimeException`. Catch and log, don't try to recover granularly.
