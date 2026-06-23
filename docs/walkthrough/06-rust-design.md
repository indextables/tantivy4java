# 06 — Rust Native Design Deep Dive

This doc is the design half of the walkthrough: how the Rust crate is put together, which patterns appear over and over again, and why the important invariants exist. For the what-and-where map, see `03-rust-native.md`.

The Rust crate is built around a small number of load-bearing mechanisms:

1. **Object lifetime through an Arc registry** (`utils.rs`) — so Java handles can't become dangling.
2. **A thin JNI layer that never does real work** — files named `jni_*.rs` contain only bridging logic.
3. **A single async runtime for Quickwit operations** (`runtime_manager.rs`) — with per-operation runtimes as an escape hatch for the external table readers.
4. **A layered storage abstraction with range coalescing** — L1 → L2 → L3 all behind the Quickwit `Storage` trait.
5. **RAII memory reservations tied to a pluggable `MemoryPool` trait** — so the JVM (or Spark) can account for native allocations.
6. **Panic-catching at the JNI boundary** — so Rust panics become Java exceptions, not JVM crashes.

The rest of this doc walks each in turn, with real excerpts from the code.

## 1. Object lifetime: the Arc registry in `utils.rs`

Every long-lived object that Java holds a handle to lives in a global registry:

```rust
// native/src/utils.rs
pub static ARC_REGISTRY: Lazy<Mutex<HashMap<jlong, Box<dyn Any + Send + Sync>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

static ARC_NEXT_ID: AtomicU64 = AtomicU64::new(1);

pub fn arc_to_jlong<T: Send + Sync + 'static>(arc: Arc<T>) -> jlong {
    let mut registry = ARC_REGISTRY.lock().unwrap();
    let id = ARC_NEXT_ID.fetch_add(1, Ordering::SeqCst) as jlong;
    registry.insert(id, Box::new(arc));
    id
}
```

The shape of this is important. Three observations:

**The jlong is an ID, not a pointer.** Registry IDs are monotonically increasing `u64`s cast to `jlong`. They never alias real memory, and they never have raw-pointer dereference semantics. This is the single most important property of the design — it's what lets Java mishandle a handle (e.g., use-after-close) and get a clean "invalid pointer" error instead of a segfault.

**Downcasting is type-checked at runtime.** Lookups go through:

```rust
pub fn with_arc_safe<T, R, F>(ptr: jlong, f: F) -> Option<R>
where
    T: Send + Sync + 'static,
    F: FnOnce(&Arc<T>) -> R,
{
    let registry = ARC_REGISTRY.lock().unwrap();
    let boxed = registry.get(&ptr)?;
    let arc = boxed.downcast_ref::<Arc<T>>()?;   // safe downcast
    Some(f(arc))
}
```

The `downcast_ref::<Arc<T>>()` call is what replaces the `*mut dyn Any as *mut T` cast that used to live in an earlier version of this code. That raw cast produced real SIGSEGV crashes in `IndexWriter::commit()` because the vtable layout wasn't what the code assumed. The safe downcast returns `None` on mismatch; JNI callers treat `None` as "invalid pointer" and throw.

**Release is explicit.** `release_arc(jlong)` removes the entry from the registry; when the last `Arc` clone held by internal code drops, the destructor runs. There is no GC hook on the Rust side.

This pattern applies to *everything* long-lived: `TantivyIndex`, `TantivyIndexWriter`, `Schema`, `Document`, every `SplitSearcher`, every `SplitCacheManager`, every `Query`. It's the single invariant that keeps the JNI boundary safe.

The `GLOBAL_JVM` static (also in `utils.rs`) is a separate affair — it's a `OnceLock<JavaVM>` captured at library load so that Rust threads without a JNI env can call back into Java (used by `memory_pool/jvm_pool.rs` for watermark callbacks).

## 2. JNI bridge files: the three-stage pattern

Every file named `jni_*.rs` follows the same three-stage shape: *pull the Arc, do the work, translate the result*. Here's a real example:

```rust
// native/src/searcher/jni_searcher.rs
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Searcher_nativeSearch(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    query_ptr: jlong,
    limit: jint,
    // ... other args
) -> jlong {
    // Stage 1: pull inputs from the registry
    let query_clone = match with_arc_safe::<Box<dyn TantivyQuery>, _>(query_ptr, |q| {
        q.box_clone()
    }) {
        Some(q) => q,
        None => { handle_error(&mut env, "Invalid Query pointer"); return 0; }
    };

    // Stage 2: do the work
    let result = with_arc_safe::<Mutex<TantivySearcher>, _>(ptr, |searcher_mutex| {
        let searcher = searcher_mutex.lock().unwrap();
        let collector = TopDocs::with_limit(limit as usize).order_by_score();
        searcher.search(query_clone.as_ref(), &collector)
            .map_err(|e| e.to_string())
    });

    // Stage 3: translate the result back to a jlong/jobject
    // (converting Vec<(f32, DocAddress)> into a Java object or another registered Arc)
    // ...
}
```

Every JNI function in the crate is variations on this. The first stage is always `with_arc_safe` calls; the second is usually a single Rust function call; the third is the only part that varies (some return a new handle, some return primitives, some return serialized byte buffers).

The discipline is: *don't do real logic inside a JNI function*. If a `jni_*.rs` file starts growing interesting control flow, it's a sign the logic should move into a sibling pure-Rust file in the same module. `split_searcher/` is a good example — the `jni_*` files are short, and `search.rs`, `prewarm_impl.rs`, etc. are where the real work lives.

### Why `extern "system"` and `#[no_mangle]`?

JNI requires a C ABI and a specific symbol name (`Java_<package>_<class>_<method>`). `extern "system"` selects the right ABI for the platform (`"stdcall"` on Windows x86, `"C"` everywhere else). `#[no_mangle]` prevents Rust from mangling the name so the JVM can find it.

## 3. Async/sync boundary: two runtime patterns

The crate has **two distinct async strategies** depending on which part you're in.

### Strategy A: the Quickwit singleton runtime (`runtime_manager.rs`)

Search, split access, merge, and all Quickwit-backed operations go through one global runtime:

```rust
// native/src/runtime_manager.rs
pub struct QuickwitRuntimeManager {
    runtime: Arc<Runtime>,
    is_shutting_down: AtomicBool,
    active_searcher_count: AtomicUsize,
    upload_semaphore: Arc<Semaphore>,      // global upload concurrency
    download_semaphore: Arc<Semaphore>,    // global download concurrency
    config: RuntimeConfig,
}

impl QuickwitRuntimeManager {
    fn new() -> anyhow::Result<Self> {
        let config = get_runtime_config().clone();
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(config.worker_threads)
                .thread_name("quickwit-runtime")
                .enable_all()
                .build()?
        );
        // ...
    }
}
```

The runtime is configured **once** (`RUNTIME_CONFIG` is a `OnceLock`) and used for every Quickwit operation. Configuration happens before first use — `configure_runtime(config)` returns `false` if it's too late. The defaults (`num_cpus()` for workers/downloads/uploads) are typically fine for anything short of pathological workloads.

The two semaphores are what keep Quickwit from saturating S3. A split merge might want to download 100 files, but if the semaphore limits downloads to `num_cpus`, you get a natural back-pressure point without writing your own rate limiter.

JNI calls enter this runtime via `runtime.block_on(async { ... })` or by spawning and waiting on a `oneshot`. The important consequence is that the Java thread blocks. That's fine — Spark task threads are expected to block on native work, and the worker threads inside the runtime are what keep things moving.

### Strategy B: per-operation runtimes in the external table readers

The `delta_reader/`, `iceberg_reader/`, and `parquet_reader/` modules don't go through `QuickwitRuntimeManager`. They build a runtime per call:

```rust
// native/src/delta_reader/distributed.rs (paraphrased)
pub fn get_snapshot_info(url_str: &str, config: &DeltaStorageConfig) -> Result<DeltaSnapshotInfo> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    rt.block_on(async {
        // delta-kernel-rs calls
    })
}
```

The rationale: these operations are **one-shot** and short. A Delta file listing is a single S3 `LIST` plus a log replay. Spinning up the whole Quickwit runtime (with its semaphores, searcher tracking, etc.) adds no value. A `current_thread` runtime is cheaper to create and tears down cleanly when the call returns.

This isn't ideal at scale — if you list a thousand Delta tables in parallel you pay for a thousand runtimes — but the external table readers are discovery tools, not hot paths, and the simplicity is worth it.

**Never mix the two strategies in one call.** Calling `QuickwitRuntimeManager::block_on()` from inside a `current_thread` runtime will deadlock if there's any cross-task work. Each module sticks to one or the other.

## 4. Tiered storage: L1 → L2 → L3 behind the `Storage` trait

The cache is the most architecturally interesting part of the crate. Quickwit's `Storage` trait expects byte-range reads (`get_slice(path, range) -> Bytes`). Everything — S3, local files, cached reads — implements that trait, and they can be stacked.

`persistent_cache_storage.rs` is the stacking wrapper. It holds:

- An inner `storage: Arc<dyn Storage>` (the remote backend — S3, Azure, or file://)
- An `Arc<L2DiskCache>` (the persistent on-disk cache)
- `storage_loc` and `split_id` (the cache key components)

Its `get_slice` implementation is the entire tiered-read algorithm:

```rust
// native/src/persistent_cache_storage.rs (simplified)
async fn get_slice(&self, path: &Path, byte_range: Range<usize>) -> StorageResult<OwnedBytes> {
    let component = Self::extract_component(path);
    let requested_range = byte_range.start as u64 .. byte_range.end as u64;

    // Ask L2 what it has for this range (via range coalescing)
    let coalesce_result = self.disk_cache.get_coalesced(
        &self.storage_loc, &self.split_id, &component, requested_range.clone()
    );

    if coalesce_result.fully_cached {
        // L2 hit
        self.stats.l2_hits.fetch_add(1, Ordering::Relaxed);
        return Ok(Self::combine_segments(&coalesce_result.cached_segments, &requested_range));
    }

    if coalesce_result.cached_bytes > 0 && !coalesce_result.gaps.is_empty() {
        // partial L2 hit — fetch only the gaps
        self.stats.l2_partial_hits.fetch_add(1, Ordering::Relaxed);
        return self.fetch_and_combine(path, &requested_range, &coalesce_result).await;
    }

    // L2 miss — fetch whole range from L3
    let bytes = self.storage.get_slice(path, byte_range.clone()).await?;
    self.record_and_cache(&component, Some(disk_range), bytes.as_slice());
    Ok(bytes)
}
```

Two things to notice.

### Range coalescing — the actual coalescing lives in `disk_cache/range_index.rs`

`get_coalesced` returns a `CoalesceResult` with three fields: `cached_segments` (the byte ranges the cache already has that overlap this request), `gaps` (the uncached spans between them), and `fully_cached` (a shortcut flag). The heavy lifting — interval overlap queries, byte counting, gap calculation — happens inside `disk_cache/range_index.rs`, which uses a binary-search-based interval data structure over the manifest entries for (`storage_loc`, `split_id`, `component`). Lookups are `O(log n + k)` where `k` is the number of overlapping ranges.

The coalescing is what makes the cache's `get_slice` smarter than just "is this exact range cached?". A Quickwit search might ask for a 32 KB span; the cache might have that span across three adjacent 12 KB entries; coalescing combines them into one response without refetching from S3.

### L1 isn't in this path directly

`persistent_cache_storage.rs` is the L2 layer. The L1 layer (in `global_cache/l1_cache.rs`) is a Quickwit-level concept — it caches *Tantivy searcher internals* (fast field data, term dictionary lookups, decoded posting lists), not raw byte ranges. It sits above `persistent_cache_storage`, inside the Quickwit `Searcher`, and its hits never reach `get_slice` at all. That's why you'll see both "L1 hit rate" and "L2 hit rate" reported separately in the cache stats — they're measuring two different things.

## 5. The L2 disk cache in detail

`disk_cache/mod.rs` defines `L2DiskCache`. Its responsibilities divide along submodule lines:

| Submodule        | Responsibility                                              |
| ---------------- | ----------------------------------------------------------- |
| `types.rs`       | `DiskCacheConfig`, `CompressionAlgorithm`, queue modes      |
| `manifest.rs`    | Persistent manifest: what's cached, where, with what checksum |
| `range_index.rs` | The interval data structure that powers coalescing          |
| `lru.rs`         | Split-level LRU eviction                                    |
| `mmap_cache.rs`  | Bounded cache of open file handles / mmap regions           |
| `background.rs`  | Async writer thread draining the write queue                |
| `compression.rs` | LZ4 encoder/decoder with component-aware skip rules         |
| `get_ops.rs`     | Read-path implementation                                    |
| `write_ops.rs`   | Write-path implementation                                   |

The config tells you what the knobs are:

```rust
#[derive(Debug, Clone)]
pub struct DiskCacheConfig {
    pub root_path: PathBuf,
    pub max_size_bytes: u64,              // 0 = auto: 2/3 of available disk
    pub compression: CompressionAlgorithm,
    pub min_compress_size: usize,         // skip compression under this
    pub manifest_sync_interval_secs: u64,
    pub mmap_cache_size: usize,
    pub write_queue_mode: WriteQueueMode, // bounded (Fragment) or size-based
    pub drop_writes_when_full: bool,      // back-pressure policy
}
```

### Writes are asynchronous

The hot-path search never waits for disk. When `persistent_cache_storage.rs` wants to cache a newly-fetched byte range, it calls `disk_cache.put(...)` which enqueues the write and returns immediately. A background thread (defined in `background.rs`) drains the queue and performs the actual compression + file write + manifest update.

This is what the `preloadComponents(...).join()` contract is about: prewarming needs to guarantee the writes are *durable* before it returns, so it uses a blocking flush path that waits for the queue to drain. Normal search reads don't need this and use the async path.

### Compression is conservative

`compression.rs` compresses most components with LZ4 but explicitly skips:

- Data below `min_compress_size` (not worth the CPU).
- Components that are already compressed by Tantivy (`.store`) or have their own layout (`.term`).
- Components accessed at random by byte offset (`.idx`, `.pos`, `.fast`) — compressing these would require decompression for every access, which defeats the point.

The enum has a `Zstd` variant, but only LZ4 is wired up in practice. If you want Zstd you'd implement it in `compression.rs` and it'd slot in without changes elsewhere.

### LRU is split-granular

When the cache hits 95% capacity, the LRU evicts entire splits, not individual components. This is a trade-off: evicting a split means a search has to refetch all its components, but the manifest stays small and the eviction decision is simple. Component-level eviction would fragment the cache and inflate manifest size.

## 6. Memory: the `MemoryPool` trait and RAII reservations

Memory accounting is where tantivy4java differs most from vanilla Tantivy. The design lives in `memory_pool/`.

### The trait

```rust
// native/src/memory_pool/pool.rs
pub trait MemoryPool: Send + Sync + Debug {
    fn try_acquire(&self, size: usize, category: &'static str) -> Result<(), MemoryError>;
    fn release(&self, size: usize, category: &'static str);
    fn used(&self) -> usize;
    fn peak(&self) -> usize;
    fn granted(&self) -> usize;
}
```

Implementations:

- `UnlimitedMemoryPool` — the default. `try_acquire` always succeeds; `used`/`peak` track statistics but don't enforce a limit.
- `JvmMemoryPool` — calls back into Java through JNI when it crosses a high watermark. This is the Spark integration point.

The pool is *pluggable*: `GLOBAL_MEMORY_POOL` is a `Lazy<Arc<dyn MemoryPool>>` that starts as `UnlimitedMemoryPool` and can be replaced once via `set_global_pool(...)` at startup. Changing it after allocations have been made is not supported.

### RAII reservations

The `MemoryReservation` type is an RAII guard: it holds a size and a reference to the pool, and its `Drop` impl calls `pool.release(size, category)`:

```rust
// native/src/memory_pool/reservation.rs
pub struct MemoryReservation {
    pool: Arc<dyn MemoryPool>,
    size: usize,
    category: &'static str,
}

impl MemoryReservation {
    pub fn try_new(pool: &Arc<dyn MemoryPool>, size: usize, category: &'static str)
        -> Result<Self, MemoryError>
    {
        if size == 0 {
            return Ok(Self { pool: Arc::clone(pool), size: 0, category });
        }
        pool.try_acquire(size, category)?;
        Ok(Self { pool: Arc::clone(pool), size, category })
    }
}

impl Drop for MemoryReservation {
    fn drop(&mut self) {
        if self.size > 0 {
            self.pool.release(self.size, self.category);
        }
    }
}
```

The beauty of this is that Rust's ownership system *is* the cleanup story. A reservation that's stored in a struct lives as long as the struct. A reservation that's dropped on the floor is returned immediately.

### Writer reservations

`native/src/index.rs` uses this to back the `Index.Memory.*` constants. When an `IndexWriter` is created:

```rust
// native/src/index.rs (paraphrased)
static WRITER_RESERVATIONS: Lazy<Mutex<HashMap<jlong, MemoryReservation>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

fn create_writer(index: &Index, heap_size: usize, num_threads: usize) -> jlong {
    let reservation = MemoryReservation::try_new(
        &get_global_pool(), heap_size, "index_writer_heap"
    ).expect("failed to reserve writer heap");

    let writer = index.writer_with_num_threads(num_threads, heap_size)?;
    let arc = Arc::new(TantivyIndexWriter::new(writer));
    let handle = arc_to_jlong(arc);

    WRITER_RESERVATIONS.lock().unwrap().insert(handle, reservation);
    handle
}
```

When the Java `IndexWriter.close()` is called, the handle is removed from `WRITER_RESERVATIONS`, the reservation is dropped, and the heap is released back to the pool — all before the underlying Tantivy writer is destroyed.

### Watermark batching in `JvmMemoryPool`

The naive implementation would call back into Java on every `try_acquire`. That would be a disaster — allocations happen in hot paths, and JNI round-trips cost hundreds of nanoseconds.

Instead, `JvmMemoryPool` keeps an atomic counter of bytes owed to the JVM. On `try_acquire`, it bumps the counter. Only when the counter crosses a high-watermark threshold (default 90% of granted budget) does it JNI-call `acquireMemory()` to request more from Java. Similarly, on `release`, it waits until the counter drops below a low watermark (default 25%) before calling `releaseMemory()`. Small bursts of allocation/release never touch Java at all.

This is the single most important optimization in `memory_pool/`. See `docs/UNIFIED_MEMORY_MANAGEMENT_DESIGN.md` for the full analysis.

## 7. Query optimization: analysis, not transformation

`split_query/` is a small but important subsystem. Its job is to take a Java-constructed `SplitQuery` tree, convert it into a Quickwit `QueryAst`, and produce *hints* about cost.

The cost model is deliberately simple:

```rust
// native/src/split_query/query_optimizer.rs
pub enum QueryCost {
    Low,    // single FST lookup, O(1)
    Medium, // prefix traversal, or sorted-range scan
    High,   // full FST scan, or multiple regex expansions
}

pub struct QueryAnalysis {
    pub has_expensive_wildcard: bool,
    pub has_cheap_filters: bool,
}
```

These flags don't *change* the query. What they do is drive tracing and enable early short-circuits — if `has_cheap_filters` is true and the cheap filter is selective, we can sometimes answer the query from a fast field without evaluating the wildcard at all.

IP and CIDR rewriting lives in `ip_expansion.rs` and is wired up by `split_query/ip_rewriter.rs` — these *do* transform the query (CIDR `10.0.0.0/8` becomes a disjunction of term queries), but the transformation is lossless and happens before the query reaches the searcher.

Schema caching for splits (so we don't re-fetch the schema on every query to the same split) lives in `split_query/schema_cache.rs`.

## 8. Error propagation: `convert_throwable` and the panic hook

The Rust side of the JNI boundary has to handle three error modes: Rust `Result::Err`, Rust `panic!`, and native unwinding from FFI code it calls. `utils.rs` has a wrapper for the first two:

```rust
// native/src/utils.rs
pub fn convert_throwable<T, F>(env: &mut JNIEnv, f: F) -> anyhow::Result<T>
where
    F: FnOnce(&mut JNIEnv) -> anyhow::Result<T>,
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| f(env))) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => {
            let _ = env.throw_new("java/lang/RuntimeException", error.to_string());
            Err(error)
        }
        Err(panic_info) => {
            let panic_msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                format!("Rust panic: {}", s)
            } else if let Some(s) = panic_info.downcast_ref::<String>() {
                format!("Rust panic: {}", s)
            } else {
                "Rust panic (unknown payload)".to_string()
            };
            let _ = env.throw_new("java/lang/RuntimeException", &panic_msg);
            Err(anyhow::anyhow!("{}", panic_msg))
        }
    }
}
```

`catch_unwind` + `AssertUnwindSafe` is a small but critical piece — it converts a panic into a `Result::Err`, preventing the unwind from crossing into JNI code (which is not unwind-safe and would abort the JVM). The panic payload is downcast to a string and thrown as a `RuntimeException`.

**Not every JNI function uses `convert_throwable`.** Many use the simpler `handle_error(env, message)` path (just `env.throw_new("java/lang/RuntimeException", msg)` after a manual error check). The panic protection is opt-in — functions that call into risky code (merge, aggregation evaluation, anything that might panic on bad input) wrap themselves; simple ones don't. If you're adding a new JNI function that touches user data, use `convert_throwable`.

### The global panic hook

`install_panic_hook()` (called once via `std::sync::Once`) installs a panic hook that logs to stderr:

```rust
pub fn install_panic_hook() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let default_hook = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            eprintln!(
                "tantivy4java: panic on thread {:?}: {}",
                std::thread::current().name(), info
            );
            default_hook(info);
        }));
    });
}
```

Why this matters: panics inside Tokio worker threads or disk-cache background threads are **not** on a JNI call stack. `catch_unwind` won't rescue them into a Java exception. The best we can do is log them loudly. The hook ensures that a panic in a background thread shows up in the application log instead of vanishing.

## 9. Quickwit split merges (what's actually in the code)

There has been some aspirational design work on "process-isolated" merges (see `detail_designs/PROCESS_BASED_MERGE_GUIDE.md`) describing a standalone `tantivy4java-merge` binary and a `MergeBinaryExtractor` Java helper. **That binary is not in the crate.** `native/Cargo.toml` has no `[[bin]]` target and there is no `bin/` directory under `native/src/`.

In practice, split merges run in-process:

- `quickwit_split/` contains the merge orchestration logic.
- Merges are async functions executed on the `QuickwitRuntimeManager` runtime.
- Concurrency is controlled by the runtime's upload/download semaphores.
- Each merge reserves its memory budget through the `MemoryPool`.

If you want true process isolation, you'd implement it on the Java side by spawning separate JVMs — the Rust side doesn't provide it today.

## 10. Parquet companion mode

`parquet_companion/` is a big subsystem that solves one specific problem: the split file includes fast field data and a document store, both of which are huge. If you already have a Parquet file sitting next to the split that contains the same columns, why duplicate them?

Companion mode creates splits with *references* to external Parquet files for the STORE and FASTFIELD components. When the searcher wants a doc or a fast field, instead of reading from the split, it:

1. Consults the companion manifest to find which Parquet file and which row group.
2. Goes through its own cached Parquet reader (`parquet_reader/`) to fetch the relevant columns.
3. Transcodes Parquet column data into the format Tantivy expects.
4. Caches the transcoded bytes in L2 under a distinct key (`parquet_transcoded_<segment>_<col_hash>`).

The trade-off is clear: splits shrink by 80–90%, but retrievals do an extra network hop. The L2 transcoded-bytes cache is what makes this acceptable — once a column has been transcoded once, subsequent reads hit the same cache as regular splits.

The architectural takeaway: **companion mode slots in without changing any of the layers above.** The `Storage` abstraction, `persistent_cache_storage.rs`, and the query path have no idea whether they're reading from a regular split or a companion one. All the companion-specific logic is in `parquet_companion/` and is triggered by the directory wrapping in the searcher setup.

## 11. External table readers: the non-search path

`delta_reader/`, `iceberg_reader/`, and `parquet_reader/` live alongside the search engine but share very little with it. Their jobs are narrow: "given a Delta/Iceberg/Parquet table URI and credentials, return the file list and schema." They don't hit any Tantivy code, don't go through `persistent_cache_storage.rs`, and don't use `QuickwitRuntimeManager`.

They share `common.rs` for helpers: AWS/Azure credential extraction from JNI `HashMap`s, path normalization, and `DeltaStorageConfig` (which all three reuse because it's really "common object store config" with a misleading name).

The duplication between the three modules is deliberate — each one wraps a different upstream library (`delta-kernel-rs`, `iceberg-rust`, manual Parquet listing) and those libraries have different error types, different async shapes, different configuration. A shared abstraction would be leaky. Keeping them parallel and cross-referencing `common.rs` turned out to be cleaner.

## 12. Invariants a Rust contributor should hold

If you're writing new code in `native/src/`, these are the rules that keep the crate working:

1. **Every Java-visible object goes through `arc_to_jlong` / `with_arc_safe`.** No raw pointers across the JNI boundary, ever.
2. **JNI bridge files contain no real logic.** If your `jni_*.rs` file has branches that aren't about argument translation or error checking, the logic belongs next door.
3. **Quickwit-backed async work uses `QuickwitRuntimeManager`; one-shot table-discovery async work uses a `current_thread` runtime.** Don't mix.
4. **Allocations that can grow unbounded go through a `MemoryPool`.** Store the `MemoryReservation` in the struct that owns the memory so Drop does the right thing.
5. **Storage access goes through the `Storage` trait.** If you need a new backend, implement the trait; don't bypass it.
6. **Wrap JNI functions that might panic in `convert_throwable`.** If the function is pure translation of already-validated values, `handle_error` is enough.
7. **Don't add `Box::from_raw` or similar raw-pointer reconstruction.** The registry exists to make this unnecessary. Every crash the crate has ever had came from bypassing it.
8. **Background threads need the panic hook.** They won't propagate errors any other way.

The bulk of the crate's complexity sits in four files: `utils.rs`, `persistent_cache_storage.rs`, `disk_cache/mod.rs`, and `split_searcher/` (which is really a directory, but treat it as one). Everything else is either glue, bridging, or specialized subsystems that don't touch the core invariants. Once the four load-bearing pieces are clear, the rest of the code tends to explain itself.
