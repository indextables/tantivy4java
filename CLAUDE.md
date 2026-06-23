# tantivy4java

Java bindings for [Tantivy](https://github.com/quickwit-oss/tantivy) (Rust search engine) via JNI. Originally ported from the Python tantivy bindings with API parity — the Java API mirrors Python tantivy patterns. Used by IndexTables4Spark to embed full-text search directly in Spark executors.

## Build & Test

```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@11

make compile          # Build Java + Rust native (via cargo)
make test             # Unit tests (excludes cloud/integration)
make test-cloud       # Cloud tests only (requires S3/Azure credentials)
make test-all         # All tests (unit + cloud)
make package          # Build JAR (skips tests)
make clean            # Remove build artifacts

# Single test:
mvn test -Dtest='SplitSearcherTest'
```

First-time setup: `make setup` (installs Java 11, Maven, Rust, protoc).

## Project Structure

| Directory | What lives here |
|-----------|----------------|
| `src/main/java/io/indextables/tantivy4java/` | Java API — see packages below |
| `native/src/` | Rust JNI implementation (searcher, disk cache, batch retrieval, delta/iceberg readers) |
| `native/build.rs` | JNI build script (compiles Rust → .dylib/.so, bundled into JAR) |
| `docs/` | Feature guides and developer documentation |
| `detail_designs/` | Design documents for major features |
| `scripts/` | Setup and build helper scripts |
| `examples/` | Usage examples |

## Java API Packages

| Package | Purpose |
|---------|---------|
| `core` | Index, IndexWriter, Searcher, Schema, SchemaBuilder, Document, Field types |
| `split` | SplitSearcher, SplitCacheManager, SplitQuery types, merge operations — the primary API used by IndexTables4Spark |
| `query` | Query builder, BooleanQuery, Occur, Range, Snippet |
| `aggregation` | Terms, Histogram, DateHistogram, Range, Stats, Count, Min/Max/Sum/Avg |
| `batch` | Batch document retrieval (optimized S3 GET consolidation) |
| `iceberg` | Iceberg table reader (snapshots, schema, file entries) |
| `delta` | Delta Lake table reader |
| `parquet` | Parquet table reader (companion indexing) |
| `memory` | Native memory management (arena allocation with MIN/DEFAULT/LARGE/XL heap sizes) |
| `config` | FileSystem and cache configuration |

## Key Concepts

- **Split files** (`.split`) — Quickwit-format immutable search index segments stored on S3/Azure/local disk
- **SplitSearcher** — Primary search API. Opens a split, runs queries, returns results. Supports aggregations.
- **SplitCacheManager** — JVM-wide cache with L1 (memory) and L2 (NVMe disk) tiers. Auto-detects `/local_disk0` on Databricks/EMR.
- **Companion indexing** — Build search indexes alongside existing Parquet/Iceberg/Delta tables without modifying originals
- **Process-based merge** — Merge operations run in isolated Rust processes for linear parallel scalability

## Known Limitations

- **RangeAggregation** — not implemented in the native Rust layer. Returns empty results.
- **MillionRecordBulkRetrievalTest** — isolated memory issue, does not affect core functionality

## Architecture Notes

- Rust native library compiled via `cargo` during Maven build, bundled into the JAR
- JNI bridge uses direct memory sharing — zero copy between Java and Rust where possible
- Targets Java 11+
- Memory arenas: MIN=15MB, DEFAULT=50MB, LARGE=128MB, XL=256MB (use `Index.Memory` constants)
- Debug logging: set `TANTIVY4JAVA_DEBUG=1`
- Package: `io.indextables.tantivy4java`
