# JNI Boundary

Java and Rust have clear responsibilities. Don't cross the boundary unnecessarily — every JNI call has overhead.

**Java side** handles:
- I/O orchestration (S3 downloads/uploads, file management)
- Process management (merge binary extraction, lifecycle)
- Configuration and API surface
- Cache coordination (SplitCacheManager)

**Rust side** handles:
- Search, indexing, and merging (Tantivy core operations)
- Disk cache storage (LRU, compression, manifest)
- Batch retrieval optimization
- Aggregation computation

When adding new functionality, ask: "Does this need Tantivy/native performance, or is it orchestration?" Put it on the right side.
