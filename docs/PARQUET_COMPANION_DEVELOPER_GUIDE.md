# Parquet Companion Mode — Developer Guide

## Overview

Parquet companion mode creates **minimal Quickwit splits that reference external parquet files** instead of duplicating data in tantivy's internal stores. This reduces split size by 45-70% (depending on schema and fast field mode) while preserving full search, aggregation, and document retrieval capabilities.

**How it works:** During split creation, tantivy indexes the parquet data (inverted index + optionally fast fields) but skips storing the raw documents. Instead, it embeds a `_parquet_manifest.json` inside the split bundle that maps tantivy doc IDs back to parquet file rows. At query time, document retrieval reads directly from the original parquet files.

## Quick Start

```java
// 1. Create a parquet companion split
ParquetCompanionConfig config = new ParquetCompanionConfig("/data/my_table")
        .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID);

QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
        Arrays.asList("/data/my_table/part-0001.parquet", "/data/my_table/part-0002.parquet"),
        "/output/my_split.split",
        config);

// 2. Search the split — note: table_root is provided at read time, not baked into the split
SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("my-cache")
        .withMaxCacheSize(200_000_000)
        .withParquetTableRoot("/data/my_table");  // where parquet files live NOW
try (SplitCacheManager cm = SplitCacheManager.getInstance(cacheConfig);
     SplitSearcher searcher = cm.createSplitSearcher("file:///output/my_split.split", metadata)) {

    // Prewarm fast fields for aggregations
    searcher.preloadParquetFastFields("price", "category").join();

    // Run aggregation
    TermsAggregation agg = new TermsAggregation("categories", "category", 100, 0);
    SearchResult result = searcher.search(new SplitMatchAllQuery(), 0, "cats", agg);
}
```

## Decoupled Table Root

The `table_root` (the base path for resolving parquet file locations) is **not stored inside the split**. Instead, it is provided at read time via `CacheConfig.withParquetTableRoot()`. This design enables:

- **Portability**: The same split can be used with parquet files at different locations (e.g., different S3 buckets, local vs. cloud).
- **Environment flexibility**: Dev, staging, and production can use the same splits with different storage paths.
- **Credential separation**: Parquet files can use different credentials than the split storage.

### Credential fallback strategy

When creating parquet storage at read time:
1. If `CacheConfig.withParquetStorage()` provides dedicated credentials → use those
2. Else if the cache manager has AWS/Azure credentials → use those (shared creds)
3. Else → unauthenticated / local storage

```java
// Same credentials for splits and parquet files (most common)
SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("my-cache")
        .withAwsCredentials(accessKey, secretKey)
        .withAwsRegion("us-east-1")
        .withParquetTableRoot("s3://my-bucket/tables/events/");  // uses same AWS creds

// Different credentials for parquet files
ParquetCompanionConfig.ParquetStorageConfig parquetCreds =
        new ParquetCompanionConfig.ParquetStorageConfig()
                .withAwsCredentials(otherAccessKey, otherSecretKey)
                .withAwsRegion("eu-west-1");

SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("my-cache")
        .withAwsCredentials(splitAccessKey, splitSecretKey)  // for split storage
        .withParquetTableRoot("s3://other-bucket/data/")     // parquet location
        .withParquetStorage(parquetCreds);                    // parquet-specific creds
```

## Fast Field Modes

Fast fields are columnar data used for aggregations, range queries, and sorting. Three modes control where this data comes from:

| Mode | Numerics/Bool/Date | Strings | Split Size | Use Case |
|------|-------------------|---------|------------|----------|
| `DISABLED` | Native tantivy | Native tantivy | Largest companion | No aggregations needed; or using native fast fields |
| `HYBRID` | Native tantivy | From parquet | Medium | Best balance — numeric aggregations use native columnar data; string aggregations transcode from parquet on demand |
| `PARQUET_ONLY` | From parquet | From parquet | Smallest | Maximum size reduction; all fast fields decoded from parquet at query time |

**Benchmark: split sizes** (100K rows, 10 fields — 4 numeric, 1 date, 1 IP, 5 unique-UUID strings):

| Configuration | Size | vs Standalone |
|---------------|------|---------------|
| Standalone split | 50.95 MB | baseline |
| Companion `DISABLED` | 28.38 MB | -44% |
| Companion `HYBRID` | 17.97 MB | -65% |
| Companion `PARQUET_ONLY` | 16.14 MB | -68% |

The savings increase with more string fields (unique UUIDs create large term dictionaries and fast field stores). With a simpler schema (4 fields, low-cardinality strings), savings are ~50% across all modes.

### When to use each mode

- **DISABLED**: You only need full-text search and document retrieval. No aggregations or range queries.
- **HYBRID** (recommended): You need aggregations on both numeric and string fields. Numeric types are stored natively in the split for fast access; string types are transcoded from parquet lazily per-query.
- **PARQUET_ONLY**: Maximum storage savings. All fast field data comes from parquet. Slightly higher aggregation latency since every column must be transcoded.

### Text field tokenizer restriction

Only text fields with the `"raw"` tokenizer can serve as fast fields from parquet. Text fields using `"default"`, `"en_stem"`, or other tokenizers that split text into tokens during indexing are **rejected** for parquet fast field transcoding — the tokenized representation is incompatible with parquet's raw strings. These fields will throw a `RuntimeException` with "not configured as fast field" if used in aggregations under HYBRID or PARQUET_ONLY mode.

```java
// Override a field's tokenizer to "raw" to enable fast field transcoding
Map<String, String> overrides = new HashMap<>();
overrides.put("category", "raw");  // enables fast field for this text column

ParquetCompanionConfig config = new ParquetCompanionConfig(tableRoot)
        .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID)
        .withTokenizerOverrides(overrides);
```

## Schema Derivation

Parquet Arrow types are automatically mapped to tantivy field types:

| Parquet / Arrow Type | Tantivy Type | Notes |
|---------------------|--------------|-------|
| BOOLEAN | Bool | |
| INT8, INT16, INT32, INT64 | I64 | All integer widths map to i64 |
| UINT8, UINT16, UINT32, UINT64 | U64 | |
| FLOAT, DOUBLE | F64 | |
| BYTE_ARRAY, UTF8, LARGE_UTF8 | Text (Str) | Tokenizer = "raw" by default |
| UTF8 (with `withIpAddressFields`) | IpAddr | User-declared IP address columns |
| BINARY, LARGE_BINARY | Bytes | |
| TIMESTAMP (all units) | DateTime | Normalized to microseconds |
| DATE32, DATE64 | DateTime | |
| LIST, MAP, STRUCT | Json | Complex types serialized as JSON |

**Key design decisions:**
- **No stored fields** — parquet is the store. Document retrieval reads from parquet files.
- **Text fields indexed** with "raw" tokenizer by default (searchable but not tokenized).
- **Numeric/bool/date/ip fields** indexed and optionally fast, depending on `FastFieldMode`.
- **IP address fields** — parquet has no native IP type, so IPs are stored as UTF8 strings. Declare them with `withIpAddressFields()` to create tantivy `IpAddr` fields. In HYBRID mode, IP fields use native fast fields (like numerics).
- **JSON types** (from LIST/MAP/STRUCT) are always stored (tantivy needs the JSON value for queries).

### IP address fields

Parquet has no native IP type — IP addresses are stored as UTF8 strings. Declare IP columns explicitly so they become tantivy `IpAddr` fields with proper indexing and fast field support:

```java
ParquetCompanionConfig config = new ParquetCompanionConfig(tableRoot)
        .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID)
        .withIpAddressFields("src_ip", "dst_ip");  // UTF8 columns parsed as IP addresses
```

In HYBRID mode, IP address fields use native fast fields (like numerics), not parquet transcoding. String values are parsed as IPv4 or IPv6 addresses during indexing.

### Skipping fields

```java
ParquetCompanionConfig config = new ParquetCompanionConfig(tableRoot)
        .withSkipFields("_metadata_col", "internal_id");  // these columns will not be indexed
```

## Indexing Pipeline

`QuickwitSplit.createFromParquet()` performs the following steps:

1. **Read schema** from the first parquet file's Arrow schema
2. **Validate schema consistency** across all input files
3. **Resolve name mapping** (Iceberg field IDs if enabled)
4. **Derive tantivy schema** from Arrow types
5. **Create tantivy index** with single-threaded writer (ensures doc IDs match parquet row order)
6. **Index all rows** from all parquet files sequentially, tracking per-file row offsets
7. **Collect statistics** for requested fields
8. **Build manifest** with file metadata, row group info, and column mapping
9. **Embed manifest** as `_parquet_manifest.json` inside the split bundle
10. **Return metadata** including split ID, doc count, footer offsets, and column statistics

```java
ParquetCompanionConfig config = new ParquetCompanionConfig("/data/table")
        .withFastFieldMode(ParquetCompanionConfig.FastFieldMode.HYBRID)
        .withStatisticsFields("price", "created_at")    // compute min/max/null stats
        .withIndexUid("products-index")
        .withSourceId("etl-pipeline")
        .withNodeId("worker-1");

QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(
        parquetFiles, outputPath, config);

// Access column statistics for split pruning
Map<String, ColumnStatistics> stats = metadata.getColumnStatistics();
ColumnStatistics priceStats = stats.get("price");
if (priceStats != null && !priceStats.overlapsRange(minPrice, maxPrice)) {
    // Skip this split — no matching rows
}
```

## Document Retrieval

Documents are fetched directly from parquet files using the manifest's doc ID mapping:

```java
try (SplitSearcher searcher = cm.createSplitSearcher(splitUri, metadata)) {
    SearchResult result = searcher.search(query, 10);
    for (SearchHit hit : result.getHits()) {
        // This reads from the original parquet file, not from the split
        try (Document doc = searcher.doc(hit.getDocAddress())) {
            String name = doc.getFirst("name").toString();
        }
    }

    // Projected retrieval — returns JSON string with only the requested columns
    String json = searcher.docProjected(hit.getDocAddress(), "name", "price");
    // Returns: {"name":"Widget","price":9.99}

    // Batch projected retrieval — efficient for multiple docs
    DocAddress[] addresses = result.getHits().stream()
            .map(SearchHit::getDocAddress).toArray(DocAddress[]::new);
    byte[] jsonArray = searcher.docBatchProjected(addresses, "name", "price");
    // Returns UTF-8 JSON array: [{"name":"Widget","price":9.99}, ...]
}
```

### Performance optimizations

Document retrieval from remote parquet files (S3/Azure) benefits from two key optimizations:

**Page-level reads**: The reader loads parquet offset indexes, enabling surgical byte-range reads of individual pages (~few KB) rather than full column chunks (~30MB for 1M rows). This is critical for single-doc retrievals from large files.

**ByteRangeCache**: A shared in-memory cache across doc retrievals for the same searcher. Dictionary pages (typically 800KB–1MB per column) are fetched once and reused for all subsequent doc retrievals from the same file. For a batch of 5 single-doc retrievals, this reduces S3 bytes by ~80%.

These optimizations are automatic — no configuration needed.

## Aggregation and Lazy Transcoding

Aggregation queries on parquet companion splits use **lazy per-query transcoding**: fast field columns are transcoded from parquet to tantivy's columnar format only when a query actually needs them.

### How it works

1. Query arrives (e.g., stats aggregation on "price" + terms aggregation on "category")
2. `ensure_fast_fields_for_query()` parses the query JSON and extracts field names: `["price", "category"]`
3. Checks which columns are already transcoded (monotonically growing set)
4. Transcodes only the new columns needed
5. Caches the result in memory (per-searcher) and L2 disk cache (persistent)
6. Subsequent queries reuse cached transcoded data

### Prewarm vs. lazy

| Approach | When | How |
|----------|------|-----|
| **Lazy** (default) | Every search/aggregation | Automatic — fields transcoded on first use |
| **Prewarm fast fields** | Before aggregation/range queries | `searcher.preloadParquetFastFields("field1", "field2").join()` |
| **Prewarm columns** | Before doc retrieval | `searcher.preloadParquetColumns("name", "price").join()` |

Prewarm is recommended for latency-sensitive workloads where you know which fields will be queried.

**Important:** L2 disk cache is **required** for prewarm to be effective. Without a `TieredCacheConfig`, prewarmed data goes to L1 memory only. L1 data can be evicted at any time, and will not survive searcher recreation. Always configure a disk cache when using prewarm:

```java
SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
        .withDiskCachePath("/mnt/nvme/tantivy_cache")
        .withMaxDiskSize(100_000_000_000L);

SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("my-cache")
        .withMaxCacheSize(200_000_000)
        .withTieredCache(tieredConfig);  // REQUIRED for prewarm persistence
```

With L2 disk cache + full prewarm (`preloadComponents` + `preloadParquetFastFields` + `preloadParquetColumns`), all search, range, and aggregation queries can achieve **zero S3/Azure requests**.

## Merge Support

Parquet companion splits can be merged like regular splits:

```java
QuickwitSplit.MergeConfig mergeConfig = new QuickwitSplit.MergeConfig(
        "products-index", "etl-pipeline", "worker-1");

QuickwitSplit.SplitMetadata merged = QuickwitSplit.mergeSplits(
        Arrays.asList(split1Path, split2Path), outputPath, mergeConfig);
```

**Merge constraints:**
- All source splits must have parquet manifests (no mixing parquet/non-parquet splits)
- No deletions allowed in source splits (identity doc ID mapping required)
- All source splits must use the same `FastFieldMode`
- Row offsets are automatically adjusted in the combined manifest

## L2 Disk Cache for Transcoded Fast Fields

Transcoded fast field bytes are persisted to the L2 disk cache (if configured). This means:
- Transcoded data survives JVM restarts
- New `SplitSearcher` instances reuse previously transcoded data without re-reading parquet
- Cache keys use `parquet_transcoded_{segment}_{column_hash}` to distinguish from native data

### Enabling disk cache

```java
SplitCacheManager.CacheConfig cacheConfig = new SplitCacheManager.CacheConfig("my-cache")
        .withMaxCacheSize(200_000_000);

SplitCacheManager.TieredCacheConfig tieredConfig = new SplitCacheManager.TieredCacheConfig()
        .withDiskCachePath("/mnt/nvme/tantivy_cache")
        .withMaxDiskSize(100_000_000_000L);
cacheConfig.withTieredCache(tieredConfig);

try (SplitCacheManager cm = SplitCacheManager.getInstance(cacheConfig)) {
    // Transcoded fast fields are automatically cached to disk
    try (SplitSearcher s = cm.createSplitSearcher(splitUri, metadata)) {
        s.preloadParquetFastFields("price", "name").join();
        // Transcoded bytes now in L2 disk cache
    }

    // New searcher loads from L2 — no re-transcoding needed
    try (SplitSearcher s2 = cm.createSplitSearcher(splitUri, metadata)) {
        s2.preloadParquetFastFields("price", "name").join();  // L2 cache hit
    }
}
```

## Column Statistics

Statistics are computed during `createFromParquet` for split pruning:

```java
ParquetCompanionConfig config = new ParquetCompanionConfig(tableRoot)
        .withStatisticsFields("price", "created_at", "category")
        .withStatisticsTruncateLength(128);  // max string length for min/max

QuickwitSplit.SplitMetadata metadata = QuickwitSplit.createFromParquet(files, output, config);

Map<String, ColumnStatistics> stats = metadata.getColumnStatistics();

// Numeric range pruning
ColumnStatistics priceStats = stats.get("price");
boolean skip = priceStats != null && !priceStats.overlapsDoubleRange(10.0, 50.0);

// Timestamp range pruning
ColumnStatistics dateStats = stats.get("created_at");
boolean skipDate = dateStats != null && !dateStats.overlapsTimestampRange(startMicros, endMicros);
```

**Supported types:** I64 (min/max long), F64 (min/max double), Str (min/max string, truncated), DateTime (min/max timestamp micros), Bool (min/max bool). NaN values are excluded from F64 statistics. Null values are tracked via `nullCount`.

## Monitoring

`getParquetRetrievalStats()` returns a JSON string with metadata about the parquet companion configuration:

```java
String stats = searcher.getParquetRetrievalStats();
// Returns JSON: {"total_files":2,"total_rows":100000,"total_row_groups":4,
//                "fast_field_mode":"Hybrid","columns":["id","name","price",...],
//                "file_sizes":[12345678,9876543]}
```

Returns `null` if the split has no parquet companion manifest.

## Name Mapping (Iceberg Support)

Parquet companion mode supports field ID-based name resolution for Iceberg tables:

```java
// Explicit mapping: parquet physical name → display name
Map<String, String> mapping = new HashMap<>();
mapping.put("col_0001", "user_id");
mapping.put("col_0002", "event_type");

ParquetCompanionConfig config = new ParquetCompanionConfig(tableRoot)
        .withFieldIdMapping(mapping);

// Auto-detect from Iceberg schema metadata
ParquetCompanionConfig config = new ParquetCompanionConfig(tableRoot)
        .withAutoDetectNameMapping(true);  // reads iceberg.schema from parquet KV metadata
```

## Quickwit Fork Changes

Parquet companion mode requires minimal changes to the Quickwit fork (6 files, ~183 lines added):

| File | Change | Purpose |
|------|--------|---------|
| `hot_directory.rs` | Made `StaticDirectoryCacheBuilder`, `StaticSliceCacheBuilder`, `list_index_files` public | Used by split creation to build hotcache |
| `union_directory.rs` | Added `acquire_lock()` | Required by `Index::open(UnionDirectory)` |
| `field_presence.rs` | `ExistsQuery` fallback for fast fields | Parquet companion splits lack `_field_presence` synthetic field; uses tantivy native `ExistsQuery` when all fields are fast |
| `leaf.rs` | Added `OverlayDirectory`, `SplitOverrides`, `open_index_with_caches_and_schema_override()` | Enables injecting modified meta.json and transcoded .fast data at search time |
| `lib.rs` (search) | Re-exported `SplitOverrides` | Public API for parquet companion integration |
| `lib.rs` (directories) | Re-exported new public symbols | |

## Known Limitations

1. **Non-raw tokenizer text fields**: Cannot serve as fast fields from parquet. Use `withTokenizerOverrides()` to set "raw" tokenizer, or use DISABLED mode for these fields.
2. **No deletions in merge**: Merged parquet companion splits must not have deletions (identity doc ID mapping is required for manifest correctness).
3. **Single-threaded indexing**: `createFromParquet` uses a single-threaded writer to guarantee doc ID order matches parquet row order. This is by design.
4. **Parquet files must be accessible at read time**: The `parquet_table_root` must point to where the parquet files exist when the split is searched. If files are moved or renamed, update the `withParquetTableRoot()` path accordingly.
5. **IP address fields require explicit declaration**: Parquet has no native IP type. Use `withIpAddressFields("src_ip", "dst_ip")` to declare which UTF8 columns contain IP addresses.
