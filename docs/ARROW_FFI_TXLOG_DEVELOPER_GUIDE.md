# Arrow FFI Transaction Log Developer Guide

## Overview

Four feature requests (FR1-FR4) add native Arrow FFI support to the transaction log, eliminating JVM-side orchestration overhead and enabling zero-copy data exchange.

## FR1: `nativeListFilesArrowFfi` — Native List Files

Replaces the 8-step JVM orchestration pipeline with a single native call:

1. `getSnapshotInfo()` (cached)
2. `pruneManifests()` (manifest-level partition pruning)
3. `readManifest()` per surviving manifest
4. `readPostCheckpointChanges()` + merge
5. Partition filter (file-level)
6. Data skipping by min/max stats
7. Cooldown filter (skip actions)
8. Schema restore + Arrow FFI export

### Arrow Schema

19 fixed columns + N dynamic partition columns + optional stats:

| # | Name | Type | Nullable |
|---|------|------|----------|
| 0 | path | Utf8 | No |
| 1 | size | Int64 | No |
| 2 | modification_time | Int64 | No |
| 3 | data_change | Boolean | No |
| 4 | num_records | Int64 | Yes |
| 5 | footer_start_offset | Int64 | Yes |
| 6 | footer_end_offset | Int64 | Yes |
| 7 | has_footer_offsets | Boolean | Yes |
| 8 | delete_opstamp | Int64 | Yes |
| 9 | split_tags | List\<Utf8\> | Yes |
| 10 | num_merge_ops | Int32 | Yes |
| 11 | doc_mapping_json | Utf8 | Yes |
| 12 | doc_mapping_ref | Utf8 | Yes |
| 13 | uncompressed_size_bytes | Int64 | Yes |
| 14 | time_range_start | Int64 | Yes |
| 15 | time_range_end | Int64 | Yes |
| 16 | companion_source_files | List\<Utf8\> | Yes |
| 17 | companion_delta_version | Int64 | Yes |
| 18 | companion_fast_field_mode | Utf8 | Yes |
| 19+ | partition:{name} | Utf8 | Yes |
| N-2 | min_values (if include_stats) | Utf8 | Yes |
| N-1 | max_values (if include_stats) | Utf8 | Yes |

### Java API

```java
// Pre-allocate FFI memory using your Arrow C Data Interface library.
// The number of columns is 19 base + N partition columns + optional 2 stats columns.
// The caller (e.g., Spark ArrowFfiBridge) must allocate ArrowArray/ArrowSchema structs
// and pass their memory addresses.
int numCols = 19 + partitionColumns.length + (includeStats ? 2 : 0);
long[] arrayAddrs = new long[numCols];   // filled with ArrowArray struct addresses
long[] schemaAddrs = new long[numCols];  // filled with ArrowSchema struct addresses

// Use the validated wrapper method:
String resultJson = TransactionLogReader.listFilesArrowFfi(
    tablePath, config,
    partitionFilterJson,  // PartitionFilter.toJson() or null
    dataFilterJson,       // PartitionFilter.toJson() for stats-based skipping or null
    excludeCooldown,      // boolean
    includeStats,         // boolean
    arrayAddrs, schemaAddrs
);

// Parse result metadata
Map<String, Object> result = objectMapper.readValue(resultJson, Map.class);
long numRows = ((Number) result.get("numRows")).longValue();
List<String> partCols = (List<String>) result.get("partitionColumns");
```

#### Result JSON Fields

| Field | Type | Description |
|-------|------|-------------|
| `numRows` | long | Number of file entries in Arrow batch |
| `numColumns` | int | Number of columns in the Arrow schema |
| `schemaJson` | String | JSON array of `{name, type, nullable}` field descriptors |
| `partitionColumns` | String[] | Partition column names (from table metadata) |
| `protocolJson` | String | Protocol action as JSON |
| `metadataConfigJson` | String? | Metadata config map as JSON (schema dedup registry) |
| `metrics.totalFilesBeforeFiltering` | long | Total files before any filtering |
| `metrics.filesAfterPartitionPruning` | long | Files remaining after partition filter |
| `metrics.filesAfterDataSkipping` | long | Files remaining after min/max skipping |
| `metrics.filesAfterCooldownFiltering` | long | Files remaining after cooldown exclusion |
| `metrics.manifestsTotal` | long | Total manifests in checkpoint |
| `metrics.manifestsPruned` | long | Manifests skipped by partition bounds |

#### Test Helper

For testing without Arrow Java dependencies, use the Rust-side round-trip helper that allocates FFI memory internally:

```java
// Returns JSON with paths, sizes, partitionColumns, and metrics
String result = TransactionLogReader.nativeTestListFilesRoundtrip(
    tablePath, config,
    partitionFilterJson,  // or null
    dataFilterJson,       // or null
    excludeCooldown ? 1 : 0
);
```

### Partition Filter JSON Format

```json
{"op": "and", "filters": [
  {"op": "eq", "column": "year", "value": "2024"},
  {"op": "gte", "column": "month", "value": "06"}
]}
```

Supported operators: `eq`, `neq`, `gt`, `gte`, `lt`, `lte`, `in`, `is_null`, `is_not_null`, `string_starts_with`, `string_ends_with`, `string_contains`, `and`, `or`, `not`

### Data Skipping

The `dataFilterJson` parameter uses the same PartitionFilter format but operates on `min_values`/`max_values` from the AddAction stats. The native `can_skip_by_stats()` method eliminates files where all data is provably outside the filter bounds.

**Comparison semantics:** Values are compared with numeric-aware parsing (i64 first, then f64, then string fallback). This matches the Scala `PartitionPruning.compareValues()` behavior since all stat values are stored as strings via `.toString()`.

**Truncation awareness:** For `Gte`, `Lt`, and `Lte` operators, when the filter value is a proper string extension of the stat value (e.g., filter `"aaab"` vs stat max `"aaa"`), the file is conservatively NOT skipped. This handles the case where stat values may have been truncated during collection (see `StatisticsTruncation.scala`).

---

## FR2: `nativeWriteVersionArrowFfi` — Arrow-Based Write

Writes a version file from an Arrow batch instead of JSON. The batch must include an `action_type` column.

### Arrow Schema for Write

Required column: `action_type` (Utf8) with values: `"add"`, `"remove"`, `"protocol"`, `"metadata"`, `"mergeskip"`

All other columns are optional (missing = null for that row). Each action type reads its own subset:

- **add**: path, size, modification_time, data_change, partition_values (JSON Utf8), num_records, min_values (JSON), max_values (JSON), footer_start_offset, footer_end_offset, split_tags (List\<Utf8\> or JSON), doc_mapping_json, doc_mapping_ref, uncompressed_size_bytes, time_range_start, time_range_end, companion_source_files, companion_delta_version, companion_fast_field_mode
- **remove**: path, deletion_timestamp, data_change, partition_values (JSON), size
- **protocol**: min_reader_version (Int32), min_writer_version (Int32), reader_features (List\<Utf8\> or JSON), writer_features (List\<Utf8\> or JSON)
- **metadata**: id, schema_string, partition_columns (List\<Utf8\> or JSON), configuration (JSON), created_time
- **mergeskip**: path, skip_timestamp, reason, operation, retry_after, skip_count

### Java API

```java
// Use the validated wrapper method:
WriteResult writeResult = TransactionLogWriter.writeVersionArrowFfi(
    tablePath, config,
    arrowArrayAddr,   // FFI_ArrowArray pointer
    arrowSchemaAddr,  // FFI_ArrowSchema pointer
    retry             // true = retry on conflict, false = single attempt
);
```

---

## FR3: `nativeReadNextRetainedFilesBatchArrowFfi` — Purge Cursor via Arrow

Streams retained files from a cursor via Arrow FFI instead of TANT buffer. Uses the same 19-column base schema as FR1 (no partition columns, no stats).

### Java API

```java
long cursor = TransactionLogReader.openRetainedFilesCursor(tablePath, config, retentionMs);
try {
    int numCols = 19; // base schema (no partitions for purge)
    long[] arrayAddrs = new long[numCols];   // filled with ArrowArray struct addresses
    long[] schemaAddrs = new long[numCols];  // filled with ArrowSchema struct addresses

    int rowCount;
    while ((rowCount = TransactionLogReader.readNextRetainedFilesBatchArrowFfi(
            cursor, 10000, arrayAddrs, schemaAddrs)) > 0) {
        // Process Arrow columns...
    }
} finally {
    TransactionLogReader.closeRetainedFilesCursor(cursor);
}
```

---

## FR4: Native Range Filter Elimination

Eliminates redundant range filters in the search query when the split's min/max statistics prove the filter is always-true.

### How It Works

1. Java passes `field_min_values` and `field_max_values` as JSON strings in the `splitConfigMap` when creating a `SplitSearcher`
2. The native layer stores these on `CachedSearcherContext`
3. Before each search/aggregation, `optimize_query_with_field_stats()` walks the QueryAst
4. Range filters in `"must"` and `"filter"` positions of `"bool"` queries are checked against field stats
5. If `file_min >= lower_bound AND file_max <= upper_bound`, the range is always-true and removed
6. If all `"must"` clauses are removed **and no other clauses exist** (no should/must_not/filter), the query becomes `"match_all"`. Otherwise only the empty `"must"` key is removed.

### Java API

No Java-side changes needed. The statistics flow entirely through the Rust layer:

```java
// Step 1: FR1 lists files — stats are cached on the Rust cache manager automatically
String result = TransactionLogReader.listFilesArrowFfi(tablePath, config, ...);

// Step 2: Create searcher — native layer looks up the split's stats from cache
SplitSearcher searcher = cacheManager.createSplitSearcher(splitUri, metadata);
// Range filters that encompass the split's [min,max] are auto-eliminated
```

**How it works internally:**
1. `nativeListFilesArrowFfi` loads file entries with min/max stats from the transaction log
2. If a `cache_manager` is provided, it calls `put_all_file_field_stats()` to cache stats on the `GlobalSplitCacheManager`, keyed by file path
3. When `createNativeWithSharedCache` creates a searcher, it looks up the split URI across all cache managers via `get_file_field_stats()`
4. If found, the stats are stored on `CachedSearcherContext` and used by `optimize_query_with_field_stats()`

### Benefits

- Eliminates unnecessary fast field transcoding (range filter removal means the field may not need to be loaded)
- Works for both plain search and aggregation queries (histograms, terms aggs, etc.)
- Conservative: only removes provably redundant filters
- Truncation-aware: uses same starts_with safety check as data skipping

---

## File Summary

### New Rust Files
| File | Purpose |
|------|---------|
| `native/src/txlog/list_files.rs` | FR1 orchestration function |
| `native/src/txlog/arrow_ffi_import.rs` | FR2 Arrow→Action conversion |

### Modified Rust Files
| File | Changes |
|------|---------|
| `native/src/txlog/arrow_ffi.rs` | Replaced with unified schema (dynamic partitions, List\<Utf8\>, optional stats) |
| `native/src/txlog/arrow_ffi_tests.rs` | Updated for new schema (column indices, List\<Utf8\>, partition columns) |
| `native/src/txlog/partition_pruning.rs` | +6 filter variants, +`can_skip_by_stats()` with truncation awareness |
| `native/src/txlog/jni.rs` | +3 JNI entry points (FR1, FR2, FR3), +1 test helper (`nativeTestListFilesRoundtrip`) |
| `native/src/txlog/purge.rs` | +`read_next_retained_files_batch_ffi()` |
| `native/src/txlog/mod.rs` | +2 module declarations |
| `native/src/txlog/integration_tests.rs` | +6 FR integration tests (data skipping, Arrow import, range elimination) |
| `native/src/split_searcher/async_impl.rs` | +`optimize_query_with_field_stats()`, integration in search path |
| `native/src/split_searcher/jni_search.rs` | FR4 integration in aggregation path |
| `native/src/split_searcher/jni_lifecycle.rs` | Extract field_min/max_values from splitConfigMap |
| `native/src/split_searcher/types.rs` | +field_min_values, +field_max_values on CachedSearcherContext |
| `native/src/split_cache_manager/manager.rs` | +file_field_stats cache, +put_all_file_field_stats(), +get_file_field_stats() |

### New Java Files
| File | Purpose |
|------|---------|
| `ArrowFfiListFilesIntegrationTest.java` | 6 end-to-end tests: no filter, partition, data skip, combined, empty, column count |

### Modified Java Files
| File | Changes |
|------|---------|
| `PartitionFilter.java` | +3 string operators: `stringStartsWith`, `stringEndsWith`, `stringContains`; null element validation in `in()` |
| `TransactionLogReader.java` | +2 private native methods + public wrappers with validation (FR1, FR3), +1 test helper |
| `TransactionLogWriter.java` | +1 private native method + public wrapper with validation (FR2) |
