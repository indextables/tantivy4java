# Arrow FFI Native Statistics

Compute per-column min/max statistics in Rust during `addArrowBatch`, eliminating per-row JVM overhead for data skipping statistics.

## Overview

Previously, statistics were computed on the JVM side per-row during `write()`:

```scala
override def write(record: InternalRow): Unit = {
  stats.updateRow(record)   // per-row JVM statistics (removed)
  bridge.bufferRow(record)
}
```

Now, statistics are computed natively in Rust during `addArrowBatch` using the existing `StatisticsAccumulator` infrastructure (shared with the parquet companion indexing path). The JVM side can skip `stats.updateRow()` entirely.

## Enabling Statistics

Add `"stats": true` to fields in the `fieldConfigJson` passed to `beginSplitFromArrow`:

```java
String fieldConfig = """
[
  {"name": "id",    "type": "i64",  "stats": true},
  {"name": "score", "type": "f64",  "stats": true},
  {"name": "name",  "type": "text", "tokenizer": "raw", "stats": true},
  {"name": "active","type": "bool", "stats": true},
  {"name": "blob",  "type": "bytes"}
]
""";

long handle = QuickwitSplit.beginSplitFromArrow(
    schemaAddr, partitionCols, heapSize,
    fieldConfig, maxDocsPerSplit, outputDir);
```

Fields without `"stats": true` (or with `"stats": false`) are not tracked.

## Retrieving Statistics

Use `finishAllSplitsRaw()` to get the raw result maps which include statistics:

```java
List<Map<String, Object>> results = QuickwitSplit.finishAllSplitsRaw(handle, outputDir);

for (Map<String, Object> result : results) {
    // Standard split metadata (same keys as PartitionSplitResult)
    String splitPath = (String) result.get("splitPath");
    long numDocs = (Long) result.get("numDocs");
    String docMappingJson = (String) result.get("docMappingJson");

    // Statistics (present only when stats columns were configured)
    @SuppressWarnings("unchecked")
    Map<String, String> minValues = (Map<String, String>) result.get("minValues");
    @SuppressWarnings("unchecked")
    Map<String, String> maxValues = (Map<String, String>) result.get("maxValues");

    if (minValues != null) {
        // minValues: {"id": "0", "score": "1.5", "name": "alice", "active": "false"}
        // maxValues: {"id": "99", "score": "99.5", "name": "zoe", "active": "true"}
    }
}
```

The existing `finishAllSplits()` still works and returns `List<PartitionSplitResult>` — it simply does not expose the statistics fields.

## Statistics Format

Values are returned as strings. The format depends on the Arrow column type:

| Arrow Type | Stats Format | Example min | Example max |
|---|---|---|---|
| Int32, Int64 | Decimal string | `"0"` | `"99"` |
| Float32, Float64 | Decimal string | `"1.5"` | `"99.5"` |
| Boolean | `"false"` / `"true"` | `"false"` | `"true"` |
| Utf8 | Raw string | `"alice"` | `"zoe"` |
| Date32 | Microseconds string | `"1705276800000000"` | `"1735689600000000"` |
| Timestamp(μs) | Microseconds string | `"1705311000000000"` | `"1735689600000000"` |

Non-eligible types (Binary, JSON, nested types) are silently skipped — no accumulator is created.

## Per-Partition Tracking

Statistics are tracked per partition. Each `PartitionWriter` has its own set of accumulators, so the min/max values in each result map reflect only the rows in that partition:

```java
List<Map<String, Object>> results = QuickwitSplit.finishAllSplitsRaw(handle, outputDir);

for (Map<String, Object> result : results) {
    String partitionKey = (String) result.get("partitionKey");
    // e.g. "event_date=2023-01-15" → stats only for rows in that partition
    Map<String, String> minValues = (Map<String, String>) result.get("minValues");
    Map<String, String> maxValues = (Map<String, String>) result.get("maxValues");
}
```

## Null Handling

Null values are skipped (same as the JVM-side `DatasetStatistics` behavior). A column where every row is null produces no entry in `minValues`/`maxValues`.

## String Truncation

The `StatisticsAccumulator` supports string truncation to prevent long values from bloating statistics. The truncation length is currently set to 0 (disabled) for the Arrow FFI path — the JVM side applies `StatisticsTruncation` before writing to the transaction log, consistent with the existing Spark integration pattern.

## Split Rolling

Statistics survive automatic split rolling. When a partition writer is rolled (due to `maxDocsPerSplit`), its accumulators are finalized into the rolled split's result. The replacement writer gets fresh accumulators.

## Spark Integration

The JVM-side Spark writer can derive the `fieldConfigJson` with stats flags from existing configuration:

```scala
private def buildFieldConfigJson(): String = {
  val statsColumns = tantivyOptions.getStatsColumns  // from spark.indextables.dataSkipping.statsColumns
  val numIndexedCols = tantivyOptions.getNumIndexedCols  // from spark.indextables.dataSkipping.numIndexedCols

  writeSchema.fields.zipWithIndex.map { case (field, idx) =>
    val fieldType = resolveFieldType(field)
    val tokenizer = resolveTokenizer(field)
    val enableStats = statsColumns.contains(field.name) ||
      (numIndexedCols < 0 || idx < numIndexedCols)

    s"""{"name":"${field.name}","type":"$fieldType"$tokenizer,"stats":$enableStats}"""
  }.mkString("[", ",", "]")
}

// Use finishAllSplitsRaw to get statistics
val results = QuickwitSplit.finishAllSplitsRaw(handle, outputDir)
for (result <- results.asScala) {
  val minValues = Option(result.get("minValues")).map(_.asInstanceOf[java.util.Map[String, String]].asScala.toMap)
  val maxValues = Option(result.get("maxValues")).map(_.asInstanceOf[java.util.Map[String, String]].asScala.toMap)
  // Apply StatisticsTruncation on JVM side before writing to transaction log
}
```

## Architecture

```
fieldConfigJson ("stats": true)
        │
        ▼
begin_split_from_arrow()
  ├─ parse_field_config_json() → stats_columns HashSet
  └─ create_partition_writer() → StatisticsAccumulator per stats column
        │
        ▼
add_arrow_batch()
  └─ build_doc_from_arrow_row()
       └─ add_arrow_value_to_doc()  ← existing observation hooks
            └─ accumulator.observe_i64/f64/string/bool/timestamp_micros()
        │
        ▼
finalize_partition_writer_into_split()
  └─ accumulator.finalize() → ColumnStatisticsResult
       └─ convert typed min/max to string maps
        │
        ▼
JNI: "minValues" / "maxValues" in result HashMap
        │
        ▼
Java: finishAllSplitsRaw() → List<Map<String, Object>>
```

The `StatisticsAccumulator` is the same implementation used by `create_split_from_parquet` in the parquet companion path (`statistics.rs`).

## Result Map Keys

`finishAllSplitsRaw()` returns maps with all the standard keys plus statistics:

| Key | Type | Description |
|---|---|---|
| `partitionKey` | `String` | Hive-style partition path |
| `partitionValues` | `Map<String, String>` | Column name → value |
| `splitPath` | `String` | Output file path |
| `splitId` | `String` | UUID |
| `numDocs` | `Long` | Document count |
| `footerStartOffset` | `Long` | Split footer byte offset |
| `footerEndOffset` | `Long` | Split footer end byte offset |
| `docMappingJson` | `String` | Field schema for read path |
| `uncompressedSizeBytes` | `Long` | Total uncompressed index size |
| `hotcacheStartOffset` | `Long` | Hotcache byte offset |
| `hotcacheLength` | `Long` | Hotcache byte length |
| `createTimestamp` | `Long` | Unix epoch seconds |
| `maturity` | `String` | Split maturity status |
| `numMergeOps` | `Long` | Merge operation count |
| `deleteOpstamp` | `Long` | Delete operation stamp |
| **`minValues`** | **`Map<String, String>`** | **Per-column min values (absent if no stats)** |
| **`maxValues`** | **`Map<String, String>`** | **Per-column max values (absent if no stats)** |

## File Reference

| File | Changes |
|---|---|
| `native/src/parquet_companion/arrow_ffi_import.rs` | `FieldConfigParseResult` with `stats_columns`, accumulators on `PartitionWriter`, finalization in `finalize_partition_writer_into_split` and `roll_partition_split`, 3 new tests |
| `native/src/parquet_companion/statistics.rs` | Existing `StatisticsAccumulator` (no changes, reused) |
| `native/src/quickwit_split/jni_functions.rs` | `minValues`/`maxValues` emitted as nested HashMaps in both `finishAllSplits` and `rollPartitionSplit` JNI functions |
| `src/.../QuickwitSplit.java` | New `finishAllSplitsRaw()` method returning `List<Map<String, Object>>` |
