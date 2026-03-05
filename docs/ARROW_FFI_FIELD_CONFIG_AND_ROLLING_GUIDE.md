# Arrow FFI Field Configuration, Metadata, and Split Rolling

This guide covers three features added to the Arrow FFI write path that bring it to parity with the TANT batch path.

## Background

The Arrow FFI write path streams columnar data from the JVM into Rust via the Arrow C Data Interface:

```
beginSplitFromArrow(schema, partitionCols, heapSize, ...) → handle
addArrowBatch(handle, arrayAddr, schemaAddr)              → cumulativeDocCount
finishAllSplits(handle, outputDir)                        → List<PartitionSplitResult>
```

Previously, this path had three gaps compared to the TANT batch path:

1. **No field configuration** — Arrow Utf8 columns were all treated as raw text, breaking full-text search, JSON sub-field queries, and IP address queries.
2. **Missing metadata** — `PartitionSplitResult` lacked `docMappingJson`, hotcache offsets, uncompressed size, and other fields needed by the read path.
3. **No split rolling** — No way to cap documents per split. The caller couldn't trigger rolling because it has no visibility into per-partition doc counts inside Rust.

## Field Configuration (Priority 1)

### Problem

Arrow schemas carry data types but not indexing semantics. A `Utf8` column could be:

| Intended Type | Tantivy Behavior |
|---|---|
| **text** | Tokenized for full-text search (IndexQuery) |
| **string/keyword** | Exact match, "raw" tokenizer |
| **json** | Parsed for sub-field filter pushdown |
| **ip** | Parsed for CIDR range queries |

Without explicit configuration, all Utf8 columns defaulted to "raw" text — breaking 44 out of 380 Spark tests.

### Solution

Pass a `fieldConfigJson` parameter to `beginSplitFromArrow`:

```java
String fieldConfig = """
[
  {"name": "id",       "type": "i64"},
  {"name": "content",  "type": "text", "tokenizer": "default"},
  {"name": "metadata", "type": "json"},
  {"name": "ip_addr",  "type": "ip"},
  {"name": "category", "type": "text", "tokenizer": "raw"}
]
""";

long handle = QuickwitSplit.beginSplitFromArrow(
    schemaAddr, partitionCols, heapSize, fieldConfig);
```

**Supported `type` values with overrides:**

| Type | Effect |
|---|---|
| `"text"` | Uses specified `tokenizer` (default: `"default"` which lowercases and tokenizes). `"raw"` is the schema derivation default and produces no override. |
| `"json"` | Column treated as JSON — enables sub-field queries and filter pushdown |
| `"ip"` | Column parsed as IP address — enables CIDR range queries |
| `"i64"`, `"f64"`, `"bool"`, `"datetime"`, `"bytes"` | No override needed — Arrow type mapping handles these correctly |

Fields not listed in the JSON array use default Arrow type inference.

### Rust internals

`parse_field_config_json()` in `arrow_ffi_import.rs` converts the JSON into a `SchemaDerivationConfig`:

- `"json"` → `config.json_fields`
- `"ip"` → `config.ip_address_fields`
- `"text"` with non-raw tokenizer → `config.tokenizer_overrides`

The config is stored in `ArrowFfiSplitContext.schema_config` and passed to both `derive_tantivy_schema_with_mapping()` (schema creation) and `add_arrow_value_to_doc()` (per-row indexing), ensuring consistent field handling.

### Backward compatibility

The 3-arg overload still works and passes `null` for fieldConfigJson:

```java
// Still valid — all Utf8 columns default to raw text
long handle = QuickwitSplit.beginSplitFromArrow(schemaAddr, partitionCols, heapSize);
```

## Extended PartitionSplitResult Metadata (Priority 2)

### Problem

`PartitionSplitResult` only exposed 7 fields. The read path needs `docMappingJson` for fast field detection and aggregate pushdown, plus hotcache offsets for cache prewarming.

### Solution

`PartitionSplitResult` now includes 15 fields:

| Field | Type | Source |
|---|---|---|
| `partitionKey` | `String` | Hive-style partition path |
| `partitionValues` | `Map<String, String>` | Column name → value |
| `splitPath` | `String` | Output file path |
| `splitId` | `String` | UUID |
| `numDocs` | `long` | Document count |
| `footerStartOffset` | `long` | Split footer byte offset |
| `footerEndOffset` | `long` | Split footer end byte offset |
| `docMappingJson` | `String` | **NEW** — Field schema for read path |
| `uncompressedSizeBytes` | `long` | **NEW** — Total uncompressed index size |
| `hotcacheStartOffset` | `long` | **NEW** — Hotcache byte offset |
| `hotcacheLength` | `long` | **NEW** — Hotcache byte length |
| `createTimestamp` | `long` | **NEW** — Unix epoch seconds |
| `maturity` | `String` | **NEW** — Split maturity status |
| `numMergeOps` | `int` | **NEW** — Merge operation count |
| `deleteOpstamp` | `long` | **NEW** — Delete operation stamp |

### Usage

```java
List<PartitionSplitResult> results = QuickwitSplit.finishAllSplits(handle, outputDir);
for (PartitionSplitResult result : results) {
    // docMappingJson is the source of truth for fast fields, field types, etc.
    String docMapping = result.getDocMappingJson();

    // Hotcache offsets for cache prewarming
    long hotcacheStart = result.getHotcacheStartOffset();
    long hotcacheLen = result.getHotcacheLength();

    // Storage stats
    long uncompressed = result.getUncompressedSizeBytes();
}
```

The `docMappingJson` comes from Rust's `extract_doc_mapping_from_index()`, which inspects the actual tantivy schema. This is the same source of truth used by the TANT batch path.

## Automatic Split Rolling (Priority 3)

### Problem

The TANT batch path supports `maxRowsPerSplit` — when a partition exceeds the threshold, the current split is finalized and a new writer starts. The Arrow FFI path had no equivalent. The caller can't implement this because it has no visibility into per-partition document counts (Rust routes rows internally by partition column values).

### Solution

Split rolling is handled automatically inside Rust during `addArrowBatch`. When a partition writer reaches the `maxDocsPerSplit` threshold:

1. The current writer is committed and finalized into a split file
2. The result is accumulated in an internal `rolled_splits` list
3. A fresh writer is created for the partition
4. Writing continues seamlessly

Rolled splits are prepended to the results returned by `finishAllSplits`.

### API

```java
// Enable auto-rolling: 1M docs per split, output to /tmp/splits
long handle = QuickwitSplit.beginSplitFromArrow(
    schemaAddr,
    partitionCols,
    heapSize,
    fieldConfigJson,
    1_000_000,        // maxDocsPerSplit (0 = unlimited)
    "/tmp/splits"     // outputDir (required when maxDocsPerSplit > 0)
);

// Add batches as usual — rolling happens transparently
while (hasMoreData()) {
    QuickwitSplit.addArrowBatch(handle, arrayAddr, batchSchemaAddr);
}

// finishAllSplits returns ALL splits: auto-rolled + final remaining
List<PartitionSplitResult> results =
    QuickwitSplit.finishAllSplits(handle, "/tmp/splits");

// results may contain multiple splits per partition
for (PartitionSplitResult r : results) {
    System.out.printf("Partition '%s': %d docs → %s%n",
        r.getPartitionKey(), r.getNumDocs(), r.getSplitPath());
}
```

### Behavior details

- **`maxDocsPerSplit = 0`** (default): No rolling. All docs for a partition go into one split.
- **`outputDir` required**: When `maxDocsPerSplit > 0`, Rust needs the output directory to write rolled splits during `addArrowBatch`. An error is thrown if it's missing.
- **Per-partition threshold**: Each partition tracks its own doc count independently.
- **Empty writers skipped**: If a partition is rolled and no more rows arrive for it, the empty replacement writer is skipped in `finishAllSplits`.
- **Result ordering**: Rolled splits appear first (in chronological order), followed by the final remaining splits.

### Manual rolling

The `rollPartitionSplit` API is also available for explicit manual rolling:

```java
PartitionSplitResult rolled = QuickwitSplit.rollPartitionSplit(
    handle, "event_date=2023-01-15", "/tmp/splits");
```

This is useful when the caller has external criteria for rolling (e.g., time-based rather than count-based).

### Rust internals

`add_arrow_batch` is `async` because rolling calls `create_quickwit_split` which is async. The JNI layer uses `QuickwitRuntimeManager::global().handle().block_on(...)` to bridge the async/sync boundary — the same pattern used by `finishAllSplits`.

A shared helper `finalize_partition_writer_into_split()` handles the commit → metadata → split creation → footer update pipeline, used by both auto-rolling and `finish_all_splits` to avoid duplication.

## Spark Integration Example

For Spark's `IndexTables4SparkArrowDataWriter`, the field config can be derived from existing Spark options:

```scala
private def buildFieldConfigJson(): String = {
  val tantivyOptions = IndexTables4SparkOptions(options)
  writeSchema.fields.map { field =>
    val fieldType = tantivyOptions.getTypemap(field.name).getOrElse(
      field.dataType match {
        case StringType                              => "text"
        case IntegerType | LongType                  => "i64"
        case FloatType | DoubleType                  => "f64"
        case BooleanType                             => "bool"
        case TimestampType | DateType                => "datetime"
        case _: StructType | _: ArrayType | _: MapType => "json"
        case BinaryType                              => "bytes"
        case _                                       => "text"
      })
    val tokenizer = tantivyOptions.getTokenizer(field.name)
      .map(t => s""","tokenizer":"$t"""").getOrElse("")
    s"""{"name":"${field.name}","type":"$fieldType"$tokenizer}"""
  }.mkString("[", ",", "]")
}

// Usage
nativeHandle = QuickwitSplit.beginSplitFromArrow(
  schemaAddr, partitionColsArray, heapSize,
  buildFieldConfigJson(),
  maxRowsPerSplit,  // from spark.indextables.write.maxRowsPerSplit
  outputDir)
```

## File Reference

| File | Changes |
|---|---|
| `native/src/parquet_companion/arrow_ffi_import.rs` | `parse_field_config_json`, `schema_config` on context, auto-rolling in `add_arrow_batch`, `finalize_partition_writer_into_split` helper |
| `native/src/quickwit_split/jni_functions.rs` | JNI signatures updated for field config, max docs, output dir; `addArrowBatch` uses `block_on`; all Priority 2 metadata fields passed through |
| `src/.../QuickwitSplit.java` | `PartitionSplitResult` expanded to 15 fields; `beginSplitFromArrow` overloads; `rollPartitionSplit` API |
