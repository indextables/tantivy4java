# indextables_spark Arrow FFI Write Integration Guide

This guide covers how to integrate the new Arrow FFI write path in tantivy4java for creating Quickwit splits directly from Spark's columnar batches. It replaces the row-at-a-time JNI overhead with batch-level Arrow C Data Interface transfers.

---

## 1. Architecture Overview

### Current Write Pipeline (Row-at-a-time)

```
Spark InternalRow
  → TantivySearchEngine.addDocument() [per-row JNI call]
  → Tantivy Index (temp dir)
  → QuickwitSplit.convertIndexFromPath()
  → .split file
  → S3/Azure upload
```

### New Arrow FFI Write Pipeline (Columnar)

```
Spark ColumnarBatch
  → ArrowFfiBridge.exportBatch() [one JNI call per batch]
  → FFI memory addresses
  → QuickwitSplit.addArrowBatch() [Rust imports batch, routes to per-partition writers]
  → QuickwitSplit.finishAllSplits()
  → .split files (one per partition)
  → S3/Azure upload per partition
```

### When to Use Which Path

| Path | Use When |
|------|----------|
| **Arrow FFI** (new) | New writes from Spark DataFrames, bulk loads, any `DataWriter[ColumnarBatch]` path |
| **Row-at-a-time** (legacy) | Fallback when Spark doesn't support columnar writes, single-row upserts |
| **Parquet companion** | External parquet files already exist; indexing only, no doc storage in split |

### Performance Comparison

| Metric | Row-at-a-time | Arrow FFI |
|--------|---------------|-----------|
| JNI calls per batch (4096 rows) | 4,096 | 1 |
| Serialization | Per-field Java→JNI→Rust | Zero-copy FFI pointer transfer |
| Memory copies | Per-field value copying | Zero (shared memory via FFI) |
| Partition routing | Java-side HashMap lookup per row | Rust-side extraction from Arrow columns |

---

## 2. Arrow FFI Write Integration

### Lifecycle Pattern

```java
long handle = 0;
try {
    // 1. Export schema via ArrowFfiBridge
    long schemaAddr = ArrowFfiBridge.exportSchema(arrowSchema);

    // 2. Begin split creation
    String[] partitionCols = new String[]{"event_date", "region"};
    handle = QuickwitSplit.beginSplitFromArrow(schemaAddr, partitionCols,
                                                Index.Memory.DEFAULT_HEAP_SIZE);

    // 3. Stream batches
    for (ColumnarBatch batch : batches) {
        long arrayAddr = ArrowFfiBridge.exportBatch(batch);
        long batchSchemaAddr = ArrowFfiBridge.exportBatchSchema(batch);
        long totalDocs = QuickwitSplit.addArrowBatch(handle, arrayAddr, batchSchemaAddr);
    }

    // 4. Finalize all partition splits
    List<QuickwitSplit.PartitionSplitResult> results =
        QuickwitSplit.finishAllSplits(handle, "/tmp/output");
    handle = 0; // handle consumed by finishAllSplits

    // 5. Upload each partition's split to cloud
    for (QuickwitSplit.PartitionSplitResult result : results) {
        uploadToCloud(result.getSplitPath(), buildCloudPath(result));
    }
} finally {
    if (handle != 0) {
        QuickwitSplit.cancelSplit(handle);
    }
}
```

### Memory Lifecycle

- **Schema export**: JVM allocates `FFI_ArrowSchema` struct. Rust takes ownership via `std::ptr::replace` (writes empty sentinel back). JVM memory is freed when GC collects the original schema.
- **Batch export**: Same pattern — JVM retains buffer ownership until Rust import completes. Since `addArrowBatch()` is synchronous, buffers are safe for the duration of the call.
- **After import**: Rust owns Arrow `ArrayData` backed by the original JVM buffers. The `align_buffers()` call may copy data if alignment is wrong (rare).

### Error Handling

```java
long handle = QuickwitSplit.beginSplitFromArrow(schemaAddr, partitionCols, heapSize);
try {
    // ... addArrowBatch calls ...
    List<PartitionSplitResult> results = QuickwitSplit.finishAllSplits(handle, outputDir);
    handle = 0; // consumed
    return results;
} catch (Exception e) {
    if (handle != 0) {
        QuickwitSplit.cancelSplit(handle);
    }
    throw e;
}
```

---

## 3. Partition Handling with Arrow FFI

### Key Design: Partition Routing in Rust

Unlike the current Java-side partition routing (where `partitionWriters` is a `Map` in Java), the Arrow FFI path routes partitions **in Rust**:

1. Java passes full batches including partition columns via FFI
2. Java passes partition column names at `beginSplitFromArrow()` time
3. Rust extracts partition values per-row from the Arrow columns
4. Rust lazily creates per-partition `IndexWriter` on first row for each new partition value
5. Non-partition columns are indexed; partition columns are excluded from the tantivy index

### Partition Flow

```
beginSplitFromArrow(schema, ["event_date", "region"], heapSize)
  → Rust identifies partition columns in schema
  → Rust derives tantivy schema from non-partition columns only

addArrowBatch(handle, batch)
  → For each row:
    1. Extract partition values: event_date="2023-01-15", region="us"
    2. Build key: "event_date=2023-01-15/region=us"
    3. Get-or-create PartitionWriter for that key
    4. Convert non-partition columns to TantivyDocument
    5. Add document to that partition's IndexWriter

finishAllSplits(handle, outputDir)
  → Returns List<PartitionSplitResult>, one per partition:
    - partitionKey: "event_date=2023-01-15/region=us"
    - partitionValues: {"event_date": "2023-01-15", "region": "us"}
    - splitPath: "/tmp/output/event_date=2023-01-15/region=us/part-uuid.split"
    - numDocs: 1500
```

### Java-Side Integration

```java
// In DataWriter[ColumnarBatch].commit():
List<PartitionSplitResult> results = QuickwitSplit.finishAllSplits(handle, localTempDir);

// Construct AddAction per partition
for (PartitionSplitResult result : results) {
    AddAction action = new AddAction();
    action.setSplitPath(result.getSplitPath());
    action.setPartitionValues(result.getPartitionValues());
    action.setNumDocs(result.getNumDocs());
    action.setFooterStartOffset(result.getFooterStartOffset());
    action.setFooterEndOffset(result.getFooterEndOffset());
    actions.add(action);
}
```

### Memory Consideration

Each partition's `IndexWriter` uses `heapSize` bytes. For tables with many partitions in one task:

```
Total memory = numPartitions × heapSize
```

For high-cardinality partitioning (e.g., daily partitions across years), use a smaller heap:

```java
// 50MB per partition (default) — fine for <20 partitions
long handle = QuickwitSplit.beginSplitFromArrow(schemaAddr, partitionCols,
                                                Index.Memory.DEFAULT_HEAP_SIZE);

// For high-cardinality partitioning (100+ partitions), use minimum heap
long handle = QuickwitSplit.beginSplitFromArrow(schemaAddr, partitionCols,
                                                Index.Memory.MIN_HEAP_SIZE);
```

---

## 4. Split Rolling Integration

### Current Pattern

```java
// Current row-at-a-time split rolling
void addDocument(InternalRow row) {
    writer.addDocument(doc);
    currentRowCount++;
    if (currentRowCount >= maxRowsPerSplit) {
        maybeRollSplit(); // finalize current, start new
    }
}
```

### Arrow FFI Pattern

`addArrowBatch()` returns cumulative total doc count. Check after each batch:

```java
long totalDocs = 0;
long handle = QuickwitSplit.beginSplitFromArrow(schemaAddr, partitionCols, heapSize);
List<PartitionSplitResult> allResults = new ArrayList<>();

for (ColumnarBatch batch : batches) {
    long arrayAddr = ArrowFfiBridge.exportBatch(batch);
    long schemaAddr = ArrowFfiBridge.exportBatchSchema(batch);
    totalDocs = QuickwitSplit.addArrowBatch(handle, arrayAddr, schemaAddr);

    // Check split rolling threshold
    if (totalDocs >= maxRowsPerSplit) {
        // Finalize all current partitions
        List<PartitionSplitResult> results =
            QuickwitSplit.finishAllSplits(handle, localTempDir);
        allResults.addAll(results);

        // Start fresh context for remaining batches
        handle = QuickwitSplit.beginSplitFromArrow(schemaAddr, partitionCols, heapSize);
        totalDocs = 0;
    }
}

// Finalize remaining data
if (totalDocs > 0) {
    allResults.addAll(QuickwitSplit.finishAllSplits(handle, localTempDir));
}
```

**Note**: Split rolling is per-context (all partitions roll together). For per-partition rolling (where partition A rolls at 100K rows while partition B continues), a `maxRowsPerSplit` parameter in Rust would be needed — this can be a future enhancement.

---

## 5. Schema Conversion

### Current Path
```
Spark StructType → SparkToTantivyConverter → tantivy SchemaBuilder calls
```

### New Arrow FFI Path
```
Spark StructType → ArrowUtils.toArrowSchema() → FFI export → Rust derive_tantivy_schema_with_mapping()
```

### Field Type Mapping

| Spark Type | Arrow Type | Tantivy Type | Notes |
|------------|-----------|--------------|-------|
| `StringType` | `Utf8` | Text (raw tokenizer) | Indexed + stored + fast |
| `LongType` | `Int64` | I64 | Indexed + stored + fast |
| `IntegerType` | `Int32` | I64 | Widened to i64 |
| `DoubleType` | `Float64` | F64 | Indexed + stored + fast |
| `FloatType` | `Float32` | F64 | Widened to f64 |
| `BooleanType` | `Boolean` | Bool | Indexed + stored + fast |
| `TimestampType` | `Timestamp(us, tz)` | DateTime | Indexed + stored + fast |
| `DateType` | `Date32` | DateTime | Indexed + stored + fast |
| `StructType` | `Struct` | JSON | Stored + indexed |
| `ArrayType` | `List` | JSON | Stored + indexed |
| `DecimalType` | `Decimal128` | F64 | Lossy for values > 2^53 |
| `BinaryType` | `Binary` | Bytes | Indexed + stored |

### Key Difference from Companion Mode

Standard splits (Arrow FFI path) set **STORED** on all fields — documents are stored in tantivy's doc store. Companion mode omits STORED because parquet is the store.

### Tokenizer Configuration

Arrow schema metadata doesn't carry tokenizer information. By default, all text fields use the "raw" tokenizer (exact match). To use different tokenizers (e.g., "default" for full-text search), pass tokenizer configuration separately — this is a future enhancement.

---

## 6. DataWriter[InternalRow] vs DataWriter[ColumnarBatch] Migration

### Current Implementation

```java
class IndexTables4SparkDataWriter extends DataWriter[InternalRow] {
    void write(InternalRow row) {
        // Per-row JNI call
        tantivySearchEngine.addDocument(row);
        maybeRollSplit();
    }
}
```

### New Columnar Implementation

```java
class IndexTables4SparkColumnarDataWriter extends DataWriter[ColumnarBatch] {
    private long ffiHandle;

    IndexTables4SparkColumnarDataWriter(StructType schema, String[] partitionCols) {
        long schemaAddr = ArrowFfiBridge.exportSchema(
            ArrowUtils.toArrowSchema(schema));
        this.ffiHandle = QuickwitSplit.beginSplitFromArrow(
            schemaAddr, partitionCols, Index.Memory.DEFAULT_HEAP_SIZE);
    }

    void write(ColumnarBatch batch) {
        long arrayAddr = ArrowFfiBridge.exportBatch(batch);
        long schemaAddr = ArrowFfiBridge.exportBatchSchema(batch);
        QuickwitSplit.addArrowBatch(ffiHandle, arrayAddr, schemaAddr);
    }

    WriterCommitMessage commit() {
        List<PartitionSplitResult> results =
            QuickwitSplit.finishAllSplits(ffiHandle, localTempDir);
        ffiHandle = 0;
        // Upload splits, construct commit message...
        return new CommitMessage(results);
    }

    void abort() {
        if (ffiHandle != 0) {
            QuickwitSplit.cancelSplit(ffiHandle);
            ffiHandle = 0;
        }
    }
}
```

### Writer Factory Integration

```java
class IndexTables4SparkWriterFactory implements DataWriterFactory {
    // Spark 3.4+: implement columnar write support
    boolean supportColumnarWrite() { return true; }

    DataWriter<ColumnarBatch> createColumnarWriter(int partitionId, long taskId) {
        return new IndexTables4SparkColumnarDataWriter(schema, partitionColumns);
    }

    // Fallback for non-columnar: Spark auto-converts ColumnarBatch → InternalRow
    DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        return new IndexTables4SparkDataWriter(schema, partitionColumns);
    }
}
```

### Simplified Partition Handling

With Arrow FFI, no `partitionWriters` map is needed in Java — Rust manages per-partition writers:

```java
// OLD: Java manages partition routing
Map<String, PartitionWriter> partitionWriters; // complex Java-side state
void write(InternalRow row) {
    String key = extractPartitionKey(row); // expensive per-row
    PartitionWriter pw = partitionWriters.getOrElseUpdate(key, ...);
    pw.addDocument(row);
}

// NEW: Rust manages partition routing
void write(ColumnarBatch batch) {
    QuickwitSplit.addArrowBatch(handle, exportedArray, exportedSchema);
    // That's it — Rust handles everything
}
```

---

## 7. Cloud Upload Pattern

### Two-Stage Pattern (Unchanged)

```java
// 1. Create splits locally
List<PartitionSplitResult> results =
    QuickwitSplit.finishAllSplits(handle, localTempDir);

// 2. Upload each partition's split to cloud
for (PartitionSplitResult result : results) {
    // Local path mirrors partition layout:
    //   /tmp/output/event_date=2023-01-15/region=us/part-uuid.split
    String localPath = result.getSplitPath();

    // Construct cloud target path
    String cloudPath = String.format("s3://%s/%s/%s/%s",
        bucket, tableName, result.getPartitionKey(),
        new File(localPath).getName());

    CloudStorageProviderFactory.uploadFile(localPath, cloudPath);

    // Cleanup local file
    new File(localPath).delete();
}
```

### Directory Structure

```
localTempDir/
├── event_date=2023-01-15/
│   ├── region=us/
│   │   └── part-abc123.split
│   └── region=eu/
│       └── part-def456.split
└── event_date=2023-01-16/
    └── region=us/
        └── part-ghi789.split
```

---

## 8. Statistics and Metadata

### Split Metadata from Arrow FFI

Each `PartitionSplitResult` includes:

| Field | Description |
|-------|-------------|
| `partitionKey` | Hive-style key, e.g., `"event_date=2023-01-15/region=us"` |
| `partitionValues` | `Map<String, String>` of col→value pairs |
| `splitPath` | Local filesystem path where split was written |
| `splitId` | Quickwit split UUID |
| `numDocs` | Document count in this split |
| `footerStartOffset` | Footer offset for lazy loading |
| `footerEndOffset` | End offset of footer |

### AddAction Construction

```java
for (PartitionSplitResult result : results) {
    AddAction action = AddAction.builder()
        .splitId(result.getSplitId())
        .splitPath(cloudPath)
        .numDocs(result.getNumDocs())
        .partitionValues(result.getPartitionValues())
        .footerStartOffset(result.getFooterStartOffset())
        .footerEndOffset(result.getFooterEndOffset())
        .build();
    actions.add(action);
}
```

---

## 9. Configuration Mapping

### Existing Spark Configs → Arrow FFI

| Spark Config | Arrow FFI Equivalent | Notes |
|-------------|---------------------|-------|
| `spark.indextables.indexing.typemap` | Arrow schema metadata | Type info carried in Arrow schema |
| `spark.indextables.indexing.fastfields` | All fields are fast by default | Future: config parameter |
| `spark.indextables.indexing.nonfastfields` | N/A (all fast) | Future: config parameter |
| `spark.indextables.indexing.tokenizer` | Default "raw" tokenizer | Future: per-field config |
| `spark.indextables.text.maxTokenLength` | N/A | Future: config parameter |
| `spark.indextables.indexing.storeonlyfields` | N/A (all stored+indexed) | Future: config parameter |
| `spark.indextables.indexing.indexonlyfields` | N/A (all stored+indexed) | Future: config parameter |
| `spark.indextables.maxRowsPerSplit` | Check `addArrowBatch()` return value | See Split Rolling section |
| `__partition_columns` | `partitionColumns` parameter | Passed to `beginSplitFromArrow()` |

### Future Configuration Enhancement

For advanced field-level configuration (tokenizers, fast/stored flags), a JSON config parameter can be passed to `beginSplitFromArrow`:

```java
// Future API (not yet implemented)
String fieldConfig = """
{
  "fields": {
    "title": {"tokenizer": "default", "fast": true, "stored": true},
    "body": {"tokenizer": "default", "fast": false, "stored": true},
    "score": {"fast": true, "stored": false}
  }
}
""";
long handle = QuickwitSplit.beginSplitFromArrow(schemaAddr, partitionCols, heapSize, fieldConfig);
```

---

## 10. Error Handling & Resource Safety

### Handle Lifecycle Contract

Every `beginSplitFromArrow()` call MUST be matched by exactly one of:
- `finishAllSplits()` — normal completion
- `cancelSplit()` — abort/cleanup

### Pattern in DataWriter

```java
class ArrowFfiSplitWriter implements Closeable {
    private long handle;

    ArrowFfiSplitWriter(long schemaAddr, String[] partitionCols, long heapSize) {
        this.handle = QuickwitSplit.beginSplitFromArrow(schemaAddr, partitionCols, heapSize);
    }

    void addBatch(long arrayAddr, long schemaAddr) {
        if (handle == 0) throw new IllegalStateException("Writer already closed");
        QuickwitSplit.addArrowBatch(handle, arrayAddr, schemaAddr);
    }

    List<PartitionSplitResult> finish(String outputDir) {
        if (handle == 0) throw new IllegalStateException("Writer already closed");
        List<PartitionSplitResult> results = QuickwitSplit.finishAllSplits(handle, outputDir);
        handle = 0;
        return results;
    }

    @Override
    public void close() {
        if (handle != 0) {
            QuickwitSplit.cancelSplit(handle);
            handle = 0;
        }
    }
}
```

### Error Scenarios

| Scenario | Behavior |
|----------|----------|
| `addArrowBatch` fails | Handle still valid; can retry or cancel |
| `finishAllSplits` fails after partial writes | Some split files may exist on disk; cleanup needed |
| JVM crash between begin and finish | Temp directories auto-cleaned by OS |
| Schema mismatch in batch | `addArrowBatch` throws; handle still valid |
| Null FFI pointer | `addArrowBatch` throws; handle still valid |

### Rust-Side Safety

All JNI entry points are wrapped in `convert_throwable`, which catches Rust panics and converts them to Java exceptions. This prevents JVM crashes from Rust-side errors.

---

## 11. Merge-on-Write and Purge-on-Write Compatibility

Arrow FFI splits are **standard Quickwit splits** — identical format to those created by the row-at-a-time path or `convertIndexFromPath()`.

| Operation | Impact |
|-----------|--------|
| `evaluateAndExecuteMergeOnWrite()` | No changes needed — works on committed splits |
| `MergeSplitsExecutor` | No changes — merges standard splits |
| `PurgeOrphanedSplitsExecutor` | No changes — operates on transaction log |
| `SplitSearcher` queries | No changes — standard splits fully searchable |
| Document retrieval | No changes — docs stored in tantivy doc store |

---

## 12. Testing Strategy

### Unit Tests (Rust)

12 unit tests in `native/src/parquet_companion/arrow_ffi_import.rs`:

**Non-partitioned (7 tests):**
1. `test_begin_creates_schema` — tantivy schema derived correctly with STORED flag
2. `test_add_single_batch` — single batch, verify doc count
3. `test_add_multiple_batches` — streaming 3 batches, verify 300 docs
4. `test_finish_creates_valid_split` — full pipeline, verify split file created
5. `test_cancel_cleans_up` — verify temp dirs removed on cancel
6. `test_schema_mismatch_rejected` — wrong schema batch rejected
7. `test_all_field_types` — Bool, I64, F64, Utf8 round-trip

**Partitioned (5 tests):**
8. `test_partitioned_single_partition` — one partition value → 1 split
9. `test_partitioned_multiple_partitions` — 3 partition values → 3 splits
10. `test_partitioned_across_batches` — partition rows across multiple batches
11. `test_partition_columns_excluded_from_index` — partition cols not in tantivy schema
12. `test_multi_level_partitioning` — two partition columns, verify key format

Run with: `cd native && cargo test --lib parquet_companion::arrow_ffi_import`

### Integration Tests (Java)

Recommended integration tests for the indextables_spark team:

1. **Round-trip test**: Create Arrow batch → `beginSplitFromArrow` → `addArrowBatch` → `finishAllSplits` → open with `SplitSearcher` → verify all docs searchable and retrievable
2. **Partition test**: Multi-partition batch → verify correct number of splits with correct doc counts per partition → verify partition directory structure
3. **Split rolling test**: Large dataset with `maxRowsPerSplit` → verify multiple split groups created
4. **Type coverage test**: All Spark types (String, Long, Double, Boolean, Timestamp, Struct, Array) → verify correct tantivy field types
5. **Error handling test**: Cancel mid-stream → verify no resource leaks
6. **Mixed mode test**: Some tasks use Arrow FFI, others use row path → verify both produce compatible splits

### Running Existing Tests

```bash
# Rust unit tests (12 new + 292 existing)
cd native && cargo test --lib parquet_companion::arrow_ffi_import
cd native && cargo test --lib parquet_companion  # full suite

# Java tests (ensure native library is built first)
mvn clean package -DskipTests
mvn test -pl . -Dtest="ParquetCompanion*"
```
