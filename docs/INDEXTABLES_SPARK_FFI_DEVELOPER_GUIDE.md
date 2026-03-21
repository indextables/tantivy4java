# IndexTables Spark: FFI Developer Guide

> Integrating `indextables_spark` with the Rust-native transaction log and Arrow FFI interfaces.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Data Exchange: Two FFI Protocols](#data-exchange-two-ffi-protocols)
3. [Transaction Log: Rust-Native Implementation](#transaction-log-rust-native-implementation)
4. [Read Path: Arrow FFI Streaming](#read-path-arrow-ffi-streaming)
5. [Write Path: Arrow FFI Ingestion](#write-path-arrow-ffi-ingestion)
6. [Distributed Scanning Primitives](#distributed-scanning-primitives)
7. [Schema Deduplication](#schema-deduplication)
8. [Avro State Format (v4)](#avro-state-format-v4)
9. [Configuration Reference](#configuration-reference)
10. [Migration from Scala Transaction Log](#migration-from-scala-transaction-log)
11. [Error Handling and Diagnostics](#error-handling-and-diagnostics)

---

## Architecture Overview

IndexTables Spark embeds the Tantivy search engine directly within Spark executors
via JNI. The system has three major FFI boundaries:

```
┌─────────────────────────────────────────────────────────────────┐
│                     Spark Driver / Executors                    │
│  (Scala/Java)                                                   │
├─────────────────┬───────────────────────┬───────────────────────┤
│  Transaction     │  Read Path            │  Write Path           │
│  Log             │  (Arrow FFI)          │  (Arrow FFI)          │
│                  │                       │                       │
│  TxLogReader ──► │  ArrowFfiBridge ◄──── │  ArrowFfiWriteBridge  │
│  TxLogWriter ──► │  SplitSearchEngine    │  ──► QuickwitSplit    │
├─────────────────┴───────────────────────┴───────────────────────┤
│                        JNI Boundary                             │
├─────────────────────────────────────────────────────────────────┤
│                     Rust Native Layer                           │
│  (tantivy4java / indextables-native)                            │
│                                                                 │
│  txlog::jni ─── txlog::distributed ─── txlog::serialization    │
│  txlog::arrow_ffi ─── txlog::avro ─── txlog::schema_dedup      │
│                                                                 │
│  split_searcher ─── quickwit_split ─── prewarm                 │
└─────────────────────────────────────────────────────────────────┘
```

### Key Packages

| Layer | Package | Language |
|-------|---------|----------|
| Spark integration | `io.indextables.spark.*` | Scala |
| JNI txlog wrappers | `io.indextables.jni.txlog` | Java |
| JNI split wrappers | `io.indextables.tantivy4java.split` | Java |
| Native txlog | `tantivy4java::txlog` | Rust |
| Native split search | `tantivy4java::split_searcher` | Rust |

---

## Data Exchange: Two FFI Protocols

The system uses two distinct binary protocols for Rust-to-Java data exchange,
chosen based on the consumer:

### TANT Byte Buffers (Transaction Log Metadata)

Used for transaction log operations where data is consumed by Java code directly.
Returned as `byte[]` from JNI calls and parsed into Java data classes.

```
┌──────────────────────────────────────────────┐
│ Magic: 0x54414E54 ("TANT")         4 bytes   │
├──────────────────────────────────────────────┤
│ Document 0                                   │
│   field_count (u16)                          │
│   For each field:                            │
│     name_len (u16) + name (UTF-8)            │
│     type (u8): 0=text, 1=int, 3=bool, 6=json│
│     value_count (u16)                        │
│     values (type-dependent encoding)         │
├──────────────────────────────────────────────┤
│ Document 1 ...                               │
├──────────────────────────────────────────────┤
│ Offset Table                                 │
│   offset per document (u32 each)             │
├──────────────────────────────────────────────┤
│ Footer:                                      │
│   table_start_offset (u32)                   │
│   document_count (u32)                       │
│   Magic: 0x54414E54                4 bytes   │
└──────────────────────────────────────────────┘
```

Java consumption pattern:

```java
// Native call returns TANT byte buffer
byte[] buffer = TransactionLogReader.nativeGetSnapshotInfo(tablePath, configMap);

// Parse via BatchDocumentReader (TANT protocol decoder)
List<Map<String, Object>> maps = BatchDocumentReader.parseToMaps(buffer);

// Convert to typed Java object
TxLogSnapshotInfo info = TxLogSnapshotInfo.fromMap(maps.get(0));
```

### Arrow C Data Interface (Search Results & Bulk Data)

Used for search result streaming where data feeds directly into Spark's
columnar batch pipeline. Zero-copy pointer handoff — no serialization.

```
┌────────────────┐     FFI pointers      ┌─────────────────┐
│ Rust           │ ───────────────────►   │ Java/Spark      │
│ RecordBatch    │  ArrowArray*           │ ColumnarBatch   │
│                │  ArrowSchema*          │                 │
└────────────────┘                        └─────────────────┘
    No data copy — ownership transfers via pointer
```

Java/Scala consumption pattern:

```scala
// Allocate empty C structs on Java side
val (arrays, schemas, arrayAddrs, schemaAddrs) = bridge.allocateStructs(numCols)

// Native call fills the structs in-place via pointer
val rowCount = splitSearcher.nextBatch(arrayAddrs, schemaAddrs)

// Import into Spark ColumnarBatch (zero-copy)
val batch = bridge.importAsColumnarBatchStreaming(arrays, schemas, rowCount)
```

### When to Use Which Protocol

| Use Case | Protocol | Reason |
|----------|----------|--------|
| Transaction log metadata | TANT | Small payloads, Java-native consumption |
| File entry listing | TANT or Arrow | TANT for metadata ops; Arrow for Spark scans |
| Search result streaming | Arrow FFI | Zero-copy into Spark ColumnarBatch |
| Write path (Spark → Rust) | Arrow FFI | Zero-copy columnar ingestion |
| Checkpoint creation | JSON (via JNI) | Entries serialized as JSON string parameter |

---

## Transaction Log: Rust-Native Implementation

The Rust txlog module (`native/src/txlog/`) replaces the Scala `TransactionLog`
with native performance while maintaining full backward compatibility.

### Java API Classes

All classes are in `io.indextables.jni.txlog`:

```java
// ── Read Operations ──────────────────────────────────────────

TransactionLogReader.getSnapshotInfo(tablePath, config)
    → TxLogSnapshotInfo
    // Driver-side: reads _last_checkpoint + _manifest + lists versions
    // Cost: 1 GET + 1 GET + 1 LIST

TransactionLogReader.readManifest(tablePath, config, stateDir, manifestPath, metadataConfigJson)
    → List<TxLogFileEntry>
    // Executor-side: reads one Avro manifest file
    // Cost: 1 GET  (highly parallelizable)

TransactionLogReader.readPostCheckpointChanges(tablePath, config, versionPathsJson, metadataConfigJson)
    → TxLogChanges
    // Driver-side: reads incremental version files since checkpoint
    // Returns: added files + removed paths + skip actions

TransactionLogReader.getCurrentVersion(tablePath, config)
    → long
    // Cost: 1 GET + 1 LIST

// ── Write Operations ─────────────────────────────────────────

TransactionLogWriter.addFiles(tablePath, config, addsJson)
    → WriteResult  { version, retries, conflictedVersions }
    // Optimistic concurrency with exponential backoff + jitter

TransactionLogWriter.removeFile(tablePath, config, filePath)
    → long (version)

TransactionLogWriter.skipFile(tablePath, config, skipJson)
    → long (version)

TransactionLogWriter.createCheckpoint(tablePath, config, entriesJson, metadataJson, protocolJson)
    → LastCheckpointInfo  { version, size, format, numFiles }
```

### Config Map

All JNI methods accept a `Map<String, String> config` parameter that maps to the
Rust `DeltaStorageConfig`. Keys are extracted by the `build_storage_config()`
JNI helper:

```java
Map<String, String> config = new HashMap<>();

// AWS S3
config.put("aws_access_key_id", "AKIA...");
config.put("aws_secret_access_key", "secret...");
config.put("aws_session_token", "token...");    // Optional
config.put("aws_region", "us-east-1");
config.put("aws_endpoint", "https://...");      // Optional (MinIO, etc.)
config.put("aws_force_path_style", "true");     // Optional

// Azure Blob Storage
config.put("azure_storage_account_name", "myaccount");
config.put("azure_storage_account_key", "key...");
config.put("azure_bearer_token", "eyJ...");     // Alternative to key

// Schema dedup metadata (for manifest reads)
config.put("docMappingSchema.abc123...", "{...}");  // Schema registry entries
```

### Data Classes

**TxLogFileEntry** — 23 fields representing one split file in the table:

```java
public class TxLogFileEntry implements Serializable {
    // Required AddAction fields
    String path;                          // Relative path to split file
    long size;                            // File size in bytes
    long modificationTime;                // Epoch milliseconds
    boolean dataChange;                   // True if data modification
    long numRecords;                      // Document count (-1 if unknown)

    // Optional AddAction fields
    Map<String, String> partitionValues;  // Partition column→value
    String stats;                         // JSON statistics
    Map<String, String> minValues;        // Column min values
    Map<String, String> maxValues;        // Column max values
    long footerStartOffset;               // Split footer byte offset (-1 if absent)
    long footerEndOffset;                 // Split footer byte offset (-1 if absent)
    boolean hasFooterOffsets;             // Whether footer offsets are valid
    Map<String, String> splitTags;        // Arbitrary tags
    int numMergeOps;                      // Merge operation count
    String docMappingJson;                // Quickwit doc mapping schema
    long uncompressedSizeBytes;           // Uncompressed size (-1 if absent)
    long timeRangeStart;                  // Epoch micros (-1 if absent)
    long timeRangeEnd;                    // Epoch micros (-1 if absent)
    List<String> companionSourceFiles;    // Parquet file paths (companion mode)
    long companionDeltaVersion;           // Delta version indexed (-1 if absent)
    String companionFastFieldMode;        // "DISABLED"|"HYBRID"|"PARQUET_ONLY"

    // Streaming metadata
    long addedAtVersion;                  // Version when file was added
    long addedAtTimestamp;                // Epoch millis when file was added

    // Factory method from TANT buffer parse
    static TxLogFileEntry fromMap(Map<String, Object> map);
}
```

**TxLogSnapshotInfo** — checkpoint metadata for distributed reads:

```java
public class TxLogSnapshotInfo implements Serializable {
    long checkpointVersion;               // Checkpoint version number
    String stateDir;                      // State directory name
    List<String> manifestPaths;           // Manifest files to read in parallel
    List<String> postCheckpointPaths;     // Version files after checkpoint
    String protocolJson;                  // Protocol action as JSON
    String metadataJson;                  // Metadata action as JSON
    long numManifests;                    // Number of manifests
}
```

**TxLogChanges** — incremental changes since last checkpoint:

```java
public class TxLogChanges implements Serializable {
    List<TxLogFileEntry> addedFiles;      // New files added
    List<String> removedPaths;            // Files removed
    List<TxLogSkipAction> skipActions;    // Skip records
    long maxVersion;                      // Highest version seen
}
```

**WriteResult** — result of a write operation:

```java
public class WriteResult implements Serializable {
    long version;                         // Written version number
    int retries;                          // Number of conflict retries
    List<Long> conflictedVersions;        // Versions that conflicted
}
```

---

## Read Path: Arrow FFI Streaming

The read path streams search results from Rust to Spark as Arrow columnar batches
via the C Data Interface. This is the hot path — zero-copy is critical.

### Arrow Schema for FileEntry (23 Columns)

When file entries are exported via Arrow FFI (e.g., for Spark DataFrame scans of
the transaction log itself), the following schema is used:

| Col | Name | Arrow Type | Nullable | Notes |
|-----|------|-----------|----------|-------|
| 0 | `path` | Utf8 | NO | Split file path |
| 1 | `size` | Int64 | NO | File size bytes |
| 2 | `modification_time` | Int64 | NO | Epoch millis |
| 3 | `data_change` | Boolean | NO | |
| 4 | `num_records` | Int64 | YES | |
| 5 | `partition_values` | Utf8 | YES | JSON-encoded map |
| 6 | `stats` | Utf8 | YES | JSON statistics |
| 7 | `min_values` | Utf8 | YES | JSON-encoded map |
| 8 | `max_values` | Utf8 | YES | JSON-encoded map |
| 9 | `footer_start_offset` | Int64 | YES | |
| 10 | `footer_end_offset` | Int64 | YES | |
| 11 | `has_footer_offsets` | Boolean | YES | |
| 12 | `split_tags` | Utf8 | YES | JSON-encoded map |
| 13 | `num_merge_ops` | Int32 | YES | |
| 14 | `doc_mapping_json` | Utf8 | YES | |
| 15 | `uncompressed_size_bytes` | Int64 | YES | |
| 16 | `time_range_start` | Int64 | YES | Epoch micros |
| 17 | `time_range_end` | Int64 | YES | Epoch micros |
| 18 | `companion_source_files` | Utf8 | YES | JSON array |
| 19 | `companion_delta_version` | Int64 | YES | |
| 20 | `companion_fast_field_mode` | Utf8 | YES | |
| 21 | `added_at_version` | Int64 | NO | |
| 22 | `added_at_timestamp` | Int64 | NO | Epoch millis |

### Streaming Read Flow (CompanionColumnarPartitionReader)

This is the primary read path used by Spark to scan split files:

```
  Spark Catalyst                         Rust Native
  ─────────────                          ───────────
  1. Plan scan with filters
     └─► Create InputPartitions
         (one per AddAction/split)

  2. For each partition:
     CompanionColumnarPartitionReader
     │
     ├─ SplitReaderContext.createSplitSearchEngine()
     │   ├─ Reconstruct SplitMetadata from AddAction
     │   ├─ Validate footer offsets
     │   ├─ Create SplitCacheManager config
     │   └─ Return SplitSearchEngine
     │
     ├─ Build SplitQuery from Spark filters
     │
     ├─ splitSearcher.startStreamingRetrieval(query, limit)
     │                                        │
     │                                        └─► Rust: execute query,
     │                                            prepare batch iterator
     │
     └─ Loop: next() / get()
        │
        ├─ bridge.allocateStructs(numCols)
        │   → ArrowArray[numCols], ArrowSchema[numCols]
        │   → arrayAddrs[numCols], schemaAddrs[numCols]
        │
        ├─ streamingSession.nextBatch(arrayAddrs, schemaAddrs)
        │                              │
        │                              └─► Rust: fill Arrow C structs
        │                                  at the provided addresses
        │                                  (zero-copy pointer handoff)
        │                                  Returns: row count
        │
        ├─ bridge.importAsColumnarBatchStreaming(arrays, schemas, rows)
        │   └─ For each column:
        │       Data.importVector(allocator, array, schema, provider)
        │       → FieldVector (owns the Arrow memory)
        │
        ├─ Attach partition columns as ConstantColumnVector
        │
        └─ Return ColumnarBatch to Spark
```

### ArrowFfiBridge API

```scala
class ArrowFfiBridge extends AutoCloseable {
  // Allocate empty Arrow C structs for FFI handoff
  def allocateStructs(numCols: Int): (
    Array[ArrowArray],    // Empty C structs to be filled by native
    Array[ArrowSchema],   // Empty C structs to be filled by native
    Array[Long],          // Memory addresses of ArrowArray structs
    Array[Long]           // Memory addresses of ArrowSchema structs
  )

  // Import filled structs into Spark ColumnarBatch (streaming - per-batch provider)
  def importAsColumnarBatchStreaming(
    arrays: Array[ArrowArray],
    schemas: Array[ArrowSchema],
    numRows: Int
  ): ColumnarBatch

  // Import filled structs into Spark ColumnarBatch (single-batch - reusable provider)
  def importAsColumnarBatch(
    arrays: Array[ArrowArray],
    schemas: Array[ArrowSchema],
    numRows: Int
  ): ColumnarBatch
}
```

> **Important**: Use `importAsColumnarBatchStreaming` for the streaming read path.
> It creates a fresh `CDataDictionaryProvider` per batch, preventing stale dictionary
> references across batches. Use `importAsColumnarBatch` only for single-shot imports.

### Type Hints for Non-Companion Splits

When reading non-companion split files, the partition reader sends type hints to
the native layer so Rust knows how to encode Arrow columns:

```scala
// Build type hints: alternating [fieldName, arrowType] pairs
val typeHints = Array("score", "i32", "price", "f32", "date", "date32")
```

Supported type hint values: `i32`, `i64`, `f32`, `f64`, `bool`, `date32`,
`timestamp_micros`, `string`, `binary`, and complex types as JSON
(`{"struct": [...]}`, `{"list": ...}`, `{"map": ...}`).

---

## Write Path: Arrow FFI Ingestion

The write path sends data from Spark to Rust via Arrow FFI for indexing into
Tantivy split files.

### Two-Layer Architecture

```
  Spark DataWriter<InternalRow>
  │
  │  Layer 1 (Spark constraint — will disappear when Spark adds DataWriter<ColumnarBatch>)
  │  ──────────────────────────────────────────────────────────────────────────────────
  │  ArrowFfiWriteBridge
  │  ├─ bufferRow(InternalRow)         // Accumulate rows into Arrow vectors
  │  ├─ exportBatch() → (arrayAddr, schemaAddr)  // Export when batch full
  │  └─ exportSchema() → schemaAddr    // One-time schema export
  │
  │  Layer 2 (Permanent — zero-copy FFI to Rust)
  │  ──────────────────────────────────────────────────────────────────────────────────
  │  QuickwitSplit native methods
  │  ├─ beginSplitFromArrow(schemaAddr, partCols, heap, fieldConfig, maxDocs, outDir)
  │  │   → nativeHandle
  │  ├─ addArrowBatch(handle, arrayAddrs, schemaAddrs)  // Rust imports Arrow columns
  │  └─ finishAllSplits(handle) → List<SplitMetadata>   // Commit and produce .split files
  │
  └─► Transaction log commit: addFiles(addActions)
```

### Write Flow (IndexTables4SparkArrowDataWriter)

```scala
class IndexTables4SparkArrowDataWriter extends DataWriter[InternalRow] {

  override def write(record: InternalRow): Unit = {
    ensureInitialized()                   // Lazy: beginSplitFromArrow on first call
    bridge.bufferRow(record)              // Buffer into Arrow VectorSchemaRoot
    if (bridge.bufferedRowCount >= batchSize) {
      flushBatch()                        // Export to Rust via FFI
    }
  }

  override def commit(): WriterCommitMessage = {
    bridge.flush()                        // Export remaining rows
    val splits = QuickwitSplit.finishAllSplitsRaw(nativeHandle)
    val addActions = buildAddActions(splits)
    // Upload to cloud if needed, then return actions for txlog commit
  }
}
```

### Field Configuration JSON

The write path sends a field configuration JSON to the native layer describing
how each Spark column should be indexed:

```json
[
  {"name": "title",   "type": "text",     "tokenizer": "default"},
  {"name": "body",    "type": "text",     "tokenizer": "raw"},
  {"name": "score",   "type": "i64"},
  {"name": "price",   "type": "f64"},
  {"name": "active",  "type": "bool"},
  {"name": "created", "type": "datetime"},
  {"name": "payload", "type": "json"}
]
```

### Spark Type → Tantivy Type Mapping

| Spark DataType | Tantivy Field Type | Arrow Wire Type | Notes |
|----------------|-------------------|-----------------|-------|
| StringType | `text` | Utf8 | Tokenizer: `"raw"` (exact) or `"default"` (analyzed) |
| IntegerType | `i64` | Int64 | Tantivy only has i64 |
| LongType | `i64` | Int64 | |
| FloatType | `f64` | Float64 | Tantivy only has f64 |
| DoubleType | `f64` | Float64 | |
| BooleanType | `bool` | Bool | |
| TimestampType | `datetime` | Timestamp(MICRO, UTC) | Stored as epoch microseconds |
| DateType | `datetime` | Date(DAY) | Converted to epoch microseconds |
| BinaryType | `bytes` | Binary | |
| StructType | `json` | Utf8 (JSON) | Serialized as JSON string |
| ArrayType | `json` | Utf8 (JSON) | Serialized as JSON string |
| MapType | `json` | Utf8 (JSON) | Serialized as JSON string |

### ArrowFfiWriteConfig

```
spark.indextables.write.arrowFfi.enabled    = true     (default)
spark.indextables.write.arrowFfi.batchSize  = 8192     (rows per Arrow batch)
spark.indextables.indexWriter.heapSize      = 268435456 (256MB native heap)
```

---

## Distributed Scanning Primitives

The transaction log supports a driver/executor split pattern for distributed
Spark queries over large tables. This follows the same 2-phase pattern used
by the Delta and Iceberg table readers.

### Phase 1: Driver (Lightweight Metadata)

The driver reads minimal metadata to plan the distributed scan:

```scala
// Step 1: Get snapshot info (driver-side)
val config = buildConfigMap(sparkConf, hadoopConf)
val snapshot = TransactionLogReader.getSnapshotInfo(tablePath, config)

// snapshot contains:
//   checkpointVersion: 100
//   stateDir: "state-v00000000000000000100"
//   manifestPaths: ["manifest-0000.avro", "manifest-0001.avro", ...]
//   postCheckpointPaths: ["00000000000000000101.json", ...]
//   protocolJson: "{...}"
//   metadataJson: "{...}"

// Step 2: Read post-checkpoint changes (driver-side, small)
val changes = TransactionLogReader.readPostCheckpointChanges(
  tablePath, config,
  objectMapper.writeValueAsString(snapshot.getPostCheckpointPaths()),
  metadataConfigJson  // Schema registry from metadata.configuration
)

// Step 3: Distribute manifest paths to executors
val manifestTasks = snapshot.getManifestPaths().map { path =>
  new ManifestReadTask(tablePath, config, snapshot.getStateDir(), path)
}
```

### Phase 2: Executor (Parallel Manifest Reads)

Each executor reads one manifest file independently:

```scala
// Executor-side: read one manifest (parallelizable across cluster)
val entries: List[TxLogFileEntry] = TransactionLogReader.readManifest(
  tablePath, config,
  stateDir,           // From snapshot
  manifestPath,       // One manifest per executor task
  metadataConfigJson  // Schema registry for doc_mapping_ref → doc_mapping_json
)

// Each entry becomes one InputPartition for the scan
entries.foreach { entry =>
  createInputPartition(entry)
}
```

### Cost Model

| Operation | I/O Cost | Where |
|-----------|----------|-------|
| `getSnapshotInfo` | 1 GET + 1 GET + 1 LIST | Driver |
| `readPostCheckpointChanges` | N GETs (small JSON files) | Driver |
| `readManifest` | 1 GET (Avro binary) | Executor |
| `getCurrentVersion` | 1 GET + 1 LIST | Driver |
| `addFiles` | 1 LIST + 1 PUT (with retry) | Driver |
| `createCheckpoint` | N PUTs (manifests) + 2 PUTs (_manifest, _last_checkpoint) | Driver |

---

## Schema Deduplication

Split files carry a `docMappingJson` field containing the Quickwit schema. For
tables with thousands of splits sharing the same schema, this creates significant
storage overhead. The Rust txlog implements automatic deduplication.

### How It Works

```
Write Path:
  AddAction.docMappingJson = '{"fields":[{"name":"title","type":"text"},...]}'
                ↓
  SHA-256 hash of canonical (sorted-key) JSON → 16-char Base64 ref
                ↓
  Store in MetadataAction.configuration:
    "docMappingSchema.abc123XYZdef==" → '{"fields":[...]}'
                ↓
  Replace in AddAction:
    docMappingJson = null
    docMappingRef  = "abc123XYZdef=="

Read Path:
  AddAction.docMappingRef = "abc123XYZdef=="
                ↓
  Lookup in metadata_config["docMappingSchema.abc123XYZdef=="]
                ↓
  Restore: AddAction.docMappingJson = '{"fields":[...]}'
```

### Usage in Java

Schema dedup is transparent — the Rust layer handles it during `readManifest()`
and `readPostCheckpointChanges()` calls when you pass the `metadataConfigJson`
parameter. This parameter should contain the schema registry entries from the
table's `MetadataAction.configuration`:

```java
// Extract schema registry from metadata
ObjectMapper mapper = new ObjectMapper();
Map<String, String> metadataConfig = new HashMap<>();
MetadataAction metadata = parseMetadata(snapshot.getMetadataJson());
for (Map.Entry<String, String> entry : metadata.getConfiguration().entrySet()) {
    if (entry.getKey().startsWith("docMappingSchema.")) {
        metadataConfig.put(entry.getKey(), entry.getValue());
    }
}
String metadataConfigJson = mapper.writeValueAsString(metadataConfig);

// Pass to manifest reads — Rust restores full docMappingJson automatically
List<TxLogFileEntry> entries = TransactionLogReader.readManifest(
    tablePath, config, stateDir, manifestPath, metadataConfigJson);

// entries[i].getDocMappingJson() is fully restored (not null)
```

---

## Avro State Format (v4)

The transaction log uses an Avro-based state format for checkpoints, matching
the Scala implementation for full backward compatibility.

### Directory Structure

```
table_root/
├── _transaction_log/
│   ├── 00000000000000000000.json    ← Version 0 (protocol + metadata)
│   ├── 00000000000000000001.json    ← Version 1 (add actions)
│   ├── ...
│   ├── state-v00000000000000000100/ ← Checkpoint at version 100
│   │   ├── _manifest                ← JSON: StateManifest
│   │   ├── manifest-0000.avro       ← Avro binary: FileEntry records
│   │   ├── manifest-0001.avro
│   │   └── ...
│   └── _last_checkpoint             ← JSON: LastCheckpointInfo
├── splits/
│   ├── split-001.split
│   ├── split-002.split
│   └── ...
└── ...
```

### Avro Field IDs

The Avro schema uses explicit field IDs for stable evolution. These match the
Scala `AvroSchemas.scala` definitions exactly:

| Range | Purpose | Fields |
|-------|---------|--------|
| 100-109 | Basic file info | path(100), partitionValues(101), size(102), modificationTime(103), dataChange(104) |
| 110-119 | Statistics | stats(110), minValues(111), maxValues(112), numRecords(113) |
| 120-129 | Footer offsets | footerStartOffset(120), footerEndOffset(121), hasFooterOffsets(124) |
| 130-139 | Split metadata | splitTags(132), numMergeOps(134), docMappingRef(135), uncompressedSizeBytes(136) |
| 140-149 | Streaming metadata | addedAtVersion(140), addedAtTimestamp(141) |
| 150-159 | Companion mode | companionSourceFiles(150), companionDeltaVersion(151), companionFastFieldMode(152) |

### Backward Compatibility

The Rust implementation maintains full compatibility with Scala-written txlogs:

- **Version files**: JSON lines with gzip detection (Scala writes gzip, Rust reads both)
- **Avro manifests**: Same field IDs and schema structure
- **camelCase JSON**: `#[serde(rename_all = "camelCase")]` matches Scala serialization
- **Unknown fields**: `serde(default)` and Avro schema evolution handle gracefully

---

## Configuration Reference

### Storage Configuration Keys

Passed via the `config` map to all JNI transaction log methods:

| Key | Description | Required |
|-----|-------------|----------|
| `aws_access_key_id` | AWS access key | For S3 |
| `aws_secret_access_key` | AWS secret key | For S3 |
| `aws_session_token` | STS session token | Optional |
| `aws_region` | AWS region | For S3 |
| `aws_endpoint` | Custom S3 endpoint | Optional |
| `aws_force_path_style` | Path-style access | Optional (for MinIO) |
| `azure_storage_account_name` | Azure account | For Azure |
| `azure_storage_account_key` | Azure key | For Azure (or bearer token) |
| `azure_bearer_token` | OAuth bearer token | For Azure (or account key) |

### Spark Configuration Keys

Set via `spark.conf.set(...)` or in `spark-defaults.conf`:

| Key | Default | Description |
|-----|---------|-------------|
| **Read Path** | | |
| `spark.indextables.read.mode` | `"fast"` | `"fast"` (limit 250) or `"complete"` (unlimited) |
| `spark.indextables.read.batchOptimization.enabled` | `true` | Enable batch retrieval optimization |
| `spark.indextables.read.batchOptimization.profile` | `"balanced"` | `"conservative"`, `"balanced"`, `"aggressive"`, `"disabled"` |
| **Write Path** | | |
| `spark.indextables.write.arrowFfi.enabled` | `true` | Enable Arrow FFI write path |
| `spark.indextables.write.arrowFfi.batchSize` | `8192` | Rows per Arrow batch before flush |
| `spark.indextables.indexWriter.heapSize` | `268435456` | Native heap size (256MB) |
| **Indexing** | | |
| `spark.indextables.indexing.typemap.<field>` | | Field type override (e.g., `"text"`, `"i64"`) |
| `spark.indextables.indexing.tokenizer.<field>` | | Tokenizer override (e.g., `"raw"`, `"default"`) |
| `spark.indextables.indexing.fastfields` | | Comma-separated fast field names |
| `spark.indextables.indexing.text.maxTokenLength` | `255` | Max token length (Quickwit-compatible) |
| **L2 Disk Cache** | | |
| `spark.indextables.cache.disk.enabled` | auto | Auto-enabled if `/local_disk0` exists |
| `spark.indextables.cache.disk.path` | `/local_disk0/tantivy4spark_slicecache` | Cache directory |
| `spark.indextables.cache.disk.maxSize` | auto (2/3 disk) | Maximum cache size |
| `spark.indextables.cache.disk.compression` | `"lz4"` | `"lz4"`, `"zstd"`, or `"none"` |
| **Cloud Storage** | | |
| `spark.indextables.aws.accessKey` | | AWS access key |
| `spark.indextables.aws.secretKey` | | AWS secret key |
| `spark.indextables.aws.region` | | AWS region |
| `spark.indextables.s3.endpoint` | | Custom S3 endpoint |
| `spark.indextables.azure.accountName` | | Azure storage account |
| `spark.indextables.azure.accountKey` | | Azure account key |
| `spark.indextables.azure.bearerToken` | | Azure OAuth token |
| **Companion Mode** | | |
| `spark.indextables.companion.parquetTableRoot` | | Source Parquet table path |
| `spark.indextables.companion.fastFieldMode` | `"DISABLED"` | `"DISABLED"`, `"HYBRID"`, `"PARQUET_ONLY"` |

### Configuration Precedence

```
Read options (highest) → Spark session config → Hadoop config (lowest)
```

Both old-style (`spark.indextables.indexing.typemap.title = "text"`) and new-style
(`spark.indextables.indexing.typemap.text = "title,content"`) dual syntax is
supported for typemap, tokenizer, indexrecordoption, and tokenlength keys.

---

## Migration from Scala Transaction Log

### Interface Mapping

The Rust txlog exposes stateless JNI methods that map to the existing Scala
`TransactionLogInterface` methods:

| Scala TransactionLogInterface | Rust JNI (Java Wrapper) |
|-------------------------------|------------------------|
| `listFiles()` | `getSnapshotInfo()` + `readManifest()` per manifest + `readPostCheckpointChanges()` |
| `listFilesWithPartitionFilters()` | Same + client-side partition pruning on `TxLogFileEntry.partitionValues` |
| `addFile(addAction)` | `addFiles(tablePath, config, addsJson)` |
| `addFiles(addActions)` | `addFiles(tablePath, config, addsJson)` |
| `removeFile(path, ts)` | `removeFile(tablePath, config, path)` |
| `getSchema()` | `getSnapshotInfo()` → parse `metadataJson` → extract `schemaString` |
| `getMetadata()` | `getSnapshotInfo()` → parse `metadataJson` |
| `getProtocol()` | `getSnapshotInfo()` → parse `protocolJson` |
| `getCurrentVersion()` | `getCurrentVersion(tablePath, config)` |
| `commitMergeSplits()` | `removeFile()` per removal + `addFiles()` for new splits |
| `getCheckpointActions()` | `getSnapshotInfo()` + `readManifest()` per manifest |
| `createCheckpoint()` | `createCheckpoint(tablePath, config, entriesJson, metadataJson, protocolJson)` |

### Key Differences

1. **Stateless vs Stateful**: The Rust JNI methods are stateless — no persistent
   `TransactionLog` object. Each call creates its own storage connection. The Scala
   implementation maintains cached state.

2. **Two-Phase Read**: `listFiles()` becomes a two-step process:
   `getSnapshotInfo()` (driver) → `readManifest()` per manifest (executor).
   This enables distributed manifest reading across the Spark cluster.

3. **Conflict Retry Built-In**: `addFiles()` includes automatic optimistic
   concurrency retry with exponential backoff (10 attempts, 100ms–5000ms).
   The Scala implementation handles retry at the caller level.

4. **Schema Dedup Automatic**: Pass `metadataConfigJson` to read methods and
   `docMappingRef` is transparently resolved. The Scala implementation requires
   explicit `SchemaRegistry` calls.

### Adapter Pattern

To plug the Rust txlog into the existing Spark codebase, create an adapter that
implements `TransactionLogInterface`:

```scala
class RustTransactionLog(
  tablePath: Path,
  config: Map[String, String]
) extends TransactionLogInterface {

  override def listFiles(): Seq[AddAction] = {
    val snapshot = TransactionLogReader.getSnapshotInfo(
      tablePath.toString, config.asJava)

    // Read manifests (could be parallelized via Spark)
    val manifestEntries = snapshot.getManifestPaths.asScala.flatMap { mp =>
      TransactionLogReader.readManifest(
        tablePath.toString, config.asJava,
        snapshot.getStateDir, mp,
        extractMetadataConfig(snapshot.getMetadataJson)
      ).asScala
    }

    // Read post-checkpoint changes
    val changes = TransactionLogReader.readPostCheckpointChanges(
      tablePath.toString, config.asJava,
      toJson(snapshot.getPostCheckpointPaths),
      extractMetadataConfig(snapshot.getMetadataJson)
    )

    // Merge: checkpoint entries + added - removed
    val removedPaths = changes.getRemovedPaths.asScala.toSet
    val baseEntries = manifestEntries.filterNot(e => removedPaths.contains(e.getPath))
    val allEntries = baseEntries ++ changes.getAddedFiles.asScala

    allEntries.map(toScalaAddAction).toSeq
  }

  override def addFiles(addActions: Seq[AddAction]): Long = {
    val addsJson = toJson(addActions.map(toRustAddAction))
    val result = TransactionLogWriter.addFiles(
      tablePath.toString, config.asJava, addsJson)
    result.getVersion
  }

  // ... implement remaining methods
}
```

### TxLogFileEntry → Scala AddAction Conversion

```scala
private def toScalaAddAction(entry: TxLogFileEntry): AddAction = {
  AddAction(
    path = entry.getPath,
    partitionValues = entry.getPartitionValues.asScala.toMap,
    size = entry.getSize,
    modificationTime = entry.getModificationTime,
    dataChange = entry.isDataChange,
    stats = Option(entry.getStats),
    numRecords = if (entry.getNumRecords >= 0) Some(entry.getNumRecords) else None,
    footerStartOffset = if (entry.getFooterStartOffset >= 0) Some(entry.getFooterStartOffset) else None,
    footerEndOffset = if (entry.getFooterEndOffset >= 0) Some(entry.getFooterEndOffset) else None,
    hasFooterOffsets = entry.getHasFooterOffsets,
    minValues = if (entry.getMinValues.isEmpty) None else Some(entry.getMinValues.asScala.toMap),
    maxValues = if (entry.getMaxValues.isEmpty) None else Some(entry.getMaxValues.asScala.toMap),
    splitTags = if (entry.getSplitTags.isEmpty) None else Some(entry.getSplitTags.asScala.toMap),
    numMergeOps = if (entry.getNumMergeOps > 0) Some(entry.getNumMergeOps) else None,
    docMappingJson = Option(entry.getDocMappingJson),
    uncompressedSizeBytes = if (entry.getUncompressedSizeBytes >= 0) Some(entry.getUncompressedSizeBytes) else None,
    timeRangeStart = if (entry.getTimeRangeStart >= 0) Some(entry.getTimeRangeStart.toString) else None,
    timeRangeEnd = if (entry.getTimeRangeEnd >= 0) Some(entry.getTimeRangeEnd.toString) else None,
    companionSourceFiles = if (entry.getCompanionSourceFiles.isEmpty) None
                           else Some(entry.getCompanionSourceFiles.asScala.toSeq),
    companionDeltaVersion = if (entry.getCompanionDeltaVersion >= 0) Some(entry.getCompanionDeltaVersion) else None,
    companionFastFieldMode = Option(entry.getCompanionFastFieldMode)
  )
}
```

---

## Error Handling and Diagnostics

### JNI Exception Propagation

All Rust errors are converted to Java exceptions via `to_java_exception()`.
The Rust error types map to:

| Rust Error | Java Exception | When |
|------------|----------------|------|
| `TxLogError::NotInitialized` | `RuntimeException` | Table has no version files |
| `TxLogError::MaxRetriesExceeded` | `RuntimeException` | Write conflict after 10 retries |
| `TxLogError::Storage(anyhow)` | `RuntimeException` | S3/Azure/filesystem errors |
| `TxLogError::Serde(msg)` | `RuntimeException` | JSON/Avro parse errors |
| `TxLogError::Io(io::Error)` | `RuntimeException` | File I/O errors |

### Debug Logging

Enable native debug output:

```bash
export TANTIVY4JAVA_DEBUG=1
```

This activates the `debug_println!` macro throughout the Rust layer, producing
detailed output for:
- Storage operations (GET, PUT, LIST)
- Version file parsing
- Manifest reading
- Schema deduplication
- Write conflict retries
- Arrow FFI export

### Sentinel Values

The TANT protocol uses sentinel values for missing optional fields:

| Type | Sentinel | Meaning |
|------|----------|---------|
| `long` | `-1` | Field not present |
| `int` | `0` | Field not present |
| `boolean` | `false` | Field not present |
| `String` | `null` | Field not present |
| `Map` | empty map | No entries |
| `List` | empty list | No entries |

When converting `TxLogFileEntry` to Scala `AddAction`, check for these sentinels
and convert to `Option.None` as shown in the conversion example above.
