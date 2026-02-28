# Partition Filter & Arrow FFI Developer Guide

This guide covers two features added to tantivy4java's distributed table scanners (Delta, Iceberg, Hive Parquet):

1. **Partition predicate filtering** — prune files at the native layer before serialization
2. **Arrow FFI export** — zero-copy columnar export for Delta checkpoint parts and Iceberg manifest files

## Partition Filtering

### Overview

All three distributed table scanners now accept an optional `PartitionFilter` that is serialized to JSON, passed through JNI, parsed once in Rust as a `PartitionPredicate`, and evaluated against each entry's `partition_values` map **before** TANT buffer serialization. This means filtered-out entries never cross the JNI boundary.

### Java API — `PartitionFilter`

`PartitionFilter` is a `Serializable` class (safe for Spark broadcast) in `io.indextables.tantivy4java.filter`.

#### Factory Methods

```java
import io.indextables.tantivy4java.filter.PartitionFilter;

// Equality
PartitionFilter.eq("year", "2024")

// Inequality
PartitionFilter.neq("status", "deleted")

// Set membership
PartitionFilter.in("region", "us-east-1", "us-west-2", "eu-west-1")

// Comparisons (string by default)
PartitionFilter.gt("year", "2022")
PartitionFilter.gte("month", "06")
PartitionFilter.lt("day", "15")
PartitionFilter.lte("hour", "12")

// Numeric comparisons — use .withType("long") or .withType("double")
PartitionFilter.gt("year", "2022").withType("long")
PartitionFilter.lte("price", "99.99").withType("double")

// Null checks
PartitionFilter.isNull("category")
PartitionFilter.isNotNull("region")

// Boolean combinators
PartitionFilter.and(
    PartitionFilter.eq("year", "2024"),
    PartitionFilter.in("month", "01", "02", "03")
)

PartitionFilter.or(
    PartitionFilter.eq("region", "us-east-1"),
    PartitionFilter.eq("region", "eu-west-1")
)

PartitionFilter.not(PartitionFilter.eq("status", "deleted"))
```

#### Serialization

`PartitionFilter` implements `Serializable`. The internal representation is a JSON string, so Spark can broadcast it efficiently:

```java
// Get the JSON representation (used internally for JNI)
String json = filter.toJson();

// Spark broadcast works automatically
Broadcast<PartitionFilter> broadcastFilter = sc.broadcast(filter);
```

### Filtering Semantics

| Scenario | Behavior |
|----------|----------|
| **Null filter** | No filtering — all entries pass (backward compatible) |
| **Missing column** in `eq`/`gt`/`gte`/`lt`/`lte`/`in` | Entry excluded (`false`) |
| **Missing column** in `neq` | Entry included (`true`) |
| **Missing column** in `is_null` | Entry included (`true`) |
| **Missing column** in `is_not_null` | Entry excluded (`false`) |
| **Numeric comparison with unparseable value** | Falls back to string comparison |

### Delta Table Scanner

```java
import io.indextables.tantivy4java.delta.DeltaTableReader;
import io.indextables.tantivy4java.filter.PartitionFilter;

PartitionFilter filter = PartitionFilter.and(
    PartitionFilter.eq("year", "2024"),
    PartitionFilter.in("month", "01", "02", "03")
);

// listFiles — filter applied after scan
List<DeltaFileEntry> files = DeltaTableReader.listFiles(
    tableUrl, -1, config, false, filter);

// readCheckpointPart — filter applied after checkpoint read
List<DeltaFileEntry> entries = DeltaTableReader.readCheckpointPart(
    tableUrl, config, partPath, filter);

// readPostCheckpointChanges — filter applied to added_files
DeltaLogChanges changes = DeltaTableReader.readPostCheckpointChanges(
    tableUrl, config, commitPaths, filter);
```

### Iceberg Table Scanner

```java
import io.indextables.tantivy4java.iceberg.IcebergTableReader;

PartitionFilter filter = PartitionFilter.eq("region", "us-east-1");

// listFiles — filter applied after scan
List<IcebergFileEntry> files = IcebergTableReader.listFiles(
    catalogName, namespace, tableName, config, -1, false, filter);

// readManifestFile — filter applied after manifest read
List<IcebergFileEntry> entries = IcebergTableReader.readManifestFile(
    catalogName, namespace, tableName, config, manifestPath, false, filter);
```

### Hive Parquet Table Scanner

The Parquet scanner gets the **biggest benefit** from partition filtering because it prunes entire partition directories before executors even list files:

```java
import io.indextables.tantivy4java.parquet.ParquetTableReader;

PartitionFilter filter = PartitionFilter.and(
    PartitionFilter.eq("year", "2024"),
    PartitionFilter.gt("month", "06").withType("long")
);

// getTableInfo — partition directories pruned by parsing key=value from paths
ParquetTableInfo info = ParquetTableReader.getTableInfo(tableUrl, config, filter);
// info.getPartitionDirectories() contains only matching directories

// listPartitionFiles — file entries filtered by partition values
List<ParquetFileEntry> files = ParquetTableReader.listPartitionFiles(
    tableUrl, config, partitionPrefix, filter);
```

### Spark Integration Example

```java
// DRIVER: Get snapshot info + create filter
PartitionFilter filter = PartitionFilter.and(
    PartitionFilter.eq("year", "2024"),
    PartitionFilter.in("month", "01", "02", "03")
);

DeltaSnapshotInfo info = DeltaTableReader.getSnapshotInfo(tableUrl, config);
Broadcast<PartitionFilter> bFilter = sc.broadcast(filter);
Broadcast<Map<String, String>> bConfig = sc.broadcast(config);

// EXECUTORS: Read checkpoint parts with predicate pushdown
JavaRDD<DeltaFileEntry> filesRDD = sc.parallelize(info.getCheckpointPartPaths(), 100)
    .flatMap(partPath -> DeltaTableReader.readCheckpointPart(
        tableUrl, bConfig.value(), partPath, bFilter.value()).iterator());
```

---

## Arrow FFI Export

### Overview

For high-volume scenarios (Delta checkpoint parts with ~54K entries each, Iceberg manifests with thousands of entries), the standard TANT buffer path creates multiple data copies:

```
parquet/avro → Rust struct → TANT buffer → byte[] (JNI) → Map (Java) → Entry (Java)
```

The Arrow FFI path eliminates TANT serialization and Java-side parsing:

```
parquet/avro → Rust struct → flat RecordBatch → FFI export (zero JNI serialization)
```

### Delta Checkpoint Arrow FFI

Exports 6 columns matching `DeltaFileEntry` fields:

| Column | Arrow Type | Description |
|--------|-----------|-------------|
| `path` | Utf8 | Relative parquet file path |
| `size` | Int64 | File size in bytes |
| `modification_time` | Int64 | Epoch millis |
| `num_records` | Int64 | -1 if unknown |
| `partition_values` | Utf8 | JSON-serialized `{"year":"2024"}` |
| `has_deletion_vector` | Boolean | Deletion vector present |

```java
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;

// Allocate FFI structs (6 columns)
ArrowArray[] arrays = new ArrowArray[6];
ArrowSchema[] schemas = new ArrowSchema[6];
long[] arrayAddrs = new long[6];
long[] schemaAddrs = new long[6];
for (int i = 0; i < 6; i++) {
    arrays[i] = ArrowArray.allocateNew(allocator);
    schemas[i] = ArrowSchema.allocateNew(allocator);
    arrayAddrs[i] = arrays[i].memoryAddress();
    schemaAddrs[i] = schemas[i].memoryAddress();
}

// Read with partition filter + FFI export
PartitionFilter filter = PartitionFilter.eq("year", "2024");
int numRows = DeltaTableReader.readCheckpointPartArrowFfi(
    tableUrl, config, partPath, filter, arrayAddrs, schemaAddrs);

// Import as Arrow vectors (zero-copy from native memory)
Data.importVector(allocator, arrays[0], schemas[0], dictProvider);
// ... process columnar data directly
```

### Iceberg Manifest Arrow FFI

Exports 7 columns matching `IcebergFileEntry` fields:

| Column | Arrow Type | Description |
|--------|-----------|-------------|
| `path` | Utf8 | Full URI (e.g. `s3://bucket/data/part-00000.parquet`) |
| `file_format` | Utf8 | `"parquet"`, `"orc"`, `"avro"` |
| `record_count` | Int64 | Number of records |
| `file_size_bytes` | Int64 | File size |
| `partition_values` | Utf8 | JSON-serialized partition values |
| `content_type` | Utf8 | `"data"`, `"equality_deletes"`, `"position_deletes"` |
| `snapshot_id` | Int64 | Snapshot that added the file |

```java
// Allocate FFI structs (7 columns)
long[] arrayAddrs = new long[7];
long[] schemaAddrs = new long[7];
// ... allocate as above ...

PartitionFilter filter = PartitionFilter.eq("region", "us-east-1");
int numRows = IcebergTableReader.readManifestFileArrowFfi(
    catalogName, namespace, tableName, config, manifestPath,
    filter, arrayAddrs, schemaAddrs);
```

### When to Use Arrow FFI vs TANT

| Scenario | Recommendation |
|----------|----------------|
| Small tables (< 10K files) | TANT path — simpler, no Arrow dependency |
| Large tables (> 100K files) | Arrow FFI — avoids Java object allocation overhead |
| Spark consumers | Arrow FFI — data stays columnar end-to-end |
| Non-Spark Java consumers | TANT path — no Arrow dependency needed |

The TANT path (`readCheckpointPart()`, `readManifestFile()`) remains fully supported for callers without Arrow dependencies.

---

## JSON Wire Format Reference

The `PartitionFilter.toJson()` output uses this format (also accepted by the Rust `PartitionPredicate` parser):

```json
// Equality
{"op": "eq", "column": "year", "value": "2024"}

// Inequality
{"op": "neq", "column": "status", "value": "deleted"}

// Set membership
{"op": "in", "column": "region", "values": ["us-east-1", "us-west-2"]}

// Comparison (with optional type)
{"op": "gt", "column": "year", "value": "2022", "type": "long"}
{"op": "lte", "column": "price", "value": "99.99", "type": "double"}

// Null checks
{"op": "is_null", "column": "category"}
{"op": "is_not_null", "column": "region"}

// Boolean combinators
{"op": "and", "filters": [{"op": "eq", ...}, {"op": "in", ...}]}
{"op": "or", "filters": [{"op": "eq", ...}, {"op": "eq", ...}]}
{"op": "not", "filter": {"op": "eq", ...}}
```

## Files Modified

### New Files
- `src/main/java/io/indextables/tantivy4java/filter/PartitionFilter.java` — Java filter class

### Rust
- `native/src/common.rs` — `PartitionPredicate` enum, `evaluate()`, `parse_optional_predicate()`, `filter_by_predicate()`, 29 unit tests
- `native/src/delta_reader/jni.rs` — predicate params on 3 JNI methods + Arrow FFI entry point
- `native/src/delta_reader/distributed.rs` — `read_checkpoint_part_arrow_ffi()`
- `native/src/iceberg_reader/jni.rs` — predicate params on 2 JNI methods + Arrow FFI entry point
- `native/src/iceberg_reader/distributed.rs` — `read_iceberg_manifest_arrow_ffi()`
- `native/src/parquet_reader/jni.rs` — predicate params on 2 JNI methods + partition directory pruning

### Java
- `src/main/java/io/indextables/tantivy4java/delta/DeltaTableReader.java` — `PartitionFilter` overloads + `readCheckpointPartArrowFfi()`
- `src/main/java/io/indextables/tantivy4java/iceberg/IcebergTableReader.java` — `PartitionFilter` overloads + `readManifestFileArrowFfi()`
- `src/main/java/io/indextables/tantivy4java/parquet/ParquetTableReader.java` — `PartitionFilter` overloads
