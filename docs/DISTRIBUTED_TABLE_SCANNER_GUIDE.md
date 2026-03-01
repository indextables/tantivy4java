# Distributed Table Scanner Developer Guide

## Overview

The distributed table scanner provides building blocks for scanning Delta, Iceberg, and Hive-style partitioned parquet tables at scale. Instead of loading all file metadata into a single process (which OOMs for tables with millions of files), work is split between a lightweight **driver** call and parallelizable **executor** calls.

## Architecture

All three formats follow the same three-primitive pattern:

```
┌──────────────────────────────────────────────────────────┐
│  DRIVER (single process, kilobytes of memory)            │
│                                                          │
│  getSnapshotInfo() → paths to distribute                 │
│  readChanges()     → small post-snapshot reconciliation  │
└──────────────┬───────────────────────────────────────────┘
               │  Spark parallelize / broadcast
               ▼
┌──────────────────────────────────────────────────────────┐
│  EXECUTORS (hundreds of processes, ~10MB each)           │
│                                                          │
│  readPart()  → file entries from ONE checkpoint/manifest │
└──────────────────────────────────────────────────────────┘
```

## Delta Lake

### Primitives

| Method | Role | Memory |
|--------|------|--------|
| `DeltaTableReader.getSnapshotInfo(url, config)` | Driver: reads `_last_checkpoint`, lists commit files, extracts schema | O(num_parts + num_commits) |
| `DeltaTableReader.readCheckpointPart(url, config, partPath)` | Executor: reads ONE checkpoint parquet, extracts `add` entries | ~54K entries per part |
| `DeltaTableReader.readPostCheckpointChanges(url, config, commitPaths)` | Driver: reads JSON commits after checkpoint, returns adds/removes | O(post-checkpoint changes) |

### DeltaSnapshotInfo

Returned by `getSnapshotInfo()`. Contains:

```java
long getVersion()                    // Checkpoint version
String getSchemaJson()               // Delta schema JSON
List<String> getPartitionColumns()   // Partition column names
List<String> getCheckpointPartPaths() // Paths to distribute to executors
List<String> getCommitFilePaths()    // Paths for readPostCheckpointChanges()
long getNumAddFiles()                // Hint from _last_checkpoint (-1 if unknown)
```

### DeltaLogChanges

Returned by `readPostCheckpointChanges()`. Contains:

```java
List<DeltaFileEntry> getAddedFiles()  // Files added after checkpoint
Set<String> getRemovedPaths()         // Files removed after checkpoint
```

### Complete Spark Example

```java
// === DRIVER: lightweight metadata discovery (~1 second) ===
Map<String, String> config = new HashMap<>();
config.put("aws_access_key_id", "AKIA...");
config.put("aws_secret_access_key", "...");
config.put("aws_region", "us-east-1");

DeltaSnapshotInfo info = DeltaTableReader.getSnapshotInfo(
    "s3://my-bucket/delta-table", config);

System.out.println("Version: " + info.getVersion());
System.out.println("Checkpoint parts: " + info.getCheckpointPartPaths().size());
System.out.println("Post-checkpoint commits: " + info.getCommitFilePaths().size());

// === DRIVER: process post-checkpoint changes (small, driver-safe) ===
DeltaLogChanges changes = DeltaTableReader.readPostCheckpointChanges(
    "s3://my-bucket/delta-table", config, info.getCommitFilePaths());

// === DISTRIBUTED: read checkpoint parts across executors ===
Broadcast<Map<String, String>> configBC = sc.broadcast(config);
JavaRDD<String> partsRDD = sc.parallelize(info.getCheckpointPartPaths(), 200);

JavaRDD<DeltaFileEntry> checkpointFiles = partsRDD.flatMap(partPath ->
    DeltaTableReader.readCheckpointPart(
        "s3://my-bucket/delta-table", configBC.value(), partPath).iterator()
);

// === DISTRIBUTED: apply post-checkpoint removes ===
Broadcast<Set<String>> removesBC = sc.broadcast(changes.getRemovedPaths());
JavaRDD<DeltaFileEntry> activeFiles = checkpointFiles
    .filter(f -> !removesBC.value().contains(f.getPath()))
    .union(sc.parallelize(changes.getAddedFiles()));

// activeFiles now contains all active file entries, distributed across executors
long totalFiles = activeFiles.count();
System.out.println("Total active files: " + totalFiles);
```

### Delta Technical Details

**Checkpoint format**: Multi-part checkpoints use zero-padded filenames:
```
_delta_log/00000000000000094320.checkpoint.0000000001.0000001123.parquet
```

**`_last_checkpoint` JSON**:
```json
{"version":94320,"size":61126995,"parts":1123,"numOfAddFiles":61126995}
```

**Checkpoint `add` column structure** (Arrow StructArray):
- `path` (StringArray) — file path relative to table root
- `size` (Int64Array) — file size in bytes
- `modificationTime` (Int64Array) — epoch millis
- `partitionValues` (MapArray<Utf8, Utf8>) — partition key/value pairs
- `stats` (StringArray) — JSON with `numRecords`
- `deletionVector` (StructArray) — non-null if DV exists

## Apache Iceberg

### Primitives

| Method | Role | Memory |
|--------|------|--------|
| `IcebergTableReader.getSnapshotInfo(catalog, ns, table, config)` | Driver: opens catalog, reads manifest list | O(num_manifests) |
| `IcebergTableReader.readManifestFile(catalog, ns, table, config, path)` | Executor: reads ONE manifest avro file | ~thousands of entries per manifest |

### IcebergSnapshotInfo

Returned by `getSnapshotInfo()`. Contains:

```java
long getSnapshotId()                          // Resolved snapshot ID
String getSchemaJson()                        // Iceberg schema JSON
String getPartitionSpecJson()                 // Default partition spec JSON
List<ManifestFileInfo> getManifestFiles()     // Full manifest metadata
List<String> getManifestFilePaths()           // Convenience: just the paths
```

### ManifestFileInfo

Per-manifest metadata:

```java
String getManifestPath()          // Full path to manifest avro file
long getManifestLength()          // File size in bytes
long getAddedSnapshotId()         // Which snapshot added this manifest
long getAddedFilesCount()         // Files with Added status
long getExistingFilesCount()      // Files with Existing status
long getDeletedFilesCount()       // Files with Deleted status
int getPartitionSpecId()          // Partition spec for this manifest
```

### Complete Spark Example

```java
// === DRIVER: lightweight metadata discovery ===
Map<String, String> config = new HashMap<>();
config.put("catalog_type", "rest");
config.put("uri", "http://localhost:8181");
config.put("warehouse", "s3://warehouse");
config.put("s3.access-key-id", "AKIA...");
config.put("s3.secret-access-key", "...");
config.put("s3.region", "us-east-1");

IcebergSnapshotInfo info = IcebergTableReader.getSnapshotInfo(
    "my-catalog", "default", "events", config);

System.out.println("Snapshot: " + info.getSnapshotId());
System.out.println("Manifests: " + info.getManifestFiles().size());

// === DISTRIBUTED: read manifests across executors ===
Broadcast<Map<String, String>> configBC = sc.broadcast(config);
JavaRDD<String> manifestsRDD = sc.parallelize(info.getManifestFilePaths(), 100);

JavaRDD<IcebergFileEntry> filesRDD = manifestsRDD.flatMap(manifestPath ->
    IcebergTableReader.readManifestFile(
        "my-catalog", "default", "events",
        configBC.value(), manifestPath).iterator()
);

// filesRDD now has all active data files, distributed
Dataset<Row> df = spark.createDataFrame(filesRDD, IcebergFileEntry.class);
```

### Snapshot-Specific Scanning

```java
// Scan a specific snapshot by ID
IcebergSnapshotInfo info = IcebergTableReader.getSnapshotInfo(
    "catalog", "ns", "table", config, 1234567890L);
```

## Hive-Style Parquet Directories

### Primitives

| Method | Role | Memory |
|--------|------|--------|
| `ParquetTableReader.getTableInfo(url, config)` | Driver: lists root, discovers partition dirs + schema | O(num_partitions) |
| `ParquetTableReader.listPartitionFiles(url, config, prefix)` | Executor: lists files in ONE partition | O(files per partition) |

### ParquetTableInfo

Returned by `getTableInfo()`. Contains:

```java
String getSchemaJson()                     // Arrow schema JSON from first file
List<String> getPartitionColumns()         // Inferred from directory names
List<String> getPartitionDirectories()     // Partition dir paths for executors
List<ParquetFileEntry> getRootFiles()      // Root-level files (unpartitioned)
boolean isPartitioned()                    // Whether table is partitioned
```

### ParquetFileEntry

```java
String getPath()                            // Full file path
long getSize()                              // File size in bytes
long getLastModified()                      // Epoch millis
Map<String, String> getPartitionValues()    // Parsed from directory path
```

### Complete Spark Example

```java
// === DRIVER: discover partition directories ===
Map<String, String> config = new HashMap<>();
config.put("aws_access_key_id", "AKIA...");
config.put("aws_secret_access_key", "...");
config.put("aws_region", "us-east-1");

ParquetTableInfo info = ParquetTableReader.getTableInfo(
    "s3://bucket/hive-table", config);

System.out.println("Partitioned: " + info.isPartitioned());
System.out.println("Partition columns: " + info.getPartitionColumns());
System.out.println("Partition dirs: " + info.getPartitionDirectories().size());

if (info.isPartitioned()) {
    // === DISTRIBUTED: list files per partition across executors ===
    Broadcast<Map<String, String>> configBC = sc.broadcast(config);
    JavaRDD<String> partitionsRDD = sc.parallelize(
        info.getPartitionDirectories(), 100);

    JavaRDD<ParquetFileEntry> filesRDD = partitionsRDD.flatMap(partDir ->
        ParquetTableReader.listPartitionFiles(
            "s3://bucket/hive-table", configBC.value(), partDir).iterator()
    );

    Dataset<Row> df = spark.createDataFrame(filesRDD, ParquetFileEntry.class);
} else {
    // Unpartitioned: root files already available
    List<ParquetFileEntry> files = info.getRootFiles();
}
```

### Partition Value Parsing

Partition values are extracted from Hive-style directory names:
```
year=2024/month=01/day=15/part-00000.parquet
→ {"year": "2024", "month": "01", "day": "15"}
```

Values are percent-decoded (e.g., `city=New%20York` → `"New York"`).

## Credential Configuration

All three formats share the same credential config map keys:

### AWS S3
```java
config.put("aws_access_key_id", "AKIA...");
config.put("aws_secret_access_key", "...");
config.put("aws_session_token", "...");   // optional, for temporary creds
config.put("aws_region", "us-east-1");
config.put("aws_endpoint", "http://localhost:9000");  // optional, for MinIO
config.put("aws_force_path_style", "true");            // optional, for MinIO
```

### Azure Blob Storage
```java
config.put("azure_account_name", "mystorageaccount");
config.put("azure_access_key", "...");
// OR
config.put("azure_bearer_token", "...");
```

### Iceberg Catalog
```java
config.put("catalog_type", "rest");    // or "glue", "hms"
config.put("uri", "http://...");
config.put("warehouse", "s3://...");
config.put("credential", "client_id:client_secret");  // OAuth2
config.put("token", "...");                            // bearer token
```

### Iceberg Storage (overrides catalog defaults)
```java
config.put("s3.access-key-id", "...");
config.put("s3.secret-access-key", "...");
config.put("s3.region", "...");
config.put("adls.account-name", "...");
config.put("adls.account-key", "...");
```

## Serialization

All entry and info classes implement `java.io.Serializable` for Spark:
- `DeltaFileEntry`, `DeltaSnapshotInfo`, `DeltaLogChanges`
- `IcebergFileEntry`, `IcebergSnapshotInfo`, `IcebergSnapshotInfo.ManifestFileInfo`
- `ParquetFileEntry`, `ParquetTableInfo`

## Backward Compatibility

Existing APIs are fully preserved:

| Existing API | Status |
|-------------|--------|
| `DeltaTableReader.listFiles()` | Unchanged |
| `DeltaTableReader.readSchema()` | Unchanged |
| `IcebergTableReader.listFiles()` | Unchanged |
| `IcebergTableReader.readSchema()` | Unchanged |
| `IcebergTableReader.listSnapshots()` | Unchanged |
| `ParquetSchemaReader.readSchema()` | Unchanged |

The existing single-call APIs remain the simplest option for small-to-medium tables. Use the distributed primitives when table size exceeds available driver memory.

## Migration from listFiles()

Replace a single `listFiles()` call with the distributed pattern when OOM occurs:

**Before (OOM on large tables):**
```java
List<DeltaFileEntry> files = DeltaTableReader.listFiles(tableUrl, config);
```

**After (distributed, any size):**
```java
DeltaSnapshotInfo info = DeltaTableReader.getSnapshotInfo(tableUrl, config);
DeltaLogChanges changes = DeltaTableReader.readPostCheckpointChanges(
    tableUrl, config, info.getCommitFilePaths());

// Distribute checkpoint reading across Spark executors
JavaRDD<DeltaFileEntry> files = sc.parallelize(info.getCheckpointPartPaths(), 200)
    .flatMap(part -> DeltaTableReader.readCheckpointPart(
        tableUrl, configBC.value(), part).iterator())
    .filter(f -> !removesBC.value().contains(f.getPath()))
    .union(sc.parallelize(changes.getAddedFiles()));
```

## Troubleshooting

### "Empty snapshot info response"
The Delta table has no `_last_checkpoint` file. This happens when the table is very new or has never been checkpointed. Fall back to `listFiles()` for small tables, or ensure checkpointing is configured.

### OOM on readCheckpointPart()
Each checkpoint part typically contains ~54K entries. If individual parts are still too large, increase executor memory. This is rare — 54K entries is ~10MB.

### Iceberg manifest file not found
The manifest path from `getSnapshotInfo()` may point to a storage location that requires different credentials than the catalog. Ensure storage credentials are included in the config map (e.g., `s3.access-key-id` for Iceberg storage access).

### Parquet partition discovery returns no partitions
The table may be unpartitioned. Check `info.isPartitioned()` — if false, use `info.getRootFiles()` directly instead of distributing `listPartitionFiles()` calls.
