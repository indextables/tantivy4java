# Transaction Log — Java Developer Guide

The transaction log (`io.indextables.jni.txlog`) is a Delta-Lake-compatible,
object-store-backed log that tracks the set of active split files for a table.
It provides optimistic-concurrency writes, Avro-format checkpoints, and a
two-phase distributed read model (driver lists paths; executors read content).

---

## Table of Contents

1. [Concepts](#concepts)
2. [Quick Start](#quick-start)
3. [Writing to the Log](#writing-to-the-log)
4. [Reading the Log — Distributed Pattern](#reading-the-log--distributed-pattern)
5. [Checkpointing](#checkpointing)
6. [Purging Old Data](#purging-old-data)
7. [Configuration Reference](#configuration-reference)
8. [Storage Backends](#storage-backends)
9. [Data Classes](#data-classes)
10. [Operational Guidance](#operational-guidance)

---

## Concepts

### Version files

Every write appends one gzip-compressed JSON-lines file to
`<table>/_transaction_log/<version>.json` (e.g. `00000000000000000003.json`).
Each line in the file is one action:

```
{"add":{"path":"split-42.split","size":1000,...}}
{"remove":{"path":"split-07.split","dataChange":true}}
{"mergeskip":{"path":"stale.split","skipTimestamp":...,"reason":"merge","skipCount":2}}
```

Version files are written with *put-if-absent* semantics: if two writers race
to the same version the loser backs off and retries at the next free version.

### Checkpoints

After every successful write the native layer optionally writes an Avro
*state checkpoint* to `<table>/_txlog_state/<version>/`. A checkpoint
captures the full active file set so that subsequent reads need not replay
every version file from the beginning.  Checkpoints are controlled by the
`checkpoint_interval` config key (default: enabled after every write).

### Two-phase distributed read

Reading the full file list is split into two cheap phases to suit distributed
execution engines (e.g. Spark):

```
Phase 1 (driver)     getSnapshotInfo()   — 1–2 GETs + 1 LIST
Phase 2 (executors)  readManifest()      — 1 GET per manifest shard (parallelisable)
                     readPostCheckpointChanges()  — N GETs (post-cp version files)
```

---

## Quick Start

### Maven coordinates

```xml
<dependency>
    <groupId>io.indextables</groupId>
    <artifactId>tantivy4java</artifactId>
    <version>0.34.2</version>
</dependency>
```

### Minimal write + read cycle

```java
import io.indextables.jni.txlog.*;
import java.util.*;

// ── 1. Storage config (local filesystem — no credentials needed) ──────────
Map<String, String> cfg = Collections.emptyMap();
String table = "file:///data/my-table";

// ── 2. Write a batch of files ─────────────────────────────────────────────
String adds = """
    [{"path":"split-001.split","partitionValues":{},"size":50000,
      "modificationTime":1700000000000,"dataChange":true,"numRecords":1000}]
    """;
WriteResult wr = TransactionLogWriter.addFiles(table, cfg, adds);
System.out.println("Committed at version " + wr.getVersion());

// ── 3. Read back the full active file list ────────────────────────────────
TxLogSnapshotInfo snap = TransactionLogReader.getSnapshotInfo(table, cfg);

List<TxLogFileEntry> files = new ArrayList<>();
for (String manifestPath : snap.getManifestPaths()) {
    files.addAll(TransactionLogReader.readManifest(
        table, cfg, snap.getStateDir(), manifestPath, null));
}

if (!snap.getPostCheckpointPaths().isEmpty()) {
    String pathsJson = new ObjectMapper()
        .writeValueAsString(snap.getPostCheckpointPaths());
    TxLogChanges changes = TransactionLogReader.readPostCheckpointChanges(
        table, cfg, pathsJson, null);
    files.addAll(changes.getAddedFiles());
    // apply removals
    Set<String> removed = new HashSet<>(changes.getRemovedPaths());
    files.removeIf(e -> removed.contains(e.getPath()));
}

System.out.println("Active files: " + files.size());
```

---

## Writing to the Log

### `addFiles` — add one or more files

```java
// JSON array of AddAction objects
String addsJson = """
    [
      {"path":"p1.split","partitionValues":{"year":"2024"},"size":10000,
       "modificationTime":1700000000000,"dataChange":true,"numRecords":500},
      {"path":"p2.split","partitionValues":{"year":"2024"},"size":12000,
       "modificationTime":1700000000000,"dataChange":true,"numRecords":600}
    ]
    """;

WriteResult result = TransactionLogWriter.addFiles(tablePath, cfg, addsJson);
// result.getVersion()            — committed version
// result.getRetries()            — how many conflicts were resolved
// result.getConflictedVersions() — version numbers that lost the race
```

`addFiles` uses optimistic concurrency with exponential backoff.  Under low
contention retries will be zero.  Under heavy concurrent writes expect a small
number of retries; all writers eventually succeed.

### `removeFile` — mark a file as deleted

```java
long version = TransactionLogWriter.removeFile(tablePath, cfg, "p1.split");
```

### `skipFile` — record a merge-skip action

Skip actions are advisory metadata.  They record that a file was skipped
during a merge operation and should not be merged again until the skip expires.

```java
String skipJson = """
    {"path":"stale.split","skipTimestamp":1700000000000,
     "reason":"merge","skipCount":2}
    """;
long version = TransactionLogWriter.skipFile(tablePath, cfg, skipJson);
```

### `writeVersion` — write arbitrary mixed actions

Use this when you need a single version file that mixes protocol, metadata,
and data actions (e.g. table initialisation or schema migration):

```java
String actionsJsonLines = """
    {"protocol":{"minReaderVersion":4,"minWriterVersion":4}}
    {"metaData":{"id":"abc-123","schemaString":"{}","partitionColumns":[],
                 "format":{"provider":"parquet"},"configuration":{}}}
    {"add":{"path":"init.split","partitionValues":{},"size":0,
            "modificationTime":0,"dataChange":true}}
    """;
WriteResult result = TransactionLogWriter.writeVersion(tablePath, cfg, actionsJsonLines);
```

### `initializeTable` — bootstrap a brand-new table

```java
String protocol = "{\"minReaderVersion\":4,\"minWriterVersion\":4}";
String metadata = """
    {"id":"my-table-id","schemaString":"{}","partitionColumns":[],
     "format":{"provider":"parquet"},"configuration":{}}
    """;
TransactionLogWriter.initializeTable(tablePath, cfg, protocol, metadata);
```

`initializeTable` fails with an exception if version 0 already exists.

### `writeVersionOnce` — single-attempt write (no retry)

Returns a `WriteResult` with `version == -1` when the target version is
already taken.  Useful when the caller wants to manage concurrency itself.

```java
WriteResult r = TransactionLogWriter.writeVersionOnce(tablePath, cfg, actionsJsonLines);
if (r.getVersion() == -1) {
    // conflict — caller decides what to do
}
```

---

## Reading the Log — Distributed Pattern

### Phase 1: `getSnapshotInfo` (driver)

A single cheap call that reads `_last_checkpoint` and lists post-checkpoint
version files.  Returns paths for the executor phase without reading any file
content:

```java
TxLogSnapshotInfo snap = TransactionLogReader.getSnapshotInfo(tablePath, cfg);

long checkpointVersion       = snap.getCheckpointVersion();  // -1 if no checkpoint yet
String stateDir              = snap.getStateDir();           // pass to readManifest
List<String> manifestPaths   = snap.getManifestPaths();      // distribute to executors
List<String> postCpPaths     = snap.getPostCheckpointPaths();// for readPostCheckpointChanges
String protocolJson          = snap.getProtocolJson();       // table protocol
String metadataJson          = snap.getMetadataJson();       // table metadata / schema
```

`getSnapshotInfo` uses an in-process TTL cache (default 5 minutes). 
Subsequent calls within the TTL window return the cached value at zero I/O
cost.

### Phase 2a: `readManifest` (executor-side, fully parallel)

Each manifest file is an independent Avro-format shard.  Read them in parallel
across your executor pool:

```java
// In a Spark map(), Flink task, thread pool, etc.
List<TxLogFileEntry> entries = TransactionLogReader.readManifest(
    tablePath,       // table location
    cfg,             // storage credentials
    snap.getStateDir(),
    manifestPath,    // one path from snap.getManifestPaths()
    null             // optional metadataConfigJson — pass null unless deduplication metadata needed
);
```

### Phase 2b: `readPostCheckpointChanges` (driver-side)

Reads version files written after the checkpoint and returns deltas:

```java
String pathsJson = new ObjectMapper()
    .writeValueAsString(snap.getPostCheckpointPaths());

TxLogChanges changes = TransactionLogReader.readPostCheckpointChanges(
    tablePath, cfg, pathsJson,
    null  // optional metadataConfigJson
);

List<TxLogFileEntry> newFiles = changes.getAddedFiles();
List<String>         removed  = changes.getRemovedPaths();
List<TxLogSkipAction> skips   = changes.getSkipActions();
long maxVersion               = changes.getMaxVersion();
```

### Log replay — merge checkpoint + post-checkpoint

```java
// Build a mutable map keyed by file path
Map<String, TxLogFileEntry> active = new LinkedHashMap<>();

// Add checkpoint entries (from all manifest shards)
for (TxLogFileEntry e : checkpointEntries) {
    active.put(e.getPath(), e);
}

// Apply post-checkpoint adds (may overwrite if file was re-added)
for (TxLogFileEntry e : changes.getAddedFiles()) {
    active.put(e.getPath(), e);
}

// Apply post-checkpoint removes
for (String path : changes.getRemovedPaths()) {
    active.remove(path);
}

// active.values() is now the complete live file set
```

### Listing and reading individual version files

For diagnostic or time-travel purposes:

```java
long[] versions = TransactionLogReader.listVersions(tablePath, cfg);
String jsonLines = TransactionLogReader.readVersion(tablePath, cfg, versions[0]);
long current     = TransactionLogReader.getCurrentVersion(tablePath, cfg);
```

### Listing skip actions

```java
// Returns all skip actions written in the last 10 minutes
long tenMinutesMs = 600_000L;
List<TxLogSkipAction> skips = TransactionLogReader.listSkipActions(
    tablePath, cfg, tenMinutesMs);
// Pass 0 to retrieve all skip actions regardless of age
```

### Cache invalidation

The in-process cache is automatically invalidated on every write.  If an
external process has written to the table (or if you need an immediate fresh
read after a purge), invalidate manually:

```java
TransactionLogReader.invalidateCache(tablePath);
```

---

## Checkpointing

Checkpoints are created automatically after each `addFiles` / `removeFile` /
`skipFile` call (unless `checkpoint_interval=0`).  You can also create one
explicitly:

```java
// Provide the complete current set of active files
String entriesJson = new ObjectMapper().writeValueAsString(allActiveFiles);
String metadataJson = "{\"id\":\"my-table\",...}";
String protocolJson = "{\"minReaderVersion\":4,\"minWriterVersion\":4}";

LastCheckpointInfo cp = TransactionLogWriter.createCheckpoint(
    tablePath, cfg, entriesJson, metadataJson, protocolJson);

System.out.printf("Checkpoint at version %d with %d files%n",
    cp.getVersion(), cp.getNumFiles());
```

**Disable auto-checkpoint** when you want manual control, e.g. to batch
several writes before checkpointing:

```java
Map<String, String> noCp = new HashMap<>(baseCfg);
noCp.put("checkpoint_interval", "0");

// These writes do not trigger auto-checkpoint
TransactionLogWriter.addFiles(tablePath, noCp, batch1);
TransactionLogWriter.addFiles(tablePath, noCp, batch2);

// Checkpoint explicitly at a known stable state
TransactionLogWriter.createCheckpoint(tablePath, cfg, allFiles, metadata, protocol);
```

---

## Purging Old Data

Purge operations delete old version files and Avro state directories that are
no longer needed for time-travel queries.

```java
long retentionMs = 7 * 24 * 3600 * 1000L; // 7 days

// Dry run first — see what would be deleted
String dryState    = TransactionLogWriter.deleteExpiredStates(tablePath, cfg, retentionMs, true);
String dryVersions = TransactionLogWriter.deleteExpiredVersions(tablePath, cfg, retentionMs, true);
System.out.println("Would delete states: " + dryState);
System.out.println("Would delete versions: " + dryVersions);

// Live run
TransactionLogWriter.deleteExpiredStates(tablePath, cfg, retentionMs, false);
TransactionLogWriter.deleteExpiredVersions(tablePath, cfg, retentionMs, false);

// Invalidate cache so the next read picks up the new state
TransactionLogReader.invalidateCache(tablePath);
```

Both methods return a JSON string `{"found": N, "deleted": N}`.

For inspecting which versions are within the retention window before deleting:

```java
long[] retained = TransactionLogReader.listRetainedVersions(tablePath, cfg, retentionMs);
```

### Streaming retained-files cursor

When you need to enumerate every file path referenced by any non-expired
version (e.g. to reconcile against object storage), use the streaming cursor
API to avoid loading all entries into memory at once:

```java
long cursorHandle = TransactionLogReader.openRetainedFilesCursor(
    tablePath, cfg, retentionMs);
try {
    List<Map<String, Object>> batch;
    while ((batch = TransactionLogReader.readNextRetainedFilesBatch(cursorHandle, 1000)) != null) {
        for (Map<String, Object> row : batch) {
            String path    = (String)  row.get("path");
            long   size    = (Long)    row.get("size");
            long   version = (Long)    row.get("version");
            // ... process or accumulate
        }
    }
} finally {
    TransactionLogReader.closeRetainedFilesCursor(cursorHandle);
}
```

---

## Configuration Reference

All configuration is passed as `Map<String, String>` alongside every API call.
Keys are optional; omitted keys use the defaults shown.

### Storage / credentials

| Key | Description |
|-----|-------------|
| `aws_access_key_id` | AWS access key |
| `aws_secret_access_key` | AWS secret key |
| `aws_session_token` | AWS STS session token (temporary credentials) |
| `aws_region` | AWS region (e.g. `us-east-1`) |
| `aws_endpoint` | Custom S3 endpoint URL (for MinIO, LocalStack, etc.) |
| `aws_force_path_style` | `"true"` to force path-style S3 URLs (required for MinIO) |
| `azure_account_name` | Azure storage account name |
| `azure_access_key` | Azure shared-key credential |
| `azure_bearer_token` | Azure OAuth 2.0 bearer token (Service Principal / Managed Identity) |

### Cache behaviour

| Key | Default | Description |
|-----|---------|-------------|
| `cache.ttl.ms` | `300000` | Override all cache TTLs at once (ms). Set to `0` to disable caching. |
| `cache.version.ttl.ms` | `300000` | Per-version-file cache TTL (ms) |
| `cache.snapshot.ttl.ms` | `600000` | Per-snapshot cache TTL (ms) |
| `cache.file_list.ttl.ms` | `120000` | Per-file-list cache TTL (ms) |
| `cache.metadata.ttl.ms` | `1800000` | Protocol/metadata cache TTL (ms) |
| `cache.version.capacity` | `1000` | Max cached version entries |
| `cache.snapshot.capacity` | `100` | Max cached snapshot entries |
| `cache.file_list.capacity` | `50` | Max cached file-list entries |
| `cache.enabled` | `"true"` | Set to `"false"` to disable all caching |

### Concurrency

| Key | Default | Description |
|-----|---------|-------------|
| `max_concurrent_reads` | `32` | Maximum number of parallel object-store GETs issued simultaneously during version-file fan-outs and compaction manifest reads. Reduce if you are hitting S3/Azure rate limits; increase (up to ~64) if you have very wide network throughput and many post-checkpoint versions to catch up. |

### Checkpoint behaviour

| Key | Default | Description |
|-----|---------|-------------|
| `checkpoint_interval` or `checkpoint.interval` | `1` (every write) | How many writes between auto-checkpoints. Set to `0` to disable auto-checkpointing entirely. |

### Example: tuned production config

```java
Map<String, String> cfg = new HashMap<>();

// AWS credentials
cfg.put("aws_access_key_id",     System.getenv("AWS_ACCESS_KEY_ID"));
cfg.put("aws_secret_access_key", System.getenv("AWS_SECRET_ACCESS_KEY"));
cfg.put("aws_region",            "us-east-1");

// Cache: longer TTL, larger capacity for a high-read table
cfg.put("cache.version.ttl.ms",   "600000");   // 10 min
cfg.put("cache.snapshot.ttl.ms",  "1200000");  // 20 min
cfg.put("cache.version.capacity", "2000");
cfg.put("cache.snapshot.capacity","200");

// Concurrency: 48 parallel GETs for a table with many post-cp versions
cfg.put("max_concurrent_reads", "48");

// Auto-checkpoint after every write (default, shown explicitly)
cfg.put("checkpoint_interval", "1");
```

### Example: disable caching (for tests or one-shot jobs)

```java
Map<String, String> cfg = new HashMap<>(baseCfg);
cfg.put("cache.enabled", "false");
```

---

## Storage Backends

The `tablePath` argument accepts any of:

| Scheme | Example | Notes |
|--------|---------|-------|
| `file://` | `file:///data/my-table` | Local filesystem |
| `s3://` | `s3://my-bucket/tables/my-table` | AWS S3 or S3-compatible |
| `azure://` | `azure://my-container/tables/my-table` | Azure Blob Storage |
| Bare path | `/data/my-table` | Local filesystem (convenience) |

For **MinIO** or other S3-compatible stores, also set:
```java
cfg.put("aws_endpoint",          "http://minio:9000");
cfg.put("aws_force_path_style",  "true");
```

For **Azure OAuth** (Service Principal), obtain a bearer token before calling:
```java
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.core.credential.TokenRequestContext;

var credential = new ClientSecretCredentialBuilder()
    .clientId(clientId).clientSecret(secret).tenantId(tenantId).build();

String token = credential
    .getToken(new TokenRequestContext().addScopes("https://storage.azure.com/.default"))
    .block().getToken();

cfg.put("azure_account_name", "mystorageaccount");
cfg.put("azure_bearer_token", token);
```

---

## Data Classes

### `TxLogFileEntry` — one active file

| Getter | Type | Description |
|--------|------|-------------|
| `getPath()` | `String` | File path relative to table root |
| `getSize()` | `long` | File size in bytes |
| `getNumRecords()` | `long` | Record count (`-1` if unknown) |
| `getModificationTime()` | `long` | Epoch ms when file was created |
| `getPartitionValues()` | `Map<String,String>` | Partition column → value |
| `getMinValues()` | `Map<String,String>` | Per-column min for data skipping |
| `getMaxValues()` | `Map<String,String>` | Per-column max for data skipping |
| `getFooterStartOffset()` | `long` | Byte offset of split footer (`-1` if absent) |
| `getFooterEndOffset()` | `long` | Byte offset end of split footer (`-1` if absent) |
| `getHasFooterOffsets()` | `boolean` | True if footer offsets are present |
| `getDocMappingJson()` | `String` | Quickwit doc-mapping JSON (null if absent) |
| `getTimeRangeStart()` | `long` | Time range start in epoch micros (`-1` if absent) |
| `getTimeRangeEnd()` | `long` | Time range end in epoch micros (`-1` if absent) |
| `getSplitTags()` | `List<String>` | Tags attached to the split |
| `getNumMergeOps()` | `int` | Number of merge operations applied |
| `getUncompressedSizeBytes()` | `long` | Uncompressed size (`-1` if absent) |
| `getCompanionSourceFiles()` | `List<String>` | Parquet companion file paths |
| `getCompanionDeltaVersion()` | `long` | Companion Delta table version (`-1` if absent) |
| `getCompanionFastFieldMode()` | `String` | e.g. `"HYBRID"` (null if absent) |
| `getAddedAtVersion()` | `long` | Log version this file was added in |
| `getAddedAtTimestamp()` | `long` | Epoch ms when this file was added |

### `TxLogSnapshotInfo` — lightweight driver-side snapshot

| Getter | Type | Description |
|--------|------|-------------|
| `getCheckpointVersion()` | `long` | Latest checkpoint version (`-1` if none) |
| `getStateDir()` | `String` | State directory path (pass to `readManifest`) |
| `getManifestPaths()` | `List<String>` | Paths to distribute to executors |
| `getPostCheckpointPaths()` | `List<String>` | Version file paths after checkpoint |
| `getProtocolJson()` | `String` | Table protocol as JSON |
| `getMetadataJson()` | `String` | Table metadata / schema as JSON |
| `getNumManifests()` | `long` | Number of manifest shards |

### `TxLogChanges` — post-checkpoint delta

| Getter | Type | Description |
|--------|------|-------------|
| `getAddedFiles()` | `List<TxLogFileEntry>` | Files added since checkpoint |
| `getRemovedPaths()` | `List<String>` | File paths removed since checkpoint |
| `getSkipActions()` | `List<TxLogSkipAction>` | Merge-skip actions since checkpoint |
| `getMaxVersion()` | `long` | Highest version number in this range |

### `WriteResult` — result of a write operation

| Getter | Type | Description |
|--------|------|-------------|
| `getVersion()` | `long` | Committed version number (`-1` if conflict with `writeVersionOnce`) |
| `getRetries()` | `int` | Number of conflict retries needed |
| `getConflictedVersions()` | `List<Long>` | Version numbers that were already taken |

### `LastCheckpointInfo` — checkpoint metadata

| Getter | Type | Description |
|--------|------|-------------|
| `getVersion()` | `long` | Version at which checkpoint was written |
| `getNumFiles()` | `long` | Number of active files captured |
| `getFormat()` | `String` | Always `"avro-state"` |

### `TxLogSkipAction` — merge-skip advisory

| Getter | Type | Description |
|--------|------|-------------|
| `getPath()` | `String` | File path that was skipped |
| `getSkipTimestamp()` | `long` | Epoch ms when the skip was recorded |
| `getReason()` | `String` | Skip reason (e.g. `"merge"`) |
| `getSkipCount()` | `int` | How many times this file has been skipped |

---

## Operational Guidance

### Concurrency

All write methods (`addFiles`, `removeFile`, `skipFile`, `writeVersion`) use
optimistic concurrency with up to 10 attempts and exponential backoff with
jitter.  Under sustained contention from many writers, increase the number of
attempts by switching to `writeVersionOnce` with application-managed retry, or
ensure writers are sharded by table partition.

Monitor `WriteResult.getRetries()` to detect contention:

```java
WriteResult wr = TransactionLogWriter.addFiles(table, cfg, addsJson);
if (wr.getRetries() > 3) {
    log.warn("High write contention on {}: {} retries needed", table, wr.getRetries());
}
```

### Cache tuning

The cache is keyed per `(tablePath, TTL, capacity)`.  Two callers with
identical config strings share the same in-process cache instance.

- **Read-heavy, stable tables**: increase `cache.snapshot.ttl.ms` to 20–30 min
- **Frequently written tables**: reduce TTLs or set `cache.enabled=false` for
  writers that must always see the latest version
- **Memory-constrained environments**: reduce capacities
  (`cache.version.capacity=200`, etc.)

### Object-store rate limiting

Each `getSnapshotInfo` call issues approximately 2 GETs and 1 LIST.  Under
heavy load (many parallel tasks calling `getSnapshotInfo`) use the cache to
reduce I/O:

```java
// All tasks sharing a JVM share this cache key automatically
cfg.put("cache.ttl.ms", "30000");  // 30 second TTL is often enough for a query
```

For tables with many post-checkpoint version files to catch up, `max_concurrent_reads`
controls how many GETs are issued simultaneously.  The default of 32 is suitable
for most S3 deployments.  Lower it if you observe `SlowDown` / 503 throttling;
raise it (up to 64) if you have very high-bandwidth connections and the table
is significantly behind its checkpoint:

```java
cfg.put("max_concurrent_reads", "16");  // more conservative for shared prefix
cfg.put("max_concurrent_reads", "64");  // aggressive catch-up on dedicated prefix
```

### Spark integration pattern

```java
// Driver: get snapshot info (cached after first call)
TxLogSnapshotInfo snap = TransactionLogReader.getSnapshotInfo(tablePath, cfg);

// Broadcast immutable config to avoid re-serializing credentials per task
Broadcast<Map<String, String>> broadcastCfg = sc.broadcast(cfg);
Broadcast<TxLogSnapshotInfo>   broadcastSnap = sc.broadcast(snap);

// Executor: read each manifest shard in parallel
List<TxLogFileEntry> allEntries = sc
    .parallelize(snap.getManifestPaths(), snap.getManifestPaths().size())
    .flatMap(manifestPath -> {
        TxLogSnapshotInfo s = broadcastSnap.value();
        Map<String, String> c = broadcastCfg.value();
        return TransactionLogReader.readManifest(
            tablePath, c, s.getStateDir(), manifestPath, null).iterator();
    })
    .collect();

// Driver: read post-checkpoint deltas and apply
if (!snap.getPostCheckpointPaths().isEmpty()) {
    String pathsJson = new ObjectMapper()
        .writeValueAsString(snap.getPostCheckpointPaths());
    TxLogChanges changes = TransactionLogReader.readPostCheckpointChanges(
        tablePath, cfg, pathsJson, null);
    // merge changes into allEntries ...
}
```

### Debugging

Set the environment variable `TANTIVY4JAVA_DEBUG=1` before starting the JVM to
enable verbose native-layer logging:

```bash
TANTIVY4JAVA_DEBUG=1 java -jar my-app.jar
```

This logs every object-store GET, cache hit/miss, version conflict, and
checkpoint write to stderr.
