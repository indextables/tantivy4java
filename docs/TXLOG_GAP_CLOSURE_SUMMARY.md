# Transaction Log Gap Closure Summary

All 8 gaps identified in `docs/gaps/` have been closed. This document summarizes the changes and what consumers need to know.

## GAP-1: Generic Version Writer

**Problem**: Only single-type version files were supported (addFiles, removeFile, skipFile). Operations like `initialize()`, `commitMergeSplits()`, and `overwriteFiles()` need mixed-action version files.

**New APIs**:

```java
// Write mixed actions with automatic retry (10 attempts, exponential backoff)
WriteResult result = TransactionLogWriter.writeVersion(tablePath, config, actionsJson);

// Single attempt, no retry — version=-1 on conflict
WriteResult result = TransactionLogWriter.writeVersionOnce(tablePath, config, actionsJson);
```

**actionsJson format** — standard Delta-compatible JSON-lines:
```
{"protocol":{"minReaderVersion":4,"minWriterVersion":4}}
{"metaData":{"id":"abc","schemaString":"{...}","partitionColumns":[],"configuration":{}}}
{"add":{"path":"new.split","size":5000,"partitionValues":{},"modificationTime":0,"dataChange":true}}
{"remove":{"path":"old.split","deletionTimestamp":1700000000000,"dataChange":true}}
```

---

## GAP-2: listVersions API

**Problem**: No way to enumerate version file numbers for DESCRIBE TRANSACTION LOG.

**New API**:

```java
long[] versions = TransactionLogReader.listVersions(tablePath, config);
// Returns sorted array: [0, 1, 2, 3, ...]
```

---

## GAP-3: Missing FileEntry Fields

**Problem**: `TxLogFileEntry` was missing `deleteOpstamp` (critical for merge/prewarm) and `docMappingRef` (schema dedup hash).

**Changes**:

| Field | Type | Default | Usage |
|-------|------|---------|-------|
| `deleteOpstamp` | `long` | `-1` | Merge operations, prewarm, split search engine |
| `docMappingRef` | `String` | `null` | Schema deduplication hash reference |

```java
TxLogFileEntry entry = ...;
if (entry.hasDeleteOpstamp()) {
    long opstamp = entry.getDeleteOpstamp();
}
String ref = entry.getDocMappingRef(); // null if full docMappingJson is inline
```

**Arrow FFI impact**: Schema grew from 23 to 25 columns. New columns:
- Column 12: `delete_opstamp` (Int64, nullable)
- Column 16: `doc_mapping_ref` (Utf8, nullable)

**Avro field ID**: `deleteOpstamp` = 123

---

## GAP-4: Native Caching (Option B)

**Problem**: Every JNI call hit cloud storage — expensive for repeated reads.

**Implementation**: Internal LRU cache keyed by `(tablePath, cacheTtlMs)` with automatic TTL expiration.

**Configuration**:

```java
Map<String, String> config = new HashMap<>();
config.put("aws_access_key_id", "...");
config.put("aws_region", "us-east-1");

// Cache TTL (default: 300000ms = 5 minutes)
config.put("cache.ttl.ms", "300000");

// Disable caching
config.put("cache.ttl.ms", "0");
```

**What is cached**:
- `getSnapshotInfo()` — protocol, metadata, last checkpoint info (TTL-based)
- `readManifest()` — manifest file entries (permanent — manifests are immutable)
- Write operations automatically invalidate the table's cache

**No API changes** — caching is transparent. Same method signatures, same behavior, just faster on repeated calls.

---

## GAP-5: readVersion API

**Problem**: No way to read a specific version file for debugging/auditing.

**New API**:

```java
String jsonLines = TransactionLogReader.readVersion(tablePath, config, 42);
// Returns raw JSON-lines content:
// {"protocol":{"minReaderVersion":4,"minWriterVersion":4}}
// {"add":{"path":"file.split",...}}
```

Handles both plain `.json` and gzip-compressed `.json.gz` formats transparently.

---

## GAP-6: Expanded SkipAction Fields

**Problem**: `TxLogSkipAction` only had 3 fields (path, reason, skipCount). Missing fields needed for cooldown/retry mechanism.

**New fields**:

| Field | Type | Default | Usage |
|-------|------|---------|-------|
| `skipTimestamp` | `long` | `-1` | When the skip was recorded |
| `operation` | `String` | `null` | What triggered it ("merge", "read") |
| `partitionValues` | `Map<String,String>` | empty | Partition info for skipped file |
| `size` | `long` | `-1` | File size |
| `retryAfter` | `long` | `-1` | Epoch ms after which retry is allowed |

**Cooldown check**:

```java
TxLogSkipAction skip = ...;
if (skip.isInCooldown()) {
    // File should not be retried yet
}
```

**JSON format for skipFile()**:

```json
{
  "path": "file.split",
  "skipTimestamp": 1700000000000,
  "reason": "merge_conflict",
  "operation": "merge",
  "retryAfter": 1700000300000,
  "skipCount": 1
}
```

---

## GAP-7: splitTags Type Fix

**Problem**: `splitTags` was `Map<String, String>` but Scala uses `Set[String]` and Avro stores it as `array<string>`.

**Change**: `Map<String, String>` → `List<String>` (Java) / `Vec<String>` (Rust)

```java
// Before (WRONG)
Map<String, String> tags = entry.getSplitTags();

// After (CORRECT)
List<String> tags = entry.getSplitTags();
// Values: ["env:prod", "region:us-east-1"]
```

**Migration note**: If you were using `splitTags.get("key")`, change to `splitTags.contains("key:value")` or iterate the list.

---

## GAP-8: Table Initialization

**Problem**: No dedicated method to create a new table (version 0 with Protocol + Metadata).

**New API**:

```java
String protocolJson = "{\"minReaderVersion\":4,\"minWriterVersion\":4}";
String metadataJson = "{\"id\":\"abc-123\",\"schemaString\":\"{...}\",\"partitionColumns\":[],\"configuration\":{}}";

TransactionLogWriter.initializeTable(tablePath, config, protocolJson, metadataJson);
// Throws RuntimeException if version 0 already exists
```

---

## Column Layout Reference (25 columns)

After GAP-3, the Arrow FFI schema has 25 columns:

| Col | Name | Type | Nullable |
|-----|------|------|----------|
| 0 | `path` | Utf8 | NO |
| 1 | `size` | Int64 | NO |
| 2 | `modification_time` | Int64 | NO |
| 3 | `data_change` | Boolean | NO |
| 4 | `num_records` | Int64 | YES |
| 5 | `partition_values` | Utf8 (JSON) | YES |
| 6 | `stats` | Utf8 | YES |
| 7 | `min_values` | Utf8 (JSON) | YES |
| 8 | `max_values` | Utf8 (JSON) | YES |
| 9 | `footer_start_offset` | Int64 | YES |
| 10 | `footer_end_offset` | Int64 | YES |
| 11 | `has_footer_offsets` | Boolean | YES |
| 12 | `delete_opstamp` | Int64 | YES |
| 13 | `split_tags` | Utf8 (JSON array) | YES |
| 14 | `num_merge_ops` | Int32 | YES |
| 15 | `doc_mapping_json` | Utf8 | YES |
| 16 | `doc_mapping_ref` | Utf8 | YES |
| 17 | `uncompressed_size_bytes` | Int64 | YES |
| 18 | `time_range_start` | Int64 | YES |
| 19 | `time_range_end` | Int64 | YES |
| 20 | `companion_source_files` | Utf8 (JSON array) | YES |
| 21 | `companion_delta_version` | Int64 | YES |
| 22 | `companion_fast_field_mode` | Utf8 | YES |
| 23 | `added_at_version` | Int64 | NO |
| 24 | `added_at_timestamp` | Int64 | NO |

## Test Coverage

- **127 Rust unit tests** passing (121 original + 6 new cache tests)
- **20 Java JNI integration tests** (require native build)
