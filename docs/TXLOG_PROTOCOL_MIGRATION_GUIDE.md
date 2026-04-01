# Transaction Log Protocol Migration Guide

## For: search_test team
## Version: tantivy4java txlog protocol update (Scala v0.5.4 compliance)

---

## Summary of Changes

The Rust txlog implementation has been updated to match the Scala v0.5.4 protocol exactly. This affects the on-disk format, the JNI API surface, and checkpoint behavior.

### Breaking Changes

| Change | Before | After | Impact |
|--------|--------|-------|--------|
| `_manifest.avro` format | JSON | Avro binary (zstd compressed) | Tables written by new Rust cannot be read by old Rust |
| Manifest directory | Per-state (`state-vN/manifest-NNNN.avro`) | Shared (`manifests/manifest-{uuid8}.avro`) | Path resolution changed |
| `timeRangeStart/End` internal type | `Option[Long]` (epoch ms) | `Option[String]` (ISO 8601 or epoch string) | Internal change only — Arrow FFI stays Int64 |
| `SkipAction.operation` | `Option[String]` | `String` (required, default empty) | Always present in JSON |
| `SkipAction.skipCount` | `Option[Int]` | `Int` (required, default 1) | Always present in JSON |
| `LastCheckpointInfo.sizeInBytes` | `Option[Long]` | `Long` (required) | Always present in `_last_checkpoint` JSON |
| `LastCheckpointInfo.numFiles` | `Option[Long]` | `Long` (required) | Always present |
| `LastCheckpointInfo.createdTime` | `Option[Long]` | `Long` (required) | Always present |
| `LastCheckpointInfo.format` | `String` (required) | `Option[String]` | May be absent |
| FileEntry Avro schema | Included `deleteOpstamp`, `docMappingJson`, `timeRangeStart`, `timeRangeEnd` | Removed (Scala compat) | Not written to manifests |
| FileEntry Avro namespace | `io.indextables.txlog` | `io.indextables.state` | Matches Scala |
| FileEntry Avro field order | `path, size, modTime, dataChange, partitionValues` | `path, partitionValues, size, modTime, dataChange` | Matches Scala |
| `hasFooterOffsets` Avro type | Nullable boolean | Non-nullable boolean (default false) | Schema change |
| Auto-checkpoint | Full compaction every write | Incremental with tombstones, selective compaction | More efficient |
| `_manifest.avro` write | Unconditional `put` | Conditional `put_if_absent` | Conflict detection |
| `_last_checkpoint` update | Unconditional overwrite | Version-aware (skip if older) | Concurrent safety |
| Stale checkpoint | Trusted blindly | Probes for newer state dirs | Handles stale `_last_checkpoint` |

---

## What search_test Needs to Change

### 1. Arrow FFI Column Types — NO CHANGE for timeRange

**`timeRangeStart` and `timeRangeEnd`** columns remain `Int64` in the Arrow FFI batch. No code changes needed in `ArrowFileEntryExtractor` or `ActionsToArrowConverter`.

Internally, the Rust code now stores these as `Option<String>` (for Scala protocol compatibility with ISO 8601 format), but the Arrow FFI export converts String→i64 transparently. Values written as integers in version files are preserved through the roundtrip.

### 2. SkipAction Field Changes

**`operation`** is now always present (empty string default). **`skipCount`** is now always present (default 1).

**In Arrow FFI reading:**
```scala
// BEFORE:
val operation = getOptionalString(batch, row, "operation")
val skipCount = getOptionalInt(batch, row, "skip_count")

// AFTER:
val operation = getString(batch, row, "operation")  // never null
val skipCount = getInt(batch, row, "skip_count")    // never null, default 1
```

Your `SkipAction` case class should match:
```scala
case class SkipAction(
  path: String,
  skipTimestamp: Long,
  reason: String,
  operation: String = "",       // was Option[String]
  partitionValues: Option[Map[String, String]] = None,
  size: Option[Long] = None,
  retryAfter: Option[Long] = None,
  skipCount: Int = 1            // was Option[Int]
)
```

### 3. LastCheckpointInfo Field Changes

Your existing `LastCheckpointInfo` case class already matches Scala. No changes needed — the Rust layer now produces the same JSON format as Scala.

### 4. FileEntry Fields Not in Avro

The following `AddAction` fields are **no longer written to Avro manifest files** (removed for Scala compatibility):
- `deleteOpstamp`
- `docMappingJson` (use `docMappingRef` + schema registry instead)
- `timeRangeStart` / `timeRangeEnd`

These fields are still present in **version files** (JSON) and in the **Arrow FFI** batch. They are just not persisted in the Avro state checkpoint manifests. If your code reads these from the Arrow FFI listFiles result, they will still be populated from version file replay.

### 5. Version File Compression

Version files have a **2-byte compression indicator prefix** before the gzip data. The Rust reader now correctly handles this format (strips prefix, then decompresses). If your code reads version files directly, be aware of this prefix.

---

## Checkpoint Behavior Changes

### Incremental Checkpoints

The Rust writer now uses **incremental checkpoints** matching Scala behavior:
- Previous manifest references are **reused** (not rewritten)
- Only new file entries are written to new shared manifests
- Tombstones accumulate across incremental checkpoints
- No full replay needed on each checkpoint

### Compaction Triggers

Compaction (full manifest rewrite) triggers when:
1. **Tombstone ratio > 10%** of total manifest entries
2. **Manifest count > 20** (fragmentation)

When triggered, compaction uses **selective compaction**: only dirty manifests (high tombstone ratio) are rewritten; clean manifests are kept as-is. This matches Scala's `TombstoneDistributor` behavior.

### Stale Checkpoint Detection

The reader now probes for `state-v(N+1)`, `state-v(N+2)`, etc. when reading `_last_checkpoint` to detect if the checkpoint hint is stale due to concurrent writes. This matches Scala's `verifyCheckpointVersion`.

### Conditional Writes

- `_manifest.avro` is now written with `put_if_absent` (atomic commit point)
- `_last_checkpoint` is only updated if the new version is newer than the existing one

---

## Checkpoint Interval

Your current checkpoint interval is **10** (set via `spark.indextables.checkpoint.interval`).

**Do you need to change it?** Probably not. The new incremental checkpoint mode makes frequent checkpoints much cheaper:
- Before: every checkpoint rewrote ALL manifest files (O(total files))
- After: incremental checkpoints only write NEW manifest files + accumulate tombstones (O(new files))

With incremental writes, a checkpoint interval of 10 is reasonable even for high-throughput workloads. You may even want to **decrease** it (e.g., to 1 or 5) for faster recovery, since the cost per checkpoint is now proportional to the number of changes, not total table size.

---

## Streaming Version Filtering

A new `streaming` module provides version-based manifest and entry filtering:
- `filter_manifests_by_version(manifests, since_version)` — skips manifests with `maxAddedAtVersion <= since_version`
- `filter_entries_by_version_range(entries, from, to, tombstones)` — filters entries by `addedAtVersion` range
- `compute_changes(manifest, entries, from, to)` — returns a `ChangeSet` with adds and removes

This enables incremental processing without rescanning existing data.

---

## Backward Compatibility

### Reading tables written by old Rust code

Tables written by the previous Rust implementation (JSON `_manifest`, per-state manifests) **cannot be read by the new code** without migration. The new reader only supports Avro binary format.

**Migration path:** Re-checkpoint the table using the new Rust writer. This reads the old format via version file replay and writes a new Scala-compatible checkpoint.

### Reading tables written by Scala

This was the primary goal. The new Rust code can read tables written by Scala v0.5.4 including:
- Avro binary `_manifest.avro` with zstd compression
- Shared `manifests/` directory
- Tombstones (filtering applied on read)
- Schema registry deduplication (`docMappingRef` → full JSON lookup)
- Partition bounds on manifests
- Post-truncate self-contained checkpoints (cached metadata)
- Gzipped version files with 2-byte compression indicator prefix

### Reading tables written by new Rust code

Tables written by the new Rust code use the Scala protocol and can be read by both:
- The new Rust code
- The Scala v0.5.4 code

---

## Regression Test Coverage

The implementation includes **55 protocol regression tests** in `protocol_regression_tests.rs` and **17 Scala-generated fixtures** in `test_fixtures/indextables_spark_v0.5.4/`:

| Fixture | Covers |
|---------|--------|
| 01_single_write | Basic write + checkpoint |
| 02_five_appends | Shared manifest reuse across versions |
| 03_with_removes | Overwrite creating removes |
| 04_partitioned | Partition values and bounds |
| 05_many_transactions | 15 writes, multiple checkpoints |
| 06_schema_dedup | docMappingRef + schemaRegistry |
| 07_two_writes_no_checkpoint | Sub-checkpoint-interval writes |
| 08_ten_writes_at_checkpoint | Checkpoint boundary |
| 09_partitioned_multi_append | Partition bounds across appends |
| 10_large_batch | 500 rows single write |
| 11_mixed_operations | Append/overwrite/append sequence |
| 12_partition_drop | DROP PARTITIONS command |
| 13_after_purge | PURGE old versions |
| 14_after_merge | MERGE SPLITS command |
| 15_drop_then_purge | Combined drop + purge |
| 16_with_skip_actions | Merge with skip/cooldown |
| 17_truncated_log | TRUNCATE TIME TRAVEL (self-contained checkpoint) |

To regenerate fixtures after Scala protocol changes:
```bash
cd /Users/schenksj/tmp/x/it_native_refactor/indextables_spark
git checkout v0.5.4
JAVA_HOME=/opt/homebrew/opt/openjdk@11 mvn test-compile scalatest:test \
  -DwildcardSuites='io.indextables.spark.transaction.avro.FixtureGeneratorTest'
```

---

## Testing Recommendations

1. **Run existing integration tests** — verify they pass with the new native library
2. **Test time range fields** — verify ISO 8601 string values work end-to-end
3. **Test skip actions** — verify `operation` and `skipCount` are always present
4. **Test checkpoint read** — verify the Arrow FFI batch has correct column types
5. **Test incremental checkpoint** — add files, remove files, verify tombstones accumulate
6. **Test compaction** — add many removes to trigger compaction (>10% tombstone ratio)
7. **Test partition drop** — drop partitions, verify tombstones applied correctly
8. **Test post-truncate read** — truncate time travel, verify data still readable from checkpoint
9. **Test stale checkpoint** — concurrent writes, verify reader finds latest state

---

## JNI API Surface

No JNI method signatures changed. The changes are all in the data format:
- `TransactionLogReader.listFilesArrowFfi()` — SkipAction fields always present (operation, skipCount no longer nullable); timeRange columns unchanged (still Int64)
- `TransactionLogWriter.writeVersionArrowFfi()` — expects new column types
- `TransactionLogWriter.writeVersion()` — JSON format unchanged
- `TransactionLogWriter.initializeTable()` — unchanged

Configuration keys unchanged:
- `checkpoint_interval` — still supported, still defaults to 10
- `cache.ttl.ms` — unchanged
- `session.timezone.offset.seconds` — unchanged
