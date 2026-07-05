# Table-Format Reader Code Review

**Scope:** `native/src/delta_reader/`, `native/src/iceberg_reader/`, `native/src/parquet_reader/`,
shared helpers in `native/src/common.rs`, and the corresponding Java entry points
(`io.indextables.tantivy4java.delta/iceberg/parquet`).

**Date:** 2026-07-04

Overall the code is well-structured (clean driver/executor split, good unit-test coverage of the
serialization and predicate layers, careful Arrow FFI export with all-or-nothing writes). The
findings below are ordered by severity. Line numbers refer to the current `main` working tree.

---

## Resolution status (2026-07-04)

All findings were independently re-validated against the code (all confirmed) and addressed as
follows. Line numbers in the finding text below refer to the pre-fix tree.

| Finding | Status | Resolution |
|---------|--------|------------|
| H1 | ✅ Fixed | `PartitionPredicate::evaluate_partial()` (Kleene three-valued logic); `getTableInfo` pruning keeps directories whose predicate is indeterminate at the first level; executors re-apply the full predicate |
| H2 | ✅ Fixed | `get_table_info_async` walks down one partition directory chain (one LIST per level) to discover all partition columns |
| H3 | ✅ Fixed | `get_snapshot_info` scans all checkpoint parts until metaData (and protocol) rows are found; missing column in one part is no longer an error |
| H4 | ✅ Fixed | `protocol` action is read from the checkpoint and validated (`validate_protocol`); unknown readerFeatures / minReaderVersion > 3 / columnMapping mode `id` are rejected with clear errors |
| H5 | ✅ Fixed | After the sequential probe, one offset-bounded LIST (`find_log_markers_after`) detects newer checkpoints; the reader re-probes from the newest checkpoint, and a broken commit chain errors loudly |
| H6 | ✅ Fixed | Compact mode now always includes `has_deletion_vector` (Delta) and `content_type` (Iceberg); only `partition_values` (and iceberg `sequence_number`) are skipped |
| H7 | ✅ Fixed | `create_object_store` parses the container from the URL username for `abfs`/`abfss` and derives the account from the host when not configured; `parquet_schema_reader` preserves userinfo in base URLs |
| H8 | ✅ Fixed | S3/Azure builders now start from `from_env()` (env vars, standard chain) with explicit config overriding |
| M1 | ✅ Fixed | Shared `percent_decode` in common.rs applied in `delta_log_prefix`, `url_to_object_path`, and `parse_file_url`. Delta `add.path` values remain verbatim (percent-encoded per Delta spec) — documented convention: callers joining `table_root + path` must decode |
| M2 | ✅ Fixed | Malformed non-empty commit lines now fail the call with file + line number |
| M3 | ✅ Fixed | `translate_to_file_io_props` maps catalog-style keys (aws_access_key_id, region_name, …) to FileIO keys; vended-credential catalogs documented as requiring the catalog path |
| M4 | ✅ Fixed | `sequence_number` captured on every entry (TANT full mode + Arrow FFI column 8 + Java getter) |
| M5 | ✅ Fixed | Unresolvable `schema_id` is now an error in both `read_schema_with_catalog` and `get_snapshot_info_with_catalog` |
| M6 | ✅ Fixed | `resolved_snapshot_id` embedded in every serialized entry (`IcebergFileEntry.getResolvedSnapshotId()`); missing per-entry snapshot fallback unified to `-1` sentinel in the manifest path |
| M7 | ✅ Partly fixed | Runtime creation unified to `new_current_thread()` everywhere (scan.rs was multi-threaded). Shared-runtime/client caching via `runtime_manager` left as a follow-up optimization |
| M8 | ✅ Fixed | `literal_to_string_typed` renders Date/Timestamp in ISO form and decimals with scale applied, using each manifest's partition result types |
| L1 | ✅ Fixed | Hidden-file (`.`/`_`) filter applied to root-level listing and schema selection |
| L2 | ✅ Fixed | Missing `size` now serializes as `-1` (documented on `DeltaFileEntry.getSize()`) |
| L3 | ✅ Fixed | Unused `LastCheckpointInfo.size` removed |
| L4 | ✅ Documented | Defensive mapping retained (only rewrites physical-name keys); collision hazard documented at the call site |
| L5 | ✅ Fixed | `write_field_header`/`write_string`/`extract_jlong_array` consolidated into common.rs |
| L6 | ✅ Fixed | `debug_assert` at the serialization layer; 2 GiB guard error now points at the distributed APIs |
| L7 | ⏸ Deferred | Exception taxonomy is a cross-layer API design change; all errors still surface as RuntimeException (messages made more distinguishable in M2/L9 fixes) |
| L8 | ✅ Fixed | `nativeGetCurrentSnapshotId` throws on config-extraction failure like its siblings |
| L9 | ✅ Fixed | Missing commit file now reports "version may exceed the table's latest version or the commit has been removed by log cleanup" |
| L10 | ✅ Fixed | Log-change entries serialize `table_version = -1` (documented sentinel) instead of a real-looking 0 |

---

## High severity

### H1. Parquet `getTableInfo` predicate pruning can silently drop *all* data for multi-level partitions

`parquet_reader/jni.rs:62-70` prunes partition directories by evaluating the user predicate against
partition values parsed from the directory path. But `get_parquet_table_info` uses
`list_with_delimiter` (`parquet_reader/distributed.rs:100-140`), so each directory entry is only the
**first** partition level (e.g. `year=2024/`). Predicate semantics for a missing column are
"exclude" (`common.rs:342-379`: `Eq`/`Gt`/`In` on a missing column → `false`).

Consequence: for a table partitioned by `year/month`, a filter like `month = '01'` — or
`AND(year=2024, month=01)` — evaluates `month` against a map that only contains `year`, returns
`false` for every directory, and prunes **everything**. The caller gets an empty table with no
error. The Java API (`ParquetTableReader.getTableInfo(url, config, filter)`) passes the same
`PartitionFilter` object to both levels, so this is the natural way users will hit it.

Fix options: (a) evaluate only the sub-predicates whose column is present at this level (partial
evaluation with "unknown → keep"), or (b) recurse with `list_with_delimiter` down each partition
level before pruning, or (c) restrict driver-side pruning to predicates that reference only
first-level columns and document it.

### H2. `ParquetTableInfo.partition_columns` only reports the first partition level

Same root cause as H1: the "Walk the path to find all partition levels" loop
(`parquet_reader/distributed.rs:126-137`) walks a prefix that can never contain more than one
`key=value` segment, because `list_with_delimiter` returns single-level prefixes. A `year/month/day`
table reports `partition_columns = ["year"]`. Any downstream schema/partition handling built on this
field is wrong for multi-level tables.

### H3. Delta distributed reads assume the `metaData` action lives in the first checkpoint part

`get_snapshot_info` reads schema and partition columns only from `checkpoint_part_paths[0]`
(`delta_reader/distributed.rs:120-124`). The Delta protocol does not guarantee which part of a
multi-part checkpoint contains the `metaData` row — writers distribute actions across parts. When
`metaData` is in another part, this fails with "No metaData row found in checkpoint" (or, if a part
has no `metaData` **column** at all, "Checkpoint parquet has no 'metaData' column") for a perfectly
valid table. The fallback should scan subsequent parts until a `metaData` row is found.

### H4. Delta distributed path performs no protocol / reader-feature check

The hand-rolled distributed reader (`get_snapshot_info`, `read_checkpoint_part`,
`read_post_checkpoint_changes`) never reads the `protocol` action. It will happily process tables
whose reader features it does not implement:

- **V2 checkpoints** (`v2Checkpoint` feature): checkpoint files are UUID-named and use
  `sidecar` actions. `construct_checkpoint_paths` (`distributed.rs:355-367`) only builds classic
  names, so this at least fails loudly — but with a confusing "Failed to HEAD checkpoint" error
  rather than "unsupported table feature".
- **Deletion vectors:** files are surfaced with `has_deletion_vector`, but `num_records` still
  reflects the pre-delete count and nothing forces the caller to notice (see H6).
- **Column mapping mode `id`** (vs `name`): `build_column_mapping` only handles
  `delta.columnMapping.physicalName` metadata; `id` mode tables would pass through untranslated.

A cheap `protocol` check in `get_snapshot_info` (the protocol row is in the checkpoint next to
`metaData`, and in commit 0) that rejects unknown `readerFeatures` would convert silent/confusing
failures into a clear error. Note the non-distributed `list_delta_files` path is safe here because
delta-kernel enforces the protocol.

### H5. Stale `_last_checkpoint` + log cleanup can silently return an old snapshot

`get_current_version` computes `current = checkpoint_version + count(sequential commit files after
it)` (`distributed.rs:264-265`), and `get_snapshot_info` builds its commit list the same way
(`list_commit_files_after`, `distributed.rs:378-409`). The Delta spec explicitly allows
`_last_checkpoint` to lag behind the newest checkpoint. If `_last_checkpoint` points at version *M*
while a newer checkpoint exists at *N* and log cleanup has deleted commit JSONs in `(M, N]`, the
sequential HEAD probe stops at the first missing file and the reader reports version *M* — an
outdated snapshot — with no error. Mitigation: after probing, verify the commit chain actually
reaches a version ≥ the newest checkpoint (e.g. one `list` of `_delta_log/` bounded by prefix, or a
probe for a newer `*.checkpoint.parquet`), or at minimum document the retention assumption.

### H6. "Compact" serialization mode drops correctness-critical flags

- Delta: compact mode omits `has_deletion_vector` (`delta_reader/serialization.rs:76-85`). A
  compact-mode consumer scanning the listed parquet files directly will include logically deleted
  rows with no signal that DVs exist.
- Iceberg: compact mode omits `content_type` (`iceberg_reader/serialization.rs:75-85`). Position-
  and equality-delete files become indistinguishable from data files, so a consumer will read
  delete files as data *and* miss the deletes they encode.

Compact mode should either keep these one-byte/short fields (they are tiny compared to the path
string) or refuse to serialize entries where `has_deletion_vector == true` / `content_type !=
"data"`.

### H7. `abfss://` / `abfs://` URLs parse the container from the wrong URL component

`create_object_store` uses `url.host_str()` as the Azure container name
(`delta_reader/engine.rs:77-98`). The standard ABFS URL shape is
`abfss://<container>@<account>.dfs.core.windows.net/path`, where the container is the **username**
component and `host_str()` is the account endpoint. The current code passes
`account.dfs.core.windows.net` as the container. `az://container/path` works; `abfss://` in its
canonical form cannot. Parse `url.username()` when non-empty for the `abfs`/`abfss` schemes (and
derive the account name from the host if not configured).

### H8. No default cloud credential chain

`AmazonS3Builder::new()` / `MicrosoftAzureBuilder::new()` are used with only the explicitly
configured keys (`engine.rs:48-98`). Neither reads environment variables, profiles, IMDS/IRSA, or
workload identity. Deployments that rely on IAM instance roles or `AWS_ACCESS_KEY_ID` env vars —
the common case for Spark executors — get anonymous clients and opaque 403s. Consider
`AmazonS3Builder::from_env()` (and the Azure equivalent) as the base builder, with the config map
overriding. Also note that when `aws_region` is absent no region is set at all, which fails for S3
rather than falling back to the SDK default chain.

---

## Medium severity

### M1. Percent-encoded URL paths are passed to `object_store` as raw keys

`delta_log_prefix` (`delta_reader/distributed.rs:927-936`) and `url_to_object_path`
(`parquet_reader/distributed.rs:390-408`) build object keys from `Url::path()`, which is
**percent-encoded** (`/tmp/my table` → `/tmp/my%20table` after `normalize_url` /
`Url::from_directory_path`). `ObjectPath::from` does not decode, so any table path containing a
space, `+`, `#`, unicode, etc. resolves to the wrong object key. All existing tests use
encoding-neutral paths, so the bug is latent. Decode the path (e.g. `percent_encoding::
percent_decode_str(url.path())`) before constructing `ObjectPath`.

Related inconsistency: Delta `add.path` values are percent-encoded per the Delta spec, and the
distributed reader returns them verbatim (no decode), while the parquet reader *does* percent-decode
partition values from paths (`parquet_reader/distributed.rs:317-356`). Whoever joins
`table_root + entry.path` on the Java side needs a single documented convention.

### M2. Malformed Delta commit lines are silently skipped

`read_post_checkpoint_changes_async` ignores any line that fails JSON parsing
(`distributed.rs:624-627`, `Err(_) => continue`). A truncated or corrupted commit file (partial
upload, torn write) silently produces an incomplete file list — exactly the failure mode where you
want a loud error, since this feeds incremental sync (`get_changes_between`). Recommend failing the
whole call on unparseable non-empty lines.

### M3. Iceberg executor-side manifest reads bypass the catalog's credential model

`read_iceberg_manifest` builds a `FileIO` directly from the raw config map
(`iceberg_reader/distributed.rs:129-155`). This only works when the map happens to contain
FileIO-style keys (`s3.access-key-id`, …). Two real setups break:

- **Glue catalog** users configure `aws_access_key_id`/`region_name` (per `catalog.rs` docs); those
  keys are meaningless to `FileIO`, so executor reads run unauthenticated.
- **REST catalogs with vended credentials** (including Unity Catalog): storage credentials are
  vended per-table by the catalog at `load_table` time. Bypassing the catalog loses them entirely.

At minimum, translate known catalog-style keys to FileIO keys and document that vended-credential
catalogs require the full (slow) catalog path; ideally offer a mode that obtains FileIO from a
one-time `load_table` and reuses it.

### M4. Iceberg delete-file semantics are not actionable

`list_files_with_catalog` / `read_manifest_with_file_io` return delete files interleaved with data
files, distinguished only by `content_type`. Sequence numbers — required to decide *which* data
files a position/equality delete applies to — are not captured (`iceberg_reader/scan.rs:243-252`,
`distributed.rs:250-277` ignore `entry.sequence_number()`). A consumer cannot correctly apply
deletes from this output, and `record_count` on data files overcounts. Either expose
`sequence_number` (data + delete files) or document that tables with delete files are unsupported
and consider failing when `content_type != "data"` is encountered.

### M5. Silent schema fallback on Iceberg time travel

When a snapshot's `schema_id` doesn't resolve, both `read_schema_with_catalog`
(`iceberg_reader/scan.rs:305-319`) and `get_snapshot_info_with_catalog`
(`distributed.rs:185-190`) silently fall back to `current_schema()`. For time-travel reads this can
return a schema that does not match the snapshot's data with no warning. An error (or at least a
logged warning) would be safer.

### M6. Resolved snapshot ID is dropped from Iceberg `listFiles` results

`serialize_iceberg_entries` takes `_actual_snapshot_id` and ignores it
(`iceberg_reader/serialization.rs:23-27`). The per-entry `snapshot_id` is the snapshot that *added*
each file, so a caller listing "latest" has no way to learn which snapshot was actually read —
unlike Delta, which embeds `table_version` in every entry. This matters for consistent
list-then-poll patterns (`getCurrentSnapshotId` comparisons against an unknown baseline).

Related inconsistency: the fallback for a missing per-entry snapshot id is
`manifest_file.added_snapshot_id` in `scan.rs:250` but `0` in `distributed.rs:275`.

### M7. One-shot Tokio runtimes and clients per JNI call

Every call builds a fresh runtime, object store / catalog client, and connection pool
(`delta_reader/distributed.rs:85-87` et al.). Iceberg's `scan.rs` additionally uses a
**multi-threaded** `Runtime::new()` (`scan.rs:157`, `281`, `371`) where every other entry point uses
`new_current_thread()` — inconsistent and heavier (spawns worker threads per call). For
streaming-poll usage (`get_current_version` "on every poll cycle") this means re-doing TLS/auth
setup each tick. The repo already has `runtime_manager.rs`; routing these through a shared runtime
and caching `ObjectStore` instances keyed by (scheme, bucket, creds-hash) would cut latency and FD
churn. If per-call runtimes are kept, they at least guarantee no cross-call state, but the
current_thread/multi-thread mix should be unified.

### M8. Iceberg partition values for non-string types use Debug/raw representations

`literal_to_string` (`iceberg_reader/scan.rs:102-120`) renders `Date` as its underlying epoch-day
int, `Timestamp` as micros, and falls back to `format!("{:?}")` for decimals, fixed, binary, and
non-primitive literals (producing values like `Decimal(12345)`). Predicate evaluation is pure string
compare on these (`common.rs`), so a user filtering `date = '2024-01-01'` against an
identity-partitioned date column silently matches nothing (the stored value is `"19723"`). Delta, by
contrast, carries partition values as the human-readable strings from the log. At minimum this
asymmetry needs documenting; better, render date/timestamp literals in ISO form to match user
expectations and Delta behavior.

---

## Low severity / polish

- **L1. Hidden-file filtering is inconsistent in the parquet reader.** `list_partition_files`
  skips `.`/`_`-prefixed files (`parquet_reader/distributed.rs:220-224`), but root-level collection
  (`:143-154`) and `read_schema_from_first_file`'s root loop (`:255-271`) do not. A
  `_delta_log`-adjacent temp file like `_tmp.parquet` at the root would be listed and could be
  picked as the schema source.
- **L2. Missing-size sentinel conflation.** Checkpoint adds with a missing `size` become `0`
  (`delta_reader/distributed.rs:565`) and missing `num_records` becomes `-1` in serialization; `0`
  is a legal size. Consider `-1` for unknown size too.
- **L3. `LastCheckpointInfo.size` is parsed but never used** (`distributed.rs:61-66`). Either
  validate the checkpoint row count against it or drop the field.
- **L4. Delta "defensive" column mapping can double-translate.** `list_delta_files` applies
  `apply_column_mapping` on top of delta-kernel output "just in case" (`scan.rs:95-104`). If kernel
  already returns logical names and some physical name collides with another column's logical name,
  keys get remapped incorrectly. Prefer trusting the kernel (add a test pinning its behavior) over
  double-mapping.
- **L5. `get_jstring`-style duplication.** `extract_jlong_array` is duplicated verbatim in
  `delta_reader/jni.rs:458-469` and `iceberg_reader/jni.rs:435-446`; the TANT `write_field_header` /
  `write_string` helpers are copy-pasted in all three `serialization.rs` files. Move to `common.rs`.
  (The `std::mem::forget(safe_arr)` in `extract_jlong_array` is unnecessary in jni 0.21 — dropping a
  borrowed `JLongArray` wrapper does not delete the reference — but it is harmless.)
- **L6. TANT offsets are `u32`.** Serialization silently assumes buffers < 4 GiB; the JNI layer's
  2 GiB `i32::MAX` guard in `buffer_to_jbytearray` (`common.rs:45-78`) happens to protect it, but a
  debug assert at the serialization layer would make the invariant local. Related: the
  non-distributed `nativeListFiles` materializes the entire table file list in one buffer, so very
  large tables (the code's own test data mentions 61M add files) will hit this guard — the
  distributed API is the answer, but the error message won't say so.
- **L7. All errors surface as `java.lang.RuntimeException`** (`common.rs:16-19`). Callers cannot
  distinguish "table not found" from "credentials rejected" from "unsupported feature" without
  string matching. A small exception taxonomy (or an error-code prefix convention) would help the
  Spark-facing layer.
- **L8. `nativeGetCurrentSnapshotId` swallows config-extraction errors.**
  `iceberg_reader/jni.rs:221-224` returns `-1` without throwing when `extract_hashmap` fails,
  whereas every sibling entry point throws. `-1` is not otherwise a legal return here (errors throw),
  so a config marshalling bug becomes indistinguishable from... nothing — the caller just sees -1.
- **L9. Delta `get_changes_between` trusts the caller's `to_version`.** If `to_version` exceeds the
  actual latest version, the loop fails with "Failed to read commit N.json: not found" rather than a
  clear "version out of range" (`distributed.rs:285-309`). Cheap to pre-validate via the same HEAD
  probe used elsewhere.
- **L10. `serialize_log_changes` hard-codes `table_version = 0`** for added entries
  (`delta_reader/serialization.rs:255-259`); the Java `DeltaFileEntry` presumably exposes this as a
  real-looking version. A `-1` sentinel would be less misleading.

---

## Positive observations

- The Arrow FFI export paths validate all target addresses up front and build every
  `FFI_ArrowArray`/`FFI_ArrowSchema` before writing any, so a mid-export failure cannot leave Java
  holding half-initialized structs (`delta_reader/distributed.rs:1006-1048`).
- Delta log replay (`read_post_checkpoint_changes_async`) correctly implements last-action-wins
  per path with oldest-first ordering, including re-add-after-remove, and is well tested.
- Iceberg partition-value extraction correctly uses **each manifest's own partition spec** rather
  than the table default, which is the right call for spec-evolved tables, and there is a test
  documenting the intent.
- `list_commit_files_after`'s sequential HEAD probing is a smart O(k) alternative to listing a
  200K-object `_delta_log/` (subject to H5 above).
- The predicate engine has clearly documented missing-column semantics, `total_cmp` NaN handling,
  and thorough tests including numeric-vs-lexicographic edge cases.
- `percent_decode` in the parquet reader handles multi-byte UTF-8 sequences correctly (accumulating
  bytes before UTF-8 validation), with tests.

---

## Suggested priorities

1. H1/H2 (parquet multi-level partition pruning) — silent, total data loss for a mainstream layout.
2. H6 (compact mode dropping DV/content-type flags) — silent wrong results on tables with deletes.
3. H7/H8 (abfss parsing, credential chain) — hard blockers for common deployments.
4. H3/H4/H5 (Delta distributed protocol gaps) — correctness under multi-part checkpoints, newer
   Delta features, and log cleanup.
5. M1 (percent-encoded keys) — latent, will surface as unreproducible "file not found" on paths
   with spaces.
