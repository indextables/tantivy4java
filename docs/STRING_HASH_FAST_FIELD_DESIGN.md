# String Hash Fast Field Optimization for Parquet Companion HYBRID Mode

## Problem Statement

In HYBRID mode, string fast fields (parquet `BYTE_ARRAY`/`STRING` columns) are **never stored in the native tantivy `.fast` bundle**. They only appear after `transcode_and_cache` reads every row of the parquet column, builds a `VoidSSTable` compressed dictionary, bitpacks the per-document ordinals, and merges the result with the native bundle. This is expensive:

- Must read **all parquet rows** even if the query matches 1 document
- Must build a **full VoidSSTable** (FST-like structure) over every distinct value
- Cost is proportional to **total row count × column width**, not result set size
- For a 500K-row split with a `status` string column: **100ms–1s** of I/O and CPU before any aggregation begins

Common operations do not need the actual string values at all — they only need to know *whether* a value is present or *whether* it equals a specific value. For these cases we can store a pre-computed numeric hash alongside each document and use it instead of the transcoded string column.

---

## Key Research Findings

### How string fast fields work in tantivy

A string fast field is a **dictionary-encoded columnar file** with two components:

```
[VoidSSTable: sorted terms → ordinals (compressed)]
[Column<u64>: per-document term ordinals (bitpacked)]
[col_index_num_bytes: u32 LE]
[dict_len: u32 LE]
```

The `StrColumn` / `BytesColumn` wraps these two pieces. Critically, tantivy's aggregation engine operates on the **ordinal column** (`Column<u64>`) during the hot collection loop — zero string decodes in the hot path. Strings are only decoded once at result-build time via `sorted_ords_to_term_cb`.

### The TermsAggregation hot path is already ordinal-based

`TermsAggregation` collects into a `FxHashMap<u64 ordinal, u32 count>` with no string allocation. The dictionary is only consulted at the end to decode ordinals back to strings for the result JSON. This means **any integer column can drive bucket collection**, and string decoding can be deferred to a targeted lookup.

### In HYBRID mode, string fast fields are unavailable without transcoding

`ParquetAugmentedDirectory.get_file_handle()` returns:
- **Cache hit**: pre-merged (native + transcoded) bytes
- **Cache miss**: falls through to inner directory → native-only bundle bytes

Native bundle bytes in HYBRID mode contain only `U64`, `I64`, `F64`, `Bool`, `Date`, `IpAddr` columns — **not** string columns. So any operation that touches a string fast field triggers a full `transcode_and_cache` call.

### `columns_to_transcode` drives I/O

`ensure_fast_fields_for_query` calls `columns_to_transcode` to find which fields referenced in the query/aggregation JSON need to be loaded from parquet. A `U64` field has `FieldSource::Native` and is excluded from this set — native fields are always in the bundle and require **zero parquet reads**.

---

## Solution Overview

For each string fast field in HYBRID mode, during companion indexing also store a hidden `U64` fast field named `_phash_<fieldname>` containing a stable 64-bit hash of the string value. This field lands in the **native tantivy `.fast` bundle** — no parquet transcoding ever needed.

At query time, a pre-processing step rewrites aggregation/query JSON to use `_phash_<fieldname>` wherever the actual string value is not required. For `TermsAggregation` (which *does* need string bucket keys), a **post-query touch-up pass** resolves hash bucket keys back to real strings by scanning the native hash column to find one representative document per bucket, then reading those specific cells from parquet.

---

## Hash Field Specification

### Naming convention

```
_phash_<tantivy_field_name>
```

Examples: `_phash_status`, `_phash_category`, `_phash_event_type`

**Conflict detection**: at schema derivation time, if a user parquet column is literally named `_phash_X` and X is also a parquet column, log a warning and skip adding the hash field for X.

### Hash encoding

| Value | Stored `u64` |
|-------|-------------|
| null / absent | *field absent for document* (tantivy null = no value) |
| non-null string | `xxh64(utf8_bytes, seed=0)` if result ≠ 0, else `1` |

The `0` sentinel is never stored for non-null values (guarantees null-distinguishability). The probability that `xxh64` produces `0` for a real string is 1/2^64 ≈ 5×10⁻²⁰.

### Hash function

**xxHash 64-bit** (`xxhash-rust` crate, `xxh64` feature). Requirements:
- Deterministic across JVM restarts and machines
- Non-cryptographic (fast)
- 64-bit output (negligible collision probability)

```toml
# Cargo.toml
xxhash-rust = { version = "0.8", features = ["xxh64"] }
```

```rust
use xxhash_rust::xxh64::xxh64;

fn hash_string_value(s: &str) -> u64 {
    let h = xxh64(s.as_bytes(), 0);
    if h == 0 { 1 } else { h }
}
```

### Collision probability

For N distinct string values: approximately N²/2^65 (birthday problem).
- N = 10,000 → ~3×10⁻¹² (negligible)
- N = 1,000,000 → ~3×10⁻⁸ (negligible)

This is the same order of approximation as HyperLogLog-based cardinality estimation, which is already accepted in production Elasticsearch/Quickwit deployments.

---

## Manifest Extension

Add `string_hash_fields` to `ParquetManifest` to record which string fields have hash counterparts:

```rust
// manifest.rs
pub struct ParquetCompanionManifest {
    // ... existing fields ...

    /// Maps tantivy string field name → hash field name for HYBRID mode optimization.
    /// Only populated in HYBRID mode. Empty map means no hash fields present
    /// (e.g., old splits created before this feature).
    #[serde(default)]
    pub string_hash_fields: HashMap<String, String>,  // "status" → "_phash_status"
}
```

Backward compatible: old splits have an empty map → all optimizations are no-ops.

---

## Phase 1: Indexing — Add Hash Fields

### `schema_derivation.rs`

For each column whose **tantivy type** is `Str` or `Bytes` and that produces a string fast field in HYBRID mode:

```rust
// After adding the text field with "raw" tokenizer:
// NOTE: Check tantivy_type, NOT parquet_type. IP address fields come from Utf8 parquet
// columns but resolve to tantivy_type == "IpAddr" — they are native fast fields in
// HYBRID mode and must not receive a hash counterpart.
if fast_field_mode == FastFieldMode::Hybrid
    && (tantivy_type == "Str" || tantivy_type == "Bytes")
{
    let hash_field_name = format!("_phash_{}", mapped_field_name);
    if !user_column_names.contains(&hash_field_name) {
        let u64_opts = NumericOptions::default().set_fast();  // fast only: not indexed, not stored
        schema_builder.add_u64_field(&hash_field_name, u64_opts);
        hash_field_map.insert(mapped_field_name.clone(), hash_field_name);
    }
}
```

### `indexing.rs` — document building loop

```rust
// Existing: add string value to text field
doc.add_text(text_field, &string_value);

// New: add xxHash64 to hidden hash field
if let Some(&hash_field) = hash_field_for.get(&field_name) {
    doc.add_u64(hash_field, hash_string_value(&string_value));
}
// Null/absent string: do nothing — tantivy represents null as absent value
```

### `merge_impl.rs` — `combine_parquet_manifests`

Union the `string_hash_fields` maps from both input manifests (they are identical for same-schema splits; add a consistency assertion):

```rust
let mut combined_hash_fields = manifest_a.string_hash_fields.clone();
for (k, v) in &manifest_b.string_hash_fields {
    debug_assert_eq!(combined_hash_fields.get(k), Some(v),
        "string_hash_fields mismatch during merge for field {}", k);
    combined_hash_fields.insert(k.clone(), v.clone());
}
result_manifest.string_hash_fields = combined_hash_fields;
```

---

## Phase 2: Query Pre-processing — Rewrite Aggregation JSON

### New file: `parquet_companion/hash_field_rewriter.rs`

Walks the aggregation and query JSON trees, replacing string field references with `_phash_` counterparts for operations that do not need actual string values.

```rust
/// Rewrite aggregation JSON to use hash fields for hash-compatible operations.
///
/// Returns: (rewritten_json, set_of_string_fields_that_still_need_transcoding)
///
/// The second return value is used by columns_to_transcode to skip string columns
/// whose operations were fully redirected to hash fields.
pub fn rewrite_aggs_for_hash_fields(
    agg_json: &str,
    string_hash_fields: &HashMap<String, String>,
) -> anyhow::Result<(String, HashSet<String>)>;

/// Rewrite query JSON: exists queries on string fast fields → hash field check.
pub fn rewrite_query_for_hash_fields(
    query_json: &str,
    string_hash_fields: &HashMap<String, String>,
) -> anyhow::Result<(String, HashSet<String>)>;
```

### Operations that CAN be redirected (no string value needed)

| Aggregation type | Redirect | Notes |
|-----------------|----------|-------|
| `value_count` | ✅ Replace `field` with `_phash_X` | Counts non-null values; hash field absent = null |
| `cardinality` | ✅ Replace `field` with `_phash_X` | Distinct hash count ≈ distinct string count |
| `terms` | ✅ Replace `field` with `_phash_X` | Buckets by hash; string keys resolved in touch-up (see Phase 3) |
| `exists` query (fast field) | ✅ Replace with range `_phash_X >= 1` | Any non-zero hash = non-null string |

### Operations that CANNOT be redirected

| Operation | Why |
|-----------|-----|
| `terms` with `min_doc_count: 0` | Must enumerate all terms including zero-count; requires full dictionary |
| `range` agg on string field | Hash ordering ≠ string ordering |
| `stats` / `extended_stats` on string | Would emit meaningless numeric min/max/sum/avg |
| `terms` with `include`/`exclude` string filters | **Can** be redirected: hash each filter value in the rewriter (see edge cases) |

### `columns_to_transcode` update

After rewriting, only pass the `fields_still_needing_transcoding` set from the rewriter to `columns_to_transcode`. If a string field's only uses were redirected to hash fields, it is absent from this set and is never transcoded.

---

## Phase 3: Post-Query Touch-up — Resolve Hash Bucket Keys to Strings

This is the key innovation that extends the hash optimization to cover `TermsAggregation`, which always needs string bucket keys in its output.

### Full flow for `TermsAggregation`

```
INPUT:  {"agg_name": {"terms": {"field": "status"}}}

STEP 1 — Rewrite (Phase 2):
  {"agg_name": {"terms": {"field": "_phash_status"}}}
  columns_to_transcode → []  (nothing to transcode!)

STEP 2 — Execute tantivy aggregation (no parquet I/O for status column):
  {"agg_name": {"buckets": [
    {"key": 14891908177209451419, "doc_count": 42},
    {"key":  3891234567890123456, "doc_count": 17},
    {"key":  9876543210987654321, "doc_count":  5}
  ]}}

STEP 3 — Touch-up: resolve hash keys → string values

  a. Collect unique hash values from result buckets:
     need_to_resolve = {14891..., 3891..., 9876...}

  b. Open _phash_status Column<u64> from native .fast bundle
     (in-memory, zero parquet I/O)

  c. Scan docs with early exit:
     for doc_id in 0..num_docs:
         hash = column.first(doc_id)
         if hash ∈ need_to_resolve and hash ∉ hash_to_docid:
             hash_to_docid[hash] = doc_id
             need_to_resolve.remove(hash)
             if need_to_resolve.is_empty(): break   ← early exit

  d. Map doc_id → (parquet_file, parquet_row) via existing docid-to-row mapping

  e. Batch-read actual string values from parquet:
     read_string_cells_batch("status", [(file0, row_42), (file0, row_99), ...])
     Uses CachedParquetReader → ByteRangeCache (dictionary pages cached across calls)

  f. Build resolution map: {14891... → "active", 3891... → "pending", 9876... → "closed"}

  g. Replace hash keys in result JSON in-place.

OUTPUT: {"agg_name": {"buckets": [
  {"key": "active",  "doc_count": 42},
  {"key": "pending", "doc_count": 17},
  {"key": "closed",  "doc_count":  5}
]}}
```

### Performance characteristics

For a split with N_docs = 500K and K = 50 distinct string bucket keys:

| Step | Cost |
|------|------|
| Column scan (step c) | ~0.5–2ms — in-memory bitpacked u64 iteration |
| Docid → parquet row lookup (step d) | O(K) hashmap lookups, negligible |
| Batch parquet cell reads (step e) | K targeted reads; ByteRangeCache reuses dictionary pages |
| **Total touch-up** | **~2–5ms** |
| **Avoided: full parquet transcode** | **100ms–1s** (reading all N_docs rows + VoidSSTable build) |

The column scan terminates early as soon as all K representative documents are found. For uniformly distributed values, this is often within the first N_docs/K documents.

### Sub-aggregations work without modification

Sub-aggregations are computed over the same document sets whether the parent bucket is keyed by string or by hash. The touch-up only changes the bucket key label — sub-aggregation results (`avg`, `sum`, nested `terms`, etc.) are untouched.

### New file: `parquet_companion/hash_touchup.rs`

```rust
/// For each hash bucket key in `agg_result`, find a representative document,
/// look up its actual string value from parquet, and replace the hash key.
/// Mutates `agg_result` in place.
pub async fn touchup_hash_bucket_keys(
    agg_result: &mut serde_json::Value,
    fields_to_resolve: &[HashFieldResolution],  // (agg_json_path, hash_field_name, parquet_col)
    fast_field_store: &dyn FastFieldStore,
    docid_to_row: &DocIdToParquetRow,
    parquet_reader: &CachedParquetReader,
    resort_paths: &[AggPath],  // paths where order: _key requires re-sort after touch-up
) -> anyhow::Result<()>;

pub struct HashFieldResolution {
    pub agg_path: Vec<String>,      // JSON path to the TermsAgg in result tree
    pub hash_field_name: String,    // "_phash_status"
    pub parquet_column_name: String,// "status" (original parquet column name)
}
```

### New method on `CachedParquetReader`

```rust
/// Read a single column's values for a batch of specific (file, row) pairs.
/// Groups rows by file and row group for efficient I/O.
/// ByteRangeCache is used automatically — dictionary pages fetched once and reused.
pub async fn read_string_cells_batch(
    &self,
    parquet_column: &str,
    row_refs: &[(FileIndex, RowInFile)],
) -> anyhow::Result<Vec<Option<String>>>;
```

---

## Phase 4: Prewarm Integration

When `preloadParquetFastFields("status")` is called, the `_phash_status` hash field should also be marked as available in `transcoded_fast_columns`, eliminating redundant transcode checks on subsequent queries that use the hash optimization path.

### Fix 1: `augmented_directory.rs` — `effective_column_names` appends hash counterparts

```rust
pub fn effective_column_names(&self, requested: Option<&[String]>) -> Vec<String> {
    let mut names: Vec<String> = columns_to_transcode(&self.manifest, self.mode, requested)
        .into_iter()
        .map(|col| col.tantivy_name)
        .collect();
    // Hash fields are native U64, always in the bundle alongside their parent string column.
    // Register them so subsequent queries that use the hash path skip the transcode check.
    for name in names.clone().iter() {
        if let Some(hash_name) = self.manifest.string_hash_fields.get(name) {
            names.push(hash_name.clone());
        }
    }
    names
}
```

`nativePrewarmParquetFastFields` already inserts everything `effective_column_names` returns into `transcoded_fast_columns` — no changes needed in `jni_prewarm.rs`.

### Fix 2: `async_impl.rs` — lazy transcode path also registers hash counterparts

After marking columns as transcoded on success (the existing loop at line ~100–106):

```rust
{
    let mut existing = context.transcoded_fast_columns.lock().unwrap();
    for col in &columns_to_add {
        existing.insert(col.clone());
        // Hash counterpart is native U64 — always present in the merged bytes.
        if let Some(manifest) = context.parquet_manifest.as_ref() {
            if let Some(hash_name) = manifest.string_hash_fields.get(col) {
                existing.insert(hash_name.clone());
            }
        }
    }
}
```

### Fix 3: `async_impl.rs` — short-circuit when only hash fields are in `columns_to_add`

Hash fields (`_phash_*`) are native U64 — accessible via the inner directory without any parquet I/O. When they appear in `columns_to_add`, skip `transcode_and_cache` entirely:

```rust
if !columns_to_add.is_empty() {
    // Partition: hash fields are native U64, never require parquet transcoding.
    let (native_hash_cols, parquet_cols): (Vec<String>, Vec<String>) =
        if let Some(manifest) = context.parquet_manifest.as_ref() {
            let hash_values: std::collections::HashSet<&str> = manifest
                .string_hash_fields.values().map(|s| s.as_str()).collect();
            columns_to_add.iter().cloned()
                .partition(|col| hash_values.contains(col.as_str()))
        } else {
            (vec![], columns_to_add.clone())
        };

    // Register hash fields without I/O.
    if !native_hash_cols.is_empty() {
        let mut existing = context.transcoded_fast_columns.lock().unwrap();
        for col in &native_hash_cols {
            existing.insert(col.clone());
        }
    }

    // Only launch transcode I/O for actual parquet-sourced columns.
    if !parquet_cols.is_empty() {
        // ... existing full_column_set build + transcode loop, using parquet_cols ...
    }
}
```

### Invariant after these three fixes

After any prewarm or lazy-transcode path runs for string field `X`:

| Path | `transcoded_fast_columns` result |
|------|----------------------------------|
| `preloadParquetFastFields("X")` | `{"X", "_phash_X"}` |
| lazy transcode triggered by query needing `"X"` | `{"X", "_phash_X"}` |
| query needing only `"_phash_X"` (hash-rewritten) | `{"_phash_X"}` (no parquet I/O) |

---

## Edge Cases

### `order: _key` (sort buckets by term value)

Hash ordering ≠ string ordering. The rewriter records which aggregation paths specified `order: _key`. After touch-up resolves all hash keys to strings, `touchup_hash_bucket_keys` re-sorts those bucket arrays lexicographically before returning.

### `min_doc_count: 0` (emit all terms including zero-count)

Requires iterating the entire term dictionary to emit zero-count buckets — impossible with hash fields alone (we don't have a registry of all possible hashes). The rewriter detects `min_doc_count: 0` and **falls back to transcoding** for that field.

### `include`/`exclude` string filters on `TermsAgg`

```json
{"terms": {"field": "status", "include": ["active", "pending"]}}
```

The rewriter hashes each filter value at rewrite time:
```json
{"terms": {"field": "_phash_status", "include": [14891908177209451419, 3891234567890123456]}}
```

`include`/`exclude` filters on a `U64` terms agg operate on the hash values. Result touch-up then resolves the surviving bucket keys to strings as normal.

### `missing` parameter (bucket for documents with no value)

```json
{"terms": {"field": "status", "missing": "unknown"}}
```

In the original aggregation, `missing` creates a bucket for null documents labeled `"unknown"`. After rewriting to `_phash_status`:
- Null documents have no value in `_phash_status` → tantivy's `missing` parameter uses the sentinel value
- The rewriter maps `missing: "unknown"` → `missing: 0` (the hash field's null sentinel)
- Touch-up recognizes the `0` bucket and relabels it `"unknown"` (the original `missing` string)

### Nested `TermsAgg`

```json
{"by_status": {"terms": {"field": "status"},
               "aggs": {"by_category": {"terms": {"field": "category"}}}}}
```

Each level is rewritten and touched up independently:
- Outer: `_phash_status` → scan column, batch-read `status` values, replace outer keys
- Inner (per outer bucket): `_phash_category` → scan column, batch-read `category` values, replace inner keys

The column scans for each level are independent. Both can be parallelized if needed.

### Old splits without hash fields

`manifest.string_hash_fields` is empty (or the field is absent in JSON, defaulting to empty via `#[serde(default)]`). All three rewriting/touchup paths detect an empty map and are no-ops. Existing behavior is fully preserved.

### IP address fields

IP address fields come from `Utf8` parquet columns but are declared via `withIpAddressFields()` and resolve to `tantivy_type == "IpAddr"`. In HYBRID mode they are stored as **native** fast fields (like numerics) — they are never transcoded from parquet. The hash optimization is unnecessary for them and the schema derivation condition guards against this by checking `tantivy_type == "Str"` rather than `parquet_type == Utf8`. An `IpAddr` column never receives a `_phash_` counterpart.

### Hash field name conflict

If a user parquet column is literally named `_phash_status` and `status` is also a column, schema derivation logs a warning and skips adding the hidden hash field for `status`. The optimization is silently disabled for that column; it falls back to normal string transcoding.

---

## Java API

No changes to the public API are required — the hash optimization is fully transparent to Java callers. The `ParquetCompanionConfig` may optionally add an opt-out:

```java
/**
 * Controls whether hidden string hash fields are added in HYBRID mode
 * for hash-based aggregation optimization. Default: true.
 *
 * Disable if: (a) parquet column names conflict with _phash_* naming,
 * (b) you need exact TermsAgg results with min_doc_count=0 on all fields,
 * or (c) for debugging to compare hashed vs. transcoded results.
 */
public ParquetCompanionConfig withStringHashOptimization(boolean enabled) {
    this.stringHashOptimization = enabled;
    return this;
}
```

---

## Files Modified

| File | Change |
|------|--------|
| `Cargo.toml` | Add `xxhash-rust = { version = "0.8", features = ["xxh64"] }` |
| `parquet_companion/schema_derivation.rs` | Add `_phash_*` U64 fast field for each HYBRID string fast field; populate `string_hash_fields` map |
| `parquet_companion/indexing.rs` | Compute and store `hash_string_value()` during document building |
| `parquet_companion/manifest.rs` | Add `string_hash_fields: HashMap<String, String>` to `ParquetCompanionManifest` |
| `parquet_companion/hash_field_rewriter.rs` | **New**: rewrite agg/query JSON; return fields still needing transcoding |
| `parquet_companion/hash_touchup.rs` | **New**: scan Column\<u64\> for representative docs; batch parquet cell reads; replace hash keys; re-sort if needed |
| `parquet_companion/doc_retrieval.rs` | Add `read_string_cells_batch()` to `CachedParquetReader` |
| `parquet_companion/transcode.rs` | Pass rewritten JSON to `columns_to_transcode`; skip fields redirected to hash |
| `parquet_companion/augmented_directory.rs` | `effective_column_names` appends hash counterparts (prewarm fix 1) |
| `parquet_companion/merge_impl.rs` | Union `string_hash_fields` in `combine_parquet_manifests` |
| `split_searcher/async_impl.rs` | Register hash counterparts on lazy transcode (fix 2); short-circuit hash-only `columns_to_add` (fix 3) |
| `ParquetCompanionConfig.java` | Optional `withStringHashOptimization(boolean)` |

---

## Test Plan

### Rust unit tests

| Test | Validates |
|------|-----------|
| `test_hash_field_created_during_indexing` | Schema has `_phash_*` fields; tantivy index contains U64 values for string docs |
| `test_hash_value_stable_across_calls` | Same string → same hash value across multiple calls and restarts |
| `test_null_string_no_hash_field_value` | Null parquet string → no `_phash_*` value for that doc |
| `test_rewriter_value_count` | `value_count` on string field → `value_count` on `_phash_` field; original field absent from `columns_to_transcode` |
| `test_rewriter_cardinality` | `cardinality` redirected similarly |
| `test_rewriter_terms_basic` | `terms` field name replaced; result has hash keys |
| `test_rewriter_terms_min_doc_count_zero` | `min_doc_count: 0` → no rewrite; original field in `columns_to_transcode` |
| `test_rewriter_terms_order_key` | `order: _key` path recorded for re-sort |
| `test_rewriter_terms_include_filter` | Include strings hashed at rewrite time |
| `test_rewriter_nested_terms` | Both levels rewritten; both recorded for touch-up |
| `test_touchup_basic` | Hash keys in result replaced with correct string values |
| `test_touchup_order_key_resort` | Buckets re-sorted lexicographically after touch-up |
| `test_touchup_missing_param` | Zero-hash bucket relabeled with `missing` string |
| `test_touchup_subaggs_preserved` | Sub-aggregation numeric results unchanged by touch-up |
| `test_prewarm_registers_hash_field` | After `preloadParquetFastFields("status")`, `transcoded_fast_columns` contains both `"status"` and `"_phash_status"` |
| `test_hash_only_query_no_transcode` | Query needing only `_phash_status` → no `transcode_and_cache` call; correct result |
| `test_merge_preserves_hash_fields` | Merged split has `string_hash_fields` in manifest; aggregation works post-merge |

### Java integration tests (new `ParquetHashFieldTest.java`)

| Test | Validates |
|------|-----------|
| `testValueCountNoTranscode` | `value_count` on string field returns correct count; parquet column not read |
| `testCardinalityNoTranscode` | `cardinality` on string field returns correct distinct count |
| `testTermsAggFullRoundtrip` | `terms` on string field: bucket keys are real strings, counts correct |
| `testTermsAggWithSubAgg` | `terms` + `avg` sub-agg: both keys and numeric results correct |
| `testTermsAggOrderKey` | Buckets sorted lexicographically when `order: _key` specified |
| `testTermsAggIncludeFilter` | Only specified string values appear in results |
| `testTermsAggMinDocCountZeroFallback` | `min_doc_count: 0` falls back to transcoding; all zero-count terms present |
| `testNestedTermsAgg` | Two-level `terms` agg; both levels have correct string keys |
| `testPrewarmCoversHashField` | After prewarm of `"status"`, query on `_phash_status` triggers no additional I/O |
| `testOldSplitCompatibility` | Split created without hash fields: normal transcoding path used, no errors |
| `testHashFieldNameConflict` | Column named `_phash_status` with parent `status`: warning logged, fallback to transcoding |

---

## Expected Performance Impact

| Query pattern | Before | After |
|--------------|--------|-------|
| `value_count` on 1 string field | Full parquet column scan + SSTable build (~200ms) | Zero parquet reads for that field (~0ms) |
| `cardinality` on 2 string fields | 2× full scans | Zero parquet reads for both |
| `terms` on 1 string field, K=50 distinct values | Full parquet column scan + SSTable build | Hash column scan (~1ms) + K targeted parquet cell reads (~5ms) |
| Mixed: `terms("category")` + `value_count("status")` | 2× full scans | 1× scan (only `category`); `status` uses hash only |
| `terms` with `min_doc_count: 0` | Unchanged (transcoding required) | Unchanged |
| TermsAgg after prewarm | Prewarm: full scan (once); queries: L1/L2 cache hit | Prewarm: full scan (once); queries: L1/L2 cache hit (unchanged — prewarm path unaffected) |
