# Parquet Companion Mode — Complete Design Document

**Version:** 1.1
**Date:** February 2026
**Status:** Design Complete (Gap Resolutions Addressed), Pending Implementation

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Motivation and Goals](#2-motivation-and-goals)
3. [Architecture Overview](#3-architecture-overview)
4. [Manifest Format and Split Bundle Integration](#4-manifest-format-and-split-bundle-integration)
5. [Fast Field Modes (Column<T> Adapter)](#5-fast-field-modes-columnt-adapter)
6. [Parquet I/O Through Existing Cache Stack](#6-parquet-io-through-existing-cache-stack)
7. [Parallel Byte-Range Fetching Strategy](#7-parallel-byte-range-fetching-strategy)
8. [Range Coalescing for Cross-Column Reads](#8-range-coalescing-for-cross-column-reads)
9. [Credential Management for Parquet Storage](#9-credential-management-for-parquet-storage)
10. [Indexing Pipeline (Parquet to Split)](#10-indexing-pipeline-parquet-to-split)
11. [Query Pipeline (Search to Parquet Retrieval)](#11-query-pipeline-search-to-parquet-retrieval)
12. [Schema Auto-Derivation](#12-schema-auto-derivation)
13. [Multi-Parquet File Addressing](#13-multi-parquet-file-addressing)
14. [Prewarm Strategy for Parquet Columns](#14-prewarm-strategy-for-parquet-columns)
15. [Table Root and Regional Replication](#15-table-root-and-regional-replication)
16. [Performance Analysis](#16-performance-analysis)
17. [Java API Design](#17-java-api-design)
18. [Split Merge Handling](#18-split-merge-handling)
19. [Gap Resolutions and Design Addenda](#19-gap-resolutions-and-design-addenda)
20. [Implementation Plan](#20-implementation-plan)
21. [Design Decisions Log](#21-design-decisions-log)

---

## 1. Executive Summary

Parquet Companion Mode enables tantivy4java to create **minimal quickwit splits** that reference
external parquet files for document storage and optionally for fast field (columnar) data. Instead
of duplicating data between parquet and the split's internal store/fast-field files, the split
contains only the inverted index (term dictionaries, postings lists, field norms) while the parquet
files serve as the source of truth for field values.

**Key properties:**
- **Zero changes** to tantivy or quickwit source code
- A split can reference **one or more** parquet files
- Parquet access uses the **same L1/L2/L3 cache stack** as split access
- Parquet pages are accessed with **page-level granularity** and **parallel I/O**
- **Three fast field modes**: disabled, hybrid, parquet-only
- Parquet files may reside in **different storage** than splits (separate credentials)
- **Regional replication** supported via relative paths with configurable table roots

---

## 2. Motivation and Goals

### Problem

When indexing data that already lives in parquet format, creating a quickwit split duplicates
significant amounts of data:

- **Store files** (.store): Duplicate of all stored field values (~40-60% of split size)
- **Fast field files** (.fast): Duplicate of columnar data, especially large for string fields
  (~20-40% of split size for text-heavy schemas)

For a 10GB parquet table, the corresponding split might be 6-8GB, with most of that being
redundant copies of data already in parquet.

### Goals

1. **Minimize split size**: Splits contain only inverted index data (term dicts, postings, field
   norms). Target: 5-15% of parquet data size.
2. **Millisecond document retrieval**: Sub-5ms latency for retrieving document fields from parquet
   after search, leveraging page-level access and the L2 disk cache.
3. **Native aggregation support**: Parquet columns can serve as fast fields for tantivy
   aggregations (terms, histogram, date_histogram, range) without changes to tantivy's internal
   aggregation code.
4. **Minimal infrastructure changes**: Reuse the existing Storage trait, cache stack, prewarm
   infrastructure, and parallel I/O patterns.
5. **Multi-cloud flexibility**: Support parquet files in different storage backends (S3, Azure,
   local) with independent credentials from the split storage.

---

## 3. Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Java Application                                │
│                                                                         │
│  SplitSearcher.search(query)  →  DocIds  →  retrieveFromParquet(docIds) │
│                                                                         │
│  ParquetCompanionConfig:                                                │
│    .withTableRoot("s3://us-east-1/tables/sales/")                       │
│    .withFastFieldMode(HYBRID)                                           │
│    .withParquetCredentials(awsConfig)   ← separate from split creds     │
│                                                                         │
└────────┬───────────────────────────────────────────┬────────────────────┘
         │                                           │
    ┌────▼────┐                              ┌───────▼────────┐
    │  Split  │                              │  Parquet Files │
    │ (small) │                              │  (external)    │
    │         │                              │                │
    │ .term   │                              │ part-00000.pq  │
    │ .pos    │                              │ part-00001.pq  │
    │ .idx    │                              │ part-00002.pq  │
    │ .fieldnm│                              │    ...         │
    │ .fast*  │ ← minimal or empty           │                │
    │ .store* │ ← stub (routing to parquet)  │                │
    │ _parquet│                              │                │
    │ _manifst│                              │                │
    └────┬────┘                              └───────┬────────┘
         │                                           │
         │          ┌───────────────────┐            │
         └──────────►  Storage + Cache  ◄────────────┘
                    │                   │
                    │  L1: ByteRange    │
                    │  L2: DiskCache    │  ← shared cache infrastructure
                    │  L3: S3/Azure/FS  │  ← separate Storage instances per credential
                    └───────────────────┘
```

### Key Abstraction: ParquetAugmentedDirectory

The centerpiece of the integration is a custom `Directory` implementation that wraps the normal
`BundleDirectory`. When tantivy requests `.fast` files, this directory transparently provides data
decoded from parquet pages and transcoded into tantivy's columnar format. Tantivy sees standard
columnar data — it has no knowledge of parquet.

```
                    tantivy requests "{segment}.fast"
                                │
                    ┌───────────▼───────────────┐
                    │  ParquetAugmentedDirectory │
                    │                           │
                    │  Mode 1: delegate to inner│
                    │  Mode 2: merge native +   │
                    │          parquet columns   │
                    │  Mode 3: all from parquet  │
                    └───────────┬───────────────┘
                                │
                    tantivy ColumnarReader::open(bytes)
                                │
                    FastFieldReaders → aggregations, sorting, etc.
```

---

## 4. Manifest Format and Split Bundle Integration

### Storage Location

The parquet manifest is embedded in the split bundle as `_parquet_manifest.json` via
`SplitPayloadBuilder::add_payload()`. Tantivy/quickwit ignore unknown files in the bundle —
`BundleDirectory::open_read()` only serves files that are explicitly requested by name, and
tantivy never requests `_parquet_manifest.json`.

**Zero quickwit/tantivy changes required.**

### Manifest Schema

```json
{
  "version": 1,
  "fast_field_mode": "hybrid",

  "segment_row_ranges": [
    { "segment_ord": 0, "row_offset": 0, "num_rows": 80000 },
    { "segment_ord": 1, "row_offset": 80000, "num_rows": 20000 }
  ],

  "parquet_files": [
    {
      "relative_path": "data/part-00000.parquet",
      "row_offset": 0,
      "num_rows": 50000,
      "file_size_bytes": 12345678,
      "has_offset_index": true,

      "row_groups": [
        {
          "ordinal": 0,
          "num_rows": 10000,
          "row_offset_in_file": 0,
          "columns": {
            "title": {
              "byte_range": [1024, 51200],
              "compression": "SNAPPY",
              "encoding": "RLE_DICTIONARY"
            },
            "score": {
              "byte_range": [51200, 55300],
              "compression": "UNCOMPRESSED",
              "encoding": "PLAIN"
            }
          }
        }
      ]
    },
    {
      "relative_path": "data/part-00001.parquet",
      "row_offset": 50000,
      "num_rows": 50000,
      "file_size_bytes": 11234567,
      "has_offset_index": true
    }
  ],

  "total_rows": 100000,

  "column_mapping": {
    "title": { "parquet_type": "BYTE_ARRAY", "logical_type": "String", "tantivy_type": "Str" },
    "score": { "parquet_type": "DOUBLE", "logical_type": null, "tantivy_type": "F64" },
    "id": { "parquet_type": "INT64", "logical_type": null, "tantivy_type": "I64" },
    "active": { "parquet_type": "BOOLEAN", "logical_type": null, "tantivy_type": "Bool" },
    "created_at": { "parquet_type": "INT64", "logical_type": "Timestamp(MICROS,UTC)", "tantivy_type": "DateTime" },
    "tags": { "parquet_type": "LIST<BYTE_ARRAY>", "logical_type": "List<String>", "tantivy_type": "Json" },
    "metadata": { "parquet_type": "STRUCT", "logical_type": "Struct", "tantivy_type": "Json" }
  },

  "parquet_storage_config": {
    "credential_type": "aws",
    "note": "Actual credentials provided at query time via ParquetCompanionConfig, not stored in manifest"
  }
}
```

### Key Manifest Properties

- **segment_row_ranges**: Maps segment ordinals to parquet row ranges — enables multi-segment
  DocId translation (`global_row = segment_row_ranges[seg_ord].row_offset + local_doc_id`)
- **relative_path**: Paths are relative to the table root (provided at query time)
- **row_offset**: Cumulative row count — enables DocId-to-file mapping via binary search
- **has_offset_index**: Per-file flag — whether parquet file has OffsetIndex for page-level access
- **column_mapping**: Pre-computed type mapping from parquet to tantivy column types, including
  complex types (LIST, MAP, STRUCT) mapped to tantivy `Json` type
- **No page-level data**: Manifest stores column-level byte ranges only. OffsetIndex is fetched
  from the parquet footer on demand and cached in L2 (keeps manifest < 100KB for wide tables)
- **No credentials stored**: Manifest only records that credentials are needed; actual credentials
  are provided at query time via `ParquetCompanionConfig`

### Embedding in Split Bundle

```rust
// At split creation time:
let manifest_json = serde_json::to_vec(&manifest)?;

let mut builder = SplitPayloadBuilder::new();
// ... add normal split files ...
builder.add_payload(
    "_parquet_manifest.json".to_string(),
    Box::new(manifest_json),
);
builder.finalize(hotcache)?;
```

---

## 5. Fast Field Modes (Column<T> Adapter)

### Overview

The Column<T> adapter provides tantivy-compatible fast field data decoded from parquet pages.
It intercepts at the Directory level, presenting parquet-decoded data in tantivy's native
columnar format. Three modes control which fields come from parquet vs native fast fields.

### Mode 1: Disabled

Standard quickwit behavior. All fast fields are stored natively in the split.

- **At split creation**: Write all fast fields normally
- **At query time**: Directory delegates `.fast` reads to inner BundleDirectory
- **Use case**: Maximum search/aggregation performance, standard split sizes

### Mode 2: Hybrid (Recommended Default)

Numeric/date/timestamp/bool fields use native fast fields. String/bytes fields use parquet.

- **At split creation**: Write fast fields only for numeric types (I64, U64, F64, Bool,
  DateTime, IpAddr). Skip Str and Bytes fast fields.
- **At query time**:
  1. Read native `.fast` file from split (compact — numerics only)
  2. Decode string columns from cached parquet pages
  3. Merge both into a unified tantivy columnar file via `ColumnarWriter`
  4. Cache transcoded bytes in L1
- **Use case**: Best balance — compact splits with full aggregation support. String fast
  fields are the largest storage consumers, and this eliminates them.

**Why hybrid is the sweet spot:**

| Field Type | Native Fast Field Size (1M rows) | Parquet Column Size (1M rows) |
|-----------|----------------------------------|-------------------------------|
| i64       | ~8 MB (bitpacked)                | ~4-8 MB (DELTA encoded)       |
| f64       | ~8 MB (raw)                      | ~8 MB (PLAIN)                 |
| bool      | ~125 KB (bitpacked)              | ~125 KB (RLE)                 |
| String (100-char avg) | ~100+ MB (dict + ordinals) | ~20-40 MB (dict + RLE) |

Numerics are tiny in either format. Strings are the dominant cost, and parquet stores them
more efficiently due to better compression and dictionary encoding.

### Mode 3: Parquet Only

All fast fields come from parquet. Maximum split size reduction.

- **At split creation**: Write no fast fields (minimal/empty `.fast` file)
- **At query time**:
  1. Decode ALL needed columns from cached parquet pages
  2. Build complete tantivy columnar file via `ColumnarWriter`
  3. Cache transcoded bytes in L1
- **Use case**: Minimal split footprint, acceptable first-query latency (~35ms for 1M rows)

### Transcode Pipeline

The transcode from parquet to tantivy columnar format uses tantivy's public
`ColumnarWriter` API from the `tantivy_columnar` crate:

```rust
fn transcode_parquet_to_columnar(
    parquet_columns: &HashMap<String, DecodedColumn>,
    native_fast_bytes: Option<OwnedBytes>,  // For hybrid mode
    num_docs: u32,
    mode: FastFieldMode,
) -> io::Result<Vec<u8>> {
    let mut writer = ColumnarWriter::default();

    // In hybrid mode, re-record native numeric columns
    if let (FastFieldMode::Hybrid { .. }, Some(native_bytes)) = (&mode, &native_fast_bytes) {
        let reader = ColumnarReader::open(native_bytes.clone())?;
        for (name, handle) in reader.list_columns()? {
            match handle.column_type() {
                // Numeric types: re-record from native data
                ColumnType::I64 | ColumnType::U64 | ColumnType::F64 |
                ColumnType::Bool | ColumnType::DateTime | ColumnType::IpAddr => {
                    let col = handle.open()?;
                    record_dynamic_column(&mut writer, &name, &col, num_docs);
                }
                // String/Bytes: skip — will come from parquet
                ColumnType::Str | ColumnType::Bytes => {}
            }
        }
    }

    // Record parquet-decoded columns
    for (field_name, decoded) in parquet_columns {
        match decoded {
            DecodedColumn::I64(values) => {
                for (row_id, val) in values.iter().enumerate() {
                    writer.record_numerical(row_id as u32, field_name, *val);
                }
            }
            DecodedColumn::F64(values) => {
                for (row_id, val) in values.iter().enumerate() {
                    writer.record_numerical(row_id as u32, field_name, *val);
                }
            }
            DecodedColumn::Str(values) => {
                for (row_id, val) in values.iter().enumerate() {
                    writer.record_str(row_id as u32, field_name, val);
                }
            }
            DecodedColumn::Bool(values) => {
                for (row_id, val) in values.iter().enumerate() {
                    writer.record_bool(row_id as u32, field_name, *val);
                }
            }
            DecodedColumn::DateTime(values) => {
                for (row_id, val) in values.iter().enumerate() {
                    writer.record_datetime(row_id as u32, field_name, *val);
                }
            }
            // ... other types
        }
    }

    let mut buf = Vec::new();
    writer.serialize(num_docs, &mut buf)?;
    Ok(buf)
}
```

### Field Classification Logic

```rust
fn field_source(column_type: &ColumnType, mode: &FastFieldMode) -> FieldSource {
    match mode {
        FastFieldMode::Disabled => FieldSource::Native,
        FastFieldMode::ParquetOnly => FieldSource::Parquet,
        FastFieldMode::Hybrid { .. } => {
            match column_type {
                // Compact types: keep native (tiny overhead, O(1) access)
                ColumnType::I64 | ColumnType::U64 | ColumnType::F64 |
                ColumnType::Bool | ColumnType::DateTime | ColumnType::IpAddr
                    => FieldSource::Native,

                // Large types: serve from parquet (major size savings)
                ColumnType::Str | ColumnType::Bytes
                    => FieldSource::Parquet,
            }
        }
    }
}
```

### Directory Implementation

```rust
pub struct ParquetAugmentedDirectory {
    inner: Arc<dyn Directory>,
    parquet_context: Arc<ParquetContext>,
    mode: FastFieldMode,
    /// Cache of transcoded .fast bytes per segment, keyed by segment filename
    transcoded_cache: Mutex<HashMap<PathBuf, OwnedBytes>>,
}

impl Directory for ParquetAugmentedDirectory {
    fn open_read(&self, path: &Path) -> Result<FileSlice, OpenReadError> {
        if self.mode == FastFieldMode::Disabled {
            return self.inner.open_read(path);
        }

        // Only intercept .fast files
        if path.extension() != Some(OsStr::new("fast")) {
            return self.inner.open_read(path);
        }

        // Check transcoded cache
        if let Some(cached) = self.transcoded_cache.lock().unwrap().get(path) {
            return Ok(FileSlice::from(cached.clone()));
        }

        // Build transcoded fast field data
        let bytes = self.build_augmented_fast_fields(path)?;
        self.transcoded_cache.lock().unwrap()
            .insert(path.to_path_buf(), bytes.clone());
        Ok(FileSlice::from(bytes))
    }

    // All other Directory methods delegate to inner
    fn exists(&self, path: &Path) -> Result<bool, OpenReadError> {
        if path.extension() == Some(OsStr::new("fast")) &&
           self.mode != FastFieldMode::Disabled {
            return Ok(true); // We can always provide .fast data
        }
        self.inner.exists(path)
    }

    // ... atomic_read, atomic_write, etc. delegate to inner
}
```

### Fast Field Column Projection

The fast field transcode also respects column projection. Parquet tables may have dozens of
string columns, but a query's aggregation may only need 1-2 of them. We transcode only the
fields actually needed.

**Demand-driven transcode strategy:**

```rust
impl ParquetAugmentedDirectory {
    fn build_augmented_fast_fields(
        &self,
        path: &Path,
        needed_fields: &HashSet<String>,  // From aggregation WarmupInfo
    ) -> io::Result<OwnedBytes> {
        let mut writer = ColumnarWriter::default();

        // In hybrid mode: re-record native numeric columns from split
        if let FastFieldMode::Hybrid { .. } = &self.mode {
            let native_bytes = self.inner.open_read(path)?.read_bytes()?;
            let reader = ColumnarReader::open(native_bytes)?;
            for (name, handle) in reader.list_columns()? {
                match handle.column_type() {
                    ColumnType::I64 | ColumnType::U64 | ColumnType::F64 |
                    ColumnType::Bool | ColumnType::DateTime | ColumnType::IpAddr => {
                        // Always include native numeric columns
                        let col = handle.open()?;
                        record_dynamic_column(&mut writer, &name, &col, num_docs);
                    }
                    _ => {} // String/Bytes come from parquet — only if needed
                }
            }
        }

        // ONLY decode parquet columns that are actually needed
        for field_name in needed_fields {
            if self.should_serve_from_parquet(field_name) {
                let decoded = self.decode_parquet_column(field_name).await?;
                record_decoded_column(&mut writer, field_name, &decoded);
            }
        }

        let mut buf = Vec::new();
        writer.serialize(self.num_docs, &mut buf)?;
        Ok(OwnedBytes::new(buf))
    }
}
```

**Aggregation field auto-discovery:**

Since parquet column names are 1:1 with tantivy/quickwit field names, the system
automatically discovers which parquet columns to use for aggregations by matching names.
No upfront `declareParquetFastFields()` call is needed.

```java
// Aggregation fields are auto-discovered from parquet column names
// No explicit declaration required — the system matches by name
Map<String, Object> aggs = Map.of(
    "top_categories", Map.of("terms", Map.of("field", "category")));
AggregationResult result = searcher.searchWithAggregations(query, 0, aggs);

// Optional: eager prewarm for lower first-query latency
searcher.preloadParquetFastFields("category", "brand").join();
```

When tantivy requests a `.fast` file, the `ParquetAugmentedDirectory`:
1. Determines which fields are needed (from aggregation WarmupInfo or all fast fields)
2. Looks up each field name in the manifest's `column_mapping`
3. If found and type-compatible, transcodes from parquet
4. If not found in parquet, falls back to native fast field data (hybrid mode)

### Design Questions

1. **Lazy vs eager transcode**: Recommendation: **lazy with optional eager prewarm** via
   `preloadParquetFastFields()`. Lazy transcode happens on first `.fast` file access;
   eager prewarm triggers it proactively.

2. **Multi-segment splits**: Each segment has its own `.fast` file. The manifest tracks
   which rows belong to which segment via `segment_row_ranges` — a mapping from
   `segment_ord` to `(row_offset, num_rows)` in the global parquet row space.

---

## 6. Parquet I/O Through Existing Cache Stack

### Storage Integration

Parquet files are accessed through the same `Storage` trait used for split files. A separate
`Storage` instance is created for parquet access, potentially with different credentials:

```
Split Storage:    Arc<dyn Storage>  ← configured with split credentials
Parquet Storage:  Arc<dyn Storage>  ← configured with parquet credentials
                         │
Both wrap in:    StorageWithPersistentCache
                         │
                    Shared L2DiskCache instance
```

### AsyncFileReader Implementation

Arrow-rs's `AsyncFileReader` trait is implemented by delegating to our `Storage` trait:

```rust
pub struct CachedParquetReader {
    storage: Arc<StorageWithPersistentCache>,
    parquet_path: PathBuf,
    file_size: u64,
    metadata: Option<Arc<ParquetMetaData>>,
}

impl AsyncFileReader for CachedParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, Result<Bytes>> {
        let storage = self.storage.clone();
        let path = self.parquet_path.clone();
        async move {
            let bytes = storage.get_slice(
                &path,
                range.start as usize..range.end as usize,
            ).await?;
            Ok(Bytes::from(bytes.to_vec()))
        }.boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, Result<Vec<Bytes>>> {
        let storage = self.storage.clone();
        let path = self.parquet_path.clone();
        async move {
            // Parallel fetch — same pattern as quickwit prewarm
            let futures: Vec<_> = ranges.iter().map(|range| {
                let storage = storage.clone();
                let path = path.clone();
                async move {
                    let bytes = storage.get_slice(
                        &path,
                        range.start as usize..range.end as usize,
                    ).await?;
                    Ok(Bytes::from(bytes.to_vec()))
                }
            }).collect();
            futures::future::try_join_all(futures).await
        }.boxed()
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, Result<Arc<ParquetMetaData>>> {
        // Return pre-loaded metadata from manifest
        let metadata = self.metadata.clone();
        async move {
            metadata.ok_or_else(|| {
                ParquetError::General("No metadata available".to_string())
            })
        }.boxed()
    }
}
```

### Cache Key Strategy

Parquet file data uses the same cache key structure as split data, but with parquet-specific
identifiers:

```
storage_loc:  "s3://parquet-bucket/tables/sales"    ← from resolved table root
split_id:     "part-00000.parquet"                   ← parquet filename
component:    "part-00000.parquet"                   ← same (entire file is one component)
byte_range:   Some(1024..5120)                       ← specific page range
```

This allows the L2DiskCache's range coalescing to work naturally — if we fetch pages [1024..5120]
and later request [3000..8000], the coalescing finds the overlap and only fetches the gap
[5120..8000].

---

## 7. Parallel Byte-Range Fetching Strategy

### Pattern: futures::future::try_join_all

Matching quickwit's proven parallel I/O pattern, all byte-range fetches are issued as
independent async futures joined with `try_join_all`:

```rust
async fn fetch_column_pages(
    storage: &Arc<StorageWithPersistentCache>,
    columns: &[(String, Vec<PageLocation>)],  // (column_name, page_locations from footer)
    parquet_path: &Path,
) -> Result<HashMap<String, Vec<Bytes>>> {
    let mut all_futures = Vec::new();

    for (col_name, page_locations) in columns {
        // page_locations come from the parquet footer's OffsetIndex (cached — see §19.8),
        // NOT from the manifest (which stores only column-level byte ranges).
        for page in page_locations {
            let storage = storage.clone();
            let path = parquet_path.to_path_buf();
            let col = col_name.clone();
            let range = page.offset as u64..page.offset as u64 + page.compressed_page_size as u64;

            all_futures.push(async move {
                let bytes = storage.get_slice(
                    &path,
                    range.start as usize..range.end as usize,
                ).await?;
                Ok((col, bytes))
            });
        }
    }

    // All pages across all columns fetched in parallel
    let results = futures::future::try_join_all(all_futures).await?;

    // Group by column name
    let mut by_column: HashMap<String, Vec<Bytes>> = HashMap::new();
    for (col_name, bytes) in results {
        by_column.entry(col_name).or_default().push(bytes);
    }
    Ok(by_column)
}
```

Each future independently goes through `StorageWithPersistentCache::get_slice()` which:
1. Checks L2 disk cache with range coalescing
2. Fetches only gaps from L3 (S3/Azure)
3. Caches fetched data in L2

Tokio's async scheduler handles the concurrency automatically.

---

## 8. Range Coalescing for Cross-Column Reads

### Two-Level Coalescing

**Level 1 — Per-file, automatic (existing infrastructure):**

Each `get_slice()` call goes through `StorageWithPersistentCache` which uses
`L2DiskCache::get_coalesced()`. If we previously cached [0..5000] and now request
[3000..8000], only [5000..8000] is fetched from L3.

**Level 2 — Cross-column gap-fill (new optimization):**

When fetching multiple columns from the same parquet file, their page ranges may be close
in the file (parquet stores columns sequentially within a row group). We merge close-by
ranges before issuing I/O:

```rust
fn coalesce_page_ranges(
    ranges: &mut Vec<Range<u64>>,
    max_gap: u64,  // e.g., 64KB
) {
    if ranges.is_empty() { return; }
    ranges.sort_by_key(|r| r.start);

    let mut merged = vec![ranges[0].clone()];
    for range in &ranges[1..] {
        let last = merged.last_mut().unwrap();
        if range.start <= last.end + max_gap {
            // Gap is small enough — merge
            last.end = last.end.max(range.end);
        } else {
            merged.push(range.clone());
        }
    }
    *ranges = merged;
}
```

**Example:**
```
Column A pages: [1024..5120]     (4 KB)
Column B pages: [5200..9300]     (4 KB)   ← 80 byte gap
Column C pages: [50000..54000]   (4 KB)   ← 41 KB gap

With max_gap = 64KB:
  Merged: [1024..9300], [50000..54000]
  → 2 S3 requests instead of 3
  → Extra 80 bytes fetched (insignificant)
  → All fetched data cached in L2 for future use
```

The L2 cache benefits from this because the cached super-range serves future sub-range
requests via `get_subrange()` without additional S3 calls.

---

## 9. Credential Management for Parquet Storage

### Design Principle

Parquet files often reside in different storage backends than splits. A split might be in
`s3://index-bucket/` with one set of AWS credentials, while the parquet data is in
`s3://data-lake-bucket/` with different credentials, or even in Azure Blob Storage.

The manifest records **what** credential type is needed (aws, azure, local), but **never**
stores actual credentials. Credentials are provided at query time via `ParquetCompanionConfig`.

### Supported Credential Types

All existing credential types supported by tantivy4java are available for parquet access:

**AWS S3:**
```java
ParquetCompanionConfig.ParquetStorageConfig parquetStorage =
    new ParquetCompanionConfig.ParquetStorageConfig()
        .withAwsCredentials("access-key", "secret-key")
        .withAwsRegion("us-east-1");

// With session token (STS, IAM roles, federated access)
ParquetCompanionConfig.ParquetStorageConfig parquetStorage =
    new ParquetCompanionConfig.ParquetStorageConfig()
        .withAwsCredentials("access-key", "secret-key", "session-token")
        .withAwsRegion("us-west-2");

// Custom S3-compatible endpoint (MinIO, etc.)
ParquetCompanionConfig.ParquetStorageConfig parquetStorage =
    new ParquetCompanionConfig.ParquetStorageConfig()
        .withAwsCredentials("access-key", "secret-key")
        .withAwsRegion("us-east-1")
        .withCustomEndpoint("https://minio.internal:9000")
        .withPathStyle(true);
```

**Azure Blob Storage:**
```java
// Account key authentication
ParquetCompanionConfig.ParquetStorageConfig parquetStorage =
    new ParquetCompanionConfig.ParquetStorageConfig()
        .withAzureCredentials("account-name", "account-key");

// OAuth bearer token (Azure AD / Service Principal)
ParquetCompanionConfig.ParquetStorageConfig parquetStorage =
    new ParquetCompanionConfig.ParquetStorageConfig()
        .withAzureBearerToken("account-name", "bearer-token");
```

**Local filesystem:**
```java
// No credentials needed for local parquet files
ParquetCompanionConfig.ParquetStorageConfig parquetStorage =
    new ParquetCompanionConfig.ParquetStorageConfig()
        .withLocalFileSystem();
```

### Storage Instance Architecture

```
SplitCacheManager
  ├── split_storage: Arc<dyn Storage>           ← split credentials
  │     └── StorageWithPersistentCache          ← L2 disk cache
  │           └── S3Storage / AzureStorage      ← L3 remote
  │
  └── parquet_storage: Arc<dyn Storage>         ← parquet credentials (may differ!)
        └── StorageWithPersistentCache          ← SAME L2 disk cache instance
              └── S3Storage / AzureStorage      ← L3 remote (potentially different bucket/account)
```

Both storage instances share the **same L2DiskCache** for cache efficiency, but use different
underlying remote storage backends with independent credentials.

### Rust-Side Storage Factory

```rust
fn create_parquet_storage(
    config: &ParquetStorageConfig,
    disk_cache: Arc<L2DiskCache>,
) -> Arc<dyn Storage> {
    let remote_storage: Arc<dyn Storage> = match config {
        ParquetStorageConfig::Aws { access_key, secret_key, session_token, region, endpoint, path_style } => {
            // Build quickwit S3Storage with parquet-specific credentials
            create_s3_storage(access_key, secret_key, session_token, region, endpoint, path_style)
        }
        ParquetStorageConfig::Azure { account_name, credential } => {
            create_azure_storage(account_name, credential)
        }
        ParquetStorageConfig::Local => {
            create_local_storage()
        }
    };

    // Wrap with shared L2 disk cache
    Arc::new(StorageWithPersistentCache::with_disk_cache_only(
        remote_storage,
        disk_cache,
        "parquet".to_string(),   // storage_loc prefix
        "default".to_string(),   // split_id (overridden per file)
    ))
}
```

### Cross-Account / Cross-Cloud Examples

```java
// Split in AWS, parquet data in Azure
SplitCacheManager.CacheConfig splitConfig = new SplitCacheManager.CacheConfig("search-cache")
    .withMaxCacheSize(500_000_000)
    .withAwsCredentials("split-access-key", "split-secret-key")
    .withAwsRegion("us-east-1");

ParquetCompanionConfig parquetConfig = new ParquetCompanionConfig()
    .withTableRoot("azure://data-container/tables/sales/")
    .withFastFieldMode(FastFieldMode.HYBRID)
    .withParquetStorage(
        new ParquetStorageConfig()
            .withAzureCredentials("azure-account", "azure-key")
    );

try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(splitConfig)) {
    try (SplitSearcher searcher = cacheManager.createSplitSearcher(
            "s3://index-bucket/sales.split",
            parquetConfig)) {
        // Split read from S3 with AWS creds
        // Parquet data read from Azure with Azure creds
        // Both share the same L2 disk cache
        SearchResult result = searcher.search(query, 10);
    }
}
```

---

## 10. Indexing Pipeline (Parquet to Split)

### Multi-File Indexing — The Primary Path

A single split can index **multiple parquet files** in one operation. This is the
**recommended** approach because it avoids split merges entirely:

```java
// Index multiple parquet files into a single split — NO merge needed
List<String> parquetFiles = Arrays.asList(
    "s3://data-lake/tables/sales/part-00000.parquet",
    "s3://data-lake/tables/sales/part-00001.parquet",
    "s3://data-lake/tables/sales/part-00002.parquet",
    "s3://data-lake/tables/sales/part-00003.parquet"
);

QuickwitSplit.SplitMetadata result = QuickwitSplit.createFromParquet(
    parquetFiles,           // All files indexed into ONE split
    "/tmp/sales.split",
    splitConfig,
    parquetConfig
);
```

**Why this is better than merge:**
- **No merge overhead**: No download → extract → merge → re-bundle pipeline
- **No deletion constraints**: Merge requires zero deletions; single-split indexing has no such restriction
- **Simpler manifest**: All parquet files tracked from the start with correct row offsets
- **Identical query performance**: DocId → parquet row mapping works the same regardless
  of how many parquet files are in the manifest

**When to use merge instead:**
- When new parquet files arrive incrementally and each was already indexed into its own split
- When combining splits that were created independently by different workers

### Pipeline Overview

```
Parquet Files (1..N)
      │
      ▼
[1. Read parquet schema from first file, validate consistency across all files]
      │
      ▼
[2. Auto-derive tantivy schema]
      │  - All fields indexed (for search)
      │  - No fields stored (parquet is the store — stub .store generated)
      │  - Fast fields per mode config
      │  - Complex types (LIST/MAP/STRUCT) mapped to JSON fields
      │
      ▼
[3. Iterate ALL parquet files' rows → tantivy Documents]
      │  - Maintain row order = parquet row order across ALL files
      │  - Track file boundaries and cumulative row offsets for manifest
      │  - Skip null values (tantivy handles absent fields natively)
      │
      ▼
[4. IndexWriter.addDocument() → commit]
      │  - Track segment boundaries for segment_row_ranges
      │
      ▼
[5. Convert index to split]
      │  - QuickwitSplit.convertIndexFromPath()
      │  - Generate stub .store file
      │
      ▼
[6. Embed _parquet_manifest.json]
      │  - SplitPayloadBuilder.add_payload()
      │  - Column-level metadata (byte ranges, types, compression)
      │  - has_offset_index flag per file
      │  - segment_row_ranges for multi-segment mapping
      │  - Include relative parquet paths
      │
      ▼
[7. Upload split to storage]
```

### Step-by-Step Detail

**Step 1: Schema Derivation and Cross-File Validation**

Schema is auto-derived from the first parquet file (see [Section 12](#12-schema-auto-derivation)).
When multiple parquet files are provided, the schema of each subsequent file is validated
against the first for compatibility:

```rust
fn validate_schema_consistency(
    primary_schema: &ArrowSchema,
    file_schemas: &[(String, ArrowSchema)],
) -> Result<()> {
    for (file_path, schema) in file_schemas {
        // Field count must match
        if schema.fields().len() != primary_schema.fields().len() {
            return Err(anyhow!(
                "Schema mismatch: '{}' has {} fields but primary has {}",
                file_path, schema.fields().len(), primary_schema.fields().len()
            ));
        }
        // Each field name and type must match
        for (primary_field, file_field) in primary_schema.fields().iter().zip(schema.fields()) {
            if primary_field.name() != file_field.name() ||
               primary_field.data_type() != file_field.data_type() {
                return Err(anyhow!(
                    "Schema mismatch in '{}': field '{}' ({:?}) vs '{}' ({:?})",
                    file_path,
                    primary_field.name(), primary_field.data_type(),
                    file_field.name(), file_field.data_type(),
                ));
            }
        }
    }
    Ok(())
}
```

**Step 2: tantivy Schema Derivation** (see [Section 12](#12-schema-auto-derivation))

**Step 3: Row Iteration**
```rust
for (file_idx, parquet_file) in parquet_files.iter().enumerate() {
    let reader = ParquetRecordBatchReader::try_new(file, batch_size)?;
    let mut row_offset_in_file = 0;

    for batch in reader {
        let batch = batch?;
        for row_idx in 0..batch.num_rows() {
            let mut doc = Document::new();

            for (col_idx, field) in tantivy_fields.iter().enumerate() {
                let column = batch.column(col_idx);
                add_arrow_value_to_doc(&mut doc, field, column, row_idx)?;
            }

            writer.add_document(doc)?;
            row_offset_in_file += 1;
        }
    }

    manifest.parquet_files.push(ParquetFileEntry {
        relative_path: parquet_file.relative_path.clone(),
        row_offset: cumulative_row_offset,
        num_rows: row_offset_in_file,
        file_size_bytes: parquet_file.file_size,
        row_groups: extract_row_group_metadata(parquet_file)?,
    });

    cumulative_row_offset += row_offset_in_file;
}

writer.commit()?;
```

**Step 4-5: Split Conversion**
The index is converted to a quickwit split using the existing `convertIndexFromPath()` API.
The split configuration controls which fast fields are written based on the mode.

**Step 6: Manifest Embedding**
```rust
// Build manifest with column-level metadata (no page-level OffsetIndex — fetched on demand)
for file_entry in &mut manifest.parquet_files {
    let metadata = read_parquet_metadata(&file_entry.resolved_path)?;

    // Check if this file has OffsetIndex (optional in parquet spec)
    file_entry.has_offset_index = metadata.offset_index().is_some();

    for (rg_idx, rg) in metadata.row_groups().iter().enumerate() {
        for (col_idx, col) in rg.columns().iter().enumerate() {
            let col_name = metadata.file_metadata().schema_descr()
                .column(col_idx).name();

            // Store column-level metadata only (byte range, compression, encoding)
            file_entry.row_groups[rg_idx].columns.insert(
                col_name.to_string(),
                ColumnChunkInfo {
                    byte_range: [
                        col.data_page_offset() as u64,
                        (col.data_page_offset() + col.compressed_size()) as u64,
                    ],
                    compression: format!("{:?}", col.compression()),
                    encoding: format!("{:?}", col.encodings()),
                },
            );
        }
    }
}

// Record segment_row_ranges from the index metadata
let index_meta = index.load_metas()?;
for (seg_ord, seg_meta) in index_meta.segments.iter().enumerate() {
    manifest.segment_row_ranges.push(SegmentRowRange {
        segment_ord: seg_ord as u32,
        row_offset: cumulative_segment_offset,
        num_rows: seg_meta.max_doc() as u64,
    });
    cumulative_segment_offset += seg_meta.max_doc() as u64;
}
```

---

## 11. Query Pipeline (Search to Parquet Retrieval)

### Flow

```
[1. Search] → tantivy/quickwit returns Vec<(Score, DocId)>
      │
[2. Determine projected columns]
      │  CRITICAL: Only fetch columns the caller explicitly requested
      │  Never default to all columns — parquet may have 100+ columns
      │
[3. Map DocIds to parquet locations]
      │  Binary search on manifest.parquet_files[].row_offset
      │  → { file_idx, row_in_file } per DocId
      │
[4. Group by file, build RowSelection per file]
      │
[5. Compute byte ranges using OffsetIndex — ONLY for projected columns]
      │  RowSelection.scan_ranges(page_locations) per projected column only
      │  Skip all non-requested columns entirely (zero I/O for them)
      │
[6. Cross-column range coalescing (across projected columns only)]
      │  Merge close-by ranges (max_gap threshold)
      │
[7. Parallel fetch through cache stack]
      │  futures::future::try_join_all(storage.get_slice(...))
      │  Each goes through L2 coalescing → L3 gap fetch
      │
[8. Decode pages, extract selected rows — only projected columns]
      │  Arrow-rs page decoders handle decompression
      │  Return field values for requested DocIds, requested columns only
```

### Column Projection — Critical Design Principle

**We MUST only retrieve columns the user explicitly asked for.** Parquet tables may contain
dozens or hundreds of columns, but a typical query only needs 2-5 fields. Fetching all columns
would waste bandwidth, cache space, and decode time.

Column projection is enforced at every layer:

```
Layer                    Projection enforcement
─────────────────────────────────────────────────────────
Java API (searcher.doc)  User specifies field list
  → JNI boundary         Field list passed as String[]
    → Rust doc retrieval  Only requested columns decoded
      → Page fetching     Only pages for projected columns fetched
        → Cache           Only projected column pages cached
          → S3/Azure      Only projected column byte ranges requested
```

**Three projection patterns:**

1. **Explicit field list** (recommended):
   ```java
   // Only fetch title and price — NOT all 50 columns
   Document doc = searcher.doc(hit.getDocAddress(), "title", "price");
   ```

2. **Search result fields** (from query specification):
   ```java
   // Query specifies which fields to return
   SearchResult result = searcher.search(query, 10,
       SearchOptions.withFields("title", "price", "category"));

   // Documents in result already have only the requested fields
   for (SearchHit hit : result.getHits()) {
       Document doc = hit.getDocument();  // Only title, price, category populated
   }
   ```

3. **Schema-aware default** (fallback, NOT all-fields):
   ```java
   // If no fields specified, use a configurable default set
   // NEVER defaults to all columns
   ParquetCompanionConfig config = new ParquetCompanionConfig()
       .withDefaultRetrievalFields("title", "id", "timestamp");
   ```

**There is NO "retrieve all fields" default.** If no field list is provided and no default
is configured, the system returns an error guiding the user to specify fields.

### Document Retrieval API

```java
// After search
SearchResult result = searcher.search(query, 10);

// CORRECT: Retrieve only the fields you need
for (SearchHit hit : result.getHits()) {
    Document doc = searcher.doc(hit.getDocAddress(), "title", "price");
    String title = doc.getFirst("title").toString();
    double price = doc.getFirst("price").asDouble();
}

// ALSO CORRECT: Batch retrieval with field projection
List<Document> docs = searcher.docs(
    result.getHits().stream().map(h -> h.getDocAddress()).collect(toList()),
    "title", "price", "category"  // Only these 3 columns fetched from parquet
);

// ERROR: No fields specified and no default configured
// searcher.doc(hit.getDocAddress());  // → throws IllegalArgumentException
//   "Parquet companion mode requires explicit field list.
//    Use searcher.doc(address, field1, field2, ...) or
//    configure withDefaultRetrievalFields() in ParquetCompanionConfig"
```

### Batch Retrieval with Projection

When retrieving multiple documents, batch the requests AND project only needed columns:

```rust
fn batch_retrieve_from_parquet(
    doc_addresses: &[(u32, u32)],   // (segment_ord, local_doc_id) pairs
    projected_columns: &[String],   // ONLY these columns are fetched
    manifest: &ParquetManifest,
    storage: &Arc<StorageWithPersistentCache>,
) -> Result<Vec<HashMap<String, FieldValue>>> {
    // Validate projected columns exist in manifest
    for col in projected_columns {
        if !manifest.column_mapping.contains_key(col) {
            return Err(anyhow!("Column '{}' not found in parquet manifest", col));
        }
    }

    // Translate (segment_ord, local_doc_id) → global parquet rows,
    // then group by parquet file (see §13 and §19.4)
    let global_rows: Vec<u64> = doc_addresses.iter()
        .map(|&(seg_ord, doc_id)| translate_to_global_row(seg_ord, doc_id, manifest))
        .collect();
    let groups = group_rows_by_file(&global_rows, manifest);

    // For each file, build RowSelection and fetch ONLY projected columns in parallel
    let mut all_futures = Vec::new();
    for (file_idx, rows_in_file) in &groups {
        let file_entry = &manifest.parquet_files[*file_idx];
        let row_selection = build_row_selection(rows_in_file, file_entry.num_rows as usize);

        // CRITICAL: iterate only over projected_columns, not all columns
        for column_name in projected_columns {
            // Fetch OffsetIndex from parquet footer (cached after first access — see §19.8)
            let page_locations = self.get_or_fetch_offset_index(file_entry, column_name).await?;
            let page_ranges = row_selection.scan_ranges(&page_locations);

            // Issue parallel fetches for only this projected column's pages
            for range in page_ranges {
                all_futures.push(fetch_and_decode_page(
                    storage, file_entry, column_name, range
                ));
            }
        }
    }

    let results = futures::future::try_join_all(all_futures).await?;
    assemble_documents(results, &global_rows, projected_columns)
}
```

### Surgical Page-Level Retrieval

**This is a critical performance requirement.** A parquet column may have hundreds or thousands
of pages, but a document retrieval for 10 DocIds may only need 2-3 pages. We MUST fetch only
the pages that contain our target rows, not the entire column.

#### How It Works: OffsetIndex + RowSelection

Each parquet column chunk has an `OffsetIndex` — an array of `PageLocation` entries:

```
PageLocation {
    offset: i64,             // Byte position in file
    compressed_page_size: i32,  // Bytes to read (including page header)
    first_row_index: i64,    // First row number in this page
}
```

Example for a column with 100,000 rows across 100 pages:
```
Page 0:   offset=1024,   size=4096,  first_row=0
Page 1:   offset=5120,   size=4096,  first_row=1000
Page 2:   offset=9216,   size=4096,  first_row=2000
...
Page 50:  offset=205824,  size=4096,  first_row=50000
Page 51:  offset=209920,  size=4096,  first_row=51000
...
Page 99:  offset=409600,  size=4096,  first_row=99000
```

If we need DocIds [50200, 50500, 50800], ALL of those fall within page 50 (rows 50000-50999).
We fetch **1 page** (4KB) instead of **100 pages** (400KB). That's a **99% I/O reduction**.

#### RowSelection Construction from DocIds

```rust
/// Convert a sorted list of row-in-file indices into an arrow-rs RowSelection
///
/// The RowSelection represents which rows to keep (select) vs skip.
/// arrow-rs's scan_ranges() then maps this to byte ranges using the OffsetIndex.
fn build_row_selection(
    row_indices: &[u32],         // Sorted row indices within the file
    total_rows_in_file: usize,
) -> RowSelection {
    // Convert sparse row indices to consecutive ranges
    // e.g., [50200, 50500, 50800] → [50200..50201, 50500..50501, 50800..50801]
    let ranges = row_indices.iter().map(|&r| r as usize..(r as usize + 1));

    RowSelection::from_consecutive_ranges(ranges, total_rows_in_file)
    // Result: Skip(50200), Select(1), Skip(299), Select(1), Skip(299), Select(1), Skip(49199)
}
```

#### Byte Range Computation via scan_ranges()

```rust
/// Given a RowSelection and an OffsetIndex, compute the exact byte ranges to fetch.
///
/// This is the SURGICAL step — it maps selected rows to only the pages containing them.
fn compute_page_byte_ranges(
    selection: &RowSelection,
    page_locations: &[PageLocation],  // From parquet footer (cached after first access — see §19.8)
) -> Vec<Range<u64>> {
    // arrow-rs does the heavy lifting:
    // For each page, check if any selected rows fall within it.
    // Only return byte ranges for pages that contain selected rows.
    selection.scan_ranges(page_locations)
    // Result for DocIds [50200, 50500, 50800]:
    //   → [ 205824..209920 ]   (only page 50!)
    //
    // If DocIds were [200, 50500, 99500]:
    //   → [ 1024..5120, 205824..209920, 409600..413696 ]  (pages 0, 50, 99)
}
```

#### Complete Surgical Retrieval Pipeline

```rust
async fn retrieve_documents_surgically(
    doc_addresses: &[(u32, u32)],   // (segment_ord, local_doc_id) pairs from search results
    projected_columns: &[String],
    manifest: &ParquetManifest,
    storage: &Arc<StorageWithPersistentCache>,
) -> Result<Vec<HashMap<String, FieldValue>>> {
    let mut metrics = RetrievalMetrics::default();

    // Step 1: Translate (segment_ord, local_doc_id) → global parquet rows,
    // then group by parquet file (see §13 and §19.4)
    let global_rows: Vec<u64> = doc_addresses.iter()
        .map(|&(seg_ord, doc_id)| translate_to_global_row(seg_ord, doc_id, manifest))
        .collect();
    let file_groups = group_rows_by_file(&global_rows, manifest);

    let mut all_fetch_futures = Vec::new();

    for (file_idx, rows_in_file) in &file_groups {
        let file_entry = &manifest.parquet_files[*file_idx];

        // Step 2: Build RowSelection for this file
        let selection = build_row_selection(rows_in_file, file_entry.num_rows as usize);

        // Step 3: For each projected column, compute surgical byte ranges
        for col_name in projected_columns {
            // Fetch OffsetIndex from parquet footer (cached after first access — see §19.8)
            // The manifest does NOT contain page-level data; only column-level byte ranges.
            let page_locations = self.get_or_fetch_offset_index(file_entry, col_name).await?;

            // SURGICAL: Only get byte ranges for pages containing our rows
            let needed_ranges = selection.scan_ranges(&page_locations);

            // Track metrics
            metrics.total_pages += page_locations.len();
            metrics.fetched_pages += needed_ranges.len();
            metrics.skipped_pages += page_locations.len() - needed_ranges.len();

            // Step 4: Issue parallel fetches for ONLY the needed pages
            let resolved_path = manifest.resolve_path(
                &file_entry.relative_path
            );

            for range in needed_ranges {
                metrics.fetched_bytes += range.end - range.start;

                let storage = storage.clone();
                let path = resolved_path.clone();
                let col = col_name.clone();

                all_fetch_futures.push(async move {
                    let bytes = storage.get_slice(
                        &path,
                        range.start as usize..range.end as usize,
                    ).await?;
                    Ok::<_, anyhow::Error>(FetchedPage {
                        column: col,
                        file_idx: *file_idx,
                        byte_range: range,
                        data: bytes,
                    })
                });
            }
        }
    }

    // Step 5: Execute all page fetches in parallel
    let fetched_pages = futures::future::try_join_all(all_fetch_futures).await?;

    // Log surgical efficiency metrics
    debug_println!(
        "PARQUET_RETRIEVAL: {} docs, {} columns → fetched {}/{} pages ({:.1}% skipped), {} bytes",
        doc_addresses.len(),
        projected_columns.len(),
        metrics.fetched_pages,
        metrics.total_pages,
        metrics.skip_percentage(),
        metrics.fetched_bytes,
    );

    // Step 6: Decode only the fetched pages and extract target rows
    decode_and_assemble(fetched_pages, &global_rows, projected_columns, manifest)
}
```

#### Metrics and Monitoring

```rust
#[derive(Default)]
struct RetrievalMetrics {
    total_pages: usize,     // Total pages across all columns
    fetched_pages: usize,   // Pages actually fetched
    skipped_pages: usize,   // Pages skipped (no target rows)
    fetched_bytes: u64,     // Total bytes fetched
    cached_bytes: u64,      // Bytes served from L2 cache
}

impl RetrievalMetrics {
    fn skip_percentage(&self) -> f64 {
        if self.total_pages == 0 { return 0.0; }
        (self.skipped_pages as f64 / self.total_pages as f64) * 100.0
    }

    fn log_summary(&self) {
        debug_println!(
            "PAGE_METRICS: total={} fetched={} skipped={} ({:.1}%) bytes={}",
            self.total_pages, self.fetched_pages, self.skipped_pages,
            self.skip_percentage(), self.fetched_bytes
        );
    }
}
```

Java-side monitoring:
```java
// After retrieval, check metrics
ParquetRetrievalStats stats = searcher.getParquetRetrievalStats();
System.out.println("Pages fetched: " + stats.getFetchedPages());
System.out.println("Pages skipped: " + stats.getSkippedPages());
System.out.println("Skip rate: " + stats.getSkipPercentage() + "%");
System.out.println("Bytes fetched: " + stats.getFetchedBytes());
System.out.println("Bytes from cache: " + stats.getCachedBytes());
```

#### Cross-Column Coalescing Safety

When merging page ranges across columns, we must ensure coalescing doesn't pull in
unnecessary data:

```rust
fn safe_cross_column_coalesce(
    column_ranges: &HashMap<String, Vec<Range<u64>>>,
    max_gap: u64,
) -> Vec<Range<u64>> {
    // Flatten all ranges from all projected columns
    let mut all_ranges: Vec<Range<u64>> = column_ranges
        .values()
        .flat_map(|ranges| ranges.iter().cloned())
        .collect();

    if all_ranges.is_empty() {
        return vec![];
    }

    all_ranges.sort_by_key(|r| r.start);

    // Only merge ranges that are BOTH:
    // 1. Close together (gap < max_gap)
    // 2. From the same region of the file (prevents merging a page at offset 1000
    //    with a page at offset 1000000 just because nothing is between them)
    let mut merged = vec![all_ranges[0].clone()];
    for range in &all_ranges[1..] {
        let last = merged.last_mut().unwrap();
        let gap = if range.start > last.end {
            range.start - last.end
        } else {
            0
        };

        if gap <= max_gap {
            // Safe to merge — gap is small, the extra bytes will be cached
            // and may serve future requests
            last.end = last.end.max(range.end);
        } else {
            // Gap too large — don't merge, fetch separately
            merged.push(range.clone());
        }
    }

    merged
}
```

**Important**: The max_gap threshold (default 64KB) prevents pathological merging. Without
this guard, two pages 100MB apart would be merged into a single 100MB fetch, defeating the
purpose of surgical retrieval.

#### When Full-Column Decode IS Needed

For **fast field transcode** (aggregations), we need the complete column because aggregations
scan every document. In this case, we skip RowSelection and fetch all pages:

```rust
fn fetch_full_column_for_fast_field(
    file_entry: &ParquetFileEntry,
    col_name: &str,
    storage: &Arc<StorageWithPersistentCache>,
) -> impl Future<Output = Result<Vec<u8>>> {
    // For fast field transcode: fetch ALL pages (no surgical skip)
    // This is correct because aggregations need every row's value
    let col_info = file_entry.get_column_info(col_name);
    let byte_range = col_info.byte_range; // Full column chunk range

    let path = file_entry.resolved_path.clone();
    async move {
        storage.get_slice(&path, byte_range.0 as usize..byte_range.1 as usize).await
    }
}
```

The two paths are clearly separated:

| Access Pattern | Page Strategy | When Used |
|---------------|--------------|-----------|
| Document retrieval | **Surgical** — only pages with target rows | `searcher.doc(addr, fields)` |
| Fast field transcode | **Full column** — all pages needed | Aggregations, sorting |
| Column prewarm | **Full column** — populate L2 cache | `preloadParquetColumns()` |

#### Efficiency Examples

**Example 1: Retrieving 10 documents from a 1M-row, 2-column query**
```
Column "title": 1000 pages × 4KB each = 4MB total
Column "price": 200 pages × 4KB each = 800KB total

DocIds map to 3 title pages and 2 price pages:
  Surgical fetch: 3 + 2 = 5 pages = 20KB
  Full column:    1000 + 200 = 1200 pages = 4.8MB
  Savings: 99.6% I/O reduction
```

**Example 2: Retrieving 100 documents scattered across all row groups**
```
Column "title": 1000 pages
DocIds spread across 40 pages:
  Surgical fetch: 40 pages = 160KB
  Full column: 1000 pages = 4MB
  Savings: 96% I/O reduction
```

**Example 3: Fast field transcode for terms aggregation on "category"**
```
Column "category": 500 pages = 2MB
Need ALL values for aggregation → fetch all 500 pages
No surgical skip — this is correct behavior for aggregations
```

---

## 12. Schema Auto-Derivation

### Parquet to Tantivy Type Mapping

| Parquet Physical | Arrow/Logical Type | Tantivy Field Type | ColumnarWriter Method |
|-----------------|-------------------|-------------------|----------------------|
| BOOLEAN | Boolean | Bool | `record_bool()` |
| INT32 | Int32 | I64 | `record_numerical(i64)` |
| INT32 | Date32 | DateTime | `record_datetime()` |
| INT64 | Int64 | I64 | `record_numerical(i64)` |
| INT64 | Timestamp(*) | DateTime | `record_datetime()` |
| INT64 | Date64 | DateTime | `record_datetime()` |
| FLOAT | Float32 | F64 | `record_numerical(f64)` |
| DOUBLE | Float64 | F64 | `record_numerical(f64)` |
| BYTE_ARRAY | Utf8/String | TEXT (indexed) / Str (fast) | `record_str()` |
| BYTE_ARRAY | Binary | Bytes | `record_bytes()` |
| FIXED_LEN | UUID (16 bytes) | Bytes | `record_bytes()` |
| INT32/INT64 | Decimal | F64 (scaled) | `record_numerical(f64)` |
| LIST<*> | List<*> | Json (JSON-encoded) | `record_str()` via JSON |
| MAP<K,V> | Map<K,V> | Json (JSON-encoded) | `record_str()` via JSON |
| STRUCT{...} | Struct{...} | Json (JSON-encoded) | `record_str()` via JSON |

### Schema Builder Logic

```rust
fn derive_tantivy_schema(
    parquet_schema: &ArrowSchema,
    config: &ParquetCompanionConfig,
) -> tantivy::Schema {
    let mut builder = SchemaBuilder::new();

    for field in parquet_schema.fields() {
        let name = field.name();
        let tantivy_type = map_arrow_to_tantivy(field.data_type());

        match tantivy_type {
            TantivyType::Text => {
                // Text fields: always indexed, never stored
                let tokenizer = config.tokenizer_for(name).unwrap_or("default");
                let mut options = TextOptions::default()
                    .set_indexing_options(
                        TextFieldIndexing::default()
                            .set_tokenizer(tokenizer)
                            .set_index_option(IndexRecordOption::WithFreqsAndPositions)
                    );
                // No .set_stored() — parquet is the store
                // Fast field only if mode requires it
                if config.should_have_fast_field(name, &tantivy_type) {
                    options = options.set_fast(Some(tokenizer));
                }
                builder.add_text_field(name, options);
            }
            TantivyType::I64 => {
                let mut options = IntOptions::default()
                    .set_indexed();
                // No .set_stored()
                if config.should_have_fast_field(name, &tantivy_type) {
                    options = options.set_fast();
                }
                builder.add_i64_field(name, options);
            }
            // ... similar for F64, Bool, DateTime, Bytes
        }
    }

    builder.build()
}
```

### User Overrides

The auto-derivation can be customized:

```java
ParquetCompanionConfig config = new ParquetCompanionConfig()
    .withTokenizer("title", "en_stem")       // Custom tokenizer for title field
    .withTokenizer("description", "default") // Default tokenizer for description
    .withSkipField("internal_id")            // Don't index this field
    .withFastFieldMode(FastFieldMode.HYBRID);
```

---

## 13. Multi-Parquet File Addressing

### Row Offset Table

The manifest maintains a sorted array of parquet files with cumulative row offsets:

```
files[0]: { path: "part-00000.parquet", row_offset: 0,      num_rows: 50000  }
files[1]: { path: "part-00001.parquet", row_offset: 50000,   num_rows: 50000  }
files[2]: { path: "part-00002.parquet", row_offset: 100000,  num_rows: 30000  }
```

### DocId to File Mapping

DocId translation is a two-step process:

1. **Segment-aware translation**: Convert `(segment_ord, local_doc_id)` → global parquet row
   using `segment_row_ranges` (see §19.4)
2. **File-level binary search**: Map global parquet row → (file_index, row_in_file)

```rust
/// Step 1: Translate segment-local DocId to global parquet row
fn translate_to_global_row(
    segment_ord: u32,
    local_doc_id: u32,
    manifest: &ParquetManifest,
) -> u64 {
    let seg_range = &manifest.segment_row_ranges[segment_ord as usize];
    seg_range.row_offset + local_doc_id as u64
}

/// Step 2: Map global parquet row to (file_index, row_within_file)
fn locate_row_in_file(global_row: u64, manifest: &ParquetManifest) -> (usize, u32) {
    // Binary search on row_offset
    let file_idx = manifest.parquet_files
        .partition_point(|f| f.row_offset <= global_row) - 1;

    let row_in_file = global_row - manifest.parquet_files[file_idx].row_offset;
    (file_idx, row_in_file as u32)
}

// Example:
// (segment_ord=1, local_doc_id=25000) with segment_row_ranges[1].row_offset=50000
//   → global_row = 50000 + 25000 = 75000
//   → binary_search → files[1], row_in_file = 75000 - 50000 = 25000
```

### Multi-File Batch Retrieval

When search returns DocIds from multiple parquet files, they are first translated to global
parquet rows via `segment_row_ranges`, then grouped by file and fetched in parallel:

```rust
fn group_rows_by_file(
    global_rows: &[u64],       // Already translated from (segment_ord, local_doc_id)
    manifest: &ParquetManifest,
) -> HashMap<usize, Vec<u32>> {
    let mut groups: HashMap<usize, Vec<u32>> = HashMap::new();
    for &global_row in global_rows {
        let (file_idx, row_in_file) = locate_row_in_file(global_row, manifest);
        groups.entry(file_idx).or_default().push(row_in_file);
    }
    groups
}
```

Each file group's page fetches run in parallel via separate futures, and all file groups
also run in parallel with each other.

---

## 14. Prewarm Strategy for Parquet Columns

### Prewarm Levels

**Level 1 — Metadata prewarm** (automatic, from manifest):
Minimal I/O needed. Column-level metadata (byte ranges, types, compression) is embedded in
`_parquet_manifest.json` which lives in the split bundle. Page-level OffsetIndex is fetched
from the parquet footer on first surgical access and cached (see §19.8).

**Level 2 — Column page prewarm** (explicit API):
```java
// Prewarm specific parquet columns to L2 disk cache
searcher.preloadParquetColumns("title", "score", "category").join();
```

This fetches all pages for the specified columns from L3 to L2. Subsequent queries hit
L2 instead of L3.

**Level 3 — Fast field transcode prewarm** (explicit API):
```java
// Prewarm AND transcode parquet columns to tantivy fast field format
searcher.preloadParquetFastFields("title", "score", "category").join();
```

This goes further: fetches pages → decodes → transcodes to tantivy columnar format →
caches the transcoded bytes in L1. First aggregation query has zero decode overhead.

### Prewarm Implementation

```rust
async fn prewarm_parquet_columns(
    manifest: &ParquetManifest,
    storage: &Arc<StorageWithPersistentCache>,
    columns: &[String],
) -> Result<()> {
    let mut futures = Vec::new();

    for file_entry in &manifest.parquet_files {
        for column_name in columns {
            // Use column-level byte_range from the manifest (NOT page-level data,
            // which is not stored — see §19.8). For prewarm, we fetch the entire
            // column chunk, which is the right granularity since aggregations scan
            // all rows anyway.
            for rg in &file_entry.row_groups {
                if let Some(col_entry) = rg.columns.get(column_name.as_str()) {
                    let storage = storage.clone();
                    let path = manifest.resolve_path(&file_entry.relative_path);
                    let range = col_entry.byte_range[0]..col_entry.byte_range[1];

                    futures.push(async move {
                        // Fetches from L3 if not in L2, caches in L2
                        storage.get_slice(
                            &Path::new(&path),
                            range.start as usize..range.end as usize,
                        ).await
                    });
                }
            }
        }
    }

    // All column chunks across all files across all columns fetched in parallel
    let results = futures::future::join_all(futures).await;
    let success = results.iter().filter(|r| r.is_ok()).count();
    let total = results.len();
    debug_println!("PARQUET_PREWARM: {}/{} column chunks prewarmed successfully", success, total);

    Ok(())
}
```

---

## 15. Table Root and Regional Replication

### Concept

The manifest stores **relative paths** to parquet files. At query time, the caller provides
a **table root** that is prepended to resolve full URIs:

```
Manifest:  relative_path = "data/part-00000.parquet"
Table root = "s3://us-east-1-bucket/tables/sales/"
Resolved   = "s3://us-east-1-bucket/tables/sales/data/part-00000.parquet"
```

This enables the same split to work with parquet files replicated across regions or clouds.

### Resolution Logic

```rust
fn resolve_parquet_path(
    table_root: &str,
    relative_path: &str,
) -> String {
    if table_root.ends_with('/') {
        format!("{}{}", table_root, relative_path)
    } else {
        format!("{}/{}", table_root, relative_path)
    }
}
```

### Multi-Region Example

```java
// Same split, US East region
SplitSearcher us_searcher = cacheManager.createSplitSearcher(
    "s3://index-bucket/sales.split",
    new ParquetCompanionConfig()
        .withTableRoot("s3://us-east-1-data/tables/sales/")
        .withParquetStorage(usEastConfig));

// Same split, EU West region
SplitSearcher eu_searcher = cacheManager.createSplitSearcher(
    "s3://index-bucket/sales.split",
    new ParquetCompanionConfig()
        .withTableRoot("s3://eu-west-1-data/tables/sales/")
        .withParquetStorage(euWestConfig));

// Both splits are identical — only the parquet data source differs
```

### Cache Isolation

Different table roots produce different cache keys (because `storage_loc` includes the
resolved path). This ensures:
- US East data and EU West data are cached independently
- No cross-region cache corruption
- Each region builds its own L2 cache

---

## 16. Performance Analysis

### Latency Breakdown

**Document retrieval (10 docs, warm L2 cache):**

| Step | Time | Notes |
|------|------|-------|
| DocId → file mapping | <0.01ms | Binary search on manifest |
| Build RowSelection | <0.01ms | Convert doc list to row selection |
| Compute page ranges | <0.1ms | OffsetIndex from cache (or footer fetch on first access) |
| Fetch pages from L2 | 1-3ms | Local disk via mmap |
| Decompress pages | 0.1-1ms | Snappy/LZ4 decompression |
| Extract row values | <0.1ms | Sequential scan within page |
| **Total** | **~2-5ms** | Comparable to native store access |

**Document retrieval (10 docs, cold cache — first query):**

| Step | Time | Notes |
|------|------|-------|
| Fetch pages from L3 (S3) | 50-200ms | Network round trip |
| Cache in L2 | <1ms | Background write |
| Decompress + extract | ~1ms | Same as warm path |
| **Total** | **~50-200ms** | First query only; subsequent: 2-5ms |

**Fast field transcode (first access per segment):**

| Rows | Numeric columns (5) | String columns (3) | Total |
|------|---------------------|---------------------|-------|
| 100K | ~2ms | ~5ms | ~7ms |
| 1M | ~15ms | ~30ms | ~45ms |
| 10M | ~100ms | ~250ms | ~350ms |

After transcode, fast field access is **identical** to native (O(1) array indexing).

### Storage Savings

**Example: 1M document, 10-field schema with 3 text + 7 numeric fields:**

| Component | Standard Split | Mode 2 (Hybrid) | Mode 3 (Parquet Only) |
|-----------|---------------|-----------------|----------------------|
| Term dictionaries | 20 MB | 20 MB | 20 MB |
| Postings lists | 40 MB | 40 MB | 40 MB |
| Field norms | 5 MB | 5 MB | 5 MB |
| Fast fields (numeric) | 56 MB | 56 MB | 0 MB |
| Fast fields (string) | 300 MB | 0 MB | 0 MB |
| Store | 200 MB | 0 MB | 0 MB |
| **Total split size** | **621 MB** | **121 MB** | **65 MB** |
| **Reduction** | — | **80%** | **90%** |

---

## 17. Java API Design

### Configuration

```java
public class ParquetCompanionConfig {

    public enum FastFieldMode {
        DISABLED,       // All fast fields native in split
        HYBRID,         // Numerics native, strings from parquet
        PARQUET_ONLY    // All fast fields from parquet
    }

    // Table root for resolving relative parquet paths
    private String tableRoot;

    // Fast field mode
    private FastFieldMode fastFieldMode = FastFieldMode.HYBRID;

    // Parquet storage credentials (may differ from split storage)
    private ParquetStorageConfig parquetStorage;

    // Optional: per-field tokenizer overrides
    private Map<String, String> tokenizerOverrides = new HashMap<>();

    // Optional: fields to skip during indexing
    private Set<String> skipFields = new HashSet<>();

    // Missing parquet file behavior (default: FAIL)
    private ParquetMissingFilePolicy missingFilePolicy = ParquetMissingFilePolicy.FAIL;

    // Default fields for doc retrieval when no explicit field list is provided
    // If empty and no fields specified, doc() throws IllegalArgumentException
    private List<String> defaultRetrievalFields = new ArrayList<>();

    // Builder methods
    public ParquetCompanionConfig withTableRoot(String root) { ... }
    public ParquetCompanionConfig withFastFieldMode(FastFieldMode mode) { ... }
    public ParquetCompanionConfig withParquetStorage(ParquetStorageConfig config) { ... }
    public ParquetCompanionConfig withTokenizer(String fieldName, String tokenizer) { ... }
    public ParquetCompanionConfig withSkipField(String fieldName) { ... }
    public ParquetCompanionConfig withMissingFilePolicy(ParquetMissingFilePolicy policy) { ... }
    public ParquetCompanionConfig withDefaultRetrievalFields(String... fields) { ... }

    // Parquet storage credential configuration
    public static class ParquetStorageConfig {
        public ParquetStorageConfig withAwsCredentials(String accessKey, String secretKey) { ... }
        public ParquetStorageConfig withAwsCredentials(String accessKey, String secretKey, String sessionToken) { ... }
        public ParquetStorageConfig withAwsRegion(String region) { ... }
        public ParquetStorageConfig withCustomEndpoint(String endpoint) { ... }
        public ParquetStorageConfig withPathStyle(boolean pathStyle) { ... }
        public ParquetStorageConfig withAzureCredentials(String accountName, String accountKey) { ... }
        public ParquetStorageConfig withAzureBearerToken(String accountName, String bearerToken) { ... }
        public ParquetStorageConfig withLocalFileSystem() { ... }
    }
}
```

### Split Creation from Parquet (Multi-File — Recommended)

The primary indexing path accepts **multiple parquet files** in a single call, producing
one split that references all of them. This avoids the need for merge operations.

```java
// RECOMMENDED: Index multiple parquet files into a single split
// This is the primary path — avoids merge entirely
List<String> parquetFiles = Arrays.asList(
    "s3://data-lake/tables/sales/part-00000.parquet",
    "s3://data-lake/tables/sales/part-00001.parquet",
    "s3://data-lake/tables/sales/part-00002.parquet",
    "s3://data-lake/tables/sales/part-00003.parquet"
);

ParquetCompanionConfig config = new ParquetCompanionConfig()
    .withTableRoot("s3://data-lake/tables/sales/")
    .withFastFieldMode(FastFieldMode.HYBRID)
    .withTokenizer("title", "en_stem")
    .withMissingFilePolicy(ParquetMissingFilePolicy.FAIL)  // Default
    .withParquetStorage(
        new ParquetStorageConfig()
            .withAwsCredentials("data-lake-key", "data-lake-secret")
            .withAwsRegion("us-east-1")
    );

// Creates ONE split referencing ALL parquet files
// Schema is auto-derived from the first file and validated against all others
QuickwitSplit.SplitMetadata result = QuickwitSplit.createFromParquet(
    parquetFiles,
    "/tmp/output/sales.split",
    splitConfig,    // Normal MergeConfig for the split itself
    config          // Parquet companion configuration
);

System.out.println("Split size: " + result.getUncompressedSizeBytes()); // ~80% smaller
System.out.println("Parquet files: " + parquetFiles.size());  // All tracked in manifest
```

### Searching with Parquet Companion

```java
// Configure split cache (for split access)
SplitCacheManager.CacheConfig splitCache = new SplitCacheManager.CacheConfig("search-cache")
    .withMaxCacheSize(500_000_000)
    .withAwsCredentials("index-key", "index-secret")  // Split storage creds
    .withAwsRegion("us-east-1")
    .withTieredCache(new TieredCacheConfig()
        .withDiskCachePath("/mnt/nvme/cache")
        .withMaxDiskSize(100_000_000_000L));

// Configure parquet companion (for parquet access — may use different creds)
ParquetCompanionConfig parquetConfig = new ParquetCompanionConfig()
    .withTableRoot("s3://data-lake/tables/sales/")
    .withFastFieldMode(FastFieldMode.HYBRID)
    .withParquetStorage(
        new ParquetStorageConfig()
            .withAwsCredentials("data-lake-key", "data-lake-secret")
            .withAwsRegion("us-east-1")
    );

try (SplitCacheManager cacheManager = SplitCacheManager.getInstance(splitCache)) {
    try (SplitSearcher searcher = cacheManager.createSplitSearcher(
            "s3://index-bucket/sales.split",
            parquetConfig)) {

        // Prewarm parquet columns for fast field access
        searcher.preloadParquetFastFields("title", "category").join();

        // Search — uses split's inverted index
        SplitQuery query = new SplitTermQuery("title", "laptop");
        SearchResult result = searcher.search(query, 10);

        // Document retrieval — transparently reads from parquet
        for (SearchHit hit : result.getHits()) {
            Document doc = searcher.doc(hit.getDocAddress());
            System.out.println("Title: " + doc.getFirst("title"));
            System.out.println("Price: " + doc.getFirst("price"));
        }

        // Aggregations — string fast fields served from parquet (hybrid mode)
        Map<String, Object> aggs = Map.of(
            "categories", Map.of("terms", Map.of("field", "category")));
        AggregationResult aggResult = searcher.searchWithAggregations(query, 0, aggs);
    }
}
```

---

## 18. Split Merge Handling

When splits with parquet manifests are merged, the merge process must preserve and correctly
update the `_parquet_manifest.json`. This requires changes to three stages of the merge
pipeline: extraction, manifest combination, and split creation.

### Current Merge Pipeline (Before Parquet Changes)

The existing merge pipeline in `native/src/quickwit_split/` works as follows:

```
1. Download + Extract (download.rs, temp_management.rs)
   ├─ Download split files from S3/Azure/local
   └─ Extract ONLY known Tantivy files: meta.json, {segment_id}.{store,pos,idx,term,fieldnorm,fast}
      ⚠️ Custom bundle files like _parquet_manifest.json are silently discarded

2. Merge (merge_impl.rs)
   ├─ Open each extracted directory as a Tantivy Index
   ├─ Combine via UnionDirectory
   └─ Perform segment-level merge → new merged index in temp directory

3. Create Merged Split (upload.rs → split_creation.rs)
   ├─ Scan merged temp directory for files
   ├─ Generate hotcache
   ├─ SplitPayloadBuilder.get_split_payload(files, empty_split_fields, hotcache)
   └─ Write split file with new footer
      ⚠️ serialized_split_fields always empty — no custom data preserved
```

### Problem: DocId Renumbering During Merge

When Tantivy merges segments, DocIds are **stacked deterministically** based on input order:

```
merger.rs:335-346 (tantivy source):
  // Create the total list of doc ids by stacking the doc ids from the different segment.
  // - Segment 0's doc ids become doc id [0, seg0.max_doc)
  // - Segment 1's doc ids become  [seg0.max_doc, seg0.max_doc + seg1.max_doc)
  // - Segment 2's doc ids become  [seg0.max_doc + seg1.max_doc, ...)
```

**With no deletions**, the identity `DocId N = parquet row N` survives because documents
are simply concatenated. The manifest's `row_offset` per parquet file must be adjusted to
reflect the new stacking:

```
Before merge:
  Split A manifest:  part-00000 (row_offset=0, num_rows=50000)
                     part-00001 (row_offset=50000, num_rows=30000)
  Split B manifest:  part-00002 (row_offset=0, num_rows=40000)

After merge (A stacked before B):
  Merged manifest:   part-00000 (row_offset=0, num_rows=50000)        ← unchanged
                     part-00001 (row_offset=50000, num_rows=30000)    ← unchanged
                     part-00002 (row_offset=80000, num_rows=40000)    ← adjusted: 0 → 80000
```

**With deletions**, the identity breaks because deleted documents are compacted out:

```
Split A: 50000 rows, 100 deleted → contributes 49900 alive docs → DocIds [0, 49900)
Split B: 40000 rows, 0 deleted → contributes 40000 alive docs → DocIds [49900, 89900)

Problem: DocId 5 in the merged split may NOT correspond to parquet row 5
         if rows 0-4 had deletions in the original split A.
```

### Design Decision: Deletion Handling

**Option 1: Disallow merge of parquet companion splits with deletions** (SELECTED)
- Parquet companion mode targets append-only data lake workloads
- Deletions are extremely rare in this usage pattern (parquet files are immutable)
- The merge process validates that no deletions exist and fails fast with a clear error
- If a user needs deletions, they should re-index from parquet (not merge existing splits)

**Option 2: Store a DocId → parquet row remapping table** (REJECTED)
- Expensive: O(num_docs) storage for the remapping
- Complex: Must be maintained through subsequent merges
- Defeats the purpose of the zero-overhead identity mapping

**Option 3: Accept broken identity and use binary-search remapping** (REJECTED)
- Would require an additional data structure in the manifest
- Adds latency to every document retrieval
- Complexity not justified for the append-only target workload

### Merge Pipeline Changes

The merge pipeline requires three modifications:

#### Change 1: Extract Parquet Manifest from Source Splits

**File**: `temp_management.rs` — `extract_split_to_directory_impl()`

After extracting known Tantivy files, also extract `_parquet_manifest.json` from the
BundleDirectory (if it exists). Write it to a known location in the temp directory:

```rust
// After existing file extraction loop (line ~196)

// Extract parquet manifest if present (parquet companion mode)
let manifest_path = Path::new("_parquet_manifest.json");
if bundle_directory.exists(manifest_path)? {
    debug_log!("📦 PARQUET: Extracting _parquet_manifest.json from split bundle");
    streaming_copy_file(
        bundle_directory.as_ref(),
        &output_directory,
        manifest_path
    )?;
    copied_files += 1;
}
```

#### Change 2: Combine Manifests During Merge

**New file**: `parquet_manifest_merge.rs`

After the segment merge completes but before creating the merged split file, combine the
parquet manifests from all source splits:

```rust
/// Combines parquet manifests from multiple source splits into a single merged manifest.
///
/// The merge follows the same segment stacking order that Tantivy uses for DocIds:
/// - Split 0's parquet files keep their original row_offsets
/// - Split 1's parquet files have row_offsets adjusted by Split 0's total row count
/// - Split 2's parquet files adjusted by Split 0 + Split 1 total row count
/// - etc.
///
/// IMPORTANT: This function validates that NO documents were deleted in any source split.
/// If deletions are detected, the merge fails with a clear error because the DocId identity
/// mapping (DocId N = parquet row N) cannot be preserved through a merge with deletions.
pub fn combine_parquet_manifests(
    source_temp_dirs: &[&Path],
    output_dir: &Path,
) -> Result<Option<()>> {
    // Phase 1: Collect manifests from all source splits
    let mut manifests: Vec<(usize, ParquetManifest)> = Vec::new();

    for (i, temp_dir) in source_temp_dirs.iter().enumerate() {
        let manifest_path = temp_dir.join("_parquet_manifest.json");
        if manifest_path.exists() {
            let manifest_bytes = std::fs::read(&manifest_path)?;
            let manifest: ParquetManifest = serde_json::from_slice(&manifest_bytes)?;
            manifests.push((i, manifest));
        }
    }

    // If no splits have parquet manifests, nothing to do
    if manifests.is_empty() {
        return Ok(None);
    }

    // Validate: ALL splits must have manifests (cannot merge parquet and non-parquet splits)
    if manifests.len() != source_temp_dirs.len() {
        return Err(anyhow!(
            "Cannot merge parquet companion splits with non-parquet splits. \
             {} of {} splits have parquet manifests.",
            manifests.len(), source_temp_dirs.len()
        ));
    }

    // Phase 2: Validate no deletions in any source split
    // Read each source's meta.json to check for deleted documents
    for (i, temp_dir) in source_temp_dirs.iter().enumerate() {
        validate_no_deletions(temp_dir, i)?;
    }

    // Phase 3: Combine manifests with adjusted row_offsets
    let mut merged_files: Vec<ParquetFileEntry> = Vec::new();
    let mut cumulative_row_offset: u64 = 0;

    // Validate fast_field_mode consistency across all manifests
    let first_mode = &manifests[0].1.fast_field_mode;
    for (i, (_, manifest)) in manifests.iter().enumerate() {
        if &manifest.fast_field_mode != first_mode {
            return Err(anyhow!(
                "Cannot merge splits with different fast_field_modes: \
                 split 0 has {:?}, split {} has {:?}",
                first_mode, i, manifest.fast_field_mode
            ));
        }
    }

    for (_split_idx, manifest) in &manifests {
        let split_row_base = cumulative_row_offset;

        for mut file_entry in manifest.parquet_files.clone() {
            // Adjust row_offset: add the cumulative offset from prior splits
            file_entry.row_offset += split_row_base;
            merged_files.push(file_entry);
        }

        // Advance cumulative offset by this split's total row count
        let split_total_rows: u64 = manifest.parquet_files
            .iter()
            .map(|f| f.num_rows)
            .sum();
        cumulative_row_offset += split_total_rows;
    }

    // Phase 4: Build merged manifest with all required fields
    // Merged split always produces exactly one segment (tantivy merge combines all inputs)
    let segment_row_ranges = vec![SegmentRowRange {
        segment_ord: 0,
        row_offset: 0,
        num_rows: cumulative_row_offset,
    }];

    // Use column_mapping from first source manifest (already validated as schema-consistent)
    let column_mapping = manifests[0].1.column_mapping.clone();

    let merged_manifest = ParquetManifest {
        version: 1,
        fast_field_mode: first_mode.clone(),
        segment_row_ranges,
        parquet_files: merged_files,
        total_rows: cumulative_row_offset,
        column_mapping,
    };

    let merged_json = serde_json::to_string_pretty(&merged_manifest)?;
    std::fs::write(
        output_dir.join("_parquet_manifest.json"),
        merged_json.as_bytes()
    )?;

    debug_log!(
        "✅ PARQUET MERGE: Combined {} manifests, total {} parquet files, {} rows",
        manifests.len(),
        merged_manifest.parquet_files.len(),
        cumulative_row_offset
    );

    Ok(Some(()))
}

/// Validates that a source split has no deleted documents.
/// This is required because DocId identity mapping cannot survive deletions.
fn validate_no_deletions(temp_dir: &Path, split_index: usize) -> Result<()> {
    // Open the index and check for deleted docs
    let mmap_directory = MmapDirectory::open(temp_dir)?;
    let tokenizer_manager = create_quickwit_tokenizer_manager();
    let index = open_index(mmap_directory.box_clone(), &tokenizer_manager)?;
    let index_meta = index.load_metas()?;

    for segment_meta in &index_meta.segments {
        if segment_meta.num_deleted_docs() > 0 {
            return Err(anyhow!(
                "Cannot merge parquet companion split {} (segment {}): \
                 contains {} deleted documents. Parquet companion mode \
                 requires append-only workloads with no deletions. \
                 Re-index from parquet files instead.",
                split_index,
                segment_meta.id().uuid_string(),
                segment_meta.num_deleted_docs()
            ));
        }
    }

    Ok(())
}
```

#### Change 3: Include Manifest in Merged Split Bundle

**File**: `split_creation.rs` — `create_quickwit_split()`

The existing code already scans the filesystem for files to include (lines 277-290).
Since Change 2 writes `_parquet_manifest.json` to the merged temp directory, it will be
automatically picked up by the filesystem scan — **no additional changes needed** in
`create_quickwit_split()` because the file filter already includes non-hidden, non-.split
files.

However, we must ensure the manifest file is present in `index_dir` before the scan:

```rust
// In merge_impl.rs, AFTER perform_quickwit_merge() and BEFORE create_merged_split_file():

// Combine parquet manifests from source splits (if any)
let source_temp_paths: Vec<&Path> = extracted_splits.iter()
    .map(|s| s.temp_path.as_path())
    .collect();
combine_parquet_manifests(&source_temp_paths, &output_temp_dir)?;
```

### Merge Ordering Guarantees

The merge process must maintain a deterministic relationship between the segment stacking
order and the manifest combination order. The current implementation guarantees this because:

1. **Split directories are pushed to `split_directories` in iteration order** (merge_impl.rs:236)
2. **`open_split_directories()` preserves this order** when creating UnionDirectory
3. **Tantivy stacks segments in the order they appear** in the UnionDirectory
4. **`combine_parquet_manifests()` processes source directories in the same order**

This is enforced by using the same `extracted_splits` ordering for both the segment merge
and the manifest combination.

### Edge Cases

#### Non-Parquet Splits Mixed with Parquet Splits
**Behavior**: Merge fails with a clear error. Cannot mix splits that have parquet manifests
with splits that don't — the DocId space would be inconsistent.

#### Different Fast Field Modes
**Behavior**: Merge fails with a clear error. All source splits must use the same
`fast_field_mode`. Mixing modes would produce an internally inconsistent manifest.

#### Different Table Roots
**Behavior**: Allowed. The manifest stores relative paths — the table root is provided at
query time, not stored in the manifest. Splits referencing different parquet file sets
from different original table roots can be merged as long as the relative paths are
consistent when resolved at query time.

#### Overlapping Parquet Files
**Behavior**: Allowed but unusual. If two source splits reference the same parquet file
(e.g., because they index different row ranges of the same file), both entries are
preserved in the merged manifest with their adjusted row_offsets. The DocId → parquet
mapping remains correct.

#### Large Number of Parquet File Entries
**Behavior**: The merged manifest may grow significantly. For example, merging 100 splits
each with 10 parquet files produces a manifest with 1000 entries. The manifest uses JSON,
so a 1000-entry manifest is approximately 100-200KB — well within acceptable limits for
a bundle file.

### Validation at Query Time

After a merge, the `SplitSearcher` validates manifest consistency when opening the split:

```rust
fn validate_manifest_consistency(manifest: &ParquetManifest, num_docs: u32) -> Result<()> {
    // Total rows in manifest should equal num_docs in the split
    let total_manifest_rows: u64 = manifest.parquet_files
        .iter()
        .map(|f| f.num_rows)
        .sum();

    if total_manifest_rows != num_docs as u64 {
        return Err(anyhow!(
            "Parquet manifest row count ({}) does not match split document count ({}). \
             This may indicate a corrupted merge or deletions after indexing.",
            total_manifest_rows, num_docs
        ));
    }

    // Verify row_offsets are monotonically increasing and contiguous
    let mut expected_offset = 0u64;
    for file_entry in &manifest.parquet_files {
        if file_entry.row_offset != expected_offset {
            return Err(anyhow!(
                "Non-contiguous row_offset: expected {} but got {} for file '{}'",
                expected_offset, file_entry.row_offset, file_entry.relative_path
            ));
        }
        expected_offset += file_entry.num_rows;
    }

    Ok(())
}
```

---

## 19. Gap Resolutions and Design Addenda

This section addresses design gaps identified during review. Each subsection resolves a
specific gap with a concrete design decision and implementation approach.

### 19.1 Dictionary Page Handling (Critical)

**Gap**: Parquet dictionary-encoded columns store a dictionary page before data pages. The
manifest's column byte ranges include the dictionary page, but surgical page-level retrieval
must ensure the dictionary page is always fetched alongside data pages.

**Resolution**: arrow-rs handles dictionary pages transparently. When using
`ParquetRecordBatchReaderBuilder` with our `CachedParquetReader` (AsyncFileReader), the
reader automatically fetches the dictionary page for RLE_DICTIONARY encoded columns before
reading data pages. No special handling is needed in our code.

For manual page-level fetches (bypassing arrow-rs), the column chunk `byte_range` in the
manifest always starts at the dictionary page offset (if one exists). When we fetch the
OffsetIndex from the parquet footer for surgical retrieval, the first `PageLocation` entry
may point to a dictionary page — arrow-rs's `SerializedPageReader` handles the page type
dispatch internally.

**Implementation**: No special code. Rely on arrow-rs's complete codec/dictionary handling.

### 19.2 Store File Handling (Critical)

**Gap**: In parquet companion mode, `.store` files are replaced by parquet for document
storage. Tantivy expects `.store` files to exist for `searcher.doc()` calls.

**Decision**: Stub store + SplitSearcher-level routing.

**Resolution**:

At split creation:
- Write a minimal valid `.store` file containing zero-length documents
- The store header is valid so tantivy won't error when opening the index
- Each document has an empty payload (just the document header with zero fields)

At query time:
- `SplitSearcher.doc()` detects parquet companion mode (manifest present)
- Routes document retrieval to the parquet reader instead of tantivy's store reader
- The `ParquetAugmentedDirectory` does NOT intercept `.store` reads — it only handles `.fast`
- The routing decision happens at the SplitSearcher/Rust wrapper level, above tantivy

```rust
// In the SplitSearcher native wrapper:
fn doc_with_projection(
    &self,
    segment_ord: u32,
    doc_id: u32,
    fields: &[String],
) -> Result<HashMap<String, FieldValue>> {
    if let Some(ref manifest) = self.parquet_manifest {
        // Parquet companion mode: retrieve from parquet
        let global_row = manifest.segment_row_ranges[segment_ord as usize].row_offset
            + doc_id as u64;
        self.retrieve_from_parquet(global_row, fields)
    } else {
        // Standard mode: retrieve from tantivy store
        self.retrieve_from_store(segment_ord, doc_id)
    }
}
```

**Why stub instead of no store file**: Tantivy's `SegmentReader::open()` checks for the
store file's existence. An absent file causes an error during index opening, not during
doc retrieval. The stub avoids this initialization error while the routing prevents any
actual data from being read from it.

### 19.3 Null Value Handling (Critical)

**Gap**: Parquet columns support null values natively. Tantivy fast fields don't have a
null concept — a field is simply absent for a document.

**Resolution**:

During indexing (parquet → tantivy documents):
- Skip `writer.add_*()` for null values — tantivy treats absent values correctly
- The document simply doesn't have that field for that row

During fast field transcode (parquet → tantivy columnar):
- Skip `writer.record_*()` calls for null entries
- tantivy's `ColumnarWriter` handles sparse data: if row N is not recorded for a column,
  that column has no value for row N
- Aggregations correctly treat missing values as absent (excluded from counts, sums, etc.)

During document retrieval (parquet → Java):
- Return `null` in the Java `Document` object for null parquet values
- This matches tantivy4java's existing behavior for absent stored fields

```rust
// During transcode
for (row_id, value) in decoded_column.iter().enumerate() {
    match value {
        Some(v) => writer.record_numerical(row_id as u32, field_name, v),
        None => {} // Skip — tantivy handles absent values natively
    }
}
```

No sentinel values or special encoding needed.

### 19.4 Multi-Segment Row Alignment (Critical)

**Gap**: Splits may contain multiple segments. The simple `DocId = parquet row` identity
only works within a single segment.

**Decision**: Support multiple segments with a `segment_row_ranges` mapping table.

**Resolution**:

The manifest includes a `segment_row_ranges` array that maps each segment's ordinal to
a range in the global parquet row space:

```json
"segment_row_ranges": [
  { "segment_ord": 0, "row_offset": 0, "num_rows": 50000 },
  { "segment_ord": 1, "row_offset": 50000, "num_rows": 30000 }
]
```

DocId translation:
- tantivy search returns `(segment_ord, local_doc_id)` pairs
- Global parquet row = `segment_row_ranges[segment_ord].row_offset + local_doc_id`
- Binary search on `parquet_files[].row_offset` then maps global row to file + row-in-file

```rust
fn translate_docid_to_parquet_row(
    segment_ord: u32,
    local_doc_id: u32,
    manifest: &ParquetManifest,
) -> u64 {
    let seg_range = &manifest.segment_row_ranges[segment_ord as usize];
    seg_range.row_offset + local_doc_id as u64
}
```

During indexing:
- Track each `writer.commit()` / segment boundary and record the row range
- If multiple commits create multiple segments, each gets its own entry
- Segments are always contiguous: segment 0 gets rows [0, N), segment 1 gets [N, N+M), etc.

During merge:
- Merged split always produces exactly one segment (tantivy merge combines all input segments)
- The merged manifest's `segment_row_ranges` has a single entry: `[{ord: 0, offset: 0, num_rows: total}]`
- Source splits' segment mappings are irrelevant post-merge because all rows are restacked

**Validation**: At query time, verify `sum(segment_row_ranges[].num_rows) == total_rows` and
`sum(parquet_files[].num_rows) == total_rows`.

### 19.5 Page Decompression Pipeline (Critical)

**Gap**: The design describes fetching compressed page bytes but doesn't specify the
decompression pipeline in detail.

**Resolution**: arrow-rs handles all decompression internally. We use
`ParquetRecordBatchReaderBuilder` with our `CachedParquetReader` (which implements
`AsyncFileReader`).

Complete pipeline:

```
1. CachedParquetReader.get_bytes(range) → compressed bytes from L2 cache or L3
                         ↓
2. arrow-rs SerializedPageReader → decompresses using column codec
   (SNAPPY, ZSTD, LZ4, GZIP, BROTLI, or UNCOMPRESSED)
                         ↓
3. arrow-rs ArrayReader → decodes into Arrow arrays
   (handles RLE, DELTA, DICTIONARY, PLAIN encodings)
                         ↓
4. Our conversion layer → Arrow arrays to tantivy values
   (RecordBatch columns → HashMap<String, FieldValue>)
```

For document retrieval with surgical page access:

```rust
async fn retrieve_rows_from_parquet(
    reader: CachedParquetReader,
    row_selection: RowSelection,
    projection: ProjectionMask,
) -> Result<RecordBatch> {
    let builder = ParquetRecordBatchReaderBuilder::new_with_options(
        reader,
        ArrowReaderOptions::new(),
    )
    .with_projection(projection)      // Only projected columns decoded
    .with_row_selection(row_selection); // Only pages with target rows fetched

    let mut stream = builder.build()?;
    let mut batches = Vec::new();
    while let Some(batch) = stream.next().await {
        batches.push(batch?);
    }
    arrow::compute::concat_batches(&batches[0].schema(), &batches)
}
```

For fast field transcode (full column):

```rust
async fn transcode_column_from_parquet(
    reader: CachedParquetReader,
    column_name: &str,
) -> Result<RecordBatch> {
    let projection = ProjectionMask::columns(
        reader.metadata().file_metadata().schema_descr(),
        [column_name],
    );

    let builder = ParquetRecordBatchReaderBuilder::new_with_options(
        reader,
        ArrowReaderOptions::new(),
    )
    .with_projection(projection);
    // No row_selection → all pages fetched (correct for aggregations)

    let mut stream = builder.build()?;
    // ... collect all batches
}
```

No manual decompression code is written. arrow-rs is the complete implementation.

### 19.6 OffsetIndex Absence Fallback (Important)

**Gap**: Not all parquet files have an OffsetIndex (it's optional in the parquet spec).
Without it, surgical page-level retrieval is impossible.

**Resolution**:

Detection at split creation:
- When building the manifest, check each parquet file's metadata for OffsetIndex availability
- Record `"has_offset_index": true/false` per parquet file entry

At query time:
- **Files WITH OffsetIndex**: Fetch footer → extract OffsetIndex → cache → surgical page access
- **Files WITHOUT OffsetIndex**: Fall back to full column chunk fetch

```rust
async fn fetch_column_data(
    file_entry: &ParquetFileEntry,
    col_name: &str,
    row_selection: Option<&RowSelection>,
    storage: &Arc<StorageWithPersistentCache>,
) -> Result<Bytes> {
    if file_entry.has_offset_index && row_selection.is_some() {
        // Surgical path: fetch OffsetIndex from footer, then fetch only needed pages
        let offset_index = self.get_or_fetch_offset_index(file_entry, col_name).await?;
        let page_ranges = row_selection.unwrap().scan_ranges(&offset_index);
        self.fetch_page_ranges(file_entry, &page_ranges, storage).await
    } else {
        // Fallback: fetch entire column chunk
        let col_info = file_entry.get_column_info(col_name)?;
        storage.get_slice(&file_entry.resolved_path, col_info.byte_range()).await
    }
}
```

The fallback is still efficient because:
- Column projection is still enforced (only requested columns fetched)
- The full column chunk is cached in L2 after first access
- A warning is logged suggesting `write_page_index = true` when regenerating parquet files

### 19.7 Aggregation Field Discovery (Important)

**Gap**: How does the system know which parquet columns to transcode for aggregations?

**Decision**: Auto-discover by 1:1 name match between parquet columns and tantivy fields.

**Resolution**: Since parquet column names are identical to tantivy/quickwit field names,
the `ParquetAugmentedDirectory` automatically discovers which columns to transcode:

```rust
impl ParquetAugmentedDirectory {
    fn build_augmented_fast_fields(
        &self,
        path: &Path,
        needed_fields: &HashSet<String>,
    ) -> io::Result<OwnedBytes> {
        let mut writer = ColumnarWriter::default();

        for field_name in needed_fields {
            // Check if this field exists in the parquet manifest
            if let Some(col_info) = self.manifest.column_mapping.get(field_name) {
                // Found in parquet — transcode from parquet
                let decoded = self.decode_parquet_column(field_name).await?;
                record_decoded_column(&mut writer, field_name, &decoded);
            } else if let FastFieldMode::Hybrid { .. } = &self.mode {
                // Not in parquet, hybrid mode — try native fast field
                if let Ok(native_col) = self.read_native_column(path, field_name) {
                    record_native_column(&mut writer, field_name, &native_col);
                }
            }
            // If not found anywhere, skip — tantivy handles missing columns gracefully
        }

        let mut buf = Vec::new();
        writer.serialize(self.num_docs, &mut buf)?;
        Ok(OwnedBytes::new(buf))
    }
}
```

No upfront `declareParquetFastFields()` call is needed. The type compatibility is checked
against the `column_mapping` in the manifest which stores both parquet and tantivy types.

### 19.8 Manifest Size Strategy (Important)

**Gap**: A 1000-column, 10000-page parquet file could produce a manifest exceeding 10MB
if page-level OffsetIndex data is embedded.

**Decision**: Column-level metadata only. No page-level data in manifest.

**Resolution**:

The manifest stores per row group per column:
- `byte_range`: `[start, end]` — the column chunk's position in the parquet file
- `compression`: codec name (SNAPPY, ZSTD, etc.)
- `encoding`: encoding name (RLE_DICTIONARY, PLAIN, etc.)

The manifest does NOT store:
- Individual page offsets, sizes, or first_row values
- OffsetIndex data (serialized page location arrays)

At query time, when surgical page-level access is needed:
1. Fetch the parquet footer (last N bytes of the file — typically 1-10KB)
2. Parse the `OffsetIndex` from the footer metadata
3. Cache the parsed `OffsetIndex` in memory (per-file, per-column)
4. Use the cached `OffsetIndex` for all subsequent surgical retrievals

```rust
/// Lazily fetches and caches OffsetIndex for a parquet file's column.
/// The footer is a single small read, and the parsed index is cached for the
/// lifetime of the SplitSearcher instance.
async fn get_or_fetch_offset_index(
    &self,
    file_entry: &ParquetFileEntry,
    col_name: &str,
) -> Result<Vec<PageLocation>> {
    // Check in-memory cache first
    let cache_key = (file_entry.relative_path.clone(), col_name.to_string());
    if let Some(cached) = self.offset_index_cache.get(&cache_key) {
        return Ok(cached.clone());
    }

    // Fetch parquet footer (last 8 bytes gives footer length, then read footer)
    let footer = self.fetch_parquet_footer(file_entry).await?;
    let metadata = parse_parquet_metadata(&footer)?;

    // Extract OffsetIndex for the requested column
    let col_idx = metadata.file_metadata().schema_descr()
        .columns().iter().position(|c| c.name() == col_name)
        .ok_or_else(|| anyhow!("Column '{}' not in parquet file", col_name))?;

    let offset_index = metadata.offset_index()
        .and_then(|oi| oi.get(0))  // row group 0 (adjust for multi-RG)
        .and_then(|rg_oi| rg_oi.get(col_idx))
        .ok_or_else(|| anyhow!("No OffsetIndex for column '{}'", col_name))?;

    let page_locations = offset_index.page_locations().clone();

    // Cache for future use
    self.offset_index_cache.insert(cache_key, page_locations.clone());
    Ok(page_locations)
}
```

Manifest size estimates:
- 10 columns, 10 row groups: ~5KB
- 100 columns, 50 row groups: ~50KB
- 1000 columns, 100 row groups: ~500KB (still manageable)

### 19.9 Crate Dependencies (Important)

**Gap**: New Rust crate dependencies needed for parquet support.

**Resolution**:

```toml
[dependencies]
# Arrow/Parquet ecosystem — pin to version compatible with tantivy's arrow usage
arrow = { version = "53", features = ["prettyprint"] }
parquet = { version = "53", features = ["async", "arrow"] }
arrow-schema = "53"
arrow-array = "53"

# Serialization (serde_json already in use for existing manifest/config)
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# Existing deps (already present):
# futures, tokio, anyhow, bytes
```

**Version compatibility**: arrow-rs v53 should be compatible with tantivy 0.22's existing
arrow dependency. Before implementation, verify with:

```bash
cargo tree -d -p arrow  # Check for duplicate arrow versions
cargo tree -d -p parquet  # Check parquet version conflicts
```

If there's a diamond dependency conflict (tantivy uses a different arrow version), options:
1. Upgrade tantivy's arrow dep to match (preferred)
2. Use `patch` section in Cargo.toml to force a single version
3. Use arrow-rs features selectively to minimize the surface area

The `parquet` crate's `async` feature brings in `tokio` (already used) and `futures`
(already used). No new runtime dependencies.

### 19.10 Complex/Nested Parquet Types (Important)

**Gap**: Parquet supports LIST, MAP, and STRUCT types that don't have direct tantivy
equivalents.

**Decision**: JSON-encode complex types. Index as tantivy JSON fields.

**Resolution**:

| Parquet Type | Arrow Type | tantivy Type | JSON Encoding |
|---|---|---|---|
| `LIST<INT64>` | `List<Int64>` | Json | `[1, 2, 3]` |
| `LIST<STRING>` | `List<Utf8>` | Json | `["a", "b"]` |
| `MAP<STRING,INT64>` | `Map<Utf8,Int64>` | Json | `{"key1": 1, "key2": 2}` |
| `STRUCT{a:INT, b:STR}` | `Struct` | Json | `{"a": 1, "b": "text"}` |
| Nested `LIST<STRUCT{...}>` | `List<Struct>` | Json | `[{"a":1}, {"a":2}]` |

At indexing time:
```rust
fn convert_complex_to_json(column: &ArrayRef, row: usize) -> serde_json::Value {
    match column.data_type() {
        DataType::List(_) => {
            let list = column.as_any().downcast_ref::<ListArray>().unwrap();
            let values = list.value(row);
            arrow_array_to_json(&values)
        }
        DataType::Map(_, _) => {
            let map = column.as_any().downcast_ref::<MapArray>().unwrap();
            let entries = map.value(row);
            arrow_map_to_json(&entries)
        }
        DataType::Struct(_) => {
            let struct_arr = column.as_any().downcast_ref::<StructArray>().unwrap();
            arrow_struct_to_json(struct_arr, row)
        }
        _ => serde_json::Value::Null,
    }
}
```

Schema derivation adds JSON fields for complex types:
```rust
DataType::List(_) | DataType::Map(_, _) | DataType::Struct(_) => {
    let options = JsonObjectOptions::default()
        .set_stored()
        .set_indexing_options(
            TextFieldIndexing::default()
                .set_tokenizer("default")
                .set_index_option(IndexRecordOption::WithFreqsAndPositions)
        );
    builder.add_json_field(name, options);
}
```

This leverages tantivy4java's existing comprehensive JSON field support (see CLAUDE.md).
Queries on complex type fields use tantivy's JSON field query API:
```java
// Query nested values in a LIST or STRUCT column
SplitQuery query = new SplitTermQuery("metadata", "category", "electronics");
```

For fast field transcode: JSON-encoded strings are recorded as `Str` fast fields and can
participate in terms aggregations.

### 19.11 Backwards Compatibility (Important)

**Gap**: How do old and new versions of tantivy4java interact with parquet-enabled splits?

**Resolution**: Fully backwards compatible in both directions.

**Old tantivy4java reading a split WITH parquet manifest:**
- Old code never requests `_parquet_manifest.json` from the bundle
- `BundleDirectory` ignores unknown files (only serves explicitly requested paths)
- The split functions as a normal split:
  - If fast fields exist (disabled/hybrid mode): aggregations work normally
  - If `.store` is a stub: `doc()` returns empty documents (degraded but non-crashing)
  - If parquet-only mode with empty `.fast`: aggregations fail (expected — old code
    doesn't know about parquet mode)

**New tantivy4java reading a split WITHOUT parquet manifest:**
- `SplitSearcher` checks for `_parquet_manifest.json` in the bundle at initialization
- If absent: operates in standard mode, no parquet code paths activated
- All existing functionality unchanged — zero behavioral difference

**Manifest version field:**
- `"version": 1` in the manifest enables future format changes
- Reader validates the version and fails fast with a clear error if unsupported:
  ```rust
  if manifest.version > SUPPORTED_MANIFEST_VERSION {
      return Err(anyhow!(
          "Parquet manifest version {} not supported. Maximum supported: {}. \
           Upgrade tantivy4java to read this split.",
          manifest.version, SUPPORTED_MANIFEST_VERSION
      ));
  }
  ```

### 19.12 Parquet File Staleness Detection

**Gap**: External parquet files can be deleted, replaced, or modified after the split is
created. The split's manifest becomes stale.

**Resolution**: Validate `file_size_bytes` on first access per file.

At query time, when a parquet file is first accessed:
1. Compare the actual file size with the manifest's `file_size_bytes`
2. If mismatch: fail with clear error

```rust
async fn validate_parquet_file(
    file_entry: &ParquetFileEntry,
    storage: &Arc<dyn Storage>,
) -> Result<()> {
    let actual_size = storage.file_num_bytes(
        &Path::new(&file_entry.resolved_path)
    ).await?;

    if actual_size != file_entry.file_size_bytes {
        return Err(anyhow!(
            "Parquet file '{}' has changed since split was created. \
             Expected size: {} bytes, actual size: {} bytes. \
             Re-index from parquet files to create a new split.",
            file_entry.relative_path, file_entry.file_size_bytes, actual_size
        ));
    }
    Ok(())
}
```

Properties:
- **Cheap check**: single `HEAD` request on S3, `stat()` on local filesystem
- **One-time per file per SplitSearcher instance**: result is cached after first check
- **Catches deletions**: file-not-found error is a clear signal
- **Catches rewrites**: size change detects most modification scenarios
- **Doesn't catch same-size rewrites**: rare edge case. For strict validation, an optional
  `etag` or `last_modified` field can be added to the manifest in a future version,
  enabled via `ParquetCompanionConfig.withStrictValidation(true)`

### 19.13 JNI Boundary Changes

**Gap**: New JNI native methods are needed for projected document retrieval and parquet
configuration.

**Resolution**: New native methods for the parquet companion feature set:

**Document retrieval with field projection:**
```java
// Existing: retrieves all stored fields
native long nativeDoc(long searcherPtr, int segmentOrd, int docId);

// New: retrieves only projected fields (parquet or native)
native long nativeDocProjected(
    long searcherPtr, int segmentOrd, int docId, String[] fields
);
```

**Parquet configuration:**
```java
// Set parquet companion config on a cache manager
native void nativeSetParquetConfig(long cacheManagerPtr, String configJson);

// Create SplitSearcher with parquet companion support
native long nativeCreateSplitSearcherWithParquet(
    long cacheManagerPtr, String splitUri, String parquetConfigJson
);
```

**Parquet prewarm and statistics:**
```java
// Prewarm specific parquet columns to L2 cache
native void nativePrewarmParquetColumns(long searcherPtr, String[] columns);

// Prewarm and transcode parquet columns to fast field format
native void nativePrewarmParquetFastFields(long searcherPtr, String[] columns);

// Get parquet retrieval statistics (returns JSON)
native String nativeGetParquetRetrievalStats(long searcherPtr);
```

**JNI data flow for projected doc retrieval:**
```
Java: searcher.doc(address, "title", "price")
  → JNI: nativeDocProjected(ptr, segOrd, docId, ["title", "price"])
    → Rust: check parquet manifest present?
      YES → translate (segOrd, docId) to global parquet row
           → fetch "title" and "price" pages only from parquet
           → decode and return field values as JSON
      NO  → standard tantivy store retrieval
    → JNI: return JSON string of field values
  → Java: parse into Document with only projected fields
```

The `configJson` parameters use JSON serialization to avoid complex JNI struct passing.
This matches the existing pattern used by `MergeConfig` and other tantivy4java config
objects.

### 19.14 Missing Parquet File Recovery

**Gap**: When a split references a parquet file that no longer exists or is inaccessible,
the system needs a defined behavior.

**Decision**: Fail by default, with user-configurable behavior.

**Resolution**:

```java
public enum ParquetMissingFilePolicy {
    FAIL,    // Default: throw error immediately with diagnostic info
    WARN     // Log warning, return partial results (null for parquet fields)
}

ParquetCompanionConfig config = new ParquetCompanionConfig()
    .withTableRoot("s3://data/tables/sales/")
    .withMissingFilePolicy(ParquetMissingFilePolicy.FAIL);  // Default
```

**FAIL mode (default):**
- Any missing/inaccessible parquet file immediately throws an error
- Error message includes: resolved file path, expected size, storage error details,
  and guidance to re-index
- Predictable, debuggable behavior for production

**WARN mode:**
- Missing parquet files logged as warnings with full diagnostic info
- Document retrieval returns `null` for all parquet-backed fields
- Fast field transcode skips unavailable columns (aggregation results may be partial)
- Useful for graceful degradation in analytics workloads where partial results are acceptable

```rust
async fn fetch_parquet_data(
    file_entry: &ParquetFileEntry,
    storage: &Arc<dyn Storage>,
    policy: MissingFilePolicy,
) -> Result<Option<Bytes>> {
    match storage.get_slice(&file_entry.resolved_path, range).await {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) => match policy {
            MissingFilePolicy::Fail => Err(anyhow!(
                "Parquet file '{}' is inaccessible: {}. \
                 Re-index from parquet files to create a new split.",
                file_entry.relative_path, e
            )),
            MissingFilePolicy::Warn => {
                warn!(
                    "PARQUET_WARN: File '{}' unavailable: {}. \
                     Returning null for parquet-backed fields.",
                    file_entry.relative_path, e
                );
                Ok(None)  // Caller interprets None as missing → null fields
            }
        }
    }
}
```

---

## 20. Implementation Plan

### Phase 1: Foundation (Manifest + Basic Retrieval)

1. **Crate dependencies**: Add arrow/parquet crates, verify version compatibility (§19.9)
2. **Manifest format**: Define Rust structs with `segment_row_ranges`, `has_offset_index`,
   complex type support in `column_mapping`, JSON serialization (§4, §19.4, §19.10)
3. **Manifest embedding**: Integrate with SplitPayloadBuilder
4. **Manifest reading**: Extract from split bundle at SplitSearcher creation, version check (§19.11)
5. **Parquet Storage factory**: Create separate Storage instance with parquet credentials
6. **CachedParquetReader**: AsyncFileReader implementation backed by Storage (§19.5)
7. **DocId-to-parquet mapping**: Segment-aware translation via `segment_row_ranges` then
   binary search on file row offsets (§19.4)
8. **Store file stub**: Generate minimal valid `.store` at split creation (§19.2)
9. **Document retrieval routing**: SplitSearcher routes `doc()` to parquet when manifest
   present, with field projection (§19.2, §19.13)
10. **Null value handling**: Skip nulls in indexing and transcode paths (§19.3)
11. **File staleness validation**: Check `file_size_bytes` on first parquet access (§19.12)
12. **Missing file policy**: FAIL/WARN configurable behavior (§19.14)
13. **Java API**: ParquetCompanionConfig, ParquetStorageConfig, ParquetMissingFilePolicy,
    `nativeDocProjected()` JNI method (§17, §19.13)

### Phase 2: Fast Field Modes (Column<T> Adapter)

1. **ParquetAugmentedDirectory**: Custom Directory wrapping BundleDirectory
2. **Page decode pipeline**: arrow-rs `ParquetRecordBatchReaderBuilder` with full
   decompression handling (§19.5). Dictionary pages handled transparently (§19.1)
3. **ColumnarWriter transcode**: Decoded values → tantivy columnar format, nulls skipped (§19.3)
4. **Aggregation field auto-discovery**: Match field names 1:1 between parquet columns and
   tantivy fields, no upfront declaration needed (§19.7)
5. **Mode 1 (Disabled)**: Pass-through to inner directory
6. **Mode 3 (Parquet Only)**: Full transcode from parquet
7. **Mode 2 (Hybrid)**: Merge native numerics + parquet strings
8. **L1 caching**: Cache transcoded bytes per segment
9. **Java API**: FastFieldMode enum, integration with SplitSearcher

### Phase 3: Indexing Pipeline + Merge Support

1. **Schema auto-derivation**: Parquet schema → tantivy schema with mode-aware fast fields,
   complex types (LIST/MAP/STRUCT) mapped to JSON fields (§19.10)
2. **Row iteration**: RecordBatchReader → tantivy Documents (maintaining order),
   segment boundary tracking for `segment_row_ranges` (§19.4)
3. **Split creation**: Index → split conversion with manifest embedding + store stub
4. **OffsetIndex detection**: Record `has_offset_index` per file; fallback path for files
   without OffsetIndex (§19.6)
5. **Lazy OffsetIndex fetch**: Footer fetch + parse + cache on first surgical access (§19.8)
6. **Merge manifest extraction**: Extract `_parquet_manifest.json` in `temp_management.rs`
7. **Merge manifest combination**: `combine_parquet_manifests()` with row_offset adjustment,
   rebuild `segment_row_ranges` for merged single-segment output
8. **Merge validation**: No-deletion check, mode consistency check
9. **Java API**: `QuickwitSplit.createFromParquet()`

### Phase 4: Performance Optimization

1. **Cross-column range coalescing**: Gap-fill merge for nearby page ranges
2. **Parallel page fetch**: Verify try_join_all pattern for parquet pages
3. **Prewarm API**: `preloadParquetColumns()`, `preloadParquetFastFields()`
4. **Batch document retrieval**: Group-by-file optimization
5. **Benchmarks**: Latency and throughput validation

### Phase 5: Testing and Documentation

1. **Unit tests**: Manifest serialization, type mapping, segment-aware DocId mapping,
   null handling, complex type JSON encoding
2. **Integration tests**: End-to-end split creation + search + retrieval
3. **Mode tests**: All three fast field modes with aggregations
4. **Multi-cloud tests**: Separate credentials for split vs parquet
5. **Merge tests**: Manifest preservation, row_offset adjustment, deletion rejection,
   segment_row_ranges rebuild, multi-segment split merge
6. **OffsetIndex tests**: Files with and without OffsetIndex, fallback behavior
7. **Staleness tests**: Modified/deleted parquet file detection
8. **Missing file tests**: FAIL and WARN policy behavior
9. **Backwards compatibility tests**: Old splits without manifest, new splits with old code
10. **Performance tests**: Latency benchmarks for all paths

---

## 21. Design Decisions Log

| # | Decision | Rationale | Date |
|---|----------|-----------|------|
| 1 | Split references 1+ parquet files (not parquet references split) | Splits index more data than a single parquet file contains | Feb 2026 |
| 2 | Auto-derive schema from parquet, skip store fields | Minimize data duplication — parquet IS the store | Feb 2026 |
| 3 | Identity DocId mapping (DocId N = parquet row N) per segment | Zero mapping overhead, simplest implementation; segment-aware via `segment_row_ranges` | Feb 2026 |
| 4 | Manifest embedded in split bundle as `_parquet_manifest.json` | Zero quickwit/tantivy changes; bundle ignores unknown files | Feb 2026 |
| 5 | Relative paths with configurable table root | Supports regional replication across S3 buckets/Azure containers | Feb 2026 |
| 6 | Reuse existing StorageWithPersistentCache for parquet I/O | Zero new cache code; parquet gets L2 coalescing for free | Feb 2026 |
| 7 | Parallel I/O via futures::future::try_join_all | Matches proven quickwit prewarm pattern | Feb 2026 |
| 8 | Cross-column gap-fill coalescing (64KB default threshold) | Reduces S3 requests; extra bytes cached for future use | Feb 2026 |
| 9 | Column-level metadata only in manifest; OffsetIndex fetched from footer on demand | Keeps manifest < 100KB for wide tables; footer fetch is single small read cached in L2 | Feb 2026 |
| 10 | Three fast field modes (disabled/hybrid/parquet-only) | Flexibility: users choose storage-performance tradeoff | Feb 2026 |
| 11 | Directory-level interception for fast field adapter | Zero tantivy/quickwit changes; transparent to all consumers | Feb 2026 |
| 12 | ColumnarWriter transcode (parquet → tantivy format) | Uses tantivy public API; all aggregations work natively | Feb 2026 |
| 13 | Separate credential support for parquet storage | Parquet data often in different bucket/account than splits | Feb 2026 |
| 14 | Hybrid mode as recommended default | Best balance: numerics tiny as native, strings huge savings from parquet | Feb 2026 |
| 15 | Strict column projection — never retrieve all fields by default | Parquet tables may have 100+ columns; fetching all wastes bandwidth/cache/CPU | Feb 2026 |
| 16 | Demand-driven fast field transcode — only transcode needed columns | Same projection principle applies to fast field transcode from parquet | Feb 2026 |
| 17 | Surgical page-level retrieval — only fetch pages containing target rows | RowSelection.scan_ranges() maps DocIds to exact page byte ranges; skip rate 95-99% typical | Feb 2026 |
| 18 | Full-column fetch for fast field transcode (no surgical skip) | Aggregations scan all docs, so all pages are needed — separate code path from doc retrieval | Feb 2026 |
| 19 | Cross-column coalescing with max_gap guard (64KB default) | Prevents pathological merging of distant pages while enabling close-by range optimization | Feb 2026 |
| 20 | Disallow merge of parquet splits with deletions | Identity mapping (DocId=row) breaks with deletions; target workload is append-only data lakes | Feb 2026 |
| 21 | Fail merge if parquet and non-parquet splits are mixed | DocId space inconsistency: some DocIds map to parquet rows, others don't | Feb 2026 |
| 22 | Manifest written as file in temp dir (auto-included by fs scan) | Leverages existing split_creation.rs file collection; zero changes to SplitPayloadBuilder call | Feb 2026 |
| 23 | Validate manifest consistency at query time (row count = num_docs) | Catch corrupted merges early; fail loudly rather than return wrong results | Feb 2026 |
| 24 | Require same fast_field_mode across merged splits | Prevents internally inconsistent manifest where some files expect hybrid, others parquet-only | Feb 2026 |
| 25 | Stub `.store` file + SplitSearcher-level routing for doc retrieval | Tantivy requires `.store` to exist at index open; routing above tantivy avoids internal changes | Feb 2026 |
| 26 | Multi-segment support with `segment_row_ranges` mapping | Supports splits with multiple segments without requiring single-segment enforcement | Feb 2026 |
| 27 | Auto-discover aggregation fields by 1:1 name match | Parquet column names = tantivy field names; no upfront declaration needed | Feb 2026 |
| 28 | Skip null values in ColumnarWriter (tantivy handles absent natively) | No sentinel values or special encoding; tantivy's sparse column support handles it | Feb 2026 |
| 29 | arrow-rs handles all page decompression and dictionary pages | Complete codec support (SNAPPY/ZSTD/LZ4/GZIP); no manual decompression code | Feb 2026 |
| 30 | `has_offset_index` flag per file; fallback to full column chunk | Not all parquet files have OffsetIndex; graceful degradation preserves functionality | Feb 2026 |
| 31 | JSON-encode complex parquet types (LIST/MAP/STRUCT) | Leverages tantivy4java's existing JSON field support; searchable and aggregatable | Feb 2026 |
| 32 | Validate `file_size_bytes` for staleness detection on first access | Cheap check (HEAD/stat); catches deletions and most rewrites; cached per SplitSearcher | Feb 2026 |
| 33 | Configurable missing file policy: FAIL (default) or WARN | Production gets predictable errors; analytics workloads can opt into partial results | Feb 2026 |
| 34 | JNI projected doc retrieval via `nativeDocProjected(ptr, seg, doc, fields[])` | Enforces column projection at JNI boundary; JSON return avoids complex struct passing | Feb 2026 |
| 35 | Manifest version field with forward-compatibility check | Enables future manifest format evolution without breaking older readers | Feb 2026 |
| 36 | Multi-file indexing as primary path (single split, N parquet files) | Avoids merge entirely; schema validated across all files; simpler manifest | Feb 2026 |
| 37 | Schema validation across all parquet files at indexing time | Fail fast on schema mismatches; prevents runtime errors during query | Feb 2026 |

---

*This document is a living artifact. Update it as implementation evolves and new decisions are made.*
