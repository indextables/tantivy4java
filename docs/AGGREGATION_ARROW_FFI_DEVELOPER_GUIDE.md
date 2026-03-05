# Aggregation Arrow FFI Export — Developer Guide

## Overview

tantivy4java provides Arrow FFI export for aggregation results, enabling zero-copy transfer of aggregation data from the native Rust layer to JVM consumers as Arrow columnar arrays. This eliminates the per-result JNI overhead of extracting aggregation values one at a time.

Two aggregation result paths are available:

| Path | Method | Format | Use Case |
|------|--------|--------|----------|
| **Object-based** | `searchResult.getAggregation()` | Java objects per bucket | Simple consumers, small result sets |
| **Columnar (Arrow FFI)** | `aggregateArrowFfi()` | Arrow C Data Interface | High-throughput consumers (Spark, analytics), large bucket counts |

The Arrow FFI path also supports **multi-split aggregation merge**: intermediate results from multiple splits are merged entirely in native code before export, avoiding O(N) JNI round-trips.

## How It Works

```
Java allocates C structs (ArrowArray + ArrowSchema per column)
    → passes memory addresses as long[] to JNI
    → Rust executes search with aggregations (limit=0)
    → Rust deserializes intermediate aggregation results
    → (multi-split) Rust merges intermediates via merge_fruits()
    → Rust finalizes into AggregationResults
    → Rust converts target aggregation to Arrow RecordBatch
    → Rust writes FFI structs to pre-allocated addresses
    → Java imports column data from native memory (zero-copy)
```

### Detailed Flow

1. **Schema query** — Call `getAggregationArrowSchema()` to discover column names, types, and row count before allocating FFI memory.
2. **Allocate C structs** — Pre-allocate `ArrowArray` and `ArrowSchema` structs (one per column, ~200 bytes each).
3. **Execute aggregation** — Call `aggregateArrowFfi()` (single-split) or `multiSplitAggregateArrowFfi()` (multi-split).
4. **Import vectors** — Reconstruct Arrow `FieldVector` objects from the FFI struct addresses. No data is copied.
5. **Read data** — Access column data directly from native memory.

## Arrow Schema per Aggregation Type

| Aggregation Type | Arrow Columns | Row Count |
|---|---|---|
| **Stats** | `count:Int64, sum:Float64, min:Float64, max:Float64, avg:Float64` | 1 |
| **Count / Cardinality** | `value:Int64` | 1 |
| **Sum / Avg / Min / Max** | `value:Float64` | 1 |
| **Terms** | `key:Utf8, doc_count:Int64, {sub_agg}:Float64...` | N buckets |
| **Histogram** | `key:Float64, doc_count:Int64, {sub_agg}:Float64...` | N buckets |
| **DateHistogram** | `key:Timestamp(us), doc_count:Int64, {sub_agg}:Float64...` | N buckets |
| **Range** | `key:Utf8, doc_count:Int64, from:Float64, to:Float64, {sub_agg}:Float64...` | N ranges |

**Sub-aggregations:** One level of metric sub-aggregations is flattened as additional Float64 columns.

### Nested Bucket Sub-Aggregation Flattening (FR-2)

When a bucket aggregation has a **nested Terms bucket sub-aggregation**, the results are automatically flattened into cross-product rows. This supports SQL-style multi-column GROUP BY queries.

| Outer Aggregation | Nested Inner | Arrow Columns | Row Count |
|---|---|---|---|
| **Terms → Terms** | `key_0:Utf8, key_1:Utf8, doc_count:Int64, {metric_sub_aggs}:Float64...` | outer × inner |
| **DateHistogram → Terms** | `key_0:Timestamp(us), key_1:Utf8, doc_count:Int64, {metric_sub_aggs}:Float64...` | outer × inner |
| **Histogram → Terms** | `key_0:Float64, key_1:Utf8, doc_count:Int64, {metric_sub_aggs}:Float64...` | outer × inner |

**Example:** `GROUP BY status, category` with `AVG(score)`:
```
Aggregation JSON: {"status_terms":{"terms":{"field":"status","size":100},
  "aggs":{"category_terms":{"terms":{"field":"category","size":100},
    "aggs":{"avg_score":{"avg":{"field":"score"}}}}}}}

Arrow output (flattened cross-product):
| key_0 (Utf8) | key_1 (Utf8) | doc_count (Int64) | avg_score (Float64) |
|--------------|--------------|-------------------|---------------------|
| ok           | A            | 2                 | 80.0                |
| ok           | B            | 1                 | 95.0                |
| error        | A            | 1                 | 90.0                |
| error        | B            | 1                 | 60.0                |
```

**Scope:** Exactly one level of nested Terms sub-aggregation within Terms, Histogram, or DateHistogram. Deeper nesting is not supported — use the object-based API for those cases.

## API Reference

### SplitSearcher — Single-Split

```java
// Step 1: Query schema to determine column count
String schemaJson = searcher.getAggregationArrowSchema(
    queryAstJson,  // Quickwit QueryAst JSON, e.g. {"type":"match_all"}
    aggName,       // name of the aggregation to export
    aggJson        // full aggregation request JSON
);
// Returns: {"columns":[{"name":"key","type":"Utf8"},...],"row_count":2}

// Step 2: Allocate FFI memory (one ArrowArray + ArrowSchema per column)
int numCols = parseColumnCount(schemaJson);
long[] arrayAddrs = new long[numCols];
long[] schemaAddrs = new long[numCols];
for (int i = 0; i < numCols; i++) {
    arrayAddrs[i] = allocateNativeMemory(256);  // >= sizeof(ArrowArray)
    schemaAddrs[i] = allocateNativeMemory(256); // >= sizeof(ArrowSchema)
}

// Step 3: Execute aggregation and export via FFI
int rowCount = searcher.aggregateArrowFfi(
    queryAstJson, aggName, aggJson,
    arrayAddrs, schemaAddrs
);

// Step 4: Import Arrow vectors from FFI addresses
// (use your Arrow consumer library to import from the C Data Interface)
```

### SplitCacheManager — Multi-Split Merge

```java
// Merge aggregations across multiple splits in native code
List<SplitSearcher> searchers = Arrays.asList(searcher1, searcher2, searcher3);

int rowCount = cacheManager.multiSplitAggregateArrowFfi(
    searchers,
    queryAstJson,  // same query applied to all splits
    aggName,       // aggregation name to export
    aggJson,       // aggregation request JSON
    arrayAddrs,    // pre-allocated ArrowArray addresses
    schemaAddrs    // pre-allocated ArrowSchema addresses
);
```

The multi-split path:
1. Searches each split with `limit=0` (aggregation-only, no hits)
2. Collects intermediate aggregation result bytes from each split
3. Deserializes via `postcard::from_bytes`
4. Merges all intermediates via `IntermediateAggregationResults::merge_fruits()`
5. Finalizes into `AggregationResults`
6. Converts to Arrow and exports via FFI

This is significantly faster than the alternative of running N separate aggregations and merging in Java, because:
- Only 1 JNI crossing instead of O(N * buckets)
- Intermediate merge happens in native code with zero serialization overhead
- Final result is exported as a single Arrow RecordBatch

## Complete Example

### Terms Aggregation with Sub-Aggregation

```java
// Setup
SplitCacheManager.CacheConfig config = new SplitCacheManager.CacheConfig("my-cache");
SplitCacheManager cacheManager = SplitCacheManager.getInstance(config);
SplitSearcher searcher = cacheManager.createSplitSearcher(splitUri, metadata);

// Define aggregation: terms on "status" with avg sub-agg on "score"
String queryAst = "{\"type\":\"match_all\"}";
String aggJson = "{\"status_terms\":{\"terms\":{\"field\":\"status\",\"size\":100}," +
    "\"aggs\":{\"avg_score\":{\"avg\":{\"field\":\"score\"}}}}}";

// Step 1: Get schema
String schemaJson = searcher.getAggregationArrowSchema(queryAst, "status_terms", aggJson);
// schemaJson: {"columns":[
//   {"name":"key","type":"Utf8"},
//   {"name":"doc_count","type":"Int64"},
//   {"name":"avg_score","type":"Float64"}
// ],"row_count":2}

// Step 2: Allocate FFI memory
int numCols = 3; // key, doc_count, avg_score
long[] arrayAddrs = new long[numCols];
long[] schemaAddrs = new long[numCols];
// ... allocate 256 bytes per address ...

// Step 3: Execute
int rowCount = searcher.aggregateArrowFfi(
    queryAst, "status_terms", aggJson, arrayAddrs, schemaAddrs);
// rowCount = 2 (one row per unique status value)

// Step 4: Import Arrow vectors
// Column 0: key (Utf8) — ["ok", "error"]
// Column 1: doc_count (Int64) — [3, 2]
// Column 2: avg_score (Float64) — [85.0, 75.0]
```

### Multi-Split Stats Merge

```java
List<SplitSearcher> searchers = Arrays.asList(searcher1, searcher2);
String queryAst = "{\"type\":\"match_all\"}";
String aggJson = "{\"score_stats\":{\"stats\":{\"field\":\"score\"}}}";

int numCols = 5; // count, sum, min, max, avg
long[] arrayAddrs = new long[numCols];
long[] schemaAddrs = new long[numCols];
// ... allocate ...

int rowCount = cacheManager.multiSplitAggregateArrowFfi(
    searchers, queryAst, "score_stats", aggJson, arrayAddrs, schemaAddrs);
// rowCount = 1 (stats always produce exactly 1 row)

// Column 0: count (Int64) — merged count across all splits
// Column 1: sum (Float64) — merged sum
// Column 2: min (Float64) — global minimum
// Column 3: max (Float64) — global maximum
// Column 4: avg (Float64) — weighted average (sum/count)
```

## Architecture

### File Layout

| File | Purpose |
|------|---------|
| `native/src/split_searcher/aggregation_arrow_ffi.rs` | Core conversion: `AggregationResult` → Arrow `RecordBatch` |
| `native/src/split_searcher/jni_agg_arrow.rs` | JNI entry points: single-split, multi-split, schema query, test read-back helper |
| `native/src/split_searcher/mod.rs` | Module registration |
| `SplitSearcher.java` | `getAggregationArrowSchema()`, `aggregateArrowFfi()` |
| `SplitCacheManager.java` | `multiSplitAggregateArrowFfi()` |
| `AggregationArrowFfiTest.java` | Integration tests (14 tests) |

### Rust Conversion Pipeline

```
AggregationResult (tantivy)
    ├── MetricResult
    │   ├── Stats → 1-row RecordBatch (count, sum, min, max, avg)
    │   ├── Count/Cardinality → 1-row RecordBatch (value: i64)
    │   └── Sum/Avg/Min/Max → 1-row RecordBatch (value: f64)
    └── BucketResult
        ├── Terms → N-row RecordBatch (key: Utf8, doc_count: i64, sub_aggs...)
        │   └── (nested Terms) → cross-product RecordBatch (key_0: Utf8, key_1: Utf8, doc_count: i64, sub_aggs...)
        ├── Histogram → N-row RecordBatch (key: f64, doc_count: i64, sub_aggs...)
        │   └── (nested Terms) → cross-product RecordBatch (key_0: f64, key_1: Utf8, doc_count: i64, sub_aggs...)
        ├── DateHistogram → N-row RecordBatch (key: Timestamp(us), doc_count: i64, sub_aggs...)
        │   └── (nested Terms) → cross-product RecordBatch (key_0: Timestamp(us), key_1: Utf8, doc_count: i64, sub_aggs...)
        └── Range → N-row RecordBatch (key: Utf8, doc_count: i64, from: f64, to: f64, sub_aggs...)
```

### FFI Export

Each column is exported via `std::ptr::write_unaligned` to pre-allocated C struct addresses:

```rust
unsafe {
    std::ptr::write_unaligned(array_ptr, FFI_ArrowArray::new(&data));
    std::ptr::write_unaligned(schema_ptr, FFI_ArrowSchema::try_from(field)?);
}
```

This matches the pattern used by `docBatchArrowFfi()` in `arrow_ffi_export.rs`.

## Query Format

The `queryAstJson` parameter uses Quickwit's QueryAst JSON format with **snake_case** type tags:

```json
{"type": "match_all"}
{"type": "term", "field": "status", "value": "ok"}
{"type": "bool", "must": [{"type": "term", "field": "status", "value": "ok"}]}
```

The `aggJson` parameter uses Elasticsearch-compatible aggregation JSON:

```json
{
  "my_terms": {
    "terms": {"field": "status", "size": 100},
    "aggs": {
      "avg_score": {"avg": {"field": "score"}}
    }
  }
}
```

## Nested Bucket Flattening Example

### Multi-Column GROUP BY via Nested Terms

```java
// GROUP BY status, category with AVG(score)
String queryAst = "{\"type\":\"match_all\"}";
String aggJson = "{\"status_terms\":{\"terms\":{\"field\":\"status\",\"size\":100}," +
    "\"aggs\":{\"category_terms\":{\"terms\":{\"field\":\"category\",\"size\":100}," +
    "\"aggs\":{\"avg_score\":{\"avg\":{\"field\":\"score\"}}}}}}}";

// Schema query returns the flattened structure
String schemaJson = searcher.getAggregationArrowSchema(queryAst, "status_terms", aggJson);
// {"columns":[
//   {"name":"key_0","type":"Utf8"},       // outer key (status)
//   {"name":"key_1","type":"Utf8"},       // inner key (category)
//   {"name":"doc_count","type":"Int64"},  // inner bucket doc count
//   {"name":"avg_score","type":"Float64"} // metric sub-agg
// ],"row_count":4}

int numCols = 4;
long[] arrayAddrs = new long[numCols];
long[] schemaAddrs = new long[numCols];
// ... allocate ...

int rowCount = searcher.aggregateArrowFfi(
    queryAst, "status_terms", aggJson, arrayAddrs, schemaAddrs);
// rowCount = 4 (cross-product of 2 statuses × 2 categories)

// Column 0: key_0 (Utf8) — ["ok", "ok", "error", "error"]
// Column 1: key_1 (Utf8) — ["A", "B", "A", "B"]
// Column 2: doc_count (Int64) — [2, 1, 1, 1]
// Column 3: avg_score (Float64) — [80.0, 95.0, 90.0, 60.0]
```

### DateHistogram × Terms (Time-Series GROUP BY)

```java
// GROUP BY date_histogram(timestamp, '30d'), status
String aggJson = "{\"ts_hist\":{\"date_histogram\":{\"field\":\"timestamp\"," +
    "\"fixed_interval\":\"30d\"}," +
    "\"aggs\":{\"status_terms\":{\"terms\":{\"field\":\"status\",\"size\":100}}}}}";

int rowCount = searcher.aggregateArrowFfi(
    queryAst, "ts_hist", aggJson, arrayAddrs, schemaAddrs);

// Column 0: key_0 (Timestamp(us)) — [2024-01-01, 2024-01-01, 2024-02-01, ...]
// Column 1: key_1 (Utf8) — ["ok", "error", "ok", ...]
// Column 2: doc_count (Int64) — [3, 0, 0, ...]
```

## Limitations

- **Metric sub-aggregations**: One level of metric sub-aggs flattened as Float64 columns.
- **Nested bucket flattening**: One level of nested Terms sub-agg is flattened into cross-product rows. Deeper nesting is not supported in Arrow output.
- **Range aggregation**: Not implemented in the native split search layer. Returns empty results.
- **Fast fields required**: Terms/histogram aggregations require the target field to have `fast=true` in the schema.
- **Zero Arrow Java dependency**: tantivy4java passes raw `long[]` addresses through JNI. The consumer must provide its own Arrow library for importing FFI structs.

## Testing

Run the integration tests:

```bash
mvn test -pl . -Dtest=AggregationArrowFfiTest
```

Run the Rust unit tests:

```bash
cd native && cargo test --lib aggregation_arrow_ffi
```

### Test Coverage (14 Java integration tests + 9 Rust unit tests)

| Test | What It Validates |
|------|-------------------|
| `testSchemaQueryTerms` | Schema JSON for terms agg: key + doc_count columns, row count |
| `testSchemaQueryStats` | Schema JSON for stats agg: 5 metric columns, 1 row |
| `testSchemaQueryHistogram` | Schema JSON for histogram agg: key + doc_count, bucket count |
| `testSchemaQueryTermsWithSubAgg` | Schema JSON includes flattened sub-agg column name |
| `testSingleSplitTermsFfi` | FFI round-trip: validates key values ("ok", "error") and doc_counts (3, 2) |
| `testSingleSplitStatsFfi` | FFI round-trip: validates count=5, sum=405, min=60, max=95, avg=81 |
| `testSingleSplitHistogramFfi` | FFI round-trip: validates bucket keys and total doc count |
| `testSingleSplitTermsWithSubAggFfi` | FFI round-trip: validates flattened avg_score sub-agg values (ok=85.0, error=75.0) |
| `testSingleSplitDateHistogramFfi` | FFI round-trip: validates Timestamp column type and doc count across monthly buckets |
| `testEmptyResult` | Empty query returns 0 rows gracefully |
| `testMultiSplitTermsMerge` | Multi-split merge: validates merged doc_counts (ok=4, error=4 from 3+1, 2+2) |
| `testMultiSplitStatsMerge` | Multi-split merge: validates merged count=7, sum=495, min=40, max=95, avg=70.71 |
| `testNestedTermsFlattening` | FR-2: Terms→Terms cross-product flattening with metric sub-agg validation |
| `testNestedDateHistogramTermsFlattening` | FR-2: DateHistogram→Terms cross-product with Timestamp key_0 type |

### Test Read-Back Helper

Tests use a native `nativeReadAggArrowColumnsAsJson()` helper that imports the exported FFI structs back into Arrow arrays and returns column data as JSON. This validates the complete FFI round-trip without requiring Arrow Java as a dependency.
