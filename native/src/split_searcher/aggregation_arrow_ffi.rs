// aggregation_arrow_ffi.rs - Convert AggregationResults to Arrow RecordBatch and export via FFI
//
// This module provides zero-copy export of aggregation results as Arrow columnar data
// via the Arrow C Data Interface (FFI). It eliminates per-result JNI overhead by
// converting tantivy's AggregationResults directly to Arrow RecordBatch format.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{
    Array, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};

use crate::memory_pool::{self, MemoryReservation};
use tantivy::aggregation::agg_result::{
    AggregationResult, BucketEntries, BucketEntry, BucketResult, MetricResult,
    RangeBucketEntry,
};
use tantivy::aggregation::Key;

/// Convert a single named AggregationResult to an Arrow RecordBatch.
///
/// Conversion rules:
/// - Stats → 1 row: count:i64, sum:f64, min:f64, max:f64, avg:f64
/// - Count/Cardinality → 1 row: value:i64
/// - Sum/Avg/Min/Max → 1 row: value:f64
/// - Terms → N rows: key:Utf8, doc_count:i64, {sub_agg_name}:f64...
/// - Histogram → N rows: key:f64, doc_count:i64, {sub_agg_name}:f64...
/// - DateHistogram → N rows: key:Timestamp(us), doc_count:i64, {sub_agg_name}:f64...
/// - Range → N rows: key:Utf8, doc_count:i64, from:f64, to:f64, {sub_agg_name}:f64...
///
/// Sub-aggregations: only one level of metric sub-aggs flattened as columns.
pub fn aggregation_result_to_record_batch(
    _name: &str,
    result: &AggregationResult,
    is_date_histogram: bool,
) -> Result<RecordBatch> {
    aggregation_result_to_record_batch_with_hash_resolution(
        _name,
        result,
        is_date_histogram,
        None,
    )
}

/// Convert a single named AggregationResult to an Arrow RecordBatch,
/// resolving U64 hash bucket keys back to original strings using the provided map.
pub fn aggregation_result_to_record_batch_with_hash_resolution(
    _name: &str,
    result: &AggregationResult,
    is_date_histogram: bool,
    hash_resolution_map: Option<&HashMap<u64, String>>,
) -> Result<RecordBatch> {
    match result {
        AggregationResult::MetricResult(metric) => metric_to_record_batch(metric),
        AggregationResult::BucketResult(bucket) => {
            bucket_to_record_batch(bucket, is_date_histogram, hash_resolution_map)
        }
    }
}

/// Export a RecordBatch's columns via Arrow FFI to pre-allocated C struct addresses.
///
/// Returns the number of rows in the batch.
pub fn export_record_batch_ffi(
    batch: &RecordBatch,
    array_addrs: &[i64],
    schema_addrs: &[i64],
) -> Result<usize> {
    let num_cols = batch.num_columns();
    if array_addrs.len() < num_cols || schema_addrs.len() < num_cols {
        anyhow::bail!(
            "Insufficient FFI addresses: need {} columns but got {} array_addrs and {} schema_addrs",
            num_cols,
            array_addrs.len(),
            schema_addrs.len()
        );
    }

    let batch_schema = batch.schema();
    for (i, col) in batch.columns().iter().enumerate() {
        if array_addrs[i] == 0 || schema_addrs[i] == 0 {
            anyhow::bail!(
                "Null FFI address for column {}: array_addr={}, schema_addr={}",
                i,
                array_addrs[i],
                schema_addrs[i]
            );
        }

        let data = col.to_data();
        let array_ptr = array_addrs[i] as *mut FFI_ArrowArray;
        let schema_ptr = schema_addrs[i] as *mut FFI_ArrowSchema;

        let field = batch_schema.field(i);

        unsafe {
            std::ptr::write_unaligned(array_ptr, FFI_ArrowArray::new(&data));
            std::ptr::write_unaligned(
                schema_ptr,
                FFI_ArrowSchema::try_from(field.as_ref())
                    .map_err(|e| {
                        anyhow::anyhow!(
                            "FFI_ArrowSchema conversion failed for column {}: {}",
                            i,
                            e
                        )
                    })?,
            );
        }
    }

    Ok(batch.num_rows())
}

/// Estimate the memory footprint of a RecordBatch in bytes.
///
/// Sums the buffer sizes of all columns. This is a lower bound since it doesn't
/// account for FFI struct overhead, but captures the bulk of the memory.
fn estimate_record_batch_size(batch: &RecordBatch) -> usize {
    batch
        .columns()
        .iter()
        .map(|col| col.get_buffer_memory_size())
        .sum()
}

/// Export a RecordBatch via FFI with memory pool tracking.
///
/// Creates a MemoryReservation for the estimated batch size before export,
/// returning both the row count and the reservation. The caller must hold
/// the reservation alive until the Java side has consumed the FFI data.
pub fn export_record_batch_ffi_tracked(
    batch: &RecordBatch,
    array_addrs: &[i64],
    schema_addrs: &[i64],
) -> Result<(usize, MemoryReservation)> {
    let estimated_size = estimate_record_batch_size(batch);

    // Best-effort: if pool denies, proceed with empty reservation (data still exported)
    let reservation = MemoryReservation::try_new(
        &memory_pool::global_pool(),
        estimated_size,
        "arrow_ffi",
    )
    .unwrap_or_else(|_| MemoryReservation::empty(&memory_pool::global_pool(), "arrow_ffi"));

    let row_count = export_record_batch_ffi(batch, array_addrs, schema_addrs)?;
    Ok((row_count, reservation))
}

/// Return a JSON string describing the Arrow schema for the given aggregation result.
/// Format: {"columns": [{"name": "key", "type": "Utf8"}, ...], "row_count": N}
pub fn aggregation_result_arrow_schema_json(
    _name: &str,
    result: &AggregationResult,
    is_date_histogram: bool,
) -> Result<String> {
    let batch = aggregation_result_to_record_batch(_name, result, is_date_histogram)?;
    let schema = batch.schema();
    let columns: Vec<serde_json::Value> = schema
        .fields()
        .iter()
        .map(|f| {
            serde_json::json!({
                "name": f.name(),
                "type": format!("{:?}", f.data_type()),
            })
        })
        .collect();
    let result = serde_json::json!({
        "columns": columns,
        "row_count": batch.num_rows(),
    });
    Ok(result.to_string())
}

// ---- Metric conversion ----

fn metric_to_record_batch(metric: &MetricResult) -> Result<RecordBatch> {
    match metric {
        MetricResult::Stats(stats) => {
            let schema = Arc::new(Schema::new(vec![
                Field::new("count", DataType::Int64, false),
                Field::new("sum", DataType::Float64, false),
                Field::new("min", DataType::Float64, true),
                Field::new("max", DataType::Float64, true),
                Field::new("avg", DataType::Float64, true),
            ]));
            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![stats.count as i64])),
                    Arc::new(Float64Array::from(vec![stats.sum])),
                    Arc::new(Float64Array::from(vec![stats.min.unwrap_or(f64::NAN)])),
                    Arc::new(Float64Array::from(vec![stats.max.unwrap_or(f64::NAN)])),
                    Arc::new(Float64Array::from(vec![stats.avg.unwrap_or(f64::NAN)])),
                ],
            )
            .context("Failed to create Stats RecordBatch")
        }
        MetricResult::Count(v) | MetricResult::Cardinality(v) => {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                true,
            )]));
            let val = v.value.map(|x| x as i64);
            RecordBatch::try_new(
                schema,
                vec![Arc::new(Int64Array::from(vec![val]))],
            )
            .context("Failed to create Count/Cardinality RecordBatch")
        }
        MetricResult::Average(v)
        | MetricResult::Max(v)
        | MetricResult::Min(v)
        | MetricResult::Sum(v) => {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Float64,
                true,
            )]));
            RecordBatch::try_new(
                schema,
                vec![Arc::new(Float64Array::from(vec![v.value]))],
            )
            .context("Failed to create SingleMetric RecordBatch")
        }
        _ => anyhow::bail!("Unsupported metric type for Arrow conversion"),
    }
}

// ---- Bucket conversion ----

fn bucket_to_record_batch(
    bucket: &BucketResult,
    is_date_histogram: bool,
    hash_resolution_map: Option<&HashMap<u64, String>>,
) -> Result<RecordBatch> {
    match bucket {
        BucketResult::Terms { buckets, .. } => terms_to_record_batch(buckets, hash_resolution_map),
        BucketResult::Histogram { buckets } => {
            if is_date_histogram {
                date_histogram_to_record_batch(buckets, hash_resolution_map)
            } else {
                histogram_to_record_batch(buckets, hash_resolution_map)
            }
        }
        BucketResult::Range { buckets } => range_to_record_batch(buckets, hash_resolution_map),
    }
}

fn terms_to_record_batch(
    buckets: &[BucketEntry],
    hash_resolution_map: Option<&HashMap<u64, String>>,
) -> Result<RecordBatch> {
    // Check if there is a nested bucket sub-aggregation (FR-2: flattening)
    if let Some(nested) = find_nested_bucket_sub_agg(buckets.first().map(|b| &b.sub_aggregation)) {
        return flatten_nested_bucket_terms(buckets, &nested.0, &nested.1, hash_resolution_map);
    }

    let sub_agg_names = collect_metric_sub_agg_names(
        buckets.first().map(|b| &b.sub_aggregation),
    );

    let mut fields = vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("doc_count", DataType::Int64, false),
    ];
    for name in &sub_agg_names {
        fields.push(Field::new(name, DataType::Float64, true));
    }
    let schema = Arc::new(Schema::new(fields));

    let keys: Vec<String> = buckets.iter().map(|b| key_to_string_resolved(&b.key, hash_resolution_map)).collect();
    let doc_counts: Vec<i64> = buckets.iter().map(|b| b.doc_count as i64).collect();

    let mut columns: Vec<Arc<dyn arrow_array::Array>> = vec![
        Arc::new(StringArray::from(keys)),
        Arc::new(Int64Array::from(doc_counts)),
    ];

    for name in &sub_agg_names {
        let values: Vec<Option<f64>> = buckets
            .iter()
            .map(|b| extract_metric_value(&b.sub_aggregation, name))
            .collect();
        columns.push(Arc::new(Float64Array::from(values)));
    }

    RecordBatch::try_new(schema, columns).context("Failed to create Terms RecordBatch")
}

fn histogram_to_record_batch(
    buckets: &BucketEntries<BucketEntry>,
    hash_resolution_map: Option<&HashMap<u64, String>>,
) -> Result<RecordBatch> {
    let entries: Vec<&BucketEntry> = match buckets {
        BucketEntries::Vec(v) => v.iter().collect(),
        BucketEntries::HashMap(m) => m.values().collect(),
    };

    // Check for nested bucket sub-aggregation (FR-2: flattening)
    if let Some(nested) = find_nested_bucket_sub_agg(entries.first().map(|b| &b.sub_aggregation)) {
        return flatten_nested_bucket_histogram(&entries, &nested.0, &nested.1, false, hash_resolution_map);
    }

    let sub_agg_names = collect_metric_sub_agg_names(
        entries.first().map(|b| &b.sub_aggregation),
    );

    let mut fields = vec![
        Field::new("key", DataType::Float64, false),
        Field::new("doc_count", DataType::Int64, false),
    ];
    for name in &sub_agg_names {
        fields.push(Field::new(name, DataType::Float64, true));
    }
    let schema = Arc::new(Schema::new(fields));

    let keys: Vec<f64> = entries.iter().map(|b| key_to_f64(&b.key)).collect();
    let doc_counts: Vec<i64> = entries.iter().map(|b| b.doc_count as i64).collect();

    let mut columns: Vec<Arc<dyn arrow_array::Array>> = vec![
        Arc::new(Float64Array::from(keys)),
        Arc::new(Int64Array::from(doc_counts)),
    ];

    for name in &sub_agg_names {
        let values: Vec<Option<f64>> = entries
            .iter()
            .map(|b| extract_metric_value(&b.sub_aggregation, name))
            .collect();
        columns.push(Arc::new(Float64Array::from(values)));
    }

    RecordBatch::try_new(schema, columns)
        .context("Failed to create Histogram RecordBatch")
}

fn date_histogram_to_record_batch(
    buckets: &BucketEntries<BucketEntry>,
    hash_resolution_map: Option<&HashMap<u64, String>>,
) -> Result<RecordBatch> {
    let entries: Vec<&BucketEntry> = match buckets {
        BucketEntries::Vec(v) => v.iter().collect(),
        BucketEntries::HashMap(m) => m.values().collect(),
    };

    // Check for nested bucket sub-aggregation (FR-2: flattening)
    if let Some(nested) = find_nested_bucket_sub_agg(entries.first().map(|b| &b.sub_aggregation)) {
        return flatten_nested_bucket_histogram(&entries, &nested.0, &nested.1, true, hash_resolution_map);
    }

    let sub_agg_names = collect_metric_sub_agg_names(
        entries.first().map(|b| &b.sub_aggregation),
    );

    let mut fields = vec![
        Field::new(
            "key",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("doc_count", DataType::Int64, false),
    ];
    for name in &sub_agg_names {
        fields.push(Field::new(name, DataType::Float64, true));
    }
    let schema = Arc::new(Schema::new(fields));

    // Keys are millisecond timestamps from tantivy → convert to microseconds for Arrow
    let keys: Vec<i64> = entries
        .iter()
        .map(|b| {
            let ms = key_to_f64(&b.key) as i64;
            ms * 1000 // ms → µs
        })
        .collect();
    let doc_counts: Vec<i64> = entries.iter().map(|b| b.doc_count as i64).collect();

    let mut columns: Vec<Arc<dyn arrow_array::Array>> = vec![
        Arc::new(TimestampMicrosecondArray::from(keys)),
        Arc::new(Int64Array::from(doc_counts)),
    ];

    for name in &sub_agg_names {
        let values: Vec<Option<f64>> = entries
            .iter()
            .map(|b| extract_metric_value(&b.sub_aggregation, name))
            .collect();
        columns.push(Arc::new(Float64Array::from(values)));
    }

    RecordBatch::try_new(schema, columns)
        .context("Failed to create DateHistogram RecordBatch")
}

fn range_to_record_batch(
    buckets: &BucketEntries<RangeBucketEntry>,
    hash_resolution_map: Option<&HashMap<u64, String>>,
) -> Result<RecordBatch> {
    let entries: Vec<&RangeBucketEntry> = match buckets {
        BucketEntries::Vec(v) => v.iter().collect(),
        BucketEntries::HashMap(m) => m.values().collect(),
    };

    // Check for nested bucket sub-aggregation (FR-2: flattening)
    if let Some(nested) = find_nested_bucket_sub_agg(entries.first().map(|b| &b.sub_aggregation)) {
        return flatten_nested_bucket_range(&entries, &nested.0, &nested.1, hash_resolution_map);
    }

    let sub_agg_names = collect_metric_sub_agg_names(
        entries.first().map(|b| &b.sub_aggregation),
    );

    let mut fields = vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("doc_count", DataType::Int64, false),
        Field::new("from", DataType::Float64, true),
        Field::new("to", DataType::Float64, true),
    ];
    for name in &sub_agg_names {
        fields.push(Field::new(name, DataType::Float64, true));
    }
    let schema = Arc::new(Schema::new(fields));

    let keys: Vec<String> = entries.iter().map(|b| key_to_string(&b.key)).collect();
    let doc_counts: Vec<i64> = entries.iter().map(|b| b.doc_count as i64).collect();
    let froms: Vec<Option<f64>> = entries.iter().map(|b| b.from).collect();
    let tos: Vec<Option<f64>> = entries.iter().map(|b| b.to).collect();

    let mut columns: Vec<Arc<dyn arrow_array::Array>> = vec![
        Arc::new(StringArray::from(keys)),
        Arc::new(Int64Array::from(doc_counts)),
        Arc::new(Float64Array::from(froms)),
        Arc::new(Float64Array::from(tos)),
    ];

    for name in &sub_agg_names {
        let values: Vec<Option<f64>> = entries
            .iter()
            .map(|b| extract_metric_value(&b.sub_aggregation, name))
            .collect();
        columns.push(Arc::new(Float64Array::from(values)));
    }

    RecordBatch::try_new(schema, columns).context("Failed to create Range RecordBatch")
}

// ---- FR-2: Nested Bucket Sub-Aggregation Flattening ----
//
// When a bucket aggregation (Terms, Histogram, DateHistogram) has a nested
// Terms bucket sub-aggregation, flatten into cross-product rows:
//   outer_key × inner_buckets → key_0, key_1, doc_count, metric_sub_aggs...

/// Find the first nested bucket sub-aggregation in a bucket entry's sub_aggregation map.
/// Returns (name, BucketResult) if found.
fn find_nested_bucket_sub_agg(
    sub_agg: Option<&tantivy::aggregation::agg_result::AggregationResults>,
) -> Option<(String, BucketResult)> {
    sub_agg.and_then(|aggs| {
        aggs.0.iter().find_map(|(name, result)| match result {
            AggregationResult::BucketResult(bucket) => Some((name.clone(), bucket.clone())),
            _ => None,
        })
    })
}

/// Extract inner BucketEntry list from a BucketResult (Terms only for now).
fn extract_inner_buckets(bucket: &BucketResult) -> Vec<&BucketEntry> {
    match bucket {
        BucketResult::Terms { buckets, .. } => buckets.iter().collect(),
        BucketResult::Histogram { buckets } => match buckets {
            BucketEntries::Vec(v) => v.iter().collect(),
            BucketEntries::HashMap(m) => m.values().collect(),
        },
        BucketResult::Range { .. } => Vec::new(),
    }
}

/// Count the depth of nested bucket sub-aggregations (Terms chains).
/// Returns the total number of key levels (including the leaf bucket level).
fn count_nesting_depth(buckets: &[BucketEntry]) -> usize {
    let first = match buckets.first() {
        Some(b) => b,
        None => return 0,
    };
    match find_nested_bucket_sub_agg(Some(&first.sub_aggregation)) {
        Some((_, ref nested)) => {
            let inner = extract_inner_buckets(nested);
            // Borrow the inner entries to get owned BucketEntry references
            let inner_owned: Vec<BucketEntry> = inner.into_iter().cloned().collect();
            1 + count_nesting_depth(&inner_owned)
        }
        None => 1, // Leaf level (has only metric sub-aggs or nothing)
    }
}

/// Recursively collect all flattened rows from N levels of nested Terms buckets.
/// Each row has N key strings + doc_count + metric sub-agg values.
fn collect_nested_rows(
    buckets: &[BucketEntry],
    prefix_keys: &[String],
    depth: usize,
    metric_names: &[String],
    key_columns: &mut Vec<Vec<String>>,
    doc_count_values: &mut Vec<i64>,
    metric_values: &mut Vec<Vec<Option<f64>>>,
    hash_resolution_map: Option<&HashMap<u64, String>>,
) {
    for bucket in buckets {
        let mut current_keys = prefix_keys.to_vec();
        current_keys.push(key_to_string_resolved(&bucket.key, hash_resolution_map));

        if current_keys.len() == depth {
            // Leaf level: emit a row
            for (col_idx, key_val) in current_keys.iter().enumerate() {
                key_columns[col_idx].push(key_val.clone());
            }
            doc_count_values.push(bucket.doc_count as i64);
            for (i, name) in metric_names.iter().enumerate() {
                metric_values[i].push(extract_metric_value(&bucket.sub_aggregation, name));
            }
        } else {
            // Intermediate level: recurse into nested bucket sub-agg
            let inner_buckets: Vec<&BucketEntry> = bucket
                .sub_aggregation
                .0
                .values()
                .find_map(|r| match r {
                    AggregationResult::BucketResult(b) => Some(extract_inner_buckets(b)),
                    _ => None,
                })
                .unwrap_or_default();

            let inner_owned: Vec<BucketEntry> = inner_buckets.into_iter().cloned().collect();
            collect_nested_rows(
                &inner_owned,
                &current_keys,
                depth,
                metric_names,
                key_columns,
                doc_count_values,
                metric_values,
                hash_resolution_map,
            );
        }
    }
}

/// Find the innermost (leaf) metric sub-aggregation names by walking the nesting chain.
fn find_leaf_metric_names(buckets: &[BucketEntry]) -> Vec<String> {
    let first = match buckets.first() {
        Some(b) => b,
        None => return Vec::new(),
    };
    match find_nested_bucket_sub_agg(Some(&first.sub_aggregation)) {
        Some((_, ref nested)) => {
            let inner = extract_inner_buckets(nested);
            let inner_owned: Vec<BucketEntry> = inner.into_iter().cloned().collect();
            find_leaf_metric_names(&inner_owned)
        }
        None => collect_metric_sub_agg_names(Some(&first.sub_aggregation)),
    }
}

/// Flatten Terms → nested Terms (N levels deep) into cross-product rows.
/// Produces: key_0:Utf8, key_1:Utf8, ..., key_N-1:Utf8, doc_count:Int64, {metric_sub_aggs}:Float64...
fn flatten_nested_bucket_terms(
    outer_buckets: &[BucketEntry],
    _nested_name: &str,
    _nested_template: &BucketResult,
    hash_resolution_map: Option<&HashMap<u64, String>>,
) -> Result<RecordBatch> {
    // Determine total nesting depth (number of key columns)
    let depth = count_nesting_depth(outer_buckets);
    let metric_names = find_leaf_metric_names(outer_buckets);

    // Build schema: key_0, key_1, ..., key_N-1, doc_count, metric_sub_aggs...
    let mut fields: Vec<Field> = (0..depth)
        .map(|i| Field::new(format!("key_{}", i), DataType::Utf8, false))
        .collect();
    fields.push(Field::new("doc_count", DataType::Int64, false));
    for name in &metric_names {
        fields.push(Field::new(name, DataType::Float64, true));
    }
    let schema = Arc::new(Schema::new(fields));

    // Collect rows recursively
    let mut key_columns: Vec<Vec<String>> = (0..depth).map(|_| Vec::new()).collect();
    let mut doc_count_values: Vec<i64> = Vec::new();
    let mut metric_values: Vec<Vec<Option<f64>>> =
        metric_names.iter().map(|_| Vec::new()).collect();

    collect_nested_rows(
        outer_buckets,
        &[],
        depth,
        &metric_names,
        &mut key_columns,
        &mut doc_count_values,
        &mut metric_values,
        hash_resolution_map,
    );

    // Build Arrow columns
    let mut columns: Vec<Arc<dyn arrow_array::Array>> = key_columns
        .into_iter()
        .map(|col| Arc::new(StringArray::from(col)) as Arc<dyn arrow_array::Array>)
        .collect();
    columns.push(Arc::new(Int64Array::from(doc_count_values)));
    for vals in &metric_values {
        columns.push(Arc::new(Float64Array::from(vals.clone())));
    }

    RecordBatch::try_new(schema, columns)
        .context("Failed to create flattened nested Terms RecordBatch")
}

/// Collected inner nested bucket data from a sequence of outer bucket entries.
/// Shared by Histogram, DateHistogram, and Range flatten functions.
struct InnerFlattenedData {
    inner_depth: usize,
    metric_names: Vec<String>,
    inner_key_columns: Vec<Vec<String>>,
    doc_count_values: Vec<i64>,
    metric_values: Vec<Vec<Option<f64>>>,
    /// Number of inner rows produced per outer entry (for replicating outer-specific columns).
    rows_per_outer: Vec<usize>,
}

/// Extract and flatten nested Terms sub-aggregation data from a sequence of outer bucket entries.
///
/// Each outer entry's `sub_aggregation` is scanned for a nested BucketResult, then recursively
/// flattened via `collect_nested_rows`. The caller is responsible for building outer-specific
/// columns (e.g. histogram key, range from/to) using `rows_per_outer` to replicate values.
fn flatten_inner_nested_buckets(
    outer_sub_aggs: &[&tantivy::aggregation::agg_result::AggregationResults],
    hash_resolution_map: Option<&HashMap<u64, String>>,
) -> InnerFlattenedData {
    let first_inner: Vec<BucketEntry> = outer_sub_aggs
        .first()
        .and_then(|agg| {
            agg.0.values().find_map(|r| match r {
                AggregationResult::BucketResult(b) => {
                    Some(extract_inner_buckets(b).into_iter().cloned().collect::<Vec<_>>())
                }
                _ => None,
            })
        })
        .unwrap_or_default();

    let inner_depth = if first_inner.is_empty() { 1 } else { count_nesting_depth(&first_inner) };
    let metric_names = if first_inner.is_empty() { Vec::new() } else { find_leaf_metric_names(&first_inner) };

    let mut inner_key_columns: Vec<Vec<String>> = (0..inner_depth).map(|_| Vec::new()).collect();
    let mut doc_count_values: Vec<i64> = Vec::new();
    let mut metric_values: Vec<Vec<Option<f64>>> = metric_names.iter().map(|_| Vec::new()).collect();
    let mut rows_per_outer: Vec<usize> = Vec::with_capacity(outer_sub_aggs.len());

    for sub_agg in outer_sub_aggs {
        let inner_buckets: Vec<BucketEntry> = sub_agg
            .0
            .values()
            .find_map(|r| match r {
                AggregationResult::BucketResult(b) => {
                    Some(extract_inner_buckets(b).into_iter().cloned().collect::<Vec<_>>())
                }
                _ => None,
            })
            .unwrap_or_default();

        let mut inner_keys: Vec<Vec<String>> = (0..inner_depth).map(|_| Vec::new()).collect();
        let mut inner_docs: Vec<i64> = Vec::new();
        let mut inner_metrics: Vec<Vec<Option<f64>>> = metric_names.iter().map(|_| Vec::new()).collect();

        collect_nested_rows(
            &inner_buckets, &[], inner_depth, &metric_names,
            &mut inner_keys, &mut inner_docs, &mut inner_metrics,
            hash_resolution_map,
        );

        rows_per_outer.push(inner_docs.len());
        for (col_idx, col) in inner_keys.into_iter().enumerate() {
            inner_key_columns[col_idx].extend(col);
        }
        doc_count_values.extend(inner_docs);
        for (i, vals) in inner_metrics.into_iter().enumerate() {
            metric_values[i].extend(vals);
        }
    }

    InnerFlattenedData { inner_depth, metric_names, inner_key_columns, doc_count_values, metric_values, rows_per_outer }
}

/// Build the trailing Arrow columns shared by all flatten functions: inner key columns,
/// doc_count, and metric sub-aggregation columns.
fn build_inner_columns(data: &mut InnerFlattenedData) -> Vec<Arc<dyn arrow_array::Array>> {
    let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::new();
    for col in data.inner_key_columns.drain(..) {
        columns.push(Arc::new(StringArray::from(col)));
    }
    columns.push(Arc::new(Int64Array::from(std::mem::take(&mut data.doc_count_values))));
    for vals in data.metric_values.drain(..) {
        columns.push(Arc::new(Float64Array::from(vals)));
    }
    columns
}

/// Append inner key + doc_count + metric fields to a schema field list.
fn append_inner_fields(fields: &mut Vec<Field>, data: &InnerFlattenedData, key_start_idx: usize) {
    for i in key_start_idx..(key_start_idx + data.inner_depth) {
        fields.push(Field::new(format!("key_{}", i), DataType::Utf8, false));
    }
    fields.push(Field::new("doc_count", DataType::Int64, false));
    for name in &data.metric_names {
        fields.push(Field::new(name, DataType::Float64, true));
    }
}

/// Flatten Histogram/DateHistogram → nested Terms (N levels deep) into cross-product rows.
/// key_0 is Float64|Timestamp(us), remaining key_1..key_N-1 are Utf8 from nested Terms.
fn flatten_nested_bucket_histogram(
    outer_entries: &[&BucketEntry],
    _nested_name: &str,
    _nested_template: &BucketResult,
    is_date_histogram: bool,
    hash_resolution_map: Option<&HashMap<u64, String>>,
) -> Result<RecordBatch> {
    let outer_sub_aggs: Vec<&_> = outer_entries.iter().map(|e| &e.sub_aggregation).collect();
    let mut data = flatten_inner_nested_buckets(&outer_sub_aggs, hash_resolution_map);

    // Build schema
    let key_0_type = if is_date_histogram {
        DataType::Timestamp(TimeUnit::Microsecond, None)
    } else {
        DataType::Float64
    };
    let mut fields = vec![Field::new("key_0", key_0_type, false)];
    append_inner_fields(&mut fields, &data, 1);
    let schema = Arc::new(Schema::new(fields));

    // Build outer key column
    let mut key_0_f64: Vec<f64> = Vec::new();
    let mut key_0_ts: Vec<i64> = Vec::new();
    for (outer, &n) in outer_entries.iter().zip(&data.rows_per_outer) {
        for _ in 0..n {
            if is_date_histogram {
                key_0_ts.push(key_to_f64(&outer.key) as i64 * 1000);
            } else {
                key_0_f64.push(key_to_f64(&outer.key));
            }
        }
    }

    let key_0_col: Arc<dyn arrow_array::Array> = if is_date_histogram {
        Arc::new(TimestampMicrosecondArray::from(key_0_ts))
    } else {
        Arc::new(Float64Array::from(key_0_f64))
    };

    let mut columns = vec![key_0_col];
    columns.extend(build_inner_columns(&mut data));

    RecordBatch::try_new(schema, columns)
        .context("Failed to create flattened nested Histogram RecordBatch")
}

/// Flatten Range → nested Terms (N levels deep) into cross-product rows.
/// key_0 is Utf8 (range bucket key), from:Float64, to:Float64, then key_1..key_N-1 from nested Terms.
fn flatten_nested_bucket_range(
    outer_entries: &[&RangeBucketEntry],
    _nested_name: &str,
    _nested_template: &BucketResult,
    hash_resolution_map: Option<&HashMap<u64, String>>,
) -> Result<RecordBatch> {
    let outer_sub_aggs: Vec<&_> = outer_entries.iter().map(|e| &e.sub_aggregation).collect();
    let mut data = flatten_inner_nested_buckets(&outer_sub_aggs, hash_resolution_map);

    // Build schema: key_0 (range key), from, to, inner keys, doc_count, metrics
    let mut fields = vec![
        Field::new("key_0", DataType::Utf8, false),
        Field::new("from", DataType::Float64, true),
        Field::new("to", DataType::Float64, true),
    ];
    append_inner_fields(&mut fields, &data, 1);
    let schema = Arc::new(Schema::new(fields));

    // Build outer columns
    let mut key_0_values: Vec<String> = Vec::new();
    let mut from_values: Vec<Option<f64>> = Vec::new();
    let mut to_values: Vec<Option<f64>> = Vec::new();
    for (outer, &n) in outer_entries.iter().zip(&data.rows_per_outer) {
        for _ in 0..n {
            key_0_values.push(key_to_string(&outer.key));
            from_values.push(outer.from);
            to_values.push(outer.to);
        }
    }

    let mut columns: Vec<Arc<dyn arrow_array::Array>> = vec![
        Arc::new(StringArray::from(key_0_values)),
        Arc::new(Float64Array::from(from_values)),
        Arc::new(Float64Array::from(to_values)),
    ];
    columns.extend(build_inner_columns(&mut data));

    RecordBatch::try_new(schema, columns)
        .context("Failed to create flattened nested Range RecordBatch")
}

// ---- Helpers ----

fn key_to_string(key: &Key) -> String {
    match key {
        Key::Str(s) => s.clone(),
        Key::I64(v) => v.to_string(),
        Key::U64(v) => v.to_string(),
        Key::F64(v) => v.to_string(),
    }
}

/// Like `key_to_string`, but resolves U64 hash keys back to original string values
/// using the provided hash resolution map (from Phase 3 hash touchup).
fn key_to_string_resolved(key: &Key, hash_resolution_map: Option<&HashMap<u64, String>>) -> String {
    match key {
        Key::U64(v) => {
            if let Some(map) = hash_resolution_map {
                if let Some(resolved) = map.get(v) {
                    return resolved.clone();
                }
            }
            v.to_string()
        }
        _ => key_to_string(key),
    }
}

fn key_to_f64(key: &Key) -> f64 {
    match key {
        Key::F64(v) => *v,
        Key::I64(v) => *v as f64,
        Key::U64(v) => *v as f64,
        Key::Str(s) => s.parse::<f64>().unwrap_or(0.0),
    }
}

/// Collect the names of metric sub-aggregations from the first bucket's sub_aggregation map.
/// Only includes metric results (not nested bucket results).
fn collect_metric_sub_agg_names(
    sub_agg: Option<&tantivy::aggregation::agg_result::AggregationResults>,
) -> Vec<String> {
    match sub_agg {
        Some(aggs) => aggs
            .0
            .iter()
            .filter(|(_, v)| matches!(v, AggregationResult::MetricResult(_)))
            .map(|(k, _)| k.clone())
            .collect(),
        None => Vec::new(),
    }
}

/// Extract a single f64 value from a metric sub-aggregation by name.
fn extract_metric_value(
    sub_agg: &tantivy::aggregation::agg_result::AggregationResults,
    name: &str,
) -> Option<f64> {
    sub_agg.0.get(name).and_then(|r| match r {
        AggregationResult::MetricResult(m) => match m {
            MetricResult::Average(v)
            | MetricResult::Count(v)
            | MetricResult::Max(v)
            | MetricResult::Min(v)
            | MetricResult::Sum(v)
            | MetricResult::Cardinality(v) => v.value,
            MetricResult::Stats(s) => Some(s.sum),
            _ => None,
        },
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::aggregation::agg_result::AggregationResults;
    use tantivy::aggregation::metric::{Stats, SingleMetricResult};

    #[test]
    fn test_stats_to_record_batch() {
        let stats = MetricResult::Stats(Stats {
            count: 100,
            sum: 500.0,
            min: Some(1.0),
            max: Some(10.0),
            avg: Some(5.0),
        });
        let result = AggregationResult::MetricResult(stats);
        let batch =
            aggregation_result_to_record_batch("test_stats", &result, false).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 5);
        assert_eq!(batch.schema().field(0).name(), "count");
        assert_eq!(batch.schema().field(4).name(), "avg");

        let count_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count_col.value(0), 100);
    }

    #[test]
    fn test_count_to_record_batch() {
        let count = MetricResult::Count(SingleMetricResult {
            value: Some(42.0),
        });
        let result = AggregationResult::MetricResult(count);
        let batch =
            aggregation_result_to_record_batch("test_count", &result, false).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 1);
        let val_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(val_col.value(0), 42);
    }

    #[test]
    fn test_terms_to_record_batch() {
        let buckets = vec![
            BucketEntry {
                key: Key::Str("foo".to_string()),
                doc_count: 10,
                key_as_string: None,
                sub_aggregation: AggregationResults::default(),
            },
            BucketEntry {
                key: Key::Str("bar".to_string()),
                doc_count: 5,
                key_as_string: None,
                sub_aggregation: AggregationResults::default(),
            },
        ];
        let bucket_result = BucketResult::Terms {
            buckets,
            sum_other_doc_count: 0,
            doc_count_error_upper_bound: None,
        };
        let result = AggregationResult::BucketResult(bucket_result);
        let batch =
            aggregation_result_to_record_batch("test_terms", &result, false).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2); // key + doc_count

        let key_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(key_col.value(0), "foo");
        assert_eq!(key_col.value(1), "bar");

        let count_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count_col.value(0), 10);
        assert_eq!(count_col.value(1), 5);
    }

    #[test]
    fn test_histogram_to_record_batch() {
        let entries = vec![
            BucketEntry {
                key: Key::F64(0.0),
                doc_count: 3,
                key_as_string: None,
                sub_aggregation: AggregationResults::default(),
            },
            BucketEntry {
                key: Key::F64(10.0),
                doc_count: 7,
                key_as_string: None,
                sub_aggregation: AggregationResults::default(),
            },
        ];
        let bucket_result = BucketResult::Histogram {
            buckets: BucketEntries::Vec(entries),
        };
        let result = AggregationResult::BucketResult(bucket_result);
        let batch =
            aggregation_result_to_record_batch("test_hist", &result, false).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        let key_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(key_col.value(0), 0.0);
        assert_eq!(key_col.value(1), 10.0);
    }

    #[test]
    fn test_empty_terms_to_record_batch() {
        let bucket_result = BucketResult::Terms {
            buckets: vec![],
            sum_other_doc_count: 0,
            doc_count_error_upper_bound: None,
        };
        let result = AggregationResult::BucketResult(bucket_result);
        let batch =
            aggregation_result_to_record_batch("empty", &result, false).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_terms_with_sub_aggs() {
        // Create terms with a sub-aggregation
        let make_sub_agg = |avg_val: f64| -> AggregationResults {
            let map = vec![(
                "avg_price".to_string(),
                AggregationResult::MetricResult(MetricResult::Average(
                    SingleMetricResult {
                        value: Some(avg_val),
                    },
                )),
            )]
            .into_iter()
            .collect();
            AggregationResults(map)
        };

        let buckets = vec![
            BucketEntry {
                key: Key::Str("electronics".to_string()),
                doc_count: 20,
                key_as_string: None,
                sub_aggregation: make_sub_agg(99.5),
            },
            BucketEntry {
                key: Key::Str("books".to_string()),
                doc_count: 15,
                key_as_string: None,
                sub_aggregation: make_sub_agg(12.3),
            },
        ];
        let bucket_result = BucketResult::Terms {
            buckets,
            sum_other_doc_count: 0,
            doc_count_error_upper_bound: None,
        };
        let result = AggregationResult::BucketResult(bucket_result);
        let batch =
            aggregation_result_to_record_batch("test_sub", &result, false).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3); // key + doc_count + avg_price

        assert_eq!(batch.schema().field(2).name(), "avg_price");
        let avg_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!((avg_col.value(0) - 99.5).abs() < 0.01);
        assert!((avg_col.value(1) - 12.3).abs() < 0.01);
    }

    #[test]
    fn test_export_record_batch_ffi_validates_addresses() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            DataType::Int64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();

        // Not enough addresses
        let err = export_record_batch_ffi(&batch, &[], &[]).unwrap_err();
        assert!(err.to_string().contains("Insufficient FFI addresses"));

        // Null address
        let err = export_record_batch_ffi(&batch, &[0], &[0]).unwrap_err();
        assert!(err.to_string().contains("Null FFI address"));
    }

    #[test]
    fn test_nested_terms_flattening() {
        // Outer: Terms("country") → Inner: Terms("city") with sub-agg avg_sales
        let make_inner = |cities: Vec<(&str, u64, f64)>| -> AggregationResults {
            let inner_buckets: Vec<BucketEntry> = cities
                .into_iter()
                .map(|(city, count, avg)| {
                    let sub = vec![(
                        "avg_sales".to_string(),
                        AggregationResult::MetricResult(MetricResult::Average(
                            SingleMetricResult { value: Some(avg) },
                        )),
                    )]
                    .into_iter()
                    .collect();
                    BucketEntry {
                        key: Key::Str(city.to_string()),
                        doc_count: count,
                        key_as_string: None,
                        sub_aggregation: AggregationResults(sub),
                    }
                })
                .collect();
            let inner_result = BucketResult::Terms {
                buckets: inner_buckets,
                sum_other_doc_count: 0,
                doc_count_error_upper_bound: None,
            };
            let map = vec![(
                "city_terms".to_string(),
                AggregationResult::BucketResult(inner_result),
            )]
            .into_iter()
            .collect();
            AggregationResults(map)
        };

        let outer_buckets = vec![
            BucketEntry {
                key: Key::Str("US".to_string()),
                doc_count: 150,
                key_as_string: None,
                sub_aggregation: make_inner(vec![("NYC", 100, 50.0), ("LA", 50, 30.0)]),
            },
            BucketEntry {
                key: Key::Str("UK".to_string()),
                doc_count: 80,
                key_as_string: None,
                sub_aggregation: make_inner(vec![("London", 80, 40.0)]),
            },
        ];

        let result = AggregationResult::BucketResult(BucketResult::Terms {
            buckets: outer_buckets,
            sum_other_doc_count: 0,
            doc_count_error_upper_bound: None,
        });
        let batch = aggregation_result_to_record_batch("nested", &result, false).unwrap();

        // Should flatten to 3 rows: US/NYC, US/LA, UK/London
        assert_eq!(batch.num_rows(), 3);
        // Columns: key_0, key_1, doc_count, avg_sales
        assert_eq!(batch.num_columns(), 4);
        assert_eq!(batch.schema().field(0).name(), "key_0");
        assert_eq!(batch.schema().field(1).name(), "key_1");
        assert_eq!(batch.schema().field(2).name(), "doc_count");
        assert_eq!(batch.schema().field(3).name(), "avg_sales");

        let key0 = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(key0.value(0), "US");
        assert_eq!(key0.value(1), "US");
        assert_eq!(key0.value(2), "UK");

        let key1 = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(key1.value(0), "NYC");
        assert_eq!(key1.value(1), "LA");
        assert_eq!(key1.value(2), "London");

        let counts = batch.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(counts.value(0), 100);
        assert_eq!(counts.value(1), 50);
        assert_eq!(counts.value(2), 80);

        let avg = batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        assert!((avg.value(0) - 50.0).abs() < 0.01);
        assert!((avg.value(1) - 30.0).abs() < 0.01);
        assert!((avg.value(2) - 40.0).abs() < 0.01);
    }

    #[test]
    fn test_nested_3_level_terms_flattening() {
        // 3-level: region → category → quarter with avg_sales
        let make_leaf = |quarter: &str, count: u64, avg: f64| -> BucketEntry {
            let sub = vec![(
                "avg_sales".to_string(),
                AggregationResult::MetricResult(MetricResult::Average(
                    SingleMetricResult { value: Some(avg) },
                )),
            )]
            .into_iter()
            .collect();
            BucketEntry {
                key: Key::Str(quarter.to_string()),
                doc_count: count,
                key_as_string: None,
                sub_aggregation: AggregationResults(sub),
            }
        };

        let make_mid = |categories: Vec<(&str, Vec<BucketEntry>)>| -> AggregationResults {
            let inner_buckets: Vec<BucketEntry> = categories
                .into_iter()
                .map(|(cat, quarters)| {
                    let nested = BucketResult::Terms {
                        buckets: quarters,
                        sum_other_doc_count: 0,
                        doc_count_error_upper_bound: None,
                    };
                    let map = vec![(
                        "quarter_terms".to_string(),
                        AggregationResult::BucketResult(nested),
                    )]
                    .into_iter()
                    .collect();
                    BucketEntry {
                        key: Key::Str(cat.to_string()),
                        doc_count: 0,
                        key_as_string: None,
                        sub_aggregation: AggregationResults(map),
                    }
                })
                .collect();
            let mid_result = BucketResult::Terms {
                buckets: inner_buckets,
                sum_other_doc_count: 0,
                doc_count_error_upper_bound: None,
            };
            let map = vec![(
                "category_terms".to_string(),
                AggregationResult::BucketResult(mid_result),
            )]
            .into_iter()
            .collect();
            AggregationResults(map)
        };

        let outer_buckets = vec![
            BucketEntry {
                key: Key::Str("US".to_string()),
                doc_count: 0,
                key_as_string: None,
                sub_aggregation: make_mid(vec![
                    ("Electronics", vec![make_leaf("Q1", 10, 100.0), make_leaf("Q2", 20, 200.0)]),
                    ("Books", vec![make_leaf("Q1", 5, 50.0)]),
                ]),
            },
            BucketEntry {
                key: Key::Str("UK".to_string()),
                doc_count: 0,
                key_as_string: None,
                sub_aggregation: make_mid(vec![
                    ("Electronics", vec![make_leaf("Q1", 8, 80.0)]),
                ]),
            },
        ];

        let result = AggregationResult::BucketResult(BucketResult::Terms {
            buckets: outer_buckets,
            sum_other_doc_count: 0,
            doc_count_error_upper_bound: None,
        });
        let batch = aggregation_result_to_record_batch("nested3", &result, false).unwrap();

        // Should flatten to 4 rows: US/Electronics/Q1, US/Electronics/Q2, US/Books/Q1, UK/Electronics/Q1
        assert_eq!(batch.num_rows(), 4);
        // Columns: key_0, key_1, key_2, doc_count, avg_sales
        assert_eq!(batch.num_columns(), 5);
        assert_eq!(batch.schema().field(0).name(), "key_0");
        assert_eq!(batch.schema().field(1).name(), "key_1");
        assert_eq!(batch.schema().field(2).name(), "key_2");
        assert_eq!(batch.schema().field(3).name(), "doc_count");
        assert_eq!(batch.schema().field(4).name(), "avg_sales");

        let key0 = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let key1 = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let key2 = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        let counts = batch.column(3).as_any().downcast_ref::<Int64Array>().unwrap();

        // Row 0: US / Electronics / Q1
        assert_eq!(key0.value(0), "US");
        assert_eq!(key1.value(0), "Electronics");
        assert_eq!(key2.value(0), "Q1");
        assert_eq!(counts.value(0), 10);

        // Row 3: UK / Electronics / Q1
        assert_eq!(key0.value(3), "UK");
        assert_eq!(key1.value(3), "Electronics");
        assert_eq!(key2.value(3), "Q1");
        assert_eq!(counts.value(3), 8);
    }

    #[test]
    fn test_schema_json() {
        let stats = MetricResult::Stats(Stats {
            count: 10,
            sum: 50.0,
            min: Some(1.0),
            max: Some(10.0),
            avg: Some(5.0),
        });
        let result = AggregationResult::MetricResult(stats);
        let json = aggregation_result_arrow_schema_json("s", &result, false).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["row_count"], 1);
        let cols = parsed["columns"].as_array().unwrap();
        assert_eq!(cols.len(), 5);
        assert_eq!(cols[0]["name"], "count");
    }

    #[test]
    fn test_date_histogram_nested_terms_hash_resolution() {
        // DateHistogram with 2 time buckets, each containing nested Terms with U64 hash keys.
        // A hash_resolution_map resolves hashes back to strings in the flattened output.
        let make_inner_terms = |keys: Vec<(u64, u64)>| -> AggregationResults {
            let inner_buckets: Vec<BucketEntry> = keys
                .into_iter()
                .map(|(hash, count)| BucketEntry {
                    key: Key::U64(hash),
                    doc_count: count,
                    key_as_string: None,
                    sub_aggregation: AggregationResults::default(),
                })
                .collect();
            let inner_result = BucketResult::Terms {
                buckets: inner_buckets,
                sum_other_doc_count: 0,
                doc_count_error_upper_bound: None,
            };
            let map = vec![(
                "category_terms".to_string(),
                AggregationResult::BucketResult(inner_result),
            )]
            .into_iter()
            .collect();
            AggregationResults(map)
        };

        let outer_buckets = vec![
            BucketEntry {
                key: Key::F64(1704067200000.0), // 2024-01-01 in ms
                doc_count: 30,
                key_as_string: Some("2024-01-01T00:00:00Z".to_string()),
                sub_aggregation: make_inner_terms(vec![(111, 20), (222, 10)]),
            },
            BucketEntry {
                key: Key::F64(1706745600000.0), // 2024-02-01 in ms
                doc_count: 15,
                key_as_string: Some("2024-02-01T00:00:00Z".to_string()),
                sub_aggregation: make_inner_terms(vec![(222, 8), (333, 7)]),
            },
        ];

        let result = AggregationResult::BucketResult(BucketResult::Histogram {
            buckets: BucketEntries::Vec(outer_buckets),
        });

        // Build resolution map: hash → original string
        let mut resolution_map = HashMap::new();
        resolution_map.insert(111, "electronics".to_string());
        resolution_map.insert(222, "books".to_string());
        resolution_map.insert(333, "clothing".to_string());

        let batch = aggregation_result_to_record_batch_with_hash_resolution(
            "date_hist",
            &result,
            true, // is_date_histogram
            Some(&resolution_map),
        )
        .unwrap();

        // Flattened: 4 rows (2 outer x 2 inner each)
        assert_eq!(batch.num_rows(), 4);
        // Columns: key_0 (timestamp), key_1 (resolved string), doc_count
        assert_eq!(batch.num_columns(), 3);
        assert_eq!(batch.schema().field(0).name(), "key_0");
        assert_eq!(batch.schema().field(1).name(), "key_1");
        assert_eq!(batch.schema().field(2).name(), "doc_count");

        // key_1 should be resolved strings, not numeric hashes
        let key1 = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(key1.value(0), "electronics");
        assert_eq!(key1.value(1), "books");
        assert_eq!(key1.value(2), "books");
        assert_eq!(key1.value(3), "clothing");
    }

    #[test]
    fn test_range_nested_terms_hash_resolution() {
        // Range with 2 range buckets, each containing nested Terms with U64 hash keys.
        // Verifies that the Arrow FFI path resolves hashes back to strings for Range aggregations.
        let make_inner_terms = |keys: Vec<(u64, u64)>| -> AggregationResults {
            let inner_buckets: Vec<BucketEntry> = keys
                .into_iter()
                .map(|(hash, count)| BucketEntry {
                    key: Key::U64(hash),
                    doc_count: count,
                    key_as_string: None,
                    sub_aggregation: AggregationResults::default(),
                })
                .collect();
            let inner_result = BucketResult::Terms {
                buckets: inner_buckets,
                sum_other_doc_count: 0,
                doc_count_error_upper_bound: None,
            };
            let map = vec![(
                "severity_terms".to_string(),
                AggregationResult::BucketResult(inner_result),
            )]
            .into_iter()
            .collect();
            AggregationResults(map)
        };

        let range_entries = vec![
            RangeBucketEntry {
                key: Key::Str("*-50".to_string()),
                doc_count: 30,
                sub_aggregation: make_inner_terms(vec![(111, 20), (222, 10)]),
                from: None,
                to: Some(50.0),
                from_as_string: None,
                to_as_string: None,
            },
            RangeBucketEntry {
                key: Key::Str("50-*".to_string()),
                doc_count: 25,
                sub_aggregation: make_inner_terms(vec![(222, 15), (333, 10)]),
                from: Some(50.0),
                to: None,
                from_as_string: None,
                to_as_string: None,
            },
        ];

        let result = AggregationResult::BucketResult(BucketResult::Range {
            buckets: BucketEntries::Vec(range_entries),
        });

        // Build resolution map: hash → original string
        let mut resolution_map = HashMap::new();
        resolution_map.insert(111, "INFO".to_string());
        resolution_map.insert(222, "WARN".to_string());
        resolution_map.insert(333, "ERROR".to_string());

        let batch = aggregation_result_to_record_batch_with_hash_resolution(
            "range_agg",
            &result,
            false,
            Some(&resolution_map),
        )
        .unwrap();

        // Flattened: 4 rows (2 outer x 2 inner each)
        assert_eq!(batch.num_rows(), 4);
        // Columns: key_0 (range key), from, to, key_1 (resolved string), doc_count
        assert_eq!(batch.num_columns(), 5);
        assert_eq!(batch.schema().field(0).name(), "key_0");
        assert_eq!(batch.schema().field(1).name(), "from");
        assert_eq!(batch.schema().field(2).name(), "to");
        assert_eq!(batch.schema().field(3).name(), "key_1");
        assert_eq!(batch.schema().field(4).name(), "doc_count");

        // key_0 should be the range bucket keys
        let key0 = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(key0.value(0), "*-50");
        assert_eq!(key0.value(1), "*-50");
        assert_eq!(key0.value(2), "50-*");
        assert_eq!(key0.value(3), "50-*");

        // key_1 should be resolved strings, not numeric hashes
        let key1 = batch.column(3).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(key1.value(0), "INFO");
        assert_eq!(key1.value(1), "WARN");
        assert_eq!(key1.value(2), "WARN");
        assert_eq!(key1.value(3), "ERROR");

        // doc_count values
        let doc_counts = batch.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(doc_counts.value(0), 20); // *-50, INFO
        assert_eq!(doc_counts.value(1), 10); // *-50, WARN
        assert_eq!(doc_counts.value(2), 15); // 50-*, WARN
        assert_eq!(doc_counts.value(3), 10); // 50-*, ERROR

        // from/to values
        let from_col = batch.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
        let to_col = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        assert!(from_col.is_null(0)); // *-50 has no from
        assert!((to_col.value(0) - 50.0).abs() < 0.01);
        assert!((from_col.value(2) - 50.0).abs() < 0.01);
        assert!(to_col.is_null(2)); // 50-* has no to
    }
}
