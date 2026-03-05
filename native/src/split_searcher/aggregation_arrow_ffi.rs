// aggregation_arrow_ffi.rs - Convert AggregationResults to Arrow RecordBatch and export via FFI
//
// This module provides zero-copy export of aggregation results as Arrow columnar data
// via the Arrow C Data Interface (FFI). It eliminates per-result JNI overhead by
// converting tantivy's AggregationResults directly to Arrow RecordBatch format.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{
    Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
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
    match result {
        AggregationResult::MetricResult(metric) => metric_to_record_batch(metric),
        AggregationResult::BucketResult(bucket) => {
            bucket_to_record_batch(bucket, is_date_histogram)
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
) -> Result<RecordBatch> {
    match bucket {
        BucketResult::Terms { buckets, .. } => terms_to_record_batch(buckets),
        BucketResult::Histogram { buckets } => {
            if is_date_histogram {
                date_histogram_to_record_batch(buckets)
            } else {
                histogram_to_record_batch(buckets)
            }
        }
        BucketResult::Range { buckets } => range_to_record_batch(buckets),
    }
}

fn terms_to_record_batch(buckets: &[BucketEntry]) -> Result<RecordBatch> {
    // Check if there is a nested bucket sub-aggregation (FR-2: flattening)
    if let Some(nested) = find_nested_bucket_sub_agg(buckets.first().map(|b| &b.sub_aggregation)) {
        return flatten_nested_bucket_terms(buckets, &nested.0, &nested.1);
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

    let keys: Vec<String> = buckets.iter().map(|b| key_to_string(&b.key)).collect();
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

fn histogram_to_record_batch(buckets: &BucketEntries<BucketEntry>) -> Result<RecordBatch> {
    let entries: Vec<&BucketEntry> = match buckets {
        BucketEntries::Vec(v) => v.iter().collect(),
        BucketEntries::HashMap(m) => m.values().collect(),
    };

    // Check for nested bucket sub-aggregation (FR-2: flattening)
    if let Some(nested) = find_nested_bucket_sub_agg(entries.first().map(|b| &b.sub_aggregation)) {
        return flatten_nested_bucket_histogram(&entries, &nested.0, &nested.1, false);
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
) -> Result<RecordBatch> {
    let entries: Vec<&BucketEntry> = match buckets {
        BucketEntries::Vec(v) => v.iter().collect(),
        BucketEntries::HashMap(m) => m.values().collect(),
    };

    // Check for nested bucket sub-aggregation (FR-2: flattening)
    if let Some(nested) = find_nested_bucket_sub_agg(entries.first().map(|b| &b.sub_aggregation)) {
        return flatten_nested_bucket_histogram(&entries, &nested.0, &nested.1, true);
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
) -> Result<RecordBatch> {
    let entries: Vec<&RangeBucketEntry> = match buckets {
        BucketEntries::Vec(v) => v.iter().collect(),
        BucketEntries::HashMap(m) => m.values().collect(),
    };

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

/// Flatten Terms → nested Terms into cross-product rows.
/// Produces: key_0:Utf8, key_1:Utf8, doc_count:Int64, {metric_sub_aggs}:Float64...
fn flatten_nested_bucket_terms(
    outer_buckets: &[BucketEntry],
    _nested_name: &str,
    nested_template: &BucketResult,
) -> Result<RecordBatch> {
    // Get inner metric sub-agg names from the innermost bucket
    let inner_metric_names = match nested_template {
        BucketResult::Terms { buckets, .. } => {
            collect_metric_sub_agg_names(buckets.first().map(|b| &b.sub_aggregation))
        }
        _ => Vec::new(),
    };

    let mut fields = vec![
        Field::new("key_0", DataType::Utf8, false),
        Field::new("key_1", DataType::Utf8, false),
        Field::new("doc_count", DataType::Int64, false),
    ];
    for name in &inner_metric_names {
        fields.push(Field::new(name, DataType::Float64, true));
    }
    let schema = Arc::new(Schema::new(fields));

    let mut key_0_values: Vec<String> = Vec::new();
    let mut key_1_values: Vec<String> = Vec::new();
    let mut doc_count_values: Vec<i64> = Vec::new();
    let mut metric_values: Vec<Vec<Option<f64>>> =
        inner_metric_names.iter().map(|_| Vec::new()).collect();

    for outer in outer_buckets {
        let outer_key = key_to_string(&outer.key);

        // Find the nested bucket result in this outer bucket's sub_aggregation
        let inner_buckets: Vec<&BucketEntry> = outer
            .sub_aggregation
            .0
            .values()
            .find_map(|r| match r {
                AggregationResult::BucketResult(b) => Some(extract_inner_buckets(b)),
                _ => None,
            })
            .unwrap_or_default();

        for inner in &inner_buckets {
            key_0_values.push(outer_key.clone());
            key_1_values.push(key_to_string(&inner.key));
            doc_count_values.push(inner.doc_count as i64);

            for (i, name) in inner_metric_names.iter().enumerate() {
                metric_values[i].push(extract_metric_value(&inner.sub_aggregation, name));
            }
        }
    }

    let mut columns: Vec<Arc<dyn arrow_array::Array>> = vec![
        Arc::new(StringArray::from(key_0_values)),
        Arc::new(StringArray::from(key_1_values)),
        Arc::new(Int64Array::from(doc_count_values)),
    ];
    for vals in &metric_values {
        columns.push(Arc::new(Float64Array::from(vals.clone())));
    }

    RecordBatch::try_new(schema, columns)
        .context("Failed to create flattened nested Terms RecordBatch")
}

/// Flatten Histogram/DateHistogram → nested Terms into cross-product rows.
/// Produces: key_0:Float64|Timestamp(us), key_1:Utf8, doc_count:Int64, {metric_sub_aggs}:Float64...
fn flatten_nested_bucket_histogram(
    outer_entries: &[&BucketEntry],
    _nested_name: &str,
    nested_template: &BucketResult,
    is_date_histogram: bool,
) -> Result<RecordBatch> {
    let inner_metric_names = match nested_template {
        BucketResult::Terms { buckets, .. } => {
            collect_metric_sub_agg_names(buckets.first().map(|b| &b.sub_aggregation))
        }
        _ => Vec::new(),
    };

    let key_0_type = if is_date_histogram {
        DataType::Timestamp(TimeUnit::Microsecond, None)
    } else {
        DataType::Float64
    };

    let mut fields = vec![
        Field::new("key_0", key_0_type.clone(), false),
        Field::new("key_1", DataType::Utf8, false),
        Field::new("doc_count", DataType::Int64, false),
    ];
    for name in &inner_metric_names {
        fields.push(Field::new(name, DataType::Float64, true));
    }
    let schema = Arc::new(Schema::new(fields));

    let mut key_0_f64: Vec<f64> = Vec::new();
    let mut key_0_ts: Vec<i64> = Vec::new();
    let mut key_1_values: Vec<String> = Vec::new();
    let mut doc_count_values: Vec<i64> = Vec::new();
    let mut metric_values: Vec<Vec<Option<f64>>> =
        inner_metric_names.iter().map(|_| Vec::new()).collect();

    for outer in outer_entries {
        let inner_buckets: Vec<&BucketEntry> = outer
            .sub_aggregation
            .0
            .values()
            .find_map(|r| match r {
                AggregationResult::BucketResult(b) => Some(extract_inner_buckets(b)),
                _ => None,
            })
            .unwrap_or_default();

        for inner in &inner_buckets {
            if is_date_histogram {
                let ms = key_to_f64(&outer.key) as i64;
                key_0_ts.push(ms * 1000); // ms → µs
            } else {
                key_0_f64.push(key_to_f64(&outer.key));
            }
            key_1_values.push(key_to_string(&inner.key));
            doc_count_values.push(inner.doc_count as i64);

            for (i, name) in inner_metric_names.iter().enumerate() {
                metric_values[i].push(extract_metric_value(&inner.sub_aggregation, name));
            }
        }
    }

    let key_0_col: Arc<dyn arrow_array::Array> = if is_date_histogram {
        Arc::new(TimestampMicrosecondArray::from(key_0_ts))
    } else {
        Arc::new(Float64Array::from(key_0_f64))
    };

    let mut columns: Vec<Arc<dyn arrow_array::Array>> = vec![
        key_0_col,
        Arc::new(StringArray::from(key_1_values)),
        Arc::new(Int64Array::from(doc_count_values)),
    ];
    for vals in &metric_values {
        columns.push(Arc::new(Float64Array::from(vals.clone())));
    }

    RecordBatch::try_new(schema, columns)
        .context("Failed to create flattened nested Histogram RecordBatch")
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
}
