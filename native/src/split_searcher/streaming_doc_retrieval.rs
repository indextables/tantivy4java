// streaming_doc_retrieval.rs - Streaming Arrow FFI retrieval for non-companion (regular) splits
//
// Converts Tantivy doc store documents to Arrow RecordBatches and streams them
// through the same StreamingRetrievalSession / mpsc::channel(2) architecture
// used by the companion (parquet) path.
//
// Architecture:
//   perform_bulk_search() → Vec<(segment_ord, doc_id)>
//   start_tantivy_streaming_retrieval() → spawns tokio producer task
//   producer retrieves docs in BATCH_SIZE chunks via doc_async()
//   converts each chunk to RecordBatch → sends through channel
//   consumer calls next_batch() (same as companion path)
//
// Memory bound: same ~24MB peak as companion path (2 × batch in channel).

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use arrow_array::builder::*;
use arrow_array::{ArrayRef, RecordBatch, StructArray, ListArray, MapArray};
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Fields, FieldRef, Schema, TimeUnit};
use tantivy::schema::{FieldEntry, FieldType, OwnedValue};
use tokio::sync::mpsc;

use crate::batch_retrieval::simple::{SimpleBatchConfig, SimpleBatchOptimizer};
use crate::parquet_companion::streaming_ffi::StreamingRetrievalSession;
use crate::perf_println;
use super::types::CachedSearcherContext;

/// Batch size for doc store retrieval. Smaller than companion's 128K because
/// doc store is row-oriented (each doc_async is an I/O op).
const TANTIVY_BATCH_SIZE: usize = 4096;

/// Max concurrent doc_async calls per batch.
/// Matches BASE_CONCURRENT_REQUESTS used by docBatchProjected.
const DOC_ASYNC_CONCURRENCY: usize = super::cache_config::BASE_CONCURRENT_REQUESTS;

/// Build an Arrow schema from a Tantivy schema, optionally filtered by projected fields.
///
/// Only includes stored fields (non-stored fields are not in the doc store).
/// Returns the schema and a vec of (tantivy Field, arrow column index) for efficient lookup.
///
/// If `type_hints` is provided, it overrides the default tantivy→Arrow type mapping
/// for specific fields. This is used when the caller knows the desired output types
/// (e.g., Spark passes Int32 for IntegerType even though tantivy stores as i64).
pub fn tantivy_schema_to_arrow(
    schema: &tantivy::schema::Schema,
    projected_fields: Option<&[String]>,
    type_hints: Option<&HashMap<String, DataType>>,
) -> Result<(Arc<Schema>, Vec<(tantivy::schema::Field, String)>)> {
    let mut arrow_fields = Vec::new();
    let mut field_mapping = Vec::new();

    for (field, field_entry) in schema.fields() {
        let field_name = field_entry.name().to_string();

        // Filter by projected fields if specified
        if let Some(proj) = projected_fields {
            if !proj.iter().any(|p| p == &field_name) {
                continue;
            }
        }

        // Only include stored fields
        if !field_entry.is_stored() {
            continue;
        }

        // Use type hint if available, otherwise derive from tantivy field type
        let arrow_type = if let Some(hints) = type_hints {
            if let Some(hint) = hints.get(&field_name) {
                hint.clone()
            } else {
                tantivy_field_type_to_arrow(field_entry)
            }
        } else {
            tantivy_field_type_to_arrow(field_entry)
        };
        arrow_fields.push(Field::new(&field_name, arrow_type, true));
        field_mapping.push((field, field_name));
    }

    if arrow_fields.is_empty() {
        anyhow::bail!("No stored fields found for streaming schema (check field projection)");
    }

    Ok((Arc::new(Schema::new(arrow_fields)), field_mapping))
}

/// Map a Tantivy FieldEntry to an Arrow DataType.
fn tantivy_field_type_to_arrow(entry: &FieldEntry) -> DataType {
    match entry.field_type() {
        FieldType::Str(_) => DataType::Utf8,
        FieldType::I64(_) => DataType::Int64,
        FieldType::U64(_) => DataType::UInt64,
        FieldType::F64(_) => DataType::Float64,
        FieldType::Bool(_) => DataType::Boolean,
        FieldType::Date(_) => DataType::Timestamp(TimeUnit::Microsecond, None),
        FieldType::Bytes(_) => DataType::Binary,
        FieldType::IpAddr(_) => DataType::Utf8,
        FieldType::JsonObject(_) => DataType::Utf8,
        FieldType::Facet(_) => DataType::Utf8,
    }
}

/// Start a streaming retrieval session for non-companion (regular Tantivy) splits.
///
/// Uses the same StreamingRetrievalSession infrastructure as companion mode.
/// The producer retrieves documents from Tantivy's doc store in batches,
/// converts them to Arrow RecordBatches, and streams through an mpsc channel.
pub async fn start_tantivy_streaming_retrieval(
    ctx: &Arc<CachedSearcherContext>,
    doc_ids: Vec<(u32, u32)>,
    projected_fields: Option<Vec<String>>,
    type_hints: Option<HashMap<String, DataType>>,
) -> Result<StreamingRetrievalSession> {
    let schema = ctx.cached_searcher.schema();
    let (arrow_schema, field_mapping) =
        tantivy_schema_to_arrow(&schema, projected_fields.as_deref(), type_hints.as_ref())?;

    if doc_ids.is_empty() {
        // Empty session — same pattern as companion path
        let (_, rx) = mpsc::channel::<Result<RecordBatch>>(1);
        return Ok(StreamingRetrievalSession::new_empty(rx, arrow_schema));
    }

    let num_docs = doc_ids.len();

    // ========================================================================
    // Store-file range consolidation + prefetch (same as docBatchProjected)
    // Done BEFORE spawning the producer so doc_async() hits the cache.
    // ========================================================================
    prefetch_store_ranges(ctx, &doc_ids).await;

    let (tx, rx) = mpsc::channel::<Result<RecordBatch>>(2);

    let searcher = ctx.cached_searcher.clone();
    let arrow_schema_clone = arrow_schema.clone();

    let handle = tokio::spawn(async move {
        let tx_err = tx.clone();
        let result = produce_tantivy_batches(
            tx,
            doc_ids,
            &searcher,
            &arrow_schema_clone,
            &field_mapping,
        )
        .await;

        if let Err(e) = result {
            let _ = tx_err.send(Err(e)).await;
        }
    });

    perf_println!(
        "⏱️ TANTIVY_STREAMING: session started — {} docs, {} columns",
        num_docs,
        arrow_schema.fields().len()
    );

    Ok(StreamingRetrievalSession::new(rx, arrow_schema, handle))
}

/// Prefetch consolidated byte ranges from the doc store file before streaming.
///
/// Same consolidation + prefetch logic as `docBatchProjected` in batch_doc_retrieval.rs.
/// This ensures that subsequent `doc_async()` calls hit the ByteRangeCache instead of
/// making individual S3 requests per document.
async fn prefetch_store_ranges(
    ctx: &CachedSearcherContext,
    doc_ids: &[(u32, u32)],
) {
    let doc_addresses: Vec<tantivy::DocAddress> = doc_ids
        .iter()
        .map(|&(seg, doc)| tantivy::DocAddress::new(seg, doc))
        .collect();

    let optimizer = SimpleBatchOptimizer::new(SimpleBatchConfig::default());
    if !optimizer.should_optimize(doc_addresses.len()) {
        // Record baseline metric even when prefetch is skipped
        let num_segments = ctx.cached_searcher.segment_readers().len();
        let stats = crate::batch_retrieval::simple::PrefetchStats::empty();
        crate::split_cache_manager::record_batch_metrics(None, doc_ids.len(), &stats, num_segments, 0);
        return;
    }

    perf_println!(
        "⏱️ TANTIVY_STREAMING: consolidating store ranges for {} docs",
        doc_addresses.len()
    );

    let ranges = match optimizer.consolidate_ranges(&doc_addresses, &ctx.cached_searcher) {
        Ok(r) => r,
        Err(e) => {
            perf_println!("⏱️ TANTIVY_STREAMING: range consolidation failed (continuing): {}", e);
            return;
        }
    };

    perf_println!(
        "⏱️ TANTIVY_STREAMING: consolidated {} docs → {} ranges",
        doc_addresses.len(),
        ranges.len()
    );

    let prefetch_result = if let Some(cache) = ctx.byte_range_cache.as_ref() {
        crate::batch_retrieval::simple::prefetch_ranges_with_cache(
            ranges.clone(),
            ctx.cached_storage.clone(),
            &ctx.split_uri,
            cache,
            &ctx.bundle_file_offsets,
        )
        .await
    } else {
        optimizer
            .prefetch_ranges(ranges.clone(), ctx.cached_storage.clone(), &ctx.split_uri)
            .await
    };

    let num_segments = ctx.cached_searcher.segment_readers().len();
    match prefetch_result {
        Ok(stats) => {
            perf_println!(
                "⏱️ TANTIVY_STREAMING: prefetched {} ranges, {} bytes in {}ms",
                stats.ranges_fetched, stats.bytes_fetched, stats.duration_ms
            );
            crate::split_cache_manager::record_batch_metrics(
                None, doc_addresses.len(), &stats, num_segments, 0,
            );
        }
        Err(e) => {
            perf_println!("⏱️ TANTIVY_STREAMING: prefetch failed (continuing): {}", e);
        }
    }
}

/// Producer: retrieves Tantivy documents in batches, converts to RecordBatch,
/// sends through channel.
async fn produce_tantivy_batches(
    tx: mpsc::Sender<Result<RecordBatch>>,
    mut doc_ids: Vec<(u32, u32)>,
    searcher: &Arc<tantivy::Searcher>,
    arrow_schema: &Arc<Schema>,
    field_mapping: &[(tantivy::schema::Field, String)],
) -> Result<()> {
    let t_total = std::time::Instant::now();
    let total_docs = doc_ids.len();

    perf_println!(
        "⏱️ TANTIVY_STREAMING: producer start — {} total docs",
        total_docs
    );

    // Sort by (segment_ord, doc_id) for cache locality
    doc_ids.sort();

    let num_segments = searcher.segment_readers().len();
    let mut rows_emitted = 0usize;
    let mut batches_sent = 0usize;

    for chunk in doc_ids.chunks(TANTIVY_BATCH_SIZE) {
        let t_batch = std::time::Instant::now();

        // Retrieve documents concurrently using buffered stream
        let docs = retrieve_docs_concurrent(chunk, searcher, num_segments).await?;

        // Convert to RecordBatch
        let batch = docs_to_record_batch(&docs, arrow_schema, field_mapping)?;
        let batch_rows = batch.num_rows();

        // Send through channel (blocks if consumer is behind)
        if tx.send(Ok(batch)).await.is_err() {
            perf_println!(
                "⏱️ TANTIVY_STREAMING: consumer dropped after {} rows — stopping",
                rows_emitted
            );
            return Ok(());
        }

        rows_emitted += batch_rows;
        batches_sent += 1;

        perf_println!(
            "⏱️ TANTIVY_STREAMING: batch {} — {} rows in {}ms",
            batches_sent,
            batch_rows,
            t_batch.elapsed().as_millis()
        );
    }

    perf_println!(
        "⏱️ TANTIVY_STREAMING: producer complete — {} batches, {} rows, took {}ms",
        batches_sent,
        rows_emitted,
        t_total.elapsed().as_millis()
    );

    Ok(())
}

/// Retrieve a chunk of documents concurrently using buffered futures.
async fn retrieve_docs_concurrent(
    chunk: &[(u32, u32)],
    searcher: &Arc<tantivy::Searcher>,
    num_segments: usize,
) -> Result<Vec<tantivy::TantivyDocument>> {
    use futures::stream::{StreamExt, TryStreamExt};

    // Collect into owned vec to avoid lifetime issues with async closures
    let owned_ids: Vec<(u32, u32)> = chunk.to_vec();

    let doc_futures = owned_ids.into_iter().map(move |(seg_ord, doc_id)| {
        let s = searcher.clone();
        async move {
            if seg_ord as usize >= num_segments {
                return Err(anyhow::anyhow!(
                    "Invalid segment ordinal {}: index has {} segment(s)",
                    seg_ord,
                    num_segments
                ));
            }
            let addr = tantivy::DocAddress::new(seg_ord, doc_id);
            tokio::time::timeout(std::time::Duration::from_secs(5), s.doc_async(addr))
                .await
                .map_err(|_| anyhow::anyhow!("doc_async timed out for {:?}", addr))?
                .map_err(|e| anyhow::anyhow!("doc_async failed for {:?}: {}", addr, e))
        }
    });

    futures::stream::iter(doc_futures)
        .buffered(DOC_ASYNC_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await
}

/// Convert a batch of TantivyDocuments to an Arrow RecordBatch.
///
/// For each column in the arrow schema, iterates all docs and extracts the first
/// matching field value. Missing values become nulls.
fn docs_to_record_batch(
    docs: &[tantivy::TantivyDocument],
    arrow_schema: &Arc<Schema>,
    field_mapping: &[(tantivy::schema::Field, String)],
) -> Result<RecordBatch> {
    let num_rows = docs.len();
    let num_cols = field_mapping.len();
    let mut columns: Vec<arrow_array::ArrayRef> = Vec::with_capacity(num_cols);

    for (col_idx, (tantivy_field, _field_name)) in field_mapping.iter().enumerate() {
        let arrow_type = arrow_schema.field(col_idx).data_type();
        let col = build_arrow_column(docs, *tantivy_field, arrow_type, num_rows)?;
        columns.push(col);
    }

    RecordBatch::try_new(arrow_schema.clone(), columns)
        .context("Failed to create RecordBatch from Tantivy documents")
}

/// Build a single Arrow column from the specified field across all documents.
///
/// When type hints specify a narrower type (e.g. Int32 instead of Int64),
/// values are narrowed during construction — no extra cast pass needed.
fn build_arrow_column(
    docs: &[tantivy::TantivyDocument],
    field: tantivy::schema::Field,
    arrow_type: &DataType,
    num_rows: usize,
) -> Result<arrow_array::ArrayRef> {
    match arrow_type {
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
            for doc in docs {
                match find_first_value(doc, field) {
                    Some(OwnedValue::Str(s)) => builder.append_value(&s),
                    Some(OwnedValue::IpAddr(ip)) => {
                        builder.append_value(canonical_ip_string(&ip))
                    }
                    Some(OwnedValue::Facet(f)) => builder.append_value(f.to_path_string()),
                    Some(OwnedValue::PreTokStr(pts)) => builder.append_value(&pts.text),
                    Some(OwnedValue::Object(obj)) => {
                        let json = owned_value_to_json_string(&OwnedValue::Object(obj));
                        builder.append_value(&json);
                    }
                    Some(OwnedValue::Array(arr)) => {
                        let json = owned_value_to_json_string(&OwnedValue::Array(arr));
                        builder.append_value(&json);
                    }
                    Some(OwnedValue::Null) | None => builder.append_null(),
                    Some(other) => {
                        // Fallback: convert to string
                        builder.append_value(format!("{:?}", other));
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(num_rows);
            for doc in docs {
                match find_first_value(doc, field) {
                    Some(OwnedValue::I64(v)) => builder.append_value(v),
                    Some(OwnedValue::U64(v)) => builder.append_value(v as i64),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::with_capacity(num_rows);
            for doc in docs {
                match find_first_value(doc, field) {
                    Some(OwnedValue::I64(v)) => builder.append_value(v as i32),
                    Some(OwnedValue::U64(v)) => builder.append_value(v as i32),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int16 => {
            let mut builder = Int16Builder::with_capacity(num_rows);
            for doc in docs {
                match find_first_value(doc, field) {
                    Some(OwnedValue::I64(v)) => builder.append_value(v as i16),
                    Some(OwnedValue::U64(v)) => builder.append_value(v as i16),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int8 => {
            let mut builder = Int8Builder::with_capacity(num_rows);
            for doc in docs {
                match find_first_value(doc, field) {
                    Some(OwnedValue::I64(v)) => builder.append_value(v as i8),
                    Some(OwnedValue::U64(v)) => builder.append_value(v as i8),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::UInt64 => {
            let mut builder = UInt64Builder::with_capacity(num_rows);
            for doc in docs {
                match find_first_value(doc, field) {
                    Some(OwnedValue::U64(v)) => builder.append_value(v),
                    Some(OwnedValue::I64(v)) => builder.append_value(v as u64),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(num_rows);
            for doc in docs {
                match find_first_value(doc, field) {
                    Some(OwnedValue::F64(v)) => builder.append_value(v),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float32 => {
            let mut builder = Float32Builder::with_capacity(num_rows);
            for doc in docs {
                match find_first_value(doc, field) {
                    Some(OwnedValue::F64(v)) => builder.append_value(v as f32),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(num_rows);
            for doc in docs {
                match find_first_value(doc, field) {
                    Some(OwnedValue::Bool(v)) => builder.append_value(v),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let mut builder = TimestampMicrosecondBuilder::with_capacity(num_rows);
            for doc in docs {
                match find_first_value(doc, field) {
                    Some(OwnedValue::Date(dt)) => {
                        let micros = dt.into_timestamp_nanos() / 1_000;
                        builder.append_value(micros);
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Date32 => {
            let mut builder = Date32Builder::with_capacity(num_rows);
            for doc in docs {
                match find_first_value(doc, field) {
                    Some(OwnedValue::Date(dt)) => {
                        let micros = dt.into_timestamp_nanos() / 1_000;
                        let days = (micros / 86_400_000_000) as i32;
                        builder.append_value(days);
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Binary => {
            let mut builder = BinaryBuilder::with_capacity(num_rows, num_rows * 64);
            for doc in docs {
                match find_first_value(doc, field) {
                    Some(OwnedValue::Bytes(b)) => builder.append_value(&b),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        // Complex types: Struct, Map — extract OwnedValues and build recursively
        DataType::Struct(_) | DataType::Map(_, _) => {
            let values: Vec<Option<OwnedValue>> = docs
                .iter()
                .map(|doc| find_first_value(doc, field))
                .collect();
            owned_values_to_arrow(&values, arrow_type)
        }
        // List types: collect ALL values for the field into an Array.
        // Tantivy multi-value fields store each value as a separate field entry,
        // so find_first_value would only return the first element.
        DataType::List(_) | DataType::LargeList(_) => {
            let values: Vec<Option<OwnedValue>> = docs
                .iter()
                .map(|doc| {
                    let all = find_all_values(doc, field);
                    if all.is_empty() {
                        None
                    } else if all.len() == 1 {
                        // Single value — unwrap to the underlying Array.
                        // Handles three storage patterns:
                        //   1. OwnedValue::Array directly (multi-value or JSON array)
                        //   2. OwnedValue::Object({"_values": [...]}) — Spark connector wrapping
                        //   3. OwnedValue::Str("[90, 85, 92]") — JSON-serialized text
                        match all.into_iter().next().unwrap() {
                            v @ OwnedValue::Array(_) => Some(v),
                            OwnedValue::Object(entries) => {
                                // Spark connector wraps arrays as {"_values": [...]}
                                let arr = entries.iter()
                                    .find(|(k, _)| k == "_values")
                                    .and_then(|(_, v)| match v {
                                        OwnedValue::Array(_) => Some(v.clone()),
                                        _ => None,
                                    });
                                if let Some(v) = arr {
                                    Some(v)
                                } else {
                                    // No _values key — check for any single Array value
                                    let arrays: Vec<_> = entries.iter()
                                        .filter_map(|(_, v)| match v {
                                            OwnedValue::Array(_) => Some(v.clone()),
                                            _ => None,
                                        })
                                        .collect();
                                    if arrays.len() == 1 {
                                        Some(arrays.into_iter().next().unwrap())
                                    } else {
                                        Some(OwnedValue::Array(vec![OwnedValue::Object(entries)]))
                                    }
                                }
                            }
                            OwnedValue::Str(s) => {
                                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&s) {
                                    if let Some(arr) = parsed.as_array() {
                                        Some(OwnedValue::Array(
                                            arr.iter().map(json_value_to_owned).collect(),
                                        ))
                                    } else {
                                        Some(OwnedValue::Array(vec![OwnedValue::Str(s)]))
                                    }
                                } else {
                                    Some(OwnedValue::Array(vec![OwnedValue::Str(s)]))
                                }
                            }
                            other => Some(OwnedValue::Array(vec![other])),
                        }
                    } else {
                        Some(OwnedValue::Array(all))
                    }
                })
                .collect();
            owned_values_to_arrow(&values, arrow_type)
        }
        _ => {
            // Fallback: treat as Utf8
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
            for doc in docs {
                match find_first_value(doc, field) {
                    Some(val) => builder.append_value(format!("{:?}", val)),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

/// Find the first value for a given field in a TantivyDocument.
fn find_first_value(
    doc: &tantivy::TantivyDocument,
    field: tantivy::schema::Field,
) -> Option<OwnedValue> {
    for (f, val) in doc.field_values() {
        if f == field {
            return Some(val.into());
        }
    }
    None
}

/// Find ALL values for a given field in a TantivyDocument.
/// Tantivy multi-value fields store each value as a separate field entry.
fn find_all_values(
    doc: &tantivy::TantivyDocument,
    field: tantivy::schema::Field,
) -> Vec<OwnedValue> {
    doc.field_values()
        .filter(|(f, _)| *f == field)
        .map(|(_, val)| val.into())
        .collect()
}

/// Convert an IPv6 address to its canonical display form.
fn canonical_ip_string(ip: &std::net::Ipv6Addr) -> String {
    match ip.to_ipv4_mapped() {
        Some(v4) => v4.to_string(),
        None => ip.to_string(),
    }
}

/// Convert serde_json::Value to OwnedValue (inverse of owned_value_to_json).
fn json_value_to_owned(v: &serde_json::Value) -> OwnedValue {
    match v {
        serde_json::Value::Null => OwnedValue::Null,
        serde_json::Value::Bool(b) => OwnedValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                OwnedValue::I64(i)
            } else if let Some(u) = n.as_u64() {
                OwnedValue::U64(u)
            } else if let Some(f) = n.as_f64() {
                OwnedValue::F64(f)
            } else {
                OwnedValue::Null
            }
        }
        serde_json::Value::String(s) => OwnedValue::Str(s.clone()),
        serde_json::Value::Array(arr) => {
            OwnedValue::Array(arr.iter().map(json_value_to_owned).collect())
        }
        serde_json::Value::Object(map) => {
            OwnedValue::Object(
                map.iter()
                    .map(|(k, v)| (k.clone(), json_value_to_owned(v)))
                    .collect(),
            )
        }
    }
}

/// Convert OwnedValue to a JSON string for Object/Array types.
fn owned_value_to_json_string(value: &OwnedValue) -> String {
    let json_value = owned_value_to_json(value);
    serde_json::to_string(&json_value).unwrap_or_else(|_| "null".to_string())
}

/// Convert OwnedValue to serde_json::Value.
fn owned_value_to_json(value: &OwnedValue) -> serde_json::Value {
    match value {
        OwnedValue::Str(s) => serde_json::Value::String(s.clone()),
        OwnedValue::I64(v) => serde_json::Value::Number((*v).into()),
        OwnedValue::U64(v) => serde_json::Value::Number((*v).into()),
        OwnedValue::F64(v) => serde_json::Number::from_f64(*v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        OwnedValue::Bool(v) => serde_json::Value::Bool(*v),
        OwnedValue::Date(dt) => {
            let nanos = dt.into_timestamp_nanos();
            serde_json::Value::Number(nanos.into())
        }
        OwnedValue::Object(obj) => {
            let map: serde_json::Map<String, serde_json::Value> = obj
                .iter()
                .map(|(k, v)| (k.clone(), owned_value_to_json(v)))
                .collect();
            serde_json::Value::Object(map)
        }
        OwnedValue::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(owned_value_to_json).collect())
        }
        OwnedValue::IpAddr(ip) => serde_json::Value::String(canonical_ip_string(ip)),
        OwnedValue::Facet(f) => serde_json::Value::String(f.to_path_string()),
        OwnedValue::PreTokStr(pts) => serde_json::Value::String(pts.text.clone()),
        OwnedValue::Bytes(b) => {
            use base64::{engine::general_purpose::STANDARD, Engine};
            serde_json::Value::String(STANDARD.encode(b))
        }
        OwnedValue::Null => serde_json::Value::Null,
    }
}

// =============================================================================
// OwnedValue → Arrow complex type conversion
// =============================================================================
//
// Converts tantivy OwnedValue (Object, Array) to proper Arrow StructArray,
// ListArray, MapArray based on a target DataType provided via type hints.
//
// This is the reverse of convert_arrow_to_owned_value() in indexing.rs (PR #114).
// The write path does Arrow → OwnedValue; this does OwnedValue → Arrow.

/// Convert a batch of OwnedValues to an Arrow array of the target DataType.
///
/// Handles complex types (Struct, List, Map) recursively, with scalar leaf types.
/// Each element in `values` corresponds to one row; `None` means null.
fn owned_values_to_arrow(
    values: &[Option<OwnedValue>],
    target_type: &DataType,
) -> Result<ArrayRef> {
    match target_type {
        DataType::Struct(fields) => build_struct_from_owned_values(values, fields),
        DataType::List(inner) => build_list_from_owned_values(values, inner),
        DataType::LargeList(inner) => build_list_from_owned_values(values, inner),
        DataType::Map(entries_field, sorted) => {
            build_map_from_owned_values(values, entries_field, *sorted)
        }
        _ => build_scalar_from_owned_values(values, target_type),
    }
}

/// Build a StructArray from OwnedValue::Object values.
///
/// For each struct field, extracts the matching entry from each OwnedValue::Object
/// and recursively builds the child column. Missing fields become nulls.
fn build_struct_from_owned_values(
    values: &[Option<OwnedValue>],
    fields: &Fields,
) -> Result<ArrayRef> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(fields.len());

    for field in fields.iter() {
        // Extract this field's value from each row's OwnedValue::Object
        let field_values: Vec<Option<OwnedValue>> = values
            .iter()
            .map(|opt| {
                opt.as_ref().and_then(|v| match v {
                    OwnedValue::Object(entries) => entries
                        .iter()
                        .find(|(k, _)| k == field.name())
                        .map(|(_, v)| v.clone()),
                    _ => None,
                })
            })
            .collect();

        let col = owned_values_to_arrow(&field_values, field.data_type())?;
        columns.push(col);
    }

    // Build null buffer: null if the row's OwnedValue was None
    let null_flags: Vec<bool> = values.iter().map(|v| v.is_some()).collect();
    let null_buffer = NullBuffer::from(null_flags);

    Ok(Arc::new(StructArray::new(
        fields.clone(),
        columns,
        Some(null_buffer),
    )))
}

/// Build a ListArray from OwnedValue::Array values.
///
/// Flattens all list elements into a single child array with an offsets buffer
/// tracking where each row's list starts and ends.
fn build_list_from_owned_values(
    values: &[Option<OwnedValue>],
    inner_field: &FieldRef,
) -> Result<ArrayRef> {
    let num_rows = values.len();
    let mut offsets: Vec<i32> = Vec::with_capacity(num_rows + 1);
    let mut all_elements: Vec<Option<OwnedValue>> = Vec::new();
    let mut null_flags: Vec<bool> = Vec::with_capacity(num_rows);

    offsets.push(0);
    for v in values {
        match v {
            Some(OwnedValue::Array(items)) => {
                for item in items {
                    all_elements.push(Some(item.clone()));
                }
                offsets.push(all_elements.len() as i32);
                null_flags.push(true);
            }
            Some(OwnedValue::Str(s)) => {
                // Try to parse as JSON array (e.g. "[90, 85, 92]")
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(s) {
                    if let Some(arr) = parsed.as_array() {
                        for elem in arr {
                            all_elements.push(Some(json_value_to_owned(elem)));
                        }
                        offsets.push(all_elements.len() as i32);
                        null_flags.push(true);
                        continue;
                    }
                }
                // Single scalar string — wrap as one-element list
                all_elements.push(Some(OwnedValue::Str(s.clone())));
                offsets.push(all_elements.len() as i32);
                null_flags.push(true);
            }
            None => {
                offsets.push(all_elements.len() as i32);
                null_flags.push(false);
            }
            Some(_) => {
                // Non-array value in a list column — treat as null
                offsets.push(all_elements.len() as i32);
                null_flags.push(false);
            }
        }
    }

    let element_array = owned_values_to_arrow(&all_elements, inner_field.data_type())?;
    let offsets_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let null_buffer = NullBuffer::from(null_flags);

    Ok(Arc::new(ListArray::new(
        inner_field.clone(),
        offsets_buffer,
        element_array,
        Some(null_buffer),
    )))
}

/// Build a MapArray from OwnedValue::Object values.
///
/// Each OwnedValue::Object's key-value pairs become entries in the map.
/// Keys are always converted to the target key type; values are recursively converted.
fn build_map_from_owned_values(
    values: &[Option<OwnedValue>],
    entries_field: &FieldRef,
    sorted: bool,
) -> Result<ArrayRef> {
    let struct_fields = match entries_field.data_type() {
        DataType::Struct(fields) => fields,
        _ => return Err(anyhow!("Map entries field must be Struct, got {:?}", entries_field.data_type())),
    };
    if struct_fields.len() != 2 {
        return Err(anyhow!("Map entries must have exactly 2 fields (key, value)"));
    }
    let key_field = &struct_fields[0];
    let value_field = &struct_fields[1];

    let num_rows = values.len();
    let mut offsets: Vec<i32> = Vec::with_capacity(num_rows + 1);
    let mut all_keys: Vec<Option<OwnedValue>> = Vec::new();
    let mut all_values: Vec<Option<OwnedValue>> = Vec::new();
    let mut null_flags: Vec<bool> = Vec::with_capacity(num_rows);

    offsets.push(0);
    for v in values {
        match v {
            Some(OwnedValue::Object(entries)) => {
                for (k, val) in entries {
                    all_keys.push(Some(OwnedValue::Str(k.clone())));
                    all_values.push(Some(val.clone()));
                }
                offsets.push(all_keys.len() as i32);
                null_flags.push(true);
            }
            None => {
                offsets.push(all_keys.len() as i32);
                null_flags.push(false);
            }
            Some(_) => {
                offsets.push(all_keys.len() as i32);
                null_flags.push(false);
            }
        }
    }

    let keys_array = owned_values_to_arrow(&all_keys, key_field.data_type())?;
    let values_array = owned_values_to_arrow(&all_values, value_field.data_type())?;

    // Build the inner struct (entries are not individually nullable in a Map)
    let entries_struct = StructArray::new(
        struct_fields.clone(),
        vec![keys_array, values_array],
        None,
    );

    let offsets_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets));
    let null_buffer = NullBuffer::from(null_flags);

    Ok(Arc::new(MapArray::new(
        entries_field.clone(),
        offsets_buffer,
        entries_struct,
        Some(null_buffer),
        sorted,
    )))
}

/// Build a scalar Arrow array from OwnedValues.
///
/// Handles all tantivy scalar types (Str, I64, U64, F64, Bool, Date, etc.)
/// with best-effort type coercion to match the target DataType.
fn build_scalar_from_owned_values(
    values: &[Option<OwnedValue>],
    target_type: &DataType,
) -> Result<ArrayRef> {
    let num_rows = values.len();
    match target_type {
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
            for v in values {
                match v {
                    Some(OwnedValue::Str(s)) => builder.append_value(s),
                    Some(OwnedValue::IpAddr(ip)) => builder.append_value(canonical_ip_string(ip)),
                    Some(OwnedValue::Facet(f)) => builder.append_value(f.to_path_string()),
                    Some(OwnedValue::PreTokStr(pts)) => builder.append_value(&pts.text),
                    Some(OwnedValue::Object(_)) | Some(OwnedValue::Array(_)) => {
                        builder.append_value(owned_value_to_json_string(v.as_ref().unwrap()));
                    }
                    Some(OwnedValue::I64(n)) => builder.append_value(n.to_string()),
                    Some(OwnedValue::U64(n)) => builder.append_value(n.to_string()),
                    Some(OwnedValue::F64(n)) => builder.append_value(n.to_string()),
                    Some(OwnedValue::Bool(b)) => builder.append_value(b.to_string()),
                    Some(OwnedValue::Null) | None => builder.append_null(),
                    Some(other) => builder.append_value(format!("{:?}", other)),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(num_rows);
            for v in values {
                match v {
                    Some(OwnedValue::I64(n)) => builder.append_value(*n),
                    Some(OwnedValue::U64(n)) => builder.append_value(*n as i64),
                    Some(OwnedValue::Str(s)) => match s.parse::<i64>() {
                        Ok(n) => builder.append_value(n),
                        Err(_) => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::with_capacity(num_rows);
            for v in values {
                match v {
                    Some(OwnedValue::I64(n)) => builder.append_value(*n as i32),
                    Some(OwnedValue::U64(n)) => builder.append_value(*n as i32),
                    Some(OwnedValue::Str(s)) => match s.parse::<i32>() {
                        Ok(n) => builder.append_value(n),
                        Err(_) => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int16 => {
            let mut builder = Int16Builder::with_capacity(num_rows);
            for v in values {
                match v {
                    Some(OwnedValue::I64(n)) => builder.append_value(*n as i16),
                    Some(OwnedValue::U64(n)) => builder.append_value(*n as i16),
                    Some(OwnedValue::Str(s)) => match s.parse::<i16>() {
                        Ok(n) => builder.append_value(n),
                        Err(_) => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int8 => {
            let mut builder = Int8Builder::with_capacity(num_rows);
            for v in values {
                match v {
                    Some(OwnedValue::I64(n)) => builder.append_value(*n as i8),
                    Some(OwnedValue::U64(n)) => builder.append_value(*n as i8),
                    Some(OwnedValue::Str(s)) => match s.parse::<i8>() {
                        Ok(n) => builder.append_value(n),
                        Err(_) => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::UInt64 => {
            let mut builder = UInt64Builder::with_capacity(num_rows);
            for v in values {
                match v {
                    Some(OwnedValue::U64(n)) => builder.append_value(*n),
                    Some(OwnedValue::I64(n)) => builder.append_value(*n as u64),
                    Some(OwnedValue::Str(s)) => match s.parse::<u64>() {
                        Ok(n) => builder.append_value(n),
                        Err(_) => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(num_rows);
            for v in values {
                match v {
                    Some(OwnedValue::F64(n)) => builder.append_value(*n),
                    Some(OwnedValue::I64(n)) => builder.append_value(*n as f64),
                    Some(OwnedValue::U64(n)) => builder.append_value(*n as u64 as f64),
                    Some(OwnedValue::Str(s)) => match s.parse::<f64>() {
                        Ok(n) => builder.append_value(n),
                        Err(_) => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float32 => {
            let mut builder = Float32Builder::with_capacity(num_rows);
            for v in values {
                match v {
                    Some(OwnedValue::F64(n)) => builder.append_value(*n as f32),
                    Some(OwnedValue::I64(n)) => builder.append_value(*n as f32),
                    Some(OwnedValue::Str(s)) => match s.parse::<f32>() {
                        Ok(n) => builder.append_value(n),
                        Err(_) => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(num_rows);
            for v in values {
                match v {
                    Some(OwnedValue::Bool(b)) => builder.append_value(*b),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let mut builder = TimestampMicrosecondBuilder::with_capacity(num_rows);
            for v in values {
                match v {
                    Some(OwnedValue::Date(dt)) => {
                        let micros = dt.into_timestamp_nanos() / 1_000;
                        builder.append_value(micros);
                    }
                    Some(OwnedValue::I64(n)) => builder.append_value(*n),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Date32 => {
            let mut builder = Date32Builder::with_capacity(num_rows);
            for v in values {
                match v {
                    Some(OwnedValue::Date(dt)) => {
                        let micros = dt.into_timestamp_nanos() / 1_000;
                        let days = (micros / 86_400_000_000) as i32;
                        builder.append_value(days);
                    }
                    Some(OwnedValue::I64(n)) => {
                        // Assume i64 is microseconds, convert to days
                        let days = (*n / 86_400_000_000) as i32;
                        builder.append_value(days);
                    }
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Binary => {
            let mut builder = BinaryBuilder::with_capacity(num_rows, num_rows * 64);
            for v in values {
                match v {
                    Some(OwnedValue::Bytes(b)) => builder.append_value(b),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => {
            // Fallback: stringify everything
            let mut builder = StringBuilder::with_capacity(num_rows, num_rows * 32);
            for v in values {
                match v {
                    Some(val) => builder.append_value(format!("{:?}", val)),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;
    use tantivy::schema::{SchemaBuilder, STORED, TEXT, FAST};

    #[test]
    fn test_tantivy_schema_to_arrow_basic() {
        let mut sb = SchemaBuilder::new();
        sb.add_text_field("title", TEXT | STORED);
        sb.add_i64_field("count", STORED | FAST);
        sb.add_f64_field("score", STORED);
        sb.add_bool_field("active", STORED);
        let schema = sb.build();

        let (arrow_schema, mapping) = tantivy_schema_to_arrow(&schema, None, None).unwrap();
        assert_eq!(arrow_schema.fields().len(), 4);
        assert_eq!(mapping.len(), 4);

        assert_eq!(arrow_schema.field(0).name(), "title");
        assert_eq!(*arrow_schema.field(0).data_type(), DataType::Utf8);

        assert_eq!(arrow_schema.field(1).name(), "count");
        assert_eq!(*arrow_schema.field(1).data_type(), DataType::Int64);

        assert_eq!(arrow_schema.field(2).name(), "score");
        assert_eq!(*arrow_schema.field(2).data_type(), DataType::Float64);

        assert_eq!(arrow_schema.field(3).name(), "active");
        assert_eq!(*arrow_schema.field(3).data_type(), DataType::Boolean);
    }

    #[test]
    fn test_tantivy_schema_to_arrow_projected() {
        let mut sb = SchemaBuilder::new();
        sb.add_text_field("title", TEXT | STORED);
        sb.add_i64_field("count", STORED | FAST);
        sb.add_f64_field("score", STORED);
        let schema = sb.build();

        let fields = vec!["title".to_string(), "score".to_string()];
        let (arrow_schema, mapping) =
            tantivy_schema_to_arrow(&schema, Some(&fields), None).unwrap();
        assert_eq!(arrow_schema.fields().len(), 2);
        assert_eq!(mapping.len(), 2);
        assert_eq!(arrow_schema.field(0).name(), "title");
        assert_eq!(arrow_schema.field(1).name(), "score");
    }

    #[test]
    fn test_tantivy_schema_to_arrow_no_stored() {
        let mut sb = SchemaBuilder::new();
        // TEXT without STORED — field is indexed but not stored
        sb.add_text_field("body", TEXT);
        let schema = sb.build();

        let result = tantivy_schema_to_arrow(&schema, None, None);
        assert!(result.is_err()); // No stored fields → error
    }

    #[test]
    fn test_docs_to_record_batch() {
        let mut sb = SchemaBuilder::new();
        let title_field = sb.add_text_field("title", TEXT | STORED);
        let count_field = sb.add_i64_field("count", STORED | FAST);
        let schema = sb.build();

        let (arrow_schema, field_mapping) = tantivy_schema_to_arrow(&schema, None, None).unwrap();

        let mut doc1 = tantivy::TantivyDocument::default();
        doc1.add_text(title_field, "hello");
        doc1.add_i64(count_field, 42);

        let mut doc2 = tantivy::TantivyDocument::default();
        doc2.add_text(title_field, "world");
        doc2.add_i64(count_field, 99);

        let docs = vec![doc1, doc2];
        let batch = docs_to_record_batch(&docs, &arrow_schema, &field_mapping).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 2);

        let title_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap();
        assert_eq!(title_col.value(0), "hello");
        assert_eq!(title_col.value(1), "world");

        let count_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap();
        assert_eq!(count_col.value(0), 42);
        assert_eq!(count_col.value(1), 99);
    }

    #[test]
    fn test_type_hints_narrow_int_and_float() {
        let mut sb = SchemaBuilder::new();
        let id_field = sb.add_i64_field("id", STORED | FAST);
        let score_field = sb.add_f64_field("score", STORED);
        let rank_field = sb.add_i64_field("rank", STORED);
        let schema = sb.build();

        // Request Int32 for "id", Float32 for "score", leave "rank" as default (Int64)
        let mut hints = HashMap::new();
        hints.insert("id".to_string(), DataType::Int32);
        hints.insert("score".to_string(), DataType::Float32);

        let (arrow_schema, field_mapping) =
            tantivy_schema_to_arrow(&schema, None, Some(&hints)).unwrap();

        assert_eq!(*arrow_schema.field(0).data_type(), DataType::Int32);   // narrowed
        assert_eq!(*arrow_schema.field(1).data_type(), DataType::Float32); // narrowed
        assert_eq!(*arrow_schema.field(2).data_type(), DataType::Int64);   // default

        // Build docs and verify narrowed values
        let mut doc1 = tantivy::TantivyDocument::default();
        doc1.add_i64(id_field, 42);
        doc1.add_f64(score_field, 3.14);
        doc1.add_i64(rank_field, 100);

        let docs = vec![doc1];
        let batch = docs_to_record_batch(&docs, &arrow_schema, &field_mapping).unwrap();
        assert_eq!(batch.num_rows(), 1);

        // Verify Int32 column
        let id_col = batch.column(0).as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .expect("id should be Int32Array");
        assert_eq!(id_col.value(0), 42);

        // Verify Float32 column
        let score_col = batch.column(1).as_any()
            .downcast_ref::<arrow_array::Float32Array>()
            .expect("score should be Float32Array");
        assert!((score_col.value(0) - 3.14f32).abs() < 0.001);

        // Verify Int64 column (default, no hint)
        let rank_col = batch.column(2).as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("rank should be Int64Array");
        assert_eq!(rank_col.value(0), 100);
    }

    #[test]
    fn test_type_hints_int16_and_int8() {
        let mut sb = SchemaBuilder::new();
        let short_field = sb.add_i64_field("short_val", STORED);
        let byte_field = sb.add_i64_field("byte_val", STORED);
        let schema = sb.build();

        let mut hints = HashMap::new();
        hints.insert("short_val".to_string(), DataType::Int16);
        hints.insert("byte_val".to_string(), DataType::Int8);

        let (arrow_schema, field_mapping) =
            tantivy_schema_to_arrow(&schema, None, Some(&hints)).unwrap();

        assert_eq!(*arrow_schema.field(0).data_type(), DataType::Int16);
        assert_eq!(*arrow_schema.field(1).data_type(), DataType::Int8);

        let mut doc = tantivy::TantivyDocument::default();
        doc.add_i64(short_field, 1000);
        doc.add_i64(byte_field, 42);

        let batch = docs_to_record_batch(&[doc], &arrow_schema, &field_mapping).unwrap();

        let short_col = batch.column(0).as_any()
            .downcast_ref::<arrow_array::Int16Array>()
            .expect("should be Int16Array");
        assert_eq!(short_col.value(0), 1000);

        let byte_col = batch.column(1).as_any()
            .downcast_ref::<arrow_array::Int8Array>()
            .expect("should be Int8Array");
        assert_eq!(byte_col.value(0), 42);
    }

    #[test]
    fn test_owned_values_to_struct_array() {
        let values = vec![
            Some(OwnedValue::Object(vec![
                ("name".to_string(), OwnedValue::Str("Alice".to_string())),
                ("age".to_string(), OwnedValue::I64(30)),
            ])),
            Some(OwnedValue::Object(vec![
                ("name".to_string(), OwnedValue::Str("Bob".to_string())),
                ("age".to_string(), OwnedValue::I64(25)),
            ])),
            None, // null row
        ];

        let target = DataType::Struct(
            vec![
                Arc::new(Field::new("name", DataType::Utf8, true)),
                Arc::new(Field::new("age", DataType::Int64, true)),
            ]
            .into(),
        );

        let array = owned_values_to_arrow(&values, &target).unwrap();
        let struct_arr = array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_arr.len(), 3);

        // Check name column
        let names = struct_arr.column(0).as_any()
            .downcast_ref::<arrow_array::StringArray>().unwrap();
        assert_eq!(names.value(0), "Alice");
        assert_eq!(names.value(1), "Bob");
        assert!(struct_arr.is_null(2)); // whole struct is null

        // Check age column
        let ages = struct_arr.column(1).as_any()
            .downcast_ref::<arrow_array::Int64Array>().unwrap();
        assert_eq!(ages.value(0), 30);
        assert_eq!(ages.value(1), 25);
    }

    #[test]
    fn test_owned_values_to_list_array() {
        let values = vec![
            Some(OwnedValue::Array(vec![
                OwnedValue::Str("a".to_string()),
                OwnedValue::Str("b".to_string()),
                OwnedValue::Str("c".to_string()),
            ])),
            Some(OwnedValue::Array(vec![
                OwnedValue::Str("x".to_string()),
            ])),
            None, // null list
            Some(OwnedValue::Array(vec![])), // empty list
        ];

        let target = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));

        let array = owned_values_to_arrow(&values, &target).unwrap();
        let list_arr = array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list_arr.len(), 4);

        // Row 0: ["a", "b", "c"]
        let row0 = list_arr.value(0);
        let strs = row0.as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
        assert_eq!(strs.len(), 3);
        assert_eq!(strs.value(0), "a");
        assert_eq!(strs.value(1), "b");
        assert_eq!(strs.value(2), "c");

        // Row 1: ["x"]
        let row1 = list_arr.value(1);
        assert_eq!(row1.len(), 1);

        // Row 2: null
        assert!(list_arr.is_null(2));

        // Row 3: empty list
        assert!(!list_arr.is_null(3));
        assert_eq!(list_arr.value(3).len(), 0);
    }

    #[test]
    fn test_owned_values_to_map_array() {
        let values = vec![
            Some(OwnedValue::Object(vec![
                ("key1".to_string(), OwnedValue::I64(100)),
                ("key2".to_string(), OwnedValue::I64(200)),
            ])),
            None, // null map
        ];

        let entries_field = Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", DataType::Utf8, false)),
                    Arc::new(Field::new("value", DataType::Int64, true)),
                ]
                .into(),
            ),
            false,
        );
        let target = DataType::Map(Arc::new(entries_field), false);

        let array = owned_values_to_arrow(&values, &target).unwrap();
        let map_arr = array.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map_arr.len(), 2);

        // Row 0: {"key1": 100, "key2": 200}
        let entry0 = map_arr.value(0);
        let keys_struct = entry0.as_any().downcast_ref::<StructArray>().unwrap();
        let keys = keys_struct.column(0).as_any()
            .downcast_ref::<arrow_array::StringArray>().unwrap();
        assert_eq!(keys.value(0), "key1");
        assert_eq!(keys.value(1), "key2");

        let vals = keys_struct.column(1).as_any()
            .downcast_ref::<arrow_array::Int64Array>().unwrap();
        assert_eq!(vals.value(0), 100);
        assert_eq!(vals.value(1), 200);

        // Row 1: null
        assert!(map_arr.is_null(1));
    }

    #[test]
    fn test_owned_values_nested_struct_with_list() {
        // Struct containing a List field
        let values = vec![
            Some(OwnedValue::Object(vec![
                ("id".to_string(), OwnedValue::I64(1)),
                ("tags".to_string(), OwnedValue::Array(vec![
                    OwnedValue::Str("rust".to_string()),
                    OwnedValue::Str("arrow".to_string()),
                ])),
            ])),
        ];

        let target = DataType::Struct(
            vec![
                Arc::new(Field::new("id", DataType::Int64, true)),
                Arc::new(Field::new(
                    "tags",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                )),
            ]
            .into(),
        );

        let array = owned_values_to_arrow(&values, &target).unwrap();
        let struct_arr = array.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(struct_arr.len(), 1);

        // Check id
        let ids = struct_arr.column(0).as_any()
            .downcast_ref::<arrow_array::Int64Array>().unwrap();
        assert_eq!(ids.value(0), 1);

        // Check tags list
        let tags = struct_arr.column(1).as_any()
            .downcast_ref::<ListArray>().unwrap();
        let tag_vals = tags.value(0);
        let strs = tag_vals.as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
        assert_eq!(strs.len(), 2);
        assert_eq!(strs.value(0), "rust");
        assert_eq!(strs.value(1), "arrow");
    }

    #[test]
    fn test_build_arrow_column_struct_via_type_hint() {
        // End-to-end: tantivy doc with JSON field → struct column via type hint
        let mut sb = SchemaBuilder::new();
        let json_field = sb.add_json_field("data", tantivy::schema::STORED);
        let schema = sb.build();

        let mut hints = HashMap::new();
        hints.insert(
            "data".to_string(),
            DataType::Struct(
                vec![
                    Arc::new(Field::new("x", DataType::Float64, true)),
                    Arc::new(Field::new("y", DataType::Float64, true)),
                ]
                .into(),
            ),
        );

        let (arrow_schema, field_mapping) =
            tantivy_schema_to_arrow(&schema, None, Some(&hints)).unwrap();

        // Verify schema has Struct type, not Utf8
        assert!(matches!(
            arrow_schema.field(0).data_type(),
            DataType::Struct(_)
        ));

        // Build a doc with JSON object
        let mut doc = tantivy::TantivyDocument::default();
        let mut obj = std::collections::BTreeMap::new();
        obj.insert("x".to_string(), OwnedValue::F64(1.5));
        obj.insert("y".to_string(), OwnedValue::F64(2.5));
        doc.add_object(json_field, obj);

        let batch = docs_to_record_batch(&[doc], &arrow_schema, &field_mapping).unwrap();
        let struct_col = batch.column(0).as_any()
            .downcast_ref::<StructArray>()
            .expect("should be StructArray, not StringArray");

        let x = struct_col.column(0).as_any()
            .downcast_ref::<arrow_array::Float64Array>().unwrap();
        assert_eq!(x.value(0), 1.5);

        let y = struct_col.column(1).as_any()
            .downcast_ref::<arrow_array::Float64Array>().unwrap();
        assert_eq!(y.value(0), 2.5);
    }

    #[test]
    fn test_date32_type_hint_converts_microseconds_to_days() {
        // Date32 hint: tantivy DateTime (micros since epoch) → i32 days since epoch
        let mut sb = SchemaBuilder::new();
        let date_opts = tantivy::schema::DateOptions::default()
            .set_stored()
            .set_indexed()
            .set_fast();
        let date_field = sb.add_date_field("created", date_opts);
        let schema = sb.build();

        let mut hints = HashMap::new();
        hints.insert("created".to_string(), DataType::Date32);

        let (arrow_schema, field_mapping) =
            tantivy_schema_to_arrow(&schema, None, Some(&hints)).unwrap();

        assert_eq!(*arrow_schema.field(0).data_type(), DataType::Date32);

        // 2024-01-15 = 19737 days since 1970-01-01
        // In microseconds: 19737 * 86_400_000_000 = 1_705_276_800_000_000
        let micros = 19737i64 * 86_400_000_000i64;
        let dt = tantivy::DateTime::from_timestamp_micros(micros);

        let mut doc = tantivy::TantivyDocument::default();
        doc.add_date(date_field, dt);

        let batch = docs_to_record_batch(&[doc], &arrow_schema, &field_mapping).unwrap();
        let date_col = batch.column(0).as_any()
            .downcast_ref::<arrow_array::Date32Array>()
            .expect("should be Date32Array");

        assert_eq!(date_col.value(0), 19737);
    }

    #[test]
    fn test_date32_null_handling() {
        // Date32 with missing values → null
        let mut sb = SchemaBuilder::new();
        let date_opts = tantivy::schema::DateOptions::default()
            .set_stored()
            .set_indexed();
        sb.add_date_field("created", date_opts);
        let text_opts = tantivy::schema::TextOptions::default()
            .set_stored();
        let title_field = sb.add_text_field("title", text_opts);
        let schema = sb.build();

        let mut hints = HashMap::new();
        hints.insert("created".to_string(), DataType::Date32);

        let (arrow_schema, field_mapping) =
            tantivy_schema_to_arrow(&schema, None, Some(&hints)).unwrap();

        // Doc without date field → null
        let mut doc = tantivy::TantivyDocument::default();
        doc.add_text(title_field, "no date");

        let batch = docs_to_record_batch(&[doc], &arrow_schema, &field_mapping).unwrap();
        let date_col = batch.column(0).as_any()
            .downcast_ref::<arrow_array::Date32Array>()
            .expect("should be Date32Array");

        assert!(date_col.is_null(0));
    }

    /// Regression: multi-value text field should produce a ListArray, not just
    /// the first value. find_all_values collects all entries for the field.
    #[test]
    fn test_list_from_multi_value_field() {
        let mut sb = SchemaBuilder::new();
        let text_opts = tantivy::schema::TextOptions::default().set_stored();
        let tags_field = sb.add_text_field("tags", text_opts.clone());
        let schema = sb.build();

        let mut hints = HashMap::new();
        hints.insert(
            "tags".to_string(),
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
        );

        let (arrow_schema, field_mapping) =
            tantivy_schema_to_arrow(&schema, None, Some(&hints)).unwrap();

        assert!(matches!(
            arrow_schema.field(0).data_type(),
            DataType::List(_)
        ));

        // Doc with multiple values for the same field (tantivy multi-value)
        let mut doc = tantivy::TantivyDocument::default();
        doc.add_text(tags_field, "alpha");
        doc.add_text(tags_field, "beta");
        doc.add_text(tags_field, "gamma");

        let batch = docs_to_record_batch(&[doc], &arrow_schema, &field_mapping).unwrap();
        let list_col = batch.column(0).as_any()
            .downcast_ref::<ListArray>()
            .expect("should be ListArray");

        assert!(!list_col.is_null(0));
        let row0 = list_col.value(0);
        let strs = row0.as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
        assert_eq!(strs.len(), 3);
        assert_eq!(strs.value(0), "alpha");
        assert_eq!(strs.value(1), "beta");
        assert_eq!(strs.value(2), "gamma");
    }

    /// Regression: map with integer keys. JSON/OwnedValue keys are always strings;
    /// build_scalar_from_owned_values must parse them to the target key type.
    #[test]
    fn test_map_with_integer_keys() {
        let key_type = DataType::Int32;
        let val_type = DataType::Utf8;
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Arc::new(Field::new("key", key_type.clone(), false)),
                    Arc::new(Field::new("value", val_type.clone(), true)),
                ]
                .into(),
            ),
            false,
        ));
        let map_type = DataType::Map(entries_field, false);

        // OwnedValue::Object always has string keys
        let values: Vec<Option<OwnedValue>> = vec![Some(OwnedValue::Object(vec![
            ("10".to_string(), OwnedValue::Str("ten".to_string())),
            ("20".to_string(), OwnedValue::Str("twenty".to_string())),
        ]))];

        let arr = owned_values_to_arrow(&values, &map_type).unwrap();
        let map_arr = arr.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map_arr.len(), 1);

        let entries = map_arr.value(0);
        let struct_arr = entries.as_any().downcast_ref::<StructArray>().unwrap();
        let keys = struct_arr.column(0).as_any()
            .downcast_ref::<arrow_array::Int32Array>().unwrap();
        let vals = struct_arr.column(1).as_any()
            .downcast_ref::<arrow_array::StringArray>().unwrap();

        assert_eq!(keys.value(0), 10);
        assert_eq!(keys.value(1), 20);
        assert_eq!(vals.value(0), "ten");
        assert_eq!(vals.value(1), "twenty");
    }

    /// Regression: string-serialized JSON integer array stored in a text field
    /// should produce a valid Int32 ListArray with no nulls.
    #[test]
    fn test_list_from_json_string_int_array() {
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));

        // Simulate: text field value is a JSON-serialized integer array
        let values: Vec<Option<OwnedValue>> = vec![
            Some(OwnedValue::Str("[90, 85, 92]".to_string())),
            Some(OwnedValue::Str("[78, 88, 95]".to_string())),
        ];

        let arr = owned_values_to_arrow(&values, &list_type).unwrap();
        let list_arr = arr.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list_arr.len(), 2);
        assert!(!list_arr.is_null(0));
        assert!(!list_arr.is_null(1));

        // First row: [90, 85, 92]
        let row0 = list_arr.value(0);
        let ints0 = row0.as_any().downcast_ref::<arrow_array::Int32Array>().unwrap();
        assert_eq!(ints0.len(), 3);
        assert!(!ints0.is_null(0));
        assert!(!ints0.is_null(1));
        assert!(!ints0.is_null(2));
        assert_eq!(ints0.value(0), 90);
        assert_eq!(ints0.value(1), 85);
        assert_eq!(ints0.value(2), 92);

        // Second row: [78, 88, 95]
        let row1 = list_arr.value(1);
        let ints1 = row1.as_any().downcast_ref::<arrow_array::Int32Array>().unwrap();
        assert_eq!(ints1.len(), 3);
        assert_eq!(ints1.value(0), 78);
        assert_eq!(ints1.value(1), 88);
        assert_eq!(ints1.value(2), 95);
    }

    /// Regression: JSON-serialized string array also parses correctly.
    #[test]
    fn test_list_from_json_string_str_array() {
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));

        let values: Vec<Option<OwnedValue>> = vec![
            Some(OwnedValue::Str(r#"["alpha", "beta"]"#.to_string())),
        ];

        let arr = owned_values_to_arrow(&values, &list_type).unwrap();
        let list_arr = arr.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list_arr.len(), 1);

        let row0 = list_arr.value(0);
        let strs = row0.as_any().downcast_ref::<arrow_array::StringArray>().unwrap();
        assert_eq!(strs.len(), 2);
        assert_eq!(strs.value(0), "alpha");
        assert_eq!(strs.value(1), "beta");
    }

    /// End-to-end regression: text field storing "[90, 85, 92]" with List(Int32)
    /// type hint, going through the full docs_to_record_batch → build_arrow_column
    /// → find_all_values → JSON parse path.
    #[test]
    fn test_e2e_text_field_json_int_array_to_list_i32() {
        let mut sb = SchemaBuilder::new();
        let text_opts = tantivy::schema::TextOptions::default().set_stored();
        let scores_field = sb.add_text_field("scores", text_opts);
        let schema = sb.build();

        let mut hints = HashMap::new();
        hints.insert(
            "scores".to_string(),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        );

        let (arrow_schema, field_mapping) =
            tantivy_schema_to_arrow(&schema, None, Some(&hints)).unwrap();

        // Store a JSON-serialized integer array as a single text field value
        let mut doc = tantivy::TantivyDocument::default();
        doc.add_text(scores_field, "[90, 85, 92]");

        let batch = docs_to_record_batch(&[doc], &arrow_schema, &field_mapping).unwrap();
        let list_col = batch.column(0).as_any()
            .downcast_ref::<ListArray>()
            .expect("should be ListArray");

        assert!(!list_col.is_null(0));
        let row0 = list_col.value(0);
        let ints = row0.as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .expect("child should be Int32Array");

        assert_eq!(ints.len(), 3, "should have 3 elements from [90, 85, 92]");
        assert!(!ints.is_null(0), "element 0 should not be null");
        assert!(!ints.is_null(1), "element 1 should not be null");
        assert!(!ints.is_null(2), "element 2 should not be null");
        assert_eq!(ints.value(0), 90);
        assert_eq!(ints.value(1), 85);
        assert_eq!(ints.value(2), 92);
    }

    /// Regression: Spark connector wraps arrays as {"_values": [90, 85, 92]} in a JSON field.
    /// The List dispatch must unwrap the "_values" key to get the actual array.
    #[test]
    fn test_e2e_json_field_values_wrapped_int_array_to_list_i32() {
        let mut sb = SchemaBuilder::new();
        let json_field = sb.add_json_field("scores", tantivy::schema::STORED);
        let schema = sb.build();

        let mut hints = HashMap::new();
        hints.insert(
            "scores".to_string(),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        );

        let (arrow_schema, field_mapping) =
            tantivy_schema_to_arrow(&schema, None, Some(&hints)).unwrap();

        // Replicate Spark connector: stores {"_values": [90, 85, 92]} in JSON field
        let mut doc = tantivy::TantivyDocument::default();
        let mut obj = std::collections::BTreeMap::new();
        obj.insert(
            "_values".to_string(),
            OwnedValue::Array(vec![
                OwnedValue::I64(90),
                OwnedValue::I64(85),
                OwnedValue::I64(92),
            ]),
        );
        doc.add_object(json_field, obj);

        let batch = docs_to_record_batch(&[doc], &arrow_schema, &field_mapping).unwrap();
        let list_col = batch.column(0).as_any()
            .downcast_ref::<ListArray>()
            .expect("should be ListArray");

        assert!(!list_col.is_null(0), "row 0 should not be null");
        let row0 = list_col.value(0);
        let ints = row0.as_any()
            .downcast_ref::<arrow_array::Int32Array>()
            .expect("child should be Int32Array");

        assert_eq!(ints.len(), 3, "should have 3 elements from [90, 85, 92]");
        assert!(!ints.is_null(0), "element 0 should not be null");
        assert!(!ints.is_null(1), "element 1 should not be null");
        assert!(!ints.is_null(2), "element 2 should not be null");
        assert_eq!(ints.value(0), 90);
        assert_eq!(ints.value(1), 85);
        assert_eq!(ints.value(2), 92);
    }
}
