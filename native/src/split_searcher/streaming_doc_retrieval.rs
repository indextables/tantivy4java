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

use anyhow::{Context, Result};
use arrow_array::builder::*;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
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

/// Convert an IPv6 address to its canonical display form.
fn canonical_ip_string(ip: &std::net::Ipv6Addr) -> String {
    match ip.to_ipv4_mapped() {
        Some(v4) => v4.to_string(),
        None => ip.to_string(),
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

#[cfg(test)]
mod tests {
    use super::*;
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
}
