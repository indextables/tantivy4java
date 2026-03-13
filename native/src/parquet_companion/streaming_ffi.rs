// streaming_ffi.rs - Streaming Arrow FFI retrieval for large companion-mode result sets
//
// Implements a session-based streaming pipeline for returning millions of rows
// from companion-mode splits without materializing everything in memory.
//
// Architecture:
//   start_streaming_retrieval() → spawns tokio producer task
//   producer reads files sequentially, accumulates TARGET_BATCH_SIZE batches
//   sends batches through mpsc::channel(2) → consumer calls next_batch()
//
// Memory bound: ~24MB peak (2 × 12MB batches in channel) regardless of total rows.
//
// For result sets < 50K rows, use the fused path (fused_retrieval.rs) instead.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use quickwit_storage::Storage;
use tokio::sync::mpsc;

use crate::perf_println;
use super::arrow_ffi_export::rename_columns_to_tantivy;
use super::arrow_to_tant::read_parquet_batches_for_file;
use super::cached_reader::{ByteRangeCache, CoalesceConfig};
use super::manifest::{ColumnMapping, ParquetManifest};
use super::read_strategy::{ReadStrategy, compute_selectivity};
use super::transcode::MetadataCache;

/// Target rows per output batch. Balances Spark vectorized execution
/// efficiency with memory footprint.
/// At ~100 bytes/row average: 128K rows ≈ 12MB per batch.
const TARGET_BATCH_SIZE: usize = 128 * 1024;

/// Opaque handle for a streaming retrieval session.
/// Holds the consumer end of the channel and the output schema.
pub struct StreamingRetrievalSession {
    receiver: mpsc::Receiver<Result<RecordBatch>>,
    schema: Arc<Schema>,
    _producer_handle: Option<tokio::task::JoinHandle<()>>,
}

impl StreamingRetrievalSession {
    /// Create an empty session for queries with no matches.
    /// The receiver channel is already closed (sender dropped), so blocking_next
    /// will immediately return None.
    pub fn new_empty(
        receiver: mpsc::Receiver<Result<RecordBatch>>,
        schema: Arc<Schema>,
    ) -> Self {
        Self {
            receiver,
            schema,
            _producer_handle: None,
        }
    }

    /// Get the output schema (with tantivy field names).
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Get the number of columns in the output.
    pub fn num_columns(&self) -> usize {
        self.schema.fields().len()
    }

    /// Receive the next batch from the producer.
    /// Returns None when the stream is complete.
    pub fn blocking_next(&mut self) -> Option<Result<RecordBatch>> {
        self.receiver.blocking_recv()
    }
}

/// Start a streaming retrieval session.
///
/// Returns immediately with a session handle. The producer runs on the
/// tokio runtime and feeds batches through channel(2) — it works one
/// batch ahead while Java processes the current batch.
///
/// # Arguments
/// * `groups` - File groups: file_idx → [(original_index, row_in_file)]
/// * `projected_fields` - Optional field name projection
/// * `manifest` - Parquet companion manifest
/// * `storage` - Storage backend for reading parquet files
/// * `metadata_cache` - Optional parquet metadata cache
/// * `byte_cache` - Optional byte range cache
/// * `coalesce_config` - Optional coalescing configuration
pub fn start_streaming_retrieval(
    groups: HashMap<usize, Vec<(usize, u64)>>,
    projected_fields: Option<Vec<String>>,
    manifest: Arc<ParquetManifest>,
    storage: Arc<dyn Storage>,
    metadata_cache: Option<MetadataCache>,
    byte_cache: Option<ByteRangeCache>,
    coalesce_config: Option<CoalesceConfig>,
) -> Result<StreamingRetrievalSession> {
    // Determine output schema from manifest column mapping
    let tantivy_schema = build_tantivy_schema(&manifest, projected_fields.as_deref())?;

    let (tx, rx) = mpsc::channel::<Result<RecordBatch>>(2);

    let projected_fields_arc: Option<Arc<[String]>> =
        projected_fields.map(|f| f.into());

    let handle = tokio::spawn(async move {
        let tx_err = tx.clone();
        let result = produce_batches(
            tx,
            groups,
            projected_fields_arc,
            &manifest,
            &storage,
            metadata_cache.as_ref(),
            byte_cache.as_ref(),
            coalesce_config,
        )
        .await;

        if let Err(e) = result {
            let _ = tx_err.send(Err(e)).await;
        }
    });

    Ok(StreamingRetrievalSession {
        receiver: rx,
        schema: tantivy_schema,
        _producer_handle: Some(handle),
    })
}

/// Producer: reads files sequentially, emits TARGET_BATCH_SIZE batches.
///
/// Files are processed in file_idx order for deterministic output.
/// Rows within each file are already sorted by row_in_file (from Phase 2).
/// The BatchAccumulator combines small file results into larger batches.
async fn produce_batches(
    tx: mpsc::Sender<Result<RecordBatch>>,
    groups: HashMap<usize, Vec<(usize, u64)>>,
    projected_fields: Option<Arc<[String]>>,
    manifest: &ParquetManifest,
    storage: &Arc<dyn Storage>,
    metadata_cache: Option<&MetadataCache>,
    byte_cache: Option<&ByteRangeCache>,
    coalesce_config: Option<CoalesceConfig>,
) -> Result<()> {
    let t_total = std::time::Instant::now();
    let total_docs: usize = groups.values().map(|v| v.len()).sum();

    perf_println!(
        "⏱️ STREAMING: producer start — {} files, {} total docs",
        groups.len(), total_docs
    );

    // Sort file groups by file_idx for deterministic output order
    let mut sorted_groups: Vec<(usize, Vec<(usize, u64)>)> = groups.into_iter().collect();
    sorted_groups.sort_by_key(|(file_idx, _)| *file_idx);

    let mut accumulator = BatchAccumulator::new(TARGET_BATCH_SIZE);
    let mut files_processed = 0usize;
    let mut rows_emitted = 0usize;

    for (file_idx, rows) in sorted_groups {
        let t_file = std::time::Instant::now();
        let num_rows_in_file = rows.len();

        // Adaptive I/O strategy: select coalesce config based on per-file selectivity
        if file_idx >= manifest.parquet_files.len() {
            return Err(anyhow::anyhow!(
                "Invalid file_idx {} >= {} parquet files in manifest",
                file_idx, manifest.parquet_files.len()
            ));
        }
        let file_entry = &manifest.parquet_files[file_idx];
        let selectivity = compute_selectivity(num_rows_in_file, file_entry.num_rows as usize);
        let strategy = ReadStrategy::for_selectivity(selectivity);
        let effective_coalesce = coalesce_config
            .map(|base| strategy.coalesce_config(base))
            .or_else(|| {
                // When no base config provided, use strategy defaults with a reasonable base
                let base = CoalesceConfig {
                    max_gap: 512 * 1024,
                    max_total: 8 * 1024 * 1024,
                };
                Some(strategy.coalesce_config(base))
            });

        perf_println!(
            "⏱️ STREAMING: file[{}] selectivity={:.1}% strategy={:?}",
            file_idx, selectivity * 100.0, strategy
        );

        // Read this file's rows as RecordBatches
        let batches = read_parquet_batches_for_file(
            file_idx,
            &rows,
            projected_fields.as_deref().map(|s| s as &[String]),
            manifest,
            storage,
            metadata_cache,
            byte_cache,
            effective_coalesce,
        )
        .await?;

        perf_println!(
            "⏱️ STREAMING: file[{}] read {} rows in {} batches, took {}ms",
            file_idx, num_rows_in_file, batches.len(), t_file.elapsed().as_millis()
        );

        // Feed batches through the accumulator → emit TARGET_BATCH_SIZE chunks
        for batch in batches {
            let emitted = accumulator.push(batch)?;
            for emit_batch in emitted {
                let renamed = rename_columns_to_tantivy(&emit_batch, &manifest.column_mapping)?;
                let normalized = normalize_timestamps(renamed)?;
                rows_emitted += normalized.num_rows();
                if tx.send(Ok(normalized)).await.is_err() {
                    if rows_emitted == 0 {
                        return Err(anyhow::anyhow!(
                            "Consumer dropped before any data was sent — possible consumer error"
                        ));
                    }
                    perf_println!(
                        "⏱️ STREAMING: consumer dropped after {} rows — stopping producer",
                        rows_emitted
                    );
                    return Ok(());
                }
            }
        }

        files_processed += 1;
    }

    // Flush remaining rows
    if let Some(final_batch) = accumulator.flush()? {
        let renamed = rename_columns_to_tantivy(&final_batch, &manifest.column_mapping)?;
        let normalized = normalize_timestamps(renamed)?;
        rows_emitted += normalized.num_rows();
        let _ = tx.send(Ok(normalized)).await;
    }

    perf_println!(
        "⏱️ STREAMING: producer complete — {} files, {} rows emitted, took {}ms",
        files_processed, rows_emitted, t_total.elapsed().as_millis()
    );

    Ok(())
}

/// Accumulates RecordBatches from file reads into TARGET_BATCH_SIZE output batches.
///
/// Prevents emitting tiny batches (bad for Spark vectorized execution) and avoids
/// materializing everything at once (unbounded memory).
struct BatchAccumulator {
    target_size: usize,
    pending: Vec<RecordBatch>,
    pending_rows: usize,
}

impl BatchAccumulator {
    fn new(target_size: usize) -> Self {
        Self {
            target_size,
            pending: Vec::new(),
            pending_rows: 0,
        }
    }

    /// Push a batch. Returns any batches that should be emitted (0 or 1).
    fn push(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        let rows = batch.num_rows();
        self.pending.push(batch);
        self.pending_rows += rows;

        if self.pending_rows >= self.target_size {
            Ok(vec![self.drain()?])
        } else {
            Ok(vec![])
        }
    }

    /// Concatenate pending batches into one, reset accumulator.
    fn drain(&mut self) -> Result<RecordBatch> {
        let batches = std::mem::take(&mut self.pending);
        self.pending_rows = 0;
        if batches.len() == 1 {
            Ok(batches.into_iter().next().unwrap())
        } else {
            let schema = batches[0].schema();
            arrow::compute::concat_batches(&schema, batches.iter())
                .context("BatchAccumulator: failed to concat pending batches")
        }
    }

    /// Flush any remaining pending batches.
    fn flush(self) -> Result<Option<RecordBatch>> {
        if self.pending.is_empty() {
            Ok(None)
        } else if self.pending.len() == 1 {
            Ok(self.pending.into_iter().next())
        } else {
            let schema = self.pending[0].schema();
            Ok(Some(
                arrow::compute::concat_batches(&schema, self.pending.iter())
                    .context("BatchAccumulator: failed to flush pending batches")?,
            ))
        }
    }
}

/// Normalize timestamps to microseconds for Spark compatibility.
/// Spark only supports Timestamp(MICROSECOND).
fn normalize_timestamps(batch: RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    let mut needs_cast = false;

    // Check if any timestamp columns need casting
    for field in schema.fields() {
        if let DataType::Timestamp(unit, _) = field.data_type() {
            if *unit != TimeUnit::Microsecond {
                needs_cast = true;
                break;
            }
        }
    }

    if !needs_cast {
        return Ok(batch);
    }

    // Cast timestamp columns to microseconds
    let new_fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Timestamp(unit, tz) if *unit != TimeUnit::Microsecond => {
                Arc::new(f.as_ref().clone().with_data_type(
                    DataType::Timestamp(TimeUnit::Microsecond, tz.clone()),
                ))
            }
            _ => f.clone(),
        })
        .collect();

    let new_columns: Vec<_> = batch
        .columns()
        .iter()
        .zip(schema.fields().iter())
        .map(|(col, field)| match field.data_type() {
            DataType::Timestamp(unit, tz) if *unit != TimeUnit::Microsecond => {
                let target = DataType::Timestamp(TimeUnit::Microsecond, tz.clone());
                arrow::compute::cast(col.as_ref(), &target)
                    .context("Failed to cast timestamp to microseconds")
            }
            _ => Ok(col.clone()),
        })
        .collect::<Result<Vec<_>>>()?;

    let new_schema = Arc::new(Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns)
        .context("Failed to create timestamp-normalized RecordBatch")
}

/// Public entry point for building the tantivy schema from a manifest.
/// Used by JNI code to create schema for empty sessions.
pub fn build_tantivy_schema_pub(
    manifest: &ParquetManifest,
    projected_fields: Option<&[String]>,
) -> Result<Arc<Schema>> {
    build_tantivy_schema(manifest, projected_fields)
}

/// Build the output schema with tantivy field names from manifest column mapping.
/// Optionally filtered by projected_fields.
fn build_tantivy_schema(
    manifest: &ParquetManifest,
    projected_fields: Option<&[String]>,
) -> Result<Arc<Schema>> {
    let mappings: Vec<&ColumnMapping> = if let Some(fields) = projected_fields {
        manifest
            .column_mapping
            .iter()
            .filter(|m| fields.iter().any(|f| f == &m.tantivy_field_name))
            .collect()
    } else {
        manifest.column_mapping.iter().collect()
    };

    if mappings.is_empty() {
        anyhow::bail!("No matching columns found in manifest for streaming schema");
    }

    let fields: Vec<Field> = mappings
        .iter()
        .map(|m| {
            let dt = parquet_type_to_arrow_type(&m.parquet_type, &m.tantivy_type);
            Field::new(&m.tantivy_field_name, dt, true)
        })
        .collect();

    Ok(Arc::new(Schema::new(fields)))
}

/// Map manifest parquet_type string to Arrow DataType.
/// This is a best-effort mapping for schema construction; the actual data types
/// come from the parquet file metadata during reads.
fn parquet_type_to_arrow_type(parquet_type: &str, tantivy_type: &str) -> DataType {
    match tantivy_type {
        "IpAddr" => DataType::Utf8,
        "Bool" => DataType::Boolean,
        "F64" => DataType::Float64,
        "I64" => DataType::Int64,
        "U64" => DataType::UInt64,
        "Date" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "Bytes" => DataType::Binary,
        _ => match parquet_type {
            "BOOLEAN" => DataType::Boolean,
            "INT32" => DataType::Int32,
            "INT64" => DataType::Int64,
            "FLOAT" => DataType::Float32,
            "DOUBLE" => DataType::Float64,
            "BYTE_ARRAY" | "FIXED_LEN_BYTE_ARRAY" => DataType::Utf8,
            _ => DataType::Utf8,
        },
    }
}

/// Write a RecordBatch to pre-allocated FFI addresses.
/// Same pattern as arrow_ffi_export.rs but operates on a single batch.
///
/// IMPORTANT: If the FFI addresses have been written to before (e.g., in a
/// streaming loop), the previous `FFI_ArrowArray`/`FFI_ArrowSchema` values are
/// read and dropped first to release their Arrow buffer memory via the `release`
/// callback. Callers that allocate fresh zeroed memory for each call are also safe
/// (a zeroed `FFI_ArrowArray` has `release = null`, which means no-op on drop).
///
/// Timestamps are normalized to microseconds for Spark compatibility. The
/// streaming path's `produce_batches` already normalizes, so the check here
/// is a fast no-op in that case.
pub(crate) fn write_batch_to_ffi(
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

    let schema = batch.schema();
    for (i, col) in batch.columns().iter().enumerate() {
        if array_addrs[i] == 0 || schema_addrs[i] == 0 {
            anyhow::bail!(
                "Null FFI address for column {}: array_addr={}, schema_addr={}",
                i,
                array_addrs[i],
                schema_addrs[i]
            );
        }

        // Normalize timestamps to microseconds (fast no-op if already microsecond)
        let cast_col;
        let col_for_export: &dyn arrow_array::Array = match col.data_type() {
            DataType::Timestamp(unit, tz) if *unit != TimeUnit::Microsecond => {
                let target = DataType::Timestamp(TimeUnit::Microsecond, tz.clone());
                cast_col = arrow::compute::cast(col.as_ref(), &target)
                    .context(format!(
                        "Failed to cast column {} from {:?} to Timestamp(Microsecond)",
                        i,
                        col.data_type()
                    ))?;
                cast_col.as_ref()
            }
            _ => col.as_ref(),
        };

        // Handle non-zero offset
        let data = if col_for_export.offset() != 0 {
            let take_indices =
                arrow_array::UInt32Array::from_iter_values(0..batch.num_rows() as u32);
            arrow::compute::take(col_for_export, &take_indices, None)
                .context("Failed to normalize column offset via take()")?
                .to_data()
        } else {
            col_for_export.to_data()
        };

        let array_ptr = array_addrs[i] as *mut FFI_ArrowArray;
        let schema_ptr = schema_addrs[i] as *mut FFI_ArrowSchema;

        let orig_field = schema.field(i);
        let export_field: Field = match orig_field.data_type() {
            DataType::Timestamp(unit, tz) if *unit != TimeUnit::Microsecond => orig_field
                .as_ref()
                .clone()
                .with_data_type(DataType::Timestamp(TimeUnit::Microsecond, tz.clone())),
            _ => orig_field.as_ref().clone(),
        };

        unsafe {
            // Drop previous FFI contents to release Arrow buffers.
            // A zeroed struct (release = null) is safe to read+drop (no-op).
            let prev_array = std::ptr::read_unaligned(array_ptr);
            drop(prev_array);
            let prev_schema = std::ptr::read_unaligned(schema_ptr);
            drop(prev_schema);

            std::ptr::write_unaligned(array_ptr, FFI_ArrowArray::new(&data));
            std::ptr::write_unaligned(
                schema_ptr,
                FFI_ArrowSchema::try_from(&export_field).map_err(|e| {
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::*;

    #[test]
    fn test_batch_accumulator_basic() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let mut acc = BatchAccumulator::new(100);

        // Push small batch (50 rows) — should not emit
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..50))],
        )
        .unwrap();
        let emitted = acc.push(batch1).unwrap();
        assert!(emitted.is_empty());

        // Push another small batch (60 rows, total 110 >= 100) — should emit
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(50..110))],
        )
        .unwrap();
        let emitted = acc.push(batch2).unwrap();
        assert_eq!(emitted.len(), 1);
        assert_eq!(emitted[0].num_rows(), 110);
    }

    #[test]
    fn test_batch_accumulator_flush() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let mut acc = BatchAccumulator::new(1000);

        // Push small batch — won't emit
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..10))],
        )
        .unwrap();
        let emitted = acc.push(batch).unwrap();
        assert!(emitted.is_empty());

        // Flush should return the pending batch
        let flushed = acc.flush().unwrap();
        assert!(flushed.is_some());
        assert_eq!(flushed.unwrap().num_rows(), 10);
    }

    #[test]
    fn test_batch_accumulator_empty_flush() {
        let acc = BatchAccumulator::new(100);
        let flushed = acc.flush().unwrap();
        assert!(flushed.is_none());
    }

    #[test]
    fn test_normalize_timestamps_noop() {
        // No timestamp columns → should be a noop
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let result = normalize_timestamps(batch.clone()).unwrap();
        assert_eq!(result.schema(), batch.schema());
    }

    #[test]
    fn test_normalize_timestamps_cast() {
        // Nanosecond timestamps should be cast to microseconds
        let schema = Arc::new(Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(
                TimestampNanosecondArray::from(vec![1_000_000_000i64, 2_000_000_000i64]),
            )],
        )
        .unwrap();
        let result = normalize_timestamps(batch).unwrap();
        assert_eq!(
            *result.schema().field(0).data_type(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        let col = result
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert_eq!(col.value(0), 1_000_000); // ns → µs
        assert_eq!(col.value(1), 2_000_000);
    }

    #[test]
    fn test_parquet_type_to_arrow_type() {
        assert_eq!(parquet_type_to_arrow_type("INT64", "I64"), DataType::Int64);
        assert_eq!(parquet_type_to_arrow_type("BOOLEAN", "Bool"), DataType::Boolean);
        assert_eq!(parquet_type_to_arrow_type("DOUBLE", "F64"), DataType::Float64);
        assert_eq!(parquet_type_to_arrow_type("BYTE_ARRAY", "Str"), DataType::Utf8);
        assert_eq!(
            parquet_type_to_arrow_type("BYTE_ARRAY", "IpAddr"),
            DataType::Utf8
        );
        assert_eq!(
            parquet_type_to_arrow_type("INT64", "Date"),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }
}
