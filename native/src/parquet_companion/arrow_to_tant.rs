// arrow_to_tant.rs - Arrow to TANT binary serialization for parquet companion mode
//
// Converts Arrow RecordBatch data directly to TANT binary format, bypassing
// the expensive JSON intermediate (Rust serde_json serialize → UTF-8 → Java Jackson parse).
// This makes docBatch() work transparently for both standard and companion splits.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::DataType;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use quickwit_storage::Storage;

use super::cached_reader::{ByteRangeCache, CachedParquetReader, CoalesceConfig};
use super::doc_retrieval::{
    arrow_json_value, build_column_projection, build_row_selection_for_rows_in_selected_groups,
    compute_row_group_filter,
};
use super::docid_mapping::group_doc_addresses_by_file;
use super::manifest::{ColumnMapping, ParquetManifest};
use super::transcode::MetadataCache;

use crate::debug_println;

/// TANT binary format constants (same as batch_serialization.rs)
const MAGIC_NUMBER: u32 = 0x54414E54;
const FIELD_TYPE_TEXT: u8 = 0;
const FIELD_TYPE_INTEGER: u8 = 1;
const FIELD_TYPE_FLOAT: u8 = 2;
const FIELD_TYPE_BOOLEAN: u8 = 3;
const FIELD_TYPE_DATE: u8 = 4;
const FIELD_TYPE_BYTES: u8 = 5;
const FIELD_TYPE_JSON: u8 = 6;
const FIELD_TYPE_IP_ADDR: u8 = 7;
const FIELD_TYPE_UNSIGNED: u8 = 8;

/// Map Arrow DataType to TANT field type code, with optional override from column mapping.
fn arrow_type_to_tant_code(data_type: &DataType, tantivy_type: Option<&str>) -> u8 {
    // Check for IP address override: Utf8 column mapped as "IpAddr"
    if let Some("IpAddr") = tantivy_type {
        return FIELD_TYPE_IP_ADDR;
    }

    match data_type {
        DataType::Boolean => FIELD_TYPE_BOOLEAN,
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => FIELD_TYPE_INTEGER,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            FIELD_TYPE_UNSIGNED
        }
        DataType::Float32 | DataType::Float64 => FIELD_TYPE_FLOAT,
        DataType::Decimal128(_, _) => FIELD_TYPE_FLOAT,
        DataType::Decimal256(_, _) => FIELD_TYPE_TEXT,
        DataType::Utf8 | DataType::LargeUtf8 => FIELD_TYPE_TEXT,
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            FIELD_TYPE_BYTES
        }
        DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => FIELD_TYPE_DATE,
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::Map(_, _)
        | DataType::Struct(_) => FIELD_TYPE_JSON,
        _ => FIELD_TYPE_TEXT, // fallback
    }
}

/// Build per-column info: (tantivy_field_name, tant_type_code) for each column in the Arrow schema.
///
/// Uses column_mapping to resolve parquet column names → tantivy field names and detect
/// type overrides (e.g. Utf8 → IpAddr).
fn build_column_info(
    arrow_schema: &arrow_schema::SchemaRef,
    column_mapping: &[ColumnMapping],
) -> Vec<(String, u8)> {
    arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let parquet_name = field.name();

            // Reverse lookup: parquet column name → tantivy field name + type
            let mapping = column_mapping
                .iter()
                .find(|m| &m.parquet_column_name == parquet_name);

            let tantivy_name = mapping
                .map(|m| m.tantivy_field_name.clone())
                .unwrap_or_else(|| parquet_name.clone());

            let tantivy_type = mapping.map(|m| m.tantivy_type.as_str());
            let type_code = arrow_type_to_tant_code(field.data_type(), tantivy_type);

            (tantivy_name, type_code)
        })
        .collect()
}

/// Write a single Arrow value as TANT typed value bytes (just the value, not the type code or field header).
fn write_arrow_value_to_tant(
    buf: &mut Vec<u8>,
    array: &ArrayRef,
    row_idx: usize,
    data_type: &DataType,
) -> Result<()> {
    use arrow_array::*;

    match data_type {
        DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| anyhow::anyhow!("Expected BooleanArray array"))?;
            buf.push(if arr.value(row_idx) { 1 } else { 0 });
        }
        DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>().ok_or_else(|| anyhow::anyhow!("Expected Int8Array array"))?;
            buf.extend_from_slice(&(arr.value(row_idx) as i64).to_ne_bytes());
        }
        DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>().ok_or_else(|| anyhow::anyhow!("Expected Int16Array array"))?;
            buf.extend_from_slice(&(arr.value(row_idx) as i64).to_ne_bytes());
        }
        DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| anyhow::anyhow!("Expected Int32Array array"))?;
            buf.extend_from_slice(&(arr.value(row_idx) as i64).to_ne_bytes());
        }
        DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| anyhow::anyhow!("Expected Int64Array array"))?;
            buf.extend_from_slice(&arr.value(row_idx).to_ne_bytes());
        }
        DataType::UInt8 => {
            let arr = array.as_any().downcast_ref::<UInt8Array>().ok_or_else(|| anyhow::anyhow!("Expected UInt8Array array"))?;
            buf.extend_from_slice(&(arr.value(row_idx) as u64).to_ne_bytes());
        }
        DataType::UInt16 => {
            let arr = array.as_any().downcast_ref::<UInt16Array>().ok_or_else(|| anyhow::anyhow!("Expected UInt16Array array"))?;
            buf.extend_from_slice(&(arr.value(row_idx) as u64).to_ne_bytes());
        }
        DataType::UInt32 => {
            let arr = array.as_any().downcast_ref::<UInt32Array>().ok_or_else(|| anyhow::anyhow!("Expected UInt32Array array"))?;
            buf.extend_from_slice(&(arr.value(row_idx) as u64).to_ne_bytes());
        }
        DataType::UInt64 => {
            let arr = array.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| anyhow::anyhow!("Expected UInt64Array array"))?;
            buf.extend_from_slice(&arr.value(row_idx).to_ne_bytes());
        }
        DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>().ok_or_else(|| anyhow::anyhow!("Expected Float32Array array"))?;
            buf.extend_from_slice(&(arr.value(row_idx) as f64).to_ne_bytes());
        }
        DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>().ok_or_else(|| anyhow::anyhow!("Expected Float64Array array"))?;
            buf.extend_from_slice(&arr.value(row_idx).to_ne_bytes());
        }
        DataType::Decimal128(_, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal128Array>().ok_or_else(|| anyhow::anyhow!("Expected Decimal128Array array"))?;
            let raw = arr.value(row_idx) as f64;
            let val = raw / 10f64.powi(*scale as i32);
            buf.extend_from_slice(&val.to_ne_bytes());
        }
        DataType::Decimal256(_, scale) => {
            let arr = array.as_any().downcast_ref::<Decimal256Array>().ok_or_else(|| anyhow::anyhow!("Expected Decimal256Array array"))?;
            let raw = arr.value(row_idx);
            let s = if *scale == 0 {
                raw.to_string()
            } else {
                let val: f64 =
                    raw.to_string().parse::<f64>().unwrap_or(0.0) / 10f64.powi(*scale as i32);
                val.to_string()
            };
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_ne_bytes());
            buf.extend_from_slice(bytes);
        }
        DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>().ok_or_else(|| anyhow::anyhow!("Expected StringArray array"))?;
            let s = arr.value(row_idx);
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_ne_bytes());
            buf.extend_from_slice(bytes);
        }
        DataType::LargeUtf8 => {
            let arr = array.as_any().downcast_ref::<LargeStringArray>().ok_or_else(|| anyhow::anyhow!("Expected LargeStringArray array"))?;
            let s = arr.value(row_idx);
            let bytes = s.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_ne_bytes());
            buf.extend_from_slice(bytes);
        }
        DataType::Binary => {
            let arr = array.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| anyhow::anyhow!("Expected BinaryArray array"))?;
            let bytes = arr.value(row_idx);
            buf.extend_from_slice(&(bytes.len() as u32).to_ne_bytes());
            buf.extend_from_slice(bytes);
        }
        DataType::LargeBinary => {
            let arr = array.as_any().downcast_ref::<LargeBinaryArray>().ok_or_else(|| anyhow::anyhow!("Expected LargeBinaryArray array"))?;
            let bytes = arr.value(row_idx);
            buf.extend_from_slice(&(bytes.len() as u32).to_ne_bytes());
            buf.extend_from_slice(bytes);
        }
        DataType::FixedSizeBinary(_) => {
            let arr = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| anyhow::anyhow!("Expected FixedSizeBinaryArray array"))?;
            let bytes = arr.value(row_idx);
            buf.extend_from_slice(&(bytes.len() as u32).to_ne_bytes());
            buf.extend_from_slice(bytes);
        }
        DataType::Timestamp(unit, _) => {
            use arrow_schema::TimeUnit;
            let nanos: i64 = match unit {
                TimeUnit::Second => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(|| anyhow::anyhow!("Expected TimestampSecondArray array"))?;
                    arr.value(row_idx).checked_mul(1_000_000_000)
                        .ok_or_else(|| anyhow::anyhow!("Timestamp seconds overflow converting to nanos"))?
                }
                TimeUnit::Millisecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(|| anyhow::anyhow!("Expected TimestampMillisecondArray array"))?;
                    arr.value(row_idx).checked_mul(1_000_000)
                        .ok_or_else(|| anyhow::anyhow!("Timestamp millis overflow converting to nanos"))?
                }
                TimeUnit::Microsecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampMicrosecondArray>()
                        .ok_or_else(|| anyhow::anyhow!("Expected TimestampMicrosecondArray array"))?;
                    arr.value(row_idx).checked_mul(1_000)
                        .ok_or_else(|| anyhow::anyhow!("Timestamp micros overflow converting to nanos"))?
                }
                TimeUnit::Nanosecond => {
                    let arr = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .ok_or_else(|| anyhow::anyhow!("Expected TimestampNanosecondArray array"))?;
                    arr.value(row_idx)
                }
            };
            buf.extend_from_slice(&nanos.to_ne_bytes());
        }
        DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>().ok_or_else(|| anyhow::anyhow!("Expected Date32Array array"))?;
            let nanos = arr.value(row_idx) as i64 * 86_400 * 1_000_000_000;
            buf.extend_from_slice(&nanos.to_ne_bytes());
        }
        DataType::Date64 => {
            let arr = array.as_any().downcast_ref::<Date64Array>().ok_or_else(|| anyhow::anyhow!("Expected Date64Array array"))?;
            let nanos = arr.value(row_idx) * 1_000_000;
            buf.extend_from_slice(&nanos.to_ne_bytes());
        }
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::Map(_, _)
        | DataType::Struct(_) => {
            // Complex types: serialize via arrow_json_value, then length-prefixed string
            let slice = array.slice(row_idx, 1);
            let json_value = arrow_json_value(&slice, 0);
            let json_str =
                serde_json::to_string(&json_value).unwrap_or_else(|_| "null".to_string());
            let bytes = json_str.as_bytes();
            buf.extend_from_slice(&(bytes.len() as u32).to_ne_bytes());
            buf.extend_from_slice(bytes);
        }
        _ => {
            // Fallback: write as empty text
            buf.extend_from_slice(&0u32.to_ne_bytes());
        }
    }

    Ok(())
}

/// Extract a single row from a RecordBatch as TANT document bytes.
///
/// Null values are omitted from the output (matching existing behavior in extract_row_as_map).
fn extract_row_as_tant(
    batch: &RecordBatch,
    row_idx: usize,
    column_info: &[(String, u8)],
) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(256);

    // Reserve space for field_count (u16)
    let field_count_pos = buf.len();
    buf.extend_from_slice(&0u16.to_ne_bytes());

    let mut field_count = 0u16;

    for (col_idx, (field_name, type_code)) in column_info.iter().enumerate() {
        let array = batch.column(col_idx);

        if array.is_null(row_idx) {
            continue; // Skip null values
        }

        // Write field name
        let name_bytes = field_name.as_bytes();
        buf.extend_from_slice(&(name_bytes.len() as u16).to_ne_bytes());
        buf.extend_from_slice(name_bytes);

        // Write type code
        buf.push(*type_code);

        // Write value count (always 1 for parquet — each column is a single value)
        buf.extend_from_slice(&1u16.to_ne_bytes());

        // Write value
        write_arrow_value_to_tant(&mut buf, array, row_idx, array.data_type())?;

        field_count += 1;
    }

    // Patch field count at the reserved position
    let count_bytes = field_count.to_ne_bytes();
    buf[field_count_pos] = count_bytes[0];
    buf[field_count_pos + 1] = count_bytes[1];

    Ok(buf)
}

/// Assemble per-document TANT bytes into a complete TANT buffer with header/footer/offset table.
fn assemble_tant_buffer(doc_buffers: Vec<Option<Vec<u8>>>) -> Result<Vec<u8>> {
    let mut buffer = Vec::with_capacity(doc_buffers.len() * 256 + 16);

    // Header magic
    buffer.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    // Write documents and collect offsets
    let mut offsets = Vec::with_capacity(doc_buffers.len());
    for doc_opt in &doc_buffers {
        offsets.push(buffer.len() as u32);
        match doc_opt {
            Some(doc_bytes) => buffer.extend_from_slice(doc_bytes),
            None => {
                // Empty document: field_count = 0
                buffer.extend_from_slice(&0u16.to_ne_bytes());
            }
        }
    }

    // Offset table
    let offset_table_start = buffer.len() as u32;
    for offset in &offsets {
        buffer.extend_from_slice(&offset.to_ne_bytes());
    }

    // Footer: offset_table_pos + doc_count + footer_magic
    buffer.extend_from_slice(&offset_table_start.to_ne_bytes());
    buffer.extend_from_slice(&(doc_buffers.len() as u32).to_ne_bytes());
    buffer.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    Ok(buffer)
}

/// Batch retrieve documents from parquet files and serialize directly to TANT binary format.
///
/// This is the core optimization: instead of the JSON round-trip path
/// (Arrow → serde_json::Value → JSON bytes → Java Jackson parse → type conversion),
/// this writes Arrow data directly to TANT binary format that Java's BatchDocumentReader
/// can parse natively.
///
/// The function mirrors `batch_retrieve_from_parquet` in structure but produces TANT bytes
/// instead of `Vec<HashMap<String, serde_json::Value>>`.
pub async fn batch_parquet_to_tant_buffer(
    addresses: &[(u32, u32)], // (segment_ord, doc_id)
    projected_fields: Option<&[String]>,
    manifest: &ParquetManifest,
    storage: &Arc<dyn Storage>,
    metadata_cache: Option<&MetadataCache>,
    byte_cache: Option<&ByteRangeCache>,
    coalesce_config: Option<CoalesceConfig>,
) -> Result<Vec<u8>> {
    let groups =
        group_doc_addresses_by_file(addresses, manifest).map_err(|e| anyhow::anyhow!("{}", e))?;
    batch_parquet_to_tant_buffer_by_groups(
        groups, addresses.len(), projected_fields, manifest, storage,
        metadata_cache, byte_cache, coalesce_config,
    ).await
}

/// Core batch retrieval with pre-resolved file groups.
///
/// Accepts groups already resolved via fast fields (or legacy segment→global→file).
/// Each entry in groups is: file_idx → [(original_index, row_in_file)].
pub async fn batch_parquet_to_tant_buffer_by_groups(
    groups: std::collections::HashMap<usize, Vec<(usize, u64)>>,
    result_count: usize,
    projected_fields: Option<&[String]>,
    manifest: &ParquetManifest,
    storage: &Arc<dyn Storage>,
    metadata_cache: Option<&MetadataCache>,
    byte_cache: Option<&ByteRangeCache>,
    coalesce_config: Option<CoalesceConfig>,
) -> Result<Vec<u8>> {
    let t_total = std::time::Instant::now();
    perf_println!(
        "⏱️ PROJ_DIAG: batch_parquet_to_tant_buffer_by_groups START - {} files, {} result docs, projected_fields={:?}",
        groups.len(), result_count, projected_fields
    );

    // Share projected_fields across parallel tasks via Arc (avoid per-task Vec clone)
    let projected_fields_shared: Option<Arc<[String]>> =
        projected_fields.map(|f| f.into());

    // Process each file's rows in parallel
    let file_futures: Vec<_> = groups
        .into_iter()
        .map(|(file_idx, rows)| {
            let storage = storage.clone();
            let manifest = manifest.clone();
            let projected_fields_owned = projected_fields_shared.clone();
            let metadata_cache = metadata_cache.cloned();
            let byte_cache = byte_cache.cloned();

            async move {
                let t_file = std::time::Instant::now();
                let file_entry = &manifest.parquet_files[file_idx];
                let parquet_path = &file_entry.relative_path;

                perf_println!(
                    "⏱️ PROJ_DIAG: file[{}]='{}' retrieving {} rows, file_size={}",
                    file_idx, parquet_path, rows.len(), file_entry.file_size_bytes
                );

                // Check metadata cache to avoid re-reading footer from S3/Azure
                let path_buf = std::path::PathBuf::from(parquet_path);
                let cached_meta = metadata_cache.as_ref().and_then(|cache| {
                    cache
                        .lock()
                        .ok()
                        .and_then(|guard| guard.get(&path_buf).cloned())
                });
                let meta_was_cached = cached_meta.is_some();

                let reader = if let Some(meta) = cached_meta {
                    CachedParquetReader::with_metadata(
                        storage.clone(),
                        path_buf.clone(),
                        file_entry.file_size_bytes,
                        meta,
                    )
                } else {
                    CachedParquetReader::new(
                        storage.clone(),
                        path_buf.clone(),
                        file_entry.file_size_bytes,
                    )
                };
                let reader = if let Some(ref bc) = byte_cache {
                    reader.with_byte_cache(bc.clone())
                } else {
                    reader
                };
                let reader = if let Some(config) = coalesce_config {
                    reader.with_coalesce_config(config)
                } else {
                    reader
                };

                let t_builder = std::time::Instant::now();
                let builder = ParquetRecordBatchStreamBuilder::new(reader)
                    .await
                    .context("Failed to create parquet stream builder")?;
                perf_println!(
                    "⏱️ PROJ_DIAG: file[{}] ParquetRecordBatchStreamBuilder::new took {}ms (metadata_cached={})",
                    file_idx, t_builder.elapsed().as_millis(), meta_was_cached
                );

                // Cache the metadata for subsequent reads
                if let Some(cache) = &metadata_cache {
                    if let Ok(mut guard) = cache.lock() {
                        if !guard.contains_key(&path_buf) {
                            guard.insert(path_buf.clone(), builder.metadata().clone());
                        }
                    }
                }

                let parquet_schema = builder.schema().clone();
                let parquet_metadata = builder.metadata().clone();
                let parquet_file_schema = builder.parquet_schema().clone();

                let total_parquet_columns = parquet_schema.fields().len();
                perf_println!(
                    "⏱️ PROJ_DIAG: file[{}] parquet schema has {} columns, {} row groups",
                    file_idx, total_parquet_columns, parquet_metadata.num_row_groups()
                );

                // Build column projection
                let proj_fields = projected_fields_owned.as_deref();
                let projection = build_column_projection(
                    proj_fields,
                    &parquet_schema,
                    &manifest.column_mapping,
                );

                perf_println!(
                    "⏱️ PROJ_DIAG: file[{}] build_column_projection: input={:?}, output_indices={:?} (of {} total columns)",
                    file_idx,
                    proj_fields.map(|f| f.len()),
                    projection.as_ref().map(|p| p.clone()),
                    total_parquet_columns
                );

                let builder = if let Some(ref proj) = projection {
                    let mask = parquet::arrow::ProjectionMask::roots(
                        &parquet_file_schema,
                        proj.iter().cloned(),
                    );
                    perf_println!(
                        "⏱️ PROJ_DIAG: file[{}] applying ProjectionMask with {} root indices",
                        file_idx, proj.len()
                    );
                    builder.with_projection(mask)
                } else {
                    perf_println!(
                        "⏱️ PROJ_DIAG: file[{}] NO projection applied (reading ALL {} columns)",
                        file_idx, total_parquet_columns
                    );
                    builder
                };

                // Row indices within the file (already sorted by group_doc_addresses_by_file)
                let row_indices: Vec<usize> =
                    rows.iter().map(|(_, row)| *row as usize).collect();

                // Step 1: Determine which row groups contain our target rows
                let rg_filter = compute_row_group_filter(&row_indices, &parquet_metadata);

                // Step 2: Apply row group filter
                let builder = if let Some(ref filter) = rg_filter {
                    let selected_rgs: Vec<usize> = filter
                        .iter()
                        .enumerate()
                        .filter(|(_, selected)| **selected)
                        .map(|(idx, _)| idx)
                        .collect();

                    perf_println!(
                        "⏱️ PROJ_DIAG: file[{}] row_group_filter: selected {}/{} row groups (indices={:?})",
                        file_idx, selected_rgs.len(), parquet_metadata.num_row_groups(), selected_rgs
                    );

                    builder.with_row_groups(selected_rgs)
                } else {
                    perf_println!(
                        "⏱️ PROJ_DIAG: file[{}] row_group_filter: ALL {} row groups needed",
                        file_idx, parquet_metadata.num_row_groups()
                    );
                    builder
                };

                // Step 3: Build RowSelection within the selected row groups
                let row_selection = build_row_selection_for_rows_in_selected_groups(
                    &row_indices,
                    &parquet_metadata,
                    rg_filter.as_deref(),
                );
                let has_row_selection = row_selection.is_some();
                let builder = if let Some(selection) = row_selection {
                    builder.with_row_selection(selection)
                } else {
                    builder
                };
                perf_println!(
                    "⏱️ PROJ_DIAG: file[{}] row_selection applied={}, target_rows={:?}",
                    file_idx, has_row_selection, row_indices
                );

                let t_stream = std::time::Instant::now();
                let mut stream = builder
                    .build()
                    .context("Failed to build parquet record batch stream")?;
                perf_println!(
                    "⏱️ PROJ_DIAG: file[{}] stream.build() took {}ms",
                    file_idx, t_stream.elapsed().as_millis()
                );

                // Build column info AFTER projection is applied (schema may have fewer columns)
                // We need to wait until we see the first batch to get the projected schema.
                let mut column_info: Option<Vec<(String, u8)>> = None;

                // Collect TANT bytes for each row
                let mut collected_rows: Vec<Vec<u8>> = Vec::new();
                let mut batch_count = 0u32;
                use futures::StreamExt;
                let t_read = std::time::Instant::now();
                while let Some(batch_result) = stream.next().await {
                    let batch = batch_result.context("Failed to read parquet batch")?;
                    batch_count += 1;

                    if batch_count == 1 {
                        perf_println!(
                            "⏱️ PROJ_DIAG: file[{}] FIRST stream.next() took {}ms — batch has {} rows, {} columns",
                            file_idx, t_read.elapsed().as_millis(), batch.num_rows(), batch.num_columns()
                        );
                        // Log column names in the batch to verify projection
                        let batch_schema = batch.schema();
                        let col_names: Vec<&str> = batch_schema.fields().iter().map(|f| f.name().as_str()).collect();
                        perf_println!(
                            "⏱️ PROJ_DIAG: file[{}] batch column names: {:?}",
                            file_idx, col_names
                        );
                    }

                    // Build column info lazily from the projected batch schema
                    let info = column_info.get_or_insert_with(|| {
                        build_column_info(&batch.schema(), &manifest.column_mapping)
                    });

                    for row_idx in 0..batch.num_rows() {
                        let doc_bytes = extract_row_as_tant(&batch, row_idx, info)?;
                        collected_rows.push(doc_bytes);
                    }
                }
                perf_println!(
                    "⏱️ PROJ_DIAG: file[{}] stream exhausted: {} batches, {} rows total, read took {}ms",
                    file_idx, batch_count, collected_rows.len(), t_read.elapsed().as_millis()
                );

                perf_println!(
                    "⏱️ PROJ_DIAG: file[{}] TOTAL file processing took {}ms",
                    file_idx, t_file.elapsed().as_millis()
                );

                // Return (original_index, doc_bytes) pairs for reassembly
                let mut drain_iter = collected_rows.drain(..);
                let indexed_results: Vec<(usize, Vec<u8>)> = rows
                    .iter()
                    .map(|(original_idx, _)| {
                        let data = drain_iter.next().unwrap_or_default();
                        (*original_idx, data)
                    })
                    .collect();

                Ok::<_, anyhow::Error>(indexed_results)
            }
        })
        .collect();

    // Execute all file retrievals in parallel
    let all_file_results = futures::future::try_join_all(file_futures).await?;

    // Reassemble results in original order
    let mut doc_buffers: Vec<Option<Vec<u8>>> = vec![None; result_count];
    for file_results in all_file_results {
        for (original_idx, doc_bytes) in file_results {
            doc_buffers[original_idx] = Some(doc_bytes);
        }
    }

    let result = assemble_tant_buffer(doc_buffers);
    perf_println!(
        "⏱️ PROJ_DIAG: batch_parquet_to_tant_buffer_by_groups TOTAL took {}ms",
        t_total.elapsed().as_millis()
    );
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::*;
    use arrow_schema::{Field, Schema, TimeUnit};
    use std::sync::Arc;

    #[test]
    fn test_arrow_type_to_tant_code() {
        assert_eq!(
            arrow_type_to_tant_code(&DataType::Boolean, None),
            FIELD_TYPE_BOOLEAN
        );
        assert_eq!(
            arrow_type_to_tant_code(&DataType::Int32, None),
            FIELD_TYPE_INTEGER
        );
        assert_eq!(
            arrow_type_to_tant_code(&DataType::Int64, None),
            FIELD_TYPE_INTEGER
        );
        assert_eq!(
            arrow_type_to_tant_code(&DataType::UInt64, None),
            FIELD_TYPE_UNSIGNED
        );
        assert_eq!(
            arrow_type_to_tant_code(&DataType::Float64, None),
            FIELD_TYPE_FLOAT
        );
        assert_eq!(
            arrow_type_to_tant_code(&DataType::Utf8, None),
            FIELD_TYPE_TEXT
        );
        assert_eq!(
            arrow_type_to_tant_code(&DataType::Binary, None),
            FIELD_TYPE_BYTES
        );
        assert_eq!(
            arrow_type_to_tant_code(
                &DataType::Timestamp(TimeUnit::Nanosecond, None),
                None
            ),
            FIELD_TYPE_DATE
        );
        assert_eq!(
            arrow_type_to_tant_code(&DataType::Date32, None),
            FIELD_TYPE_DATE
        );
        assert_eq!(
            arrow_type_to_tant_code(&DataType::Decimal128(18, 6), None),
            FIELD_TYPE_FLOAT
        );
        assert_eq!(
            arrow_type_to_tant_code(&DataType::Decimal256(38, 10), None),
            FIELD_TYPE_TEXT
        );
    }

    #[test]
    fn test_ip_address_override() {
        // Utf8 with IpAddr mapping → IP_ADDR type code
        assert_eq!(
            arrow_type_to_tant_code(&DataType::Utf8, Some("IpAddr")),
            FIELD_TYPE_IP_ADDR
        );
        // Utf8 with non-IpAddr mapping → TEXT
        assert_eq!(
            arrow_type_to_tant_code(&DataType::Utf8, Some("Str")),
            FIELD_TYPE_TEXT
        );
        // Utf8 with no mapping → TEXT
        assert_eq!(
            arrow_type_to_tant_code(&DataType::Utf8, None),
            FIELD_TYPE_TEXT
        );
    }

    #[test]
    fn test_write_int_values() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![42i64, -100, i64::MAX]));
        let mut buf = Vec::new();
        write_arrow_value_to_tant(&mut buf, &array, 0, &DataType::Int64).unwrap();
        assert_eq!(buf.len(), 8);
        let val = i64::from_ne_bytes(buf[0..8].try_into().unwrap());
        assert_eq!(val, 42);

        buf.clear();
        write_arrow_value_to_tant(&mut buf, &array, 1, &DataType::Int64).unwrap();
        let val = i64::from_ne_bytes(buf[0..8].try_into().unwrap());
        assert_eq!(val, -100);
    }

    #[test]
    fn test_write_int8_promoted_to_i64() {
        let array: ArrayRef = Arc::new(Int8Array::from(vec![127i8]));
        let mut buf = Vec::new();
        write_arrow_value_to_tant(&mut buf, &array, 0, &DataType::Int8).unwrap();
        assert_eq!(buf.len(), 8);
        let val = i64::from_ne_bytes(buf[0..8].try_into().unwrap());
        assert_eq!(val, 127);
    }

    #[test]
    fn test_write_timestamp_nanos() {
        // Second → nanos
        let array: ArrayRef = Arc::new(TimestampSecondArray::from(vec![1_000i64]));
        let mut buf = Vec::new();
        write_arrow_value_to_tant(
            &mut buf,
            &array,
            0,
            &DataType::Timestamp(TimeUnit::Second, None),
        )
        .unwrap();
        let nanos = i64::from_ne_bytes(buf[0..8].try_into().unwrap());
        assert_eq!(nanos, 1_000_000_000_000); // 1000s × 1e9

        // Millisecond → nanos
        let array: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![1_500i64]));
        buf.clear();
        write_arrow_value_to_tant(
            &mut buf,
            &array,
            0,
            &DataType::Timestamp(TimeUnit::Millisecond, None),
        )
        .unwrap();
        let nanos = i64::from_ne_bytes(buf[0..8].try_into().unwrap());
        assert_eq!(nanos, 1_500_000_000); // 1500ms × 1e6

        // Microsecond → nanos
        let array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![2_000i64]));
        buf.clear();
        write_arrow_value_to_tant(
            &mut buf,
            &array,
            0,
            &DataType::Timestamp(TimeUnit::Microsecond, None),
        )
        .unwrap();
        let nanos = i64::from_ne_bytes(buf[0..8].try_into().unwrap());
        assert_eq!(nanos, 2_000_000); // 2000µs × 1e3

        // Nanosecond → nanos (passthrough)
        let array: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![123_456_789i64]));
        buf.clear();
        write_arrow_value_to_tant(
            &mut buf,
            &array,
            0,
            &DataType::Timestamp(TimeUnit::Nanosecond, None),
        )
        .unwrap();
        let nanos = i64::from_ne_bytes(buf[0..8].try_into().unwrap());
        assert_eq!(nanos, 123_456_789);
    }

    #[test]
    fn test_write_string_values() {
        let array: ArrayRef = Arc::new(StringArray::from(vec!["hello"]));
        let mut buf = Vec::new();
        write_arrow_value_to_tant(&mut buf, &array, 0, &DataType::Utf8).unwrap();
        // Should be: length (4 bytes) + "hello" (5 bytes)
        assert_eq!(buf.len(), 9);
        let len = u32::from_ne_bytes(buf[0..4].try_into().unwrap());
        assert_eq!(len, 5);
        assert_eq!(&buf[4..9], b"hello");
    }

    #[test]
    fn test_write_boolean() {
        let array: ArrayRef = Arc::new(BooleanArray::from(vec![true, false]));
        let mut buf = Vec::new();
        write_arrow_value_to_tant(&mut buf, &array, 0, &DataType::Boolean).unwrap();
        assert_eq!(buf.len(), 1);
        assert_eq!(buf[0], 1);

        buf.clear();
        write_arrow_value_to_tant(&mut buf, &array, 1, &DataType::Boolean).unwrap();
        assert_eq!(buf[0], 0);
    }

    #[test]
    fn test_write_binary() {
        let array: ArrayRef = Arc::new(BinaryArray::from(vec![&[0xDE, 0xAD, 0xBE, 0xEF][..]]));
        let mut buf = Vec::new();
        write_arrow_value_to_tant(&mut buf, &array, 0, &DataType::Binary).unwrap();
        // length (4) + data (4) = 8 bytes
        assert_eq!(buf.len(), 8);
        let len = u32::from_ne_bytes(buf[0..4].try_into().unwrap());
        assert_eq!(len, 4);
        assert_eq!(&buf[4..8], &[0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn test_full_record_batch() {
        // Build a RecordBatch with multiple column types
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("active", DataType::Boolean, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["alice", "bob"])),
                Arc::new(BooleanArray::from(vec![true, false])),
            ],
        )
        .unwrap();

        let column_info = vec![
            ("id".to_string(), FIELD_TYPE_INTEGER),
            ("name".to_string(), FIELD_TYPE_TEXT),
            ("active".to_string(), FIELD_TYPE_BOOLEAN),
        ];

        // Extract both rows
        let doc0 = extract_row_as_tant(&batch, 0, &column_info).unwrap();
        let doc1 = extract_row_as_tant(&batch, 1, &column_info).unwrap();

        // Assemble into TANT buffer
        let buffer =
            assemble_tant_buffer(vec![Some(doc0), Some(doc1)]).unwrap();

        // Verify magic number
        let header_magic = u32::from_ne_bytes(buffer[0..4].try_into().unwrap());
        assert_eq!(header_magic, MAGIC_NUMBER);

        // Verify footer
        let footer_magic = u32::from_ne_bytes(
            buffer[buffer.len() - 4..buffer.len()]
                .try_into()
                .unwrap(),
        );
        assert_eq!(footer_magic, MAGIC_NUMBER);

        // Verify document count
        let doc_count = u32::from_ne_bytes(
            buffer[buffer.len() - 8..buffer.len() - 4]
                .try_into()
                .unwrap(),
        );
        assert_eq!(doc_count, 2);

        // Verify first document: field_count = 3
        let offset_table_pos = u32::from_ne_bytes(
            buffer[buffer.len() - 12..buffer.len() - 8]
                .try_into()
                .unwrap(),
        ) as usize;
        let doc0_offset = u32::from_ne_bytes(
            buffer[offset_table_pos..offset_table_pos + 4]
                .try_into()
                .unwrap(),
        ) as usize;
        let field_count =
            u16::from_ne_bytes(buffer[doc0_offset..doc0_offset + 2].try_into().unwrap());
        assert_eq!(field_count, 3);
    }

    #[test]
    fn test_null_values_omitted() {
        // Build a RecordBatch with a null value
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true), // nullable
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec![None::<&str>])),
            ],
        )
        .unwrap();

        let column_info = vec![
            ("id".to_string(), FIELD_TYPE_INTEGER),
            ("name".to_string(), FIELD_TYPE_TEXT),
        ];

        let doc = extract_row_as_tant(&batch, 0, &column_info).unwrap();

        // field_count should be 1 (null "name" is omitted)
        let field_count = u16::from_ne_bytes(doc[0..2].try_into().unwrap());
        assert_eq!(field_count, 1);
    }

    #[test]
    fn test_build_column_info_with_mapping() {
        let arrow_schema: arrow_schema::SchemaRef = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("col_a", DataType::Int64, false),
            Field::new("col_b", DataType::Utf8, false),
        ]));

        let mapping = vec![
            ColumnMapping {
                tantivy_field_name: "my_int".to_string(),
                parquet_column_name: "col_a".to_string(),
                physical_ordinal: 0,
                parquet_type: "INT64".to_string(),
                tantivy_type: "I64".to_string(),
                field_id: None,
                fast_field_tokenizer: None,
            },
            ColumnMapping {
                tantivy_field_name: "my_ip".to_string(),
                parquet_column_name: "col_b".to_string(),
                physical_ordinal: 1,
                parquet_type: "BYTE_ARRAY".to_string(),
                tantivy_type: "IpAddr".to_string(),
                field_id: None,
                fast_field_tokenizer: None,
            },
        ];

        let info = build_column_info(&arrow_schema, &mapping);
        assert_eq!(info.len(), 2);
        assert_eq!(info[0], ("my_int".to_string(), FIELD_TYPE_INTEGER));
        assert_eq!(info[1], ("my_ip".to_string(), FIELD_TYPE_IP_ADDR));
    }

    #[test]
    fn test_build_column_info_no_mapping() {
        let arrow_schema: arrow_schema::SchemaRef = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("score", DataType::Float64, false),
        ]));

        let info = build_column_info(&arrow_schema, &[]);
        assert_eq!(info.len(), 1);
        assert_eq!(info[0], ("score".to_string(), FIELD_TYPE_FLOAT));
    }

    #[test]
    fn test_assemble_tant_buffer_empty() {
        let buffer = assemble_tant_buffer(vec![]).unwrap();

        // Header magic
        let header_magic = u32::from_ne_bytes(buffer[0..4].try_into().unwrap());
        assert_eq!(header_magic, MAGIC_NUMBER);

        // Footer magic
        let footer_magic = u32::from_ne_bytes(
            buffer[buffer.len() - 4..buffer.len()]
                .try_into()
                .unwrap(),
        );
        assert_eq!(footer_magic, MAGIC_NUMBER);

        // Doc count = 0
        let doc_count = u32::from_ne_bytes(
            buffer[buffer.len() - 8..buffer.len() - 4]
                .try_into()
                .unwrap(),
        );
        assert_eq!(doc_count, 0);
    }
}
