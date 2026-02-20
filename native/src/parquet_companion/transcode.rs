// transcode.rs - Parquet to tantivy columnar transcoding (Phase 2)
//
// Converts parquet column data into tantivy's columnar format for fast field access.
// Used by ParquetAugmentedDirectory to serve fast field reads from parquet data.
//
// Strategy:
//   1. Read specified columns from parquet files via CachedParquetReader
//   2. For each row, record the value into ColumnarWriter using the appropriate type method
//   3. Serialize the ColumnarWriter to produce valid .fast file bytes
//   4. In hybrid mode: merge native numeric columns with parquet string columns
//   5. In parquet-only mode: transcode ALL columns from parquet

use std::collections::HashMap;
use std::io::Write;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use arrow_array::{Array, RecordBatch};
use arrow_schema::DataType;

use tantivy::columnar::{ColumnType, ColumnarWriter};
use tantivy::columnar::column_index::{serialize_column_index, SerializableColumnIndex, SerializableOptionalIndex};
use tantivy::columnar::column_values::{serialize_u64_based_column_values, CodecType};
#[allow(unused_imports)]
use tantivy::columnar::iterable::Iterable; // needed for Box<dyn Iterable> coercion

use sstable::{Dictionary, RangeSSTable, VoidSSTable};

use quickwit_storage::Storage;

use super::cached_reader::CachedParquetReader;
use super::manifest::{FastFieldMode, ParquetManifest};

/// Shared metadata cache type — reused across doc retrieval and transcoding
/// to avoid redundant footer reads from S3/Azure.
pub type MetadataCache = Arc<Mutex<HashMap<std::path::PathBuf, Arc<parquet::file::metadata::ParquetMetaData>>>>;

use crate::debug_println;

/// Source of data for a fast field column
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldSource {
    /// Use native tantivy columnar data
    Native,
    /// Decode from parquet at query time
    Parquet,
}

/// Determine the data source for a fast field based on column type and mode.
pub fn field_source(column_type: &str, mode: FastFieldMode) -> FieldSource {
    match mode {
        FastFieldMode::Disabled => FieldSource::Native,
        FastFieldMode::Hybrid => {
            // Numeric/bool/date/ip → native, string/bytes → parquet
            match column_type {
                "I64" | "U64" | "F64" | "Bool" | "Date" | "IpAddr" => FieldSource::Native,
                "Str" | "Bytes" => FieldSource::Parquet,
                _ => FieldSource::Native, // Default to native for unknown types
            }
        }
        FastFieldMode::ParquetOnly => FieldSource::Parquet,
    }
}

/// Information about a column to transcode from parquet
#[derive(Debug, Clone)]
pub struct TranscodeColumn {
    /// Tantivy field name (used as column name in ColumnarWriter)
    pub tantivy_name: String,
    /// Parquet column name (may differ from tantivy name)
    pub parquet_name: String,
    /// Tantivy type string (e.g. "I64", "Str", "Bool")
    pub tantivy_type: String,
    /// Column index in parquet schema
    pub parquet_col_idx: usize,
    /// Fast field tokenizer for text columns (e.g. "raw", "default").
    /// When set to a non-"raw" tokenizer, transcoding applies tokenization to
    /// produce individual term entries in the columnar dictionary.
    pub fast_field_tokenizer: Option<String>,
}

/// Transcode parquet columns into tantivy columnar format (.fast file bytes).
///
/// String columns use a direct serialization path that bypasses ColumnarWriter,
/// building tantivy columnar bytes directly from arrow StringArray buffers.
/// Non-string columns use the traditional ColumnarWriter row-by-row path.
///
/// # Arguments
/// * `columns` - Columns to transcode from parquet
/// * `manifest` - The parquet manifest
/// * `storage` - Storage for reading parquet files
/// * `num_docs` - Total number of documents in the segment
/// * `metadata_cache` - Optional shared metadata cache (avoids re-reading footers)
///
/// # Returns
/// Bytes in tantivy columnar format suitable for .fast file
pub async fn transcode_columns_from_parquet(
    columns: &[TranscodeColumn],
    manifest: &ParquetManifest,
    storage: &Arc<dyn Storage>,
    num_docs: u32,
    metadata_cache: Option<&MetadataCache>,
) -> Result<Vec<u8>> {
    if columns.is_empty() {
        let mut writer = ColumnarWriter::default();
        let mut output = Vec::new();
        writer.serialize(num_docs, &mut output)
            .context("Failed to serialize empty columnar")?;
        return Ok(output);
    }

    // Split columns into string (direct path) and non-string (ColumnarWriter path)
    let str_columns: Vec<TranscodeColumn> = columns.iter()
        .filter(|c| c.tantivy_type == "Str")
        .cloned()
        .collect();
    let other_columns: Vec<TranscodeColumn> = columns.iter()
        .filter(|c| c.tantivy_type != "Str")
        .cloned()
        .collect();

    debug_println!(
        "📊 TRANSCODE: Transcoding {} columns ({} str direct, {} via ColumnarWriter) from parquet ({} docs across {} files)",
        columns.len(), str_columns.len(), other_columns.len(), num_docs, manifest.parquet_files.len()
    );

    if str_columns.is_empty() {
        // All non-string: use existing ColumnarWriter path
        return transcode_via_columnar_writer(&other_columns, manifest, storage, num_docs, metadata_cache).await;
    }

    if other_columns.is_empty() {
        // All string: use direct serialization path
        return transcode_str_columns_direct(&str_columns, manifest, storage, num_docs, metadata_cache).await;
    }

    // Both types: serialize each independently and merge
    let other_bytes = transcode_via_columnar_writer(&other_columns, manifest, storage, num_docs, metadata_cache).await?;
    let str_bytes = transcode_str_columns_direct(&str_columns, manifest, storage, num_docs, metadata_cache).await?;

    // merge_two_columnars expects first arg WITH tantivy footer, second arg raw
    let other_wrapped = wrap_with_tantivy_footer(&other_bytes);
    merge_two_columnars(Some(&other_wrapped), &str_bytes)
}

/// Transcode columns via tantivy's ColumnarWriter (row-by-row path).
/// Used for non-string column types (numeric, bool, date, bytes, etc.).
async fn transcode_via_columnar_writer(
    columns: &[TranscodeColumn],
    manifest: &ParquetManifest,
    storage: &Arc<dyn Storage>,
    num_docs: u32,
    metadata_cache: Option<&MetadataCache>,
) -> Result<Vec<u8>> {
    let mut writer = ColumnarWriter::default();
    let parquet_col_names: Vec<&str> = columns.iter().map(|c| c.parquet_name.as_str()).collect();

    let mut global_row: u32 = 0;
    for file_entry in &manifest.parquet_files {
        let parquet_path = &file_entry.relative_path;
        let path_buf = std::path::PathBuf::from(parquet_path);

        let cached_meta = metadata_cache.and_then(|cache| {
            cache.lock().ok().and_then(|guard| guard.get(&path_buf).cloned())
        });

        let reader = if let Some(meta) = cached_meta {
            debug_println!("📊 TRANSCODE: Using cached metadata for {:?}", parquet_path);
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

        let builder = parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .context("Failed to create parquet stream builder for transcode")?;

        if let Some(cache) = metadata_cache {
            if let Ok(mut guard) = cache.lock() {
                if !guard.contains_key(&path_buf) {
                    guard.insert(path_buf.clone(), builder.metadata().clone());
                    debug_println!("📊 TRANSCODE: Cached metadata for {:?}", parquet_path);
                }
            }
        }

        let parquet_schema = builder.schema().clone();
        let parquet_file_schema = builder.parquet_schema().clone();

        let col_indices: Vec<usize> = parquet_col_names
            .iter()
            .filter_map(|name| {
                parquet_schema.fields().iter().position(|f| f.name() == *name)
            })
            .collect();

        let builder = if !col_indices.is_empty() {
            builder.with_projection(parquet::arrow::ProjectionMask::roots(
                &parquet_file_schema,
                col_indices.iter().cloned(),
            ))
        } else {
            builder
        };

        let mut stream = builder.build()
            .context("Failed to build parquet stream for transcode")?;

        use futures::StreamExt;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.context("Failed to read parquet batch for transcode")?;
            record_batch_to_columnar(&batch, &mut writer, columns, global_row)?;
            global_row += batch.num_rows() as u32;
        }
    }

    debug_println!(
        "📊 TRANSCODE: ColumnarWriter recorded {} rows, serializing...",
        global_row
    );

    let mut output = Vec::new();
    writer.serialize(num_docs, &mut output)
        .context("Failed to serialize transcoded columnar")?;

    debug_println!(
        "📊 TRANSCODE: ColumnarWriter serialized {} bytes of columnar data",
        output.len()
    );

    Ok(output)
}

/// Collected data for a single string column, ready for direct serialization.
struct StrColumnData {
    /// Tantivy column name
    name: String,
    /// Unique terms sorted lexicographically
    sorted_terms: Vec<Vec<u8>>,
    /// Per-non-null-row ordinal (indexes into sorted_terms after sort)
    ordinals: Vec<u64>,
    /// Doc IDs that have non-null values (None = all rows have values, Full cardinality)
    non_null_doc_ids: Option<Vec<u32>>,
}

/// Direct string column transcoding that bypasses ColumnarWriter.
///
/// Builds tantivy columnar bytes directly from arrow StringArray buffers:
/// - Zero per-row String allocation (reads bytes directly from arrow buffer)
/// - Single hash lookup per row (HashMap::get on &[u8], alloc only for new terms)
/// - No ColumnarWriter stacker replay during serialization
///
/// The output is raw tantivy columnar bytes (no tantivy Footer).
async fn transcode_str_columns_direct(
    str_columns: &[TranscodeColumn],
    manifest: &ParquetManifest,
    storage: &Arc<dyn Storage>,
    num_docs: u32,
    metadata_cache: Option<&MetadataCache>,
) -> Result<Vec<u8>> {
    use arrow_array::{StringArray, LargeStringArray};

    // Per-column collection state
    struct Collector {
        name: String,
        parquet_name: String,
        dict: HashMap<Vec<u8>, u32>,
        ordinals: Vec<u64>,
        non_null_doc_ids: Vec<u32>,
        has_nulls: bool,
    }

    let mut collectors: Vec<Collector> = str_columns.iter().map(|c| Collector {
        name: c.tantivy_name.clone(),
        parquet_name: c.parquet_name.clone(),
        dict: HashMap::new(),
        ordinals: Vec::new(),
        non_null_doc_ids: Vec::new(),
        has_nulls: false,
    }).collect();

    let parquet_col_names: Vec<&str> = str_columns.iter().map(|c| c.parquet_name.as_str()).collect();

    // Process each parquet file sequentially (rows must be recorded in order)
    let mut global_row: u32 = 0;
    for file_entry in &manifest.parquet_files {
        let parquet_path = &file_entry.relative_path;
        let path_buf = std::path::PathBuf::from(parquet_path);

        let cached_meta = metadata_cache.and_then(|cache| {
            cache.lock().ok().and_then(|guard| guard.get(&path_buf).cloned())
        });

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

        let builder = parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .context("Failed to create parquet stream builder for str transcode")?;

        if let Some(cache) = metadata_cache {
            if let Ok(mut guard) = cache.lock() {
                if !guard.contains_key(&path_buf) {
                    guard.insert(path_buf.clone(), builder.metadata().clone());
                }
            }
        }

        let parquet_schema = builder.schema().clone();
        let parquet_file_schema = builder.parquet_schema().clone();

        let col_indices: Vec<usize> = parquet_col_names
            .iter()
            .filter_map(|name| {
                parquet_schema.fields().iter().position(|f| f.name() == *name)
            })
            .collect();

        let builder = if !col_indices.is_empty() {
            builder.with_projection(parquet::arrow::ProjectionMask::roots(
                &parquet_file_schema,
                col_indices.iter().cloned(),
            ))
        } else {
            builder
        };

        let mut stream = builder.build()
            .context("Failed to build parquet stream for str transcode")?;

        use futures::StreamExt;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.context("Failed to read parquet batch for str transcode")?;
            let batch_schema = batch.schema();

            for collector in collectors.iter_mut() {
                let col_idx = match batch_schema.fields().iter()
                    .position(|f| f.name() == &collector.parquet_name)
                {
                    Some(idx) => idx,
                    None => continue,
                };

                let array = batch.column(col_idx);

                // Extract strings zero-copy from arrow buffers
                match array.data_type() {
                    DataType::Utf8 => {
                        let str_array = array.as_any().downcast_ref::<StringArray>()
                            .context("Expected StringArray for Utf8 column")?;
                        for row in 0..str_array.len() {
                            let doc_id = global_row + row as u32;
                            if str_array.is_null(row) {
                                collector.has_nulls = true;
                                continue;
                            }
                            let bytes = str_array.value(row).as_bytes();
                            let ord = if let Some(&existing) = collector.dict.get(bytes) {
                                existing
                            } else {
                                let next_id = collector.dict.len() as u32;
                                collector.dict.insert(bytes.to_vec(), next_id);
                                next_id
                            };
                            collector.ordinals.push(ord as u64);
                            collector.non_null_doc_ids.push(doc_id);
                        }
                    }
                    DataType::LargeUtf8 => {
                        let str_array = array.as_any().downcast_ref::<LargeStringArray>()
                            .context("Expected LargeStringArray for LargeUtf8 column")?;
                        for row in 0..str_array.len() {
                            let doc_id = global_row + row as u32;
                            if str_array.is_null(row) {
                                collector.has_nulls = true;
                                continue;
                            }
                            let bytes = str_array.value(row).as_bytes();
                            let ord = if let Some(&existing) = collector.dict.get(bytes) {
                                existing
                            } else {
                                let next_id = collector.dict.len() as u32;
                                collector.dict.insert(bytes.to_vec(), next_id);
                                next_id
                            };
                            collector.ordinals.push(ord as u64);
                            collector.non_null_doc_ids.push(doc_id);
                        }
                    }
                    other => {
                        anyhow::bail!(
                            "Unexpected data type {:?} for Str column '{}'",
                            other, collector.name
                        );
                    }
                }
            }

            global_row += batch.num_rows() as u32;
        }
    }

    debug_println!(
        "📊 TRANSCODE: Direct str collection complete: {} rows, {} columns",
        global_row, collectors.len()
    );

    // Post-process: sort dictionaries, remap ordinals, build StrColumnData
    let column_data: Vec<StrColumnData> = collectors.into_iter().map(|collector| {
        // Sort dictionary entries lexicographically
        let mut entries: Vec<(Vec<u8>, u32)> = collector.dict.into_iter().collect();
        entries.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        // Build old-to-new ordinal remap
        let mut old_to_new = vec![0u64; entries.len()];
        let sorted_terms: Vec<Vec<u8>> = entries.into_iter()
            .enumerate()
            .map(|(new_ord, (term, old_ord))| {
                old_to_new[old_ord as usize] = new_ord as u64;
                term
            })
            .collect();

        // Remap ordinals from insertion order to sorted order
        let ordinals: Vec<u64> = collector.ordinals.into_iter()
            .map(|o| old_to_new[o as usize])
            .collect();

        let non_null_doc_ids = if collector.has_nulls {
            Some(collector.non_null_doc_ids)
        } else {
            None
        };

        StrColumnData {
            name: collector.name,
            sorted_terms,
            ordinals,
            non_null_doc_ids,
        }
    }).collect();

    let result = serialize_str_columnar(column_data, num_docs)?;

    debug_println!(
        "📊 TRANSCODE: Direct str serialized {} bytes of columnar data",
        result.len()
    );

    Ok(result)
}

/// Build a complete tantivy columnar file containing only string columns,
/// using direct serialization (bypassing ColumnarWriter).
///
/// Columnar file format:
///   [col1_data][col2_data]...[RangeSSTable][sstable_len: u64 LE][num_rows: u32 LE][version+magic: 8 bytes]
///
/// Each string column's data:
///   [dictionary][column_index][column_values][col_index_num_bytes: u32 LE][dict_len: u32 LE]
fn serialize_str_columnar(
    mut columns: Vec<StrColumnData>,
    num_docs: u32,
) -> Result<Vec<u8>> {
    // Sort columns by name (RangeSSTable envelope requires sorted keys)
    columns.sort_unstable_by(|a, b| a.name.cmp(&b.name));

    let mut buf: Vec<u8> = Vec::new();
    let mut column_entries: Vec<(Vec<u8>, std::ops::Range<u64>)> = Vec::new();

    for col in &columns {
        let start_offset = buf.len() as u64;

        // 1. Write term dictionary (VoidSSTable — maps sorted terms to ())
        let dict_buf = {
            let mut dict_writer = Dictionary::<VoidSSTable>::builder(Vec::new())
                .context("Failed to create VoidSSTable dictionary builder")?;
            for term in &col.sorted_terms {
                dict_writer.insert(term, &())
                    .context("Failed to insert term into dictionary")?;
            }
            dict_writer.finish()
                .context("Failed to finalize term dictionary")?
        };
        let dict_len = dict_buf.len() as u32;
        buf.extend_from_slice(&dict_buf);

        // 2. Write column index (Full if all rows present, Optional if nulls exist)
        let col_index = if let Some(ref doc_ids) = col.non_null_doc_ids {
            SerializableColumnIndex::Optional(SerializableOptionalIndex {
                non_null_row_ids: Box::new(&doc_ids[..]),
                num_rows: num_docs,
            })
        } else {
            SerializableColumnIndex::Full
        };
        let col_index_num_bytes = serialize_column_index(col_index, &mut buf)
            .context("Failed to serialize column index")?;

        // 3. Write column values (ordinals as u64, bitpacked)
        serialize_u64_based_column_values(
            &&col.ordinals[..],
            &[CodecType::Bitpacked, CodecType::BlockwiseLinear],
            &mut buf,
        ).context("Failed to serialize column values")?;

        // 4. Write column_index_num_bytes trailer (u32 LE)
        buf.write_all(&col_index_num_bytes.to_le_bytes())
            .context("Failed to write column index length")?;

        // 5. Write dict_len trailer (u32 LE)
        buf.write_all(&dict_len.to_le_bytes())
            .context("Failed to write dictionary length")?;

        let end_offset = buf.len() as u64;

        // Build envelope key: column_name + \0 (JSON_END_OF_PATH) + type_code
        let mut key = Vec::with_capacity(col.name.len() + 2);
        key.extend_from_slice(col.name.as_bytes());
        key.push(0u8); // JSON_END_OF_PATH
        key.push(ColumnType::Str.to_code());
        column_entries.push((key, start_offset..end_offset));
    }

    // Write RangeSSTable envelope index (maps column keys to byte ranges)
    let mut sstable_writer = Dictionary::<RangeSSTable>::builder(Vec::new())
        .context("Failed to create RangeSSTable envelope builder")?;
    for (key, range) in &column_entries {
        sstable_writer.insert(key, range)
            .context("Failed to insert column range into envelope")?;
    }
    let sstable_bytes = sstable_writer.finish()
        .context("Failed to finalize envelope SSTable")?;
    let sstable_len = sstable_bytes.len() as u64;
    buf.extend_from_slice(&sstable_bytes);
    buf.write_all(&sstable_len.to_le_bytes())
        .context("Failed to write sstable length")?;

    // Write num_rows footer (u32 LE, matches BinarySerializable for u32)
    buf.write_all(&num_docs.to_le_bytes())
        .context("Failed to write num_rows")?;

    // Write columnar format version + magic footer
    // Version::V2 = 2u32 LE, MAGIC_BYTES = [2, 113, 119, 66]
    buf.write_all(&[2u8, 0, 0, 0, 2, 113, 119, 66])
        .context("Failed to write version footer")?;

    Ok(buf)
}

/// Record all rows from a RecordBatch into a ColumnarWriter.
///
/// Maps arrow array types to the appropriate ColumnarWriter record_* method.
fn record_batch_to_columnar(
    batch: &RecordBatch,
    writer: &mut ColumnarWriter,
    columns: &[TranscodeColumn],
    start_row: u32,
) -> Result<()> {
    let schema = batch.schema();

    for col_info in columns {
        // Find this column in the batch by parquet name
        let col_idx = match schema.fields().iter().position(|f| f.name() == &col_info.parquet_name) {
            Some(idx) => idx,
            None => continue, // Column not in this batch (projection didn't include it)
        };

        let array = batch.column(col_idx);
        let tantivy_name = &col_info.tantivy_name;

        for row in 0..batch.num_rows() {
            let doc_id = start_row + row as u32;

            if array.is_null(row) {
                continue; // Skip nulls — tantivy handles absent values natively
            }

            record_arrow_value(writer, doc_id, tantivy_name, array, row, &col_info.tantivy_type)?;
        }
    }

    Ok(())
}

/// Record a single Arrow array value into the ColumnarWriter.
fn record_arrow_value(
    writer: &mut ColumnarWriter,
    doc_id: u32,
    column_name: &str,
    array: &dyn Array,
    row: usize,
    tantivy_type: &str,
) -> Result<()> {
    use arrow_array::*;

    match tantivy_type {
        "I64" => {
            let val = extract_i64(array, row)?;
            writer.record_numerical(doc_id, column_name, val);
        }
        "U64" => {
            let val = extract_u64(array, row)?;
            writer.record_numerical(doc_id, column_name, val);
        }
        "F64" => {
            let val = extract_f64(array, row)?;
            writer.record_numerical(doc_id, column_name, val);
        }
        "Bool" => {
            let arr = array.as_any().downcast_ref::<BooleanArray>()
                .context("Expected BooleanArray for Bool column")?;
            writer.record_bool(doc_id, column_name, arr.value(row));
        }
        "Str" => {
            let val = extract_string(array, row)?;
            writer.record_str(doc_id, column_name, &val);
        }
        "Bytes" => {
            let val = extract_bytes(array, row)?;
            writer.record_bytes(doc_id, column_name, &val);
        }
        "Date" => {
            let micros = extract_timestamp_micros(array, row)?;
            // Tantivy DateTime uses nanoseconds internally
            let nanos = micros * 1000;
            let dt = tantivy::DateTime::from_timestamp_nanos(nanos);
            writer.record_datetime(doc_id, column_name, dt);
        }
        _ => {
            // Unknown type, skip
            debug_println!(
                "📊 TRANSCODE: Skipping unknown tantivy type '{}' for column '{}'",
                tantivy_type, column_name
            );
        }
    }

    Ok(())
}

/// Extract an i64 value from various Arrow integer types.
fn extract_i64(array: &dyn Array, row: usize) -> Result<i64> {
    use arrow_array::*;
    match array.data_type() {
        DataType::Int8 => Ok(array.as_any().downcast_ref::<Int8Array>().ok_or_else(|| anyhow::anyhow!("Expected Int8Array array"))?.value(row) as i64),
        DataType::Int16 => Ok(array.as_any().downcast_ref::<Int16Array>().ok_or_else(|| anyhow::anyhow!("Expected Int16Array array"))?.value(row) as i64),
        DataType::Int32 => Ok(array.as_any().downcast_ref::<Int32Array>().ok_or_else(|| anyhow::anyhow!("Expected Int32Array array"))?.value(row) as i64),
        DataType::Int64 => Ok(array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| anyhow::anyhow!("Expected Int64Array array"))?.value(row)),
        _ => anyhow::bail!("Cannot extract i64 from {:?}", array.data_type()),
    }
}

/// Extract a u64 value from various Arrow unsigned integer types.
fn extract_u64(array: &dyn Array, row: usize) -> Result<u64> {
    use arrow_array::*;
    match array.data_type() {
        DataType::UInt8 => Ok(array.as_any().downcast_ref::<UInt8Array>().ok_or_else(|| anyhow::anyhow!("Expected UInt8Array array"))?.value(row) as u64),
        DataType::UInt16 => Ok(array.as_any().downcast_ref::<UInt16Array>().ok_or_else(|| anyhow::anyhow!("Expected UInt16Array array"))?.value(row) as u64),
        DataType::UInt32 => Ok(array.as_any().downcast_ref::<UInt32Array>().ok_or_else(|| anyhow::anyhow!("Expected UInt32Array array"))?.value(row) as u64),
        DataType::UInt64 => Ok(array.as_any().downcast_ref::<UInt64Array>().ok_or_else(|| anyhow::anyhow!("Expected UInt64Array array"))?.value(row)),
        // Also support signed types coerced to u64
        DataType::Int64 => Ok(array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| anyhow::anyhow!("Expected Int64Array array"))?.value(row) as u64),
        _ => anyhow::bail!("Cannot extract u64 from {:?}", array.data_type()),
    }
}

/// Extract an f64 value from various Arrow float types.
fn extract_f64(array: &dyn Array, row: usize) -> Result<f64> {
    use arrow_array::*;
    match array.data_type() {
        DataType::Float32 => Ok(array.as_any().downcast_ref::<Float32Array>().ok_or_else(|| anyhow::anyhow!("Expected Float32Array array"))?.value(row) as f64),
        DataType::Float64 => Ok(array.as_any().downcast_ref::<Float64Array>().ok_or_else(|| anyhow::anyhow!("Expected Float64Array array"))?.value(row)),
        _ => anyhow::bail!("Cannot extract f64 from {:?}", array.data_type()),
    }
}

/// Extract a string value from Utf8/LargeUtf8 arrays.
fn extract_string(array: &dyn Array, row: usize) -> Result<String> {
    use arrow_array::*;
    match array.data_type() {
        DataType::Utf8 => Ok(array.as_any().downcast_ref::<StringArray>().ok_or_else(|| anyhow::anyhow!("Expected StringArray array"))?.value(row).to_string()),
        DataType::LargeUtf8 => Ok(array.as_any().downcast_ref::<LargeStringArray>().ok_or_else(|| anyhow::anyhow!("Expected LargeStringArray array"))?.value(row).to_string()),
        _ => anyhow::bail!("Cannot extract string from {:?}", array.data_type()),
    }
}

/// Extract bytes from Binary/LargeBinary arrays.
fn extract_bytes(array: &dyn Array, row: usize) -> Result<Vec<u8>> {
    use arrow_array::*;
    match array.data_type() {
        DataType::Binary => Ok(array.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| anyhow::anyhow!("Expected BinaryArray array"))?.value(row).to_vec()),
        DataType::LargeBinary => Ok(array.as_any().downcast_ref::<LargeBinaryArray>().ok_or_else(|| anyhow::anyhow!("Expected LargeBinaryArray array"))?.value(row).to_vec()),
        _ => anyhow::bail!("Cannot extract bytes from {:?}", array.data_type()),
    }
}

/// Extract a timestamp as microseconds since epoch from various timestamp types.
fn extract_timestamp_micros(array: &dyn Array, row: usize) -> Result<i64> {
    use arrow_array::*;
    use arrow_schema::TimeUnit;
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => {
            Ok(array.as_any().downcast_ref::<TimestampSecondArray>().ok_or_else(|| anyhow::anyhow!("Expected TimestampSecondArray array"))?.value(row) * 1_000_000)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Ok(array.as_any().downcast_ref::<TimestampMillisecondArray>().ok_or_else(|| anyhow::anyhow!("Expected TimestampMillisecondArray array"))?.value(row) * 1_000)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Ok(array.as_any().downcast_ref::<TimestampMicrosecondArray>().ok_or_else(|| anyhow::anyhow!("Expected TimestampMicrosecondArray array"))?.value(row))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Ok(array.as_any().downcast_ref::<TimestampNanosecondArray>().ok_or_else(|| anyhow::anyhow!("Expected TimestampNanosecondArray array"))?.value(row) / 1_000)
        }
        DataType::Date32 => {
            Ok(array.as_any().downcast_ref::<Date32Array>().ok_or_else(|| anyhow::anyhow!("Expected Date32Array array"))?.value(row) as i64 * 86_400_000_000i64)
        }
        DataType::Date64 => {
            Ok(array.as_any().downcast_ref::<Date64Array>().ok_or_else(|| anyhow::anyhow!("Expected Date64Array array"))?.value(row) * 1_000i64)
        }
        _ => anyhow::bail!("Cannot extract timestamp from {:?}", array.data_type()),
    }
}

/// Determine which columns from the manifest need to be transcoded from parquet
/// based on the fast field mode.
pub fn columns_to_transcode(
    manifest: &ParquetManifest,
    mode: FastFieldMode,
    requested_columns: Option<&[String]>,
) -> Vec<TranscodeColumn> {
    manifest.column_mapping
        .iter()
        .filter(|mapping| {
            // Check if this column should come from parquet
            let source = field_source(&mapping.tantivy_type, mode);
            if source != FieldSource::Parquet {
                return false;
            }
            // Skip Str columns with non-"raw" tokenizers — tokenized text fields
            // are incompatible with parquet's raw string storage for fast field aggregation.
            if mapping.tantivy_type == "Str" {
                let tok = mapping.fast_field_tokenizer.as_deref().unwrap_or("raw");
                if tok != "raw" {
                    return false;
                }
            }
            // If specific columns requested, filter to those
            if let Some(requested) = requested_columns {
                return requested.iter().any(|r| r == &mapping.tantivy_field_name);
            }
            true
        })
        .map(|mapping| TranscodeColumn {
            tantivy_name: mapping.tantivy_field_name.clone(),
            parquet_name: mapping.parquet_column_name.clone(),
            tantivy_type: mapping.tantivy_type.clone(),
            parquet_col_idx: mapping.physical_ordinal,
            fast_field_tokenizer: mapping.fast_field_tokenizer.clone(),
        })
        .collect()
}

/// Build a merged columnar that combines native fast field data with transcoded parquet data.
///
/// In hybrid mode:
/// - Numeric/bool/date/ip columns come from the native .fast file
/// - String/bytes columns come from parquet
///
/// In parquet-only mode:
/// - All columns come from parquet (native_fast_bytes is ignored)
///
/// # Arguments
/// * `native_fast_bytes` - Original .fast file bytes from the split (None in parquet-only mode)
/// * `parquet_columnar_bytes` - Transcoded columnar bytes from parquet
/// * `mode` - Fast field mode
///
/// # Returns
/// Merged columnar bytes suitable for .fast file
pub fn merge_columnar_bytes(
    native_fast_bytes: Option<&[u8]>,
    parquet_columnar_bytes: &[u8],
    mode: FastFieldMode,
) -> Result<Vec<u8>> {
    match mode {
        FastFieldMode::Disabled => {
            // Should not be called in disabled mode, but return native if available
            match native_fast_bytes {
                Some(bytes) => Ok(bytes.to_vec()),
                None => Ok(parquet_columnar_bytes.to_vec()),
            }
        }
        FastFieldMode::ParquetOnly => {
            // All columns from parquet
            Ok(parquet_columnar_bytes.to_vec())
        }
        FastFieldMode::Hybrid => {
            // In hybrid mode, we need to merge two columnar files.
            // The native file has numeric/bool/date/ip columns.
            // The parquet file has string/bytes columns.
            // Since they have disjoint column names, we can use tantivy's merge_columnar.
            merge_two_columnars(native_fast_bytes, parquet_columnar_bytes)
        }
    }
}

/// Strip the tantivy Footer from .fast file bytes to get raw columnar bytes.
///
/// Tantivy .fast files have format:
///   [columnar body][footer_json][footer_json_len: u32 LE][magic: u32 LE = 1337]
///
/// ColumnarReader expects just the columnar body (which has its own internal footer),
/// so we must strip the outer tantivy Footer first.
pub(crate) fn strip_tantivy_footer(fast_bytes: &[u8]) -> Result<&[u8]> {
    if fast_bytes.len() < 8 {
        anyhow::bail!("Fast file too small to contain footer ({} bytes)", fast_bytes.len());
    }

    let len = fast_bytes.len();

    // Read magic (last 4 bytes)
    let magic = u32::from_le_bytes([
        fast_bytes[len - 4], fast_bytes[len - 3],
        fast_bytes[len - 2], fast_bytes[len - 1],
    ]);
    if magic != 1337 {
        anyhow::bail!("Footer magic mismatch: expected 1337, got {}", magic);
    }

    // Read footer_json_len (previous 4 bytes)
    let footer_json_len = u32::from_le_bytes([
        fast_bytes[len - 8], fast_bytes[len - 7],
        fast_bytes[len - 6], fast_bytes[len - 5],
    ]) as usize;

    let total_footer_size = footer_json_len + 8; // footer_json + footer_json_len(4) + magic(4)
    if total_footer_size > len {
        anyhow::bail!(
            "Footer claims {} bytes but file is only {} bytes",
            total_footer_size, len
        );
    }

    Ok(&fast_bytes[..len - total_footer_size])
}

/// Copy all columns from a ColumnarReader into a ColumnarWriter.
///
/// Iterates every column, opens it, and re-records all values at the original doc IDs.
/// This preserves column data at the same row positions (unlike StackMergeOrder which
/// concatenates rows).
fn copy_all_columns(
    reader: &tantivy::columnar::ColumnarReader,
    writer: &mut ColumnarWriter,
) -> Result<()> {
    use tantivy::columnar::DynamicColumn;

    let num_rows = reader.num_rows();

    for (col_name, handle) in reader.list_columns()
        .context("Failed to list columns in columnar")?
    {
        let column = handle.open()
            .with_context(|| format!("Failed to open column '{}'", col_name))?;

        match column {
            DynamicColumn::I64(col) => {
                for doc_id in 0..num_rows {
                    for val in col.values_for_doc(doc_id) {
                        writer.record_numerical(doc_id, &col_name, val);
                    }
                }
            }
            DynamicColumn::U64(col) => {
                for doc_id in 0..num_rows {
                    for val in col.values_for_doc(doc_id) {
                        writer.record_numerical(doc_id, &col_name, val);
                    }
                }
            }
            DynamicColumn::F64(col) => {
                for doc_id in 0..num_rows {
                    for val in col.values_for_doc(doc_id) {
                        writer.record_numerical(doc_id, &col_name, val);
                    }
                }
            }
            DynamicColumn::Bool(col) => {
                for doc_id in 0..num_rows {
                    for val in col.values_for_doc(doc_id) {
                        writer.record_bool(doc_id, &col_name, val);
                    }
                }
            }
            DynamicColumn::DateTime(col) => {
                for doc_id in 0..num_rows {
                    for val in col.values_for_doc(doc_id) {
                        writer.record_datetime(doc_id, &col_name, val);
                    }
                }
            }
            DynamicColumn::IpAddr(col) => {
                for doc_id in 0..num_rows {
                    for val in col.values_for_doc(doc_id) {
                        writer.record_ip_addr(doc_id, &col_name, val);
                    }
                }
            }
            DynamicColumn::Str(str_col) => {
                // StrColumn implements Deref<Target=BytesColumn>, so ords() is available
                let ords_col = str_col.ords();
                let mut buf = String::new();
                for doc_id in 0..num_rows {
                    for ord in ords_col.values_for_doc(doc_id) {
                        buf.clear();
                        if str_col.ord_to_str(ord, &mut buf)? {
                            writer.record_str(doc_id, &col_name, &buf);
                        }
                    }
                }
            }
            DynamicColumn::Bytes(bytes_col) => {
                let ords_col = bytes_col.ords();
                let mut buf = Vec::new();
                for doc_id in 0..num_rows {
                    for ord in ords_col.values_for_doc(doc_id) {
                        buf.clear();
                        if bytes_col.ord_to_bytes(ord, &mut buf)? {
                            writer.record_bytes(doc_id, &col_name, &buf);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

/// Merge two columnar byte arrays that have disjoint column names.
///
/// Iterates all columns from both readers and re-records them into a single
/// ColumnarWriter at the same row positions. This is the correct merge strategy
/// for Hybrid mode where native has numeric columns and parquet has string columns.
///
/// Note: native_bytes includes tantivy Footer which is stripped before parsing.
/// parquet_bytes are raw columnar bytes (no tantivy Footer).
fn merge_two_columnars(
    native_bytes: Option<&[u8]>,
    parquet_bytes: &[u8],
) -> Result<Vec<u8>> {
    use tantivy::columnar::ColumnarReader;

    let mut writer = ColumnarWriter::default();
    let mut num_docs: u32 = 0;

    // Process native columnar (strip tantivy Footer first)
    if let Some(native) = native_bytes {
        if !native.is_empty() {
            let body = strip_tantivy_footer(native)
                .context("Failed to strip footer from native fast fields")?;
            let reader = ColumnarReader::open(body.to_vec())
                .context("Failed to open native columnar for merge")?;
            num_docs = reader.num_rows();
            copy_all_columns(&reader, &mut writer)
                .context("Failed to copy native columns")?;
        }
    }

    // Process parquet columnar (raw bytes, no tantivy Footer)
    if !parquet_bytes.is_empty() {
        let reader = ColumnarReader::open(parquet_bytes.to_vec())
            .context("Failed to open parquet columnar for merge")?;
        if num_docs == 0 {
            num_docs = reader.num_rows();
        }
        copy_all_columns(&reader, &mut writer)
            .context("Failed to copy parquet columns")?;
    }

    let mut output = Vec::new();
    writer.serialize(num_docs, &mut output)
        .context("Failed to serialize merged columnar")?;

    debug_println!(
        "📊 TRANSCODE: Merged columnars → {} bytes ({} docs)",
        output.len(), num_docs
    );

    Ok(output)
}

/// Wrap raw tantivy columnar bytes with a tantivy Footer.
///
/// Tantivy .fast files are not just raw columnar bytes — they have a Footer
/// appended that contains version info, CRC32 checksum, and a magic number (1337).
/// Without this Footer, tantivy's file reader will reject the data with
/// "Footer magic byte mismatch".
///
/// Footer format:
///   [body bytes]
///   [JSON: {"version":{...},"crc":CRC32}]
///   [footer_json_len: u32 LE]
///   [magic: u32 LE = 1337]
pub fn wrap_with_tantivy_footer(body: &[u8]) -> Vec<u8> {
    // Compute CRC32 of the body
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(body);
    let crc = hasher.finalize();

    // Get the tantivy version and serialize it
    let version = tantivy::version();
    let version_json = serde_json::to_string(version)
        .expect("Failed to serialize tantivy version");

    // Build footer JSON matching tantivy's Footer struct format
    let footer_json = format!("{{\"version\":{},\"crc\":{}}}", version_json, crc);

    let mut output = Vec::with_capacity(body.len() + footer_json.len() + 8);
    output.extend_from_slice(body);
    output.extend_from_slice(footer_json.as_bytes());
    output.extend_from_slice(&(footer_json.len() as u32).to_le_bytes());
    output.extend_from_slice(&1337u32.to_le_bytes()); // FOOTER_MAGIC_NUMBER

    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_source_disabled() {
        assert_eq!(field_source("I64", FastFieldMode::Disabled), FieldSource::Native);
        assert_eq!(field_source("Str", FastFieldMode::Disabled), FieldSource::Native);
    }

    #[test]
    fn test_field_source_hybrid() {
        assert_eq!(field_source("I64", FastFieldMode::Hybrid), FieldSource::Native);
        assert_eq!(field_source("F64", FastFieldMode::Hybrid), FieldSource::Native);
        assert_eq!(field_source("Bool", FastFieldMode::Hybrid), FieldSource::Native);
        assert_eq!(field_source("Date", FastFieldMode::Hybrid), FieldSource::Native);
        assert_eq!(field_source("Str", FastFieldMode::Hybrid), FieldSource::Parquet);
        assert_eq!(field_source("Bytes", FastFieldMode::Hybrid), FieldSource::Parquet);
    }

    #[test]
    fn test_field_source_parquet_only() {
        assert_eq!(field_source("I64", FastFieldMode::ParquetOnly), FieldSource::Parquet);
        assert_eq!(field_source("Str", FastFieldMode::ParquetOnly), FieldSource::Parquet);
    }

    #[test]
    fn test_columns_to_transcode_disabled() {
        let manifest = make_test_manifest();
        let cols = columns_to_transcode(&manifest, FastFieldMode::Disabled, None);
        assert!(cols.is_empty(), "No columns should be transcoded in Disabled mode");
    }

    #[test]
    fn test_columns_to_transcode_hybrid() {
        let manifest = make_test_manifest();
        let cols = columns_to_transcode(&manifest, FastFieldMode::Hybrid, None);
        // Only Str columns should be transcoded in hybrid mode
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].tantivy_name, "name");
        assert_eq!(cols[0].tantivy_type, "Str");
    }

    #[test]
    fn test_columns_to_transcode_parquet_only() {
        let manifest = make_test_manifest();
        let cols = columns_to_transcode(&manifest, FastFieldMode::ParquetOnly, None);
        // All columns should be transcoded
        assert_eq!(cols.len(), 3);
    }

    #[test]
    fn test_columns_to_transcode_with_filter() {
        let manifest = make_test_manifest();
        let requested = vec!["score".to_string()];
        let cols = columns_to_transcode(&manifest, FastFieldMode::ParquetOnly, Some(&requested));
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].tantivy_name, "score");
    }

    #[test]
    fn test_columnar_writer_roundtrip() {
        // Test that we can write and read back columnar data
        let mut writer = ColumnarWriter::default();
        writer.record_numerical(0u32, "count", 42i64);
        writer.record_numerical(1u32, "count", 100i64);
        writer.record_str(0u32, "name", "alice");
        writer.record_str(1u32, "name", "bob");
        writer.record_bool(0u32, "active", true);
        writer.record_bool(1u32, "active", false);

        let mut output = Vec::new();
        writer.serialize(2u32, &mut output).unwrap();
        assert!(!output.is_empty(), "Serialized columnar should not be empty");

        // Verify we can open it back
        let reader = tantivy::columnar::ColumnarReader::open(output).unwrap();
        assert_eq!(reader.num_rows(), 2);
    }

    #[test]
    fn test_strip_tantivy_footer_roundtrip() {
        // Create columnar bytes, wrap with footer, strip footer, verify identical
        let mut writer = ColumnarWriter::default();
        writer.record_numerical(0u32, "x", 42i64);
        writer.record_str(0u32, "s", "hello");
        let mut body = Vec::new();
        writer.serialize(1u32, &mut body).unwrap();

        let wrapped = wrap_with_tantivy_footer(&body);
        assert!(wrapped.len() > body.len(), "Wrapped should be larger than body");

        let stripped = strip_tantivy_footer(&wrapped).unwrap();
        assert_eq!(stripped, body.as_slice(), "Stripped should equal original body");
    }

    #[test]
    fn test_strip_tantivy_footer_bad_magic() {
        let data = vec![0u8; 16]; // No valid footer
        assert!(strip_tantivy_footer(&data).is_err());
    }

    #[test]
    fn test_strip_tantivy_footer_too_small() {
        let data = vec![0u8; 4]; // Too small for footer
        assert!(strip_tantivy_footer(&data).is_err());
    }

    #[test]
    fn test_merge_disjoint_columnars() {
        // Create two columnars with disjoint columns, merge, verify both present
        let mut writer1 = ColumnarWriter::default();
        writer1.record_numerical(0u32, "count", 10i64);
        writer1.record_numerical(1u32, "count", 20i64);
        let mut bytes1 = Vec::new();
        writer1.serialize(2u32, &mut bytes1).unwrap();

        let mut writer2 = ColumnarWriter::default();
        writer2.record_str(0u32, "name", "alice");
        writer2.record_str(1u32, "name", "bob");
        let mut bytes2 = Vec::new();
        writer2.serialize(2u32, &mut bytes2).unwrap();

        // Wrap bytes1 as if it were a native .fast file (with tantivy Footer)
        let wrapped1 = wrap_with_tantivy_footer(&bytes1);

        // Merge: native (with footer) + parquet (raw)
        let merged = merge_two_columnars(Some(&wrapped1), &bytes2).unwrap();

        // Verify merged columnar has both columns
        let reader = tantivy::columnar::ColumnarReader::open(merged).unwrap();
        assert_eq!(reader.num_rows(), 2);
        let columns = reader.list_columns().unwrap();
        let col_names: Vec<&str> = columns.iter().map(|(n, _)| n.as_str()).collect();
        assert!(col_names.contains(&"count"), "Should have 'count' column, got {:?}", col_names);
        assert!(col_names.contains(&"name"), "Should have 'name' column, got {:?}", col_names);
    }

    #[test]
    fn test_merge_parquet_only_no_native() {
        // Merge with None native bytes — should return parquet bytes as-is
        let mut writer = ColumnarWriter::default();
        writer.record_str(0u32, "city", "NYC");
        let mut bytes = Vec::new();
        writer.serialize(1u32, &mut bytes).unwrap();

        let merged = merge_two_columnars(None, &bytes).unwrap();
        let reader = tantivy::columnar::ColumnarReader::open(merged).unwrap();
        assert_eq!(reader.num_rows(), 1);
        let columns = reader.list_columns().unwrap();
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].0, "city");
    }

    #[test]
    fn test_direct_str_transcode_roundtrip() {
        // Build columnar via direct path, open with ColumnarReader, verify correctness
        let col = StrColumnData {
            name: "city".to_string(),
            sorted_terms: vec![
                b"alice_town".to_vec(),
                b"bob_city".to_vec(),
                b"charlie_ville".to_vec(),
            ],
            // doc0="bob_city" (ord 1), doc1="alice_town" (ord 0), doc2="charlie_ville" (ord 2)
            ordinals: vec![1, 0, 2],
            non_null_doc_ids: None, // Full cardinality
        };

        let bytes = serialize_str_columnar(vec![col], 3).unwrap();

        // Verify ColumnarReader can open and read it
        let reader = tantivy::columnar::ColumnarReader::open(bytes).unwrap();
        assert_eq!(reader.num_rows(), 3);

        let columns = reader.list_columns().unwrap();
        assert_eq!(columns.len(), 1, "Expected 1 column, got {}", columns.len());
        assert_eq!(columns[0].0, "city");

        // Open the string column and verify terms and ordinals
        let (_, handle) = &columns[0];
        let dyn_col = handle.open().unwrap();
        match dyn_col {
            tantivy::columnar::DynamicColumn::Str(str_col) => {
                let ords = str_col.ords();
                // doc0 = "bob_city" (sorted ord 1)
                let vals0: Vec<u64> = ords.values_for_doc(0).collect();
                assert_eq!(vals0, vec![1], "doc0 ordinal");
                // doc1 = "alice_town" (sorted ord 0)
                let vals1: Vec<u64> = ords.values_for_doc(1).collect();
                assert_eq!(vals1, vec![0], "doc1 ordinal");
                // doc2 = "charlie_ville" (sorted ord 2)
                let vals2: Vec<u64> = ords.values_for_doc(2).collect();
                assert_eq!(vals2, vec![2], "doc2 ordinal");

                // Verify term dictionary
                let mut buf = String::new();
                assert!(str_col.ord_to_str(0, &mut buf).unwrap());
                assert_eq!(buf, "alice_town");
                buf.clear();
                assert!(str_col.ord_to_str(1, &mut buf).unwrap());
                assert_eq!(buf, "bob_city");
                buf.clear();
                assert!(str_col.ord_to_str(2, &mut buf).unwrap());
                assert_eq!(buf, "charlie_ville");
            }
            other => panic!("Expected Str column, got {:?}", other),
        }
    }

    #[test]
    fn test_direct_str_transcode_nullable() {
        // String column with nulls at known positions
        let col = StrColumnData {
            name: "tag".to_string(),
            sorted_terms: vec![b"alpha".to_vec(), b"beta".to_vec()],
            // Only doc0 and doc2 have values (doc1 is null)
            ordinals: vec![0, 1], // alpha, beta
            non_null_doc_ids: Some(vec![0, 2]),
        };

        let bytes = serialize_str_columnar(vec![col], 3).unwrap();

        let reader = tantivy::columnar::ColumnarReader::open(bytes).unwrap();
        assert_eq!(reader.num_rows(), 3);

        let columns = reader.list_columns().unwrap();
        assert_eq!(columns.len(), 1);

        let (_, handle) = &columns[0];
        let dyn_col = handle.open().unwrap();
        match dyn_col {
            tantivy::columnar::DynamicColumn::Str(str_col) => {
                let ords = str_col.ords();
                // doc0 = "alpha"
                let vals0: Vec<u64> = ords.values_for_doc(0).collect();
                assert_eq!(vals0, vec![0]);
                // doc1 = null (no values)
                let vals1: Vec<u64> = ords.values_for_doc(1).collect();
                assert!(vals1.is_empty(), "doc1 should have no values (null)");
                // doc2 = "beta"
                let vals2: Vec<u64> = ords.values_for_doc(2).collect();
                assert_eq!(vals2, vec![1]);
            }
            other => panic!("Expected Str column, got {:?}", other),
        }
    }

    #[test]
    fn test_direct_str_transcode_multi_column() {
        // Two string columns in the same columnar
        let col1 = StrColumnData {
            name: "city".to_string(),
            sorted_terms: vec![b"NYC".to_vec(), b"SF".to_vec()],
            ordinals: vec![0, 1],
            non_null_doc_ids: None,
        };
        let col2 = StrColumnData {
            name: "state".to_string(),
            sorted_terms: vec![b"CA".to_vec(), b"NY".to_vec()],
            ordinals: vec![1, 0],
            non_null_doc_ids: None,
        };

        let bytes = serialize_str_columnar(vec![col1, col2], 2).unwrap();

        let reader = tantivy::columnar::ColumnarReader::open(bytes).unwrap();
        assert_eq!(reader.num_rows(), 2);

        let columns = reader.list_columns().unwrap();
        assert_eq!(columns.len(), 2, "Expected 2 columns, got {}", columns.len());

        let col_names: Vec<&str> = columns.iter().map(|(n, _)| n.as_str()).collect();
        assert!(col_names.contains(&"city"), "Missing 'city' column");
        assert!(col_names.contains(&"state"), "Missing 'state' column");
    }

    #[test]
    fn test_direct_str_vs_columnar_writer_equivalence() {
        // Verify direct path produces output readable identically to ColumnarWriter path
        let terms = vec!["hello", "world", "foo", "bar"];

        // ColumnarWriter path
        let mut writer = ColumnarWriter::default();
        for (i, term) in terms.iter().enumerate() {
            writer.record_str(i as u32, "text", term);
        }
        let mut cw_bytes = Vec::new();
        writer.serialize(4, &mut cw_bytes).unwrap();
        let cw_reader = tantivy::columnar::ColumnarReader::open(cw_bytes).unwrap();

        // Direct path: terms must be sorted, ordinals remapped
        let mut sorted: Vec<(&str, u32)> = terms.iter().enumerate()
            .map(|(i, &t)| (t, i as u32))
            .collect();
        sorted.sort_by_key(|&(t, _)| t);
        let mut old_to_new = vec![0u64; terms.len()];
        let sorted_terms: Vec<Vec<u8>> = sorted.iter().enumerate().map(|(new, &(t, old))| {
            old_to_new[old as usize] = new as u64;
            t.as_bytes().to_vec()
        }).collect();
        let ordinals: Vec<u64> = (0..terms.len()).map(|i| old_to_new[i]).collect();

        let col = StrColumnData {
            name: "text".to_string(),
            sorted_terms,
            ordinals,
            non_null_doc_ids: None,
        };
        let direct_bytes = serialize_str_columnar(vec![col], 4).unwrap();
        let direct_reader = tantivy::columnar::ColumnarReader::open(direct_bytes).unwrap();

        // Both should have same structure
        assert_eq!(cw_reader.num_rows(), direct_reader.num_rows());
        let cw_cols = cw_reader.list_columns().unwrap();
        let direct_cols = direct_reader.list_columns().unwrap();
        assert_eq!(cw_cols.len(), direct_cols.len());
        assert_eq!(cw_cols[0].0, direct_cols[0].0);

        // Verify identical string values per doc
        let cw_str = match cw_cols[0].1.open().unwrap() {
            tantivy::columnar::DynamicColumn::Str(s) => s,
            _ => panic!("Expected Str"),
        };
        let direct_str = match direct_cols[0].1.open().unwrap() {
            tantivy::columnar::DynamicColumn::Str(s) => s,
            _ => panic!("Expected Str"),
        };

        for doc in 0..4u32 {
            let cw_ords: Vec<u64> = cw_str.ords().values_for_doc(doc).collect();
            let direct_ords: Vec<u64> = direct_str.ords().values_for_doc(doc).collect();
            assert_eq!(cw_ords.len(), direct_ords.len(), "doc {} ordinal count", doc);

            for (&cw_ord, &direct_ord) in cw_ords.iter().zip(direct_ords.iter()) {
                let mut cw_buf = String::new();
                let mut direct_buf = String::new();
                cw_str.ord_to_str(cw_ord, &mut cw_buf).unwrap();
                direct_str.ord_to_str(direct_ord, &mut direct_buf).unwrap();
                assert_eq!(cw_buf, direct_buf, "doc {} term mismatch", doc);
            }
        }
    }

    #[test]
    fn test_direct_str_transcode_multi_column_values() {
        // Verify multi-column output has correct values (not just column presence)
        let col1 = StrColumnData {
            name: "city".to_string(),
            sorted_terms: vec![b"NYC".to_vec(), b"SF".to_vec()],
            ordinals: vec![0, 1], // doc0=NYC, doc1=SF
            non_null_doc_ids: None,
        };
        let col2 = StrColumnData {
            name: "state".to_string(),
            sorted_terms: vec![b"CA".to_vec(), b"NY".to_vec()],
            ordinals: vec![1, 0], // doc0=NY, doc1=CA
            non_null_doc_ids: None,
        };

        let bytes = serialize_str_columnar(vec![col1, col2], 2).unwrap();
        let reader = tantivy::columnar::ColumnarReader::open(bytes).unwrap();
        let columns = reader.list_columns().unwrap();

        for (name, handle) in &columns {
            let dyn_col = handle.open().unwrap();
            match dyn_col {
                tantivy::columnar::DynamicColumn::Str(str_col) => {
                    let ords = str_col.ords();
                    if name == "city" {
                        let vals0: Vec<u64> = ords.values_for_doc(0).collect();
                        let vals1: Vec<u64> = ords.values_for_doc(1).collect();
                        assert_eq!(vals0, vec![0], "city doc0 should be NYC (ord 0)");
                        assert_eq!(vals1, vec![1], "city doc1 should be SF (ord 1)");
                        let mut buf = String::new();
                        str_col.ord_to_str(0, &mut buf).unwrap();
                        assert_eq!(buf, "NYC");
                        buf.clear();
                        str_col.ord_to_str(1, &mut buf).unwrap();
                        assert_eq!(buf, "SF");
                    } else if name == "state" {
                        let vals0: Vec<u64> = ords.values_for_doc(0).collect();
                        let vals1: Vec<u64> = ords.values_for_doc(1).collect();
                        assert_eq!(vals0, vec![1], "state doc0 should be NY (ord 1)");
                        assert_eq!(vals1, vec![0], "state doc1 should be CA (ord 0)");
                        let mut buf = String::new();
                        str_col.ord_to_str(0, &mut buf).unwrap();
                        assert_eq!(buf, "CA");
                        buf.clear();
                        str_col.ord_to_str(1, &mut buf).unwrap();
                        assert_eq!(buf, "NY");
                    }
                }
                other => panic!("Expected Str column for '{}', got {:?}", name, other),
            }
        }
    }

    #[test]
    fn test_direct_str_transcode_empty_string() {
        // Verify empty strings and unicode are handled correctly
        let col = StrColumnData {
            name: "text".to_string(),
            sorted_terms: vec![
                b"".to_vec(),                   // empty string
                "caf\u{e9}".as_bytes().to_vec(), // unicode café
                "hello".as_bytes().to_vec(),
                "\u{4e2d}\u{6587}".as_bytes().to_vec(), // Chinese characters (中文)
            ],
            ordinals: vec![2, 0, 1, 3], // doc0=hello, doc1=empty, doc2=café, doc3=中文
            non_null_doc_ids: None,
        };

        let bytes = serialize_str_columnar(vec![col], 4).unwrap();
        let reader = tantivy::columnar::ColumnarReader::open(bytes).unwrap();
        assert_eq!(reader.num_rows(), 4);

        let columns = reader.list_columns().unwrap();
        let (_, handle) = &columns[0];
        let dyn_col = handle.open().unwrap();
        match dyn_col {
            tantivy::columnar::DynamicColumn::Str(str_col) => {
                let mut buf = String::new();

                // doc0 = "hello" (ord 2)
                let ords0: Vec<u64> = str_col.ords().values_for_doc(0).collect();
                assert_eq!(ords0, vec![2]);
                str_col.ord_to_str(2, &mut buf).unwrap();
                assert_eq!(buf, "hello");

                // doc1 = "" (ord 0, empty string)
                buf.clear();
                let ords1: Vec<u64> = str_col.ords().values_for_doc(1).collect();
                assert_eq!(ords1, vec![0]);
                str_col.ord_to_str(0, &mut buf).unwrap();
                assert_eq!(buf, "");

                // doc2 = "café" (ord 1)
                buf.clear();
                let ords2: Vec<u64> = str_col.ords().values_for_doc(2).collect();
                assert_eq!(ords2, vec![1]);
                str_col.ord_to_str(1, &mut buf).unwrap();
                assert_eq!(buf, "caf\u{e9}");

                // doc3 = "中文" (ord 3)
                buf.clear();
                let ords3: Vec<u64> = str_col.ords().values_for_doc(3).collect();
                assert_eq!(ords3, vec![3]);
                str_col.ord_to_str(3, &mut buf).unwrap();
                assert_eq!(buf, "\u{4e2d}\u{6587}");
            }
            other => panic!("Expected Str column, got {:?}", other),
        }
    }

    #[test]
    fn test_direct_str_transcode_large_cardinality() {
        // Many distinct terms to stress the dictionary and bitpacking
        let num_terms = 500;
        let sorted_terms: Vec<Vec<u8>> = (0..num_terms)
            .map(|i| format!("term_{:04}", i).into_bytes())
            .collect();

        // Each doc gets a different term: doc_i → term_i
        let ordinals: Vec<u64> = (0..num_terms as u64).collect();

        let col = StrColumnData {
            name: "tag".to_string(),
            sorted_terms: sorted_terms.clone(),
            ordinals,
            non_null_doc_ids: None,
        };

        let bytes = serialize_str_columnar(vec![col], num_terms as u32).unwrap();
        let reader = tantivy::columnar::ColumnarReader::open(bytes).unwrap();
        assert_eq!(reader.num_rows(), num_terms as u32);

        let columns = reader.list_columns().unwrap();
        assert_eq!(columns.len(), 1);

        let (_, handle) = &columns[0];
        let dyn_col = handle.open().unwrap();
        match dyn_col {
            tantivy::columnar::DynamicColumn::Str(str_col) => {
                // Verify first, middle, and last docs
                for doc in [0u32, 250, 499] {
                    let ords: Vec<u64> = str_col.ords().values_for_doc(doc).collect();
                    assert_eq!(ords, vec![doc as u64], "doc {} ordinal", doc);
                    let mut buf = String::new();
                    str_col.ord_to_str(doc as u64, &mut buf).unwrap();
                    assert_eq!(buf, format!("term_{:04}", doc), "doc {} term", doc);
                }
            }
            other => panic!("Expected Str column, got {:?}", other),
        }
    }

    #[test]
    fn test_direct_str_transcode_duplicate_heavy() {
        // Many rows, few unique terms (tests ordinal reuse and bitpacking efficiency)
        let sorted_terms = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        let num_docs = 1000u32;
        // Cycle through 3 terms: a, b, c, a, b, c, ...
        let ordinals: Vec<u64> = (0..num_docs).map(|i| (i % 3) as u64).collect();

        let col = StrColumnData {
            name: "category".to_string(),
            sorted_terms,
            ordinals,
            non_null_doc_ids: None,
        };

        let bytes = serialize_str_columnar(vec![col], num_docs).unwrap();
        let reader = tantivy::columnar::ColumnarReader::open(bytes).unwrap();
        assert_eq!(reader.num_rows(), num_docs);

        let columns = reader.list_columns().unwrap();
        let (_, handle) = &columns[0];
        let dyn_col = handle.open().unwrap();
        match dyn_col {
            tantivy::columnar::DynamicColumn::Str(str_col) => {
                // Verify term dictionary has exactly 3 terms
                let mut buf = String::new();
                assert!(str_col.ord_to_str(0, &mut buf).unwrap());
                assert_eq!(buf, "a");
                buf.clear();
                assert!(str_col.ord_to_str(1, &mut buf).unwrap());
                assert_eq!(buf, "b");
                buf.clear();
                assert!(str_col.ord_to_str(2, &mut buf).unwrap());
                assert_eq!(buf, "c");

                // Verify cycling pattern at various doc positions
                for doc in [0u32, 3, 6, 99, 999] {
                    let ords: Vec<u64> = str_col.ords().values_for_doc(doc).collect();
                    assert_eq!(ords, vec![(doc % 3) as u64], "doc {} ordinal", doc);
                }
            }
            other => panic!("Expected Str column, got {:?}", other),
        }
    }

    #[test]
    fn test_direct_str_transcode_all_null() {
        // Column where every row is null — should produce Optional cardinality
        // with zero ordinals and zero non_null_doc_ids
        let col = StrColumnData {
            name: "empty_col".to_string(),
            sorted_terms: vec![],
            ordinals: vec![],
            non_null_doc_ids: Some(vec![]),
        };

        let bytes = serialize_str_columnar(vec![col], 5).unwrap();
        let reader = tantivy::columnar::ColumnarReader::open(bytes).unwrap();
        assert_eq!(reader.num_rows(), 5);

        let columns = reader.list_columns().unwrap();
        assert_eq!(columns.len(), 1);

        let (_, handle) = &columns[0];
        let dyn_col = handle.open().unwrap();
        match dyn_col {
            tantivy::columnar::DynamicColumn::Str(str_col) => {
                // All docs should have no values
                for doc in 0..5u32 {
                    let ords: Vec<u64> = str_col.ords().values_for_doc(doc).collect();
                    assert!(ords.is_empty(), "doc {} should have no values", doc);
                }
            }
            other => panic!("Expected Str column, got {:?}", other),
        }
    }

    #[test]
    fn test_direct_str_transcode_single_term() {
        // Edge case: single unique term across all docs (ordinal always 0)
        let col = StrColumnData {
            name: "status".to_string(),
            sorted_terms: vec![b"active".to_vec()],
            ordinals: vec![0, 0, 0, 0, 0],
            non_null_doc_ids: None,
        };

        let bytes = serialize_str_columnar(vec![col], 5).unwrap();
        let reader = tantivy::columnar::ColumnarReader::open(bytes).unwrap();
        assert_eq!(reader.num_rows(), 5);

        let columns = reader.list_columns().unwrap();
        let (_, handle) = &columns[0];
        let dyn_col = handle.open().unwrap();
        match dyn_col {
            tantivy::columnar::DynamicColumn::Str(str_col) => {
                for doc in 0..5u32 {
                    let ords: Vec<u64> = str_col.ords().values_for_doc(doc).collect();
                    assert_eq!(ords, vec![0], "doc {} should have ordinal 0", doc);
                }
                let mut buf = String::new();
                str_col.ord_to_str(0, &mut buf).unwrap();
                assert_eq!(buf, "active");
            }
            other => panic!("Expected Str column, got {:?}", other),
        }
    }

    #[test]
    fn test_direct_str_transcode_nullable_first_and_last() {
        // Nulls at boundaries: first and last docs are null, middle has values
        let col = StrColumnData {
            name: "label".to_string(),
            sorted_terms: vec![b"x".to_vec(), b"y".to_vec(), b"z".to_vec()],
            ordinals: vec![0, 1, 2], // only 3 non-null ordinals for docs 1, 2, 3
            non_null_doc_ids: Some(vec![1, 2, 3]),
        };

        let bytes = serialize_str_columnar(vec![col], 5).unwrap();
        let reader = tantivy::columnar::ColumnarReader::open(bytes).unwrap();
        assert_eq!(reader.num_rows(), 5);

        let columns = reader.list_columns().unwrap();
        let (_, handle) = &columns[0];
        let dyn_col = handle.open().unwrap();
        match dyn_col {
            tantivy::columnar::DynamicColumn::Str(str_col) => {
                // doc0 = null
                let ords0: Vec<u64> = str_col.ords().values_for_doc(0).collect();
                assert!(ords0.is_empty(), "doc0 should be null");
                // doc1 = "x"
                let ords1: Vec<u64> = str_col.ords().values_for_doc(1).collect();
                assert_eq!(ords1, vec![0]);
                // doc2 = "y"
                let ords2: Vec<u64> = str_col.ords().values_for_doc(2).collect();
                assert_eq!(ords2, vec![1]);
                // doc3 = "z"
                let ords3: Vec<u64> = str_col.ords().values_for_doc(3).collect();
                assert_eq!(ords3, vec![2]);
                // doc4 = null
                let ords4: Vec<u64> = str_col.ords().values_for_doc(4).collect();
                assert!(ords4.is_empty(), "doc4 should be null");
            }
            other => panic!("Expected Str column, got {:?}", other),
        }
    }

    #[test]
    fn test_direct_str_mixed_nullable_and_full() {
        // One full column + one nullable column in same columnar
        let col1 = StrColumnData {
            name: "country".to_string(),
            sorted_terms: vec![b"DE".to_vec(), b"US".to_vec()],
            ordinals: vec![1, 0, 1], // US, DE, US
            non_null_doc_ids: None,   // Full cardinality
        };
        let col2 = StrColumnData {
            name: "region".to_string(),
            sorted_terms: vec![b"east".to_vec(), b"west".to_vec()],
            ordinals: vec![1, 0], // only doc0=west, doc2=east
            non_null_doc_ids: Some(vec![0, 2]),
        };

        let bytes = serialize_str_columnar(vec![col1, col2], 3).unwrap();
        let reader = tantivy::columnar::ColumnarReader::open(bytes).unwrap();
        assert_eq!(reader.num_rows(), 3);

        let columns = reader.list_columns().unwrap();
        assert_eq!(columns.len(), 2);

        for (name, handle) in &columns {
            let dyn_col = handle.open().unwrap();
            match dyn_col {
                tantivy::columnar::DynamicColumn::Str(str_col) => {
                    let ords = str_col.ords();
                    if name == "country" {
                        // All 3 docs have values
                        assert_eq!(ords.values_for_doc(0).collect::<Vec<_>>(), vec![1]); // US
                        assert_eq!(ords.values_for_doc(1).collect::<Vec<_>>(), vec![0]); // DE
                        assert_eq!(ords.values_for_doc(2).collect::<Vec<_>>(), vec![1]); // US
                    } else if name == "region" {
                        // doc0=west, doc1=null, doc2=east
                        assert_eq!(ords.values_for_doc(0).collect::<Vec<_>>(), vec![1]); // west
                        assert!(ords.values_for_doc(1).collect::<Vec<u64>>().is_empty()); // null
                        assert_eq!(ords.values_for_doc(2).collect::<Vec<_>>(), vec![0]); // east
                    }
                }
                other => panic!("Expected Str for '{}', got {:?}", name, other),
            }
        }
    }

    fn make_test_manifest() -> ParquetManifest {
        use super::super::manifest::*;
        ParquetManifest {
            version: SUPPORTED_MANIFEST_VERSION,
            table_root: "/data".to_string(),
            fast_field_mode: FastFieldMode::Hybrid,
            segment_row_ranges: vec![
                SegmentRowRange { segment_ord: 0, row_offset: 0, num_rows: 100 },
            ],
            parquet_files: vec![
                ParquetFileEntry {
                    relative_path: "part-0001.parquet".to_string(),
                    file_size_bytes: 1024,
                    row_offset: 0,
                    num_rows: 100,
                    has_offset_index: false,
                    row_groups: vec![],
                },
            ],
            column_mapping: vec![
                ColumnMapping {
                    tantivy_field_name: "id".to_string(),
                    parquet_column_name: "id".to_string(),
                    physical_ordinal: 0,
                    parquet_type: "INT64".to_string(),
                    tantivy_type: "I64".to_string(),
                    field_id: None,
                    fast_field_tokenizer: None,
                },
                ColumnMapping {
                    tantivy_field_name: "name".to_string(),
                    parquet_column_name: "name".to_string(),
                    physical_ordinal: 1,
                    parquet_type: "BYTE_ARRAY".to_string(),
                    tantivy_type: "Str".to_string(),
                    field_id: None,
                    fast_field_tokenizer: Some("raw".to_string()),
                },
                ColumnMapping {
                    tantivy_field_name: "score".to_string(),
                    parquet_column_name: "score".to_string(),
                    physical_ordinal: 2,
                    parquet_type: "DOUBLE".to_string(),
                    tantivy_type: "F64".to_string(),
                    field_id: None,
                    fast_field_tokenizer: None,
                },
            ],
            total_rows: 100,
            storage_config: None,
            metadata: std::collections::HashMap::new(),
        }
    }
}
