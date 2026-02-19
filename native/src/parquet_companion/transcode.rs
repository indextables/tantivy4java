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
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use arrow_array::{Array, RecordBatch};
use arrow_schema::DataType;

use tantivy::columnar::ColumnarWriter;

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
/// This reads parquet files through the storage layer, extracts the specified columns,
/// and writes them into a tantivy ColumnarWriter to produce valid .fast file bytes.
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
        // Return empty columnar with no columns
        let mut writer = ColumnarWriter::default();
        let mut output = Vec::new();
        writer.serialize(num_docs, &mut output)
            .context("Failed to serialize empty columnar")?;
        return Ok(output);
    }

    debug_println!(
        "📊 TRANSCODE: Transcoding {} columns from parquet ({} docs across {} files)",
        columns.len(), num_docs, manifest.parquet_files.len()
    );

    let mut writer = ColumnarWriter::default();

    // Build the parquet column names we need to project
    let parquet_col_names: Vec<&str> = columns.iter().map(|c| c.parquet_name.as_str()).collect();

    // Process each parquet file sequentially (rows must be recorded in order)
    let mut global_row: u32 = 0;
    for file_entry in &manifest.parquet_files {
        // Use relative_path directly — the storage is rooted at table_root
        let parquet_path = &file_entry.relative_path;
        let path_buf = std::path::PathBuf::from(parquet_path);

        // Check metadata cache to avoid re-reading footer from S3/Azure
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

        // Cache the metadata for subsequent reads (by doc retrieval or other transcodes)
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

        // Build projection mask for just the columns we need
        let col_indices: Vec<usize> = parquet_col_names
            .iter()
            .filter_map(|name| {
                parquet_schema.fields().iter().position(|f| f.name() == *name)
            })
            .collect();

        let builder = if !col_indices.is_empty() {
            // Use roots() not leaves() because col_indices are top-level arrow field
            // positions, not parquet leaf column indices. For schemas with nested types
            // (List, Map, Struct), leaf indices diverge from top-level positions.
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
        "📊 TRANSCODE: Recorded {} rows, serializing columnar...",
        global_row
    );

    let mut output = Vec::new();
    writer.serialize(num_docs, &mut output)
        .context("Failed to serialize transcoded columnar")?;

    debug_println!(
        "📊 TRANSCODE: Serialized {} bytes of columnar data",
        output.len()
    );

    Ok(output)
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
        DataType::Int8 => Ok(array.as_any().downcast_ref::<Int8Array>().unwrap().value(row) as i64),
        DataType::Int16 => Ok(array.as_any().downcast_ref::<Int16Array>().unwrap().value(row) as i64),
        DataType::Int32 => Ok(array.as_any().downcast_ref::<Int32Array>().unwrap().value(row) as i64),
        DataType::Int64 => Ok(array.as_any().downcast_ref::<Int64Array>().unwrap().value(row)),
        _ => anyhow::bail!("Cannot extract i64 from {:?}", array.data_type()),
    }
}

/// Extract a u64 value from various Arrow unsigned integer types.
fn extract_u64(array: &dyn Array, row: usize) -> Result<u64> {
    use arrow_array::*;
    match array.data_type() {
        DataType::UInt8 => Ok(array.as_any().downcast_ref::<UInt8Array>().unwrap().value(row) as u64),
        DataType::UInt16 => Ok(array.as_any().downcast_ref::<UInt16Array>().unwrap().value(row) as u64),
        DataType::UInt32 => Ok(array.as_any().downcast_ref::<UInt32Array>().unwrap().value(row) as u64),
        DataType::UInt64 => Ok(array.as_any().downcast_ref::<UInt64Array>().unwrap().value(row)),
        // Also support signed types coerced to u64
        DataType::Int64 => Ok(array.as_any().downcast_ref::<Int64Array>().unwrap().value(row) as u64),
        _ => anyhow::bail!("Cannot extract u64 from {:?}", array.data_type()),
    }
}

/// Extract an f64 value from various Arrow float types.
fn extract_f64(array: &dyn Array, row: usize) -> Result<f64> {
    use arrow_array::*;
    match array.data_type() {
        DataType::Float32 => Ok(array.as_any().downcast_ref::<Float32Array>().unwrap().value(row) as f64),
        DataType::Float64 => Ok(array.as_any().downcast_ref::<Float64Array>().unwrap().value(row)),
        _ => anyhow::bail!("Cannot extract f64 from {:?}", array.data_type()),
    }
}

/// Extract a string value from Utf8/LargeUtf8 arrays.
fn extract_string(array: &dyn Array, row: usize) -> Result<String> {
    use arrow_array::*;
    match array.data_type() {
        DataType::Utf8 => Ok(array.as_any().downcast_ref::<StringArray>().unwrap().value(row).to_string()),
        DataType::LargeUtf8 => Ok(array.as_any().downcast_ref::<LargeStringArray>().unwrap().value(row).to_string()),
        _ => anyhow::bail!("Cannot extract string from {:?}", array.data_type()),
    }
}

/// Extract bytes from Binary/LargeBinary arrays.
fn extract_bytes(array: &dyn Array, row: usize) -> Result<Vec<u8>> {
    use arrow_array::*;
    match array.data_type() {
        DataType::Binary => Ok(array.as_any().downcast_ref::<BinaryArray>().unwrap().value(row).to_vec()),
        DataType::LargeBinary => Ok(array.as_any().downcast_ref::<LargeBinaryArray>().unwrap().value(row).to_vec()),
        _ => anyhow::bail!("Cannot extract bytes from {:?}", array.data_type()),
    }
}

/// Extract a timestamp as microseconds since epoch from various timestamp types.
fn extract_timestamp_micros(array: &dyn Array, row: usize) -> Result<i64> {
    use arrow_array::*;
    use arrow_schema::TimeUnit;
    match array.data_type() {
        DataType::Timestamp(TimeUnit::Second, _) => {
            Ok(array.as_any().downcast_ref::<TimestampSecondArray>().unwrap().value(row) * 1_000_000)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            Ok(array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap().value(row) * 1_000)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            Ok(array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value(row))
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            Ok(array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap().value(row) / 1_000)
        }
        DataType::Date32 => {
            Ok(array.as_any().downcast_ref::<Date32Array>().unwrap().value(row) as i64 * 86_400_000_000i64)
        }
        DataType::Date64 => {
            Ok(array.as_any().downcast_ref::<Date64Array>().unwrap().value(row) * 1_000i64)
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
