// arrow_ffi_export.rs - Arrow FFI export for parquet companion mode
//
// Exports parquet data as Arrow columnar arrays via the Arrow C Data Interface (FFI),
// enabling zero-copy transfer of RecordBatch data to the JVM. This is the companion
// mode counterpart to arrow_to_tant.rs — instead of serializing to TANT binary format,
// it writes Arrow FFI structs directly to pre-allocated memory addresses provided by
// the Java caller.
//
// This path eliminates all serialization overhead for companion mode document retrieval,
// enabling consumers like Spark to receive native Arrow columnar data.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{RecordBatch, UInt32Array};
use arrow_schema::{Field, Schema};
use quickwit_storage::Storage;

use crate::perf_println;
use super::arrow_to_tant::read_parquet_batches_by_groups;
use super::cached_reader::{ByteRangeCache, CoalesceConfig};
use super::manifest::{ColumnMapping, ParquetManifest};
use super::transcode::MetadataCache;

/// Export parquet batch data via Arrow FFI to pre-allocated C struct addresses.
///
/// This function reads parquet data using the shared pipeline from `arrow_to_tant.rs`,
/// concatenates all file batches, reorders rows to match the original request order,
/// renames columns from parquet names to tantivy field names, then writes each column
/// via the Arrow C Data Interface to pre-allocated memory addresses.
///
/// # Arguments
/// * `groups` - File groups: file_idx → [(original_index, row_in_file)]
/// * `result_count` - Total number of documents requested (for row reordering)
/// * `projected_fields` - Optional field name projection
/// * `manifest` - Parquet companion manifest
/// * `storage` - Storage backend for reading parquet files
/// * `metadata_cache` - Optional parquet metadata cache
/// * `byte_cache` - Optional byte range cache
/// * `coalesce_config` - Optional coalescing configuration
/// * `array_addrs` - Pre-allocated FFI_ArrowArray addresses (one per projected column)
/// * `schema_addrs` - Pre-allocated FFI_ArrowSchema addresses (one per projected column)
///
/// # Returns
/// The number of rows written, or an error.
pub async fn batch_parquet_to_arrow_ffi(
    groups: std::collections::HashMap<usize, Vec<(usize, u64)>>,
    result_count: usize,
    projected_fields: Option<&[String]>,
    manifest: &ParquetManifest,
    storage: &Arc<dyn Storage>,
    metadata_cache: Option<&MetadataCache>,
    byte_cache: Option<&ByteRangeCache>,
    coalesce_config: Option<CoalesceConfig>,
    array_addrs: &[i64],
    schema_addrs: &[i64],
) -> Result<usize> {
    let t_total = std::time::Instant::now();
    perf_println!(
        "⏱️ FFI_DIAG: batch_parquet_to_arrow_ffi START - {} files, {} result docs, {} columns",
        groups.len(), result_count, array_addrs.len()
    );

    // Step 1: Read parquet data using the shared pipeline
    let file_results = read_parquet_batches_by_groups(
        groups,
        projected_fields,
        manifest,
        storage,
        metadata_cache,
        byte_cache,
        coalesce_config,
    )
    .await?;

    perf_println!(
        "⏱️ FFI_DIAG: parquet read complete - {} file groups, took {}ms",
        file_results.len(), t_total.elapsed().as_millis()
    );

    // Step 2: Collect all batches and build a global ordering index
    // Each file result gives us (rows: Vec<(original_idx, row_in_file)>, batches: Vec<RecordBatch>)
    // We need to track which output row each input row maps to.
    let mut all_batches: Vec<RecordBatch> = Vec::new();
    let mut row_to_original: Vec<usize> = Vec::with_capacity(result_count);

    for (rows, batches) in &file_results {
        let batch_row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        // The batches contain rows in the order of `rows` (sorted by row_in_file),
        // and each (original_idx, _) tells us where each row should go in the output.
        if batch_row_count != rows.len() {
            anyhow::bail!(
                "Row count mismatch: expected {} rows from file group, got {} from parquet",
                rows.len(), batch_row_count
            );
        }
        for (original_idx, _) in rows {
            row_to_original.push(*original_idx);
        }
        all_batches.extend(batches.iter().cloned());
    }

    if all_batches.is_empty() || row_to_original.is_empty() {
        perf_println!("⏱️ FFI_DIAG: no data to export");
        return Ok(0);
    }

    // Step 3: Concatenate all batches into a single RecordBatch
    let t_concat = std::time::Instant::now();
    let first_schema = all_batches[0].schema();
    let combined = arrow::compute::concat_batches(&first_schema, all_batches.iter())
        .context("Failed to concatenate record batches")?;
    perf_println!(
        "⏱️ FFI_DIAG: concat_batches: {} rows, {} columns, took {}ms",
        combined.num_rows(), combined.num_columns(), t_concat.elapsed().as_millis()
    );

    // Step 4: Reorder rows to match original request order
    // Build permutation: output_position[i] = source_row that should go to position i
    let t_reorder = std::time::Instant::now();
    let mut perm: Vec<(usize, usize)> = row_to_original
        .iter()
        .enumerate()
        .map(|(src_row, &orig_idx)| (orig_idx, src_row))
        .collect();
    perm.sort_by_key(|(orig_idx, _)| *orig_idx);

    let indices = UInt32Array::from_iter_values(perm.iter().map(|(_, src_row)| *src_row as u32));
    let reordered_columns: Vec<_> = combined
        .columns()
        .iter()
        .map(|col| arrow::compute::take(col.as_ref(), &indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()
        .context("Failed to reorder columns via take()")?;
    let reordered = RecordBatch::try_new(combined.schema(), reordered_columns)
        .context("Failed to create reordered RecordBatch")?;
    perf_println!(
        "⏱️ FFI_DIAG: row reorder took {}ms",
        t_reorder.elapsed().as_millis()
    );

    // Step 5: Rename columns from parquet names to tantivy field names
    let t_rename = std::time::Instant::now();
    let renamed = rename_columns_to_tantivy(&reordered, &manifest.column_mapping)?;
    perf_println!(
        "⏱️ FFI_DIAG: column rename took {}ms",
        t_rename.elapsed().as_millis()
    );

    // Step 6: Export each column via Arrow FFI
    let num_cols = renamed.num_columns();
    if array_addrs.len() < num_cols || schema_addrs.len() < num_cols {
        anyhow::bail!(
            "Insufficient FFI addresses: need {} columns but got {} array_addrs and {} schema_addrs",
            num_cols, array_addrs.len(), schema_addrs.len()
        );
    }

    let t_ffi = std::time::Instant::now();
    for (i, col) in renamed.columns().iter().enumerate() {
        // Validate addresses are non-null before unsafe write
        if array_addrs[i] == 0 || schema_addrs[i] == 0 {
            anyhow::bail!(
                "Null FFI address for column {}: array_addr={}, schema_addr={}",
                i, array_addrs[i], schema_addrs[i]
            );
        }

        let data = if col.offset() != 0 {
            // Non-zero offset: use take() to create an offset-0 copy
            // (required for FFI consumers that don't handle offsets)
            let take_indices = UInt32Array::from_iter_values(0..renamed.num_rows() as u32);
            arrow::compute::take(col.as_ref(), &take_indices, None)
                .context("Failed to normalize column offset via take()")?
                .to_data()
        } else {
            col.to_data()
        };

        let array_ptr = array_addrs[i] as *mut FFI_ArrowArray;
        let schema_ptr = schema_addrs[i] as *mut FFI_ArrowSchema;

        unsafe {
            std::ptr::write_unaligned(array_ptr, FFI_ArrowArray::new(&data));
            std::ptr::write_unaligned(
                schema_ptr,
                FFI_ArrowSchema::try_from(data.data_type())
                    .map_err(|e| anyhow::anyhow!("FFI_ArrowSchema conversion failed for column {}: {}", i, e))?,
            );
        }
    }
    perf_println!(
        "⏱️ FFI_DIAG: FFI export of {} columns took {}ms",
        num_cols, t_ffi.elapsed().as_millis()
    );

    let row_count = renamed.num_rows();
    perf_println!(
        "⏱️ FFI_DIAG: batch_parquet_to_arrow_ffi TOTAL: {} rows, {} columns, took {}ms",
        row_count, num_cols, t_total.elapsed().as_millis()
    );

    Ok(row_count)
}

/// Rename RecordBatch columns from parquet names to tantivy field names.
///
/// This is a zero-copy operation — only the schema metadata changes, the data
/// buffers are shared via Arc.
fn rename_columns_to_tantivy(
    batch: &RecordBatch,
    column_mapping: &[ColumnMapping],
) -> Result<RecordBatch> {
    // Build a lookup: parquet_column_name → tantivy_field_name
    let pq_to_tantivy: std::collections::HashMap<&str, &str> = column_mapping
        .iter()
        .map(|m| (m.parquet_column_name.as_str(), m.tantivy_field_name.as_str()))
        .collect();

    let renamed_fields: Vec<Arc<Field>> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| {
            let name = f.name().as_str();
            let tantivy_name = pq_to_tantivy
                .get(name)
                .copied()
                .unwrap_or(name);
            Arc::new(f.as_ref().clone().with_name(tantivy_name))
        })
        .collect();

    let renamed_schema = Arc::new(Schema::new(renamed_fields));
    RecordBatch::try_new(renamed_schema, batch.columns().to_vec())
        .context("Failed to create renamed RecordBatch")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::*;
    use arrow_schema::{DataType, Field, Schema};

    #[test]
    fn test_rename_columns_to_tantivy() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pq_name", DataType::Utf8, false),
            Field::new("pq_score", DataType::Int64, false),
            Field::new("unmapped_col", DataType::Float64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["alice", "bob"])),
                Arc::new(Int64Array::from(vec![100, 200])),
                Arc::new(Float64Array::from(vec![1.5, 2.5])),
            ],
        )
        .unwrap();

        let mappings = vec![
            ColumnMapping {
                tantivy_field_name: "name".to_string(),
                parquet_column_name: "pq_name".to_string(),
                physical_ordinal: 0,
                parquet_type: "BYTE_ARRAY".to_string(),
                tantivy_type: "Str".to_string(),
                field_id: None,
                fast_field_tokenizer: None,
            },
            ColumnMapping {
                tantivy_field_name: "score".to_string(),
                parquet_column_name: "pq_score".to_string(),
                physical_ordinal: 1,
                parquet_type: "INT64".to_string(),
                tantivy_type: "I64".to_string(),
                field_id: None,
                fast_field_tokenizer: None,
            },
        ];

        let renamed = rename_columns_to_tantivy(&batch, &mappings).unwrap();
        let schema = renamed.schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(field_names, vec!["name", "score", "unmapped_col"]);
        assert_eq!(renamed.num_rows(), 2);
        assert_eq!(renamed.num_columns(), 3);
    }

    #[test]
    fn test_rename_preserves_data() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col_a", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![10, 20, 30]))],
        )
        .unwrap();

        let mappings = vec![ColumnMapping {
            tantivy_field_name: "field_a".to_string(),
            parquet_column_name: "col_a".to_string(),
            physical_ordinal: 0,
            parquet_type: "INT32".to_string(),
            tantivy_type: "I64".to_string(),
            field_id: None,
            fast_field_tokenizer: None,
        }];

        let renamed = rename_columns_to_tantivy(&batch, &mappings).unwrap();
        let col = renamed.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(col.value(0), 10);
        assert_eq!(col.value(1), 20);
        assert_eq!(col.value(2), 30);
    }
}
