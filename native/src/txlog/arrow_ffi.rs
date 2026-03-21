// txlog/arrow_ffi.rs - Arrow FFI export for transaction log FileEntry records
//
// Converts Vec<FileEntry> into Arrow RecordBatch and exports via the Arrow C Data
// Interface (FFI) for Spark consumption. Each FileEntry maps to one row with columns
// derived from AddAction fields plus the streaming metadata (added_at_version,
// added_at_timestamp).

use std::sync::Arc;

use arrow::array::*;
use arrow::compute;
use arrow::datatypes::*;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;

use super::actions::FileEntry;
use super::error::{TxLogError, Result};

/// Arrow schema for FileEntry rows.
///
/// 23 columns covering AddAction fields plus streaming metadata.
pub fn file_entry_arrow_schema() -> Schema {
    Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("size", DataType::Int64, false),
        Field::new("modification_time", DataType::Int64, false),
        Field::new("data_change", DataType::Boolean, false),
        Field::new("num_records", DataType::Int64, true),
        Field::new("partition_values", DataType::Utf8, true),
        Field::new("stats", DataType::Utf8, true),
        Field::new("min_values", DataType::Utf8, true),
        Field::new("max_values", DataType::Utf8, true),
        Field::new("footer_start_offset", DataType::Int64, true),
        Field::new("footer_end_offset", DataType::Int64, true),
        Field::new("has_footer_offsets", DataType::Boolean, true),
        Field::new("split_tags", DataType::Utf8, true),
        Field::new("num_merge_ops", DataType::Int32, true),
        Field::new("doc_mapping_json", DataType::Utf8, true),
        Field::new("uncompressed_size_bytes", DataType::Int64, true),
        Field::new("time_range_start", DataType::Int64, true),
        Field::new("time_range_end", DataType::Int64, true),
        Field::new("companion_source_files", DataType::Utf8, true),
        Field::new("companion_delta_version", DataType::Int64, true),
        Field::new("companion_fast_field_mode", DataType::Utf8, true),
        Field::new("added_at_version", DataType::Int64, false),
        Field::new("added_at_timestamp", DataType::Int64, false),
    ])
}

/// Helper: serialize a HashMap<String, String> to a JSON string, or None if empty/absent.
fn hashmap_to_json(map: &Option<std::collections::HashMap<String, String>>) -> Option<String> {
    match map {
        Some(m) if !m.is_empty() => serde_json::to_string(m).ok(),
        _ => None,
    }
}

/// Helper: serialize a non-optional HashMap to JSON, or None if empty.
fn hashmap_required_to_json(map: &std::collections::HashMap<String, String>) -> Option<String> {
    if map.is_empty() {
        None
    } else {
        serde_json::to_string(map).ok()
    }
}

/// Convert a slice of FileEntry into an Arrow RecordBatch.
pub fn file_entries_to_record_batch(entries: &[FileEntry]) -> Result<RecordBatch> {
    let schema = Arc::new(file_entry_arrow_schema());
    let len = entries.len();

    // Not-null columns
    let mut path_builder = StringBuilder::with_capacity(len, len * 64);
    let mut size_builder = Int64Builder::with_capacity(len);
    let mut mod_time_builder = Int64Builder::with_capacity(len);
    let mut data_change_builder = BooleanBuilder::with_capacity(len);
    let mut added_version_builder = Int64Builder::with_capacity(len);
    let mut added_ts_builder = Int64Builder::with_capacity(len);

    // Nullable Int64 columns
    let mut num_records_builder = Int64Builder::with_capacity(len);
    let mut footer_start_builder = Int64Builder::with_capacity(len);
    let mut footer_end_builder = Int64Builder::with_capacity(len);
    let mut uncomp_size_builder = Int64Builder::with_capacity(len);
    let mut time_start_builder = Int64Builder::with_capacity(len);
    let mut time_end_builder = Int64Builder::with_capacity(len);
    let mut comp_delta_ver_builder = Int64Builder::with_capacity(len);

    // Nullable Utf8 columns
    let mut partition_vals_builder = StringBuilder::with_capacity(len, len * 32);
    let mut stats_builder = StringBuilder::with_capacity(len, len * 64);
    let mut min_vals_builder = StringBuilder::with_capacity(len, len * 32);
    let mut max_vals_builder = StringBuilder::with_capacity(len, len * 32);
    let mut split_tags_builder = StringBuilder::with_capacity(len, len * 32);
    let mut doc_mapping_builder = StringBuilder::with_capacity(len, len * 128);
    let mut comp_src_files_builder = StringBuilder::with_capacity(len, len * 64);
    let mut comp_ff_mode_builder = StringBuilder::with_capacity(len, len * 16);

    // Nullable Boolean
    let mut has_footer_offsets_builder = BooleanBuilder::with_capacity(len);

    // Nullable Int32
    let mut num_merge_ops_builder = Int32Builder::with_capacity(len);

    for entry in entries {
        let add = &entry.add;

        // Not-null
        path_builder.append_value(&add.path);
        size_builder.append_value(add.size);
        mod_time_builder.append_value(add.modification_time);
        data_change_builder.append_value(add.data_change);
        added_version_builder.append_value(entry.added_at_version);
        added_ts_builder.append_value(entry.added_at_timestamp);

        // Nullable Int64
        num_records_builder.append_option(add.num_records);
        footer_start_builder.append_option(add.footer_start_offset);
        footer_end_builder.append_option(add.footer_end_offset);
        uncomp_size_builder.append_option(add.uncompressed_size_bytes);
        time_start_builder.append_option(add.time_range_start);
        time_end_builder.append_option(add.time_range_end);
        comp_delta_ver_builder.append_option(add.companion_delta_version);
        has_footer_offsets_builder.append_option(add.has_footer_offsets);

        // Nullable Utf8 (JSON-encoded maps)
        match hashmap_required_to_json(&add.partition_values) {
            Some(s) => partition_vals_builder.append_value(&s),
            None => partition_vals_builder.append_null(),
        }
        match &add.stats {
            Some(s) => stats_builder.append_value(s),
            None => stats_builder.append_null(),
        }
        match hashmap_to_json(&add.min_values) {
            Some(s) => min_vals_builder.append_value(&s),
            None => min_vals_builder.append_null(),
        }
        match hashmap_to_json(&add.max_values) {
            Some(s) => max_vals_builder.append_value(&s),
            None => max_vals_builder.append_null(),
        }
        match hashmap_to_json(&add.split_tags) {
            Some(s) => split_tags_builder.append_value(&s),
            None => split_tags_builder.append_null(),
        }
        match &add.doc_mapping_json {
            Some(s) => doc_mapping_builder.append_value(s),
            None => doc_mapping_builder.append_null(),
        }
        match &add.companion_source_files {
            Some(files) => {
                let json = serde_json::to_string(files)
                    .map_err(|e| TxLogError::Serde(e.to_string()))?;
                comp_src_files_builder.append_value(&json);
            }
            None => comp_src_files_builder.append_null(),
        }
        match &add.companion_fast_field_mode {
            Some(s) => comp_ff_mode_builder.append_value(s),
            None => comp_ff_mode_builder.append_null(),
        }

        // Nullable Int32
        num_merge_ops_builder.append_option(add.num_merge_ops);
    }

    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(path_builder.finish()),
        Arc::new(size_builder.finish()),
        Arc::new(mod_time_builder.finish()),
        Arc::new(data_change_builder.finish()),
        Arc::new(num_records_builder.finish()),
        Arc::new(partition_vals_builder.finish()),
        Arc::new(stats_builder.finish()),
        Arc::new(min_vals_builder.finish()),
        Arc::new(max_vals_builder.finish()),
        Arc::new(footer_start_builder.finish()),
        Arc::new(footer_end_builder.finish()),
        Arc::new(has_footer_offsets_builder.finish()),
        Arc::new(split_tags_builder.finish()),
        Arc::new(num_merge_ops_builder.finish()),
        Arc::new(doc_mapping_builder.finish()),
        Arc::new(uncomp_size_builder.finish()),
        Arc::new(time_start_builder.finish()),
        Arc::new(time_end_builder.finish()),
        Arc::new(comp_src_files_builder.finish()),
        Arc::new(comp_delta_ver_builder.finish()),
        Arc::new(comp_ff_mode_builder.finish()),
        Arc::new(added_version_builder.finish()),
        Arc::new(added_ts_builder.finish()),
    ];

    RecordBatch::try_new(schema, columns)
        .map_err(|e| TxLogError::Storage(anyhow::anyhow!("Arrow error: {}", e)))
}

/// Export FileEntry records via Arrow FFI to pre-allocated C struct addresses.
///
/// Converts entries to a RecordBatch, normalizes any non-zero array offsets
/// (required for FFI consumers that don't handle offsets), then writes each
/// column's FFI_ArrowArray and FFI_ArrowSchema to the provided addresses.
///
/// # Safety
///
/// The caller must ensure that `array_addrs` and `schema_addrs` point to valid,
/// writable memory locations for FFI_ArrowArray and FFI_ArrowSchema respectively.
/// Each address pair must be pre-allocated by the Java caller.
///
/// # Returns
///
/// The number of rows exported.
pub unsafe fn export_file_entries_ffi(
    entries: &[FileEntry],
    array_addrs: &[i64],
    schema_addrs: &[i64],
) -> Result<usize> {
    let batch = file_entries_to_record_batch(entries)?;
    let num_cols = batch.num_columns();
    let num_rows = batch.num_rows();

    if array_addrs.len() < num_cols || schema_addrs.len() < num_cols {
        return Err(TxLogError::Storage(anyhow::anyhow!(
            "Insufficient FFI addresses: need {} columns but got {} array_addrs and {} schema_addrs",
            num_cols, array_addrs.len(), schema_addrs.len()
        )));
    }

    // Phase 2: validate addresses and prepare all FFI data (can fail)
    let batch_schema = batch.schema();
    let mut ffi_pairs: Vec<(FFI_ArrowArray, FFI_ArrowSchema)> = Vec::with_capacity(num_cols);

    for (i, col) in batch.columns().iter().enumerate() {
        if array_addrs[i] == 0 || schema_addrs[i] == 0 {
            return Err(TxLogError::Storage(anyhow::anyhow!(
                "Null FFI address for column {}: array_addr={}, schema_addr={}",
                i, array_addrs[i], schema_addrs[i]
            )));
        }

        // Normalize non-zero offsets with arrow::compute::take()
        let data = if col.offset() != 0 {
            let take_indices = UInt32Array::from_iter_values(0..num_rows as u32);
            let normalized = compute::take(col.as_ref(), &take_indices, None)
                .map_err(|e| TxLogError::Storage(anyhow::anyhow!("Arrow error: {}", e)))?;
            normalized.to_data()
        } else {
            col.to_data()
        };

        let field = batch_schema.field(i);
        let ffi_array = FFI_ArrowArray::new(&data);
        let ffi_schema = FFI_ArrowSchema::try_from(field.as_ref())
            .map_err(|e| TxLogError::Storage(anyhow::anyhow!(
                "FFI_ArrowSchema conversion failed for column {}: {}", i, e
            )))?;
        ffi_pairs.push((ffi_array, ffi_schema));
    }

    // Phase 3: write all pointers (infallible, no early returns after this point)
    for (i, (ffi_array, ffi_schema)) in ffi_pairs.into_iter().enumerate() {
        let array_ptr = array_addrs[i] as *mut FFI_ArrowArray;
        let schema_ptr = schema_addrs[i] as *mut FFI_ArrowSchema;
        std::ptr::write_unaligned(array_ptr, ffi_array);
        std::ptr::write_unaligned(schema_ptr, ffi_schema);
    }

    Ok(num_rows)
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::txlog::actions::AddAction;
    use std::collections::HashMap;

    fn make_test_entry(path: &str, size: i64, version: i64) -> FileEntry {
        FileEntry {
            add: AddAction {
                path: path.to_string(),
                partition_values: HashMap::new(),
                size,
                modification_time: 1700000000000,
                data_change: true,
                stats: None,
                min_values: None,
                max_values: None,
                num_records: Some(100),
                footer_start_offset: None,
                footer_end_offset: None,
                has_footer_offsets: None,
                split_tags: None,
                num_merge_ops: None,
                doc_mapping_json: None,
                doc_mapping_ref: None,
                uncompressed_size_bytes: None,
                time_range_start: None,
                time_range_end: None,
                companion_source_files: None,
                companion_delta_version: None,
                companion_fast_field_mode: None,
            },
            added_at_version: version,
            added_at_timestamp: 1700000000000 + version,
        }
    }

    #[test]
    fn test_schema_field_count() {
        let schema = file_entry_arrow_schema();
        assert_eq!(schema.fields().len(), 23);
        // Verify a few key field names and types
        assert_eq!(schema.field(0).name(), "path");
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert!(!schema.field(0).is_nullable());

        assert_eq!(schema.field(4).name(), "num_records");
        assert!(schema.field(4).is_nullable());

        assert_eq!(schema.field(11).name(), "has_footer_offsets");
        assert!(schema.field(11).is_nullable());

        assert_eq!(schema.field(21).name(), "added_at_version");
        assert!(!schema.field(21).is_nullable());

        assert_eq!(schema.field(22).name(), "added_at_timestamp");
        assert!(!schema.field(22).is_nullable());
    }

    #[test]
    fn test_empty_entries_to_batch() {
        let batch = file_entries_to_record_batch(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 23);
    }

    #[test]
    fn test_entries_to_batch_roundtrip() {
        let entries = vec![
            make_test_entry("split-001.split", 12345, 1),
            make_test_entry("split-002.split", 67890, 2),
        ];
        let batch = file_entries_to_record_batch(&entries).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 23);

        // Verify path column
        let path_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(path_col.value(0), "split-001.split");
        assert_eq!(path_col.value(1), "split-002.split");

        // Verify size column
        let size_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(size_col.value(0), 12345);
        assert_eq!(size_col.value(1), 67890);

        // Verify data_change column
        let dc_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert!(dc_col.value(0));
        assert!(dc_col.value(1));

        // Verify num_records (nullable but set)
        let nr_col = batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(!nr_col.is_null(0));
        assert_eq!(nr_col.value(0), 100);

        // Verify added_at_version
        let ver_col = batch
            .column(21)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ver_col.value(0), 1);
        assert_eq!(ver_col.value(1), 2);

        // Verify added_at_timestamp
        let ts_col = batch
            .column(22)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ts_col.value(0), 1700000000001);
        assert_eq!(ts_col.value(1), 1700000000002);
    }

    #[test]
    fn test_entries_with_optional_fields() {
        let mut entry = make_test_entry("split-opt.split", 999, 5);
        // Set some optional fields
        let mut tags = HashMap::new();
        tags.insert("region".to_string(), "us-east-1".to_string());
        entry.add.split_tags = Some(tags);
        entry.add.num_merge_ops = Some(3);
        entry.add.doc_mapping_json = Some("{\"field\":\"value\"}".to_string());
        entry.add.companion_source_files = Some(vec!["file1.parquet".to_string(), "file2.parquet".to_string()]);
        entry.add.companion_delta_version = Some(42);
        entry.add.companion_fast_field_mode = Some("hybrid".to_string());
        entry.add.footer_start_offset = Some(1024);
        entry.add.footer_end_offset = Some(2048);
        entry.add.uncompressed_size_bytes = Some(500000);
        entry.add.time_range_start = Some(1000);
        entry.add.time_range_end = Some(2000);

        let mut min_vals = HashMap::new();
        min_vals.insert("score".to_string(), "0".to_string());
        entry.add.min_values = Some(min_vals);

        let mut max_vals = HashMap::new();
        max_vals.insert("score".to_string(), "100".to_string());
        entry.add.max_values = Some(max_vals);

        // Create a second entry with all optional fields as None
        let entry_none = make_test_entry("split-none.split", 111, 6);

        let batch = file_entries_to_record_batch(&[entry, entry_none]).unwrap();
        assert_eq!(batch.num_rows(), 2);

        // split_tags: row 0 populated, row 1 null (col 12 after has_footer_offsets at 11)
        let tags_col = batch.column(12).as_any().downcast_ref::<StringArray>().unwrap();
        assert!(!tags_col.is_null(0));
        let tags_json: HashMap<String, String> = serde_json::from_str(tags_col.value(0)).unwrap();
        assert_eq!(tags_json.get("region").unwrap(), "us-east-1");
        assert!(tags_col.is_null(1));

        // num_merge_ops: row 0 = 3, row 1 = null
        let merge_col = batch.column(13).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(merge_col.value(0), 3);
        assert!(merge_col.is_null(1));

        // doc_mapping_json: row 0 populated, row 1 null
        let dm_col = batch.column(14).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(dm_col.value(0), "{\"field\":\"value\"}");
        assert!(dm_col.is_null(1));

        // companion_source_files: row 0 = JSON array, row 1 = null
        let csf_col = batch.column(18).as_any().downcast_ref::<StringArray>().unwrap();
        assert!(!csf_col.is_null(0));
        let files: Vec<String> = serde_json::from_str(csf_col.value(0)).unwrap();
        assert_eq!(files, vec!["file1.parquet", "file2.parquet"]);
        assert!(csf_col.is_null(1));

        // companion_delta_version: row 0 = 42, row 1 = null
        let cdv_col = batch.column(19).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(cdv_col.value(0), 42);
        assert!(cdv_col.is_null(1));

        // companion_fast_field_mode: row 0 = "hybrid", row 1 = null
        let cff_col = batch.column(20).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(cff_col.value(0), "hybrid");
        assert!(cff_col.is_null(1));

        // footer offsets: row 0 populated, row 1 null
        let fs_col = batch.column(9).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(fs_col.value(0), 1024);
        assert!(fs_col.is_null(1));

        let fe_col = batch.column(10).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(fe_col.value(0), 2048);
        assert!(fe_col.is_null(1));

        // min_values / max_values: row 0 populated, row 1 null
        let min_col = batch.column(7).as_any().downcast_ref::<StringArray>().unwrap();
        assert!(!min_col.is_null(0));
        let min_map: HashMap<String, String> = serde_json::from_str(min_col.value(0)).unwrap();
        assert_eq!(min_map.get("score").unwrap(), "0");
        assert!(min_col.is_null(1));

        let max_col = batch.column(8).as_any().downcast_ref::<StringArray>().unwrap();
        assert!(!max_col.is_null(0));
        let max_map: HashMap<String, String> = serde_json::from_str(max_col.value(0)).unwrap();
        assert_eq!(max_map.get("score").unwrap(), "100");
        assert!(max_col.is_null(1));
    }
}
