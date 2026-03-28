// txlog/arrow_ffi.rs - Arrow FFI export for transaction log FileEntry records
//
// Converts Vec<FileEntry> into Arrow RecordBatch and exports via the Arrow C Data
// Interface (FFI). Features:
//   - Dynamic "partition:{name}" Utf8 columns from partition_columns
//   - split_tags and companion_source_files as List<Utf8> (not JSON strings)
//   - Optional min_values/max_values columns for CBO statistics

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::*;
use arrow::compute;
use arrow::datatypes::*;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow::record_batch::RecordBatch;

use super::actions::FileEntry;
use super::error::{TxLogError, Result};

/// Build the Arrow schema dynamically based on partition columns and options.
///
/// Base schema has 19 fixed columns. Additional columns are appended:
///   - N "partition:{name}" Utf8 columns (one per partition column)
///   - Optionally "min_values" and "max_values" Utf8 columns (JSON-encoded stats)
pub fn file_entry_arrow_schema(
    partition_columns: &[String],
    include_stats: bool,
) -> Schema {
    let mut fields = vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("size", DataType::Int64, false),
        Field::new("modification_time", DataType::Int64, false),
        Field::new("data_change", DataType::Boolean, false),
        Field::new("num_records", DataType::Int64, true),
        Field::new("footer_start_offset", DataType::Int64, true),
        Field::new("footer_end_offset", DataType::Int64, true),
        Field::new("has_footer_offsets", DataType::Boolean, true),
        Field::new("delete_opstamp", DataType::Int64, true),
        Field::new("split_tags", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true),
        Field::new("num_merge_ops", DataType::Int32, true),
        Field::new("doc_mapping_json", DataType::Utf8, true),
        Field::new("doc_mapping_ref", DataType::Utf8, true),
        Field::new("uncompressed_size_bytes", DataType::Int64, true),
        Field::new("time_range_start", DataType::Int64, true),
        Field::new("time_range_end", DataType::Int64, true),
        Field::new("companion_source_files", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true),
        Field::new("companion_delta_version", DataType::Int64, true),
        Field::new("companion_fast_field_mode", DataType::Utf8, true),
    ];

    for col_name in partition_columns {
        fields.push(Field::new(format!("partition:{}", col_name), DataType::Utf8, true));
    }

    if include_stats {
        fields.push(Field::new("min_values", DataType::Utf8, true));
        fields.push(Field::new("max_values", DataType::Utf8, true));
    }

    Schema::new(fields)
}

/// Helper: serialize an optional HashMap<String, String> to a JSON string.
fn hashmap_to_json(map: &Option<HashMap<String, String>>) -> Option<String> {
    match map {
        Some(m) if !m.is_empty() => serde_json::to_string(m).ok(),
        _ => None,
    }
}

/// Convert FileEntry slice to Arrow RecordBatch.
pub fn file_entries_to_record_batch(
    entries: &[FileEntry],
    partition_columns: &[String],
    include_stats: bool,
) -> Result<RecordBatch> {
    let schema = Arc::new(file_entry_arrow_schema(partition_columns, include_stats));
    let len = entries.len();

    let mut path_builder = StringBuilder::with_capacity(len, len * 64);
    let mut size_builder = Int64Builder::with_capacity(len);
    let mut mod_time_builder = Int64Builder::with_capacity(len);
    let mut data_change_builder = BooleanBuilder::with_capacity(len);
    let mut num_records_builder = Int64Builder::with_capacity(len);
    let mut footer_start_builder = Int64Builder::with_capacity(len);
    let mut footer_end_builder = Int64Builder::with_capacity(len);
    let mut has_footer_offsets_builder = BooleanBuilder::with_capacity(len);
    let mut delete_opstamp_builder = Int64Builder::with_capacity(len);
    let mut num_merge_ops_builder = Int32Builder::with_capacity(len);
    let mut doc_mapping_builder = StringBuilder::with_capacity(len, len * 128);
    let mut doc_mapping_ref_builder = StringBuilder::with_capacity(len, len * 32);
    let mut uncomp_size_builder = Int64Builder::with_capacity(len);
    let mut time_start_builder = Int64Builder::with_capacity(len);
    let mut time_end_builder = Int64Builder::with_capacity(len);
    let mut comp_delta_ver_builder = Int64Builder::with_capacity(len);
    let mut comp_ff_mode_builder = StringBuilder::with_capacity(len, len * 16);

    let mut split_tags_builder = ListBuilder::new(StringBuilder::new());
    let mut comp_src_files_builder = ListBuilder::new(StringBuilder::new());

    let mut partition_builders: Vec<StringBuilder> = partition_columns.iter()
        .map(|_| StringBuilder::with_capacity(len, len * 32))
        .collect();

    let mut min_vals_builder = if include_stats { Some(StringBuilder::with_capacity(len, len * 64)) } else { None };
    let mut max_vals_builder = if include_stats { Some(StringBuilder::with_capacity(len, len * 64)) } else { None };

    for entry in entries {
        let add = &entry.add;

        path_builder.append_value(&add.path);
        size_builder.append_value(add.size);
        mod_time_builder.append_value(add.modification_time);
        data_change_builder.append_value(add.data_change);
        num_records_builder.append_option(add.num_records);
        footer_start_builder.append_option(add.footer_start_offset);
        footer_end_builder.append_option(add.footer_end_offset);
        has_footer_offsets_builder.append_option(add.has_footer_offsets);
        delete_opstamp_builder.append_option(add.delete_opstamp);
        num_merge_ops_builder.append_option(add.num_merge_ops);
        uncomp_size_builder.append_option(add.uncompressed_size_bytes);
        time_start_builder.append_option(add.time_range_start);
        time_end_builder.append_option(add.time_range_end);
        comp_delta_ver_builder.append_option(add.companion_delta_version);

        match &add.doc_mapping_json {
            Some(s) => doc_mapping_builder.append_value(s),
            None => doc_mapping_builder.append_null(),
        }
        match &add.doc_mapping_ref {
            Some(s) => doc_mapping_ref_builder.append_value(s),
            None => doc_mapping_ref_builder.append_null(),
        }
        match &add.companion_fast_field_mode {
            Some(s) => comp_ff_mode_builder.append_value(s),
            None => comp_ff_mode_builder.append_null(),
        }

        // split_tags: List<Utf8>
        match &add.split_tags {
            Some(tags) => {
                for tag in tags {
                    split_tags_builder.values().append_value(tag);
                }
                split_tags_builder.append(true);
            }
            None => split_tags_builder.append(false),
        }

        // companion_source_files: List<Utf8>
        match &add.companion_source_files {
            Some(files) => {
                for file in files {
                    comp_src_files_builder.values().append_value(file);
                }
                comp_src_files_builder.append(true);
            }
            None => comp_src_files_builder.append(false),
        }

        // Partition columns
        for (i, col_name) in partition_columns.iter().enumerate() {
            match add.partition_values.get(col_name) {
                Some(v) if !v.is_empty() => partition_builders[i].append_value(v),
                _ => partition_builders[i].append_null(),
            }
        }

        if let Some(ref mut builder) = min_vals_builder {
            match hashmap_to_json(&add.min_values) {
                Some(s) => builder.append_value(&s),
                None => builder.append_null(),
            }
        }
        if let Some(ref mut builder) = max_vals_builder {
            match hashmap_to_json(&add.max_values) {
                Some(s) => builder.append_value(&s),
                None => builder.append_null(),
            }
        }
    }

    let mut columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(path_builder.finish()),
        Arc::new(size_builder.finish()),
        Arc::new(mod_time_builder.finish()),
        Arc::new(data_change_builder.finish()),
        Arc::new(num_records_builder.finish()),
        Arc::new(footer_start_builder.finish()),
        Arc::new(footer_end_builder.finish()),
        Arc::new(has_footer_offsets_builder.finish()),
        Arc::new(delete_opstamp_builder.finish()),
        Arc::new(split_tags_builder.finish()),
        Arc::new(num_merge_ops_builder.finish()),
        Arc::new(doc_mapping_builder.finish()),
        Arc::new(doc_mapping_ref_builder.finish()),
        Arc::new(uncomp_size_builder.finish()),
        Arc::new(time_start_builder.finish()),
        Arc::new(time_end_builder.finish()),
        Arc::new(comp_src_files_builder.finish()),
        Arc::new(comp_delta_ver_builder.finish()),
        Arc::new(comp_ff_mode_builder.finish()),
    ];

    for builder in partition_builders.iter_mut() {
        columns.push(Arc::new(builder.finish()));
    }
    if let Some(ref mut builder) = min_vals_builder {
        columns.push(Arc::new(builder.finish()));
    }
    if let Some(ref mut builder) = max_vals_builder {
        columns.push(Arc::new(builder.finish()));
    }

    RecordBatch::try_new(schema, columns)
        .map_err(|e| TxLogError::Storage(anyhow::anyhow!("Arrow error: {}", e)))
}

/// Export FileEntry records via Arrow FFI to pre-allocated C struct addresses.
///
/// # Safety
///
/// The caller must ensure that:
/// - `array_addrs` and `schema_addrs` each contain at least `num_columns` non-zero addresses.
/// - Each address points to at least `size_of::<FFI_ArrowArray>()` or
///   `size_of::<FFI_ArrowSchema>()` bytes of writable memory.
/// - Destinations must be uninitialized or the caller accepts that previous values
///   are overwritten without their `Drop` being called (potential leak if reusing).
///
/// # Returns
///
/// The number of rows exported.
pub unsafe fn export_file_entries_ffi(
    entries: &[FileEntry],
    partition_columns: &[String],
    include_stats: bool,
    array_addrs: &[i64],
    schema_addrs: &[i64],
) -> Result<usize> {
    let batch = file_entries_to_record_batch(entries, partition_columns, include_stats)?;
    let num_cols = batch.num_columns();
    let num_rows = batch.num_rows();

    if array_addrs.len() < num_cols || schema_addrs.len() < num_cols {
        return Err(TxLogError::Storage(anyhow::anyhow!(
            "Insufficient FFI addresses: need {} columns but got {} array_addrs and {} schema_addrs",
            num_cols, array_addrs.len(), schema_addrs.len()
        )));
    }

    let batch_schema = batch.schema();
    let mut ffi_pairs: Vec<(FFI_ArrowArray, FFI_ArrowSchema)> = Vec::with_capacity(num_cols);

    for (i, col) in batch.columns().iter().enumerate() {
        if array_addrs[i] == 0 || schema_addrs[i] == 0 {
            return Err(TxLogError::Storage(anyhow::anyhow!(
                "Null FFI address for column {}: array_addr={}, schema_addr={}",
                i, array_addrs[i], schema_addrs[i]
            )));
        }

        // Normalize non-zero offsets: some FFI consumers (Spark's ArrowColumnVector)
        // don't handle Arrow array offsets correctly, reading from index 0 instead of
        // the offset. Materializing via take() guarantees offset=0 in the exported data.
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

    for (i, (ffi_array, ffi_schema)) in ffi_pairs.into_iter().enumerate() {
        let array_ptr = array_addrs[i] as *mut FFI_ArrowArray;
        let schema_ptr = schema_addrs[i] as *mut FFI_ArrowSchema;
        std::ptr::write_unaligned(array_ptr, ffi_array);
        std::ptr::write_unaligned(schema_ptr, ffi_schema);
    }

    Ok(num_rows)
}

/// Return the number of columns for a given configuration.
pub fn column_count(partition_columns: &[String], include_stats: bool) -> usize {
    let base = 19;
    let partitions = partition_columns.len();
    let stats = if include_stats { 2 } else { 0 };
    base + partitions + stats
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::txlog::actions::AddAction;

    fn make_entry(path: &str, partitions: &[(&str, &str)], tags: Option<Vec<&str>>) -> FileEntry {
        FileEntry {
            add: AddAction {
                path: path.to_string(),
                partition_values: partitions.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
                size: 1000,
                modification_time: 1700000000000,
                data_change: true,
                stats: None,
                min_values: Some([("age".to_string(), "20".to_string())].into()),
                max_values: Some([("age".to_string(), "40".to_string())].into()),
                num_records: Some(100),
                footer_start_offset: Some(500),
                footer_end_offset: Some(1000),
                has_footer_offsets: Some(true),
                delete_opstamp: None,
                split_tags: tags.map(|t| t.into_iter().map(|s| s.to_string()).collect()),
                num_merge_ops: Some(2),
                doc_mapping_json: None,
                doc_mapping_ref: None,
                uncompressed_size_bytes: Some(2000),
                time_range_start: None,
                time_range_end: None,
                companion_source_files: None,
                companion_delta_version: None,
                companion_fast_field_mode: None,
            },
            added_at_version: 5,
            added_at_timestamp: 1700000001000,
        }
    }

    #[test]
    fn test_schema_no_partitions() {
        let schema = file_entry_arrow_schema(&[], false);
        assert_eq!(schema.fields().len(), 19);
    }

    #[test]
    fn test_schema_with_partitions() {
        let cols = vec!["year".to_string(), "month".to_string()];
        let schema = file_entry_arrow_schema(&cols, false);
        assert_eq!(schema.fields().len(), 21);
        assert_eq!(schema.field(19).name(), "partition:year");
        assert_eq!(schema.field(20).name(), "partition:month");
    }

    #[test]
    fn test_schema_with_stats() {
        let schema = file_entry_arrow_schema(&[], true);
        assert_eq!(schema.fields().len(), 21);
        assert_eq!(schema.field(19).name(), "min_values");
        assert_eq!(schema.field(20).name(), "max_values");
    }

    #[test]
    fn test_column_count() {
        assert_eq!(column_count(&[], false), 19);
        assert_eq!(column_count(&["a".into(), "b".into()], false), 21);
        assert_eq!(column_count(&[], true), 21);
        assert_eq!(column_count(&["a".into()], true), 22);
    }

    #[test]
    fn test_record_batch_basic() {
        let entries = vec![
            make_entry("file1.split", &[("year", "2024")], Some(vec!["tag1", "tag2"])),
            make_entry("file2.split", &[("year", "2023")], None),
        ];
        let partition_cols = vec!["year".to_string()];
        let batch = file_entries_to_record_batch(&entries, &partition_cols, false).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 20);

        let path_col = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(path_col.value(0), "file1.split");
        assert_eq!(path_col.value(1), "file2.split");

        let year_col = batch.column(19).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(year_col.value(0), "2024");
        assert_eq!(year_col.value(1), "2023");

        // Verify footer offsets at columns 5 and 6
        let fs_col = batch.column(5).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(fs_col.value(0), 500); // from make_entry
        let fe_col = batch.column(6).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(fe_col.value(0), 1000);
        // has_footer_offsets at column 7
        let hfo_col = batch.column(7).as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(hfo_col.value(0), true);

        let tags_col = batch.column(9).as_any().downcast_ref::<ListArray>().unwrap();
        assert!(!tags_col.is_null(0));
        assert!(tags_col.is_null(1));

        let tags_0 = tags_col.value(0);
        let tags_arr = tags_0.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(tags_arr.len(), 2);
        assert_eq!(tags_arr.value(0), "tag1");
        assert_eq!(tags_arr.value(1), "tag2");
    }

    #[test]
    fn test_record_batch_with_stats() {
        let entries = vec![make_entry("file1.split", &[], None)];
        let batch = file_entries_to_record_batch(&entries, &[], true).unwrap();
        assert_eq!(batch.num_columns(), 21);

        let min_col = batch.column(19).as_any().downcast_ref::<StringArray>().unwrap();
        assert!(!min_col.is_null(0));
        let min_json: HashMap<String, String> = serde_json::from_str(min_col.value(0)).unwrap();
        assert_eq!(min_json.get("age").unwrap(), "20");
    }

    #[test]
    fn test_missing_partition_value_is_null() {
        let entries = vec![make_entry("file1.split", &[("year", "2024")], None)];
        let partition_cols = vec!["year".to_string(), "month".to_string()];
        let batch = file_entries_to_record_batch(&entries, &partition_cols, false).unwrap();

        let month_col = batch.column(20).as_any().downcast_ref::<StringArray>().unwrap();
        assert!(month_col.is_null(0));
    }

    #[test]
    fn test_empty_batch() {
        let batch = file_entries_to_record_batch(&[], &[], false).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 19);
    }

    #[test]
    fn test_partitions_and_stats_together() {
        let entries = vec![make_entry("f.split", &[("year", "2024")], None)];
        let partition_cols = vec!["year".to_string()];
        let batch = file_entries_to_record_batch(&entries, &partition_cols, true).unwrap();
        // 19 base + 1 partition + 2 stats = 22
        assert_eq!(batch.num_columns(), 22);
        // partition:year at index 19
        assert_eq!(batch.schema().field(19).name(), "partition:year");
        let year_col = batch.column(19).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(year_col.value(0), "2024");
        // min_values at index 20, max_values at index 21
        assert_eq!(batch.schema().field(20).name(), "min_values");
        assert_eq!(batch.schema().field(21).name(), "max_values");
    }

    #[test]
    fn test_ffi_export_insufficient_addresses() {
        let entries = vec![make_entry("f.split", &[], None)];
        // Only provide 10 addresses but need 19 columns
        let mut arrays: Vec<std::mem::MaybeUninit<arrow::ffi::FFI_ArrowArray>> =
            (0..10).map(|_| std::mem::MaybeUninit::uninit()).collect();
        let mut schemas: Vec<std::mem::MaybeUninit<arrow::ffi::FFI_ArrowSchema>> =
            (0..10).map(|_| std::mem::MaybeUninit::uninit()).collect();
        let arr_addrs: Vec<i64> = arrays.iter_mut().map(|a| a.as_mut_ptr() as i64).collect();
        let sch_addrs: Vec<i64> = schemas.iter_mut().map(|s| s.as_mut_ptr() as i64).collect();

        let result = unsafe { export_file_entries_ffi(&entries, &[], false, &arr_addrs, &sch_addrs) };
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("Insufficient FFI addresses"));
    }
}
