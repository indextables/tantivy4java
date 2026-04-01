// txlog/arrow_ffi_import.rs - Arrow FFI import for write path (FR2)
//
// Converts an Arrow RecordBatch (received via FFI) into Vec<Action> for writing
// to the transaction log. The batch must have an "action_type" column that
// dispatches each row to the correct action type.

use std::collections::HashMap;

use arrow::array::*;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};
use arrow::record_batch::RecordBatch;

use super::actions::*;
use super::error::{TxLogError, Result};

/// Import an Arrow batch from FFI pointers and convert to actions.
///
/// # Safety
///
/// The caller must ensure that:
/// - `array_addr` and `schema_addr` are non-zero and point to valid, initialized
///   `FFI_ArrowArray` and `FFI_ArrowSchema` structs respectively.
/// - Each address points to at least `size_of::<FFI_ArrowArray>()` or
///   `size_of::<FFI_ArrowSchema>()` bytes of readable memory.
/// - After this call, ownership of the `FFI_ArrowArray` is transferred to Rust.
///   The caller must not release or reuse the FFI structs.
pub unsafe fn import_and_convert_actions(array_addr: i64, schema_addr: i64) -> Result<Vec<Action>> {
    if array_addr == 0 || schema_addr == 0 {
        return Err(TxLogError::Storage(anyhow::anyhow!("Null FFI address: array_addr={}, schema_addr={}", array_addr, schema_addr)));
    }
    let ffi_array = std::ptr::read_unaligned(array_addr as *const FFI_ArrowArray);
    let ffi_schema = std::ptr::read_unaligned(schema_addr as *const FFI_ArrowSchema);

    let array_data = from_ffi(ffi_array, &ffi_schema)
        .map_err(|e| TxLogError::Storage(anyhow::anyhow!("Arrow FFI import failed: {}", e)))?;

    let batch = RecordBatch::from(arrow::array::StructArray::from(array_data));
    arrow_batch_to_actions(&batch)
}

/// Convert an Arrow RecordBatch to a Vec<Action>.
///
/// Required column: "action_type" (Utf8) with values: "add", "remove", "protocol", "metadata", "mergeskip"
/// All other columns are optional — missing columns treated as null for every row.
pub fn arrow_batch_to_actions(batch: &RecordBatch) -> Result<Vec<Action>> {
    let num_rows = batch.num_rows();
    if num_rows == 0 {
        return Ok(Vec::new());
    }

    let schema = batch.schema();

    // Find action_type column (required)
    let action_type_idx = schema.index_of("action_type")
        .map_err(|_| TxLogError::Storage(anyhow::anyhow!(
            "Arrow batch missing required 'action_type' column"
        )))?;
    let action_type_col = batch.column(action_type_idx)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| TxLogError::Storage(anyhow::anyhow!(
            "'action_type' column must be Utf8"
        )))?;

    let mut actions = Vec::with_capacity(num_rows);

    for row in 0..num_rows {
        if action_type_col.is_null(row) {
            continue;
        }
        let action_type = action_type_col.value(row);
        let action = match action_type {
            "add" => Action::Add(extract_add_action(batch, row)?),
            "remove" => Action::Remove(extract_remove_action(batch, row)?),
            "protocol" => Action::Protocol(extract_protocol_action(batch, row)?),
            "metadata" => Action::MetaData(extract_metadata_action(batch, row)?),
            "mergeskip" | "merge_skip" => Action::MergeSkip(extract_skip_action(batch, row)?),
            other => {
                return Err(TxLogError::Storage(anyhow::anyhow!(
                    "Unknown action_type '{}' at row {}", other, row
                )));
            }
        };
        actions.push(action);
    }

    Ok(actions)
}

// ============================================================================
// Column extraction helpers
// ============================================================================

fn get_string(batch: &RecordBatch, col_name: &str, row: usize) -> Option<String> {
    batch.schema().index_of(col_name).ok().and_then(|idx| {
        let col = batch.column(idx);
        if col.is_null(row) {
            return None;
        }
        col.as_any().downcast_ref::<StringArray>().map(|a| a.value(row).to_string())
    })
}

fn get_string_required(batch: &RecordBatch, col_name: &str, row: usize) -> Result<String> {
    get_string(batch, col_name, row).ok_or_else(|| TxLogError::Storage(anyhow::anyhow!(
        "Missing required column '{}' at row {}", col_name, row
    )))
}

fn get_i64(batch: &RecordBatch, col_name: &str, row: usize) -> Option<i64> {
    batch.schema().index_of(col_name).ok().and_then(|idx| {
        let col = batch.column(idx);
        if col.is_null(row) {
            return None;
        }
        col.as_any().downcast_ref::<Int64Array>().map(|a| a.value(row))
    })
}

fn get_i64_or(batch: &RecordBatch, col_name: &str, row: usize, default: i64) -> i64 {
    get_i64(batch, col_name, row).unwrap_or(default)
}

fn get_i32(batch: &RecordBatch, col_name: &str, row: usize) -> Option<i32> {
    batch.schema().index_of(col_name).ok().and_then(|idx| {
        let col = batch.column(idx);
        if col.is_null(row) {
            return None;
        }
        col.as_any().downcast_ref::<Int32Array>().map(|a| a.value(row))
    })
}

fn get_bool(batch: &RecordBatch, col_name: &str, row: usize) -> Option<bool> {
    batch.schema().index_of(col_name).ok().and_then(|idx| {
        let col = batch.column(idx);
        if col.is_null(row) {
            return None;
        }
        col.as_any().downcast_ref::<BooleanArray>().map(|a| a.value(row))
    })
}

fn get_bool_or(batch: &RecordBatch, col_name: &str, row: usize, default: bool) -> bool {
    get_bool(batch, col_name, row).unwrap_or(default)
}

fn get_u32(batch: &RecordBatch, col_name: &str, row: usize) -> Option<u32> {
    // Accept either Int32 or UInt32
    batch.schema().index_of(col_name).ok().and_then(|idx| {
        let col = batch.column(idx);
        if col.is_null(row) {
            return None;
        }
        if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
            return Some(a.value(row));
        }
        if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
            let v = a.value(row);
            if v >= 0 { return Some(v as u32); }
        }
        None
    })
}

fn get_json_map(batch: &RecordBatch, col_name: &str, row: usize) -> Option<HashMap<String, String>> {
    get_string(batch, col_name, row).and_then(|s| serde_json::from_str(&s).ok())
}

fn get_string_list(batch: &RecordBatch, col_name: &str, row: usize) -> Option<Vec<String>> {
    batch.schema().index_of(col_name).ok().and_then(|idx| {
        let col = batch.column(idx);
        if col.is_null(row) {
            return None;
        }
        // Try List<Utf8> first
        if let Some(list_arr) = col.as_any().downcast_ref::<ListArray>() {
            let values = list_arr.value(row);
            if let Some(str_arr) = values.as_any().downcast_ref::<StringArray>() {
                return Some((0..str_arr.len()).map(|i| str_arr.value(i).to_string()).collect());
            }
        }
        // Fall back to JSON string
        if let Some(str_arr) = col.as_any().downcast_ref::<StringArray>() {
            return serde_json::from_str(str_arr.value(row)).ok();
        }
        None
    })
}

// ============================================================================
// Action extractors
// ============================================================================

fn extract_add_action(batch: &RecordBatch, row: usize) -> Result<AddAction> {
    Ok(AddAction {
        path: get_string_required(batch, "path", row)?,
        size: get_i64_or(batch, "size", row, 0),
        modification_time: get_i64_or(batch, "modification_time", row, 0),
        data_change: get_bool_or(batch, "data_change", row, true),
        partition_values: get_json_map(batch, "partition_values", row).unwrap_or_default(),
        stats: get_string(batch, "stats", row),
        min_values: get_json_map(batch, "min_values", row),
        max_values: get_json_map(batch, "max_values", row),
        num_records: get_i64(batch, "num_records", row),
        footer_start_offset: get_i64(batch, "footer_start_offset", row),
        footer_end_offset: get_i64(batch, "footer_end_offset", row),
        has_footer_offsets: get_bool(batch, "has_footer_offsets", row),
        delete_opstamp: get_i64(batch, "delete_opstamp", row),
        split_tags: get_string_list(batch, "split_tags", row),
        num_merge_ops: get_i32(batch, "num_merge_ops", row),
        doc_mapping_json: get_string(batch, "doc_mapping_json", row),
        doc_mapping_ref: get_string(batch, "doc_mapping_ref", row),
        uncompressed_size_bytes: get_i64(batch, "uncompressed_size_bytes", row),
        time_range_start: get_i64(batch, "time_range_start", row).map(|v| v.to_string()),
        time_range_end: get_i64(batch, "time_range_end", row).map(|v| v.to_string()),
        companion_source_files: get_string_list(batch, "companion_source_files", row),
        companion_delta_version: get_i64(batch, "companion_delta_version", row),
        companion_fast_field_mode: get_string(batch, "companion_fast_field_mode", row),
    })
}

fn extract_remove_action(batch: &RecordBatch, row: usize) -> Result<RemoveAction> {
    Ok(RemoveAction {
        path: get_string_required(batch, "path", row)?,
        deletion_timestamp: get_i64(batch, "deletion_timestamp", row),
        data_change: get_bool_or(batch, "data_change", row, true),
        partition_values: get_json_map(batch, "partition_values", row),
        size: get_i64(batch, "size", row),
    })
}

fn extract_protocol_action(batch: &RecordBatch, row: usize) -> Result<ProtocolAction> {
    Ok(ProtocolAction {
        min_reader_version: get_u32(batch, "min_reader_version", row).unwrap_or(1),
        min_writer_version: get_u32(batch, "min_writer_version", row).unwrap_or(4),
        reader_features: get_string_list(batch, "reader_features", row).unwrap_or_default(),
        writer_features: get_string_list(batch, "writer_features", row).unwrap_or_default(),
    })
}

fn extract_metadata_action(batch: &RecordBatch, row: usize) -> Result<MetadataAction> {
    Ok(MetadataAction {
        // Accept both "metadata_id" (per FR2 spec) and "id" (short form)
        id: get_string(batch, "metadata_id", row)
            .or_else(|| get_string(batch, "id", row))
            .unwrap_or_default(),
        name: get_string(batch, "metadata_name", row)
            .or_else(|| get_string(batch, "name", row)),
        description: get_string(batch, "metadata_description", row)
            .or_else(|| get_string(batch, "description", row)),
        schema_string: get_string(batch, "schema_string", row).unwrap_or_default(),
        partition_columns: get_string_list(batch, "partition_columns", row).unwrap_or_default(),
        format: FormatSpec {
            provider: get_string(batch, "format_provider", row)
                .unwrap_or_else(|| "parquet".to_string()),
            options: get_json_map(batch, "format_options", row).unwrap_or_default(),
        },
        configuration: get_json_map(batch, "configuration", row).unwrap_or_default(),
        created_time: get_i64(batch, "created_time", row),
    })
}

fn extract_skip_action(batch: &RecordBatch, row: usize) -> Result<SkipAction> {
    Ok(SkipAction {
        path: get_string_required(batch, "path", row)?,
        skip_timestamp: get_i64_or(batch, "skip_timestamp", row, 0),
        reason: get_string(batch, "reason", row).unwrap_or_default(),
        operation: get_string(batch, "operation", row).unwrap_or_default(),
        partition_values: get_json_map(batch, "partition_values", row),
        size: get_i64(batch, "size", row),
        retry_after: get_i64(batch, "retry_after", row),
        skip_count: get_i32(batch, "skip_count", row).unwrap_or(1),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::datatypes::{DataType, Field, Schema};

    fn make_add_batch(paths: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("action_type", DataType::Utf8, false),
            Field::new("path", DataType::Utf8, false),
            Field::new("size", DataType::Int64, false),
            Field::new("modification_time", DataType::Int64, false),
            Field::new("data_change", DataType::Boolean, false),
        ]));
        let action_types: Vec<&str> = paths.iter().map(|_| "add").collect();
        RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(action_types)),
            Arc::new(StringArray::from(paths.to_vec())),
            Arc::new(Int64Array::from(vec![1000i64; paths.len()])),
            Arc::new(Int64Array::from(vec![1700000000000i64; paths.len()])),
            Arc::new(BooleanArray::from(vec![true; paths.len()])),
        ]).unwrap()
    }

    #[test]
    fn test_add_actions() {
        let batch = make_add_batch(&["file1.split", "file2.split"]);
        let actions = arrow_batch_to_actions(&batch).unwrap();
        assert_eq!(actions.len(), 2);
        match &actions[0] {
            Action::Add(add) => {
                assert_eq!(add.path, "file1.split");
                assert_eq!(add.size, 1000);
                assert!(add.data_change);
            }
            _ => panic!("Expected Add action"),
        }
    }

    #[test]
    fn test_mixed_actions() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("action_type", DataType::Utf8, false),
            Field::new("path", DataType::Utf8, true),
            Field::new("size", DataType::Int64, true),
            Field::new("modification_time", DataType::Int64, true),
            Field::new("data_change", DataType::Boolean, true),
            Field::new("deletion_timestamp", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(vec!["add", "remove"])),
            Arc::new(StringArray::from(vec!["file1.split", "file2.split"])),
            Arc::new(Int64Array::from(vec![Some(1000), None])),
            Arc::new(Int64Array::from(vec![Some(1700000000000), None])),
            Arc::new(BooleanArray::from(vec![Some(true), Some(true)])),
            Arc::new(Int64Array::from(vec![None, Some(1700000001000)])),
        ]).unwrap();
        let actions = arrow_batch_to_actions(&batch).unwrap();
        assert_eq!(actions.len(), 2);
        assert!(matches!(&actions[0], Action::Add(_)));
        assert!(matches!(&actions[1], Action::Remove(_)));
    }

    #[test]
    fn test_empty_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("action_type", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::new_empty(schema);
        let actions = arrow_batch_to_actions(&batch).unwrap();
        assert!(actions.is_empty());
    }

    #[test]
    fn test_missing_action_type_fails() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![
            Arc::new(StringArray::from(vec!["file.split"])),
        ]).unwrap();
        assert!(arrow_batch_to_actions(&batch).is_err());
    }
}
