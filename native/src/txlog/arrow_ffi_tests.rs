// txlog/arrow_ffi_tests.rs - Integration tests for Arrow FFI export of FileEntry records
//
// Tests cover: RecordBatch column types, nullability, full field population,
// multi-entry batches, partition values JSON roundtrip, FFI export/reimport,
// and the end-to-end pipeline: write version -> Avro checkpoint -> read -> Arrow FFI.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema, from_ffi};

use crate::txlog::actions::*;
use crate::txlog::arrow_ffi::*;

// ============================================================================
// Test helpers
// ============================================================================

fn make_test_entry(path: &str, size: i64, num_records: Option<i64>) -> FileEntry {
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
            num_records,
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
        added_at_version: 1,
        added_at_timestamp: 1700000000000,
    }
}

fn make_full_entry(path: &str) -> FileEntry {
    let mut pv = HashMap::new();
    pv.insert("year".to_string(), "2024".to_string());
    pv.insert("month".to_string(), "01".to_string());

    let mut tags = HashMap::new();
    tags.insert("source".to_string(), "indexer-1".to_string());

    let mut min_vals = HashMap::new();
    min_vals.insert("timestamp".to_string(), "1000".to_string());
    let mut max_vals = HashMap::new();
    max_vals.insert("timestamp".to_string(), "2000".to_string());

    FileEntry {
        add: AddAction {
            path: path.to_string(),
            partition_values: pv,
            size: 50000,
            modification_time: 1700000000000,
            data_change: true,
            stats: Some(r#"{"numRecords":100}"#.to_string()),
            min_values: Some(min_vals),
            max_values: Some(max_vals),
            num_records: Some(100),
            footer_start_offset: Some(49000),
            footer_end_offset: Some(50000),
            has_footer_offsets: Some(true),
            split_tags: Some(tags),
            num_merge_ops: Some(2),
            doc_mapping_json: Some(
                r#"{"fields":[{"name":"title","type":"text"}]}"#.to_string(),
            ),
            doc_mapping_ref: None,
            uncompressed_size_bytes: Some(100000),
            time_range_start: Some(1000),
            time_range_end: Some(2000),
            companion_source_files: Some(vec![
                "file1.parquet".to_string(),
                "file2.parquet".to_string(),
            ]),
            companion_delta_version: Some(42),
            companion_fast_field_mode: Some("HYBRID".to_string()),
        },
        added_at_version: 5,
        added_at_timestamp: 1700000050000,
    }
}

// ============================================================================
// 1. Arrow RecordBatch Conversion Tests
// ============================================================================

#[test]
fn test_ffi_batch_column_types() {
    let entries = vec![make_test_entry("test.split", 1000, Some(50))];
    let batch = file_entries_to_record_batch(&entries).unwrap();
    let schema = batch.schema();

    // Verify each column has the correct Arrow DataType
    let expected: Vec<(&str, DataType)> = vec![
        ("path", DataType::Utf8),
        ("size", DataType::Int64),
        ("modification_time", DataType::Int64),
        ("data_change", DataType::Boolean),
        ("num_records", DataType::Int64),
        ("partition_values", DataType::Utf8),
        ("stats", DataType::Utf8),
        ("min_values", DataType::Utf8),
        ("max_values", DataType::Utf8),
        ("footer_start_offset", DataType::Int64),
        ("footer_end_offset", DataType::Int64),
        ("has_footer_offsets", DataType::Boolean),
        ("split_tags", DataType::Utf8),
        ("num_merge_ops", DataType::Int32),
        ("doc_mapping_json", DataType::Utf8),
        ("uncompressed_size_bytes", DataType::Int64),
        ("time_range_start", DataType::Int64),
        ("time_range_end", DataType::Int64),
        ("companion_source_files", DataType::Utf8),
        ("companion_delta_version", DataType::Int64),
        ("companion_fast_field_mode", DataType::Utf8),
        ("added_at_version", DataType::Int64),
        ("added_at_timestamp", DataType::Int64),
    ];

    assert_eq!(schema.fields().len(), expected.len());
    for (i, (name, dtype)) in expected.iter().enumerate() {
        let field = schema.field(i);
        assert_eq!(field.name(), *name, "Column {} name mismatch", i);
        assert_eq!(
            field.data_type(),
            dtype,
            "Column '{}' type mismatch",
            name
        );
    }
}

#[test]
fn test_ffi_batch_required_columns_not_null() {
    let entries = vec![make_test_entry("required.split", 5000, None)];
    let batch = file_entries_to_record_batch(&entries).unwrap();

    // Required (non-nullable) columns: path, size, modification_time, data_change,
    // added_at_version, added_at_timestamp
    let required_columns: Vec<(&str, usize)> = vec![
        ("path", 0),
        ("size", 1),
        ("modification_time", 2),
        ("data_change", 3),
        ("added_at_version", 21),
        ("added_at_timestamp", 22),
    ];

    for (name, idx) in &required_columns {
        let col = batch.column(*idx);
        assert_eq!(
            col.null_count(),
            0,
            "Required column '{}' (idx {}) should have no nulls",
            name,
            idx
        );
    }

    // Verify schema declares them as non-nullable
    let schema = batch.schema();
    for (name, idx) in &required_columns {
        assert!(
            !schema.field(*idx).is_nullable(),
            "Required column '{}' should be non-nullable in schema",
            name
        );
    }
}

#[test]
fn test_ffi_batch_nullable_columns() {
    // Entry with all Optional fields as None
    let entry = make_test_entry("nullable.split", 100, None);
    let batch = file_entries_to_record_batch(&[entry]).unwrap();

    // All nullable columns should be null for this entry
    let nullable_columns: Vec<(&str, usize)> = vec![
        ("num_records", 4),
        ("partition_values", 5),
        ("stats", 6),
        ("min_values", 7),
        ("max_values", 8),
        ("footer_start_offset", 9),
        ("footer_end_offset", 10),
        ("has_footer_offsets", 11),
        ("split_tags", 12),
        ("num_merge_ops", 13),
        ("doc_mapping_json", 14),
        ("uncompressed_size_bytes", 15),
        ("time_range_start", 16),
        ("time_range_end", 17),
        ("companion_source_files", 18),
        ("companion_delta_version", 19),
        ("companion_fast_field_mode", 20),
    ];

    for (name, idx) in &nullable_columns {
        let col = batch.column(*idx);
        assert!(
            col.is_null(0),
            "Nullable column '{}' (idx {}) should be null when source field is None",
            name,
            idx
        );
    }

    // Verify schema declares them as nullable
    let schema = batch.schema();
    for (name, idx) in &nullable_columns {
        assert!(
            schema.field(*idx).is_nullable(),
            "Column '{}' should be nullable in schema",
            name
        );
    }
}

#[test]
fn test_ffi_batch_all_fields_populated() {
    let entry = make_full_entry("full.split");
    let batch = file_entries_to_record_batch(&[entry]).unwrap();
    assert_eq!(batch.num_rows(), 1);

    // path
    let path_col = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(path_col.value(0), "full.split");

    // size
    let size_col = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(size_col.value(0), 50000);

    // modification_time
    let mod_col = batch.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(mod_col.value(0), 1700000000000);

    // data_change
    let dc_col = batch.column(3).as_any().downcast_ref::<BooleanArray>().unwrap();
    assert!(dc_col.value(0));

    // num_records
    let nr_col = batch.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(!nr_col.is_null(0));
    assert_eq!(nr_col.value(0), 100);

    // partition_values (JSON roundtrip)
    let pv_col = batch.column(5).as_any().downcast_ref::<StringArray>().unwrap();
    assert!(!pv_col.is_null(0));
    let pv: HashMap<String, String> = serde_json::from_str(pv_col.value(0)).unwrap();
    assert_eq!(pv.get("year").unwrap(), "2024");
    assert_eq!(pv.get("month").unwrap(), "01");

    // stats
    let stats_col = batch.column(6).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(stats_col.value(0), r#"{"numRecords":100}"#);

    // min_values
    let min_col = batch.column(7).as_any().downcast_ref::<StringArray>().unwrap();
    assert!(!min_col.is_null(0));
    let min_map: HashMap<String, String> = serde_json::from_str(min_col.value(0)).unwrap();
    assert_eq!(min_map.get("timestamp").unwrap(), "1000");

    // max_values
    let max_col = batch.column(8).as_any().downcast_ref::<StringArray>().unwrap();
    assert!(!max_col.is_null(0));
    let max_map: HashMap<String, String> = serde_json::from_str(max_col.value(0)).unwrap();
    assert_eq!(max_map.get("timestamp").unwrap(), "2000");

    // footer_start_offset
    let fso_col = batch.column(9).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(fso_col.value(0), 49000);

    // footer_end_offset
    let feo_col = batch.column(10).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(feo_col.value(0), 50000);

    // split_tags (JSON)
    let tags_col = batch.column(12).as_any().downcast_ref::<StringArray>().unwrap();
    assert!(!tags_col.is_null(0));
    let tags: HashMap<String, String> = serde_json::from_str(tags_col.value(0)).unwrap();
    assert_eq!(tags.get("source").unwrap(), "indexer-1");

    // num_merge_ops
    let nmo_col = batch.column(13).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(nmo_col.value(0), 2);

    // doc_mapping_json
    let dm_col = batch.column(14).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(
        dm_col.value(0),
        r#"{"fields":[{"name":"title","type":"text"}]}"#
    );

    // uncompressed_size_bytes
    let usb_col = batch.column(15).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(usb_col.value(0), 100000);

    // time_range_start
    let trs_col = batch.column(16).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(trs_col.value(0), 1000);

    // time_range_end
    let tre_col = batch.column(17).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(tre_col.value(0), 2000);

    // companion_source_files (JSON array)
    let csf_col = batch.column(18).as_any().downcast_ref::<StringArray>().unwrap();
    assert!(!csf_col.is_null(0));
    let files: Vec<String> = serde_json::from_str(csf_col.value(0)).unwrap();
    assert_eq!(files, vec!["file1.parquet", "file2.parquet"]);

    // companion_delta_version
    let cdv_col = batch.column(19).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(cdv_col.value(0), 42);

    // companion_fast_field_mode
    let cfm_col = batch.column(20).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(cfm_col.value(0), "HYBRID");

    // added_at_version
    let aav_col = batch.column(21).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(aav_col.value(0), 5);

    // added_at_timestamp
    let aat_col = batch.column(22).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(aat_col.value(0), 1700000050000);
}

#[test]
fn test_ffi_batch_multiple_entries() {
    let entries: Vec<FileEntry> = (0..100)
        .map(|i| {
            let mut entry = make_test_entry(
                &format!("split-{:04}.split", i),
                1000 + i as i64,
                if i % 3 == 0 { Some(i as i64 * 10) } else { None },
            );
            entry.added_at_version = i as i64;
            entry.added_at_timestamp = 1700000000000 + i as i64;
            // Every 5th entry gets some optional fields
            if i % 5 == 0 {
                entry.add.stats = Some(format!(r#"{{"numRecords":{}}}"#, i * 10));
                entry.add.num_merge_ops = Some(i as i32);
            }
            entry
        })
        .collect();

    let batch = file_entries_to_record_batch(&entries).unwrap();
    assert_eq!(batch.num_rows(), 100);
    assert_eq!(batch.num_columns(), 23);

    // Spot-check: first entry
    let path_col = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(path_col.value(0), "split-0000.split");
    assert_eq!(path_col.value(99), "split-0099.split");

    // Spot-check: sizes
    let size_col = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(size_col.value(0), 1000);
    assert_eq!(size_col.value(50), 1050);
    assert_eq!(size_col.value(99), 1099);

    // Spot-check: num_records is set for i%3==0, null otherwise
    let nr_col = batch.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(!nr_col.is_null(0)); // i=0: 0%3==0
    assert_eq!(nr_col.value(0), 0);
    assert!(nr_col.is_null(1)); // i=1: 1%3!=0
    assert!(nr_col.is_null(2)); // i=2: 2%3!=0
    assert!(!nr_col.is_null(3)); // i=3: 3%3==0
    assert_eq!(nr_col.value(3), 30);

    // Spot-check: stats set for i%5==0, null otherwise
    let stats_col = batch.column(6).as_any().downcast_ref::<StringArray>().unwrap();
    assert!(!stats_col.is_null(0)); // i=0: 0%5==0
    assert!(stats_col.is_null(1)); // i=1: 1%5!=0
    assert!(!stats_col.is_null(5)); // i=5: 5%5==0

    // Spot-check: added_at_version
    let ver_col = batch.column(21).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ver_col.value(0), 0);
    assert_eq!(ver_col.value(42), 42);
    assert_eq!(ver_col.value(99), 99);
}

#[test]
fn test_ffi_batch_partition_values_json() {
    let mut entry = make_test_entry("partitioned.split", 2000, Some(50));
    let mut pv = HashMap::new();
    pv.insert("year".to_string(), "2024".to_string());
    pv.insert("month".to_string(), "01".to_string());
    entry.add.partition_values = pv;

    let batch = file_entries_to_record_batch(&[entry]).unwrap();

    let pv_col = batch.column(5).as_any().downcast_ref::<StringArray>().unwrap();
    assert!(!pv_col.is_null(0));

    // Roundtrip: parse the JSON back to HashMap and verify
    let json_str = pv_col.value(0);
    let roundtripped: HashMap<String, String> = serde_json::from_str(json_str).unwrap();
    assert_eq!(roundtripped.len(), 2);
    assert_eq!(roundtripped.get("year").unwrap(), "2024");
    assert_eq!(roundtripped.get("month").unwrap(), "01");

    // Also verify the JSON is valid by re-serializing
    let re_serialized = serde_json::to_string(&roundtripped).unwrap();
    let re_parsed: HashMap<String, String> = serde_json::from_str(&re_serialized).unwrap();
    assert_eq!(re_parsed, roundtripped);
}

// ============================================================================
// 2. FFI Export Tests
// ============================================================================

#[test]
fn test_ffi_export_to_pointers() {
    let entries = vec![
        make_test_entry("ffi-001.split", 1000, Some(10)),
        make_test_entry("ffi-002.split", 2000, Some(20)),
    ];

    let schema = file_entry_arrow_schema();
    let num_cols = schema.fields().len();

    let mut ffi_arrays: Vec<FFI_ArrowArray> =
        (0..num_cols).map(|_| FFI_ArrowArray::empty()).collect();
    let mut ffi_schemas: Vec<FFI_ArrowSchema> =
        (0..num_cols).map(|_| FFI_ArrowSchema::empty()).collect();

    let array_addrs: Vec<i64> = ffi_arrays
        .iter_mut()
        .map(|a| a as *mut FFI_ArrowArray as i64)
        .collect();
    let schema_addrs: Vec<i64> = ffi_schemas
        .iter_mut()
        .map(|s| s as *mut FFI_ArrowSchema as i64)
        .collect();

    let row_count = unsafe { export_file_entries_ffi(&entries, &array_addrs, &schema_addrs) }
        .unwrap();

    assert_eq!(row_count, 2);
}

#[test]
fn test_ffi_export_reimport() {
    let entries = vec![
        make_full_entry("reimport-001.split"),
        make_test_entry("reimport-002.split", 999, None),
    ];

    let schema = file_entry_arrow_schema();
    let num_cols = schema.fields().len();

    let mut ffi_arrays: Vec<FFI_ArrowArray> =
        (0..num_cols).map(|_| FFI_ArrowArray::empty()).collect();
    let mut ffi_schemas: Vec<FFI_ArrowSchema> =
        (0..num_cols).map(|_| FFI_ArrowSchema::empty()).collect();

    let array_addrs: Vec<i64> = ffi_arrays
        .iter_mut()
        .map(|a| a as *mut FFI_ArrowArray as i64)
        .collect();
    let schema_addrs: Vec<i64> = ffi_schemas
        .iter_mut()
        .map(|s| s as *mut FFI_ArrowSchema as i64)
        .collect();

    let row_count = unsafe { export_file_entries_ffi(&entries, &array_addrs, &schema_addrs) }
        .unwrap();
    assert_eq!(row_count, 2);

    // Reimport each column and verify data
    for i in 0..num_cols {
        let ffi_array = std::mem::replace(&mut ffi_arrays[i], FFI_ArrowArray::empty());
        let ffi_schema = std::mem::replace(&mut ffi_schemas[i], FFI_ArrowSchema::empty());
        let data = unsafe { from_ffi(ffi_array, &ffi_schema) }.unwrap();
        let array = make_array(data);
        assert_eq!(
            array.len(),
            2,
            "Reimported column {} should have 2 rows",
            i
        );
    }

    // Re-export to verify specific column values after reimport
    let entries2 = vec![make_full_entry("reimport-verify.split")];
    let mut ffi_arrays2: Vec<FFI_ArrowArray> =
        (0..num_cols).map(|_| FFI_ArrowArray::empty()).collect();
    let mut ffi_schemas2: Vec<FFI_ArrowSchema> =
        (0..num_cols).map(|_| FFI_ArrowSchema::empty()).collect();
    let addrs2: Vec<i64> = ffi_arrays2
        .iter_mut()
        .map(|a| a as *mut FFI_ArrowArray as i64)
        .collect();
    let saddrs2: Vec<i64> = ffi_schemas2
        .iter_mut()
        .map(|s| s as *mut FFI_ArrowSchema as i64)
        .collect();

    unsafe { export_file_entries_ffi(&entries2, &addrs2, &saddrs2) }.unwrap();

    // Reimport path column (index 0) and verify
    let ffi_a = std::mem::replace(&mut ffi_arrays2[0], FFI_ArrowArray::empty());
    let ffi_s = std::mem::replace(&mut ffi_schemas2[0], FFI_ArrowSchema::empty());
    let data = unsafe { from_ffi(ffi_a, &ffi_s) }.unwrap();
    let path_array = make_array(data);
    let path_col = path_array.as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(path_col.value(0), "reimport-verify.split");

    // Reimport size column (index 1) and verify
    let ffi_a = std::mem::replace(&mut ffi_arrays2[1], FFI_ArrowArray::empty());
    let ffi_s = std::mem::replace(&mut ffi_schemas2[1], FFI_ArrowSchema::empty());
    let data = unsafe { from_ffi(ffi_a, &ffi_s) }.unwrap();
    let size_array = make_array(data);
    let size_col = size_array.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(size_col.value(0), 50000);

    // Reimport added_at_version column (index 21) and verify
    let ffi_a = std::mem::replace(&mut ffi_arrays2[21], FFI_ArrowArray::empty());
    let ffi_s = std::mem::replace(&mut ffi_schemas2[21], FFI_ArrowSchema::empty());
    let data = unsafe { from_ffi(ffi_a, &ffi_s) }.unwrap();
    let ver_array = make_array(data);
    let ver_col = ver_array.as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ver_col.value(0), 5);
}

#[test]
fn test_ffi_export_empty() {
    let entries: Vec<FileEntry> = vec![];

    let schema = file_entry_arrow_schema();
    let num_cols = schema.fields().len();

    let mut ffi_arrays: Vec<FFI_ArrowArray> =
        (0..num_cols).map(|_| FFI_ArrowArray::empty()).collect();
    let mut ffi_schemas: Vec<FFI_ArrowSchema> =
        (0..num_cols).map(|_| FFI_ArrowSchema::empty()).collect();

    let array_addrs: Vec<i64> = ffi_arrays
        .iter_mut()
        .map(|a| a as *mut FFI_ArrowArray as i64)
        .collect();
    let schema_addrs: Vec<i64> = ffi_schemas
        .iter_mut()
        .map(|s| s as *mut FFI_ArrowSchema as i64)
        .collect();

    let row_count = unsafe { export_file_entries_ffi(&entries, &array_addrs, &schema_addrs) }
        .unwrap();

    assert_eq!(row_count, 0);

    // Reimport first column and verify 0 rows
    let ffi_a = std::mem::replace(&mut ffi_arrays[0], FFI_ArrowArray::empty());
    let ffi_s = std::mem::replace(&mut ffi_schemas[0], FFI_ArrowSchema::empty());
    let data = unsafe { from_ffi(ffi_a, &ffi_s) }.unwrap();
    let array = make_array(data);
    assert_eq!(array.len(), 0);
}

// ============================================================================
// 3. End-to-End: Storage -> Checkpoint -> Arrow FFI
// ============================================================================

#[tokio::test]
async fn test_e2e_write_checkpoint_export_ffi() {
    // Step 1: Create test entries as AddActions and write version 0
    let add_actions: Vec<AddAction> = (0..5)
        .map(|i| AddAction {
            path: format!("e2e-split-{:03}.split", i),
            partition_values: {
                let mut pv = HashMap::new();
                pv.insert("shard".to_string(), format!("shard-{}", i % 2));
                pv
            },
            size: 10000 + i * 1000,
            modification_time: 1700000000000 + i * 1000,
            data_change: true,
            stats: Some(format!(r#"{{"numRecords":{}}}"#, (i + 1) * 100)),
            min_values: None,
            max_values: None,
            num_records: Some((i + 1) * 100),
            footer_start_offset: None,
            footer_end_offset: None,
            has_footer_offsets: None,
            split_tags: None,
            num_merge_ops: if i > 0 { Some(i as i32) } else { None },
            doc_mapping_json: if i == 0 {
                Some(r#"{"fields":[{"name":"body","type":"text"}]}"#.to_string())
            } else {
                None
            },
            doc_mapping_ref: None,
            uncompressed_size_bytes: Some((i + 1) * 50000),
            time_range_start: Some(1000 + i * 100),
            time_range_end: Some(2000 + i * 100),
            companion_source_files: if i == 2 {
                Some(vec!["data.parquet".to_string()])
            } else {
                None
            },
            companion_delta_version: if i == 2 { Some(7) } else { None },
            companion_fast_field_mode: if i == 2 {
                Some("HYBRID".to_string())
            } else {
                None
            },
        })
        .collect();

    // Step 2: Convert AddActions to FileEntry (simulating what log_replay does)
    let file_entries: Vec<FileEntry> = add_actions
        .iter()
        .enumerate()
        .map(|(i, add)| FileEntry {
            add: add.clone(),
            added_at_version: 0,
            added_at_timestamp: 1700000000000 + i as i64,
        })
        .collect();

    // Step 3: Write entries to Avro manifest and read them back (simulating checkpoint)
    let avro_bytes =
        crate::txlog::avro::manifest_writer::write_manifest_bytes(&file_entries).unwrap();
    let read_back =
        crate::txlog::avro::manifest_reader::read_manifest_bytes(&avro_bytes).unwrap();

    assert_eq!(read_back.len(), 5, "Should read back all 5 entries from Avro");

    // Step 4: Convert the read-back entries to Arrow RecordBatch
    let batch = file_entries_to_record_batch(&read_back).unwrap();
    assert_eq!(batch.num_rows(), 5);
    assert_eq!(batch.num_columns(), 23);

    // Step 5: Verify all fields match the original add_actions
    let path_col = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
    for i in 0..5 {
        assert_eq!(
            path_col.value(i),
            format!("e2e-split-{:03}.split", i),
            "Path mismatch at row {}",
            i
        );
    }

    let size_col = batch.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    for i in 0..5i64 {
        assert_eq!(
            size_col.value(i as usize),
            10000 + i * 1000,
            "Size mismatch at row {}",
            i
        );
    }

    let nr_col = batch.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
    for i in 0..5i64 {
        assert!(!nr_col.is_null(i as usize));
        assert_eq!(
            nr_col.value(i as usize),
            (i + 1) * 100,
            "num_records mismatch at row {}",
            i
        );
    }

    // Verify stats column
    let stats_col = batch.column(6).as_any().downcast_ref::<StringArray>().unwrap();
    for i in 0..5 {
        assert!(!stats_col.is_null(i));
        assert!(
            stats_col.value(i).contains("numRecords"),
            "Stats at row {} should contain numRecords",
            i
        );
    }

    // Verify num_merge_ops: row 0 = None (null), rows 1-4 = Some(i)
    let nmo_col = batch.column(13).as_any().downcast_ref::<Int32Array>().unwrap();
    assert!(nmo_col.is_null(0));
    for i in 1..5 {
        assert!(!nmo_col.is_null(i));
        assert_eq!(nmo_col.value(i), i as i32);
    }

    // Verify doc_mapping_json: only row 0 has it
    let dm_col = batch.column(14).as_any().downcast_ref::<StringArray>().unwrap();
    assert!(!dm_col.is_null(0));
    assert!(dm_col.value(0).contains("body"));
    for i in 1..5 {
        assert!(dm_col.is_null(i));
    }

    // Verify companion fields: only row 2 has them
    let csf_col = batch.column(18).as_any().downcast_ref::<StringArray>().unwrap();
    assert!(csf_col.is_null(0));
    assert!(csf_col.is_null(1));
    assert!(!csf_col.is_null(2));
    let files: Vec<String> = serde_json::from_str(csf_col.value(2)).unwrap();
    assert_eq!(files, vec!["data.parquet"]);
    assert!(csf_col.is_null(3));
    assert!(csf_col.is_null(4));

    let cdv_col = batch.column(19).as_any().downcast_ref::<Int64Array>().unwrap();
    assert!(cdv_col.is_null(0));
    assert!(!cdv_col.is_null(2));
    assert_eq!(cdv_col.value(2), 7);

    let cfm_col = batch.column(20).as_any().downcast_ref::<StringArray>().unwrap();
    assert!(cfm_col.is_null(0));
    assert!(!cfm_col.is_null(2));
    assert_eq!(cfm_col.value(2), "HYBRID");

    // Step 6: Export via FFI and reimport to verify the full pipeline
    let schema = file_entry_arrow_schema();
    let num_cols = schema.fields().len();

    let mut ffi_arrays: Vec<FFI_ArrowArray> =
        (0..num_cols).map(|_| FFI_ArrowArray::empty()).collect();
    let mut ffi_schemas: Vec<FFI_ArrowSchema> =
        (0..num_cols).map(|_| FFI_ArrowSchema::empty()).collect();

    let array_addrs: Vec<i64> = ffi_arrays
        .iter_mut()
        .map(|a| a as *mut FFI_ArrowArray as i64)
        .collect();
    let schema_addrs: Vec<i64> = ffi_schemas
        .iter_mut()
        .map(|s| s as *mut FFI_ArrowSchema as i64)
        .collect();

    let row_count =
        unsafe { export_file_entries_ffi(&read_back, &array_addrs, &schema_addrs) }.unwrap();
    assert_eq!(row_count, 5);

    // Reimport path column and verify
    let ffi_a = std::mem::replace(&mut ffi_arrays[0], FFI_ArrowArray::empty());
    let ffi_s = std::mem::replace(&mut ffi_schemas[0], FFI_ArrowSchema::empty());
    let data = unsafe { from_ffi(ffi_a, &ffi_s) }.unwrap();
    let reimported_path = make_array(data);
    let reimported_path_col = reimported_path
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(reimported_path_col.len(), 5);
    assert_eq!(reimported_path_col.value(0), "e2e-split-000.split");
    assert_eq!(reimported_path_col.value(4), "e2e-split-004.split");

    // Reimport size column and verify
    let ffi_a = std::mem::replace(&mut ffi_arrays[1], FFI_ArrowArray::empty());
    let ffi_s = std::mem::replace(&mut ffi_schemas[1], FFI_ArrowSchema::empty());
    let data = unsafe { from_ffi(ffi_a, &ffi_s) }.unwrap();
    let reimported_size = make_array(data);
    let reimported_size_col = reimported_size
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(reimported_size_col.value(0), 10000);
    assert_eq!(reimported_size_col.value(4), 14000);
}
