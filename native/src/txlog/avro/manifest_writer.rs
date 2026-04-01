// txlog/avro/manifest_writer.rs - Write Avro manifests with FileEntry records

use std::collections::HashMap;
use apache_avro::{Writer, types::Value};

use crate::txlog::actions::FileEntry;
use crate::txlog::error::{TxLogError, Result};
use super::schemas::file_entry_schema;

/// Serialize a list of FileEntry records to Avro bytes.
pub fn write_manifest_bytes(entries: &[FileEntry]) -> Result<Vec<u8>> {
    let schema = file_entry_schema();
    let mut writer = Writer::new(&schema, Vec::new());

    for entry in entries {
        let value = file_entry_to_avro_value(entry);
        writer.append(value)
            .map_err(|e| TxLogError::Avro(format!("Failed to write Avro record: {}", e)))?;
    }

    writer.into_inner()
        .map_err(|e| TxLogError::Avro(format!("Failed to flush Avro writer: {}", e)))
}

/// Convert a FileEntry to an Avro Value (Record).
fn file_entry_to_avro_value(entry: &FileEntry) -> Value {
    let a = &entry.add;

    let partition_values: HashMap<String, Value> = a.partition_values.iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();

    Value::Record(vec![
        ("path".to_string(), Value::String(a.path.clone())),
        ("partitionValues".to_string(), Value::Map(partition_values)),
        ("size".to_string(), Value::Long(a.size)),
        ("modificationTime".to_string(), Value::Long(a.modification_time)),
        ("dataChange".to_string(), Value::Boolean(a.data_change)),
        ("stats".to_string(), nullable_string(&a.stats)),
        ("minValues".to_string(), nullable_string_map(&a.min_values)),
        ("maxValues".to_string(), nullable_string_map(&a.max_values)),
        ("numRecords".to_string(), nullable_long(&a.num_records)),
        ("footerStartOffset".to_string(), nullable_long(&a.footer_start_offset)),
        ("footerEndOffset".to_string(), nullable_long(&a.footer_end_offset)),
        ("hasFooterOffsets".to_string(), Value::Boolean(a.has_footer_offsets.unwrap_or(false))),
        ("deleteOpstamp".to_string(), nullable_long(&a.delete_opstamp)),
        ("splitTags".to_string(), nullable_string_array(&a.split_tags)),
        ("numMergeOps".to_string(), nullable_int(&a.num_merge_ops)),
        ("docMappingJson".to_string(), nullable_string(&a.doc_mapping_json)),
        ("docMappingRef".to_string(), nullable_string(&a.doc_mapping_ref)),
        ("uncompressedSizeBytes".to_string(), nullable_long(&a.uncompressed_size_bytes)),
        ("timeRangeStart".to_string(), nullable_string(&a.time_range_start)),
        ("timeRangeEnd".to_string(), nullable_string(&a.time_range_end)),
        ("addedAtVersion".to_string(), Value::Long(entry.added_at_version)),
        ("addedAtTimestamp".to_string(), Value::Long(entry.added_at_timestamp)),
        ("companionSourceFiles".to_string(), nullable_string_array(&a.companion_source_files)),
        ("companionDeltaVersion".to_string(), nullable_long(&a.companion_delta_version)),
        ("companionFastFieldMode".to_string(), nullable_string(&a.companion_fast_field_mode)),
    ])
}

// ============================================================================
// Avro nullable value helpers
// ============================================================================

fn nullable_string(opt: &Option<String>) -> Value {
    match opt {
        Some(s) => Value::Union(1, Box::new(Value::String(s.clone()))),
        None => Value::Union(0, Box::new(Value::Null)),
    }
}

fn nullable_long(opt: &Option<i64>) -> Value {
    match opt {
        Some(n) => Value::Union(1, Box::new(Value::Long(*n))),
        None => Value::Union(0, Box::new(Value::Null)),
    }
}

fn nullable_int(opt: &Option<i32>) -> Value {
    match opt {
        Some(n) => Value::Union(1, Box::new(Value::Int(*n))),
        None => Value::Union(0, Box::new(Value::Null)),
    }
}

fn nullable_string_map(opt: &Option<HashMap<String, String>>) -> Value {
    match opt {
        Some(m) => {
            let avro_map: HashMap<String, Value> = m.iter()
                .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                .collect();
            Value::Union(1, Box::new(Value::Map(avro_map)))
        }
        None => Value::Union(0, Box::new(Value::Null)),
    }
}

fn nullable_string_array(opt: &Option<Vec<String>>) -> Value {
    match opt {
        Some(arr) => {
            let avro_arr: Vec<Value> = arr.iter().map(|s| Value::String(s.clone())).collect();
            Value::Union(1, Box::new(Value::Array(avro_arr)))
        }
        None => Value::Union(0, Box::new(Value::Null)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::txlog::actions::AddAction;
    use crate::txlog::avro::manifest_reader::read_manifest_bytes;

    fn make_entry(path: &str) -> FileEntry {
        FileEntry {
            add: AddAction {
                path: path.to_string(),
                partition_values: HashMap::new(),
                size: 1000,
                modification_time: 1700000000000,
                data_change: true,
                stats: None, min_values: None, max_values: None,
                num_records: Some(50),
                footer_start_offset: None, footer_end_offset: None, has_footer_offsets: None, delete_opstamp: None,
                split_tags: None, num_merge_ops: None,
                doc_mapping_json: None, doc_mapping_ref: Some("abc123".to_string()),
                uncompressed_size_bytes: Some(5000),
                time_range_start: Some("1000".to_string()), time_range_end: Some("2000".to_string()),
                companion_source_files: None, companion_delta_version: None,
                companion_fast_field_mode: None,
            },
            added_at_version: 10,
            added_at_timestamp: 1700000000000,
        }
    }

    #[test]
    fn test_write_read_roundtrip() {
        let entries = vec![make_entry("a.split"), make_entry("b.split")];
        let bytes = write_manifest_bytes(&entries).unwrap();
        let read_back = read_manifest_bytes(&bytes).unwrap();
        assert_eq!(read_back.len(), 2);
        assert_eq!(read_back[0].add.path, "a.split");
        assert_eq!(read_back[1].add.path, "b.split");
        assert_eq!(read_back[0].added_at_version, 10);
        assert_eq!(read_back[0].add.num_records, Some(50));
        assert_eq!(read_back[0].add.doc_mapping_ref, Some("abc123".to_string()));
    }

    #[test]
    fn test_write_empty() {
        let bytes = write_manifest_bytes(&[]).unwrap();
        let read_back = read_manifest_bytes(&bytes).unwrap();
        assert!(read_back.is_empty());
    }

    #[test]
    fn test_write_with_partition_values() {
        let mut entry = make_entry("partitioned.split");
        entry.add.partition_values.insert("year".to_string(), "2024".to_string());
        entry.add.partition_values.insert("month".to_string(), "01".to_string());

        let bytes = write_manifest_bytes(&[entry]).unwrap();
        let read_back = read_manifest_bytes(&bytes).unwrap();
        assert_eq!(read_back[0].add.partition_values.get("year"), Some(&"2024".to_string()));
        assert_eq!(read_back[0].add.partition_values.get("month"), Some(&"01".to_string()));
    }
}
