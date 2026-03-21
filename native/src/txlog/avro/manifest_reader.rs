// txlog/avro/manifest_reader.rs - Read single Avro manifest → Vec<FileEntry>

use std::collections::HashMap;
use apache_avro::{Reader, types::Value};
use crate::debug_println;

use crate::txlog::actions::{AddAction, FileEntry};
use crate::txlog::error::{TxLogError, Result};

/// Read a single Avro manifest file and return FileEntry records.
pub fn read_manifest_bytes(data: &[u8]) -> Result<Vec<FileEntry>> {
    let reader = Reader::new(data)
        .map_err(|e| TxLogError::Avro(format!("Failed to open Avro reader: {}", e)))?;

    let mut entries = Vec::new();
    for value_result in reader {
        let value = value_result
            .map_err(|e| TxLogError::Avro(format!("Failed to read Avro record: {}", e)))?;
        let entry = avro_value_to_file_entry(&value)?;
        entries.push(entry);
    }

    debug_println!("📖 AVRO_MANIFEST: Read {} entries from manifest", entries.len());
    Ok(entries)
}

/// Convert an Avro Value (Record) to a FileEntry.
fn avro_value_to_file_entry(value: &Value) -> Result<FileEntry> {
    let record = match value {
        Value::Record(fields) => fields,
        _ => return Err(TxLogError::Avro("Expected Avro Record".to_string())),
    };

    let field_map: HashMap<&str, &Value> = record.iter()
        .map(|(name, val)| (name.as_str(), val))
        .collect();

    let add = AddAction {
        path: get_string(&field_map, "path")?,
        partition_values: get_string_map(&field_map, "partitionValues"),
        size: get_long(&field_map, "size")?,
        modification_time: get_long(&field_map, "modificationTime")?,
        data_change: get_bool(&field_map, "dataChange").unwrap_or(true),
        stats: get_optional_string(&field_map, "stats"),
        min_values: get_optional_string_map(&field_map, "minValues"),
        max_values: get_optional_string_map(&field_map, "maxValues"),
        num_records: get_optional_long(&field_map, "numRecords"),
        footer_start_offset: get_optional_long(&field_map, "footerStartOffset"),
        footer_end_offset: get_optional_long(&field_map, "footerEndOffset"),
        has_footer_offsets: get_optional_bool(&field_map, "hasFooterOffsets"),
        split_tags: get_optional_string_map(&field_map, "splitTags"),
        num_merge_ops: get_optional_int(&field_map, "numMergeOps"),
        doc_mapping_json: get_optional_string(&field_map, "docMappingJson"),
        doc_mapping_ref: get_optional_string(&field_map, "docMappingRef"),
        uncompressed_size_bytes: get_optional_long(&field_map, "uncompressedSizeBytes"),
        time_range_start: get_optional_long(&field_map, "timeRangeStart"),
        time_range_end: get_optional_long(&field_map, "timeRangeEnd"),
        companion_source_files: get_optional_string_array(&field_map, "companionSourceFiles"),
        companion_delta_version: get_optional_long(&field_map, "companionDeltaVersion"),
        companion_fast_field_mode: get_optional_string(&field_map, "companionFastFieldMode"),
    };

    let added_at_version = get_long(&field_map, "addedAtVersion").unwrap_or(0);
    let added_at_timestamp = get_long(&field_map, "addedAtTimestamp").unwrap_or(0);

    Ok(FileEntry {
        add,
        added_at_version,
        added_at_timestamp,
    })
}

// ============================================================================
// Avro value extraction helpers
// ============================================================================

fn get_string(map: &HashMap<&str, &Value>, key: &str) -> Result<String> {
    match map.get(key) {
        Some(Value::String(s)) => Ok(s.clone()),
        Some(other) => Err(TxLogError::Avro(format!("Expected string for '{}', got {:?}", key, other))),
        None => Err(TxLogError::Avro(format!("Missing required field '{}'", key))),
    }
}

fn get_long(map: &HashMap<&str, &Value>, key: &str) -> Result<i64> {
    match map.get(key) {
        Some(Value::Long(n)) => Ok(*n),
        Some(Value::Int(n)) => Ok(*n as i64),
        Some(other) => Err(TxLogError::Avro(format!("Expected long for '{}', got {:?}", key, other))),
        None => Err(TxLogError::Avro(format!("Missing required field '{}'", key))),
    }
}

fn get_bool(map: &HashMap<&str, &Value>, key: &str) -> Result<bool> {
    match map.get(key) {
        Some(Value::Boolean(b)) => Ok(*b),
        Some(other) => Err(TxLogError::Avro(format!("Expected boolean for '{}', got {:?}", key, other))),
        None => Err(TxLogError::Avro(format!("Missing required field '{}'", key))),
    }
}

fn get_string_map(map: &HashMap<&str, &Value>, key: &str) -> HashMap<String, String> {
    match map.get(key) {
        Some(Value::Map(m)) => {
            m.iter()
                .filter_map(|(k, v)| {
                    if let Value::String(s) = v {
                        Some((k.clone(), s.clone()))
                    } else {
                        None
                    }
                })
                .collect()
        }
        _ => HashMap::new(),
    }
}

fn get_optional_string(map: &HashMap<&str, &Value>, key: &str) -> Option<String> {
    match map.get(key) {
        Some(Value::Union(_, inner)) => match inner.as_ref() {
            Value::String(s) => Some(s.clone()),
            Value::Null => None,
            _ => None,
        },
        Some(Value::String(s)) => Some(s.clone()),
        _ => None,
    }
}

fn get_optional_long(map: &HashMap<&str, &Value>, key: &str) -> Option<i64> {
    match map.get(key) {
        Some(Value::Union(_, inner)) => match inner.as_ref() {
            Value::Long(n) => Some(*n),
            Value::Int(n) => Some(*n as i64),
            Value::Null => None,
            _ => None,
        },
        Some(Value::Long(n)) => Some(*n),
        Some(Value::Int(n)) => Some(*n as i64),
        _ => None,
    }
}

fn get_optional_int(map: &HashMap<&str, &Value>, key: &str) -> Option<i32> {
    match map.get(key) {
        Some(Value::Union(_, inner)) => match inner.as_ref() {
            Value::Int(n) => Some(*n),
            Value::Long(n) => Some(*n as i32),
            Value::Null => None,
            _ => None,
        },
        Some(Value::Int(n)) => Some(*n),
        _ => None,
    }
}

fn get_optional_bool(map: &HashMap<&str, &Value>, key: &str) -> Option<bool> {
    match map.get(key) {
        Some(Value::Union(_, inner)) => match inner.as_ref() {
            Value::Boolean(b) => Some(*b),
            Value::Null => None,
            _ => None,
        },
        Some(Value::Boolean(b)) => Some(*b),
        _ => None,
    }
}

fn get_optional_string_map(map: &HashMap<&str, &Value>, key: &str) -> Option<HashMap<String, String>> {
    match map.get(key) {
        Some(Value::Union(_, inner)) => match inner.as_ref() {
            Value::Map(m) => {
                let result: HashMap<String, String> = m.iter()
                    .filter_map(|(k, v)| {
                        if let Value::String(s) = v { Some((k.clone(), s.clone())) } else { None }
                    })
                    .collect();
                if result.is_empty() { None } else { Some(result) }
            }
            Value::Null => None,
            _ => None,
        },
        Some(Value::Map(m)) => {
            let result: HashMap<String, String> = m.iter()
                .filter_map(|(k, v)| {
                    if let Value::String(s) = v { Some((k.clone(), s.clone())) } else { None }
                })
                .collect();
            if result.is_empty() { None } else { Some(result) }
        }
        _ => None,
    }
}

fn get_optional_string_array(map: &HashMap<&str, &Value>, key: &str) -> Option<Vec<String>> {
    match map.get(key) {
        Some(Value::Union(_, inner)) => match inner.as_ref() {
            Value::Array(arr) => {
                let result: Vec<String> = arr.iter()
                    .filter_map(|v| if let Value::String(s) = v { Some(s.clone()) } else { None })
                    .collect();
                if result.is_empty() { None } else { Some(result) }
            }
            Value::Null => None,
            _ => None,
        },
        Some(Value::Array(arr)) => {
            let result: Vec<String> = arr.iter()
                .filter_map(|v| if let Value::String(s) = v { Some(s.clone()) } else { None })
                .collect();
            if result.is_empty() { None } else { Some(result) }
        }
        _ => None,
    }
}
