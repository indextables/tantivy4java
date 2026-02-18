// delta_reader/serialization.rs - DeltaFileEntry → TANT byte buffer
//
// Serializes DeltaFileEntry structs into the TANT batch document protocol,
// reusing the same binary format as batch document retrieval for efficient
// Rust→Java transfer via BatchDocumentReader.parseToMaps().

use super::scan::{DeltaFileEntry, DeltaSchemaField};

/// Magic number for batch protocol validation ("TANT")
const MAGIC_NUMBER: u32 = 0x54414E54;

/// Field type codes (matching BatchDocumentReader on the Java side)
const FIELD_TYPE_TEXT: u8 = 0;
const FIELD_TYPE_INTEGER: u8 = 1;
const FIELD_TYPE_BOOLEAN: u8 = 3;
const FIELD_TYPE_JSON: u8 = 6;

/// Serialize a list of DeltaFileEntry into the TANT byte buffer format.
///
/// Each entry becomes one "document" with either 7 fields (full) or 5 fields (compact):
///   Full:    path, size, modification_time, num_records, partition_values, has_deletion_vector, table_version
///   Compact: path, size, modification_time, num_records, table_version
///
/// Compact mode skips partition_values (JSON) and has_deletion_vector (BOOLEAN)
/// for callers that only need file identity and basic metadata.
pub fn serialize_delta_entries(entries: &[DeltaFileEntry], table_version: u64, compact: bool) -> Vec<u8> {
    let per_entry = if compact { 200 } else { 300 };
    let estimated = 4 + entries.len() * per_entry + entries.len() * 4 + 12;
    let mut buf = Vec::with_capacity(estimated);

    // Header magic
    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    // Write each entry as a document, collecting offsets
    let mut offsets = Vec::with_capacity(entries.len());
    for entry in entries {
        offsets.push(buf.len() as u32);
        serialize_entry(&mut buf, entry, table_version, compact);
    }

    // Offset table
    let offset_table_start = buf.len() as u32;
    for offset in &offsets {
        buf.extend_from_slice(&offset.to_ne_bytes());
    }

    // Footer: offset_table_pos + doc_count + magic
    buf.extend_from_slice(&offset_table_start.to_ne_bytes());
    buf.extend_from_slice(&(entries.len() as u32).to_ne_bytes());
    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    buf
}

fn serialize_entry(buf: &mut Vec<u8>, entry: &DeltaFileEntry, table_version: u64, compact: bool) {
    let field_count: u16 = if compact { 5 } else { 7 };
    buf.extend_from_slice(&field_count.to_ne_bytes());

    // 1. path (TEXT)
    write_field_header(buf, "path", FIELD_TYPE_TEXT, 1);
    write_string(buf, &entry.path);

    // 2. size (INTEGER)
    write_field_header(buf, "size", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&entry.size.to_ne_bytes());

    // 3. modification_time (INTEGER)
    write_field_header(buf, "modification_time", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&entry.modification_time.to_ne_bytes());

    // 4. num_records (INTEGER, -1 if unknown)
    write_field_header(buf, "num_records", FIELD_TYPE_INTEGER, 1);
    let num_records = entry.num_records.map(|n| n as i64).unwrap_or(-1i64);
    buf.extend_from_slice(&num_records.to_ne_bytes());

    if !compact {
        // 5. partition_values (JSON) — skipped in compact mode
        write_field_header(buf, "partition_values", FIELD_TYPE_JSON, 1);
        let pv_json = serde_json::to_string(&entry.partition_values).unwrap_or_else(|_| "{}".to_string());
        write_string(buf, &pv_json);

        // 6. has_deletion_vector (BOOLEAN) — skipped in compact mode
        write_field_header(buf, "has_deletion_vector", FIELD_TYPE_BOOLEAN, 1);
        buf.push(if entry.has_deletion_vector { 1 } else { 0 });
    }

    // table_version (INTEGER) — always included
    write_field_header(buf, "table_version", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&(table_version as i64).to_ne_bytes());
}

/// Serialize a list of DeltaSchemaField plus the raw schema JSON into the TANT byte buffer format.
///
/// Produces a single "document" with fields:
///   schema_json (TEXT) — the full Delta schema JSON
///   table_version (INTEGER) — the resolved snapshot version
///   field_count (INTEGER) — number of top-level columns
///
/// Followed by one document per field:
///   name (TEXT), data_type (TEXT), nullable (BOOLEAN), metadata (TEXT/JSON)
pub fn serialize_delta_schema(
    fields: &[DeltaSchemaField],
    schema_json: &str,
    table_version: u64,
) -> Vec<u8> {
    let doc_count = 1 + fields.len(); // 1 header doc + N field docs
    let estimated = 4 + doc_count * 200 + schema_json.len() + 12;
    let mut buf = Vec::with_capacity(estimated);

    // Header magic
    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    let mut offsets = Vec::with_capacity(doc_count);

    // Document 0: header with schema_json, table_version, field_count
    offsets.push(buf.len() as u32);
    {
        let field_count: u16 = 3;
        buf.extend_from_slice(&field_count.to_ne_bytes());

        write_field_header(&mut buf, "schema_json", FIELD_TYPE_TEXT, 1);
        write_string(&mut buf, schema_json);

        write_field_header(&mut buf, "table_version", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&(table_version as i64).to_ne_bytes());

        write_field_header(&mut buf, "field_count", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&(fields.len() as i64).to_ne_bytes());
    }

    // Documents 1..N: one per schema field
    for field in fields {
        offsets.push(buf.len() as u32);
        let field_count: u16 = 4;
        buf.extend_from_slice(&field_count.to_ne_bytes());

        write_field_header(&mut buf, "name", FIELD_TYPE_TEXT, 1);
        write_string(&mut buf, &field.name);

        write_field_header(&mut buf, "data_type", FIELD_TYPE_TEXT, 1);
        write_string(&mut buf, &field.data_type);

        write_field_header(&mut buf, "nullable", FIELD_TYPE_BOOLEAN, 1);
        buf.push(if field.nullable { 1 } else { 0 });

        write_field_header(&mut buf, "metadata", FIELD_TYPE_TEXT, 1);
        write_string(&mut buf, &field.metadata);
    }

    // Offset table
    let offset_table_start = buf.len() as u32;
    for offset in &offsets {
        buf.extend_from_slice(&offset.to_ne_bytes());
    }

    // Footer: offset_table_pos + doc_count + magic
    buf.extend_from_slice(&offset_table_start.to_ne_bytes());
    buf.extend_from_slice(&(doc_count as u32).to_ne_bytes());
    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    buf
}

/// Serialize a DeltaSnapshotInfo into the TANT byte buffer format.
///
/// Produces a single document with fields:
///   version, schema_json, partition_columns_json, checkpoint_part_paths_json,
///   commit_file_paths_json, num_add_files
pub fn serialize_snapshot_info(info: &super::distributed::DeltaSnapshotInfo) -> Vec<u8> {
    let estimated = 4 + 2000 + info.schema_json.len() + 12;
    let mut buf = Vec::with_capacity(estimated);

    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    let mut offsets = Vec::with_capacity(1);
    offsets.push(buf.len() as u32);

    let field_count: u16 = 6;
    buf.extend_from_slice(&field_count.to_ne_bytes());

    // 1. version (INTEGER)
    write_field_header(&mut buf, "version", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&(info.version as i64).to_ne_bytes());

    // 2. schema_json (TEXT)
    write_field_header(&mut buf, "schema_json", FIELD_TYPE_TEXT, 1);
    write_string(&mut buf, &info.schema_json);

    // 3. partition_columns_json (JSON)
    write_field_header(&mut buf, "partition_columns_json", FIELD_TYPE_JSON, 1);
    let pc_json = serde_json::to_string(&info.partition_columns).unwrap_or_else(|_| "[]".to_string());
    write_string(&mut buf, &pc_json);

    // 4. checkpoint_part_paths_json (JSON)
    write_field_header(&mut buf, "checkpoint_part_paths_json", FIELD_TYPE_JSON, 1);
    let cp_json = serde_json::to_string(&info.checkpoint_part_paths).unwrap_or_else(|_| "[]".to_string());
    write_string(&mut buf, &cp_json);

    // 5. commit_file_paths_json (JSON)
    write_field_header(&mut buf, "commit_file_paths_json", FIELD_TYPE_JSON, 1);
    let cf_json = serde_json::to_string(&info.commit_file_paths).unwrap_or_else(|_| "[]".to_string());
    write_string(&mut buf, &cf_json);

    // 6. num_add_files (INTEGER, -1 if unknown)
    write_field_header(&mut buf, "num_add_files", FIELD_TYPE_INTEGER, 1);
    let num_add = info.num_add_files.map(|n| n as i64).unwrap_or(-1i64);
    buf.extend_from_slice(&num_add.to_ne_bytes());

    // Offset table
    let offset_table_start = buf.len() as u32;
    for offset in &offsets {
        buf.extend_from_slice(&offset.to_ne_bytes());
    }

    // Footer
    buf.extend_from_slice(&offset_table_start.to_ne_bytes());
    buf.extend_from_slice(&(1u32).to_ne_bytes());
    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    buf
}

/// Serialize DeltaLogChanges into the TANT byte buffer format.
///
/// Document 0: header with num_added, num_removed
/// Documents 1..N: added file entries (reuses serialize_entry format)
/// Documents N+1..M: removed paths (path only)
pub fn serialize_log_changes(changes: &super::distributed::DeltaLogChanges) -> Vec<u8> {
    let doc_count = 1 + changes.added_files.len() + changes.removed_paths.len();
    let estimated = 4 + doc_count * 200 + 12;
    let mut buf = Vec::with_capacity(estimated);

    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    let mut offsets = Vec::with_capacity(doc_count);

    // Document 0: header
    offsets.push(buf.len() as u32);
    {
        let field_count: u16 = 2;
        buf.extend_from_slice(&field_count.to_ne_bytes());

        write_field_header(&mut buf, "num_added", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&(changes.added_files.len() as i64).to_ne_bytes());

        write_field_header(&mut buf, "num_removed", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&(changes.removed_paths.len() as i64).to_ne_bytes());
    }

    // Documents 1..N: added files (full format, version=0 since it's post-checkpoint)
    for entry in &changes.added_files {
        offsets.push(buf.len() as u32);
        serialize_entry(&mut buf, entry, 0, false);
    }

    // Documents N+1..M: removed paths
    for path in &changes.removed_paths {
        offsets.push(buf.len() as u32);
        let field_count: u16 = 1;
        buf.extend_from_slice(&field_count.to_ne_bytes());
        write_field_header(&mut buf, "path", FIELD_TYPE_TEXT, 1);
        write_string(&mut buf, path);
    }

    // Offset table
    let offset_table_start = buf.len() as u32;
    for offset in &offsets {
        buf.extend_from_slice(&offset.to_ne_bytes());
    }

    // Footer
    buf.extend_from_slice(&offset_table_start.to_ne_bytes());
    buf.extend_from_slice(&(doc_count as u32).to_ne_bytes());
    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    buf
}

fn write_field_header(buf: &mut Vec<u8>, name: &str, field_type: u8, value_count: u16) {
    let name_bytes = name.as_bytes();
    buf.extend_from_slice(&(name_bytes.len() as u16).to_ne_bytes());
    buf.extend_from_slice(name_bytes);
    buf.push(field_type);
    buf.extend_from_slice(&value_count.to_ne_bytes());
}

fn write_string(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    buf.extend_from_slice(&(bytes.len() as u32).to_ne_bytes());
    buf.extend_from_slice(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_serialize_empty() {
        let buf = serialize_delta_entries(&[], 5, false);
        // Header + footer only
        assert_eq!(&buf[..4], &MAGIC_NUMBER.to_ne_bytes());
        let len = buf.len();
        assert_eq!(&buf[len - 4..], &MAGIC_NUMBER.to_ne_bytes());
        // doc count = 0
        let doc_count = u32::from_ne_bytes([buf[len - 8], buf[len - 7], buf[len - 6], buf[len - 5]]);
        assert_eq!(doc_count, 0);
    }

    #[test]
    fn test_serialize_single_entry() {
        let entry = DeltaFileEntry {
            path: "part-00000.parquet".to_string(),
            size: 12345,
            modification_time: 1700000000000,
            num_records: Some(100),
            partition_values: HashMap::new(),
            has_deletion_vector: false,
        };
        let buf = serialize_delta_entries(&[entry], 3, false);

        // Validate header magic
        let header = u32::from_ne_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(header, MAGIC_NUMBER);

        // Validate footer
        let len = buf.len();
        let footer = u32::from_ne_bytes([buf[len - 4], buf[len - 3], buf[len - 2], buf[len - 1]]);
        assert_eq!(footer, MAGIC_NUMBER);

        let doc_count = u32::from_ne_bytes([buf[len - 8], buf[len - 7], buf[len - 6], buf[len - 5]]);
        assert_eq!(doc_count, 1);
    }

    #[test]
    fn test_serialize_multiple_entries() {
        let entries: Vec<DeltaFileEntry> = (0..5)
            .map(|i| DeltaFileEntry {
                path: format!("part-{:05}.parquet", i),
                size: 1000 * (i as i64 + 1),
                modification_time: 1700000000000 + i as i64,
                num_records: Some(50 + i as u64),
                partition_values: {
                    let mut m = HashMap::new();
                    m.insert("date".to_string(), format!("2024-01-{:02}", i + 1));
                    m
                },
                has_deletion_vector: i % 2 == 0,
            })
            .collect();

        let buf = serialize_delta_entries(&entries, 10, false);
        let len = buf.len();

        // Validate doc count
        let doc_count = u32::from_ne_bytes([buf[len - 8], buf[len - 7], buf[len - 6], buf[len - 5]]);
        assert_eq!(doc_count, 5);
    }

    #[test]
    fn test_serialize_unknown_num_records() {
        let entry = DeltaFileEntry {
            path: "part-00000.parquet".to_string(),
            size: 5000,
            modification_time: 1700000000000,
            num_records: None,
            partition_values: HashMap::new(),
            has_deletion_vector: false,
        };
        let buf = serialize_delta_entries(&[entry], 1, false);

        // Parse the buffer to find num_records field value
        // After header(4) + field_count(2) + path field + size field + mod_time field
        // we should find num_records = -1
        // Just verify the buffer is well-formed
        let len = buf.len();
        let doc_count = u32::from_ne_bytes([buf[len - 8], buf[len - 7], buf[len - 6], buf[len - 5]]);
        assert_eq!(doc_count, 1);
        assert!(buf.len() > 16); // non-trivial size
    }

    #[test]
    fn test_serialize_with_partition_values() {
        let mut pv = HashMap::new();
        pv.insert("year".to_string(), "2024".to_string());
        pv.insert("month".to_string(), "01".to_string());

        let entry = DeltaFileEntry {
            path: "year=2024/month=01/part-00000.parquet".to_string(),
            size: 8000,
            modification_time: 1700000000000,
            num_records: Some(200),
            partition_values: pv,
            has_deletion_vector: false,
        };
        let buf = serialize_delta_entries(&[entry], 7, false);
        assert!(buf.len() > 50);

        // Verify the JSON partition_values string is in the buffer
        let buf_str = String::from_utf8_lossy(&buf);
        assert!(buf_str.contains("year"));
        assert!(buf_str.contains("2024"));
    }

    #[test]
    fn test_serialize_compact_smaller_than_full() {
        let mut pv = HashMap::new();
        pv.insert("year".to_string(), "2024".to_string());
        pv.insert("month".to_string(), "01".to_string());

        let entry = DeltaFileEntry {
            path: "year=2024/month=01/part-00000.parquet".to_string(),
            size: 8000,
            modification_time: 1700000000000,
            num_records: Some(200),
            partition_values: pv,
            has_deletion_vector: true,
        };

        let full_buf = serialize_delta_entries(&[entry.clone()], 7, false);
        let compact_buf = serialize_delta_entries(&[entry], 7, true);

        // Compact should be smaller (no partition_values JSON, no has_deletion_vector)
        assert!(compact_buf.len() < full_buf.len(),
            "compact ({}) should be smaller than full ({})", compact_buf.len(), full_buf.len());

        // Both should have valid magic
        assert_eq!(&compact_buf[..4], &MAGIC_NUMBER.to_ne_bytes());
        let clen = compact_buf.len();
        assert_eq!(&compact_buf[clen - 4..], &MAGIC_NUMBER.to_ne_bytes());

        // Both should have doc count = 1
        let compact_count = u32::from_ne_bytes([compact_buf[clen - 8], compact_buf[clen - 7], compact_buf[clen - 6], compact_buf[clen - 5]]);
        assert_eq!(compact_count, 1);

        // Compact should NOT contain partition_values or has_deletion_vector field names
        let compact_str = String::from_utf8_lossy(&compact_buf);
        assert!(!compact_str.contains("partition_values"));
        assert!(!compact_str.contains("has_deletion_vector"));

        // But compact SHOULD contain path, size, table_version
        assert!(compact_str.contains("path"));
        assert!(compact_str.contains("size"));
        assert!(compact_str.contains("table_version"));
    }
}
