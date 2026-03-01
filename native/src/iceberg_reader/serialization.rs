// iceberg_reader/serialization.rs - Iceberg data → TANT byte buffer
//
// Serializes Iceberg file entries, schema fields, and snapshots into the TANT
// batch document protocol for efficient Rust→Java transfer via
// BatchDocumentReader.parseToMaps().

use super::scan::{IcebergFileEntry, IcebergSchemaField, IcebergSnapshot};

/// Magic number for batch protocol validation ("TANT")
const MAGIC_NUMBER: u32 = 0x54414E54;

/// Field type codes (matching BatchDocumentReader on the Java side)
const FIELD_TYPE_TEXT: u8 = 0;
const FIELD_TYPE_INTEGER: u8 = 1;
const FIELD_TYPE_BOOLEAN: u8 = 3;
const FIELD_TYPE_JSON: u8 = 6;

/// Serialize a list of IcebergFileEntry into the TANT byte buffer format.
///
/// Each entry becomes one document with either 7 fields (full) or 5 fields (compact):
///   Full:    path, file_format, record_count, file_size_bytes, partition_values, content_type, snapshot_id
///   Compact: path, file_format, record_count, file_size_bytes, snapshot_id
pub fn serialize_iceberg_entries(
    entries: &[IcebergFileEntry],
    _actual_snapshot_id: i64,
    compact: bool,
) -> Vec<u8> {
    let per_entry = if compact { 200 } else { 350 };
    let estimated = 4 + entries.len() * per_entry + entries.len() * 4 + 12;
    let mut buf = Vec::with_capacity(estimated);

    // Header magic
    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    let mut offsets = Vec::with_capacity(entries.len());
    for entry in entries {
        offsets.push(buf.len() as u32);
        serialize_file_entry(&mut buf, entry, compact);
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

fn serialize_file_entry(buf: &mut Vec<u8>, entry: &IcebergFileEntry, compact: bool) {
    let field_count: u16 = if compact { 5 } else { 7 };
    buf.extend_from_slice(&field_count.to_ne_bytes());

    // 1. path (TEXT)
    write_field_header(buf, "path", FIELD_TYPE_TEXT, 1);
    write_string(buf, &entry.path);

    // 2. file_format (TEXT)
    write_field_header(buf, "file_format", FIELD_TYPE_TEXT, 1);
    write_string(buf, &entry.file_format);

    // 3. record_count (INTEGER)
    write_field_header(buf, "record_count", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&entry.record_count.to_ne_bytes());

    // 4. file_size_bytes (INTEGER)
    write_field_header(buf, "file_size_bytes", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&entry.file_size_bytes.to_ne_bytes());

    if !compact {
        // 5. partition_values (JSON)
        write_field_header(buf, "partition_values", FIELD_TYPE_JSON, 1);
        let pv_json = serde_json::to_string(&entry.partition_values)
            .unwrap_or_else(|_| "{}".to_string());
        write_string(buf, &pv_json);

        // 6. content_type (TEXT)
        write_field_header(buf, "content_type", FIELD_TYPE_TEXT, 1);
        write_string(buf, &entry.content_type);
    }

    // 7/5. snapshot_id (INTEGER)
    write_field_header(buf, "snapshot_id", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&entry.snapshot_id.to_ne_bytes());
}

/// Serialize a list of IcebergSchemaField plus the raw schema JSON into TANT format.
///
/// Document layout:
///   Doc 0 (header): schema_json, snapshot_id, field_count
///   Docs 1..N: name, data_type, field_id, nullable, doc
pub fn serialize_iceberg_schema(
    fields: &[IcebergSchemaField],
    schema_json: &str,
    snapshot_id: i64,
) -> Vec<u8> {
    let doc_count = 1 + fields.len();
    let estimated = 4 + doc_count * 200 + schema_json.len() + 12;
    let mut buf = Vec::with_capacity(estimated);

    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    let mut offsets = Vec::with_capacity(doc_count);

    // Document 0: header with schema_json, snapshot_id, field_count
    offsets.push(buf.len() as u32);
    {
        let field_count: u16 = 3;
        buf.extend_from_slice(&field_count.to_ne_bytes());

        write_field_header(&mut buf, "schema_json", FIELD_TYPE_TEXT, 1);
        write_string(&mut buf, schema_json);

        write_field_header(&mut buf, "snapshot_id", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&snapshot_id.to_ne_bytes());

        write_field_header(&mut buf, "field_count", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&(fields.len() as i64).to_ne_bytes());
    }

    // Documents 1..N: one per schema field
    for field in fields {
        offsets.push(buf.len() as u32);
        let field_count: u16 = 5;
        buf.extend_from_slice(&field_count.to_ne_bytes());

        write_field_header(&mut buf, "name", FIELD_TYPE_TEXT, 1);
        write_string(&mut buf, &field.name);

        write_field_header(&mut buf, "data_type", FIELD_TYPE_TEXT, 1);
        write_string(&mut buf, &field.data_type);

        write_field_header(&mut buf, "field_id", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&(field.field_id as i64).to_ne_bytes());

        write_field_header(&mut buf, "nullable", FIELD_TYPE_BOOLEAN, 1);
        buf.push(if field.nullable { 1 } else { 0 });

        write_field_header(&mut buf, "doc", FIELD_TYPE_TEXT, 1);
        write_string(&mut buf, field.doc.as_deref().unwrap_or(""));
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

/// Serialize a list of IcebergSnapshot into TANT format.
///
/// Each snapshot becomes one document with fields:
///   snapshot_id, parent_snapshot_id, sequence_number, timestamp_ms,
///   manifest_list, operation, summary (JSON)
pub fn serialize_iceberg_snapshots(snapshots: &[IcebergSnapshot]) -> Vec<u8> {
    let estimated = 4 + snapshots.len() * 400 + snapshots.len() * 4 + 12;
    let mut buf = Vec::with_capacity(estimated);

    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    let mut offsets = Vec::with_capacity(snapshots.len());
    for snapshot in snapshots {
        offsets.push(buf.len() as u32);
        serialize_snapshot(&mut buf, snapshot);
    }

    // Offset table
    let offset_table_start = buf.len() as u32;
    for offset in &offsets {
        buf.extend_from_slice(&offset.to_ne_bytes());
    }

    // Footer
    buf.extend_from_slice(&offset_table_start.to_ne_bytes());
    buf.extend_from_slice(&(snapshots.len() as u32).to_ne_bytes());
    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    buf
}

fn serialize_snapshot(buf: &mut Vec<u8>, snapshot: &IcebergSnapshot) {
    let field_count: u16 = 7;
    buf.extend_from_slice(&field_count.to_ne_bytes());

    // 1. snapshot_id (INTEGER)
    write_field_header(buf, "snapshot_id", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&snapshot.snapshot_id.to_ne_bytes());

    // 2. parent_snapshot_id (INTEGER, -1 if None)
    write_field_header(buf, "parent_snapshot_id", FIELD_TYPE_INTEGER, 1);
    let parent_id = snapshot.parent_snapshot_id.unwrap_or(-1);
    buf.extend_from_slice(&parent_id.to_ne_bytes());

    // 3. sequence_number (INTEGER)
    write_field_header(buf, "sequence_number", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&snapshot.sequence_number.to_ne_bytes());

    // 4. timestamp_ms (INTEGER)
    write_field_header(buf, "timestamp_ms", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&snapshot.timestamp_ms.to_ne_bytes());

    // 5. manifest_list (TEXT)
    write_field_header(buf, "manifest_list", FIELD_TYPE_TEXT, 1);
    write_string(buf, &snapshot.manifest_list);

    // 6. operation (TEXT)
    write_field_header(buf, "operation", FIELD_TYPE_TEXT, 1);
    write_string(buf, &snapshot.operation);

    // 7. summary (JSON)
    write_field_header(buf, "summary", FIELD_TYPE_JSON, 1);
    let summary_json = serde_json::to_string(&snapshot.summary)
        .unwrap_or_else(|_| "{}".to_string());
    write_string(buf, &summary_json);
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

/// Serialize an IcebergSnapshotInfo into the TANT byte buffer format.
///
/// Document 0: header with snapshot_id, schema_json, partition_spec_json, manifest_count
/// Documents 1..N: one per manifest with manifest metadata
pub fn serialize_iceberg_snapshot_info(info: &super::distributed::IcebergSnapshotInfo) -> Vec<u8> {
    let doc_count = 1 + info.manifest_entries.len();
    let estimated = 4 + doc_count * 300 + info.schema_json.len() + 12;
    let mut buf = Vec::with_capacity(estimated);

    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    let mut offsets = Vec::with_capacity(doc_count);

    // Document 0: header
    offsets.push(buf.len() as u32);
    {
        let field_count: u16 = 4;
        buf.extend_from_slice(&field_count.to_ne_bytes());

        write_field_header(&mut buf, "snapshot_id", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&info.snapshot_id.to_ne_bytes());

        write_field_header(&mut buf, "schema_json", FIELD_TYPE_TEXT, 1);
        write_string(&mut buf, &info.schema_json);

        write_field_header(&mut buf, "partition_spec_json", FIELD_TYPE_TEXT, 1);
        write_string(&mut buf, &info.partition_spec_json);

        write_field_header(&mut buf, "manifest_count", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&(info.manifest_entries.len() as i64).to_ne_bytes());
    }

    // Documents 1..N: manifest entries
    for mf in &info.manifest_entries {
        offsets.push(buf.len() as u32);
        let field_count: u16 = 7;
        buf.extend_from_slice(&field_count.to_ne_bytes());

        write_field_header(&mut buf, "manifest_path", FIELD_TYPE_TEXT, 1);
        write_string(&mut buf, &mf.manifest_path);

        write_field_header(&mut buf, "manifest_length", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&mf.manifest_length.to_ne_bytes());

        write_field_header(&mut buf, "added_snapshot_id", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&mf.added_snapshot_id.to_ne_bytes());

        write_field_header(&mut buf, "added_files_count", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&mf.added_files_count.to_ne_bytes());

        write_field_header(&mut buf, "existing_files_count", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&mf.existing_files_count.to_ne_bytes());

        write_field_header(&mut buf, "deleted_files_count", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&mf.deleted_files_count.to_ne_bytes());

        write_field_header(&mut buf, "partition_spec_id", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&(mf.partition_spec_id as i64).to_ne_bytes());
    }

    // Offset table + footer
    let offset_table_start = buf.len() as u32;
    for offset in &offsets {
        buf.extend_from_slice(&offset.to_ne_bytes());
    }
    buf.extend_from_slice(&offset_table_start.to_ne_bytes());
    buf.extend_from_slice(&(doc_count as u32).to_ne_bytes());
    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    buf
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_serialize_empty_entries() {
        let buf = serialize_iceberg_entries(&[], 100, false);
        assert_eq!(&buf[..4], &MAGIC_NUMBER.to_ne_bytes());
        let len = buf.len();
        assert_eq!(&buf[len - 4..], &MAGIC_NUMBER.to_ne_bytes());
        let doc_count = u32::from_ne_bytes([buf[len - 8], buf[len - 7], buf[len - 6], buf[len - 5]]);
        assert_eq!(doc_count, 0);
    }

    #[test]
    fn test_serialize_single_entry() {
        let entry = IcebergFileEntry {
            path: "s3://bucket/data/part-00000.parquet".to_string(),
            file_format: "parquet".to_string(),
            record_count: 1000,
            file_size_bytes: 50000,
            partition_values: HashMap::new(),
            content_type: "data".to_string(),
            snapshot_id: 12345,
        };
        let buf = serialize_iceberg_entries(&[entry], 12345, false);

        let header = u32::from_ne_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(header, MAGIC_NUMBER);

        let len = buf.len();
        let footer = u32::from_ne_bytes([buf[len - 4], buf[len - 3], buf[len - 2], buf[len - 1]]);
        assert_eq!(footer, MAGIC_NUMBER);

        let doc_count = u32::from_ne_bytes([buf[len - 8], buf[len - 7], buf[len - 6], buf[len - 5]]);
        assert_eq!(doc_count, 1);
    }

    #[test]
    fn test_serialize_compact_smaller() {
        let mut pv = HashMap::new();
        pv.insert("year".to_string(), "2024".to_string());

        let entry = IcebergFileEntry {
            path: "s3://bucket/data/part-00000.parquet".to_string(),
            file_format: "parquet".to_string(),
            record_count: 1000,
            file_size_bytes: 50000,
            partition_values: pv,
            content_type: "data".to_string(),
            snapshot_id: 12345,
        };

        let full_buf = serialize_iceberg_entries(&[entry.clone()], 12345, false);
        let compact_buf = serialize_iceberg_entries(&[entry], 12345, true);

        assert!(
            compact_buf.len() < full_buf.len(),
            "compact ({}) should be smaller than full ({})",
            compact_buf.len(),
            full_buf.len()
        );

        let compact_str = String::from_utf8_lossy(&compact_buf);
        assert!(!compact_str.contains("partition_values"));
        assert!(!compact_str.contains("content_type"));
        assert!(compact_str.contains("path"));
    }

    #[test]
    fn test_serialize_schema() {
        let fields = vec![
            IcebergSchemaField {
                name: "id".to_string(),
                data_type: "long".to_string(),
                field_id: 1,
                nullable: false,
                doc: Some("Primary key".to_string()),
            },
            IcebergSchemaField {
                name: "name".to_string(),
                data_type: "string".to_string(),
                field_id: 2,
                nullable: true,
                doc: None,
            },
        ];

        let buf = serialize_iceberg_schema(&fields, r#"{"type":"struct"}"#, 100);

        let header = u32::from_ne_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(header, MAGIC_NUMBER);

        let len = buf.len();
        let doc_count = u32::from_ne_bytes([buf[len - 8], buf[len - 7], buf[len - 6], buf[len - 5]]);
        assert_eq!(doc_count, 3); // 1 header + 2 fields
    }

    #[test]
    fn test_serialize_snapshots() {
        let mut summary = HashMap::new();
        summary.insert("operation".to_string(), "append".to_string());

        let snapshots = vec![IcebergSnapshot {
            snapshot_id: 100,
            parent_snapshot_id: None,
            sequence_number: 1,
            timestamp_ms: 1700000000000,
            manifest_list: "s3://bucket/metadata/snap-manifest-list.avro".to_string(),
            operation: "append".to_string(),
            summary,
        }];

        let buf = serialize_iceberg_snapshots(&snapshots);

        let header = u32::from_ne_bytes([buf[0], buf[1], buf[2], buf[3]]);
        assert_eq!(header, MAGIC_NUMBER);

        let len = buf.len();
        let doc_count = u32::from_ne_bytes([buf[len - 8], buf[len - 7], buf[len - 6], buf[len - 5]]);
        assert_eq!(doc_count, 1);
    }

    #[test]
    fn test_serialize_empty_snapshots() {
        let buf = serialize_iceberg_snapshots(&[]);
        let len = buf.len();
        let doc_count = u32::from_ne_bytes([buf[len - 8], buf[len - 7], buf[len - 6], buf[len - 5]]);
        assert_eq!(doc_count, 0);
    }

    #[test]
    fn test_serialize_snapshot_with_parent() {
        let snapshots = vec![IcebergSnapshot {
            snapshot_id: 200,
            parent_snapshot_id: Some(100),
            sequence_number: 2,
            timestamp_ms: 1700000001000,
            manifest_list: "s3://bucket/metadata/snap-200.avro".to_string(),
            operation: "overwrite".to_string(),
            summary: HashMap::new(),
        }];

        let buf = serialize_iceberg_snapshots(&snapshots);
        let buf_str = String::from_utf8_lossy(&buf);
        assert!(buf_str.contains("snapshot_id"));
        assert!(buf_str.contains("parent_snapshot_id"));
        assert!(buf_str.contains("overwrite"));
    }
}
