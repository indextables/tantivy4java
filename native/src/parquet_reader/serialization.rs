// parquet_reader/serialization.rs - ParquetTableInfo/ParquetFileEntry → TANT byte buffer
//
// Serializes parquet reader data structures into the TANT batch document protocol
// for efficient Rust→Java transfer via BatchDocumentReader.parseToMaps().

use super::distributed::{ParquetTableInfo, ParquetFileEntry};

/// Magic number for batch protocol validation ("TANT")
const MAGIC_NUMBER: u32 = 0x54414E54;

/// Field type codes (matching BatchDocumentReader on the Java side)
const FIELD_TYPE_TEXT: u8 = 0;
const FIELD_TYPE_INTEGER: u8 = 1;
const FIELD_TYPE_BOOLEAN: u8 = 3;

/// Serialize a ParquetTableInfo into TANT byte buffer format.
///
/// Doc 0 (header): schema_json, partition_columns_json, num_partitions, num_root_files, is_partitioned
/// Docs 1..N: partition directory paths (path)
/// Docs N+1..M: root file entries (path, size, last_modified) — only for unpartitioned tables
pub fn serialize_parquet_table_info(info: &ParquetTableInfo) -> Vec<u8> {
    let total_docs = 1 + info.partition_directories.len() + info.root_parquet_files.len();
    let estimated = 4 + total_docs * 200 + total_docs * 4 + 12;
    let mut buf = Vec::with_capacity(estimated);

    // Header magic
    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    let mut offsets = Vec::with_capacity(total_docs);

    // Doc 0: header metadata
    offsets.push(buf.len() as u32);
    {
        let field_count: u16 = 5;
        buf.extend_from_slice(&field_count.to_ne_bytes());

        // schema_json (TEXT)
        write_field_header(&mut buf, "schema_json", FIELD_TYPE_TEXT, 1);
        write_string(&mut buf, &info.schema_json);

        // partition_columns_json (TEXT)
        let columns_json = serde_json::to_string(&info.partition_columns)
            .unwrap_or_else(|_| "[]".to_string());
        write_field_header(&mut buf, "partition_columns_json", FIELD_TYPE_TEXT, 1);
        write_string(&mut buf, &columns_json);

        // num_partitions (INTEGER)
        write_field_header(&mut buf, "num_partitions", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&(info.partition_directories.len() as i64).to_ne_bytes());

        // num_root_files (INTEGER)
        write_field_header(&mut buf, "num_root_files", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&(info.root_parquet_files.len() as i64).to_ne_bytes());

        // is_partitioned (BOOLEAN)
        write_field_header(&mut buf, "is_partitioned", FIELD_TYPE_BOOLEAN, 1);
        buf.push(if info.is_partitioned { 1 } else { 0 });
    }

    // Docs 1..N: partition directory paths
    for dir in &info.partition_directories {
        offsets.push(buf.len() as u32);
        let field_count: u16 = 1;
        buf.extend_from_slice(&field_count.to_ne_bytes());

        write_field_header(&mut buf, "path", FIELD_TYPE_TEXT, 1);
        write_string(&mut buf, dir);
    }

    // Docs N+1..M: root file entries (unpartitioned)
    for file in &info.root_parquet_files {
        offsets.push(buf.len() as u32);
        serialize_file_entry(&mut buf, file);
    }

    // Offset table
    let offset_table_start = buf.len() as u32;
    for offset in &offsets {
        buf.extend_from_slice(&offset.to_ne_bytes());
    }

    // Footer: offset_table_pos + doc_count + magic
    buf.extend_from_slice(&offset_table_start.to_ne_bytes());
    buf.extend_from_slice(&(total_docs as u32).to_ne_bytes());
    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    buf
}

/// Serialize a list of ParquetFileEntry into TANT byte buffer format.
///
/// Each entry becomes one document with: path, size, last_modified, partition_values_json
pub fn serialize_parquet_file_entries(entries: &[ParquetFileEntry]) -> Vec<u8> {
    let estimated = 4 + entries.len() * 300 + entries.len() * 4 + 12;
    let mut buf = Vec::with_capacity(estimated);

    // Header magic
    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    let mut offsets = Vec::with_capacity(entries.len());
    for entry in entries {
        offsets.push(buf.len() as u32);
        serialize_file_entry(&mut buf, entry);
    }

    // Offset table
    let offset_table_start = buf.len() as u32;
    for offset in &offsets {
        buf.extend_from_slice(&offset.to_ne_bytes());
    }

    // Footer
    buf.extend_from_slice(&offset_table_start.to_ne_bytes());
    buf.extend_from_slice(&(entries.len() as u32).to_ne_bytes());
    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    buf
}

/// Serialize one ParquetFileEntry into the buffer.
fn serialize_file_entry(buf: &mut Vec<u8>, entry: &ParquetFileEntry) {
    let field_count: u16 = 4;
    buf.extend_from_slice(&field_count.to_ne_bytes());

    // path (TEXT)
    write_field_header(buf, "path", FIELD_TYPE_TEXT, 1);
    write_string(buf, &entry.path);

    // size (INTEGER)
    write_field_header(buf, "size", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&entry.size.to_ne_bytes());

    // last_modified (INTEGER)
    write_field_header(buf, "last_modified", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&entry.last_modified.to_ne_bytes());

    // partition_values (TEXT as JSON)
    let pv_json = serde_json::to_string(&entry.partition_values)
        .unwrap_or_else(|_| "{}".to_string());
    write_field_header(buf, "partition_values", FIELD_TYPE_TEXT, 1);
    write_string(buf, &pv_json);
}

// ─── TANT protocol helpers ──────────────────────────────────────────────────

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
    fn test_serialize_table_info_partitioned() {
        let info = ParquetTableInfo {
            schema_json: r#"{"fields":[]}"#.to_string(),
            partition_columns: vec!["year".to_string()],
            partition_directories: vec!["year=2024/".to_string()],
            root_parquet_files: vec![],
            is_partitioned: true,
        };

        let buf = serialize_parquet_table_info(&info);
        // Should have magic header
        assert_eq!(&buf[..4], &MAGIC_NUMBER.to_ne_bytes());
        // Should have magic footer
        let len = buf.len();
        assert_eq!(&buf[len - 4..], &MAGIC_NUMBER.to_ne_bytes());
        // 2 docs: header + 1 partition dir
        let doc_count =
            u32::from_ne_bytes([buf[len - 8], buf[len - 7], buf[len - 6], buf[len - 5]]);
        assert_eq!(doc_count, 2);
    }

    #[test]
    fn test_serialize_table_info_unpartitioned() {
        let info = ParquetTableInfo {
            schema_json: r#"{"fields":[]}"#.to_string(),
            partition_columns: vec![],
            partition_directories: vec![],
            root_parquet_files: vec![ParquetFileEntry {
                path: "data.parquet".to_string(),
                size: 1000,
                last_modified: 1700000000000,
                partition_values: HashMap::new(),
            }],
            is_partitioned: false,
        };

        let buf = serialize_parquet_table_info(&info);
        let len = buf.len();
        // 2 docs: header + 1 root file
        let doc_count =
            u32::from_ne_bytes([buf[len - 8], buf[len - 7], buf[len - 6], buf[len - 5]]);
        assert_eq!(doc_count, 2);
    }

    #[test]
    fn test_serialize_file_entries_empty() {
        let buf = serialize_parquet_file_entries(&[]);
        assert_eq!(&buf[..4], &MAGIC_NUMBER.to_ne_bytes());
        let len = buf.len();
        let doc_count =
            u32::from_ne_bytes([buf[len - 8], buf[len - 7], buf[len - 6], buf[len - 5]]);
        assert_eq!(doc_count, 0);
    }

    #[test]
    fn test_serialize_file_entries() {
        let entries = vec![
            ParquetFileEntry {
                path: "year=2024/part-0.parquet".to_string(),
                size: 5000,
                last_modified: 1700000000000,
                partition_values: HashMap::from([("year".to_string(), "2024".to_string())]),
            },
        ];

        let buf = serialize_parquet_file_entries(&entries);
        let len = buf.len();
        let doc_count =
            u32::from_ne_bytes([buf[len - 8], buf[len - 7], buf[len - 6], buf[len - 5]]);
        assert_eq!(doc_count, 1);
    }
}
