// txlog/serialization.rs - TANT byte buffer serialization for JNI
//
// Reuses the exact same binary format as delta_reader/serialization.rs
// for efficient Rust→Java transfer via BatchDocumentReader.parseToMaps().

use std::collections::HashMap;
use super::actions::*;
use super::distributed::{TxLogSnapshotInfo, TxLogChanges, WriteResult};

const MAGIC_NUMBER: u32 = 0x54414E54;
const FIELD_TYPE_TEXT: u8 = 0;
const FIELD_TYPE_INTEGER: u8 = 1;
const FIELD_TYPE_BOOLEAN: u8 = 3;
const FIELD_TYPE_JSON: u8 = 6;

// ============================================================================
// FileEntry serialization
// ============================================================================

/// Serialize file entries into TANT byte buffer.
pub fn serialize_file_entries(entries: &[FileEntry]) -> Vec<u8> {
    let estimated = 4 + entries.len() * 400 + entries.len() * 4 + 12;
    let mut buf = Vec::with_capacity(estimated);
    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    let mut offsets = Vec::with_capacity(entries.len());
    for entry in entries {
        offsets.push(buf.len() as u32);
        serialize_file_entry(&mut buf, entry);
    }

    write_footer(&mut buf, &offsets);
    buf
}

fn serialize_file_entry(buf: &mut Vec<u8>, entry: &FileEntry) {
    let a = &entry.add;
    // Count non-null optional fields to determine field count
    let mut field_count: u16 = 7; // path, size, mod_time, data_change, num_records, added_at_version, added_at_timestamp
    if !a.partition_values.is_empty() { field_count += 1; }
    if a.stats.is_some() { field_count += 1; }
    if a.min_values.is_some() { field_count += 1; }
    if a.max_values.is_some() { field_count += 1; }
    if a.footer_start_offset.is_some() { field_count += 1; }
    if a.footer_end_offset.is_some() { field_count += 1; }
    if a.has_footer_offsets.is_some() { field_count += 1; }
    if a.delete_opstamp.is_some() { field_count += 1; }
    if a.split_tags.is_some() { field_count += 1; }
    if a.num_merge_ops.is_some() { field_count += 1; }
    if a.doc_mapping_json.is_some() { field_count += 1; }
    if a.doc_mapping_ref.is_some() { field_count += 1; }
    if a.uncompressed_size_bytes.is_some() { field_count += 1; }
    if a.time_range_start.is_some() { field_count += 1; }
    if a.time_range_end.is_some() { field_count += 1; }
    if a.companion_source_files.is_some() { field_count += 1; }
    if a.companion_delta_version.is_some() { field_count += 1; }
    if a.companion_fast_field_mode.is_some() { field_count += 1; }

    buf.extend_from_slice(&field_count.to_ne_bytes());

    // Required fields
    write_field_header(buf, "path", FIELD_TYPE_TEXT, 1);
    write_string(buf, &a.path);

    write_field_header(buf, "size", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&a.size.to_ne_bytes());

    write_field_header(buf, "modification_time", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&a.modification_time.to_ne_bytes());

    write_field_header(buf, "data_change", FIELD_TYPE_BOOLEAN, 1);
    buf.push(if a.data_change { 1 } else { 0 });

    write_field_header(buf, "num_records", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&a.num_records.unwrap_or(-1).to_ne_bytes());

    write_field_header(buf, "added_at_version", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&entry.added_at_version.to_ne_bytes());

    write_field_header(buf, "added_at_timestamp", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&entry.added_at_timestamp.to_ne_bytes());

    // Optional fields
    if !a.partition_values.is_empty() {
        write_field_header(buf, "partition_values", FIELD_TYPE_JSON, 1);
        let json = serde_json::to_string(&a.partition_values).unwrap_or_else(|_| "{}".to_string());
        write_string(buf, &json);
    }
    if let Some(ref s) = a.stats {
        write_field_header(buf, "stats", FIELD_TYPE_TEXT, 1);
        write_string(buf, s);
    }
    if let Some(ref m) = a.min_values {
        write_field_header(buf, "min_values", FIELD_TYPE_JSON, 1);
        let json = serde_json::to_string(m).unwrap_or_else(|_| "{}".to_string());
        write_string(buf, &json);
    }
    if let Some(ref m) = a.max_values {
        write_field_header(buf, "max_values", FIELD_TYPE_JSON, 1);
        let json = serde_json::to_string(m).unwrap_or_else(|_| "{}".to_string());
        write_string(buf, &json);
    }
    if let Some(v) = a.footer_start_offset {
        write_field_header(buf, "footer_start_offset", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&v.to_ne_bytes());
    }
    if let Some(v) = a.footer_end_offset {
        write_field_header(buf, "footer_end_offset", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&v.to_ne_bytes());
    }
    if let Some(v) = a.has_footer_offsets {
        write_field_header(buf, "has_footer_offsets", FIELD_TYPE_BOOLEAN, 1);
        buf.push(if v { 1 } else { 0 });
    }
    if let Some(v) = a.delete_opstamp {
        write_field_header(buf, "delete_opstamp", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&v.to_ne_bytes());
    }
    if let Some(ref tags) = a.split_tags {
        write_field_header(buf, "split_tags", FIELD_TYPE_JSON, 1);
        let json = serde_json::to_string(tags).unwrap_or_else(|_| "[]".to_string());
        write_string(buf, &json);
    }
    if let Some(v) = a.num_merge_ops {
        write_field_header(buf, "num_merge_ops", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&(v as i64).to_ne_bytes());
    }
    if let Some(ref s) = a.doc_mapping_json {
        write_field_header(buf, "doc_mapping_json", FIELD_TYPE_TEXT, 1);
        write_string(buf, s);
    }
    if let Some(ref s) = a.doc_mapping_ref {
        write_field_header(buf, "doc_mapping_ref", FIELD_TYPE_TEXT, 1);
        write_string(buf, s);
    }
    if let Some(v) = a.uncompressed_size_bytes {
        write_field_header(buf, "uncompressed_size_bytes", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&v.to_ne_bytes());
    }
    if let Some(ref v) = a.time_range_start {
        write_field_header(buf, "time_range_start", FIELD_TYPE_TEXT, 1);
        write_string(buf, v);
    }
    if let Some(ref v) = a.time_range_end {
        write_field_header(buf, "time_range_end", FIELD_TYPE_TEXT, 1);
        write_string(buf, v);
    }
    if let Some(ref files) = a.companion_source_files {
        write_field_header(buf, "companion_source_files", FIELD_TYPE_JSON, 1);
        let json = serde_json::to_string(files).unwrap_or_else(|_| "[]".to_string());
        write_string(buf, &json);
    }
    if let Some(v) = a.companion_delta_version {
        write_field_header(buf, "companion_delta_version", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&v.to_ne_bytes());
    }
    if let Some(ref s) = a.companion_fast_field_mode {
        write_field_header(buf, "companion_fast_field_mode", FIELD_TYPE_TEXT, 1);
        write_string(buf, s);
    }
}

// ============================================================================
// TxLogSnapshotInfo serialization
// ============================================================================

pub fn serialize_snapshot_info(info: &TxLogSnapshotInfo) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4096);
    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    let mut offsets = Vec::with_capacity(1);
    offsets.push(buf.len() as u32);

    let field_count: u16 = 7;
    buf.extend_from_slice(&field_count.to_ne_bytes());

    write_field_header(&mut buf, "checkpoint_version", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&info.checkpoint_version.to_ne_bytes());

    write_field_header(&mut buf, "state_dir", FIELD_TYPE_TEXT, 1);
    write_string(&mut buf, &info.state_dir);

    write_field_header(&mut buf, "manifest_paths_json", FIELD_TYPE_JSON, 1);
    let mp_json = serde_json::to_string(&info.manifest_paths.iter().map(|m| &m.path).collect::<Vec<_>>())
        .unwrap_or_else(|_| "[]".to_string());
    write_string(&mut buf, &mp_json);

    write_field_header(&mut buf, "post_checkpoint_paths_json", FIELD_TYPE_JSON, 1);
    let pcp_json = serde_json::to_string(&info.post_checkpoint_version_paths)
        .unwrap_or_else(|_| "[]".to_string());
    write_string(&mut buf, &pcp_json);

    write_field_header(&mut buf, "protocol_json", FIELD_TYPE_JSON, 1);
    let proto_json = match &info.protocol {
        Some(p) => serde_json::to_string(p).unwrap_or_else(|_| "{}".to_string()),
        None => String::new(), // Empty string signals "no protocol found"
    };
    write_string(&mut buf, &proto_json);

    write_field_header(&mut buf, "metadata_json", FIELD_TYPE_JSON, 1);
    let meta_json = serde_json::to_string(&info.metadata).unwrap_or_else(|_| "{}".to_string());
    write_string(&mut buf, &meta_json);

    write_field_header(&mut buf, "num_manifests", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&(info.manifest_paths.len() as i64).to_ne_bytes());

    write_footer(&mut buf, &offsets);
    buf
}

// ============================================================================
// TxLogChanges serialization
// ============================================================================

pub fn serialize_changes(changes: &TxLogChanges) -> Vec<u8> {
    let doc_count = 1 + changes.added_files.len() + changes.removed_paths.len() + changes.skip_actions.len();
    let mut buf = Vec::with_capacity(4 + doc_count * 200 + 12);
    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    let mut offsets = Vec::with_capacity(doc_count);

    // Header document
    offsets.push(buf.len() as u32);
    let field_count: u16 = 4;
    buf.extend_from_slice(&field_count.to_ne_bytes());
    write_field_header(&mut buf, "num_added", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&(changes.added_files.len() as i64).to_ne_bytes());
    write_field_header(&mut buf, "num_removed", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&(changes.removed_paths.len() as i64).to_ne_bytes());
    write_field_header(&mut buf, "num_skips", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&(changes.skip_actions.len() as i64).to_ne_bytes());
    write_field_header(&mut buf, "max_version", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&changes.max_version.to_ne_bytes());

    // Added files
    for entry in &changes.added_files {
        offsets.push(buf.len() as u32);
        serialize_file_entry(&mut buf, entry);
    }

    // Removed paths
    for path in &changes.removed_paths {
        offsets.push(buf.len() as u32);
        let fc: u16 = 1;
        buf.extend_from_slice(&fc.to_ne_bytes());
        write_field_header(&mut buf, "path", FIELD_TYPE_TEXT, 1);
        write_string(&mut buf, path);
    }

    // Skip actions (expanded fields for GAP-6)
    for skip in &changes.skip_actions {
        offsets.push(buf.len() as u32);
        let mut fc: u16 = 4; // path, reason, skip_count, skip_timestamp
        if !skip.operation.is_empty() { fc += 1; }
        if skip.partition_values.is_some() { fc += 1; }
        if skip.size.is_some() { fc += 1; }
        if skip.retry_after.is_some() { fc += 1; }
        buf.extend_from_slice(&fc.to_ne_bytes());
        write_field_header(&mut buf, "path", FIELD_TYPE_TEXT, 1);
        write_string(&mut buf, &skip.path);
        write_field_header(&mut buf, "skip_timestamp", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&skip.skip_timestamp.to_ne_bytes());
        write_field_header(&mut buf, "reason", FIELD_TYPE_TEXT, 1);
        write_string(&mut buf, &skip.reason);
        write_field_header(&mut buf, "skip_count", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&(skip.skip_count as i64).to_ne_bytes());
        if !skip.operation.is_empty() {
            let op = &skip.operation;
            write_field_header(&mut buf, "operation", FIELD_TYPE_TEXT, 1);
            write_string(&mut buf, op);
        }
        if let Some(ref pv) = skip.partition_values {
            write_field_header(&mut buf, "partition_values", FIELD_TYPE_JSON, 1);
            let json = serde_json::to_string(pv).unwrap_or_else(|_| "{}".to_string());
            write_string(&mut buf, &json);
        }
        if let Some(v) = skip.size {
            write_field_header(&mut buf, "size", FIELD_TYPE_INTEGER, 1);
            buf.extend_from_slice(&v.to_ne_bytes());
        }
        if let Some(v) = skip.retry_after {
            write_field_header(&mut buf, "retry_after", FIELD_TYPE_INTEGER, 1);
            buf.extend_from_slice(&v.to_ne_bytes());
        }
    }

    write_footer(&mut buf, &offsets);
    buf
}

// ============================================================================
// WriteResult serialization
// ============================================================================

pub fn serialize_write_result(result: &WriteResult) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);
    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    let mut offsets = Vec::with_capacity(1);
    offsets.push(buf.len() as u32);

    let field_count: u16 = 3;
    buf.extend_from_slice(&field_count.to_ne_bytes());

    write_field_header(&mut buf, "version", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&result.version.to_ne_bytes());

    write_field_header(&mut buf, "retries", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&(result.retries as i64).to_ne_bytes());

    write_field_header(&mut buf, "conflicted_versions_json", FIELD_TYPE_JSON, 1);
    let cv_json = serde_json::to_string(&result.conflicted_versions).unwrap_or_else(|_| "[]".to_string());
    write_string(&mut buf, &cv_json);

    write_footer(&mut buf, &offsets);
    buf
}

// ============================================================================
// LastCheckpointInfo serialization
// ============================================================================

pub fn serialize_last_checkpoint(info: &LastCheckpointInfo) -> Vec<u8> {
    let mut buf = Vec::with_capacity(256);
    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    let mut offsets = Vec::with_capacity(1);
    offsets.push(buf.len() as u32);

    let field_count: u16 = 4;
    buf.extend_from_slice(&field_count.to_ne_bytes());

    write_field_header(&mut buf, "version", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&info.version.to_ne_bytes());

    write_field_header(&mut buf, "size", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&info.size.to_ne_bytes());

    write_field_header(&mut buf, "format", FIELD_TYPE_TEXT, 1);
    write_string(&mut buf, info.format.as_deref().unwrap_or("avro-state"));

    write_field_header(&mut buf, "num_files", FIELD_TYPE_INTEGER, 1);
    buf.extend_from_slice(&info.num_files.to_ne_bytes());

    write_footer(&mut buf, &offsets);
    buf
}

// ============================================================================
// SkipAction serialization (for listSkipActions)
// ============================================================================

pub fn serialize_skip_actions(skips: &[super::actions::SkipAction]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + skips.len() * 200 + 12);
    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());

    let mut offsets = Vec::with_capacity(skips.len());
    for skip in skips {
        offsets.push(buf.len() as u32);
        let mut fc: u16 = 4; // path, skip_timestamp, reason, skip_count
        if !skip.operation.is_empty() { fc += 1; }
        if skip.partition_values.is_some() { fc += 1; }
        if skip.size.is_some() { fc += 1; }
        if skip.retry_after.is_some() { fc += 1; }
        buf.extend_from_slice(&fc.to_ne_bytes());
        write_field_header(&mut buf, "path", FIELD_TYPE_TEXT, 1);
        write_string(&mut buf, &skip.path);
        write_field_header(&mut buf, "skip_timestamp", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&skip.skip_timestamp.to_ne_bytes());
        write_field_header(&mut buf, "reason", FIELD_TYPE_TEXT, 1);
        write_string(&mut buf, &skip.reason);
        write_field_header(&mut buf, "skip_count", FIELD_TYPE_INTEGER, 1);
        buf.extend_from_slice(&(skip.skip_count as i64).to_ne_bytes());
        if !skip.operation.is_empty() {
            let op = &skip.operation;
            write_field_header(&mut buf, "operation", FIELD_TYPE_TEXT, 1);
            write_string(&mut buf, op);
        }
        if let Some(ref pv) = skip.partition_values {
            write_field_header(&mut buf, "partition_values", FIELD_TYPE_JSON, 1);
            let json = serde_json::to_string(pv).unwrap_or_else(|_| "{}".to_string());
            write_string(&mut buf, &json);
        }
        if let Some(v) = skip.size {
            write_field_header(&mut buf, "size", FIELD_TYPE_INTEGER, 1);
            buf.extend_from_slice(&v.to_ne_bytes());
        }
        if let Some(v) = skip.retry_after {
            write_field_header(&mut buf, "retry_after", FIELD_TYPE_INTEGER, 1);
            buf.extend_from_slice(&v.to_ne_bytes());
        }
    }

    write_footer(&mut buf, &offsets);
    buf
}

// ============================================================================
// Helpers (same as delta_reader/serialization.rs)
// ============================================================================

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

fn write_footer(buf: &mut Vec<u8>, offsets: &[u32]) {
    let offset_table_start = buf.len() as u32;
    for offset in offsets {
        buf.extend_from_slice(&offset.to_ne_bytes());
    }
    buf.extend_from_slice(&offset_table_start.to_ne_bytes());
    buf.extend_from_slice(&(offsets.len() as u32).to_ne_bytes());
    buf.extend_from_slice(&MAGIC_NUMBER.to_ne_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_empty_entries() {
        let buf = serialize_file_entries(&[]);
        assert_eq!(&buf[..4], &MAGIC_NUMBER.to_ne_bytes());
        let len = buf.len();
        assert_eq!(&buf[len-4..], &MAGIC_NUMBER.to_ne_bytes());
        let doc_count = u32::from_ne_bytes([buf[len-8], buf[len-7], buf[len-6], buf[len-5]]);
        assert_eq!(doc_count, 0);
    }

    #[test]
    fn test_serialize_write_result() {
        let result = WriteResult {
            version: 42,
            retries: 2,
            conflicted_versions: vec![40, 41],
        };
        let buf = serialize_write_result(&result);
        assert_eq!(&buf[..4], &MAGIC_NUMBER.to_ne_bytes());
        let len = buf.len();
        let doc_count = u32::from_ne_bytes([buf[len-8], buf[len-7], buf[len-6], buf[len-5]]);
        assert_eq!(doc_count, 1);
    }
}
