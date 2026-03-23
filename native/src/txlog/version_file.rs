// txlog/version_file.rs - Read/write individual version JSON files
//
// Version files contain one JSON action per line ({"add":{...}}, {"remove":{...}}, etc).
// Files may be gzip-compressed (detected by magic bytes).

use super::actions::{Action, ActionEnvelope};
use super::compression;
use super::error::Result;
use super::storage::TxLogStorage;
use bytes::Bytes;

/// Parse a version file's bytes into a list of actions.
/// Handles both plain and gzip-compressed content.
pub fn parse_version_file(data: &[u8]) -> Result<Vec<Action>> {
    let text_bytes = compression::maybe_decompress(data)?;
    let text = String::from_utf8(text_bytes)
        .map_err(|e| super::error::TxLogError::Storage(anyhow::anyhow!("Invalid UTF-8: {}", e)))?;

    let mut actions = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let envelope: ActionEnvelope = serde_json::from_str(line)?;
        if let Some(action) = envelope.into_action() {
            actions.push(action);
        }
    }
    Ok(actions)
}

/// Serialize actions into gzip-compressed JSON lines (Scala-compatible format).
pub fn serialize_version_file(actions: &[Action]) -> Result<Vec<u8>> {
    let mut lines = String::new();
    for action in actions {
        let envelope = ActionEnvelope::from_action(action);
        let json = serde_json::to_string(&envelope)?;
        lines.push_str(&json);
        lines.push('\n');
    }
    let compressed = compression::gzip_compress(lines.as_bytes())?;
    Ok(compressed)
}

/// Read and parse a version file from storage.
pub async fn read_version(storage: &TxLogStorage, version: i64) -> Result<Vec<Action>> {
    let path = TxLogStorage::version_path(version);
    let data = storage.get(&path).await?;
    parse_version_file(&data)
}

/// Write a version file to storage with conditional semantics.
/// Returns true if written successfully, false if version already exists.
pub async fn write_version(
    storage: &TxLogStorage,
    version: i64,
    actions: &[Action],
) -> Result<bool> {
    let data = serialize_version_file(actions)?;
    let path = TxLogStorage::version_path(version);
    storage.put_if_absent(&path, Bytes::from(data)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::txlog::actions::*;
    use std::collections::HashMap;

    #[test]
    fn test_parse_plain_json() {
        let data = br#"{"add":{"path":"f.split","partitionValues":{},"size":100,"modificationTime":0,"dataChange":true}}
{"remove":{"path":"old.split","dataChange":true}}"#;
        let actions = parse_version_file(data).unwrap();
        assert_eq!(actions.len(), 2);
        match &actions[0] {
            Action::Add(a) => assert_eq!(a.path, "f.split"),
            _ => panic!("expected Add"),
        }
        match &actions[1] {
            Action::Remove(r) => assert_eq!(r.path, "old.split"),
            _ => panic!("expected Remove"),
        }
    }

    #[test]
    fn test_serialize_and_parse_roundtrip() {
        let actions = vec![
            Action::Protocol(ProtocolAction::v4()),
            Action::Add(AddAction {
                path: "test.split".to_string(),
                partition_values: HashMap::new(),
                size: 500,
                modification_time: 1700000000000,
                data_change: true,
                stats: None,
                min_values: None,
                max_values: None,
                num_records: Some(42),
                footer_start_offset: None,
                footer_end_offset: None,
                has_footer_offsets: None, delete_opstamp: None,
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
            }),
        ];
        let bytes = serialize_version_file(&actions).unwrap();
        // Should be gzip compressed
        assert!(compression::is_gzip(&bytes));

        let parsed = parse_version_file(&bytes).unwrap();
        assert_eq!(parsed.len(), 2);
        match &parsed[0] {
            Action::Protocol(p) => assert_eq!(p.min_reader_version, 4),
            _ => panic!("expected Protocol"),
        }
        match &parsed[1] {
            Action::Add(a) => {
                assert_eq!(a.path, "test.split");
                assert_eq!(a.num_records, Some(42));
            }
            _ => panic!("expected Add"),
        }
    }

    #[test]
    fn test_parse_skip_action() {
        let data = br#"{"mergeskip":{"path":"bad.split","skipTimestamp":1700000000000,"reason":"merge","skipCount":2}}"#;
        let actions = parse_version_file(data).unwrap();
        assert_eq!(actions.len(), 1);
        match &actions[0] {
            Action::MergeSkip(s) => {
                assert_eq!(s.path, "bad.split");
                assert_eq!(s.reason, "merge");
                assert_eq!(s.skip_count, Some(2));
            }
            _ => panic!("expected MergeSkip"),
        }
    }
}
