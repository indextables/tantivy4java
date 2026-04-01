// txlog/actions.rs - Transaction log action types
//
// All five action types from the Scala TransactionLog, matching the
// exact JSON serialization format for backward compatibility.

use std::collections::HashMap;
use serde::{Deserialize, Deserializer, Serialize};

/// Deserialize a field that may be null in JSON into a Default value.
/// Handles the case where Scala writes `"field": null` instead of omitting it.
fn deserialize_null_as_default<'de, D, T>(deserializer: D) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    T: Default + Deserialize<'de>,
{
    let opt = Option::deserialize(deserializer)?;
    Ok(opt.unwrap_or_default())
}

// ============================================================================
// Action envelope — wraps each action type for JSON line serialization
// ============================================================================

/// One JSON line in a version file. Exactly one field is populated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionEnvelope {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol: Option<ProtocolAction>,
    #[serde(rename = "metaData", skip_serializing_if = "Option::is_none")]
    pub metadata: Option<MetadataAction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub add: Option<AddAction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remove: Option<RemoveAction>,
    #[serde(rename = "mergeskip", skip_serializing_if = "Option::is_none")]
    pub merge_skip: Option<SkipAction>,
}

impl ActionEnvelope {
    pub fn into_action(self) -> Option<Action> {
        if let Some(p) = self.protocol {
            Some(Action::Protocol(p))
        } else if let Some(m) = self.metadata {
            Some(Action::MetaData(m))
        } else if let Some(a) = self.add {
            Some(Action::Add(a))
        } else if let Some(r) = self.remove {
            Some(Action::Remove(r))
        } else if let Some(s) = self.merge_skip {
            Some(Action::MergeSkip(s))
        } else {
            None
        }
    }

    pub fn from_action(action: &Action) -> Self {
        let mut envelope = ActionEnvelope {
            protocol: None,
            metadata: None,
            add: None,
            remove: None,
            merge_skip: None,
        };
        match action {
            Action::Protocol(p) => envelope.protocol = Some(p.clone()),
            Action::MetaData(m) => envelope.metadata = Some(m.clone()),
            Action::Add(a) => envelope.add = Some(a.clone()),
            Action::Remove(r) => envelope.remove = Some(r.clone()),
            Action::MergeSkip(s) => envelope.merge_skip = Some(s.clone()),
        }
        envelope
    }
}

// ============================================================================
// Top-level Action enum
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Action {
    Protocol(ProtocolAction),
    MetaData(MetadataAction),
    Add(AddAction),
    Remove(RemoveAction),
    MergeSkip(SkipAction),
}

// ============================================================================
// Protocol action (v4 = Avro state format)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProtocolAction {
    pub min_reader_version: u32,
    pub min_writer_version: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty", deserialize_with = "deserialize_null_as_default")]
    pub reader_features: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty", deserialize_with = "deserialize_null_as_default")]
    pub writer_features: Vec<String>,
}

impl ProtocolAction {
    /// Create a v4 protocol action (Avro state format).
    pub fn v4() -> Self {
        Self {
            min_reader_version: 4,
            min_writer_version: 4,
            reader_features: vec![],
            writer_features: vec![],
        }
    }
}

// ============================================================================
// Metadata action
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FormatSpec {
    #[serde(default = "default_format_provider")]
    pub provider: String,
    #[serde(default)]
    pub options: HashMap<String, String>,
}

fn default_format_provider() -> String {
    "parquet".to_string()
}

impl Default for FormatSpec {
    fn default() -> Self {
        Self {
            provider: default_format_provider(),
            options: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetadataAction {
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub schema_string: String,
    #[serde(default, deserialize_with = "deserialize_null_as_default")]
    pub partition_columns: Vec<String>,
    #[serde(default, deserialize_with = "deserialize_null_as_default")]
    pub format: FormatSpec,
    #[serde(default, deserialize_with = "deserialize_null_as_default")]
    pub configuration: HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_time: Option<i64>,
}

impl MetadataAction {
    /// Create an empty MetadataAction (used when no metadata is found in version files).
    pub fn empty() -> Self {
        MetadataAction {
            id: String::new(),
            name: None,
            description: None,
            schema_string: String::new(),
            partition_columns: vec![],
            format: FormatSpec::default(),
            configuration: HashMap::new(),
            created_time: None,
        }
    }
}

// ============================================================================
// Add action — file addition with extensive metadata
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddAction {
    pub path: String,
    #[serde(default)]
    pub partition_values: HashMap<String, String>,
    pub size: i64,
    pub modification_time: i64,
    #[serde(default = "default_true")]
    pub data_change: bool,

    // Statistics
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_values: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_values: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_records: Option<i64>,

    // Footer optimization
    #[serde(skip_serializing_if = "Option::is_none")]
    pub footer_start_offset: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub footer_end_offset: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub has_footer_offsets: Option<bool>,

    // Delete tracking
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete_opstamp: Option<i64>,

    // Split metadata
    #[serde(skip_serializing_if = "Option::is_none")]
    pub split_tags: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_merge_ops: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc_mapping_json: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc_mapping_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uncompressed_size_bytes: Option<i64>,

    // Time range (String for Scala protocol compat)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_range_start: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_range_end: Option<String>,

    // Companion mode
    #[serde(skip_serializing_if = "Option::is_none")]
    pub companion_source_files: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub companion_delta_version: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub companion_fast_field_mode: Option<String>,
}

fn default_true() -> bool {
    true
}

fn default_one() -> i32 {
    1
}

// ============================================================================
// Remove action
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoveAction {
    pub path: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_timestamp: Option<i64>,
    #[serde(default = "default_true")]
    pub data_change: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_values: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
}

// ============================================================================
// Skip action
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SkipAction {
    pub path: String,
    pub skip_timestamp: i64,
    pub reason: String,
    #[serde(default)]
    pub operation: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_values: Option<HashMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after: Option<i64>,
    #[serde(default = "default_one")]
    pub skip_count: i32,
}

// ============================================================================
// FileEntry — Avro state superset of AddAction with streaming metadata
// ============================================================================

#[derive(Debug, Clone)]
pub struct FileEntry {
    pub add: AddAction,
    pub added_at_version: i64,
    pub added_at_timestamp: i64,
}

// ============================================================================
// State manifest types (Avro state checkpoint metadata)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PartitionBounds {
    pub min_values: HashMap<String, String>,
    pub max_values: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManifestInfo {
    pub path: String,
    #[serde(alias = "numEntries")]
    pub file_count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_bounds: Option<PartitionBounds>,
    // Scala compat fields
    #[serde(default)]
    pub min_added_at_version: i64,
    #[serde(default)]
    pub max_added_at_version: i64,
    #[serde(default)]
    pub tombstone_count: i64,
    #[serde(default = "default_neg_one")]
    pub live_entry_count: i64,
}

fn default_neg_one() -> i64 {
    -1
}

impl Default for ManifestInfo {
    fn default() -> Self {
        Self {
            path: String::new(),
            file_count: 0,
            partition_bounds: None,
            min_added_at_version: 0,
            max_added_at_version: 0,
            tombstone_count: 0,
            live_entry_count: -1,
        }
    }
}

/// State manifest filename constant — matches Scala's StateManifestIO.MANIFEST_FILE_NAME.
pub const STATE_MANIFEST_FILENAME: &str = "_manifest.avro";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StateManifest {
    #[serde(alias = "stateVersion")]
    pub version: i64,
    pub manifests: Vec<ManifestInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition_bounds: Option<PartitionBounds>,
    #[serde(alias = "createdAt")]
    pub created_time: i64,
    #[serde(alias = "numFiles")]
    pub total_file_count: i64,
    #[serde(default)]
    pub format: String,
    /// Cached protocol action JSON (so checkpoint is self-contained after TRUNCATE)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub protocol_json: Option<String>,
    /// Cached metadata action JSON — matches Scala StateManifest.metadata field
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<String>,
    /// Schema registry for doc mapping deduplication — matches Scala StateManifest.schemaRegistry
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub schema_registry: HashMap<String, String>,
    // --- Scala compat fields ---
    /// Paths of removed files (applied during read to filter out dead entries)
    #[serde(default)]
    pub tombstones: Vec<String>,
    /// State format version
    #[serde(default)]
    pub format_version: i32,
    /// Total size of all live files in bytes
    #[serde(default)]
    pub total_bytes: i64,
    /// Protocol version (4 = Avro state format)
    #[serde(default)]
    pub protocol_version: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LastCheckpointInfo {
    pub version: i64,
    pub size: i64,
    pub size_in_bytes: i64,
    pub num_files: i64,
    pub created_time: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state_dir: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parts: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checkpoint_id: Option<String>,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_action_envelope_roundtrip_add() {
        let add = AddAction {
            path: "split-001.split".to_string(),
            partition_values: HashMap::new(),
            size: 12345,
            modification_time: 1700000000000,
            data_change: true,
            stats: None,
            min_values: None,
            max_values: None,
            num_records: Some(100),
            footer_start_offset: None,
            footer_end_offset: None,
            has_footer_offsets: None,
            delete_opstamp: None,
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
        };
        let envelope = ActionEnvelope::from_action(&Action::Add(add));
        let json = serde_json::to_string(&envelope).unwrap();
        assert!(json.contains("\"add\""));
        assert!(json.contains("split-001.split"));

        let parsed: ActionEnvelope = serde_json::from_str(&json).unwrap();
        let action = parsed.into_action().unwrap();
        match action {
            Action::Add(a) => assert_eq!(a.path, "split-001.split"),
            other => panic!("expected Add, got {:?}", other),
        }
    }

    #[test]
    fn test_action_envelope_roundtrip_skip() {
        let skip = SkipAction {
            path: "bad-split.split".to_string(),
            skip_timestamp: 1700000000000,
            reason: "merge".to_string(),
            operation: "merge_v2".to_string(),
            partition_values: None,
            size: None,
            retry_after: Some(1700000060000),
            skip_count: 3,
        };
        let envelope = ActionEnvelope::from_action(&Action::MergeSkip(skip));
        let json = serde_json::to_string(&envelope).unwrap();
        assert!(json.contains("\"mergeskip\""));

        let parsed: ActionEnvelope = serde_json::from_str(&json).unwrap();
        let action = parsed.into_action().unwrap();
        match action {
            Action::MergeSkip(s) => {
                assert_eq!(s.reason, "merge");
                assert_eq!(s.skip_count, 3);
            }
            other => panic!("expected MergeSkip, got {:?}", other),
        }
    }

    #[test]
    fn test_protocol_v4() {
        let p = ProtocolAction::v4();
        assert_eq!(p.min_reader_version, 4);
        assert_eq!(p.min_writer_version, 4);
    }

    #[test]
    fn test_last_checkpoint_info_roundtrip() {
        let info = LastCheckpointInfo {
            version: 42,
            size: 1000,
            size_in_bytes: 50000,
            num_files: 1000,
            format: Some("avro-state".to_string()),
            state_dir: Some("state-v42".to_string()),
            created_time: 1700000000000,
            parts: None,
            checkpoint_id: None,
        };
        let json = serde_json::to_string(&info).unwrap();
        let parsed: LastCheckpointInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.version, 42);
        assert_eq!(parsed.format, Some("avro-state".to_string()));
        assert_eq!(parsed.state_dir, Some("state-v42".to_string()));
    }
}
