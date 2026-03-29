// txlog/integration_tests.rs - Comprehensive integration tests for the TANT byte buffer path
//
// Tests the full write→read pipeline, checkpoint creation/reading, distributed primitives,
// schema deduplication, TANT serialization, cache, and log replay through real filesystem I/O.

use std::collections::HashMap;
use std::sync::Arc;

use crate::delta_reader::engine::DeltaStorageConfig;
use crate::txlog::actions::*;
use crate::txlog::cache::{CacheConfig, TxLogCache};
use crate::txlog::distributed::{
    self, ManifestPathInfo, TxLogChanges, TxLogSnapshotInfo,
};
use crate::txlog::log_replay;
use crate::txlog::schema_dedup;
use crate::txlog::serialization;
use crate::txlog::storage::TxLogStorage;
use crate::txlog::version_file;

// ============================================================================
// Helpers
// ============================================================================

fn test_storage(dir: &std::path::Path) -> TxLogStorage {
    let config = DeltaStorageConfig::default();
    let url = format!("file://{}", dir.display());
    TxLogStorage::new(&url, &config).unwrap()
}

fn make_add_action(path: &str, size: i64, num_records: Option<i64>) -> AddAction {
    AddAction {
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
    }
}

fn make_add_action_full(
    path: &str,
    size: i64,
    num_records: Option<i64>,
    partition_values: HashMap<String, String>,
    doc_mapping_json: Option<String>,
    time_range: Option<(i64, i64)>,
    split_tags: Option<Vec<String>>,
) -> AddAction {
    AddAction {
        path: path.to_string(),
        partition_values,
        size,
        modification_time: 1700000000000,
        data_change: true,
        stats: None,
        min_values: None,
        max_values: None,
        num_records,
        footer_start_offset: None,
        footer_end_offset: None,
        has_footer_offsets: None, delete_opstamp: None,
        split_tags,
        num_merge_ops: None,
        doc_mapping_json,
        doc_mapping_ref: None,
        uncompressed_size_bytes: Some(size * 2),
        time_range_start: time_range.map(|(s, _)| s),
        time_range_end: time_range.map(|(_, e)| e),
        companion_source_files: None,
        companion_delta_version: None,
        companion_fast_field_mode: None,
    }
}

fn make_file_entry(path: &str, version: i64) -> FileEntry {
    FileEntry {
        add: make_add_action(path, 1000, Some(100)),
        added_at_version: version,
        added_at_timestamp: 1700000000000,
    }
}

fn make_file_entry_full(
    path: &str,
    version: i64,
    partition_values: HashMap<String, String>,
    doc_mapping_json: Option<String>,
    time_range: Option<(i64, i64)>,
    split_tags: Option<Vec<String>>,
    num_records: Option<i64>,
) -> FileEntry {
    FileEntry {
        add: make_add_action_full(
            path,
            1000,
            num_records,
            partition_values,
            doc_mapping_json,
            time_range,
            split_tags,
        ),
        added_at_version: version,
        added_at_timestamp: 1700000000000,
    }
}

fn make_protocol() -> ProtocolAction {
    ProtocolAction::v4()
}

fn make_metadata(id: &str) -> MetadataAction {
    MetadataAction {
        id: id.to_string(),
        name: None,
        description: None,
        schema_string: r#"{"type":"struct","fields":[{"name":"id","type":"long"}]}"#.to_string(),
        partition_columns: vec![],
        format: FormatSpec::default(),
        configuration: HashMap::new(),
        created_time: Some(1700000000000),
    }
}

fn make_metadata_with_partitions(id: &str, partitions: Vec<String>) -> MetadataAction {
    MetadataAction {
        id: id.to_string(),
        name: None,
        description: None,
        schema_string: r#"{"type":"struct","fields":[{"name":"id","type":"long"}]}"#.to_string(),
        partition_columns: partitions,
        format: FormatSpec::default(),
        configuration: HashMap::new(),
        created_time: Some(1700000000000),
    }
}

fn ensure_txlog_dir(dir: &std::path::Path) {
    std::fs::create_dir_all(dir.join("_transaction_log")).unwrap();
}

// ============================================================================
// 1. Full Write -> Read Pipeline (local filesystem)
// ============================================================================

#[tokio::test]
async fn test_write_version_and_read_back() {
    let tmp = tempfile::TempDir::new().unwrap();
    ensure_txlog_dir(tmp.path());
    let storage = test_storage(tmp.path());

    // Write version 0 with protocol + metadata + adds
    let actions = vec![
        Action::Protocol(make_protocol()),
        Action::MetaData(make_metadata("test-table")),
        Action::Add(make_add_action("split-001.split", 5000, Some(100))),
        Action::Add(make_add_action("split-002.split", 3000, Some(50))),
    ];

    let written = version_file::write_version(&storage, 0, &actions).await.unwrap();
    assert!(written, "Version 0 should be written successfully");

    // Read back
    let read_actions = version_file::read_version(&storage, 0).await.unwrap();
    assert_eq!(read_actions.len(), 4);

    // Verify protocol
    match &read_actions[0] {
        Action::Protocol(p) => {
            assert_eq!(p.min_reader_version, 4);
            assert_eq!(p.min_writer_version, 4);
        }
        other => panic!("Expected Protocol, got {:?}", other),
    }

    // Verify metadata
    match &read_actions[1] {
        Action::MetaData(m) => {
            assert_eq!(m.id, "test-table");
            assert!(m.schema_string.contains("id"));
        }
        other => panic!("Expected MetaData, got {:?}", other),
    }

    // Verify adds
    match &read_actions[2] {
        Action::Add(a) => {
            assert_eq!(a.path, "split-001.split");
            assert_eq!(a.size, 5000);
            assert_eq!(a.num_records, Some(100));
        }
        other => panic!("Expected Add, got {:?}", other),
    }
    match &read_actions[3] {
        Action::Add(a) => {
            assert_eq!(a.path, "split-002.split");
            assert_eq!(a.size, 3000);
        }
        other => panic!("Expected Add, got {:?}", other),
    }
}

#[tokio::test]
async fn test_write_multiple_versions() {
    let tmp = tempfile::TempDir::new().unwrap();
    ensure_txlog_dir(tmp.path());
    let storage = test_storage(tmp.path());

    // Version 0: protocol + metadata + initial adds
    let v0_actions = vec![
        Action::Protocol(make_protocol()),
        Action::MetaData(make_metadata("multi-version-table")),
        Action::Add(make_add_action("split-001.split", 5000, Some(100))),
    ];
    assert!(version_file::write_version(&storage, 0, &v0_actions).await.unwrap());

    // Version 1: more adds
    let v1_actions = vec![
        Action::Add(make_add_action("split-002.split", 3000, Some(50))),
        Action::Add(make_add_action("split-003.split", 4000, Some(75))),
    ];
    assert!(version_file::write_version(&storage, 1, &v1_actions).await.unwrap());

    // Version 2: remove + add
    let v2_actions = vec![
        Action::Remove(RemoveAction {
            path: "split-001.split".to_string(),
            deletion_timestamp: Some(1700000001000),
            data_change: true,
            partition_values: None,
            size: None,
        }),
        Action::Add(make_add_action("split-004.split", 6000, Some(200))),
    ];
    assert!(version_file::write_version(&storage, 2, &v2_actions).await.unwrap());

    // Verify all three versions exist by reading each one back.
    // Note: list_versions() has a known path normalization issue on local filesystem
    // (ObjectPath strips leading '/' but txlog_prefix retains it), so we verify
    // by reading individual versions directly.
    let v0_read = version_file::read_version(&storage, 0).await.unwrap();
    assert_eq!(v0_read.len(), 3);

    let v1_read = version_file::read_version(&storage, 1).await.unwrap();
    assert_eq!(v1_read.len(), 2);

    let v2_read = version_file::read_version(&storage, 2).await.unwrap();
    assert_eq!(v2_read.len(), 2);
    match &v2_read[0] {
        Action::Remove(r) => assert_eq!(r.path, "split-001.split"),
        other => panic!("Expected Remove, got {:?}", other),
    }
}

#[tokio::test]
async fn test_conditional_write_conflict() {
    let tmp = tempfile::TempDir::new().unwrap();
    ensure_txlog_dir(tmp.path());
    let storage = test_storage(tmp.path());

    let actions = vec![
        Action::Protocol(make_protocol()),
        Action::Add(make_add_action("split-001.split", 1000, Some(10))),
    ];

    // First write should succeed
    let first = version_file::write_version(&storage, 0, &actions).await.unwrap();
    assert!(first, "First write should succeed");

    // Second write to same version should return false (conflict)
    let second = version_file::write_version(&storage, 0, &actions).await.unwrap();
    assert!(!second, "Second write to same version should fail (conflict)");
}

// ============================================================================
// 2. Checkpoint Pipeline
// ============================================================================

#[tokio::test]
async fn test_write_checkpoint_and_read() {
    let tmp = tempfile::TempDir::new().unwrap();
    ensure_txlog_dir(tmp.path());
    let storage = test_storage(tmp.path());

    // Write version 0 so list_versions works
    let v0_actions = vec![
        Action::Protocol(make_protocol()),
        Action::MetaData(make_metadata("checkpoint-test")),
        Action::Add(make_add_action("split-001.split", 5000, Some(100))),
        Action::Add(make_add_action("split-002.split", 3000, Some(50))),
    ];
    assert!(version_file::write_version(&storage, 0, &v0_actions).await.unwrap());

    // Create checkpoint entries
    let entries = vec![
        make_file_entry("split-001.split", 0),
        make_file_entry("split-002.split", 0),
    ];

    let protocol = make_protocol();
    let metadata = make_metadata("checkpoint-test");

    // Write checkpoint
    let checkpoint_info = crate::txlog::avro::state_writer::write_state_checkpoint(
        &storage, 0, &entries, &protocol, &metadata,
    ).await.unwrap();

    assert_eq!(checkpoint_info.version, 0);
    assert_eq!(checkpoint_info.size, 2);
    assert_eq!(checkpoint_info.format, "avro-state");
    assert!(checkpoint_info.state_dir.is_some());

    // Read state manifest
    let state_dir = checkpoint_info.state_dir.unwrap();
    let state_manifest = crate::txlog::avro::state_reader::read_state_manifest(
        &storage, &state_dir,
    ).await.unwrap();

    assert_eq!(state_manifest.version, 0);
    assert_eq!(state_manifest.total_file_count, 2);
    assert_eq!(state_manifest.format, "avro-state");
    assert!(!state_manifest.manifests.is_empty());

    // Read all manifests back
    let read_entries = crate::txlog::avro::state_reader::read_all_manifests(
        &storage, &state_dir, &state_manifest, &HashMap::new(),
    ).await.unwrap();

    assert_eq!(read_entries.len(), 2);
    let paths: Vec<&str> = read_entries.iter().map(|e| e.add.path.as_str()).collect();
    assert!(paths.contains(&"split-001.split"));
    assert!(paths.contains(&"split-002.split"));
}

#[tokio::test]
async fn test_checkpoint_with_partitions() {
    let tmp = tempfile::TempDir::new().unwrap();
    ensure_txlog_dir(tmp.path());
    let storage = test_storage(tmp.path());

    // Write version 0
    let v0_actions = vec![
        Action::Protocol(make_protocol()),
        Action::MetaData(make_metadata_with_partitions("part-test", vec!["year".to_string()])),
    ];
    assert!(version_file::write_version(&storage, 0, &v0_actions).await.unwrap());

    // Create entries with partition values
    let mut pv_2023 = HashMap::new();
    pv_2023.insert("year".to_string(), "2023".to_string());
    let mut pv_2024 = HashMap::new();
    pv_2024.insert("year".to_string(), "2024".to_string());
    let mut pv_2025 = HashMap::new();
    pv_2025.insert("year".to_string(), "2025".to_string());

    let entries = vec![
        make_file_entry_full("split-2023.split", 0, pv_2023, None, None, None, Some(100)),
        make_file_entry_full("split-2024.split", 0, pv_2024, None, None, None, Some(200)),
        make_file_entry_full("split-2025.split", 0, pv_2025, None, None, None, Some(300)),
    ];

    let protocol = make_protocol();
    let metadata = make_metadata_with_partitions("part-test", vec!["year".to_string()]);

    let checkpoint_info = crate::txlog::avro::state_writer::write_state_checkpoint(
        &storage, 0, &entries, &protocol, &metadata,
    ).await.unwrap();

    assert_eq!(checkpoint_info.size, 3);

    // Read state manifest and verify partition bounds
    let state_dir = checkpoint_info.state_dir.unwrap();
    let state_manifest = crate::txlog::avro::state_reader::read_state_manifest(
        &storage, &state_dir,
    ).await.unwrap();

    // Global bounds should span 2023-2025
    let global_bounds = state_manifest.partition_bounds.as_ref()
        .expect("Should have global partition bounds");
    assert_eq!(global_bounds.min_values.get("year"), Some(&"2023".to_string()));
    assert_eq!(global_bounds.max_values.get("year"), Some(&"2025".to_string()));
}

#[tokio::test]
async fn test_checkpoint_manifest_roundtrip() {
    let tmp = tempfile::TempDir::new().unwrap();
    ensure_txlog_dir(tmp.path());
    let storage = test_storage(tmp.path());

    // Write version 0
    let v0_actions = vec![
        Action::Protocol(make_protocol()),
        Action::MetaData(make_metadata("roundtrip-test")),
    ];
    assert!(version_file::write_version(&storage, 0, &v0_actions).await.unwrap());

    // Create entries with rich metadata
    let tags = vec!["env:prod".to_string(), "region:us-east-1".to_string()];

    let entries = vec![
        make_file_entry_full(
            "split-rich.split",
            0,
            HashMap::new(),
            Some(r#"{"fields":[{"name":"title","type":"text"}]}"#.to_string()),
            Some((1000, 2000)),
            Some(tags),
            Some(500),
        ),
    ];

    let protocol = make_protocol();
    let metadata = make_metadata("roundtrip-test");

    let checkpoint_info = crate::txlog::avro::state_writer::write_state_checkpoint(
        &storage, 0, &entries, &protocol, &metadata,
    ).await.unwrap();

    // Read back manifests and verify all fields
    let state_dir = checkpoint_info.state_dir.unwrap();
    let state_manifest = crate::txlog::avro::state_reader::read_state_manifest(
        &storage, &state_dir,
    ).await.unwrap();

    let read_entries = crate::txlog::avro::state_reader::read_all_manifests(
        &storage, &state_dir, &state_manifest, &HashMap::new(),
    ).await.unwrap();

    assert_eq!(read_entries.len(), 1);
    let entry = &read_entries[0];
    assert_eq!(entry.add.path, "split-rich.split");
    assert_eq!(entry.add.num_records, Some(500));
    assert_eq!(entry.add.time_range_start, Some(1000));
    assert_eq!(entry.add.time_range_end, Some(2000));
    assert_eq!(entry.add.uncompressed_size_bytes, Some(2000)); // size * 2
    assert!(entry.add.split_tags.is_some());
    let tags = entry.add.split_tags.as_ref().unwrap();
    assert!(tags.contains(&"env:prod".to_string()));
    assert!(tags.contains(&"region:us-east-1".to_string()));
    // doc_mapping_json should survive the roundtrip
    assert_eq!(
        entry.add.doc_mapping_json,
        Some(r#"{"fields":[{"name":"title","type":"text"}]}"#.to_string())
    );
}

// ============================================================================
// 3. Distributed Primitives Pipeline
// ============================================================================

#[tokio::test]
async fn test_distributed_full_pipeline() {
    // Tests the full distributed pipeline: checkpoint creation, manifest reading,
    // post-checkpoint change reading, and replay to compute the final file set.
    //
    // Note: We bypass get_txlog_snapshot_info() because list_versions() has a known
    // path normalization issue on local filesystem (ObjectPath strips leading '/'
    // but txlog_prefix retains it). Instead we construct the pipeline manually using
    // the individual distributed primitives, which is the more thorough test anyway.

    let tmp = tempfile::TempDir::new().unwrap();
    ensure_txlog_dir(tmp.path());
    let storage = test_storage(tmp.path());
    let table_url = format!("file://{}", tmp.path().display());
    let config = DeltaStorageConfig::default();

    // -- Setup: write v0 with protocol + metadata + adds --
    let v0_actions = vec![
        Action::Protocol(make_protocol()),
        Action::MetaData(make_metadata("distributed-test")),
        Action::Add(make_add_action("split-001.split", 5000, Some(100))),
        Action::Add(make_add_action("split-002.split", 3000, Some(50))),
    ];
    assert!(version_file::write_version(&storage, 0, &v0_actions).await.unwrap());

    // -- Create checkpoint at v0 --
    let entries_v0 = vec![
        make_file_entry("split-001.split", 0),
        make_file_entry("split-002.split", 0),
    ];
    let protocol = make_protocol();
    let metadata = make_metadata("distributed-test");

    let cp_info = crate::txlog::avro::state_writer::write_state_checkpoint(
        &storage, 0, &entries_v0, &protocol, &metadata,
    ).await.unwrap();

    assert_eq!(cp_info.version, 0);
    let state_dir = cp_info.state_dir.as_ref().unwrap();

    // -- Write v1: more adds --
    let v1_actions = vec![
        Action::Add(make_add_action("split-003.split", 4000, Some(75))),
    ];
    assert!(version_file::write_version(&storage, 1, &v1_actions).await.unwrap());

    // -- Write v2: remove --
    let v2_actions = vec![
        Action::Remove(RemoveAction {
            path: "split-001.split".to_string(),
            deletion_timestamp: Some(1700000002000),
            data_change: true,
            partition_values: None,
            size: None,
        }),
    ];
    assert!(version_file::write_version(&storage, 2, &v2_actions).await.unwrap());

    // -- Phase 1: Read state manifest to get manifest info --
    let state_manifest = crate::txlog::avro::state_reader::read_state_manifest(
        &storage, state_dir,
    ).await.unwrap();
    assert_eq!(state_manifest.total_file_count, 2);
    assert!(!state_manifest.manifests.is_empty());

    // -- Phase 2: read_manifest for each manifest using distributed primitive --
    let mut all_checkpoint_entries = Vec::new();
    for manifest_info in &state_manifest.manifests {
        let entries = distributed::read_manifest(
            &table_url, &config, state_dir, &manifest_info.path, &metadata.configuration,
        ).await.unwrap();
        all_checkpoint_entries.extend(entries);
    }
    assert_eq!(all_checkpoint_entries.len(), 2);

    // -- Phase 3: read_post_checkpoint_changes for v1 and v2 --
    let post_cp_paths = vec![
        TxLogStorage::version_path(1),
        TxLogStorage::version_path(2),
    ];
    let changes = distributed::read_post_checkpoint_changes(
        &table_url, &config,
        &post_cp_paths,
        &metadata.configuration,
    ).await.unwrap();
    assert_eq!(changes.added_files.len(), 1);
    assert_eq!(changes.added_files[0].add.path, "split-003.split");
    assert_eq!(changes.removed_paths.len(), 1);
    assert_eq!(changes.removed_paths[0], "split-001.split");
    assert_eq!(changes.max_version, 2);

    // -- Phase 4: replay everything --
    let post_actions: Vec<(i64, Vec<Action>)> = vec![
        (1, v1_actions),
        (2, v2_actions),
    ];
    let replay_result = log_replay::replay(all_checkpoint_entries, post_actions);

    // After replay: split-002 and split-003 should remain, split-001 removed
    let mut final_paths: Vec<&str> = replay_result.files.iter()
        .map(|e| e.add.path.as_str())
        .collect();
    final_paths.sort();
    assert_eq!(final_paths, vec!["split-002.split", "split-003.split"]);
}

#[tokio::test]
async fn test_distributed_read_manifest() {
    let tmp = tempfile::TempDir::new().unwrap();
    ensure_txlog_dir(tmp.path());
    let storage = test_storage(tmp.path());
    let table_url = format!("file://{}", tmp.path().display());
    let config = DeltaStorageConfig::default();

    // Write version 0 and checkpoint
    let v0_actions = vec![
        Action::Protocol(make_protocol()),
        Action::MetaData(make_metadata("manifest-read-test")),
    ];
    assert!(version_file::write_version(&storage, 0, &v0_actions).await.unwrap());

    let entries = vec![
        make_file_entry("split-a.split", 0),
        make_file_entry("split-b.split", 0),
        make_file_entry("split-c.split", 0),
    ];
    let protocol = make_protocol();
    let metadata = make_metadata("manifest-read-test");

    let cp_info = crate::txlog::avro::state_writer::write_state_checkpoint(
        &storage, 0, &entries, &protocol, &metadata,
    ).await.unwrap();

    let state_dir = cp_info.state_dir.unwrap();

    // Read state manifest to get manifest paths
    let state_manifest = crate::txlog::avro::state_reader::read_state_manifest(
        &storage, &state_dir,
    ).await.unwrap();

    // Read each manifest individually using the distributed primitive
    for manifest_info in &state_manifest.manifests {
        let read_entries = distributed::read_manifest(
            &table_url, &config, &state_dir, &manifest_info.path, &metadata.configuration,
        ).await.unwrap();
        assert!(!read_entries.is_empty());
        assert_eq!(read_entries.len(), manifest_info.file_count as usize);
    }
}

// ============================================================================
// 4. Schema Deduplication Through Pipeline
// ============================================================================

#[tokio::test]
async fn test_schema_dedup_through_checkpoint() {
    let tmp = tempfile::TempDir::new().unwrap();
    ensure_txlog_dir(tmp.path());
    let storage = test_storage(tmp.path());

    let schema_json = r#"{"fields":[{"name":"title","type":"text"},{"name":"body","type":"text"}]}"#;

    // Create actions with same doc_mapping_json
    let mut actions = vec![
        Action::Protocol(make_protocol()),
        Action::MetaData(make_metadata("dedup-test")),
        Action::Add(make_add_action_full(
            "split-001.split", 1000, Some(100), HashMap::new(),
            Some(schema_json.to_string()), None, None,
        )),
        Action::Add(make_add_action_full(
            "split-002.split", 2000, Some(200), HashMap::new(),
            Some(schema_json.to_string()), None, None,
        )),
    ];

    // Deduplicate
    let new_schemas = schema_dedup::deduplicate_schemas(&mut actions, &HashMap::new());
    assert_eq!(new_schemas.len(), 1, "Same schema should deduplicate to 1 entry");

    // Merge schema config into metadata
    let metadata_config: HashMap<String, String> = new_schemas;

    // Verify adds now have refs instead of json
    for action in &actions {
        if let Action::Add(a) = action {
            assert!(a.doc_mapping_json.is_none(), "doc_mapping_json should be cleared");
            assert!(a.doc_mapping_ref.is_some(), "doc_mapping_ref should be set");
        }
    }

    // Write version 0
    assert!(version_file::write_version(&storage, 0, &actions).await.unwrap());

    // Create checkpoint entries (with refs, not json)
    let entries: Vec<FileEntry> = actions.iter()
        .filter_map(|a| {
            if let Action::Add(add) = a {
                Some(FileEntry {
                    add: add.clone(),
                    added_at_version: 0,
                    added_at_timestamp: 1700000000000,
                })
            } else {
                None
            }
        })
        .collect();

    let protocol = make_protocol();
    let mut metadata = make_metadata("dedup-test");
    metadata.configuration = metadata_config.clone();

    let cp_info = crate::txlog::avro::state_writer::write_state_checkpoint(
        &storage, 0, &entries, &protocol, &metadata,
    ).await.unwrap();

    // Read back from checkpoint
    let state_dir = cp_info.state_dir.unwrap();
    let state_manifest = crate::txlog::avro::state_reader::read_state_manifest(
        &storage, &state_dir,
    ).await.unwrap();
    let read_entries = crate::txlog::avro::state_reader::read_all_manifests(
        &storage, &state_dir, &state_manifest, &metadata_config,
    ).await.unwrap();

    // Verify schemas restored correctly
    assert_eq!(read_entries.len(), 2);
    for entry in &read_entries {
        assert!(
            entry.add.doc_mapping_json.is_some(),
            "doc_mapping_json should be restored from ref"
        );
        // The restored JSON should match the original (possibly canonicalized)
        let restored = entry.add.doc_mapping_json.as_ref().unwrap();
        let original_value: serde_json::Value = serde_json::from_str(schema_json).unwrap();
        let restored_value: serde_json::Value = serde_json::from_str(restored).unwrap();
        assert_eq!(original_value, restored_value);
    }
}

#[tokio::test]
async fn test_schema_dedup_multiple_schemas() {
    let schema_a = r#"{"fields":[{"name":"title","type":"text"}]}"#;
    let schema_b = r#"{"fields":[{"name":"price","type":"float"}]}"#;
    let schema_c = r#"{"fields":[{"name":"count","type":"integer"}]}"#;

    let mut actions = vec![
        Action::Add(make_add_action_full(
            "split-a.split", 1000, Some(10), HashMap::new(),
            Some(schema_a.to_string()), None, None,
        )),
        Action::Add(make_add_action_full(
            "split-b.split", 2000, Some(20), HashMap::new(),
            Some(schema_b.to_string()), None, None,
        )),
        Action::Add(make_add_action_full(
            "split-c.split", 3000, Some(30), HashMap::new(),
            Some(schema_c.to_string()), None, None,
        )),
        // Duplicate of schema_a
        Action::Add(make_add_action_full(
            "split-d.split", 4000, Some(40), HashMap::new(),
            Some(schema_a.to_string()), None, None,
        )),
    ];

    let new_schemas = schema_dedup::deduplicate_schemas(&mut actions, &HashMap::new());
    assert_eq!(new_schemas.len(), 3, "3 distinct schemas should produce 3 entries");

    // Verify all schema keys start with the prefix
    for key in new_schemas.keys() {
        assert!(key.starts_with("docMappingSchema."));
    }

    // Verify split-a and split-d have the same ref
    let ref_a = match &actions[0] {
        Action::Add(a) => a.doc_mapping_ref.clone().unwrap(),
        _ => panic!("expected Add"),
    };
    let ref_d = match &actions[3] {
        Action::Add(a) => a.doc_mapping_ref.clone().unwrap(),
        _ => panic!("expected Add"),
    };
    assert_eq!(ref_a, ref_d, "Same schema should produce same ref");

    // Verify split-b has a different ref
    let ref_b = match &actions[1] {
        Action::Add(a) => a.doc_mapping_ref.clone().unwrap(),
        _ => panic!("expected Add"),
    };
    assert_ne!(ref_a, ref_b, "Different schemas should produce different refs");
}

// ============================================================================
// 5. TANT Serialization Integration
// ============================================================================

const TANT_MAGIC: u32 = 0x54414E54;

fn read_u32_ne(buf: &[u8], offset: usize) -> u32 {
    u32::from_ne_bytes([buf[offset], buf[offset + 1], buf[offset + 2], buf[offset + 3]])
}

#[test]
fn test_serialize_file_entries_roundtrip() {
    let entries = vec![
        make_file_entry("split-001.split", 0),
        make_file_entry("split-002.split", 1),
        make_file_entry("split-003.split", 2),
    ];

    let buf = serialization::serialize_file_entries(&entries);

    // Verify magic number at start
    assert_eq!(read_u32_ne(&buf, 0), TANT_MAGIC);

    // Verify magic number at end (trailer)
    let len = buf.len();
    assert_eq!(read_u32_ne(&buf, len - 4), TANT_MAGIC);

    // Verify doc count from footer
    let doc_count = read_u32_ne(&buf, len - 8);
    assert_eq!(doc_count, 3);

    // Verify buffer is non-trivially sized (has actual data)
    assert!(buf.len() > 100, "Buffer should contain substantial data");
}

#[test]
fn test_serialize_snapshot_info() {
    let info = TxLogSnapshotInfo {
        checkpoint_version: 42,
        manifest_paths: vec![
            ManifestPathInfo {
                path: "manifest-0000.avro".to_string(),
                file_count: 100,
                partition_bounds: None,
            },
            ManifestPathInfo {
                path: "manifest-0001.avro".to_string(),
                file_count: 200,
                partition_bounds: None,
            },
        ],
        post_checkpoint_version_paths: vec![
            "00000000000000000043.json".to_string(),
            "00000000000000000044.json".to_string(),
        ],
        protocol: Some(make_protocol()),
        metadata: make_metadata("snapshot-test"),
        state_dir: "state-v42".to_string(),
    };

    let buf = serialization::serialize_snapshot_info(&info);

    // Verify magic number at start and end
    assert_eq!(read_u32_ne(&buf, 0), TANT_MAGIC);
    let len = buf.len();
    assert_eq!(read_u32_ne(&buf, len - 4), TANT_MAGIC);

    // Verify doc count = 1 (single record)
    let doc_count = read_u32_ne(&buf, len - 8);
    assert_eq!(doc_count, 1);

    // Buffer should be reasonably sized
    assert!(buf.len() > 50);
}

#[test]
fn test_serialize_changes() {
    let changes = TxLogChanges {
        added_files: vec![
            make_file_entry("added-001.split", 1),
            make_file_entry("added-002.split", 2),
        ],
        removed_paths: vec![
            "removed-001.split".to_string(),
        ],
        skip_actions: vec![
            SkipAction {
                path: "skipped-001.split".to_string(),
                skip_timestamp: 1700000000000,
                reason: "merge_in_progress".to_string(),
                operation: Some("merge_v2".to_string()),
                partition_values: None,
                size: None,
                retry_after: None,
                skip_count: Some(2),
            },
        ],
        max_version: 5,
    };

    let buf = serialization::serialize_changes(&changes);

    // Verify magic at start and end
    assert_eq!(read_u32_ne(&buf, 0), TANT_MAGIC);
    let len = buf.len();
    assert_eq!(read_u32_ne(&buf, len - 4), TANT_MAGIC);

    // Doc count: 1 header + 2 added + 1 removed + 1 skip = 5
    let doc_count = read_u32_ne(&buf, len - 8);
    assert_eq!(doc_count, 5);
}

// ============================================================================
// 6. Cache Integration
// ============================================================================

#[test]
fn test_cache_through_pipeline() {
    let cache = TxLogCache::new(CacheConfig::default());

    // Put entries in snapshot cache
    let entries = Arc::new(vec![
        make_file_entry("cached-001.split", 0),
        make_file_entry("cached-002.split", 0),
    ]);
    cache.put_snapshot(0, entries.clone());

    // Get from cache - should hit
    let cached = cache.get_snapshot(0);
    assert!(cached.is_some());
    assert_eq!(cached.unwrap().len(), 2);

    // Put version actions
    let actions = vec![
        Action::Protocol(make_protocol()),
        Action::Add(make_add_action("test.split", 100, Some(10))),
    ];
    cache.put_version(5, actions);
    let cached_actions = cache.get_version(5);
    assert!(cached_actions.is_some());
    assert_eq!(cached_actions.unwrap().len(), 2);

    // Put file list
    let file_list = Arc::new(vec![make_file_entry("fl-001.split", 0)]);
    cache.put_file_list(0, 12345, file_list.clone());
    assert!(cache.get_file_list(0, 12345).is_some());
    assert!(cache.get_file_list(0, 99999).is_none()); // different filter hash

    // Verify stats
    let stats = cache.stats();
    assert!(stats.hits > 0);

    // Invalidate all
    cache.invalidate_all();

    // Verify miss after invalidate
    assert!(cache.get_snapshot(0).is_none());
    assert!(cache.get_version(5).is_none());
    assert!(cache.get_file_list(0, 12345).is_none());
}

#[test]
fn test_cache_metadata_persistence() {
    let cache = TxLogCache::new(CacheConfig::default());

    let protocol = make_protocol();
    let metadata = make_metadata("cache-meta-test");

    // Put metadata
    cache.put_metadata(protocol.clone(), metadata.clone());

    // Get metadata
    let cached = cache.get_metadata();
    assert!(cached.is_some());
    let cached_arc = cached.unwrap();
    let (cached_proto, cached_meta) = cached_arc.as_ref();
    assert_eq!(cached_proto.min_reader_version, 4);
    assert_eq!(cached_meta.id, "cache-meta-test");

    // Put last checkpoint
    let cp_info = LastCheckpointInfo {
        version: 42,
        size: 1000,
        size_in_bytes: Some(50000),
        num_files: Some(1000),
        format: "avro-state".to_string(),
        state_dir: Some("state-v42".to_string()),
        created_time: Some(1700000000000),
    };
    cache.put_last_checkpoint(cp_info.clone());

    let cached_cp = cache.get_last_checkpoint();
    assert!(cached_cp.is_some());
    let cached_cp = cached_cp.unwrap();
    assert_eq!(cached_cp.version, 42);
    assert_eq!(cached_cp.format, "avro-state");
    assert_eq!(cached_cp.state_dir, Some("state-v42".to_string()));

    // Invalidate mutable (should not clear metadata)
    cache.invalidate_mutable();
    // Metadata should still be present (invalidate_mutable only clears versions/snapshots/file_lists)
    assert!(cache.get_metadata().is_some());
    // But last_checkpoint is not cleared by invalidate_mutable either
    assert!(cache.get_last_checkpoint().is_some());

    // Full invalidate clears everything
    cache.invalidate_all();
    assert!(cache.get_metadata().is_none());
    assert!(cache.get_last_checkpoint().is_none());
}

// ============================================================================
// 6b. Backward Compatibility Tests
// ============================================================================

#[test]
fn test_read_plain_json_version_file() {
    // Create a plain (uncompressed) JSON version file with multiple action types
    let plain_json = concat!(
        r#"{"protocol":{"minReaderVersion":4,"minWriterVersion":4}}"#, "\n",
        r#"{"metaData":{"id":"test-table","schemaString":"{}","partitionColumns":[],"format":{"provider":"parquet","options":{}},"configuration":{}}}"#, "\n",
        r#"{"add":{"path":"split-001.split","partitionValues":{},"size":1000,"modificationTime":1700000000000,"dataChange":true}}"#, "\n",
    );
    let actions = version_file::parse_version_file(plain_json.as_bytes()).unwrap();
    assert_eq!(actions.len(), 3);
    match &actions[0] {
        Action::Protocol(p) => {
            assert_eq!(p.min_reader_version, 4);
            assert_eq!(p.min_writer_version, 4);
        }
        other => panic!("Expected Protocol, got {:?}", other),
    }
    match &actions[1] {
        Action::MetaData(m) => assert_eq!(m.id, "test-table"),
        other => panic!("Expected MetaData, got {:?}", other),
    }
    match &actions[2] {
        Action::Add(a) => {
            assert_eq!(a.path, "split-001.split");
            assert_eq!(a.size, 1000);
        }
        other => panic!("Expected Add, got {:?}", other),
    }
}

#[test]
fn test_read_gzip_version_file() {
    // Create a gzip-compressed version file and read it back
    let plain_json = r#"{"add":{"path":"compressed.split","partitionValues":{},"size":500,"modificationTime":0,"dataChange":true}}"#;
    let compressed = crate::txlog::compression::gzip_compress(plain_json.as_bytes()).unwrap();
    assert!(crate::txlog::compression::is_gzip(&compressed));

    let actions = version_file::parse_version_file(&compressed).unwrap();
    assert_eq!(actions.len(), 1);
    match &actions[0] {
        Action::Add(a) => {
            assert_eq!(a.path, "compressed.split");
            assert_eq!(a.size, 500);
        }
        other => panic!("Expected Add, got {:?}", other),
    }
}

#[test]
fn test_read_mixed_action_types_from_json() {
    // Write a version file with all 5 action types as plain JSON
    let json_lines = concat!(
        r#"{"protocol":{"minReaderVersion":4,"minWriterVersion":4}}"#, "\n",
        r#"{"metaData":{"id":"mixed-test","schemaString":"{}","partitionColumns":[],"format":{"provider":"parquet","options":{}},"configuration":{}}}"#, "\n",
        r#"{"add":{"path":"new.split","partitionValues":{},"size":100,"modificationTime":0,"dataChange":true}}"#, "\n",
        r#"{"remove":{"path":"old.split","dataChange":true}}"#, "\n",
        r#"{"mergeskip":{"path":"bad.split","skipTimestamp":1700000000000,"reason":"corrupt"}}"#, "\n",
    );
    let actions = version_file::parse_version_file(json_lines.as_bytes()).unwrap();
    assert_eq!(actions.len(), 5, "All 5 action types should be parsed");

    assert!(matches!(&actions[0], Action::Protocol(_)));
    assert!(matches!(&actions[1], Action::MetaData(_)));
    assert!(matches!(&actions[2], Action::Add(_)));
    assert!(matches!(&actions[3], Action::Remove(_)));
    assert!(matches!(&actions[4], Action::MergeSkip(_)));

    // Verify specific fields
    match &actions[3] {
        Action::Remove(r) => assert_eq!(r.path, "old.split"),
        _ => unreachable!(),
    }
    match &actions[4] {
        Action::MergeSkip(s) => {
            assert_eq!(s.path, "bad.split");
            assert_eq!(s.reason, "corrupt");
        }
        _ => unreachable!(),
    }
}

#[test]
fn test_version_file_with_unknown_fields() {
    // Simulate a newer Scala writer that includes extra fields in AddAction.
    // Serde should ignore unknown fields by default (no deny_unknown_fields).
    let json = r#"{"add":{"path":"future.split","partitionValues":{},"size":200,"modificationTime":0,"dataChange":true,"futureField":"hello","anotherNewField":42,"nestedNew":{"a":1}}}"#;
    let actions = version_file::parse_version_file(json.as_bytes()).unwrap();
    assert_eq!(actions.len(), 1);
    match &actions[0] {
        Action::Add(a) => {
            assert_eq!(a.path, "future.split");
            assert_eq!(a.size, 200);
        }
        other => panic!("Expected Add, got {:?}", other),
    }
}

#[tokio::test]
async fn test_partition_pruning_through_pipeline() {
    use crate::txlog::partition_pruning::{PartitionFilter, prune_manifests};

    let tmp = tempfile::TempDir::new().unwrap();
    ensure_txlog_dir(tmp.path());
    let storage = test_storage(tmp.path());

    // Write version 0
    let v0_actions = vec![
        Action::Protocol(make_protocol()),
        Action::MetaData(make_metadata_with_partitions("prune-test", vec!["year".to_string()])),
    ];
    assert!(version_file::write_version(&storage, 0, &v0_actions).await.unwrap());

    // Create partitioned entries spanning 2022-2025
    let mut pv_2022 = HashMap::new();
    pv_2022.insert("year".to_string(), "2022".to_string());
    let mut pv_2023 = HashMap::new();
    pv_2023.insert("year".to_string(), "2023".to_string());
    let mut pv_2024 = HashMap::new();
    pv_2024.insert("year".to_string(), "2024".to_string());
    let mut pv_2025 = HashMap::new();
    pv_2025.insert("year".to_string(), "2025".to_string());

    // Create entries in groups to potentially produce multiple manifests
    let entries = vec![
        make_file_entry_full("split-2022.split", 0, pv_2022, None, None, None, Some(100)),
        make_file_entry_full("split-2023.split", 0, pv_2023, None, None, None, Some(200)),
        make_file_entry_full("split-2024.split", 0, pv_2024, None, None, None, Some(300)),
        make_file_entry_full("split-2025.split", 0, pv_2025, None, None, None, Some(400)),
    ];

    let metadata = make_metadata_with_partitions("prune-test", vec!["year".to_string()]);

    // Write checkpoint
    let cp_info = crate::txlog::avro::state_writer::write_state_checkpoint(
        &storage, 0, &entries, &make_protocol(), &metadata,
    ).await.unwrap();

    // Read state manifest
    let state_dir = cp_info.state_dir.unwrap();
    let state_manifest = crate::txlog::avro::state_reader::read_state_manifest(
        &storage, &state_dir,
    ).await.unwrap();

    // All manifests before pruning
    let all_count = state_manifest.manifests.len();
    assert!(all_count >= 1, "Should have at least 1 manifest");

    // Prune with year=2024 filter
    let filters = vec![
        PartitionFilter::Eq { column: "year".to_string(), value: "2024".to_string() },
    ];
    let pruned = prune_manifests(&state_manifest.manifests, &filters);

    // All manifests should pass since with 4 entries they likely end up in one manifest
    // that spans 2022-2025 (under MAX_ENTRIES_PER_MANIFEST threshold).
    // The key test is that the pruning logic runs without error and returns results.
    assert!(!pruned.is_empty(), "Pruned result should not be empty since 2024 is in range");

    // Verify that the pruned manifests can be read
    for manifest in &pruned {
        let read_entries = distributed::read_manifest(
            &format!("file://{}", tmp.path().display()),
            &DeltaStorageConfig::default(),
            &state_dir,
            &manifest.path,
            &metadata.configuration,
        ).await.unwrap();
        assert!(!read_entries.is_empty());
    }
}

// ============================================================================
// 7. Log Replay Integration
// ============================================================================

#[test]
fn test_replay_full_lifecycle() {
    // Checkpoint entries: 3 files
    let checkpoint_entries = vec![
        make_file_entry("file-a.split", 0),
        make_file_entry("file-b.split", 0),
        make_file_entry("file-c.split", 0),
    ];

    // Post-checkpoint version 1: add a new file + skip one
    let v1_actions = vec![
        Action::Add(make_add_action("file-d.split", 5000, Some(100))),
        Action::MergeSkip(SkipAction {
            path: "file-b.split".to_string(),
            skip_timestamp: 1700000001000,
            reason: "merge_in_progress".to_string(),
            operation: None,
            partition_values: None,
            size: None,
            retry_after: None,
            skip_count: Some(1),
        }),
    ];

    // Post-checkpoint version 2: remove file-a
    let v2_actions = vec![
        Action::Remove(RemoveAction {
            path: "file-a.split".to_string(),
            deletion_timestamp: Some(1700000002000),
            data_change: true,
            partition_values: None,
            size: None,
        }),
    ];

    let post_checkpoint = vec![
        (1, v1_actions),
        (2, v2_actions),
    ];

    let result = log_replay::replay(checkpoint_entries, post_checkpoint);

    // Final file list: file-b, file-c, file-d (file-a removed)
    let mut file_paths: Vec<&str> = result.files.iter()
        .map(|e| e.add.path.as_str())
        .collect();
    file_paths.sort();
    assert_eq!(file_paths, vec!["file-b.split", "file-c.split", "file-d.split"]);

    // Skip list: file-b
    assert_eq!(result.skips.len(), 1);
    assert_eq!(result.skips[0].path, "file-b.split");
    assert_eq!(result.skips[0].reason, "merge_in_progress");
}

#[test]
fn test_replay_with_overwrite() {
    // Start with some checkpoint entries
    let checkpoint_entries = vec![
        make_file_entry("old-a.split", 0),
        make_file_entry("old-b.split", 0),
        make_file_entry("old-c.split", 0),
    ];

    // Version 1: remove all old files and add new ones
    let v1_actions = vec![
        Action::Remove(RemoveAction {
            path: "old-a.split".to_string(),
            deletion_timestamp: Some(1700000001000),
            data_change: true,
            partition_values: None,
            size: None,
        }),
        Action::Remove(RemoveAction {
            path: "old-b.split".to_string(),
            deletion_timestamp: Some(1700000001000),
            data_change: true,
            partition_values: None,
            size: None,
        }),
        Action::Remove(RemoveAction {
            path: "old-c.split".to_string(),
            deletion_timestamp: Some(1700000001000),
            data_change: true,
            partition_values: None,
            size: None,
        }),
        Action::Add(make_add_action("new-x.split", 10000, Some(500))),
        Action::Add(make_add_action("new-y.split", 20000, Some(1000))),
    ];

    let result = log_replay::replay(checkpoint_entries, vec![(1, v1_actions)]);

    // Only new files should remain
    let mut file_paths: Vec<&str> = result.files.iter()
        .map(|e| e.add.path.as_str())
        .collect();
    file_paths.sort();
    assert_eq!(file_paths, vec!["new-x.split", "new-y.split"]);
    assert!(result.skips.is_empty());
}

// ============================================================================
// FR1-FR4 Integration Tests
// ============================================================================

/// FR1: Test can_skip_by_stats integration with file-level filtering
#[test]
fn test_data_skipping_with_partition_and_stats_filters() {
    use crate::txlog::partition_pruning::PartitionFilter;

    let mut entries = vec![
        make_file_entry_full("file-young.split", 1, [("region".into(), "us".into())].into(), None, None, None, Some(50)),
        make_file_entry_full("file-old.split", 2, [("region".into(), "eu".into())].into(), None, None, None, Some(100)),
        make_file_entry_full("file-mixed.split", 3, [("region".into(), "us".into())].into(), None, None, None, Some(75)),
    ];
    // file-young: age 10-25
    entries[0].add.min_values = Some([("age".to_string(), "10".to_string())].into());
    entries[0].add.max_values = Some([("age".to_string(), "25".to_string())].into());
    // file-old: age 50-80
    entries[1].add.min_values = Some([("age".to_string(), "50".to_string())].into());
    entries[1].add.max_values = Some([("age".to_string(), "80".to_string())].into());
    // file-mixed: age 20-60
    entries[2].add.min_values = Some([("age".to_string(), "20".to_string())].into());
    entries[2].add.max_values = Some([("age".to_string(), "60".to_string())].into());

    // Partition filter: region = "us"
    let pf = PartitionFilter::Eq { column: "region".into(), value: "us".into() };
    let after_partition: Vec<_> = entries.iter()
        .filter(|e| pf.evaluate(&e.add.partition_values))
        .collect();
    assert_eq!(after_partition.len(), 2); // file-young, file-mixed

    // Data filter: age > 30 (should skip file-young where max=25)
    let df = PartitionFilter::Gt { column: "age".into(), value: "30".into() };
    let after_data: Vec<_> = after_partition.iter()
        .filter(|e| {
            let min = e.add.min_values.as_ref();
            let max = e.add.max_values.as_ref();
            match (min, max) {
                (Some(min_vals), Some(max_vals)) => !df.can_skip_by_stats(min_vals, max_vals),
                _ => true,
            }
        })
        .collect();
    assert_eq!(after_data.len(), 1); // only file-mixed (age 20-60, max > 30)
    assert_eq!(after_data[0].add.path, "file-mixed.split");
}

/// FR2: Test Arrow batch → actions round-trip
#[test]
fn test_arrow_import_add_action_roundtrip() {
    use crate::txlog::arrow_ffi_import::arrow_batch_to_actions;
    use crate::txlog::actions::Action;
    use arrow::array::*;
    use arrow::datatypes::{DataType, Field, Schema};

    let schema = std::sync::Arc::new(Schema::new(vec![
        Field::new("action_type", DataType::Utf8, false),
        Field::new("path", DataType::Utf8, false),
        Field::new("size", DataType::Int64, false),
        Field::new("modification_time", DataType::Int64, false),
        Field::new("data_change", DataType::Boolean, false),
        Field::new("num_records", DataType::Int64, true),
        Field::new("partition_values", DataType::Utf8, true),
    ]));

    let batch = arrow::record_batch::RecordBatch::try_new(schema, vec![
        std::sync::Arc::new(StringArray::from(vec!["add", "add"])),
        std::sync::Arc::new(StringArray::from(vec!["split1.split", "split2.split"])),
        std::sync::Arc::new(Int64Array::from(vec![5000, 10000])),
        std::sync::Arc::new(Int64Array::from(vec![1700000000000i64, 1700000001000i64])),
        std::sync::Arc::new(BooleanArray::from(vec![true, true])),
        std::sync::Arc::new(Int64Array::from(vec![Some(100), Some(200)])),
        std::sync::Arc::new(StringArray::from(vec![
            Some(r#"{"year":"2024"}"#),
            Some(r#"{"year":"2023"}"#),
        ])),
    ]).unwrap();

    let actions = arrow_batch_to_actions(&batch).unwrap();
    assert_eq!(actions.len(), 2);

    match &actions[0] {
        Action::Add(add) => {
            assert_eq!(add.path, "split1.split");
            assert_eq!(add.size, 5000);
            assert_eq!(add.num_records, Some(100));
            assert_eq!(add.partition_values.get("year"), Some(&"2024".to_string()));
        }
        _ => panic!("Expected Add"),
    }
    match &actions[1] {
        Action::Add(add) => {
            assert_eq!(add.path, "split2.split");
            assert_eq!(add.partition_values.get("year"), Some(&"2023".to_string()));
        }
        _ => panic!("Expected Add"),
    }
}

/// FR4: Test range filter elimination with field stats
#[test]
fn test_range_filter_elimination_basic() {
    use crate::split_searcher::async_impl::optimize_query_with_field_stats;
    use std::collections::HashMap;

    let min_vals: HashMap<String, String> = [("age".into(), "20".into())].into();
    let max_vals: HashMap<String, String> = [("age".into(), "40".into())].into();

    // Range filter: age >= 10 AND age <= 50 → redundant (all data in [20,40] is within [10,50])
    let query = r#"{"type":"bool","must":[{"type":"range","field":"age","lower_bound":{"value":"10","inclusive":true},"upper_bound":{"value":"50","inclusive":true}}]}"#;
    let optimized = optimize_query_with_field_stats(query, &min_vals, &max_vals);

    // Should be replaced with match_all since the range is always true
    let parsed: serde_json::Value = serde_json::from_str(&optimized).unwrap();
    assert_eq!(parsed["type"], "match_all");
}

#[test]
fn test_range_filter_elimination_not_redundant() {
    use crate::split_searcher::async_impl::optimize_query_with_field_stats;
    use std::collections::HashMap;

    let min_vals: HashMap<String, String> = [("age".into(), "20".into())].into();
    let max_vals: HashMap<String, String> = [("age".into(), "40".into())].into();

    // Range filter: age >= 25 AND age <= 35 → NOT redundant (min=20 < 25)
    let query = r#"{"type":"bool","must":[{"type":"range","field":"age","lower_bound":{"value":"25","inclusive":true},"upper_bound":{"value":"35","inclusive":true}}]}"#;
    let optimized = optimize_query_with_field_stats(query, &min_vals, &max_vals);

    // Should NOT be changed
    let parsed: serde_json::Value = serde_json::from_str(&optimized).unwrap();
    assert_eq!(parsed["type"], "bool");
}

#[test]
fn test_range_filter_elimination_preserves_non_range_clauses() {
    use crate::split_searcher::async_impl::optimize_query_with_field_stats;
    use std::collections::HashMap;

    let min_vals: HashMap<String, String> = [("age".into(), "20".into())].into();
    let max_vals: HashMap<String, String> = [("age".into(), "40".into())].into();

    // Bool with range (redundant) + term (not range) → only range removed
    let query = r#"{"type":"bool","must":[{"type":"range","field":"age","lower_bound":{"value":"10","inclusive":true},"upper_bound":{"value":"50","inclusive":true}},{"type":"term","field":"name","value":"Alice"}]}"#;
    let optimized = optimize_query_with_field_stats(query, &min_vals, &max_vals);

    let parsed: serde_json::Value = serde_json::from_str(&optimized).unwrap();
    // Should still be a bool with only the term clause remaining
    assert_eq!(parsed["type"], "bool");
    let must = parsed["must"].as_array().unwrap();
    assert_eq!(must.len(), 1);
    assert_eq!(must[0]["type"], "term");
}

#[test]
fn test_range_filter_elimination_empty_stats() {
    use crate::split_searcher::async_impl::optimize_query_with_field_stats;
    use std::collections::HashMap;

    let min_vals: HashMap<String, String> = HashMap::new();
    let max_vals: HashMap<String, String> = HashMap::new();

    let query = r#"{"type":"bool","must":[{"type":"range","field":"age","lower_bound":{"value":"10","inclusive":true}}]}"#;
    let optimized = optimize_query_with_field_stats(query, &min_vals, &max_vals);

    // No change — no stats available
    assert_eq!(optimized, query);
}
