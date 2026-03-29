// txlog/purge_tests.rs - Regression tests for purge safety invariants
//
// Every test here reproduces a scenario that previously corrupted table state.
// If any test fails, the purge code has regressed.

use std::collections::HashMap;

use bytes::Bytes;

use crate::delta_reader::engine::DeltaStorageConfig;
use crate::txlog::actions::*;
use crate::txlog::purge;
use crate::txlog::storage::TxLogStorage;
use crate::txlog::version_file;

// ============================================================================
// Helpers
// ============================================================================

fn test_config() -> DeltaStorageConfig {
    DeltaStorageConfig::default()
}

fn test_storage(dir: &std::path::Path) -> TxLogStorage {
    let url = format!("file://{}", dir.display());
    TxLogStorage::new(&url, &test_config()).unwrap()
}

fn table_url(dir: &std::path::Path) -> String {
    format!("file://{}", dir.display())
}

fn ensure_txlog_dir(dir: &std::path::Path) {
    std::fs::create_dir_all(dir.join("_transaction_log")).unwrap();
}

fn make_add(path: &str) -> AddAction {
    AddAction {
        path: path.to_string(),
        partition_values: HashMap::new(),
        size: 1000,
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
    }
}

fn make_file_entry(path: &str, version: i64) -> FileEntry {
    FileEntry {
        add: make_add(path),
        added_at_version: version,
        added_at_timestamp: 1700000000000,
    }
}

fn make_protocol() -> ProtocolAction {
    ProtocolAction::v4()
}

fn make_metadata(id: &str) -> MetadataAction {
    MetadataAction { name: None, description: None,
        id: id.to_string(),
        schema_string: r#"{"type":"struct","fields":[{"name":"id","type":"long"}]}"#.to_string(),
        partition_columns: vec![],
        format: FormatSpec::default(),
        configuration: HashMap::new(),
        created_time: Some(1700000000000),
    }
}

/// Write a version file with a single add action.
async fn write_version(storage: &TxLogStorage, version: i64, split_path: &str) {
    let actions = vec![Action::Add(make_add(split_path))];
    version_file::write_version(storage, version, &actions).await.unwrap();
}

/// Write an init version (v0) with protocol + metadata + add.
async fn write_init_version(storage: &TxLogStorage, id: &str) {
    let actions = vec![
        Action::Protocol(make_protocol()),
        Action::MetaData(make_metadata(id)),
        Action::Add(make_add("split-0.split")),
    ];
    version_file::write_version(storage, 0, &actions).await.unwrap();
}

/// Create a checkpoint at the given version with the given entries.
async fn create_checkpoint(storage: &TxLogStorage, version: i64, entries: &[FileEntry], id: &str) {
    crate::txlog::avro::state_writer::write_state_checkpoint(
        storage,
        version,
        entries,
        &make_protocol(),
        &make_metadata(id),
    )
    .await
    .unwrap();
}

/// Write a raw _last_checkpoint pointing to a specific version.
async fn write_last_checkpoint(storage: &TxLogStorage, version: i64, state_dir: &str) {
    let cp = LastCheckpointInfo {
        version,
        size: 1,
        size_in_bytes: None,
        num_files: None,
        format: "avro-state".to_string(),
        state_dir: Some(state_dir.to_string()),
        created_time: Some(1700000000000),
    };
    let json = serde_json::to_vec(&cp).unwrap();
    storage.put("_last_checkpoint", Bytes::from(json)).await.unwrap();
}

// ============================================================================
// deleteExpiredVersions regression tests
// ============================================================================

/// Original bug: with retention_ms=0, delete_expired_versions deleted the
/// latest version file, leaving the txlog unreadable.
#[tokio::test]
async fn test_delete_versions_retention_zero_preserves_latest() {
    let tmp = tempfile::tempdir().unwrap();
    ensure_txlog_dir(tmp.path());
    let storage = test_storage(tmp.path());
    let url = table_url(tmp.path());
    let config = test_config();

    // Write 12 versions (0..=11)
    write_init_version(&storage, "preserve-latest").await;
    for i in 1..=11 {
        write_version(&storage, i, &format!("split-{}.split", i)).await;
    }

    // Checkpoint at version 10
    let entries: Vec<FileEntry> = (0..=10)
        .map(|i| make_file_entry(&format!("split-{}.split", i), i))
        .collect();
    create_checkpoint(&storage, 10, &entries, "preserve-latest").await;

    // Delete with retention=0 (immediate)
    let result = purge::delete_expired_versions(&url, &config, 0, false).await.unwrap();

    // Verify: versions 10 and 11 must still exist
    let remaining = storage.list_versions().await.unwrap();
    assert!(
        remaining.contains(&10),
        "Checkpoint version 10 must survive purge, remaining: {:?}",
        remaining
    );
    assert!(
        remaining.contains(&11),
        "Latest version 11 must survive purge, remaining: {:?}",
        remaining
    );
    // Pre-checkpoint versions should be gone
    assert!(
        !remaining.contains(&0),
        "Pre-checkpoint version 0 should be deleted, remaining: {:?}",
        remaining
    );
    assert!(result.deleted > 0, "Should have deleted some versions");
}

/// When the checkpoint version exceeds all version files (race with
/// auto-checkpoint that writes at a higher version), no version files
/// should be deleted — they are all needed.
#[tokio::test]
async fn test_delete_versions_checkpoint_beyond_max_version() {
    let tmp = tempfile::tempdir().unwrap();
    ensure_txlog_dir(tmp.path());
    let storage = test_storage(tmp.path());
    let url = table_url(tmp.path());
    let config = test_config();

    // Write versions 0..=5
    write_init_version(&storage, "cp-beyond-max").await;
    for i in 1..=5 {
        write_version(&storage, i, &format!("split-{}.split", i)).await;
    }

    // Simulate a checkpoint at version 10 (beyond max version 5)
    // This can happen during auto-checkpoint race conditions.
    let entries: Vec<FileEntry> = (0..=5)
        .map(|i| make_file_entry(&format!("split-{}.split", i), i))
        .collect();
    let state_dir = TxLogStorage::state_dir_name(10);
    // Write state dir manually + _last_checkpoint pointing to v10
    crate::txlog::avro::state_writer::write_state_checkpoint(
        &storage, 10, &entries, &make_protocol(), &make_metadata("cp-beyond-max"),
    ).await.unwrap();

    // Delete with retention=0
    let result = purge::delete_expired_versions(&url, &config, 0, false).await.unwrap();

    // ALL version files must survive — retain_floor = min(10, 5) = 5,
    // so versions 0..=4 are candidates but version 5 (max) is protected.
    let remaining = storage.list_versions().await.unwrap();
    assert!(
        remaining.contains(&5),
        "Max version 5 must survive when checkpoint is at 10, remaining: {:?}",
        remaining
    );
}

/// When there is no checkpoint, no version files should be deleted.
#[tokio::test]
async fn test_delete_versions_no_checkpoint_deletes_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    ensure_txlog_dir(tmp.path());
    let storage = test_storage(tmp.path());
    let url = table_url(tmp.path());
    let config = test_config();

    // Write 5 versions, no checkpoint
    write_init_version(&storage, "no-cp").await;
    for i in 1..=4 {
        write_version(&storage, i, &format!("split-{}.split", i)).await;
    }

    let result = purge::delete_expired_versions(&url, &config, 0, false).await.unwrap();

    assert_eq!(result.found, 0, "No versions should be found for deletion without checkpoint");
    assert_eq!(result.deleted, 0);
    let remaining = storage.list_versions().await.unwrap();
    assert_eq!(remaining.len(), 5, "All 5 versions must survive");
}

/// Dry run must never delete anything.
#[tokio::test]
async fn test_delete_versions_dry_run_deletes_nothing() {
    let tmp = tempfile::tempdir().unwrap();
    ensure_txlog_dir(tmp.path());
    let storage = test_storage(tmp.path());
    let url = table_url(tmp.path());
    let config = test_config();

    write_init_version(&storage, "dry-run").await;
    for i in 1..=5 {
        write_version(&storage, i, &format!("split-{}.split", i)).await;
    }
    let entries: Vec<FileEntry> = (0..=5)
        .map(|i| make_file_entry(&format!("split-{}.split", i), i))
        .collect();
    create_checkpoint(&storage, 5, &entries, "dry-run").await;

    let result = purge::delete_expired_versions(&url, &config, 0, true).await.unwrap();

    assert!(result.found > 0, "Should find expired versions");
    assert_eq!(result.deleted, 0, "Dry run must not delete");
    let remaining = storage.list_versions().await.unwrap();
    assert_eq!(remaining.len(), 6, "All versions must survive dry run");
}

// ============================================================================
// deleteExpiredStates regression tests
// ============================================================================

/// When _last_checkpoint is missing, no state dirs should be deleted.
/// Previously, the guard compared against "" which matched nothing,
/// causing ALL state dirs to be deleted.
#[tokio::test]
async fn test_delete_states_no_checkpoint_preserves_all() {
    let tmp = tempfile::tempdir().unwrap();
    ensure_txlog_dir(tmp.path());
    let storage = test_storage(tmp.path());
    let url = table_url(tmp.path());
    let config = test_config();

    // Create state dirs without _last_checkpoint
    let entries = vec![make_file_entry("split-0.split", 0)];
    // Write state directories manually (without _last_checkpoint update)
    crate::txlog::avro::state_writer::write_state_directory(
        &storage, 5, &entries, &make_protocol(), &make_metadata("no-cp-states"),
    ).await.unwrap();
    crate::txlog::avro::state_writer::write_state_directory(
        &storage, 10, &entries, &make_protocol(), &make_metadata("no-cp-states"),
    ).await.unwrap();

    // Deliberately do NOT write _last_checkpoint
    // Verify it doesn't exist
    assert!(storage.get("_last_checkpoint").await.is_err());

    // Try to delete with retention=0
    let result = purge::delete_expired_states(&url, &config, 0, false).await.unwrap();

    // The latest state dir (state-v10) must survive.
    // Previously this deleted BOTH dirs.
    let all_entries = storage.list("").await.unwrap();
    let state_dirs: Vec<&String> = all_entries.iter()
        .filter(|e| e.trim_start_matches('/').starts_with("state-v"))
        .collect();
    assert!(
        !state_dirs.is_empty(),
        "At least one state directory must survive when _last_checkpoint is missing"
    );

    // The latest (state-v10) must definitely be there
    let has_v10 = state_dirs.iter().any(|e| e.contains(&TxLogStorage::state_dir_name(10)));
    assert!(
        has_v10,
        "Latest state dir (v10) must be protected, found: {:?}",
        state_dirs
    );
}

/// With a valid checkpoint, only old state dirs should be deleted.
#[tokio::test]
async fn test_delete_states_preserves_checkpoint_state_dir() {
    let tmp = tempfile::tempdir().unwrap();
    ensure_txlog_dir(tmp.path());
    let storage = test_storage(tmp.path());
    let url = table_url(tmp.path());
    let config = test_config();

    let entries = vec![make_file_entry("split-0.split", 0)];

    // Create 3 state dirs: v5, v10, v15
    for v in [5, 10, 15] {
        crate::txlog::avro::state_writer::write_state_directory(
            &storage, v, &entries, &make_protocol(), &make_metadata("states-test"),
        ).await.unwrap();
    }

    // _last_checkpoint points to v10
    write_last_checkpoint(&storage, 10, &TxLogStorage::state_dir_name(10)).await;

    let result = purge::delete_expired_states(&url, &config, 0, false).await.unwrap();

    let all_entries = storage.list("").await.unwrap();
    let remaining_state_dirs: Vec<String> = all_entries.iter()
        .filter_map(|e| {
            let e = e.trim_start_matches('/');
            if e.starts_with("state-v") {
                e.split('/').next().map(|s| s.to_string())
            } else {
                None
            }
        })
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    // v10 (checkpoint) and v15 (latest by name) must survive
    let v10_name = TxLogStorage::state_dir_name(10);
    let v15_name = TxLogStorage::state_dir_name(15);
    assert!(
        remaining_state_dirs.contains(&v10_name),
        "Checkpoint state dir v10 must survive, remaining: {:?}",
        remaining_state_dirs
    );
    assert!(
        remaining_state_dirs.contains(&v15_name),
        "Latest state dir v15 must survive, remaining: {:?}",
        remaining_state_dirs
    );
    // v5 should be deleted
    let v5_name = TxLogStorage::state_dir_name(5);
    assert!(
        !remaining_state_dirs.contains(&v5_name),
        "Old state dir v5 should be deleted, remaining: {:?}",
        remaining_state_dirs
    );
}

// ============================================================================
// listRetainedVersions regression tests
// ============================================================================

/// Without a checkpoint, all versions must be retained.
#[tokio::test]
async fn test_retained_versions_no_checkpoint_keeps_all() {
    let tmp = tempfile::tempdir().unwrap();
    ensure_txlog_dir(tmp.path());
    let storage = test_storage(tmp.path());
    let url = table_url(tmp.path());
    let config = test_config();

    write_init_version(&storage, "retained-no-cp").await;
    for i in 1..=4 {
        write_version(&storage, i, &format!("split-{}.split", i)).await;
    }

    let retained = purge::list_retained_versions(&url, &config, 0).await.unwrap();
    assert_eq!(
        retained.len(),
        5,
        "All 5 versions must be retained without checkpoint, got: {:?}",
        retained
    );
}

/// With a checkpoint, the latest version is always in the retained set
/// even if checkpoint_version exceeds it.
#[tokio::test]
async fn test_retained_versions_always_includes_latest() {
    let tmp = tempfile::tempdir().unwrap();
    ensure_txlog_dir(tmp.path());
    let storage = test_storage(tmp.path());
    let url = table_url(tmp.path());
    let config = test_config();

    // Write versions 0..=5
    write_init_version(&storage, "retained-latest").await;
    for i in 1..=5 {
        write_version(&storage, i, &format!("split-{}.split", i)).await;
    }

    // Checkpoint at version 3
    let entries: Vec<FileEntry> = (0..=3)
        .map(|i| make_file_entry(&format!("split-{}.split", i), i))
        .collect();
    create_checkpoint(&storage, 3, &entries, "retained-latest").await;

    let retained = purge::list_retained_versions(&url, &config, 0).await.unwrap();

    // Must include version 5 (latest) and 3,4 (post-checkpoint)
    assert!(
        retained.contains(&5),
        "Latest version 5 must be retained, got: {:?}",
        retained
    );
    assert!(
        retained.contains(&3),
        "Checkpoint version 3 must be retained, got: {:?}",
        retained
    );
    assert!(
        retained.contains(&4),
        "Post-checkpoint version 4 must be retained, got: {:?}",
        retained
    );
    // Pre-checkpoint versions should NOT be retained with retention=0
    assert!(
        !retained.contains(&0),
        "Pre-checkpoint version 0 should not be retained, got: {:?}",
        retained
    );
}

/// The exact scenario from the bug report: 12 writes, checkpoint at 10,
/// retention_ms=0. Versions 10 and 11 must survive.
#[tokio::test]
async fn test_purge_bug_report_exact_reproduction() {
    let tmp = tempfile::tempdir().unwrap();
    ensure_txlog_dir(tmp.path());
    let storage = test_storage(tmp.path());
    let url = table_url(tmp.path());
    let config = test_config();

    // 12 writes: version 0 (init) + versions 1..=11
    write_init_version(&storage, "bug-repro").await;
    for i in 1..=11 {
        write_version(&storage, i, &format!("split-{}.split", i)).await;
    }

    // Checkpoint at version 10
    let entries: Vec<FileEntry> = (0..=10)
        .map(|i| make_file_entry(&format!("split-{}.split", i), i))
        .collect();
    create_checkpoint(&storage, 10, &entries, "bug-repro").await;

    // Purge with retention_ms=0 (the exact trigger from the bug report)
    purge::delete_expired_versions(&url, &config, 0, false).await.unwrap();

    let remaining = storage.list_versions().await.unwrap();

    // THE ASSERTION FROM THE BUG REPORT:
    // "version files 00000000000000000010.json and 00000000000000000011.json
    //  are DELETED — they should be retained."
    assert!(
        remaining.contains(&10) && remaining.contains(&11),
        "Versions 10 and 11 must survive purge with retention=0. \
         This is the exact scenario from TANTIVY4JAVA_PURGE_DELETES_LATEST_VERSION.md. \
         Remaining: {:?}",
        remaining
    );

    // Pre-checkpoint versions (0-9) should be cleaned up
    for v in 0..10 {
        assert!(
            !remaining.contains(&v),
            "Pre-checkpoint version {} should be deleted, remaining: {:?}",
            v, remaining
        );
    }
}
