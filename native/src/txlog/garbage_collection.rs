// txlog/garbage_collection.rs - Clean up old version files and orphaned state directories
//
// Removes version files older than retention period and state directories
// not referenced by the current checkpoint.

use super::error::Result;
use super::storage::TxLogStorage;

/// Configuration for garbage collection.
#[derive(Debug, Clone)]
pub struct GcConfig {
    /// Number of days to retain version files after they are checkpointed.
    pub log_retention_days: u32,
    /// Number of hours to retain orphaned state directories.
    pub checkpoint_retention_hours: u32,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            log_retention_days: 30,
            checkpoint_retention_hours: 2,
        }
    }
}

/// Result of a garbage collection run.
#[derive(Debug, Clone, Default)]
pub struct GcResult {
    pub deleted_version_files: u32,
    pub deleted_state_dirs: u32,
    pub bytes_freed: i64,
}

/// Clean up old version files and orphaned state directories.
///
/// Steps:
/// 1. Read `_last_checkpoint` to find the current checkpoint version.
/// 2. List all version files; delete those at or before the checkpoint version
///    AND older than the retention period.
/// 3. List all `state-v<N>/` directories; delete those not referenced by
///    `_last_checkpoint` and older than `checkpoint_retention_hours`.
pub async fn garbage_collect(
    storage: &TxLogStorage,
    config: &GcConfig,
) -> Result<GcResult> {
    let mut result = GcResult::default();

    // 1. Read _last_checkpoint to get current checkpoint version
    let checkpoint_data = storage.get("_last_checkpoint").await?;
    let last_cp: super::actions::LastCheckpointInfo = serde_json::from_slice(&checkpoint_data)?;
    let checkpoint_version = last_cp.version;
    let checkpoint_state_dir = last_cp.state_dir.clone()
        .unwrap_or_else(|| TxLogStorage::state_dir_name(checkpoint_version));

    let now_ms = current_timestamp_ms();
    let retention_ms = config.log_retention_days as i64 * 24 * 60 * 60 * 1000;
    let cp_retention_ms = config.checkpoint_retention_hours as i64 * 60 * 60 * 1000;

    // 2. Delete old version files at or before checkpoint version
    let versions = storage.list_versions().await?;
    for version in &versions {
        if *version > checkpoint_version {
            // Post-checkpoint version, keep it
            continue;
        }

        // Check if the version file is old enough to delete.
        // We approximate the version file age by reading its content and checking
        // the modification_time of the first AddAction (if any), or we use a
        // simpler heuristic: treat all pre-checkpoint versions as eligible if
        // the checkpoint itself was created longer ago than the retention period.
        //
        // For simplicity and correctness, we check the checkpoint's created_time.
        // If checkpoint is recent but retention is long, we won't delete.
        let checkpoint_age_ms = last_cp.created_time.map(|ct| now_ms - ct).unwrap_or(0);
        if checkpoint_age_ms >= retention_ms {
            let path = TxLogStorage::version_path(*version);
            match storage.delete(&path).await {
                Ok(()) => result.deleted_version_files += 1,
                Err(_) => {} // Best effort
            }
        }
    }

    // 3. Delete orphaned state directories
    let all_entries = storage.list("").await?;
    let state_dirs: Vec<String> = all_entries.iter()
        .filter_map(|entry| {
            // Extract state dir name from paths like "state-v42/_manifest" or "state-v42/manifest-0000.avro"
            let entry = entry.trim_start_matches('/');
            if entry.starts_with("state-v") {
                entry.split('/').next().map(|s| s.to_string())
            } else {
                None
            }
        })
        .collect::<std::collections::HashSet<String>>()
        .into_iter()
        .collect();

    for state_dir in &state_dirs {
        if *state_dir == checkpoint_state_dir {
            // This is the current checkpoint's state dir, keep it
            continue;
        }

        // Try to read the _manifest to check created_time
        let manifest_path = format!("{}/_manifest", state_dir);
        let should_delete = match storage.get(&manifest_path).await {
            Ok(data) => {
                match serde_json::from_slice::<super::actions::StateManifest>(&data) {
                    Ok(manifest) => {
                        let age_ms = now_ms - manifest.created_time;
                        age_ms >= cp_retention_ms
                    }
                    Err(_) => true, // Corrupted manifest, clean it up
                }
            }
            Err(_) => true, // Can't read manifest, consider orphaned
        };

        if should_delete {
            // Delete all files in the state directory
            let dir_entries = storage.list(state_dir).await.unwrap_or_default();
            for entry in &dir_entries {
                let full_path = format!("{}/{}", state_dir, entry);
                let _ = storage.delete(&full_path).await;
            }
            result.deleted_state_dirs += 1;
        }
    }

    Ok(result)
}

fn current_timestamp_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta_reader::engine::DeltaStorageConfig;
    use crate::txlog::actions::*;
    use crate::txlog::version_file;
    use std::collections::HashMap;

    fn test_storage(dir: &std::path::Path) -> TxLogStorage {
        let config = DeltaStorageConfig::default();
        let url = format!("file://{}", dir.display());
        TxLogStorage::new(&url, &config).unwrap()
    }

    fn ensure_txlog_dir(dir: &std::path::Path) {
        std::fs::create_dir_all(dir.join("_transaction_log")).unwrap();
    }

    fn make_add_action(path: &str) -> AddAction {
        AddAction {
            path: path.to_string(),
            partition_values: HashMap::new(),
            size: 100,
            modification_time: 1700000000000,
            data_change: true,
            stats: None, min_values: None, max_values: None, num_records: Some(10),
            footer_start_offset: None, footer_end_offset: None, has_footer_offsets: None,
            split_tags: None, num_merge_ops: None,
            doc_mapping_json: None, doc_mapping_ref: None,
            uncompressed_size_bytes: None,
            time_range_start: None, time_range_end: None,
            companion_source_files: None, companion_delta_version: None,
            companion_fast_field_mode: None,
        }
    }

    fn make_file_entry(path: &str, version: i64) -> FileEntry {
        FileEntry {
            add: make_add_action(path),
            added_at_version: version,
            added_at_timestamp: 1700000000000,
        }
    }

    #[tokio::test]
    async fn test_gc_preserves_current_and_post_checkpoint() {
        let tmp = tempfile::TempDir::new().unwrap();
        ensure_txlog_dir(tmp.path());
        let storage = test_storage(tmp.path());

        // Write versions 0, 1, 2
        let v0 = vec![
            Action::Protocol(ProtocolAction::v4()),
            Action::MetaData(MetadataAction {
                id: "gc-test".into(),
                schema_string: "{}".into(),
                partition_columns: vec![],
                format: FormatSpec::default(),
                configuration: HashMap::new(),
                created_time: Some(1700000000000),
            }),
            Action::Add(make_add_action("s1.split")),
        ];
        assert!(version_file::write_version(&storage, 0, &v0).await.unwrap());
        assert!(version_file::write_version(&storage, 1, &[Action::Add(make_add_action("s2.split"))]).await.unwrap());
        assert!(version_file::write_version(&storage, 2, &[Action::Add(make_add_action("s3.split"))]).await.unwrap());

        // Create checkpoint at version 1
        let entries = vec![
            make_file_entry("s1.split", 0),
            make_file_entry("s2.split", 1),
        ];
        let metadata = MetadataAction {
            id: "gc-test".into(),
            schema_string: "{}".into(),
            partition_columns: vec![],
            format: FormatSpec::default(),
            configuration: HashMap::new(),
            created_time: Some(1700000000000),
        };
        crate::txlog::avro::state_writer::write_state_checkpoint(
            &storage, 1, &entries, &ProtocolAction::v4(), &metadata,
        ).await.unwrap();

        // Run GC with zero retention (delete immediately)
        let gc_config = GcConfig {
            log_retention_days: 0,
            checkpoint_retention_hours: 0,
        };
        let result = garbage_collect(&storage, &gc_config).await.unwrap();

        // Versions 0 and 1 should be deleted (at or before checkpoint version 1)
        // Version 2 should be preserved (post-checkpoint)
        assert!(result.deleted_version_files >= 1, "Should delete at least 1 old version file");

        // Version 2 (post-checkpoint) should still be readable
        let v2_read = version_file::read_version(&storage, 2).await;
        assert!(v2_read.is_ok(), "Post-checkpoint version 2 should be preserved");
    }

    #[test]
    fn test_gc_config_defaults() {
        let config = GcConfig::default();
        assert_eq!(config.log_retention_days, 30);
        assert_eq!(config.checkpoint_retention_hours, 2);
    }
}
