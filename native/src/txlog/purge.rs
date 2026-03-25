// txlog/purge.rs - Purge primitives for transaction log cleanup
//
// Provides retention-based version/state expiration and retained file enumeration
// for the Spark-side anti-join purge algorithm.

use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::Mutex;

use arrow::array::*;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;

use crate::delta_reader::engine::DeltaStorageConfig;
use crate::debug_println;

use super::actions::*;
use super::error::{TxLogError, Result};
use super::log_replay;
use super::storage::TxLogStorage;
use super::version_file;

// ============================================================================
// listRetainedVersions
// ============================================================================

/// List versions within the retention window (non-expired).
/// The latest checkpoint version and all post-checkpoint versions are always retained.
pub async fn list_retained_versions(
    table_path: &str,
    config: &DeltaStorageConfig,
    retention_ms: i64,
) -> Result<Vec<i64>> {
    let storage = TxLogStorage::new(table_path, config)?;
    let all_versions = storage.list_versions().await?;

    if all_versions.is_empty() {
        return Ok(vec![]);
    }

    // Find checkpoint version
    let checkpoint_version = match storage.get("_last_checkpoint").await {
        Ok(data) => {
            let cp: LastCheckpointInfo = serde_json::from_slice(&data)?;
            cp.version
        }
        Err(_) => -1, // No checkpoint
    };

    if retention_ms <= 0 {
        // Keep only post-checkpoint versions + checkpoint version itself
        return Ok(all_versions.into_iter()
            .filter(|v| *v >= checkpoint_version)
            .collect());
    }

    let now_ms = current_timestamp_ms();

    // For retention > 0, keep versions newer than (now - retention) or post-checkpoint
    let mut retained = Vec::new();
    for v in &all_versions {
        if *v >= checkpoint_version {
            // Post-checkpoint: always retain
            retained.push(*v);
        } else {
            // Pre-checkpoint: check age by reading version file modification time
            // Approximate: use version number ordering + checkpoint created_time
            // Versions close to checkpoint are likely recent enough
            let path = TxLogStorage::version_path(*v);
            if let Ok(data) = storage.get(&path).await {
                if let Ok(actions) = version_file::parse_version_file(&data) {
                    // Use the first add's modification_time as the version timestamp
                    let version_ts = actions.iter().find_map(|a| match a {
                        Action::Add(add) => Some(add.modification_time),
                        _ => None,
                    }).unwrap_or(0);
                    if now_ms - version_ts < retention_ms {
                        retained.push(*v);
                    }
                }
            }
        }
    }

    retained.sort();
    Ok(retained)
}

// ============================================================================
// Retained files cursor (Arrow FFI streaming)
// ============================================================================

/// State for streaming retained files via Arrow FFI.
pub struct RetainedFilesCursor {
    entries: Vec<FileEntry>,
    position: usize,
}

/// Global registry of open cursors.
static CURSOR_REGISTRY: once_cell::sync::Lazy<Mutex<HashMap<i64, RetainedFilesCursor>>> =
    once_cell::sync::Lazy::new(|| Mutex::new(HashMap::new()));

static NEXT_CURSOR_ID: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(1);

/// Open a cursor over all file paths referenced by non-expired versions.
pub async fn open_retained_files_cursor(
    table_path: &str,
    config: &DeltaStorageConfig,
    retention_ms: i64,
) -> Result<i64> {
    let retained_versions = list_retained_versions(table_path, config, retention_ms).await?;

    let storage = TxLogStorage::new(table_path, config)?;

    // Collect all file entries from retained versions
    let mut all_entries: Vec<FileEntry> = Vec::new();

    // Read from checkpoint if it exists
    if let Ok(cp_data) = storage.get("_last_checkpoint").await {
        if let Ok(cp) = serde_json::from_slice::<LastCheckpointInfo>(&cp_data) {
            let state_dir = cp.state_dir.clone()
                .unwrap_or_else(|| TxLogStorage::state_dir_name(cp.version));
            let state_manifest = super::avro::state_reader::read_state_manifest(&storage, &state_dir).await?;

            // Read manifest entries
            for manifest_info in &state_manifest.manifests {
                if let Ok(entries) = super::avro::state_reader::read_single_manifest(
                    &storage, &state_dir, &manifest_info.path, &HashMap::new(),
                ).await {
                    all_entries.extend(entries);
                }
            }

            // Apply post-checkpoint changes from retained versions
            for v in &retained_versions {
                if *v > cp.version {
                    if let Ok(actions) = version_file::read_version(&storage, *v).await {
                        for action in actions {
                            match action {
                                Action::Add(add) => {
                                    let ts = add.modification_time;
                                    all_entries.push(FileEntry {
                                        add,
                                        added_at_version: *v,
                                        added_at_timestamp: ts,
                                    });
                                }
                                Action::Remove(r) => {
                                    all_entries.retain(|e| e.add.path != r.path);
                                }
                                _ => {}
                            }
                        }
                    }
                }
            }
        }
    } else {
        // No checkpoint — replay all retained versions
        let mut versioned_actions: Vec<(i64, Vec<Action>)> = Vec::new();
        for v in &retained_versions {
            if let Ok(actions) = version_file::read_version(&storage, *v).await {
                versioned_actions.push((*v, actions));
            }
        }
        let replay_result = log_replay::replay(vec![], versioned_actions);
        all_entries = replay_result.files;
    }

    // Deduplicate by path (keep latest version)
    let mut seen = std::collections::HashSet::new();
    all_entries.retain(|e| seen.insert(e.add.path.clone()));

    let cursor_id = NEXT_CURSOR_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let cursor = RetainedFilesCursor {
        entries: all_entries,
        position: 0,
    };

    CURSOR_REGISTRY.lock().insert(cursor_id, cursor);
    debug_println!("📊 PURGE: Opened retained files cursor {} with {} entries", cursor_id, seen.len());

    Ok(cursor_id)
}

/// Read the next batch of retained files as an Arrow RecordBatch.
/// Returns None when exhausted.
pub fn read_next_retained_files_batch(cursor_id: i64, batch_size: usize) -> Result<Option<RecordBatch>> {
    let mut registry = CURSOR_REGISTRY.lock();
    let cursor = registry.get_mut(&cursor_id)
        .ok_or_else(|| TxLogError::Storage(anyhow::anyhow!("Invalid cursor handle: {}", cursor_id)))?;

    if cursor.position >= cursor.entries.len() {
        return Ok(None);
    }

    let end = std::cmp::min(cursor.position + batch_size, cursor.entries.len());
    let batch_entries = &cursor.entries[cursor.position..end];
    cursor.position = end;

    // Build Arrow RecordBatch: { path: Utf8, size: Int64, version: Int64 }
    let schema = Arc::new(Schema::new(vec![
        Field::new("path", DataType::Utf8, false),
        Field::new("size", DataType::Int64, false),
        Field::new("version", DataType::Int64, false),
    ]));

    let len = batch_entries.len();
    let mut path_builder = StringBuilder::with_capacity(len, len * 64);
    let mut size_builder = Int64Builder::with_capacity(len);
    let mut version_builder = Int64Builder::with_capacity(len);

    for entry in batch_entries {
        path_builder.append_value(&entry.add.path);
        size_builder.append_value(entry.add.size);
        version_builder.append_value(entry.added_at_version);
    }

    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(path_builder.finish()),
        Arc::new(size_builder.finish()),
        Arc::new(version_builder.finish()),
    ];

    let batch = RecordBatch::try_new(schema, columns)
        .map_err(|e| TxLogError::Storage(anyhow::anyhow!("Arrow error: {}", e)))?;

    Ok(Some(batch))
}

/// Close a retained files cursor and release resources.
pub fn close_retained_files_cursor(cursor_id: i64) {
    let removed = CURSOR_REGISTRY.lock().remove(&cursor_id);
    if removed.is_some() {
        debug_println!("📊 PURGE: Closed retained files cursor {}", cursor_id);
    }
}

// ============================================================================
// deleteExpiredStates
// ============================================================================

#[derive(Debug, Clone, serde::Serialize)]
pub struct DeleteResult {
    pub found: i32,
    pub deleted: i32,
}

/// Delete Avro state directories older than the retention period.
/// Always preserves the latest state directory.
pub async fn delete_expired_states(
    table_path: &str,
    config: &DeltaStorageConfig,
    retention_ms: i64,
    dry_run: bool,
) -> Result<DeleteResult> {
    let storage = TxLogStorage::new(table_path, config)?;
    let now_ms = current_timestamp_ms();

    // Find latest checkpoint state dir
    let latest_state_dir = match storage.get("_last_checkpoint").await {
        Ok(data) => {
            let cp: LastCheckpointInfo = serde_json::from_slice(&data)?;
            cp.state_dir.unwrap_or_else(|| TxLogStorage::state_dir_name(cp.version))
        }
        Err(_) => String::new(),
    };

    // List all state directories
    let all_entries = storage.list("").await?;
    let state_dirs: Vec<String> = all_entries.iter()
        .filter_map(|entry| {
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

    let mut found = 0i32;
    let mut deleted = 0i32;

    for state_dir in &state_dirs {
        if *state_dir == latest_state_dir {
            continue; // Always preserve latest
        }

        // Check age — only count as "found" if actually expired
        let manifest_path = format!("{}/_manifest", state_dir);
        let is_expired = if retention_ms <= 0 {
            true // Immediate cleanup of all non-latest
        } else {
            match storage.get(&manifest_path).await {
                Ok(data) => {
                    match serde_json::from_slice::<StateManifest>(&data) {
                        Ok(manifest) if manifest.created_time > 0 => {
                            (now_ms - manifest.created_time) >= retention_ms
                        }
                        _ => false, // Can't determine age → keep (safe default)
                    }
                }
                Err(_) => false, // Can't read manifest → keep (safe default)
            }
        };

        if !is_expired {
            continue;
        }

        found += 1;
        if !dry_run {
            let dir_entries = storage.list(state_dir).await.unwrap_or_default();
            for entry in &dir_entries {
                let _ = storage.delete(entry).await;
            }
            deleted += 1;
        }
    }

    Ok(DeleteResult { found, deleted })
}

// ============================================================================
// deleteExpiredVersions
// ============================================================================

/// Delete version files older than the retention period that are
/// fully captured in a checkpoint (safe to remove).
pub async fn delete_expired_versions(
    table_path: &str,
    config: &DeltaStorageConfig,
    retention_ms: i64,
    dry_run: bool,
) -> Result<DeleteResult> {
    let storage = TxLogStorage::new(table_path, config)?;

    // Find checkpoint version
    let checkpoint_version = match storage.get("_last_checkpoint").await {
        Ok(data) => {
            let cp: LastCheckpointInfo = serde_json::from_slice(&data)?;
            cp.version
        }
        Err(_) => return Ok(DeleteResult { found: 0, deleted: 0 }), // No checkpoint, nothing to clean
    };

    let now_ms = current_timestamp_ms();
    let all_versions = storage.list_versions().await?;

    let mut found = 0i32;
    let mut deleted = 0i32;

    for version in &all_versions {
        if *version >= checkpoint_version {
            continue; // Post-checkpoint, keep
        }

        // Check age — only count as "found" if actually expired
        let path = TxLogStorage::version_path(*version);
        let is_expired = if retention_ms <= 0 {
            true // Immediate cleanup of all pre-checkpoint
        } else if let Ok(data) = storage.get(&path).await {
            if let Ok(actions) = version_file::parse_version_file(&data) {
                // Find the best timestamp from any action in the version file
                let version_ts = actions.iter().find_map(|a| match a {
                    Action::Add(add) if add.modification_time > 0 => Some(add.modification_time),
                    Action::MetaData(m) => m.created_time.filter(|t| *t > 0),
                    Action::Remove(r) => r.deletion_timestamp.filter(|t| *t > 0),
                    Action::MergeSkip(s) if s.skip_timestamp > 0 => Some(s.skip_timestamp),
                    _ => None,
                });
                match version_ts {
                    Some(ts) => (now_ms - ts) >= retention_ms,
                    None => false, // No timestamp found → can't determine age → keep
                }
            } else {
                false // Can't parse → keep (safe default)
            }
        } else {
            continue; // Can't read, skip
        };

        if !is_expired {
            continue;
        }

        found += 1;
        if !dry_run {
            match storage.delete(&path).await {
                Ok(()) => deleted += 1,
                Err(_) => {} // Best effort
            }
        }
    }

    Ok(DeleteResult { found, deleted })
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

    #[test]
    fn test_delete_result_serializes() {
        let r = DeleteResult { found: 5, deleted: 3 };
        let json = serde_json::to_string(&r).unwrap();
        assert!(json.contains("\"found\":5"));
        assert!(json.contains("\"deleted\":3"));
    }

    #[test]
    fn test_cursor_lifecycle() {
        let cursor = RetainedFilesCursor {
            entries: vec![],
            position: 0,
        };
        let id = NEXT_CURSOR_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        CURSOR_REGISTRY.lock().insert(id, cursor);
        assert!(CURSOR_REGISTRY.lock().contains_key(&id));
        close_retained_files_cursor(id);
        assert!(!CURSOR_REGISTRY.lock().contains_key(&id));
    }

    #[test]
    fn test_read_batch_empty_cursor() {
        let id = NEXT_CURSOR_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        CURSOR_REGISTRY.lock().insert(id, RetainedFilesCursor {
            entries: vec![],
            position: 0,
        });
        let batch = read_next_retained_files_batch(id, 100).unwrap();
        assert!(batch.is_none());
        close_retained_files_cursor(id);
    }
}
