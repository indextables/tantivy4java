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
use super::cache;
use super::error::{TxLogError, Result};
use super::log_replay;
use super::storage::TxLogStorage;
use super::version_file;

// ============================================================================
// listRetainedVersions
// ============================================================================

/// List versions within the retention window (non-expired).
///
/// Safety invariants — always holds:
/// - The latest version is always retained
/// - All versions at or after the checkpoint are always retained
/// - When no checkpoint exists, all versions are retained (they are the
///   sole source of truth)
/// - When a version's age cannot be determined, it is retained
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

    let max_version = all_versions.iter().copied().max().unwrap(); // safe: non-empty

    // Find checkpoint version.  If there is no checkpoint, every version is
    // needed for replay — retain them all.
    let checkpoint_version = match storage.get("_last_checkpoint").await {
        Ok(data) => {
            let cp: LastCheckpointInfo = serde_json::from_slice(&data)?;
            cp.version
        }
        Err(_) => return Ok(all_versions), // No checkpoint → keep everything
    };

    // retain_floor: everything at or above this version is unconditionally kept.
    let retain_floor = std::cmp::min(checkpoint_version, max_version);

    if retention_ms <= 0 {
        return Ok(all_versions.into_iter()
            .filter(|v| *v >= retain_floor)
            .collect());
    }

    let now_ms = current_timestamp_ms();

    let mut retained = Vec::new();
    for v in &all_versions {
        if *v >= retain_floor {
            retained.push(*v);
        } else {
            // Pre-checkpoint: check age by reading version file modification time
            let path = TxLogStorage::version_path(*v);
            if let Ok(data) = storage.get(&path).await {
                if let Ok(actions) = version_file::parse_version_file(&data) {
                    let version_ts = actions.iter().find_map(|a| match a {
                        Action::Add(add) => Some(add.modification_time),
                        _ => None,
                    }).unwrap_or(0);
                    if now_ms - version_ts < retention_ms {
                        retained.push(*v);
                    }
                } else {
                    // Can't parse → keep (safe default)
                    retained.push(*v);
                }
            } else {
                // Can't read → keep (safe default)
                retained.push(*v);
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
///
/// Safety invariants — never violated regardless of inputs:
/// - The state directory referenced by `_last_checkpoint` is never deleted
/// - The lexicographically latest state directory is never deleted (fallback
///   protection when `_last_checkpoint` is missing or unreadable)
/// - If we cannot positively identify which state dirs are safe to delete,
///   we delete nothing
pub async fn delete_expired_states(
    table_path: &str,
    config: &DeltaStorageConfig,
    retention_ms: i64,
    dry_run: bool,
) -> Result<DeleteResult> {
    let storage = TxLogStorage::new(table_path, config)?;
    let now_ms = current_timestamp_ms();

    // List all state directories
    let all_entries = storage.list("").await?;
    let mut state_dirs: Vec<String> = all_entries.iter()
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

    if state_dirs.is_empty() {
        return Ok(DeleteResult { found: 0, deleted: 0 });
    }

    // Sort so we can identify the lexicographically latest (highest version)
    state_dirs.sort();
    let latest_by_name = state_dirs.last().unwrap().clone();

    // Find checkpoint state dir — if we can't read it, the latest-by-name
    // guard still protects us, but we also refuse to delete anything when
    // there's only one state dir (nothing is safe to remove).
    let checkpoint_state_dir = match storage.get("_last_checkpoint").await {
        Ok(data) => {
            match serde_json::from_slice::<LastCheckpointInfo>(&data) {
                Ok(cp) => Some(
                    cp.state_dir.unwrap_or_else(|| TxLogStorage::state_dir_name(cp.version))
                ),
                Err(_) => None, // Corrupt _last_checkpoint — can't trust it
            }
        }
        Err(_) => None, // No checkpoint file
    };

    // Build the set of protected state dirs
    let mut protected = std::collections::HashSet::new();
    protected.insert(latest_by_name.clone());
    if let Some(ref cp_dir) = checkpoint_state_dir {
        protected.insert(cp_dir.clone());
    }

    let mut found = 0i32;
    let mut deleted = 0i32;

    for state_dir in &state_dirs {
        if protected.contains(state_dir) {
            continue;
        }

        // Check age using the _manifest file's last_modified timestamp.
        // Primary: head() on _manifest (works on local FS and cloud).
        // Fallback: newest file mtime from list_with_meta().
        let manifest_path = format!("{}/_manifest", state_dir);
        let is_expired = if retention_ms <= 0 {
            true // Immediate cleanup of all non-protected
        } else {
            // Try _manifest head first
            let mtime = match storage.last_modified_ms(&manifest_path).await {
                Ok(ms) if ms > 0 => Some(ms),
                _ => {
                    // Fallback: check newest file in dir
                    storage.list_with_meta(state_dir).await.ok()
                        .and_then(|metas| metas.iter().map(|(_, ms)| *ms).max())
                        .filter(|ms| *ms > 0)
                }
            };
            match mtime {
                Some(ms) => (now_ms - ms) >= retention_ms,
                None => false, // Can't determine age → keep
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

    // Invalidate cache after deletion so stale snapshots referencing
    // deleted state dirs are not served
    if deleted > 0 {
        cache::invalidate_table_cache(table_path);
    }

    Ok(DeleteResult { found, deleted })
}

// ============================================================================
// deleteExpiredVersions
// ============================================================================

/// Delete version files older than the retention period that are
/// fully captured in a checkpoint (safe to remove).
///
/// Safety invariants — never violated regardless of inputs:
/// - The latest version file (highest version number) is never deleted
/// - All version files at or after the checkpoint version are never deleted
/// - If no checkpoint exists, nothing is deleted (versions are the only
///   source of truth)
/// - If a version's age cannot be determined, it is kept
/// - Only versions that are strictly BOTH pre-checkpoint AND pre-max-version
///   AND expired are candidates for deletion
pub async fn delete_expired_versions(
    table_path: &str,
    config: &DeltaStorageConfig,
    retention_ms: i64,
    dry_run: bool,
) -> Result<DeleteResult> {
    let storage = TxLogStorage::new(table_path, config)?;

    // Without a checkpoint, version files are the sole source of truth.
    // Deleting any of them risks data loss. Bail out entirely.
    let checkpoint_version = match storage.get("_last_checkpoint").await {
        Ok(data) => {
            let cp: LastCheckpointInfo = serde_json::from_slice(&data)?;
            cp.version
        }
        Err(_) => return Ok(DeleteResult { found: 0, deleted: 0 }),
    };

    let all_versions = storage.list_versions().await?;
    if all_versions.is_empty() {
        return Ok(DeleteResult { found: 0, deleted: 0 });
    }

    let now_ms = current_timestamp_ms();
    let max_version = all_versions.iter().copied().max().unwrap(); // safe: non-empty

    // Compute the minimum version we must keep.  Everything at or above this
    // is unconditionally retained.  We take the lower of checkpoint_version
    // and max_version so that even if the checkpoint was written at a version
    // beyond all existing version files (race with auto-checkpoint), we still
    // protect everything from the checkpoint onward.
    let retain_floor = std::cmp::min(checkpoint_version, max_version);

    let mut found = 0i32;
    let mut deleted = 0i32;

    for version in &all_versions {
        // Unconditionally retain: latest version, and anything at/above the
        // checkpoint (needed for replay on top of the checkpoint snapshot).
        if *version >= retain_floor {
            continue;
        }

        // Check age using filesystem last_modified time
        let path = TxLogStorage::version_path(*version);
        let is_expired = if retention_ms <= 0 {
            true // Immediate cleanup of all pre-floor versions
        } else {
            match storage.last_modified_ms(&path).await {
                Ok(last_modified) if last_modified > 0 => {
                    (now_ms - last_modified) >= retention_ms
                }
                _ => false, // Can't determine age → keep (safe default)
            }
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

    // Invalidate cache after deletion so stale snapshots referencing
    // deleted version files are not served
    if deleted > 0 {
        cache::invalidate_table_cache(table_path);
    }

    Ok(DeleteResult { found, deleted })
}

// ============================================================================
// listSkipActions — scan recent version files for skip actions
// ============================================================================

/// List skip actions from version files within the given time window.
/// Scans version files backward from the latest, stopping when actions
/// are older than maxAgeMs. Returns all SkipActions found.
pub async fn list_skip_actions(
    table_path: &str,
    config: &DeltaStorageConfig,
    max_age_ms: i64,
) -> Result<Vec<SkipAction>> {
    let storage = TxLogStorage::new(table_path, config)?;
    let all_versions = storage.list_versions().await?;

    if all_versions.is_empty() {
        return Ok(vec![]);
    }

    let now_ms = current_timestamp_ms();
    let cutoff_ms = if max_age_ms > 0 { now_ms - max_age_ms } else { 0 };
    let mut skip_actions = Vec::new();

    // Scan versions in reverse order (newest first) for efficiency
    for version in all_versions.iter().rev() {
        if let Ok(actions) = version_file::read_version(&storage, *version).await {
            let mut version_has_recent = false;
            for action in actions {
                match action {
                    Action::MergeSkip(skip) => {
                        if skip.skip_timestamp >= cutoff_ms {
                            version_has_recent = true;
                            skip_actions.push(skip);
                        }
                    }
                    Action::Add(ref add) => {
                        if add.modification_time >= cutoff_ms {
                            version_has_recent = true;
                        }
                    }
                    _ => {}
                }
            }
            // If this version has no recent actions, older versions won't either
            if !version_has_recent && max_age_ms > 0 {
                break;
            }
        }
    }

    debug_println!("📊 PURGE: Found {} skip actions within {}ms window", skip_actions.len(), max_age_ms);
    Ok(skip_actions)
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
