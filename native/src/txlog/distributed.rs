// txlog/distributed.rs - Distributable primitives (driver + executor)
//
// Follows the same 2-phase pattern as delta_reader/distributed.rs:
//   Phase 1 (driver): lightweight metadata reads → return paths to distribute
//   Phase 2 (executor): read individual manifests in parallel

use std::collections::HashMap;
use std::sync::Arc;

use crate::delta_reader::engine::DeltaStorageConfig;
use crate::debug_println;

use super::actions::*;
use super::cache;
use super::error::{TxLogError, Result};
use super::log_replay;
use super::schema_dedup;
use super::storage::TxLogStorage;
use super::version_file;

// ============================================================================
// Driver-side primitive: get_txlog_snapshot_info
// ============================================================================

/// Information needed to distribute manifest reads.
#[derive(Debug, Clone)]
pub struct TxLogSnapshotInfo {
    pub checkpoint_version: i64,
    pub manifest_paths: Vec<ManifestPathInfo>,
    pub post_checkpoint_version_paths: Vec<String>,
    pub protocol: ProtocolAction,
    pub metadata: MetadataAction,
    pub state_dir: String,
}

#[derive(Debug, Clone)]
pub struct ManifestPathInfo {
    pub path: String,
    pub file_count: i64,
    pub partition_bounds: Option<PartitionBounds>,
}

/// Driver-side: read _last_checkpoint + state manifest + list post-checkpoint versions.
///
/// Cost: 1 GET (_last_checkpoint) + 1 GET (_manifest) + 1 LIST (version files)
/// With caching: 0 I/O if cached and within TTL.
pub async fn get_txlog_snapshot_info(
    table_path: &str,
    config: &DeltaStorageConfig,
) -> Result<TxLogSnapshotInfo> {
    get_txlog_snapshot_info_with_cache(table_path, config, &HashMap::new()).await
}

/// Cached variant that accepts a config map for `cache.ttl.ms` extraction.
pub async fn get_txlog_snapshot_info_with_cache(
    table_path: &str,
    config: &DeltaStorageConfig,
    config_map: &HashMap<String, String>,
) -> Result<TxLogSnapshotInfo> {
    // Check cache first
    if let Some(ttl) = cache::extract_cache_ttl(config_map) {
        let c = cache::get_or_create_cache(table_path, ttl);
        if let Some(cached_meta) = c.get_metadata() {
            if let Some(cached_cp) = c.get_last_checkpoint() {
                let state_dir = cached_cp.state_dir.clone()
                    .unwrap_or_else(|| TxLogStorage::state_dir_name(cached_cp.version));

                // We still need to read the state manifest for manifest paths,
                // but we can use the cached protocol/metadata
                let storage = TxLogStorage::new(table_path, config)?;
                let state_manifest = super::avro::state_reader::read_state_manifest(&storage, &state_dir).await?;
                let all_versions = storage.list_versions().await?;
                let post_cp_paths: Vec<String> = all_versions.iter()
                    .filter(|v| **v > cached_cp.version)
                    .map(|v| TxLogStorage::version_path(*v))
                    .collect();

                let manifest_paths: Vec<ManifestPathInfo> = state_manifest.manifests.iter()
                    .map(|m| ManifestPathInfo {
                        path: m.path.clone(),
                        file_count: m.file_count,
                        partition_bounds: m.partition_bounds.clone(),
                    })
                    .collect();

                debug_println!("📊 DISTRIBUTED: snapshot_info from CACHE: checkpoint v{}, {} manifests",
                    cached_cp.version, manifest_paths.len());

                return Ok(TxLogSnapshotInfo {
                    checkpoint_version: cached_cp.version,
                    manifest_paths,
                    post_checkpoint_version_paths: post_cp_paths,
                    protocol: cached_meta.0.clone(),
                    metadata: cached_meta.1.clone(),
                    state_dir,
                });
            }
        }
    }

    let storage = TxLogStorage::new(table_path, config)?;

    // Read _last_checkpoint
    let checkpoint_data = storage.get("_last_checkpoint").await?;
    let last_cp: LastCheckpointInfo = serde_json::from_slice(&checkpoint_data)?;

    let state_dir = last_cp.state_dir.clone()
        .unwrap_or_else(|| TxLogStorage::state_dir_name(last_cp.version));

    // Read state manifest
    let state_manifest = super::avro::state_reader::read_state_manifest(&storage, &state_dir).await?;

    // List post-checkpoint version files
    let all_versions = storage.list_versions().await?;
    let post_cp_paths: Vec<String> = all_versions.iter()
        .filter(|v| **v > last_cp.version)
        .map(|v| TxLogStorage::version_path(*v))
        .collect();

    // Read post-checkpoint versions for potential protocol/metadata updates
    let mut all_version_actions: Vec<(i64, Vec<Action>)> = Vec::new();
    for v in all_versions.iter().filter(|v| **v > last_cp.version) {
        if let Ok(actions) = version_file::read_version(&storage, *v).await {
            all_version_actions.push((*v, actions));
        }
    }

    // Try to extract protocol/metadata from post-checkpoint versions first,
    // then fall back to version 0 as a last resort
    let v0_actions = match version_file::read_version(&storage, 0).await {
        Ok(actions) => actions,
        Err(_) => vec![], // May not exist if table was created differently
    };
    let (protocol, metadata) = log_replay::extract_metadata(&v0_actions, &all_version_actions);
    let protocol = protocol.unwrap_or_else(ProtocolAction::v4);
    let metadata = metadata.unwrap_or_else(|| MetadataAction {
        id: String::new(),
        schema_string: String::new(),
        partition_columns: vec![],
        format: FormatSpec::default(),
        configuration: HashMap::new(),
        created_time: None,
    });

    let manifest_paths: Vec<ManifestPathInfo> = state_manifest.manifests.iter()
        .map(|m| ManifestPathInfo {
            path: m.path.clone(),
            file_count: m.file_count,
            partition_bounds: m.partition_bounds.clone(),
        })
        .collect();

    debug_println!("📊 DISTRIBUTED: snapshot_info: checkpoint v{}, {} manifests, {} post-cp versions",
        last_cp.version, manifest_paths.len(), post_cp_paths.len());

    let checkpoint_version = last_cp.version;

    // Populate cache
    if let Some(ttl) = cache::extract_cache_ttl(config_map) {
        let c = cache::get_or_create_cache(table_path, ttl);
        c.put_metadata(protocol.clone(), metadata.clone());
        c.put_last_checkpoint(last_cp);
    }

    Ok(TxLogSnapshotInfo {
        checkpoint_version,
        manifest_paths,
        post_checkpoint_version_paths: post_cp_paths,
        protocol,
        metadata,
        state_dir,
    })
}

// ============================================================================
// Driver-side primitive: get_current_version
// ============================================================================

/// Get the current (latest) version number.
/// Cost: 1 GET (_last_checkpoint) + 1 LIST (post-checkpoint files)
pub async fn get_current_version(
    table_path: &str,
    config: &DeltaStorageConfig,
) -> Result<i64> {
    let storage = TxLogStorage::new(table_path, config)?;
    let all_versions = storage.list_versions().await?;
    all_versions.last().copied().ok_or_else(|| {
        TxLogError::NotInitialized { path: table_path.to_string() }
    })
}

// ============================================================================
// Driver-side primitive: read_post_checkpoint_changes
// ============================================================================

/// Changes since the checkpoint.
#[derive(Debug, Clone)]
pub struct TxLogChanges {
    pub added_files: Vec<FileEntry>,
    pub removed_paths: Vec<String>,
    pub skip_actions: Vec<SkipAction>,
    pub max_version: i64,
}

/// Read post-checkpoint version files and compute incremental changes.
/// Driver-side because version files are small and few.
pub async fn read_post_checkpoint_changes(
    table_path: &str,
    config: &DeltaStorageConfig,
    version_paths: &[String],
    metadata_config: &HashMap<String, String>,
) -> Result<TxLogChanges> {
    let storage = TxLogStorage::new(table_path, config)?;

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut skips = Vec::new();
    let mut max_version: i64 = 0;

    for path in version_paths {
        // Parse version number from path (e.g., "00000000000000000042.json" → 42)
        // The 20-digit zero-padded format is safe to parse directly as i64.
        let name = path.rsplit('/').next().unwrap_or(path);
        let version = name.trim_end_matches(".json")
            .parse::<i64>()
            .unwrap_or_else(|_| {
                crate::debug_println!("⚠️ TXLOG: Failed to parse version from path: {}", path);
                0
            });
        if version > max_version {
            max_version = version;
        }

        let data = storage.get(path).await?;
        let actions = version_file::parse_version_file(&data)?;

        for action in actions {
            match action {
                Action::Add(mut add) => {
                    schema_dedup::restore_schemas_on_adds(
                        std::slice::from_mut(&mut add),
                        metadata_config,
                    );
                    // Use the file's own modification_time as the added_at_timestamp
                    let timestamp = add.modification_time;
                    added.push(FileEntry {
                        add,
                        added_at_version: version,
                        added_at_timestamp: timestamp,
                    });
                }
                Action::Remove(r) => removed.push(r.path),
                Action::MergeSkip(s) => skips.push(s),
                _ => {}
            }
        }
    }

    Ok(TxLogChanges {
        added_files: added,
        removed_paths: removed,
        skip_actions: skips,
        max_version,
    })
}

// ============================================================================
// Executor-side primitive: read_manifest
// ============================================================================

/// Read ONE Avro manifest file → Vec<FileEntry>.
/// Highly parallelizable: each executor reads one manifest.
///
/// Cost: 1 GET (manifest avro file). With caching: 0 if manifest already cached.
/// Manifests are immutable, so they are cached globally without TTL.
pub async fn read_manifest(
    table_path: &str,
    config: &DeltaStorageConfig,
    state_dir: &str,
    manifest_path: &str,
    metadata_config: &HashMap<String, String>,
) -> Result<Vec<FileEntry>> {
    // Check global manifest cache (manifests are immutable — no TTL needed)
    let cache_key = format!("{}/{}/{}", table_path, state_dir, manifest_path);
    if let Some(cached) = cache::get_cached_manifest(&cache_key) {
        debug_println!("📊 DISTRIBUTED: read_manifest CACHE HIT: {}", manifest_path);
        return Ok(cached.as_ref().clone());
    }

    let storage = TxLogStorage::new(table_path, config)?;
    let entries = super::avro::state_reader::read_single_manifest(
        &storage,
        state_dir,
        manifest_path,
        metadata_config,
    ).await?;

    // Cache the result (manifests are immutable)
    cache::put_cached_manifest(&cache_key, Arc::new(entries.clone()));
    Ok(entries)
}

// ============================================================================
// Driver-side primitives: write_version / write_checkpoint
// ============================================================================

/// Write retry configuration.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 10,
            base_delay_ms: 100,
            max_delay_ms: 5000,
        }
    }
}

/// Result of a write operation.
#[derive(Debug, Clone)]
pub struct WriteResult {
    pub version: i64,
    pub retries: u32,
    pub conflicted_versions: Vec<i64>,
}

/// Write a new version file with automatic conflict retry.
pub async fn write_version(
    table_path: &str,
    config: &DeltaStorageConfig,
    actions: Vec<Action>,
    retry_config: RetryConfig,
) -> Result<WriteResult> {
    let storage = TxLogStorage::new(table_path, config)?;
    let mut conflicted = Vec::new();

    for attempt in 0..retry_config.max_attempts {
        // Determine target version
        let current_versions = storage.list_versions().await?;
        let target_version = current_versions.last().map(|v| v + 1).unwrap_or(0);

        // Try to write
        let written = version_file::write_version(&storage, target_version, &actions).await?;
        if written {
            debug_println!("✅ DISTRIBUTED: Wrote version {} (attempt {})", target_version, attempt);
            // Invalidate cached data for this table since state changed
            cache::invalidate_table_cache(table_path);
            return Ok(WriteResult {
                version: target_version,
                retries: attempt,
                conflicted_versions: conflicted,
            });
        }

        // Conflict
        conflicted.push(target_version);
        debug_println!("⚠️ DISTRIBUTED: Version conflict at {}, retry {}/{}",
            target_version, attempt + 1, retry_config.max_attempts);

        // Exponential backoff with jitter to prevent thundering herd
        let base_delay = std::cmp::min(
            retry_config.base_delay_ms * (1 << attempt.min(10)),
            retry_config.max_delay_ms,
        );
        // Add ±25% jitter
        let jitter_range = base_delay / 4;
        let jitter = if jitter_range > 0 {
            // Simple deterministic jitter based on attempt number to avoid rand dependency
            (attempt as u64 * 7919) % (jitter_range * 2) // pseudo-random spread
        } else {
            0
        };
        let delay = base_delay.saturating_sub(jitter_range).saturating_add(jitter);
        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
    }

    Err(TxLogError::MaxRetriesExceeded {
        retries: retry_config.max_attempts,
        last_conflict: *conflicted.last().unwrap_or(&0),
    })
}

/// Write a new version file with a single attempt (no retry).
/// Returns WriteResult with version=-1 if version already exists.
pub async fn write_version_once(
    table_path: &str,
    config: &DeltaStorageConfig,
    actions: Vec<Action>,
) -> Result<WriteResult> {
    let storage = TxLogStorage::new(table_path, config)?;
    let current_versions = storage.list_versions().await?;
    let target_version = current_versions.last().map(|v| v + 1).unwrap_or(0);

    let written = version_file::write_version(&storage, target_version, &actions).await?;
    if written {
        cache::invalidate_table_cache(table_path);
        Ok(WriteResult {
            version: target_version,
            retries: 0,
            conflicted_versions: vec![],
        })
    } else {
        Ok(WriteResult {
            version: -1,
            retries: 0,
            conflicted_versions: vec![target_version],
        })
    }
}

/// List all version numbers in the transaction log.
pub async fn list_versions(
    table_path: &str,
    config: &DeltaStorageConfig,
) -> Result<Vec<i64>> {
    let storage = TxLogStorage::new(table_path, config)?;
    storage.list_versions().await
}

/// Read raw JSON-lines content from a specific version file.
pub async fn read_version_raw(
    table_path: &str,
    config: &DeltaStorageConfig,
    version: i64,
) -> Result<String> {
    let storage = TxLogStorage::new(table_path, config)?;
    let path = TxLogStorage::version_path(version);
    let data = storage.get(&path).await?;
    let text_bytes = super::compression::maybe_decompress(&data)?;
    String::from_utf8(text_bytes)
        .map_err(|e| super::error::TxLogError::Storage(anyhow::anyhow!("Invalid UTF-8: {}", e)))
}

/// Initialize a new table: write version 0 with Protocol + Metadata.
/// Fails if version 0 already exists.
pub async fn initialize_table(
    table_path: &str,
    config: &DeltaStorageConfig,
    protocol: ProtocolAction,
    metadata: MetadataAction,
) -> Result<()> {
    let storage = TxLogStorage::new(table_path, config)?;
    let actions = vec![
        Action::Protocol(protocol),
        Action::MetaData(metadata),
    ];
    let written = version_file::write_version(&storage, 0, &actions).await?;
    if !written {
        return Err(super::error::TxLogError::Storage(
            anyhow::anyhow!("Table already initialized: version 0 exists at {}", table_path)
        ));
    }
    cache::invalidate_table_cache(table_path);
    Ok(())
}

/// Create an Avro state checkpoint at the given version.
pub async fn write_checkpoint(
    table_path: &str,
    config: &DeltaStorageConfig,
    entries: Vec<FileEntry>,
    metadata: MetadataAction,
    protocol: ProtocolAction,
) -> Result<LastCheckpointInfo> {
    let storage = TxLogStorage::new(table_path, config)?;
    let version = storage.list_versions().await?
        .last().copied().unwrap_or(0);

    super::avro::state_writer::write_state_checkpoint(
        &storage,
        version,
        &entries,
        &protocol,
        &metadata,
    ).await
}
