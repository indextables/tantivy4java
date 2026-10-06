// txlog/distributed.rs - Distributable primitives (driver + executor)
//
// Follows the same 2-phase pattern as delta_reader/distributed.rs:
//   Phase 1 (driver): lightweight metadata reads → return paths to distribute
//   Phase 2 (executor): read individual manifests in parallel

use std::collections::HashMap;
use std::sync::Arc;

use crate::delta_reader::engine::DeltaStorageConfig;
use crate::debug_println;

use super::actions::{*, STATE_MANIFEST_FILENAME};
use super::cache;
use super::error::{TxLogError, Result};
use super::log_replay;
use super::schema_dedup;
use super::storage::TxLogStorage;
use super::version_file;

use futures::stream::{self, StreamExt};

// ============================================================================
// Concurrency helpers
// ============================================================================

/// Extract the maximum concurrent read limit from a config map.
/// Key: `max_concurrent_reads` (integer, default 32, 0 → default).
pub(crate) fn extract_max_concurrent(config_map: &HashMap<String, String>) -> usize {
    config_map.get("max_concurrent_reads")
        .and_then(|s| s.parse::<usize>().ok())
        .map(|n| if n == 0 { 32 } else { n })
        .unwrap_or(32)
}

/// Parse a cached metadata JSON string, handling both formats:
/// - Rust-written (bare):    `{"id":"...", "schemaString":"...", ...}`
/// - Scala-written (wrapped): `{"metaData": {"id":"...", ...}}`
///
/// Scala's `CheckpointCommand.scala` wraps the metadata before serialising:
/// ```scala
/// val metadataWrapper = Map("metaData" -> metadata)
/// ```
/// The `.ok()` fallback means a parse failure is silent, which previously
/// caused `getSchema()` to return `None` when no version files were available.
pub(crate) fn parse_metadata_json(json_str: &str) -> Option<MetadataAction> {
    // Fast path: bare format written by Rust
    if let Ok(m) = serde_json::from_str::<MetadataAction>(json_str) {
        return Some(m);
    }
    // Slow path: Scala-written envelope {"metaData": {...}}
    let value: serde_json::Value = serde_json::from_str(json_str).ok()?;
    let inner = value.get("metaData")?;
    serde_json::from_value(inner.clone()).ok()
}

/// Read multiple version files concurrently, silently ignoring all errors
/// (including NotFound). Returns (version, actions) pairs sorted ascending.
///
/// `max_concurrent` bounds the number of in-flight object-store GETs to prevent
/// S3/Azure rate-limit throttling on large catch-up reads.
async fn read_versions_concurrent(
    storage: &TxLogStorage,
    versions: impl IntoIterator<Item = i64>,
    max_concurrent: usize,
) -> Vec<(i64, Vec<Action>)> {
    let versions: Vec<i64> = versions.into_iter().collect();
    let futures = versions.into_iter().map(|v| {
        let storage = storage; // copy &TxLogStorage (references are Copy)
        async move {
            match version_file::read_version(storage, v).await {
                Ok(actions) => Some((v, actions)),
                Err(_) => None,
            }
        }
    });
    let mut results: Vec<(i64, Vec<Action>)> = stream::iter(futures)
        .buffer_unordered(max_concurrent)
        .filter_map(|opt| async move { opt })
        .collect()
        .await;
    results.sort_by_key(|(v, _)| *v);
    results
}

/// Run up to `max_concurrent` futures concurrently, collecting results in order.
///
/// Uses `buffer_unordered` for bounded parallelism (same pattern as
/// `read_versions_concurrent`). Results are returned in the original
/// iterator order, not completion order.
pub(crate) async fn join_all_bounded<F, T>(
    futures: Vec<F>,
    max_concurrent: usize,
) -> Result<Vec<T>>
where
    F: std::future::Future<Output = Result<T>>,
{
    // Wrap each future with its index so we can restore insertion order
    // after buffer_unordered processes them in completion order.
    let indexed = futures.into_iter().enumerate().map(|(i, f)| async move {
        f.await.map(|v| (i, v))
    });
    let mut results: Vec<(usize, T)> = stream::iter(indexed)
        .buffer_unordered(max_concurrent)
        .collect::<Vec<Result<(usize, T)>>>()
        .await
        .into_iter()
        .collect::<Result<Vec<(usize, T)>>>()?;
    results.sort_by_key(|(i, _)| *i);
    Ok(results.into_iter().map(|(_, v)| v).collect())
}

/// Returns true if the error represents a "not found" condition from
/// any object store backend (S3, Azure, local filesystem).
fn is_not_found_error(e: &TxLogError) -> bool {
    let s = e.to_string();
    s.contains("not found") || s.contains("NotFound")
        || s.contains("No such file") || s.contains("404")
}

// ============================================================================
// Driver-side primitive: get_txlog_snapshot_info
// ============================================================================

/// Information needed to distribute manifest reads.
#[derive(Debug, Clone)]
pub struct TxLogSnapshotInfo {
    pub checkpoint_version: i64,
    pub manifest_paths: Vec<ManifestPathInfo>,
    pub post_checkpoint_version_paths: Vec<String>,
    pub protocol: Option<ProtocolAction>,
    pub metadata: MetadataAction,
    pub state_dir: String,
    /// Tombstones from the checkpoint's StateManifest (paths of removed files).
    pub tombstones: Vec<String>,
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
    let cache_cfg = cache::CacheConfig::from_map(config_map);
    let max_concurrent = cache_cfg.max_concurrent_reads;

    // Check cache first
    if cache_cfg.enabled {
        let c = cache::get_or_create_cache(table_path, cache_cfg.clone());
        if let Some(cached_meta) = c.get_metadata() {
            if let Some(cached_cp) = c.get_last_checkpoint() {
                let state_dir = cached_cp.state_dir.clone()
                    .unwrap_or_else(|| TxLogStorage::state_dir_name(cached_cp.version));

                // Use per-table caches to avoid S3 calls on every query:
                // - state_manifests: immutable per checkpoint, cached indefinitely
                // - version_list: cached with version_ttl (default 5 min)
                // TxLogStorage::new() is cheap (no I/O), so always create it;
                // the S3 calls below are guarded by the cache checks.
                let storage = TxLogStorage::new(table_path, config)?;

                let state_manifest = if let Some(cached_sm) = c.get_state_manifest(&state_dir) {
                    debug_println!("📊 DISTRIBUTED: state_manifest cache hit for {}", state_dir);
                    cached_sm
                } else {
                    let m = super::avro::state_reader::read_state_manifest(&storage, &state_dir).await?;
                    c.put_state_manifest(&state_dir, m.clone());
                    m
                };

                let all_versions = if let Some(cached_vl) = c.get_version_list() {
                    debug_println!("📊 DISTRIBUTED: version_list cache hit for {}", table_path);
                    cached_vl
                } else {
                    // Probe for post-checkpoint versions using HEAD requests instead of LIST.
                    // For a stable table (no new commits) this costs 1 HEAD (~50ms) vs
                    // 1 LIST (~500–1000ms).
                    let versions = probe_versions_since(&storage, cached_cp.version).await?;
                    c.put_version_list(versions.clone());
                    versions
                };

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

                // If there are post-checkpoint versions, read them for potential
                // protocol/metadata overrides. This is critical for concurrent
                // metadata updates — the cached metadata may be stale.
                let (effective_protocol, mut effective_metadata) = if post_cp_paths.is_empty() {
                    (Some(cached_meta.0.clone()), cached_meta.1.clone())
                } else {
                    let post_cp_versions: Vec<i64> = all_versions.iter().copied()
                        .filter(|&v| v > cached_cp.version)
                        .collect();
                    let post_cp_actions = read_versions_concurrent(&storage, post_cp_versions, max_concurrent).await;
                    let (post_protocol, post_metadata) = log_replay::extract_metadata(&[], &post_cp_actions);
                    (
                        post_protocol.or(Some(cached_meta.0.clone())),
                        post_metadata.unwrap_or_else(|| cached_meta.1.clone()),
                    )
                };

                // Bug 2b fix: Merge schema_registry into effective_metadata.configuration
                // (same as non-cached path — see snapshot_from_checkpoint()).
                for (k, v) in &state_manifest.schema_registry {
                    effective_metadata.configuration.entry(k.clone()).or_insert_with(|| v.clone());
                }

                debug_println!("📊 DISTRIBUTED: snapshot_info from CACHE: checkpoint v{}, {} manifests, {} post-cp versions",
                    cached_cp.version, manifest_paths.len(), post_cp_paths.len());

                return Ok(TxLogSnapshotInfo {
                    checkpoint_version: cached_cp.version,
                    manifest_paths,
                    post_checkpoint_version_paths: post_cp_paths,
                    protocol: effective_protocol,
                    metadata: effective_metadata,
                    state_dir,
                    tombstones: state_manifest.tombstones.clone(),
                });
            }
        }
    }

    let storage = TxLogStorage::new(table_path, config)?;

    // Try to read _last_checkpoint — if it doesn't exist, fall back to version scanning
    let checkpoint_result = storage.get("_last_checkpoint").await;

    match checkpoint_result {
        Ok(checkpoint_data) => {
            // Normal path: checkpoint exists — verify version hint isn't stale
            let mut last_cp: LastCheckpointInfo = serde_json::from_slice(&checkpoint_data)?;
            let _t_verify = std::time::Instant::now();
            let verified_version = verify_checkpoint_version(&storage, last_cp.version).await;
            if crate::ffi_profiler::is_enabled() {
                crate::ffi_profiler::record(crate::ffi_profiler::Section::TxLogCheckpointVerify, _t_verify.elapsed().as_nanos() as u64);
            }
            if verified_version > last_cp.version {
                debug_println!("⚠️ DISTRIBUTED: _last_checkpoint stale: hint={}, actual={}",
                    last_cp.version, verified_version);
                last_cp.version = verified_version;
                last_cp.state_dir = Some(TxLogStorage::state_dir_name(verified_version));
            }
            snapshot_from_checkpoint(&storage, table_path, config_map, last_cp).await
        }
        Err(_) => {
            // Fallback path: no checkpoint — scan all version files
            debug_println!("📊 DISTRIBUTED: No _last_checkpoint found, falling back to version scan");
            snapshot_from_version_scan(&storage, table_path, config_map).await
        }
    }
}

/// Build snapshot info from an existing checkpoint.
async fn snapshot_from_checkpoint(
    storage: &TxLogStorage,
    table_path: &str,
    config_map: &HashMap<String, String>,
    last_cp: LastCheckpointInfo,
) -> Result<TxLogSnapshotInfo> {
    let max_concurrent = extract_max_concurrent(config_map);
    let cache_cfg = cache::CacheConfig::from_map(config_map);
    let state_dir = last_cp.state_dir.clone()
        .unwrap_or_else(|| TxLogStorage::state_dir_name(last_cp.version));

    // Read state manifest
    let state_manifest = super::avro::state_reader::read_state_manifest(storage, &state_dir).await?;

    // Find post-checkpoint version files by probing with HEAD requests instead of LIST.
    // Cost: O(n) HEADs where n = post-checkpoint commits + 1 miss; for a stable table
    // that is exactly 1 HEAD (~50ms) vs 1 LIST (~500–1000ms).
    let post_cp_versions = probe_versions_since(storage, last_cp.version).await?;
    let post_cp_paths: Vec<String> = post_cp_versions.iter()
        .map(|v| TxLogStorage::version_path(*v))
        .collect();
    let all_version_actions = read_versions_concurrent(storage, post_cp_versions.clone(), max_concurrent).await;

    // Source protocol/metadata from the checkpoint's cached JSON first,
    // then override with any updates from post-checkpoint version files.
    // Do NOT read version 0 — it may have been deleted by TRUNCATE TIME TRAVEL.
    let cp_protocol: Option<ProtocolAction> = state_manifest.protocol_json.as_ref()
        .and_then(|s| serde_json::from_str(s).ok());
    let cp_metadata: Option<MetadataAction> = state_manifest.metadata.as_ref()
        .and_then(|s| parse_metadata_json(s));

    // Check post-checkpoint versions for overrides
    let (post_protocol, post_metadata) = log_replay::extract_metadata(&[], &all_version_actions);

    // Post-checkpoint overrides checkpoint
    let protocol_opt = post_protocol.or(cp_protocol);
    let mut metadata = post_metadata.or(cp_metadata).unwrap_or_else(MetadataAction::empty);

    // Bug 1 fix: Backward-probing fallback — if the current checkpoint has null metadata
    // (schema_string is empty), walk earlier checkpoints to find one that has it.
    // Mirrors the Scala reader's getMetadata() fallback (OptimizedTransactionLog.scala:1321-1330),
    // which scans backwards through version files.  After TRUNCATE TIME TRAVEL the version files
    // are gone, but a previous checkpoint's state manifest may still carry the cached metadata.
    //
    // Issue #153: Batched concurrent probes — probe BATCH_SIZE versions at a time
    // instead of one-at-a-time to amortize S3/Azure round-trip latency.
    //
    // Semantic improvement over the original sequential code: the old code stopped
    // probing on the first missing state directory (Err), meaning a gap at version N
    // would prevent finding valid metadata at version N-1.  The batched version
    // tolerates gaps — it issues all probes in a batch concurrently and skips
    // individual failures, which is more resilient after partial GC or failed
    // checkpoint writes.
    if metadata.schema_string.is_empty() && last_cp.version > 0 {
        let _t_backward = std::time::Instant::now();
        const BACKWARD_BATCH: i64 = 4;
        let mut probe_ceiling = last_cp.version - 1;

        'outer: while metadata.schema_string.is_empty() && probe_ceiling >= 0 {
            let probe_floor = std::cmp::max(0, probe_ceiling - BACKWARD_BATCH + 1);
            // N.B. `.rev()` makes join_all return results in DESCENDING version order,
            // so the loop below processes newest-first and takes the first valid match.
            let probe_futs: Vec<_> = (probe_floor..=probe_ceiling).rev().map(|v| {
                let dir = TxLogStorage::state_dir_name(v);
                async move {
                    (v, super::avro::state_reader::read_state_manifest(storage, &dir).await)
                }
            }).collect();

            let results = futures::future::join_all(probe_futs).await;

            // Results are in descending version order (due to .rev() above).
            // Take the first (newest) version that carries valid metadata.
            for (v, result) in results {
                match result {
                    Ok(prev_manifest) => {
                        if let Some(ref md_str) = prev_manifest.metadata {
                            if let Some(m) = parse_metadata_json(md_str) {
                                if !m.schema_string.is_empty() {
                                    metadata = m;
                                    for (k, val) in &prev_manifest.schema_registry {
                                        metadata.configuration.entry(k.clone())
                                            .or_insert_with(|| val.clone());
                                    }
                                    break 'outer;
                                }
                            }
                        }
                    }
                    Err(_) => {
                        // State directory doesn't exist at this version — skip
                        // and continue checking earlier versions in the batch.
                        // This is intentionally more resilient than the old sequential
                        // code which stopped on the first gap.
                        debug_println!("⚠️ DISTRIBUTED: backward probe: state dir missing at v{}", v);
                    }
                }
            }

            if probe_floor == 0 { break; }
            probe_ceiling = probe_floor - 1;
        }
        if crate::ffi_profiler::is_enabled() {
            crate::ffi_profiler::record(crate::ffi_profiler::Section::TxLogBackwardProbe, _t_backward.elapsed().as_nanos() as u64);
        }
    }

    // Bug 2a fix: Merge the current checkpoint's schema_registry into metadata.configuration
    // so that doc_mapping_ref → doc_mapping_json resolution works in list_files.rs.
    // The Scala reader passes stateManifest.schemaRegistry separately to toAddActions()
    // (TransactionLogCheckpoint.scala:669); we merge it into the configuration map that
    // list_files.rs line 95 uses for restore_schemas().  Use or_insert_with to avoid
    // overwriting prefixed entries already present in metadata.configuration.
    for (k, v) in &state_manifest.schema_registry {
        metadata.configuration.entry(k.clone()).or_insert_with(|| v.clone());
    }

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

    // Populate cache (use v4 default for cache since it needs a concrete value).
    // Also cache state_manifest and version_list so the next query's cache-hit path
    // can skip both the S3 GET (_manifest.avro) and the S3 LIST (version files).
    if cache_cfg.enabled {
        let c = cache::get_or_create_cache(table_path, cache_cfg);
        let cache_protocol = protocol_opt.clone().unwrap_or_else(ProtocolAction::v4);
        c.put_metadata(cache_protocol, metadata.clone());
        c.put_last_checkpoint(last_cp);
        c.put_state_manifest(&state_dir, state_manifest.clone());
        c.put_version_list(post_cp_versions);
    }

    Ok(TxLogSnapshotInfo {
        checkpoint_version,
        manifest_paths,
        post_checkpoint_version_paths: post_cp_paths,
        protocol: protocol_opt,
        metadata,
        state_dir,
        tombstones: state_manifest.tombstones.clone(),
    })
}

/// Build snapshot info by scanning all version files (no checkpoint exists).
async fn snapshot_from_version_scan(
    storage: &TxLogStorage,
    table_path: &str,
    config_map: &HashMap<String, String>,
) -> Result<TxLogSnapshotInfo> {
    let max_concurrent = extract_max_concurrent(config_map);
    let cache_cfg = cache::CacheConfig::from_map(config_map);
    let all_versions = storage.list_versions().await?;

    if all_versions.is_empty() {
        return Err(TxLogError::NotInitialized {
            path: table_path.to_string(),
        });
    }

    // Read ALL version files to extract protocol/metadata — concurrently
    let all_version_actions = read_versions_concurrent(storage, all_versions.iter().copied(), max_concurrent).await;

    // Extract protocol/metadata from all versions
    let v0_actions = all_version_actions.iter()
        .find(|(v, _)| *v == 0)
        .map(|(_, a)| a.clone())
        .unwrap_or_default();
    let non_v0_actions: Vec<(i64, Vec<Action>)> = all_version_actions.iter()
        .filter(|(v, _)| *v > 0)
        .cloned()
        .collect();
    let (protocol_opt, metadata) = log_replay::extract_metadata(&v0_actions, &non_v0_actions);
    let metadata = metadata.unwrap_or_else(MetadataAction::empty);

    // All version files are "post-checkpoint" since there is no checkpoint
    let post_cp_paths: Vec<String> = all_versions.iter()
        .map(|v| TxLogStorage::version_path(*v))
        .collect();

    debug_println!("📊 DISTRIBUTED: snapshot_info from version scan: {} versions, no checkpoint",
        all_versions.len());

    // Populate cache (use v4 default for cache since it needs a concrete value)
    if cache_cfg.enabled {
        let c = cache::get_or_create_cache(table_path, cache_cfg);
        let cache_protocol = protocol_opt.clone().unwrap_or_else(ProtocolAction::v4);
        c.put_metadata(cache_protocol, metadata.clone());
    }

    Ok(TxLogSnapshotInfo {
        checkpoint_version: -1,
        manifest_paths: vec![],
        post_checkpoint_version_paths: post_cp_paths,
        protocol: protocol_opt,
        metadata,
        state_dir: String::new(),
        tombstones: vec![],
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

    // max_version reflects the highest version in the input list regardless of
    // whether files are present (matches original behavior: update before fetch check).
    let max_version: i64 = version_paths.iter()
        .filter_map(|path| {
            let name = path.rsplit('/').next().unwrap_or(path);
            name.trim_end_matches(".json").parse::<i64>().ok()
        })
        .max()
        .unwrap_or(0);

    // Fetch all version files concurrently. Tolerate NotFound (TRUNCATE/PURGE races);
    // propagate real I/O errors via try_join_all short-circuit.
    let fetch_futs: Vec<_> = version_paths.iter().map(|path| {
        let storage = &storage;
        async move {
            match storage.get(path).await {
                Ok(data) => Ok(Some(data)),
                Err(e) => {
                    if is_not_found_error(&e) {
                        debug_println!("⚠️ DISTRIBUTED: Version file {} not found, skipping (may have been deleted by TRUNCATE/PURGE)", path);
                        Ok(None)
                    } else {
                        Err(e)
                    }
                }
            }
        }
    }).collect();
    let fetch_results = futures::future::try_join_all(fetch_futs).await?;

    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut skips = Vec::new();

    // Process in original path order (try_join_all preserves input order).
    for (path, maybe_data) in version_paths.iter().zip(fetch_results.into_iter()) {
        let data = match maybe_data {
            Some(d) => d,
            None => continue,
        };
        let name = path.rsplit('/').next().unwrap_or(path);
        let version = name.trim_end_matches(".json")
            .parse::<i64>()
            .unwrap_or_else(|_| {
                crate::debug_println!("⚠️ TXLOG: Failed to parse version from path: {}", path);
                0
            });
        let actions = version_file::parse_version_file(&data)?;
        for action in actions {
            match action {
                Action::Add(mut add) => {
                    schema_dedup::restore_schemas_on_adds(
                        std::slice::from_mut(&mut add),
                        metadata_config,
                    );
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
    // On the first attempt we LIST to find the current version. On each conflict
    // the correct next candidate is last_conflict + 1 — no re-LIST needed.
    let mut next_version: Option<i64> = None;

    for attempt in 0..retry_config.max_attempts {
        // Determine target version: LIST on first attempt; derive from conflict thereafter.
        let target_version = match next_version {
            Some(v) => v,
            None => {
                let current_versions = storage.list_versions().await?;
                current_versions.last().map(|v| v + 1).unwrap_or(0)
            }
        };

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

        // Conflict: target_version is now taken; next candidate is target_version + 1.
        conflicted.push(target_version);
        next_version = Some(target_version + 1);
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

// ============================================================================
// Auto-checkpoint support (GAP-10)
// ============================================================================

/// Check if auto-checkpoint is enabled. Disabled when checkpoint_interval=0.
fn is_auto_checkpoint_enabled(config_map: &HashMap<String, String>) -> bool {
    let val_str = config_map.get("checkpoint_interval")
        .or_else(|| config_map.get("checkpoint.interval"));
    match val_str {
        Some(s) => {
            let interval: i64 = s.parse().unwrap_or(1);
            interval > 0
        }
        None => true, // enabled by default
    }
}

/// Create a checkpoint (state directory + _last_checkpoint) after every write.
///
/// Matches Scala behavior: uses incremental writes when a previous checkpoint exists
/// (reusing existing manifest references and accumulating tombstones), falling back to
/// compacted writes when no checkpoint exists or tombstone ratio is high.
/// Set checkpoint_interval=0 to disable entirely.
/// Failures are logged but never fail the write operation.
pub async fn maybe_auto_checkpoint(
    table_path: &str,
    config: &DeltaStorageConfig,
    config_map: &HashMap<String, String>,
    written_version: i64,
) {
    if written_version < 0 || !is_auto_checkpoint_enabled(config_map) {
        return;
    }
    let max_concurrent = extract_max_concurrent(config_map);

    debug_println!("📊 DISTRIBUTED: auto-checkpoint at version {}", written_version);

    let storage = match TxLogStorage::new(table_path, config) {
        Ok(s) => s,
        Err(e) => {
            debug_println!("⚠️ DISTRIBUTED: auto-checkpoint storage init failed: {}", e);
            return;
        }
    };

    // Try to read previous checkpoint
    let prev_checkpoint = read_previous_checkpoint(&storage).await;

    match prev_checkpoint {
        Some((cp_info, base_manifest, cp_protocol, cp_metadata)) => {
            // Previous checkpoint exists — try incremental write
            let cp_version = cp_info.version;

            // Read only post-checkpoint version files
            let all_versions = match storage.list_versions().await {
                Ok(v) => v,
                Err(e) => {
                    debug_println!("⚠️ DISTRIBUTED: list_versions failed: {}", e);
                    return;
                }
            };
            let post_cp_versions: Vec<i64> = all_versions.iter().copied()
                .filter(|&v| v > cp_version && v <= written_version)
                .collect();
            let post_cp_actions = read_versions_concurrent(&storage, post_cp_versions, max_concurrent).await;

            // Extract new adds and removes from post-checkpoint versions
            let mut new_adds: Vec<FileEntry> = Vec::new();
            let mut removed_paths: std::collections::HashSet<String> = std::collections::HashSet::new();
            for (version, actions) in &post_cp_actions {
                for action in actions {
                    match action {
                        Action::Add(add) => {
                            let timestamp = add.modification_time;
                            new_adds.push(FileEntry {
                                add: add.clone(),
                                added_at_version: *version,
                                added_at_timestamp: timestamp,
                            });
                        }
                        Action::Remove(r) => {
                            removed_paths.insert(r.path.clone());
                        }
                        _ => {}
                    }
                }
            }

            // Determine protocol/metadata: post-checkpoint overrides checkpoint
            let (version_protocol, version_metadata) = log_replay::extract_metadata(&[], &post_cp_actions);
            let protocol = version_protocol.or(cp_protocol).unwrap_or_else(ProtocolAction::v4);
            let metadata = version_metadata.or(cp_metadata).unwrap_or_else(MetadataAction::empty);

            // Decide: incremental, selective compaction, or full compaction
            use super::tombstone_distributor;

            if tombstone_distributor::needs_compaction(&base_manifest, removed_paths.len()) {
                // Compaction needed — try selective first, fall back to full
                let partition_columns = metadata.partition_columns.clone();
                let all_tombstones: std::collections::HashSet<String> = base_manifest.tombstones.iter()
                    .chain(removed_paths.iter())
                    .cloned()
                    .collect();

                let manifests_with_tombstones = tombstone_distributor::distribute_tombstones_to_manifests(
                    &base_manifest.manifests, &all_tombstones, &partition_columns,
                );
                let (keep, rewrite) = tombstone_distributor::selective_partition(
                    &manifests_with_tombstones, tombstone_distributor::COMPACTION_TOMBSTONE_THRESHOLD,
                );

                if tombstone_distributor::is_selective_compaction_beneficial(&keep, &rewrite) {
                    debug_println!("📊 DISTRIBUTED: selective compaction: keeping {} clean, rewriting {} dirty manifests",
                        keep.len(), rewrite.len());

                    // Read only dirty manifests, filter tombstones, write new
                    let state_dir = cp_info.state_dir.clone()
                        .unwrap_or_else(|| TxLogStorage::state_dir_name(cp_version));
                    let metadata_config = base_manifest.schema_registry.clone();
                    // Read dirty manifests concurrently (bounded); filter tombstones after.
                    let rewrite_futs: Vec<_> = rewrite.iter()
                        .map(|mi| super::avro::state_reader::read_single_manifest(
                            &storage, &state_dir, &mi.path, &metadata_config,
                        ))
                        .collect();
                    let rewrite_results = stream::iter(rewrite_futs)
                        .buffer_unordered(max_concurrent)
                        .collect::<Vec<super::error::Result<Vec<FileEntry>>>>().await;
                    let rewritten_entries: Vec<FileEntry> = match rewrite_results
                        .into_iter()
                        .collect::<super::error::Result<Vec<Vec<FileEntry>>>>()
                    {
                        Err(e) => {
                            debug_println!("⚠️ DISTRIBUTED: selective compaction aborted — dirty manifest read failed: {}", e);
                            return;
                        }
                        Ok(nested) => nested.into_iter()
                            .flat_map(|entries| entries.into_iter()
                                .filter(|e| !all_tombstones.contains(&e.add.path)))
                            .collect(),
                    };
                    // Combine: new adds (filtered against removes) + rewritten live entries
                    let mut all_live = rewritten_entries;
                    all_live.extend(new_adds.iter()
                        .filter(|e| !all_tombstones.contains(&e.add.path))
                        .cloned());

                    // Clean manifests are reused; write new manifest for rewritten + new
                    // For simplicity, do a compacted write with all live files from
                    // kept manifests + rewritten entries + new adds.
                    // A full selective compaction would reuse kept manifest refs, but
                    // that requires state_writer changes beyond scope here.
                    // Instead, read kept manifest entries too for a clean compacted write.
                    // Read clean manifests concurrently (bounded).
                    let keep_futs: Vec<_> = keep.iter()
                        .map(|mi| super::avro::state_reader::read_single_manifest(
                            &storage, &state_dir, &mi.path, &metadata_config,
                        ))
                        .collect();
                    let keep_results = stream::iter(keep_futs)
                        .buffer_unordered(max_concurrent)
                        .collect::<Vec<super::error::Result<Vec<FileEntry>>>>().await;
                    let keep_live: Vec<FileEntry> = match keep_results
                        .into_iter()
                        .collect::<super::error::Result<Vec<Vec<FileEntry>>>>()
                    {
                        Err(e) => {
                            debug_println!("⚠️ DISTRIBUTED: selective compaction aborted — clean manifest read failed: {}", e);
                            return;
                        }
                        Ok(nested) => nested.into_iter()
                            .flat_map(|entries| entries.into_iter()
                                .filter(|e| !all_tombstones.contains(&e.add.path)))
                            .collect(),
                    };
                    all_live.extend(keep_live);
                    all_live.sort_by(|a, b| a.add.path.cmp(&b.add.path));

                    match write_checkpoint_at_version(table_path, config, all_live, metadata, protocol, written_version).await {
                        Ok(info) => debug_println!("✅ DISTRIBUTED: selective compaction at v{}, {} files", info.version, info.num_files),
                        Err(e) => debug_println!("⚠️ DISTRIBUTED: selective compaction failed (non-fatal): {}", e),
                    }
                } else {
                    // Full compaction — read everything, apply tombstones, replay, write fresh
                    debug_println!("📊 DISTRIBUTED: full compaction (selective not beneficial)");
                    let state_dir = cp_info.state_dir.unwrap_or_else(|| TxLogStorage::state_dir_name(cp_version));
                    let metadata_config = base_manifest.schema_registry.clone();
                    // Read all checkpoint manifests concurrently (bounded).
                    let cp_futs: Vec<_> = base_manifest.manifests.iter()
                        .map(|mi| super::avro::state_reader::read_single_manifest(
                            &storage, &state_dir, &mi.path, &metadata_config,
                        ))
                        .collect();
                    let cp_results = stream::iter(cp_futs)
                        .buffer_unordered(max_concurrent)
                        .collect::<Vec<super::error::Result<Vec<FileEntry>>>>().await;
                    let mut cp_entries: Vec<FileEntry> = match cp_results
                        .into_iter()
                        .collect::<super::error::Result<Vec<Vec<FileEntry>>>>()
                    {
                        Err(e) => {
                            debug_println!("⚠️ DISTRIBUTED: full compaction aborted — manifest read failed: {}", e);
                            return;
                        }
                        Ok(nested) => nested.into_iter().flatten().collect(),
                    };
                    // Filter out tombstoned entries from base checkpoint before replay
                    if !base_manifest.tombstones.is_empty() {
                        let tombstone_set: std::collections::HashSet<&str> =
                            base_manifest.tombstones.iter().map(|s| s.as_str()).collect();
                        let before = cp_entries.len();
                        cp_entries.retain(|e| !tombstone_set.contains(e.add.path.as_str()));
                        debug_println!("📖 DISTRIBUTED: Applied {} base tombstones during compaction, {} → {} entries",
                            base_manifest.tombstones.len(), before, cp_entries.len());
                    }
                    let replay_result = log_replay::replay(cp_entries, post_cp_actions);
                    match write_checkpoint_at_version(table_path, config, replay_result.files, metadata, protocol, written_version).await {
                        Ok(info) => debug_println!("✅ DISTRIBUTED: full compaction at v{}, {} files", info.version, info.num_files),
                        Err(e) => debug_println!("⚠️ DISTRIBUTED: full compaction failed (non-fatal): {}", e),
                    }
                }
            } else {
                // Low tombstone ratio — incremental write (reuse existing manifests)
                match super::avro::state_writer::write_incremental_state_checkpoint(
                    &storage, written_version, &base_manifest, &new_adds, &removed_paths, &protocol, &metadata,
                ).await {
                    Ok(info) => {
                        cache::invalidate_table_cache(table_path);
                        debug_println!("✅ DISTRIBUTED: incremental checkpoint at v{}, {} files", info.version, info.num_files);
                    }
                    Err(e) => debug_println!("⚠️ DISTRIBUTED: incremental checkpoint failed (non-fatal): {}", e),
                }
            }
        }
        None => {
            // No previous checkpoint — full replay and compacted write
            debug_println!("📊 DISTRIBUTED: no previous checkpoint, doing full replay");
            let all_versions = match storage.list_versions().await {
                Ok(v) => v,
                Err(e) => {
                    debug_println!("⚠️ DISTRIBUTED: list_versions failed: {}", e);
                    return;
                }
            };
            let versions_to_read: Vec<i64> = all_versions.iter().copied()
                .filter(|&v| v <= written_version)
                .collect();
            let versioned_actions = read_versions_concurrent(&storage, versions_to_read, max_concurrent).await;
            let v0_actions = versioned_actions.iter()
                .find(|(v, _)| *v == 0)
                .map(|(_, a)| a.clone())
                .unwrap_or_default();
            let non_v0: Vec<(i64, Vec<Action>)> = versioned_actions.iter()
                .filter(|(v, _)| *v > 0)
                .cloned()
                .collect();
            let (protocol_opt, metadata_opt) = log_replay::extract_metadata(&v0_actions, &non_v0);
            let protocol = protocol_opt.unwrap_or_else(ProtocolAction::v4);
            let metadata = metadata_opt.unwrap_or_else(MetadataAction::empty);
            let replay_result = log_replay::replay(vec![], versioned_actions);

            match write_checkpoint_at_version(table_path, config, replay_result.files, metadata, protocol, written_version).await {
                Ok(info) => debug_println!("✅ DISTRIBUTED: initial checkpoint at v{}, {} files", info.version, info.num_files),
                Err(e) => debug_println!("⚠️ DISTRIBUTED: initial checkpoint failed (non-fatal): {}", e),
            }
        }
    }
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

    match super::avro::state_writer::write_state_checkpoint(
        &storage, version, &entries, &protocol, &metadata,
    ).await {
        Ok(result) => {
            cache::invalidate_table_cache(table_path);
            Ok(result)
        }
        Err(super::error::TxLogError::AlreadyExists(_)) => {
            // Checkpoint already exists at this version (e.g., auto-checkpoint wrote it).
            // Read the existing state manifest directly — _last_checkpoint may not yet
            // be updated by the winning writer (TOCTOU window).
            debug_println!("📊 DISTRIBUTED: checkpoint already exists at v{}, reading existing", version);
            read_existing_checkpoint_info(&storage, version).await
        }
        Err(e) => Err(e),
    }
}

/// Create an Avro state checkpoint at a specific version.
/// Used by auto-checkpoint to avoid a race where list_versions().last() advances
/// past the version that was actually replayed.
pub async fn write_checkpoint_at_version(
    table_path: &str,
    config: &DeltaStorageConfig,
    entries: Vec<FileEntry>,
    metadata: MetadataAction,
    protocol: ProtocolAction,
    version: i64,
) -> Result<LastCheckpointInfo> {
    let storage = TxLogStorage::new(table_path, config)?;

    match super::avro::state_writer::write_state_checkpoint(
        &storage, version, &entries, &protocol, &metadata,
    ).await {
        Ok(result) => {
            cache::invalidate_table_cache(table_path);
            Ok(result)
        }
        Err(super::error::TxLogError::AlreadyExists(_)) => {
            // Checkpoint already exists — concurrent or auto-checkpoint wrote it first.
            debug_println!("📊 DISTRIBUTED: checkpoint already exists at v{}, reading existing", version);
            read_existing_checkpoint_info(&storage, version).await
        }
        Err(e) => Err(e),
    }
}

/// Read checkpoint info from an existing state manifest at a given version.
///
/// Used when our checkpoint write was rejected (AlreadyExists). Reads the winning
/// writer's `_manifest.avro` directly rather than `_last_checkpoint`, because
/// `_last_checkpoint` may not yet be updated (TOCTOU window between the winning
/// writer's `_manifest.avro` write and its `_last_checkpoint` update).
async fn read_existing_checkpoint_info(
    storage: &TxLogStorage,
    version: i64,
) -> Result<LastCheckpointInfo> {
    let state_dir = TxLogStorage::state_dir_name(version);
    let manifest = super::avro::state_reader::read_state_manifest(storage, &state_dir).await?;

    Ok(LastCheckpointInfo {
        version,
        size: manifest.total_file_count,
        size_in_bytes: manifest.total_bytes,
        num_files: manifest.total_file_count,
        format: Some(manifest.format),
        state_dir: Some(state_dir),
        created_time: manifest.created_time,
        parts: None,
        checkpoint_id: None,
    })
}

// ============================================================================
// Post-checkpoint version probing
// ============================================================================

/// Find version JSON files (incremental commits) newer than `since_version` by
/// probing with HEAD requests rather than an S3 LIST.
///
/// **Why probe instead of LIST?**
/// - S3 LIST scans the entire `_delta_log/` prefix → ~500–1000 ms
/// - HEAD per file → ~30–50 ms each
/// - For the common case (no commits since checkpoint), cost is 1 HEAD → ~50 ms
///
/// Uses batched concurrent probes (issue #153): issues BATCH_SIZE HEAD requests
/// at a time.  If every probe in a batch hits, we advance and issue another
/// batch.  The first batch with any miss tells us the frontier.
///
/// Falls back to a full LIST once the probe count exceeds `MAX_PROBE` (100) to
/// avoid issuing thousands of HEADs on a very stale table.
pub(crate) async fn probe_versions_since(
    storage: &TxLogStorage,
    since_version: i64,
) -> Result<Vec<i64>> {
    const MAX_PROBE: i64 = 100;
    const BATCH_SIZE: i64 = 8;

    let mut versions = Vec::new();
    let mut v = since_version + 1;

    loop {
        let probed_so_far = v - (since_version + 1);
        if probed_so_far >= MAX_PROBE {
            // Fell off the fast path — too many versions; switch to a full LIST
            debug_println!("⚠️ DISTRIBUTED: probe_versions_since hit limit at v{}, falling back to list_versions", v);
            let all = storage.list_versions().await?;
            let remaining: Vec<i64> = all.into_iter().filter(|&x| x >= v).collect();
            versions.extend(remaining);
            break;
        }

        let batch_end = std::cmp::min(BATCH_SIZE, MAX_PROBE - probed_so_far);
        let probe_futs: Vec<_> = (0..batch_end).map(|offset| {
            let probe_v = v + offset;
            let path = TxLogStorage::version_path(probe_v);
            async move {
                (probe_v, storage.exists(&path).await.unwrap_or(false))
            }
        }).collect();

        let results = futures::future::join_all(probe_futs).await;

        // Results are in ascending version order (matching 0..batch_end iterator).
        // Find the highest contiguous version that exists.
        let mut any_hit = false;
        for (probe_v, exists) in &results {
            if *exists && *probe_v == v {
                versions.push(v);
                v += 1;
                any_hit = true;
            } else {
                break;
            }
        }

        if !any_hit {
            break; // No newer version exists
        }

        // If not all probes in the batch hit, we found the frontier
        if results.iter().any(|(_, exists)| !exists) {
            break;
        }
    }

    Ok(versions)
}

// ============================================================================
// Stale checkpoint detection
// ============================================================================

/// Verify a checkpoint version hint by probing for newer state directories.
///
/// Uses batched concurrent probes (issue #153): instead of sequential HEAD
/// requests, issues BATCH_SIZE probes at a time.  Each batch checks versions
/// [base+1, base+BATCH_SIZE] concurrently via HEAD requests (`storage.exists`).
/// If every probe in a batch hits, we advance and issue another batch.
/// The first batch with any miss tells us the frontier — the highest
/// contiguous version is the answer.
///
/// Batch size of 4 balances between:
/// - Common case (0-1 stale): 3-4 wasted HEAD probes (cheap)
/// - Stale case (many versions behind): 4× fewer round-trips than sequential
async fn verify_checkpoint_version(
    storage: &TxLogStorage,
    hint_version: i64,
) -> i64 {
    const MAX_VERSION_PROBE: i64 = 1000;
    const BATCH_SIZE: i64 = 4;

    let mut version = hint_version;
    let mut total_probed: i64 = 0;

    loop {
        if total_probed >= MAX_VERSION_PROBE {
            debug_println!("⚠️ DISTRIBUTED: verify_checkpoint_version hit probe limit ({}) at v{}", MAX_VERSION_PROBE, version);
            break;
        }

        let batch_end = std::cmp::min(BATCH_SIZE, MAX_VERSION_PROBE - total_probed);
        let probe_futs: Vec<_> = (1..=batch_end).map(|offset| {
            let v = version + offset;
            let next_dir = TxLogStorage::state_dir_name(v);
            let next_manifest = format!("{}/{}", next_dir, STATE_MANIFEST_FILENAME);
            async move {
                // Use HEAD (exists) instead of GET to avoid downloading manifest bodies.
                (v, storage.exists(&next_manifest).await.unwrap_or(false))
            }
        }).collect();

        let results = futures::future::join_all(probe_futs).await;

        // Results are in ascending version order (matching the (1..=batch_end) iterator).
        // Find the highest contiguous version that exists.
        let mut advanced = false;
        for (v, exists) in &results {
            if *exists && *v == version + 1 {
                version = *v;
                total_probed += 1;
                advanced = true;
                debug_println!("📊 DISTRIBUTED: _last_checkpoint regression: found state at v{}", version);
            } else {
                break;
            }
        }

        if !advanced {
            break; // No newer version exists
        }

        // If not all probes in the batch hit, we found the frontier
        if results.iter().any(|(_, exists)| !exists) {
            break;
        }
    }
    version
}

/// Read the previous checkpoint's metadata and StateManifest.
/// Returns None if no checkpoint exists or it can't be read.
async fn read_previous_checkpoint(
    storage: &TxLogStorage,
) -> Option<(LastCheckpointInfo, StateManifest, Option<ProtocolAction>, Option<MetadataAction>)> {
    let cp_data = storage.get("_last_checkpoint").await.ok()?;
    let mut cp_info: LastCheckpointInfo = serde_json::from_slice(&cp_data).ok()?;

    // Verify checkpoint isn't stale
    let verified = verify_checkpoint_version(storage, cp_info.version).await;
    if verified > cp_info.version {
        cp_info.version = verified;
        cp_info.state_dir = Some(TxLogStorage::state_dir_name(verified));
    }

    let state_dir = cp_info.state_dir.clone()
        .unwrap_or_else(|| TxLogStorage::state_dir_name(cp_info.version));
    let manifest_path = format!("{}/{}", state_dir, STATE_MANIFEST_FILENAME);
    let manifest_data = storage.get(&manifest_path).await.ok()?;
    let manifest = super::avro::state_reader::parse_state_manifest(&manifest_data).ok()?;

    let protocol: Option<ProtocolAction> = manifest.protocol_json.as_ref()
        .and_then(|s| serde_json::from_str(s).ok());
    let metadata: Option<MetadataAction> = manifest.metadata.as_ref()
        .and_then(|s| parse_metadata_json(s));

    Some((cp_info, manifest, protocol, metadata))
}
