// txlog/list_files.rs - Native list files orchestration (FR1)
//
// Replaces the 8-step JVM pipeline with a single native call:
//   1. get_txlog_snapshot_info_with_cache
//   2. prune_manifests (manifest-level)
//   3. read_manifest for each surviving manifest
//   4. read_post_checkpoint_changes → merge
//   5. partition filter (file-level)
//   6. data skip by stats (file-level)
//   7. cooldown filter (skip actions)
//   8. schema restore + v2 Arrow FFI export

use std::collections::{HashMap, HashSet};

use std::sync::Arc;

use crate::delta_reader::engine::DeltaStorageConfig;
use crate::split_cache_manager::GlobalSplitCacheManager;

use super::actions::{FileEntry, ManifestInfo};
use super::arrow_ffi;
use super::distributed;
use super::error::{TxLogError, Result};
use super::partition_pruning::{self, PartitionFilter};
use super::schema_dedup;

/// Result returned by list_files_arrow_ffi.
#[derive(Debug, Clone)]
pub struct ListFilesResult {
    /// Number of rows exported to Arrow FFI.
    pub num_rows: i64,
    /// Number of columns in the Arrow schema.
    pub num_columns: i32,
    /// JSON schema of the exported Arrow RecordBatch.
    pub schema_json: String,
    /// Partition column names extracted from metadata.
    pub partition_columns: Vec<String>,
    /// Protocol JSON (from txlog metadata).
    pub protocol_json: String,
    /// Metadata config JSON (schema registry, etc.).
    pub metadata_config_json: Option<String>,
    /// Metrics for observability.
    pub total_files_before_filtering: i64,
    pub files_after_partition_pruning: i64,
    pub files_after_data_skipping: i64,
    pub files_after_cooldown_filtering: i64,
    pub manifests_total: i64,
    pub manifests_pruned: i64,
}

/// Native list files orchestration: snapshot → filter → Arrow FFI export.
///
/// # Safety
///
/// The caller must ensure that `array_addrs` and `schema_addrs` point to valid,
/// writable FFI memory locations.
pub async unsafe fn list_files_arrow_ffi(
    table_path: &str,
    config: &DeltaStorageConfig,
    config_map: &HashMap<String, String>,
    partition_filter_json: Option<&str>,
    data_filter_json: Option<&str>,
    exclude_cooldown_files: bool,
    include_stats: bool,
    array_addrs: &[i64],
    schema_addrs: &[i64],
    cache_manager: Option<&Arc<GlobalSplitCacheManager>>,
) -> Result<ListFilesResult> {
    // 1. Get snapshot info (cached by table_path + TTL)
    let _t_snapshot = std::time::Instant::now();
    let snapshot = distributed::get_txlog_snapshot_info_with_cache(
        table_path, config, config_map,
    ).await?;
    if crate::ffi_profiler::is_enabled() {
        crate::ffi_profiler::record(crate::ffi_profiler::Section::TxLogSnapshot, _t_snapshot.elapsed().as_nanos() as u64);
    }

    let protocol_json = snapshot.protocol
        .as_ref()
        .map(|p| serde_json::to_string(p).unwrap_or_default())
        .unwrap_or_default();

    // Extract partition columns from metadata
    let partition_columns: Vec<String> = snapshot.metadata.partition_columns.clone();

    // Extract metadata_config from metadata
    let metadata_config: HashMap<String, String> = snapshot.metadata.configuration.clone();
    let metadata_config_json = if metadata_config.is_empty() {
        None
    } else {
        serde_json::to_string(&metadata_config).ok()
    };

    // Parse filters — return error for malformed JSON instead of silently ignoring
    let partition_filter: Option<PartitionFilter> = match partition_filter_json {
        Some(s) if !s.is_empty() => Some(serde_json::from_str(s)
            .map_err(|e| TxLogError::Serde(format!("Invalid partition filter JSON: {}", e)))?),
        _ => None,
    };
    let data_filter: Option<PartitionFilter> = match data_filter_json {
        Some(s) if !s.is_empty() => Some(serde_json::from_str(s)
            .map_err(|e| TxLogError::Serde(format!("Invalid data filter JSON: {}", e)))?),
        _ => None,
    };

    // Extract field types from the table schema for type-aware data skipping.
    // The Rust layer owns the schema — field types are ALWAYS derived from
    // MetadataAction.schema_string, never passed from the JVM.
    crate::debug_println!("LIST_FILES: schema_string length={}, first 200 chars: {}",
        snapshot.metadata.schema_string.len(),
        &snapshot.metadata.schema_string[..std::cmp::min(200, snapshot.metadata.schema_string.len())]);
    let field_types = extract_field_types_from_schema(&snapshot.metadata.schema_string);
    crate::debug_println!("LIST_FILES: extracted field_types: {:?}", field_types);

    // Extract session timezone offset for timestamp data skipping.
    // Passed from JVM as "session.timezone.offset.seconds" in the config map.
    let tz_offset_secs: Option<i32> = config_map.get("session.timezone.offset.seconds")
        .and_then(|s| s.parse::<i32>().ok());

    // 2. Convert ManifestPathInfo → ManifestInfo for pruning
    let manifest_infos: Vec<ManifestInfo> = snapshot.manifest_paths.iter().map(|mp| {
        ManifestInfo {
            path: mp.path.clone(),
            file_count: mp.file_count,
            partition_bounds: mp.partition_bounds.clone(),
            min_added_at_version: 0,
            max_added_at_version: 0,
            tombstone_count: 0,
            live_entry_count: -1,
        }
    }).collect();

    let manifests_total = manifest_infos.len() as i64;

    // Manifest-level pruning with partition filter
    let _t_prune = std::time::Instant::now();
    let surviving_manifests = if let Some(ref pf) = partition_filter {
        partition_pruning::prune_manifests(&manifest_infos, std::slice::from_ref(pf))
    } else {
        manifest_infos.iter().collect()
    };
    let manifests_pruned = manifests_total - surviving_manifests.len() as i64;
    if crate::ffi_profiler::is_enabled() {
        crate::ffi_profiler::record(crate::ffi_profiler::Section::TxLogManifestPrune, _t_prune.elapsed().as_nanos() as u64);
    }

    // 3. Read manifests (executors would do this in Spark)
    let _t_read = std::time::Instant::now();
    let mut checkpoint_entries: Vec<FileEntry> = Vec::new();
    for manifest in &surviving_manifests {
        let entries = distributed::read_manifest(
            table_path, config, &snapshot.state_dir, &manifest.path, &metadata_config,
        ).await?;
        checkpoint_entries.extend(entries);
    }

    // 4. Read post-checkpoint changes and merge
    let changes = distributed::read_post_checkpoint_changes(
        table_path, config, &snapshot.post_checkpoint_version_paths, &metadata_config,
    ).await?;

    // Apply adds/removes to checkpoint state
    let mut file_map: HashMap<String, FileEntry> = HashMap::new();
    for entry in checkpoint_entries {
        file_map.insert(entry.add.path.clone(), entry);
    }
    for entry in changes.added_files {
        file_map.insert(entry.add.path.clone(), entry);
    }
    for path in &changes.removed_paths {
        file_map.remove(path);
    }

    let mut all_files: Vec<FileEntry> = file_map.into_values().collect();
    all_files.sort_by(|a, b| a.add.path.cmp(&b.add.path));

    if crate::ffi_profiler::is_enabled() {
        crate::ffi_profiler::record(crate::ffi_profiler::Section::TxLogManifestRead, _t_read.elapsed().as_nanos() as u64);
    }

    let total_files_before_filtering = all_files.len() as i64;

    // 5. Partition filter (file-level)
    let _t_filter = std::time::Instant::now();
    if let Some(ref pf) = partition_filter {
        all_files.retain(|entry| pf.evaluate(&entry.add.partition_values));
    }
    let files_after_partition_pruning = all_files.len() as i64;
    if crate::ffi_profiler::is_enabled() {
        crate::ffi_profiler::record(crate::ffi_profiler::Section::TxLogPartitionFilter, _t_filter.elapsed().as_nanos() as u64);
    }

    // 6. Data skipping by min/max stats
    let _t_skip = std::time::Instant::now();
    if let Some(ref df) = data_filter {
        all_files.retain(|entry| {
            let min = entry.add.min_values.as_ref();
            let max = entry.add.max_values.as_ref();
            match (min, max) {
                (Some(min_vals), Some(max_vals)) => {
                    !df.can_skip_by_stats_typed(min_vals, max_vals, &field_types, tz_offset_secs)
                }
                _ => true, // No stats → can't skip
            }
        });
    }
    let files_after_data_skipping = all_files.len() as i64;
    if crate::ffi_profiler::is_enabled() {
        crate::ffi_profiler::record(crate::ffi_profiler::Section::TxLogDataSkip, _t_skip.elapsed().as_nanos() as u64);
    }

    // 7. Cooldown filter (exclude files with active skip actions)
    if exclude_cooldown_files && !changes.skip_actions.is_empty() {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| std::cmp::min(d.as_millis(), i64::MAX as u128) as i64)
            .unwrap_or(0);

        let cooldown_paths: HashSet<&str> = changes.skip_actions.iter()
            .filter(|skip| {
                skip.retry_after
                    .map_or(false, |retry_after| now_ms < retry_after)
            })
            .map(|skip| skip.path.as_str())
            .collect();

        if !cooldown_paths.is_empty() {
            all_files.retain(|entry| !cooldown_paths.contains(entry.add.path.as_str()));
        }
    }
    let files_after_cooldown_filtering = all_files.len() as i64;

    // 8. Restore doc_mapping schemas from schema registry
    schema_dedup::restore_schemas(&mut all_files, &metadata_config);

    // FR4: Cache per-file field statistics on the split cache manager for
    // range filter elimination during SplitSearcher creation.
    if let Some(cm) = cache_manager {
        cm.put_all_file_field_stats(&all_files);
    }

    // 9. Export via Arrow FFI
    let _t_export = std::time::Instant::now();
    let num_cols = arrow_ffi::column_count(&partition_columns, include_stats);

    // schemaJson is the TABLE's data schema (from MetadataAction.schema_string),
    // NOT the Arrow column schema of the listing. The JVM needs this for StructType construction.
    let schema_json = snapshot.metadata.schema_string.clone();

    let num_rows = arrow_ffi::export_file_entries_ffi(
        &all_files,
        &partition_columns,
        include_stats,
        array_addrs,
        schema_addrs,
    )? as i64;
    if crate::ffi_profiler::is_enabled() {
        crate::ffi_profiler::record(crate::ffi_profiler::Section::TxLogArrowExport, _t_export.elapsed().as_nanos() as u64);
    }

    Ok(ListFilesResult {
        num_rows,
        num_columns: num_cols as i32,
        schema_json,
        partition_columns,
        protocol_json,
        metadata_config_json,
        total_files_before_filtering,
        files_after_partition_pruning,
        files_after_data_skipping,
        files_after_cooldown_filtering,
        manifests_total,
        manifests_pruned,
    })
}

/// Extract field name → type mappings from a Spark StructType JSON schema.
///
/// The schema_string format is:
/// ```json
/// {"type":"struct","fields":[{"name":"id","type":"long","nullable":true,"metadata":{}},
///   {"name":"event_date","type":"date","nullable":true,"metadata":{}}]}
/// ```
///
/// Returns a map like {"id":"long","event_date":"date","ts":"timestamp"}.
/// Only date and timestamp types matter for typed data skipping — all other types
/// use the default numeric-aware comparison which already works correctly.
fn extract_field_types_from_schema(schema_string: &str) -> HashMap<String, String> {
    let mut result = HashMap::new();
    if schema_string.is_empty() {
        return result;
    }

    let schema: serde_json::Value = match serde_json::from_str(schema_string) {
        Ok(v) => v,
        Err(_) => return result,
    };

    if let Some(fields) = schema.get("fields").and_then(|f| f.as_array()) {
        for field in fields {
            let name = field.get("name").and_then(|n| n.as_str());
            let ftype = field.get("type").and_then(|t| t.as_str());
            if let (Some(name), Some(ftype)) = (name, ftype) {
                // Only store types that need special handling
                match ftype {
                    "date" | "timestamp" | "timestamp_ntz" => {
                        result.insert(name.to_string(), ftype.to_string());
                    }
                    _ => {} // numeric/string types handled by default compare_values
                }
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_field_types_from_spark_schema() {
        let schema = r#"{"type":"struct","fields":[{"name":"id","type":"string","nullable":true,"metadata":{}},{"name":"data_date","type":"date","nullable":true,"metadata":{}},{"name":"ts","type":"timestamp","nullable":true,"metadata":{}},{"name":"seq_num","type":"integer","nullable":true,"metadata":{}}]}"#;
        let types = extract_field_types_from_schema(schema);
        assert_eq!(types.get("data_date"), Some(&"date".to_string()), "data_date should be date");
        assert_eq!(types.get("ts"), Some(&"timestamp".to_string()), "ts should be timestamp");
        assert!(types.get("id").is_none(), "string type should not be included");
        assert!(types.get("seq_num").is_none(), "integer type should not be included");
        assert_eq!(types.len(), 2);
    }

    #[test]
    fn test_extract_empty_schema() {
        let types = extract_field_types_from_schema("");
        assert!(types.is_empty());
    }

    #[test]
    fn test_extract_invalid_schema() {
        let types = extract_field_types_from_schema("not json");
        assert!(types.is_empty());
    }
}
