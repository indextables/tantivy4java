// delta_reader/distributed.rs - Distributed table scanning primitives for Delta Lake
//
// Provides building blocks for distributed Delta table scanning that avoid OOM
// by splitting the work into lightweight driver-side metadata discovery and
// parallelizable executor-side checkpoint part reading.
//
// Three primitives:
//   1. get_snapshot_info()         — Driver: reads _last_checkpoint + lists commits
//   2. read_checkpoint_part()      — Executor: reads ONE checkpoint parquet part
//   3. read_post_checkpoint_changes() — Driver: reads post-checkpoint JSON commits

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::Result;
use arrow_array::Array;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use url::Url;

use crate::debug_println;
use super::engine::{DeltaStorageConfig, create_object_store};
use super::scan::{DeltaFileEntry, normalize_url};

// ─── Data structures ────────────────────────────────────────────────────────

/// Lightweight snapshot metadata returned by get_snapshot_info().
/// Contains only paths — does NOT read checkpoint contents.
#[derive(Debug, Clone)]
pub struct DeltaSnapshotInfo {
    /// Checkpoint version
    pub version: u64,
    /// Schema JSON from the first checkpoint part's metaData row
    pub schema_json: String,
    /// Partition column names from the metaData row (translated to logical names)
    pub partition_columns: Vec<String>,
    /// Paths to checkpoint parquet parts (relative to _delta_log/)
    pub checkpoint_part_paths: Vec<String>,
    /// Paths to post-checkpoint commit JSON files
    pub commit_file_paths: Vec<String>,
    /// Total number of add files recorded in _last_checkpoint (if available)
    pub num_add_files: Option<u64>,
    /// Physical column name → logical column name mapping for Delta column mapping mode.
    /// Empty if the table does not use column mapping.
    pub column_mapping: HashMap<String, String>,
}

/// Post-checkpoint add/remove changes from JSON commit files.
#[derive(Debug, Clone)]
pub struct DeltaLogChanges {
    /// Files added after the checkpoint
    pub added_files: Vec<DeltaFileEntry>,
    /// Paths removed after the checkpoint
    pub removed_paths: HashSet<String>,
}

/// Parsed _last_checkpoint JSON content.
#[derive(Debug, Clone)]
struct LastCheckpointInfo {
    version: u64,
    size: u64,
    parts: Option<u64>,
    num_of_add_files: Option<u64>,
}

// ─── Public API ─────────────────────────────────────────────────────────────

/// Driver-side: Get lightweight snapshot metadata for a Delta table.
///
/// Reads _last_checkpoint (one small GET), lists commit files, and reads
/// schema/partitionColumns from the first checkpoint part's metaData row.
/// Returns paths only — does NOT read checkpoint file contents.
pub fn get_snapshot_info(
    url_str: &str,
    config: &DeltaStorageConfig,
) -> Result<DeltaSnapshotInfo> {
    debug_println!("🔧 DELTA_DIST: get_snapshot_info url={}", url_str);

    let url = normalize_url(url_str)?;
    let store = create_object_store(&url, config)?;
    let log_prefix = delta_log_prefix(&url);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    rt.block_on(async {
        // Step 1: Read _last_checkpoint
        let checkpoint_info = read_last_checkpoint(&store, &log_prefix).await?;
        let checkpoint_info = checkpoint_info.ok_or_else(|| {
            anyhow::anyhow!(
                "No _last_checkpoint found for table {}. \
                 Use DeltaTableReader.listFiles() for tables without checkpoints.",
                url_str
            )
        })?;

        debug_println!(
            "🔧 DELTA_DIST: Checkpoint version={}, parts={:?}",
            checkpoint_info.version,
            checkpoint_info.parts
        );

        // Step 2: Construct checkpoint part paths
        let checkpoint_part_paths =
            construct_checkpoint_paths(checkpoint_info.version, checkpoint_info.parts);

        // Step 3: List post-checkpoint commit files
        let commit_file_paths =
            list_commit_files_after(&store, &log_prefix, checkpoint_info.version).await?;

        debug_println!(
            "🔧 DELTA_DIST: {} checkpoint parts, {} post-checkpoint commits",
            checkpoint_part_paths.len(),
            commit_file_paths.len()
        );

        // Step 4: Read schema from first checkpoint part
        let first_part = &checkpoint_part_paths[0];
        let first_part_obj_path = make_log_path(&log_prefix, first_part);
        let (schema_json, partition_columns) =
            read_metadata_from_checkpoint(&store, &first_part_obj_path).await?;

        // Step 5: Build column mapping and translate partition column names
        let column_mapping = build_column_mapping(&schema_json);
        let partition_columns = if column_mapping.is_empty() {
            partition_columns
        } else {
            partition_columns
                .into_iter()
                .map(|col| column_mapping.get(&col).cloned().unwrap_or(col))
                .collect()
        };

        debug_println!(
            "🔧 DELTA_DIST: Column mapping has {} entries, partition_columns={:?}",
            column_mapping.len(),
            partition_columns
        );

        Ok(DeltaSnapshotInfo {
            version: checkpoint_info.version,
            schema_json,
            partition_columns,
            checkpoint_part_paths,
            commit_file_paths,
            num_add_files: checkpoint_info.num_of_add_files,
            column_mapping,
        })
    })
}

/// Executor-side: Read one checkpoint parquet part and extract add file entries.
///
/// Reads ONE checkpoint parquet file, projects only the `add` column, and
/// extracts DeltaFileEntry structs. Typically ~54K entries per part for large tables.
///
/// If `column_mapping` is non-empty, partition value keys are translated from
/// physical to logical names before being returned.
pub fn read_checkpoint_part(
    url_str: &str,
    config: &DeltaStorageConfig,
    part_path: &str,
    column_mapping: &HashMap<String, String>,
) -> Result<Vec<DeltaFileEntry>> {
    debug_println!("🔧 DELTA_DIST: read_checkpoint_part part={}", part_path);

    let url = normalize_url(url_str)?;
    let store = create_object_store(&url, config)?;
    let log_prefix = delta_log_prefix(&url);
    let obj_path = make_log_path(&log_prefix, part_path);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    rt.block_on(async {
        let mut entries = read_checkpoint_part_async(&store, &obj_path).await?;
        apply_column_mapping(&mut entries, column_mapping);
        Ok(entries)
    })
}

/// Driver-side: Read post-checkpoint JSON commit files and apply log replay.
///
/// Reads commit JSON files after the checkpoint, parses add/remove actions,
/// and applies log replay (latest action per path wins).
///
/// If `column_mapping` is non-empty, partition value keys in added files are
/// translated from physical to logical names.
pub fn read_post_checkpoint_changes(
    url_str: &str,
    config: &DeltaStorageConfig,
    commit_paths: &[String],
    column_mapping: &HashMap<String, String>,
) -> Result<DeltaLogChanges> {
    debug_println!(
        "🔧 DELTA_DIST: read_post_checkpoint_changes, {} commits",
        commit_paths.len()
    );

    if commit_paths.is_empty() {
        return Ok(DeltaLogChanges {
            added_files: Vec::new(),
            removed_paths: HashSet::new(),
        });
    }

    let url = normalize_url(url_str)?;
    let store = create_object_store(&url, config)?;
    let log_prefix = delta_log_prefix(&url);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;

    rt.block_on(async {
        let mut changes = read_post_checkpoint_changes_async(&store, &log_prefix, commit_paths).await?;
        apply_column_mapping(&mut changes.added_files, column_mapping);
        Ok(changes)
    })
}

// ─── Internal async functions ───────────────────────────────────────────────

/// Read and parse _delta_log/_last_checkpoint.
async fn read_last_checkpoint(
    store: &Arc<dyn ObjectStore>,
    log_prefix: &str,
) -> Result<Option<LastCheckpointInfo>> {
    let path = ObjectPath::from(format!("{}/_last_checkpoint", log_prefix));

    match store.get(&path).await {
        Ok(result) => {
            let bytes = result.bytes().await?;
            let text = std::str::from_utf8(&bytes)?;
            parse_last_checkpoint(text).map(Some)
        }
        Err(object_store::Error::NotFound { .. }) => Ok(None),
        Err(e) => Err(anyhow::anyhow!("Failed to read _last_checkpoint: {}", e)),
    }
}

/// Parse _last_checkpoint JSON content.
fn parse_last_checkpoint(json_str: &str) -> Result<LastCheckpointInfo> {
    let v: serde_json::Value = serde_json::from_str(json_str)
        .map_err(|e| anyhow::anyhow!("Failed to parse _last_checkpoint JSON: {}", e))?;

    let version = v["version"]
        .as_u64()
        .ok_or_else(|| anyhow::anyhow!("_last_checkpoint missing 'version' field"))?;
    let size = v["size"].as_u64().unwrap_or(0);
    let parts = v["parts"].as_u64();
    let num_of_add_files = v["numOfAddFiles"].as_u64();

    Ok(LastCheckpointInfo {
        version,
        size,
        parts,
        num_of_add_files,
    })
}

/// Construct checkpoint parquet file paths for a given version.
///
/// Single-part: `{version:020}.checkpoint.parquet`
/// Multi-part: `{version:020}.checkpoint.{i:010}.{parts:010}.parquet` for i in 1..=parts
pub fn construct_checkpoint_paths(version: u64, parts: Option<u64>) -> Vec<String> {
    match parts {
        Some(n) if n > 1 => (1..=n)
            .map(|i| {
                format!(
                    "{:020}.checkpoint.{:010}.{:010}.parquet",
                    version, i, n
                )
            })
            .collect(),
        _ => vec![format!("{:020}.checkpoint.parquet", version)],
    }
}

/// List commit JSON files after a given checkpoint version.
///
/// Uses `list_with_delimiter` (not `list`) to get only immediate children of
/// _delta_log/, avoiding recursion into subdirectories like _delta_log/_commits/.
async fn list_commit_files_after(
    store: &Arc<dyn ObjectStore>,
    log_prefix: &str,
    checkpoint_version: u64,
) -> Result<Vec<String>> {
    let prefix = ObjectPath::from(format!("{}/", log_prefix));
    let list_result = store.list_with_delimiter(Some(&prefix)).await?;

    let mut commit_files: Vec<(u64, String)> = Vec::new();

    for obj_meta in &list_result.objects {
        let filename = obj_meta
            .location
            .filename()
            .unwrap_or_default()
            .to_string();

        // Match pattern: {version:020}.json
        if filename.ends_with(".json") && !filename.contains("checkpoint") {
            if let Some(version_str) = filename.strip_suffix(".json") {
                if let Ok(v) = version_str.parse::<u64>() {
                    if v > checkpoint_version {
                        commit_files.push((v, filename));
                    }
                }
            }
        }
    }

    // Sort by version ascending
    commit_files.sort_by_key(|(v, _)| *v);
    Ok(commit_files.into_iter().map(|(_, f)| f).collect())
}

/// Read metaData from the first checkpoint part (column-projected, single row).
async fn read_metadata_from_checkpoint(
    store: &Arc<dyn ObjectStore>,
    checkpoint_path: &ObjectPath,
) -> Result<(String, Vec<String>)> {
    use parquet::arrow::async_reader::ParquetObjectReader;
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use arrow_array::cast::AsArray;
    use futures::StreamExt;

    let meta = store.head(checkpoint_path).await
        .map_err(|e| anyhow::anyhow!("Failed to HEAD checkpoint {}: {}", checkpoint_path, e))?;

    let reader = ParquetObjectReader::new(Arc::clone(store), checkpoint_path.clone())
        .with_file_size(meta.size as u64);

    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    let arrow_schema = builder.schema().clone();

    // Find the metaData column index
    let metadata_idx = arrow_schema
        .fields()
        .iter()
        .position(|f| f.name() == "metaData")
        .ok_or_else(|| anyhow::anyhow!("Checkpoint parquet has no 'metaData' column"))?;

    // Project only metaData column
    let mask = parquet::arrow::ProjectionMask::roots(
        builder.parquet_schema(),
        std::iter::once(metadata_idx),
    );

    let mut stream = builder.with_projection(mask).build()?;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        let metadata_col = batch.column(0);

        // metaData is a struct column — find non-null rows
        let struct_array = metadata_col.as_struct();

        for row in 0..struct_array.len() {
            if struct_array.is_null(row) {
                continue;
            }

            // Extract schemaString and partitionColumns from the struct
            let schema_json = extract_struct_string_field(struct_array, "schemaString", row)
                .unwrap_or_default();
            let partition_columns = extract_struct_string_list_field(struct_array, "partitionColumns", row)
                .unwrap_or_default();

            if !schema_json.is_empty() {
                return Ok((schema_json, partition_columns));
            }
        }
    }

    Err(anyhow::anyhow!(
        "No metaData row found in checkpoint {}",
        checkpoint_path
    ))
}

/// Read one checkpoint parquet part and extract add file entries.
async fn read_checkpoint_part_async(
    store: &Arc<dyn ObjectStore>,
    checkpoint_path: &ObjectPath,
) -> Result<Vec<DeltaFileEntry>> {
    use parquet::arrow::async_reader::ParquetObjectReader;
    use parquet::arrow::ParquetRecordBatchStreamBuilder;
    use futures::StreamExt;

    let meta = store.head(checkpoint_path).await
        .map_err(|e| anyhow::anyhow!("Failed to HEAD checkpoint {}: {}", checkpoint_path, e))?;

    let reader = ParquetObjectReader::new(Arc::clone(store), checkpoint_path.clone())
        .with_file_size(meta.size as u64);

    let builder = ParquetRecordBatchStreamBuilder::new(reader).await?;
    let arrow_schema = builder.schema().clone();

    // Find the 'add' column index
    let add_idx = arrow_schema
        .fields()
        .iter()
        .position(|f| f.name() == "add")
        .ok_or_else(|| anyhow::anyhow!("Checkpoint parquet has no 'add' column"))?;

    // Project only the 'add' column
    let mask = parquet::arrow::ProjectionMask::roots(
        builder.parquet_schema(),
        std::iter::once(add_idx),
    );

    let mut stream = builder.with_projection(mask).build()?;
    let mut entries = Vec::new();

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        extract_add_files_from_batch(&batch, &mut entries)?;
    }

    debug_println!(
        "🔧 DELTA_DIST: Extracted {} add entries from {}",
        entries.len(),
        checkpoint_path
    );

    Ok(entries)
}

/// Extract DeltaFileEntry structs from the 'add' column of a RecordBatch.
fn extract_add_files_from_batch(
    batch: &arrow_array::RecordBatch,
    entries: &mut Vec<DeltaFileEntry>,
) -> Result<()> {
    use arrow_array::cast::AsArray;

    let add_col = batch.column(0);
    let struct_array = add_col.as_struct();

    for row in 0..struct_array.len() {
        // Only process non-null 'add' rows
        if struct_array.is_null(row) {
            continue;
        }

        let path = extract_struct_string_field(struct_array, "path", row)
            .unwrap_or_default();
        if path.is_empty() {
            continue;
        }

        let size = extract_struct_i64_field(struct_array, "size", row).unwrap_or(0);
        let modification_time =
            extract_struct_i64_field(struct_array, "modificationTime", row).unwrap_or(0);

        // Parse partition values from MapArray or StructArray
        let partition_values = extract_partition_values(struct_array, row);

        // Parse numRecords from stats JSON
        let num_records = extract_struct_string_field(struct_array, "stats", row)
            .and_then(|s| parse_num_records_from_stats(&s));

        // Check for deletion vector
        let has_deletion_vector = check_deletion_vector(struct_array, row);

        entries.push(DeltaFileEntry {
            path,
            size,
            modification_time,
            num_records,
            partition_values,
            has_deletion_vector,
        });
    }

    Ok(())
}

/// Read post-checkpoint JSON commit files and apply log replay.
///
/// Canonical Delta log replay: process commits oldest-first. For each path,
/// the last action (add or remove) across all commits wins. This correctly
/// handles re-add-after-remove scenarios.
async fn read_post_checkpoint_changes_async(
    store: &Arc<dyn ObjectStore>,
    log_prefix: &str,
    commit_paths: &[String],
) -> Result<DeltaLogChanges> {
    // Per-path state: Some(entry) = last action was add, None = last action was remove.
    let mut path_state: HashMap<String, Option<DeltaFileEntry>> = HashMap::new();

    // Ensure commits are processed oldest-first. Sorting by filename is correct
    // because Delta commit filenames are zero-padded version numbers
    // (e.g., "00000000000000000001.json").
    let mut sorted_paths = commit_paths.to_vec();
    sorted_paths.sort();

    for commit_file in &sorted_paths {
        let path = make_log_path(log_prefix, commit_file);
        let result = store.get(&path).await
            .map_err(|e| anyhow::anyhow!("Failed to read commit {}: {}", commit_file, e))?;
        let bytes = result.bytes().await?;
        let text = std::str::from_utf8(&bytes)?;

        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let v: serde_json::Value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(_) => continue,
            };

            if let Some(add) = v.get("add") {
                let file_path = add["path"].as_str().unwrap_or_default().to_string();
                if file_path.is_empty() {
                    continue;
                }

                let entry = DeltaFileEntry {
                    path: file_path.clone(),
                    size: add["size"].as_i64().unwrap_or(0),
                    modification_time: add["modificationTime"].as_i64().unwrap_or(0),
                    num_records: add["stats"]
                        .as_str()
                        .and_then(|s| parse_num_records_from_stats(s)),
                    partition_values: parse_partition_values_json(add.get("partitionValues")),
                    has_deletion_vector: add.get("deletionVector").is_some()
                        && !add["deletionVector"].is_null(),
                };

                path_state.insert(file_path, Some(entry));
            } else if let Some(remove) = v.get("remove") {
                if let Some(p) = remove["path"].as_str() {
                    path_state.insert(p.to_string(), None);
                }
            }
        }
    }

    // Partition final state into adds and removes
    let mut added_files = Vec::new();
    let mut removed_paths = HashSet::new();

    for (path, state) in path_state {
        match state {
            Some(entry) => added_files.push(entry),
            None => { removed_paths.insert(path); }
        }
    }

    Ok(DeltaLogChanges {
        added_files,
        removed_paths,
    })
}

// ─── Arrow extraction helpers ───────────────────────────────────────────────

/// Extract a string field from a StructArray at the given row.
fn extract_struct_string_field(
    struct_array: &arrow_array::StructArray,
    field_name: &str,
    row: usize,
) -> Option<String> {
    use arrow_array::cast::AsArray;

    let col_idx = struct_array
        .fields()
        .iter()
        .position(|f| f.name() == field_name)?;
    let col = struct_array.column(col_idx);

    if col.is_null(row) {
        return None;
    }

    // Try StringArray first, then LargeStringArray
    if let Some(arr) = col.as_any().downcast_ref::<arrow_array::StringArray>() {
        Some(arr.value(row).to_string())
    } else if let Some(arr) = col.as_any().downcast_ref::<arrow_array::LargeStringArray>() {
        Some(arr.value(row).to_string())
    } else {
        None
    }
}

/// Extract an i64 field from a StructArray at the given row.
fn extract_struct_i64_field(
    struct_array: &arrow_array::StructArray,
    field_name: &str,
    row: usize,
) -> Option<i64> {
    let col_idx = struct_array
        .fields()
        .iter()
        .position(|f| f.name() == field_name)?;
    let col = struct_array.column(col_idx);

    if col.is_null(row) {
        return None;
    }

    if let Some(arr) = col.as_any().downcast_ref::<arrow_array::Int64Array>() {
        Some(arr.value(row))
    } else {
        None
    }
}

/// Extract a string list field from a StructArray (for partitionColumns).
fn extract_struct_string_list_field(
    struct_array: &arrow_array::StructArray,
    field_name: &str,
    row: usize,
) -> Option<Vec<String>> {
    use arrow_array::cast::AsArray;

    let col_idx = struct_array
        .fields()
        .iter()
        .position(|f| f.name() == field_name)?;
    let col = struct_array.column(col_idx);

    if col.is_null(row) {
        return None;
    }

    // partitionColumns is a ListArray of strings
    if let Some(list_arr) = col.as_any().downcast_ref::<arrow_array::ListArray>() {
        let values = list_arr.value(row);
        let string_arr = values.as_string::<i32>();
        let mut result = Vec::with_capacity(string_arr.len());
        for i in 0..string_arr.len() {
            if !string_arr.is_null(i) {
                result.push(string_arr.value(i).to_string());
            }
        }
        return Some(result);
    }

    None
}

/// Extract partition values from a struct's partitionValues field.
fn extract_partition_values(
    struct_array: &arrow_array::StructArray,
    row: usize,
) -> HashMap<String, String> {
    // partitionValues is stored as a MapArray<Utf8, Utf8> in checkpoint parquet
    let col_idx = struct_array
        .fields()
        .iter()
        .position(|f| f.name() == "partitionValues");

    let col_idx = match col_idx {
        Some(i) => i,
        None => return HashMap::new(),
    };

    let col = struct_array.column(col_idx);
    if col.is_null(row) {
        return HashMap::new();
    }

    if let Some(map_arr) = col.as_any().downcast_ref::<arrow_array::MapArray>() {
        parse_map_array(map_arr, row)
    } else {
        HashMap::new()
    }
}

/// Parse a MapArray row into a HashMap<String, String>.
fn parse_map_array(
    map_array: &arrow_array::MapArray,
    row: usize,
) -> HashMap<String, String> {
    let entries = map_array.value(row);
    let struct_arr = entries
        .as_any()
        .downcast_ref::<arrow_array::StructArray>()
        .unwrap();

    let keys = struct_arr.column(0);
    let values = struct_arr.column(1);

    let key_arr = keys
        .as_any()
        .downcast_ref::<arrow_array::StringArray>();
    let val_arr = values
        .as_any()
        .downcast_ref::<arrow_array::StringArray>();

    match (key_arr, val_arr) {
        (Some(k), Some(v)) => {
            let mut map = HashMap::new();
            for i in 0..k.len() {
                if !k.is_null(i) && !v.is_null(i) {
                    map.insert(k.value(i).to_string(), v.value(i).to_string());
                }
            }
            map
        }
        _ => HashMap::new(),
    }
}

/// Check if a deletion vector exists for the given row.
fn check_deletion_vector(
    struct_array: &arrow_array::StructArray,
    row: usize,
) -> bool {
    let col_idx = struct_array
        .fields()
        .iter()
        .position(|f| f.name() == "deletionVector");

    match col_idx {
        Some(i) => !struct_array.column(i).is_null(row),
        None => false,
    }
}

/// Parse numRecords from a Delta stats JSON string.
fn parse_num_records_from_stats(stats_str: &str) -> Option<u64> {
    let v: serde_json::Value = serde_json::from_str(stats_str).ok()?;
    v["numRecords"].as_u64()
}

/// Parse partition values from a JSON Value (for commit JSON files).
fn parse_partition_values_json(value: Option<&serde_json::Value>) -> HashMap<String, String> {
    match value {
        Some(serde_json::Value::Object(map)) => {
            map.iter()
                .filter_map(|(k, v)| {
                    v.as_str().map(|s| (k.clone(), s.to_string()))
                })
                .collect()
        }
        _ => HashMap::new(),
    }
}

// ─── Column mapping helpers ─────────────────────────────────────────────────

/// Parse Delta schema JSON to build a physical_name → logical_name mapping.
///
/// Delta tables with `delta.columnMapping.mode = 'name'` store physical column
/// IDs (UUIDs like `col-350d02e8-...`) in checkpoint/commit files instead of
/// logical column names. This function extracts the mapping from schema metadata.
///
/// Returns an empty map if no column mapping metadata is found.
pub fn build_column_mapping(schema_json: &str) -> HashMap<String, String> {
    let v: serde_json::Value = match serde_json::from_str(schema_json) {
        Ok(v) => v,
        Err(_) => return HashMap::new(),
    };

    let mut mapping = HashMap::new();
    if let Some(fields) = v.get("fields").and_then(|f| f.as_array()) {
        for field in fields {
            let logical_name = match field.get("name").and_then(|n| n.as_str()) {
                Some(n) => n,
                None => continue,
            };
            if let Some(metadata) = field.get("metadata").and_then(|m| m.as_object()) {
                if let Some(physical_name) = metadata
                    .get("delta.columnMapping.physicalName")
                    .and_then(|p| p.as_str())
                {
                    mapping.insert(physical_name.to_string(), logical_name.to_string());
                }
            }
        }
    }

    mapping
}

/// Translate partition value keys from physical to logical names using the mapping.
///
/// No-op if the mapping is empty. Only keys found in the mapping are translated;
/// keys not in the mapping are left unchanged (handles mixed physical + logical).
pub fn apply_column_mapping(entries: &mut [DeltaFileEntry], mapping: &HashMap<String, String>) {
    if mapping.is_empty() {
        return;
    }
    for entry in entries.iter_mut() {
        let old_pvs = std::mem::take(&mut entry.partition_values);
        for (key, value) in old_pvs {
            let logical_key = mapping.get(&key).cloned().unwrap_or(key);
            entry.partition_values.insert(logical_key, value);
        }
    }
}

/// Parse a column mapping JSON string (`{"col-xxx":"kdate",...}`) into a HashMap.
///
/// Returns an empty map if the input is None, empty, or invalid JSON.
pub fn parse_column_mapping_json(json: Option<&str>) -> HashMap<String, String> {
    match json {
        Some(s) if !s.is_empty() && s != "{}" => {
            serde_json::from_str(s).unwrap_or_default()
        }
        _ => HashMap::new(),
    }
}

// ─── Path helpers ───────────────────────────────────────────────────────────

/// Get the _delta_log prefix for an object store path.
fn delta_log_prefix(url: &Url) -> String {
    let path = url.path();
    let path = path.strip_prefix('/').unwrap_or(path);
    let path = path.strip_suffix('/').unwrap_or(path);
    if path.is_empty() {
        "_delta_log".to_string()
    } else {
        format!("{}/_delta_log", path)
    }
}

/// Create an ObjectPath from a log prefix and a filename.
fn make_log_path(log_prefix: &str, filename: &str) -> ObjectPath {
    ObjectPath::from(format!("{}/{}", log_prefix, filename))
}

// ─── Arrow FFI export ────────────────────────────────────────────────────────

/// Read a checkpoint part and export filtered entries via Arrow FFI.
///
/// Builds a flat RecordBatch with 6 columns:
///   path (Utf8), size (Int64), modification_time (Int64),
///   num_records (Int64), partition_values (Utf8/JSON), has_deletion_vector (Boolean)
///
/// Exports each column to the pre-allocated FFI addresses provided by the caller.
/// Returns the number of rows written.
pub fn read_checkpoint_part_arrow_ffi(
    url_str: &str,
    config: &DeltaStorageConfig,
    part_path: &str,
    predicate: Option<&crate::common::PartitionPredicate>,
    column_mapping: &HashMap<String, String>,
    array_addrs: &[i64],
    schema_addrs: &[i64],
) -> Result<usize> {
    use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
    use arrow_array::{StringArray, Int64Array, BooleanArray};
    use arrow_schema::{DataType, Field};

    const NUM_COLS: usize = 6;

    if array_addrs.len() < NUM_COLS || schema_addrs.len() < NUM_COLS {
        anyhow::bail!(
            "Insufficient FFI addresses: need {} but got {} array_addrs and {} schema_addrs",
            NUM_COLS, array_addrs.len(), schema_addrs.len()
        );
    }

    // 1. Read checkpoint → Vec<DeltaFileEntry> (with column mapping applied)
    let entries = read_checkpoint_part(url_str, config, part_path, column_mapping)?;

    // 2. Apply partition predicate filter
    let entries: Vec<DeltaFileEntry> = match predicate {
        Some(pred) => entries.into_iter().filter(|e| pred.evaluate(&e.partition_values)).collect(),
        None => entries,
    };

    let num_rows = entries.len();

    // 3. Build flat Arrow arrays
    let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
    let sizes: Vec<i64> = entries.iter().map(|e| e.size).collect();
    let mod_times: Vec<i64> = entries.iter().map(|e| e.modification_time).collect();
    let num_recs: Vec<i64> = entries.iter().map(|e| e.num_records.map(|n| n as i64).unwrap_or(-1)).collect();
    let pvs: Vec<String> = entries.iter().map(|e| {
        serde_json::to_string(&e.partition_values).unwrap_or_else(|_| "{}".to_string())
    }).collect();
    let pv_refs: Vec<&str> = pvs.iter().map(|s| s.as_str()).collect();
    let has_dvs: Vec<bool> = entries.iter().map(|e| e.has_deletion_vector).collect();

    let arrays: Vec<(Arc<dyn arrow_array::Array>, Field)> = vec![
        (Arc::new(StringArray::from(paths)), Field::new("path", DataType::Utf8, false)),
        (Arc::new(Int64Array::from(sizes)), Field::new("size", DataType::Int64, false)),
        (Arc::new(Int64Array::from(mod_times)), Field::new("modification_time", DataType::Int64, false)),
        (Arc::new(Int64Array::from(num_recs)), Field::new("num_records", DataType::Int64, false)),
        (Arc::new(StringArray::from(pv_refs)), Field::new("partition_values", DataType::Utf8, false)),
        (Arc::new(BooleanArray::from(has_dvs)), Field::new("has_deletion_vector", DataType::Boolean, false)),
    ];

    // 4. Validate ALL addresses upfront before writing anything.
    //    This prevents partial writes if a later address is null — either all
    //    columns are exported or none are.
    for i in 0..NUM_COLS {
        if array_addrs[i] == 0 || schema_addrs[i] == 0 {
            anyhow::bail!(
                "Null FFI address for column {}: array_addr={}, schema_addr={}",
                i, array_addrs[i], schema_addrs[i]
            );
        }
    }

    // 5. Build FFI structs in a temporary vec first, then write all at once.
    //    If any schema conversion fails, nothing is written.
    let mut ffi_pairs: Vec<(FFI_ArrowArray, FFI_ArrowSchema)> = Vec::with_capacity(NUM_COLS);
    for (i, (array, field)) in arrays.iter().enumerate() {
        let data = array.to_data();
        let ffi_array = FFI_ArrowArray::new(&data);
        let ffi_schema = FFI_ArrowSchema::try_from(field)
            .map_err(|e| anyhow::anyhow!("FFI_ArrowSchema failed for col {}: {}", i, e))?;
        ffi_pairs.push((ffi_array, ffi_schema));
    }

    // 6. All FFI structs built successfully — write them all out.
    for (i, (ffi_array, ffi_schema)) in ffi_pairs.into_iter().enumerate() {
        let array_ptr = array_addrs[i] as *mut FFI_ArrowArray;
        let schema_ptr = schema_addrs[i] as *mut FFI_ArrowSchema;
        unsafe {
            std::ptr::write_unaligned(array_ptr, ffi_array);
            std::ptr::write_unaligned(schema_ptr, ffi_schema);
        }
    }

    Ok(num_rows)
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_last_checkpoint() {
        let json = r#"{"version":94320,"size":61126995,"parts":1123,"numOfAddFiles":61126995}"#;
        let info = parse_last_checkpoint(json).unwrap();
        assert_eq!(info.version, 94320);
        assert_eq!(info.size, 61126995);
        assert_eq!(info.parts, Some(1123));
        assert_eq!(info.num_of_add_files, Some(61126995));
    }

    #[test]
    fn test_parse_last_checkpoint_no_parts() {
        let json = r#"{"version":100,"size":5000}"#;
        let info = parse_last_checkpoint(json).unwrap();
        assert_eq!(info.version, 100);
        assert_eq!(info.size, 5000);
        assert_eq!(info.parts, None);
        assert_eq!(info.num_of_add_files, None);
    }

    #[test]
    fn test_construct_checkpoint_paths_single() {
        let paths = construct_checkpoint_paths(100, None);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0], "00000000000000000100.checkpoint.parquet");
    }

    #[test]
    fn test_construct_checkpoint_paths_single_part_1() {
        let paths = construct_checkpoint_paths(100, Some(1));
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0], "00000000000000000100.checkpoint.parquet");
    }

    #[test]
    fn test_construct_checkpoint_paths_multi() {
        let paths = construct_checkpoint_paths(94320, Some(1123));
        assert_eq!(paths.len(), 1123);
        assert_eq!(
            paths[0],
            "00000000000000094320.checkpoint.0000000001.0000001123.parquet"
        );
        assert_eq!(
            paths[1],
            "00000000000000094320.checkpoint.0000000002.0000001123.parquet"
        );
        assert_eq!(
            paths[1122],
            "00000000000000094320.checkpoint.0000001123.0000001123.parquet"
        );
    }

    #[test]
    fn test_parse_num_records_from_stats() {
        assert_eq!(
            parse_num_records_from_stats(r#"{"numRecords":50}"#),
            Some(50)
        );
        assert_eq!(
            parse_num_records_from_stats(r#"{"numRecords":0}"#),
            Some(0)
        );
        assert_eq!(parse_num_records_from_stats(r#"{}"#), None);
        assert_eq!(parse_num_records_from_stats("invalid"), None);
    }

    #[test]
    fn test_parse_partition_values_json() {
        let v: serde_json::Value = serde_json::from_str(
            r#"{"year":"2024","month":"01"}"#,
        ).unwrap();
        let pv = parse_partition_values_json(Some(&v));
        assert_eq!(pv.len(), 2);
        assert_eq!(pv.get("year").unwrap(), "2024");
        assert_eq!(pv.get("month").unwrap(), "01");
    }

    #[test]
    fn test_parse_partition_values_json_empty() {
        let pv = parse_partition_values_json(None);
        assert!(pv.is_empty());
    }

    #[test]
    fn test_delta_log_prefix() {
        let url = Url::parse("s3://bucket/path/to/table/").unwrap();
        assert_eq!(delta_log_prefix(&url), "path/to/table/_delta_log");

        let url = Url::parse("file:///tmp/my_table/").unwrap();
        assert_eq!(delta_log_prefix(&url), "tmp/my_table/_delta_log");
    }

    #[test]
    fn test_read_post_checkpoint_changes_empty() {
        // Empty commit list should return empty changes
        let config = DeltaStorageConfig::default();
        let changes = read_post_checkpoint_changes(
            "file:///tmp/nonexistent_table",
            &config,
            &[],
            &HashMap::new(),
        ).unwrap();
        assert!(changes.added_files.is_empty());
        assert!(changes.removed_paths.is_empty());
    }

    #[test]
    fn test_get_snapshot_info_local() {
        // Create a minimal Delta table with a checkpoint
        let tmp = tempfile::tempdir().unwrap();
        let table_dir = tmp.path().join("test_snapshot_info");
        let delta_log = table_dir.join("_delta_log");
        std::fs::create_dir_all(&delta_log).unwrap();

        // Write commit 0: protocol + metadata + add
        let schema_string = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}}]}"#;
        let commit0 = format!(
            "{}\n{}\n{}",
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            format!(
                r#"{{"metaData":{{"id":"test-id","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{}","partitionColumns":["name"],"configuration":{{}},"createdTime":1700000000000}}}}"#,
                schema_string.replace('"', r#"\""#)
            ),
            r#"{"add":{"path":"part-00000.parquet","partitionValues":{},"size":5000,"modificationTime":1700000000000,"dataChange":true,"stats":"{\"numRecords\":50}"}}"#,
        );
        std::fs::write(delta_log.join("00000000000000000000.json"), &commit0).unwrap();
        std::fs::write(table_dir.join("part-00000.parquet"), &[0u8]).unwrap();

        // Create a checkpoint parquet file with metaData and add rows
        write_test_checkpoint(
            &delta_log.join("00000000000000000000.checkpoint.parquet"),
            schema_string,
            &["name"],
            &[("part-00000.parquet", 5000, 1700000000000i64, 50)],
        );

        // Write _last_checkpoint
        std::fs::write(
            delta_log.join("_last_checkpoint"),
            r#"{"version":0,"size":1}"#,
        ).unwrap();

        // Write a post-checkpoint commit
        let commit1 = r#"{"add":{"path":"part-00001.parquet","partitionValues":{},"size":6000,"modificationTime":1700000001000,"dataChange":true,"stats":"{\"numRecords\":60}"}}"#;
        std::fs::write(delta_log.join("00000000000000000001.json"), commit1).unwrap();

        // Test get_snapshot_info
        let config = DeltaStorageConfig::default();
        let info = get_snapshot_info(table_dir.to_str().unwrap(), &config).unwrap();

        assert_eq!(info.version, 0);
        assert_eq!(info.checkpoint_part_paths.len(), 1);
        assert_eq!(
            info.checkpoint_part_paths[0],
            "00000000000000000000.checkpoint.parquet"
        );
        assert_eq!(info.commit_file_paths.len(), 1);
        assert_eq!(info.commit_file_paths[0], "00000000000000000001.json");
        assert!(!info.schema_json.is_empty());
        assert_eq!(info.partition_columns, vec!["name"]);
    }

    /// Helper to write a test checkpoint parquet file with metaData and add rows.
    fn write_test_checkpoint(
        path: &std::path::Path,
        schema_string: &str,
        partition_columns: &[&str],
        adds: &[(&str, i64, i64, i64)], // (path, size, mod_time, num_records)
    ) {
        use arrow_array::{
            builder::{StringBuilder, Int64Builder, BooleanBuilder, MapBuilder},
            StructArray, RecordBatch,
        };
        use arrow_schema::{DataType, Field, Fields, Schema};

        let num_rows = 1 + adds.len(); // 1 metaData row + N add rows

        // metaData struct fields
        let metadata_fields = Fields::from(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("schemaString", DataType::Utf8, true),
            Field::new(
                "partitionColumns",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
        ]);

        // Build metaData column — non-null only for row 0
        let mut id_builder = StringBuilder::new();
        let mut schema_builder = StringBuilder::new();
        let mut partition_list_builder = arrow_array::builder::ListBuilder::new(StringBuilder::new());

        // Row 0: metaData row
        id_builder.append_value("test-id");
        schema_builder.append_value(schema_string);
        let values = partition_list_builder.values();
        for col in partition_columns {
            values.append_value(*col);
        }
        partition_list_builder.append(true);

        // Remaining rows: null metaData
        for _ in 0..adds.len() {
            id_builder.append_null();
            schema_builder.append_null();
            partition_list_builder.append(false);
        }

        let metadata_nulls = {
            let mut bools = vec![false; num_rows];
            bools[0] = true; // only row 0 (metaData) is non-null
            arrow::buffer::NullBuffer::from(bools.as_slice())
        };

        let metadata_struct = StructArray::new(
            metadata_fields,
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(schema_builder.finish()),
                Arc::new(partition_list_builder.finish()),
            ],
            Some(metadata_nulls),
        );

        // add struct fields
        let add_fields = Fields::from(vec![
            Field::new("path", DataType::Utf8, true),
            Field::new("size", DataType::Int64, true),
            Field::new("modificationTime", DataType::Int64, true),
            Field::new("stats", DataType::Utf8, true),
            Field::new(
                "partitionValues",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("keys", DataType::Utf8, false),
                            Field::new("values", DataType::Utf8, true),
                        ])),
                        false,
                    )),
                    false,
                ),
                true,
            ),
        ]);

        // Build add column — null for row 0, non-null for add rows
        let mut path_builder = StringBuilder::new();
        let mut size_builder = Int64Builder::new();
        let mut mod_time_builder = Int64Builder::new();
        let mut stats_builder = StringBuilder::new();
        let mut pv_builder = MapBuilder::new(
            None,
            StringBuilder::new(),
            StringBuilder::new(),
        );

        // Row 0: null add
        path_builder.append_null();
        size_builder.append_null();
        mod_time_builder.append_null();
        stats_builder.append_null();
        pv_builder.append(true).unwrap();

        // Remaining rows: add entries
        for (p, s, mt, nr) in adds {
            path_builder.append_value(*p);
            size_builder.append_value(*s);
            mod_time_builder.append_value(*mt);
            stats_builder.append_value(format!(r#"{{"numRecords":{}}}"#, nr));
            pv_builder.append(true).unwrap();
        }

        let add_nulls = {
            let mut bools = vec![false; num_rows];
            for i in 1..num_rows {
                bools[i] = true; // add rows are non-null
            }
            arrow::buffer::NullBuffer::from(bools.as_slice())
        };

        let add_struct = StructArray::new(
            add_fields,
            vec![
                Arc::new(path_builder.finish()),
                Arc::new(size_builder.finish()),
                Arc::new(mod_time_builder.finish()),
                Arc::new(stats_builder.finish()),
                Arc::new(pv_builder.finish()),
            ],
            Some(add_nulls),
        );

        // Create the RecordBatch with both columns
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "metaData",
                DataType::Struct(metadata_struct.fields().clone()),
                true,
            ),
            Field::new(
                "add",
                DataType::Struct(add_struct.fields().clone()),
                true,
            ),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(metadata_struct),
                Arc::new(add_struct),
            ],
        )
        .unwrap();

        let file = std::fs::File::create(path).unwrap();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    #[test]
    fn test_read_checkpoint_part_local() {
        let tmp = tempfile::tempdir().unwrap();
        let checkpoint_path = tmp.path().join("test.checkpoint.parquet");

        write_test_checkpoint(
            &checkpoint_path,
            r#"{"type":"struct","fields":[]}"#,
            &[],
            &[
                ("part-00000.parquet", 5000, 1700000000000, 50),
                ("part-00001.parquet", 6000, 1700000001000, 60),
                ("part-00002.parquet", 7000, 1700000002000, 70),
            ],
        );

        let config = DeltaStorageConfig::default();
        let url = Url::from_directory_path(tmp.path()).unwrap();
        let store = create_object_store(&url, &config).unwrap();
        let obj_path = ObjectPath::from(format!(
            "{}/test.checkpoint.parquet",
            tmp.path().to_str().unwrap().strip_prefix('/').unwrap()
        ));

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        let entries = rt.block_on(read_checkpoint_part_async(&store, &obj_path)).unwrap();

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].path, "part-00000.parquet");
        assert_eq!(entries[0].size, 5000);
        assert_eq!(entries[0].num_records, Some(50));
        assert_eq!(entries[1].path, "part-00001.parquet");
        assert_eq!(entries[1].size, 6000);
        assert_eq!(entries[2].path, "part-00002.parquet");
        assert_eq!(entries[2].size, 7000);
    }

    #[test]
    fn test_read_post_checkpoint_changes_local() {
        let tmp = tempfile::tempdir().unwrap();
        let table_dir = tmp.path().join("test_changes");
        let delta_log = table_dir.join("_delta_log");
        std::fs::create_dir_all(&delta_log).unwrap();

        // We need _last_checkpoint and a commit file for the table to be recognized
        std::fs::write(
            delta_log.join("_last_checkpoint"),
            r#"{"version":0,"size":1}"#,
        ).unwrap();

        // Write commit 1: add + remove
        let commit1 = [
            r#"{"add":{"path":"part-00001.parquet","partitionValues":{},"size":6000,"modificationTime":1700000001000,"dataChange":true,"stats":"{\"numRecords\":60}"}}"#,
            r#"{"remove":{"path":"part-old.parquet","deletionTimestamp":1700000001000,"dataChange":true}}"#,
        ].join("\n");
        std::fs::write(delta_log.join("00000000000000000001.json"), &commit1).unwrap();

        // Write commit 2: another add
        let commit2 = r#"{"add":{"path":"part-00002.parquet","partitionValues":{"year":"2024"},"size":7000,"modificationTime":1700000002000,"dataChange":true}}"#;
        std::fs::write(delta_log.join("00000000000000000002.json"), commit2).unwrap();

        let config = DeltaStorageConfig::default();
        let changes = read_post_checkpoint_changes(
            table_dir.to_str().unwrap(),
            &config,
            &[
                "00000000000000000001.json".to_string(),
                "00000000000000000002.json".to_string(),
            ],
            &HashMap::new(),
        ).unwrap();

        assert_eq!(changes.added_files.len(), 2);
        assert!(changes.removed_paths.contains("part-old.parquet"));

        // Verify added files
        let paths: Vec<&str> = changes.added_files.iter().map(|f| f.path.as_str()).collect();
        assert!(paths.contains(&"part-00001.parquet"));
        assert!(paths.contains(&"part-00002.parquet"));
    }

    // ─── Column mapping tests ────────────────────────────────────────────

    #[test]
    fn test_build_column_mapping_with_physical_names() {
        let schema_json = r#"{
            "type": "struct",
            "fields": [
                {
                    "name": "kdate",
                    "type": "string",
                    "nullable": true,
                    "metadata": {
                        "delta.columnMapping.physicalName": "col-350d02e8-4a5c-4cfd-af33-68919953e591",
                        "delta.columnMapping.id": 3
                    }
                },
                {
                    "name": "id",
                    "type": "long",
                    "nullable": false,
                    "metadata": {
                        "delta.columnMapping.physicalName": "col-aaa11111-2222-3333-4444-555566667777",
                        "delta.columnMapping.id": 1
                    }
                }
            ]
        }"#;

        let mapping = build_column_mapping(schema_json);
        assert_eq!(mapping.len(), 2);
        assert_eq!(
            mapping.get("col-350d02e8-4a5c-4cfd-af33-68919953e591").unwrap(),
            "kdate"
        );
        assert_eq!(
            mapping.get("col-aaa11111-2222-3333-4444-555566667777").unwrap(),
            "id"
        );
    }

    #[test]
    fn test_build_column_mapping_no_mapping() {
        // Schema without column mapping metadata → empty map
        let schema_json = r#"{
            "type": "struct",
            "fields": [
                {"name": "id", "type": "long", "nullable": false, "metadata": {}},
                {"name": "name", "type": "string", "nullable": true, "metadata": {}}
            ]
        }"#;

        let mapping = build_column_mapping(schema_json);
        assert!(mapping.is_empty());
    }

    #[test]
    fn test_build_column_mapping_invalid_json() {
        let mapping = build_column_mapping("not valid json");
        assert!(mapping.is_empty());
    }

    #[test]
    fn test_apply_column_mapping() {
        let mut entries = vec![
            DeltaFileEntry {
                path: "part-0.parquet".to_string(),
                size: 1000,
                modification_time: 0,
                num_records: Some(10),
                partition_values: {
                    let mut m = HashMap::new();
                    m.insert("col-aaa".to_string(), "2024-01-01".to_string());
                    m.insert("col-bbb".to_string(), "us-east".to_string());
                    m
                },
                has_deletion_vector: false,
            },
        ];

        let mut mapping = HashMap::new();
        mapping.insert("col-aaa".to_string(), "kdate".to_string());
        mapping.insert("col-bbb".to_string(), "region".to_string());

        apply_column_mapping(&mut entries, &mapping);

        assert_eq!(entries[0].partition_values.get("kdate").unwrap(), "2024-01-01");
        assert_eq!(entries[0].partition_values.get("region").unwrap(), "us-east");
        assert!(!entries[0].partition_values.contains_key("col-aaa"));
        assert!(!entries[0].partition_values.contains_key("col-bbb"));
    }

    #[test]
    fn test_apply_column_mapping_empty() {
        let mut entries = vec![
            DeltaFileEntry {
                path: "part-0.parquet".to_string(),
                size: 1000,
                modification_time: 0,
                num_records: Some(10),
                partition_values: {
                    let mut m = HashMap::new();
                    m.insert("year".to_string(), "2024".to_string());
                    m
                },
                has_deletion_vector: false,
            },
        ];

        let mapping = HashMap::new();
        apply_column_mapping(&mut entries, &mapping);

        // No-op: original keys preserved
        assert_eq!(entries[0].partition_values.get("year").unwrap(), "2024");
    }

    #[test]
    fn test_apply_column_mapping_partial() {
        // Mixed physical + logical keys — only mapped ones translate
        let mut entries = vec![
            DeltaFileEntry {
                path: "part-0.parquet".to_string(),
                size: 1000,
                modification_time: 0,
                num_records: None,
                partition_values: {
                    let mut m = HashMap::new();
                    m.insert("col-aaa".to_string(), "2024-01-01".to_string());
                    m.insert("already_logical".to_string(), "foo".to_string());
                    m
                },
                has_deletion_vector: false,
            },
        ];

        let mut mapping = HashMap::new();
        mapping.insert("col-aaa".to_string(), "kdate".to_string());

        apply_column_mapping(&mut entries, &mapping);

        assert_eq!(entries[0].partition_values.get("kdate").unwrap(), "2024-01-01");
        assert_eq!(entries[0].partition_values.get("already_logical").unwrap(), "foo");
        assert!(!entries[0].partition_values.contains_key("col-aaa"));
    }

    #[test]
    fn test_parse_column_mapping_json() {
        let json = r#"{"col-aaa":"kdate","col-bbb":"region"}"#;
        let mapping = parse_column_mapping_json(Some(json));
        assert_eq!(mapping.len(), 2);
        assert_eq!(mapping.get("col-aaa").unwrap(), "kdate");
        assert_eq!(mapping.get("col-bbb").unwrap(), "region");
    }

    #[test]
    fn test_parse_column_mapping_json_none() {
        let mapping = parse_column_mapping_json(None);
        assert!(mapping.is_empty());
    }

    #[test]
    fn test_parse_column_mapping_json_empty() {
        let mapping = parse_column_mapping_json(Some("{}"));
        assert!(mapping.is_empty());

        let mapping = parse_column_mapping_json(Some(""));
        assert!(mapping.is_empty());
    }
}
