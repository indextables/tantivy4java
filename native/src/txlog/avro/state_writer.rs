// txlog/avro/state_writer.rs - Write state-v<N>/ directory atomically

use std::collections::HashMap;
use bytes::Bytes;

use crate::debug_println;
use crate::txlog::actions::*;
use crate::txlog::error::Result;
use crate::txlog::partition_pruning::compare_values;
use crate::txlog::storage::TxLogStorage;

use super::manifest_writer;

/// Maximum entries per manifest file for partition-level pruning.
const MAX_ENTRIES_PER_MANIFEST: usize = 50_000;

/// Write a complete Avro state checkpoint.
///
/// Creates `state-v<version>/` with manifest files, `_manifest`, and updates `_last_checkpoint`.
pub async fn write_state_checkpoint(
    storage: &TxLogStorage,
    version: i64,
    entries: &[FileEntry],
    _protocol: &ProtocolAction,
    metadata: &MetadataAction,
) -> Result<LastCheckpointInfo> {
    let state_dir = TxLogStorage::state_dir_name(version);
    debug_println!("📝 STATE_WRITER: Writing checkpoint at {} with {} entries", state_dir, entries.len());

    // Group entries by partition key for manifest splitting
    let groups = group_entries_by_partition(entries, &metadata.partition_columns);

    // Write manifest files
    let mut manifest_infos = Vec::new();
    let mut manifest_idx = 0;
    let mut total_size_bytes: i64 = 0;

    for group in &groups {
        let manifest_name = format!("manifest-{:04}.avro", manifest_idx);
        let avro_bytes = manifest_writer::write_manifest_bytes(group)?;
        total_size_bytes += avro_bytes.len() as i64;

        let manifest_path = format!("{}/{}", state_dir, manifest_name);
        storage.put(&manifest_path, Bytes::from(avro_bytes)).await?;

        // Compute partition bounds for this manifest
        let bounds = compute_partition_bounds(group, &metadata.partition_columns);

        manifest_infos.push(ManifestInfo {
            path: manifest_name,
            file_count: group.len() as i64,
            partition_bounds: bounds,
        });

        manifest_idx += 1;
    }

    // Compute global partition bounds
    let global_bounds = compute_partition_bounds(entries, &metadata.partition_columns);

    let now = current_timestamp_ms();
    let state_manifest = StateManifest {
        version,
        manifests: manifest_infos,
        partition_bounds: global_bounds,
        created_time: now,
        total_file_count: entries.len() as i64,
        format: "avro-state".to_string(),
    };

    // Write _manifest
    let manifest_json = serde_json::to_vec(&state_manifest)
        .map_err(|e| crate::txlog::error::TxLogError::Storage(anyhow::anyhow!("Serialize manifest: {}", e)))?;
    let manifest_path = format!("{}/_manifest", state_dir);
    storage.put(&manifest_path, Bytes::from(manifest_json)).await?;

    // Write _last_checkpoint
    let last_checkpoint = LastCheckpointInfo {
        version,
        size: entries.len() as i64,
        size_in_bytes: Some(total_size_bytes),
        num_files: Some(entries.len() as i64),
        format: "avro-state".to_string(),
        state_dir: Some(state_dir),
        created_time: Some(now),
    };
    let checkpoint_json = serde_json::to_vec(&last_checkpoint)
        .map_err(|e| crate::txlog::error::TxLogError::Storage(anyhow::anyhow!("Serialize checkpoint: {}", e)))?;
    storage.put("_last_checkpoint", Bytes::from(checkpoint_json)).await?;

    debug_println!("✅ STATE_WRITER: Checkpoint written at version {} ({} manifests, {} entries)",
        version, manifest_idx, entries.len());

    Ok(last_checkpoint)
}

/// Group entries into manifest-sized partitions.
/// Groups by partition key values for efficient pruning.
fn group_entries_by_partition(
    entries: &[FileEntry],
    partition_columns: &[String],
) -> Vec<Vec<FileEntry>> {
    if partition_columns.is_empty() || entries.len() <= MAX_ENTRIES_PER_MANIFEST {
        // No partitioning or small enough for one manifest
        return vec![entries.to_vec()];
    }

    // Group by first partition column value
    let mut groups: HashMap<String, Vec<FileEntry>> = HashMap::new();
    for entry in entries {
        let key = partition_columns.iter()
            .filter_map(|col| entry.add.partition_values.get(col))
            .cloned()
            .collect::<Vec<_>>()
            .join("|");
        groups.entry(key).or_default().push(entry.clone());
    }

    // Consolidate small groups to stay near MAX_ENTRIES_PER_MANIFEST
    let mut result: Vec<Vec<FileEntry>> = Vec::new();
    let mut current_batch: Vec<FileEntry> = Vec::new();

    for (_key, group) in groups {
        if current_batch.len() + group.len() > MAX_ENTRIES_PER_MANIFEST && !current_batch.is_empty() {
            result.push(std::mem::take(&mut current_batch));
        }
        current_batch.extend(group);
    }
    if !current_batch.is_empty() {
        result.push(current_batch);
    }

    result
}

/// Compute min/max partition bounds across a set of entries.
fn compute_partition_bounds(
    entries: &[FileEntry],
    partition_columns: &[String],
) -> Option<PartitionBounds> {
    if partition_columns.is_empty() || entries.is_empty() {
        return None;
    }

    let mut min_values: HashMap<String, String> = HashMap::new();
    let mut max_values: HashMap<String, String> = HashMap::new();

    for entry in entries {
        for col in partition_columns {
            if let Some(val) = entry.add.partition_values.get(col) {
                min_values.entry(col.clone())
                    .and_modify(|existing| {
                        if compare_values(val, existing) == std::cmp::Ordering::Less {
                            *existing = val.clone();
                        }
                    })
                    .or_insert_with(|| val.clone());
                max_values.entry(col.clone())
                    .and_modify(|existing| {
                        if compare_values(val, existing) == std::cmp::Ordering::Greater {
                            *existing = val.clone();
                        }
                    })
                    .or_insert_with(|| val.clone());
            }
        }
    }

    if min_values.is_empty() {
        None
    } else {
        Some(PartitionBounds { min_values, max_values })
    }
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
    use crate::txlog::actions::AddAction;

    fn make_entry(path: &str, pv: HashMap<String, String>) -> FileEntry {
        FileEntry {
            add: AddAction {
                path: path.to_string(),
                partition_values: pv,
                size: 100, modification_time: 0, data_change: true,
                stats: None, min_values: None, max_values: None, num_records: None,
                footer_start_offset: None, footer_end_offset: None, has_footer_offsets: None,
                split_tags: None, num_merge_ops: None,
                doc_mapping_json: None, doc_mapping_ref: None,
                uncompressed_size_bytes: None,
                time_range_start: None, time_range_end: None,
                companion_source_files: None, companion_delta_version: None,
                companion_fast_field_mode: None,
            },
            added_at_version: 1,
            added_at_timestamp: 0,
        }
    }

    #[test]
    fn test_group_entries_no_partitions() {
        let entries = vec![
            make_entry("a.split", HashMap::new()),
            make_entry("b.split", HashMap::new()),
        ];
        let groups = group_entries_by_partition(&entries, &[]);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].len(), 2);
    }

    #[test]
    fn test_compute_bounds() {
        let mut pv1 = HashMap::new();
        pv1.insert("year".to_string(), "2023".to_string());
        let mut pv2 = HashMap::new();
        pv2.insert("year".to_string(), "2024".to_string());

        let entries = vec![make_entry("a.split", pv1), make_entry("b.split", pv2)];
        let bounds = compute_partition_bounds(&entries, &["year".to_string()]).unwrap();
        assert_eq!(bounds.min_values.get("year"), Some(&"2023".to_string()));
        assert_eq!(bounds.max_values.get("year"), Some(&"2024".to_string()));
    }

    #[test]
    fn test_compute_bounds_empty() {
        let bounds = compute_partition_bounds(&[], &["year".to_string()]);
        assert!(bounds.is_none());
    }
}
