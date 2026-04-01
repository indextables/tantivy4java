// txlog/avro/state_writer.rs - Write state-v<N>/ directory atomically

use std::collections::HashMap;
use bytes::Bytes;
use apache_avro::types::Value;

use crate::debug_println;
use crate::txlog::actions::*;
use crate::txlog::error::{TxLogError, Result};
use crate::txlog::partition_pruning::compare_values;
use crate::txlog::storage::TxLogStorage;

use super::manifest_writer;

/// Maximum entries per manifest file for partition-level pruning.
const MAX_ENTRIES_PER_MANIFEST: usize = 50_000;

/// Write a complete Avro state checkpoint.
///
/// Creates `state-v<version>/` with manifest files, `_manifest.avro`, and updates `_last_checkpoint`.
/// Only updates `_last_checkpoint` if this version is newer than the existing one.
pub async fn write_state_checkpoint(
    storage: &TxLogStorage,
    version: i64,
    entries: &[FileEntry],
    protocol: &ProtocolAction,
    metadata: &MetadataAction,
) -> Result<LastCheckpointInfo> {
    let last_checkpoint = write_state_directory(storage, version, entries, protocol, metadata).await?;
    write_last_checkpoint_if_newer(storage, &last_checkpoint).await?;
    Ok(last_checkpoint)
}

/// Write an incremental Avro state checkpoint.
///
/// Reuses existing manifest references from `base_manifest`, writes only new manifest(s),
/// accumulates tombstones, and updates `_last_checkpoint`.
/// Only updates `_last_checkpoint` if this version is newer than the existing one.
pub async fn write_incremental_state_checkpoint(
    storage: &TxLogStorage,
    version: i64,
    base_manifest: &StateManifest,
    new_entries: &[FileEntry],
    removed_paths: &std::collections::HashSet<String>,
    protocol: &ProtocolAction,
    metadata: &MetadataAction,
) -> Result<LastCheckpointInfo> {
    let last_checkpoint = write_incremental_state_directory(
        storage, version, base_manifest, new_entries, removed_paths, protocol, metadata,
    ).await?;
    write_last_checkpoint_if_newer(storage, &last_checkpoint).await?;
    Ok(last_checkpoint)
}

/// Write the state directory (manifests + _manifest.avro) WITHOUT updating _last_checkpoint.
///
/// Manifest data files are written to the shared `manifests/` directory.
/// The state directory only contains `_manifest.avro` which references them.
pub async fn write_state_directory(
    storage: &TxLogStorage,
    version: i64,
    entries: &[FileEntry],
    protocol: &ProtocolAction,
    metadata: &MetadataAction,
) -> Result<LastCheckpointInfo> {
    let state_dir = TxLogStorage::state_dir_name(version);
    debug_println!("📝 STATE_WRITER: Writing state dir {} with {} entries", state_dir, entries.len());

    // Group entries by partition key for manifest splitting
    let groups = group_entries_by_partition(entries, &metadata.partition_columns);

    // Write manifest files to shared manifests/ directory
    let mut manifest_infos = Vec::new();
    let mut total_size_bytes: i64 = 0;

    for group in &groups {
        if group.is_empty() {
            continue; // Skip empty groups (defensive — group_entries_by_partition shouldn't produce them)
        }

        let manifest_id = generate_manifest_id();
        let manifest_relative_path = format!("manifests/manifest-{}.avro", manifest_id);
        let avro_bytes = manifest_writer::write_manifest_bytes(group)?;
        total_size_bytes += avro_bytes.len() as i64;

        // Write manifest with put_if_absent for conflict detection.
        // AlreadyExists is handled by TxLogStorage (returns Ok(false)); any Err here is a real storage failure.
        storage.put_if_absent(&manifest_relative_path, Bytes::from(avro_bytes)).await?;

        // Compute partition bounds for this manifest
        let bounds = compute_partition_bounds(group, &metadata.partition_columns);

        manifest_infos.push(ManifestInfo {
            path: manifest_relative_path,
            file_count: group.len() as i64,
            partition_bounds: bounds,
            min_added_at_version: group.iter().map(|e| e.added_at_version).min().unwrap(), // safe: group is non-empty
            max_added_at_version: group.iter().map(|e| e.added_at_version).max().unwrap(), // safe: group is non-empty
            tombstone_count: 0,
            live_entry_count: group.len() as i64,
        });
    }

    // Compute global partition bounds
    let global_bounds = compute_partition_bounds(entries, &metadata.partition_columns);

    let now = current_timestamp_ms();
    let protocol_json = serde_json::to_string(protocol).ok();
    let metadata_json_cached = serde_json::to_string(metadata).ok();

    // Build schema registry from metadata configuration
    let schema_registry: HashMap<String, String> = metadata.configuration.iter()
        .filter(|(k, _)| k.starts_with("docMappingSchema."))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    let state_manifest = StateManifest {
        version,
        manifests: manifest_infos,
        partition_bounds: global_bounds,
        created_time: now,
        total_file_count: entries.len() as i64,
        format: "avro-state".to_string(),
        protocol_json,
        metadata: metadata_json_cached,
        schema_registry,
        tombstones: vec![],
        format_version: 1,
        total_bytes: entries.iter().map(|e| e.add.size).sum(),
        protocol_version: protocol.min_writer_version as i32,
    };

    // Write _manifest.avro with put_if_absent — this is the atomic commit point.
    // If another writer already created this state version, we detect the conflict.
    let avro_bytes = serialize_state_manifest_avro(&state_manifest)?;
    let manifest_path = format!("{}/{}", state_dir, STATE_MANIFEST_FILENAME);
    let written = storage.put_if_absent(&manifest_path, Bytes::from(avro_bytes)).await?;
    if !written {
        debug_println!("⚠️ STATE_WRITER: State manifest already exists at {}, concurrent write detected", manifest_path);
    }

    let last_checkpoint = LastCheckpointInfo {
        version,
        size: entries.len() as i64,
        size_in_bytes: total_size_bytes,
        num_files: entries.len() as i64,
        format: Some("avro-state".to_string()),
        state_dir: Some(state_dir),
        created_time: now,
        parts: None,
        checkpoint_id: None,
    };

    debug_println!("✅ STATE_WRITER: State dir written at version {} ({} manifests, {} entries)",
        version, state_manifest.manifests.len(), entries.len());

    Ok(last_checkpoint)
}

/// Write an incremental state directory that reuses existing manifest references.
///
/// Only writes NEW manifest file(s) for `new_entries`. Accumulates tombstones from
/// `base_manifest.tombstones ∪ removed_paths`.
pub async fn write_incremental_state_directory(
    storage: &TxLogStorage,
    version: i64,
    base_manifest: &StateManifest,
    new_entries: &[FileEntry],
    removed_paths: &std::collections::HashSet<String>,
    protocol: &ProtocolAction,
    metadata: &MetadataAction,
) -> Result<LastCheckpointInfo> {
    let state_dir = TxLogStorage::state_dir_name(version);
    debug_println!("📝 STATE_WRITER: Writing incremental state dir {} with {} new entries, {} removals",
        state_dir, new_entries.len(), removed_paths.len());

    let now = current_timestamp_ms();

    // Start with existing manifest references, normalizing legacy paths
    let mut manifest_infos: Vec<ManifestInfo> = base_manifest.manifests.iter()
        .map(|info| {
            let mut normalized = info.clone();
            // Normalize legacy bare filenames to include their source state dir
            if !normalized.path.contains('/') {
                // Legacy path — prefix with source state dir path
                if let Some(ref base_state_dir) = find_state_dir_for_manifest(base_manifest.version) {
                    normalized.path = format!("{}/{}", base_state_dir, info.path);
                }
            }
            normalized
        })
        .collect();

    // Write new manifest(s) for new_entries to shared manifests/ directory
    let mut total_size_bytes: i64 = base_manifest.total_bytes;

    if !new_entries.is_empty() {
        let groups = group_entries_by_partition(new_entries, &metadata.partition_columns);
        for group in &groups {
            if group.is_empty() {
                continue;
            }

            let manifest_id = generate_manifest_id();
            let manifest_relative_path = format!("manifests/manifest-{}.avro", manifest_id);
            let avro_bytes = manifest_writer::write_manifest_bytes(group)?;
            total_size_bytes += group.iter().map(|e| e.add.size).sum::<i64>();

            // AlreadyExists handled by TxLogStorage (Ok(false)); any Err is a real failure.
            storage.put_if_absent(&manifest_relative_path, Bytes::from(avro_bytes)).await?;

            let bounds = compute_partition_bounds(group, &metadata.partition_columns);

            manifest_infos.push(ManifestInfo {
                path: manifest_relative_path,
                file_count: group.len() as i64,
                partition_bounds: bounds,
                min_added_at_version: group.iter().map(|e| e.added_at_version).min().unwrap(), // safe: non-empty
                max_added_at_version: group.iter().map(|e| e.added_at_version).max().unwrap(), // safe: non-empty
                tombstone_count: 0,
                live_entry_count: group.len() as i64,
            });
        }
    }

    // Accumulate tombstones: base ∪ newly removed (Set dedup, matching Scala)
    let mut tombstone_set: std::collections::HashSet<String> = base_manifest.tombstones.iter().cloned().collect();
    tombstone_set.extend(removed_paths.iter().cloned());
    let tombstones: Vec<String> = tombstone_set.into_iter().collect();

    // Calculate total live file count: sum of all manifest entries - tombstone count
    let total_manifest_entries: i64 = manifest_infos.iter().map(|m| m.file_count).sum();
    let live_file_count = total_manifest_entries - tombstones.len() as i64;

    let protocol_json = serde_json::to_string(protocol).ok();
    let metadata_json_cached = serde_json::to_string(metadata).ok();

    let schema_registry: HashMap<String, String> = metadata.configuration.iter()
        .filter(|(k, _)| k.starts_with("docMappingSchema."))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    let state_manifest = StateManifest {
        version,
        manifests: manifest_infos,
        partition_bounds: None, // Recomputed at read time
        created_time: now,
        total_file_count: live_file_count,
        format: "avro-state".to_string(),
        protocol_json,
        metadata: metadata_json_cached,
        schema_registry,
        tombstones,
        format_version: 1,
        total_bytes: total_size_bytes,
        protocol_version: protocol.min_writer_version as i32,
    };

    // Write _manifest.avro with put_if_absent — atomic commit point
    let avro_bytes = serialize_state_manifest_avro(&state_manifest)?;
    let manifest_path = format!("{}/{}", state_dir, STATE_MANIFEST_FILENAME);
    let written = storage.put_if_absent(&manifest_path, Bytes::from(avro_bytes)).await?;
    if !written {
        debug_println!("⚠️ STATE_WRITER: Incremental state manifest already exists at {}", manifest_path);
    }

    let last_checkpoint = LastCheckpointInfo {
        version,
        size: live_file_count,
        size_in_bytes: total_size_bytes,
        num_files: live_file_count,
        format: Some("avro-state".to_string()),
        state_dir: Some(state_dir),
        created_time: now,
        parts: None,
        checkpoint_id: None,
    };

    debug_println!("✅ STATE_WRITER: Incremental state dir written at version {} ({} manifests, {} tombstones, {} live files)",
        version, state_manifest.manifests.len(), state_manifest.tombstones.len(), live_file_count);

    Ok(last_checkpoint)
}

/// Serialize a StateManifest to Avro binary bytes with zstd compression.
fn serialize_state_manifest_avro(state_manifest: &StateManifest) -> Result<Vec<u8>> {
    use apache_avro::{Writer, Codec};

    let schema = super::schemas::state_manifest_schema();
    let avro_value = state_manifest_to_avro_value(state_manifest, &schema);

    let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Zstandard(Default::default()));
    writer.append(avro_value)
        .map_err(|e| TxLogError::Avro(format!("Failed to write StateManifest Avro record: {}", e)))?;
    writer.into_inner()
        .map_err(|e| TxLogError::Avro(format!("Failed to flush StateManifest Avro writer: {}", e)))
}

/// Convert a StateManifest to an Avro Value matching the Scala schema.
fn state_manifest_to_avro_value(manifest: &StateManifest, _schema: &apache_avro::Schema) -> Value {
    // Build manifests array (nested ManifestInfoItem records)
    let manifests_array: Vec<Value> = manifest.manifests.iter().map(|info| {
        let partition_bounds = match &info.partition_bounds {
            Some(bounds) => {
                let map: HashMap<String, Value> = bounds.min_values.keys()
                    .chain(bounds.max_values.keys())
                    .collect::<std::collections::HashSet<_>>()
                    .into_iter()
                    .map(|col_name| {
                        let min_val = bounds.min_values.get(col_name);
                        let max_val = bounds.max_values.get(col_name);
                        (col_name.clone(), Value::Record(vec![
                            ("min".to_string(), match min_val {
                                Some(s) => Value::Union(1, Box::new(Value::String(s.clone()))),
                                None => Value::Union(0, Box::new(Value::Null)),
                            }),
                            ("max".to_string(), match max_val {
                                Some(s) => Value::Union(1, Box::new(Value::String(s.clone()))),
                                None => Value::Union(0, Box::new(Value::Null)),
                            }),
                        ]))
                    })
                    .collect();
                Value::Union(1, Box::new(Value::Map(map)))
            }
            None => Value::Union(0, Box::new(Value::Null)),
        };

        Value::Record(vec![
            ("path".to_string(), Value::String(info.path.clone())),
            ("numEntries".to_string(), Value::Long(info.file_count)),
            ("minAddedAtVersion".to_string(), Value::Long(info.min_added_at_version)),
            ("maxAddedAtVersion".to_string(), Value::Long(info.max_added_at_version)),
            ("partitionBounds".to_string(), partition_bounds),
            ("tombstoneCount".to_string(), Value::Long(info.tombstone_count)),
            ("liveEntryCount".to_string(), Value::Long(info.live_entry_count)),
        ])
    }).collect();

    // Build tombstones array
    let tombstones_array: Vec<Value> = manifest.tombstones.iter()
        .map(|s| Value::String(s.clone()))
        .collect();

    // Build schemaRegistry map
    let schema_registry_map: HashMap<String, Value> = manifest.schema_registry.iter()
        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
        .collect();

    // Metadata field (nullable string)
    let metadata_value = match &manifest.metadata {
        Some(s) => Value::Union(1, Box::new(Value::String(s.clone()))),
        None => Value::Union(0, Box::new(Value::Null)),
    };

    Value::Record(vec![
        ("formatVersion".to_string(), Value::Int(manifest.format_version)),
        ("stateVersion".to_string(), Value::Long(manifest.version)),
        ("createdAt".to_string(), Value::Long(manifest.created_time)),
        ("numFiles".to_string(), Value::Long(manifest.total_file_count)),
        ("totalBytes".to_string(), Value::Long(manifest.total_bytes)),
        ("manifests".to_string(), Value::Array(manifests_array)),
        ("tombstones".to_string(), Value::Array(tombstones_array)),
        ("schemaRegistry".to_string(), Value::Map(schema_registry_map)),
        ("protocolVersion".to_string(), Value::Int(manifest.protocol_version)),
        ("metadata".to_string(), metadata_value),
    ])
}

/// Generate a short manifest ID from UUID v4.
fn generate_manifest_id() -> String {
    uuid::Uuid::new_v4().to_string().replace('-', "")[..8].to_string()
}

/// Infer the state directory name for a given version.
fn find_state_dir_for_manifest(version: i64) -> Option<String> {
    Some(TxLogStorage::state_dir_name(version))
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
        .unwrap_or_else(|_| {
            debug_println!("⚠️ STATE_WRITER: SystemTime::now() failed, using epoch 0");
            0
        })
}

/// Write _last_checkpoint only if this version is newer than the existing one.
/// Matches Scala's `writeLastCheckpointIfNewer` behavior.
async fn write_last_checkpoint_if_newer(
    storage: &TxLogStorage,
    info: &LastCheckpointInfo,
) -> Result<()> {
    // Read existing checkpoint version
    if let Ok(existing_data) = storage.get("_last_checkpoint").await {
        if let Ok(existing) = serde_json::from_slice::<LastCheckpointInfo>(&existing_data) {
            if existing.version > info.version {
                debug_println!("⚠️ STATE_WRITER: Skipping _last_checkpoint update: existing v{} > new v{}",
                    existing.version, info.version);
                return Ok(());
            }
        }
    }

    let checkpoint_json = serde_json::to_vec(info)
        .map_err(|e| TxLogError::Storage(anyhow::anyhow!("Serialize checkpoint: {}", e)))?;
    storage.put("_last_checkpoint", Bytes::from(checkpoint_json)).await?;
    debug_println!("✅ STATE_WRITER: _last_checkpoint updated to version {}", info.version);
    Ok(())
}

#[cfg(test)]
pub(crate) mod tests {
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
                delete_opstamp: None,
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

    #[test]
    fn test_state_manifest_avro_roundtrip() {
        // Build a StateManifest
        let manifest = StateManifest {
            version: 42,
            manifests: vec![
                ManifestInfo {
                    path: "manifests/manifest-abc12345.avro".to_string(),
                    file_count: 100,
                    partition_bounds: None,
                    min_added_at_version: 1,
                    max_added_at_version: 42,
                    tombstone_count: 0,
                    live_entry_count: 100,
                },
            ],
            partition_bounds: None,
            created_time: 1700000000000,
            total_file_count: 100,
            format: "avro-state".to_string(),
            protocol_json: None,
            metadata: Some(r#"{"id":"test"}"#.to_string()),
            schema_registry: HashMap::new(),
            tombstones: vec!["removed.split".to_string()],
            format_version: 1,
            total_bytes: 50000,
            protocol_version: 4,
        };

        // Serialize to Avro bytes
        let bytes = serialize_state_manifest_avro(&manifest).unwrap();

        // Read back using state_reader
        let parsed = super::super::state_reader::parse_state_manifest(&bytes).unwrap();
        assert_eq!(parsed.version, 42);
        assert_eq!(parsed.manifests.len(), 1);
        assert_eq!(parsed.manifests[0].path, "manifests/manifest-abc12345.avro");
        assert_eq!(parsed.manifests[0].file_count, 100);
        assert_eq!(parsed.total_file_count, 100);
        assert_eq!(parsed.tombstones, vec!["removed.split".to_string()]);
        assert_eq!(parsed.format_version, 1);
        assert_eq!(parsed.total_bytes, 50000);
        assert_eq!(parsed.protocol_version, 4);
        assert_eq!(parsed.metadata, Some(r#"{"id":"test"}"#.to_string()));
    }

    #[test]
    fn test_generate_manifest_id() {
        let id = generate_manifest_id();
        assert_eq!(id.len(), 8);
        // Should be hex characters
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    /// Test helper: serialize a StateManifest to Avro bytes (for use in protocol_regression_tests).
    pub fn serialize_for_test(manifest: &StateManifest) -> Vec<u8> {
        serialize_state_manifest_avro(manifest).unwrap()
    }

    /// Test helper: generate a manifest ID (for use in protocol_regression_tests).
    pub fn generate_manifest_id_for_test() -> String {
        generate_manifest_id()
    }
}
