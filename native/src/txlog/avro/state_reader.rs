// txlog/avro/state_reader.rs - Read state-v<N>/ directory (manifest list + parts)

use crate::debug_println;
use crate::txlog::actions::{FileEntry, StateManifest};
use crate::txlog::error::{TxLogError, Result};
use crate::txlog::schema_dedup;
use crate::txlog::storage::TxLogStorage;

use super::manifest_reader;

/// Read the StateManifest from a state directory.
///
/// Tries Avro binary format first (Scala-written), falls back to JSON (Rust-written).
pub async fn read_state_manifest(
    storage: &TxLogStorage,
    state_dir: &str,
) -> Result<StateManifest> {
    let manifest_path = format!("{}/{}", state_dir, crate::txlog::actions::STATE_MANIFEST_FILENAME);
    let data = storage.get(&manifest_path).await?;
    let manifest = parse_state_manifest(&data)?;
    debug_println!("📖 STATE_READER: Read state manifest at {} with {} manifests, {} total files",
        state_dir, manifest.manifests.len(), manifest.total_file_count);
    Ok(manifest)
}

/// Parse a state manifest from Avro binary bytes.
pub fn parse_state_manifest(data: &[u8]) -> Result<StateManifest> {
    parse_state_manifest_avro(data)
}

/// Parse a StateManifest from Avro binary format (Scala-written _manifest.avro).
fn parse_state_manifest_avro(data: &[u8]) -> Result<StateManifest> {
    use apache_avro::Reader;

    let reader = Reader::new(data)
        .map_err(|e| TxLogError::Avro(format!("Failed to open Avro reader for StateManifest: {}", e)))?;

    let mut manifest: Option<StateManifest> = None;
    for value_result in reader {
        let value = value_result
            .map_err(|e| TxLogError::Avro(format!("Failed to read StateManifest record: {}", e)))?;
        manifest = Some(avro_value_to_state_manifest(&value)?);
        break; // Only one record expected
    }

    manifest.ok_or_else(|| TxLogError::Avro("Empty StateManifest Avro file".to_string()))
}

/// Convert an Avro Value (Record) to a StateManifest.
fn avro_value_to_state_manifest(value: &apache_avro::types::Value) -> Result<StateManifest> {
    use apache_avro::types::Value;
    use std::collections::HashMap;

    let record = match value {
        Value::Record(fields) => fields,
        _ => return Err(TxLogError::Avro("Expected Avro Record for StateManifest".to_string())),
    };

    let field_map: HashMap<&str, &Value> = record.iter()
        .map(|(name, val)| (name.as_str(), val))
        .collect();

    // Parse manifests array (nested ManifestInfoItem records)
    let manifests = parse_manifest_info_array(field_map.get("manifests"))?;

    // Parse tombstones array
    let tombstones = match field_map.get("tombstones") {
        Some(Value::Array(arr)) => arr.iter()
            .filter_map(|v| if let Value::String(s) = v { Some(s.clone()) } else { None })
            .collect(),
        _ => vec![],
    };

    // Parse schemaRegistry map
    let schema_registry = match field_map.get("schemaRegistry") {
        Some(Value::Map(m)) => m.iter()
            .filter_map(|(k, v)| {
                if let Value::String(s) = v { Some((k.clone(), s.clone())) } else { None }
            })
            .collect(),
        _ => HashMap::new(),
    };

    // Parse metadata (nullable string)
    let metadata = get_avro_optional_string(&field_map, "metadata");

    // Parse protocolVersion — Scala stores this; Rust uses it to set protocol_version
    let protocol_version = match field_map.get("protocolVersion") {
        Some(Value::Int(n)) => *n,
        Some(Value::Long(n)) => *n as i32,
        _ => 4,
    };

    let format_version = match field_map.get("formatVersion") {
        Some(Value::Int(n)) => *n,
        Some(Value::Long(n)) => *n as i32,
        _ => 1,
    };

    let version = match field_map.get("stateVersion") {
        Some(Value::Long(n)) => *n,
        Some(Value::Int(n)) => *n as i64,
        _ => 0,
    };

    let created_time = match field_map.get("createdAt") {
        Some(Value::Long(n)) => *n,
        Some(Value::Int(n)) => *n as i64,
        _ => 0,
    };

    let total_file_count = match field_map.get("numFiles") {
        Some(Value::Long(n)) => *n,
        Some(Value::Int(n)) => *n as i64,
        _ => 0,
    };

    let total_bytes = match field_map.get("totalBytes") {
        Some(Value::Long(n)) => *n,
        Some(Value::Int(n)) => *n as i64,
        _ => 0,
    };

    // Construct protocol_json from protocolVersion so distributed.rs can parse it
    let protocol_json = {
        let protocol = crate::txlog::actions::ProtocolAction {
            min_reader_version: protocol_version as u32,
            min_writer_version: protocol_version as u32,
            reader_features: vec![],
            writer_features: vec![],
        };
        serde_json::to_string(&protocol).ok()
    };

    Ok(StateManifest {
        version,
        manifests,
        partition_bounds: None, // Global bounds not in Scala schema; computed from manifests
        created_time,
        total_file_count,
        format: "avro-state".to_string(),
        protocol_json,
        metadata,
        schema_registry,
        tombstones,
        format_version,
        total_bytes,
        protocol_version,
    })
}

/// Parse the manifests array from the StateManifest Avro record.
fn parse_manifest_info_array(value: Option<&&apache_avro::types::Value>) -> Result<Vec<crate::txlog::actions::ManifestInfo>> {
    use apache_avro::types::Value;
    use std::collections::HashMap;
    use crate::txlog::actions::ManifestInfo;

    let arr = match value {
        Some(Value::Array(arr)) => arr,
        _ => return Ok(vec![]),
    };

    let mut infos = Vec::with_capacity(arr.len());
    for item in arr {
        let record = match item {
            Value::Record(fields) => fields,
            _ => continue,
        };
        let fm: HashMap<&str, &Value> = record.iter()
            .map(|(name, val)| (name.as_str(), val))
            .collect();

        let path = match fm.get("path") {
            Some(Value::String(s)) => s.clone(),
            _ => continue,
        };
        let file_count = match fm.get("numEntries") {
            Some(Value::Long(n)) => *n,
            Some(Value::Int(n)) => *n as i64,
            _ => 0,
        };
        let min_added_at_version = match fm.get("minAddedAtVersion") {
            Some(Value::Long(n)) => *n,
            Some(Value::Int(n)) => *n as i64,
            _ => 0,
        };
        let max_added_at_version = match fm.get("maxAddedAtVersion") {
            Some(Value::Long(n)) => *n,
            Some(Value::Int(n)) => *n as i64,
            _ => 0,
        };
        let tombstone_count = match fm.get("tombstoneCount") {
            Some(Value::Long(n)) => *n,
            Some(Value::Int(n)) => *n as i64,
            _ => 0,
        };
        let live_entry_count = match fm.get("liveEntryCount") {
            Some(Value::Long(n)) => *n,
            Some(Value::Int(n)) => *n as i64,
            _ => -1,
        };

        // Parse partition bounds: Map<columnName, {min, max}>
        let partition_bounds = parse_partition_bounds_map(fm.get("partitionBounds"));

        infos.push(ManifestInfo {
            path,
            file_count,
            partition_bounds,
            min_added_at_version,
            max_added_at_version,
            tombstone_count,
            live_entry_count,
        });
    }

    Ok(infos)
}

/// Parse Scala's partitionBounds: Map<String, {min: Option<String>, max: Option<String>}>
/// into Rust's PartitionBounds { min_values, max_values }.
fn parse_partition_bounds_map(value: Option<&&apache_avro::types::Value>) -> Option<crate::txlog::actions::PartitionBounds> {
    use apache_avro::types::Value;
    use std::collections::HashMap;
    use crate::txlog::actions::PartitionBounds;

    let map = match value {
        Some(Value::Union(_, inner)) => match inner.as_ref() {
            Value::Map(m) => m,
            Value::Null => return None,
            _ => return None,
        },
        Some(Value::Map(m)) => m,
        _ => return None,
    };

    if map.is_empty() {
        return None;
    }

    let mut min_values = HashMap::new();
    let mut max_values = HashMap::new();

    for (col_name, bounds_value) in map {
        if let Value::Record(fields) = bounds_value {
            let fm: HashMap<&str, &Value> = fields.iter()
                .map(|(name, val)| (name.as_str(), val))
                .collect();
            if let Some(min_str) = get_avro_optional_string(&fm, "min") {
                min_values.insert(col_name.clone(), min_str);
            }
            if let Some(max_str) = get_avro_optional_string(&fm, "max") {
                max_values.insert(col_name.clone(), max_str);
            }
        }
    }

    if min_values.is_empty() && max_values.is_empty() {
        None
    } else {
        Some(PartitionBounds { min_values, max_values })
    }
}

/// Extract an optional string from an Avro field map (handles Union(null, string)).
fn get_avro_optional_string(map: &std::collections::HashMap<&str, &apache_avro::types::Value>, key: &str) -> Option<String> {
    use apache_avro::types::Value;
    match map.get(key) {
        Some(Value::Union(_, inner)) => match inner.as_ref() {
            Value::String(s) => Some(s.clone()),
            Value::Null => None,
            _ => None,
        },
        Some(Value::String(s)) => Some(s.clone()),
        _ => None,
    }
}

/// Read all manifests in a state directory and return all FileEntry records.
/// Reads manifests concurrently with a bounded concurrency limit to avoid
/// overwhelming S3 with hundreds of concurrent GETs.
///
/// Automatically applies tombstone filtering if tombstones are present.
pub async fn read_all_manifests(
    storage: &TxLogStorage,
    state_dir: &str,
    state_manifest: &StateManifest,
    metadata_config: &std::collections::HashMap<String, String>,
) -> Result<Vec<FileEntry>> {
    use futures::stream::{self, StreamExt};

    const MAX_CONCURRENT_MANIFEST_READS: usize = 16;

    let manifest_futures = state_manifest.manifests.iter().map(|info| {
        let manifest_path = resolve_manifest_path(state_dir, &info.path);
        let storage_ref = storage;
        async move {
            let data = storage_ref.get(&manifest_path).await?;
            manifest_reader::read_manifest_bytes(&data)
        }
    });

    let results: Vec<Result<Vec<FileEntry>>> = stream::iter(manifest_futures)
        .buffer_unordered(MAX_CONCURRENT_MANIFEST_READS)
        .collect()
        .await;

    let mut all_entries = Vec::with_capacity(state_manifest.total_file_count as usize);
    for result in results {
        let mut entries = result?;
        all_entries.append(&mut entries);
    }

    // Restore schemas from dedup refs
    schema_dedup::restore_schemas(&mut all_entries, metadata_config);

    // Apply tombstones — filter out removed files
    if !state_manifest.tombstones.is_empty() {
        let tombstone_set: std::collections::HashSet<&str> =
            state_manifest.tombstones.iter().map(|s| s.as_str()).collect();
        let before = all_entries.len();
        all_entries.retain(|entry| !tombstone_set.contains(entry.add.path.as_str()));
        debug_println!("📖 STATE_READER: Applied {} tombstones, filtered {} → {} entries",
            state_manifest.tombstones.len(), before, all_entries.len());
    }

    debug_println!("📖 STATE_READER: Read {} total entries from {} manifests",
        all_entries.len(), state_manifest.manifests.len());
    Ok(all_entries)
}

/// Read a single manifest from a state directory.
pub async fn read_single_manifest(
    storage: &TxLogStorage,
    state_dir: &str,
    manifest_path: &str,
    metadata_config: &std::collections::HashMap<String, String>,
) -> Result<Vec<FileEntry>> {
    let full_path = resolve_manifest_path(state_dir, manifest_path);
    let data = storage.get(&full_path).await?;
    let mut entries = manifest_reader::read_manifest_bytes(&data)?;
    schema_dedup::restore_schemas(&mut entries, metadata_config);
    Ok(entries)
}

/// Resolve a manifest file path relative to the txlog root.
///
/// Manifest paths in the StateManifest may be:
/// - `manifests/manifest-{id}.avro` — shared directory (already relative to txlog root)
/// - `state-vN/manifest-{id}.avro` — normalized legacy (already relative to txlog root)
/// - `manifest-NNNN.avro` — bare filename (relative to state dir, needs prefix)
pub fn resolve_manifest_path(state_dir: &str, manifest_path: &str) -> String {
    if manifest_path.contains('/') {
        // Already contains a directory component — treat as relative to txlog root
        manifest_path.to_string()
    } else {
        // Bare filename — relative to state dir
        format!("{}/{}", state_dir, manifest_path)
    }
}
