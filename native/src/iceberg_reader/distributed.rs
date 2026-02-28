// iceberg_reader/distributed.rs - Distributed table scanning primitives for Iceberg
//
// Provides building blocks for distributed Iceberg table scanning:
//   1. get_iceberg_snapshot_info()  — Driver: reads catalog → manifest list paths
//   2. read_iceberg_manifest()      — Executor: reads ONE manifest avro file

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use iceberg::Catalog;
use iceberg::io::FileIO;
use iceberg::spec::ManifestStatus;
use iceberg::TableIdent;

use crate::debug_println;
use super::catalog::create_catalog;
use super::scan::{
    parse_namespace, format_to_string, content_type_to_string, literal_to_string,
    IcebergFileEntry,
};

// ─── Data structures ────────────────────────────────────────────────────────

/// Lightweight snapshot metadata returned by get_iceberg_snapshot_info().
/// Contains manifest file paths — does NOT read manifest contents.
#[derive(Debug, Clone)]
pub struct IcebergSnapshotInfo {
    /// Resolved snapshot ID
    pub snapshot_id: i64,
    /// Full Iceberg schema as JSON
    pub schema_json: String,
    /// Partition spec as JSON
    pub partition_spec_json: String,
    /// Manifest file entries with metadata
    pub manifest_entries: Vec<ManifestFileInfo>,
}

/// Metadata about a single manifest file (from the manifest list).
#[derive(Debug, Clone)]
pub struct ManifestFileInfo {
    /// Full path to the manifest avro file
    pub manifest_path: String,
    /// File size in bytes
    pub manifest_length: i64,
    /// Snapshot that added this manifest
    pub added_snapshot_id: i64,
    /// Number of files with Added status in this manifest
    pub added_files_count: i64,
    /// Number of files with Existing status in this manifest
    pub existing_files_count: i64,
    /// Number of files with Deleted status in this manifest
    pub deleted_files_count: i64,
    /// Partition spec ID for this manifest
    pub partition_spec_id: i32,
}

// ─── Public API ─────────────────────────────────────────────────────────────

/// Driver-side: Get lightweight snapshot metadata for an Iceberg table.
///
/// Opens catalog → table → resolves snapshot → reads manifest list.
/// Returns manifest file paths and metadata — does NOT read manifest contents.
pub fn get_iceberg_snapshot_info(
    catalog_name: &str,
    config: &HashMap<String, String>,
    namespace: &str,
    table_name: &str,
    snapshot_id: Option<i64>,
) -> Result<IcebergSnapshotInfo> {
    debug_println!(
        "🔧 ICEBERG_DIST: get_snapshot_info for {}.{} snapshot={:?}",
        namespace, table_name, snapshot_id
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create Tokio runtime: {}", e))?;

    rt.block_on(async {
        let catalog = create_catalog(catalog_name, config).await?;
        get_snapshot_info_with_catalog(catalog.as_ref(), namespace, table_name, snapshot_id).await
    })
}

/// Executor-side: Read one manifest avro file and extract file entries.
///
/// Creates a FileIO directly from config properties, avoiding the expensive
/// catalog lookup + table load that would add 2 network round-trips per call.
/// The config map's storage credential keys (s3.access-key-id, s3.region, etc.)
/// are passed directly to FileIO.
pub fn read_iceberg_manifest(
    config: &HashMap<String, String>,
    manifest_path: &str,
) -> Result<Vec<IcebergFileEntry>> {
    debug_println!(
        "🔧 ICEBERG_DIST: read_manifest path={}",
        manifest_path
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create Tokio runtime: {}", e))?;

    rt.block_on(async {
        // Build FileIO directly from config properties — no catalog or table load needed.
        // The config map already contains the storage credential keys (s3.access-key-id,
        // s3.secret-access-key, s3.region, adls.account-name, etc.) that FileIO needs.
        let file_io = FileIO::from_path(manifest_path)
            .map_err(|e| anyhow::anyhow!("Failed to create FileIO for {}: {}", manifest_path, e))?
            .with_props(config.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build FileIO: {}", e))?;

        read_manifest_with_file_io(&file_io, manifest_path).await
    })
}

// ─── Internal async functions ───────────────────────────────────────────────

/// Get snapshot info with a catalog reference (testable with MemoryCatalog).
pub(crate) async fn get_snapshot_info_with_catalog(
    catalog: &dyn Catalog,
    namespace: &str,
    table_name: &str,
    snapshot_id: Option<i64>,
) -> Result<IcebergSnapshotInfo> {
    let ns = parse_namespace(namespace);
    let table_ident = TableIdent::new(ns, table_name.to_string());
    let table = catalog.load_table(&table_ident).await
        .map_err(|e| anyhow::anyhow!("Failed to load table {}.{}: {}", namespace, table_name, e))?;

    let metadata = table.metadata();

    // Resolve snapshot
    let snapshot = match snapshot_id {
        Some(id) => metadata
            .snapshot_by_id(id)
            .ok_or_else(|| anyhow::anyhow!("Snapshot {} not found", id))?,
        None => metadata
            .current_snapshot()
            .ok_or_else(|| anyhow::anyhow!("Table has no current snapshot"))?,
    };
    let actual_snap_id = snapshot.snapshot_id();

    // Get schema JSON
    let schema = match snapshot.schema_id() {
        Some(schema_id) => metadata
            .schema_by_id(schema_id)
            .unwrap_or_else(|| metadata.current_schema()),
        None => metadata.current_schema(),
    };
    let schema_json = serde_json::to_string(schema.as_ref())
        .unwrap_or_else(|_| "{}".to_string());

    // Get partition spec JSON
    let partition_spec = metadata.default_partition_spec();
    let partition_spec_json = serde_json::to_string(partition_spec)
        .unwrap_or_else(|_| "{}".to_string());

    // Load manifest list
    let file_io = table.file_io();
    let manifest_list = snapshot
        .load_manifest_list(file_io, metadata)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to load manifest list: {}", e))?;

    let mut manifest_entries = Vec::with_capacity(manifest_list.entries().len());

    for mf in manifest_list.entries() {
        manifest_entries.push(ManifestFileInfo {
            manifest_path: mf.manifest_path.clone(),
            manifest_length: mf.manifest_length,
            added_snapshot_id: mf.added_snapshot_id,
            added_files_count: mf.added_files_count.unwrap_or(0) as i64,
            existing_files_count: mf.existing_files_count.unwrap_or(0) as i64,
            deleted_files_count: mf.deleted_files_count.unwrap_or(0) as i64,
            partition_spec_id: mf.partition_spec_id,
        });
    }

    debug_println!(
        "🔧 ICEBERG_DIST: Snapshot {} has {} manifests",
        actual_snap_id,
        manifest_entries.len()
    );

    Ok(IcebergSnapshotInfo {
        snapshot_id: actual_snap_id,
        schema_json,
        partition_spec_json,
        manifest_entries,
    })
}

/// Read one manifest file using a FileIO instance.
async fn read_manifest_with_file_io(
    file_io: &FileIO,
    manifest_path: &str,
) -> Result<Vec<IcebergFileEntry>> {
    let manifest_input = file_io.new_input(manifest_path)
        .map_err(|e| anyhow::anyhow!("Failed to open manifest {}: {}", manifest_path, e))?;
    let manifest_bytes = manifest_input.read().await
        .map_err(|e| anyhow::anyhow!("Failed to read manifest {}: {}", manifest_path, e))?;

    let manifest = iceberg::spec::Manifest::parse_avro(manifest_bytes.as_ref())
        .map_err(|e| anyhow::anyhow!("Failed to parse manifest: {}", e))?;

    let manifest_partition_spec = manifest.metadata().partition_spec();
    let mut entries = Vec::new();

    for entry in manifest.entries() {
        if entry.status() == ManifestStatus::Deleted {
            continue;
        }

        let data_file = entry.data_file();

        let mut partition_values = HashMap::new();
        let partition_fields = data_file.partition().fields();
        for (idx, spec_field) in manifest_partition_spec.fields().iter().enumerate() {
            if let Some(Some(literal)) = partition_fields.get(idx) {
                partition_values.insert(
                    spec_field.name.clone(),
                    literal_to_string(literal),
                );
            }
        }

        entries.push(IcebergFileEntry {
            path: data_file.file_path().to_string(),
            file_format: format_to_string(data_file.file_format()),
            record_count: data_file.record_count() as i64,
            file_size_bytes: data_file.file_size_in_bytes() as i64,
            partition_values,
            content_type: content_type_to_string(data_file.content_type()),
            snapshot_id: entry.snapshot_id().unwrap_or(0),
        });
    }

    debug_println!(
        "🔧 ICEBERG_DIST: Read {} entries from manifest {}",
        entries.len(),
        manifest_path
    );

    Ok(entries)
}

// ─── Arrow FFI export ────────────────────────────────────────────────────────

/// Read an Iceberg manifest and export filtered entries via Arrow FFI.
///
/// Builds a flat RecordBatch with 7 columns:
///   path (Utf8), file_format (Utf8), record_count (Int64),
///   file_size_bytes (Int64), partition_values (Utf8/JSON),
///   content_type (Utf8), snapshot_id (Int64)
///
/// Returns the number of rows written.
pub fn read_iceberg_manifest_arrow_ffi(
    config: &HashMap<String, String>,
    manifest_path: &str,
    predicate: Option<&crate::common::PartitionPredicate>,
    array_addrs: &[i64],
    schema_addrs: &[i64],
) -> Result<usize> {
    use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
    use arrow_array::{StringArray, Int64Array, Array};
    use arrow_schema::{DataType, Field};

    const NUM_COLS: usize = 7;

    if array_addrs.len() < NUM_COLS || schema_addrs.len() < NUM_COLS {
        anyhow::bail!(
            "Insufficient FFI addresses: need {} but got {} array_addrs and {} schema_addrs",
            NUM_COLS, array_addrs.len(), schema_addrs.len()
        );
    }

    // 1. Read manifest → Vec<IcebergFileEntry>
    let entries = read_iceberg_manifest(config, manifest_path)?;

    // 2. Apply partition predicate filter
    let entries: Vec<IcebergFileEntry> = match predicate {
        Some(pred) => entries.into_iter().filter(|e| pred.evaluate(&e.partition_values)).collect(),
        None => entries,
    };

    let num_rows = entries.len();

    // 3. Build flat Arrow arrays
    let paths: Vec<&str> = entries.iter().map(|e| e.path.as_str()).collect();
    let formats: Vec<&str> = entries.iter().map(|e| e.file_format.as_str()).collect();
    let rec_counts: Vec<i64> = entries.iter().map(|e| e.record_count).collect();
    let file_sizes: Vec<i64> = entries.iter().map(|e| e.file_size_bytes).collect();
    let pvs: Vec<String> = entries.iter().map(|e| {
        serde_json::to_string(&e.partition_values).unwrap_or_else(|_| "{}".to_string())
    }).collect();
    let pv_refs: Vec<&str> = pvs.iter().map(|s| s.as_str()).collect();
    let content_types: Vec<&str> = entries.iter().map(|e| e.content_type.as_str()).collect();
    let snap_ids: Vec<i64> = entries.iter().map(|e| e.snapshot_id).collect();

    let arrays: Vec<(Arc<dyn Array>, Field)> = vec![
        (Arc::new(StringArray::from(paths)), Field::new("path", DataType::Utf8, false)),
        (Arc::new(StringArray::from(formats)), Field::new("file_format", DataType::Utf8, false)),
        (Arc::new(Int64Array::from(rec_counts)), Field::new("record_count", DataType::Int64, false)),
        (Arc::new(Int64Array::from(file_sizes)), Field::new("file_size_bytes", DataType::Int64, false)),
        (Arc::new(StringArray::from(pv_refs)), Field::new("partition_values", DataType::Utf8, false)),
        (Arc::new(StringArray::from(content_types)), Field::new("content_type", DataType::Utf8, false)),
        (Arc::new(Int64Array::from(snap_ids)), Field::new("snapshot_id", DataType::Int64, false)),
    ];

    // 4. Export each column via FFI
    for (i, (array, field)) in arrays.iter().enumerate() {
        if array_addrs[i] == 0 || schema_addrs[i] == 0 {
            anyhow::bail!(
                "Null FFI address for column {}: array_addr={}, schema_addr={}",
                i, array_addrs[i], schema_addrs[i]
            );
        }

        let data = array.to_data();
        let array_ptr = array_addrs[i] as *mut FFI_ArrowArray;
        let schema_ptr = schema_addrs[i] as *mut FFI_ArrowSchema;

        unsafe {
            std::ptr::write_unaligned(array_ptr, FFI_ArrowArray::new(&data));
            std::ptr::write_unaligned(
                schema_ptr,
                FFI_ArrowSchema::try_from(field)
                    .map_err(|e| anyhow::anyhow!("FFI_ArrowSchema failed for col {}: {}", i, e))?,
            );
        }
    }

    Ok(num_rows)
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_file_info_struct() {
        let info = ManifestFileInfo {
            manifest_path: "s3://bucket/metadata/snap-1234-m0.avro".to_string(),
            manifest_length: 50000,
            added_snapshot_id: 1234,
            added_files_count: 100,
            existing_files_count: 0,
            deleted_files_count: 5,
            partition_spec_id: 0,
        };

        assert_eq!(info.manifest_path, "s3://bucket/metadata/snap-1234-m0.avro");
        assert_eq!(info.manifest_length, 50000);
        assert_eq!(info.added_files_count, 100);
        assert_eq!(info.deleted_files_count, 5);
    }

    #[test]
    fn test_iceberg_snapshot_info_struct() {
        let info = IcebergSnapshotInfo {
            snapshot_id: 12345,
            schema_json: r#"{"type":"struct"}"#.to_string(),
            partition_spec_json: "{}".to_string(),
            manifest_entries: vec![
                ManifestFileInfo {
                    manifest_path: "m0.avro".to_string(),
                    manifest_length: 1000,
                    added_snapshot_id: 12345,
                    added_files_count: 50,
                    existing_files_count: 0,
                    deleted_files_count: 0,
                    partition_spec_id: 0,
                },
            ],
        };

        assert_eq!(info.snapshot_id, 12345);
        assert_eq!(info.manifest_entries.len(), 1);
    }
}
