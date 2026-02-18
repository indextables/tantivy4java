// iceberg_reader/distributed.rs - Distributed table scanning primitives for Iceberg
//
// Provides building blocks for distributed Iceberg table scanning:
//   1. get_iceberg_snapshot_info()  — Driver: reads catalog → manifest list paths
//   2. read_iceberg_manifest()      — Executor: reads ONE manifest avro file

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use iceberg::Catalog;
use iceberg::spec::ManifestStatus;
use iceberg::table::Table;
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

    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| anyhow::anyhow!("Failed to create Tokio runtime: {}", e))?;

    rt.block_on(async {
        let catalog = create_catalog(catalog_name, config).await?;
        get_snapshot_info_with_catalog(catalog.as_ref(), namespace, table_name, snapshot_id).await
    })
}

/// Executor-side: Read one manifest avro file and extract file entries.
///
/// Reads a single manifest file from storage using the catalog's FileIO config.
/// Returns Vec<IcebergFileEntry> — typically thousands per manifest.
pub fn read_iceberg_manifest(
    catalog_name: &str,
    config: &HashMap<String, String>,
    namespace: &str,
    table_name: &str,
    manifest_path: &str,
) -> Result<Vec<IcebergFileEntry>> {
    debug_println!(
        "🔧 ICEBERG_DIST: read_manifest path={}",
        manifest_path
    );

    let rt = tokio::runtime::Runtime::new()
        .map_err(|e| anyhow::anyhow!("Failed to create Tokio runtime: {}", e))?;

    rt.block_on(async {
        let catalog = create_catalog(catalog_name, config).await?;
        let ns = parse_namespace(namespace);
        let table_ident = TableIdent::new(ns, table_name.to_string());
        let table = catalog.load_table(&table_ident).await
            .map_err(|e| anyhow::anyhow!("Failed to load table: {}", e))?;

        read_manifest_with_table(&table, manifest_path).await
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

/// Read one manifest file using a Table's FileIO.
async fn read_manifest_with_table(
    table: &Table,
    manifest_path: &str,
) -> Result<Vec<IcebergFileEntry>> {
    let file_io = table.file_io();

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
