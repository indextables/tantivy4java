// delta_reader/scan.rs - Core logic: Snapshot → Scan → file listing
//
// Reads a Delta table's transaction log and returns the list of active
// parquet files with metadata at a given version (or latest).

use std::collections::HashMap;
use anyhow::Result;
use serde_json;
use url::Url;

use delta_kernel::snapshot::Snapshot;
use delta_kernel::scan::state::ScanFile;
use delta_kernel::schema::StructField;

use crate::debug_println;
use super::engine::{DeltaStorageConfig, create_engine};

/// Metadata about a single active parquet file in a Delta table.
#[derive(Debug, Clone)]
pub struct DeltaFileEntry {
    /// Parquet file path relative to table root
    pub path: String,
    /// File size in bytes
    pub size: i64,
    /// Epoch millis when the file was created
    pub modification_time: i64,
    /// Number of records (from stats), None if unknown
    pub num_records: Option<u64>,
    /// Partition column name → value mappings
    pub partition_values: HashMap<String, String>,
    /// Whether this file has an associated deletion vector
    pub has_deletion_vector: bool,
}

impl DeltaFileEntry {
    fn from_scan_file(scan_file: ScanFile) -> Self {
        let num_records = scan_file.stats.as_ref().map(|s| s.num_records);
        let has_dv = scan_file.dv_info.has_vector();
        DeltaFileEntry {
            path: scan_file.path,
            size: scan_file.size,
            modification_time: scan_file.modification_time,
            num_records,
            partition_values: scan_file.partition_values,
            has_deletion_vector: has_dv,
        }
    }
}

/// List all active parquet files in a Delta table at the given version.
///
/// Returns `(file_entries, actual_version)` where actual_version is the
/// resolved snapshot version (equals `version` if specified, or latest).
pub fn list_delta_files(
    url_str: &str,
    config: &DeltaStorageConfig,
    version: Option<u64>,
) -> Result<(Vec<DeltaFileEntry>, u64)> {
    debug_println!("🔧 DELTA_SCAN: Listing files for url={}, version={:?}", url_str, version);

    let url = normalize_url(url_str)?;
    let engine = create_engine(&url, config)?;

    // Build snapshot (at specific version or latest)
    let snapshot = {
        let mut builder = Snapshot::builder_for(url);
        if let Some(v) = version {
            builder = builder.at_version(v);
        }
        builder.build(&engine)?
    };
    let actual_version = snapshot.version();
    debug_println!("🔧 DELTA_SCAN: Snapshot at version {}", actual_version);

    // Read schema before building scan (scan_builder consumes snapshot)
    let schema = snapshot.schema();
    let schema_json = serde_json::to_string(schema.as_ref()).unwrap_or_default();

    // Build scan over the snapshot (scan_builder takes Arc<Snapshot>)
    let scan = snapshot.scan_builder().build()?;

    // Iterate scan metadata and collect file entries via visitor callback
    let mut entries: Vec<DeltaFileEntry> = Vec::new();
    let scan_metadata = scan.scan_metadata(&engine)?;

    for meta_result in scan_metadata {
        let meta = meta_result?;
        entries = meta.visit_scan_files(entries, |entries, scan_file| {
            entries.push(DeltaFileEntry::from_scan_file(scan_file));
        })?;
    }

    debug_println!("🔧 DELTA_SCAN: Found {} active files at version {}", entries.len(), actual_version);

    // Defensive column mapping: delta-kernel's ScanFile likely already uses logical
    // names, but apply mapping just in case to handle edge cases.
    let column_mapping = super::distributed::build_column_mapping(&schema_json);
    if !column_mapping.is_empty() {
        debug_println!(
            "🔧 DELTA_SCAN: Applying defensive column mapping ({} entries)",
            column_mapping.len()
        );
        super::distributed::apply_column_mapping(&mut entries, &column_mapping);
    }

    Ok((entries, actual_version))
}

/// A field in the Delta table schema.
#[derive(Debug, Clone)]
pub struct DeltaSchemaField {
    /// Column name
    pub name: String,
    /// Delta type string (e.g. "string", "long", "integer", "boolean", "double",
    /// "float", "short", "byte", "binary", "date", "timestamp", "timestamp_ntz",
    /// or JSON for complex types like struct/array/map)
    pub data_type: String,
    /// Whether the column is nullable
    pub nullable: bool,
    /// Column metadata as JSON string
    pub metadata: String,
}

impl DeltaSchemaField {
    fn from_struct_field(field: &StructField) -> Self {
        let data_type = serde_json::to_string(&field.data_type)
            .unwrap_or_else(|_| "\"unknown\"".to_string());
        // Strip surrounding quotes for primitive types (e.g. "\"string\"" → "string")
        let data_type = if data_type.starts_with('"') && data_type.ends_with('"') {
            data_type[1..data_type.len() - 1].to_string()
        } else {
            data_type
        };
        let metadata = serde_json::to_string(&field.metadata)
            .unwrap_or_else(|_| "{}".to_string());
        DeltaSchemaField {
            name: field.name.clone(),
            data_type,
            nullable: field.nullable,
            metadata,
        }
    }
}

/// Read the schema of a Delta table from its transaction log.
///
/// Returns `(fields, schema_json, actual_version)` where:
/// - `fields` is the list of top-level columns with types
/// - `schema_json` is the full Delta schema as a JSON string (for advanced callers)
/// - `actual_version` is the resolved snapshot version
pub fn read_delta_schema(
    url_str: &str,
    config: &DeltaStorageConfig,
    version: Option<u64>,
) -> Result<(Vec<DeltaSchemaField>, String, u64)> {
    debug_println!("🔧 DELTA_SCHEMA: Reading schema for url={}, version={:?}", url_str, version);

    let url = normalize_url(url_str)?;
    let engine = create_engine(&url, config)?;

    let snapshot = {
        let mut builder = Snapshot::builder_for(url);
        if let Some(v) = version {
            builder = builder.at_version(v);
        }
        builder.build(&engine)?
    };
    let actual_version = snapshot.version();

    let schema = snapshot.schema();
    debug_println!("🔧 DELTA_SCHEMA: Schema has {} fields at version {}", schema.fields().count(), actual_version);

    let fields: Vec<DeltaSchemaField> = schema
        .fields()
        .map(DeltaSchemaField::from_struct_field)
        .collect();

    let schema_json = serde_json::to_string(schema.as_ref())
        .map_err(|e| anyhow::anyhow!("Failed to serialize schema: {}", e))?;

    Ok((fields, schema_json, actual_version))
}

/// Normalize a table URL, converting bare paths to file:// URLs.
///
/// IMPORTANT: The URL must end with a trailing slash so that `Url::join()`
/// appends child paths instead of replacing the last segment. Delta-kernel
/// internally does `table_root.join("_delta_log/")` which requires this.
pub(crate) fn normalize_url(url_str: &str) -> Result<Url> {
    if url_str.starts_with("s3://")
        || url_str.starts_with("s3a://")
        || url_str.starts_with("az://")
        || url_str.starts_with("azure://")
        || url_str.starts_with("abfs://")
        || url_str.starts_with("abfss://")
        || url_str.starts_with("file://")
    {
        let mut url = Url::parse(url_str)
            .map_err(|e| anyhow::anyhow!("Invalid URL '{}': {}", url_str, e))?;
        ensure_trailing_slash(&mut url);
        Ok(url)
    } else {
        // Treat as local path → convert to file:// URL with trailing slash
        let abs_path = std::path::Path::new(url_str)
            .canonicalize()
            .map_err(|e| anyhow::anyhow!("Cannot resolve path '{}': {}", url_str, e))?;
        // Use from_directory_path which adds trailing slash (unlike from_file_path)
        Url::from_directory_path(&abs_path)
            .map_err(|_| anyhow::anyhow!("Cannot convert path to URL: {:?}", abs_path))
    }
}

/// Ensure a URL path ends with '/' so Url::join() appends instead of replacing.
fn ensure_trailing_slash(url: &mut Url) {
    let path = url.path().to_string();
    if !path.ends_with('/') {
        url.set_path(&format!("{}/", path));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_url_trailing_slash() {
        // file:// URL without trailing slash should get one
        let url = normalize_url("file:///tmp/my_table").unwrap();
        assert!(url.path().ends_with('/'), "URL should end with /: {}", url);
        assert_eq!(url.as_str(), "file:///tmp/my_table/");

        // file:// URL with trailing slash should keep it
        let url = normalize_url("file:///tmp/my_table/").unwrap();
        assert_eq!(url.as_str(), "file:///tmp/my_table/");

        // s3:// URL should get trailing slash
        let url = normalize_url("s3://bucket/path/to/table").unwrap();
        assert!(url.path().ends_with('/'), "URL should end with /: {}", url);
    }

    #[test]
    fn test_normalize_url_join_behavior() {
        // The critical test: joining _delta_log/ should append, not replace
        let url = normalize_url("file:///tmp/my_table").unwrap();
        let log_url = url.join("_delta_log/").unwrap();
        assert_eq!(log_url.as_str(), "file:///tmp/my_table/_delta_log/");
    }

    #[test]
    fn test_read_delta_schema_local() {
        // Create a minimal Delta table with a multi-field schema
        let tmp = tempfile::tempdir().unwrap();
        let table_dir = tmp.path().join("test_schema_delta");
        let delta_log = table_dir.join("_delta_log");
        std::fs::create_dir_all(&delta_log).unwrap();

        let schema_string = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"name","type":"string","nullable":true,"metadata":{}},{"name":"score","type":"double","nullable":true,"metadata":{}},{"name":"active","type":"boolean","nullable":false,"metadata":{}}]}"#;
        let commit0 = format!(
            "{}\n{}\n{}",
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            format!(r#"{{"metaData":{{"id":"schema-test","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{}","partitionColumns":[],"configuration":{{}},"createdTime":1700000000000}}}}"#,
                schema_string.replace('"', r#"\""#)),
            r#"{"add":{"path":"data.parquet","partitionValues":{},"size":1000,"modificationTime":1700000000000,"dataChange":true}}"#,
        );
        std::fs::write(delta_log.join("00000000000000000000.json"), &commit0).unwrap();
        std::fs::write(table_dir.join("data.parquet"), &[0u8]).unwrap();

        let config = DeltaStorageConfig::default();
        let (fields, schema_json, version) = read_delta_schema(
            table_dir.to_str().unwrap(),
            &config,
            None,
        ).unwrap();

        assert_eq!(version, 0);
        assert_eq!(fields.len(), 4);

        // Check field names and types
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[0].data_type, "long");
        assert!(!fields[0].nullable);

        assert_eq!(fields[1].name, "name");
        assert_eq!(fields[1].data_type, "string");
        assert!(fields[1].nullable);

        assert_eq!(fields[2].name, "score");
        assert_eq!(fields[2].data_type, "double");
        assert!(fields[2].nullable);

        assert_eq!(fields[3].name, "active");
        assert_eq!(fields[3].data_type, "boolean");
        assert!(!fields[3].nullable);

        // Verify the full schema JSON round-trips
        assert!(schema_json.contains("\"type\":\"struct\""));
        assert!(schema_json.contains("\"name\":\"id\""));
        assert!(schema_json.contains("\"name\":\"score\""));
    }

    #[test]
    fn test_read_delta_schema_complex_types() {
        // Test schema with nested/complex types
        let tmp = tempfile::tempdir().unwrap();
        let table_dir = tmp.path().join("test_complex_schema");
        let delta_log = table_dir.join("_delta_log");
        std::fs::create_dir_all(&delta_log).unwrap();

        let schema_string = r#"{"type":"struct","fields":[{"name":"id","type":"long","nullable":false,"metadata":{}},{"name":"tags","type":{"type":"array","elementType":"string","containsNull":true},"nullable":true,"metadata":{}},{"name":"props","type":{"type":"map","keyType":"string","valueType":"string","valueContainsNull":true},"nullable":true,"metadata":{}}]}"#;
        let commit0 = format!(
            "{}\n{}\n{}",
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            format!(r#"{{"metaData":{{"id":"complex-test","format":{{"provider":"parquet","options":{{}}}},"schemaString":"{}","partitionColumns":[],"configuration":{{}},"createdTime":1700000000000}}}}"#,
                schema_string.replace('"', r#"\""#)),
            r#"{"add":{"path":"data.parquet","partitionValues":{},"size":1000,"modificationTime":1700000000000,"dataChange":true}}"#,
        );
        std::fs::write(delta_log.join("00000000000000000000.json"), &commit0).unwrap();
        std::fs::write(table_dir.join("data.parquet"), &[0u8]).unwrap();

        let config = DeltaStorageConfig::default();
        let (fields, schema_json, _) = read_delta_schema(
            table_dir.to_str().unwrap(),
            &config,
            None,
        ).unwrap();

        assert_eq!(fields.len(), 3);
        assert_eq!(fields[0].name, "id");
        assert_eq!(fields[0].data_type, "long");

        // Complex types should be JSON strings
        assert_eq!(fields[1].name, "tags");
        assert!(fields[1].data_type.contains("array"), "expected array type, got: {}", fields[1].data_type);

        assert_eq!(fields[2].name, "props");
        assert!(fields[2].data_type.contains("map"), "expected map type, got: {}", fields[2].data_type);

        // Full JSON should contain the complex types
        assert!(schema_json.contains("array"));
        assert!(schema_json.contains("map"));
    }

    #[test]
    fn test_list_delta_files_local() {
        // Create a minimal Delta table in a temp dir
        let tmp = tempfile::tempdir().unwrap();
        let table_dir = tmp.path().join("test_delta");
        let delta_log = table_dir.join("_delta_log");
        std::fs::create_dir_all(&delta_log).unwrap();

        // Write commit 0: protocol + metadata + add action
        let commit0 = [
            r#"{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}"#,
            r#"{"metaData":{"id":"test-id","format":{"provider":"parquet","options":{}},"schemaString":"{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"metadata\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":1700000000000}}"#,
            r#"{"add":{"path":"part-00000.parquet","partitionValues":{},"size":5000,"modificationTime":1700000000000,"dataChange":true,"stats":"{\"numRecords\":50}"}}"#,
        ].join("\n");
        std::fs::write(delta_log.join("00000000000000000000.json"), &commit0).unwrap();

        // Create dummy parquet file
        std::fs::write(table_dir.join("part-00000.parquet"), &[0u8]).unwrap();

        // Test listing
        let config = DeltaStorageConfig::default();
        let (entries, version) = list_delta_files(
            table_dir.to_str().unwrap(),
            &config,
            None,
        ).unwrap();

        assert_eq!(version, 0);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].path, "part-00000.parquet");
        assert_eq!(entries[0].size, 5000);
        assert_eq!(entries[0].num_records, Some(50));
        assert!(!entries[0].has_deletion_vector);
    }
}
