// delta_reader/scan.rs - Core logic: Snapshot → Scan → file listing
//
// Reads a Delta table's transaction log and returns the list of active
// parquet files with metadata at a given version (or latest).

use std::collections::HashMap;
use anyhow::Result;
use url::Url;

use delta_kernel::snapshot::Snapshot;
use delta_kernel::scan::state::ScanFile;

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
    Ok((entries, actual_version))
}

/// Normalize a table URL, converting bare paths to file:// URLs.
///
/// IMPORTANT: The URL must end with a trailing slash so that `Url::join()`
/// appends child paths instead of replacing the last segment. Delta-kernel
/// internally does `table_root.join("_delta_log/")` which requires this.
fn normalize_url(url_str: &str) -> Result<Url> {
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
