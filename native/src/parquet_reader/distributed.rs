// parquet_reader/distributed.rs - Distributed table scanning primitives for Hive-style parquet
//
// Provides building blocks for distributed Hive-style partitioned parquet directory scanning:
//   1. get_parquet_table_info()   — Driver: lists partition dirs + reads schema from first file
//   2. list_partition_files()     — Executor: lists .parquet files in ONE partition directory

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;
use url::Url;

use crate::debug_println;
use crate::delta_reader::engine::{DeltaStorageConfig, create_object_store};
use crate::parquet_schema_reader::arrow_schema_to_json;

// ─── Data structures ────────────────────────────────────────────────────────

/// Lightweight table metadata returned by get_parquet_table_info().
/// Contains partition directory paths — does NOT list files within partitions.
#[derive(Debug, Clone)]
pub struct ParquetTableInfo {
    /// Arrow schema JSON from the first parquet file's footer
    pub schema_json: String,
    /// Inferred partition column names (from directory key=value patterns)
    pub partition_columns: Vec<String>,
    /// Partition directory paths (key=value/ prefixes)
    pub partition_directories: Vec<String>,
    /// Root-level .parquet files (for unpartitioned tables)
    pub root_parquet_files: Vec<ParquetFileEntry>,
    /// Whether the table is partitioned
    pub is_partitioned: bool,
}

/// A single parquet file entry with metadata.
#[derive(Debug, Clone)]
pub struct ParquetFileEntry {
    /// Full path to the parquet file
    pub path: String,
    /// File size in bytes
    pub size: i64,
    /// Last modified timestamp (epoch millis)
    pub last_modified: i64,
    /// Partition values parsed from path (key=value segments)
    pub partition_values: HashMap<String, String>,
}

// ─── Public API ─────────────────────────────────────────────────────────────

/// Driver-side: Get lightweight table metadata for a Hive-style parquet directory.
///
/// Lists immediate children of root URL using `list_with_delimiter()`.
/// Returns partition directory paths and schema — does NOT recurse into partitions.
pub fn get_parquet_table_info(
    table_url: &str,
    config: &DeltaStorageConfig,
) -> Result<ParquetTableInfo> {
    debug_println!("🔧 PARQUET_DIST: get_table_info for {}", table_url);

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create tokio runtime: {}", e))?;

    rt.block_on(get_table_info_async(table_url, config))
}

/// Executor-side: List all .parquet files under a single partition directory.
///
/// Lists files using `list(prefix)` and parses partition values from path segments.
pub fn list_partition_files(
    table_url: &str,
    config: &DeltaStorageConfig,
    partition_prefix: &str,
) -> Result<Vec<ParquetFileEntry>> {
    debug_println!(
        "🔧 PARQUET_DIST: list_partition_files prefix={}",
        partition_prefix
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create tokio runtime: {}", e))?;

    rt.block_on(list_partition_files_async(table_url, config, partition_prefix))
}

// ─── Internal async functions ───────────────────────────────────────────────

async fn get_table_info_async(
    table_url: &str,
    config: &DeltaStorageConfig,
) -> Result<ParquetTableInfo> {
    let url = normalize_table_url(table_url)?;
    let store = create_object_store(&url, config)?;

    // List immediate children with delimiter (single LIST call, no recursion)
    let prefix = url_to_object_path(&url);
    let list_result = store
        .list_with_delimiter(Some(&prefix))
        .await
        .map_err(|e| anyhow::anyhow!("Failed to list table directory '{}': {}", table_url, e))?;

    debug_println!(
        "🔧 PARQUET_DIST: Found {} prefixes, {} objects at root",
        list_result.common_prefixes.len(),
        list_result.objects.len()
    );

    // Separate partition directories from root files
    let mut partition_directories = Vec::new();
    let mut partition_columns = Vec::new();
    let mut partition_columns_found = false;

    for cp in &list_result.common_prefixes {
        let dir_name = cp.as_ref();
        // Check if this looks like a Hive partition (key=value pattern)
        if let Some(last_segment) = dir_name.rsplit('/').find(|s| !s.is_empty()) {
            if last_segment.contains('=') {
                partition_directories.push(dir_name.to_string());

                // Extract partition column name from first directory
                if !partition_columns_found {
                    // Walk the path to find all partition levels
                    for segment in dir_name.split('/') {
                        if let Some(eq_pos) = segment.find('=') {
                            let key = &segment[..eq_pos];
                            if !partition_columns.contains(&key.to_string()) {
                                partition_columns.push(key.to_string());
                            }
                        }
                    }
                    partition_columns_found = true;
                }
            }
        }
    }

    // Collect root .parquet files
    let mut root_parquet_files = Vec::new();
    for obj in &list_result.objects {
        let path_str = obj.location.as_ref();
        if path_str.ends_with(".parquet") || path_str.ends_with(".parq") {
            root_parquet_files.push(ParquetFileEntry {
                path: path_str.to_string(),
                size: obj.size as i64,
                last_modified: obj.last_modified.timestamp_millis(),
                partition_values: HashMap::new(),
            });
        }
    }

    let is_partitioned = !partition_directories.is_empty();

    // Read schema from first available parquet file
    let schema_json = read_schema_from_first_file(
        &store,
        &list_result,
        &partition_directories,
        &prefix,
    )
    .await
    .unwrap_or_else(|e| {
        debug_println!("🔧 PARQUET_DIST: Could not read schema: {}", e);
        "{}".to_string()
    });

    debug_println!(
        "🔧 PARQUET_DIST: Table info: {} partition dirs, {} root files, partitioned={}",
        partition_directories.len(),
        root_parquet_files.len(),
        is_partitioned
    );

    Ok(ParquetTableInfo {
        schema_json,
        partition_columns,
        partition_directories,
        root_parquet_files,
        is_partitioned,
    })
}

async fn list_partition_files_async(
    table_url: &str,
    config: &DeltaStorageConfig,
    partition_prefix: &str,
) -> Result<Vec<ParquetFileEntry>> {
    let url = normalize_table_url(table_url)?;
    let store = create_object_store(&url, config)?;

    let prefix = ObjectPath::from(partition_prefix);

    let mut entries = Vec::new();

    let objects: Vec<_> = {
        use futures::TryStreamExt;
        store
            .list(Some(&prefix))
            .try_collect()
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to list partition '{}': {}",
                    partition_prefix,
                    e
                )
            })?
    };

    for obj in objects {
        let path_str = obj.location.as_ref();
        if !path_str.ends_with(".parquet") && !path_str.ends_with(".parq") {
            continue;
        }

        // Skip hidden files and metadata
        let filename = path_str.rsplit('/').next().unwrap_or(path_str);
        if filename.starts_with('.') || filename.starts_with('_') {
            continue;
        }

        let partition_values = parse_partition_values_from_path(path_str);

        entries.push(ParquetFileEntry {
            path: path_str.to_string(),
            size: obj.size as i64,
            last_modified: obj.last_modified.timestamp_millis(),
            partition_values,
        });
    }

    debug_println!(
        "🔧 PARQUET_DIST: Listed {} parquet files in partition {}",
        entries.len(),
        partition_prefix
    );

    Ok(entries)
}

/// Read Arrow schema JSON from the first parquet file found.
async fn read_schema_from_first_file(
    store: &Arc<dyn ObjectStore>,
    list_result: &object_store::ListResult,
    partition_directories: &[String],
    _root_prefix: &ObjectPath,
) -> Result<String> {
    use parquet::arrow::async_reader::ParquetObjectReader;

    // Try root-level parquet files first
    for obj in &list_result.objects {
        let path_str = obj.location.as_ref();
        if path_str.ends_with(".parquet") || path_str.ends_with(".parq") {
            let reader = ParquetObjectReader::new(Arc::clone(store), obj.location.clone())
                .with_file_size(obj.size as u64);
            let builder =
                parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(reader)
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to read parquet schema from '{}': {}", path_str, e)
                    })?;

            let schema = builder.schema();
            let schema_json = arrow_schema_to_json(schema.as_ref());
            return Ok(schema_json);
        }
    }

    // If no root files, try first file in first partition directory
    if let Some(first_dir) = partition_directories.first() {
        let prefix = ObjectPath::from(first_dir.as_str());
        use futures::TryStreamExt;
        let objects: Vec<_> = store.list(Some(&prefix)).try_collect().await?;

        for obj in objects {
            let path_str = obj.location.as_ref();
            if path_str.ends_with(".parquet") || path_str.ends_with(".parq") {
                let filename = path_str.rsplit('/').next().unwrap_or(path_str);
                if filename.starts_with('.') || filename.starts_with('_') {
                    continue;
                }

                let reader =
                    ParquetObjectReader::new(Arc::clone(store), obj.location.clone())
                        .with_file_size(obj.size as u64);
                let builder =
                    parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(reader)
                        .await
                        .map_err(|e| {
                            anyhow::anyhow!(
                                "Failed to read parquet schema from '{}': {}",
                                path_str,
                                e
                            )
                        })?;

                let schema = builder.schema();
                let schema_json = arrow_schema_to_json(schema.as_ref());
                return Ok(schema_json);
            }
        }
    }

    Err(anyhow::anyhow!("No parquet files found to read schema from"))
}

// ─── Helpers ────────────────────────────────────────────────────────────────

/// Parse partition key=value pairs from a file path.
///
/// Given a path like `year=2024/month=01/part-00000.parquet`,
/// returns `{"year": "2024", "month": "01"}`.
pub(crate) fn parse_partition_values_from_path(path: &str) -> HashMap<String, String> {
    let mut values = HashMap::new();
    for segment in path.split('/') {
        if let Some(eq_pos) = segment.find('=') {
            let key = &segment[..eq_pos];
            let value = &segment[eq_pos + 1..];
            // URL-decode the value
            let decoded = percent_decode(value);
            values.insert(key.to_string(), decoded);
        }
    }
    values
}

/// Simple percent-decoding for partition values.
fn percent_decode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '%' {
            let hex: String = chars.by_ref().take(2).collect();
            if hex.len() == 2 {
                if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                    result.push(byte as char);
                    continue;
                }
            }
            result.push('%');
            result.push_str(&hex);
        } else {
            result.push(c);
        }
    }
    result
}

/// Normalize a table URL: ensure trailing slash and parse.
fn normalize_table_url(url_str: &str) -> Result<Url> {
    let mut s = url_str.to_string();

    // Add file:// scheme for bare paths
    if s.starts_with('/') {
        s = format!("file://{}", s);
    }

    let mut url = Url::parse(&s)
        .map_err(|e| anyhow::anyhow!("Invalid table URL '{}': {}", url_str, e))?;

    // Ensure trailing slash for directory URLs
    let path = url.path().to_string();
    if !path.ends_with('/') {
        url.set_path(&format!("{}/", path));
    }

    Ok(url)
}

/// Convert a URL to an ObjectPath for object_store operations.
fn url_to_object_path(url: &Url) -> ObjectPath {
    match url.scheme() {
        "s3" | "s3a" => {
            // For S3, the path starts after the bucket name
            let path = url.path();
            let trimmed = path.trim_start_matches('/');
            ObjectPath::from(trimmed)
        }
        "az" | "azure" | "abfs" | "abfss" => {
            let path = url.path();
            let trimmed = path.trim_start_matches('/');
            ObjectPath::from(trimmed)
        }
        _ => {
            // For file:// and others, use the full path
            ObjectPath::from(url.path())
        }
    }
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_partition_values_simple() {
        let values = parse_partition_values_from_path("year=2024/month=01/part-00000.parquet");
        assert_eq!(values.get("year").unwrap(), "2024");
        assert_eq!(values.get("month").unwrap(), "01");
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn test_parse_partition_values_single_level() {
        let values = parse_partition_values_from_path("region=us-east-1/data.parquet");
        assert_eq!(values.get("region").unwrap(), "us-east-1");
        assert_eq!(values.len(), 1);
    }

    #[test]
    fn test_parse_partition_values_no_partitions() {
        let values = parse_partition_values_from_path("data/part-00000.parquet");
        assert!(values.is_empty());
    }

    #[test]
    fn test_parse_partition_values_encoded() {
        let values =
            parse_partition_values_from_path("city=New%20York/part-00000.parquet");
        assert_eq!(values.get("city").unwrap(), "New York");
    }

    #[test]
    fn test_percent_decode() {
        assert_eq!(percent_decode("hello%20world"), "hello world");
        assert_eq!(percent_decode("no%2Fslash"), "no/slash");
        assert_eq!(percent_decode("plain"), "plain");
        assert_eq!(percent_decode(""), "");
    }

    #[test]
    fn test_normalize_table_url_bare_path() {
        let url = normalize_table_url("/tmp/my_table").unwrap();
        assert_eq!(url.scheme(), "file");
        assert!(url.path().ends_with('/'));
    }

    #[test]
    fn test_normalize_table_url_s3() {
        let url = normalize_table_url("s3://bucket/prefix/table").unwrap();
        assert_eq!(url.scheme(), "s3");
        assert!(url.path().ends_with('/'));
    }

    #[test]
    fn test_normalize_table_url_trailing_slash() {
        let url = normalize_table_url("s3://bucket/table/").unwrap();
        assert!(url.path().ends_with('/'));
        // Should not add double slash
        assert!(!url.path().ends_with("//"));
    }

    #[test]
    fn test_parquet_table_info_struct() {
        let info = ParquetTableInfo {
            schema_json: r#"{"fields":[]}"#.to_string(),
            partition_columns: vec!["year".to_string(), "month".to_string()],
            partition_directories: vec![
                "year=2024/month=01/".to_string(),
                "year=2024/month=02/".to_string(),
            ],
            root_parquet_files: vec![],
            is_partitioned: true,
        };

        assert!(info.is_partitioned);
        assert_eq!(info.partition_columns.len(), 2);
        assert_eq!(info.partition_directories.len(), 2);
    }

    #[test]
    fn test_parquet_file_entry_struct() {
        let entry = ParquetFileEntry {
            path: "year=2024/month=01/part-00000.parquet".to_string(),
            size: 1024000,
            last_modified: 1700000000000,
            partition_values: HashMap::from([
                ("year".to_string(), "2024".to_string()),
                ("month".to_string(), "01".to_string()),
            ]),
        };

        assert_eq!(entry.size, 1024000);
        assert_eq!(entry.partition_values.len(), 2);
    }
}
