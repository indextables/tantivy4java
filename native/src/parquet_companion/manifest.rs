// manifest.rs - Data structures for the parquet companion manifest
//
// The manifest is embedded in the split bundle as `_parquet_manifest.json`
// and describes the mapping between tantivy documents and external parquet files.

use std::collections::HashMap;
use serde::{Serialize, Deserialize};

/// Current manifest format version
pub const SUPPORTED_MANIFEST_VERSION: u32 = 1;

/// Fast field sourcing mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FastFieldMode {
    /// All fast fields from native tantivy columnar (default, no parquet needed for fast fields)
    Disabled,
    /// Numeric/bool/date/ip from native, string/bytes from parquet
    Hybrid,
    /// All fast fields decoded from parquet at query time
    ParquetOnly,
}

impl Default for FastFieldMode {
    fn default() -> Self {
        FastFieldMode::Disabled
    }
}

/// Maps a tantivy segment + local doc_id range to a contiguous range of global rows
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentRowRange {
    /// Tantivy segment ordinal
    pub segment_ord: u32,
    /// Starting global row index for this segment
    pub row_offset: u64,
    /// Number of rows (documents) in this segment
    pub num_rows: u64,
}

/// Describes a single external parquet file referenced by this split
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetFileEntry {
    /// Relative path from table_root (e.g. "data/part-00001.parquet")
    pub relative_path: String,
    /// Expected file size in bytes (for staleness detection)
    pub file_size_bytes: u64,
    /// Starting global row offset for this file
    pub row_offset: u64,
    /// Number of rows in this file
    pub num_rows: u64,
    /// Whether this file has an offset index (for surgical page access)
    pub has_offset_index: bool,
    /// Per-row-group metadata
    pub row_groups: Vec<RowGroupEntry>,
}

/// Metadata for a single row group within a parquet file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowGroupEntry {
    /// Row group index within the file
    pub row_group_idx: usize,
    /// Number of rows in this row group
    pub num_rows: u64,
    /// Starting row offset within the file
    pub row_offset_in_file: u64,
    /// Per-column chunk info
    pub columns: Vec<ColumnChunkInfo>,
}

/// Byte range info for a column chunk within a row group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnChunkInfo {
    /// Column index in the parquet schema
    pub column_idx: usize,
    /// Column name
    pub column_name: String,
    /// Byte offset of the column chunk data pages
    pub data_page_offset: u64,
    /// Total compressed size of the column chunk
    pub compressed_size: u64,
    /// Total uncompressed size
    pub uncompressed_size: u64,
}

/// Maps a tantivy field to a parquet column
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnMapping {
    /// Tantivy field name
    pub tantivy_field_name: String,
    /// Parquet column name (may differ if field ID mapping is active)
    pub parquet_column_name: String,
    /// Physical column ordinal in the parquet schema
    pub physical_ordinal: usize,
    /// Parquet physical type string (e.g. "INT64", "BYTE_ARRAY")
    pub parquet_type: String,
    /// Tantivy field type string (e.g. "I64", "F64", "Str", "Bool", "Date")
    pub tantivy_type: String,
    /// Optional Iceberg field ID
    pub field_id: Option<i32>,
    /// Fast field tokenizer for text fields (e.g. "raw", "default").
    /// When set, transcoding applies this tokenizer to produce individual term entries
    /// in the columnar dictionary, matching what tantivy does during indexing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast_field_tokenizer: Option<String>,
}

/// Storage configuration metadata embedded in the manifest
/// (describes how to access the parquet files, without actual credentials)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetStorageConfigMeta {
    /// Storage protocol: "local", "s3", "azure"
    pub protocol: String,
    /// S3 bucket or Azure container (if applicable)
    pub bucket_or_container: Option<String>,
    /// AWS region (if S3)
    pub region: Option<String>,
    /// Custom endpoint URL (if applicable)
    pub endpoint: Option<String>,
}

/// The top-level manifest embedded in the split bundle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetManifest {
    /// Manifest format version
    pub version: u32,
    /// Table root path (relative paths in parquet_files are resolved against this)
    pub table_root: String,
    /// Fast field mode for this split
    pub fast_field_mode: FastFieldMode,
    /// Mapping from segments to global row ranges
    pub segment_row_ranges: Vec<SegmentRowRange>,
    /// External parquet files referenced by this split
    pub parquet_files: Vec<ParquetFileEntry>,
    /// Column name mappings (tantivy field → parquet column)
    pub column_mapping: Vec<ColumnMapping>,
    /// Total number of rows across all files
    pub total_rows: u64,
    /// Storage config metadata for the parquet files
    pub storage_config: Option<ParquetStorageConfigMeta>,
    /// Arbitrary metadata key-value pairs
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl ParquetManifest {
    /// Resolve a relative parquet path against the table root
    pub fn resolve_path(&self, relative_path: &str) -> String {
        if relative_path.starts_with('/') || relative_path.contains("://") {
            // Already absolute or has protocol
            relative_path.to_string()
        } else {
            let root = self.table_root.trim_end_matches('/');
            format!("{}/{}", root, relative_path)
        }
    }

    /// Validate manifest internal consistency
    pub fn validate(&self) -> Result<(), String> {
        if self.version != SUPPORTED_MANIFEST_VERSION {
            return Err(format!(
                "Unsupported manifest version: {} (expected {})",
                self.version, SUPPORTED_MANIFEST_VERSION
            ));
        }

        // Verify total_rows matches sum of file rows
        let file_rows: u64 = self.parquet_files.iter().map(|f| f.num_rows).sum();
        if file_rows != self.total_rows {
            return Err(format!(
                "Row count mismatch: total_rows={} but sum of file rows={}",
                self.total_rows, file_rows
            ));
        }

        // Verify segment_row_ranges covers total_rows
        let segment_rows: u64 = self.segment_row_ranges.iter().map(|s| s.num_rows).sum();
        if segment_rows != self.total_rows {
            return Err(format!(
                "Segment row count mismatch: total_rows={} but sum of segment rows={}",
                self.total_rows, segment_rows
            ));
        }

        // Verify row_offset monotonicity for files
        for i in 1..self.parquet_files.len() {
            let prev = &self.parquet_files[i - 1];
            let curr = &self.parquet_files[i];
            let expected_offset = prev.row_offset + prev.num_rows;
            if curr.row_offset != expected_offset {
                return Err(format!(
                    "File row_offset gap: file[{}] ends at {} but file[{}] starts at {}",
                    i - 1, expected_offset, i, curr.row_offset
                ));
            }
        }

        // Verify no empty files
        for (i, f) in self.parquet_files.iter().enumerate() {
            if f.num_rows == 0 {
                return Err(format!("File[{}] '{}' has zero rows", i, f.relative_path));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_manifest() -> ParquetManifest {
        ParquetManifest {
            version: SUPPORTED_MANIFEST_VERSION,
            table_root: "s3://my-bucket/tables/events".to_string(),
            fast_field_mode: FastFieldMode::Hybrid,
            segment_row_ranges: vec![
                SegmentRowRange { segment_ord: 0, row_offset: 0, num_rows: 1000 },
            ],
            parquet_files: vec![
                ParquetFileEntry {
                    relative_path: "data/part-00001.parquet".to_string(),
                    file_size_bytes: 1024 * 1024,
                    row_offset: 0,
                    num_rows: 1000,
                    has_offset_index: true,
                    row_groups: vec![],
                },
            ],
            column_mapping: vec![
                ColumnMapping {
                    tantivy_field_name: "id".to_string(),
                    parquet_column_name: "id".to_string(),
                    physical_ordinal: 0,
                    parquet_type: "INT64".to_string(),
                    tantivy_type: "I64".to_string(),
                    field_id: None,
                    fast_field_tokenizer: None,
                },
            ],
            total_rows: 1000,
            storage_config: None,
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn test_manifest_validation_ok() {
        let manifest = make_test_manifest();
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn test_manifest_validation_row_mismatch() {
        let mut manifest = make_test_manifest();
        manifest.total_rows = 999;
        assert!(manifest.validate().is_err());
    }

    #[test]
    fn test_resolve_path_relative() {
        let manifest = make_test_manifest();
        assert_eq!(
            manifest.resolve_path("data/part-00001.parquet"),
            "s3://my-bucket/tables/events/data/part-00001.parquet"
        );
    }

    #[test]
    fn test_resolve_path_absolute() {
        let manifest = make_test_manifest();
        assert_eq!(
            manifest.resolve_path("s3://other-bucket/file.parquet"),
            "s3://other-bucket/file.parquet"
        );
    }

    #[test]
    fn test_fast_field_mode_serde() {
        let json = serde_json::to_string(&FastFieldMode::Hybrid).unwrap();
        assert_eq!(json, "\"hybrid\"");
        let mode: FastFieldMode = serde_json::from_str("\"parquet_only\"").unwrap();
        assert_eq!(mode, FastFieldMode::ParquetOnly);
    }
}
