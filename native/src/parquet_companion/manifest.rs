// manifest.rs - Data structures for the parquet companion manifest
//
// The manifest is embedded in the split bundle as `_parquet_manifest.json`
// and describes the mapping between tantivy documents and external parquet files.

use std::collections::HashMap;
use serde::{Serialize, Deserialize};

use super::string_indexing::{StringIndexingMode, CompanionFieldInfo};

/// Page-level byte offset information for a single data page.
/// Used to enable page-level byte range reads at query time for parquet files
/// that lack a native offset index in their footer.
#[derive(Debug, Clone)]
pub struct PageLocationEntry {
    /// Byte offset of the page within the parquet file
    pub offset: i64,
    /// Compressed page size in bytes (including page header)
    pub compressed_page_size: i32,
    /// Index of the first row in this page (relative to the row group)
    pub first_row_index: i64,
}

/// Pack page locations into compact binary (20 bytes per entry, little-endian).
fn pack_page_locations(locations: &[PageLocationEntry]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(locations.len() * 20);
    for loc in locations {
        buf.extend_from_slice(&loc.offset.to_le_bytes());
        buf.extend_from_slice(&loc.compressed_page_size.to_le_bytes());
        buf.extend_from_slice(&loc.first_row_index.to_le_bytes());
    }
    buf
}

/// Unpack page locations from compact binary (20 bytes per entry, little-endian).
fn unpack_page_locations(bytes: &[u8]) -> Vec<PageLocationEntry> {
    let mut locations = Vec::with_capacity(bytes.len() / 20);
    let mut i = 0;
    while i + 20 <= bytes.len() {
        locations.push(PageLocationEntry {
            offset: i64::from_le_bytes(bytes[i..i + 8].try_into().unwrap()),
            compressed_page_size: i32::from_le_bytes(bytes[i + 8..i + 12].try_into().unwrap()),
            first_row_index: i64::from_le_bytes(bytes[i + 12..i + 20].try_into().unwrap()),
        });
        i += 20;
    }
    locations
}

fn serialize_page_locations<S: serde::Serializer>(
    locs: &Vec<PageLocationEntry>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    use base64::Engine;
    let packed = pack_page_locations(locs);
    let b64 = base64::engine::general_purpose::STANDARD.encode(&packed);
    serializer.serialize_str(&b64)
}

fn deserialize_page_locations<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Vec<PageLocationEntry>, D::Error> {
    use base64::Engine;
    let b64: String = serde::Deserialize::deserialize(deserializer)?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&b64)
        .map_err(serde::de::Error::custom)?;
    Ok(unpack_page_locations(&bytes))
}

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
    /// Page-level offset information, stored as compact base64-encoded binary.
    /// Each entry is 20 bytes: i64 offset + i32 compressed_page_size + i64 first_row_index (LE).
    /// Empty for files with native offset index (used directly from footer).
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "serialize_page_locations",
        deserialize_with = "deserialize_page_locations"
    )]
    pub page_locations: Vec<PageLocationEntry>,
}

/// Maps a tantivy field to a parquet column
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    /// Table root path (relative paths in parquet_files are resolved against this).
    /// No longer persisted at indexing time — provided at read time via config.
    /// Defaults to empty string for backward compatibility with old manifests.
    #[serde(default)]
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
    /// Maps tantivy string field name → hash field name for HYBRID mode optimization.
    /// Only populated in HYBRID mode. Empty map means no hash fields present
    /// (e.g., old splits created before this feature).
    #[serde(default)]
    pub string_hash_fields: HashMap<String, String>,
    /// Maps field name → compact string indexing mode for fields using non-standard
    /// indexing strategies (exact_only, text_uuid_*, text_custom_*).
    /// Empty for splits without compact string indexing.
    #[serde(default)]
    pub string_indexing_modes: HashMap<String, StringIndexingMode>,
    /// Maps companion hash field name → info about the original field and regex pattern.
    /// Only populated for *_exactonly modes that create companion `__uuids` fields.
    #[serde(default)]
    pub companion_hash_fields: HashMap<String, CompanionFieldInfo>,
}

impl ParquetManifest {
    /// Resolve a relative parquet path against the table root
    pub fn resolve_path(&self, relative_path: &str) -> String {
        if relative_path.starts_with('/') || relative_path.contains("://") {
            // Already absolute or has protocol
            relative_path.to_string()
        } else if self.table_root.is_empty() {
            // No table_root set — return path as-is (avoid producing "/path")
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
            string_hash_fields: HashMap::new(),
            string_indexing_modes: HashMap::new(),
            companion_hash_fields: HashMap::new(),
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

    #[test]
    fn test_manifest_serde_roundtrip_with_string_indexing() {
        use super::super::string_indexing::{StringIndexingMode, CompanionFieldInfo, UUID_REGEX};

        let mut manifest = make_test_manifest();
        manifest.string_indexing_modes.insert(
            "trace_id".to_string(),
            StringIndexingMode::ExactOnly,
        );
        manifest.string_indexing_modes.insert(
            "message".to_string(),
            StringIndexingMode::TextUuidExactonly,
        );
        manifest.companion_hash_fields.insert(
            "message__uuids".to_string(),
            CompanionFieldInfo {
                original_field_name: "message".to_string(),
                regex_pattern: UUID_REGEX.to_string(),
            },
        );

        let json = serde_json::to_string(&manifest).unwrap();
        let parsed: ParquetManifest = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.string_indexing_modes.len(), 2);
        assert_eq!(parsed.string_indexing_modes["trace_id"], StringIndexingMode::ExactOnly);
        assert_eq!(parsed.string_indexing_modes["message"], StringIndexingMode::TextUuidExactonly);
        assert_eq!(parsed.companion_hash_fields.len(), 1);
        assert_eq!(parsed.companion_hash_fields["message__uuids"].original_field_name, "message");
    }

    #[test]
    fn test_page_locations_pack_unpack_roundtrip() {
        let locations = vec![
            PageLocationEntry { offset: 1000, compressed_page_size: 500, first_row_index: 0 },
            PageLocationEntry { offset: 1500, compressed_page_size: 300, first_row_index: 100 },
            PageLocationEntry { offset: 1800, compressed_page_size: 200, first_row_index: 200 },
        ];
        let packed = pack_page_locations(&locations);
        assert_eq!(packed.len(), 60); // 3 * 20 bytes
        let unpacked = unpack_page_locations(&packed);
        assert_eq!(unpacked.len(), 3);
        assert_eq!(unpacked[0].offset, 1000);
        assert_eq!(unpacked[0].compressed_page_size, 500);
        assert_eq!(unpacked[0].first_row_index, 0);
        assert_eq!(unpacked[2].offset, 1800);
        assert_eq!(unpacked[2].first_row_index, 200);
    }

    #[test]
    fn test_column_chunk_info_serde_with_page_locations() {
        let info = ColumnChunkInfo {
            column_idx: 0,
            column_name: "id".to_string(),
            data_page_offset: 4096,
            compressed_size: 1024,
            uncompressed_size: 2048,
            page_locations: vec![
                PageLocationEntry { offset: 4096, compressed_page_size: 512, first_row_index: 0 },
                PageLocationEntry { offset: 4608, compressed_page_size: 512, first_row_index: 50 },
            ],
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("page_locations"));
        let parsed: ColumnChunkInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.page_locations.len(), 2);
        assert_eq!(parsed.page_locations[0].offset, 4096);
        assert_eq!(parsed.page_locations[1].first_row_index, 50);
    }

    #[test]
    fn test_column_chunk_info_serde_empty_page_locations_omitted() {
        let info = ColumnChunkInfo {
            column_idx: 0,
            column_name: "id".to_string(),
            data_page_offset: 4096,
            compressed_size: 1024,
            uncompressed_size: 2048,
            page_locations: vec![],
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(!json.contains("page_locations"), "empty page_locations should be omitted: {}", json);
    }

    #[test]
    fn test_column_chunk_info_backward_compat_no_page_locations() {
        // Old manifests without page_locations field should deserialize fine
        let json = r#"{
            "column_idx": 0,
            "column_name": "id",
            "data_page_offset": 4096,
            "compressed_size": 1024,
            "uncompressed_size": 2048
        }"#;
        let parsed: ColumnChunkInfo = serde_json::from_str(json).unwrap();
        assert!(parsed.page_locations.is_empty());
    }

    #[test]
    fn test_manifest_backward_compat_missing_new_fields() {
        // Verify that manifests without the new fields deserialize with defaults
        let json = r#"{
            "version": 1,
            "table_root": "",
            "fast_field_mode": "disabled",
            "segment_row_ranges": [{"segment_ord": 0, "row_offset": 0, "num_rows": 10}],
            "parquet_files": [{"relative_path": "p.parquet", "file_size_bytes": 100, "row_offset": 0, "num_rows": 10, "has_offset_index": false, "row_groups": []}],
            "column_mapping": [],
            "total_rows": 10,
            "storage_config": null,
            "metadata": {},
            "string_hash_fields": {}
        }"#;
        let parsed: ParquetManifest = serde_json::from_str(json).unwrap();
        assert!(parsed.string_indexing_modes.is_empty());
        assert!(parsed.companion_hash_fields.is_empty());
    }
}
