/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Core types for Binary Fuse Filter XRef

use serde::{Deserialize, Serialize};
use xorf::{BinaryFuse8, BinaryFuse16, Filter};

/// Magic bytes for Fuse XRef files
pub const FUSE_XREF_MAGIC: &[u8; 4] = b"FXRF";

/// Current format version (v2 adds compression support)
pub const FUSE_XREF_VERSION: u32 = 2;

/// Filter type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u32)]
pub enum FuseFilterType {
    /// BinaryFuse8 - 8-bit fingerprints (~1.24 bytes per key)
    Fuse8 = 1,
    /// BinaryFuse16 - 16-bit fingerprints (~2.24 bytes per key, lower FPR)
    Fuse16 = 2,
}

impl Default for FuseFilterType {
    fn default() -> Self {
        FuseFilterType::Fuse8
    }
}

/// Compression type for FuseXRef storage
///
/// Zstd provides excellent compression ratios with minimal overhead.
/// Level 3 is a good balance between speed and compression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum CompressionType {
    /// No compression (fastest, largest files)
    None = 0,
    /// Zstd compression level 1 (fastest compression)
    Zstd1 = 1,
    /// Zstd compression level 3 (default - good balance)
    Zstd3 = 3,
    /// Zstd compression level 6 (better compression, slower)
    Zstd6 = 6,
    /// Zstd compression level 9 (best compression, slowest)
    Zstd9 = 9,
}

impl Default for CompressionType {
    fn default() -> Self {
        CompressionType::Zstd3  // Good balance of speed and compression
    }
}

impl CompressionType {
    /// Get the zstd compression level (0 = no compression)
    pub fn zstd_level(&self) -> i32 {
        match self {
            CompressionType::None => 0,
            CompressionType::Zstd1 => 1,
            CompressionType::Zstd3 => 3,
            CompressionType::Zstd6 => 6,
            CompressionType::Zstd9 => 9,
        }
    }

    /// Check if compression is enabled
    pub fn is_compressed(&self) -> bool {
        !matches!(self, CompressionType::None)
    }

    /// Parse from string (for JNI)
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "none" => CompressionType::None,
            "zstd1" | "zstd-1" | "zstd_1" => CompressionType::Zstd1,
            "zstd3" | "zstd-3" | "zstd_3" | "zstd" => CompressionType::Zstd3,
            "zstd6" | "zstd-6" | "zstd_6" => CompressionType::Zstd6,
            "zstd9" | "zstd-9" | "zstd_9" => CompressionType::Zstd9,
            _ => CompressionType::Zstd3, // Default
        }
    }

    /// Get string representation
    pub fn as_str(&self) -> &'static str {
        match self {
            CompressionType::None => "none",
            CompressionType::Zstd1 => "zstd1",
            CompressionType::Zstd3 => "zstd3",
            CompressionType::Zstd6 => "zstd6",
            CompressionType::Zstd9 => "zstd9",
        }
    }
}

/// Wrapper enum for both filter types
///
/// This allows FuseXRef to hold either BinaryFuse8 or BinaryFuse16 filters.
#[derive(Debug)]
pub enum FuseFilter {
    /// 8-bit fingerprints (~0.39% FPR, ~1.24 bytes/key)
    Fuse8(BinaryFuse8),
    /// 16-bit fingerprints (~0.0015% FPR, ~2.24 bytes/key)
    Fuse16(BinaryFuse16),
}

impl FuseFilter {
    /// Check if a hash value is possibly present in the filter
    pub fn contains(&self, hash: &u64) -> bool {
        match self {
            FuseFilter::Fuse8(f) => f.contains(hash),
            FuseFilter::Fuse16(f) => f.contains(hash),
        }
    }

    /// Get the filter type
    pub fn filter_type(&self) -> FuseFilterType {
        match self {
            FuseFilter::Fuse8(_) => FuseFilterType::Fuse8,
            FuseFilter::Fuse16(_) => FuseFilterType::Fuse16,
        }
    }

    /// Get the false positive rate for this filter type
    pub fn false_positive_rate(&self) -> f64 {
        match self {
            FuseFilter::Fuse8(_) => 1.0 / 256.0,   // ~0.39%
            FuseFilter::Fuse16(_) => 1.0 / 65536.0, // ~0.0015%
        }
    }
}

/// Header for the fuse_filters.bin file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuseFiltersHeader {
    /// Magic bytes "FXRF"
    pub magic: [u8; 4],
    /// Format version
    pub version: u32,
    /// Number of filters in file
    pub num_filters: u32,
    /// Filter type (Fuse8 or Fuse16)
    pub filter_type: FuseFilterType,
}

impl FuseFiltersHeader {
    /// Create a new header for BinaryFuse8 filters
    pub fn new(num_filters: u32) -> Self {
        Self {
            magic: *FUSE_XREF_MAGIC,
            version: FUSE_XREF_VERSION,
            num_filters,
            filter_type: FuseFilterType::Fuse8,
        }
    }

    /// Validate the header
    pub fn validate(&self) -> Result<(), String> {
        if &self.magic != FUSE_XREF_MAGIC {
            return Err(format!(
                "Invalid magic bytes: expected {:?}, got {:?}",
                FUSE_XREF_MAGIC, self.magic
            ));
        }
        if self.version > FUSE_XREF_VERSION {
            return Err(format!(
                "Unsupported version: {} (max: {})",
                self.version, FUSE_XREF_VERSION
            ));
        }
        Ok(())
    }
}

/// Index entry for a single filter in fuse_filters.bin
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterIndexEntry {
    /// Byte offset into filter data section
    pub offset: u64,
    /// Size of serialized filter in bytes
    pub size: u64,
    /// Number of keys in this filter
    pub num_keys: u64,
}

/// Metadata for a single split's filter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitFilterMetadata {
    /// Index of this split (0-based)
    pub split_idx: u32,
    /// Split URI (s3://, azure://, file://, or local path)
    pub uri: String,
    /// Split ID for identification
    pub split_id: String,
    /// Footer start offset (for efficient split opening)
    pub footer_start: u64,
    /// Footer end offset
    pub footer_end: u64,
    /// Number of documents in source split
    pub num_docs: u64,
    /// Number of unique terms indexed in filter
    pub num_terms: u64,
    /// Size of serialized filter in bytes
    pub filter_size_bytes: u64,
}

impl SplitFilterMetadata {
    /// Create new split metadata
    pub fn new(
        split_idx: u32,
        uri: String,
        split_id: String,
        footer_start: u64,
        footer_end: u64,
    ) -> Self {
        Self {
            split_idx,
            uri,
            split_id,
            footer_start,
            footer_end,
            num_docs: 0,
            num_terms: 0,
            filter_size_bytes: 0,
        }
    }
}

/// Field type information for query normalization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldTypeInfo {
    /// Field name
    pub name: String,
    /// Field type (text, u64, i64, f64, bool, date, json, etc.)
    pub field_type: String,
    /// Tokenizer name for text fields (e.g., "default", "raw", "en_stem")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tokenizer: Option<String>,
    /// Whether field is indexed
    pub indexed: bool,
}

/// Global XRef header metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuseXRefHeader {
    /// Magic string "FXRF"
    pub magic: String,
    /// Format version
    pub format_version: u32,
    /// XRef identifier
    pub xref_id: String,
    /// Index UID for Quickwit compatibility
    pub index_uid: String,
    /// Number of splits indexed
    pub num_splits: u32,
    /// Total terms across all splits
    pub total_terms: u64,
    /// Filter type used
    pub filter_type: String,
    /// Creation timestamp (Unix seconds)
    pub created_at: u64,
    /// Footer start offset for bundle
    pub footer_start_offset: u64,
    /// Footer end offset for bundle
    pub footer_end_offset: u64,
    /// Field type information for query normalization
    #[serde(default)]
    pub fields: Vec<FieldTypeInfo>,
    /// Tantivy schema JSON (for advanced query parsing)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_json: Option<String>,
}

impl FuseXRefHeader {
    /// Create new header
    pub fn new(xref_id: String, index_uid: String) -> Self {
        Self {
            magic: String::from_utf8_lossy(FUSE_XREF_MAGIC).to_string(),
            format_version: FUSE_XREF_VERSION,
            xref_id,
            index_uid,
            num_splits: 0,
            total_terms: 0,
            filter_type: "BinaryFuse8".to_string(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            footer_start_offset: 0,
            footer_end_offset: 0,
            fields: Vec::new(),
            schema_json: None,
        }
    }

    /// Get field type info by name
    pub fn get_field_info(&self, field_name: &str) -> Option<&FieldTypeInfo> {
        self.fields.iter().find(|f| f.name == field_name)
    }
}

/// Complete in-memory FuseXRef structure
pub struct FuseXRef {
    /// Global header
    pub header: FuseXRefHeader,
    /// Binary Fuse filters, one per split (can be Fuse8 or Fuse16)
    pub filters: Vec<FuseFilter>,
    /// Metadata for each split
    pub metadata: Vec<SplitFilterMetadata>,
}

impl FuseXRef {
    /// Create empty FuseXRef
    pub fn new(xref_id: String, index_uid: String) -> Self {
        Self {
            header: FuseXRefHeader::new(xref_id, index_uid),
            filters: Vec::new(),
            metadata: Vec::new(),
        }
    }

    /// Check if a key possibly exists in a split's filter
    ///
    /// Keys are field-qualified: "fieldname:value"
    pub fn check(&self, split_idx: usize, field: &str, value: &str) -> bool {
        if split_idx >= self.filters.len() {
            return false;
        }

        let key = format!("{}:{}", field, value);
        let hash = fxhash::hash64(&key);
        self.filters[split_idx].contains(&hash)
    }

    /// Check if a pre-hashed key exists in a split's filter
    pub fn check_hash(&self, split_idx: usize, hash: u64) -> bool {
        if split_idx >= self.filters.len() {
            return false;
        }
        self.filters[split_idx].contains(&hash)
    }

    /// Get the filter type used by this XRef
    pub fn filter_type(&self) -> FuseFilterType {
        self.filters.first()
            .map(|f| f.filter_type())
            .unwrap_or(FuseFilterType::Fuse8)
    }

    /// Get the number of splits in this XRef
    pub fn num_splits(&self) -> usize {
        self.filters.len()
    }

    /// Get metadata for a split
    pub fn get_metadata(&self, split_idx: usize) -> Option<&SplitFilterMetadata> {
        self.metadata.get(split_idx)
    }
}

/// Result of an XRef search
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuseXRefSearchResult {
    /// Splits that possibly match the query
    pub matching_splits: Vec<MatchingSplit>,
    /// Number of matching splits
    pub num_matching_splits: usize,
    /// True if query contained clauses that could not be evaluated
    /// (range queries, wildcards, etc.)
    pub has_unevaluated_clauses: bool,
    /// Search execution time in milliseconds
    pub search_time_ms: u64,
}

impl FuseXRefSearchResult {
    /// Create new empty result
    pub fn new() -> Self {
        Self {
            matching_splits: Vec::new(),
            num_matching_splits: 0,
            has_unevaluated_clauses: false,
            search_time_ms: 0,
        }
    }

    /// Get URIs of matching splits
    pub fn split_uris(&self) -> Vec<&str> {
        self.matching_splits.iter().map(|s| s.uri.as_str()).collect()
    }

    /// Get split IDs of matching splits
    pub fn split_ids(&self) -> Vec<&str> {
        self.matching_splits.iter().map(|s| s.split_id.as_str()).collect()
    }
}

impl Default for FuseXRefSearchResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Information about a split that possibly matches a query
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchingSplit {
    /// Split URI
    pub uri: String,
    /// Split ID
    pub split_id: String,
    /// Footer start offset
    pub footer_start: u64,
    /// Footer end offset
    pub footer_end: u64,
    /// Number of docs in split (for estimation)
    pub num_docs: u64,
}

impl MatchingSplit {
    /// Create from split metadata
    pub fn from_metadata(meta: &SplitFilterMetadata) -> Self {
        Self {
            uri: meta.uri.clone(),
            split_id: meta.split_id.clone(),
            footer_start: meta.footer_start,
            footer_end: meta.footer_end,
            num_docs: meta.num_docs,
        }
    }
}

/// Build configuration for FuseXRef (reuses existing XRefBuildConfig format)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuseXRefBuildConfig {
    /// XRef identifier
    pub xref_id: String,
    /// Index UID
    pub index_uid: String,
    /// Source splits to index
    pub source_splits: Vec<FuseXRefSourceSplit>,
    /// AWS config for S3 access
    #[serde(skip_serializing_if = "Option::is_none")]
    pub aws_config: Option<crate::merge_types::MergeAwsConfig>,
    /// Azure config for Azure Blob Storage access
    #[serde(skip_serializing_if = "Option::is_none")]
    pub azure_config: Option<crate::merge_types::MergeAzureConfig>,
    /// Fields to include (empty = all indexed fields)
    #[serde(default)]
    pub included_fields: Vec<String>,
    /// Custom temp directory path
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temp_directory_path: Option<String>,
    /// Filter type (Fuse8 or Fuse16) - controls FPR vs size trade-off
    #[serde(default)]
    pub filter_type: FuseFilterType,
    /// Compression type for output file
    #[serde(default)]
    pub compression: CompressionType,
}

/// Source split information for FuseXRef building
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuseXRefSourceSplit {
    /// Split URI
    pub uri: String,
    /// Split ID
    pub split_id: String,
    /// Footer start offset
    pub footer_start: u64,
    /// Footer end offset
    pub footer_end: u64,
    /// Optional document count
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_docs: Option<u64>,
}

impl FuseXRefSourceSplit {
    /// Create new source split
    pub fn new(uri: String, split_id: String, footer_start: u64, footer_end: u64) -> Self {
        Self {
            uri,
            split_id,
            footer_start,
            footer_end,
            num_docs: None,
        }
    }
}

/// Build result/metadata returned after building a FuseXRef (internal use)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FuseXRefBuildResult {
    /// XRef identifier
    pub xref_id: String,
    /// Index UID
    pub index_uid: String,
    /// Number of splits indexed
    pub num_splits: u32,
    /// Total unique terms indexed
    pub total_terms: u64,
    /// Output file size in bytes
    pub output_size_bytes: u64,
    /// Footer start offset
    pub footer_start_offset: u64,
    /// Footer end offset
    pub footer_end_offset: u64,
    /// Build duration in milliseconds
    pub build_duration_ms: u64,
    /// Number of splits skipped due to errors
    pub splits_skipped: u32,
}

/// XRef metadata structure that matches Java XRefMetadata JSON format
///
/// This is the format returned to Java that matches the XRefMetadata class
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XRefMetadataJson {
    /// Format version
    pub format_version: u32,
    /// XRef identifier
    pub xref_id: String,
    /// Index UID
    pub index_uid: String,
    /// Registry of source splits
    pub split_registry: SplitRegistryJson,
    /// Field information
    pub fields: Vec<FieldInfoJson>,
    /// Total unique terms
    pub total_terms: u64,
    /// Build statistics
    pub build_stats: BuildStatsJson,
    /// Creation timestamp (Unix seconds)
    pub created_at: u64,
    /// Footer start offset
    pub footer_start_offset: u64,
    /// Footer end offset
    pub footer_end_offset: u64,
}

/// Split registry for Java XRefMetadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SplitRegistryJson {
    /// List of source splits
    pub splits: Vec<XRefSplitEntryJson>,
}

/// Split entry for Java XRefMetadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XRefSplitEntryJson {
    /// Split URI
    pub uri: String,
    /// Split ID
    pub split_id: String,
    /// Number of documents
    pub num_docs: u64,
    /// Footer start offset
    pub footer_start: u64,
    /// Footer end offset
    pub footer_end: u64,
    /// Size in bytes
    pub size_bytes: u64,
}

/// Field information for Java XRefMetadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldInfoJson {
    /// Field name
    pub name: String,
    /// Field type
    pub field_type: String,
    /// Number of terms
    pub num_terms: u64,
    /// Total postings
    pub total_postings: u64,
    /// Whether field has positions
    pub has_positions: bool,
}

/// Build statistics for Java XRefMetadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildStatsJson {
    /// Build duration in milliseconds
    pub build_duration_ms: u64,
    /// Bytes read during build
    pub bytes_read: u64,
    /// Output size in bytes
    pub output_size_bytes: u64,
    /// Compression ratio
    pub compression_ratio: f64,
    /// Number of splits processed
    pub splits_processed: u32,
    /// Number of splits skipped
    pub splits_skipped: u32,
    /// Number of unique terms
    pub unique_terms: u64,
}

impl XRefMetadataJson {
    /// Create from build result and metadata
    pub fn from_build(
        result: &FuseXRefBuildResult,
        metadata: &[SplitFilterMetadata],
        fields: &[FieldTypeInfo],
    ) -> Self {
        let split_entries: Vec<XRefSplitEntryJson> = metadata
            .iter()
            .map(|m| XRefSplitEntryJson {
                uri: m.uri.clone(),
                split_id: m.split_id.clone(),
                num_docs: m.num_docs,
                footer_start: m.footer_start,
                footer_end: m.footer_end,
                size_bytes: m.filter_size_bytes,
            })
            .collect();

        let field_infos: Vec<FieldInfoJson> = fields
            .iter()
            .map(|f| FieldInfoJson {
                name: f.name.clone(),
                field_type: f.field_type.clone(),
                num_terms: 0, // Not tracked per-field in FuseXRef
                total_postings: 0,
                has_positions: false, // FuseXRef doesn't track positions
            })
            .collect();

        Self {
            format_version: FUSE_XREF_VERSION,
            xref_id: result.xref_id.clone(),
            index_uid: result.index_uid.clone(),
            split_registry: SplitRegistryJson {
                splits: split_entries,
            },
            fields: field_infos,
            total_terms: result.total_terms,
            build_stats: BuildStatsJson {
                build_duration_ms: result.build_duration_ms,
                bytes_read: 0, // Not tracked
                output_size_bytes: result.output_size_bytes,
                compression_ratio: 0.0, // Not meaningful for FuseXRef
                splits_processed: result.num_splits,
                splits_skipped: result.splits_skipped,
                unique_terms: result.total_terms,
            },
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            footer_start_offset: result.footer_start_offset,
            footer_end_offset: result.footer_end_offset,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_validation() {
        let header = FuseFiltersHeader::new(10);
        assert!(header.validate().is_ok());

        let mut invalid = header.clone();
        invalid.magic = [0, 0, 0, 0];
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn test_fuse_xref_check() {
        // Create a simple filter with known keys
        let keys: Vec<u64> = vec![
            fxhash::hash64("title:hello"),
            fxhash::hash64("title:world"),
            fxhash::hash64("body:test"),
        ];

        let filter = BinaryFuse8::try_from(&keys[..]).expect("Failed to create filter");

        let mut xref = FuseXRef::new("test".to_string(), "test-index".to_string());
        xref.filters.push(FuseFilter::Fuse8(filter));
        xref.metadata.push(SplitFilterMetadata::new(
            0,
            "file:///test.split".to_string(),
            "test".to_string(),
            0,
            1000,
        ));

        // Check existing keys
        assert!(xref.check(0, "title", "hello"));
        assert!(xref.check(0, "title", "world"));
        assert!(xref.check(0, "body", "test"));

        // Check non-existing keys (should return false with high probability)
        // Note: There's a small FPR chance this could be true
        assert!(!xref.check(0, "title", "nonexistent"));
        assert!(!xref.check(0, "other", "field"));
    }

    #[test]
    fn test_fuse16_filter() {
        // Test BinaryFuse16 filter
        let keys: Vec<u64> = vec![
            fxhash::hash64("field:value1"),
            fxhash::hash64("field:value2"),
        ];

        let filter = BinaryFuse16::try_from(&keys[..]).expect("Failed to create Fuse16 filter");

        let mut xref = FuseXRef::new("test16".to_string(), "test-index".to_string());
        xref.filters.push(FuseFilter::Fuse16(filter));
        xref.metadata.push(SplitFilterMetadata::new(
            0,
            "file:///test16.split".to_string(),
            "test16".to_string(),
            0,
            1000,
        ));

        // Verify filter type
        assert_eq!(xref.filter_type(), FuseFilterType::Fuse16);

        // Check existing keys
        assert!(xref.check(0, "field", "value1"));
        assert!(xref.check(0, "field", "value2"));
    }
}
