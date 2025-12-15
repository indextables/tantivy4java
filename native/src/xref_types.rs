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

//! Cross-Reference Split Types
//!
//! This module defines the core data structures for Cross-Reference (XRef) splits.
//! An XRef split consolidates term dictionaries from multiple source splits into a
//! lightweight index where each document represents one source split.
//!
//! Key Design Principle: If the XRef references N source splits, the XRef split
//! contains exactly N documents - one per source split. Posting lists point to
//! these split-level documents, NOT to the original documents.

use serde::{Deserialize, Serialize};
use crate::merge_types::{MergeAwsConfig, MergeAzureConfig};

// Re-export for convenience
pub use crate::merge_types::MergeAzureConfig as XRefAzureConfig;

/// Format version for XRef splits
pub const XREF_FORMAT_VERSION: u32 = 1;

/// Magic bytes to identify XRef split files
pub const XREF_MAGIC: &[u8; 4] = b"XREF";

/// Source split information for XRef building
/// Contains the URI, footer offsets, and doc mapping needed to efficiently open the split
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XRefSourceSplit {
    /// Split URI (file paths, file://, s3://, azure://)
    pub uri: String,

    /// Split ID (for quick lookup)
    pub split_id: String,

    /// Footer start offset (required for efficient split opening)
    pub footer_start: u64,

    /// Footer end offset (required for efficient split opening)
    pub footer_end: u64,

    /// Optional: Document count in source split
    pub num_docs: Option<u64>,

    /// Optional: Size in bytes
    pub size_bytes: Option<u64>,

    /// Optional: Doc mapping JSON (used to derive the schema)
    pub doc_mapping_json: Option<String>,
}

impl XRefSourceSplit {
    /// Create a new source split entry with required fields
    pub fn new(uri: String, split_id: String, footer_start: u64, footer_end: u64) -> Self {
        Self {
            uri,
            split_id,
            footer_start,
            footer_end,
            num_docs: None,
            size_bytes: None,
            doc_mapping_json: None,
        }
    }

    /// Set the document count
    pub fn with_num_docs(mut self, num_docs: u64) -> Self {
        self.num_docs = Some(num_docs);
        self
    }

    /// Set the size in bytes
    pub fn with_size_bytes(mut self, size_bytes: u64) -> Self {
        self.size_bytes = Some(size_bytes);
        self
    }

    /// Set the doc mapping JSON (for schema derivation)
    pub fn with_doc_mapping_json(mut self, json: String) -> Self {
        self.doc_mapping_json = Some(json);
        self
    }
}

/// Configuration for building an XRef split
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XRefBuildConfig {
    /// Unique identifier for this XRef split
    pub xref_id: String,

    /// Index UID for Quickwit compatibility
    pub index_uid: String,

    /// Source splits with their metadata (including footer offsets)
    pub source_splits: Vec<XRefSourceSplit>,

    /// Optional: AWS config for S3 access
    pub aws_config: Option<MergeAwsConfig>,

    /// Optional: Azure config for Azure Blob Storage access
    pub azure_config: Option<MergeAzureConfig>,

    /// Fields to include in XRef (empty = all indexed fields)
    pub included_fields: Vec<String>,

    /// Whether to include position data (for phrase queries)
    /// Note: Positions increase XRef size but enable phrase query routing
    pub include_positions: bool,
}

impl XRefBuildConfig {
    /// Create a new XRef build configuration
    pub fn new(xref_id: String, index_uid: String, source_splits: Vec<XRefSourceSplit>) -> Self {
        Self {
            xref_id,
            index_uid,
            source_splits,
            aws_config: None,
            azure_config: None,
            included_fields: Vec::new(),
            include_positions: false,
        }
    }

    /// Set AWS configuration for S3 access
    pub fn with_aws_config(mut self, config: MergeAwsConfig) -> Self {
        self.aws_config = Some(config);
        self
    }

    /// Set Azure configuration for Azure Blob Storage access
    pub fn with_azure_config(mut self, config: MergeAzureConfig) -> Self {
        self.azure_config = Some(config);
        self
    }

    /// Set fields to include in the XRef (empty = all)
    pub fn with_included_fields(mut self, fields: Vec<String>) -> Self {
        self.included_fields = fields;
        self
    }

    /// Enable position data for phrase query support
    pub fn with_positions(mut self, include: bool) -> Self {
        self.include_positions = include;
        self
    }
}

/// Document representing a single source split in the XRef
///
/// In the XRef split, doc_id = split_index directly. This is a 1:1 mapping:
/// - doc_id 0 → Source Split 0
/// - doc_id 1 → Source Split 1
/// - doc_id N-1 → Source Split N-1
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XRefSplitDocument {
    /// Split URI (s3://bucket/path/split.split, file path, or azure://container/path)
    pub uri: String,

    /// Split ID (for quick lookup)
    pub split_id: String,

    /// Document count in this source split
    pub num_docs: u64,

    /// Footer offsets for fast split opening
    pub footer_start: u64,
    pub footer_end: u64,

    /// Optional: time range covered by this split (Unix timestamps in microseconds)
    pub time_range_start: Option<i64>,
    pub time_range_end: Option<i64>,

    /// Byte size of the split for load balancing
    pub size_bytes: u64,

    /// Schema hash for compatibility checking
    pub schema_hash: Option<String>,
}

impl XRefSplitDocument {
    /// Create a new XRef split document
    pub fn new(uri: String, split_id: String, num_docs: u64) -> Self {
        Self {
            uri,
            split_id,
            num_docs,
            footer_start: 0,
            footer_end: 0,
            time_range_start: None,
            time_range_end: None,
            size_bytes: 0,
            schema_hash: None,
        }
    }

    /// Set footer offsets
    pub fn with_footer_offsets(mut self, start: u64, end: u64) -> Self {
        self.footer_start = start;
        self.footer_end = end;
        self
    }

    /// Set time range
    pub fn with_time_range(mut self, start: i64, end: i64) -> Self {
        self.time_range_start = Some(start);
        self.time_range_end = Some(end);
        self
    }

    /// Set size in bytes
    pub fn with_size_bytes(mut self, size: u64) -> Self {
        self.size_bytes = size;
        self
    }
}

/// Registry of all source splits in the XRef
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XRefSplitRegistry {
    /// All split documents (indexed by doc_id)
    pub splits: Vec<XRefSplitDocument>,
}

impl XRefSplitRegistry {
    /// Create a new empty registry
    pub fn new() -> Self {
        Self { splits: Vec::new() }
    }

    /// Add a split to the registry
    pub fn add_split(&mut self, split: XRefSplitDocument) {
        self.splits.push(split);
    }

    /// Get the number of splits
    pub fn len(&self) -> usize {
        self.splits.len()
    }

    /// Check if the registry is empty
    pub fn is_empty(&self) -> bool {
        self.splits.is_empty()
    }

    /// Get a split by doc_id (which equals split index)
    pub fn get_split(&self, doc_id: usize) -> Option<&XRefSplitDocument> {
        self.splits.get(doc_id)
    }

    /// Get total document count across all splits
    pub fn total_docs(&self) -> u64 {
        self.splits.iter().map(|s| s.num_docs).sum()
    }

    /// Get total size across all splits
    pub fn total_size_bytes(&self) -> u64 {
        self.splits.iter().map(|s| s.size_bytes).sum()
    }
}

impl Default for XRefSplitRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Information about a field in the XRef split
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XRefFieldInfo {
    /// Field name
    pub name: String,

    /// Field type (text, u64, i64, f64, bool, date, etc.)
    pub field_type: String,

    /// Number of unique terms for this field across all source splits
    pub num_terms: u64,

    /// Total postings (term occurrences across splits)
    pub total_postings: u64,

    /// Whether position data is included
    pub has_positions: bool,
}

impl XRefFieldInfo {
    /// Create a new field info
    pub fn new(name: String, field_type: String) -> Self {
        Self {
            name,
            field_type,
            num_terms: 0,
            total_postings: 0,
            has_positions: false,
        }
    }
}

/// Build statistics for the XRef split
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XRefBuildStats {
    /// Build duration in milliseconds
    pub build_duration_ms: u64,

    /// Total bytes read from source splits
    pub bytes_read: u64,

    /// Final XRef split size in bytes
    pub output_size_bytes: u64,

    /// Compression ratio (XRef size / full merge size estimate)
    pub compression_ratio: f64,

    /// Number of source splits processed
    pub splits_processed: u32,

    /// Number of source splits skipped (due to errors)
    pub splits_skipped: u32,

    /// Total unique terms across all fields
    pub unique_terms: u64,
}

impl XRefBuildStats {
    /// Create new build stats
    pub fn new() -> Self {
        Self {
            build_duration_ms: 0,
            bytes_read: 0,
            output_size_bytes: 0,
            compression_ratio: 0.0,
            splits_processed: 0,
            splits_skipped: 0,
            unique_terms: 0,
        }
    }
}

impl Default for XRefBuildStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Complete metadata for an XRef split
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XRefMetadata {
    /// Magic bytes for file identification
    pub magic: [u8; 4],

    /// Format version
    pub format_version: u32,

    /// XRef split identifier
    pub xref_id: String,

    /// Index UID for Quickwit compatibility
    pub index_uid: String,

    /// Source split registry
    pub split_registry: XRefSplitRegistry,

    /// Field information
    pub fields: Vec<XRefFieldInfo>,

    /// Total unique terms across all fields
    pub total_terms: u64,

    /// Build statistics
    pub build_stats: XRefBuildStats,

    /// Creation timestamp (Unix timestamp in seconds)
    pub created_at: u64,

    /// Footer start offset for this XRef split (for SplitSearcher compatibility)
    pub footer_start_offset: u64,

    /// Footer end offset for this XRef split (for SplitSearcher compatibility)
    pub footer_end_offset: u64,
}

impl XRefMetadata {
    /// Create new XRef metadata
    pub fn new(xref_id: String, index_uid: String) -> Self {
        Self {
            magic: *XREF_MAGIC,
            format_version: XREF_FORMAT_VERSION,
            xref_id,
            index_uid,
            split_registry: XRefSplitRegistry::new(),
            fields: Vec::new(),
            total_terms: 0,
            build_stats: XRefBuildStats::new(),
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            footer_start_offset: 0,
            footer_end_offset: 0,
        }
    }

    /// Set the footer offsets for this XRef split
    pub fn with_footer_offsets(mut self, start: u64, end: u64) -> Self {
        self.footer_start_offset = start;
        self.footer_end_offset = end;
        self
    }

    /// Check if footer offsets are set
    pub fn has_footer_offsets(&self) -> bool {
        self.footer_end_offset > self.footer_start_offset
    }

    /// Get the number of source splits
    pub fn num_splits(&self) -> usize {
        self.split_registry.len()
    }

    /// Get total documents across all source splits
    pub fn total_source_docs(&self) -> u64 {
        self.split_registry.total_docs()
    }

    /// Add a field info
    pub fn add_field(&mut self, field: XRefFieldInfo) {
        self.fields.push(field);
    }

    /// Validate the metadata
    pub fn validate(&self) -> Result<(), String> {
        if self.magic != *XREF_MAGIC {
            return Err("Invalid magic bytes".to_string());
        }
        if self.format_version > XREF_FORMAT_VERSION {
            return Err(format!(
                "Unsupported format version: {} (max: {})",
                self.format_version, XREF_FORMAT_VERSION
            ));
        }
        if self.xref_id.is_empty() {
            return Err("XRef ID cannot be empty".to_string());
        }
        Ok(())
    }
}

/// Result of searching an XRef split
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XRefSearchResult {
    /// Splits that contain matching documents
    pub matching_splits: Vec<MatchingSplit>,

    /// Number of matching splits (not documents!)
    pub num_matching_splits: usize,

    /// Execution time in milliseconds
    pub search_time_ms: u64,
}

impl XRefSearchResult {
    /// Create a new search result
    pub fn new() -> Self {
        Self {
            matching_splits: Vec::new(),
            num_matching_splits: 0,
            search_time_ms: 0,
        }
    }

    /// Get URIs of splits to search
    pub fn split_uris_to_search(&self) -> Vec<&str> {
        self.matching_splits.iter().map(|s| s.uri.as_str()).collect()
    }

    /// Get split IDs to search
    pub fn split_ids_to_search(&self) -> Vec<&str> {
        self.matching_splits.iter().map(|s| s.split_id.as_str()).collect()
    }
}

impl Default for XRefSearchResult {
    fn default() -> Self {
        Self::new()
    }
}

/// Information about a matching split from XRef search
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchingSplit {
    /// Split URI
    pub uri: String,

    /// Split ID
    pub split_id: String,

    /// Document count in this source split
    pub num_docs: u64,

    /// Footer offsets for fast split opening
    pub footer_start: u64,
    pub footer_end: u64,

    /// Optional: time range for additional filtering
    pub time_range_start: Option<i64>,
    pub time_range_end: Option<i64>,

    /// Byte size for load balancing
    pub size_bytes: u64,

    /// Search score (higher = more relevant, based on term coverage)
    pub score: f32,
}

impl MatchingSplit {
    /// Create from an XRefSplitDocument
    pub fn from_split_doc(doc: &XRefSplitDocument, score: f32) -> Self {
        Self {
            uri: doc.uri.clone(),
            split_id: doc.split_id.clone(),
            num_docs: doc.num_docs,
            footer_start: doc.footer_start,
            footer_end: doc.footer_end,
            time_range_start: doc.time_range_start,
            time_range_end: doc.time_range_end,
            size_bytes: doc.size_bytes,
            score,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xref_build_config() {
        let source_splits = vec![
            XRefSourceSplit::new("split-1.split".to_string(), "split-1".to_string(), 1000, 2000)
                .with_doc_mapping_json(r#"{"field_mappings": []}"#.to_string()),
            XRefSourceSplit::new("split-2.split".to_string(), "split-2".to_string(), 3000, 4000)
                .with_doc_mapping_json(r#"{"field_mappings": []}"#.to_string()),
        ];

        let config = XRefBuildConfig::new(
            "xref-001".to_string(),
            "logs-index".to_string(),
            source_splits,
        );

        assert_eq!(config.xref_id, "xref-001");
        assert_eq!(config.index_uid, "logs-index");
        assert_eq!(config.source_splits.len(), 2);
        assert_eq!(config.source_splits[0].footer_start, 1000);
        assert_eq!(config.source_splits[0].footer_end, 2000);
        assert!(config.source_splits[0].doc_mapping_json.is_some());
        assert!(!config.include_positions);
    }

    #[test]
    fn test_xref_split_registry() {
        let mut registry = XRefSplitRegistry::new();

        registry.add_split(XRefSplitDocument::new(
            "s3://bucket/split-1.split".to_string(),
            "split-1".to_string(),
            1000,
        ));

        registry.add_split(XRefSplitDocument::new(
            "s3://bucket/split-2.split".to_string(),
            "split-2".to_string(),
            2000,
        ));

        assert_eq!(registry.len(), 2);
        assert_eq!(registry.total_docs(), 3000);

        let split = registry.get_split(0).unwrap();
        assert_eq!(split.split_id, "split-1");
    }

    #[test]
    fn test_xref_metadata_validation() {
        let metadata = XRefMetadata::new("xref-001".to_string(), "logs-index".to_string());
        assert!(metadata.validate().is_ok());

        let mut invalid = metadata.clone();
        invalid.magic = [0, 0, 0, 0];
        assert!(invalid.validate().is_err());

        let mut empty_id = XRefMetadata::new("".to_string(), "logs-index".to_string());
        assert!(empty_id.validate().is_err());
    }
}
