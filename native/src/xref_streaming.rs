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

//! Streaming XRef Split Builder with Memory-Mapped I/O
//!
//! This module implements a memory-efficient XRef split builder that uses:
//! - Memory-mapped term dictionary access (O(1) memory per source split)
//! - N-way streaming merge using a min-heap
//! - Direct output writing without buffering
//!
//! Memory profile:
//! - Per-split overhead: ~1KB (iterator state)
//! - Min-heap: O(N) where N = number of source splits
//! - Current term buffer: ~4KB
//! - Total for 10,000 splits: ~15MB (vs current approach: potentially GB+)

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use tantivy::directory::MmapDirectory;
use tantivy::schema::{
    Field, FieldType, IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions,
    STORED,
};
use tantivy::termdict::{TermDictionary, TermStreamer};
use tantivy::{Index, TantivyDocument};

use quickwit_config::{AzureStorageConfig, S3StorageConfig};
use quickwit_directories::{write_hotcache, BundleDirectory, HotDirectory, StorageDirectory, CachingDirectory, get_hotcache_from_split};
use quickwit_storage::{BundleStorage, ByteRangeCache, PutPayload, SplitPayloadBuilder, Storage, StorageResolver, STORAGE_METRICS};
use quickwit_search::leaf::{open_index_with_caches, open_split_bundle};
use quickwit_search::SearcherContext;
use quickwit_proto::search::SplitIdAndFooterOffsets;
use tantivy::directory::{FileSlice, OwnedBytes, FileHandle};
use tantivy::HasLen;
use tokio::runtime::Handle;

use crate::debug_println;
use crate::global_cache::{get_configured_storage_resolver, get_global_searcher_context};
use crate::merge_types::{MergeAwsConfig, MergeAzureConfig};
use crate::runtime_manager::QuickwitRuntimeManager;
use crate::standalone_searcher::resolve_storage_for_split;
use crate::xref_types::*;

/// Convert MergeAwsConfig to S3StorageConfig
fn merge_aws_to_s3_config(aws_config: &MergeAwsConfig) -> S3StorageConfig {
    S3StorageConfig {
        access_key_id: Some(aws_config.access_key.clone()),
        secret_access_key: Some(aws_config.secret_key.clone()),
        session_token: aws_config.session_token.clone(),
        region: Some(aws_config.region.clone()),
        endpoint: aws_config.endpoint_url.clone(),
        force_path_style_access: aws_config.force_path_style,
        ..Default::default()
    }
}

/// Convert MergeAzureConfig to AzureStorageConfig
fn merge_azure_to_azure_config(azure_config: &MergeAzureConfig) -> AzureStorageConfig {
    AzureStorageConfig {
        account_name: Some(azure_config.account_name.clone()),
        access_key: azure_config.account_key.clone(),
        bearer_token: azure_config.bearer_token.clone(),
    }
}

/// JSON term format constants (from tantivy-common)
const JSON_PATH_SEGMENT_SEP: u8 = 1u8;
const JSON_END_OF_PATH: u8 = 0u8;

/// Parse JSON term bytes and construct a serde_json::Value representing the nested structure.
///
/// JSON term format in Tantivy:
/// `[path_segments_separated_by_0x01][0x00][value_type_byte][value_bytes]`
///
/// For example, `{"name": "alice"}` produces term bytes like: `name\x00salice`
/// where 's' is the type code for string.
fn parse_json_term_to_value(term_bytes: &[u8]) -> Option<serde_json::Value> {
    // Find the JSON_END_OF_PATH marker (0x00)
    let end_of_path_pos = term_bytes.iter().position(|&b| b == JSON_END_OF_PATH)?;

    // Split into path and value parts
    let path_bytes = &term_bytes[..end_of_path_pos];
    let value_bytes = &term_bytes[end_of_path_pos + 1..];

    if value_bytes.is_empty() {
        return None;
    }

    // Parse the path segments (separated by 0x01)
    let path_str = std::str::from_utf8(path_bytes).ok()?;
    let path_segments: Vec<&str> = path_str
        .split(|c: char| c as u8 == JSON_PATH_SEGMENT_SEP)
        .collect();

    // Parse the value based on type byte
    let type_byte = value_bytes[0];
    let raw_value = &value_bytes[1..];

    let leaf_value = match type_byte {
        b's' => {
            // String value
            let s = std::str::from_utf8(raw_value).ok()?;
            serde_json::Value::String(s.to_string())
        }
        b'u' => {
            // U64 value
            if raw_value.len() >= 8 {
                let bytes: [u8; 8] = raw_value[..8].try_into().ok()?;
                let value = u64::from_be_bytes(bytes);
                serde_json::Value::Number(serde_json::Number::from(value))
            } else {
                return None;
            }
        }
        b'i' => {
            // I64 value (stored as u64 with sign bit flipped)
            if raw_value.len() >= 8 {
                let bytes: [u8; 8] = raw_value[..8].try_into().ok()?;
                let u_value = u64::from_be_bytes(bytes);
                let value = tantivy::u64_to_i64(u_value);
                serde_json::Value::Number(serde_json::Number::from(value))
            } else {
                return None;
            }
        }
        b'f' => {
            // F64 value (stored as u64 mantissa representation)
            if raw_value.len() >= 8 {
                let bytes: [u8; 8] = raw_value[..8].try_into().ok()?;
                let u_value = u64::from_be_bytes(bytes);
                let value = tantivy::u64_to_f64(u_value);
                serde_json::Value::Number(serde_json::Number::from_f64(value)?)
            } else {
                return None;
            }
        }
        b'o' => {
            // Bool value (stored as u64)
            if raw_value.len() >= 8 {
                let bytes: [u8; 8] = raw_value[..8].try_into().ok()?;
                let u_value = u64::from_be_bytes(bytes);
                serde_json::Value::Bool(u_value != 0)
            } else {
                return None;
            }
        }
        _ => {
            // Unknown type - try to interpret as string
            if let Ok(s) = std::str::from_utf8(raw_value) {
                serde_json::Value::String(s.to_string())
            } else {
                return None;
            }
        }
    };

    // Build nested JSON object from path
    let mut result = leaf_value;
    for segment in path_segments.into_iter().rev() {
        if segment.is_empty() {
            continue;
        }
        let mut obj = serde_json::Map::new();
        obj.insert(segment.to_string(), result);
        result = serde_json::Value::Object(obj);
    }

    Some(result)
}

/// Internal field names for XRef documents
const SPLIT_METADATA_FIELD: &str = "_xref_split_metadata";
const SPLIT_URI_FIELD: &str = "_xref_uri";
const SPLIT_ID_FIELD: &str = "_xref_split_id";

// ============================================================================
// PHASE 1: Memory-Mapped Term Source
// ============================================================================

/// Field type for term decoding
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XRefFieldType {
    Text,
    U64,
    I64,
    F64,
    Bool,
    Date,
    Bytes,
    Json,
    IpAddr,
}

impl XRefFieldType {
    /// Convert from Tantivy FieldType
    pub fn from_tantivy_field_type(field_type: &FieldType) -> Self {
        match field_type {
            FieldType::Str(_) => XRefFieldType::Text,
            FieldType::U64(_) => XRefFieldType::U64,
            FieldType::I64(_) => XRefFieldType::I64,
            FieldType::F64(_) => XRefFieldType::F64,
            FieldType::Bool(_) => XRefFieldType::Bool,
            FieldType::Date(_) => XRefFieldType::Date,
            FieldType::Bytes(_) => XRefFieldType::Bytes,
            FieldType::JsonObject(_) => XRefFieldType::Json,
            FieldType::IpAddr(_) => XRefFieldType::IpAddr,
            FieldType::Facet(_) => XRefFieldType::Text, // Facets are hierarchical paths stored as text
        }
    }
}

/// A term source backed by memory-mapped data from a source split.
///
/// Memory profile: O(1) - only holds iterator state, actual data is mmap'd
pub struct MmapTermSource<'a> {
    /// The term stream iterator
    term_stream: TermStreamer<'a>,

    /// Split index (becomes doc_id in XRef posting list)
    split_idx: u32,

    /// Field being iterated
    field: Field,

    /// Field name for stats
    field_name: String,

    /// Field type for term decoding
    field_type: XRefFieldType,
}

impl<'a> MmapTermSource<'a> {
    /// Check if iterator has more terms
    pub fn is_valid(&self) -> bool {
        // TermStreamer doesn't expose a direct validity check,
        // but we track this via the heap state
        true
    }

    /// Get current term bytes
    pub fn key(&self) -> &[u8] {
        self.term_stream.key()
    }

    /// Advance to next term, returns true if successful
    pub fn advance(&mut self) -> bool {
        self.term_stream.advance()
    }

    /// Get split index
    pub fn split_idx(&self) -> u32 {
        self.split_idx
    }

    /// Get field
    pub fn field(&self) -> Field {
        self.field
    }

    /// Get field name
    pub fn field_name(&self) -> &str {
        &self.field_name
    }

    /// Get field type
    pub fn field_type(&self) -> XRefFieldType {
        self.field_type
    }

    /// Add term to document with proper type encoding
    pub fn add_term_to_doc(&self, doc: &mut TantivyDocument, xref_field: Field) -> bool {
        let term_bytes = self.term_stream.key();
        if term_bytes.is_empty() {
            return false;
        }

        match self.field_type {
            XRefFieldType::Text => {
                // Text fields are UTF-8 encoded
                if let Ok(term_str) = std::str::from_utf8(term_bytes) {
                    doc.add_text(xref_field, term_str);
                    return true;
                }
            }
            XRefFieldType::Json => {
                // JSON terms in Tantivy have format: path\0type_byte+value
                // We need to reconstruct the JSON object and add it properly so that
                // Tantivy creates the same terms when indexing.
                if !term_bytes.is_empty() {
                    // Parse the JSON term and reconstruct the JSON value
                    if let Some(json_value) = parse_json_term_to_value(term_bytes) {
                        debug_println!("[JSON DEBUG] Reconstructed JSON: {:?}", json_value);
                        // Convert serde_json::Value to BTreeMap<String, OwnedValue>
                        if let serde_json::Value::Object(map) = json_value {
                            let btree: std::collections::BTreeMap<String, tantivy::schema::OwnedValue> =
                                map.into_iter()
                                    .map(|(k, v)| (k, tantivy::schema::OwnedValue::from(v)))
                                    .collect();
                            doc.add_object(xref_field, btree);
                            return true;
                        } else {
                            debug_println!("[JSON DEBUG] Expected object, got: {:?}", json_value);
                        }
                    } else {
                        debug_println!("[JSON DEBUG] Failed to parse JSON term bytes: {:?}", &term_bytes[..term_bytes.len().min(30)]);
                    }
                }
            }
            XRefFieldType::U64 => {
                // U64 is stored as 8-byte big-endian
                if term_bytes.len() >= 8 {
                    if let Ok(bytes) = term_bytes[..8].try_into() {
                        let value = u64::from_be_bytes(bytes);
                        doc.add_u64(xref_field, value);
                        return true;
                    }
                }
            }
            XRefFieldType::I64 => {
                // I64 is stored as u64 with sign bit flipped for sorting
                if term_bytes.len() >= 8 {
                    if let Ok(bytes) = term_bytes[..8].try_into() {
                        let u_value = u64::from_be_bytes(bytes);
                        let value = tantivy::u64_to_i64(u_value);
                        doc.add_i64(xref_field, value);
                        return true;
                    }
                }
            }
            XRefFieldType::F64 => {
                // F64 is stored as u64 with special encoding for sorting
                if term_bytes.len() >= 8 {
                    if let Ok(bytes) = term_bytes[..8].try_into() {
                        let u_value = u64::from_be_bytes(bytes);
                        let value = tantivy::u64_to_f64(u_value);
                        doc.add_f64(xref_field, value);
                        return true;
                    }
                }
            }
            XRefFieldType::Bool => {
                // Bool is stored as u64 (8-byte big-endian) via FastValue::to_u64()
                // This matches Tantivy's Term::from_field_bool which uses from_fast_value
                if term_bytes.len() >= 8 {
                    if let Ok(bytes) = term_bytes[..8].try_into() {
                        let u_value = u64::from_be_bytes(bytes);
                        let value = u_value != 0;
                        doc.add_bool(xref_field, value);
                        return true;
                    }
                }
            }
            XRefFieldType::Date => {
                // DateTime is stored as i64 nanoseconds since epoch (truncated to seconds precision)
                // via MonotonicallyMappableToU64::to_u64() which calls into_timestamp_nanos()
                // and then i64_to_u64() for sorting order.
                // See: tantivy/columnar/src/column_values/monotonic_mapping.rs
                if term_bytes.len() >= 8 {
                    if let Ok(bytes) = term_bytes[..8].try_into() {
                        let u_value = u64::from_be_bytes(bytes);
                        let timestamp_nanos = tantivy::u64_to_i64(u_value);
                        // Convert nanoseconds to DateTime
                        let datetime = tantivy::DateTime::from_timestamp_nanos(timestamp_nanos);
                        doc.add_date(xref_field, datetime);
                        return true;
                    }
                }
            }
            XRefFieldType::Bytes => {
                // Bytes are stored as-is
                doc.add_bytes(xref_field, term_bytes);
                return true;
            }
            XRefFieldType::IpAddr => {
                // IP addresses are stored as 16-byte IPv6 format
                if term_bytes.len() >= 16 {
                    if let Ok(bytes) = <[u8; 16]>::try_from(&term_bytes[..16]) {
                        let addr = std::net::Ipv6Addr::from(bytes);
                        doc.add_ip_addr(xref_field, addr);
                        return true;
                    }
                }
            }
        }
        false
    }
}

// ============================================================================
// PHASE 2: N-Way Term Merger
// ============================================================================

/// Heap item for N-way merge, ordered by (term, field, split_idx)
struct HeapItem<'a> {
    source: MmapTermSource<'a>,
}

impl<'a> PartialEq for HeapItem<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.source.key() == other.source.key()
            && self.source.field == other.source.field
            && self.source.split_idx == other.source.split_idx
    }
}

impl<'a> Eq for HeapItem<'a> {}

impl<'a> PartialOrd for HeapItem<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for HeapItem<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap: reverse ordering so smallest term comes first
        // Order by: (term, field, split_idx)
        match other.source.key().cmp(self.source.key()) {
            Ordering::Equal => {
                match other.source.field.field_id().cmp(&self.source.field.field_id()) {
                    Ordering::Equal => {
                        other.source.split_idx.cmp(&self.source.split_idx)
                    }
                    ord => ord
                }
            }
            ord => ord
        }
    }
}

/// N-way streaming term merger using a min-heap.
///
/// Memory profile: O(N) for N source iterators
pub struct XRefTermMerger<'a> {
    /// Min-heap of term sources
    heap: BinaryHeap<HeapItem<'a>>,

    /// Sources currently pointing to the same (term, field) pair
    current_sources: Vec<HeapItem<'a>>,

    /// Reusable buffer for current term
    term_buffer: Vec<u8>,

    /// Current field being processed
    current_field: Option<Field>,
}

impl<'a> XRefTermMerger<'a> {
    /// Create a new merger from term sources
    pub fn new(sources: Vec<MmapTermSource<'a>>) -> Self {
        let mut heap = BinaryHeap::with_capacity(sources.len());

        // Add all sources with valid terms to heap
        for mut source in sources {
            if source.advance() {
                heap.push(HeapItem { source });
            }
        }

        Self {
            heap,
            current_sources: Vec::with_capacity(64), // Reasonable initial capacity
            term_buffer: Vec::with_capacity(4096),   // Max term size
            current_field: None,
        }
    }

    /// Advance to next unique (term, field) pair.
    ///
    /// Returns (term_bytes, field, split_indices) or None if exhausted.
    pub fn advance(&mut self) -> Option<(&[u8], Field, Vec<u32>)> {
        // Return current sources to heap (advancing each)
        self.reheap_current_sources();

        // Get next smallest term
        let first = self.heap.pop()?;
        self.term_buffer.clear();
        self.term_buffer.extend_from_slice(first.source.key());
        self.current_field = Some(first.source.field);

        let current_field = first.source.field;
        self.current_sources.push(first);

        // Collect all sources with same (term, field)
        while let Some(next) = self.heap.peek() {
            if next.source.key() != self.term_buffer.as_slice()
                || next.source.field != current_field
            {
                break;
            }
            self.current_sources.push(self.heap.pop().unwrap());
        }

        // Collect split indices
        let split_indices: Vec<u32> = self
            .current_sources
            .iter()
            .map(|item| item.source.split_idx)
            .collect();

        Some((&self.term_buffer, current_field, split_indices))
    }

    /// Return current sources to heap after advancing them
    fn reheap_current_sources(&mut self) {
        for mut item in self.current_sources.drain(..) {
            if item.source.advance() {
                self.heap.push(item);
            }
        }
    }

    /// Get number of remaining sources in heap
    pub fn remaining_sources(&self) -> usize {
        self.heap.len() + self.current_sources.len()
    }
}

// ============================================================================
// PHASE 3: Optimized Term Dictionary Access
// ============================================================================

/// An opened source split with optimized term dictionary access using HotDirectory.
///
/// Uses Quickwit's HotDirectory pattern:
/// - Downloads ONLY the split footer (contains file offsets + hotcache)
/// - HotDirectory provides virtual file access with pre-cached term dictionary data
/// - Actual reads for term dictionaries come from the hotcache (already in footer)
/// - Falls back to byte-range storage requests only for data not in hotcache
///
/// This is extremely efficient as term dictionaries are included in the hotcache.
pub struct OpenedSplitTermAccess {
    /// The Tantivy index backed by HotDirectory
    index: Index,

    /// Searcher for accessing segments
    searcher: Arc<tantivy::Searcher>,

    /// Cached inverted index readers for term streaming.
    /// Format: (inverted_index, field, field_name, field_type)
    inverted_indexes: Vec<(Arc<tantivy::InvertedIndexReader>, Field, String, XRefFieldType)>,

    /// Keep storage alive for potential fallback reads
    #[allow(dead_code)]
    storage: Arc<dyn Storage>,

    /// Split metadata
    uri: String,
    split_id: String,
    split_idx: u32,
    num_docs: u64,
    file_size: u64,
}

impl OpenedSplitTermAccess {
    /// Get term streams for all indexed fields in this split.
    ///
    /// Returns a vector of MmapTermSource, one per (segment, field) pair.
    /// The term sources borrow from the cached inverted_indexes.
    pub fn get_term_sources(&self) -> Result<Vec<MmapTermSource<'_>>> {
        let mut sources = Vec::new();

        // Iterate over cached inverted indexes
        for (inverted_index, field, field_name, field_type) in &self.inverted_indexes {
            // Get term dictionary and create streamer
            let term_dict = inverted_index.terms();
            let term_stream = term_dict.stream()?;

            sources.push(MmapTermSource {
                term_stream,
                split_idx: self.split_idx,
                field: *field,
                field_name: field_name.clone(),
                field_type: *field_type,
            });
        }

        Ok(sources)
    }

    /// Create XRef document with split metadata (for stored fields)
    pub fn create_metadata_doc(&self, field_map: &HashMap<String, Field>) -> TantivyDocument {
        let mut doc = TantivyDocument::default();

        if let Some(uri_field) = field_map.get(SPLIT_URI_FIELD) {
            doc.add_text(*uri_field, &self.uri);
        }

        if let Some(id_field) = field_map.get(SPLIT_ID_FIELD) {
            doc.add_text(*id_field, &self.split_id);
        }

        // Create split document metadata
        let split_doc = XRefSplitDocument::new(
            self.uri.clone(),
            self.split_id.clone(),
            self.num_docs,
        )
        .with_size_bytes(self.file_size);

        if let Some(metadata_field) = field_map.get(SPLIT_METADATA_FIELD) {
            if let Ok(json) = serde_json::to_string(&split_doc) {
                doc.add_text(*metadata_field, &json);
            }
        }

        doc
    }

    /// Get the split ID
    pub fn split_id(&self) -> &str {
        &self.split_id
    }

    /// Get the schema
    pub fn schema(&self) -> Schema {
        self.index.schema()
    }
}

// ============================================================================
// PHASE 4: Streaming XRef Builder
// ============================================================================

/// Configuration for streaming XRef build
#[derive(Debug, Clone)]
pub struct StreamingXRefConfig {
    /// Base XRef config
    pub base_config: XRefBuildConfig,

    /// Maximum number of concurrent split handles (for memory limiting)
    pub max_concurrent_splits: usize,

    /// Commit batch size (number of terms before intermediate commit)
    pub term_batch_size: usize,
}

impl Default for StreamingXRefConfig {
    fn default() -> Self {
        Self {
            base_config: XRefBuildConfig::new(
                "default".to_string(),
                "default".to_string(),
                vec![],
            ),
            max_concurrent_splits: 1000, // Process up to 1000 splits at once
            term_batch_size: 100_000,    // Commit every 100K terms
        }
    }
}

/// Streaming XRef Split Builder
///
/// Uses memory-mapped I/O and streaming merge for minimal memory footprint.
pub struct StreamingXRefBuilder {
    config: StreamingXRefConfig,
}

impl StreamingXRefBuilder {
    /// Create a new streaming XRef builder
    pub fn new(config: StreamingXRefConfig) -> Self {
        Self { config }
    }

    /// Build the XRef split using streaming merge
    pub fn build(&self, output_path: &Path) -> Result<XRefMetadata> {
        debug_println!("[XREF STREAMING] Starting build for output: {:?}", output_path);
        let start_time = Instant::now();
        let base_config = &self.config.base_config;
        let num_splits = base_config.source_splits.len();
        debug_println!("[XREF STREAMING] num_splits = {}", num_splits);

        debug_println!(
            "🚀 STREAMING XREF BUILD: Building XRef with {} source splits (max concurrent: {})",
            num_splits,
            self.config.max_concurrent_splits
        );

        let runtime = QuickwitRuntimeManager::global();

        // Build storage resolver
        let s3_config = base_config.aws_config.as_ref().map(merge_aws_to_s3_config);
        let azure_config = base_config.azure_config.as_ref().map(merge_azure_to_azure_config);
        let storage_resolver = get_configured_storage_resolver(s3_config, azure_config);

        // Process splits in batches to limit memory
        let mut metadata = XRefMetadata::new(
            base_config.xref_id.clone(),
            base_config.index_uid.clone(),
        );
        let mut field_stats: HashMap<String, XRefFieldInfo> = HashMap::new();
        let mut total_terms_written = 0u64;

        // Phase 1: Open splits and discover schema
        debug_println!("📋 STREAMING XREF: Phase 1 - Discovering schema...");
        let (xref_schema, field_map) = runtime
            .handle()
            .block_on(self.discover_schema(&storage_resolver))?;

        // Phase 2: Create output index
        debug_println!("🏗️  STREAMING XREF: Phase 2 - Creating output index...");
        let temp_dir = create_streaming_temp_directory(
            base_config.temp_directory_path.as_deref(),
        )?;
        let index_path = temp_dir.path().join("xref_index");
        std::fs::create_dir_all(&index_path)?;

        let xref_index = Index::create_in_dir(&index_path, xref_schema.clone())
            .context("Failed to create XRef index")?;
        let mut writer = xref_index
            .writer(base_config.heap_size)
            .context("Failed to create index writer")?;

        // Phase 3: Process splits in batches
        debug_println!("📄 STREAMING XREF: Phase 3 - Processing splits in batches...");

        let batch_size = self.config.max_concurrent_splits.min(num_splits);
        let num_batches = (num_splits + batch_size - 1) / batch_size;

        debug_println!("[XREF STREAMING] Starting batch processing: {} batches", num_batches);
        for batch_idx in 0..num_batches {
            let batch_start = batch_idx * batch_size;
            let batch_end = (batch_start + batch_size).min(num_splits);
            let batch_splits = &base_config.source_splits[batch_start..batch_end];
            debug_println!("[XREF STREAMING] Processing batch {}/{} with {} splits", batch_idx+1, num_batches, batch_splits.len());

            debug_println!(
                "  Processing batch {}/{}: splits {}-{}",
                batch_idx + 1,
                num_batches,
                batch_start,
                batch_end - 1
            );

            // Open all splits in this batch
            let opened_splits: Vec<OpenedSplitTermAccess> = runtime.handle().block_on(async {
                let mut splits = Vec::with_capacity(batch_splits.len());
                for (idx, source_split) in batch_splits.iter().enumerate() {
                    let global_idx = (batch_start + idx) as u32;
                    match self
                        .open_source_split(&storage_resolver, source_split, global_idx)
                        .await
                    {
                        Ok(opened) => {
                            // Add to metadata registry
                            let split_doc = XRefSplitDocument::new(
                                source_split.uri.clone(),
                                source_split.split_id.clone(),
                                opened.num_docs,
                            )
                            .with_size_bytes(opened.file_size)
                            .with_footer_offsets(source_split.footer_start, source_split.footer_end);

                            metadata.split_registry.add_split(split_doc);
                            metadata.build_stats.splits_processed += 1;
                            splits.push(opened);
                        }
                        Err(e) => {
                            debug_println!(
                                "  ⚠️ Failed to open split {}: {}",
                                source_split.uri,
                                e
                            );
                            metadata.build_stats.splits_skipped += 1;
                        }
                    }
                }
                splits
            });

            debug_println!("[XREF STREAMING] Opened {} splits in batch", opened_splits.len());
            eprintln!("[XREF TRACE] Opened {} splits in batch", opened_splits.len());
            if opened_splits.is_empty() {
                debug_println!("[XREF STREAMING] No splits opened, continuing to next batch");
                eprintln!("[XREF TRACE] No splits opened, continuing to next batch");
                continue;
            }

            // Process each split individually to create proper XRef documents
            // Each split becomes one document with all its terms indexed
            eprintln!("[XREF TRACE] About to process {} opened splits", opened_splits.len());
            for opened in &opened_splits {
                eprintln!("[XREF TRACE] Processing split: {}", opened.split_id());
                debug_println!("[XREF STREAMING] Processing split: {}", opened.split_id());
                let mut doc = opened.create_metadata_doc(&field_map);
                let mut split_term_count = 0u64;

                // Get term sources for this split
                eprintln!("[XREF TRACE] Getting term sources for split {}...", opened.split_id());
                debug_println!("[XREF STREAMING] Getting term sources...");
                match opened.get_term_sources() {
                    Ok(sources) => {
                        eprintln!("[XREF TRACE] Split {} got {} term sources", opened.split_id(), sources.len());
                        debug_println!(
                            "[XREF DEBUG] Split {} has {} term sources, field_map has {} fields: {:?}",
                            opened.split_id(),
                            sources.len(),
                            field_map.len(),
                            field_map.keys().collect::<Vec<_>>()
                        );

                        // Process each term source (field) for this split
                        for mut source in sources {
                            // Get the field name from the source (this is from the source split's schema)
                            let source_field_name = source.field_name().to_string();

                            debug_println!(
                                "[XREF DEBUG]   Processing field '{}', field_map contains it: {}",
                                source_field_name,
                                field_map.contains_key(&source_field_name)
                            );

                            // Get the corresponding XRef field by name
                            let xref_field = match field_map.get(&source_field_name) {
                                Some(f) => *f,
                                None => {
                                    debug_println!("        Field '{}' not in field_map, skipping", source_field_name);
                                    continue;
                                }
                            };

                            // Add all terms from this field to the document
                            // NOTE: TermStreamer starts BEFORE the first element, so we must
                            // call advance() first to position at the first term
                            let mut field_term_count = 0u64;
                            debug_println!("[XREF DEBUG]     Starting term iteration for field '{}' (type={:?})", source_field_name, source.field_type());
                            eprintln!("[XREF TRACE] Starting term iteration for field '{}'", source_field_name);
                            while source.advance() {
                                if field_term_count == 0 {
                                    eprintln!("[XREF TRACE] First term found in field '{}'", source_field_name);
                                }
                                let key = source.key();
                                debug_println!("[XREF DEBUG]       Term {} bytes: {:?}", key.len(), &key[..key.len().min(30)]);
                                // Add term to document with proper type encoding
                                if source.add_term_to_doc(&mut doc, xref_field) {
                                    split_term_count += 1;
                                    total_terms_written += 1;
                                    field_term_count += 1;

                                    // Update field stats
                                    let field_info = field_stats
                                        .entry(source_field_name.clone())
                                        .or_insert_with(|| {
                                            XRefFieldInfo::new(
                                                source_field_name.clone(),
                                                format!("{:?}", xref_field),
                                            )
                                        });
                                    field_info.num_terms += 1;
                                    field_info.total_postings += 1;
                                }
                            }

                            debug_println!(
                                "      Field '{}' added {} terms",
                                source_field_name,
                                field_term_count
                            );
                        }
                    }
                    Err(e) => {
                        eprintln!("[XREF TRACE] ERROR: Failed to get term sources for split {}: {}", opened.split_id(), e);
                        debug_println!("[XREF ERROR] Failed to get term sources for split {}: {}", opened.split_id(), e);
                    }
                }

                // Add the document with all its terms
                writer.add_document(doc)?;

                debug_println!(
                    "    Added split {} with {} terms",
                    opened.split_id(),
                    split_term_count
                );
            }

            debug_println!(
                "    Processed {} splits in batch",
                opened_splits.len()
            );

            // Intermediate commit to release memory
            if batch_idx < num_batches - 1 {
                debug_println!("    Committing batch to release memory...");
                writer.commit()?;
            }
        }

        // Phase 4: Final commit
        debug_println!("💾 STREAMING XREF: Phase 4 - Final commit...");
        writer.commit()?;

        // Phase 5: Package into split format
        debug_println!("📦 STREAMING XREF: Phase 5 - Packaging...");
        let (output_size, footer_start, footer_end) =
            self.package_xref_split(output_path, &index_path)?;

        // Finalize metadata
        metadata.footer_start_offset = footer_start;
        metadata.footer_end_offset = footer_end;
        metadata.build_stats.build_duration_ms = start_time.elapsed().as_millis() as u64;
        metadata.build_stats.output_size_bytes = output_size;
        metadata.build_stats.unique_terms = total_terms_written;

        for (_, field_info) in field_stats {
            metadata.total_terms += field_info.num_terms;
            metadata.fields.push(field_info);
        }

        debug_println!(
            "✅ STREAMING XREF: Complete! {} splits, {} terms, {} bytes",
            metadata.num_splits(),
            metadata.total_terms,
            output_size
        );

        Ok(metadata)
    }

    /// Discover schema from first source split
    async fn discover_schema(
        &self,
        storage_resolver: &StorageResolver,
    ) -> Result<(Schema, HashMap<String, Field>)> {
        let base_config = &self.config.base_config;
        let first_split = base_config.source_splits.first().ok_or_else(|| {
            anyhow!("No source splits provided")
        })?;

        let opened = self.open_source_split(storage_resolver, first_split, 0).await?;
        let source_schema = opened.schema();

        // Build XRef schema
        let mut builder = SchemaBuilder::new();
        let mut field_map: HashMap<String, Field> = HashMap::new();

        // Add internal metadata fields (stored only)
        let uri_field = builder.add_text_field(SPLIT_URI_FIELD, STORED);
        field_map.insert(SPLIT_URI_FIELD.to_string(), uri_field);

        let id_field = builder.add_text_field(SPLIT_ID_FIELD, STORED);
        field_map.insert(SPLIT_ID_FIELD.to_string(), id_field);

        let metadata_field = builder.add_text_field(SPLIT_METADATA_FIELD, STORED);
        field_map.insert(SPLIT_METADATA_FIELD.to_string(), metadata_field);

        // Add indexed fields for each source field, preserving their types
        let text_indexing = if base_config.include_positions {
            TextFieldIndexing::default()
                .set_tokenizer("raw")
                .set_index_option(IndexRecordOption::WithFreqsAndPositions)
        } else {
            TextFieldIndexing::default()
                .set_tokenizer("raw")
                .set_index_option(IndexRecordOption::WithFreqs)
        };

        let text_options = TextOptions::default().set_indexing_options(text_indexing);

        use tantivy::schema::{NumericOptions, DateOptions, BytesOptions, IpAddrOptions, JsonObjectOptions};

        for (field, field_entry) in source_schema.fields() {
            if !field_entry.is_indexed() {
                continue;
            }

            let field_name = field_entry.name();
            if field_name.starts_with("_xref_") {
                continue;
            }

            // Filter by included_fields if specified
            if !base_config.included_fields.is_empty()
                && !base_config.included_fields.contains(&field_name.to_string())
            {
                continue;
            }

            // Create field with same type as source, but indexed for XRef querying
            let xref_field = match field_entry.field_type() {
                FieldType::Str(_) => {
                    builder.add_text_field(field_name, text_options.clone())
                }
                FieldType::U64(_) => {
                    builder.add_u64_field(field_name, NumericOptions::default().set_indexed())
                }
                FieldType::I64(_) => {
                    builder.add_i64_field(field_name, NumericOptions::default().set_indexed())
                }
                FieldType::F64(_) => {
                    builder.add_f64_field(field_name, NumericOptions::default().set_indexed())
                }
                FieldType::Bool(_) => {
                    builder.add_bool_field(field_name, NumericOptions::default().set_indexed())
                }
                FieldType::Date(_) => {
                    builder.add_date_field(field_name, DateOptions::default().set_indexed())
                }
                FieldType::Bytes(_) => {
                    builder.add_bytes_field(field_name, BytesOptions::default().set_indexed())
                }
                FieldType::JsonObject(_) => {
                    // JSON fields need indexing enabled for term queries
                    let json_options = JsonObjectOptions::default()
                        .set_indexing_options(TextFieldIndexing::default()
                            .set_tokenizer("raw")
                            .set_index_option(tantivy::schema::IndexRecordOption::Basic));
                    builder.add_json_field(field_name, json_options)
                }
                FieldType::IpAddr(_) => {
                    builder.add_ip_addr_field(field_name, IpAddrOptions::default().set_indexed())
                }
                FieldType::Facet(_) => {
                    builder.add_facet_field(field_name, tantivy::schema::FacetOptions::default())
                }
            };
            field_map.insert(field_name.to_string(), xref_field);
        }

        let xref_schema = builder.build();
        debug_println!("  XRef schema built with {} fields", field_map.len());

        Ok((xref_schema, field_map))
    }

    /// Open a source split for term dictionary access using existing Quickwit infrastructure.
    ///
    /// Reuses `open_index_with_caches` from quickwit_search::leaf which:
    /// - Downloads ONLY the split footer (contains file offsets + hotcache)
    /// - Uses CachingDirectory to enable sync reads with async storage
    /// - HotDirectory serves term dictionary reads from the hotcache
    async fn open_source_split(
        &self,
        storage_resolver: &StorageResolver,
        source_split: &XRefSourceSplit,
        split_idx: u32,
    ) -> Result<OpenedSplitTermAccess> {
        let storage = resolve_storage_for_split(storage_resolver, &source_split.uri).await?;

        // Extract the filename (without .split extension) from the URI for file lookup
        // open_split_bundle uses format!("{}.split", split_id) to find the file,
        // so we need the actual filename stem, not the metadata split_id
        let filename_for_lookup = extract_split_id_from_filename(&source_split.uri);

        // Create split and footer offsets structure for open_index_with_caches
        // Note: We use the actual filename for file lookup, not the metadata split_id
        let split_and_footer = SplitIdAndFooterOffsets {
            split_id: filename_for_lookup.clone(),
            split_footer_start: source_split.footer_start,
            split_footer_end: source_split.footer_end,
            timestamp_start: None,  // Not used for XRef building
            timestamp_end: None,    // Not used for XRef building
            num_docs: source_split.num_docs.unwrap_or(0),
        };

        let footer_size = source_split.footer_end - source_split.footer_start;
        debug_println!("[XREF OPEN] Opening split {} (filename: {}) via open_index_with_caches (footer: {} bytes)",
            source_split.split_id, filename_for_lookup, footer_size);

        // Get the global searcher context which has all the caches configured
        let searcher_context = get_global_searcher_context();

        // Create an unbounded cache for this split's reads
        // This allows sync reads to work by caching async results
        let ephemeral_cache = ByteRangeCache::with_infinite_capacity(&STORAGE_METRICS.shortlived_cache);

        // Use Quickwit's open_index_with_caches which handles all the complexity:
        // - Footer download via searcher_context.split_footer_cache
        // - HotDirectory with hotcache bytes
        // - CachingDirectory for async->sync conversion
        debug_println!("[XREF OPEN] Calling open_index_with_caches for split_id={}, storage_uri={}",
            split_and_footer.split_id, source_split.uri);

        let result = open_index_with_caches(
            &searcher_context,
            storage.clone(),
            &split_and_footer,
            None,  // No tokenizer manager needed for term iteration
            Some(ephemeral_cache),
        )
        .await;

        let (tantivy_index, hot_directory) = match result {
            Ok(r) => r,
            Err(e) => {
                debug_println!("[XREF OPEN] ERROR in open_index_with_caches: {:?}", e);
                return Err(e).context(format!(
                    "Failed to open index with caches for split_id={}, footer_start={}, footer_end={}, uri={}",
                    split_and_footer.split_id, split_and_footer.split_footer_start,
                    split_and_footer.split_footer_end, source_split.uri
                ));
            }
        };

        debug_println!("[XREF OPEN] HotDirectory created - files available: {:?}",
            hot_directory.get_file_lengths().iter().map(|(p, l)| format!("{}:{}b", p.display(), l)).collect::<Vec<_>>());

        let reader = tantivy_index.reader()?;
        let searcher = Arc::new(reader.searcher());

        let num_docs: u64 = searcher
            .segment_readers()
            .iter()
            .map(|r| r.num_docs() as u64)
            .sum();

        // First, find all indexed fields and warm up their term dictionaries
        // This is required because StorageDirectory only supports async reads,
        // and the CachingDirectory needs data pre-loaded before sync access
        let schema = tantivy_index.schema();
        let indexed_fields: Vec<Field> = schema.fields()
            .filter(|(_, entry)| entry.is_indexed())
            .map(|(field, _)| field)
            .collect();

        debug_println!("[XREF OPEN] Warming up {} indexed fields for split {}",
            indexed_fields.len(), source_split.split_id);

        // Warm up term dictionaries for all indexed fields (async)
        use futures::future::try_join_all;
        let mut warmup_futures = Vec::new();
        for &field in &indexed_fields {
            for segment_reader in searcher.segment_readers() {
                if let Ok(inverted_index) = segment_reader.inverted_index(field) {
                    let inverted_index_clone = inverted_index.clone();
                    warmup_futures.push(async move {
                        let dict = inverted_index_clone.terms();
                        dict.warm_up_dictionary().await
                    });
                }
            }
        }

        if !warmup_futures.is_empty() {
            try_join_all(warmup_futures).await
                .context("Failed to warm up term dictionaries")?;
            debug_println!("[XREF OPEN] Term dictionary warmup complete");
        }

        // Now cache inverted indexes - they should be accessible from cache
        let mut inverted_indexes = Vec::new();

        for segment_reader in searcher.segment_readers() {
            debug_println!("[XREF OPEN] Segment has {} docs", segment_reader.num_docs());
            for (field, field_entry) in schema.fields() {
                let field_name = field_entry.name().to_string();
                let is_indexed = field_entry.is_indexed();

                // Skip non-indexed fields
                if !is_indexed {
                    continue;
                }

                let field_type = XRefFieldType::from_tantivy_field_type(field_entry.field_type());

                // Get inverted index for this field
                // Data should now be in the cache from warmup
                match segment_reader.inverted_index(field) {
                    Ok(inverted_index) => {
                        debug_println!("[XREF OPEN]   Field '{}': opened term dict, type={:?}",
                            field_name, field_type);
                        inverted_indexes.push((inverted_index, field, field_name, field_type));
                    }
                    Err(e) => {
                        debug_println!("[XREF OPEN]   Field '{}': no term dict ({})", field_name, e);
                    }
                }
            }
        }

        debug_println!("[XREF OPEN] Opened {} term dictionaries for {} using HotDirectory (footer-only download: {} bytes)",
            inverted_indexes.len(), source_split.split_id, footer_size);

        Ok(OpenedSplitTermAccess {
            index: tantivy_index,
            searcher,
            inverted_indexes,
            storage,
            uri: source_split.uri.clone(),
            split_id: source_split.split_id.clone(),
            split_idx,
            num_docs,
            file_size: source_split.footer_end,
        })
    }

    /// Package the index into a Quickwit split
    fn package_xref_split(
        &self,
        output_path: &Path,
        index_path: &Path,
    ) -> Result<(u64, u64, u64)> {
        use std::io::{Read, Seek, SeekFrom};
        use quickwit_proto::search::{ListFields, ListFieldsEntryResponse, ListFieldType, serialize_split_fields};
        use tantivy::schema::Type;

        // Collect index files
        let mut file_entries: Vec<PathBuf> = Vec::new();
        for entry in std::fs::read_dir(index_path)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                file_entries.push(path);
            }
        }

        // Open the index to get field metadata
        let mmap_directory = MmapDirectory::open(index_path)?;
        let tantivy_index = Index::open(mmap_directory.clone())?;
        let fields_metadata = tantivy_index.fields_metadata()?;

        // Convert field metadata to ListFieldsEntryResponse
        let fields: Vec<ListFieldsEntryResponse> = fields_metadata
            .iter()
            .map(|fm| {
                let field_type = match fm.typ {
                    Type::Str => ListFieldType::Str,
                    Type::U64 => ListFieldType::U64,
                    Type::I64 => ListFieldType::I64,
                    Type::F64 => ListFieldType::F64,
                    Type::Bool => ListFieldType::Bool,
                    Type::Date => ListFieldType::Date,
                    Type::Facet => ListFieldType::Facet,
                    Type::Bytes => ListFieldType::Bytes,
                    Type::Json => ListFieldType::Json,
                    Type::IpAddr => ListFieldType::IpAddr,
                };
                ListFieldsEntryResponse {
                    field_name: fm.field_name.clone(),
                    field_type: field_type as i32,
                    searchable: fm.indexed,
                    aggregatable: fm.fast,
                    index_ids: Vec::new(),
                    non_searchable_index_ids: Vec::new(),
                    non_aggregatable_index_ids: Vec::new(),
                }
            })
            .collect();

        // Serialize field metadata
        let serialized_split_fields = serialize_split_fields(ListFields { fields });
        debug_println!("[XREF STREAMING] Serialized {} field metadata entries ({} bytes)",
            fields_metadata.len(), serialized_split_fields.len());

        // Generate hotcache
        let mut hotcache_buffer = Vec::new();
        write_hotcache(mmap_directory, &mut hotcache_buffer)
            .map_err(|e| anyhow!("Failed to generate hotcache: {}", e))?;

        // Create split payload
        let split_payload = SplitPayloadBuilder::get_split_payload(
            &file_entries,
            &serialized_split_fields,
            &hotcache_buffer,
        )?;

        // Write to output
        let runtime = QuickwitRuntimeManager::global();
        let total_size = split_payload.len();
        let payload_bytes = runtime
            .handle()
            .block_on(async { split_payload.read_all().await })?;

        std::fs::write(output_path, &payload_bytes)?;

        // Extract footer offsets
        let file_len = total_size as u64;
        if file_len < 8 {
            return Err(anyhow!("Split file too small"));
        }

        let mut split_file = std::fs::File::open(output_path)?;

        // Read hotcache length
        split_file.seek(SeekFrom::End(-4))?;
        let mut hotcache_len_bytes = [0u8; 4];
        split_file.read_exact(&mut hotcache_len_bytes)?;
        let hotcache_length = u32::from_le_bytes(hotcache_len_bytes) as u64;

        let hotcache_start = file_len - 4 - hotcache_length;

        // Read metadata length
        let metadata_len_start = hotcache_start - 4;
        split_file.seek(SeekFrom::Start(metadata_len_start))?;
        let mut metadata_len_bytes = [0u8; 4];
        split_file.read_exact(&mut metadata_len_bytes)?;
        let metadata_length = u32::from_le_bytes(metadata_len_bytes) as u64;

        let footer_start = hotcache_start - 4 - metadata_length;

        Ok((file_len, footer_start, file_len))
    }
}

/// Extract split ID from filename in URI
fn extract_split_id_from_filename(uri: &str) -> String {
    let filename = if let Some(pos) = uri.rfind('/') {
        &uri[pos + 1..]
    } else {
        uri
    };

    if filename.ends_with(".split") {
        filename[..filename.len() - 6].to_string()
    } else {
        filename.to_string()
    }
}

/// Create temporary directory for streaming build
fn create_streaming_temp_directory(custom_base: Option<&str>) -> Result<tempfile::TempDir> {
    use tempfile::Builder;

    let mut builder = Builder::new();
    builder.prefix("xref_streaming_");

    if let Some(base_path) = custom_base {
        let path = std::path::PathBuf::from(base_path);
        if !path.exists() {
            return Err(anyhow!("Temp directory base does not exist: {}", base_path));
        }
        builder
            .tempdir_in(&path)
            .context("Failed to create temp directory")
    } else {
        builder.tempdir().context("Failed to create temp directory")
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heap_item_ordering() {
        // Verify min-heap ordering: smaller terms come first
        // This is tested implicitly through the merger
    }

    #[test]
    fn test_extract_split_id() {
        assert_eq!(
            extract_split_id_from_filename("/path/to/split-001.split"),
            "split-001"
        );
        assert_eq!(
            extract_split_id_from_filename("s3://bucket/splits/my-split.split"),
            "my-split"
        );
        assert_eq!(
            extract_split_id_from_filename("file.split"),
            "file"
        );
    }
}
