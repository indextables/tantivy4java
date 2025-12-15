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

//! Cross-Reference Split Builder
//!
//! This module implements the XRef split builder which creates a lightweight index
//! that consolidates term dictionaries from multiple source splits. Each document
//! in the XRef split represents one source split, enabling fast query routing.
//!
//! KEY DESIGN: Creates N documents where N = number of source splits.
//! Each document represents one source split, and posting lists point
//! to these split-level documents (not original documents).
//!
//! Algorithm:
//! 1. Open each source split using fast retrieval (milliseconds)
//! 2. For each split, extract its term dictionary
//! 3. For each term, record that it exists in this split (split_index)
//! 4. Build XRef index with N documents (one per split)
//! 5. Each document contains all terms from that split
//! 6. Posting lists naturally point to split indices

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use tantivy::directory::MmapDirectory;
use tantivy::schema::{
    Field, IndexRecordOption, Schema, SchemaBuilder, TextFieldIndexing, TextOptions, STORED, TEXT,
};
use tantivy::{Index, IndexWriter, TantivyDocument};

use quickwit_config::{S3StorageConfig, AzureStorageConfig};
use quickwit_directories::write_hotcache;
use quickwit_storage::{PutPayload, Storage, StorageResolver, SplitPayloadBuilder};

use crate::debug_println;
use crate::global_cache::get_configured_storage_resolver;
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

/// XRef split file extension
pub const XREF_EXTENSION: &str = ".xref.split";

/// Internal field name for storing split metadata as JSON
const SPLIT_METADATA_FIELD: &str = "_xref_split_metadata";

/// Internal field name for split URI (stored)
const SPLIT_URI_FIELD: &str = "_xref_uri";

/// Internal field name for split ID (stored)
const SPLIT_ID_FIELD: &str = "_xref_split_id";

/// Builder for creating XRef splits
///
/// KEY DESIGN: Creates N documents where N = number of source splits.
/// Each document represents one source split, and posting lists point
/// to these split-level documents (not original documents).
pub struct XRefSplitBuilder {
    config: XRefBuildConfig,
}

impl XRefSplitBuilder {
    /// Create a new XRef split builder
    pub fn new(config: XRefBuildConfig) -> Self {
        Self { config }
    }

    /// Build the XRef split
    ///
    /// Algorithm:
    /// 1. Open each source split using fast retrieval (milliseconds)
    /// 2. For each split, extract its term dictionary
    /// 3. For each term, record that it exists in this split (split_index)
    /// 4. Build XRef index with N documents (one per split)
    /// 5. Each document contains all terms from that split
    /// 6. Posting lists naturally point to split indices
    pub fn build(&self, output_path: &Path) -> Result<XRefMetadata> {
        let start_time = Instant::now();
        let num_splits = self.config.source_splits.len();

        debug_println!(
            "🔧 XREF BUILD: Building XRef split with {} source splits",
            num_splits
        );

        // Use the global runtime for async operations
        let runtime = QuickwitRuntimeManager::global();

        // Phase 1: Analyze source splits and build unified schema
        debug_println!("📋 XREF BUILD: Phase 1 - Analyzing source splits...");
        let (xref_schema, field_map) =
            runtime.handle().block_on(self.build_xref_schema())?;

        // Phase 2: Create XRef index in a temporary directory
        debug_println!("🏗️  XREF BUILD: Phase 2 - Creating XRef index...");
        let temp_dir = create_xref_temp_directory(self.config.temp_directory_path.as_deref())
            .context("Failed to create temp directory")?;
        let index_path = temp_dir.path().join("xref_index");
        std::fs::create_dir_all(&index_path)?;

        let xref_index = Index::create_in_dir(&index_path, xref_schema.clone())
            .context("Failed to create XRef index")?;

        // Use configured heap size (validated on Java side, minimum 15MB)
        let mut writer = xref_index
            .writer(self.config.heap_size)
            .context("Failed to create index writer")?;

        // Phase 3: Process each source split and create XRef documents
        debug_println!("📄 XREF BUILD: Phase 3 - Processing source splits...");
        let mut metadata = XRefMetadata::new(self.config.xref_id.clone(), self.config.index_uid.clone());
        let mut field_stats: HashMap<String, XRefFieldInfo> = HashMap::new();
        let mut bytes_read: u64 = 0;

        for (split_idx, source_split) in self.config.source_splits.iter().enumerate() {
            debug_println!(
                "  Processing split {}/{}: {} (footer: {}-{})",
                split_idx + 1,
                num_splits,
                source_split.uri,
                source_split.footer_start,
                source_split.footer_end
            );

            match runtime.handle().block_on(self.process_source_split(
                source_split,
                split_idx,
                &xref_schema,
                &field_map,
                &mut writer,
                &mut field_stats,
            )) {
                Ok((split_doc, split_bytes)) => {
                    debug_println!("  ✅ Split processed successfully: {} docs", split_doc.num_docs);
                    metadata.split_registry.add_split(split_doc);
                    bytes_read += split_bytes;
                    metadata.build_stats.splits_processed += 1;
                }
                Err(e) => {
                    debug_println!("  ⚠️ XREF BUILD: Failed to process split {}: {}", source_split.uri, e);
                    metadata.build_stats.splits_skipped += 1;
                }
            }
        }

        debug_println!("📊 XREF BUILD: After processing loop: processed={}, skipped={}, registry count={}",
            metadata.build_stats.splits_processed,
            metadata.build_stats.splits_skipped,
            metadata.split_registry.splits.len()
        );

        // Phase 4: Commit the XRef index
        debug_println!("💾 XREF BUILD: Phase 4 - Committing index...");
        writer.commit().context("Failed to commit XRef index")?;

        // Phase 5: Package into Quickwit split format
        debug_println!("📦 XREF BUILD: Phase 5 - Packaging XRef split...");
        let (output_size, footer_start, footer_end) = self.package_xref_split(output_path, &index_path, &metadata)?;

        // Set footer offsets on metadata for SplitSearcher compatibility
        metadata.footer_start_offset = footer_start;
        metadata.footer_end_offset = footer_end;

        // Finalize metadata
        metadata.build_stats.build_duration_ms = start_time.elapsed().as_millis() as u64;
        metadata.build_stats.bytes_read = bytes_read;
        metadata.build_stats.output_size_bytes = output_size;

        // Calculate compression ratio (estimate: full merge would be 10x the output)
        let estimated_full_merge = bytes_read;
        if estimated_full_merge > 0 {
            metadata.build_stats.compression_ratio =
                output_size as f64 / estimated_full_merge as f64;
        }

        // Add field statistics
        for (_, field_info) in field_stats {
            metadata.total_terms += field_info.num_terms;
            metadata.build_stats.unique_terms += field_info.num_terms;
            metadata.fields.push(field_info);
        }

        debug_println!(
            "✅ XREF BUILD: Complete! {} splits, {} unique terms, {} bytes output",
            metadata.num_splits(),
            metadata.total_terms,
            output_size
        );

        Ok(metadata)
    }

    /// Build the XRef schema by analyzing source splits
    async fn build_xref_schema(&self) -> Result<(Schema, HashMap<String, Field>)> {
        // Convert config types for storage resolver
        let s3_config = self.config.aws_config.as_ref().map(merge_aws_to_s3_config);
        let azure_config = self.config.azure_config.as_ref().map(merge_azure_to_azure_config);

        // Debug: Log the S3/Azure configuration being used
        if let Some(ref aws) = self.config.aws_config {
            debug_println!("📁 XREF: Creating storage resolver with AWS config:");
            debug_println!("📁 XREF:   region: {}", aws.region);
            debug_println!("📁 XREF:   access_key (first 8): {}...", &aws.access_key[..8.min(aws.access_key.len())]);
            debug_println!("📁 XREF:   session_token: {}", if aws.session_token.is_some() { "present" } else { "none" });
            debug_println!("📁 XREF:   endpoint_url: {:?}", aws.endpoint_url);
            debug_println!("📁 XREF:   force_path_style: {}", aws.force_path_style);
        } else {
            debug_println!("📁 XREF: No AWS config provided");
        }
        if let Some(ref azure) = self.config.azure_config {
            debug_println!("📁 XREF: Creating storage resolver with Azure config:");
            debug_println!("📁 XREF:   account_name: {}", azure.account_name);
        } else {
            debug_println!("📁 XREF: No Azure config provided");
        }

        let storage_resolver = get_configured_storage_resolver(s3_config.clone(), azure_config.clone());
        debug_println!("📁 XREF: Storage resolver created");

        // Sample the first split to discover the schema
        let first_source_split = self.config.source_splits.first().ok_or_else(|| {
            anyhow!("No source splits provided")
        })?;

        debug_println!("  Opening first split for schema: {} (footer: {}-{})",
            first_source_split.uri, first_source_split.footer_start, first_source_split.footer_end);

        let (schema, _) = self.open_source_split_with_offsets(&storage_resolver, first_source_split).await?;

        // Build XRef schema with text fields for each indexed field
        let mut builder = SchemaBuilder::new();
        let mut field_map: HashMap<String, Field> = HashMap::new();

        // Add internal metadata fields
        let uri_field = builder.add_text_field(SPLIT_URI_FIELD, STORED);
        field_map.insert(SPLIT_URI_FIELD.to_string(), uri_field);

        let id_field = builder.add_text_field(SPLIT_ID_FIELD, STORED);
        field_map.insert(SPLIT_ID_FIELD.to_string(), id_field);

        let metadata_field = builder.add_text_field(SPLIT_METADATA_FIELD, STORED);
        field_map.insert(SPLIT_METADATA_FIELD.to_string(), metadata_field);

        // Add indexed text fields for each source field
        let text_indexing = if self.config.include_positions {
            TextFieldIndexing::default()
                .set_tokenizer("raw")
                .set_index_option(IndexRecordOption::WithFreqsAndPositions)
        } else {
            TextFieldIndexing::default()
                .set_tokenizer("raw")
                .set_index_option(IndexRecordOption::WithFreqs)
        };

        let text_options = TextOptions::default().set_indexing_options(text_indexing);

        for (field, field_entry) in schema.fields() {
            // Skip non-indexed fields
            if !field_entry.is_indexed() {
                continue;
            }

            let field_name = field_entry.name();

            // Skip internal fields
            if field_name.starts_with("_xref_") {
                continue;
            }

            // Filter by included_fields if specified
            if !self.config.included_fields.is_empty()
                && !self.config.included_fields.contains(&field_name.to_string())
            {
                continue;
            }

            // Add text field for term indexing
            let xref_field = builder.add_text_field(field_name, text_options.clone());
            field_map.insert(field_name.to_string(), xref_field);

            debug_println!("  Added XRef field: {} (from source field {:?})", field_name, field_entry.field_type());
        }

        let xref_schema = builder.build();
        debug_println!("  XRef schema built with {} fields", field_map.len());

        Ok((xref_schema, field_map))
    }

    /// Open a source split using the provided footer offsets
    ///
    /// IMPORTANT: This method only downloads the footer portion of the split (footer_start..footer_end),
    /// NOT the entire split. The footer contains:
    /// - BundleStorageFileOffsets (JSON metadata about file offsets within the bundle)
    /// - Hotcache (frequently accessed byte ranges)
    ///
    /// The BundleStorage then provides on-demand byte-range access for any additional data needed,
    /// enabling efficient access to splits stored remotely (S3/Azure) without downloading entire files.
    ///
    /// Uses Quickwit's open_index_with_caches pattern for proper async directory support.
    async fn open_source_split_with_offsets(
        &self,
        storage_resolver: &StorageResolver,
        source_split: &XRefSourceSplit,
    ) -> Result<(Schema, OpenedSourceSplit)> {
        use quickwit_search::leaf::open_index_with_caches;
        use quickwit_proto::search::SplitIdAndFooterOffsets;

        let storage = resolve_storage_for_split(storage_resolver, &source_split.uri).await?;

        // Extract the split_id from the filename (without .split extension)
        // open_index_with_caches constructs the path as "{split_id}.split"
        let filename_split_id = extract_split_id_from_filename(&source_split.uri);

        debug_println!("📁 XREF: Opening split {} with footer offsets {}-{} (efficient footer-only download)",
            source_split.uri, source_split.footer_start, source_split.footer_end);
        debug_println!("📁 XREF: Using filename-based split_id='{}' (metadata split_id='{}')",
            filename_split_id, source_split.split_id);

        // Create SplitIdAndFooterOffsets for Quickwit's open_index_with_caches
        // IMPORTANT: split_id must match the filename (without .split extension)
        let split_and_footer_offsets = SplitIdAndFooterOffsets {
            split_id: filename_split_id.clone(),
            split_footer_start: source_split.footer_start,
            split_footer_end: source_split.footer_end,
            num_docs: source_split.num_docs.unwrap_or(0),
            timestamp_start: None,
            timestamp_end: None,
        };

        // Get the global searcher context for caching
        let searcher_context = crate::global_cache::get_global_searcher_context();

        debug_println!("📁 XREF: Calling open_index_with_caches with split_id='{}', storage uri='{}'",
            split_and_footer_offsets.split_id, storage.uri());

        // Create ephemeral cache for warmup data - CRITICAL for sync reads after async warmup
        // Without this cache, warmed up data isn't stored and sync reads fail with
        // "StorageDirectory only supports async reads"
        let ephemeral_cache = quickwit_storage::ByteRangeCache::with_infinite_capacity(
            &quickwit_storage::STORAGE_METRICS.shortlived_cache
        );

        // Use Quickwit's open_index_with_caches which:
        // 1. Uses footer offsets for efficient split opening
        // 2. Creates HotDirectory with proper async support
        // 3. Handles byte-range requests on-demand
        // 4. With ephemeral_cache, warmed data is cached for subsequent sync reads
        let open_result = open_index_with_caches(
            &searcher_context,
            storage.clone(),
            &split_and_footer_offsets,
            None,  // No tokenizer manager override
            Some(ephemeral_cache),  // Ephemeral cache for warmup data
        ).await;

        // Enhanced error reporting for S3 XRef debugging
        let (tantivy_index, _hot_directory) = match open_result {
            Ok(result) => result,
            Err(e) => {
                debug_println!("❌ XREF: open_index_with_caches FAILED");
                debug_println!("❌ XREF:   split_uri: {}", source_split.uri);
                debug_println!("❌ XREF:   split_id: {}", split_and_footer_offsets.split_id);
                debug_println!("❌ XREF:   footer: {}-{}", split_and_footer_offsets.split_footer_start, split_and_footer_offsets.split_footer_end);
                debug_println!("❌ XREF:   storage_uri: {}", storage.uri());
                debug_println!("❌ XREF:   error: {:?}", e);
                return Err(anyhow!(
                    "Failed to open source split with caches: split_id='{}', footer={}-{}, storage_uri='{}', error: {}",
                    split_and_footer_offsets.split_id,
                    split_and_footer_offsets.split_footer_start,
                    split_and_footer_offsets.split_footer_end,
                    storage.uri(),
                    e
                ));
            }
        };

        debug_println!("📁 XREF: Opened split via open_index_with_caches (async-compatible)");

        let schema = tantivy_index.schema();
        let reader = tantivy_index.reader()?;
        let searcher = Arc::new(reader.searcher());

        // CRITICAL: Warmup ALL indexed fields' term dictionaries before sync access
        // HotDirectory/StorageDirectory only supports async reads, so we must warmup
        // the term dictionaries asynchronously before the sync iteration in process_source_split
        debug_println!("📁 XREF: Warming up term dictionaries for all indexed fields...");
        let mut term_dict_fields: HashSet<Field> = HashSet::new();
        for (field, field_entry) in schema.fields() {
            if field_entry.is_indexed() {
                term_dict_fields.insert(field);
            }
        }
        debug_println!("📁 XREF: Found {} indexed fields to warmup", term_dict_fields.len());

        // Warmup term dictionaries asynchronously
        let warmup_info = quickwit_doc_mapper::WarmupInfo {
            term_dict_fields,
            ..Default::default()
        };
        quickwit_search::leaf::warmup(&searcher, &warmup_info).await
            .context("Failed to warmup term dictionaries")?;
        debug_println!("📁 XREF: Term dictionary warmup complete");

        let num_docs: u64 = searcher
            .segment_readers()
            .iter()
            .map(|r| r.num_docs() as u64)
            .sum();

        debug_println!("📁 XREF: Split has {} docs, {} segments", num_docs, searcher.segment_readers().len());

        let split_doc = OpenedSourceSplit {
            uri: source_split.uri.clone(),
            split_id: source_split.split_id.clone(),
            index: tantivy_index,
            searcher,
            num_docs,
            file_size: source_split.footer_end,
        };

        Ok((schema, split_doc))
    }

    /// Process a source split and add its terms to the XRef index
    ///
    /// Uses footer offsets from the XRefSourceSplit to efficiently open the split
    /// without reading the entire file or re-discovering footer offsets.
    async fn process_source_split(
        &self,
        source_split: &XRefSourceSplit,
        split_idx: usize,
        xref_schema: &Schema,
        field_map: &HashMap<String, Field>,
        writer: &mut IndexWriter,
        field_stats: &mut HashMap<String, XRefFieldInfo>,
    ) -> Result<(XRefSplitDocument, u64)> {
        // Convert config types for storage resolver
        let s3_config = self.config.aws_config.as_ref().map(merge_aws_to_s3_config);
        let azure_config = self.config.azure_config.as_ref().map(merge_azure_to_azure_config);
        let storage_resolver = get_configured_storage_resolver(s3_config, azure_config);

        // Open the split using the provided footer offsets
        let (_schema, opened_split) = self.open_source_split_with_offsets(&storage_resolver, source_split).await?;
        let searcher = opened_split.searcher;
        let tantivy_index = opened_split.index;
        let num_docs = opened_split.num_docs;
        let file_size = source_split.footer_end;

        // Create XRef document for this split
        let mut xref_doc = TantivyDocument::default();

        // Add split metadata fields
        if let Some(uri_field) = field_map.get(SPLIT_URI_FIELD) {
            xref_doc.add_text(*uri_field, &source_split.uri);
        }

        if let Some(id_field) = field_map.get(SPLIT_ID_FIELD) {
            xref_doc.add_text(*id_field, &source_split.split_id);
        }

        // Create split document metadata with footer offsets
        let split_doc = XRefSplitDocument::new(
            source_split.uri.clone(),
            source_split.split_id.clone(),
            num_docs
        )
        .with_size_bytes(file_size)
        .with_footer_offsets(source_split.footer_start, source_split.footer_end);

        // Serialize split metadata
        if let Some(metadata_field) = field_map.get(SPLIT_METADATA_FIELD) {
            let metadata_json = serde_json::to_string(&split_doc)?;
            xref_doc.add_text(*metadata_field, &metadata_json);
        }

        // Extract terms from each segment and field
        for segment_reader in searcher.segment_readers() {
            for (field, field_entry) in tantivy_index.schema().fields() {
                // Skip non-indexed fields
                if !field_entry.is_indexed() {
                    continue;
                }

                let field_name = field_entry.name();

                // Get the corresponding XRef field
                let xref_field = match field_map.get(field_name) {
                    Some(f) => *f,
                    None => continue,
                };

                // Get inverted index for this field
                let inverted_index = match segment_reader.inverted_index(field) {
                    Ok(idx) => idx,
                    Err(_) => continue,
                };

                let term_dict = inverted_index.terms();
                let mut term_stream = term_dict.stream()?;
                let mut term_count = 0u64;

                // Extract all terms and add to XRef document
                while term_stream.advance() {
                    let term_bytes = term_stream.key();

                    // Try to decode as UTF-8 string
                    if let Ok(term_str) = std::str::from_utf8(term_bytes) {
                        xref_doc.add_text(xref_field, term_str);
                        term_count += 1;
                    }
                }

                // Update field statistics
                let field_info = field_stats
                    .entry(field_name.to_string())
                    .or_insert_with(|| {
                        XRefFieldInfo::new(
                            field_name.to_string(),
                            format!("{:?}", field_entry.field_type()),
                        )
                    });
                field_info.num_terms += term_count;
                field_info.total_postings += 1; // Each term in this split = 1 posting
            }
        }

        // Add the XRef document
        writer.add_document(xref_doc)?;

        Ok((split_doc, file_size))
    }

    /// Package the XRef index into a Quickwit-compatible split format
    /// Uses SplitPayloadBuilder for proper bundle format with hotcache
    /// Returns (output_size, footer_start, footer_end)
    fn package_xref_split(
        &self,
        output_path: &Path,
        index_path: &Path,
        _metadata: &XRefMetadata,
    ) -> Result<(u64, u64, u64)> {
        use std::io::{Read, Seek, SeekFrom};

        debug_println!("📦 XREF PACKAGE: Using Quickwit's SplitPayloadBuilder for proper split format");

        // STEP 1: Collect all index files from the index directory
        let mut file_entries: Vec<PathBuf> = Vec::new();
        for entry in std::fs::read_dir(index_path)? {
            let entry = entry?;
            let file_path = entry.path();
            if file_path.is_file() {
                file_entries.push(file_path);
            }
        }

        debug_println!("📦 XREF PACKAGE: Found {} files in index directory", file_entries.len());
        for file_path in &file_entries {
            debug_println!("📦 XREF PACKAGE: Will include file: {}", file_path.display());
        }

        // STEP 2: Generate hotcache using Quickwit's official function
        let mmap_directory = MmapDirectory::open(index_path)
            .context("Failed to open MmapDirectory for hotcache generation")?;

        let hotcache = {
            let mut hotcache_buffer = Vec::new();

            debug_println!("📦 XREF PACKAGE: Generating hotcache using Quickwit's write_hotcache");
            write_hotcache(mmap_directory, &mut hotcache_buffer)
                .map_err(|e| anyhow!("Failed to generate hotcache: {}", e))?;

            debug_println!("📦 XREF PACKAGE: Generated {} bytes of hotcache", hotcache_buffer.len());
            hotcache_buffer
        };

        // STEP 3: Create empty serialized split fields (standard approach)
        let serialized_split_fields: Vec<u8> = Vec::new();

        // STEP 4: Create split payload using official API
        debug_println!("📦 XREF PACKAGE: Creating split payload with SplitPayloadBuilder::get_split_payload()");
        let split_payload = SplitPayloadBuilder::get_split_payload(
            &file_entries,
            &serialized_split_fields,
            &hotcache
        ).context("Failed to create split payload")?;

        // STEP 5: Write payload to output file
        let runtime = QuickwitRuntimeManager::global();
        let total_size = split_payload.len();
        let payload_bytes = runtime.handle().block_on(async {
            split_payload.read_all().await
        }).context("Failed to read split payload")?;

        std::fs::write(output_path, &payload_bytes)
            .context("Failed to write split file")?;

        debug_println!("📦 XREF PACKAGE: Wrote {} bytes to {:?}", payload_bytes.len(), output_path);

        // STEP 6: Extract footer information from the created split
        let file_len = total_size as u64;

        // Read the last 4 bytes to get hotcache length
        if file_len < 8 {
            return Err(anyhow!("Split file too small: {} bytes", file_len));
        }

        let mut split_file = std::fs::File::open(output_path)?;

        // Read hotcache length from last 4 bytes
        split_file.seek(SeekFrom::End(-4))?;
        let mut hotcache_len_bytes = [0u8; 4];
        split_file.read_exact(&mut hotcache_len_bytes)?;
        let hotcache_length = u32::from_le_bytes(hotcache_len_bytes) as u64;

        debug_println!("📦 XREF PACKAGE: Read hotcache length from split: {} bytes", hotcache_length);

        // Calculate hotcache start offset
        let hotcache_start_offset = file_len - 4 - hotcache_length;

        // Read metadata length from 4 bytes before hotcache
        if hotcache_start_offset < 4 {
            return Err(anyhow!("Invalid hotcache start offset: {}", hotcache_start_offset));
        }

        let metadata_len_start = hotcache_start_offset - 4;
        split_file.seek(SeekFrom::Start(metadata_len_start))?;
        let mut metadata_len_bytes = [0u8; 4];
        split_file.read_exact(&mut metadata_len_bytes)?;
        let metadata_length = u32::from_le_bytes(metadata_len_bytes) as u64;

        debug_println!("📦 XREF PACKAGE: Read metadata length from split: {} bytes", metadata_length);

        // Calculate footer start (where BundleStorageFileOffsets JSON begins)
        let footer_start_offset = hotcache_start_offset - 4 - metadata_length;

        debug_println!("📦 XREF PACKAGE: Footer offsets calculated:");
        debug_println!("   footer_start_offset = {} (where BundleStorageFileOffsets begins)", footer_start_offset);
        debug_println!("   hotcache_start_offset = {} (where hotcache begins)", hotcache_start_offset);
        debug_println!("   file_len = {} (total file size)", file_len);
        debug_println!("   metadata_length = {} bytes", metadata_length);
        debug_println!("   hotcache_length = {} bytes", hotcache_length);

        // Validate footer structure
        if footer_start_offset >= hotcache_start_offset {
            return Err(anyhow!(
                "Invalid footer structure: footer_start({}) >= hotcache_start({})",
                footer_start_offset, hotcache_start_offset
            ));
        }

        debug_println!("📦 XREF PACKAGE: XRef split packaged successfully with proper Quickwit format");

        Ok((file_len, footer_start_offset, file_len))
    }
}

/// Represents an opened source split for term extraction
struct OpenedSourceSplit {
    uri: String,
    split_id: String,
    index: Index,
    searcher: Arc<tantivy::Searcher>,
    num_docs: u64,
    file_size: u64,
}

/// Extract split path from URI
///
/// For S3 and Azure URIs, this returns just the filename because the storage
/// is already resolved to the directory containing the file.
/// For file:// URIs and direct paths, this returns just the filename as well.
fn extract_split_path(split_uri: &str) -> PathBuf {
    // For all URI types, extract just the filename since the storage
    // is resolved to the directory containing the file
    if let Some(last_slash_pos) = split_uri.rfind('/') {
        let filename = &split_uri[last_slash_pos + 1..];
        return PathBuf::from(filename);
    }

    // No slash found, assume it's just a filename
    PathBuf::from(split_uri)
}

/// Extract split ID from URI
fn extract_split_id(split_uri: &str) -> String {
    // Get the filename without extension
    let path = Path::new(split_uri);
    path.file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| {
            // Generate a unique ID if extraction fails
            uuid::Uuid::new_v4().to_string()
        })
}

/// Extract split ID from the filename in a URI
///
/// This extracts the filename without the .split extension, which is what
/// open_index_with_caches expects (it constructs "{split_id}.split").
///
/// Examples:
/// - "/path/to/split1.split" -> "split1"
/// - "s3://bucket/splits/my-split.split" -> "my-split"
fn extract_split_id_from_filename(split_uri: &str) -> String {
    // Extract just the filename from the URI
    let filename = if let Some(last_slash) = split_uri.rfind('/') {
        &split_uri[last_slash + 1..]
    } else {
        split_uri
    };

    // Remove .split extension if present
    if filename.ends_with(".split") {
        filename[..filename.len() - 6].to_string()
    } else {
        filename.to_string()
    }
}

/// Create a temporary directory for XRef build operations.
///
/// If a custom temp directory path is provided, the directory will be created
/// there. Otherwise, the system default temp directory will be used.
fn create_xref_temp_directory(custom_base: Option<&str>) -> Result<tempfile::TempDir> {
    use tempfile::Builder;

    let mut builder = Builder::new();
    builder.prefix("xref_build_");

    if let Some(base_path) = custom_base {
        let base_path_buf = std::path::PathBuf::from(base_path);
        if !base_path_buf.exists() {
            return Err(anyhow::anyhow!(
                "Custom temp directory base path does not exist: {}",
                base_path
            ));
        }
        if !base_path_buf.is_dir() {
            return Err(anyhow::anyhow!(
                "Custom temp directory base path is not a directory: {}",
                base_path
            ));
        }
        debug_println!("🏗️ XREF: Using custom temp directory base: {}", base_path);
        builder.tempdir_in(&base_path_buf)
            .context("Failed to create temp directory in custom path")
    } else {
        debug_println!("🏗️ XREF: Using system temp directory");
        builder.tempdir()
            .context("Failed to create temp directory")
    }
}

// JNI bindings for XRef split building
use jni::objects::{JClass, JObject, JString};
use jni::sys::{jobject, jstring};
use jni::JNIEnv;

use crate::utils::jstring_to_string;

/// Helper to convert a string to jstring, returning null on error
fn make_jstring(env: &mut JNIEnv, s: &str) -> jstring {
    match env.new_string(s) {
        Ok(js) => js.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// JNI: Build an XRef split from multiple source splits
///
/// The source_splits parameter is an array of XRefSourceSplit objects, each containing:
/// - uri (String): Split URI
/// - splitId (String): Split identifier
/// - footerStart (long): Footer start offset for efficient split opening
/// - footerEnd (long): Footer end offset for efficient split opening
/// - numDocs (Long): Optional document count
/// - sizeBytes (Long): Optional size in bytes
/// - docMappingJson (String): Optional doc mapping JSON for schema derivation
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_xref_XRefSplit_nativeBuildXRefSplit(
    mut env: JNIEnv,
    _class: JClass,
    xref_id: JString,
    index_uid: JString,
    source_splits: JObject, // XRefSourceSplit[]
    output_path: JString,
    include_positions: jni::sys::jboolean,
    included_fields: JObject, // String[] or null
    temp_directory_path: JString, // Temp directory for intermediate files (nullable)
    heap_size: jni::sys::jlong, // Heap size for index writer in bytes
    // AWS config (all nullable)
    aws_access_key: JString,
    aws_secret_key: JString,
    aws_session_token: JString,
    aws_region: JString,
    aws_endpoint_url: JString,
    aws_force_path_style: jni::sys::jboolean,
    // Azure config (all nullable)
    azure_account_name: JString,
    azure_account_key: JString,
    azure_bearer_token: JString,
    azure_endpoint_url: JString,
) -> jstring {
    use crate::merge_types::MergeAwsConfig;
    use crate::merge_types::MergeAzureConfig;

    // Extract XRef ID
    let xref_id_str = match jstring_to_string(&mut env, &xref_id) {
        Ok(s) => s,
        Err(e) => {
            return make_jstring(&mut env, &format!("ERROR: Invalid xref_id: {}", e));
        }
    };

    // Extract index UID
    let index_uid_str = match jstring_to_string(&mut env, &index_uid) {
        Ok(s) => s,
        Err(e) => {
            return make_jstring(&mut env, &format!("ERROR: Invalid index_uid: {}", e));
        }
    };

    // Extract output path
    let output_path_str = match jstring_to_string(&mut env, &output_path) {
        Ok(s) => s,
        Err(e) => {
            return make_jstring(&mut env, &format!("ERROR: Invalid output_path: {}", e));
        }
    };

    // Extract source splits array (XRefSourceSplit objects with footer offsets)
    let source_splits_vec = match extract_xref_source_split_array(&mut env, &source_splits) {
        Ok(v) => v,
        Err(e) => {
            return make_jstring(&mut env, &format!("ERROR: Invalid source_splits: {}", e));
        }
    };

    // Extract included fields (optional)
    let included_fields_vec = if !included_fields.is_null() {
        match extract_string_array(&mut env, &included_fields) {
            Ok(v) => v,
            Err(e) => {
                return make_jstring(&mut env, &format!("ERROR: Invalid included_fields: {}", e));
            }
        }
    } else {
        Vec::new()
    };

    // Extract temp directory path (optional)
    let temp_dir_path = if !temp_directory_path.is_null() {
        jstring_to_string(&mut env, &temp_directory_path).ok()
    } else {
        None
    };

    // Build AWS config if provided
    let aws_config = if !aws_access_key.is_null() {
        let access_key = jstring_to_string(&mut env, &aws_access_key).unwrap_or_default();
        let secret_key = jstring_to_string(&mut env, &aws_secret_key).unwrap_or_default();

        if !access_key.is_empty() && !secret_key.is_empty() {
            Some(MergeAwsConfig {
                access_key,
                secret_key,
                session_token: jstring_to_string(&mut env, &aws_session_token).ok(),
                region: jstring_to_string(&mut env, &aws_region).unwrap_or_else(|_| "us-east-1".to_string()),
                endpoint_url: jstring_to_string(&mut env, &aws_endpoint_url).ok(),
                force_path_style: aws_force_path_style != 0,
            })
        } else {
            None
        }
    } else {
        None
    };

    // Build Azure config if provided
    let azure_config = if !azure_account_name.is_null() {
        let account_name = jstring_to_string(&mut env, &azure_account_name).unwrap_or_default();

        if !account_name.is_empty() {
            let account_key = jstring_to_string(&mut env, &azure_account_key).ok();
            let bearer_token = jstring_to_string(&mut env, &azure_bearer_token).ok();
            let endpoint_url = jstring_to_string(&mut env, &azure_endpoint_url).ok();

            Some(MergeAzureConfig {
                account_name,
                account_key,
                bearer_token,
                endpoint_url,
            })
        } else {
            None
        }
    } else {
        None
    };

    // Build XRef config
    let config = XRefBuildConfig {
        xref_id: xref_id_str,
        index_uid: index_uid_str,
        source_splits: source_splits_vec,
        aws_config,
        azure_config,
        included_fields: included_fields_vec,
        include_positions: include_positions != 0,
        temp_directory_path: temp_dir_path,
        heap_size: heap_size as usize,
    };

    // Debug logging: Show received configuration (helps diagnose version mismatch issues)
    debug_println!("🔧 XREF JNI: Configuration received:");
    debug_println!("  xref_id: {}", config.xref_id);
    debug_println!("  index_uid: {}", config.index_uid);
    debug_println!("  source_splits: {} splits", config.source_splits.len());
    debug_println!("  temp_directory_path: {:?}", config.temp_directory_path);
    debug_println!("  heap_size: {} bytes", config.heap_size);
    debug_println!("  aws_config present: {}", config.aws_config.is_some());
    if let Some(ref aws) = config.aws_config {
        debug_println!("    aws region: {}", aws.region);
        debug_println!("    aws access_key (first 8 chars): {}...", &aws.access_key[..8.min(aws.access_key.len())]);
        debug_println!("    aws session_token present: {}", aws.session_token.is_some());
        debug_println!("    aws endpoint_url: {:?}", aws.endpoint_url);
    }
    debug_println!("  azure_config present: {}", config.azure_config.is_some());
    if let Some(ref azure) = config.azure_config {
        debug_println!("    azure account_name: {}", azure.account_name);
        debug_println!("    azure account_key present: {}", azure.account_key.is_some());
        debug_println!("    azure bearer_token present: {}", azure.bearer_token.is_some());
    }

    // Build the XRef split
    debug_println!("🔧 XREF JNI: About to build XRef with {} source splits", config.source_splits.len());
    for (i, split) in config.source_splits.iter().enumerate() {
        debug_println!("  Split {}: uri={}, footer={}-{}", i, split.uri, split.footer_start, split.footer_end);
    }

    let builder = XRefSplitBuilder::new(config);
    let output_path = Path::new(&output_path_str);

    match builder.build(output_path) {
        Ok(metadata) => {
            debug_println!("🔧 XREF JNI: Build succeeded! {} splits in registry", metadata.num_splits());
            // Return metadata as JSON
            match serde_json::to_string(&metadata) {
                Ok(json) => make_jstring(&mut env, &json),
                Err(e) => make_jstring(&mut env, &format!("ERROR: Failed to serialize metadata: {}", e)),
            }
        }
        Err(e) => {
            debug_println!("🔧 XREF JNI: Build FAILED: {}", e);
            make_jstring(&mut env, &format!("ERROR: Failed to build XRef split: {}", e))
        }
    }
}

/// Helper to extract a String array from JNI
fn extract_string_array(env: &mut JNIEnv, array: &JObject) -> Result<Vec<String>> {
    use jni::objects::JObjectArray;

    if array.is_null() {
        return Ok(Vec::new());
    }

    // Convert to JObjectArray
    let array_ref: &JObjectArray = unsafe { &*(array as *const JObject as *const JObjectArray) };

    let len = env.get_array_length(array_ref)? as usize;
    let mut result = Vec::with_capacity(len);

    for i in 0..len {
        let elem = env.get_object_array_element(array_ref, i as i32)?;
        let jstr: JString = elem.into();
        let s = jstring_to_string(env, &jstr)?;
        result.push(s);
    }

    Ok(result)
}

/// Helper to extract an XRefSourceSplit from a Java XRefSourceSplit object
fn extract_xref_source_split(env: &mut JNIEnv, obj: &JObject) -> Result<XRefSourceSplit> {
    if obj.is_null() {
        return Err(anyhow!("XRefSourceSplit object is null"));
    }

    // Extract uri (String)
    let uri_obj = env.call_method(obj, "getUri", "()Ljava/lang/String;", &[])?
        .l()
        .context("Failed to get uri")?;
    let uri: JString = uri_obj.into();
    let uri_str = jstring_to_string(env, &uri)?;

    // Extract splitId (String)
    let split_id_obj = env.call_method(obj, "getSplitId", "()Ljava/lang/String;", &[])?
        .l()
        .context("Failed to get splitId")?;
    let split_id: JString = split_id_obj.into();
    let split_id_str = jstring_to_string(env, &split_id)?;

    // Extract footerStart (long)
    let footer_start = env.call_method(obj, "getFooterStart", "()J", &[])?
        .j()
        .context("Failed to get footerStart")? as u64;

    // Extract footerEnd (long)
    let footer_end = env.call_method(obj, "getFooterEnd", "()J", &[])?
        .j()
        .context("Failed to get footerEnd")? as u64;

    // Extract numDocs (Long - nullable)
    let num_docs_obj = env.call_method(obj, "getNumDocs", "()Ljava/lang/Long;", &[])?
        .l()
        .context("Failed to get numDocs")?;
    let num_docs = if !num_docs_obj.is_null() {
        let value = env.call_method(&num_docs_obj, "longValue", "()J", &[])?
            .j()
            .context("Failed to unbox numDocs")?;
        Some(value as u64)
    } else {
        None
    };

    // Extract sizeBytes (Long - nullable)
    let size_bytes_obj = env.call_method(obj, "getSizeBytes", "()Ljava/lang/Long;", &[])?
        .l()
        .context("Failed to get sizeBytes")?;
    let size_bytes = if !size_bytes_obj.is_null() {
        let value = env.call_method(&size_bytes_obj, "longValue", "()J", &[])?
            .j()
            .context("Failed to unbox sizeBytes")?;
        Some(value as u64)
    } else {
        None
    };

    // Extract docMappingJson (String - nullable)
    let doc_mapping_obj = env.call_method(obj, "getDocMappingJson", "()Ljava/lang/String;", &[])?
        .l()
        .context("Failed to get docMappingJson")?;
    let doc_mapping_json = if !doc_mapping_obj.is_null() {
        let doc_mapping: JString = doc_mapping_obj.into();
        Some(jstring_to_string(env, &doc_mapping)?)
    } else {
        None
    };

    let mut source_split = XRefSourceSplit::new(uri_str, split_id_str, footer_start, footer_end);
    if let Some(n) = num_docs {
        source_split = source_split.with_num_docs(n);
    }
    if let Some(s) = size_bytes {
        source_split = source_split.with_size_bytes(s);
    }
    if let Some(json) = doc_mapping_json {
        source_split = source_split.with_doc_mapping_json(json);
    }

    Ok(source_split)
}

/// Helper to extract an array of XRefSourceSplit objects from JNI
fn extract_xref_source_split_array(env: &mut JNIEnv, array: &JObject) -> Result<Vec<XRefSourceSplit>> {
    use jni::objects::JObjectArray;

    if array.is_null() {
        return Ok(Vec::new());
    }

    // Convert to JObjectArray
    let array_ref: &JObjectArray = unsafe { &*(array as *const JObject as *const JObjectArray) };

    let len = env.get_array_length(array_ref)? as usize;
    let mut result = Vec::with_capacity(len);

    for i in 0..len {
        let elem = env.get_object_array_element(array_ref, i as i32)?;
        let source_split = extract_xref_source_split(env, &elem)?;
        result.push(source_split);
    }

    Ok(result)
}

// ============================================================================
// XRef Query Transformation - Replace range queries with match_all
// ============================================================================
//
// XRef splits don't have fast fields, so range queries don't work properly.
// This function transforms range queries to match_all queries, which returns
// all splits (conservative - never misses a split that might contain matching docs).

use serde_json::Value;

/// Transform a QueryAst JSON by replacing all range queries with match_all queries.
/// This is needed for XRef splits because they don't have fast fields.
///
/// Range queries in an XRef context should return all splits because:
/// 1. XRef splits only have term dictionaries, not fast fields
/// 2. Range queries require fast fields to evaluate bounds
/// 3. By returning all splits, we ensure no relevant split is missed
pub fn transform_range_to_match_all(query_json: &str) -> Result<String> {
    let mut query_value: Value = serde_json::from_str(query_json)
        .context("Failed to parse query JSON")?;

    let transformed = transform_range_recursive(&mut query_value);

    if transformed {
        debug_println!("🔧 XREF: Transformed range queries to match_all for XRef compatibility");
    }

    let result = serde_json::to_string(&query_value)
        .context("Failed to serialize transformed query")?;

    Ok(result)
}

/// Recursively transform range queries to match_all in a JSON value.
/// Returns true if any transformation was made.
///
/// QueryAst JSON format uses {"type":"range", "field":"...", "lower_bound":..., "upper_bound":...}
fn transform_range_recursive(value: &mut Value) -> bool {
    let mut transformed = false;

    match value {
        Value::Object(map) => {
            // Check if this is a range query (has "type": "range")
            if let Some(type_val) = map.get("type") {
                if type_val.as_str() == Some("range") {
                    // Replace the entire object with {"type":"match_all"}
                    map.clear();
                    map.insert("type".to_string(), Value::String("match_all".to_string()));
                    return true;
                }
            }

            // Recursively process all values in the object
            for (_, v) in map.iter_mut() {
                if transform_range_recursive(v) {
                    transformed = true;
                }
            }
        }
        Value::Array(arr) => {
            // Recursively process all items in the array
            for item in arr.iter_mut() {
                if transform_range_recursive(item) {
                    transformed = true;
                }
            }
        }
        _ => {}
    }

    transformed
}

/// JNI binding for transforming range queries to match_all
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_xref_XRefSearcher_nativeTransformRangeToMatchAll<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    query_json: JString<'local>,
) -> jstring {
    let query_str = match jstring_to_string(&mut env, &query_json) {
        Ok(s) => s,
        Err(e) => {
            return make_jstring(&mut env, &format!("ERROR: Failed to get query string: {}", e));
        }
    };

    match transform_range_to_match_all(&query_str) {
        Ok(result) => make_jstring(&mut env, &result),
        Err(e) => {
            make_jstring(&mut env, &format!("ERROR: Transform failed: {}", e))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transform_range_to_match_all_simple() {
        // Simple range query in QueryAst format
        let input = r#"{"type":"range","field":"age","lower_bound":{"Included":25},"upper_bound":{"Included":35}}"#;
        let result = transform_range_to_match_all(input).unwrap();
        assert_eq!(result, r#"{"type":"match_all"}"#);
    }

    #[test]
    fn test_transform_range_to_match_all_nested_boolean() {
        // Boolean query with nested range query in QueryAst format
        let input = r#"{"type":"bool","must":[{"type":"term","field":"status","value":"active"},{"type":"range","field":"age","lower_bound":{"Included":25},"upper_bound":{"Included":35}}]}"#;
        let result = transform_range_to_match_all(input).unwrap();
        // The range inside must array should be replaced with match_all
        assert!(result.contains(r#""type":"match_all""#));
        assert!(!result.contains(r#""type":"range""#));
        // Term query should be preserved
        assert!(result.contains(r#""type":"term""#));
    }

    #[test]
    fn test_transform_no_range_query() {
        // Query without range - should be unchanged (JSON key order may differ)
        let input = r#"{"type":"term","field":"name","value":"test"}"#;
        let result = transform_range_to_match_all(input).unwrap();
        // Verify it's still a term query
        assert!(result.contains(r#""type":"term""#));
        assert!(result.contains(r#""field":"name""#));
        assert!(result.contains(r#""value":"test""#));
    }

    #[test]
    fn test_transform_match_all_unchanged() {
        // Match all query should be unchanged
        let input = r#"{"type":"match_all"}"#;
        let result = transform_range_to_match_all(input).unwrap();
        assert!(result.contains(r#""type":"match_all""#));
    }

    #[test]
    fn test_extract_split_id() {
        assert_eq!(extract_split_id("/path/to/split-001.split"), "split-001");
        assert_eq!(extract_split_id("s3://bucket/splits/split-002.split"), "split-002");
        assert_eq!(extract_split_id("file:///tmp/my-split.split"), "my-split");
    }

    #[test]
    fn test_extract_split_path() {
        // File:// URIs - extract just the filename
        let path = extract_split_path("file:///tmp/split.split");
        assert_eq!(path, PathBuf::from("split.split"));

        // Direct file paths - extract just the filename
        let path = extract_split_path("/direct/path/split.split");
        assert_eq!(path, PathBuf::from("split.split"));

        // S3 URIs - extract just the filename
        let path = extract_split_path("s3://bucket/path/to/file.split");
        assert_eq!(path, PathBuf::from("file.split"));

        // Azure URIs - extract just the filename
        let path = extract_split_path("azure://container/path/to/file.split");
        assert_eq!(path, PathBuf::from("file.split"));

        // Just a filename
        let path = extract_split_path("myfile.split");
        assert_eq!(path, PathBuf::from("myfile.split"));
    }
}
