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

//! Binary Fuse Filter XRef Builder
//!
//! Builds Binary Fuse8 filters from source splits by streaming term dictionaries.
//! Uses the existing HotDirectory pattern for efficient footer-only downloads.
//!
//! # Memory Profile
//!
//! - Per split: ~1KB iterator state (terms are mmap'd)
//! - Filter construction: O(terms * 1.24 bytes)
//! - Total for 10,000 splits with 50K terms each: ~620MB output

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use xorf::{BinaryFuse8, BinaryFuse16};
use tantivy::schema::FieldType;
use hex;

use quickwit_config::{AzureStorageConfig, S3StorageConfig};
use quickwit_directories::CachingDirectory;
use quickwit_proto::search::SplitIdAndFooterOffsets;
use quickwit_search::leaf::open_index_with_caches;
use quickwit_storage::{ByteRangeCache, Storage, StorageResolver, STORAGE_METRICS};

use super::types::*;
use super::storage::save_fuse_xref;
use crate::debug_println;
use crate::global_cache::{get_configured_storage_resolver, get_global_searcher_context};
use crate::merge_types::{MergeAwsConfig, MergeAzureConfig};
use crate::runtime_manager::QuickwitRuntimeManager;
use crate::standalone_searcher::resolve_storage_for_split;

/// Extract field type info from a tantivy schema
fn extract_field_info(schema: &tantivy::schema::Schema) -> Vec<FieldTypeInfo> {
    schema
        .fields()
        .filter_map(|(_field, entry)| {
            if !entry.is_indexed() {
                return None;
            }

            let field_type_str = match entry.field_type() {
                FieldType::Str(opts) => {
                    let tokenizer = opts.get_indexing_options()
                        .map(|idx_opts| idx_opts.tokenizer().to_string());
                    return Some(FieldTypeInfo {
                        name: entry.name().to_string(),
                        field_type: "text".to_string(),
                        tokenizer,
                        indexed: true,
                    });
                }
                FieldType::U64(_) => "u64",
                FieldType::I64(_) => "i64",
                FieldType::F64(_) => "f64",
                FieldType::Bool(_) => "bool",
                FieldType::Date(_) => "datetime",
                FieldType::JsonObject(_) => "json",
                FieldType::Bytes(_) => "bytes",
                FieldType::Facet(_) => "facet",
                FieldType::IpAddr(_) => "ip",
            };

            Some(FieldTypeInfo {
                name: entry.name().to_string(),
                field_type: field_type_str.to_string(),
                tokenizer: None,
                indexed: true,
            })
        })
        .collect()
}

/// Schema info extracted from first source split
struct SchemaInfo {
    fields: Vec<FieldTypeInfo>,
    schema_json: String,
}

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

/// Binary Fuse Filter XRef Builder
pub struct FuseXRefBuilder {
    config: FuseXRefBuildConfig,
}

impl FuseXRefBuilder {
    /// Create a new builder with the given configuration
    pub fn new(config: FuseXRefBuildConfig) -> Self {
        Self { config }
    }

    /// Build the FuseXRef and write to output path
    pub fn build(&self, output_path: &Path) -> Result<XRefMetadataJson> {
        let start_time = Instant::now();
        let runtime = QuickwitRuntimeManager::global();

        debug_println!(
            "[FUSE_XREF] Building FuseXRef with {} source splits",
            self.config.source_splits.len()
        );

        // Build storage resolver
        let s3_config = self.config.aws_config.as_ref().map(merge_aws_to_s3_config);
        let azure_config = self.config.azure_config.as_ref().map(merge_azure_to_azure_config);
        let storage_resolver = get_configured_storage_resolver(s3_config, azure_config);

        let mut xref = FuseXRef::new(
            self.config.xref_id.clone(),
            self.config.index_uid.clone(),
        );
        let mut total_terms: u64 = 0;
        let mut splits_skipped: u32 = 0;
        let mut schema_extracted = false;

        // Process each split and build a filter
        for (idx, source_split) in self.config.source_splits.iter().enumerate() {
            debug_println!(
                "[FUSE_XREF] Processing split {}/{}: {}",
                idx + 1,
                self.config.source_splits.len(),
                source_split.split_id
            );

            // Extract schema from first split
            let extract_schema = !schema_extracted;

            match runtime.handle().block_on(
                self.collect_terms_and_build_filter(&storage_resolver, source_split, idx as u32, extract_schema)
            ) {
                Ok((filter, metadata, num_terms, schema_info)) => {
                    total_terms += num_terms;
                    xref.filters.push(filter);
                    xref.metadata.push(metadata);

                    // Store schema from first successful split
                    if let Some(info) = schema_info {
                        debug_println!(
                            "[FUSE_XREF] Extracted schema with {} fields from first split",
                            info.fields.len()
                        );
                        xref.header.fields = info.fields;
                        xref.header.schema_json = Some(info.schema_json);
                        schema_extracted = true;
                    }
                }
                Err(e) => {
                    debug_println!(
                        "[FUSE_XREF] ⚠️ Failed to process split {}: {}",
                        source_split.uri,
                        e
                    );
                    splits_skipped += 1;
                }
            }
        }

        // Update header
        xref.header.num_splits = xref.filters.len() as u32;
        xref.header.total_terms = total_terms;

        debug_println!(
            "[FUSE_XREF] Built {} filters with {} total terms",
            xref.filters.len(),
            total_terms
        );

        // Save to output path with compression
        let (output_size, footer_start, footer_end) =
            save_fuse_xref(&xref, output_path, self.config.compression)?;

        // Update header with footer offsets
        xref.header.footer_start_offset = footer_start;
        xref.header.footer_end_offset = footer_end;

        let build_duration_ms = start_time.elapsed().as_millis() as u64;

        debug_println!(
            "[FUSE_XREF] ✅ Build complete: {} splits, {} terms, {} bytes, {}ms",
            xref.filters.len(),
            total_terms,
            output_size,
            build_duration_ms
        );

        // Create the build result (for internal tracking)
        let build_result = FuseXRefBuildResult {
            xref_id: self.config.xref_id.clone(),
            index_uid: self.config.index_uid.clone(),
            num_splits: xref.filters.len() as u32,
            total_terms,
            output_size_bytes: output_size,
            footer_start_offset: footer_start,
            footer_end_offset: footer_end,
            build_duration_ms,
            splits_skipped,
        };

        // Create XRefMetadataJson (what Java expects)
        let metadata_json = XRefMetadataJson::from_build(
            &build_result,
            &xref.metadata,
            &xref.header.fields,
        );

        Ok(metadata_json)
    }

    /// Collect all terms from a split and build a Binary Fuse filter
    ///
    /// Returns (filter, metadata, num_terms, optional_schema_info)
    async fn collect_terms_and_build_filter(
        &self,
        storage_resolver: &StorageResolver,
        source_split: &FuseXRefSourceSplit,
        split_idx: u32,
        extract_schema: bool,
    ) -> Result<(FuseFilter, SplitFilterMetadata, u64, Option<SchemaInfo>)> {
        let storage = resolve_storage_for_split(storage_resolver, &source_split.uri).await?;

        // Extract filename for lookup
        let filename_for_lookup = extract_split_id_from_filename(&source_split.uri);

        let split_and_footer = SplitIdAndFooterOffsets {
            split_id: filename_for_lookup.clone(),
            split_footer_start: source_split.footer_start,
            split_footer_end: source_split.footer_end,
            timestamp_start: None,
            timestamp_end: None,
            num_docs: source_split.num_docs.unwrap_or(0),
        };

        debug_println!(
            "[FUSE_XREF] Opening split {} (filename: {})",
            source_split.split_id,
            filename_for_lookup
        );

        let searcher_context = get_global_searcher_context();
        let ephemeral_cache = ByteRangeCache::with_infinite_capacity(&STORAGE_METRICS.shortlived_cache);

        let (tantivy_index, _hot_directory) = open_index_with_caches(
            &searcher_context,
            storage.clone(),
            &split_and_footer,
            None,
            Some(ephemeral_cache),
        )
        .await
        .context("Failed to open index with caches")?;

        let reader = tantivy_index.reader()?;
        let searcher = reader.searcher();
        let schema = tantivy_index.schema();

        // Find all indexed fields
        let indexed_fields: Vec<_> = schema
            .fields()
            .filter(|(_, entry)| entry.is_indexed())
            .collect();

        debug_println!(
            "[FUSE_XREF] Split {} has {} indexed fields",
            source_split.split_id,
            indexed_fields.len()
        );

        // Warm up term dictionaries
        use futures::future::try_join_all;
        let mut warmup_futures = Vec::new();

        for (field, _) in &indexed_fields {
            for segment_reader in searcher.segment_readers() {
                if let Ok(inverted_index) = segment_reader.inverted_index(*field) {
                    let inverted_index_clone = inverted_index.clone();
                    warmup_futures.push(async move {
                        inverted_index_clone.terms().warm_up_dictionary().await
                    });
                }
            }
        }

        if !warmup_futures.is_empty() {
            try_join_all(warmup_futures)
                .await
                .context("Failed to warm up term dictionaries")?;
        }

        // Collect all terms as field-qualified hashes
        let mut term_hashes: Vec<u64> = Vec::new();
        let mut num_docs: u64 = 0;

        for segment_reader in searcher.segment_readers() {
            num_docs += segment_reader.num_docs() as u64;

            for (field, field_entry) in &indexed_fields {
                let field_name = field_entry.name();

                // Skip fields not in included_fields (if specified)
                if !self.config.included_fields.is_empty()
                    && !self.config.included_fields.contains(&field_name.to_string())
                {
                    continue;
                }

                if let Ok(inverted_index) = segment_reader.inverted_index(*field) {
                    let term_dict = inverted_index.terms();
                    let mut stream = term_dict.stream()?;

                    while stream.advance() {
                        let term_bytes = stream.key();
                        if term_bytes.is_empty() {
                            continue;
                        }

                        // Convert term bytes to string value based on field type
                        let term_value = match field_entry.field_type() {
                            FieldType::Str(_) => {
                                std::str::from_utf8(term_bytes)
                                    .map(|s| s.to_string())
                                    .ok()
                            }
                            FieldType::U64(_) if term_bytes.len() >= 8 => {
                                term_bytes[..8].try_into().ok()
                                    .map(|bytes: [u8; 8]| u64::from_be_bytes(bytes).to_string())
                            }
                            FieldType::I64(_) if term_bytes.len() >= 8 => {
                                term_bytes[..8].try_into().ok()
                                    .map(|bytes: [u8; 8]| {
                                        let u_val = u64::from_be_bytes(bytes);
                                        tantivy::u64_to_i64(u_val).to_string()
                                    })
                            }
                            FieldType::Bool(_) if term_bytes.len() >= 8 => {
                                term_bytes[..8].try_into().ok()
                                    .map(|bytes: [u8; 8]| {
                                        let u_val = u64::from_be_bytes(bytes);
                                        (u_val != 0).to_string()
                                    })
                            }
                            FieldType::F64(_) if term_bytes.len() >= 8 => {
                                term_bytes[..8].try_into().ok()
                                    .map(|bytes: [u8; 8]| {
                                        let u_val = u64::from_be_bytes(bytes);
                                        tantivy::u64_to_f64(u_val).to_string()
                                    })
                            }
                            FieldType::Date(_) if term_bytes.len() >= 8 => {
                                term_bytes[..8].try_into().ok()
                                    .map(|bytes: [u8; 8]| {
                                        let u_val = u64::from_be_bytes(bytes);
                                        let timestamp_nanos = tantivy::u64_to_i64(u_val);
                                        timestamp_nanos.to_string()
                                    })
                            }
                            FieldType::JsonObject(_) => {
                                // For JSON fields, extract path and value
                                // Format: path\x00type_byte+value
                                // Returns (json_path, value) where path uses '.' separator
                                if let Some((json_path, value)) = parse_json_term_with_path(term_bytes) {
                                    // Construct full field path: field_name.json_path
                                    // This matches Quickwit query syntax: data.name:alice
                                    let full_field_path = if json_path.is_empty() {
                                        field_name.to_string()
                                    } else {
                                        format!("{}.{}", field_name, json_path)
                                    };
                                    let full_key = format!("{}:{}", full_field_path, value);
                                    let hash = fxhash::hash64(&full_key);
                                    term_hashes.push(hash);
                                }
                                None // We handle JSON terms specially above
                            }
                            _ => {
                                // For other types, use hex encoding
                                Some(hex::encode(term_bytes))
                            }
                        };

                        if let Some(value) = term_value {
                            // Create field-qualified key: "fieldname:value"
                            let key = format!("{}:{}", field_name, value);
                            let hash = fxhash::hash64(&key);
                            term_hashes.push(hash);
                        }
                    }
                }
            }
        }

        debug_println!(
            "[FUSE_XREF] Split {} collected {} term hashes",
            source_split.split_id,
            term_hashes.len()
        );

        // Build Binary Fuse filter based on configured type
        let filter = match self.config.filter_type {
            FuseFilterType::Fuse8 => {
                let f = if term_hashes.is_empty() {
                    BinaryFuse8::try_from(&[0u64][..])
                        .map_err(|e| anyhow!("Failed to create empty Fuse8 filter: {:?}", e))?
                } else {
                    BinaryFuse8::try_from(&term_hashes[..])
                        .map_err(|e| anyhow!("Failed to build Fuse8 filter for {} terms: {:?}", term_hashes.len(), e))?
                };
                FuseFilter::Fuse8(f)
            }
            FuseFilterType::Fuse16 => {
                let f = if term_hashes.is_empty() {
                    BinaryFuse16::try_from(&[0u64][..])
                        .map_err(|e| anyhow!("Failed to create empty Fuse16 filter: {:?}", e))?
                } else {
                    BinaryFuse16::try_from(&term_hashes[..])
                        .map_err(|e| anyhow!("Failed to build Fuse16 filter for {} terms: {:?}", term_hashes.len(), e))?
                };
                FuseFilter::Fuse16(f)
            }
        };

        // Calculate filter size (approximate)
        let filter_size = estimate_filter_size(term_hashes.len() as u64, self.config.filter_type);

        let metadata = SplitFilterMetadata {
            split_idx,
            uri: source_split.uri.clone(),
            split_id: source_split.split_id.clone(),
            footer_start: source_split.footer_start,
            footer_end: source_split.footer_end,
            num_docs,
            num_terms: term_hashes.len() as u64,
            filter_size_bytes: filter_size,
        };

        // Extract schema info if requested (for first split)
        let schema_info = if extract_schema {
            let fields = extract_field_info(&schema);
            let schema_json = serde_json::to_string(&schema)
                .unwrap_or_else(|_| "{}".to_string());
            Some(SchemaInfo { fields, schema_json })
        } else {
            None
        };

        Ok((filter, metadata, term_hashes.len() as u64, schema_info))
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

/// Parse JSON term value from term bytes
///
/// JSON term format: path\x00type_byte+value
#[allow(dead_code)]
fn parse_json_term_value(term_bytes: &[u8]) -> Option<String> {
    // Find the end-of-path marker (0x00)
    let end_pos = term_bytes.iter().position(|&b| b == 0)?;
    let value_bytes = &term_bytes[end_pos + 1..];

    if value_bytes.is_empty() {
        return None;
    }

    let type_byte = value_bytes[0];
    let raw_value = &value_bytes[1..];

    match type_byte {
        b's' => std::str::from_utf8(raw_value).ok().map(|s| s.to_string()),
        b'u' if raw_value.len() >= 8 => {
            let bytes: [u8; 8] = raw_value[..8].try_into().ok()?;
            Some(u64::from_be_bytes(bytes).to_string())
        }
        b'i' if raw_value.len() >= 8 => {
            let bytes: [u8; 8] = raw_value[..8].try_into().ok()?;
            let u_val = u64::from_be_bytes(bytes);
            Some(tantivy::u64_to_i64(u_val).to_string())
        }
        b'f' if raw_value.len() >= 8 => {
            let bytes: [u8; 8] = raw_value[..8].try_into().ok()?;
            let u_val = u64::from_be_bytes(bytes);
            Some(tantivy::u64_to_f64(u_val).to_string())
        }
        b'o' if raw_value.len() >= 8 => {
            let bytes: [u8; 8] = raw_value[..8].try_into().ok()?;
            let u_val = u64::from_be_bytes(bytes);
            Some((u_val != 0).to_string())
        }
        _ => std::str::from_utf8(raw_value).ok().map(|s| s.to_string()),
    }
}

/// Parse JSON term with full path from term bytes
///
/// JSON term format: path\x00type_byte+value
/// Returns (full_json_path, value) where path uses '.' separator
fn parse_json_term_with_path(term_bytes: &[u8]) -> Option<(String, String)> {
    // Find the end-of-path marker (0x00)
    let end_pos = term_bytes.iter().position(|&b| b == 0)?;

    // Extract the path bytes (before \x00)
    let path_bytes = &term_bytes[..end_pos];
    let path = std::str::from_utf8(path_bytes).ok()?;

    // Convert Tantivy's internal path separator (\x01) to dot notation
    // Tantivy uses \x01 as path separator internally
    let dotted_path = path.replace('\x01', ".");

    let value_bytes = &term_bytes[end_pos + 1..];

    if value_bytes.is_empty() {
        return None;
    }

    let type_byte = value_bytes[0];
    let raw_value = &value_bytes[1..];

    let value = match type_byte {
        b's' => std::str::from_utf8(raw_value).ok().map(|s| s.to_string())?,
        b'u' if raw_value.len() >= 8 => {
            let bytes: [u8; 8] = raw_value[..8].try_into().ok()?;
            u64::from_be_bytes(bytes).to_string()
        }
        b'i' if raw_value.len() >= 8 => {
            let bytes: [u8; 8] = raw_value[..8].try_into().ok()?;
            let u_val = u64::from_be_bytes(bytes);
            tantivy::u64_to_i64(u_val).to_string()
        }
        b'f' if raw_value.len() >= 8 => {
            let bytes: [u8; 8] = raw_value[..8].try_into().ok()?;
            let u_val = u64::from_be_bytes(bytes);
            tantivy::u64_to_f64(u_val).to_string()
        }
        b'o' if raw_value.len() >= 8 => {
            let bytes: [u8; 8] = raw_value[..8].try_into().ok()?;
            let u_val = u64::from_be_bytes(bytes);
            (u_val != 0).to_string()
        }
        _ => std::str::from_utf8(raw_value).ok().map(|s| s.to_string())?,
    };

    Some((dotted_path, value))
}

/// Estimate filter size in bytes based on number of keys
///
/// BinaryFuse8 uses approximately 1.24 bytes per key
/// BinaryFuse16 uses approximately 2.24 bytes per key
fn estimate_filter_size(num_keys: u64, filter_type: FuseFilterType) -> u64 {
    // BinaryFuse filter structure:
    // - fingerprints: Vec<u8/u16> with ~1.24n elements
    // - segment_length: u32
    // - segment_length_mask: u32
    // - segment_count_length: u32
    // - seed: u64
    //
    // Approximate: fingerprints size + 24 bytes overhead
    let bytes_per_fingerprint = match filter_type {
        FuseFilterType::Fuse8 => 1.0,   // 8-bit fingerprints
        FuseFilterType::Fuse16 => 2.0,  // 16-bit fingerprints
    };
    let fingerprint_estimate = (num_keys as f64 * 1.24 * bytes_per_fingerprint) as u64;
    fingerprint_estimate + 24
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert_eq!(extract_split_id_from_filename("file.split"), "file");
    }

    #[test]
    fn test_parse_json_term_value() {
        // String value: path\x00svalue
        let term_bytes = b"field\x00shello";
        assert_eq!(parse_json_term_value(term_bytes), Some("hello".to_string()));

        // Empty path
        let empty_path = b"\x00sworld";
        assert_eq!(parse_json_term_value(empty_path), Some("world".to_string()));
    }

    #[test]
    fn test_estimate_filter_size() {
        let num_keys = 1000u64;
        let size = estimate_filter_size(num_keys);

        // Should be approximately 1.24 * 1000 + 24 = ~1264 bytes
        assert!(size > 1000 && size < 2000);
    }
}
