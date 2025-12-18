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

//! Storage and serialization for Binary Fuse Filter XRef
//!
//! Uses Quickwit-compatible bundle format for S3/Azure storage:
//!
//! ```text
//! +-------------------------------------------------------+
//! | Files Section                                         |
//! |   fuse_filters.bin    - Binary fuse filter data       |
//! |   split_metadata.json - Per-split metadata array      |
//! |   xref_header.json    - Global XRef metadata          |
//! +-------------------------------------------------------+
//! | BundleStorageFileOffsets (JSON)                       |
//! +-------------------------------------------------------+
//! | Metadata length (4 bytes, little endian)              |
//! +-------------------------------------------------------+
//! | HotCache (empty for FuseXRef)                         |
//! +-------------------------------------------------------+
//! | HotCache length (4 bytes, little endian)              |
//! +-------------------------------------------------------+
//! ```

use std::collections::BTreeMap;
use std::io::{Cursor, Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use xorf::{BinaryFuse8, BinaryFuse16};

use quickwit_config::{AzureStorageConfig, S3StorageConfig};
use quickwit_storage::StorageResolver;

use super::types::*;
use crate::debug_println;
use crate::global_cache::get_configured_storage_resolver;
use crate::merge_types::{MergeAwsConfig, MergeAzureConfig};
use crate::runtime_manager::QuickwitRuntimeManager;
use crate::standalone_searcher::resolve_storage_for_split;

/// Bundle file names
const FUSE_FILTERS_FILE: &str = "fuse_filters.bin";
const SPLIT_METADATA_FILE: &str = "split_metadata.json";
const XREF_HEADER_FILE: &str = "xref_header.json";

/// Represents a loaded FuseXRef bundle
pub struct FuseXRefBundle {
    pub xref: FuseXRef,
}

impl FuseXRefBundle {
    /// Load from a file path
    pub fn load(path: &Path) -> Result<Self> {
        let xref = load_fuse_xref(path)?;
        Ok(Self { xref })
    }
}

/// Save a FuseXRef to a bundle file
///
/// Returns (total_size, footer_start, footer_end)
pub fn save_fuse_xref(
    xref: &FuseXRef,
    output_path: &Path,
    compression: CompressionType,
) -> Result<(u64, u64, u64)> {
    debug_println!(
        "[FUSE_XREF_STORAGE] Saving FuseXRef to {:?} (compression: {:?})",
        output_path,
        compression
    );

    // Create parent directories if needed
    if let Some(parent) = output_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Serialize each component (with compression for filter data)
    let filters_data = serialize_filters(&xref.filters, &xref.metadata, compression)?;
    let metadata_json = serde_json::to_vec_pretty(&SplitMetadataWrapper {
        splits: xref.metadata.clone(),
    })?;
    let header_json = serde_json::to_vec_pretty(&xref.header)?;

    debug_println!(
        "[FUSE_XREF_STORAGE] Serialized: filters={}B, metadata={}B, header={}B",
        filters_data.len(),
        metadata_json.len(),
        header_json.len()
    );

    // Build file offsets map (Quickwit bundle format)
    let mut current_offset: u64 = 0;
    let mut file_offsets: BTreeMap<String, FileRange> = BTreeMap::new();

    // File 1: fuse_filters.bin
    file_offsets.insert(
        FUSE_FILTERS_FILE.to_string(),
        FileRange {
            start: current_offset,
            end: current_offset + filters_data.len() as u64,
        },
    );
    current_offset += filters_data.len() as u64;

    // File 2: split_metadata.json
    file_offsets.insert(
        SPLIT_METADATA_FILE.to_string(),
        FileRange {
            start: current_offset,
            end: current_offset + metadata_json.len() as u64,
        },
    );
    current_offset += metadata_json.len() as u64;

    // File 3: xref_header.json
    file_offsets.insert(
        XREF_HEADER_FILE.to_string(),
        FileRange {
            start: current_offset,
            end: current_offset + header_json.len() as u64,
        },
    );
    current_offset += header_json.len() as u64;

    // Footer start is after all file data
    let footer_start = current_offset;

    // Serialize bundle metadata (file offsets)
    let bundle_metadata = BundleMetadata { files: file_offsets };
    let bundle_metadata_json = serde_json::to_vec(&bundle_metadata)?;

    // Write everything to file
    let mut file = std::fs::File::create(output_path)?;

    // Write file data
    file.write_all(&filters_data)?;
    file.write_all(&metadata_json)?;
    file.write_all(&header_json)?;

    // Write bundle metadata
    file.write_all(&bundle_metadata_json)?;

    // Write metadata length (4 bytes, little endian)
    let metadata_len = bundle_metadata_json.len() as u32;
    file.write_all(&metadata_len.to_le_bytes())?;

    // Write empty hotcache (0 bytes for FuseXRef)
    // No actual hotcache data

    // Write hotcache length (4 bytes, little endian) - 0 for no hotcache
    let hotcache_len: u32 = 0;
    file.write_all(&hotcache_len.to_le_bytes())?;

    // Get total file size
    let total_size = file.seek(SeekFrom::End(0))?;
    let footer_end = total_size;

    debug_println!(
        "[FUSE_XREF_STORAGE] ✅ Saved bundle: {} bytes, footer: {}-{}",
        total_size,
        footer_start,
        footer_end
    );

    Ok((total_size, footer_start, footer_end))
}

/// Load a FuseXRef from a bundle file
pub fn load_fuse_xref(path: &Path) -> Result<FuseXRef> {
    debug_println!("[FUSE_XREF_STORAGE] Loading FuseXRef from {:?}", path);

    let mut file = std::fs::File::open(path)
        .with_context(|| format!("Failed to open FuseXRef file: {:?}", path))?;

    let file_len = file.seek(SeekFrom::End(0))?;
    if file_len < 8 {
        return Err(anyhow!("FuseXRef file too small: {} bytes", file_len));
    }

    // Read hotcache length (last 4 bytes)
    file.seek(SeekFrom::End(-4))?;
    let mut hotcache_len_bytes = [0u8; 4];
    file.read_exact(&mut hotcache_len_bytes)?;
    let hotcache_len = u32::from_le_bytes(hotcache_len_bytes) as u64;

    // Read metadata length (before hotcache)
    let metadata_len_offset = file_len - 4 - hotcache_len - 4;
    file.seek(SeekFrom::Start(metadata_len_offset))?;
    let mut metadata_len_bytes = [0u8; 4];
    file.read_exact(&mut metadata_len_bytes)?;
    let metadata_len = u32::from_le_bytes(metadata_len_bytes) as u64;

    // Read bundle metadata
    let metadata_offset = metadata_len_offset - metadata_len;
    file.seek(SeekFrom::Start(metadata_offset))?;
    let mut metadata_json = vec![0u8; metadata_len as usize];
    file.read_exact(&mut metadata_json)?;

    let bundle_metadata: BundleMetadata = serde_json::from_slice(&metadata_json)
        .context("Failed to parse bundle metadata")?;

    debug_println!(
        "[FUSE_XREF_STORAGE] Bundle has {} files",
        bundle_metadata.files.len()
    );

    // Read header file
    let header_range = bundle_metadata
        .files
        .get(XREF_HEADER_FILE)
        .ok_or_else(|| anyhow!("Missing {} in bundle", XREF_HEADER_FILE))?;
    file.seek(SeekFrom::Start(header_range.start))?;
    let mut header_json = vec![0u8; (header_range.end - header_range.start) as usize];
    file.read_exact(&mut header_json)?;
    let header: FuseXRefHeader = serde_json::from_slice(&header_json)
        .context("Failed to parse xref header")?;

    // Read split metadata file
    let metadata_range = bundle_metadata
        .files
        .get(SPLIT_METADATA_FILE)
        .ok_or_else(|| anyhow!("Missing {} in bundle", SPLIT_METADATA_FILE))?;
    file.seek(SeekFrom::Start(metadata_range.start))?;
    let mut split_metadata_json = vec![0u8; (metadata_range.end - metadata_range.start) as usize];
    file.read_exact(&mut split_metadata_json)?;
    let split_metadata_wrapper: SplitMetadataWrapper = serde_json::from_slice(&split_metadata_json)
        .context("Failed to parse split metadata")?;

    // Read filters file
    let filters_range = bundle_metadata
        .files
        .get(FUSE_FILTERS_FILE)
        .ok_or_else(|| anyhow!("Missing {} in bundle", FUSE_FILTERS_FILE))?;
    file.seek(SeekFrom::Start(filters_range.start))?;
    let mut filters_data = vec![0u8; (filters_range.end - filters_range.start) as usize];
    file.read_exact(&mut filters_data)?;

    let filters = deserialize_filters(&filters_data, header.num_splits as usize)?;

    debug_println!(
        "[FUSE_XREF_STORAGE] ✅ Loaded FuseXRef: {} splits, {} total terms",
        filters.len(),
        header.total_terms
    );

    Ok(FuseXRef {
        header,
        filters,
        metadata: split_metadata_wrapper.splits,
    })
}

/// Load a FuseXRef from a URI (supports s3://, azure://, file://)
pub fn load_fuse_xref_from_uri(
    uri: &str,
    aws_config: Option<MergeAwsConfig>,
    azure_config: Option<MergeAzureConfig>,
) -> Result<FuseXRef> {
    debug_println!("[FUSE_XREF_STORAGE] Loading FuseXRef from URI: {}", uri);

    // Handle local file paths directly
    if uri.starts_with("file://") {
        let path = &uri[7..];
        return load_fuse_xref(Path::new(path));
    }
    if !uri.starts_with("s3://") && !uri.starts_with("azure://") {
        // Assume it's a local path
        return load_fuse_xref(Path::new(uri));
    }

    // Build storage resolver with credentials
    let s3_config = aws_config.as_ref().map(merge_aws_to_s3_config);
    let azure_config_resolved = azure_config.as_ref().map(merge_azure_to_azure_config);
    let storage_resolver = get_configured_storage_resolver(s3_config, azure_config_resolved);

    // Use runtime to download the file
    let runtime = QuickwitRuntimeManager::global();
    let data = runtime.handle().block_on(async {
        download_file_from_uri(&storage_resolver, uri).await
    })?;

    debug_println!(
        "[FUSE_XREF_STORAGE] Downloaded {} bytes from {}",
        data.len(),
        uri
    );

    // Parse from bytes
    load_fuse_xref_from_bytes(&data)
}

/// Download a file from a URI using Quickwit storage
async fn download_file_from_uri(storage_resolver: &StorageResolver, uri: &str) -> Result<Vec<u8>> {
    let storage = resolve_storage_for_split(storage_resolver, uri).await?;

    // Extract the file path from the URI
    let file_path = extract_file_path_from_uri(uri);

    debug_println!(
        "[FUSE_XREF_STORAGE] Downloading file: {} from storage",
        file_path
    );

    // Get file size
    let file_len = storage
        .file_num_bytes(Path::new(&file_path))
        .await
        .context("Failed to get file size")?;

    // Download entire file
    let data = storage
        .get_all(Path::new(&file_path))
        .await
        .context("Failed to download file")?;

    debug_println!(
        "[FUSE_XREF_STORAGE] Downloaded {} bytes (expected {})",
        data.len(),
        file_len
    );

    Ok(data.to_vec())
}

/// Extract just the filename from a URI (e.g., "s3://bucket/path/file.split" -> "file.split")
///
/// Note: resolve_storage_for_split creates storage at the parent directory level,
/// so we only need the filename, not the full path.
fn extract_file_path_from_uri(uri: &str) -> String {
    // Find the last '/' and return everything after it
    if let Some(last_slash) = uri.rfind('/') {
        return uri[last_slash + 1..].to_string();
    }
    uri.to_string()
}

/// Load a FuseXRef from raw bytes
fn load_fuse_xref_from_bytes(data: &[u8]) -> Result<FuseXRef> {
    let file_len = data.len() as u64;
    if file_len < 8 {
        return Err(anyhow!("FuseXRef data too small: {} bytes", file_len));
    }

    let mut cursor = Cursor::new(data);

    // Read hotcache length (last 4 bytes)
    cursor.seek(SeekFrom::End(-4))?;
    let mut hotcache_len_bytes = [0u8; 4];
    cursor.read_exact(&mut hotcache_len_bytes)?;
    let hotcache_len = u32::from_le_bytes(hotcache_len_bytes) as u64;

    // Read metadata length (before hotcache)
    let metadata_len_offset = file_len - 4 - hotcache_len - 4;
    cursor.seek(SeekFrom::Start(metadata_len_offset))?;
    let mut metadata_len_bytes = [0u8; 4];
    cursor.read_exact(&mut metadata_len_bytes)?;
    let metadata_len = u32::from_le_bytes(metadata_len_bytes) as u64;

    // Read bundle metadata
    let metadata_offset = metadata_len_offset - metadata_len;
    cursor.seek(SeekFrom::Start(metadata_offset))?;
    let mut metadata_json = vec![0u8; metadata_len as usize];
    cursor.read_exact(&mut metadata_json)?;

    let bundle_metadata: BundleMetadata = serde_json::from_slice(&metadata_json)
        .context("Failed to parse bundle metadata")?;

    debug_println!(
        "[FUSE_XREF_STORAGE] Bundle has {} files",
        bundle_metadata.files.len()
    );

    // Read header file
    let header_range = bundle_metadata
        .files
        .get(XREF_HEADER_FILE)
        .ok_or_else(|| anyhow!("Missing {} in bundle", XREF_HEADER_FILE))?;
    cursor.seek(SeekFrom::Start(header_range.start))?;
    let mut header_json = vec![0u8; (header_range.end - header_range.start) as usize];
    cursor.read_exact(&mut header_json)?;
    let header: FuseXRefHeader = serde_json::from_slice(&header_json)
        .context("Failed to parse xref header")?;

    // Read split metadata file
    let metadata_range = bundle_metadata
        .files
        .get(SPLIT_METADATA_FILE)
        .ok_or_else(|| anyhow!("Missing {} in bundle", SPLIT_METADATA_FILE))?;
    cursor.seek(SeekFrom::Start(metadata_range.start))?;
    let mut split_metadata_json = vec![0u8; (metadata_range.end - metadata_range.start) as usize];
    cursor.read_exact(&mut split_metadata_json)?;
    let split_metadata_wrapper: SplitMetadataWrapper = serde_json::from_slice(&split_metadata_json)
        .context("Failed to parse split metadata")?;

    // Read filters file
    let filters_range = bundle_metadata
        .files
        .get(FUSE_FILTERS_FILE)
        .ok_or_else(|| anyhow!("Missing {} in bundle", FUSE_FILTERS_FILE))?;
    cursor.seek(SeekFrom::Start(filters_range.start))?;
    let mut filters_data = vec![0u8; (filters_range.end - filters_range.start) as usize];
    cursor.read_exact(&mut filters_data)?;

    let filters = deserialize_filters(&filters_data, header.num_splits as usize)?;

    debug_println!(
        "[FUSE_XREF_STORAGE] ✅ Loaded FuseXRef from bytes: {} splits, {} total terms",
        filters.len(),
        header.total_terms
    );

    Ok(FuseXRef {
        header,
        filters,
        metadata: split_metadata_wrapper.splits,
    })
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

/// Serialize Binary Fuse filters to bytes (supports both Fuse8 and Fuse16, with optional compression)
///
/// Header format (32 bytes):
/// - Bytes 0-3: Magic ("FXRF")
/// - Bytes 4-7: Version (u32)
/// - Bytes 8-11: Num filters (u32)
/// - Bytes 12-15: Filter type (u32) - 1=Fuse8, 2=Fuse16
/// - Bytes 16: Compression type (u8) - 0=None, 1/3/6/9=Zstd level
/// - Bytes 17-19: Reserved (3 bytes)
/// - Bytes 20-23: Uncompressed size (u32) - for decompression buffer allocation
/// - Bytes 24-31: Reserved (8 bytes)
fn serialize_filters(
    filters: &[FuseFilter],
    metadata: &[SplitFilterMetadata],
    compression: CompressionType,
) -> Result<Vec<u8>> {
    use crate::debug_println;

    // Use postcard for efficient serialization
    let mut output = Vec::new();

    // Determine filter type from first filter (all should be same type)
    let filter_type = filters.first()
        .map(|f| f.filter_type())
        .unwrap_or(FuseFilterType::Fuse8);

    // Build index table and filter data first (before compression)
    let mut filter_data = Vec::new();
    let mut index_entries: Vec<FilterIndexEntry> = Vec::new();

    for (idx, filter) in filters.iter().enumerate() {
        let serialized = match filter {
            FuseFilter::Fuse8(f) => postcard::to_stdvec(f)
                .map_err(|e| anyhow!("Failed to serialize Fuse8 filter: {}", e))?,
            FuseFilter::Fuse16(f) => postcard::to_stdvec(f)
                .map_err(|e| anyhow!("Failed to serialize Fuse16 filter: {}", e))?,
        };

        // Get num_terms from metadata if available, otherwise estimate from serialized size
        let num_keys = if idx < metadata.len() {
            metadata[idx].num_terms
        } else {
            // Estimate: serialized size / bytes per key
            let bytes_per_key = match filter_type {
                FuseFilterType::Fuse8 => 1.24,
                FuseFilterType::Fuse16 => 2.24,
            };
            (serialized.len() as f64 / bytes_per_key) as u64
        };

        index_entries.push(FilterIndexEntry {
            offset: filter_data.len() as u64,
            size: serialized.len() as u64,
            num_keys,
        });

        filter_data.extend_from_slice(&serialized);
    }

    // Build the uncompressed payload (index table + filter data)
    let mut payload = Vec::new();
    for entry in &index_entries {
        payload.extend_from_slice(&entry.offset.to_le_bytes());
        payload.extend_from_slice(&entry.size.to_le_bytes());
        payload.extend_from_slice(&entry.num_keys.to_le_bytes());
    }
    payload.extend_from_slice(&filter_data);

    let uncompressed_size = payload.len() as u32;

    // Compress if requested
    let (final_payload, actual_compression) = if compression.is_compressed() {
        let compressed = zstd::encode_all(&payload[..], compression.zstd_level())
            .map_err(|e| anyhow!("Failed to compress filter data: {}", e))?;

        // Only use compression if it actually reduces size
        if compressed.len() < payload.len() {
            debug_println!(
                "[FUSE_XREF_STORAGE] Compressed {} -> {} bytes ({:.1}% reduction)",
                payload.len(),
                compressed.len(),
                (1.0 - compressed.len() as f64 / payload.len() as f64) * 100.0
            );
            (compressed, compression)
        } else {
            debug_println!(
                "[FUSE_XREF_STORAGE] Compression would increase size ({} -> {}), storing uncompressed",
                payload.len(),
                compressed.len()
            );
            (payload, CompressionType::None)
        }
    } else {
        (payload, CompressionType::None)
    };

    // Write header: magic + version + num_filters + filter_type + compression info
    output.extend_from_slice(FUSE_XREF_MAGIC);
    output.extend_from_slice(&FUSE_XREF_VERSION.to_le_bytes());
    output.extend_from_slice(&(filters.len() as u32).to_le_bytes());
    output.extend_from_slice(&(filter_type as u32).to_le_bytes());

    // Compression info (was reserved bytes in v1)
    output.push(actual_compression.zstd_level() as u8); // Byte 16: compression level
    output.extend_from_slice(&[0u8; 3]); // Bytes 17-19: reserved
    output.extend_from_slice(&uncompressed_size.to_le_bytes()); // Bytes 20-23: uncompressed size
    output.extend_from_slice(&[0u8; 8]); // Bytes 24-31: reserved

    // Write payload (compressed or uncompressed)
    output.extend_from_slice(&final_payload);

    Ok(output)
}

/// Deserialize Binary Fuse filters from bytes (supports both Fuse8 and Fuse16, with compression)
fn deserialize_filters(data: &[u8], expected_count: usize) -> Result<Vec<FuseFilter>> {
    use crate::debug_println;

    if data.len() < 32 {
        return Err(anyhow!("Filter data too small: {} bytes", data.len()));
    }

    // Read header
    let magic = &data[0..4];
    if magic != FUSE_XREF_MAGIC {
        return Err(anyhow!("Invalid filter magic: {:?}", magic));
    }

    let version = u32::from_le_bytes(data[4..8].try_into()?);
    if version > FUSE_XREF_VERSION {
        return Err(anyhow!(
            "Unsupported filter version: {} (max: {})",
            version,
            FUSE_XREF_VERSION
        ));
    }

    let num_filters = u32::from_le_bytes(data[8..12].try_into()?) as usize;
    let filter_type_raw = u32::from_le_bytes(data[12..16].try_into()?);
    let filter_type = match filter_type_raw {
        1 => FuseFilterType::Fuse8,
        2 => FuseFilterType::Fuse16,
        _ => FuseFilterType::Fuse8, // Default to Fuse8 for backwards compatibility
    };

    // Read compression info (v2+ format, bytes 16-23)
    let compression_level = data[16];
    let uncompressed_size = u32::from_le_bytes(data[20..24].try_into()?) as usize;

    if num_filters != expected_count {
        return Err(anyhow!(
            "Filter count mismatch: expected {}, got {}",
            expected_count,
            num_filters
        ));
    }

    // Get the payload (after 32-byte header)
    let payload_data = &data[32..];

    // Decompress if needed
    let decompressed: Vec<u8>;
    let payload = if compression_level > 0 {
        debug_println!(
            "[FUSE_XREF_STORAGE] Decompressing {} bytes (uncompressed: {} bytes)",
            payload_data.len(),
            uncompressed_size
        );

        decompressed = zstd::decode_all(payload_data)
            .map_err(|e| anyhow!("Failed to decompress filter data: {}", e))?;

        if decompressed.len() != uncompressed_size {
            return Err(anyhow!(
                "Decompressed size mismatch: expected {}, got {}",
                uncompressed_size,
                decompressed.len()
            ));
        }

        &decompressed[..]
    } else {
        payload_data
    };

    // Read index table from payload
    let index_entry_size = 24; // 8 + 8 + 8 bytes
    let index_end = num_filters * index_entry_size;

    if payload.len() < index_end {
        return Err(anyhow!("Payload too small for index table"));
    }

    let mut index_entries = Vec::with_capacity(num_filters);
    for i in 0..num_filters {
        let entry_start = i * index_entry_size;
        let offset = u64::from_le_bytes(payload[entry_start..entry_start + 8].try_into()?);
        let size = u64::from_le_bytes(payload[entry_start + 8..entry_start + 16].try_into()?);
        let num_keys = u64::from_le_bytes(payload[entry_start + 16..entry_start + 24].try_into()?);
        index_entries.push(FilterIndexEntry {
            offset,
            size,
            num_keys,
        });
    }

    // Read filter data from payload
    let filter_data_start = index_end;
    let mut filters = Vec::with_capacity(num_filters);

    for entry in &index_entries {
        let start = filter_data_start + entry.offset as usize;
        let end = start + entry.size as usize;

        if end > payload.len() {
            return Err(anyhow!(
                "Filter data out of bounds: {} > {}",
                end,
                payload.len()
            ));
        }

        let filter = match filter_type {
            FuseFilterType::Fuse8 => {
                let f: BinaryFuse8 = postcard::from_bytes(&payload[start..end])
                    .map_err(|e| anyhow!("Failed to deserialize Fuse8 filter: {}", e))?;
                FuseFilter::Fuse8(f)
            }
            FuseFilterType::Fuse16 => {
                let f: BinaryFuse16 = postcard::from_bytes(&payload[start..end])
                    .map_err(|e| anyhow!("Failed to deserialize Fuse16 filter: {}", e))?;
                FuseFilter::Fuse16(f)
            }
        };

        filters.push(filter);
    }

    Ok(filters)
}

// Helper types for serialization

#[derive(serde::Serialize, serde::Deserialize)]
struct SplitMetadataWrapper {
    splits: Vec<SplitFilterMetadata>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct BundleMetadata {
    files: BTreeMap<String, FileRange>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct FileRange {
    start: u64,
    end: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn create_test_xref() -> FuseXRef {
        let keys1: Vec<u64> = vec![
            fxhash::hash64("title:hello"),
            fxhash::hash64("title:world"),
        ];
        let keys2: Vec<u64> = vec![
            fxhash::hash64("body:test"),
            fxhash::hash64("body:example"),
        ];

        let filter1 = BinaryFuse8::try_from(&keys1[..]).unwrap();
        let filter2 = BinaryFuse8::try_from(&keys2[..]).unwrap();

        let mut xref = FuseXRef::new("test-xref".to_string(), "test-index".to_string());
        xref.filters.push(FuseFilter::Fuse8(filter1));
        xref.filters.push(FuseFilter::Fuse8(filter2));
        xref.metadata.push(SplitFilterMetadata::new(
            0,
            "file:///split1.split".to_string(),
            "split1".to_string(),
            0,
            1000,
        ));
        xref.metadata.push(SplitFilterMetadata::new(
            1,
            "file:///split2.split".to_string(),
            "split2".to_string(),
            0,
            2000,
        ));
        xref.header.num_splits = 2;
        xref.header.total_terms = 4;

        xref
    }

    fn create_test_xref_fuse16() -> FuseXRef {
        let keys1: Vec<u64> = vec![
            fxhash::hash64("title:hello16"),
            fxhash::hash64("title:world16"),
        ];
        let keys2: Vec<u64> = vec![
            fxhash::hash64("body:test16"),
            fxhash::hash64("body:example16"),
        ];

        let filter1 = BinaryFuse16::try_from(&keys1[..]).unwrap();
        let filter2 = BinaryFuse16::try_from(&keys2[..]).unwrap();

        let mut xref = FuseXRef::new("test-xref-16".to_string(), "test-index".to_string());
        xref.filters.push(FuseFilter::Fuse16(filter1));
        xref.filters.push(FuseFilter::Fuse16(filter2));
        xref.header.filter_type = "BinaryFuse16".to_string();
        xref.metadata.push(SplitFilterMetadata::new(
            0,
            "file:///split1.split".to_string(),
            "split1".to_string(),
            0,
            1000,
        ));
        xref.metadata.push(SplitFilterMetadata::new(
            1,
            "file:///split2.split".to_string(),
            "split2".to_string(),
            0,
            2000,
        ));
        xref.header.num_splits = 2;
        xref.header.total_terms = 4;

        xref
    }

    #[test]
    fn test_serialize_deserialize_filters_no_compression() {
        let xref = create_test_xref();

        let serialized =
            serialize_filters(&xref.filters, &xref.metadata, CompressionType::None).unwrap();
        let deserialized = deserialize_filters(&serialized, 2).unwrap();

        assert_eq!(deserialized.len(), 2);

        // Verify filter contents work
        let hash1 = fxhash::hash64("title:hello");
        let hash2 = fxhash::hash64("body:test");

        assert!(deserialized[0].contains(&hash1));
        assert!(deserialized[1].contains(&hash2));
    }

    #[test]
    fn test_serialize_deserialize_filters_with_compression() {
        let xref = create_test_xref();

        // Test with zstd compression
        let serialized =
            serialize_filters(&xref.filters, &xref.metadata, CompressionType::Zstd3).unwrap();
        let deserialized = deserialize_filters(&serialized, 2).unwrap();

        assert_eq!(deserialized.len(), 2);

        // Verify filter contents work
        let hash1 = fxhash::hash64("title:hello");
        let hash2 = fxhash::hash64("body:test");

        assert!(deserialized[0].contains(&hash1));
        assert!(deserialized[1].contains(&hash2));
    }

    #[test]
    fn test_save_load_bundle_no_compression() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.fxref");

        let xref = create_test_xref();

        // Save without compression
        let (size, footer_start, footer_end) =
            save_fuse_xref(&xref, &path, CompressionType::None).unwrap();
        assert!(size > 0);
        assert!(footer_start < footer_end);

        // Load
        let loaded = load_fuse_xref(&path).unwrap();

        assert_eq!(loaded.header.xref_id, "test-xref");
        assert_eq!(loaded.header.num_splits, 2);
        assert_eq!(loaded.filters.len(), 2);
        assert_eq!(loaded.metadata.len(), 2);

        // Verify filter functionality
        assert!(loaded.check(0, "title", "hello"));
        assert!(loaded.check(1, "body", "test"));
        assert!(!loaded.check(0, "body", "test"));
    }

    #[test]
    fn test_save_load_bundle_with_compression() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_compressed.fxref");

        let xref = create_test_xref();

        // Save with compression
        let (compressed_size, footer_start, footer_end) =
            save_fuse_xref(&xref, &path, CompressionType::Zstd3).unwrap();
        assert!(compressed_size > 0);
        assert!(footer_start < footer_end);

        // Load (should auto-decompress)
        let loaded = load_fuse_xref(&path).unwrap();

        assert_eq!(loaded.header.xref_id, "test-xref");
        assert_eq!(loaded.header.num_splits, 2);
        assert_eq!(loaded.filters.len(), 2);
        assert_eq!(loaded.metadata.len(), 2);

        // Verify filter functionality still works after decompression
        assert!(loaded.check(0, "title", "hello"));
        assert!(loaded.check(1, "body", "test"));
        assert!(!loaded.check(0, "body", "test"));
    }

    #[test]
    fn test_compression_levels() {
        let xref = create_test_xref();

        // Test all compression levels
        for compression in [
            CompressionType::None,
            CompressionType::Zstd1,
            CompressionType::Zstd3,
            CompressionType::Zstd6,
            CompressionType::Zstd9,
        ] {
            let serialized =
                serialize_filters(&xref.filters, &xref.metadata, compression).unwrap();
            let deserialized = deserialize_filters(&serialized, 2).unwrap();

            assert_eq!(deserialized.len(), 2);
            assert!(deserialized[0].contains(&fxhash::hash64("title:hello")));
            assert!(deserialized[1].contains(&fxhash::hash64("body:test")));
        }
    }
}
