// upload.rs - Cloud storage upload and split file creation
// Extracted from mod.rs during refactoring
// Contains: upload_split_to_s3_impl, upload_split_to_s3, estimate_peak_memory_usage,
//           calculate_optimal_batch_size, create_merged_split_file

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};

use quickwit_common::uri::{Uri, Protocol};
use quickwit_indexing::open_index;

use crate::debug_println;
use crate::runtime_manager::QuickwitRuntimeManager;
use super::{QuickwitSplitMetadata, SplitConfig, FooterOffsets, create_quickwit_split};
use super::split_utils::create_quickwit_tokenizer_manager;
use super::merge_config::InternalMergeConfig;

// Debug logging macro - controlled by TANTIVY4JAVA_DEBUG environment variable
macro_rules! debug_log {
    ($($arg:tt)*) => {
        debug_println!("DEBUG: {}", format!($($arg)*))
    };
}

/// Upload a local split file to cloud storage (S3 or Azure) using the storage resolver
pub async fn upload_split_to_s3_impl(local_split_path: &Path, s3_url: &str, config: &InternalMergeConfig) -> Result<()> {
    use quickwit_storage::{StorageResolver, LocalFileStorageFactory, S3CompatibleObjectStorageFactory, AzureBlobStorageFactory};
    use quickwit_config::{AzureStorageConfig, S3StorageConfig};
    use std::str::FromStr;

    debug_log!("Starting cloud storage upload from {:?} to {}", local_split_path, s3_url);

    // Parse the URI (could be S3 or Azure)
    let storage_url_uri = Uri::from_str(s3_url)?;

    // Create storage resolver with AWS and/or Azure config from MergeConfig
    let mut resolver_builder = StorageResolver::builder()
        .register(LocalFileStorageFactory::default());

    // Add S3 storage if AWS config is present
    if let Some(ref aws_config) = config.aws_config {
        let s3_config = S3StorageConfig {
            region: Some(aws_config.region.clone()),
            access_key_id: aws_config.access_key.clone(),
            secret_access_key: aws_config.secret_key.clone(),
            session_token: aws_config.session_token.clone(),
            endpoint: aws_config.endpoint.clone(),
            force_path_style_access: aws_config.force_path_style,
            ..Default::default()
        };
        resolver_builder = resolver_builder.register(S3CompatibleObjectStorageFactory::new(s3_config));
        debug_log!("Added S3 storage factory to resolver");
    }

    // Add Azure storage if Azure config is present
    if let Some(ref azure_config) = config.azure_config {
        let azure_storage_config = AzureStorageConfig {
            account_name: Some(azure_config.account_name.clone()),
            access_key: azure_config.account_key.clone(),
            bearer_token: azure_config.bearer_token.clone(),
        };
        resolver_builder = resolver_builder.register(AzureBlobStorageFactory::new(azure_storage_config));
        debug_log!("Added Azure storage factory to resolver");
    }

    let storage_resolver = resolver_builder.build()
        .map_err(|e| anyhow!("Failed to create storage resolver for upload: {}", e))?;

    // For S3/Azure URIs, we need to resolve the parent directory, not the file itself
    let (storage_uri, file_name) = if storage_url_uri.protocol() == Protocol::S3 || storage_url_uri.protocol() == Protocol::Azure {
        let uri_str = storage_url_uri.as_str();
        if let Some(last_slash) = uri_str.rfind('/') {
            let parent_uri_str = &uri_str[..last_slash]; // Get s3://bucket/splits or azure://container/path
            let file_name = &uri_str[last_slash + 1..];  // Get filename
            debug_log!("Split cloud storage URI for upload into parent: {} and file: {}", parent_uri_str, file_name);
            (Uri::from_str(parent_uri_str)?, file_name.to_string())
        } else {
            return Err(anyhow!("Invalid cloud storage URL format: {}", s3_url));
        }
    } else {
        return Err(anyhow!("Only S3 and Azure URLs are supported for upload, got: {}", s3_url));
    };

    // ✅ QUICKWIT NATIVE: Storage resolver already includes timeout and retry policies
    let storage = storage_resolver.resolve(&storage_uri).await
        .map_err(|e| anyhow!("Failed to resolve storage for '{}': {}", s3_url, e))?;

    // FIXED: Use memory mapping for file upload instead of loading entire file into RAM
    let file = std::fs::File::open(local_split_path)
        .map_err(|e| anyhow!("Failed to open local split file {:?}: {}", local_split_path, e))?;

    let file_size = file.metadata()
        .map_err(|e| anyhow!("Failed to get file metadata: {}", e))?
        .len() as usize;

    debug_log!("✅ MEMORY FIX: Uploading {} bytes using memory mapping", file_size);

    let mmap = unsafe {
        memmap2::Mmap::map(&file)
            .map_err(|e| anyhow!("Failed to memory map split file: {}", e))?
    };

    // ⚠️ MEMORY CONSTRAINT: S3 PutPayload API requires Vec<u8>, cannot use lazy loading here
    // However, we optimize by checking file size and providing clear warnings
    const LARGE_FILE_THRESHOLD: usize = 100_000_000; // 100MB

    if file_size > LARGE_FILE_THRESHOLD {
        debug_log!("⚠️ LARGE FILE WARNING: Uploading {} MB file will use ~{} MB memory",
                   file_size / 1_000_000, (file_size * 2) / 1_000_000);
        debug_log!("   Consider splitting into smaller chunks if memory is constrained");
    } else {
        debug_log!("📤 S3 UPLOAD: File size {} MB is within acceptable range for memory usage",
                   file_size / 1_000_000);
    }

    let file_data = mmap.to_vec();
    debug_log!("📤 S3 UPLOAD: Copying {} bytes for S3 upload (unavoidable due to PutPayload API)",
               file_data.len());

    storage.put(Path::new(&file_name), Box::new(file_data)).await
    .map_err(|e| anyhow!("Failed to upload split to S3: {}", e))?;

    debug_log!("Successfully uploaded {} bytes to S3: {}", file_size, s3_url);
    Ok(())
}

/// Synchronous wrapper for S3 upload
pub fn upload_split_to_s3(local_split_path: &Path, s3_url: &str, config: &InternalMergeConfig) -> Result<()> {
    // ✅ CRITICAL FIX: Use shared global runtime handle to prevent multiple runtime deadlocks
    QuickwitRuntimeManager::global().handle().block_on(upload_split_to_s3_impl(local_split_path, s3_url, config))
}

/// Estimate peak memory usage for merge operations
/// Each split requires approximately 2x file size in memory (mmap + Vec copy)
pub fn estimate_peak_memory_usage(num_splits: usize) -> usize {
    // Conservative estimate: 50MB average split size * 2x memory usage * number of splits
    const AVERAGE_SPLIT_SIZE: usize = 50_000_000; // 50MB
    const MEMORY_MULTIPLIER: usize = 2; // mmap + Vec copy

    num_splits * AVERAGE_SPLIT_SIZE * MEMORY_MULTIPLIER
}

/// Calculate optimal batch size for memory-constrained merge operations
/// Returns the maximum number of splits to process at once based on available memory
pub fn calculate_optimal_batch_size(total_splits: usize, max_memory_mb: usize) -> usize {
    const AVERAGE_SPLIT_SIZE_MB: usize = 50; // 50MB average
    const MEMORY_MULTIPLIER: usize = 2; // mmap + Vec copy

    let memory_per_split_mb = AVERAGE_SPLIT_SIZE_MB * MEMORY_MULTIPLIER;
    let max_splits_per_batch = max_memory_mb / memory_per_split_mb;

    // Ensure at least 2 splits per batch (minimum for merge), max the total number
    std::cmp::min(total_splits, std::cmp::max(2, max_splits_per_batch))
}

/// Create the merged split file using existing Quickwit split creation logic
/// Returns the footer offsets for the merged split
pub fn create_merged_split_file(merged_index_path: &Path, output_path: &str, metadata: &QuickwitSplitMetadata) -> Result<FooterOffsets> {
    use tantivy::directory::MmapDirectory;

    debug_log!("Creating merged split file at {} from index {:?}", output_path, merged_index_path);

    // Open the merged Tantivy index
    let merged_directory = MmapDirectory::open(merged_index_path)?;
    let tokenizer_manager = create_quickwit_tokenizer_manager();
    let merged_index = open_index(merged_directory, &tokenizer_manager)
        .map_err(|e| {
            let error_msg = e.to_string();
            anyhow!("Failed to open merged index for split creation: {}", error_msg)
        })?;

    // Use the existing split creation logic and capture footer offsets
    // Create a default config for merged splits
    let default_config = SplitConfig {
    index_uid: "merged".to_string(),
    source_id: "merge".to_string(),
    node_id: "merge-node".to_string(),
    doc_mapping_uid: "default".to_string(),
    partition_id: 0,
    time_range_start: None,
    time_range_end: None,
    tags: BTreeSet::new(),
    metadata: HashMap::new(),
    streaming_chunk_size: 64_000_000,
    enable_progress_tracking: false,
    enable_streaming_io: true,
    };

    // ✅ PERFORMANCE FIX: Use separate thread to prevent runtime deadlock
    let footer_offsets = {
        let merged_index_clone = merged_index.clone();
        let merged_index_path_clone = merged_index_path.to_path_buf();
        let output_path_clone = PathBuf::from(output_path);
        let metadata_clone = metadata.clone();
        let config_clone = default_config.clone();

        std::thread::spawn(move || {
            // ✅ CRITICAL FIX: Use shared global runtime instead of creating separate runtime
            // This eliminates multiple Tokio runtime conflicts that cause deadlocks
            QuickwitRuntimeManager::global().handle().block_on(create_quickwit_split(
                &merged_index_clone,
                &merged_index_path_clone,
                &output_path_clone,
                &metadata_clone,
                &config_clone
            ))
        }).join().map_err(|_| anyhow!("Split creation thread panicked"))??
    };

    debug_log!("Successfully created merged split file: {} with footer offsets: {:?}", output_path, footer_offsets);
    Ok(footer_offsets)
}
