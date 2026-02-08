// upload.rs - Cloud storage upload and split file creation
// Extracted from mod.rs during refactoring
// Contains: upload_split_to_s3_impl, upload_split_to_s3, estimate_peak_memory_usage,
//           calculate_optimal_batch_size, create_merged_split_file

use std::collections::{BTreeSet, HashMap};
use std::io;
use std::ops::Range;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use aws_sdk_s3::primitives::{ByteStream, FsBuilder, Length};

use quickwit_common::uri::{Uri, Protocol};
use quickwit_indexing::open_index;
use quickwit_storage::PutPayload;

/// Streaming file payload for memory-efficient cloud uploads.
/// Unlike Vec<u8>, this streams directly from disk without loading into heap.
#[derive(Clone)]
struct StreamingFilePayload {
    path: PathBuf,
    len: u64,
}

/// Helper trait for cloning PutPayload
trait PutPayloadClone {
    fn box_clone(&self) -> Box<dyn PutPayload>;
}

impl<T> PutPayloadClone for T
where T: 'static + PutPayload + Clone
{
    fn box_clone(&self) -> Box<dyn PutPayload> {
        Box::new(self.clone())
    }
}

#[async_trait]
impl PutPayload for StreamingFilePayload {
    fn len(&self) -> u64 {
        self.len
    }

    async fn range_byte_stream(&self, range: Range<u64>) -> io::Result<ByteStream> {
        assert!(!range.is_empty());
        assert!(range.end <= self.len);

        let len = range.end - range.start;
        let mut fs_builder = FsBuilder::new().path(&self.path);

        if range.start > 0 {
            fs_builder = fs_builder.offset(range.start);
        }
        fs_builder = fs_builder.length(Length::Exact(len));

        fs_builder
            .build()
            .await
            .map_err(|error| io::Error::other(format!("failed to create byte stream: {error}")))
    }
}

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

    // Acquire upload semaphore to limit concurrent uploads and prevent thread starvation
    let upload_semaphore = QuickwitRuntimeManager::global().upload_semaphore();
    let _upload_permit = upload_semaphore.acquire().await
        .map_err(|e| anyhow!("Failed to acquire upload permit: {}", e))?;

    debug_log!("Starting cloud storage upload from {:?} to {} (upload permit acquired)", local_split_path, s3_url);

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

    // ✅ MEMORY OPTIMIZATION: Use streaming upload instead of loading entire file into memory
    // This reduces memory usage from O(file_size) to O(chunk_size) for S3/Azure uploads
    let file_size = std::fs::metadata(local_split_path)
        .map_err(|e| anyhow!("Failed to get file metadata: {}", e))?
        .len();

    debug_log!("✅ STREAMING UPLOAD: Uploading {} bytes ({} MB) using streaming (zero heap copy)",
               file_size, file_size / 1_000_000);

    // Create a streaming file payload - this streams from disk, no heap allocation
    let file_payload = StreamingFilePayload {
        path: local_split_path.to_path_buf(),
        len: file_size,
    };

    storage.put(Path::new(&file_name), Box::new(file_payload)).await
        .map_err(|e| anyhow!("Failed to upload split to cloud storage: {}", e))?;

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
                &config_clone,
                None, // No parquet manifest for standard merge
            ))
        }).join().map_err(|_| anyhow!("Split creation thread panicked"))??
    };

    debug_log!("Successfully created merged split file: {} with footer offsets: {:?}", output_path, footer_offsets);
    Ok(footer_offsets)
}
