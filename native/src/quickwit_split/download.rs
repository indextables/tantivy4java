// download.rs - Parallel split downloading and extraction
// Extracted from mod.rs during refactoring
// Contains: download_and_extract_splits_parallel, create_storage_resolver, download_split_to_temp_file

use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use quickwit_storage::{Storage, StorageResolver};
use quickwit_common::uri::{Uri, Protocol};

use crate::debug_println;
use super::merge_config::InternalMergeConfig;
use super::temp_management::{ExtractedSplit, extract_split_to_directory_impl, create_temp_directory_with_base};
use super::merge_registry::GLOBAL_DOWNLOAD_SEMAPHORE;

// Debug logging macro - controlled by TANTIVY4JAVA_DEBUG environment variable
macro_rules! debug_log {
    ($($arg:tt)*) => {
        debug_println!("DEBUG: {}", format!($($arg)*))
    };
}

/// ⚡ PARALLEL SPLIT DOWNLOADING AND EXTRACTION
/// Downloads and extracts multiple splits concurrently for maximum performance
/// Uses connection pooling and resource management to prevent overwhelming
pub async fn download_and_extract_splits_parallel(
    split_urls: &[String],
    merge_id: &str,
    config: &InternalMergeConfig,
) -> Result<(Vec<ExtractedSplit>, Vec<String>)> {
    use futures::future::join_all;
    use std::sync::Arc;

    debug_log!("🚀 Starting parallel download of {} splits with merge_id: {}", split_urls.len(), merge_id);

    // FIXED: Use global semaphore to limit downloads across ALL merge operations
    // This prevents overwhelming the system when multiple merges run simultaneously
    let download_semaphore = &*GLOBAL_DOWNLOAD_SEMAPHORE;
    let available_permits = download_semaphore.available_permits();
    debug_log!("✅ GLOBAL DOWNLOAD LIMIT: Using global download semaphore ({} permits available)", available_permits);

    // ✅ MEMORY OPTIMIZATION: Optional per-operation limit for very large splits
    // If max_concurrent_splits is set, create additional local semaphore
    let local_limit = if config.max_concurrent_splits > 0 {
        debug_log!("✅ MEMORY LIMIT: Using per-operation limit of {} concurrent splits", config.max_concurrent_splits);
        Some(std::sync::Arc::new(tokio::sync::Semaphore::new(config.max_concurrent_splits)))
    } else {
        None
    };

    // Create shared storage resolver for connection pooling
    let shared_storage_resolver = Arc::new(create_storage_resolver(config)?);

    // ✅ MEMORY OPTIMIZATION: Wrap config in Arc to avoid cloning for each split
    // This reduces memory allocations from O(N * config_size) to O(config_size)
    let shared_config = Arc::new(config.clone());
    let merge_id_arc: Arc<str> = Arc::from(merge_id);

    // Create download tasks for each split
    let download_tasks: Vec<_> = split_urls
    .iter()
    .enumerate()
    .map(|(i, split_url)| {
        let split_url = split_url.clone();
        let merge_id = Arc::clone(&merge_id_arc);
        let config = Arc::clone(&shared_config);
        let storage_resolver = Arc::clone(&shared_storage_resolver);
        let local_limit_clone = local_limit.clone();

        async move {
            // Acquire semaphore permit to limit concurrent downloads globally
            let _global_permit = GLOBAL_DOWNLOAD_SEMAPHORE.acquire().await
                .map_err(|e| anyhow!("Failed to acquire global download permit: {}", e))?;

            // ✅ MEMORY OPTIMIZATION: Also acquire local permit if per-operation limit configured
            let _local_permit = if let Some(ref local_sem) = local_limit_clone {
                Some(local_sem.acquire().await
                    .map_err(|e| anyhow!("Failed to acquire local download permit: {}", e))?)
            } else {
                None
            };

            debug_log!("📥 Starting download task {} for: {}", i, split_url);

            // Download and extract single split - gracefully handle failures
            // Note: Arc derefs to &T, but for explicit ref we use as_ref()
            download_and_extract_single_split_resilient(
                &split_url,
                i,
                merge_id.as_ref(),
                config.as_ref(),
                &storage_resolver,
            ).await
        }
    })
    .collect();

    // Execute all downloads concurrently and wait for completion - handle failures gracefully
    debug_log!("⏳ Waiting for {} concurrent download tasks to complete", download_tasks.len());
    let results = join_all(download_tasks).await;

    // Separate successful downloads from failed ones
    let mut successful_splits = Vec::new();
    let mut skipped_splits = Vec::new();

    for (i, result) in results.into_iter().enumerate() {
        match result {
            Ok(Some(extracted_split)) => {
                successful_splits.push(extracted_split);
            }
            Ok(None) => {
                let split_url = &split_urls[i];
                debug_log!("⚠️ Split {} skipped: {}", i, split_url);
                skipped_splits.push(split_url.clone());
            }
            Err(e) => {
                let split_url = &split_urls[i];
                let error_msg = e.to_string();

                // Check if this is a configuration error that should not be bypassed
                if super::is_configuration_error(&error_msg) {
                    debug_log!("🚨 CONFIGURATION ERROR in split {}: This is a system configuration issue, not a split corruption issue", i);
                    return Err(e);  // Bubble up configuration errors - these should fail the entire operation
                }

                debug_log!("🚨 Split {} failed: {} (Error: {})", i, split_url, e);
                skipped_splits.push(split_url.clone());
            }
        }
    }

    debug_log!("✅ Download completed: {} successful, {} skipped", successful_splits.len(), skipped_splits.len());
    Ok((successful_splits, skipped_splits))
}

/// Downloads and extracts a single split with resilient error handling
pub async fn download_and_extract_single_split_resilient(
    split_url: &str,
    split_index: usize,
    merge_id: &str,
    config: &InternalMergeConfig,
    storage_resolver: &StorageResolver,
) -> Result<Option<ExtractedSplit>> {
    // ✅ PANIC HANDLING: Handle error gracefully without spawn for now
    // Note: StorageResolver doesn't implement Clone, so we handle errors instead of panics
    match download_and_extract_single_split(
        split_url,
        split_index,
        merge_id,
        config,
        storage_resolver,
    ).await {
        Ok(extracted_split) => Ok(Some(extracted_split)),
        Err(e) => {
            let error_msg = e.to_string();

            // Check if this is a panic-like error (overflow, underflow, etc.)
            if error_msg.contains("attempt to subtract with overflow") ||
               error_msg.contains("arithmetic overflow") ||
               error_msg.contains("arithmetic underflow") ||
               error_msg.contains("index out of bounds") ||
               error_msg.contains("slice index") {
                debug_log!("🚨 ARITHMETIC ERROR: Split {} caused arithmetic error - skipping gracefully: {} (Error: {})", split_index, split_url, error_msg);
                return Ok(None); // Skip this split gracefully for arithmetic errors
            }

            // Check if this is a configuration error that should not be bypassed
            if super::is_configuration_error(&error_msg) {
                debug_log!("🚨 CONFIGURATION ERROR in split {}: This is a system configuration issue, not a split corruption issue", split_index);
                return Err(e);  // Preserve configuration errors - these should fail the operation
            }

            debug_log!("⚠️ Split {} failed, adding to skip list: {} (Error: {})", split_index, split_url, error_msg);
            Ok(None) // Skip this split gracefully for non-configuration errors
        }
    }
}

/// Downloads and extracts a single split (used by parallel download function)
pub async fn download_and_extract_single_split(
    split_url: &str,
    split_index: usize,
    merge_id: &str,
    config: &InternalMergeConfig,
    storage_resolver: &StorageResolver,
) -> Result<ExtractedSplit> {
    use std::str::FromStr;

    // Create process-specific temp directory to avoid race conditions
    let temp_extract_dir = create_temp_directory_with_base(
    &format!("tantivy4java_merge_{}_split_{}_", merge_id, split_index),
    config.temp_directory_path.as_deref()
    )?;
    let temp_extract_path = temp_extract_dir.path().to_path_buf();

    debug_log!("📁 Split {}: Created temp directory: {:?}", split_index, temp_extract_path);

    if split_url.contains("://") && !split_url.starts_with("file://") {
    // Handle S3/remote URLs
    debug_log!("🌐 Split {}: Processing S3/remote URL: {}", split_index, split_url);

    // Parse the split URI
    let split_uri = Uri::from_str(split_url)?;

    // For S3/Azure URIs, we need to resolve the parent directory, not the file itself
    let (storage_uri, file_name) = if split_uri.protocol() == Protocol::S3 || split_uri.protocol() == Protocol::Azure {
        let uri_str = split_uri.as_str();
        if let Some(last_slash) = uri_str.rfind('/') {
            let parent_uri_str = &uri_str[..last_slash]; // Get s3://bucket/splits or azure://container/path
            let file_name = &uri_str[last_slash + 1..];  // Get filename
            debug_log!("🔗 Split {}: Split {} URI into parent: {} and file: {}",
                      split_index, split_uri.protocol(), parent_uri_str, file_name);
            (Uri::from_str(parent_uri_str)?, Some(file_name.to_string()))
        } else {
            (split_uri.clone(), None)
        }
    } else {
        (split_uri.clone(), None)
    };

    // Resolve storage for the URI (uses cached/pooled connections)
    // ✅ QUICKWIT NATIVE: Storage resolver already includes timeout and retry policies
    let storage = storage_resolver.resolve(&storage_uri).await
        .map_err(|e| anyhow!("Split {}: Failed to resolve storage for '{}': {}", split_index, split_url, e))?;

    // Download the split file to temporary location
    let split_filename = file_name.unwrap_or_else(|| {
        split_url.split('/').last().unwrap_or("split.split").to_string()
    });
    let temp_split_path = temp_extract_path.join(&split_filename);

    debug_log!("⬇️ Split {}: Downloading {} to {:?}", split_index, split_url, temp_split_path);

    // ✅ CORRUPTION FIX: Download directly to temp file to avoid memory buffering and network slice issues
    // This approach eliminates the "line 1, column 2291" errors by using reliable local file I/O
    debug_log!("🔧 Split {}: Downloading directly to temp file to avoid network slice issues", split_index);

    download_split_to_temp_file(&storage, &split_filename, &temp_split_path, split_index).await
        .map_err(|e| anyhow!("Split {}: Failed to download to temp file: {}", split_index, e))?;

    debug_log!("💾 Split {}: Downloaded split file to {:?}", split_index, temp_split_path);

    // Extract the downloaded split to the temp directory
    // CRITICAL: For merge operations, use full extraction instead of lazy loading
    debug_log!("🔧 Split {}: MERGE EXTRACTION - Forcing full split extraction (no lazy loading) for merge safety", split_index);
    extract_split_to_directory_impl(&temp_split_path, &temp_extract_path)?;

    } else {
    // Handle local file URLs
    let split_path = if split_url.starts_with("file://") {
        split_url.strip_prefix("file://").unwrap_or(split_url)
    } else {
        split_url
    };

    debug_log!("📂 Split {}: Processing local file: {}", split_index, split_path);

    // Validate split exists
    if !Path::new(split_path).exists() {
        return Err(anyhow!("Split {}: File not found: {}", split_index, split_path));
    }

    // Extract the split to a writable directory
    // CRITICAL: For merge operations, use full extraction instead of lazy loading
    debug_log!("🔧 Split {}: MERGE EXTRACTION - Forcing full split extraction (no lazy loading) for merge safety", split_index);
    extract_split_to_directory_impl(Path::new(split_path), &temp_extract_path)?;
    }

    debug_log!("✅ Split {}: Successfully downloaded and extracted to {:?}", split_index, temp_extract_path);

    Ok(ExtractedSplit {
    temp_dir: temp_extract_dir,
    temp_path: temp_extract_path,
    split_url: split_url.to_string(),
    split_index,
    })
}

/// Creates a shared storage resolver for connection pooling across downloads
/// ✅ QUICKWIT NATIVE: Create storage resolver using Quickwit's native configuration system
/// This replaces custom storage configuration with Quickwit's proven patterns
pub fn create_storage_resolver(config: &InternalMergeConfig) -> Result<StorageResolver> {
    use quickwit_config::{StorageConfigs, StorageConfig, S3StorageConfig, AzureStorageConfig};

    debug_log!("🔧 Creating storage resolver with multi-cloud support");

    let mut storage_configs = Vec::new();

    // AWS S3 configuration
    if let Some(ref aws_config) = config.aws_config {
        let s3_config = S3StorageConfig {
            access_key_id: aws_config.access_key.clone(),
            secret_access_key: aws_config.secret_key.clone(),
            session_token: aws_config.session_token.clone(),
            region: Some(aws_config.region.clone()),
            endpoint: aws_config.endpoint.clone(),
            force_path_style_access: aws_config.force_path_style,
            ..Default::default()
        };
        storage_configs.push(StorageConfig::S3(s3_config));
        debug_log!("   ✅ S3 config added");
    }

    // ✅ NEW: Azure Blob Storage configuration
    if let Some(ref azure_config) = config.azure_config {
        let azure_storage_config = AzureStorageConfig {
            account_name: Some(azure_config.account_name.clone()),
            access_key: azure_config.account_key.clone(),
            bearer_token: azure_config.bearer_token.clone(),
        };
        storage_configs.push(StorageConfig::Azure(azure_storage_config));
        debug_log!("   ✅ Azure config added");
    }

    // Create StorageResolver
    Ok(if !storage_configs.is_empty() {
        let configs = StorageConfigs::new(storage_configs);
        StorageResolver::configured(&configs)
    } else {
        debug_log!("   ℹ️  No cloud config - using global resolver");
        crate::global_cache::GLOBAL_STORAGE_RESOLVER.clone()
    })
}

/// ✅ CORRUPTION FIX: Download split file directly to temp file to avoid memory/network slice issues
/// This eliminates the "line 1, column 2291" errors by using reliable local file operations
pub async fn download_split_to_temp_file(
    storage: &Arc<dyn Storage>,
    split_filename: &str,
    temp_file_path: &Path,
    split_index: usize
) -> Result<()> {
    use tokio::io::AsyncWriteExt;

    debug_log!("🔧 Split {}: Starting direct file download for: {}", split_index, split_filename);

    // Create the output file
    let mut temp_file = tokio::fs::File::create(temp_file_path).await
        .map_err(|e| anyhow!("Split {}: Failed to create temp file {:?}: {}", split_index, temp_file_path, e))?;

    // Use copy_to which streams the data directly to avoid memory issues
    // This bypasses any potential buffer truncation or slice boundary issues
    storage.copy_to(Path::new(split_filename), &mut temp_file).await
        .map_err(|e| anyhow!("Split {}: Failed to copy from storage to temp file: {}", split_index, e))?;

    // Ensure all data is written to disk
    temp_file.flush().await
        .map_err(|e| anyhow!("Split {}: Failed to flush temp file: {}", split_index, e))?;

    // Verify the file was created successfully
    let file_size = tokio::fs::metadata(temp_file_path).await
        .map_err(|e| anyhow!("Split {}: Failed to get temp file metadata: {}", split_index, e))?
        .len();

    debug_log!("✅ Split {}: Successfully downloaded {} bytes to temp file {:?}", split_index, file_size, temp_file_path);

    Ok(())
}
