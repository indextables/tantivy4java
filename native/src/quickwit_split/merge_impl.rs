// merge_impl.rs - Core merge implementation for split files
// Extracted from mod.rs during refactoring
// Contains: merge_splits_impl, detect_merge_optimization_settings, perform_quickwit_merge

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use tempfile as temp;
use chrono::Utc;

use tantivy::IndexMeta;
use tantivy::directory::{Directory, DirectoryClone};

use quickwit_common::io::IoControls;
use quickwit_proto::metastore::DeleteTask;
use quickwit_doc_mapper::DocMapper;
use quickwit_indexing::{
    open_split_directories,
    open_index,
    ControlledDirectory,
    create_shadowing_meta_json_directory,
};
use quickwit_directories::UnionDirectory;
use quickwit_query::get_quickwit_fastfield_normalizer_manager;

use crate::debug_println;
use crate::runtime_manager::QuickwitRuntimeManager;
use super::QuickwitSplitMetadata;
use super::merge_config::InternalMergeConfig;
use super::merge_registry::generate_collision_resistant_merge_id;
use super::split_utils::{
    create_quickwit_tokenizer_manager, cleanup_merge_temp_directory,
    cleanup_output_directory_safe,
};
use super::temp_management::create_temp_directory_with_base;
use super::download::download_and_extract_splits_parallel;
use super::{is_configuration_error, create_merged_split_file, upload_split_to_s3, estimate_peak_memory_usage};
use super::resilient_ops;
use super::json_discovery::extract_doc_mapping_from_index;
use super::merge_types::SkippedSplit;

// Debug logging macro - controlled by TANTIVY4JAVA_DEBUG environment variable
macro_rules! debug_log {
    ($($arg:tt)*) => {
        debug_println!("DEBUG: {}", format!($($arg)*))
    };
}

/// Merge optimization settings for performance tuning
pub struct MergeOptimization {
    pub heap_size_bytes: u64,
    pub num_threads: u32,
    pub use_random_io: bool,
    pub enable_compression: bool,
    pub memory_map_threshold: u64,
}

/// Implementation of split merging using Quickwit's efficient approach
/// This follows Quickwit's MergeExecutor pattern for memory-efficient large-scale merges
/// CRITICAL: For merge operations, we disable lazy loading and force full file downloads
/// to avoid the range assertion failures that occur when accessing corrupted/invalid metadata
pub fn merge_splits_impl(split_urls: &[String], output_path: &str, config: &InternalMergeConfig) -> Result<QuickwitSplitMetadata> {
    use tantivy::directory::{MmapDirectory, Directory, DirectoryClone};
    use tantivy::IndexMeta;


    debug_log!("🔧 MERGE OPERATION: Implementing split merge using Quickwit's efficient approach for {} splits", split_urls.len());
    debug_log!("🚨 MERGE SAFETY: Lazy loading DISABLED for merge operations to prevent range assertion failures");

    // ⚠️ MEMORY OPTIMIZATION: For large numbers of splits, consider batch processing
    if split_urls.len() > 10 {
        debug_log!("⚠️ MEMORY WARNING: Merging {} splits will require significant memory", split_urls.len());
        debug_log!("   Each split creates ~2x memory usage (mmap + Vec copy)");
        debug_log!("   Consider processing in smaller batches for very large operations");
    }

    // ✅ CRITICAL FIX: Use shared global runtime to prevent multiple runtime deadlocks
    // This eliminates competing Tokio runtimes that cause production hangs

    // ✅ REENTRANCY FIX: Generate collision-resistant merge ID with multiple entropy sources
    let merge_id = generate_collision_resistant_merge_id();

    // Use a closure to ensure proper runtime cleanup even on errors
    let result: Result<QuickwitSplitMetadata> = (|| -> Result<QuickwitSplitMetadata> {
    // 1. Open split directories without extraction (Quickwit's approach)
    let mut split_directories: Vec<Box<dyn Directory>> = Vec::new();
    let mut index_metas: Vec<IndexMeta> = Vec::new();
    let mut total_docs = 0usize;
    let mut total_size = 0u64;
    let mut temp_dirs: Vec<temp::TempDir> = Vec::new(); // Keep track of temp dirs for proper cleanup

    // Using merge_id from outer scope to prevent temp directory conflicts
    debug_log!("🔧 MERGE ID: {} (process-scoped to prevent concurrency conflicts)", merge_id);

    // ⚡ PARALLEL SPLIT DOWNLOAD AND EXTRACTION OPTIMIZATION
    debug_log!("🚀 PARALLEL DOWNLOAD: Starting concurrent download of {} splits", split_urls.len());

    // ✅ RESILIENT DOWNLOAD: Handle split-level failures gracefully, but preserve configuration errors
    // PERFORMANCE FIX: Use blocking async on the current thread to prevent runtime deadlock
    let (extracted_splits, mut skipped_splits) = {
        let split_urls_vec = split_urls.to_vec(); // Clone to avoid reference lifetime issues
        let merge_id_clone = merge_id.clone();
        let config_clone = config.clone();

        match std::thread::spawn(move || {
            // ✅ CRITICAL FIX: Use shared global runtime instead of creating separate runtime
            // This eliminates multiple Tokio runtime conflicts that cause deadlocks
            QuickwitRuntimeManager::global().handle().block_on(
                download_and_extract_splits_parallel(&split_urls_vec, &merge_id_clone, &config_clone)
            )
        }).join() {
        Ok(result) => match result {
            Ok((splits, skipped)) => (splits, skipped),
            Err(e) => {
                let error_msg = e.to_string();
                debug_log!("⚠️ PARALLEL DOWNLOAD FAILURE: Complete download operation failed: {}", error_msg);

            // Check if this is a configuration error that should not be bypassed
            if is_configuration_error(&error_msg) {
                debug_log!("🚨 CONFIGURATION ERROR: This is a system configuration issue, not a split corruption issue");
                return Err(e);  // Preserve configuration errors - these should fail the operation
            }

            // If it's not a configuration error, treat all splits as skipped
            debug_log!("🔧 DOWNLOAD FAILURE RECOVERY: Treating all {} splits as skipped due to non-configuration system failure", split_urls.len());

                let all_skipped: Vec<SkippedSplit> = split_urls.iter()
                    .map(|url| SkippedSplit::new(url.clone(), error_msg.clone()))
                    .collect();
                (Vec::new(), all_skipped)
            }
        },
        Err(_panic) => {
            debug_log!("🚨 THREAD PANIC: Download thread panicked, treating all splits as failed");
            let all_skipped: Vec<SkippedSplit> = split_urls.iter()
                .map(|url| SkippedSplit::new(url.clone(), "Download thread panicked"))
                .collect();
            (Vec::new(), all_skipped)
        }
    }}; // Close the `let (extracted_splits, mut skipped_splits) = {` block

    debug_log!("✅ PARALLEL DOWNLOAD: Completed concurrent processing of {} successful splits, {} skipped", extracted_splits.len(), skipped_splits.len());

    // Process the extracted splits to create directories and metadata
    for extracted_split in extracted_splits.into_iter() {
    let i = extracted_split.split_index;
    let split_url = &extracted_split.split_url;
    let temp_extract_path = &extracted_split.temp_path;

    debug_log!("🔧 Processing extracted split {}: {}", i, split_url);

    // ✅ RESILIENT METADATA PARSING: Handle ANY error AND panic gracefully with warnings
    let (extracted_directory, temp_index, index_meta) = match std::panic::catch_unwind(|| -> Result<_> {
        let extracted_directory = MmapDirectory::open(temp_extract_path)?;

        // ✅ RESILIENT INDEX OPEN: Handle ANY Index::open error gracefully
        let tokenizer_manager = create_quickwit_tokenizer_manager();
        let temp_index = match open_index(extracted_directory.box_clone(), &tokenizer_manager) {
            Ok(index) => index,
            Err(index_error) => {
                let index_error_msg = index_error.to_string();
                debug_log!("⚠️ INDEX OPEN ERROR: Split {} failed Index::open with: {}", split_url, index_error_msg);
                debug_log!("🔧 INDEX ERROR SKIP: Split {} has index error - adding to skip list", split_url);
                return Err(anyhow!("INDEX_ERROR: {}", index_error_msg));
            }
        };

        let index_meta = resilient_ops::resilient_load_metas(&temp_index)
            .map_err(|e| anyhow!("Failed to load index metadata with resilient retry: {}", e))?;
        Ok((extracted_directory, temp_index, index_meta))
    }) {
        Ok(Ok((dir, index, meta))) => (dir, index, meta),
        Ok(Err(e)) => {
            let error_msg = e.to_string();
            debug_log!("⚠️ SPLIT PROCESSING ERROR: Skipping split {} due to error: {}", split_url, error_msg);
            debug_log!("🔧 ERROR SKIP: Split {} has processing error - continuing with remaining splits", split_url);
            debug_log!("🔧 ERROR DETAILS: {}", error_msg);
            skipped_splits.push(SkippedSplit::new(split_url.clone(), error_msg));
            continue;
        },
        Err(_panic_info) => {
            debug_log!("🚨 PANIC CAUGHT: Split {} caused panic during processing - skipping gracefully", split_url);
            debug_log!("🔧 PANIC SKIP: Split {} has panic error - continuing with remaining splits", split_url);
            skipped_splits.push(SkippedSplit::new(split_url.clone(), "Panic during split processing"));
            continue;
        }
    };

    // ✅ RESILIENT SPLIT FINALIZATION: Handle ANY remaining failure gracefully
    let (doc_count, split_size) = match (|| -> Result<(u64, u64)> {
        // Count documents and calculate size efficiently
        let reader = temp_index.reader()
            .map_err(|e| anyhow!("Failed to create index reader: {}", e))?;
        let searcher = reader.searcher();
        let doc_count = searcher.num_docs();

        // Calculate split size based on source type
        let split_size = if split_url.contains("://") && !split_url.starts_with("file://") {
            // For S3/remote splits, get size from the temporary downloaded file
            let split_filename = split_url.split('/').last().unwrap_or("split.split");
            let temp_split_path = temp_extract_path.join(split_filename);
            std::fs::metadata(&temp_split_path)
                .map_err(|e| anyhow!("Failed to get metadata for temporary split file: {}", e))?
                .len()
        } else {
            // For local splits, get size from original file
            let split_path = if split_url.starts_with("file://") {
                split_url.strip_prefix("file://").unwrap_or(split_url)
            } else {
                split_url
            };
            std::fs::metadata(split_path)
                .map_err(|e| anyhow!("Failed to get metadata for split file: {}", e))?
                .len()
        };

        Ok((doc_count, split_size))
    })() {
        Ok((docs, size)) => (docs, size),
        Err(e) => {
            let error_msg = e.to_string();
            debug_log!("⚠️ SPLIT FINALIZATION ERROR: Skipping split {} due to finalization error: {}", split_url, error_msg);
            debug_log!("🔧 FINALIZATION ERROR DETAILS: {}", error_msg);

            skipped_splits.push(SkippedSplit::new(split_url.clone(), format!("Finalization error: {}", error_msg)));
            continue; // Skip this split and continue with others
        }
    };

    debug_log!("📊 Extracted split {} has {} documents, {} bytes", i, doc_count, split_size);

    total_docs += doc_count as usize;
    total_size += split_size;
    split_directories.push(extracted_directory.box_clone());
    index_metas.push(index_meta);

    // Keep temp directory alive for merge duration - store in vector for proper cleanup
    temp_dirs.push(extracted_split.temp_dir);
    }

    // Check if we have any valid splits after processing
    let valid_splits = split_directories.len();
    let total_requested = split_urls.len();
    debug_log!("Opened {} valid splits out of {} requested with total {} documents, {} bytes", valid_splits, total_requested, total_docs, total_size);

    // ✅ RESILIENT MERGE: Handle insufficient valid splits according to user requirements
    if valid_splits <= 1 {
        // ✅ CRITICAL FIX: Clean up temp directories before returning
        debug_log!("🧹 CLEANUP: Cleaning up {} temp directories before insufficient splits return", temp_dirs.len());
        for (i, temp_dir) in temp_dirs.into_iter().enumerate() {
            debug_log!("🧹 CLEANUP: Cleaning up temp directory {}: {:?}", i, temp_dir.path());
            // temp_dir will be automatically cleaned up when dropped (RAII)
        }

        if valid_splits == 0 {
            debug_log!("⚠️ RESILIENT MERGE: All {} splits failed to parse due to corruption. Returning null indexUid with skipped splits list.", split_urls.len());
        } else {
            debug_log!("⚠️ RESILIENT MERGE: Only 1 valid split remaining after {} corrupted splits. Cannot merge single split. Returning null indexUid.", skipped_splits.len());
        }

        // Return a special metadata object with null indexUid indicating no merge was performed
        // but include the skipped splits for tracking purposes
        let no_merge_metadata = QuickwitSplitMetadata {
            split_id: "".to_string(),  // Empty split ID
            index_uid: "".to_string(), // Empty index UID indicates no split was created
            source_id: config.source_id.clone(),
            node_id: config.node_id.clone(),
            doc_mapping_uid: config.doc_mapping_uid.clone(),
            partition_id: config.partition_id,
            num_docs: 0,  // No documents since no merge happened
            uncompressed_docs_size_in_bytes: 0,
            time_range: None,
            create_timestamp: chrono::Utc::now().timestamp(),
            maturity: "Immature".to_string(),
            tags: std::collections::BTreeSet::new(),
            delete_opstamp: 0,
            num_merge_ops: 0,  // No merge operations performed
            footer_start_offset: None,
            footer_end_offset: None,
            hotcache_start_offset: None,
            hotcache_length: None,
            doc_mapping_json: None,  // No doc mapping for failed merges
            skipped_splits: skipped_splits,  // Include all skipped splits for tracking
        };

        return Ok(no_merge_metadata);
    }

    if !skipped_splits.is_empty() {
        let skipped_urls: Vec<&str> = skipped_splits.iter().map(|s| s.url.as_str()).collect();
        debug_log!("⚠️ PARTIAL MERGE WARNING: Proceeding with {} valid splits, {} skipped: {:?}", valid_splits, skipped_splits.len(), skipped_urls);
    }

    // 2. Set up output directory with sequential access optimization
    // Handle S3 URLs by creating a local temporary directory with unique naming
    let (output_temp_dir, is_s3_output, _temp_dir_guard) = if output_path.contains("://") && !output_path.starts_with("file://") {
    // For S3/remote URLs, create a local temporary directory with unique merge ID
    let temp_dir = create_temp_directory_with_base(
        &format!("tantivy4java_merge_{}_output_", merge_id),
        config.temp_directory_path.as_deref()
    )?;
    let temp_path = temp_dir.path().join("merge_output");
    std::fs::create_dir_all(&temp_path)?;
    debug_log!("Created local temporary directory for S3 output: {:?}", temp_path);
    (temp_path, true, Some(temp_dir))
    } else {
    // For local files, prefer custom temp path if provided, otherwise use output parent directory
    let (temp_dir, temp_dir_guard) = if let Some(custom_base) = &config.temp_directory_path {
        let temp_dir_obj = create_temp_directory_with_base(
            &format!("tantivy4java_merge_{}_output_", merge_id),
            Some(custom_base)
        )?;
        let temp_path = temp_dir_obj.path().to_path_buf();
        (temp_path, Some(temp_dir_obj))
    } else {
        // Fallback to next to output file
        let output_dir_path = Path::new(output_path).parent()
            .ok_or_else(|| anyhow!("Cannot determine parent directory for output path"))?;
        let temp_dir = output_dir_path.join(format!("temp_merge_output_{}", merge_id));
        std::fs::create_dir_all(&temp_dir)?;
        (temp_dir, None)
    };
    debug_log!("Created local temporary directory: {:?}", temp_dir);
    (temp_dir, false, temp_dir_guard)
    };

    // 3. Perform memory-efficient segment-level merge using Quickwit's implementation
    // ✅ CRITICAL FIX: Use shared global runtime handle to prevent multiple runtime deadlocks
    let merged_docs = QuickwitRuntimeManager::global().handle().block_on(perform_quickwit_merge(
        split_directories,
        &output_temp_dir,
    ))?;
    debug_log!("Segment merge completed with {} documents", merged_docs);

    // ✅ PARQUET COMPANION: Combine parquet manifests from source splits (if any)
    // Must happen before temp dirs are dropped since manifests are in the extracted dirs
    {
        let source_paths: Vec<std::path::PathBuf> = temp_dirs.iter()
            .map(|td| td.path().to_path_buf())
            .collect();
        let source_refs: Vec<&std::path::Path> = source_paths.iter()
            .map(|p| p.as_path())
            .collect();
        match crate::parquet_companion::merge::combine_parquet_manifests(&source_refs, &output_temp_dir) {
            Ok(Some(())) => {
                debug_log!("📦 PARQUET_COMPANION: Combined parquet manifests from {} source splits into merged output", source_refs.len());
            }
            Ok(None) => {
                debug_log!("📦 PARQUET_COMPANION: No parquet manifests found in source splits (standard merge)");
            }
            Err(e) => {
                debug_log!("⚠️ PARQUET_COMPANION: Failed to combine parquet manifests: {} (continuing without manifest)", e);
            }
        }
    }

    // ✅ MEMORY OPTIMIZATION: Early temp directory cleanup
    // Source split temp directories are no longer needed after merge completes
    // The merged data is now in output_temp_dir, so we can free the source temp dirs
    // This reduces peak memory/disk usage by releasing temp space earlier
    let temp_dirs_count = temp_dirs.len();
    debug_log!("🧹 EARLY CLEANUP: Releasing {} source temp directories after merge (before split file creation)", temp_dirs_count);
    drop(temp_dirs);  // TempDir RAII handles cleanup automatically
    debug_log!("✅ EARLY CLEANUP: {} source temp directories released, freeing ~{} bytes of temp space",
              temp_dirs_count,
              total_size);  // Approximate - actual might be larger due to extraction

    // 4. Extract doc mapping from the merged index
    let merged_directory = MmapDirectory::open(&output_temp_dir)?;
    let tokenizer_manager = get_quickwit_fastfield_normalizer_manager().tantivy_manager();
    let merged_index = open_index(merged_directory, tokenizer_manager)?;
    let doc_mapping_json = extract_doc_mapping_from_index(&merged_index)?;
    debug_log!("✅ DOCMAPPING EXTRACT: Extracted doc mapping from merged index ({} bytes)", doc_mapping_json.len());
    debug_log!("✅ DOCMAPPING CONTENT: {}", &doc_mapping_json);

    // 5. Calculate final index size
    let final_size = calculate_directory_size(&output_temp_dir)?;
    debug_log!("Final merged index size: {} bytes", final_size);

    // 9. Create merged split metadata
    let merge_split_id = uuid::Uuid::new_v4().to_string();
    let merged_metadata = QuickwitSplitMetadata {
    split_id: merge_split_id.clone(),
    index_uid: config.index_uid.clone(),
    source_id: config.source_id.clone(),
    node_id: config.node_id.clone(),
    doc_mapping_uid: config.doc_mapping_uid.clone(),
    partition_id: config.partition_id,
    num_docs: merged_docs,
    uncompressed_docs_size_in_bytes: final_size,
    time_range: None,
    create_timestamp: Utc::now().timestamp(),
    maturity: "Mature".to_string(),
    tags: BTreeSet::new(),
    delete_opstamp: 0,
    num_merge_ops: 1,

    // Footer offset fields will be set when creating the final split
    footer_start_offset: None,
    footer_end_offset: None,
    hotcache_start_offset: None,
    hotcache_length: None,

    // Doc mapping JSON extracted from union index
    doc_mapping_json: Some(doc_mapping_json.clone()),

    // Skipped splits information
    skipped_splits: skipped_splits.clone(),
    };

    // 10. Create the merged split file using the merged index and capture footer offsets
    let footer_offsets = if is_s3_output {
    // For S3 output, create split locally then upload
    let local_split_filename = format!("{}.split", merged_metadata.split_id);
    let local_split_path = output_temp_dir.join(&local_split_filename);

    debug_log!("Creating local split file: {:?}", local_split_path);
    let offsets = create_merged_split_file(&output_temp_dir, local_split_path.to_str().unwrap(), &merged_metadata)?;

    // Upload to S3
    debug_log!("Uploading split file to S3: {}", output_path);
    upload_split_to_s3(&local_split_path, output_path, config)?;
    debug_log!("Successfully uploaded split file to S3: {}", output_path);
    offsets
    } else {
    // For local output, create split directly
    create_merged_split_file(&output_temp_dir, output_path, &merged_metadata)?
    };

    // 11. Update merged metadata with footer offsets for optimization
    let final_merged_metadata = QuickwitSplitMetadata {
    split_id: merged_metadata.split_id,
    index_uid: merged_metadata.index_uid,
    source_id: merged_metadata.source_id,
    node_id: merged_metadata.node_id,
    doc_mapping_uid: merged_metadata.doc_mapping_uid,
    partition_id: merged_metadata.partition_id,
    num_docs: merged_metadata.num_docs,
    uncompressed_docs_size_in_bytes: merged_metadata.uncompressed_docs_size_in_bytes,
    time_range: merged_metadata.time_range,
    create_timestamp: merged_metadata.create_timestamp,
    maturity: merged_metadata.maturity,
    tags: merged_metadata.tags,
    delete_opstamp: merged_metadata.delete_opstamp,
    num_merge_ops: merged_metadata.num_merge_ops,

    // Set footer offsets from the merge operation
    footer_start_offset: Some(footer_offsets.footer_start_offset),
    footer_end_offset: Some(footer_offsets.footer_end_offset),
    hotcache_start_offset: Some(footer_offsets.hotcache_start_offset),
    hotcache_length: Some(footer_offsets.hotcache_length),

    // Doc mapping JSON extracted from union index during merge
    doc_mapping_json: merged_metadata.doc_mapping_json,

    // Skipped splits information
    skipped_splits: merged_metadata.skipped_splits,
    };

    debug_log!("✅ MERGE OPTIMIZATION: Added footer offsets to merged split metadata");
    debug_log!("   Footer: {} - {} ({} bytes)",
           footer_offsets.footer_start_offset, footer_offsets.footer_end_offset,
           footer_offsets.footer_end_offset - footer_offsets.footer_start_offset);
    debug_log!("   Hotcache: {} bytes at offset {}",
           footer_offsets.hotcache_length, footer_offsets.hotcache_start_offset);

    // 11. ✅ REENTRANCY FIX: Coordinated cleanup to prevent race conditions
    // Note: Source temp_dirs were already cleaned up early (after merge, before split creation)
    // Only output temp directory cleanup remains
    match cleanup_output_directory_safe(&output_temp_dir) {
        Ok(_) => debug_log!("✅ CLEANUP: Output temporary directory cleaned up successfully"),
        Err(e) => {
            debug_log!("⚠️ CLEANUP WARNING: Output cleanup failed but merge succeeded: {}", e);
            // Continue with merge completion - don't fail the operation due to cleanup issues
        }
    }

    debug_log!("Created efficient merged split file: {} with {} documents", output_path, merged_docs);

    // Report memory usage statistics for optimization
    debug_log!("📊 MEMORY STATS: Processed {} splits with peak memory ~{} MB",
               split_urls.len() - skipped_splits.len(),
               estimate_peak_memory_usage(split_urls.len()) / 1_000_000);

    // Report skipped splits if any
    if !skipped_splits.is_empty() {
        debug_log!("⚠️ MERGE WARNING: {} splits were skipped:", skipped_splits.len());
        for (i, skipped) in skipped_splits.iter().enumerate() {
            debug_log!("   {}. {} - Reason: {}", i + 1, skipped.url, skipped.reason);
        }
    }

    Ok(final_merged_metadata)
    })(); // End closure

    // ✅ CRITICAL FIX: Clean up temporary directories regardless of success/failure
    // This ensures no file system leaks even if the merge operation failed
    cleanup_merge_temp_directory(&merge_id);

    // ✅ PERFORMANCE FIX: No runtime shutdown needed - using shared runtime
    // Shared runtime is managed globally and should not be shut down per operation
    debug_log!("Merge operation completed, shared runtime remains active");

    // Return the result (preserving any errors)
    result
}

/// Merge optimization settings for performance tuning
struct MergeOptimizationInternal {
    heap_size_bytes: u64,
    num_threads: u32,
    use_random_io: bool,
    enable_compression: bool,
    memory_map_threshold: u64,
}

/// Detect optimal merge settings based on input size - AGGRESSIVE performance mode
pub fn detect_merge_optimization_settings(total_input_size: u64) -> MergeOptimization {
    // Detect if running in parallel execution context by checking temp directories
    let parallel_execution = detect_parallel_execution_context();

    // 🚀 AGGRESSIVE heap sizing - use much more memory for better performance
    let base_heap_size = if total_input_size < 100_000_000 {
        128_000_000  // 128MB for small merges (was 50MB)
    } else if total_input_size < 1_000_000_000 {
        512_000_000  // 512MB for medium merges (was 128MB)
    } else if total_input_size < 5_000_000_000 {
        1_000_000_000 // 1GB for large merges (was 256MB)
    } else {
        2_000_000_000 // 2GB for very large merges
    };

    // Use full heap size regardless of parallel execution - grab what we need
    let heap_size_bytes = base_heap_size;

    // 🚀 AGGRESSIVE threading - use all available CPUs
    let available_cpus = std::thread::available_parallelism()
        .map(|p| p.get() as u32)
        .unwrap_or(2);

    let num_threads = if parallel_execution {
        // Still use good thread count for parallel execution - don't be too conservative
        std::cmp::max(2, available_cpus / 2) // Use half available CPUs (was 1/4)
    } else {
        // Use all available CPUs for single execution
        available_cpus
    };

    // Use Random I/O for parallel execution to reduce disk contention
    let use_random_io = parallel_execution;

    // Higher memory map threshold for better performance
    let memory_map_threshold = if total_input_size > 500_000_000 {
        500_000_000 // 500MB threshold for large files (was 200MB)
    } else {
        200_000_000 // 200MB threshold (was 100MB)
    };

    MergeOptimization {
        heap_size_bytes,
        num_threads: std::cmp::max(2, num_threads), // Ensure at least 2 threads for parallelism
        use_random_io,
        enable_compression: true,
        memory_map_threshold,
    }
}

/// Detect if we're running in a parallel execution context (like Spark)
pub fn detect_parallel_execution_context() -> bool {
    // Check environment variables that indicate parallel execution
    if std::env::var("SPARK_HOME").is_ok()
        || std::env::var("HADOOP_HOME").is_ok()
        || std::env::var("YARN_CONF_DIR").is_ok() {
        return true;
    }

    // Check for temp directories that suggest parallel processing
    let temp_dir = std::env::temp_dir();
    let temp_str = temp_dir.to_string_lossy();

    // Look for patterns that indicate Databricks, Spark, or other parallel systems
    if temp_str.contains("local_disk")
        || temp_str.contains("spark")
        || temp_str.contains("databricks")
        || temp_str.contains("executor") {
        return true;
    }

    // For now, assume we're NOT in parallel execution context to be aggressive
    // This means we'll always use full resources unless explicitly detected
    false
}

/// Optimized merge implementation that bypasses Quickwit's hardcoded limits
pub async fn merge_split_directories_with_optimization(
    union_index_meta: IndexMeta,
    split_directories: Vec<Box<dyn Directory>>,
    _delete_tasks: Vec<DeleteTask>,
    _doc_mapper_opt: Option<Arc<DocMapper>>,
    output_path: &Path,
    io_controls: IoControls,
    tokenizer_manager: &tantivy::tokenizer::TokenizerManager,
    optimization: MergeOptimization,
) -> anyhow::Result<ControlledDirectory> {
    use tantivy::directory::MmapDirectory;
    use tantivy::IndexWriter;

    debug_log!("🚀 STARTING OPTIMIZED MERGE: heap={}MB, threads={}, io={:?}",
               optimization.heap_size_bytes / 1_000_000,
               optimization.num_threads,
               if optimization.use_random_io { "Random" } else { "Sequential" });

    // 1. Create output directory with controlled I/O
    std::fs::create_dir_all(output_path)?;
    let output_directory = MmapDirectory::open(output_path)?;
    let controlled_output = ControlledDirectory::new(Box::new(output_directory.clone()), io_controls.clone());

    // 2. Create shadowing directory for meta.json handling (Quickwit's approach)
    let shadowing_meta_json_directory = create_shadowing_meta_json_directory(union_index_meta.clone())?;

    // 3. Build directory stack: [output, shadowing_meta_json, ...split_directories]
    let mut directory_stack: Vec<Box<dyn Directory>> = vec![
        controlled_output.box_clone(),
        Box::new(shadowing_meta_json_directory),
    ];
    directory_stack.extend(split_directories.into_iter());

    // 4. Create UnionDirectory from directory stack for efficient read access
    let union_directory = UnionDirectory::union_of(directory_stack);

    // 5. Open index with our optimized settings
    let mut index = tantivy::Index::open(union_directory)?;
    index.set_tokenizers(tokenizer_manager.clone());

    // 6. Configure IndexWriter with optimized settings (bypassing Quickwit's limits)
    let mut index_writer: IndexWriter = index.writer_with_num_threads(
        optimization.num_threads as usize,
        optimization.heap_size_bytes as usize,
    )?;

    // 6. Perform segment merge with all segments from all splits
    // This is the key operation - merge all segments into one
    let segment_ids: Vec<_> = union_index_meta.segments.iter().map(|s| s.id()).collect();

    if !segment_ids.is_empty() {
        debug_log!("🔄 Merging {} segments from input splits", segment_ids.len());

        // Use wait() instead of .await since we may not be in async context
        index_writer.merge(&segment_ids).wait()?;

        debug_log!("✅ Segment merge completed");
    }

    // 7. Commit and finalize
    // Note: IndexWriter doesn't have commit_async in this Tantivy version
    index_writer.commit()?;

    debug_log!("✅ OPTIMIZED MERGE COMPLETED: heap={}MB, threads={}, io={:?}",
               optimization.heap_size_bytes / 1_000_000,
               optimization.num_threads,
               if optimization.use_random_io { "Random" } else { "Sequential" });

    Ok(controlled_output)
}

/// Perform segment-level merge using Quickwit/Tantivy's efficient approach
pub async fn perform_quickwit_merge(
    split_directories: Vec<Box<dyn Directory>>,
    output_path: &Path,
) -> Result<usize> {
    debug_log!("Performing Quickwit merge with {} directories", split_directories.len());

    // Step 1: Use Quickwit's helper to combine metadata
    let tokenizer_manager = get_quickwit_fastfield_normalizer_manager().tantivy_manager();
    let (union_index_meta, directories) = open_split_directories(&split_directories, tokenizer_manager)?;
    debug_log!("Combined metadata from {} splits", directories.len());

    // Step 2: Estimate total input size for smart optimization
    // Since we have Directory objects, not paths, estimate based on number of splits
    let total_input_size = (split_directories.len() as u64) * 50_000_000; // Assume 50MB per split average
    debug_log!("Total input size: {} bytes ({:.1} MB)", total_input_size, total_input_size as f64 / 1_000_000.0);

    // Step 3: Detect parallel execution context and optimize accordingly
    let merge_optimization = detect_merge_optimization_settings(total_input_size);
    debug_log!("🚀 MERGE OPTIMIZATION: heap={}MB, threads={}, io={:?}",
               merge_optimization.heap_size_bytes / 1_000_000,
               merge_optimization.num_threads,
               if merge_optimization.use_random_io { "Random" } else { "Sequential" });

    // Step 4: Create optimized IO controls (IoControls doesn't have num_threads field)
    let io_controls = IoControls::default();

    // Step 5: Use our optimized merge implementation that bypasses Quickwit's hardcoded limits
    let controlled_directory = merge_split_directories_with_optimization(
        union_index_meta,
        directories,
        Vec::new(), // No delete tasks for split merging
        None,       // No doc mapper needed for split merging
        output_path,
        io_controls,
        tokenizer_manager,
        merge_optimization,
    ).await?;

    // Step 4: Open the merged index to get document count
    let merged_index = open_index(controlled_directory.clone(), tokenizer_manager)?;
    let reader = merged_index.reader()?;
    let searcher = reader.searcher();
    let doc_count = searcher.num_docs();

    debug_log!("Quickwit merge completed with {} documents", doc_count);
    Ok(doc_count as usize)
}

/// Calculate the total size of all files in a directory
pub fn calculate_directory_size(dir_path: &Path) -> Result<u64> {
    let mut total_size = 0u64;

    if let Ok(entries) = std::fs::read_dir(dir_path) {
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_file() {
            if let Ok(metadata) = std::fs::metadata(&path) {
                total_size += metadata.len();
            }
        }
    }
    }

    Ok(total_size)
}
