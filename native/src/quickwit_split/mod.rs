use jni::objects::{JObject, JValue};
use jni::JNIEnv;
use crate::debug_println;
use crate::runtime_manager::QuickwitRuntimeManager;

/// Types for standalone merge binary
pub mod merge_types;

// Re-export types for standalone usage
pub use merge_types::{MergeSplitConfig, SplitMetadata, MergeAwsConfig, SkippedSplit};

// Debug logging macro - controlled by TANTIVY4JAVA_DEBUG environment variable
macro_rules! debug_log {
    ($($arg:tt)*) => {
    debug_println!("DEBUG: {}", format!($($arg)*))
    };
}
use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::ops::RangeInclusive;

use anyhow::{anyhow, Result, Context};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use serde::{Serialize, Deserialize};

// Quickwit imports
use quickwit_storage::{PutPayload, SplitPayloadBuilder};
use quickwit_indexing::open_index;
// write_hotcache replaced by write_hotcache_mmap in split_creation.rs for memory efficiency
use tokio::io::AsyncWriteExt;

use crate::utils::{jstring_to_string, string_to_jstring};

/// Resilient operations for Tantivy index operations that may encounter transient file corruption
/// This addresses issues like "EOF while parsing a string" in .managed.json during concurrent operations
pub mod resilient_ops;

/// JSON field discovery and doc mapping extraction
pub mod json_discovery;

/// Merge ID generation and temp directory registry
pub mod merge_registry;

/// Configuration structures and extraction for merge operations
pub mod merge_config;

/// Split utility and cleanup functions
pub mod split_utils;

/// Temporary directory management and split extraction
pub mod temp_management;

/// Parallel split downloading and extraction
pub mod download;

/// Core merge implementation
pub mod merge_impl;

/// Cloud storage upload and split file creation
pub mod upload;

/// JNI entry points for QuickwitSplit Java class
pub mod jni_functions;

/// Split file creation using Quickwit's SplitPayloadBuilder
pub mod split_creation;

// Re-export public API from merge_config
pub use merge_config::{
    InternalMergeConfig, InternalAwsConfig, InternalAzureConfig,
    extract_merge_config, extract_aws_config, extract_azure_config,
    extract_string_list_from_jobject, get_string_field_value, get_nullable_string_field_value,
};

// Re-export public API from merge_impl
pub use merge_impl::{merge_splits_impl, detect_merge_optimization_settings, calculate_directory_size};

// Re-exports used by merge_impl.rs and other submodules
pub(crate) use upload::{create_merged_split_file, upload_split_to_s3, estimate_peak_memory_usage};

// Re-export from split_creation
pub(crate) use split_creation::{FooterOffsets, create_quickwit_split};

// Internal use - only what's needed in this file
use json_discovery::extract_doc_mapping_from_index;
use split_utils::create_quickwit_tokenizer_manager;

/// Configuration for split conversion passed from Java
#[derive(Debug, Clone)]
struct SplitConfig {
    index_uid: String,
    source_id: String,
    node_id: String,
    doc_mapping_uid: String,
    partition_id: u64,
    time_range_start: Option<DateTime<Utc>>,
    time_range_end: Option<DateTime<Utc>>,
    tags: BTreeSet<String>,
    metadata: HashMap<String, String>,
    // New streaming configuration fields
    streaming_chunk_size: u64,        // 64MB default for optimal I/O
    enable_progress_tracking: bool,   // Enable detailed progress logging
    enable_streaming_io: bool,        // Use streaming I/O instead of read_all()
}

/// Split metadata structure compatible with Quickwit format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuickwitSplitMetadata {
    pub split_id: String,
    pub index_uid: String,
    pub source_id: String,
    pub node_id: String,
    pub doc_mapping_uid: String,
    pub partition_id: u64,
    pub num_docs: usize,
    pub uncompressed_docs_size_in_bytes: u64,
    pub time_range: Option<RangeInclusive<i64>>,
    pub create_timestamp: i64,
    pub maturity: String,  // "Mature" or "Immature"
    pub tags: BTreeSet<String>,
    pub delete_opstamp: u64,
    pub num_merge_ops: usize,

    // Footer offset information for lazy loading optimization
    pub footer_start_offset: Option<u64>,    // Where metadata begins (excludes hotcache)
    pub footer_end_offset: Option<u64>,      // End of file
    pub hotcache_start_offset: Option<u64>,  // Where hotcache begins
    pub hotcache_length: Option<u64>,        // Size of hotcache

    // Doc mapping JSON for SplitSearcher integration
    pub doc_mapping_json: Option<String>,    // JSON representation of the doc mapping

    // Skipped splits for merge operations with parsing failures
    pub skipped_splits: Vec<merge_types::SkippedSplit>,  // Splits that failed with reasons
}

impl SplitConfig {
    fn from_java_object(env: &mut JNIEnv, config_obj: &JObject) -> Result<Self> {
    let index_uid = {
        let jstr = env.call_method(config_obj, "getIndexUid", "()Ljava/lang/String;", &[])?
            .l()?;
        jstring_to_string(env, &jstr.into())?
    };

    let source_id = {
        let jstr = env.call_method(config_obj, "getSourceId", "()Ljava/lang/String;", &[])?
            .l()?;
        jstring_to_string(env, &jstr.into())?
    };

    let node_id = {
        let jstr = env.call_method(config_obj, "getNodeId", "()Ljava/lang/String;", &[])?
            .l()?;
        jstring_to_string(env, &jstr.into())?
    };

    let doc_mapping_uid = {
        let jstr = env.call_method(config_obj, "getDocMappingUid", "()Ljava/lang/String;", &[])?
            .l()?;
        jstring_to_string(env, &jstr.into())?
    };

    let partition_id = env.call_method(config_obj, "getPartitionId", "()J", &[])?
        .j()? as u64;

    // TODO: Full Java object parsing for time ranges, tags, and metadata
    let time_range_start = None;
    let time_range_end = None;
    let tags = BTreeSet::new();
    let metadata = HashMap::new();

    // Extract new streaming configuration fields
    let streaming_chunk_size = env.call_method(config_obj, "getStreamingChunkSize", "()J", &[])?
        .j()? as u64;
    let enable_progress_tracking = env.call_method(config_obj, "isProgressTrackingEnabled", "()Z", &[])?
        .z()?;
    let enable_streaming_io = env.call_method(config_obj, "isStreamingIOEnabled", "()Z", &[])?
        .z()?;

    Ok(SplitConfig {
        index_uid,
        source_id,
        node_id,
        doc_mapping_uid,
        partition_id,
        time_range_start,
        time_range_end,
        tags,
        metadata,
        streaming_chunk_size,
        enable_progress_tracking,
        enable_streaming_io,
    })
    }
}

fn create_split_metadata(config: &SplitConfig, num_docs: usize, uncompressed_docs_size: u64) -> QuickwitSplitMetadata {
    let split_id = Uuid::new_v4().to_string();
    let current_timestamp = Utc::now().timestamp();
    
    QuickwitSplitMetadata {
    split_id,
    index_uid: config.index_uid.clone(),
    source_id: config.source_id.clone(),
    node_id: config.node_id.clone(),
    doc_mapping_uid: config.doc_mapping_uid.clone(),
    partition_id: config.partition_id,
    num_docs,
    uncompressed_docs_size_in_bytes: uncompressed_docs_size,
    time_range: match (config.time_range_start, config.time_range_end) {
        (Some(start), Some(end)) => Some(start.timestamp()..=end.timestamp()),
        _ => None,
    },
    create_timestamp: current_timestamp,
    maturity: "Mature".to_string(),  // Default to mature for created splits
    tags: config.tags.clone(),
    delete_opstamp: 0,
    num_merge_ops: 0,
    
    // Footer offset fields initialized as None, will be set after split creation
    footer_start_offset: None,
    footer_end_offset: None,
    hotcache_start_offset: None,
    hotcache_length: None,
    
    // Doc mapping JSON initialized as None, will be set during extraction
    doc_mapping_json: None,

    // Skipped splits initialized as empty
    skipped_splits: Vec::new(),
    }
}

fn create_java_split_metadata<'a>(env: &mut JNIEnv<'a>, split_metadata: &QuickwitSplitMetadata) -> Result<JObject<'a>> {
    let split_metadata_class = env.find_class("io/indextables/tantivy4java/split/merge/QuickwitSplit$SplitMetadata")?;

    // Create null Instant objects for time ranges (these are optional)
    let time_start_obj = JObject::null();
    let time_end_obj = JObject::null();

    // Create empty HashSet for tags
    let hash_set_class = env.find_class("java/util/HashSet")?;
    let tags_set = env.new_object(&hash_set_class, "()V", &[])?;

    // Add tags to the set if any exist
    for tag in &split_metadata.tags {
        let tag_jstring = string_to_jstring(env, tag)?;
        env.call_method(
            &tags_set,
            "add",
            "(Ljava/lang/Object;)Z",
            &[JValue::Object(&tag_jstring.into())],
        )?;
    }

    // Convert all string fields to JString
    let split_id_jstring = string_to_jstring(env, &split_metadata.split_id)?;
    let index_uid_jstring = string_to_jstring(env, &split_metadata.index_uid)?;
    let source_id_jstring = string_to_jstring(env, &split_metadata.source_id)?;
    let node_id_jstring = string_to_jstring(env, &split_metadata.node_id)?;
    let doc_mapping_uid_jstring = string_to_jstring(env, &split_metadata.doc_mapping_uid)?;
    let maturity_jstring = string_to_jstring(env, &split_metadata.maturity)?;

    // Convert doc_mapping_json to JString (null if not present)
    let doc_mapping_json_jstring = if let Some(ref json) = split_metadata.doc_mapping_json {
        string_to_jstring(env, json)?
    } else {
        JObject::null().into()
    };

    // Create ArrayList for skipped splits with reasons
    let array_list_class = env.find_class("java/util/ArrayList")?;
    let skipped_splits_list = env.new_object(&array_list_class, "()V", &[])?;

    // Find the SkippedSplit class for creating detailed skip information
    let skipped_split_class = env.find_class("io/indextables/tantivy4java/split/merge/QuickwitSplit$SkippedSplit")?;

    // Add skipped splits to the list with their reasons
    for skipped_split in &split_metadata.skipped_splits {
        let url_jstring = string_to_jstring(env, &skipped_split.url)?;
        let reason_jstring = string_to_jstring(env, &skipped_split.reason)?;

        // Create SkippedSplit Java object
        let skipped_split_obj = env.new_object(
            &skipped_split_class,
            "(Ljava/lang/String;Ljava/lang/String;)V",
            &[
                JValue::Object(&url_jstring.into()),
                JValue::Object(&reason_jstring.into()),
            ],
        )?;

        env.call_method(
            &skipped_splits_list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[JValue::Object(&skipped_split_obj)],
        )?;
    }

    // Use the new Quickwit-compatible constructor
    // (String splitId, String indexUid, long partitionId, String sourceId, String nodeId,
    //  long numDocs, long uncompressedSizeBytes, Instant timeRangeStart, Instant timeRangeEnd,
    //  long createTimestamp, String maturity, Set<String> tags, long footerStartOffset,
    //  long footerEndOffset, long deleteOpstamp, int numMergeOps, String docMappingUid, String docMappingJson, List<SkippedSplit> skippedSplits)
    let metadata_obj = env.new_object(
        &split_metadata_class,
        "(Ljava/lang/String;Ljava/lang/String;JLjava/lang/String;Ljava/lang/String;JJLjava/time/Instant;Ljava/time/Instant;JLjava/lang/String;Ljava/util/Set;JJJILjava/lang/String;Ljava/lang/String;Ljava/util/List;)V",
        &[
            JValue::Object(&split_id_jstring.into()),           // splitId
            JValue::Object(&index_uid_jstring.into()),          // indexUid
            JValue::Long(split_metadata.partition_id as i64),   // partitionId
            JValue::Object(&source_id_jstring.into()),          // sourceId
            JValue::Object(&node_id_jstring.into()),            // nodeId
            JValue::Long(split_metadata.num_docs as i64),       // numDocs
            JValue::Long(split_metadata.uncompressed_docs_size_in_bytes as i64), // uncompressedSizeBytes
            JValue::Object(&time_start_obj),                    // timeRangeStart
            JValue::Object(&time_end_obj),                      // timeRangeEnd
            JValue::Long(split_metadata.create_timestamp),     // createTimestamp
            JValue::Object(&maturity_jstring.into()),          // maturity
            JValue::Object(&tags_set),                          // tags
            JValue::Long(split_metadata.footer_start_offset.map(|v| v as i64).unwrap_or(-1)), // footerStartOffset
            JValue::Long(split_metadata.footer_end_offset.map(|v| v as i64).unwrap_or(-1)),   // footerEndOffset
            JValue::Long(split_metadata.delete_opstamp as i64), // deleteOpstamp
            JValue::Int(split_metadata.num_merge_ops as i32),   // numMergeOps
            JValue::Object(&doc_mapping_uid_jstring.into()),    // docMappingUid
            JValue::Object(&doc_mapping_json_jstring.into()),   // docMappingJson
            JValue::Object(&skipped_splits_list),               // skippedSplits
        ]
    )?;

    Ok(metadata_obj)
}



fn convert_index_from_path_impl(index_path: &str, output_path: &str, config: &SplitConfig) -> Result<QuickwitSplitMetadata, anyhow::Error> {
    // ✅ CRITICAL FIX: Use shared global runtime handle to prevent multiple runtime deadlocks
    QuickwitRuntimeManager::global().handle().block_on(convert_index_from_path_impl_async(index_path, output_path, config))
}

/// Determines if an error is a configuration/system error that should not be bypassed
/// Configuration errors indicate problems with credentials, region settings, network connectivity, etc.
/// These should fail the operation rather than being treated as "skipped splits"
fn is_configuration_error(error_msg: &str) -> bool {
    let msg_lower = error_msg.to_lowercase();

    // AWS/S3 configuration errors that should not be bypassed
    if msg_lower.contains("credential") && (msg_lower.contains("missing") || msg_lower.contains("invalid") || msg_lower.contains("expired")) {
        return true;
    }

    if msg_lower.contains("access key") && (msg_lower.contains("missing") || msg_lower.contains("invalid")) {
        return true;
    }

    if msg_lower.contains("secret key") && (msg_lower.contains("missing") || msg_lower.contains("invalid")) {
        return true;
    }

    if msg_lower.contains("region") && (msg_lower.contains("invalid") || msg_lower.contains("unsupported")) {
        return true;
    }

    if msg_lower.contains("endpoint") && msg_lower.contains("invalid") {
        return true;
    }

    // AWS authorization/authentication errors
    if msg_lower.contains("unauthorized") || msg_lower.contains("forbidden") || msg_lower.contains("access denied") {
        return true;
    }

    // Storage resolver creation errors
    if msg_lower.contains("storage resolver") && msg_lower.contains("creation failed") {
        return true;
    }

    // Invalid URL/parameter errors
    if msg_lower.contains("invalid url") || msg_lower.contains("malformed") {
        return true;
    }

    // Network/DNS configuration errors - these indicate system-level issues
    if msg_lower.contains("dns error") || msg_lower.contains("failed to lookup address") {
        return true;
    }

    if msg_lower.contains("hostname not extractable") || msg_lower.contains("nodename nor servname provided") {
        return true;
    }

    if msg_lower.contains("connection refused") || msg_lower.contains("network unreachable") {
        return true;
    }

    // General connection/network errors that indicate configuration issues
    if msg_lower.contains("dispatch failure") || msg_lower.contains("connecterror") {
        return true;
    }

    // Timeout errors often indicate configuration or network issues
    if msg_lower.contains("timeout") || msg_lower.contains("timed out") {
        return true;
    }

    // Bucket-level errors that indicate configuration problems
    if msg_lower.contains("bucket") && (msg_lower.contains("does not exist") || msg_lower.contains("not found")) {
        return true;
    }

    // S3 redirect and endpoint errors - these are configuration issues
    if msg_lower.contains("permanentredirect") {
        return true;
    }

    if msg_lower.contains("must be addressed using the specified endpoint") {
        return true;
    }

    // S3 region mismatch and endpoint configuration errors
    if msg_lower.contains("the bucket you are attempting to access") {
        return true;
    }

    // AWS SDK authentication/configuration errors that appear without credentials
    if msg_lower.contains("service error") && (msg_lower.contains("unhandled") || msg_lower.contains("permanentredirect")) {
        return true;
    }

    // Storage errors - these indicate storage backend configuration issues
    if msg_lower.contains("storage error") {
        return true;
    }

    // Local file system errors that indicate path/configuration issues
    // BUT NOT individual split file missing - that should be treated as graceful skip
    if (msg_lower.contains("not found") || msg_lower.contains("no such file")) &&
       !msg_lower.contains("split") && !msg_lower.contains(".split") {
        return true;
    }

    false  // All other errors are treated as individual split issues and bypassed gracefully
}

/// Legacy function kept for compatibility - now only used for individual split file errors
/// This function is used for split-level error handling decisions
fn is_data_corruption_error(_error_msg: &str) -> bool {
    true  // Individual split file errors are treated as recoverable - bypass gracefully
}

async fn convert_index_from_path_impl_async(index_path: &str, output_path: &str, config: &SplitConfig) -> Result<QuickwitSplitMetadata, anyhow::Error> {
    use tantivy::directory::MmapDirectory;

    debug_log!("🔍 CONVERT: Starting conversion from {} to {}", index_path, output_path);

    // Open the Tantivy index using the actual Quickwit/Tantivy libraries
    let mmap_directory = MmapDirectory::open(index_path)
        .map_err(|e| anyhow!("Failed to open index directory {}: {}", index_path, e))?;
    debug_log!("🔍 CONVERT: Opened MmapDirectory successfully");
    let tokenizer_manager = create_quickwit_tokenizer_manager();
    let tantivy_index = open_index(mmap_directory, &tokenizer_manager)
        .map_err(|e| anyhow!("Failed to open Tantivy index: {}", e))?;
    
    // Get actual document count from the index
    // Important: We need to reload the reader to see any committed changes
    let reader = tantivy_index.reader()
        .map_err(|e| anyhow!("Failed to create index reader: {}", e))?;
    reader.reload()
        .map_err(|e| anyhow!("Failed to reload index reader: {}", e))?;
    let searcher = reader.searcher();
    let doc_count = searcher.num_docs() as i32;

    debug_log!("🔍 CONVERT INDEX: Found {} documents in index at {}", doc_count, index_path);
    
    // Calculate actual index size
    let index_dir = PathBuf::from(index_path);
    let mut total_size = 0u64;
    if let Ok(entries) = std::fs::read_dir(&index_dir) {
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_file() && !path.file_name().unwrap().to_str().unwrap().starts_with('.') {
            if let Ok(metadata) = std::fs::metadata(&path) {
                total_size += metadata.len();
            }
        }
    }
    }
    
    // Extract doc mapping from the index schema
    let doc_mapping_json = extract_doc_mapping_from_index(&tantivy_index)
    .map_err(|e| anyhow!("Failed to extract doc mapping from index: {}", e))?;
    
    // Create split metadata with actual values from the index
    let mut split_metadata = create_split_metadata(config, doc_count as usize, total_size);
    split_metadata.num_docs = doc_count as usize;
    split_metadata.uncompressed_docs_size_in_bytes = total_size;
    split_metadata.doc_mapping_json = Some(doc_mapping_json);
    
    // Use Quickwit's split creation functionality and get footer offsets
    let footer_offsets = create_quickwit_split(&tantivy_index, &index_dir, &PathBuf::from(output_path), &split_metadata, config).await?;
    
    // Update split metadata with footer offsets for lazy loading optimization
    split_metadata.footer_start_offset = Some(footer_offsets.footer_start_offset);
    split_metadata.footer_end_offset = Some(footer_offsets.footer_end_offset);
    split_metadata.hotcache_start_offset = Some(footer_offsets.hotcache_start_offset);
    split_metadata.hotcache_length = Some(footer_offsets.hotcache_length);
    
    Ok(split_metadata)
}
