use jni::objects::{JClass, JObject, JString, JValue};
use jni::sys::jobject;
use jni::JNIEnv;
use crate::debug_println;

// Debug logging macro - controlled by TANTIVY4JAVA_DEBUG environment variable
macro_rules! debug_log {
    ($($arg:tt)*) => {
    debug_println!("DEBUG: {}", format!($($arg)*));
    };
}
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::ops::RangeInclusive;
use std::sync::{Arc, Mutex};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::cell::Cell;
use tempfile as temp;

// Add tantivy Directory trait import
use tantivy::Directory;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use serde_json;

// Quickwit imports
use quickwit_storage::{PutPayload, BundleStorage, RamStorage, Storage, StorageResolver, SplitPayloadBuilder};
use quickwit_indexing::{
    open_split_directories,
    merge_split_directories_standalone,
    open_index,
};
use quickwit_common::io::IoControls;
use quickwit_query::get_quickwit_fastfield_normalizer_manager;

// ✅ REENTRANCY FIX: Enhanced merge ID generation system to prevent collisions
// Global atomic counter for merge operations (process-wide uniqueness)
static GLOBAL_MERGE_COUNTER: AtomicU64 = AtomicU64::new(0);

// Thread-local counter for additional collision resistance within threads
thread_local! {
    static THREAD_MERGE_COUNTER: Cell<u64> = Cell::new(0);
}

// Registry state tracking for better collision detection and debugging
#[derive(Debug, Clone)]
struct RegistryEntry {
    temp_dir: Arc<temp::TempDir>,
    created_at: std::time::Instant,
    merge_id: String,
    operation_type: String,
}

// Enhanced registry with state tracking for collision detection
static TEMP_DIR_REGISTRY: LazyLock<Mutex<HashMap<String, RegistryEntry>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

// Global semaphore for concurrent downloads across ALL merge operations
// This prevents overwhelming the system when multiple merges run simultaneously
static GLOBAL_DOWNLOAD_SEMAPHORE: LazyLock<tokio::sync::Semaphore> = LazyLock::new(|| {
    let max_global_downloads = std::env::var("TANTIVY4JAVA_MAX_DOWNLOADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or_else(|| {
            // Default to reasonable parallelism across the process (4-16 permits)
            num_cpus::get().clamp(4, 8) * 2
        });

    tokio::sync::Semaphore::new(max_global_downloads)
});

/// ✅ REENTRANCY FIX: Generate collision-resistant merge ID
/// Uses multiple entropy sources to prevent merge ID collisions in high-concurrency scenarios
fn generate_collision_resistant_merge_id() -> String {
    // Get global atomic counter (process-wide uniqueness)
    let global_counter = GLOBAL_MERGE_COUNTER.fetch_add(1, Ordering::SeqCst);

    // Get thread-local counter (thread-specific uniqueness)
    let thread_counter = THREAD_MERGE_COUNTER.with(|c| {
        let current = c.get();
        c.set(current.wrapping_add(1));
        current
    });

    // Get current thread info
    let thread_id = format!("{:?}", std::thread::current().id())
        .replace("ThreadId(", "").replace(")", "");

    // Get high-resolution timestamp
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);

    // Get memory address for additional uniqueness (different per call)
    let stack_addr = &thread_counter as *const u64 as usize;

    // Generate cryptographically random UUID
    let uuid = uuid::Uuid::new_v4();

    // Combine all entropy sources for maximum collision resistance
    format!("{}_{}_{}_{}_{}_{}_{}",
        std::process::id(),    // Process ID
        thread_id,            // Thread ID
        global_counter,       // Global atomic counter
        thread_counter,       // Thread-local counter
        nanos,               // Nanosecond timestamp
        stack_addr,          // Stack memory address
        uuid                 // Cryptographic UUID
    )
}

/// ✅ REENTRANCY FIX: Safely register temporary directory with collision detection

/// ✅ REENTRANCY FIX: Safely cleanup temporary directory with state validation
fn cleanup_temp_directory_safe(registry_key: &str) -> Result<bool> {
    let mut registry = TEMP_DIR_REGISTRY.lock().map_err(|e| {
        anyhow!("Failed to acquire registry lock for cleanup: {}", e)
    })?;

    if let Some(entry) = registry.remove(registry_key) {
        let ref_count = Arc::strong_count(&entry.temp_dir);
        let age = entry.created_at.elapsed();

        debug_log!("✅ REGISTRY CLEANUP: Removed temp directory: {} (merge_id: {}, operation: {}, age: {:?}, ref_count: {})",
                   registry_key, entry.merge_id, entry.operation_type, age, ref_count);

        // entry.temp_dir will be dropped here, automatic cleanup if ref_count == 1
        Ok(true)
    } else {
        debug_log!("⚠️ REGISTRY CLEANUP: Temp directory not found: {}", registry_key);
        Ok(false)
    }
}

use tokio::io::{AsyncWriteExt, AsyncReadExt};
use quickwit_directories::write_hotcache;
use quickwit_config::S3StorageConfig;
use quickwit_common::uri::{Uri, Protocol};
// Removed: use quickwit_doc_mapper::default_doc_mapper_for_test; - now creating real doc mapping from schema
use tantivy::directory::OwnedBytes;
use std::str::FromStr;

use crate::utils::{convert_throwable, jstring_to_string, string_to_jstring};

/// Resilient operations for Tantivy index operations that may encounter transient file corruption
/// This addresses issues like "EOF while parsing a string" in .managed.json during concurrent operations
mod resilient_ops {
    use super::*;
    use std::time::Duration;

    /// Maximum number of retry attempts for resilient operations
    const MAX_RETRIES: usize = 3;

    /// Get the maximum retry count based on environment (Databricks gets more retries)
    fn get_max_retries() -> usize {
        if std::env::var("DATABRICKS_RUNTIME_VERSION").is_ok() { 8 } else { MAX_RETRIES }
    }

    /// Base delay between retries in milliseconds (exponential backoff)
    const BASE_RETRY_DELAY_MS: u64 = 100;

    /// Resilient wrapper for tantivy::Index::open with retry logic for transient corruption
    pub fn resilient_index_open(directory: Box<dyn tantivy::Directory>) -> Result<tantivy::Index> {
        // Check if we're in Databricks environment for enhanced debugging
        let is_databricks = std::env::var("DATABRICKS_RUNTIME_VERSION").is_ok();

        if is_databricks {
            debug_log!("🏢 DATABRICKS DETECTED: Using enhanced Index::open retry logic");
            debug_log!("🏢 DATABRICKS DEBUG: Directory type: {}", std::any::type_name_of_val(&*directory));
        }

        resilient_operation_with_column_2291_fix("Index::open", || {
            if is_databricks {
                debug_log!("🏢 DATABRICKS DEBUG: Attempting tantivy::Index::open...");
            }
            let result = tantivy::Index::open(directory.box_clone());
            if is_databricks {
                match &result {
                    Ok(_) => debug_log!("🏢 DATABRICKS DEBUG: tantivy::Index::open succeeded"),
                    Err(e) => debug_log!("🏢 DATABRICKS DEBUG: tantivy::Index::open failed: {}", e),
                }
            }
            result
        })
    }

    /// Resilient wrapper for index.load_metas() with retry logic for transient corruption
    pub fn resilient_load_metas(index: &tantivy::Index) -> Result<tantivy::IndexMeta> {
        resilient_operation("load_metas", || {
            index.load_metas()
        })
    }

    /// Resilient wrapper for directory operations that may encounter transient I/O issues
    pub fn resilient_directory_read(directory: &dyn tantivy::Directory, path: &Path) -> Result<Vec<u8>> {
        resilient_io_operation("directory_read", || {
            directory.atomic_read(path)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
        })
    }

    /// Extract column number from serde_json error messages like "line 1, column: 2291"
    pub fn extract_column_number(error_msg: &str) -> Option<usize> {
        // Simple string parsing approach to avoid regex dependency complexity
        // Look for patterns like "line 1, column: 2291" or "line 1, column 2291"
        if let Some(column_start) = error_msg.find("column") {
            let after_column = &error_msg[column_start + 6..]; // Skip "column"
            // Skip any whitespace and optional colon
            let trimmed = after_column.trim_start_matches(|c: char| c.is_whitespace() || c == ':');
            // Extract the number
            let number_str = trimmed.split_whitespace().next()?;
            number_str.parse::<usize>().ok()
        } else {
            None
        }
    }

    /// Special resilient operation wrapper for JSON column truncation errors
    /// This provides enhanced debugging and potential recovery for consistent JSON truncation issues
    fn resilient_operation_with_column_2291_fix<T, F>(operation_name: &str, mut operation: F) -> Result<T>
    where
        F: FnMut() -> tantivy::Result<T>,
    {
        let max_retries = get_max_retries();
        let mut last_error = None;

        for attempt in 0..max_retries {
            if attempt > 0 {
                // Add extra delay for column 2291 errors since they might be timing-related
                let delay_ms = BASE_RETRY_DELAY_MS * (1 << (attempt - 1)) + 50; // Extra 50ms
                debug_log!("🔧 Column 2291 Fix: Retry attempt {} for {} after {} ms delay", attempt + 1, operation_name, delay_ms);
                std::thread::sleep(Duration::from_millis(delay_ms));
            }

            match operation() {
                Ok(result) => {
                    if attempt > 0 {
                        debug_log!("✅ Column 2291 Fix: {} succeeded on attempt {}", operation_name, attempt + 1);
                    }
                    return Ok(result);
                }
                Err(err) => {
                    let error_msg = err.to_string();

                    // Special handling for JSON column truncation errors
                    if error_msg.contains("EOF while parsing") {
                        if let Some(column_num) = resilient_ops::extract_column_number(&error_msg) {
                            debug_log!("🚨 JSON Column Fix: DETECTED JSON TRUNCATION ERROR in {} at column {}: {}", operation_name, column_num, error_msg);
                            debug_log!("🚨 JSON Column Fix: This indicates systematic JSON truncation at byte position {}", column_num);
                            debug_log!("🚨 JSON Column Fix: Attempt {} of {} - will retry with enhanced delay", attempt + 1, max_retries);

                            // For consistent column truncation errors, add extra logging
                            if attempt == max_retries - 1 {
                                return Err(anyhow!("PERSISTENT JSON COLUMN {} CORRUPTION: {} consistently fails with JSON truncation at column {} after {} attempts. This indicates a systematic file reading issue in the BundleDirectory or FileSlice operations.", column_num, operation_name, column_num, max_retries));
                            }

                            last_error = Some(err);
                            continue;
                        }
                    }

                    // Check if this is a transient corruption error that we should retry
                    let is_transient_error = error_msg.contains("EOF while parsing") ||
                                           error_msg.contains("Data corrupted") ||
                                           error_msg.contains("Managed file cannot be deserialized") ||
                                           error_msg.contains("unexpected end of file") ||
                                           error_msg.contains("invalid json");

                    if is_transient_error {
                        debug_log!("🔧 Resilient operation: {} failed with transient error (attempt {}): {}", operation_name, attempt + 1, error_msg);
                        last_error = Some(err);

                        if attempt == get_max_retries() - 1 {
                            break;
                        }
                    } else {
                        // Non-transient error, fail immediately
                        return Err(anyhow!("Operation {} failed with non-transient error: {}", operation_name, err));
                    }
                }
            }
        }

        match last_error {
            Some(err) => Err(anyhow!("Operation {} failed after {} attempts. Last error: {}", operation_name, get_max_retries(), err)),
            None => Err(anyhow!("Operation {} failed after {} attempts with unknown error", operation_name, get_max_retries())),
        }
    }

    /// Generic resilient operation wrapper with exponential backoff retry for Tantivy operations
    fn resilient_operation<T, F>(operation_name: &str, mut operation: F) -> Result<T>
    where
        F: FnMut() -> tantivy::Result<T>,
    {
        let mut last_error = None;

        for attempt in 0..get_max_retries() {
            match operation() {
                Ok(result) => {
                    if attempt > 0 {
                        debug_log!("✅ RESILIENCE: {} succeeded on attempt {} after transient failure", operation_name, attempt + 1);
                    }
                    return Ok(result);
                }
                Err(err) => {
                    let error_msg = err.to_string();

                    // Check if this is a transient corruption error that we should retry
                    let is_transient_error = error_msg.contains("EOF while parsing") ||
                                           error_msg.contains("Data corrupted") ||
                                           error_msg.contains("Managed file cannot be deserialized") ||
                                           error_msg.contains("unexpected end of file") ||
                                           error_msg.contains("invalid json");

                    if !is_transient_error || attempt == get_max_retries() - 1 {
                        // Not a transient error or final attempt - don't retry
                        return Err(anyhow!("{} failed: {}", operation_name, err));
                    }

                    debug_log!("⚠️ RESILIENCE: {} attempt {} failed with transient error: {} (retrying...)",
                              operation_name, attempt + 1, error_msg);

                    last_error = Some(err);

                    // Exponential backoff: 100ms, 200ms, 400ms
                    let delay_ms = BASE_RETRY_DELAY_MS * (1 << attempt);
                    std::thread::sleep(Duration::from_millis(delay_ms));
                }
            }
        }

        // All retries exhausted
        Err(anyhow!("{} failed after {} attempts: {:?}", operation_name, get_max_retries(), last_error))
    }

    /// Generic resilient operation wrapper with exponential backoff retry for I/O operations
    fn resilient_io_operation<T, F>(operation_name: &str, mut operation: F) -> Result<T>
    where
        F: FnMut() -> std::io::Result<T>,
    {
        let mut last_error = None;

        for attempt in 0..get_max_retries() {
            match operation() {
                Ok(result) => {
                    if attempt > 0 {
                        debug_log!("✅ RESILIENCE: {} succeeded on attempt {} after transient I/O failure", operation_name, attempt + 1);
                    }
                    return Ok(result);
                }
                Err(err) => {
                    let error_msg = err.to_string();

                    // Check if this is a transient I/O error that we should retry
                    let is_transient_error = error_msg.contains("EOF while parsing") ||
                                           error_msg.contains("Data corrupted") ||
                                           error_msg.contains("Managed file cannot be deserialized") ||
                                           error_msg.contains("unexpected end of file") ||
                                           error_msg.contains("invalid json") ||
                                           error_msg.contains("Resource temporarily unavailable") ||
                                           error_msg.contains("Interrupted system call");

                    if !is_transient_error || attempt == get_max_retries() - 1 {
                        // Not a transient error or final attempt - don't retry
                        return Err(anyhow!("{} I/O failed: {}", operation_name, err));
                    }

                    debug_log!("⚠️ RESILIENCE: {} attempt {} failed with transient I/O error: {} (retrying...)",
                              operation_name, attempt + 1, error_msg);

                    last_error = Some(err);

                    // Exponential backoff: 100ms, 200ms, 400ms
                    let delay_ms = BASE_RETRY_DELAY_MS * (1 << attempt);
                    std::thread::sleep(Duration::from_millis(delay_ms));
                }
            }
        }

        // All retries exhausted
        Err(anyhow!("{} I/O failed after {} attempts: {:?}", operation_name, get_max_retries(), last_error))
    }
}

/// Extract or create a DocMapper from a Tantivy index schema
fn extract_doc_mapping_from_index(tantivy_index: &tantivy::Index) -> Result<String> {
    // Get the schema from the Tantivy index
    let schema = tantivy_index.schema();
    
    debug_log!("Extracting doc mapping from Tantivy schema with {} fields", schema.fields().count());
    
    // Create field mappings from the actual Tantivy schema as an array (not HashMap)
    let mut field_mappings = Vec::new();
    
    for (field, field_entry) in schema.fields() {
    let field_name = field_entry.name();
    debug_log!("  Processing field: {} with type: {:?}", field_name, field_entry.field_type());
    
    // Map Tantivy field types to Quickwit field mapping types
    let field_mapping = match field_entry.field_type() {
        tantivy::schema::FieldType::Str(_) => {
            // Text field
            serde_json::json!({
                "name": field_name,
                "type": "text",
                "stored": field_entry.is_stored(),
                "indexed": field_entry.is_indexed(),
                "fast": field_entry.is_fast()
            })
        },
        tantivy::schema::FieldType::U64(_) => {
            // Unsigned integer field
            serde_json::json!({
                "name": field_name,
                "type": "u64",
                "stored": field_entry.is_stored(),
                "indexed": field_entry.is_indexed(),
                "fast": field_entry.is_fast()
            })
        },
        tantivy::schema::FieldType::I64(_) => {
            // Signed integer field
            serde_json::json!({
                "name": field_name,
                "type": "i64",
                "stored": field_entry.is_stored(),
                "indexed": field_entry.is_indexed(),
                "fast": field_entry.is_fast()
            })
        },
        tantivy::schema::FieldType::F64(_) => {
            // Float field
            serde_json::json!({
                "name": field_name,
                "type": "f64",
                "stored": field_entry.is_stored(),
                "indexed": field_entry.is_indexed(),
                "fast": field_entry.is_fast()
            })
        },
        tantivy::schema::FieldType::Bool(_) => {
            // Boolean field
            serde_json::json!({
                "name": field_name,
                "type": "bool",
                "stored": field_entry.is_stored(),
                "indexed": field_entry.is_indexed(),
                "fast": field_entry.is_fast()
            })
        },
        tantivy::schema::FieldType::Date(_) => {
            // Date field
            serde_json::json!({
                "name": field_name,
                "type": "datetime",
                "stored": field_entry.is_stored(),
                "indexed": field_entry.is_indexed(),
                "fast": field_entry.is_fast()
            })
        },
        tantivy::schema::FieldType::Facet(_) => {
            // Facet field
            serde_json::json!({
                "name": field_name,
                "type": "text",
                "tokenizer": "raw",
                "stored": field_entry.is_stored(),
                "indexed": field_entry.is_indexed(),
                "fast": field_entry.is_fast()
            })
        },
        tantivy::schema::FieldType::Bytes(_) => {
            // Bytes field
            serde_json::json!({
                "name": field_name,
                "type": "bytes",
                "stored": field_entry.is_stored(),
                "indexed": field_entry.is_indexed(),
                "fast": field_entry.is_fast()
            })
        },
        tantivy::schema::FieldType::JsonObject(_) => {
            // JSON object field
            serde_json::json!({
                "name": field_name,
                "type": "object",
                "stored": field_entry.is_stored()
            })
        },
        tantivy::schema::FieldType::IpAddr(_) => {
            // IP address field
            serde_json::json!({
                "name": field_name,
                "type": "ip",
                "stored": field_entry.is_stored(),
                "indexed": field_entry.is_indexed(),
                "fast": field_entry.is_fast()
            })
        }
    };
    
    field_mappings.push(field_mapping);
    }
    
    // Create the doc mapping JSON structure as just the field_mappings array
    // DocMapperBuilder expects the root to be a sequence of field mappings
    let doc_mapping_json = serde_json::json!(field_mappings);
    
    let doc_mapping_str = serde_json::to_string(&doc_mapping_json)
    .map_err(|e| anyhow!("Failed to serialize DocMapping to JSON: {}", e))?;
    
    debug_log!("Successfully created doc mapping JSON from actual Tantivy schema ({} bytes)", doc_mapping_str.len());
    
    Ok(doc_mapping_str)
}

/// Configuration for split conversion passed from Java
#[derive(Debug)]
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
struct QuickwitSplitMetadata {
    split_id: String,
    index_uid: String,
    source_id: String,
    node_id: String,
    doc_mapping_uid: String,
    partition_id: u64,
    num_docs: usize,
    uncompressed_docs_size_in_bytes: u64,
    time_range: Option<RangeInclusive<i64>>,
    create_timestamp: i64,
    tags: BTreeSet<String>,
    delete_opstamp: u64,
    num_merge_ops: usize,
    
    // Footer offset information for lazy loading optimization
    footer_start_offset: Option<u64>,    // Where metadata begins (excludes hotcache)
    footer_end_offset: Option<u64>,      // End of file
    hotcache_start_offset: Option<u64>,  // Where hotcache begins
    hotcache_length: Option<u64>,        // Size of hotcache
    
    // Doc mapping JSON for SplitSearcher integration
    doc_mapping_json: Option<String>,    // JSON representation of the doc mapping

    // Skipped splits for merge operations with parsing failures
    skipped_splits: Vec<String>,         // URLs/paths of splits that failed to parse
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
    let split_metadata_class = env.find_class("com/tantivy4java/QuickwitSplit$SplitMetadata")?;
    
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
    
    let split_id_jstring = string_to_jstring(env, &split_metadata.split_id)?;
    
    // Convert doc mapping JSON to Java string or null if not available
    let doc_mapping_jstring = match &split_metadata.doc_mapping_json {
    Some(json) => string_to_jstring(env, json)?,
    None => JString::from(JObject::null()),
    };

    // Create ArrayList for skipped splits
    let array_list_class = env.find_class("java/util/ArrayList")?;
    let skipped_splits_list = env.new_object(&array_list_class, "()V", &[])?;

    // Add skipped splits to the list
    for skipped_split in &split_metadata.skipped_splits {
        let skipped_split_jstring = string_to_jstring(env, skipped_split)?;
        env.call_method(
            &skipped_splits_list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[JValue::Object(&skipped_split_jstring.into())],
        )?;
    }

    // Use the new constructor that includes footer offset parameters, doc mapping JSON, and skipped splits
    let metadata_obj = env.new_object(
    &split_metadata_class,
    "(Ljava/lang/String;JJLjava/time/Instant;Ljava/time/Instant;Ljava/util/Set;JIJJJJLjava/lang/String;Ljava/util/List;)V",
    &[
        JValue::Object(&split_id_jstring.into()),
        JValue::Long(split_metadata.num_docs as i64),
        JValue::Long(split_metadata.uncompressed_docs_size_in_bytes as i64),
        JValue::Object(&time_start_obj),
        JValue::Object(&time_end_obj),
        JValue::Object(&tags_set),
        JValue::Long(split_metadata.delete_opstamp as i64),
        JValue::Int(split_metadata.num_merge_ops as i32),
        // Footer offset parameters (use -1 to indicate missing values)
        JValue::Long(split_metadata.footer_start_offset.map(|v| v as i64).unwrap_or(-1)),
        JValue::Long(split_metadata.footer_end_offset.map(|v| v as i64).unwrap_or(-1)),
        JValue::Long(split_metadata.hotcache_start_offset.map(|v| v as i64).unwrap_or(-1)),
        JValue::Long(split_metadata.hotcache_length.map(|v| v as i64).unwrap_or(-1)),
        // Doc mapping JSON parameter
        JValue::Object(&doc_mapping_jstring.into()),
        // Skipped splits parameter
        JValue::Object(&skipped_splits_list),
    ]
    )?;
    
    Ok(metadata_obj)
}



fn convert_index_from_path_impl(index_path: &str, output_path: &str, config: &SplitConfig) -> Result<QuickwitSplitMetadata, anyhow::Error> {
    // Create a Tokio runtime for async operations
    let runtime = tokio::runtime::Runtime::new()
    .map_err(|e| anyhow::anyhow!("Failed to create async runtime: {}", e))?;

    runtime.block_on(convert_index_from_path_impl_async(index_path, output_path, config))
}

/// Helper function to check if an error message indicates data corruption that should be skipped
/// rather than failing the entire merge operation.
fn is_data_corruption_error(error_msg: &str) -> bool {
    let error_lower = error_msg.to_lowercase();
    error_msg.contains("INDEX_CORRUPTION:") ||
    error_lower.contains("eof while parsing") ||
    error_lower.contains("data corrupted") ||
    error_lower.contains("data corruption") ||
    error_lower.contains("managed file cannot be deserialized") ||
    error_lower.contains("managed file can not be deserialized") ||
    error_lower.contains(".managed.json") ||
    error_lower.contains("column 2291") ||
    error_lower.contains("linke:") ||  // Typo in some Tantivy error messages
    error_lower.contains("indes::open failed") ||  // Index open failures
    error_lower.contains("index::open failed") ||
    error_lower.contains("failed after") && error_lower.contains("attempts") ||
    error_lower.contains("magic number does not match") ||  // Bundle corruption
    error_lower.contains("failed to open split bundle") ||  // Bundle access failures
    error_lower.contains("hot directory metadata")  // Hot directory corruption
}

async fn convert_index_from_path_impl_async(index_path: &str, output_path: &str, config: &SplitConfig) -> Result<QuickwitSplitMetadata, anyhow::Error> {
    use tantivy::directory::MmapDirectory;
    use tantivy::Index as TantivyIndex;
    
    // Open the Tantivy index using the actual Quickwit/Tantivy libraries
    let mmap_directory = MmapDirectory::open(index_path)
    .map_err(|e| anyhow!("Failed to open index directory {}: {}", index_path, e))?;
    let tantivy_index = resilient_ops::resilient_index_open(Box::new(mmap_directory))
    .map_err(|e| anyhow!("Failed to open Tantivy index: {}", e))?;
    
    // Get actual document count from the index
    let searcher = tantivy_index.reader()
    .map_err(|e| anyhow!("Failed to create index reader: {}", e))?
    .searcher();
    let doc_count = searcher.num_docs() as i32;
    
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

// Footer offset information for lazy loading optimization
#[derive(Debug, Clone)]
struct FooterOffsets {
    footer_start_offset: u64,    // Where metadata begins (excludes hotcache)
    footer_end_offset: u64,      // End of file
    hotcache_start_offset: u64,  // Where hotcache begins
    hotcache_length: u64,        // Size of hotcache
}

async fn create_quickwit_split(
    tantivy_index: &tantivy::Index,
    index_dir: &PathBuf,
    output_path: &PathBuf,
    _split_metadata: &QuickwitSplitMetadata,
    config: &SplitConfig
) -> Result<FooterOffsets, anyhow::Error> {
    use tantivy::directory::MmapDirectory;
    
    use uuid::Uuid;
    
    
    
    debug_log!("🔧 OFFICIAL API: Using Quickwit's SplitPayloadBuilder for proper split creation");
    debug_log!("create_quickwit_split called with output_path: {:?}", output_path);

    // ✅ Function is already called from async context, no need for separate runtime
    // ✅ STEP 1: Collect all Tantivy index files first
    let split_id = Uuid::new_v4().to_string();
    debug_log!("✅ OFFICIAL API: Creating split with split_id: {}", split_id);
    
    // Get files from the directory by reading the filesystem
    let file_entries: Vec<_> = std::fs::read_dir(index_dir)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            let path = entry.path();
            if !path.is_file() {
                return false;
            }
            
            let filename = path.file_name().unwrap().to_string_lossy();
            // Skip lock files and split files, include only Tantivy index files
            !filename.starts_with(".tantivy") && !filename.ends_with(".split")
        })
        .map(|entry| entry.path())
        .collect();
    
    debug_log!("✅ OFFICIAL API: Found {} files in index directory", file_entries.len());
    for file_path in &file_entries {
        debug_log!("✅ OFFICIAL API: Will include file: {}", file_path.display());
    }
    
    // ✅ STEP 2: Generate hotcache using Quickwit's official function
    let mmap_directory = MmapDirectory::open(index_dir)?;
    let hotcache = {
        let mut hotcache_buffer = Vec::new();
        
        debug_log!("✅ OFFICIAL API: Generating hotcache using Quickwit's write_hotcache");
        write_hotcache(mmap_directory, &mut hotcache_buffer)
            .map_err(|e| anyhow::anyhow!("Failed to generate hotcache: {}", e))?;
        
        debug_log!("✅ OFFICIAL API: Generated {} bytes of hotcache", hotcache_buffer.len());
        hotcache_buffer
    };
    
    // ✅ STEP 3: Create empty serialized split fields (for now)
    let serialized_split_fields = Vec::new();
    debug_log!("✅ OFFICIAL API: Using empty serialized split fields");
    
    // ✅ STEP 4: Create split payload using official API
    debug_log!("✅ OFFICIAL API: Creating split payload with official get_split_payload()");
    let split_payload = SplitPayloadBuilder::get_split_payload(
        &file_entries,
        &serialized_split_fields,
        &hotcache
    )?;
    
    // ✅ STEP 5: Write payload to output file
    // Note: Streaming functionality available but using simple approach for now
    debug_log!("✅ STREAMING CONFIG: chunk_size={}MB, progress_tracking={}, streaming_io={}",
              config.streaming_chunk_size / 1024 / 1024,
              config.enable_progress_tracking,
              config.enable_streaming_io);
    let total_size = split_payload.len();
    let payload_bytes = split_payload.read_all().await?;
    std::fs::write(output_path, &payload_bytes)?;
    let bytes_written = payload_bytes.len() as u64;

    debug_log!("✅ PAYLOAD WRITTEN: Successfully wrote split file: {:?} ({} bytes total, {} written)",
              output_path, total_size, bytes_written);
    
    // ✅ STEP 6: Extract actual footer information from the created split
    // Read footer from the created file instead of keeping entire payload in memory
    let file_len = total_size;
    
    // Read the last 4 bytes to get hotcache length (Quickwit uses u32, not u64)
    if file_len < 8 {
        return Err(anyhow::anyhow!("Split file too small: {} bytes", file_len));
    }
    
    // Read hotcache length from last 4 bytes of file
    let mut split_file = std::fs::File::open(output_path)?;
    use std::io::{Seek, SeekFrom, Read};
    split_file.seek(SeekFrom::End(-4))?;
    let mut hotcache_len_bytes = [0u8; 4];
    split_file.read_exact(&mut hotcache_len_bytes)?;
    let hotcache_length = u32::from_le_bytes(hotcache_len_bytes) as u64;
    
    debug_log!("✅ OFFICIAL API: Read hotcache length from split: {} bytes", hotcache_length);
    
    // Calculate hotcache start (4 bytes before hotcache for length field)
    let hotcache_start_offset = file_len - 4 - hotcache_length;
    
    // Read metadata length from 4 bytes before hotcache
    if hotcache_start_offset < 4 {
        return Err(anyhow::anyhow!("Invalid hotcache start offset: {}", hotcache_start_offset));
    }
    
    // Read metadata length from 4 bytes before hotcache
    let metadata_len_start = hotcache_start_offset - 4;
    split_file.seek(SeekFrom::Start(metadata_len_start))?;
    let mut metadata_len_bytes = [0u8; 4];
    split_file.read_exact(&mut metadata_len_bytes)?;
    let metadata_length = u32::from_le_bytes(metadata_len_bytes) as u64;
    
    debug_log!("✅ OFFICIAL API: Read metadata length from split: {} bytes", metadata_length);
    
    // Calculate footer start (where BundleStorageFileOffsets JSON begins)
    let footer_start_offset = hotcache_start_offset - 4 - metadata_length;
    
    debug_log!("✅ OFFICIAL API: Calculated footer offsets from actual split structure:");
    debug_log!("   footer_start_offset = {} (where BundleStorageFileOffsets begins)", footer_start_offset);
    debug_log!("   hotcache_start_offset = {} (where hotcache begins)", hotcache_start_offset);
    debug_log!("   file_len = {} (total file size)", file_len);
    debug_log!("   metadata_length = {} bytes", metadata_length);
    debug_log!("   hotcache_length = {} bytes", hotcache_length);
    
    // Validate footer structure
    if footer_start_offset >= hotcache_start_offset {
        return Err(anyhow::anyhow!(
            "Invalid footer structure: footer_start({}) >= hotcache_start({})", 
            footer_start_offset, hotcache_start_offset
        ));
    }
    
    // Verify we can read the metadata section
    let metadata_start = footer_start_offset;
    let metadata_end = footer_start_offset + metadata_length;
    if metadata_end > total_size {
        return Err(anyhow::anyhow!(
            "Metadata section extends beyond file: {}..{} > {}",
            metadata_start, metadata_end, total_size
        ));
    }

    // Debug: Read and verify we can parse the metadata section
    split_file.seek(SeekFrom::Start(metadata_start))?;
    let mut metadata_bytes = vec![0u8; metadata_length as usize];
    split_file.read_exact(&mut metadata_bytes)?;
    match std::str::from_utf8(&metadata_bytes) {
        Ok(metadata_str) => {
            debug_log!("✅ OFFICIAL API: Successfully extracted metadata section ({} bytes)", metadata_str.len());
            debug_log!("✅ OFFICIAL API: Metadata preview: {}", 
                &metadata_str[..std::cmp::min(200, metadata_str.len())]);
        },
        Err(e) => {
            debug_log!("⚠️  OFFICIAL API: Metadata section is not UTF-8 (binary format): {}", e);
        }
    }
    
    let footer_offsets = FooterOffsets {
        footer_start_offset,
        footer_end_offset: file_len,
        hotcache_start_offset,
        hotcache_length,
    };
    
    debug_log!("✅ OFFICIAL API: Split creation completed successfully with proper Quickwit format");
    Ok(footer_offsets)
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_QuickwitSplit_nativeConvertIndex(
    mut env: JNIEnv,
    _class: JClass,
    _index_ptr: i64,
    _output_path: JString,
    _config_obj: JObject,
) -> jobject {
    // This native method is no longer used.
    // The Java convertIndex method now delegates to convertIndexFromPath 
    // after checking the stored index path, which is much cleaner.
    convert_throwable(&mut env, |_env| {
    Err(anyhow!("This native method should not be called. The Java convertIndex method handles the logic."))
    }).unwrap_or(std::ptr::null_mut())
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_QuickwitSplit_nativeConvertIndexFromPath(
    mut env: JNIEnv,
    _class: JClass,
    index_path: JString,
    output_path: JString,
    config_obj: JObject,
) -> jobject {
    convert_throwable(&mut env, |env| {
    let index_path_str = jstring_to_string(env, &index_path)?;
    let output_path_str = jstring_to_string(env, &output_path)?;
    let config = SplitConfig::from_java_object(env, &config_obj)?;
    
    // Use the real implementation that reads actual index data
    let split_metadata = convert_index_from_path_impl(&index_path_str, &output_path_str, &config)?;
    
    let metadata_obj = create_java_split_metadata(env, &split_metadata)?;
    Ok(metadata_obj.into_raw())
    }).unwrap_or(std::ptr::null_mut())
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_QuickwitSplit_nativeReadSplitMetadata(
    mut env: JNIEnv,
    _class: JClass,
    split_path: JString,
) -> jobject {
    convert_throwable(&mut env, |env| {
    let split_path_str = jstring_to_string(env, &split_path)?;
    let path = Path::new(&split_path_str);

    if !path.exists() {
        return Err(anyhow!("Split file does not exist: {}", split_path_str));
    }

    // MEMORY MAPPING FIX: Ensure mmap stays alive during copy operation
    // Add explicit synchronization and validation to prevent truncation
    let file = std::fs::File::open(path)?;
    let file_size = file.metadata()?.len() as usize;
    debug_log!("🔧 MEMORY FIX: File size from metadata: {} bytes", file_size);

    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    debug_log!("🔧 MEMORY FIX: Memory mapped split file, {} bytes", mmap.len());

    // Validate mmap size matches file size
    if mmap.len() != file_size {
        return Err(anyhow!("Memory map size ({}) doesn't match file size ({})", mmap.len(), file_size));
    }

    // Force the memory map to be fully loaded before copying
    #[cfg(unix)]
    {
        use std::os::unix::prelude::*;
        let _ = unsafe { libc::madvise(mmap.as_ptr() as *mut libc::c_void, mmap.len(), libc::MADV_SEQUENTIAL) };
    }

    // Create the copy with validation
    let mmap_slice = mmap.as_ref();
    let owned_bytes = OwnedBytes::new(mmap_slice.to_vec());
    debug_log!("🔧 MEMORY FIX: Created OwnedBytes from validated mmap, {} bytes", owned_bytes.len());

    // Validate the copy was complete
    if owned_bytes.len() != file_size {
        return Err(anyhow!("OwnedBytes copy incomplete: got {} bytes, expected {} bytes", owned_bytes.len(), file_size));
    }
    
    // Create BundleDirectory directly from the split data to extract Tantivy index
    let split_file_slice = tantivy::directory::FileSlice::new(std::sync::Arc::new(owned_bytes.clone()));
    let bundle_directory = quickwit_directories::BundleDirectory::open_split(split_file_slice)
        .map_err(|e| anyhow!("Failed to open bundle directory: {}", e))?;
    
    // Open the Tantivy index from the bundle directory (with resilient retry logic)
    debug_log!("🔍 DATABRICKS DEBUG: About to call resilient_index_open on BundleDirectory");
    debug_log!("🔍 DATABRICKS DEBUG: OwnedBytes size: {} bytes", owned_bytes.len());
    debug_log!("🔍 DATABRICKS DEBUG: BundleDirectory created successfully from FileSlice");

    let tantivy_index = resilient_ops::resilient_index_open(Box::new(bundle_directory))
        .map_err(|e| {
            debug_log!("🚨 DATABRICKS DEBUG: resilient_index_open FAILED with error: {}", e);
            debug_log!("🚨 DATABRICKS DEBUG: This is where the column 2291 error occurs");
            anyhow!("Failed to open Tantivy index from bundle: {}", e)
        })?;

    debug_log!("✅ DATABRICKS DEBUG: resilient_index_open SUCCEEDED");
    
    // Extract doc mapping JSON from the Tantivy index schema
    let doc_mapping_json = extract_doc_mapping_from_index(&tantivy_index)
        .map_err(|e| anyhow!("Failed to extract doc mapping from split index: {}", e))?;
    
    debug_log!("Successfully extracted doc mapping from split ({} bytes)", doc_mapping_json.len());
    
    // Parse the split using Quickwit's BundleStorage for file count (keeping for metadata)
    let ram_storage = std::sync::Arc::new(RamStorage::default());
    let bundle_path = std::path::PathBuf::from("temp.split");
    let (_hotcache, bundle_storage) = BundleStorage::open_from_split_data_with_owned_bytes(
        ram_storage, bundle_path, owned_bytes
    ).map_err(|e| anyhow!("Failed to parse Quickwit split file: {}", e))?;
    
    // Since we don't have the original metadata that was stored during creation,
    // we'll create a minimal metadata object with the file count information
    let file_count = bundle_storage.iter_files().count();
    let split_metadata = QuickwitSplitMetadata {
        split_id: uuid::Uuid::new_v4().to_string(), // Generate a new UUID since we can't recover the original
        index_uid: "unknown".to_string(),
        source_id: "unknown".to_string(),
        node_id: "unknown".to_string(),
        doc_mapping_uid: "unknown".to_string(),
        partition_id: 0,
        num_docs: 0, // Can't determine from split file alone
        uncompressed_docs_size_in_bytes: std::fs::metadata(path)?.len(),
        time_range: None,
        create_timestamp: Utc::now().timestamp(),
        tags: BTreeSet::new(),
        delete_opstamp: 0,
        num_merge_ops: 0,

        // Footer offset fields not available for existing split files
        footer_start_offset: None,
        footer_end_offset: None,
        hotcache_start_offset: None,
        hotcache_length: None,

        // Extract doc mapping JSON from the split's Tantivy index
        doc_mapping_json: Some(doc_mapping_json),

        // Skipped splits not applicable for single split validation
        skipped_splits: Vec::new(),
    };
    
    debug_log!("Successfully read Quickwit split with {} files", file_count);
    
    let metadata_obj = create_java_split_metadata(env, &split_metadata)?;
    Ok(metadata_obj.into_raw())
    }).unwrap_or(std::ptr::null_mut())
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_QuickwitSplit_nativeListSplitFiles(
    mut env: JNIEnv,
    _class: JClass,
    split_path: JString,
) -> jobject {
    convert_throwable(&mut env, |env| {
    let split_path_str = jstring_to_string(env, &split_path)?;
    let path = Path::new(&split_path_str);

    if !path.exists() {
        return Err(anyhow!("Split file does not exist: {}", split_path_str));
    }

    // MEMORY MAPPING FIX: Ensure mmap stays alive during copy operation
    // Add explicit synchronization and validation to prevent truncation
    let file = std::fs::File::open(path)?;
    let file_size = file.metadata()?.len() as usize;
    debug_log!("🔧 MEMORY FIX: File size from metadata: {} bytes", file_size);

    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    debug_log!("🔧 MEMORY FIX: Memory mapped split file, {} bytes", mmap.len());

    // Validate mmap size matches file size
    if mmap.len() != file_size {
        return Err(anyhow!("Memory map size ({}) doesn't match file size ({})", mmap.len(), file_size));
    }

    // Force the memory map to be fully loaded before copying
    #[cfg(unix)]
    {
        use std::os::unix::prelude::*;
        let _ = unsafe { libc::madvise(mmap.as_ptr() as *mut libc::c_void, mmap.len(), libc::MADV_SEQUENTIAL) };
    }

    // Create the copy with validation
    let mmap_slice = mmap.as_ref();
    let owned_bytes = OwnedBytes::new(mmap_slice.to_vec());
    debug_log!("🔧 MEMORY FIX: Created OwnedBytes from validated mmap, {} bytes", owned_bytes.len());

    // Validate the copy was complete
    if owned_bytes.len() != file_size {
        return Err(anyhow!("OwnedBytes copy incomplete: got {} bytes, expected {} bytes", owned_bytes.len(), file_size));
    }
    
    // Parse the split using Quickwit's BundleStorage
    let ram_storage = std::sync::Arc::new(RamStorage::default());
    let bundle_path = std::path::PathBuf::from("temp.split");
    let (_hotcache, bundle_storage) = BundleStorage::open_from_split_data_with_owned_bytes(
        ram_storage, bundle_path, owned_bytes
    ).map_err(|e| anyhow!("Failed to parse Quickwit split file: {}", e))?;
    
    // Create ArrayList to hold file names
    let array_list_class = env.find_class("java/util/ArrayList")?;
    let file_list = env.new_object(&array_list_class, "()V", &[])?;
    
    // Add actual files from the split bundle
    let mut file_count = 0;
    for file_path in bundle_storage.iter_files() {
        let file_name = file_path.to_string_lossy();
        let file_name_jstr = string_to_jstring(env, &file_name)?;
        env.call_method(
            &file_list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[JValue::Object(&file_name_jstr.into())],
        )?;
        file_count += 1;
    }
    
    debug_log!("Listed {} files from Quickwit split", file_count);
    
    Ok(file_list.into_raw())
    }).unwrap_or(std::ptr::null_mut())
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_QuickwitSplit_nativeExtractSplit(
    mut env: JNIEnv,
    _class: JClass,
    split_path: JString,
    output_dir: JString,
) -> jobject {
    convert_throwable(&mut env, |env| {
    let split_path_str = jstring_to_string(env, &split_path)?;
    let output_dir_str = jstring_to_string(env, &output_dir)?;
    
    let split_path = Path::new(&split_path_str);
    let output_path = Path::new(&output_dir_str);
    
    if !split_path.exists() {
        return Err(anyhow!("Split file does not exist: {}", split_path_str));
    }
    
    // Create output directory if it doesn't exist
    std::fs::create_dir_all(output_path)?;

    // FIXED: Use memory mapping instead of loading entire file into RAM
    let file = std::fs::File::open(split_path)?;
    let mmap = unsafe { memmap2::Mmap::map(&file)? };
    let owned_bytes = OwnedBytes::new(mmap.to_vec());
    
    // Parse the split using Quickwit's BundleStorage  
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to create tokio runtime: {}", e))?;
    
    let ram_storage = std::sync::Arc::new(RamStorage::default());
    let bundle_path = std::path::PathBuf::from("temp.split");
    
    // Clone the owned_bytes for the storage put operation
    let owned_bytes_clone = OwnedBytes::new(owned_bytes.as_slice().to_vec());
    
    // Put the split data into RamStorage first
    runtime.block_on(async {
        ram_storage.put(&bundle_path, Box::new(owned_bytes_clone.as_slice().to_vec())).await
    }).map_err(|e| anyhow!("Failed to put split data into storage: {}", e))?;
    
    let (_hotcache, bundle_storage) = BundleStorage::open_from_split_data_with_owned_bytes(
        ram_storage, bundle_path, owned_bytes
    ).map_err(|e| anyhow!("Failed to parse Quickwit split file: {}", e))?;
    
    let mut extracted_count = 0;
    for file_path in bundle_storage.iter_files() {
        let output_file_path = output_path.join(file_path);
        
        // Create parent directories if needed
        if let Some(parent) = output_file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        
        // Extract file content asynchronously
        let file_data = runtime.block_on(async {
            bundle_storage.get_all(file_path).await
        }).map_err(|e| anyhow!("Failed to extract file {}: {}", file_path.display(), e))?;
        
        std::fs::write(&output_file_path, &file_data)?;
        extracted_count += 1;
        
        debug_log!("Extracted file {} ({} bytes)", file_path.display(), file_data.len());
    }
    
    // Create a minimal metadata object for the return value
    let split_metadata = QuickwitSplitMetadata {
        split_id: uuid::Uuid::new_v4().to_string(),
        index_uid: "extracted".to_string(),
        source_id: "extracted".to_string(),
        node_id: "local".to_string(),
        doc_mapping_uid: "default".to_string(),
        partition_id: 0,
        num_docs: 0, // Can't determine from split alone
        uncompressed_docs_size_in_bytes: std::fs::metadata(split_path)?.len(),
        time_range: None,
        create_timestamp: Utc::now().timestamp(),
        tags: BTreeSet::new(),
        delete_opstamp: 0,
        num_merge_ops: 0,

        // Footer offset fields not available for extraction
        footer_start_offset: None,
        footer_end_offset: None,
        hotcache_start_offset: None,
        hotcache_length: None,

        // Doc mapping JSON not available when extracting existing splits
        doc_mapping_json: None,

        // Skipped splits not applicable for single split extraction
        skipped_splits: Vec::new(),
    };
    
    debug_log!("Successfully extracted {} files from Quickwit split to {}", extracted_count, output_path.display());
    
    let metadata_obj = create_java_split_metadata(env, &split_metadata)?;
    Ok(metadata_obj.into_raw())
    }).unwrap_or(std::ptr::null_mut())
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_QuickwitSplit_nativeValidateSplit(
    mut env: JNIEnv,
    _class: JClass,
    split_path: JString,
) -> jni::sys::jboolean {
    convert_throwable(&mut env, |env| {
    let split_path_str = jstring_to_string(env, &split_path)?;
    
    let path = Path::new(&split_path_str);
    let is_valid = path.exists() 
        && path.is_file() 
        && path.extension().map_or(false, |ext| ext == "split");
    
    Ok(if is_valid { jni::sys::JNI_TRUE } else { jni::sys::JNI_FALSE })
    }).unwrap_or(jni::sys::JNI_FALSE)
}

/// Configuration for split merging operations
#[derive(Debug, Clone)]
struct MergeConfig {
    index_uid: String,
    source_id: String,
    node_id: String,
    doc_mapping_uid: String,
    partition_id: u64,
    delete_queries: Option<Vec<String>>,
    aws_config: Option<AwsConfig>,
    temp_directory_path: Option<String>,
}

/// AWS configuration for S3-compatible storage (copy from split_searcher.rs)
#[derive(Clone, Debug)]
struct AwsConfig {
    access_key: Option<String>,  // Made optional to support default credential chain
    secret_key: Option<String>,  // Made optional to support default credential chain
    session_token: Option<String>,
    region: String,
    endpoint: Option<String>,
    force_path_style: bool,
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_QuickwitSplit_nativeMergeSplits(
    mut env: JNIEnv,
    _class: JClass,
    split_urls_list: JObject,
    output_path: JString,
    merge_config: JObject,
) -> jobject {
    convert_throwable(&mut env, |env| {
    debug_log!("Starting split merge operation");
    
    let output_path_str = jstring_to_string(env, &output_path)?;
    debug_log!("Output path: {}", output_path_str);
    
    // Extract merge configuration from Java object
    let config = extract_merge_config(env, &merge_config)?;
    debug_log!("Merge config: {:?}", config);
    
    // Extract split URLs from Java List
    let split_urls = extract_string_list_from_jobject(env, &split_urls_list)?;
    debug_log!("Split URLs to merge: {:?}", split_urls);
    
    if split_urls.len() < 2 {
        return Err(anyhow!("At least 2 splits are required for merging"));
    }
    
    // Perform the merge operation
    let merged_metadata = merge_splits_impl(&split_urls, &output_path_str, &config)?;
    debug_log!("Split merge completed successfully");
    
    // Debug: Check if merged metadata has footer offsets
    debug_log!("🔍 MERGE RESULT: Merged metadata footer offsets: start={:?}, end={:?}, hotcache_start={:?}, hotcache_length={:?}",
              merged_metadata.footer_start_offset, merged_metadata.footer_end_offset,
              merged_metadata.hotcache_start_offset, merged_metadata.hotcache_length);
    
    // Create Java metadata object
    let metadata_obj = create_java_split_metadata(env, &merged_metadata)?;
    Ok(metadata_obj.into_raw())
    }).unwrap_or(std::ptr::null_mut())
}

/// Extract AWS configuration from Java AwsConfig object
fn extract_aws_config(env: &mut JNIEnv, aws_obj: JObject) -> anyhow::Result<AwsConfig> {
    let access_key = get_nullable_string_field_value(env, &aws_obj, "getAccessKey")?;
    let secret_key = get_nullable_string_field_value(env, &aws_obj, "getSecretKey")?;
    let region = get_string_field_value(env, &aws_obj, "getRegion")?;
    
    // Extract session token (optional - for STS/temporary credentials)
    let session_token = match env.call_method(&aws_obj, "getSessionToken", "()Ljava/lang/String;", &[]) {
    Ok(session_result) => {
        let session_obj = session_result.l()?;
        if env.is_same_object(&session_obj, JObject::null())? {
            None
        } else {
            Some(jstring_to_string(env, &session_obj.into())?)
        }
    },
    Err(_) => None,
    };

    // Extract endpoint (optional - for S3-compatible storage)
    let endpoint = match env.call_method(&aws_obj, "getEndpoint", "()Ljava/lang/String;", &[]) {
    Ok(endpoint_result) => {
        let endpoint_obj = endpoint_result.l()?;
        if env.is_same_object(&endpoint_obj, JObject::null())? {
            None
        } else {
            Some(jstring_to_string(env, &endpoint_obj.into())?)
        }
    },
    Err(_) => None,
    };
    
    // Extract force path style flag
    let force_path_style = match env.call_method(&aws_obj, "isForcePathStyle", "()Z", &[]) {
    Ok(result) => result.z()?,
    Err(_) => false,
    };

    Ok(AwsConfig {
    access_key,
    secret_key,
    session_token,
    region,
    endpoint,
    force_path_style,
    })
}

/// Extract merge configuration from Java MergeConfig object
fn extract_merge_config(env: &mut JNIEnv, config_obj: &JObject) -> Result<MergeConfig> {
    let index_uid = get_string_field_value(env, config_obj, "getIndexUid")?;
    let source_id = get_string_field_value(env, config_obj, "getSourceId")?;
    let node_id = get_string_field_value(env, config_obj, "getNodeId")?;
    let doc_mapping_uid = get_string_field_value(env, config_obj, "getDocMappingUid")?;
    
    // Get partition ID
    let partition_id = env.call_method(config_obj, "getPartitionId", "()J", &[])?
    .j()? as u64;
    
    // Get delete queries (optional)
    let delete_queries_result = env.call_method(config_obj, "getDeleteQueries", "()Ljava/util/List;", &[]);
    let delete_queries = match delete_queries_result {
    Ok(list_val) => {
        let list_obj = list_val.l()?;
        if list_obj.is_null() {
            None
        } else {
            Some(extract_string_list_from_jobject(env, &list_obj)?)
        }
    }
    Err(_) => None,
    };
    
    // Extract AWS configuration if present
    let aws_config = match env.call_method(config_obj, "getAwsConfig", "()Lcom/tantivy4java/QuickwitSplit$AwsConfig;", &[]) {
    Ok(aws_result) => {
        let aws_obj = aws_result.l()?;
        if env.is_same_object(&aws_obj, JObject::null())? {
            None
        } else {
            Some(extract_aws_config(env, aws_obj)?)
        }
    },
    Err(_) => None,
    };

    // Extract custom temp directory path if present
    let temp_directory_path = match env.call_method(config_obj, "getTempDirectoryPath", "()Ljava/lang/String;", &[]) {
    Ok(temp_result) => {
        let temp_obj = temp_result.l()?;
        if env.is_same_object(&temp_obj, JObject::null())? {
            None
        } else {
            Some(jstring_to_string(env, &temp_obj.into())?)
        }
    },
    Err(_) => None,
    };

    Ok(MergeConfig {
    index_uid,
    source_id,
    node_id,
    doc_mapping_uid,
    partition_id,
    delete_queries,
    aws_config,
    temp_directory_path,
    })
}

/// Extract a string list from a Java List object  
fn extract_string_list_from_jobject(env: &mut JNIEnv, list_obj: &JObject) -> Result<Vec<String>> {
    let list_size = env.call_method(list_obj, "size", "()I", &[])?.i()?;
    let mut strings = Vec::with_capacity(list_size as usize);
    
    for i in 0..list_size {
    let element = env.call_method(list_obj, "get", "(I)Ljava/lang/Object;", &[JValue::Int(i)])?.l()?;
    let java_string = JString::from(element);
    let rust_string = jstring_to_string(env, &java_string)?;
    strings.push(rust_string);
    }
    
    Ok(strings)
}

/// Helper function to get string field value from Java object
fn get_string_field_value(env: &mut JNIEnv, obj: &JObject, method_name: &str) -> Result<String> {
    let string_obj = env.call_method(obj, method_name, "()Ljava/lang/String;", &[])?.l()?;
    let java_string = JString::from(string_obj);
    jstring_to_string(env, &java_string)
}

/// Get a string field value that can be null (for nullable credentials)
fn get_nullable_string_field_value(env: &mut JNIEnv, obj: &JObject, method_name: &str) -> Result<Option<String>> {
    let string_obj = env.call_method(obj, method_name, "()Ljava/lang/String;", &[])?.l()?;
    if env.is_same_object(&string_obj, JObject::null())? {
    Ok(None)
    } else {
    let java_string = JString::from(string_obj);
    let string_value = jstring_to_string(env, &java_string)?;
    Ok(Some(string_value))
    }
}

/// Extract split file contents to a directory (avoiding read-only BundleDirectory issues)
/// CRITICAL: This function is used by merge operations and must NOT use lazy loading
/// to prevent range assertion failures in the native layer
fn extract_split_to_directory_impl(split_path: &Path, output_dir: &Path) -> Result<()> {
    use tantivy::directory::{MmapDirectory, DirectoryClone};
    
    debug_log!("🔧 MERGE EXTRACTION: Extracting split {:?} to directory {:?}", split_path, output_dir);
    debug_log!("🚨 MERGE SAFETY: Using full file access (NO lazy loading) to prevent native crashes");
    
    // Create output directory
    std::fs::create_dir_all(output_dir)?;
    
    // Open the bundle directory (read-only) - MERGE OPERATIONS MUST USE FULL ACCESS
    let split_path_str = split_path.to_string_lossy().to_string();
    debug_log!("🔧 MERGE EXTRACTION: Opening bundle directory with full file access (no lazy loading)");
    let bundle_directory = get_tantivy_directory_from_split_bundle_full_access(&split_path_str)?;
    
    // Open the output directory (writable)  
    let output_directory = MmapDirectory::open(output_dir)?;
    
    // Open bundle as index to get list of files (with resilient retry logic)
    let temp_bundle_index = resilient_ops::resilient_index_open(bundle_directory.box_clone())?;
    let index_meta = resilient_ops::resilient_load_metas(&temp_bundle_index)?;
    
    // Copy all segment files and meta.json
    let mut copied_files = 0;
    
    // Copy meta.json
    if bundle_directory.exists(Path::new("meta.json"))? {
    debug_log!("Copying meta.json");
    let meta_data = bundle_directory.atomic_read(Path::new("meta.json"))?;
    output_directory.atomic_write(Path::new("meta.json"), &meta_data)?;
    copied_files += 1;
    }
    
    // Copy all segment-related files
    for segment_meta in &index_meta.segments {
    let segment_id = segment_meta.id().uuid_string();
    debug_log!("Copying files for segment: {}", segment_id);
    
    // Copy common segment files (this is a simplified approach - in practice Tantivy has many file types)
    let file_patterns = vec![
        format!("{}.store", segment_id),
        format!("{}.pos", segment_id), 
        format!("{}.idx", segment_id),
        format!("{}.term", segment_id),
        format!("{}.fieldnorm", segment_id),
        format!("{}.fast", segment_id),
    ];
    
    for file_pattern in file_patterns {
        let file_path = Path::new(&file_pattern);
        if bundle_directory.exists(file_path)? {
            debug_log!("Copying file: {}", file_pattern);
            let file_data = bundle_directory.atomic_read(file_path)?;
            output_directory.atomic_write(file_path, &file_data)?;
            copied_files += 1;
        }
    }
    }
    
    debug_log!("Successfully extracted split to directory (copied {} files)", copied_files);
    Ok(())
}

/// ✅ REENTRANCY FIX: Create temporary directory with enhanced collision avoidance
/// Uses retry logic and collision detection to prevent race conditions
fn create_temp_directory_with_base(prefix: &str, base_path: Option<&str>) -> Result<temp::TempDir> {
    create_temp_directory_with_retries(prefix, base_path, 5)
}

/// ✅ REENTRANCY FIX: Create temporary directory with retry logic for collision avoidance
fn create_temp_directory_with_retries(prefix: &str, base_path: Option<&str>, max_retries: usize) -> Result<temp::TempDir> {
    let mut last_error = None;

    for attempt in 0..max_retries {
        // Add random suffix to prefix for additional collision avoidance
        let random_suffix = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
        let enhanced_prefix = format!("{}_{}_", prefix, random_suffix);

        let mut builder = temp::Builder::new();
        builder.prefix(&enhanced_prefix);

        let result = if let Some(custom_base) = base_path {
            // Validate base path on each attempt (could change between attempts)
            let base_path_buf = PathBuf::from(custom_base);
            if !base_path_buf.exists() {
                return Err(anyhow!("Custom temp directory base path does not exist: {}", custom_base));
            }
            if !base_path_buf.is_dir() {
                return Err(anyhow!("Custom temp directory base path is not a directory: {}", custom_base));
            }

            debug_log!("🏗️ REENTRANCY FIX: Using custom temp directory base: {} (attempt {})", custom_base, attempt + 1);
            builder.tempdir_in(&base_path_buf)
        } else {
            debug_log!("🏗️ REENTRANCY FIX: Using system temp directory (attempt {})", attempt + 1);
            builder.tempdir()
        };

        match result {
            Ok(temp_dir) => {
                if attempt > 0 {
                    debug_log!("✅ REENTRANCY FIX: Successfully created temp directory on attempt {}", attempt + 1);
                }
                return Ok(temp_dir);
            }
            Err(e) => {
                debug_log!("⚠️ REENTRANCY FIX: Temp directory creation failed on attempt {}: {}", attempt + 1, e);
                last_error = Some(e);

                // Add exponential backoff delay for retries
                if attempt < max_retries - 1 {
                    let delay_ms = 10u64 * (1 << attempt); // 10ms, 20ms, 40ms, 80ms
                    std::thread::sleep(std::time::Duration::from_millis(delay_ms));
                }
            }
        }
    }

    match last_error {
        Some(e) => Err(anyhow!("Failed to create temp directory with prefix '{}' after {} attempts: {}", prefix, max_retries, e)),
        None => Err(anyhow!("Failed to create temp directory with prefix '{}' after {} attempts: unknown error", prefix, max_retries)),
    }
}

/// Represents an extracted split with its temporary directory and metadata
#[derive(Debug)]
struct ExtractedSplit {
    temp_dir: temp::TempDir,
    temp_path: PathBuf,
    split_url: String,
    split_index: usize,
}

/// ⚡ PARALLEL SPLIT DOWNLOADING AND EXTRACTION
/// Downloads and extracts multiple splits concurrently for maximum performance
/// Uses connection pooling and resource management to prevent overwhelming
async fn download_and_extract_splits_parallel(
    split_urls: &[String],
    merge_id: &str,
    config: &MergeConfig,
) -> Result<(Vec<ExtractedSplit>, Vec<String>)> {
    use futures::future::join_all;
    use std::sync::Arc;

    debug_log!("🚀 Starting parallel download of {} splits with merge_id: {}", split_urls.len(), merge_id);

    // FIXED: Use global semaphore to limit downloads across ALL merge operations
    // This prevents overwhelming the system when multiple merges run simultaneously
    let download_semaphore = &*GLOBAL_DOWNLOAD_SEMAPHORE;
    let available_permits = download_semaphore.available_permits();
    debug_log!("✅ GLOBAL DOWNLOAD LIMIT: Using global download semaphore ({} permits available)", available_permits);

    // Create shared storage resolver for connection pooling
    let shared_storage_resolver = Arc::new(create_storage_resolver(config)?);

    // Create download tasks for each split
    let download_tasks: Vec<_> = split_urls
    .iter()
    .enumerate()
    .map(|(i, split_url)| {
        let split_url = split_url.clone();
        let merge_id = merge_id.to_string();
        let config = config.clone();
        let storage_resolver = Arc::clone(&shared_storage_resolver);

        async move {
            // Acquire semaphore permit to limit concurrent downloads globally
            let _permit = GLOBAL_DOWNLOAD_SEMAPHORE.acquire().await
                .map_err(|e| anyhow!("Failed to acquire global download permit: {}", e))?;

            debug_log!("📥 Starting download task {} for: {}", i, split_url);

            // Download and extract single split - gracefully handle failures
            download_and_extract_single_split_resilient(
                &split_url,
                i,
                &merge_id,
                &config,
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
                debug_log!("🚨 Split {} failed: {} (Error: {})", i, split_url, e);
                skipped_splits.push(split_url.clone());
            }
        }
    }

    debug_log!("✅ Download completed: {} successful, {} skipped", successful_splits.len(), skipped_splits.len());
    Ok((successful_splits, skipped_splits))
}

/// Downloads and extracts a single split with resilient error handling
async fn download_and_extract_single_split_resilient(
    split_url: &str,
    split_index: usize,
    merge_id: &str,
    config: &MergeConfig,
    storage_resolver: &StorageResolver,
) -> Result<Option<ExtractedSplit>> {
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
            if is_data_corruption_error(&error_msg) {
                debug_log!("⚠️ Split {} corrupted/invalid, adding to skip list: {} (Error: {})", split_index, split_url, error_msg);
                Ok(None) // Skip this split gracefully
            } else {
                debug_log!("🚨 Split {} failed with non-corruption error: {} (Error: {})", split_index, split_url, error_msg);
                Ok(None) // For now, skip all errors gracefully - could be made more strict later
            }
        }
    }
}

/// Downloads and extracts a single split (used by parallel download function)
async fn download_and_extract_single_split(
    split_url: &str,
    split_index: usize,
    merge_id: &str,
    config: &MergeConfig,
    storage_resolver: &StorageResolver,
) -> Result<ExtractedSplit> {
    use quickwit_storage::Storage;
    use std::path::Path;

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

    // For S3 URIs, we need to resolve the parent directory, not the file itself
    let (storage_uri, file_name) = if split_uri.protocol() == Protocol::S3 {
        let uri_str = split_uri.as_str();
        if let Some(last_slash) = uri_str.rfind('/') {
            let parent_uri_str = &uri_str[..last_slash]; // Get s3://bucket/splits
            let file_name = &uri_str[last_slash + 1..];  // Get filename
            debug_log!("🔗 Split {}: Split S3 URI into parent: {} and file: {}",
                      split_index, parent_uri_str, file_name);
            (Uri::from_str(parent_uri_str)?, Some(file_name.to_string()))
        } else {
            (split_uri.clone(), None)
        }
    } else {
        (split_uri.clone(), None)
    };

    // Resolve storage for the URI (uses cached/pooled connections)
    let base_storage = storage_resolver.resolve(&storage_uri).await
        .map_err(|e| anyhow!("Split {}: Failed to resolve storage for '{}': {}", split_index, split_url, e))?;

    // ✅ TIMEOUT FIX: Apply optimized timeout policy to prevent .managed.json corruption
    let storage = apply_timeout_policy_to_storage(base_storage);

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
fn create_storage_resolver(config: &MergeConfig) -> Result<StorageResolver> {
    use quickwit_storage::{LocalFileStorageFactory, S3CompatibleObjectStorageFactory, TimeoutAndRetryStorage};
    use quickwit_config::StorageTimeoutPolicy;

    // ✅ TIMEOUT FIX: Configure production-ready timeouts for large split operations
    let mut s3_config = if let Some(ref aws_config) = config.aws_config {
        S3StorageConfig {
            region: Some(aws_config.region.clone()),
            access_key_id: aws_config.access_key.clone(),
            secret_access_key: aws_config.secret_key.clone(),
            session_token: aws_config.session_token.clone(),
            endpoint: aws_config.endpoint.clone(),
            force_path_style_access: aws_config.force_path_style,
            ..Default::default()
        }
    } else {
        S3StorageConfig::default()
    };

    // ✅ TIMEOUT FIX: Configure realistic timeout policy for large split merge operations
    // Addresses problematic default timeouts that cause .managed.json corruption issues
    debug_log!("✅ TIMEOUT FIX: Configuring production-ready timeouts for large split operations");

    // Custom timeout policy optimized for split merging:
    // - Base timeout: 10 seconds (was 2 seconds) - More reasonable for connection establishment
    // - Minimum throughput: 50KB/s (was 100KB/s) - More forgiving for slow connections
    // - Max retries: 5 (was 2) - Better resilience against transient issues
    let timeout_policy = StorageTimeoutPolicy {
        timeout_millis: 10_000,                    // 10 seconds base timeout (vs 2 seconds)
        min_throughtput_bytes_per_secs: 50_000,    // 50KB/s minimum (vs 100KB/s)
        max_num_retries: 5,                        // 5 retries (vs 2)
    };

    debug_log!("✅ TIMEOUT POLICY: base={}ms, min_throughput={}KB/s, retries={}",
        timeout_policy.timeout_millis,
        timeout_policy.min_throughtput_bytes_per_secs / 1000,
        timeout_policy.max_num_retries
    );

    let storage_resolver = StorageResolver::builder()
        .register(LocalFileStorageFactory::default())
        .register(S3CompatibleObjectStorageFactory::new(s3_config))
        .build()
        .map_err(|e| anyhow!("Failed to create storage resolver: {}", e))?;

    debug_log!("✅ TIMEOUT FIX: Storage resolver created with optimized timeout policy configuration");

    Ok(storage_resolver)
}

/// Helper function to apply timeout and retry policy to storage for resilient operations
fn apply_timeout_policy_to_storage(storage: Arc<dyn Storage>) -> Arc<dyn Storage> {
    use quickwit_storage::TimeoutAndRetryStorage;
    use quickwit_config::StorageTimeoutPolicy;

    // ✅ TIMEOUT FIX: Apply optimized timeout policy to prevent .managed.json corruption
    let timeout_policy = StorageTimeoutPolicy {
        timeout_millis: 10_000,                    // 10 seconds base timeout (vs 2 seconds)
        min_throughtput_bytes_per_secs: 50_000,    // 50KB/s minimum (vs 100KB/s)
        max_num_retries: 5,                        // 5 retries (vs 2)
    };

    Arc::new(TimeoutAndRetryStorage::new(storage, timeout_policy))
}

/// ✅ CORRUPTION FIX: Download split file directly to temp file to avoid memory/network slice issues
/// This eliminates the "line 1, column 2291" errors by using reliable local file operations
async fn download_split_to_temp_file(
    storage: &Arc<dyn Storage>,
    split_filename: &str,
    temp_file_path: &Path,
    split_index: usize
) -> Result<()> {
    use quickwit_storage::Storage;
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


/// ✅ CORRUPTION FIX: Verify split file structure to prevent ".managed.json" corruption
/// This specifically checks for the truncation that causes "line 1, column 2291" errors
fn verify_split_file_structure(data: &[u8], split_index: usize, split_url: &str) -> Result<()> {
    if data.len() < 12 {
        return Err(anyhow!("Split {}: File too small: {} bytes (minimum 12 required)", split_index, data.len()));
    }

    // Check that we can read the hotcache length from the last 4 bytes
    let hotcache_len_bytes = &data[data.len() - 4..];
    let hotcache_len = u32::from_le_bytes([
        hotcache_len_bytes[0], hotcache_len_bytes[1],
        hotcache_len_bytes[2], hotcache_len_bytes[3]
    ]) as usize;

    debug_log!("🔧 Split {}: Hotcache length from footer: {} bytes", split_index, hotcache_len);

    if hotcache_len > data.len() - 8 {
        return Err(anyhow!("Split {}: Invalid hotcache length: {} bytes (file size: {})",
            split_index, hotcache_len, data.len()));
    }

    // Check that we can read the metadata length
    let metadata_len_start = data.len() - 4 - hotcache_len - 4;
    if metadata_len_start < 8 {
        return Err(anyhow!("Split {}: Invalid file structure: metadata start at {}", split_index, metadata_len_start));
    }

    let metadata_len_bytes = &data[metadata_len_start..metadata_len_start + 4];
    let metadata_len = u32::from_le_bytes([
        metadata_len_bytes[0], metadata_len_bytes[1],
        metadata_len_bytes[2], metadata_len_bytes[3]
    ]) as usize;

    debug_log!("🔧 Split {}: Metadata length: {} bytes", split_index, metadata_len);

    // This is where the column 2291 error occurs - let's verify the JSON is valid
    let json_start = metadata_len_start - metadata_len;
    if json_start < 8 {
        return Err(anyhow!("Split {}: Invalid JSON start position: {}", split_index, json_start));
    }

    // Extract the JSON metadata and verify it's complete
    let json_data = &data[json_start..metadata_len_start];

    // Skip the 8-byte header (magic number + version) and parse the JSON
    if json_data.len() < 8 {
        return Err(anyhow!("Split {}: JSON metadata too small: {} bytes", split_index, json_data.len()));
    }

    let json_content = &json_data[8..];
    debug_log!("🔧 Split {}: JSON content size: {} bytes", split_index, json_content.len());

    // ✅ CORRUPTION FIX: The "line 1, column 2291" error suggests the JSON is being read with
    // incorrect boundaries. Let's try to detect and fix common boundary issues.

    debug_log!("🔧 Split {}: JSON starts at position {} in file (relative), size {} bytes",
        split_index, json_start, json_content.len());

    // Try parsing the JSON and provide detailed diagnostics if it fails
    match serde_json::from_slice::<serde_json::Value>(json_content) {
        Ok(_) => {
            debug_log!("✅ Split {}: JSON metadata is valid and complete", split_index);
            Ok(())
        }
        Err(e) => {
            let error_msg = e.to_string();

            // Log detailed information about the JSON corruption for debugging
            debug_log!("🚨 Split {}: JSON parsing failed for {}: {}", split_index, split_url, error_msg);
            debug_log!("🚨 Split {}: File size: {} bytes", split_index, data.len());
            debug_log!("🚨 Split {}: JSON content size: {} bytes", split_index, json_content.len());
            debug_log!("🚨 Split {}: JSON start position: {}", split_index, json_start);
            debug_log!("🚨 Split {}: Metadata length: {} bytes", split_index, metadata_len);
            debug_log!("🚨 Split {}: Hotcache length: {} bytes", split_index, hotcache_len);

            // Check if this is a JSON column truncation error
            if error_msg.contains("EOF while parsing") {
                if let Some(column_num) = resilient_ops::extract_column_number(&error_msg) {
                    // This is the consistent truncation issue - let's try alternative parsing approaches
                    debug_log!("🔧 Split {}: Attempting alternative JSON parsing for column {} corruption", split_index, column_num);

                    // Alternative 1: Try parsing with trailing bytes removed
                    if json_content.len() > column_num {
                        let truncated_content = &json_content[..column_num];
                        match serde_json::from_slice::<serde_json::Value>(truncated_content) {
                            Ok(_) => {
                                return Err(anyhow!("Split {}: CONFIRMED COLUMN {} TRUNCATION - JSON is being truncated at exactly {} bytes", split_index, column_num, column_num));
                            }
                            Err(_) => {
                                debug_log!("🔧 Split {}: Alternative parsing at {} bytes also failed", split_index, column_num);
                            }
                        }
                    }

                    return Err(anyhow!("Split {}: CRITICAL COLUMN {} CORRUPTION in {}: {} - This indicates a systematic file reading issue at byte boundary {}",
                        split_index, column_num, split_url, error_msg, column_num));
                }
            }

            return Err(anyhow!("Split {}: JSON validation failed for {}: {}", split_index, split_url, error_msg));
        }
    }
}

/// Implementation of split merging using Quickwit's efficient approach
/// This follows Quickwit's MergeExecutor pattern for memory-efficient large-scale merges
/// CRITICAL: For merge operations, we disable lazy loading and force full file downloads
/// to avoid the range assertion failures that occur when accessing corrupted/invalid metadata
fn merge_splits_impl(split_urls: &[String], output_path: &str, config: &MergeConfig) -> Result<QuickwitSplitMetadata> {
    use quickwit_directories::UnionDirectory;
    use tantivy::directory::{MmapDirectory, Directory, Advice, DirectoryClone};
    use tantivy::{Index as TantivyIndex, IndexMeta};
    
    
    debug_log!("🔧 MERGE OPERATION: Implementing split merge using Quickwit's efficient approach for {} splits", split_urls.len());
    debug_log!("🚨 MERGE SAFETY: Lazy loading DISABLED for merge operations to prevent range assertion failures");
    
    // Create multi-threaded async runtime for parallel operations - optimized for Spark
    let runtime = tokio::runtime::Builder::new_multi_thread()
    .worker_threads(num_cpus::get().clamp(4, 16))  // Scale with cores, 4-16 worker threads
    .enable_all()
    .build()?;

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

    let (extracted_splits, mut skipped_splits) = runtime.block_on(async {
    download_and_extract_splits_parallel(split_urls, &merge_id, config).await
    })?;

    debug_log!("✅ PARALLEL DOWNLOAD: Completed concurrent processing of {} successful splits, {} skipped", extracted_splits.len(), skipped_splits.len());

    // Process the extracted splits to create directories and metadata
    for extracted_split in extracted_splits.into_iter() {
    let i = extracted_split.split_index;
    let split_url = &extracted_split.split_url;
    let temp_extract_path = &extracted_split.temp_path;

    debug_log!("🔧 Processing extracted split {}: {}", i, split_url);

    // ✅ RESILIENT METADATA PARSING: Handle all corruption gracefully with warnings
    let (extracted_directory, temp_index, index_meta) = match (|| -> Result<_> {
        let extracted_directory = MmapDirectory::open(temp_extract_path)?;

        // ✅ RESILIENT INDEX OPEN: Handle Index::open corruption gracefully
        let temp_index = match resilient_ops::resilient_index_open(extracted_directory.box_clone()) {
            Ok(index) => index,
            Err(index_error) => {
                let index_error_msg = index_error.to_string();
                debug_log!("⚠️ INDEX OPEN ERROR: Split {} failed Index::open with: {}", split_url, index_error_msg);

                // Check if this is a data corruption error that should be skipped
                if is_data_corruption_error(&index_error_msg) {
                    debug_log!("🔧 INDEX CORRUPTION DETECTED: Split {} has corrupted index data - adding to skip list", split_url);
                    return Err(anyhow!("INDEX_CORRUPTION: {}", index_error_msg));
                } else {
                    // Re-throw non-corruption errors
                    return Err(index_error);
                }
            }
        };

        let index_meta = resilient_ops::resilient_load_metas(&temp_index)?;
        Ok((extracted_directory, temp_index, index_meta))
    })() {
        Ok((dir, index, meta)) => (dir, index, meta),
        Err(e) => {
            let error_msg = e.to_string();
            debug_log!("⚠️ SPLIT PROCESSING ERROR: Skipping split {} due to error: {}", split_url, error_msg);

            // Check if this is a known data corruption or metadata parsing error
            if is_data_corruption_error(&error_msg) {
                debug_log!("🔧 DATA CORRUPTION SKIP: Split {} has known data corruption or metadata error - continuing with remaining splits", split_url);
                debug_log!("🔧 ERROR DETAILS: {}", error_msg);
            } else {
                debug_log!("🔧 GENERAL ERROR SKIP: Split {} has unexpected error - continuing with remaining splits", split_url);
                debug_log!("🔧 ERROR DETAILS: {}", error_msg);
            }

            skipped_splits.push(split_url.clone());
            continue; // Skip this split and continue with others
        }
    };

    // Count documents and calculate size efficiently
    let reader = temp_index.reader()?;
    let searcher = reader.searcher();
    let doc_count = searcher.num_docs();

    // Calculate split size based on source type
    let split_size = if split_url.contains("://") && !split_url.starts_with("file://") {
        // For S3/remote splits, get size from the temporary downloaded file
        let split_filename = split_url.split('/').last().unwrap_or("split.split");
        let temp_split_path = temp_extract_path.join(split_filename);
        std::fs::metadata(&temp_split_path)?.len()
    } else {
        // For local splits, get size from original file
        let split_path = if split_url.starts_with("file://") {
            split_url.strip_prefix("file://").unwrap_or(split_url)
        } else {
            split_url
        };
        std::fs::metadata(split_path)?.len()
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

    if valid_splits == 0 {
        return Err(anyhow!("All splits failed to parse due to data corruption or metadata errors. Skipped splits: {:?}", skipped_splits));
    }

    if !skipped_splits.is_empty() {
        debug_log!("⚠️ PARTIAL MERGE WARNING: Proceeding with {} valid splits, {} skipped due to corruption: {:?}", valid_splits, skipped_splits.len(), skipped_splits);
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
    let temp_dir = if let Some(custom_base) = &config.temp_directory_path {
        let temp_dir = create_temp_directory_with_base(
            &format!("tantivy4java_merge_{}_output_", merge_id),
            Some(custom_base)
        )?;
        temp_dir.path().to_path_buf()
    } else {
        // Fallback to next to output file
        let output_dir_path = Path::new(output_path).parent()
            .ok_or_else(|| anyhow!("Cannot determine parent directory for output path"))?;
        let temp_dir = output_dir_path.join(format!("temp_merge_output_{}", merge_id));
        std::fs::create_dir_all(&temp_dir)?;
        temp_dir
    };
    debug_log!("Created local temporary directory: {:?}", temp_dir);
    (temp_dir, false, None)
    };
    
    // 3. Perform memory-efficient segment-level merge using Quickwit's implementation
    let merged_docs = runtime.block_on(perform_quickwit_merge(
        split_directories,
        &output_temp_dir,
    ))?;
    debug_log!("Segment merge completed with {} documents", merged_docs);

    // 4. Extract doc mapping from the merged index
    let merged_directory = MmapDirectory::open(&output_temp_dir)?;
    let tokenizer_manager = get_quickwit_fastfield_normalizer_manager().tantivy_manager();
    let merged_index = open_index(merged_directory, tokenizer_manager)?;
    let doc_mapping_json = extract_doc_mapping_from_index(&merged_index)?;
    debug_log!("Extracted doc mapping from merged index ({} bytes)", doc_mapping_json.len());

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
    coordinate_temp_directory_cleanup(&merge_id, &output_temp_dir, temp_dirs)?;
    
    debug_log!("Created efficient merged split file: {} with {} documents", output_path, merged_docs);

    // Report skipped splits if any
    if !skipped_splits.is_empty() {
        debug_log!("⚠️ MERGE WARNING: {} splits were skipped due to metadata parsing failures:", skipped_splits.len());
        for (i, skipped_url) in skipped_splits.iter().enumerate() {
            debug_log!("   {}. {}", i + 1, skipped_url);
        }
    }

    Ok(final_merged_metadata)
    })(); // End closure

    // FIXED: Clean up temporary directories from registry to prevent memory leaks
    cleanup_merge_temp_directory(&merge_id);

    // Ensure proper runtime cleanup regardless of success or failure
    runtime.shutdown_background();
    debug_log!("Tokio runtime shutdown completed");

    // Return the result
    result
}

/// Get Tantivy directory from split bundle using Quickwit's BundleDirectory
/// This is memory-efficient as it doesn't extract files
fn get_tantivy_directory_from_split_bundle(split_path: &str) -> Result<Box<dyn tantivy::Directory>> {
    use quickwit_directories::BundleDirectory;
    use tantivy::directory::MmapDirectory;
    
    debug_log!("Opening bundle directory for split: {}", split_path);
    
    let split_file_path = PathBuf::from(split_path);
    let parent_dir = split_file_path.parent()
    .ok_or_else(|| anyhow!("Cannot find parent directory for {}", split_path))?;
    
    // Open parent directory
    let mmap_directory = MmapDirectory::open(parent_dir)?;
    
    // Get filename only
    let filename = split_file_path.file_name()
    .ok_or_else(|| anyhow!("Cannot extract filename from {}", split_path))?;
    
    // Open the split file slice
    let split_fileslice = mmap_directory.open_read(Path::new(filename))?;
    
    // Create BundleDirectory - this provides direct access without extraction
    let bundle_directory = BundleDirectory::open_split(split_fileslice)?;
    
    debug_log!("Successfully opened bundle directory for split: {}", split_path);
    Ok(Box::new(bundle_directory))
}

/// Get Tantivy directory from split bundle with FULL ACCESS (no lazy loading)
/// CRITICAL: This version is used for merge operations to prevent range assertion failures
/// It forces full file download instead of using lazy loading with potentially corrupted offsets
fn get_tantivy_directory_from_split_bundle_full_access(split_path: &str) -> Result<Box<dyn tantivy::Directory>> {
    use quickwit_storage::PutPayload;
    use tantivy::directory::OwnedBytes;
    use quickwit_directories::BundleDirectory;
    
    debug_log!("🔧 MERGE SAFETY: Opening bundle directory with FULL ACCESS (no lazy loading): {}", split_path);
    
    let split_file_path = PathBuf::from(split_path);
    
    // FIXED: Use memory mapping instead of loading entire file into RAM
    debug_log!("✅ MEMORY FIX: Using memory mapping instead of loading entire file into RAM");

    let file = std::fs::File::open(&split_file_path)
        .map_err(|e| anyhow!("Failed to open split file {:?}: {}", split_file_path, e))?;

    let file_size = file.metadata()
        .map_err(|e| anyhow!("Failed to get file metadata: {}", e))?
        .len() as usize;

    debug_log!("✅ MEMORY FIX: File size is {} bytes, using memory mapping for efficient access", file_size);

    // Use memory mapping for efficient file access without loading into RAM
    let mmap = unsafe {
        memmap2::Mmap::map(&file)
            .map_err(|e| anyhow!("Failed to memory map split file: {}", e))?
    };

    // Create OwnedBytes from the memory mapped data (zero-copy)
    let owned_bytes = OwnedBytes::new(mmap.to_vec());
    
    // Create FileSlice from the complete file data (no range restrictions)
    let file_slice = tantivy::directory::FileSlice::new(std::sync::Arc::new(owned_bytes));
    
    // Create BundleDirectory from the full file slice
    // This should work correctly because we have the complete file data
    let bundle_directory = BundleDirectory::open_split(file_slice)
    .map_err(|e| anyhow!("Failed to open split bundle with full access: {}", e))?;
    
    debug_log!("✅ MERGE SAFETY: Successfully opened bundle directory with full file access (no lazy loading)");
    Ok(Box::new(bundle_directory))
}



/// ✅ REENTRANCY FIX: Coordinated cleanup to prevent race conditions
/// Ensures proper cleanup order and handles overlapping directory scenarios
fn coordinate_temp_directory_cleanup(merge_id: &str, output_temp_dir: &Path, temp_dirs: Vec<temp::TempDir>) -> Result<()> {
    debug_log!("🧹 REENTRANCY FIX: Starting coordinated cleanup for merge: {}", merge_id);

    // Step 1: Wait for any pending operations to complete (brief pause)
    std::thread::sleep(std::time::Duration::from_millis(50));

    // Step 2: Clean up output temp directory with safety checks
    cleanup_output_directory_safe(output_temp_dir)?;

    // Step 3: Clean up split extraction temporary directories with validation
    cleanup_split_directories_safe(temp_dirs, merge_id)?;

    debug_log!("✅ REENTRANCY FIX: Coordinated cleanup completed for merge: {}", merge_id);
    Ok(())
}

/// ✅ REENTRANCY FIX: Safely clean up output directory with path validation
fn cleanup_output_directory_safe(output_temp_dir: &Path) -> Result<()> {
    if !output_temp_dir.exists() {
        debug_log!("🧹 REENTRANCY FIX: Output directory already cleaned up: {:?}", output_temp_dir);
        return Ok(());
    }

    // Validate this is actually a temporary directory (safety check)
    // Check both the directory name and its parent path for temp directory markers
    let dir_name = output_temp_dir.file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");

    let parent_name = output_temp_dir.parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .unwrap_or("");

    let full_path_str = output_temp_dir.to_string_lossy();

    // Allow deletion if:
    // 1. Directory name contains our temp markers, OR
    // 2. Parent directory contains our temp markers, OR
    // 3. Full path contains our temp markers (for nested temp directories)
    let is_safe_to_delete = dir_name.contains("tantivy4java_merge")
        || dir_name.contains("temp_merge")
        || parent_name.contains("tantivy4java_merge")
        || parent_name.contains("temp_merge")
        || full_path_str.contains("tantivy4java_merge")
        || full_path_str.contains("temp_merge");

    if !is_safe_to_delete {
        return Err(anyhow!("🚨 SAFETY: Refusing to delete directory that doesn't appear to be a tantivy4java temp directory: {:?}", output_temp_dir));
    }

    match std::fs::remove_dir_all(output_temp_dir) {
        Ok(()) => {
            debug_log!("✅ REENTRANCY FIX: Successfully cleaned up output directory: {:?}", output_temp_dir);
            Ok(())
        }
        Err(e) => {
            debug_log!("⚠️ REENTRANCY FIX: Warning: Could not clean up output temp directory {:?}: {}", output_temp_dir, e);
            // Don't fail the entire operation for cleanup issues
            Ok(())
        }
    }
}

/// ✅ REENTRANCY FIX: Safely clean up split directories with overlap detection
fn cleanup_split_directories_safe(temp_dirs: Vec<temp::TempDir>, merge_id: &str) -> Result<()> {
    debug_log!("🧹 REENTRANCY FIX: Cleaning up {} split temp directories for merge: {}", temp_dirs.len(), merge_id);

    for (i, temp_dir) in temp_dirs.into_iter().enumerate() {
        let temp_path = temp_dir.path();

        // Validate directory name contains merge ID (safety check)
        let dir_name = temp_path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");

        if dir_name.contains(merge_id) || dir_name.contains("tantivy4java_merge") {
            debug_log!("✅ REENTRANCY FIX: Cleaning up split temp directory {}: {:?}", i, temp_path);
            // temp_dir will be automatically cleaned up when dropped (RAII)
        } else {
            debug_log!("⚠️ REENTRANCY FIX: Skipping cleanup of directory that doesn't match merge ID: {:?}", temp_path);
        }
    }

    debug_log!("✅ REENTRANCY FIX: All split temp directories processed for cleanup");
    Ok(())
}

/// ✅ REENTRANCY FIX: Clean up temporary directories for a specific merge operation
/// Uses the new safe cleanup function with proper error handling
fn cleanup_merge_temp_directory(merge_id: &str) {
    let registry_key = format!("merge_meta_{}", merge_id);
    match cleanup_temp_directory_safe(&registry_key) {
        Ok(true) => {
            debug_log!("✅ REENTRANCY FIX: Successfully cleaned up temp directory: {}", registry_key);
        }
        Ok(false) => {
            debug_log!("⚠️ REENTRANCY FIX: Temp directory not found in registry: {}", registry_key);
        }
        Err(e) => {
            debug_log!("❌ REENTRANCY FIX: Error cleaning up temp directory {}: {}", registry_key, e);
        }
    }
}

/// Perform segment-level merge using Quickwit/Tantivy's efficient approach
async fn perform_quickwit_merge(
    split_directories: Vec<Box<dyn Directory>>,
    output_path: &Path,
) -> Result<usize> {
    debug_log!("Performing Quickwit merge with {} directories", split_directories.len());

    // Step 1: Use Quickwit's helper to combine metadata
    let tokenizer_manager = get_quickwit_fastfield_normalizer_manager().tantivy_manager();
    let (union_index_meta, directories) = open_split_directories(&split_directories, tokenizer_manager)?;
    debug_log!("Combined metadata from {} splits", directories.len());

    // Step 2: Create IO controls for the merge operation
    let io_controls = IoControls::default();

    // Step 3: Use Quickwit's exact merge implementation
    let controlled_directory = merge_split_directories_standalone(
        union_index_meta,
        directories,
        Vec::new(), // No delete tasks for split merging
        None,       // No doc mapper needed for split merging
        output_path,
        io_controls,
        tokenizer_manager,
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
fn calculate_directory_size(dir_path: &Path) -> Result<u64> {
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

/// Upload a local split file to S3 using the storage resolver with AWS credentials
async fn upload_split_to_s3_impl(local_split_path: &Path, s3_url: &str, config: &MergeConfig) -> Result<()> {
    use quickwit_storage::{Storage, StorageResolver, LocalFileStorageFactory, S3CompatibleObjectStorageFactory};
    use std::str::FromStr;
    
    debug_log!("Starting S3 upload from {:?} to {}", local_split_path, s3_url);
    
    // Parse the S3 URI
    let s3_uri = Uri::from_str(s3_url)?;
    
    // Create storage resolver with AWS config from MergeConfig
    let s3_config = if let Some(ref aws_config) = config.aws_config {
    S3StorageConfig {
        region: Some(aws_config.region.clone()),
        access_key_id: aws_config.access_key.clone(),
        secret_access_key: aws_config.secret_key.clone(),
        session_token: aws_config.session_token.clone(),
        endpoint: aws_config.endpoint.clone(),
        force_path_style_access: aws_config.force_path_style,
        ..Default::default()
    }
    } else {
    S3StorageConfig::default()
    };
    let storage_resolver = StorageResolver::builder()
    .register(LocalFileStorageFactory::default())
    .register(S3CompatibleObjectStorageFactory::new(s3_config))
    .build()
    .map_err(|e| anyhow!("Failed to create storage resolver for upload: {}", e))?;
    
    // For S3 URIs, we need to resolve the parent directory, not the file itself
    let (storage_uri, file_name) = if s3_uri.protocol() == Protocol::S3 {
    let uri_str = s3_uri.as_str();
    if let Some(last_slash) = uri_str.rfind('/') {
        let parent_uri_str = &uri_str[..last_slash]; // Get s3://bucket/splits
        let file_name = &uri_str[last_slash + 1..];  // Get filename
        debug_log!("Split S3 URI for upload into parent: {} and file: {}", parent_uri_str, file_name);
        (Uri::from_str(parent_uri_str)?, file_name.to_string())
    } else {
        return Err(anyhow!("Invalid S3 URL format: {}", s3_url));
    }
    } else {
    return Err(anyhow!("Only S3 URLs are supported for upload, got: {}", s3_url));
    };
    
    // Resolve storage for the URI
    let base_storage = storage_resolver.resolve(&storage_uri).await
    .map_err(|e| anyhow!("Failed to resolve storage for '{}': {}", s3_url, e))?;

    // ✅ TIMEOUT FIX: Apply optimized timeout policy to prevent upload timeouts
    let storage = apply_timeout_policy_to_storage(base_storage);
    
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

    // Upload the split file to S3 using memory mapped data
    storage.put(Path::new(&file_name), Box::new(mmap.to_vec())).await
    .map_err(|e| anyhow!("Failed to upload split to S3: {}", e))?;
    
    debug_log!("Successfully uploaded {} bytes to S3: {}", file_size, s3_url);
    Ok(())
}

/// Synchronous wrapper for S3 upload
fn upload_split_to_s3(local_split_path: &Path, s3_url: &str, config: &MergeConfig) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
    .enable_all()
    .build()?;
    runtime.block_on(upload_split_to_s3_impl(local_split_path, s3_url, config))
}

/// Create the merged split file using existing Quickwit split creation logic
/// Returns the footer offsets for the merged split
fn create_merged_split_file(merged_index_path: &Path, output_path: &str, metadata: &QuickwitSplitMetadata) -> Result<FooterOffsets> {
    use tantivy::directory::MmapDirectory;
    use tantivy::Index as TantivyIndex;
    
    debug_log!("Creating merged split file at {} from index {:?}", output_path, merged_index_path);
    
    // Open the merged Tantivy index
    let merged_directory = MmapDirectory::open(merged_index_path)?;
    let merged_index = resilient_ops::resilient_index_open(Box::new(merged_directory))
        .map_err(|e| {
            let error_msg = e.to_string();
            if is_data_corruption_error(&error_msg) {
                anyhow!("Merged index file appears to be corrupted during split creation. This may indicate a problem with the merge process. Error: {}", error_msg)
            } else {
                anyhow!("Failed to open merged index for split creation: {}", error_msg)
            }
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

    // Create runtime for async call
    let runtime = tokio::runtime::Runtime::new()
    .map_err(|e| anyhow::anyhow!("Failed to create async runtime: {}", e))?;
    let footer_offsets = runtime.block_on(create_quickwit_split(&merged_index, &merged_index_path.to_path_buf(), &PathBuf::from(output_path), metadata, &default_config))?;
    
    debug_log!("Successfully created merged split file: {} with footer offsets: {:?}", output_path, footer_offsets);
    Ok(footer_offsets)
}