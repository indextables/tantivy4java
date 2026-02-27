// jni_functions.rs - JNI entry points for QuickwitSplit Java class
// Extracted from mod.rs during refactoring
// Contains: All nativeXxx JNI functions for QuickwitSplit

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;

use anyhow::anyhow;
use jni::objects::{JClass, JObject, JString, JValue};
use jni::sys::jobject;
use jni::JNIEnv;
use chrono::Utc;
use tantivy::directory::Directory;

use crate::debug_println;
use crate::utils::{convert_throwable, jstring_to_string, string_to_jstring, arc_to_jlong, release_arc};
use super::{
    SplitConfig, QuickwitSplitMetadata, QuickwitRuntimeManager,
    convert_index_from_path_impl, create_java_split_metadata, default_split_config,
};
use super::merge_config::{extract_merge_config, extract_string_list_from_jobject};
use super::merge_impl::merge_splits_impl;
use super::split_utils::{open_split_with_quickwit_native, get_split_file_list};
use super::json_discovery::extract_doc_mapping_from_index;
use crate::parquet_companion::indexing::{CreateFromParquetConfig, StringFingerprintMode, create_split_from_parquet};
use crate::parquet_companion::manifest::FastFieldMode;
use crate::parquet_companion::schema_derivation::SchemaDerivationConfig;
use crate::parquet_companion::statistics::ColumnStatisticsResult;

// Debug logging macro - controlled by TANTIVY4JAVA_DEBUG environment variable
macro_rules! debug_log {
    ($($arg:tt)*) => {
        debug_println!("DEBUG: {}", format!($($arg)*))
    };
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeConvertIndex(
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
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeConvertIndexFromPath(
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
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeReadSplitMetadata(
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
    // ✅ QUICKWIT NATIVE: Use Quickwit's native functions to open the split
    // This replaces all custom memory mapping and index opening logic
    let (tantivy_index, _bundle_directory) = open_split_with_quickwit_native(&split_path_str)?;

    // Get actual document count from the split
    let reader = tantivy_index.reader()
        .map_err(|e| anyhow!("Failed to create index reader: {}", e))?;
    reader.reload()
        .map_err(|e| anyhow!("Failed to reload index reader: {}", e))?;
    let searcher = reader.searcher();
    let doc_count = searcher.num_docs() as usize;

    debug_log!("🔍 READ SPLIT: Found {} documents in split {}", doc_count, split_path_str);

    // Extract doc mapping JSON from the Tantivy index schema
    let doc_mapping_json = extract_doc_mapping_from_index(&tantivy_index)
        .map_err(|e| anyhow!("Failed to extract doc mapping from split index: {}", e))?;

    debug_log!("Successfully extracted doc mapping from split ({} bytes)", doc_mapping_json.len());

    // ✅ QUICKWIT NATIVE: Since we can't get file count from BundleDirectory API,
    // we'll use a reasonable default for metadata
    let file_count = 1; // Placeholder - real metadata would come from split creation

    // Since we don't have the original metadata that was stored during creation,
    // we'll create a minimal metadata object with the file count information
    let split_metadata = QuickwitSplitMetadata {
        split_id: uuid::Uuid::new_v4().to_string(), // Generate a new UUID since we can't recover the original
        index_uid: "unknown".to_string(),
        source_id: "unknown".to_string(),
        node_id: "unknown".to_string(),
        doc_mapping_uid: "unknown".to_string(),
        partition_id: 0,
        num_docs: doc_count, // ✅ FIXED: Get actual document count from split
        uncompressed_docs_size_in_bytes: std::fs::metadata(&split_path_str)?.len(),
        time_range: None,
        create_timestamp: Utc::now().timestamp(),
        maturity: "Mature".to_string(),
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
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeListSplitFiles(
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
    // ✅ QUICKWIT NATIVE: Get file list using Quickwit's native get_stats_split approach
    let file_list = get_split_file_list(&split_path_str)?;
    let file_names: Vec<String> = file_list
        .iter()
        .map(|path| path.to_string_lossy().to_string())
        .collect();

    debug_log!("✅ QUICKWIT NATIVE: Successfully listed {} files from split", file_names.len());

    // Create ArrayList to hold file names
    let array_list_class = env.find_class("java/util/ArrayList")?;
    let file_list = env.new_object(&array_list_class, "()V", &[])?;

    // ✅ QUICKWIT NATIVE: Add files from the pre-collected list
    let mut file_count = 0;
    for file_name in file_names {
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
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeExtractSplit(
    mut env: JNIEnv,
    _class: JClass,
    split_path: JString,
    output_dir: JString,
) -> jobject {
    convert_throwable(&mut env, |env| {
    let split_path_str = jstring_to_string(env, &split_path)?;
    let output_dir_str = jstring_to_string(env, &output_dir)?;

    debug_log!("🔧 EXTRACT START: Extracting split {} to {}", split_path_str, output_dir_str);

    let split_path = Path::new(&split_path_str);
    let output_path = Path::new(&output_dir_str);

    if !split_path.exists() {
        debug_log!("❌ EXTRACT: Split file does not exist: {}", split_path_str);
        return Err(anyhow!("Split file does not exist: {}", split_path_str));
    }

    // Create output directory if it doesn't exist
    std::fs::create_dir_all(output_path)
        .map_err(|e| anyhow!("Failed to create output directory {}: {}", output_dir_str, e))?;

    debug_log!("✅ EXTRACT: Output directory created/verified: {}", output_dir_str);

    // ✅ QUICKWIT NATIVE: Use Quickwit's native functions to open the split
    let (_tantivy_index, bundle_directory) = open_split_with_quickwit_native(&split_path_str)?;

    // ✅ QUICKWIT NATIVE: Get file list using Quickwit's native get_stats_split approach
    let mut extracted_count = 0;
    let file_list = get_split_file_list(&split_path_str)?;

    debug_log!("📁 EXTRACT: Found {} files to extract from split", file_list.len());
    if file_list.is_empty() {
        debug_log!("⚠️ EXTRACT: No files found in split - this may indicate an issue with split format");
    }

    for file_path in file_list {
        debug_log!("📄 EXTRACT: Processing file: {}", file_path.to_string_lossy());
        let output_file_path = output_path.join(&file_path);

        // Create parent directories if needed
        if let Some(parent) = output_file_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Extract file content using Quickwit's BundleDirectory methods
        let file_slice = match bundle_directory.open_read(&file_path) {
            Ok(slice) => slice,
            Err(e) => {
                debug_log!("❌ EXTRACT: Failed to open file {} for reading: {}", file_path.to_string_lossy(), e);
                return Err(anyhow!("Failed to open file {} for reading: {}", file_path.to_string_lossy(), e));
            }
        };

        let file_data = match file_slice.read_bytes() {
            Ok(data) => data,
            Err(e) => {
                debug_log!("❌ EXTRACT: Failed to read file {}: {}", file_path.to_string_lossy(), e);
                return Err(anyhow!("Failed to read file {}: {}", file_path.to_string_lossy(), e));
            }
        };

        std::fs::write(&output_file_path, file_data.as_slice())
            .map_err(|e| anyhow!("Failed to write extracted file {}: {}", output_file_path.display(), e))?;

        extracted_count += 1;
        debug_log!("✅ EXTRACT: Successfully extracted file {} ({} bytes)", file_path.to_string_lossy(), file_data.len());
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
        maturity: "Mature".to_string(),
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

    debug_log!("🎉 EXTRACT COMPLETE: Successfully extracted {} files from Quickwit split to {}", extracted_count, output_path.display());

    let metadata_obj = create_java_split_metadata(env, &split_metadata)?;
    Ok(metadata_obj.into_raw())
    }).unwrap_or(std::ptr::null_mut())
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeValidateSplit(
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

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeMergeSplits(
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

/// JNI: Create a Quickwit split from external parquet files.
/// Returns a HashMap<String, Object> with "metadata" (SplitMetadata) and "columnStatistics" (Map).
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeCreateFromParquet(
    mut env: JNIEnv,
    _class: JClass,
    parquet_files_list: JObject,
    output_path: JString,
    config_json: JString,
) -> jobject {
    convert_throwable(&mut env, |env| {
    let output_path_str = jstring_to_string(env, &output_path)?;
    let config_json_str = jstring_to_string(env, &config_json)?;

    debug_log!("🏗️ JNI createFromParquet: output={}", output_path_str);

    // Extract parquet file paths from Java List<String>
    let parquet_files = extract_string_list_from_jobject(env, &parquet_files_list)?;

    if parquet_files.is_empty() {
        return Err(anyhow!("No parquet files provided"));
    }

    debug_log!("🏗️ JNI createFromParquet: {} parquet files", parquet_files.len());

    // Parse config JSON
    let parquet_config = parse_create_from_parquet_config(&config_json_str)?;

    // Create a dummy storage (local filesystem, not used for local files)
    let storage: std::sync::Arc<dyn quickwit_storage::Storage> =
        std::sync::Arc::new(quickwit_storage::RamStorage::default());

    // Run the async pipeline on the global runtime
    let result = QuickwitRuntimeManager::global().handle().block_on(
        create_split_from_parquet(&parquet_files, &output_path_str, &parquet_config, &storage)
    )?;

    debug_log!(
        "🏗️ JNI createFromParquet: Split created with {} docs, {} statistics",
        result.metadata.num_docs, result.column_statistics.len()
    );

    // Create the result HashMap
    let hash_map_class = env.find_class("java/util/HashMap")?;
    let result_map = env.new_object(&hash_map_class, "()V", &[])?;

    // Add split metadata
    let metadata_obj = create_java_split_metadata(env, &result.metadata)?;
    let metadata_key = string_to_jstring(env, "metadata")?;
    env.call_method(
        &result_map,
        "put",
        "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
        &[JValue::Object(&metadata_key.into()), JValue::Object(&metadata_obj)],
    )?;

    // Add column statistics as Map<String, ColumnStatistics>
    let stats_map = create_java_column_statistics_map(env, &result.column_statistics)?;
    let stats_key = string_to_jstring(env, "columnStatistics")?;
    env.call_method(
        &result_map,
        "put",
        "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
        &[JValue::Object(&stats_key.into()), JValue::Object(&stats_map)],
    )?;

    Ok(result_map.into_raw())
    }).unwrap_or(std::ptr::null_mut())
}

/// Parse the JSON config into a CreateFromParquetConfig.
fn parse_create_from_parquet_config(json_str: &str) -> anyhow::Result<CreateFromParquetConfig> {
    let parsed: serde_json::Value = serde_json::from_str(json_str)
        .map_err(|e| anyhow!("Failed to parse parquet config JSON: {}", e))?;

    let table_root = parsed.get("table_root")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let fast_field_mode = match parsed.get("fast_field_mode").and_then(|v| v.as_str()) {
        Some("HYBRID") => FastFieldMode::Hybrid,
        Some("PARQUET_ONLY") => FastFieldMode::ParquetOnly,
        _ => FastFieldMode::Disabled,
    };

    let index_uid = parsed.get("index_uid")
        .and_then(|v| v.as_str())
        .unwrap_or("parquet-index")
        .to_string();

    let source_id = parsed.get("source_id")
        .and_then(|v| v.as_str())
        .unwrap_or("parquet-source")
        .to_string();

    let node_id = parsed.get("node_id")
        .and_then(|v| v.as_str())
        .unwrap_or("parquet-node")
        .to_string();

    let statistics_fields: Vec<String> = parsed.get("statistics_fields")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();

    let statistics_truncate_length = parsed.get("statistics_truncate_length")
        .and_then(|v| v.as_u64())
        .unwrap_or(256) as usize;

    let skip_fields: HashSet<String> = parsed.get("skip_fields")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();

    let tokenizer_overrides: HashMap<String, String> = parsed.get("tokenizer_overrides")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();

    let field_id_mapping: HashMap<String, String> = parsed.get("field_id_mapping")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default();

    let auto_detect_name_mapping = parsed.get("auto_detect_name_mapping")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let ip_address_fields: HashSet<String> = parsed.get("ip_address_fields")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();

    let json_fields: HashSet<String> = parsed.get("json_fields")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_default();

    let writer_heap_size = parsed.get("writer_heap_size")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
        .unwrap_or(256_000_000);

    let reader_batch_size = parsed.get("reader_batch_size")
        .and_then(|v| v.as_u64())
        .map(|v| v as usize)
        .unwrap_or(8192);

    // Parse string fingerprint configuration:
    //   - "string_fingerprint_fields": [...] → Include(set)
    //   - "string_fingerprint_exclude_fields": [...] → Exclude(set)
    //   - Fall back to "string_hash_optimization" bool → All or None
    //   - Default → All
    let string_fingerprint_mode = if let Some(arr) = parsed.get("string_fingerprint_fields").and_then(|v| v.as_array()) {
        let set: HashSet<String> = arr.iter().filter_map(|v| v.as_str().map(String::from)).collect();
        if set.is_empty() {
            StringFingerprintMode::None
        } else {
            StringFingerprintMode::Include(set)
        }
    } else if let Some(arr) = parsed.get("string_fingerprint_exclude_fields").and_then(|v| v.as_array()) {
        let set: HashSet<String> = arr.iter().filter_map(|v| v.as_str().map(String::from)).collect();
        StringFingerprintMode::Exclude(set)
    } else {
        match parsed.get("string_hash_optimization").and_then(|v| v.as_bool()) {
            Some(false) => StringFingerprintMode::None,
            _ => StringFingerprintMode::All,
        }
    };

    let fieldnorms_enabled = parsed.get("fieldnorms_enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    Ok(CreateFromParquetConfig {
        table_root,
        fast_field_mode,
        schema_config: SchemaDerivationConfig {
            fast_field_mode,
            skip_fields,
            tokenizer_overrides,
            ip_address_fields,
            json_fields,
            fieldnorms_enabled,
            store_fields: false, // Companion mode: parquet is the store
        },
        statistics_fields,
        statistics_truncate_length,
        field_id_mapping,
        auto_detect_name_mapping,
        index_uid,
        source_id,
        node_id,
        writer_heap_size,
        reader_batch_size,
        string_fingerprint_mode,
    })
}

/// Create a Java Map<String, ColumnStatistics> from Rust statistics results.
fn create_java_column_statistics_map<'a>(
    env: &mut JNIEnv<'a>,
    stats: &[ColumnStatisticsResult],
) -> anyhow::Result<JObject<'a>> {
    let hash_map_class = env.find_class("java/util/HashMap")?;
    let stats_map = env.new_object(&hash_map_class, "()V", &[])?;

    let col_stats_class = env.find_class("io/indextables/tantivy4java/split/ColumnStatistics")?;

    for stat in stats {
        let field_name_jstr = string_to_jstring(env, &stat.field_name)?;
        let field_type_jstr = string_to_jstring(env, &stat.field_type)?;

        // Create ColumnStatistics(String fieldName, String fieldType)
        let stat_obj = env.new_object(
            &col_stats_class,
            "(Ljava/lang/String;Ljava/lang/String;)V",
            &[
                JValue::Object(&field_name_jstr.into()),
                JValue::Object(&field_type_jstr.into()),
            ],
        )?;

        // Set nullable fields via setters
        if let Some(v) = stat.min_long {
            env.call_method(&stat_obj, "setMinLong", "(J)V", &[JValue::Long(v)])?;
        }
        if let Some(v) = stat.max_long {
            env.call_method(&stat_obj, "setMaxLong", "(J)V", &[JValue::Long(v)])?;
        }
        if let Some(v) = stat.min_double {
            env.call_method(&stat_obj, "setMinDouble", "(D)V", &[JValue::Double(v)])?;
        }
        if let Some(v) = stat.max_double {
            env.call_method(&stat_obj, "setMaxDouble", "(D)V", &[JValue::Double(v)])?;
        }
        if let Some(ref v) = stat.min_string {
            let jstr = string_to_jstring(env, v)?;
            env.call_method(&stat_obj, "setMinString", "(Ljava/lang/String;)V",
                &[JValue::Object(&jstr.into())])?;
        }
        if let Some(ref v) = stat.max_string {
            let jstr = string_to_jstring(env, v)?;
            env.call_method(&stat_obj, "setMaxString", "(Ljava/lang/String;)V",
                &[JValue::Object(&jstr.into())])?;
        }
        if let Some(v) = stat.min_timestamp_micros {
            env.call_method(&stat_obj, "setMinTimestampMicros", "(J)V", &[JValue::Long(v)])?;
        }
        if let Some(v) = stat.max_timestamp_micros {
            env.call_method(&stat_obj, "setMaxTimestampMicros", "(J)V", &[JValue::Long(v)])?;
        }
        if let Some(v) = stat.min_bool {
            env.call_method(&stat_obj, "setMinBool", "(Z)V", &[JValue::Bool(v as u8)])?;
        }
        if let Some(v) = stat.max_bool {
            env.call_method(&stat_obj, "setMaxBool", "(Z)V", &[JValue::Bool(v as u8)])?;
        }
        env.call_method(&stat_obj, "setNullCount", "(J)V", &[JValue::Long(stat.null_count as i64)])?;

        // Put into map
        let key_jstr = string_to_jstring(env, &stat.field_name)?;
        env.call_method(
            &stats_map,
            "put",
            "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
            &[JValue::Object(&key_jstr.into()), JValue::Object(&stat_obj)],
        )?;
    }

    Ok(stats_map)
}

/// JNI helper for tests: create a parquet file with test data.
/// Schema: id (i64), name (utf8), score (f64), active (bool), category (utf8)
/// Rows: num_rows rows, ids starting from id_offset.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeWriteTestParquet(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    num_rows: jni::sys::jint,
    id_offset: jni::sys::jlong,
) {
    convert_throwable(&mut env, |env| {
        let path_str = jstring_to_string(env, &path)?;

        use arrow_array::*;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let num = num_rows as usize;
        let offset = id_offset;

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("category", DataType::Utf8, false),
        ]));

        let ids: Vec<i64> = (0..num).map(|i| offset + i as i64).collect();
        let names: Vec<String> = (0..num)
            .map(|i| format!("item_{}", offset + i as i64))
            .collect();
        let scores: Vec<f64> = (0..num).map(|i| (i as f64) * 1.5 + 10.0).collect();
        let actives: Vec<bool> = (0..num).map(|i| i % 2 == 0).collect();
        let categories: Vec<String> = (0..num)
            .map(|i| format!("cat_{}", i % 5))
            .collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(
                    names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(scores)),
                Arc::new(BooleanArray::from(actives)),
                Arc::new(StringArray::from(
                    categories.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
            ],
        )
        .map_err(|e| anyhow!("Failed to create RecordBatch: {}", e))?;

        let file = std::fs::File::create(&path_str)
            .map_err(|e| anyhow!("Failed to create file '{}': {}", path_str, e))?;
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .map_err(|e| anyhow!("Failed to create ArrowWriter: {}", e))?;
        writer
            .write(&batch)
            .map_err(|e| anyhow!("Failed to write batch: {}", e))?;
        writer
            .close()
            .map_err(|e| anyhow!("Failed to close writer: {}", e))?;

        Ok(())
    });
}

/// JNI helper for tests: create a parquet file WITHOUT offset index (legacy format).
/// Same schema as nativeWriteTestParquet but with offset index explicitly disabled,
/// simulating legacy parquet files that don't have page-level offset information.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeWriteTestParquetNoPageIndex(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    num_rows: jni::sys::jint,
    id_offset: jni::sys::jlong,
) {
    convert_throwable(&mut env, |env| {
        let path_str = jstring_to_string(env, &path)?;

        use arrow_array::*;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;

        let num = num_rows as usize;
        let offset = id_offset;

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("category", DataType::Utf8, false),
        ]));

        let ids: Vec<i64> = (0..num).map(|i| offset + i as i64).collect();
        let names: Vec<String> = (0..num)
            .map(|i| format!("item_{}", offset + i as i64))
            .collect();
        let scores: Vec<f64> = (0..num).map(|i| (i as f64) * 1.5 + 10.0).collect();
        let actives: Vec<bool> = (0..num).map(|i| i % 2 == 0).collect();
        let categories: Vec<String> = (0..num)
            .map(|i| format!("cat_{}", i % 5))
            .collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(
                    names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(scores)),
                Arc::new(BooleanArray::from(actives)),
                Arc::new(StringArray::from(
                    categories.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
            ],
        )
        .map_err(|e| anyhow!("Failed to create RecordBatch: {}", e))?;

        // Explicitly disable offset index to simulate legacy parquet files
        let props = WriterProperties::builder()
            .set_offset_index_disabled(true)
            .build();

        let file = std::fs::File::create(&path_str)
            .map_err(|e| anyhow!("Failed to create file '{}': {}", path_str, e))?;
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))
            .map_err(|e| anyhow!("Failed to create ArrowWriter: {}", e))?;
        writer
            .write(&batch)
            .map_err(|e| anyhow!("Failed to write batch: {}", e))?;
        writer
            .close()
            .map_err(|e| anyhow!("Failed to close writer: {}", e))?;

        Ok(())
    });
}

/// JNI helper for tests: create a parquet file with ALL data types including complex ones.
/// Schema: id (i64), name (utf8), score (f64), active (bool),
///         created_at (timestamp micros), tags (list<utf8>),
///         address (struct{city:utf8, zip:utf8}), notes (utf8 nullable with nulls)
/// Rows: num_rows rows, ids starting from id_offset.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeWriteTestParquetComplex(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    num_rows: jni::sys::jint,
    id_offset: jni::sys::jlong,
) {
    let _ = convert_throwable(&mut env, |env| {
        let path_str = jstring_to_string(env, &path)?;

        use arrow_array::*;
        use arrow_array::builder::*;
        use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema, TimeUnit};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let num = num_rows as usize;
        let offset = id_offset;

        // Schema with all types
        let tags_field = Field::new("item", DataType::Utf8, true);
        let address_fields = Fields::from(vec![
            Field::new("city", DataType::Utf8, true),
            Field::new("zip", DataType::Utf8, true),
        ]);

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("tags", DataType::List(Arc::new(tags_field)), true),
            Field::new("address", DataType::Struct(address_fields.clone()), true),
            Field::new("notes", DataType::Utf8, true),
        ]));

        // Build arrays
        let ids: Vec<i64> = (0..num).map(|i| offset + i as i64).collect();
        let names: Vec<String> = (0..num).map(|i| format!("item_{}", offset + i as i64)).collect();
        let scores: Vec<f64> = (0..num).map(|i| (i as f64) * 1.5 + 10.0).collect();
        let actives: Vec<bool> = (0..num).map(|i| i % 2 == 0).collect();

        // Timestamps: 2024-01-01T00:00:00Z + i hours (in microseconds)
        let base_ts: i64 = 1704067200_000_000; // 2024-01-01T00:00:00Z in micros
        let timestamps: Vec<i64> = (0..num).map(|i| base_ts + (i as i64) * 3_600_000_000).collect();

        // Tags: list of strings ["tag_a", "tag_b"] or ["tag_c"] alternating
        let mut tags_builder = ListBuilder::new(StringBuilder::new());
        for i in 0..num {
            if i % 3 == 2 {
                // Every 3rd row: null tags
                tags_builder.append(false);
            } else if i % 2 == 0 {
                tags_builder.values().append_value(format!("tag_a_{}", i));
                tags_builder.values().append_value(format!("tag_b_{}", i));
                tags_builder.append(true);
            } else {
                tags_builder.values().append_value(format!("tag_c_{}", i));
                tags_builder.append(true);
            }
        }
        let tags_array = tags_builder.finish();

        // Address: struct {city, zip}
        let cities: Vec<Option<&str>> = (0..num).map(|i| {
            match i % 3 {
                0 => Some("New York"),
                1 => Some("London"),
                _ => Some("Tokyo"),
            }
        }).collect();
        let zips: Vec<Option<&str>> = (0..num).map(|i| {
            match i % 3 {
                0 => Some("10001"),
                1 => Some("EC1A"),
                _ => Some("100-0001"),
            }
        }).collect();
        let city_array = Arc::new(StringArray::from(cities)) as ArrayRef;
        let zip_array = Arc::new(StringArray::from(zips)) as ArrayRef;
        let address_array = StructArray::try_new(
            address_fields,
            vec![city_array, zip_array],
            None, // all non-null
        ).map_err(|e| anyhow!("Failed to create struct array: {}", e))?;

        // Notes: nullable text, every other row is null
        let notes: Vec<Option<String>> = (0..num).map(|i| {
            if i % 2 == 0 {
                Some(format!("Note for item {}", offset + i as i64))
            } else {
                None
            }
        }).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(Float64Array::from(scores)),
                Arc::new(BooleanArray::from(actives)),
                Arc::new(TimestampMicrosecondArray::from(timestamps)),
                Arc::new(tags_array),
                Arc::new(address_array),
                Arc::new(StringArray::from(notes)),
            ],
        ).map_err(|e| anyhow!("Failed to create RecordBatch: {}", e))?;

        let file = std::fs::File::create(&path_str)
            .map_err(|e| anyhow!("Failed to create file '{}': {}", path_str, e))?;
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .map_err(|e| anyhow!("Failed to create ArrowWriter: {}", e))?;
        writer.write(&batch).map_err(|e| anyhow!("Failed to write batch: {}", e))?;
        writer.close().map_err(|e| anyhow!("Failed to close writer: {}", e))?;

        Ok(())
    });
}

/// JNI helper for tests: create a complex parquet file WITHOUT a native offset index.
/// Same schema as nativeWriteTestParquetComplex but with offset index disabled,
/// simulating legacy parquet files that require manifest-sourced page locations.
/// This is the key scenario for testing the nested column page location fix:
/// primitive columns get correct manifest page locs, nested columns get empty ones.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeWriteTestParquetComplexNoPageIndex(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    num_rows: jni::sys::jint,
    id_offset: jni::sys::jlong,
) {
    let _ = convert_throwable(&mut env, |env| {
        let path_str = jstring_to_string(env, &path)?;

        use arrow_array::*;
        use arrow_array::builder::*;
        use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema, TimeUnit};
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;

        let num = num_rows as usize;
        let offset = id_offset;

        let tags_field = Field::new("item", DataType::Utf8, true);
        let address_fields = Fields::from(vec![
            Field::new("city", DataType::Utf8, true),
            Field::new("zip", DataType::Utf8, true),
        ]);

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("score", DataType::Float64, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("created_at", DataType::Timestamp(TimeUnit::Microsecond, None), true),
            Field::new("tags", DataType::List(Arc::new(tags_field)), true),
            Field::new("address", DataType::Struct(address_fields.clone()), true),
            Field::new("notes", DataType::Utf8, true),
        ]));

        let ids: Vec<i64> = (0..num).map(|i| offset + i as i64).collect();
        let names: Vec<String> = (0..num).map(|i| format!("item_{}", offset + i as i64)).collect();
        let scores: Vec<f64> = (0..num).map(|i| (i as f64) * 1.5 + 10.0).collect();
        let actives: Vec<bool> = (0..num).map(|i| i % 2 == 0).collect();

        let base_ts: i64 = 1704067200_000_000;
        let timestamps: Vec<i64> = (0..num).map(|i| base_ts + (i as i64) * 3_600_000_000).collect();

        let mut tags_builder = ListBuilder::new(StringBuilder::new());
        for i in 0..num {
            if i % 3 == 2 {
                tags_builder.append(false);
            } else if i % 2 == 0 {
                tags_builder.values().append_value(format!("tag_a_{}", i));
                tags_builder.values().append_value(format!("tag_b_{}", i));
                tags_builder.append(true);
            } else {
                tags_builder.values().append_value(format!("tag_c_{}", i));
                tags_builder.append(true);
            }
        }
        let tags_array = tags_builder.finish();

        let cities: Vec<Option<&str>> = (0..num).map(|i| {
            match i % 3 {
                0 => Some("New York"),
                1 => Some("London"),
                _ => Some("Tokyo"),
            }
        }).collect();
        let zips: Vec<Option<&str>> = (0..num).map(|i| {
            match i % 3 {
                0 => Some("10001"),
                1 => Some("EC1A"),
                _ => Some("100-0001"),
            }
        }).collect();
        let city_array = Arc::new(StringArray::from(cities)) as ArrayRef;
        let zip_array = Arc::new(StringArray::from(zips)) as ArrayRef;
        let address_array = StructArray::try_new(
            address_fields,
            vec![city_array, zip_array],
            None,
        ).map_err(|e| anyhow!("Failed to create struct array: {}", e))?;

        let notes: Vec<Option<String>> = (0..num).map(|i| {
            if i % 2 == 0 {
                Some(format!("Note for item {}", offset + i as i64))
            } else {
                None
            }
        }).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(Float64Array::from(scores)),
                Arc::new(BooleanArray::from(actives)),
                Arc::new(TimestampMicrosecondArray::from(timestamps)),
                Arc::new(tags_array),
                Arc::new(address_array),
                Arc::new(StringArray::from(notes)),
            ],
        ).map_err(|e| anyhow!("Failed to create RecordBatch: {}", e))?;

        // Explicitly disable offset index to simulate legacy parquet files
        let props = WriterProperties::builder()
            .set_offset_index_disabled(true)
            .build();

        let file = std::fs::File::create(&path_str)
            .map_err(|e| anyhow!("Failed to create file '{}': {}", path_str, e))?;
        let mut writer = ArrowWriter::try_new(file, schema, Some(props))
            .map_err(|e| anyhow!("Failed to create ArrowWriter: {}", e))?;
        writer.write(&batch).map_err(|e| anyhow!("Failed to write batch: {}", e))?;
        writer.close().map_err(|e| anyhow!("Failed to close writer: {}", e))?;

        Ok(())
    });
}

/// JNI helper for tests: create a parquet file with IP address columns (stored as UTF8 strings).
/// Schema: id (i64), src_ip (utf8), dst_ip (utf8), port (i64), label (utf8)
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeWriteTestParquetWithIps(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    num_rows: jni::sys::jint,
    id_offset: jni::sys::jlong,
) {
    let _ = convert_throwable(&mut env, |env| {
        let path_str = jstring_to_string(env, &path)?;

        use arrow_array::*;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let num = num_rows as usize;
        let offset = id_offset;

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("src_ip", DataType::Utf8, false),
            Field::new("dst_ip", DataType::Utf8, false),
            Field::new("port", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]));

        let ids: Vec<i64> = (0..num).map(|i| offset + i as i64).collect();
        let src_ips: Vec<String> = (0..num)
            .map(|i| format!("10.0.{}.{}", i / 256, i % 256))
            .collect();
        let dst_ips: Vec<String> = (0..num)
            .map(|i| format!("192.168.{}.{}", i / 256, i % 256))
            .collect();
        let ports: Vec<i64> = (0..num).map(|i| 8000 + (i as i64 % 100)).collect();
        let labels: Vec<String> = (0..num).map(|i| format!("conn_{}", i)).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(src_ips.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(dst_ips.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(Int64Array::from(ports)),
                Arc::new(StringArray::from(labels.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
            ],
        ).map_err(|e| anyhow!("Failed to create RecordBatch: {}", e))?;

        let file = std::fs::File::create(&path_str)
            .map_err(|e| anyhow!("Failed to create file '{}': {}", path_str, e))?;
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .map_err(|e| anyhow!("Failed to create ArrowWriter: {}", e))?;
        writer.write(&batch).map_err(|e| anyhow!("Failed to write batch: {}", e))?;
        writer.close().map_err(|e| anyhow!("Failed to close writer: {}", e))?;

        Ok(())
    });
}

/// JNI helper for tests: create a parquet file with JSON string columns (stored as UTF8).
/// Schema: id (i64), name (utf8), payload (utf8 — JSON objects), metadata (utf8 — JSON objects)
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeWriteTestParquetWithJsonStrings(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    num_rows: jni::sys::jint,
    id_offset: jni::sys::jlong,
) {
    let _ = convert_throwable(&mut env, |env| {
        let path_str = jstring_to_string(env, &path)?;

        use arrow_array::*;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let num = num_rows as usize;
        let offset = id_offset;

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("payload", DataType::Utf8, false),
            Field::new("metadata", DataType::Utf8, false),
        ]));

        let ids: Vec<i64> = (0..num).map(|i| offset + i as i64).collect();
        let names: Vec<String> = (0..num)
            .map(|i| format!("item_{}", offset + i as i64))
            .collect();
        let payloads: Vec<String> = (0..num)
            .map(|i| {
                let idx = offset + i as i64;
                format!(
                    r#"{{"user":"user_{}","score":{},"active":{},"tags":["tag_a","tag_b"]}}"#,
                    idx,
                    idx * 10,
                    idx % 2 == 0,
                )
            })
            .collect();
        let metadatas: Vec<String> = (0..num)
            .map(|i| {
                let idx = offset + i as i64;
                format!(
                    r#"{{"region":"us-east-{}","version":{}}}"#,
                    idx % 3,
                    idx,
                )
            })
            .collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(names.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(payloads.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(metadatas.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
            ],
        ).map_err(|e| anyhow!("Failed to create RecordBatch: {}", e))?;

        let file = std::fs::File::create(&path_str)
            .map_err(|e| anyhow!("Failed to create file '{}': {}", path_str, e))?;
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .map_err(|e| anyhow!("Failed to create ArrowWriter: {}", e))?;
        writer.write(&batch).map_err(|e| anyhow!("Failed to write batch: {}", e))?;
        writer.close().map_err(|e| anyhow!("Failed to close writer: {}", e))?;

        Ok(())
    });
}

/// JNI helper for tests: create a parquet file covering ALL data types.
/// Schema:
///   id (i64), uint_val (u64), float_val (f64), bool_val (bool),
///   text_val (utf8), binary_val (binary), ts_val (timestamp_us),
///   date_val (date32), ip_val (utf8, for IP), tags (list<utf8>),
///   address (struct{city: utf8, zip: utf8}), props (map<utf8, utf8>)
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeWriteTestParquetAllTypes(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    num_rows: jni::sys::jint,
    id_offset: jni::sys::jlong,
) {
    let _ = convert_throwable(&mut env, |env| {
        let path_str = jstring_to_string(env, &path)?;

        use arrow_array::*;
        use arrow_array::builder::*;
        use arrow_schema::{DataType, Field, Fields, Schema as ArrowSchema};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let num = num_rows as usize;
        let offset = id_offset;

        // Map type: Map<Utf8, Utf8>
        let map_field = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Utf8, true),
            ])),
            false,
        );

        let schema = Arc::new(ArrowSchema::new(vec![
            // Original 12 columns
            Field::new("id", DataType::Int64, false),
            Field::new("uint_val", DataType::UInt64, false),
            Field::new("float_val", DataType::Float64, true),
            Field::new("bool_val", DataType::Boolean, true),
            Field::new("text_val", DataType::Utf8, false),
            Field::new("binary_val", DataType::Binary, true),
            Field::new("ts_val", DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None), true),
            Field::new("date_val", DataType::Date32, true),
            Field::new("ip_val", DataType::Utf8, false),
            Field::new("tags", DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))), true),
            Field::new("address", DataType::Struct(Fields::from(vec![
                Field::new("city", DataType::Utf8, true),
                Field::new("zip", DataType::Utf8, true),
            ])), true),
            Field::new("props", DataType::Map(Arc::new(map_field), false), true),
            // 13 additional columns for wide-schema coverage (total: 25)
            Field::new("description", DataType::Utf8, false),       // text for default tokenizer
            Field::new("src_ip", DataType::Utf8, false),            // second IP field
            Field::new("event_time", DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None), true),
            Field::new("category", DataType::Utf8, false),          // categorical text (raw)
            Field::new("status_code", DataType::Int32, true),       // i32→I64
            Field::new("amount", DataType::Float64, true),          // financial amounts
            Field::new("priority", DataType::Int16, true),          // i16→I64
            Field::new("latitude", DataType::Float32, true),        // f32→F64
            Field::new("longitude", DataType::Float32, true),       // f32→F64
            Field::new("region", DataType::Utf8, false),            // region names (raw)
            Field::new("is_active", DataType::Boolean, false),      // non-nullable bool
            Field::new("retry_count", DataType::UInt32, true),      // u32→U64
            Field::new("large_text", DataType::LargeUtf8, true),    // large utf8 variant
        ]));

        // id: sequential from offset
        let ids: Vec<i64> = (0..num).map(|i| offset + i as i64).collect();
        // uint_val: large unsigned values
        let uints: Vec<u64> = (0..num).map(|i| 1_000_000_000u64 + i as u64 * 7).collect();
        // float_val: with NaN on every 10th row, null on every 7th
        let floats: Vec<Option<f64>> = (0..num).map(|i| {
            if i % 7 == 6 { None }
            else if i % 10 == 9 { Some(f64::NAN) }
            else { Some(i as f64 * 2.5 + 0.1) }
        }).collect();
        // bool_val: alternating, null every 5th
        let bools: Vec<Option<bool>> = (0..num).map(|i| {
            if i % 5 == 4 { None } else { Some(i % 2 == 0) }
        }).collect();
        // text_val
        let texts: Vec<String> = (0..num).map(|i| format!("text_{}", offset + i as i64)).collect();
        // binary_val: raw bytes, null every 4th
        let binaries: Vec<Option<Vec<u8>>> = (0..num).map(|i| {
            if i % 4 == 3 { None }
            else { Some(format!("bin_{}", i).into_bytes()) }
        }).collect();
        // ts_val: base 2024-01-01T00:00:00Z + i hours, null every 8th
        let base_ts: i64 = 1_704_067_200_000_000; // 2024-01-01T00:00:00Z in microseconds
        let timestamps: Vec<Option<i64>> = (0..num).map(|i| {
            if i % 8 == 7 { None }
            else { Some(base_ts + i as i64 * 3_600_000_000) }
        }).collect();
        // date_val: days since epoch, null every 6th
        let dates: Vec<Option<i32>> = (0..num).map(|i| {
            if i % 6 == 5 { None }
            else { Some(19723 + i as i32) } // 2024-01-01 = day 19723
        }).collect();
        // ip_val: mix of IPv4 and IPv6
        let ips: Vec<String> = (0..num).map(|i| {
            if i % 3 == 2 {
                format!("2001:db8::{:x}", i)
            } else {
                format!("10.0.{}.{}", i / 256, i % 256)
            }
        }).collect();

        // Build list (tags) array
        let mut list_builder = ListBuilder::new(StringBuilder::new());
        for i in 0..num {
            if i % 5 == 4 {
                list_builder.append_null();
            } else {
                let values = list_builder.values();
                values.append_value(format!("tag_{}", i % 3));
                if i % 2 == 0 {
                    values.append_value(format!("tag_extra_{}", i));
                }
                list_builder.append(true);
            }
        }
        let tags_array = list_builder.finish();

        // Build struct (address) array
        let cities = ["New York", "London", "Tokyo", "Berlin", "Paris"];
        let zips_arr = ["10001", "EC1A", "100-0001", "10115", "75001"];
        let city_vec: Vec<Option<&str>> = (0..num).map(|i| {
            if i % 9 == 8 { None } else { Some(cities[i % cities.len()]) }
        }).collect();
        let zip_vec: Vec<Option<&str>> = (0..num).map(|i| {
            if i % 9 == 8 { None } else { Some(zips_arr[i % zips_arr.len()]) }
        }).collect();
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("city", DataType::Utf8, true)),
                Arc::new(StringArray::from(city_vec)) as ArrayRef,
            ),
            (
                Arc::new(Field::new("zip", DataType::Utf8, true)),
                Arc::new(StringArray::from(zip_vec)) as ArrayRef,
            ),
        ]);

        // Build map (props) array: Map<Utf8, Utf8>
        let mut map_builder = MapBuilder::new(
            None,
            StringBuilder::new(),
            StringBuilder::new(),
        );
        for i in 0..num {
            if i % 6 == 5 {
                map_builder.append(false).unwrap(); // null map
            } else {
                map_builder.keys().append_value(format!("key_{}", i % 3));
                map_builder.values().append_value(format!("val_{}", i));
                if i % 2 == 0 {
                    map_builder.keys().append_value("extra_key");
                    map_builder.values().append_value(format!("extra_{}", i));
                }
                map_builder.append(true).unwrap();
            }
        }
        let map_array = map_builder.finish();

        // ── Build 13 additional columns ──
        let descriptions: Vec<&str> = (0..num).map(|i| match i % 4 {
            0 => "The quick brown fox jumps over the lazy dog",
            1 => "A fast red car drove past the old house",
            2 => "Searching for data in large distributed systems",
            _ => "Performance testing with multiple column types",
        }).collect();
        let src_ips: Vec<String> = (0..num).map(|i| {
            if i % 4 == 3 { format!("2001:db8:cafe::{:x}", i) }
            else { format!("192.168.{}.{}", i / 256, i % 256) }
        }).collect();
        let event_base_ts: i64 = 1_706_745_600_000_000; // 2024-02-01T00:00:00Z
        let event_times: Vec<Option<i64>> = (0..num).map(|i| {
            if i % 10 == 9 { None }
            else { Some(event_base_ts + i as i64 * 60_000_000) } // +1min each
        }).collect();
        let categories = ["electronics", "clothing", "food", "books", "sports"];
        let cat_vals: Vec<&str> = (0..num).map(|i| categories[i % categories.len()]).collect();
        let status_codes: Vec<Option<i32>> = (0..num).map(|i| {
            if i % 12 == 11 { None } else { Some(match i % 5 { 0 => 200, 1 => 201, 2 => 404, 3 => 500, _ => 302 }) }
        }).collect();
        let amounts: Vec<Option<f64>> = (0..num).map(|i| {
            if i % 11 == 10 { None } else { Some(i as f64 * 9.99 + 1.50) }
        }).collect();
        let priorities: Vec<Option<i16>> = (0..num).map(|i| {
            if i % 15 == 14 { None } else { Some((i % 5) as i16 + 1) }
        }).collect();
        let latitudes: Vec<Option<f32>> = (0..num).map(|i| {
            if i % 13 == 12 { None } else { Some(40.0 + (i as f32 * 0.01)) }
        }).collect();
        let longitudes: Vec<Option<f32>> = (0..num).map(|i| {
            if i % 13 == 12 { None } else { Some(-74.0 + (i as f32 * 0.01)) }
        }).collect();
        let regions = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1", "sa-east-1"];
        let region_vals: Vec<&str> = (0..num).map(|i| regions[i % regions.len()]).collect();
        let is_actives: Vec<bool> = (0..num).map(|i| i % 3 != 0).collect();
        let retry_counts: Vec<Option<u32>> = (0..num).map(|i| {
            if i % 14 == 13 { None } else { Some((i % 4) as u32) }
        }).collect();
        let large_texts: Vec<Option<String>> = (0..num).map(|i| {
            if i % 8 == 7 { None } else { Some(format!("Large text content for row {} with additional padding data for testing", i)) }
        }).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(UInt64Array::from(uints)),
                Arc::new(Float64Array::from(floats)),
                Arc::new(BooleanArray::from(bools)),
                Arc::new(StringArray::from(texts.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(BinaryArray::from(binaries.iter().map(|b| b.as_deref()).collect::<Vec<Option<&[u8]>>>())),
                Arc::new(TimestampMicrosecondArray::from(timestamps)),
                Arc::new(Date32Array::from(dates)),
                Arc::new(StringArray::from(ips.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(tags_array),
                Arc::new(struct_array),
                Arc::new(map_array),
                // 13 new columns
                Arc::new(StringArray::from(descriptions)),
                Arc::new(StringArray::from(src_ips.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(TimestampMicrosecondArray::from(event_times)),
                Arc::new(StringArray::from(cat_vals)),
                Arc::new(Int32Array::from(status_codes)),
                Arc::new(Float64Array::from(amounts)),
                Arc::new(Int16Array::from(priorities)),
                Arc::new(Float32Array::from(latitudes)),
                Arc::new(Float32Array::from(longitudes)),
                Arc::new(StringArray::from(region_vals)),
                Arc::new(BooleanArray::from(is_actives)),
                Arc::new(UInt32Array::from(retry_counts)),
                Arc::new(LargeStringArray::from(large_texts.iter().map(|s| s.as_deref()).collect::<Vec<Option<&str>>>())),
            ],
        ).map_err(|e| anyhow!("Failed to create RecordBatch: {}", e))?;

        let file = std::fs::File::create(&path_str)
            .map_err(|e| anyhow!("Failed to create file '{}': {}", path_str, e))?;
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .map_err(|e| anyhow!("Failed to create ArrowWriter: {}", e))?;
        writer.write(&batch).map_err(|e| anyhow!("Failed to write batch: {}", e))?;
        writer.close().map_err(|e| anyhow!("Failed to close writer: {}", e))?;

        Ok(())
    });
}

/// JNI helper: create a parquet file with 10 realistic fields (4 numeric, 1 date, 1 IP, 5 UUID strings).
/// Schema:
///   num_1 (i64), num_2 (i64), num_3 (f64), num_4 (f64),
///   created_at (timestamp_us),
///   ip_addr (utf8 — IP addresses),
///   uuid_1..uuid_5 (utf8 — unique UUIDs per row)
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeWriteTestParquetWide(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    num_rows: jni::sys::jint,
    id_offset: jni::sys::jlong,
) {
    let _ = convert_throwable(&mut env, |env| {
        let path_str = jstring_to_string(env, &path)?;

        use arrow_array::*;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        anyhow::ensure!(num_rows >= 0, "num_rows must be non-negative, got {}", num_rows);
        anyhow::ensure!(id_offset >= 0, "id_offset must be non-negative, got {}", id_offset);
        let num = num_rows as usize;
        let offset = id_offset as u64;

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("num_1", DataType::Int64, false),
            Field::new("num_2", DataType::Int64, false),
            Field::new("num_3", DataType::Float64, false),
            Field::new("num_4", DataType::Float64, false),
            Field::new("created_at", DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None), false),
            Field::new("ip_addr", DataType::Utf8, false),
            Field::new("uuid_1", DataType::Utf8, false),
            Field::new("uuid_2", DataType::Utf8, false),
            Field::new("uuid_3", DataType::Utf8, false),
            Field::new("uuid_4", DataType::Utf8, false),
            Field::new("uuid_5", DataType::Utf8, false),
        ]));

        // Simple deterministic hash for generating UUID-like strings
        fn mix(seed: u64) -> u64 {
            let mut h = seed;
            h = h.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            h ^= h >> 33;
            h = h.wrapping_mul(0xff51afd7ed558ccd);
            h ^= h >> 33;
            h
        }

        fn fake_uuid(row: u64, col: u64) -> String {
            let a = mix(row.wrapping_mul(31).wrapping_add(col.wrapping_mul(1000003)));
            let b = mix(a);
            format!(
                "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
                (a >> 32) as u32,
                (a >> 16) as u16,
                a as u16 & 0x0FFF,
                ((b >> 48) as u16 & 0x3FFF) | 0x8000,
                b & 0xFFFF_FFFFFFFF
            )
        }

        let num_1: Vec<i64> = (0..num).map(|i| (offset + i as u64) as i64).collect();
        let num_2: Vec<i64> = (0..num).map(|i| ((i as i64 * 997) % 100_000) + 1).collect();
        let num_3: Vec<f64> = (0..num).map(|i| (i as f64 * 3.14159) + 0.01).collect();
        let num_4: Vec<f64> = (0..num).map(|i| ((i as f64 * 2.71828).sin() * 1000.0).round() / 100.0).collect();

        // Timestamps: 2024-01-01 + i seconds
        let base_ts: i64 = 1_704_067_200_000_000; // 2024-01-01T00:00:00Z in microseconds
        let timestamps: Vec<i64> = (0..num).map(|i| base_ts + i as i64 * 1_000_000).collect();

        // IP addresses: deterministic IPv4
        let ips: Vec<String> = (0..num).map(|i| {
            format!("{}.{}.{}.{}",
                10 + (i / (256 * 256)) % 246,
                (i / 256) % 256,
                i % 256,
                (i * 7 + 3) % 256)
        }).collect();

        // 5 UUID columns — each row x column produces a unique UUID
        let uuid_1: Vec<String> = (0..num).map(|i| fake_uuid(offset + i as u64, 1)).collect();
        let uuid_2: Vec<String> = (0..num).map(|i| fake_uuid(offset + i as u64, 2)).collect();
        let uuid_3: Vec<String> = (0..num).map(|i| fake_uuid(offset + i as u64, 3)).collect();
        let uuid_4: Vec<String> = (0..num).map(|i| fake_uuid(offset + i as u64, 4)).collect();
        let uuid_5: Vec<String> = (0..num).map(|i| fake_uuid(offset + i as u64, 5)).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(num_1)),
                Arc::new(Int64Array::from(num_2)),
                Arc::new(Float64Array::from(num_3)),
                Arc::new(Float64Array::from(num_4)),
                Arc::new(TimestampMicrosecondArray::from(timestamps)),
                Arc::new(StringArray::from(ips.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(uuid_1.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(uuid_2.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(uuid_3.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(uuid_4.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(uuid_5.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
            ],
        ).map_err(|e| anyhow!("Failed to create wide RecordBatch: {}", e))?;

        let file = std::fs::File::create(&path_str)
            .map_err(|e| anyhow!("Failed to create file '{}': {}", path_str, e))?;
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .map_err(|e| anyhow!("Failed to create ArrowWriter: {}", e))?;
        writer.write(&batch).map_err(|e| anyhow!("write wide parquet: {}", e))?;
        writer.close().map_err(|e| anyhow!("close wide parquet: {}", e))?;

        Ok(())
    });
}

/// Test helper: write a parquet file for string indexing mode testing.
///
/// Schema (5 columns):
///   id (i64), trace_id (utf8 — pure UUIDs),
///   message (utf8 — text with embedded UUID),
///   error_log (utf8 — text with ERR-XXXX pattern),
///   category (utf8 — cycling "info"/"warn"/"error")
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeWriteTestParquetForStringIndexing(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
    num_rows: jni::sys::jint,
    id_offset: jni::sys::jlong,
) {
    let _ = convert_throwable(&mut env, |env| {
        let path_str = jstring_to_string(env, &path)?;

        use arrow_array::*;
        use arrow_schema::{DataType, Field, Schema as ArrowSchema};
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        anyhow::ensure!(num_rows >= 0, "num_rows must be non-negative, got {}", num_rows);
        anyhow::ensure!(id_offset >= 0, "id_offset must be non-negative, got {}", id_offset);
        let num = num_rows as usize;
        let offset = id_offset as u64;

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("trace_id", DataType::Utf8, false),
            Field::new("message", DataType::Utf8, false),
            Field::new("error_log", DataType::Utf8, false),
            Field::new("category", DataType::Utf8, false),
        ]));

        // Simple deterministic hash for generating UUID-like strings
        fn mix(seed: u64) -> u64 {
            let mut h = seed;
            h = h.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            h ^= h >> 33;
            h = h.wrapping_mul(0xff51afd7ed558ccd);
            h ^= h >> 33;
            h
        }

        fn fake_uuid(row: u64, col: u64) -> String {
            let a = mix(row.wrapping_mul(31).wrapping_add(col.wrapping_mul(1000003)));
            let b = mix(a);
            format!(
                "{:08x}-{:04x}-4{:03x}-{:04x}-{:012x}",
                (a >> 32) as u32,
                (a >> 16) as u16,
                a as u16 & 0x0FFF,
                ((b >> 48) as u16 & 0x3FFF) | 0x8000,
                b & 0xFFFF_FFFFFFFF
            )
        }

        let ids: Vec<i64> = (0..num).map(|i| (offset + i as u64) as i64).collect();

        // trace_id: pure UUIDs (one per row)
        let trace_ids: Vec<String> = (0..num).map(|i| fake_uuid(offset + i as u64, 1)).collect();

        // message: text with an embedded UUID
        let actions = ["login", "search", "purchase", "logout", "upload"];
        let messages: Vec<String> = (0..num).map(|i| {
            let uuid = fake_uuid(offset + i as u64, 2);
            format!("Request {} completed action {} for user_{}", uuid, actions[i % 5], i)
        }).collect();

        // error_log: text with ERR-XXXX custom pattern (use global offset for uniqueness across splits)
        let error_logs: Vec<String> = (0..num).map(|i| {
            let row = offset + i as u64;
            format!("ERR-{:04}: Connection to host_{}.example.com timed out after {}ms",
                    row, row % 10, 100 + row)
        }).collect();

        let categories = ["info", "warn", "error"];
        let category_vals: Vec<String> = (0..num).map(|i| categories[i % 3].to_string()).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(trace_ids.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(messages.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(error_logs.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
                Arc::new(StringArray::from(category_vals.iter().map(|s| s.as_str()).collect::<Vec<_>>())),
            ],
        ).map_err(|e| anyhow!("Failed to create string-indexing RecordBatch: {}", e))?;

        let file = std::fs::File::create(&path_str)
            .map_err(|e| anyhow!("Failed to create file '{}': {}", path_str, e))?;
        let mut writer = ArrowWriter::try_new(file, schema, None)
            .map_err(|e| anyhow!("Failed to create ArrowWriter: {}", e))?;
        writer.write(&batch).map_err(|e| anyhow!("write string-indexing parquet: {}", e))?;
        writer.close().map_err(|e| anyhow!("close string-indexing parquet: {}", e))?;

        Ok(())
    });
}


// ============================================================================
// Arrow FFI Split Creation JNI Entry Points
// ============================================================================

use std::sync::{Arc, Mutex};
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use crate::parquet_companion::arrow_ffi_import::{
    ArrowFfiSplitContext, import_arrow_schema, import_arrow_batch,
    begin_split_from_arrow, add_arrow_batch, finish_all_splits, cancel_split,
    PartitionSplitResult,
};
use jni::sys::jlong;

/// Begin creating splits from Arrow columnar data via FFI.
///
/// Imports the Arrow schema from the FFI address, extracts partition column names,
/// and creates an ArrowFfiSplitContext with partition routing.
///
/// Returns a context handle for subsequent addBatch/finish/cancel calls.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeBeginSplitFromArrow(
    mut env: JNIEnv,
    _class: JClass,
    schema_addr: jlong,
    partition_columns: jni::sys::jobjectArray,
    heap_size: jlong,
) -> jlong {
    convert_throwable(&mut env, |env| {
        // Import Arrow schema from FFI address
        let schema_ptr = schema_addr as *mut FFI_ArrowSchema;
        let arrow_schema = import_arrow_schema(schema_ptr)?;

        // Extract partition column names from Java String array
        let partition_cols_obj = unsafe { jni::objects::JObjectArray::from_raw(partition_columns) };
        let num_partition_cols = env.get_array_length(&partition_cols_obj)? as usize;
        let mut partition_col_names = Vec::with_capacity(num_partition_cols);
        for i in 0..num_partition_cols {
            let jstr = env.get_object_array_element(&partition_cols_obj, i as i32)?;
            let name = jstring_to_string(env, &jstr.into())?;
            partition_col_names.push(name);
        }

        let heap = if heap_size <= 0 { 50_000_000usize } else { heap_size as usize };

        debug_log!("ARROW_FFI_IMPORT: begin_split_from_arrow with {} columns, {} partition cols, heap={}",
            arrow_schema.fields().len(), partition_col_names.len(), heap);

        let ctx = begin_split_from_arrow(arrow_schema, &partition_col_names, heap)?;
        let ctx_arc: Arc<Mutex<ArrowFfiSplitContext>> = Arc::new(Mutex::new(ctx));
        let handle = arc_to_jlong(ctx_arc);

        Ok(handle)
    }).unwrap_or(0)
}

/// Add a batch of Arrow columnar data via FFI.
///
/// Imports the RecordBatch from FFI addresses, then routes rows to per-partition writers.
/// Returns cumulative total doc count across all partitions.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeAddArrowBatch(
    mut env: JNIEnv,
    _class: JClass,
    ctx_handle: jlong,
    array_addr: jlong,
    schema_addr: jlong,
) -> jlong {
    convert_throwable(&mut env, |_env| {
        // Import RecordBatch from FFI
        let array_ptr = array_addr as *mut FFI_ArrowArray;
        let schema_ptr = schema_addr as *mut FFI_ArrowSchema;
        let batch = import_arrow_batch(array_ptr, schema_ptr)?;

        // Retrieve context from registry
        let ctx_arc = crate::utils::jlong_to_arc::<Mutex<ArrowFfiSplitContext>>(ctx_handle)
            .ok_or_else(|| anyhow!("Invalid context handle for addArrowBatch"))?;
        let mut ctx = ctx_arc.lock()
            .map_err(|e| anyhow!("Failed to lock context: {}", e))?;

        let count = add_arrow_batch(&mut ctx, &batch)?;
        Ok(count as jlong)
    }).unwrap_or(-1)
}

/// Finalize ALL partition splits, writing each to outputDir.
///
/// Returns a Java ArrayList<HashMap<String, Object>> where each entry contains:
/// - "partitionKey" → String
/// - "partitionValues" → HashMap<String, String>
/// - "splitPath" → String
/// - "numDocs" → Long
/// - "splitId" → String
/// - "footerStartOffset" → Long
/// - "footerEndOffset" → Long
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeFinishAllSplits(
    mut env: JNIEnv,
    _class: JClass,
    ctx_handle: jlong,
    output_dir: JString,
) -> jobject {
    convert_throwable(&mut env, |env| {
        let output_dir_str = jstring_to_string(env, &output_dir)?;

        // Retrieve context clone from registry, then release the registry entry
        let ctx_arc = crate::utils::jlong_to_arc::<Mutex<ArrowFfiSplitContext>>(ctx_handle)
            .ok_or_else(|| anyhow!("Invalid context handle for finishAllSplits"))?;
        release_arc(ctx_handle); // Remove from registry; ctx_arc is now the sole owner

        // Extract the context from Arc<Mutex<>>
        let ctx = Arc::try_unwrap(ctx_arc)
            .map_err(|_| anyhow!("Context still has multiple references"))?
            .into_inner()
            .map_err(|e| anyhow!("Failed to unwrap mutex: {}", e))?;

        // Create a default SplitConfig (JNI layer uses defaults; future: accept from Java)
        let split_config = default_split_config("arrow-ffi", "arrow-ffi-source", "arrow-ffi-node");

        // Run async finish in tokio runtime
        let results = QuickwitRuntimeManager::global().handle()
            .block_on(finish_all_splits(ctx, &output_dir_str, &split_config))?;

        // Convert results to Java ArrayList<HashMap<String, Object>>
        let array_list_class = env.find_class("java/util/ArrayList")?;
        let result_list = env.new_object(&array_list_class, "()V", &[])?;

        let hash_map_class = env.find_class("java/util/HashMap")?;

        for result in &results {
            let map = env.new_object(&hash_map_class, "()V", &[])?;

            // partitionKey
            let key_jstr = string_to_jstring(env, &result.partition_key)?;
            let key_key = string_to_jstring(env, "partitionKey")?;
            env.call_method(&map, "put",
                "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
                &[JValue::Object(&key_key.into()), JValue::Object(&key_jstr.into())])?;

            // splitPath
            let path_jstr = string_to_jstring(env, &result.split_path)?;
            let path_key = string_to_jstring(env, "splitPath")?;
            env.call_method(&map, "put",
                "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
                &[JValue::Object(&path_key.into()), JValue::Object(&path_jstr.into())])?;

            // splitId
            let id_jstr = string_to_jstring(env, &result.metadata.split_id)?;
            let id_key = string_to_jstring(env, "splitId")?;
            env.call_method(&map, "put",
                "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
                &[JValue::Object(&id_key.into()), JValue::Object(&id_jstr.into())])?;

            // numDocs as Long object
            let long_class = env.find_class("java/lang/Long")?;
            let num_docs_obj = env.new_object(&long_class, "(J)V",
                &[JValue::Long(result.metadata.num_docs as i64)])?;
            let num_docs_key = string_to_jstring(env, "numDocs")?;
            env.call_method(&map, "put",
                "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
                &[JValue::Object(&num_docs_key.into()), JValue::Object(&num_docs_obj)])?;

            // footerStartOffset
            let footer_start = result.metadata.footer_start_offset.unwrap_or(0) as i64;
            let footer_start_obj = env.new_object(&long_class, "(J)V",
                &[JValue::Long(footer_start)])?;
            let footer_start_key = string_to_jstring(env, "footerStartOffset")?;
            env.call_method(&map, "put",
                "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
                &[JValue::Object(&footer_start_key.into()), JValue::Object(&footer_start_obj)])?;

            // footerEndOffset
            let footer_end = result.metadata.footer_end_offset.unwrap_or(0) as i64;
            let footer_end_obj = env.new_object(&long_class, "(J)V",
                &[JValue::Long(footer_end)])?;
            let footer_end_key = string_to_jstring(env, "footerEndOffset")?;
            env.call_method(&map, "put",
                "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
                &[JValue::Object(&footer_end_key.into()), JValue::Object(&footer_end_obj)])?;

            // partitionValues as nested HashMap
            let pv_map = env.new_object(&hash_map_class, "()V", &[])?;
            for (col, val) in &result.partition_values {
                let col_jstr = string_to_jstring(env, col)?;
                let val_jstr = string_to_jstring(env, val)?;
                env.call_method(&pv_map, "put",
                    "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
                    &[JValue::Object(&col_jstr.into()), JValue::Object(&val_jstr.into())])?;
            }
            let pv_key = string_to_jstring(env, "partitionValues")?;
            env.call_method(&map, "put",
                "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
                &[JValue::Object(&pv_key.into()), JValue::Object(&pv_map)])?;

            // Add map to result list
            env.call_method(&result_list, "add",
                "(Ljava/lang/Object;)Z",
                &[JValue::Object(&map)])?;
        }

        Ok(result_list.into_raw())
    }).unwrap_or(std::ptr::null_mut())
}

/// Cancel an in-progress split creation, releasing ALL resources across all partitions.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeCancelSplit(
    mut env: JNIEnv,
    _class: JClass,
    ctx_handle: jlong,
) {
    let _ = convert_throwable(&mut env, |_env| {
        // Release from registry — dropping the Arc cleans up everything
        release_arc(ctx_handle);
        debug_log!("ARROW_FFI_IMPORT: Cancelled split creation via nativeCancelSplit");
        Ok(())
    });
}

// ============================================================================
// Arrow FFI Test Helpers (test-only: create valid FFI structs from Rust)
// ============================================================================

use arrow_schema::{Schema as ArrowSchema, Field as ArrowField, DataType};
use arrow_array::{Array, RecordBatch, StructArray, StringArray, Int64Array, Float64Array, BooleanArray};

/// Test helper: Export an Arrow schema as an FFI_ArrowSchema struct with valid release callback.
/// Supported types: "int64", "utf8", "float64", "boolean"
/// Returns the memory address of the boxed FFI_ArrowSchema.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeTestExportSchemaFfi(
    mut env: JNIEnv,
    _class: JClass,
    field_names: jni::sys::jobjectArray,
    field_types: jni::sys::jobjectArray,
) -> jlong {
    convert_throwable(&mut env, |env| {
        let names_arr = unsafe { jni::objects::JObjectArray::from_raw(field_names) };
        let types_arr = unsafe { jni::objects::JObjectArray::from_raw(field_types) };
        let num_fields = env.get_array_length(&names_arr)? as usize;

        let mut fields = Vec::with_capacity(num_fields);
        for i in 0..num_fields {
            let name_obj = env.get_object_array_element(&names_arr, i as i32)?;
            let type_obj = env.get_object_array_element(&types_arr, i as i32)?;
            let name = jstring_to_string(env, &name_obj.into())?;
            let type_str = jstring_to_string(env, &type_obj.into())?;

            let data_type = match type_str.as_str() {
                "int64" => DataType::Int64,
                "utf8" => DataType::Utf8,
                "float64" => DataType::Float64,
                "boolean" => DataType::Boolean,
                _ => return Err(anyhow!("Unsupported test field type: {}", type_str)),
            };
            fields.push(ArrowField::new(&name, data_type, true));
        }

        let schema = ArrowSchema::new(fields);
        let ffi_schema = FFI_ArrowSchema::try_from(&schema)
            .map_err(|e| anyhow!("Failed to export schema to FFI: {}", e))?;
        let boxed = Box::new(ffi_schema);
        Ok(Box::into_raw(boxed) as jlong)
    }).unwrap_or(0)
}

/// Test helper: Export a standard test batch as FFI structs with valid release callbacks.
/// Schema: (id: Int64, name: Utf8, score: Float64, active: Boolean)
/// Data: sequential values starting from idOffset.
/// Returns long[2] = [arrayAddr, schemaAddr]
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeTestExportStandardBatchFfi(
    mut env: JNIEnv,
    _class: JClass,
    num_rows: jni::sys::jint,
    id_offset: jlong,
) -> jni::sys::jlongArray {
    convert_throwable(&mut env, |env| {
        let n = num_rows as usize;
        let offset = id_offset;

        let ids: Vec<i64> = (offset..offset + n as i64).collect();
        let names: Vec<String> = (0..n).map(|i| format!("name_{}", i as i64 + offset)).collect();
        let scores: Vec<f64> = (0..n).map(|i| (i as f64) * 1.5 + offset as f64).collect();
        let actives: Vec<bool> = (0..n).map(|i| i % 2 == 0).collect();

        let schema = ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("name", DataType::Utf8, false),
            ArrowField::new("score", DataType::Float64, true),
            ArrowField::new("active", DataType::Boolean, true),
        ]);

        let batch = RecordBatch::try_new(
            std::sync::Arc::new(schema),
            vec![
                std::sync::Arc::new(Int64Array::from(ids)),
                std::sync::Arc::new(StringArray::from(
                    names.iter().map(|s| s.as_str()).collect::<Vec<_>>()
                )),
                std::sync::Arc::new(Float64Array::from(scores)),
                std::sync::Arc::new(BooleanArray::from(actives)),
            ],
        ).map_err(|e| anyhow!("Failed to create test batch: {}", e))?;

        let struct_array = StructArray::from(batch);
        let data = struct_array.to_data();

        let ffi_schema = FFI_ArrowSchema::try_from(data.data_type())
            .map_err(|e| anyhow!("Failed to export batch schema to FFI: {}", e))?;
        let ffi_array = FFI_ArrowArray::new(&data);

        let array_addr = Box::into_raw(Box::new(ffi_array)) as jlong;
        let schema_addr = Box::into_raw(Box::new(ffi_schema)) as jlong;

        let result = env.new_long_array(2)?;
        env.set_long_array_region(&result, 0, &[array_addr, schema_addr])?;
        Ok(result.into_raw())
    }).unwrap_or(std::ptr::null_mut())
}

/// Test helper: Export a partition test batch as FFI structs.
/// Schema: (id: Int64, name: Utf8, <extraColName>: Utf8)
/// Returns long[2] = [arrayAddr, schemaAddr]
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_merge_QuickwitSplit_nativeTestExportPartitionBatchFfi(
    mut env: JNIEnv,
    _class: JClass,
    ids: jni::sys::jlongArray,
    names: jni::sys::jobjectArray,
    extra_col_name: JString,
    extra_values: jni::sys::jobjectArray,
) -> jni::sys::jlongArray {
    convert_throwable(&mut env, |env| {
        // Extract ids
        let ids_arr = unsafe { jni::objects::JLongArray::from_raw(ids) };
        let num_rows = env.get_array_length(&ids_arr)? as usize;
        let mut id_values = vec![0i64; num_rows];
        env.get_long_array_region(&ids_arr, 0, &mut id_values)?;

        // Extract names
        let names_arr = unsafe { jni::objects::JObjectArray::from_raw(names) };
        let mut name_values = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            let obj = env.get_object_array_element(&names_arr, i as i32)?;
            name_values.push(jstring_to_string(env, &obj.into())?);
        }

        // Extract extra column name and values
        let extra_name = jstring_to_string(env, &extra_col_name)?;
        let extra_arr = unsafe { jni::objects::JObjectArray::from_raw(extra_values) };
        let mut extra_vals = Vec::with_capacity(num_rows);
        for i in 0..num_rows {
            let obj = env.get_object_array_element(&extra_arr, i as i32)?;
            extra_vals.push(jstring_to_string(env, &obj.into())?);
        }

        let schema = ArrowSchema::new(vec![
            ArrowField::new("id", DataType::Int64, false),
            ArrowField::new("name", DataType::Utf8, false),
            ArrowField::new(&extra_name, DataType::Utf8, false),
        ]);

        let batch = RecordBatch::try_new(
            std::sync::Arc::new(schema),
            vec![
                std::sync::Arc::new(Int64Array::from(id_values)),
                std::sync::Arc::new(StringArray::from(
                    name_values.iter().map(|s| s.as_str()).collect::<Vec<_>>()
                )),
                std::sync::Arc::new(StringArray::from(
                    extra_vals.iter().map(|s| s.as_str()).collect::<Vec<_>>()
                )),
            ],
        ).map_err(|e| anyhow!("Failed to create partition test batch: {}", e))?;

        let struct_array = StructArray::from(batch);
        let data = struct_array.to_data();

        let ffi_schema = FFI_ArrowSchema::try_from(data.data_type())
            .map_err(|e| anyhow!("Failed to export batch schema to FFI: {}", e))?;
        let ffi_array = FFI_ArrowArray::new(&data);

        let array_addr = Box::into_raw(Box::new(ffi_array)) as jlong;
        let schema_addr = Box::into_raw(Box::new(ffi_schema)) as jlong;

        let result = env.new_long_array(2)?;
        env.set_long_array_region(&result, 0, &[array_addr, schema_addr])?;
        Ok(result.into_raw())
    }).unwrap_or(std::ptr::null_mut())
}
