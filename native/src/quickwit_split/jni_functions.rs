// jni_functions.rs - JNI entry points for QuickwitSplit Java class
// Extracted from mod.rs during refactoring
// Contains: All nativeXxx JNI functions for QuickwitSplit

use std::collections::BTreeSet;
use std::path::Path;

use anyhow::anyhow;
use jni::objects::{JClass, JObject, JString, JValue};
use jni::sys::jobject;
use jni::JNIEnv;
use chrono::Utc;
use tantivy::directory::Directory;

use crate::debug_println;
use crate::utils::{convert_throwable, jstring_to_string, string_to_jstring};
use super::{
    SplitConfig, QuickwitSplitMetadata,
    convert_index_from_path_impl, create_java_split_metadata,
};
use super::merge_config::{extract_merge_config, extract_string_list_from_jobject};
use super::merge_impl::merge_splits_impl;
use super::split_utils::{open_split_with_quickwit_native, get_split_file_list};
use super::json_discovery::extract_doc_mapping_from_index;

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
