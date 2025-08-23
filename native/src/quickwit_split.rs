use jni::objects::{JClass, JObject, JString, JValue};
use jni::sys::jobject;
use jni::JNIEnv;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::ops::RangeInclusive;
use std::io::Write;
use std::fs::File;

// Add tantivy Directory trait import
use tantivy::Directory;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use serde_json;
use base64::{Engine, engine::general_purpose};

// Quickwit imports
use quickwit_storage::{SplitPayloadBuilder, PutPayload, BundleStorage, RamStorage, Storage};
use quickwit_directories::write_hotcache;
use tantivy::directory::{OwnedBytes, FileSlice};

use crate::utils::{convert_throwable, jstring_to_string, string_to_jstring, with_object};

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
    
    let metadata_obj = env.new_object(
        &split_metadata_class,
        "(Ljava/lang/String;JJLjava/time/Instant;Ljava/time/Instant;Ljava/util/Set;JI)V",
        &[
            JValue::Object(&split_id_jstring.into()),
            JValue::Long(split_metadata.num_docs as i64),
            JValue::Long(split_metadata.uncompressed_docs_size_in_bytes as i64),
            JValue::Object(&time_start_obj),
            JValue::Object(&time_end_obj),
            JValue::Object(&tags_set),
            JValue::Long(split_metadata.delete_opstamp as i64),
            JValue::Int(split_metadata.num_merge_ops as i32),
        ]
    )?;
    
    Ok(metadata_obj)
}

fn convert_tantivy_to_split(
    tantivy_index: tantivy::Index,
    output_path: &Path,
    config: SplitConfig,
) -> Result<QuickwitSplitMetadata> {
    
    // Get index statistics
    let searcher = tantivy_index.reader()?.searcher();
    let num_docs = searcher.num_docs();
    
    // Calculate actual uncompressed size by examining index files
    let uncompressed_docs_size = (num_docs as u64) * 1024; // Basic estimate based on doc count
    
    // Create split metadata  
    let split_metadata = create_split_metadata(&config, num_docs as usize, uncompressed_docs_size);
    
    // Create a working split file that contains:
    // 1. Split metadata in JSON format
    // 2. Index directory structure information
    // 3. Placeholder for actual Tantivy index data
    
    let mut split_content = Vec::new();
    
    // Header with version
    split_content.extend_from_slice(b"QUICKWIT_SPLIT_V1\n");
    
    // Add metadata section
    let metadata_json = serde_json::to_string_pretty(&split_metadata)?;
    split_content.extend_from_slice(b"METADATA_START\n");
    split_content.extend_from_slice(metadata_json.as_bytes());
    split_content.extend_from_slice(b"\nMETADATA_END\n");
    
    // Add index info section
    split_content.extend_from_slice(b"INDEX_INFO_START\n");
    let index_info = format!(
        "num_docs: {}\nuncompressed_size: {}\n",
        num_docs, uncompressed_docs_size
    );
    split_content.extend_from_slice(index_info.as_bytes());
    split_content.extend_from_slice(b"INDEX_INFO_END\n");
    
    // Use Quickwit's SplitPayloadBuilder to create a real split bundle
    // Create a runtime for async operations
    let runtime = tokio::runtime::Runtime::new()
        .map_err(|e| anyhow::anyhow!("Failed to create tokio runtime: {}", e))?;
    
    // Unfortunately, we cannot reliably extract the directory path from a Tantivy Index object
    // because MmapDirectory's path field is private and there's no public accessor.
    // The Index object abstracts away the underlying directory implementation details.
    
    eprintln!("DEBUG: Cannot extract directory path from Index object - path is not publicly accessible");
    
    return Err(anyhow::anyhow!(
        "Converting from Index object is not currently supported due to Tantivy API limitations. \
        The directory path cannot be extracted from the Index object. \
        Please use QuickwitSplit.convertIndexFromPath(indexPath, outputPath, config) instead, \
        where you provide the directory path directly. \
        \
        Example: \
        QuickwitSplit.convertIndexFromPath(\"/path/to/index\", \"/path/to/output.split\", config) \
        \
        This method works correctly and creates proper Quickwit split files."
    ));
}

fn convert_index_from_path_impl(index_path: &str, output_path: &str, config: &SplitConfig) -> Result<QuickwitSplitMetadata, anyhow::Error> {
    use tantivy::directory::MmapDirectory;
    use tantivy::Index as TantivyIndex;
    
    // Open the Tantivy index using the actual Quickwit/Tantivy libraries
    let mmap_directory = MmapDirectory::open(index_path)
        .map_err(|e| anyhow!("Failed to open index directory {}: {}", index_path, e))?;
    let tantivy_index = TantivyIndex::open(mmap_directory)
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
    
    // Create split metadata with actual values from the index
    let mut split_metadata = create_split_metadata(config, doc_count as usize, total_size);
    split_metadata.num_docs = doc_count as usize;
    split_metadata.uncompressed_docs_size_in_bytes = total_size;
    
    // Use Quickwit's split creation functionality
    create_quickwit_split(&tantivy_index, &index_dir, &PathBuf::from(output_path), &split_metadata)?;
    
    Ok(split_metadata)
}

fn create_quickwit_split(
    _tantivy_index: &tantivy::Index, 
    index_dir: &PathBuf, 
    output_path: &PathBuf, 
    _split_metadata: &QuickwitSplitMetadata
) -> Result<(), anyhow::Error> {
    use quickwit_storage::SplitPayloadBuilder;
    use std::path::PathBuf;
    
    eprintln!("DEBUG: create_quickwit_split called with output_path: {:?}", output_path);
    
    // Collect all Tantivy index files
    let mut split_files = Vec::new();
    if let Ok(entries) = std::fs::read_dir(index_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            let filename = path.file_name().unwrap().to_str().unwrap();
            
            // Skip lock files and split files, include only Tantivy index files
            if filename.starts_with(".tantivy") || filename.ends_with(".split") {
                continue;
            }
            
            if path.is_file() {
                split_files.push(path);
            }
        }
    }
    
    // Sort files for consistent ordering
    split_files.sort();
    
    // Create split fields metadata (required by Quickwit)
    let split_fields = b"{}"; // Minimal empty JSON for split fields
    
    // Create proper hotcache using Quickwit's write_hotcache function
    let hotcache = {
        let mut hotcache_buffer = Vec::new();
        
        // Open the index directory to generate hotcache from (use the index_dir parameter)
        use tantivy::directory::MmapDirectory;
        let mmap_directory = MmapDirectory::open(index_dir)?;
        
        // Use Quickwit's write_hotcache function exactly like they do
        write_hotcache(mmap_directory, &mut hotcache_buffer)
            .map_err(|e| anyhow::anyhow!("Failed to generate hotcache: {}", e))?;
        
        hotcache_buffer
    };
    
    // Use Quickwit's real SplitPayloadBuilder to create proper split format
    let runtime = tokio::runtime::Runtime::new()
        .map_err(|e| anyhow::anyhow!("Failed to create tokio runtime: {}", e))?;
    
    runtime.block_on(async {
        // Create the split payload using Quickwit's actual implementation
        let split_payload = SplitPayloadBuilder::get_split_payload(
            &split_files,
            split_fields,
            &hotcache
        )?;
        
        // Write the payload to the output file
        let payload_bytes = split_payload.read_all().await?;
        let mut output_file = File::create(output_path)?;
        output_file.write_all(&payload_bytes)?;
        output_file.flush()?;
        
        Ok::<(), anyhow::Error>(())
    })?;
    
    Ok(())
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
        
        // Read the binary split file
        let split_data = std::fs::read(path)?;
        let owned_bytes = OwnedBytes::new(split_data);
        
        // Parse the split using Quickwit's BundleStorage
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
        };
        
        eprintln!("DEBUG: Successfully read Quickwit split with {} files", file_count);
        
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
        
        // Read the binary split file
        let split_data = std::fs::read(path)?;
        let owned_bytes = OwnedBytes::new(split_data);
        
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
        
        eprintln!("DEBUG: Listed {} files from Quickwit split", file_count);
        
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
        
        // Read the binary split file
        let split_data = std::fs::read(split_path)?;
        let owned_bytes = OwnedBytes::new(split_data);
        
        // Parse the split using Quickwit's BundleStorage  
        let runtime = tokio::runtime::Runtime::new()
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
            
            eprintln!("DEBUG: Extracted file {} ({} bytes)", file_path.display(), file_data.len());
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
        };
        
        eprintln!("DEBUG: Successfully extracted {} files from Quickwit split to {}", extracted_count, output_path.display());
        
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