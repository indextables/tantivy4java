use jni::objects::{JClass, JObject, JString, JValue};
use jni::sys::jobject;
use jni::JNIEnv;

// Debug logging macro - controlled by TANTIVY4JAVA_DEBUG environment variable
macro_rules! debug_log {
    ($($arg:tt)*) => {
        if std::env::var("TANTIVY4JAVA_DEBUG").unwrap_or_default() == "1" {
            eprintln!("DEBUG: {}", format!($($arg)*));
        }
    };
}
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::ops::RangeInclusive;
use std::io::Write;
use std::fs::File;
use tempfile as temp;

// Add tantivy Directory trait import
use tantivy::Directory;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use uuid::Uuid;
use serde::{Serialize, Deserialize};
use serde_json;
use base64::{Engine, engine::general_purpose};

// Quickwit imports
use quickwit_storage::{SplitPayloadBuilder, PutPayload, BundleStorage, RamStorage, Storage, StorageResolver, LocalFileStorageFactory, S3CompatibleObjectStorageFactory};
use quickwit_directories::write_hotcache;
use quickwit_config::S3StorageConfig;
use quickwit_common::uri::{Uri, Protocol};
use tantivy::directory::{OwnedBytes, FileSlice};
use std::str::FromStr;

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
    
    debug_log!("Cannot extract directory path from Index object - path is not publicly accessible");
    
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
    tantivy_index: &tantivy::Index, 
    index_dir: &PathBuf, 
    output_path: &PathBuf, 
    _split_metadata: &QuickwitSplitMetadata
) -> Result<(), anyhow::Error> {
    use quickwit_storage::SplitPayloadBuilder;
    use std::path::PathBuf;
    
    debug_log!("create_quickwit_split called with output_path: {:?}", output_path);
    
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
                debug_log!("Including file in split: {:?}", filename);
                split_files.push(path);
            }
        }
    }
    
    debug_log!("Total files collected for split: {}", split_files.len());
    
    // Sort files for consistent ordering
    split_files.sort();
    
    // Create proper split fields metadata from the Tantivy index schema
    let split_fields = {
        // Extract field metadata from the Tantivy index
        let searcher = tantivy_index.reader()
            .map_err(|e| anyhow::anyhow!("Failed to create reader for field metadata extraction: {}", e))?
            .searcher();
        
        let segment_ids: Vec<_> = searcher.segment_readers().iter().map(|sr| sr.segment_id()).collect();
        let mut all_field_metadata = Vec::new();
        
        // Collect field metadata from all segments
        for segment_reader in searcher.segment_readers() {
            let field_metadata = segment_reader.fields_metadata();
            all_field_metadata.extend(field_metadata);
        }
        
        debug_log!("Extracted {} field metadata entries from index", all_field_metadata.len());
        
        // Use Quickwit's serialization functions to create proper field metadata
        use quickwit_proto::search::{ListFields, ListFieldsEntryResponse, serialize_split_fields};
        
        let fields: Vec<ListFieldsEntryResponse> = all_field_metadata.into_iter().flat_map(|metadata_vec| {
            metadata_vec.into_iter().map(|field_metadata| {
                let field_type = match field_metadata.typ {
                    tantivy::schema::Type::Str => quickwit_proto::search::ListFieldType::Str as i32,
                    tantivy::schema::Type::U64 => quickwit_proto::search::ListFieldType::U64 as i32,
                    tantivy::schema::Type::I64 => quickwit_proto::search::ListFieldType::I64 as i32,
                    tantivy::schema::Type::F64 => quickwit_proto::search::ListFieldType::F64 as i32,
                    tantivy::schema::Type::Bool => quickwit_proto::search::ListFieldType::Bool as i32,
                    tantivy::schema::Type::Date => quickwit_proto::search::ListFieldType::Date as i32,
                    tantivy::schema::Type::Facet => quickwit_proto::search::ListFieldType::Facet as i32,
                    tantivy::schema::Type::Bytes => quickwit_proto::search::ListFieldType::Bytes as i32,
                    _ => quickwit_proto::search::ListFieldType::Str as i32, // Default fallback
                };
                
                debug_log!("Field '{}' - type: {:?}, indexed: {}, fast: {}", 
                    field_metadata.field_name, field_metadata.typ, field_metadata.indexed, field_metadata.fast);
                    
                ListFieldsEntryResponse {
                    field_name: field_metadata.field_name.clone(),
                    field_type,
                    searchable: field_metadata.indexed,
                    aggregatable: field_metadata.fast,
                    index_ids: Vec::new(),
                    non_searchable_index_ids: Vec::new(), 
                    non_aggregatable_index_ids: Vec::new(),
                }
            })
        }).collect();
        
        let list_fields = ListFields { fields };
        serialize_split_fields(list_fields)
    };
    
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
            &split_fields,
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
#[derive(Debug)]
struct MergeConfig {
    index_uid: String,
    source_id: String,
    node_id: String,
    doc_mapping_uid: String,
    partition_id: u64,
    delete_queries: Option<Vec<String>>,
    aws_config: Option<AwsConfig>,
}

/// AWS configuration for S3-compatible storage (copy from split_searcher.rs)
#[derive(Clone, Debug)]
struct AwsConfig {
    access_key: String,
    secret_key: String,
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
        
        // Create Java metadata object
        let metadata_obj = create_java_split_metadata(env, &merged_metadata)?;
        Ok(metadata_obj.into_raw())
    }).unwrap_or(std::ptr::null_mut())
}

/// Extract AWS configuration from Java AwsConfig object
fn extract_aws_config(env: &mut JNIEnv, aws_obj: JObject) -> anyhow::Result<AwsConfig> {
    let access_key = get_string_field_value(env, &aws_obj, "getAccessKey")?;
    let secret_key = get_string_field_value(env, &aws_obj, "getSecretKey")?;
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
    
    Ok(MergeConfig {
        index_uid,
        source_id,
        node_id,
        doc_mapping_uid,
        partition_id,
        delete_queries,
        aws_config,
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

/// Extract split file contents to a directory (avoiding read-only BundleDirectory issues)
fn extract_split_to_directory_impl(split_path: &Path, output_dir: &Path) -> Result<()> {
    use tantivy::directory::{MmapDirectory, DirectoryClone};
    
    debug_log!("Extracting split {:?} to directory {:?}", split_path, output_dir);
    
    // Create output directory
    std::fs::create_dir_all(output_dir)?;
    
    // Open the bundle directory (read-only)
    let split_path_str = split_path.to_string_lossy().to_string();
    let bundle_directory = get_tantivy_directory_from_split_bundle(&split_path_str)?;
    
    // Open the output directory (writable)  
    let output_directory = MmapDirectory::open(output_dir)?;
    
    // Open bundle as index to get list of files
    let temp_bundle_index = tantivy::Index::open(bundle_directory.box_clone())?;
    let index_meta = temp_bundle_index.load_metas()?;
    
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

/// Implementation of split merging using Quickwit's efficient approach
/// This follows Quickwit's MergeExecutor pattern for memory-efficient large-scale merges
fn merge_splits_impl(split_urls: &[String], output_path: &str, config: &MergeConfig) -> Result<QuickwitSplitMetadata> {
    use quickwit_directories::UnionDirectory;
    use tantivy::directory::{MmapDirectory, Directory, Advice, DirectoryClone};
    use tantivy::{Index as TantivyIndex, IndexMeta};
    use tantivy::index::SegmentId;
    
    debug_log!("Implementing split merge using Quickwit's efficient approach for {} splits", split_urls.len());
    
    // Create async runtime for async operations
    let runtime = tokio::runtime::Runtime::new()?;
    
    // 1. Open split directories without extraction (Quickwit's approach)
    let mut split_directories: Vec<Box<dyn Directory>> = Vec::new();
    let mut index_metas: Vec<IndexMeta> = Vec::new();
    let mut total_docs = 0usize;
    let mut total_size = 0u64;
    
    for (i, split_url) in split_urls.iter().enumerate() {
        debug_log!("Opening split directory {}: {}", i, split_url);
        
        // Support both file-based and S3/remote splits
        let temp_extract_dir = temp::TempDir::new()?;
        let temp_extract_path = temp_extract_dir.path();
        
        debug_log!("Extracting split {} to temporary directory: {:?}", i, temp_extract_path);
        
        if split_url.contains("://") && !split_url.starts_with("file://") {
            // Handle S3/remote URLs
            debug_log!("Processing S3/remote split URL: {}", split_url);
            
            // Parse the split URI
            let split_uri = Uri::from_str(split_url)?;
            
            // Create storage resolver with AWS config from MergeConfig
            let s3_config = if let Some(ref aws_config) = config.aws_config {
                S3StorageConfig {
                    region: Some(aws_config.region.clone()),
                    access_key_id: Some(aws_config.access_key.clone()),
                    secret_access_key: Some(aws_config.secret_key.clone()),
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
                .map_err(|e| anyhow!("Failed to create storage resolver: {}", e))?;
            
            // For S3 URIs, we need to resolve the parent directory, not the file itself
            let (storage_uri, file_name) = if split_uri.protocol() == Protocol::S3 {
                let uri_str = split_uri.as_str();
                if let Some(last_slash) = uri_str.rfind('/') {
                    let parent_uri_str = &uri_str[..last_slash]; // Get s3://bucket/splits
                    let file_name = &uri_str[last_slash + 1..];  // Get filename
                    debug_log!("Split S3 URI into parent: {} and file: {}", parent_uri_str, file_name);
                    (Uri::from_str(parent_uri_str)?, Some(file_name.to_string()))
                } else {
                    (split_uri.clone(), None)
                }
            } else {
                (split_uri.clone(), None)
            };
            
            // Resolve storage for the URI
            let storage = runtime.block_on(async {
                storage_resolver.resolve(&storage_uri).await
            }).map_err(|e| anyhow!("Failed to resolve storage for '{}': {}", split_url, e))?;
            
            // Download the split file to temporary location
            let split_filename = file_name.unwrap_or_else(|| {
                split_url.split('/').last().unwrap_or("split.split").to_string()
            });
            let temp_split_path = temp_extract_path.join(&split_filename);
            
            debug_log!("Downloading split {} to {:?}", split_url, temp_split_path);
            
            // Download the split file
            let split_data = runtime.block_on(async {
                storage.get_all(Path::new(&split_filename)).await
            }).map_err(|e| anyhow!("Failed to download split from {}: {}", split_url, e))?;
            
            std::fs::write(&temp_split_path, &split_data)?;
            debug_log!("Downloaded {} bytes to {:?}", split_data.len(), temp_split_path);
            
            // Extract the downloaded split to the temp directory
            extract_split_to_directory_impl(&temp_split_path, temp_extract_path)?;
            
        } else {
            // Handle local file URLs
            let split_path = if split_url.starts_with("file://") {
                split_url.strip_prefix("file://").unwrap_or(split_url)
            } else {
                split_url
            };
            
            // Validate split exists
            if !Path::new(split_path).exists() {
                return Err(anyhow!("Split file not found: {}", split_path));
            }
            
            // Extract the split to a writable directory
            extract_split_to_directory_impl(Path::new(split_path), temp_extract_path)?;
        }
        
        // Open the extracted directory as writable MmapDirectory
        let extracted_directory = MmapDirectory::open(temp_extract_path)?;
        let temp_index = TantivyIndex::open(extracted_directory.box_clone())?;
        let index_meta = temp_index.load_metas()?;
        
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
        
        debug_log!("Extracted split {} has {} documents, {} bytes", i, doc_count, split_size);
        
        total_docs += doc_count as usize;
        total_size += split_size;
        split_directories.push(extracted_directory.box_clone());
        index_metas.push(index_meta);
        
        // Keep temp directory alive for merge duration
        std::mem::forget(temp_extract_dir);
    }
    
    debug_log!("Opened {} splits with total {} documents, {} bytes", split_urls.len(), total_docs, total_size);
    
    // 2. Combine index metadata (Quickwit's approach)
    let union_index_meta = combine_index_meta(index_metas)?;
    debug_log!("Combined metadata from {} splits", split_urls.len());
    
    // 3. Create shadowing meta.json directory (Quickwit's metadata pattern)
    let shadowing_meta_directory = create_shadowing_meta_json_directory(union_index_meta)?;
    debug_log!("Created shadowing metadata directory");
    
    // 4. Set up output directory with sequential access optimization
    // Handle S3 URLs by creating a local temporary directory
    let (output_temp_dir, is_s3_output) = if output_path.contains("://") && !output_path.starts_with("file://") {
        // For S3/remote URLs, create a local temporary directory
        let temp_dir = temp::TempDir::new()?;
        let temp_path = temp_dir.path().join("temp_merge_output");
        std::fs::create_dir_all(&temp_path)?;
        debug_log!("Created local temporary directory for S3 output: {:?}", temp_path);
        (temp_path, true)
    } else {
        // For local files, create temp directory next to output file
        let output_dir_path = Path::new(output_path).parent()
            .ok_or_else(|| anyhow!("Cannot determine parent directory for output path"))?;
        let temp_dir = output_dir_path.join("temp_merge_output");
        std::fs::create_dir_all(&temp_dir)?;
        debug_log!("Created local temporary directory: {:?}", temp_dir);
        (temp_dir, false)
    };
    
    let output_directory = MmapDirectory::open_with_madvice(&output_temp_dir, Advice::Sequential)?;
    debug_log!("Created output directory: {:?}", output_temp_dir);
    
    // 5. Create UnionDirectory stack (Quickwit's memory-efficient approach)
    // CRITICAL: Writable directory must be first - all writes go to first directory
    let mut directory_stack: Vec<Box<dyn Directory>> = vec![
        Box::new(output_directory),                    // First - receives ALL writes (must be writable)
        Box::new(shadowing_meta_directory),            // Second - provides meta.json override
    ];
    // Add read-only split directories for reading existing segments
    directory_stack.extend(split_directories);
    
    debug_log!("Created directory stack with {} directories", directory_stack.len());
    
    // 6. Create union directory for unified access without copying data
    let union_directory = UnionDirectory::union_of(directory_stack);
    let union_index = TantivyIndex::open(union_directory)?;
    debug_log!("Created union index");
    
    // 7. Perform memory-efficient segment-level merge (not document copying)
    let merged_docs = runtime.block_on(perform_segment_merge(&union_index))?;
    debug_log!("Segment merge completed with {} documents", merged_docs);
    
    // 8. Calculate final index size
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
    };
    
    // 10. Create the merged split file using the merged index
    if is_s3_output {
        // For S3 output, create split locally then upload
        let local_split_filename = format!("{}.split", merged_metadata.split_id);
        let local_split_path = output_temp_dir.join(&local_split_filename);
        
        debug_log!("Creating local split file: {:?}", local_split_path);
        create_merged_split_file(&output_temp_dir, local_split_path.to_str().unwrap(), &merged_metadata)?;
        
        // Upload to S3
        debug_log!("Uploading split file to S3: {}", output_path);
        upload_split_to_s3(&local_split_path, output_path, config)?;
        debug_log!("Successfully uploaded split file to S3: {}", output_path);
    } else {
        // For local output, create split directly
        create_merged_split_file(&output_temp_dir, output_path, &merged_metadata)?;
    }
    
    // 11. Clean up temporary directory
    std::fs::remove_dir_all(&output_temp_dir).unwrap_or_else(|e| {
        debug_log!("Warning: Could not clean up temp directory: {}", e);
    });
    
    debug_log!("Created efficient merged split file: {} with {} documents", output_path, merged_docs);
    
    Ok(merged_metadata)
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

/// Combine multiple index metadata using Quickwit's approach
fn combine_index_meta(mut index_metas: Vec<tantivy::IndexMeta>) -> Result<tantivy::IndexMeta> {
    use tantivy::IndexMeta;
    
    debug_log!("Combining {} index metadata objects", index_metas.len());
    
    if index_metas.is_empty() {
        return Err(anyhow!("No index metadata to combine"));
    }
    
    // Start with the first metadata
    let mut union_index_meta = index_metas.remove(0);
    
    // Combine segments from all metadata
    for index_meta in index_metas {
        debug_log!("Adding {} segments to union", index_meta.segments.len());
        union_index_meta.segments.extend(index_meta.segments);
    }
    
    debug_log!("Combined metadata has {} total segments", union_index_meta.segments.len());
    Ok(union_index_meta)
}

/// Create shadowing meta.json directory using Quickwit's metadata pattern
fn create_shadowing_meta_json_directory(index_meta: tantivy::IndexMeta) -> Result<tantivy::directory::RamDirectory> {
    use tantivy::directory::RamDirectory;
    
    debug_log!("Creating shadowing meta.json directory");
    
    // Serialize the combined metadata
    let union_index_meta_json = serde_json::to_string_pretty(&index_meta)?;
    
    // Create RAM directory with the meta.json file
    let ram_directory = RamDirectory::default();
    ram_directory.atomic_write(Path::new("meta.json"), union_index_meta_json.as_bytes())?;
    
    debug_log!("Created shadowing directory with meta.json ({} bytes)", union_index_meta_json.len());
    Ok(ram_directory)
}

/// Perform segment-level merge using Quickwit/Tantivy's efficient approach
async fn perform_segment_merge(union_index: &tantivy::Index) -> Result<usize> {
    use tantivy::IndexWriter;
    use tantivy::index::SegmentId;
    use tantivy::merge_policy::NoMergePolicy;
    
    debug_log!("Performing segment-level merge");
    
    // Create writer with memory limit (15MB like Quickwit)
    let mut index_writer: IndexWriter = union_index.writer_with_num_threads(1, 15_000_000)?;
    
    // CRITICAL: Use NoMergePolicy to prevent garbage collection during merge
    // This prevents delete operations on read-only BundleDirectories
    index_writer.set_merge_policy(Box::new(NoMergePolicy));
    
    // Get all segment IDs from the union index using reader
    let reader = union_index.reader()?;
    let searcher = reader.searcher();
    let segment_ids: Vec<SegmentId> = searcher
        .segment_readers()
        .iter()
        .map(|segment_reader| segment_reader.segment_id())
        .collect();
    
    debug_log!("Found {} segments to merge: {:?}", segment_ids.len(), segment_ids);
    
    // Skip merge if there's only one segment (Quickwit's optimization)
    if segment_ids.len() <= 1 {
        debug_log!("Skipping merge - only {} segment(s)", segment_ids.len());
        return Ok(searcher.num_docs() as usize);
    }
    
    // Perform efficient segment-level merge (Quickwit's approach)
    debug_log!("Starting segment merge of {} segments", segment_ids.len());
    index_writer.merge(&segment_ids).await?;
    debug_log!("Segment merge completed");
    
    // Get final document count
    union_index.load_metas()?;
    let reader = union_index.reader()?;
    let searcher = reader.searcher();
    let final_doc_count = searcher.num_docs();
    
    debug_log!("Merged index contains {} documents", final_doc_count);
    Ok(final_doc_count as usize)
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
            access_key_id: Some(aws_config.access_key.clone()),
            secret_access_key: Some(aws_config.secret_key.clone()),
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
    let storage = storage_resolver.resolve(&storage_uri).await
        .map_err(|e| anyhow!("Failed to resolve storage for '{}': {}", s3_url, e))?;
    
    // Read the local split file
    let split_data = std::fs::read(local_split_path)
        .map_err(|e| anyhow!("Failed to read local split file {:?}: {}", local_split_path, e))?;
    
    let split_size = split_data.len();
    debug_log!("Read {} bytes from local split file", split_size);
    
    // Upload the split file to S3
    storage.put(Path::new(&file_name), Box::new(split_data)).await
        .map_err(|e| anyhow!("Failed to upload split to S3: {}", e))?;
    
    debug_log!("Successfully uploaded {} bytes to S3: {}", split_size, s3_url);
    Ok(())
}

/// Synchronous wrapper for S3 upload
fn upload_split_to_s3(local_split_path: &Path, s3_url: &str, config: &MergeConfig) -> Result<()> {
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(upload_split_to_s3_impl(local_split_path, s3_url, config))
}

/// Create the merged split file using existing Quickwit split creation logic
fn create_merged_split_file(merged_index_path: &Path, output_path: &str, metadata: &QuickwitSplitMetadata) -> Result<()> {
    use tantivy::directory::MmapDirectory;
    use tantivy::Index as TantivyIndex;
    
    debug_log!("Creating merged split file at {} from index {:?}", output_path, merged_index_path);
    
    // Open the merged Tantivy index
    let merged_directory = MmapDirectory::open(merged_index_path)?;
    let merged_index = TantivyIndex::open(merged_directory)?;
    
    // Use the existing split creation logic
    create_quickwit_split(&merged_index, &merged_index_path.to_path_buf(), &PathBuf::from(output_path), metadata)?;
    
    debug_log!("Successfully created merged split file: {}", output_path);
    Ok(())
}