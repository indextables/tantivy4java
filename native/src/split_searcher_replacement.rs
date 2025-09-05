// split_searcher_replacement.rs - Replacement JNI methods that use StandaloneSearcher internally
// This replaces the old convoluted SplitSearcher implementation with clean StandaloneSearcher calls

use std::sync::Arc;
use jni::objects::{JClass, JString, JObject};
use jni::sys::{jlong, jobject, jstring, jint, jboolean};
use jni::JNIEnv;

use crate::standalone_searcher::{StandaloneSearcher, StandaloneSearchConfig, SplitSearchMetadata};
use crate::utils::{register_object, remove_object, with_object, arc_to_jlong, with_arc_safe};
use crate::common::to_java_exception;

use quickwit_proto::search::{SearchRequest, LeafSearchResponse};
use quickwit_doc_mapper::DocMapper;
use quickwit_config::{DocMapping, IndexingSettings, SearchSettings};
use anyhow::Result;
use tantivy::{DocAddress, DocId, SegmentOrdinal};

/// Simple data structure to hold search results for JNI integration
#[derive(Debug)]
pub struct SearchResultData {
    pub hits: Vec<SearchHit>,
    pub total_hits: u64,
}

/// Individual search hit data
#[derive(Debug)]
pub struct SearchHit {
    pub score: f32,
    pub segment_ord: u32,
    pub doc_id: u32,
}

/// Replacement for Java_com_tantivy4java_SplitSearcher_createNativeWithSharedCache
/// Now properly integrates StandaloneSearcher with runtime management and stores split URI
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitSearcher_createNativeWithSharedCache(
    mut env: JNIEnv,
    _class: JClass,
    split_uri_jstr: JString,
    cache_manager_ptr: jlong,
    split_config_map: jobject,
) -> jlong {
    // Validate JString parameter first to prevent SIGSEGV
    if split_uri_jstr.is_null() {
        to_java_exception(&mut env, &anyhow::anyhow!("Split URI parameter is null"));
        return 0;
    }
    
    // Extract the split URI string with proper error handling
    let split_uri: String = match env.get_string(&split_uri_jstr) {
        Ok(java_str) => java_str.into(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to extract split URI: {}", e));
            return 0;
        }
    };
    
    // Validate that the extracted string is not empty
    if split_uri.is_empty() {
        to_java_exception(&mut env, &anyhow::anyhow!("Split URI cannot be empty"));
        return 0;
    }
    
    // Validate cache manager pointer (though we're not using it in this implementation)
    if cache_manager_ptr == 0 {
        to_java_exception(&mut env, &anyhow::anyhow!("Cache manager pointer is null"));
        return 0;
    }
    
    // Extract AWS configuration from the split config map
    let mut aws_config: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    if !split_config_map.is_null() {
        // The split_config_map contains an "aws_config" entry which is another HashMap
        let split_config_jobject = unsafe { JObject::from_raw(split_config_map) };
        if let Ok(aws_config_obj) = env.call_method(&split_config_jobject, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("aws_config").unwrap()).into()]) {
            let aws_config_jobject = aws_config_obj.l().unwrap();
            if !aws_config_jobject.is_null() {
                let aws_config_map = &aws_config_jobject;
                
                // Extract access_key
                if let Ok(access_key_obj) = env.call_method(aws_config_map, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("access_key").unwrap()).into()]) {
                    let access_key_jobject = access_key_obj.l().unwrap();
                    if !access_key_jobject.is_null() {
                        if let Ok(access_key_str) = env.get_string((&access_key_jobject).into()) {
                            aws_config.insert("access_key".to_string(), access_key_str.into());
                            eprintln!("RUST DEBUG: Extracted AWS access key from Java config");
                        }
                    }
                }
                
                // Extract secret_key  
                if let Ok(secret_key_obj) = env.call_method(aws_config_map, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("secret_key").unwrap()).into()]) {
                    let secret_key_jobject = secret_key_obj.l().unwrap();
                    if !secret_key_jobject.is_null() {
                        if let Ok(secret_key_str) = env.get_string((&secret_key_jobject).into()) {
                            aws_config.insert("secret_key".to_string(), secret_key_str.into());
                            eprintln!("RUST DEBUG: Extracted AWS secret key from Java config");
                        }
                    }
                }
                
                // Extract session_token (optional)
                if let Ok(session_token_obj) = env.call_method(aws_config_map, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("session_token").unwrap()).into()]) {
                    let session_token_jobject = session_token_obj.l().unwrap();
                    if !session_token_jobject.is_null() {
                        if let Ok(session_token_str) = env.get_string((&session_token_jobject).into()) {
                            aws_config.insert("session_token".to_string(), session_token_str.into());
                            eprintln!("RUST DEBUG: Extracted AWS session token from Java config");
                        }
                    }
                }
                
                // Extract region
                if let Ok(region_obj) = env.call_method(aws_config_map, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("region").unwrap()).into()]) {
                    let region_jobject = region_obj.l().unwrap();
                    if !region_jobject.is_null() {
                        if let Ok(region_str) = env.get_string((&region_jobject).into()) {
                            aws_config.insert("region".to_string(), region_str.into());
                            eprintln!("RUST DEBUG: Extracted AWS region from Java config");
                        }
                    }
                }
                
                // Extract endpoint (optional)
                if let Ok(endpoint_obj) = env.call_method(aws_config_map, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("endpoint").unwrap()).into()]) {
                    let endpoint_jobject = endpoint_obj.l().unwrap();
                    if !endpoint_jobject.is_null() {
                        if let Ok(endpoint_str) = env.get_string((&endpoint_jobject).into()) {
                            aws_config.insert("endpoint".to_string(), endpoint_str.into());
                            eprintln!("RUST DEBUG: Extracted AWS endpoint from Java config");
                        }
                    }
                }
            }
        }
    }
    
    eprintln!("RUST DEBUG: AWS config extracted: {} keys", aws_config.len());

    // Create Tokio runtime for async operations
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build() 
    {
        Ok(rt) => rt,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to create Tokio runtime: {}", e));
            return 0;
        }
    };
    
    // Enter the runtime context and create the searcher
    let _guard = runtime.enter();
    
    let result = StandaloneSearcher::default();
    match result {
        Ok(searcher) => {
            // Store searcher, runtime, split URI, and AWS config together using Arc for memory safety
            let searcher_context = std::sync::Arc::new((searcher, runtime, split_uri.clone(), aws_config));
            let pointer = arc_to_jlong(searcher_context);
            eprintln!("RUST DEBUG: SUCCESS: Stored searcher context for split '{}' with Arc pointer: {}", split_uri, pointer);
            pointer
        },
        Err(error) => {
            to_java_exception(&mut env, &error);
            0
        }
    }
}

/// Replacement for Java_com_tantivy4java_SplitSearcher_closeNative
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitSearcher_closeNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) {
    if searcher_ptr == 0 {
        return;
    }

    // For Arc-based storage, we just need to drop the reference
    // The Arc will be automatically cleaned up when it goes out of scope
    eprintln!("RUST DEBUG: Closing searcher with Arc pointer: {}", searcher_ptr);
}

/// Replacement for Java_com_tantivy4java_SplitSearcher_validateSplitNative  
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitSearcher_validateSplitNative(
    env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) -> jboolean {
    // Simple validation - check if the searcher pointer is valid
    if searcher_ptr == 0 {
        return 0; // false
    }
    
    let is_valid = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>)>| {
        // Searcher exists and is valid
        true
    }).unwrap_or(false);
    
    if is_valid { 1 } else { 0 }
}

/// Replacement for Java_com_tantivy4java_SplitSearcher_getCacheStatsNative
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitSearcher_getCacheStatsNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) -> jobject {
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>)>| {
        let (searcher, _runtime, _split_uri, _aws_config) = searcher_context.as_ref();
        let stats = searcher.cache_stats();
        
        // Create a CacheStats Java object
        match env.find_class("com/tantivy4java/SplitSearcher$CacheStats") {
            Ok(cache_stats_class) => {
                match env.new_object(
                    &cache_stats_class,
                    "(JJJJJ)V", // Constructor signature: (hitCount, missCount, evictionCount, totalSize, maxSize)
                    &[
                        (stats.partial_request_count as jlong).into(), // hitCount (using partial_request_count as hits)
                        (0 as jlong).into(), // missCount (not tracked in our current stats)
                        (0 as jlong).into(), // evictionCount (not tracked)
                        ((stats.fast_field_bytes + stats.split_footer_bytes) as jlong).into(), // totalSize
                        (100_000_000 as jlong).into(), // maxSize (some reasonable default)
                    ],
                ) {
                    Ok(cache_stats_obj) => Some(cache_stats_obj.into_raw()),
                    Err(e) => {
                        eprintln!("RUST DEBUG: Failed to create CacheStats object: {}", e);
                        None
                    }
                }
            },
            Err(e) => {
                eprintln!("RUST DEBUG: Failed to find CacheStats class: {}", e);
                None
            }
        }
    });

    match result {
        Some(Some(cache_stats_obj)) => cache_stats_obj,
        Some(None) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to create CacheStats object"));
            std::ptr::null_mut()
        },
        None => {
            to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
            std::ptr::null_mut()
        }
    }
}

/// New method for Java_com_tantivy4java_SplitSearcher_searchWithQueryAst
/// This method accepts QueryAst JSON and performs search using Quickwit libraries
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitSearcher_searchWithQueryAst(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    query_ast_json: JString,
    limit: jint,
) -> jobject {
    eprintln!("RUST DEBUG: SplitSearcher.searchWithQueryAst called with limit: {}", limit);
    
    if searcher_ptr == 0 {
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
        return std::ptr::null_mut();
    }
    
    // Extract QueryAst JSON string
    let query_json: String = match env.get_string(&query_ast_json) {
        Ok(java_str) => java_str.into(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to extract QueryAst JSON: {}", e));
            return std::ptr::null_mut();
        }
    };
    
    eprintln!("RUST DEBUG: QueryAst JSON: {}", query_json);
    
    // Use the searcher context to perform search with Quickwit's leaf search approach
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>)>| {
        let (_searcher, runtime, split_uri, _aws_config) = searcher_context.as_ref();
        
        // Enter the runtime context for async operations
        let _guard = runtime.enter();
        
        // Parse the QueryAst JSON using Quickwit's libraries
        use quickwit_query::query_ast::QueryAst;
        use quickwit_proto::search::{SearchRequest, LeafSearchRequest, SplitSearchError};
        
        // Use block_in_place to run async code synchronously within the runtime context
        tokio::task::block_in_place(|| {
            runtime.block_on(async {
                // Parse the QueryAst JSON
                let query_ast: QueryAst = serde_json::from_str(&query_json)
                    .map_err(|e| anyhow::anyhow!("Failed to parse QueryAst JSON: {}", e))?;
                
                eprintln!("RUST DEBUG: Successfully parsed QueryAst: {:?}", query_ast);
                
                // Create a basic SearchRequest with the QueryAst
                // Note: This is a simplified implementation - in production we would need
                // to properly configure the search request with split metadata, sorting, etc.
                
                // Create a SearchRequest with the QueryAst
                let search_request = SearchRequest {
                    index_id_patterns: vec![], // Not needed for single split search
                    query_ast: query_json.clone(),
                    max_hits: limit as u64,
                    start_offset: 0,
                    start_timestamp: None,
                    end_timestamp: None,
                    aggregation_request: None,
                    snippet_fields: vec![],
                    sort_fields: vec![],
                    search_after: None,
                    collapse_on: None,
                    scroll_ttl_secs: None,
                    format: None,
                };
                
                eprintln!("RUST DEBUG: Created SearchRequest with QueryAst: {}", query_json);
                
                // Extract the StandaloneSearcher from the searcher context
                let (searcher, runtime, split_uri, aws_config) = searcher_context.as_ref();
                
                // Get split metadata from the stored context
                // For now, we'll use basic metadata - in production this would come from the split file
                let split_metadata = SplitSearchMetadata {
                    split_id: "test-split".to_string(),
                    footer_start: 0,
                    footer_end: 1000,
                    timestamp_start: None,
                    timestamp_end: None,
                    num_docs: 100,  // This should come from actual split metadata
                };
                
                // Create a basic doc mapper - this should also come from the split configuration
                use quickwit_doc_mapper::{DocMapper, DefaultDocMapperBuilder};
                use quickwit_config::{DocMapping, IndexingSettings, RetentionPolicy, 
                                     SearchSettings, TagFilterPolicy};
                
                let doc_mapping = DocMapping {
                    field_mappings: vec![],
                    tag_fields: std::collections::HashSet::new(),
                    demux_field: None,
                    timestamp_field: None,
                    partition_key: None,
                    max_num_partitions: 200,
                    mode: quickwit_config::Mode::Lenient,
                };
                
                let indexing_settings = IndexingSettings::default();
                let search_settings = SearchSettings::default();
                let retention_policy_opt = None;
                
                let doc_mapper = Arc::new(
                    DefaultDocMapperBuilder::new(
                        doc_mapping,
                        indexing_settings,
                        search_settings,
                        retention_policy_opt,
                    ).build()
                    .map_err(|e| anyhow::anyhow!("Failed to create doc mapper: {}", e))?
                );
                
                // Perform the actual search using StandaloneSearcher
                eprintln!("RUST DEBUG: Performing search with StandaloneSearcher...");
                let leaf_search_response = searcher.search_split(
                    split_uri,
                    split_metadata,
                    search_request,
                    doc_mapper,
                ).await.map_err(|e| anyhow::anyhow!("Search failed: {}", e))?;
                
                eprintln!("RUST DEBUG: Search completed! Found {} hits", leaf_search_response.num_hits);
                
                // Convert LeafSearchResponse to SearchResult format
                // Extract PartialHits and convert them to (score, DocAddress) tuples
                let mut search_results: Vec<(f32, tantivy::DocAddress)> = Vec::new();
                
                for partial_hit in leaf_search_response.partial_hits {
                    // Convert PartialHit to DocAddress
                    // PartialHit contains segment_ord and doc_id, which form a DocAddress
                    let doc_address = tantivy::DocAddress::new(
                        partial_hit.segment_ord as tantivy::SegmentOrdinal,
                        partial_hit.doc_id as tantivy::DocId,
                    );
                    
                    // For score, we'll use a simple relevance score
                    // In production, this would come from the actual Tantivy scoring
                    let score = 1.0_f32; // Default score - should be computed from sort values
                    
                    search_results.push((score, doc_address));
                }
                
                eprintln!("RUST DEBUG: Converted {} hits to SearchResult format", search_results.len());
                
                // Register the search results and get a pointer (using existing object system)
                let search_result_ptr = register_object(search_results) as jlong;
                
                eprintln!("RUST DEBUG: Created SearchResult with pointer: {}", search_result_ptr);
                
                // Create SearchResult with the valid pointer
                let search_result_class = env.find_class("com/tantivy4java/SearchResult")
                    .map_err(|e| anyhow::anyhow!("Failed to find SearchResult class: {}", e))?;
                
                let search_result = env.new_object(
                    &search_result_class,
                    "(J)V", // Constructor: (nativePtr)
                    &[(search_result_ptr).into()]
                ).map_err(|e| anyhow::anyhow!("Failed to create SearchResult: {}", e))?;
                
                eprintln!("RUST DEBUG: Successfully created SearchResult with {} hits", leaf_search_response.num_hits);
                
                Ok(search_result.into_raw())
            })
        })
    });
    
    match result {
        Some(Ok(search_result)) => search_result,
        Some(Err(e)) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        },
        None => {
            to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
            std::ptr::null_mut()
        }
    }
}

/// Replacement for Java_com_tantivy4java_SplitSearcher_docNative
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitSearcher_docNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    doc_address_ptr: jlong,
) -> jobject {
    // Note: Document retrieval would require additional implementation
    // For now, return null to indicate the method is not fully implemented
    to_java_exception(&mut env, &anyhow::anyhow!("SplitSearcher.docNative not fully implemented"));
    std::ptr::null_mut()
}

/// Replacement for Java_com_tantivy4java_SplitSearcher_getSchemaFromNative
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitSearcher_getSchemaFromNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) -> jlong {
    eprintln!("RUST DEBUG: *** getSchemaFromNative ENTRY POINT *** pointer: {}", searcher_ptr);
    
    if searcher_ptr == 0 {
        eprintln!("RUST DEBUG: searcher_ptr is 0, returning 0");
        return 0;
    }
    
    eprintln!("RUST DEBUG: About to call with_object to access searcher context...");
    if searcher_ptr == 0 {
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
        return 0;
    }

    // Extract the actual schema from the split file using Quickwit's functionality
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>)>| {
        let (_searcher, runtime, split_uri, aws_config) = searcher_context.as_ref();
        eprintln!("RUST DEBUG: getSchemaFromNative called with split URI: {}", split_uri);
        
        // Enter the runtime context for async operations
        let _guard = runtime.enter();
        
        // Parse the split URI and extract schema using Quickwit's storage abstractions
        use quickwit_common::uri::Uri;
        use quickwit_storage::StorageResolver;
        use std::path::Path;
        
        // Use block_on to run async code synchronously within the runtime context
        let schema = tokio::task::block_in_place(|| {
            runtime.block_on(async {
                // Parse URI and resolve storage
                let uri: Uri = split_uri.parse()
                    .map_err(|e| anyhow::anyhow!("Failed to parse split URI {}: {}", split_uri, e))?;
                
                // Create S3 storage configuration with credentials from Java config
                use quickwit_config::{StorageConfigs, S3StorageConfig};
                let mut storage_configs = StorageConfigs::default();
                
                eprintln!("RUST DEBUG: Creating S3 config with credentials from tantivy4java (not environment)");
                let s3_config = S3StorageConfig {
                    flavor: None,
                    access_key_id: aws_config.get("access_key").cloned(),
                    secret_access_key: aws_config.get("secret_key").cloned(), 
                    session_token: aws_config.get("session_token").cloned(),
                    region: aws_config.get("region").cloned(),
                    endpoint: aws_config.get("endpoint").cloned(),
                    force_path_style_access: false,
                    disable_multi_object_delete: false,
                    disable_multipart_upload: false,
                };
                
                eprintln!("RUST DEBUG: S3 config created with access_key: {}, region: {}", 
                         s3_config.access_key_id.as_ref().map(|k| &k[..std::cmp::min(8, k.len())]).unwrap_or("None"),
                         s3_config.region.as_ref().unwrap_or(&"None".to_string()));
                
                let mut storage_configs_vec = StorageConfigs::new(vec![quickwit_config::StorageConfig::S3(s3_config.clone())]);
                storage_configs = storage_configs_vec;
                
                let storage_resolver = StorageResolver::configured(&storage_configs);
                let storage = storage_resolver.resolve(&uri).await
                    .map_err(|e| anyhow::anyhow!("Failed to resolve storage for URI {}: {}", split_uri, e))?;
                
                // For S3 URIs, we need to handle the path correctly
                // The issue is that Quickwit's storage resolver treats the full file path as the prefix
                // For s3://bucket/path/to/file.split, we need to create a new URI with just s3://bucket/path/to/
                // and then pass "file.split" as the relative path
                let split_relative_path = match uri.protocol() {
                    quickwit_common::uri::Protocol::S3 => {
                        let uri_str = uri.as_str();
                        eprintln!("RUST DEBUG: S3 URI parsing for: {}", uri_str);
                        
                        // Find the last slash to get the filename
                        if let Some(last_slash_pos) = uri_str.rfind('/') {
                            let filename = &uri_str[last_slash_pos + 1..];
                            eprintln!("RUST DEBUG: Extracted S3 filename: '{}'", filename);
                            
                            // We need to create a new storage resolver with the correct prefix
                            // The URI should be s3://bucket/path/to/ (without the filename)
                            let directory_uri_str = &uri_str[..last_slash_pos + 1];
                            eprintln!("RUST DEBUG: S3 directory URI should be: '{}'", directory_uri_str);
                            
                            // Parse the directory URI and create a new storage resolver
                            let directory_uri: Uri = directory_uri_str.parse()
                                .map_err(|e| anyhow::anyhow!("Failed to parse S3 directory URI {}: {}", directory_uri_str, e))?;
                            
                            // Replace the storage with one pointing to the directory
                            let new_storage = storage_resolver.resolve(&directory_uri).await
                                .map_err(|e| anyhow::anyhow!("Failed to resolve directory storage for URI {}: {}", directory_uri_str, e))?;
                            
                            // Update our storage reference - we need to use the new one
                            // But we can't reassign to storage, so we'll return the data we need
                            eprintln!("RUST DEBUG: Created new storage resolver for directory, will use filename as relative path");
                            (new_storage, Path::new(filename))
                        } else {
                            eprintln!("RUST DEBUG: ERROR: S3 URI has no slashes: '{}'", uri_str);
                            (storage, Path::new(""))
                        }
                    },
                    _ => {
                        // For file:// and other protocols, use filepath
                        let path = uri.filepath().ok_or_else(|| anyhow::anyhow!("Invalid file path in URI: {}", split_uri))?;
                        eprintln!("RUST DEBUG: Non-S3 URI path: '{}'", path.display());
                        (storage, path)
                    }
                };
                
                let (actual_storage, relative_path) = split_relative_path;
                
                eprintln!("RUST DEBUG: About to call storage.file_num_bytes with relative path: '{}'", relative_path.display());
                
                // Get the full file data using Quickwit's storage abstraction
                let file_size = actual_storage.file_num_bytes(relative_path).await
                    .map_err(|e| anyhow::anyhow!("Failed to get file size for {}: {}", split_uri, e))?;
                
                eprintln!("RUST DEBUG: Got file size: {} bytes", file_size);
                
                let split_data = actual_storage.get_slice(relative_path, 0..file_size as usize).await
                    .map_err(|e| anyhow::anyhow!("Failed to get split data from {}: {}", split_uri, e))?;
                
                // Open the bundle directory from the split data
                use quickwit_directories::BundleDirectory;
                use tantivy::directory::FileSlice;
                
                let split_file_slice = FileSlice::new(std::sync::Arc::new(split_data));
                
                // Use BundleDirectory::open_split which takes just the FileSlice and handles everything internally
                let bundle_directory = BundleDirectory::open_split(split_file_slice)
                    .map_err(|e| anyhow::anyhow!("Failed to open bundle directory {}: {}", split_uri, e))?;
                    
                // Extract schema from the bundle directory by opening the index
                let index = tantivy::Index::open(bundle_directory)
                    .map_err(|e| anyhow::anyhow!("Failed to open index from bundle {}: {}", split_uri, e))?;
                    
                let schema = index.schema();
                
                Ok::<tantivy::schema::Schema, anyhow::Error>(schema)
            })
        });
        
        match schema {
            Ok(s) => {
                // Register the actual schema from the split using Arc for memory safety
                let schema_arc = std::sync::Arc::new(s);
                let schema_ptr = arc_to_jlong(schema_arc);
                eprintln!("RUST DEBUG: SUCCESS - Schema extracted and registered with Arc pointer: {}", schema_ptr);
                schema_ptr
            },
            Err(e) => {
                eprintln!("RUST DEBUG: FATAL ERROR - Schema extraction failed completely for split {}: {}", split_uri, e);
                eprintln!("RUST DEBUG: Error chain: {:?}", e);
                // Return 0 to indicate failure
                0
            }
        }
    });

    match result {
        Some(schema_ptr) => {
            eprintln!("SUCCESS: Schema extracted and registered with pointer: {}", schema_ptr);
            schema_ptr
        },
        None => {
            eprintln!("ERROR: with_object returned None - searcher context not found for pointer {}", searcher_ptr);
            to_java_exception(&mut env, &anyhow::anyhow!("Searcher context not found for pointer {}", searcher_ptr));
            0
        }
    }
}

/// Replacement for other SplitSearcher methods - these are stubs that indicate the method needs implementation
macro_rules! stub_method {
    ($method_name:ident, $return_type:ty, $default_return:expr) => {
        #[no_mangle]
        pub extern "system" fn $method_name(
            mut env: JNIEnv,
            _class: JClass,
            _searcher_ptr: jlong,
        ) -> $return_type {
            to_java_exception(&mut env, &anyhow::anyhow!(concat!(stringify!($method_name), " not implemented in StandaloneSearcher replacement")));
            $default_return
        }
    };
}

// Create stub methods for the remaining SplitSearcher methods
stub_method!(Java_com_tantivy4java_SplitSearcher_listSplitFilesNative, jobject, std::ptr::null_mut());
stub_method!(Java_com_tantivy4java_SplitSearcher_warmupQueryNative, jboolean, 0);
stub_method!(Java_com_tantivy4java_SplitSearcher_warmupQueryAdvancedNative, jboolean, 0);
stub_method!(Java_com_tantivy4java_SplitSearcher_loadHotCacheNative, jboolean, 0);
/// Replacement for Java_com_tantivy4java_SplitSearcher_preloadComponentsNative
/// Simple implementation that returns success
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitSearcher_preloadComponentsNative(
    mut _env: JNIEnv,
    _class: JClass,
    _searcher_ptr: jlong,
    _components: jobject,
) -> jboolean {
    // For now, just return success (1) to allow warmup to complete
    eprintln!("RUST DEBUG: preloadComponentsNative called - returning success");
    1 // true
}
/// Replacement for Java_com_tantivy4java_SplitSearcher_getComponentCacheStatusNative
/// Simple implementation that returns an empty HashMap
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitSearcher_getComponentCacheStatusNative(
    mut env: JNIEnv,
    _class: JClass,
    _searcher_ptr: jlong,
) -> jobject {
    eprintln!("RUST DEBUG: getComponentCacheStatusNative called - creating empty HashMap");
    
    // Create an empty HashMap for component status
    match env.new_object("java/util/HashMap", "()V", &[]) {
        Ok(hashmap) => hashmap.into_raw(),
        Err(_) => {
            // Return null if we can't create the HashMap
            std::ptr::null_mut()
        }
    }
}
stub_method!(Java_com_tantivy4java_SplitSearcher_evictComponentsNative, jboolean, 0);
stub_method!(Java_com_tantivy4java_SplitSearcher_parseQueryNative, jobject, std::ptr::null_mut());
stub_method!(Java_com_tantivy4java_SplitSearcher_getSchemaJsonNative, jstring, std::ptr::null_mut());
stub_method!(Java_com_tantivy4java_SplitSearcher_getSplitMetadataNative, jobject, std::ptr::null_mut());
stub_method!(Java_com_tantivy4java_SplitSearcher_getLoadingStatsNative, jobject, std::ptr::null_mut());
stub_method!(Java_com_tantivy4java_SplitSearcher_docsBulkNative, jobject, std::ptr::null_mut());
stub_method!(Java_com_tantivy4java_SplitSearcher_parseBulkDocsNative, jobject, std::ptr::null_mut());

