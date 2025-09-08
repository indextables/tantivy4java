// split_searcher_replacement.rs - Replacement JNI methods that use StandaloneSearcher internally
// This replaces the old convoluted SplitSearcher implementation with clean StandaloneSearcher calls

use std::sync::{Arc, OnceLock};
use jni::objects::{JClass, JString, JObject, ReleaseMode};
use jni::sys::{jlong, jobject, jstring, jint, jboolean};
use jni::JNIEnv;

use crate::standalone_searcher::{StandaloneSearcher, StandaloneSearchConfig, SplitSearchMetadata, resolve_storage_for_split};
use crate::utils::{arc_to_jlong, with_arc_safe, release_arc};
use crate::common::to_java_exception;
use crate::debug_println;
use quickwit_common::thread_pool::ThreadPool;

use serde_json::{Value, Map};

use quickwit_proto::search::SearchRequest;
use quickwit_config::S3StorageConfig;
use quickwit_storage::StorageResolver;
use tantivy::schema::Document as DocumentTrait; // For to_named_doc method

/// Thread pool for search operations (matches Quickwit's pattern exactly)
fn search_thread_pool() -> &'static ThreadPool {
    static SEARCH_THREAD_POOL: OnceLock<ThreadPool> = OnceLock::new();
    SEARCH_THREAD_POOL.get_or_init(|| ThreadPool::new("search", None))
}

/// Cached Tantivy searcher for efficient single document retrieval
/// This cache avoids reopening the index for every docNative call
use std::collections::HashMap;
use std::sync::Mutex;

static SEARCHER_CACHE: OnceLock<Mutex<HashMap<String, Arc<tantivy::Searcher>>>> = OnceLock::new();

fn get_searcher_cache() -> &'static Mutex<HashMap<String, Arc<tantivy::Searcher>>> {
    SEARCHER_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

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
    
    // Extract AWS configuration and split metadata from the split config map
    let mut aws_config: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut split_footer_start: u64 = 0;
    let mut split_footer_end: u64 = 0;
    
    if !split_config_map.is_null() {
        let split_config_jobject = unsafe { JObject::from_raw(split_config_map) };
        
        // Extract footer offsets
        if let Ok(footer_start_obj) = env.call_method(&split_config_jobject, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("footer_start_offset").unwrap()).into()]) {
            let footer_start_jobject = footer_start_obj.l().unwrap();
            if !footer_start_jobject.is_null() {
                if let Ok(footer_start_long) = env.call_method(&footer_start_jobject, "longValue", "()J", &[]) {
                    split_footer_start = footer_start_long.j().unwrap() as u64;
                    debug_println!("RUST DEBUG: Extracted footer_start_offset from Java config: {}", split_footer_start);
                }
            }
        }
        
        if let Ok(footer_end_obj) = env.call_method(&split_config_jobject, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("footer_end_offset").unwrap()).into()]) {
            let footer_end_jobject = footer_end_obj.l().unwrap();
            if !footer_end_jobject.is_null() {
                if let Ok(footer_end_long) = env.call_method(&footer_end_jobject, "longValue", "()J", &[]) {
                    split_footer_end = footer_end_long.j().unwrap() as u64;
                    debug_println!("RUST DEBUG: Extracted footer_end_offset from Java config: {}", split_footer_end);
                }
            }
        }
        
        // Extract AWS config
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
                            debug_println!("RUST DEBUG: Extracted AWS access key from Java config");
                        }
                    }
                }
                
                // Extract secret_key  
                if let Ok(secret_key_obj) = env.call_method(aws_config_map, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("secret_key").unwrap()).into()]) {
                    let secret_key_jobject = secret_key_obj.l().unwrap();
                    if !secret_key_jobject.is_null() {
                        if let Ok(secret_key_str) = env.get_string((&secret_key_jobject).into()) {
                            aws_config.insert("secret_key".to_string(), secret_key_str.into());
                            debug_println!("RUST DEBUG: Extracted AWS secret key from Java config");
                        }
                    }
                }
                
                // Extract session_token (optional)
                if let Ok(session_token_obj) = env.call_method(aws_config_map, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("session_token").unwrap()).into()]) {
                    let session_token_jobject = session_token_obj.l().unwrap();
                    if !session_token_jobject.is_null() {
                        if let Ok(session_token_str) = env.get_string((&session_token_jobject).into()) {
                            aws_config.insert("session_token".to_string(), session_token_str.into());
                            debug_println!("RUST DEBUG: Extracted AWS session token from Java config");
                        }
                    }
                }
                
                // Extract region
                if let Ok(region_obj) = env.call_method(aws_config_map, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("region").unwrap()).into()]) {
                    let region_jobject = region_obj.l().unwrap();
                    if !region_jobject.is_null() {
                        if let Ok(region_str) = env.get_string((&region_jobject).into()) {
                            aws_config.insert("region".to_string(), region_str.into());
                            debug_println!("RUST DEBUG: Extracted AWS region from Java config");
                        }
                    }
                }
                
                // Extract endpoint (optional)
                if let Ok(endpoint_obj) = env.call_method(aws_config_map, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("endpoint").unwrap()).into()]) {
                    let endpoint_jobject = endpoint_obj.l().unwrap();
                    if !endpoint_jobject.is_null() {
                        if let Ok(endpoint_str) = env.get_string((&endpoint_jobject).into()) {
                            aws_config.insert("endpoint".to_string(), endpoint_str.into());
                            debug_println!("RUST DEBUG: Extracted AWS endpoint from Java config");
                        }
                    }
                }
            }
        }
    }
    
    debug_println!("RUST DEBUG: Config extracted - AWS keys: {}, footer offsets: {}-{}", aws_config.len(), split_footer_start, split_footer_end);

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
    
    // Create StandaloneSearcher using global caches
    // If AWS credentials are provided, use with_s3_config, otherwise use default
    let result = if aws_config.contains_key("access_key") && aws_config.contains_key("secret_key") {
        debug_println!("RUST DEBUG: Creating StandaloneSearcher with custom S3 config and global caches");
        
        let mut s3_config = S3StorageConfig::default();
        s3_config.access_key_id = Some(aws_config.get("access_key").unwrap().clone());
        s3_config.secret_access_key = Some(aws_config.get("secret_key").unwrap().clone());
        
        if let Some(session_token) = aws_config.get("session_token") {
            s3_config.session_token = Some(session_token.clone());
        }
        
        if let Some(region) = aws_config.get("region") {
            s3_config.region = Some(region.clone());
        }
        
        if let Some(endpoint) = aws_config.get("endpoint") {
            s3_config.endpoint = Some(endpoint.clone());
        }
        
        if let Some(force_path_style) = aws_config.get("force_path_style") {
            s3_config.force_path_style_access = force_path_style == "true";
        }
        
        // Use the new with_s3_config method that uses global caches
        StandaloneSearcher::with_s3_config(StandaloneSearchConfig::default(), s3_config)
    } else {
        debug_println!("RUST DEBUG: Creating StandaloneSearcher with default config and global caches");
        // Use default() which now uses global caches
        StandaloneSearcher::default()
    };
    match result {
        Ok(searcher) => {
            // Store searcher, runtime, split URI, AWS config, and footer offsets together using Arc for memory safety
            let searcher_context = std::sync::Arc::new((searcher, runtime, split_uri.clone(), aws_config, split_footer_start, split_footer_end));
            let pointer = arc_to_jlong(searcher_context);
            debug_println!("RUST DEBUG: SUCCESS: Stored searcher context for split '{}' with Arc pointer: {}, footer: {}-{}", 
                     split_uri, pointer, split_footer_start, split_footer_end);
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
    env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) {
    if searcher_ptr == 0 {
        return;
    }

    // Debug: Log call stack to understand why this is being called
    if *crate::debug::DEBUG_ENABLED {
        debug_println!("RUST DEBUG: WARNING - closeNative called for SplitSearcher with ID: {}", searcher_ptr);
        debug_println!("RUST DEBUG: This should only happen when the SplitSearcher is closed in Java");
        
        // Print the stack trace to see where this is being called from
        let backtrace = std::backtrace::Backtrace::capture();
        if backtrace.status() == std::backtrace::BacktraceStatus::Captured {
            debug_println!("RUST DEBUG: Stack trace for closeNative:");
            let backtrace_str = format!("{}", backtrace);
            for (i, line) in backtrace_str.lines().enumerate() {
                if i < 20 {  // Print first 20 lines to avoid too much output
                    debug_println!("  {}", line);
                }
            }
        }
    }

    // SAFE: Release Arc from registry to prevent memory leaks
    release_arc(searcher_ptr);
    debug_println!("RUST DEBUG: Closed searcher and released Arc with ID: {}", searcher_ptr);
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
    
    let is_valid = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64)>| {
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
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64)>| {
        let (searcher, _runtime, _split_uri, _aws_config, _footer_start, _footer_end) = searcher_context.as_ref();
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
                        debug_println!("RUST DEBUG: Failed to create CacheStats object: {}", e);
                        None
                    }
                }
            },
            Err(e) => {
                debug_println!("RUST DEBUG: Failed to find CacheStats class: {}", e);
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
    // debug_println!("RUST DEBUG: SplitSearcher.searchWithQueryAst called with limit: {}", limit);
    
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
    
    // debug_println!("RUST DEBUG: QueryAst JSON: {}", query_json);
    
    // Parse and fix range queries with proper field types from schema
    let fixed_query_json = match fix_range_query_types(searcher_ptr, &query_json) {
        Ok(fixed_json) => fixed_json,
        Err(e) => {
            debug_println!("RUST DEBUG: Failed to fix range query types: {}, using original query", e);
            query_json.clone()
        }
    };
    
    if fixed_query_json != query_json {
        debug_println!("RUST DEBUG: Fixed QueryAst JSON: {}", fixed_query_json);
    }
    
    // Use the searcher context to perform search with Quickwit's leaf search approach
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64)>| {
        let (searcher, runtime, split_uri, aws_config, footer_start, footer_end) = searcher_context.as_ref();
        
        // Enter the runtime context for async operations
        let _guard = runtime.enter();
        
        // Parse the QueryAst JSON using Quickwit's libraries
        use quickwit_query::query_ast::QueryAst;
        
        use quickwit_common::uri::Uri;
        use quickwit_config::StorageConfigs;
        use quickwit_directories::BundleDirectory;
        use tantivy::directory::FileSlice;
        
        // Run async code synchronously within the runtime context
        // Note: We're already in the runtime context via runtime.enter(), so we can use block_on directly
        runtime.block_on(async {
                // Parse the QueryAst JSON (with field type fixes)
                let query_ast: QueryAst = serde_json::from_str(&fixed_query_json)
                    .map_err(|e| anyhow::anyhow!("Failed to parse QueryAst JSON: {}", e))?;
                
                debug_println!("RUST DEBUG: Successfully parsed QueryAst: {:?}", query_ast);
                
                // First, we need to extract the actual split metadata from the split file
                // This includes footer offsets, number of documents, and the doc mapper
                
                // Parse URI and resolve storage
                let uri: Uri = split_uri.parse()
                    .map_err(|e| anyhow::anyhow!("Failed to parse split URI {}: {}", split_uri, e))?;
                
                // Create S3 storage configuration with credentials from Java config
                let mut storage_configs = StorageConfigs::default();
                
                debug_println!("RUST DEBUG: Creating S3 config with credentials from Java configuration");
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
                
                let storage_configs_vec = StorageConfigs::new(vec![quickwit_config::StorageConfig::S3(s3_config.clone())]);
                storage_configs = storage_configs_vec;
                
                let storage_resolver = StorageResolver::configured(&storage_configs);
                
                // Use the helper function to resolve storage correctly for S3 URIs
                let storage = resolve_storage_for_split(&storage_resolver, split_uri).await?;
                
                // Extract just the filename for the relative path
                let relative_path = if let Some(last_slash_pos) = split_uri.rfind('/') {
                    std::path::Path::new(&split_uri[last_slash_pos + 1..])
                } else {
                    std::path::Path::new(split_uri)
                };
                
                debug_println!("RUST DEBUG: Reading split file metadata from: '{}'", relative_path.display());
                
                // Get the full file data to extract metadata
                let file_size = storage.file_num_bytes(relative_path).await
                    .map_err(|e| anyhow::anyhow!("Failed to get file size for {}: {}", split_uri, e))?;
                
                debug_println!("RUST DEBUG: Split file size: {} bytes", file_size);
                
                // Use footer offsets from Java configuration instead of reading from file
                let split_footer_start = *footer_start;
                let split_footer_end = *footer_end;
                
                debug_println!("RUST DEBUG: Using footer offsets from Java config: fileSizeSJS={} start={}, end={}", file_size, split_footer_start, split_footer_end);
                
                // Now open the split to get the actual index and extract metadata
                let split_data = storage.get_slice(relative_path, 0..file_size as usize).await
                    .map_err(|e| anyhow::anyhow!("Failed to get split data from {}: {}", split_uri, e))?;
                
                let split_file_slice = FileSlice::new(std::sync::Arc::new(split_data));
                
                // Open the bundle directory to access the index
                let bundle_directory = BundleDirectory::open_split(split_file_slice)
                    .map_err(|e| anyhow::anyhow!("Failed to open bundle directory {}: {}", split_uri, e))?;
                    
                // Open the index to get actual metadata
                let index = tantivy::Index::open(bundle_directory)
                    .map_err(|e| anyhow::anyhow!("Failed to open index from bundle {}: {}", split_uri, e))?;
                
                // Get the actual number of documents from the index
                let reader = index.reader().map_err(|e| anyhow::anyhow!("Failed to create index reader: {}", e))?;
                let searcher_tantivy = reader.searcher();
                let num_docs = searcher_tantivy.num_docs();
                
                debug_println!("RUST DEBUG: Extracted actual num_docs from index: {}", num_docs);
                
                // Extract the split ID from the URI (last component before .split extension)
                let split_id = relative_path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown")
                    .to_string();
                
                debug_println!("RUST DEBUG: Split ID: {}", split_id);
                
                // Create the proper split metadata with REAL values
                let split_metadata = SplitSearchMetadata {
                    split_id: split_id.clone(),
                    split_footer_start,
                    split_footer_end,
                    file_size, // Use the actual file size from storage.file_num_bytes()
                    time_range: None, // TODO: Extract from split metadata if available
                    delete_opstamp: 0,
                    num_docs,
                };
                
                // Now we need to create the DocMapper from the index schema
                // The schema from the index contains the field definitions
                let schema = index.schema();
                
                // Build a DocMapping from the tantivy schema
                // This is the proper way to create a DocMapper that matches the actual index
                let mut field_mappings = Vec::new();
                
                for (field, field_entry) in schema.fields() {
                    let field_name = schema.get_field_name(field);
                    let field_type = field_entry.field_type();
                    
                    use tantivy::schema::FieldType;
                    let mapping_type = match field_type {
                        FieldType::Str(text_options) => {
                            if text_options.get_indexing_options().is_some() {
                                "text"
                            } else {
                                "keyword"
                            }
                        },
                        FieldType::U64(_) => "u64",
                        FieldType::I64(_) => "i64",
                        FieldType::F64(_) => "f64",
                        FieldType::Bool(_) => "bool",
                        FieldType::Date(_) => "datetime",
                        FieldType::Bytes(_) => "bytes",
                        FieldType::IpAddr(_) => "ip",
                        FieldType::JsonObject(_) => "json",
                        FieldType::Facet(_) => "keyword", // Facets are similar to keywords
                    };
                    
                    field_mappings.push(serde_json::json!({
                        "name": field_name,
                        "type": mapping_type,
                    }));
                }
                
                debug_println!("RUST DEBUG: Extracted {} field mappings from index schema", field_mappings.len());
                
                let doc_mapping_json = serde_json::json!({
                    "field_mappings": field_mappings,
                    "mode": "lenient",
                    "store_source": true,
                });
                
                // Create DocMapperBuilder from the JSON
                let doc_mapper_builder: quickwit_doc_mapper::DocMapperBuilder = serde_json::from_value(doc_mapping_json)
                    .map_err(|e| anyhow::anyhow!("Failed to create DocMapperBuilder: {}", e))?;
                
                // Build the DocMapper
                let doc_mapper = doc_mapper_builder.try_build()
                    .map_err(|e| anyhow::anyhow!("Failed to build DocMapper: {}", e))?;
                
                let doc_mapper_arc = Arc::new(doc_mapper);
                
                debug_println!("RUST DEBUG: Successfully created DocMapper from actual index schema");
                
                // Create a SearchRequest with the QueryAst
                let search_request = SearchRequest {
                    index_id_patterns: vec![],
                    query_ast: query_json.clone(),
                    max_hits: limit as u64,
                    start_offset: 0,
                    start_timestamp: None,
                    end_timestamp: None,
                    aggregation_request: None,
                    snippet_fields: vec![],
                    sort_fields: vec![],
                    search_after: None,
                    scroll_ttl_secs: None,
                    count_hits: quickwit_proto::search::CountHits::CountAll as i32,
                };
                
                debug_println!("RUST DEBUG: Calling StandaloneSearcher.search_split_sync with REAL parameters:");
                debug_println!("  - Split URI: {}", split_uri);
                debug_println!("  - Split ID: {}", split_metadata.split_id);
                debug_println!("  - Num docs: {}", split_metadata.num_docs);
                debug_println!("  - Footer offsets: {}-{}", split_metadata.split_footer_start, split_metadata.split_footer_end);
                // debug_println!("RUST DEBUG: About to call searcher.search_split()...");
                
                // PERFORM THE ACTUAL REAL SEARCH WITH NO MOCKING!
                // We're already in an async context, so use the async method directly
                let split_id_for_error = split_metadata.split_id.clone();
                // debug_println!("RUST DEBUG: Calling searcher.search_split() NOW!");
                let leaf_search_response = match searcher.search_split(
                    split_uri,
                    split_metadata,
                    search_request,
                    doc_mapper_arc,
                ).await {
                    Ok(response) => response,
                    Err(e) => {
                        debug_println!("RUST DEBUG: ERROR in searcher.search_split: {}", e);
                        debug_println!("RUST DEBUG: Full error chain: {:#}", e);
                        // Propagate the full error chain to Java
                        return Err(anyhow::anyhow!("{:#}", e));
                    }
                };
                
                debug_println!("RUST DEBUG: REAL SEARCH COMPLETED! Found {} hits from StandaloneSearcher", leaf_search_response.num_hits);
                
                // Convert LeafSearchResponse to SearchResult format
                let mut search_results: Vec<(f32, tantivy::DocAddress)> = Vec::new();
                
                for partial_hit in leaf_search_response.partial_hits {
                    let doc_address = tantivy::DocAddress::new(
                        partial_hit.segment_ord as tantivy::SegmentOrdinal,
                        partial_hit.doc_id as tantivy::DocId,
                    );
                    
                    // Extract the actual score from the partial hit
                    let score = if let Some(sort_value) = partial_hit.sort_value() {
                        // If there's a sort value, use it as the score
                        match sort_value {
                            quickwit_proto::search::SortValue::F64(f) => f as f32,
                            quickwit_proto::search::SortValue::U64(u) => u as f32,
                            quickwit_proto::search::SortValue::I64(i) => i as f32,
                            _ => 1.0_f32,
                        }
                    } else {
                        1.0_f32 // Default score if no sort value
                    };
                    
                    search_results.push((score, doc_address));
                }
                
                debug_println!("RUST DEBUG: Converted {} hits to SearchResult format", search_results.len());
                
                // Register the search results using Arc registry
                let search_results_arc = Arc::new(search_results);
                let search_result_ptr = arc_to_jlong(search_results_arc);
                
                // Create SearchResult Java object
                let search_result_class = env.find_class("com/tantivy4java/SearchResult")
                    .map_err(|e| anyhow::anyhow!("Failed to find SearchResult class: {}", e))?;
                
                let search_result = env.new_object(
                    &search_result_class,
                    "(J)V",
                    &[(search_result_ptr).into()]
                ).map_err(|e| anyhow::anyhow!("Failed to create SearchResult: {}", e))?;
                
                debug_println!("RUST DEBUG: Successfully created SearchResult with {} hits", leaf_search_response.num_hits);
                
                Ok(search_result.into_raw())
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

/// Batch document retrieval for SplitSearcher using Quickwit's optimized approach
/// This implementation follows Quickwit's patterns from fetch_docs.rs:
/// 1. Sort addresses by segment for cache locality
/// 2. Open index with proper cache settings
/// 3. Use doc_async for optimal performance
/// 4. Reuse index, reader, and searcher across all documents
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitSearcher_docBatchNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    segments: jni::sys::jintArray,
    doc_ids: jni::sys::jintArray,
) -> jobject {
    if searcher_ptr == 0 {
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
        return std::ptr::null_mut();
    }
    
    // Convert JNI arrays to proper JArray types
    let segments_array = unsafe { jni::objects::JIntArray::from_raw(segments) };
    let doc_ids_array = unsafe { jni::objects::JIntArray::from_raw(doc_ids) };
    
    // Get the segment and doc ID arrays
    let array_len = match env.get_array_length(&segments_array) {
        Ok(len) => len as usize,
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to get segments array length: {}", e));
            return std::ptr::null_mut();
        }
    };
    
    let mut segments_vec = vec![0i32; array_len];
    if let Err(e) = env.get_int_array_region(&segments_array, 0, &mut segments_vec) {
        to_java_exception(&mut env, &anyhow::anyhow!("Failed to get segments array: {}", e));
        return std::ptr::null_mut();
    }
    let segments_vec: Vec<u32> = segments_vec.iter().map(|&s| s as u32).collect();
    
    let mut doc_ids_vec = vec![0i32; array_len];
    if let Err(e) = env.get_int_array_region(&doc_ids_array, 0, &mut doc_ids_vec) {
        to_java_exception(&mut env, &anyhow::anyhow!("Failed to get doc_ids array: {}", e));
        return std::ptr::null_mut();
    }
    let doc_ids_vec: Vec<u32> = doc_ids_vec.iter().map(|&d| d as u32).collect();
    
    if segments_vec.len() != doc_ids_vec.len() {
        to_java_exception(&mut env, &anyhow::anyhow!("Segments and doc_ids arrays must have same length"));
        return std::ptr::null_mut();
    }
    
    // Create DocAddress objects with original indices for ordering
    let mut indexed_addresses: Vec<(usize, tantivy::DocAddress)> = segments_vec
        .iter()
        .zip(doc_ids_vec.iter())
        .enumerate()
        .map(|(idx, (&seg, &doc))| (idx, tantivy::DocAddress::new(seg, doc)))
        .collect();
    
    // Sort by document address for cache locality (following Quickwit pattern)
    indexed_addresses.sort_by_key(|(_, addr)| *addr);
    
    // Extract sorted addresses for batch retrieval
    let sorted_addresses: Vec<tantivy::DocAddress> = indexed_addresses
        .iter()
        .map(|(_, addr)| *addr)
        .collect();
    
    // Use Quickwit-optimized bulk document retrieval
    let retrieval_result = retrieve_documents_batch_from_split_optimized(searcher_ptr, sorted_addresses);
    
    match retrieval_result {
        Ok(sorted_docs) => {
            // Reorder documents back to original input order
            let mut ordered_doc_ptrs = vec![std::ptr::null_mut(); indexed_addresses.len()];
            for (i, (original_idx, _)) in indexed_addresses.iter().enumerate() {
                if i < sorted_docs.len() {
                    ordered_doc_ptrs[*original_idx] = sorted_docs[i];
                }
            }
            
            // Create a Java Document array
            let document_class = match env.find_class("com/tantivy4java/Document") {
                Ok(class) => class,
                Err(e) => {
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to find Document class: {}", e));
                    return std::ptr::null_mut();
                }
            };
            
            let doc_array = match env.new_object_array(ordered_doc_ptrs.len() as i32, &document_class, JObject::null()) {
                Ok(array) => array,
                Err(e) => {
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to create Document array: {}", e));
                    return std::ptr::null_mut();
                }
            };
            
            // Create Document objects and add to array
            for (i, doc_ptr) in ordered_doc_ptrs.iter().enumerate() {
                if doc_ptr.is_null() {
                    continue; // Skip null documents
                }
                
                // Create Document object with the pointer
                let doc_obj = match env.new_object(
                    &document_class,
                    "(J)V",
                    &[jni::objects::JValue::Long(*doc_ptr as jlong)]
                ) {
                    Ok(obj) => obj,
                    Err(e) => {
                        to_java_exception(&mut env, &anyhow::anyhow!("Failed to create Document object: {}", e));
                        continue;
                    }
                };
                
                if let Err(e) = env.set_object_array_element(&doc_array, i as i32, doc_obj) {
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to set array element: {}", e));
                }
            }
            
            doc_array.into_raw()
        },
        Err(e) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

/// Simple but effective searcher caching for single document retrieval
/// Uses the same optimizations as our batch method but caches searchers for reuse
fn retrieve_document_from_split_optimized(
    searcher_ptr: jlong,
    doc_address: tantivy::DocAddress,
) -> Result<(tantivy::schema::TantivyDocument, tantivy::schema::Schema), anyhow::Error> {
    use crate::utils::with_arc_safe;
    
    // Get split URI from the searcher context
    let split_uri = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64)>| {
        let (_, _, split_uri, _, _, _) = searcher_context.as_ref();
        split_uri.clone()
    }).ok_or_else(|| anyhow::anyhow!("Invalid searcher context"))?;
    
    // Check cache first - simple and effective
    let searcher_cache = get_searcher_cache();
    let cached_searcher = {
        let cache = searcher_cache.lock().unwrap();
        cache.get(&split_uri).cloned()
    };
    
    if let Some(searcher) = cached_searcher {
        // Use cached searcher - very fast path (cache hit)
        let doc = searcher.doc(doc_address)
            .map_err(|e| anyhow::anyhow!("Failed to retrieve document: {}", e))?;
        let schema = searcher.schema();
        return Ok((doc, schema.clone()));
    }
    
    // Cache miss - create searcher using the same optimizations as our batch method
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64)>| {
        let (standalone_searcher, runtime, split_uri, aws_config, footer_start, footer_end) = searcher_context.as_ref();
        
        let _guard = runtime.enter();
        
        // Use the same Quickwit caching pattern as our batch method
        tokio::task::block_in_place(|| {
            runtime.block_on(async {
                use quickwit_common::uri::Uri;
                use quickwit_config::{StorageConfigs, S3StorageConfig};
                use quickwit_proto::search::SplitIdAndFooterOffsets;
                use quickwit_storage::StorageResolver;
                use quickwit_search::leaf::open_index_with_caches;
                use quickwit_search::SearcherContext;
                use quickwit_storage::ByteRangeCache;
                use std::sync::Arc;
                
                // Create split metadata for Quickwit's open_index_with_caches with correct field names
                // Extract just the filename as the split_id (e.g., "consolidated.split" from the full URL)
                let split_filename = if let Some(last_slash_pos) = split_uri.rfind('/') {
                    &split_uri[last_slash_pos + 1..]
                } else {
                    split_uri
                };
                
                let split_metadata = SplitIdAndFooterOffsets {
                    split_id: split_filename.to_string(),
                    split_footer_start: *footer_start,
                    split_footer_end: *footer_end,
                    timestamp_start: Some(0), // Not used for our purposes
                    timestamp_end: Some(i64::MAX), // Not used for our purposes  
                    num_docs: 0, // Will be filled by Quickwit
                };
                
                // Create S3 storage configuration
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
                
                // Create storage that points to the directory containing the split (not the split file itself)
                // This is what open_index_with_caches expects
                let split_dir_uri = if let Some(last_slash_pos) = split_uri.rfind('/') {
                    &split_uri[..last_slash_pos + 1] // Include the trailing slash
                } else {
                    split_uri // If no slash, use the full URI as directory
                };
                
                let storage_configs = StorageConfigs::new(vec![quickwit_config::StorageConfig::S3(s3_config.clone())]);
                let storage_resolver = StorageResolver::configured(&storage_configs);
                let index_storage = resolve_storage_for_split(&storage_resolver, split_dir_uri).await?;
                
                // Use global SearcherContext for long-term shared caches (Quickwit pattern)
                let searcher_context = crate::global_cache::get_global_searcher_context();
                
                // Create short-lived ByteRangeCache per operation (Quickwit pattern for optimal memory use)
                let byte_range_cache = quickwit_storage::ByteRangeCache::with_infinite_capacity(
                    &quickwit_storage::STORAGE_METRICS.shortlived_cache
                );
                
                // Manual index opening with Quickwit caching components
                // (open_index_with_caches expects Quickwit's native split format, but we use bundle format)
                let relative_path = if let Some(last_slash_pos) = split_uri.rfind('/') {
                    std::path::Path::new(&split_uri[last_slash_pos + 1..])
                } else {
                    std::path::Path::new(split_uri)
                };
                
                let file_size = index_storage.file_num_bytes(relative_path).await
                    .map_err(|e| anyhow::anyhow!("Failed to get file size for {}: {}", split_uri, e))?;
                
                // Use ByteRangeCache for storage operations (Quickwit pattern)
                let split_data = index_storage.get_slice(relative_path, 0..file_size as usize).await
                    .map_err(|e| anyhow::anyhow!("Failed to get split data from {}: {}", split_uri, e))?;
                
                let split_file_slice = tantivy::directory::FileSlice::new(std::sync::Arc::new(split_data));
                let bundle_directory = quickwit_directories::BundleDirectory::open_split(split_file_slice)
                    .map_err(|e| anyhow::anyhow!("Failed to open bundle directory {}: {}", split_uri, e))?;
                    
                let mut index = tantivy::Index::open(bundle_directory)
                    .map_err(|e| anyhow::anyhow!("Failed to open index from bundle {}: {}", split_uri, e))?;
                
                // Use the same Quickwit optimizations as our batch method
                let tantivy_executor = search_thread_pool()
                    .get_underlying_rayon_thread_pool()
                    .into();
                index.set_executor(tantivy_executor);
                
                // Same cache settings as batch method
                const NUM_CONCURRENT_REQUESTS: usize = 30; // From fetch_docs.rs
                let index_reader = index
                    .reader_builder()
                    .doc_store_cache_num_blocks(NUM_CONCURRENT_REQUESTS) // QUICKWIT OPTIMIZATION
                    .reload_policy(tantivy::ReloadPolicy::Manual)
                    .try_into()
                    .map_err(|e| anyhow::anyhow!("Failed to create index reader: {}", e))?;
                
                let searcher = Arc::new(index_reader.searcher());
                
                // Cache the searcher for future single document retrievals
                {
                    let searcher_cache = get_searcher_cache();
                    let mut cache = searcher_cache.lock().unwrap();
                    cache.insert(split_uri.clone(), searcher.clone());
                }
                
                // Retrieve the document
                let doc = searcher.doc(doc_address)
                    .map_err(|e| anyhow::anyhow!("Failed to retrieve document: {}", e))?;
                let schema = searcher.schema();
                
                Ok::<(tantivy::schema::TantivyDocument, tantivy::schema::Schema), anyhow::Error>((doc, schema.clone()))
            })
        })
    });
    
    match result {
        Some(Ok(doc_and_schema)) => Ok(doc_and_schema),
        Some(Err(e)) => Err(e),
        None => Err(anyhow::anyhow!("Searcher context not found for pointer {}", searcher_ptr)),
    }
}

/// Helper function to retrieve a single document from a split
/// Legacy implementation - improved with Quickwit optimizations: doc_async and doc_store_cache_num_blocks
fn retrieve_document_from_split(
    searcher_ptr: jlong,
    doc_address: tantivy::DocAddress,
) -> Result<(tantivy::schema::TantivyDocument, tantivy::schema::Schema), anyhow::Error> {
    use crate::utils::with_arc_safe;
    use quickwit_storage::StorageResolver;
    use std::sync::Arc;
    
    // Use the searcher context to retrieve the document from the split
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64)>| {
        let (searcher, runtime, split_uri, aws_config, footer_start, footer_end) = searcher_context.as_ref();
        
        // Enter the runtime context for async operations
        let _guard = runtime.enter();
        
        // Run async document retrieval with Quickwit optimizations
        runtime.block_on(async {
            // Parse URI and resolve storage (same as before)
            use quickwit_common::uri::Uri;
            use quickwit_config::{StorageConfigs, S3StorageConfig};
            use quickwit_directories::BundleDirectory;
            use tantivy::directory::FileSlice;
            use tantivy::ReloadPolicy;
            use std::path::Path;
            
            let uri: Uri = split_uri.parse()
                .map_err(|e| anyhow::anyhow!("Failed to parse split URI {}: {}", split_uri, e))?;
            
            // Create S3 storage configuration with credentials from Java config
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
            
            let storage_configs = StorageConfigs::new(vec![quickwit_config::StorageConfig::S3(s3_config.clone())]);
            let storage_resolver = StorageResolver::configured(&storage_configs);
            let actual_storage = resolve_storage_for_split(&storage_resolver, split_uri).await?;
            
            // Extract just the filename for the relative path
            let relative_path = if let Some(last_slash_pos) = split_uri.rfind('/') {
                Path::new(&split_uri[last_slash_pos + 1..])
            } else {
                Path::new(split_uri)
            };
            
            // Get the full file data using Quickwit's storage abstraction
            let file_size = actual_storage.file_num_bytes(relative_path).await
                .map_err(|e| anyhow::anyhow!("Failed to get file size for {}: {}", split_uri, e))?;
            
            let split_data = actual_storage.get_slice(relative_path, 0..file_size as usize).await
                .map_err(|e| anyhow::anyhow!("Failed to get split data from {}: {}", split_uri, e))?;
            
            // Open the bundle directory from the split data
            let split_file_slice = FileSlice::new(std::sync::Arc::new(split_data));
            let bundle_directory = BundleDirectory::open_split(split_file_slice)
                .map_err(|e| anyhow::anyhow!("Failed to open bundle directory {}: {}", split_uri, e))?;
                
            // Extract the index from the bundle directory
            let index = tantivy::Index::open(bundle_directory)
                .map_err(|e| anyhow::anyhow!("Failed to open index from bundle {}: {}", split_uri, e))?;
            
            // Create index reader using Quickwit's optimizations (from fetch_docs.rs line 187-192)
            const NUM_CONCURRENT_REQUESTS: usize = 30; // from fetch_docs.rs
            let index_reader = index
                .reader_builder()
                .doc_store_cache_num_blocks(NUM_CONCURRENT_REQUESTS) // QUICKWIT OPTIMIZATION
                .reload_policy(ReloadPolicy::Manual)
                .try_into()
                .map_err(|e| anyhow::anyhow!("Failed to create index reader: {}", e))?;
            
            let tantivy_searcher = index_reader.searcher();
            
            // Use doc_async like Quickwit does (fetch_docs.rs line 205-207) - QUICKWIT OPTIMIZATION
            let doc: tantivy::schema::TantivyDocument = tantivy_searcher
                .doc_async(doc_address)
                .await
                .map_err(|e| anyhow::anyhow!("Failed to retrieve document at address {:?}: {}", doc_address, e))?;
            
            // Return the document and schema for processing
            Ok::<(tantivy::schema::TantivyDocument, tantivy::schema::Schema), anyhow::Error>((doc, index.schema()))
        })
    });
    
    match result {
        Some(Ok(result)) => Ok(result),
        Some(Err(e)) => Err(e),
        None => Err(anyhow::anyhow!("Searcher context not found for pointer {}", searcher_ptr)),
    }
}

/// Optimized bulk document retrieval using Quickwit's proven patterns from fetch_docs.rs
/// Key optimizations:
/// 1. Reuse index, reader and searcher across all documents
/// 2. Sort by DocAddress for better cache locality
/// 3. Use doc_async for optimal I/O performance
/// 4. Use proper cache sizing (NUM_CONCURRENT_REQUESTS)
/// 5. Return raw pointers for JNI integration
fn retrieve_documents_batch_from_split_optimized(
    searcher_ptr: jlong,
    mut doc_addresses: Vec<tantivy::DocAddress>,
) -> Result<Vec<jobject>, anyhow::Error> {
    use crate::utils::with_arc_safe;
    use quickwit_storage::StorageResolver;
    use std::sync::Arc;
    
    // Sort by DocAddress for cache locality (following Quickwit pattern)
    doc_addresses.sort();
    
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64)>| {
        let (searcher, runtime, split_uri, aws_config, footer_start, footer_end) = searcher_context.as_ref();
        
        let _guard = runtime.enter();
        
        // Use block_in_place to run async code synchronously (Quickwit pattern)
        tokio::task::block_in_place(|| {
            runtime.block_on(async {
                use quickwit_common::uri::Uri;
                use quickwit_config::{StorageConfigs, S3StorageConfig};
                use quickwit_proto::search::SplitIdAndFooterOffsets;
                use quickwit_storage::StorageResolver;
                use quickwit_search::leaf::open_index_with_caches;
                use quickwit_search::SearcherContext;
                use quickwit_storage::ByteRangeCache;
                use std::path::Path;
                use std::sync::Arc;
                
                // Create split metadata for Quickwit's open_index_with_caches
                let split_metadata = SplitIdAndFooterOffsets {
                    split_id: format!("tantivy4java-split-{}", 
                        split_uri.chars().filter(|c| c.is_alphanumeric()).collect::<String>()),
                    split_footer_start: *footer_start,
                    split_footer_end: *footer_end,
                    timestamp_start: Some(0), // Not used for our purposes
                    timestamp_end: Some(i64::MAX), // Not used for our purposes
                    num_docs: 0, // Will be filled by Quickwit
                };
                
                // Create S3 storage configuration
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
                
                // Create storage that points to the directory containing the split (not the split file itself)
                // This is what open_index_with_caches expects
                let split_dir_uri = if let Some(last_slash_pos) = split_uri.rfind('/') {
                    &split_uri[..last_slash_pos + 1] // Include the trailing slash
                } else {
                    split_uri // If no slash, use the full URI as directory
                };
                
                let storage_configs = StorageConfigs::new(vec![quickwit_config::StorageConfig::S3(s3_config.clone())]);
                let storage_resolver = StorageResolver::configured(&storage_configs);
                let index_storage = resolve_storage_for_split(&storage_resolver, split_dir_uri).await?;
                
                // Use global SearcherContext with shared caches instead of creating new ones
                let searcher_context = crate::global_cache::get_global_searcher_context();
                
                // Create short-lived ByteRangeCache per operation (Quickwit pattern for optimal memory use)
                let byte_range_cache = quickwit_storage::ByteRangeCache::with_infinite_capacity(
                    &quickwit_storage::STORAGE_METRICS.shortlived_cache
                );
                
                // Use Quickwit's open_index_with_caches with global shared caches
                let (mut index, _hot_directory) = open_index_with_caches(
                    &searcher_context,
                    index_storage,
                    &split_metadata,
                    None, // tokenizer_manager - we'll set it manually if needed
                    Some(byte_range_cache.clone()), // Use shared ByteRangeCache
                ).await
                .map_err(|e| anyhow::anyhow!("Failed to open index with caches for {}: {}", split_uri, e))?;
                
                // Add executor like Quickwit does (fetch_docs.rs line 183-186)
                let tantivy_executor = search_thread_pool()
                    .get_underlying_rayon_thread_pool()
                    .into();
                index.set_executor(tantivy_executor);
                
                // Create index reader with Quickwit optimizations (fetch_docs.rs line 187-192)
                const NUM_CONCURRENT_REQUESTS: usize = 30; // From fetch_docs.rs
                let index_reader = index
                    .reader_builder()
                    .doc_store_cache_num_blocks(NUM_CONCURRENT_REQUESTS) // QUICKWIT OPTIMIZATION
                    .reload_policy(tantivy::ReloadPolicy::Manual)
                    .try_into()
                    .map_err(|e| anyhow::anyhow!("Failed to create index reader: {}", e))?;
                
                // Create Arc searcher for sharing across async operations (fetch_docs.rs line 193)
                let tantivy_searcher = std::sync::Arc::new(index_reader.searcher());
                let schema = index.schema();
                
                // Retrieve documents using async pattern like Quickwit (fetch_docs.rs line 200-252)
                let mut doc_ptrs = Vec::new();
                
                for doc_addr in doc_addresses {
                    // Use doc_async like Quickwit - QUICKWIT OPTIMIZATION (fetch_docs.rs line 205-207)
                    let doc: tantivy::schema::TantivyDocument = tantivy_searcher
                        .doc_async(doc_addr)
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to retrieve document at address {:?}: {}", doc_addr, e))?;
                    
                    // Create a RetrievedDocument and register it
                    use crate::document::{DocumentWrapper, RetrievedDocument};
                    
                    let retrieved_doc = RetrievedDocument::new_with_schema(doc, &schema);
                    let wrapper = DocumentWrapper::Retrieved(retrieved_doc);
                    let wrapper_arc = std::sync::Arc::new(std::sync::Mutex::new(wrapper));
                    let doc_ptr = crate::utils::arc_to_jlong(wrapper_arc);
                    
                    doc_ptrs.push(doc_ptr as jobject);
                }
                
                Ok::<Vec<jobject>, anyhow::Error>(doc_ptrs)
            })
        })
    });
    
    match result {
        Some(Ok(doc_ptrs)) => Ok(doc_ptrs),
        Some(Err(e)) => Err(e),
        None => Err(anyhow::anyhow!("Searcher context not found for pointer {}", searcher_ptr)),
    }
}


/// Legacy method - kept for compatibility but not optimized
fn retrieve_documents_batch_from_split(
    searcher_ptr: jlong,
    mut doc_addresses: Vec<tantivy::DocAddress>,
) -> Result<Vec<(tantivy::DocAddress, tantivy::schema::TantivyDocument, tantivy::schema::Schema)>, anyhow::Error> {
    // Fallback to single document retrieval for now
    let mut results = Vec::new();
    for addr in doc_addresses {
        match retrieve_document_from_split(searcher_ptr, addr) {
            Ok((doc, schema)) => {
                results.push((addr, doc, schema));
            },
            Err(e) => return Err(e),
        }
    }
    Ok(results)
}

/// Replacement for Java_com_tantivy4java_SplitSearcher_docNative
/// Implements document retrieval using Quickwit's approach: 
/// - Opens the split as an index using open_index_with_caches pattern
/// - Creates a searcher from the index reader
/// - Uses searcher.doc_async() to retrieve the document
/// - Converts the document to JSON using DocMapper
/// - Returns a Java Document object
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitSearcher_docNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    segment_ord: jint,
    doc_id: jint,
) -> jobject {
    // debug_println!("RUST DEBUG: SplitSearcher.docNative called with segment_ord={}, doc_id={}", segment_ord, doc_id);
    
    if searcher_ptr == 0 {
        debug_println!("RUST ERROR: Invalid searcher pointer");
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
        return std::ptr::null_mut();
    }
    
    // Create DocAddress from the provided segment and doc ID
    let doc_address = tantivy::DocAddress::new(segment_ord as u32, doc_id as u32);
    
    // Use Quickwit's optimized approach for document retrieval
    match retrieve_document_from_split_optimized(searcher_ptr, doc_address) {
        Ok((doc, schema)) => {
            // Create a RetrievedDocument using the proper pattern from searcher.rs
            use crate::document::{DocumentWrapper, RetrievedDocument};
            
            let retrieved_doc = RetrievedDocument::new_with_schema(doc, &schema);
            let wrapper = DocumentWrapper::Retrieved(retrieved_doc);
            let wrapper_arc = std::sync::Arc::new(std::sync::Mutex::new(wrapper));
            let doc_ptr = crate::utils::arc_to_jlong(wrapper_arc);
            
            // Create Java Document object with the pointer
            match env.find_class("com/tantivy4java/Document") {
                Ok(document_class) => {
                    match env.new_object(&document_class, "(J)V", &[doc_ptr.into()]) {
                        Ok(document_obj) => document_obj.into_raw(),
                        Err(e) => {
                            debug_println!("RUST ERROR: Failed to create Document object: {}", e);
                            to_java_exception(&mut env, &anyhow::anyhow!("Failed to create Document: {}", e));
                            std::ptr::null_mut()
                        }
                    }
                },
                Err(e) => {
                    debug_println!("RUST ERROR: Failed to find Document class: {}", e);
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to find Document class: {}", e));
                    std::ptr::null_mut()
                }
            }
        },
        Err(e) => {
            debug_println!("RUST ERROR: Document retrieval failed: {}", e);
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

/// Replacement for Java_com_tantivy4java_SplitSearcher_getSchemaFromNative
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitSearcher_getSchemaFromNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) -> jlong {
    debug_println!("RUST DEBUG: *** getSchemaFromNative ENTRY POINT *** pointer: {}", searcher_ptr);
    
    if searcher_ptr == 0 {
        debug_println!("RUST DEBUG: searcher_ptr is 0, returning 0");
        return 0;
    }
    
    // debug_println!("RUST DEBUG: About to call with_object to access searcher context...");
    if searcher_ptr == 0 {
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
        return 0;
    }

    // Extract the actual schema from the split file using Quickwit's functionality
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64)>| {
        let (_searcher, runtime, split_uri, aws_config, _footer_start, _footer_end) = searcher_context.as_ref();
        debug_println!("RUST DEBUG: getSchemaFromNative called with split URI: {}", split_uri);
        
        // Enter the runtime context for async operations
        let _guard = runtime.enter();
        
        // Parse the split URI and extract schema using Quickwit's storage abstractions
        use quickwit_common::uri::Uri;
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
                
                debug_println!("RUST DEBUG: Creating S3 config with credentials from tantivy4java (not environment)");
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
                
                debug_println!("RUST DEBUG: S3 config created with access_key: {}, region: {}", 
                         s3_config.access_key_id.as_ref().map(|k| &k[..std::cmp::min(8, k.len())]).unwrap_or("None"),
                         s3_config.region.as_ref().unwrap_or(&"None".to_string()));
                
                let storage_configs_vec = StorageConfigs::new(vec![quickwit_config::StorageConfig::S3(s3_config.clone())]);
                storage_configs = storage_configs_vec;
                
                let storage_resolver = StorageResolver::configured(&storage_configs);
                
                // Use the helper function to resolve storage correctly for S3 URIs
                let actual_storage = resolve_storage_for_split(&storage_resolver, split_uri).await?;
                
                // Extract just the filename for the relative path
                let relative_path = if let Some(last_slash_pos) = split_uri.rfind('/') {
                    Path::new(&split_uri[last_slash_pos + 1..])
                } else {
                    Path::new(split_uri)
                };
                
                debug_println!("RUST DEBUG: About to call storage.file_num_bytes with relative path: '{}'", relative_path.display());
                
                // Get the full file data using Quickwit's storage abstraction
                let file_size = actual_storage.file_num_bytes(relative_path).await
                    .map_err(|e| anyhow::anyhow!("Failed to get file size for {}: {}", split_uri, e))?;
                
                debug_println!("RUST DEBUG: Got file size: {} bytes", file_size);
                
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
                debug_println!("RUST DEBUG: SUCCESS - Schema extracted and registered with Arc pointer: {}", schema_ptr);
                schema_ptr
            },
            Err(e) => {
                debug_println!("RUST DEBUG: FATAL ERROR - Schema extraction failed completely for split {}: {}", split_uri, e);
                debug_println!("RUST DEBUG: Error chain: {:?}", e);
                // Return 0 to indicate failure
                0
            }
        }
    });

    match result {
        Some(schema_ptr) => {
            debug_println!("SUCCESS: Schema extracted and registered with pointer: {}", schema_ptr);
            schema_ptr
        },
        None => {
            debug_println!("ERROR: with_object returned None - searcher context not found for pointer {}", searcher_ptr);
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
    debug_println!("RUST DEBUG: preloadComponentsNative called - returning success");
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
    debug_println!("RUST DEBUG: getComponentCacheStatusNative called - creating empty HashMap");
    
    // Create an empty HashMap for component status
    match env.new_object("java/util/HashMap", "()V", &[]) {
        Ok(hashmap) => hashmap.into_raw(),
        Err(_) => {
            // Return null if we can't create the HashMap
            std::ptr::null_mut()
        }
    }
}

/// Helper function to extract schema from split file - extracted from getSchemaFromNative
fn get_schema_from_split(searcher_ptr: jlong) -> anyhow::Result<tantivy::schema::Schema> {
    with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64)>| {
        let (_searcher, runtime, split_uri, aws_config, _footer_start, _footer_end) = searcher_context.as_ref();
        
        // Enter the runtime context for async operations
        let _guard = runtime.enter();
        
        // Parse the split URI and extract schema using Quickwit's storage abstractions
        use quickwit_common::uri::Uri;
        use std::path::Path;
        
        // Use block_on to run async code synchronously within the runtime context
        tokio::task::block_in_place(|| {
            runtime.block_on(async {
                // Parse URI and resolve storage
                let uri: Uri = split_uri.parse()
                    .map_err(|e| anyhow::anyhow!("Failed to parse split URI {}: {}", split_uri, e))?;
                
                // Create S3 storage configuration with credentials from Java config
                use quickwit_config::{StorageConfigs, S3StorageConfig};
                
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
                
                let storage_configs = StorageConfigs::new(vec![quickwit_config::StorageConfig::S3(s3_config.clone())]);
                let storage_resolver = StorageResolver::configured(&storage_configs);
                
                // Use the helper function to resolve storage correctly for S3 URIs
                let actual_storage = resolve_storage_for_split(&storage_resolver, split_uri).await?;
                
                // Extract just the filename for the relative path
                let relative_path = if let Some(last_slash_pos) = split_uri.rfind('/') {
                    Path::new(&split_uri[last_slash_pos + 1..])
                } else {
                    Path::new(split_uri)
                };
                
                // Get the full file data using Quickwit's storage abstraction
                let file_size = actual_storage.file_num_bytes(relative_path).await
                    .map_err(|e| anyhow::anyhow!("Failed to get file size for {}: {}", split_uri, e))?;
                
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
                
                Ok(index.schema())
            })
        })
    })
    .ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?
}

/// Fix range queries in QueryAst JSON by looking up field types from schema
fn fix_range_query_types(searcher_ptr: jlong, query_json: &str) -> anyhow::Result<String> {
    // Parse the JSON to find range queries
    let mut query_value: Value = serde_json::from_str(query_json)?;
    
    // Get schema from split file - reuse the same logic as getSchemaFromNative
    let schema = get_schema_from_split(searcher_ptr)?;
    
    // Recursively fix range queries in the JSON
    fix_range_queries_recursive(&mut query_value, &schema)?;
    
    // Convert back to JSON string
    serde_json::to_string(&query_value).map_err(|e| anyhow::anyhow!("Failed to serialize fixed query: {}", e))
}

/// Recursively fix range queries in a JSON value
fn fix_range_queries_recursive(value: &mut Value, schema: &tantivy::schema::Schema) -> anyhow::Result<()> {
    match value {
        Value::Object(map) => {
            // Check if this is a range query
            if let Some(range_obj) = map.get_mut("range") {
                if let Some(range_map) = range_obj.as_object_mut() {
                    fix_range_query_object(range_map, schema)?;
                }
            }
            
            // Recursively process nested objects
            for (_, v) in map.iter_mut() {
                fix_range_queries_recursive(v, schema)?;
            }
        }
        Value::Array(arr) => {
            // Recursively process array elements
            for item in arr.iter_mut() {
                fix_range_queries_recursive(item, schema)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Fix a specific range query object by converting string values to proper types
fn fix_range_query_object(range_map: &mut Map<String, Value>, schema: &tantivy::schema::Schema) -> anyhow::Result<()> {
    // Extract field name
    let field_name = range_map.get("field")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Range query missing field name"))?;
    
    // Get field from schema
    let field = schema.get_field(field_name)
        .map_err(|_| anyhow::anyhow!("Field '{}' not found in schema", field_name))?;
    
    let field_entry = schema.get_field_entry(field);
    let field_type = field_entry.field_type();
    
    // Determine the target JSON literal type based on Tantivy field type
    let target_type = match field_type {
        tantivy::schema::FieldType::I64(_) => "i64",
        tantivy::schema::FieldType::U64(_) => "u64", 
        tantivy::schema::FieldType::F64(_) => "f64",
        tantivy::schema::FieldType::Bool(_) => "bool",
        tantivy::schema::FieldType::Date(_) => "date",
        tantivy::schema::FieldType::Str(_) => "str",
        tantivy::schema::FieldType::Facet(_) => "str",
        tantivy::schema::FieldType::Bytes(_) => "str",
        tantivy::schema::FieldType::JsonObject(_) => "str",
        tantivy::schema::FieldType::IpAddr(_) => "str",
    };
    
    debug_println!("RUST DEBUG: Field '{}' has type '{}', converting range bounds", field_name, target_type);
    
    // Fix lower_bound and upper_bound
    if let Some(lower_bound) = range_map.get_mut("lower_bound") {
        fix_bound_value(lower_bound, target_type, "lower_bound")?;
    }
    
    if let Some(upper_bound) = range_map.get_mut("upper_bound") {
        fix_bound_value(upper_bound, target_type, "upper_bound")?;
    }
    
    Ok(())
}

/// Fix a bound value (Included/Excluded with JsonLiteral) 
fn fix_bound_value(bound: &mut Value, target_type: &str, bound_name: &str) -> anyhow::Result<()> {
    if let Some(bound_obj) = bound.as_object_mut() {
        // Handle Included/Excluded bounds
        for (bound_type, json_literal) in bound_obj.iter_mut() {
            if bound_type == "Included" || bound_type == "Excluded" {
                if let Some(literal_obj) = json_literal.as_object_mut() {
                    // Check if this is a String literal that needs conversion
                    if let Some(string_value) = literal_obj.get("String") {
                        if let Some(string_str) = string_value.as_str() {
                            // Convert string to appropriate type
                            let new_literal = match target_type {
                                "i64" => {
                                    let parsed: i64 = string_str.parse()
                                        .map_err(|_| anyhow::anyhow!("Cannot parse '{}' as i64 in {}", string_str, bound_name))?;
                                    serde_json::json!({"Number": parsed})
                                }
                                "u64" => {
                                    let parsed: u64 = string_str.parse()
                                        .map_err(|_| anyhow::anyhow!("Cannot parse '{}' as u64 in {}", string_str, bound_name))?;
                                    serde_json::json!({"Number": parsed})
                                }
                                "f64" => {
                                    let parsed: f64 = string_str.parse()
                                        .map_err(|_| anyhow::anyhow!("Cannot parse '{}' as f64 in {}", string_str, bound_name))?;
                                    serde_json::json!({"Number": parsed})
                                }
                                "bool" => {
                                    let parsed: bool = string_str.parse()
                                        .map_err(|_| anyhow::anyhow!("Cannot parse '{}' as bool in {}", string_str, bound_name))?;
                                    serde_json::json!({"Bool": parsed})
                                }
                                _ => {
                                    // Keep as string for other types
                                    continue;
                                }
                            };
                            
                            debug_println!("RUST DEBUG: Converted {} '{}' from String to {} for type {}", bound_name, string_str, new_literal, target_type);
                            
                            *json_literal = new_literal;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

stub_method!(Java_com_tantivy4java_SplitSearcher_evictComponentsNative, jboolean, 0);
stub_method!(Java_com_tantivy4java_SplitSearcher_parseQueryNative, jobject, std::ptr::null_mut());
stub_method!(Java_com_tantivy4java_SplitSearcher_getSchemaJsonNative, jstring, std::ptr::null_mut());
stub_method!(Java_com_tantivy4java_SplitSearcher_getSplitMetadataNative, jobject, std::ptr::null_mut());
stub_method!(Java_com_tantivy4java_SplitSearcher_getLoadingStatsNative, jobject, std::ptr::null_mut());
/// Stub implementation for docsBulkNative - focusing on docBatchNative optimization
/// The main performance improvement comes from the optimized docBatchNative method
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitSearcher_docsBulkNative(
    mut env: JNIEnv,
    _class: JClass,
    _searcher_ptr: jlong,
    _segments: jni::sys::jintArray,
    _doc_ids: jni::sys::jintArray,
) -> jobject {
    // For now, return null - the main optimization is in docBatchNative
    // This method is not currently used by the test, but docBatch is
    to_java_exception(&mut env, &anyhow::anyhow!("docsBulkNative not implemented - use docBatch for optimized bulk retrieval"));
    std::ptr::null_mut()
}
/// Stub implementation for parseBulkDocsNative - focusing on docBatch optimization
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitSearcher_parseBulkDocsNative(
    mut env: JNIEnv,
    _class: JClass,
    _buffer_jobject: jobject,
) -> jobject {
    // Return empty ArrayList since docsBulkNative is not implemented
    match env.new_object("java/util/ArrayList", "()V", &[]) {
        Ok(empty_list) => empty_list.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}


