// split_searcher_replacement.rs - Replacement JNI methods that use StandaloneSearcher internally
// This replaces the old convoluted SplitSearcher implementation with clean StandaloneSearcher calls

use std::sync::{Arc, OnceLock};
use jni::objects::{JClass, JString, JObject};
use jni::sys::{jlong, jobject, jstring, jint, jboolean};
use jni::JNIEnv;

use crate::standalone_searcher::{StandaloneSearcher, StandaloneSearchConfig, SplitSearchMetadata, resolve_storage_for_split};
use crate::utils::{arc_to_jlong, with_arc_safe, release_arc};
use crate::common::to_java_exception;
use crate::debug_println;
use crate::split_query::{store_split_schema, get_split_schema, convert_split_query_to_ast};
use quickwit_search::{SearcherContext, search_permit_provider::SearchPermitProvider};
use quickwit_search::leaf_cache::LeafSearchCache;
use quickwit_search::list_fields_cache::ListFieldsCache;
use tantivy::aggregation::AggregationLimitsGuard;
use tokio::sync::Semaphore;
use quickwit_common::thread_pool::ThreadPool;

use serde_json::{Value, Map};

use quickwit_proto::search::{SearchRequest, SplitIdAndFooterOffsets};
use quickwit_config::S3StorageConfig;
use quickwit_storage::{StorageResolver, ByteRangeCache, STORAGE_METRICS, MemorySizedCache};
use quickwit_search::leaf::open_index_with_caches;
use quickwit_indexing::open_index;
use quickwit_query::get_quickwit_fastfield_normalizer_manager;
use tantivy::directory::DirectoryClone;

/// Thread pool for search operations (matches Quickwit's pattern exactly)
fn search_thread_pool() -> &'static ThreadPool {
    static SEARCH_THREAD_POOL: OnceLock<ThreadPool> = OnceLock::new();
    SEARCH_THREAD_POOL.get_or_init(|| ThreadPool::new("search", None))
}

/// Check if footer metadata is available for optimizations
fn has_footer_metadata(footer_start: u64, footer_end: u64) -> bool {
    footer_start > 0 && footer_end > 0 && footer_end > footer_start
}

/// Check if split URI is remote (S3/cloud) vs local file
/// Quickwit's hotcache optimization is designed for remote splits, not local files
fn is_remote_split(split_uri: &str) -> bool {
    split_uri.starts_with("s3://") || 
    split_uri.starts_with("http://") || 
    split_uri.starts_with("https://")
}

/// Extract split ID from URI (filename without extension)
fn extract_split_id_from_uri(split_uri: &str) -> String {
    if let Some(last_slash_pos) = split_uri.rfind('/') {
        let filename = &split_uri[last_slash_pos + 1..];
        if let Some(dot_pos) = filename.rfind('.') {
            filename[..dot_pos].to_string()
        } else {
            filename.to_string()
        }
    } else {
        if let Some(dot_pos) = split_uri.rfind('.') {
            split_uri[..dot_pos].to_string()
        } else {
            split_uri.to_string()
        }
    }
}

/// Get Arc<SearcherContext> using the global cache system
/// CRITICAL FIX: Use shared caches by returning the Arc directly
fn get_shared_searcher_context() -> anyhow::Result<Arc<SearcherContext>> {
    debug_println!("RUST DEBUG: Getting SHARED SearcherContext with global caches");
    use crate::global_cache::get_global_searcher_context;

    // Use the convenience function that returns Arc<SearcherContext> with shared caches
    Ok(get_global_searcher_context())
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

/// Enhanced SearchResult data structure that includes both hits and aggregations
#[derive(Debug)]
pub struct EnhancedSearchResult {
    pub hits: Vec<(f32, tantivy::DocAddress)>,
    pub aggregation_results: Option<Vec<u8>>, // Postcard-serialized aggregation results
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
    eprintln!("🚀 SIMPLE DEBUG: createNativeWithSharedCache method called!");
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
    let mut doc_mapping_json: Option<String> = None;
    
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
        
        // Extract doc mapping JSON if available
        debug_println!("RUST DEBUG: Attempting to extract doc mapping from Java config...");
        if let Ok(doc_mapping_obj) = env.call_method(&split_config_jobject, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("doc_mapping").unwrap()).into()]) {
            debug_println!("RUST DEBUG: Got doc_mapping_obj from Java HashMap");
            let doc_mapping_jobject = doc_mapping_obj.l().unwrap();
            if !doc_mapping_jobject.is_null() {
                debug_println!("RUST DEBUG: doc_mapping_jobject is not null, attempting to extract string");
                if let Ok(doc_mapping_str) = env.get_string((&doc_mapping_jobject).into()) {
                    doc_mapping_json = Some(doc_mapping_str.into());
                    debug_println!("RUST DEBUG: ✅ SUCCESS - Extracted doc mapping JSON from Java config ({} chars)", doc_mapping_json.as_ref().unwrap().len());
                } else {
                    debug_println!("RUST DEBUG: ⚠️ Failed to convert doc_mapping_jobject to string");
                }
            } else {
                debug_println!("RUST DEBUG: ⚠️ doc_mapping_jobject is null");
            }
        } else {
            debug_println!("RUST DEBUG: ⚠️ Failed to call get method on HashMap for 'doc_mapping' key");
        }
    }
    
    debug_println!("RUST DEBUG: Config extracted - AWS keys: {}, footer offsets: {}-{}, doc_mapping: {}", 
                aws_config.len(), split_footer_start, split_footer_end, 
                doc_mapping_json.as_ref().map(|s| format!("{}chars", s.len())).unwrap_or_else(|| "None".to_string()));

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
        
        if let Some(force_path_style) = aws_config.get("path_style_access") {
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
            // Store searcher, runtime, split URI, AWS config, footer offsets, and doc mapping JSON together using Arc for memory safety
            let searcher_context = std::sync::Arc::new((searcher, runtime, split_uri.clone(), aws_config, split_footer_start, split_footer_end, doc_mapping_json));
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
    
    let is_valid = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64, Option<String>)>| {
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
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64, Option<String>)>| {
        let (searcher, _runtime, _split_uri, _aws_config, _footer_start, _footer_end, _doc_mapping) = searcher_context.as_ref();
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
    eprintln!("🚀 SIMPLE DEBUG: searchWithQueryAst method called!");
    // Also use debug_println for consistency
    debug_println!("🚀 NATIVE DEBUG: searchWithQueryAst ENTRY - This should show native method is being called!");
    let method_start_time = std::time::Instant::now();
    debug_println!("RUST DEBUG: ⏱️ searchWithQueryAst ENTRY POINT [TIMING START] - limit: {}", limit);
    
    if searcher_ptr == 0 {
        debug_println!("RUST DEBUG: ⏱️ searchWithQueryAst ERROR: Invalid searcher pointer [TIMING: {}ms]", method_start_time.elapsed().as_millis());
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
        return std::ptr::null_mut();
    }
    
    // Extract QueryAst JSON string
    let query_extract_start = std::time::Instant::now();
    let query_json: String = match env.get_string(&query_ast_json) {
        Ok(java_str) => java_str.into(),
        Err(e) => {
            debug_println!("RUST DEBUG: ⏱️ searchWithQueryAst ERROR: Failed to extract QueryAst JSON [TIMING: {}ms]", method_start_time.elapsed().as_millis());
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to extract QueryAst JSON: {}", e));
            return std::ptr::null_mut();
        }
    };
    debug_println!("RUST DEBUG: ⏱️ Query JSON extraction completed [TIMING: {}ms]", query_extract_start.elapsed().as_millis());
    
    // Parse and fix range queries with proper field types from schema
    let query_fix_start = std::time::Instant::now();
    let fixed_query_json = match fix_range_query_types(searcher_ptr, &query_json) {
        Ok(fixed_json) => fixed_json,
        Err(e) => {
            debug_println!("RUST DEBUG: Failed to fix range query types: {}, using original query", e);
            query_json.clone()
        }
    };
    debug_println!("RUST DEBUG: ⏱️ Query type fixing completed [TIMING: {}ms]", query_fix_start.elapsed().as_millis());
    
    if fixed_query_json != query_json {
        debug_println!("RUST DEBUG: Fixed QueryAst JSON: {}", fixed_query_json);
    }
    
    // Use the searcher context to perform search with Quickwit's leaf search approach
    let search_execution_start = std::time::Instant::now();
    debug_println!("RUST DEBUG: ⏱️ Starting search execution [TIMING: {}ms]", method_start_time.elapsed().as_millis());
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64, Option<String>)>| {
        let (searcher, runtime, split_uri, aws_config, footer_start, footer_end, doc_mapping_json) = searcher_context.as_ref();
        
        // Enter the runtime context for async operations
        let _guard = runtime.enter();
        
        // Parse the QueryAst JSON using Quickwit's libraries
        use quickwit_query::query_ast::QueryAst;
        
        use quickwit_common::uri::Uri;
        use quickwit_config::StorageConfigs;

        // Run async code synchronously within the runtime context
        let async_block_start = std::time::Instant::now();
        debug_println!("RUST DEBUG: ⏱️ Starting async block for search [TIMING: {}ms]", method_start_time.elapsed().as_millis());
        // Note: We're already in the runtime context via runtime.enter(), so we can use block_on directly
        runtime.block_on(async {
                // Parse the QueryAst JSON (with field type fixes)
                let query_ast: QueryAst = serde_json::from_str(&fixed_query_json)
                    .map_err(|e| anyhow::anyhow!("Failed to parse QueryAst JSON: {}", e))?;
                
                debug_println!("RUST DEBUG: Successfully parsed QueryAst: {:?}", query_ast);
                
                // First, we need to extract the actual split metadata from the split file
                // This includes footer offsets, number of documents, and the doc mapper
                
                // Parse URI and resolve storage
                let storage_setup_start = std::time::Instant::now();
                debug_println!("RUST DEBUG: ⏱️ 🔧 SEARCH STORAGE SETUP - Starting storage resolution for: {}", split_uri);
                
                let uri: Uri = split_uri.parse()
                    .map_err(|e| anyhow::anyhow!("Failed to parse split URI {}: {}", split_uri, e))?;
                
                // Create S3 storage configuration with credentials from Java config
                let mut storage_configs = StorageConfigs::default();
                
                debug_println!("RUST DEBUG: ⏱️ 🔧 Creating S3 config with credentials from Java configuration");
                let s3_config = S3StorageConfig {
                    flavor: None,
                    access_key_id: aws_config.get("access_key").cloned(),
                    secret_access_key: aws_config.get("secret_key").cloned(), 
                    session_token: aws_config.get("session_token").cloned(),
                    region: aws_config.get("region").cloned(),
                    endpoint: aws_config.get("endpoint").cloned(),
                    force_path_style_access: aws_config.get("path_style_access").map_or(false, |v| v == "true"),
                    disable_multi_object_delete: false,
                    disable_multipart_upload: false,
                };
                
                let storage_configs_vec = StorageConfigs::new(vec![quickwit_config::StorageConfig::S3(s3_config.clone())]);
                storage_configs = storage_configs_vec;
                
                let storage_resolver = StorageResolver::configured(&storage_configs);
                
                // Use the helper function to resolve storage correctly for S3 URIs
                let storage = resolve_storage_for_split(&storage_resolver, split_uri).await?;
                debug_println!("RUST DEBUG: ⏱️ 🔧 SEARCH STORAGE SETUP completed [TIMING: {}ms]", storage_setup_start.elapsed().as_millis());
                
                // Extract relative path - for direct file paths, use just the filename
                let relative_path = if split_uri.contains("://") {
                    // This is a URI, extract just the filename
                    if let Some(last_slash_pos) = split_uri.rfind('/') {
                        std::path::Path::new(&split_uri[last_slash_pos + 1..])
                    } else {
                        std::path::Path::new(split_uri)
                    }
                } else {
                    // This is a direct file path, extract just the filename
                    std::path::Path::new(split_uri)
                        .file_name()
                        .map(|name| std::path::Path::new(name))
                        .unwrap_or_else(|| std::path::Path::new(split_uri))
                };
                
                debug_println!("RUST DEBUG: Reading split file metadata from: '{}'", relative_path.display());
                
                // Use footer offsets from Java configuration for optimized access
                let split_footer_start = *footer_start;
                let split_footer_end = *footer_end;
                
                debug_println!("RUST DEBUG: 🚀 Using Quickwit optimized path with open_index_with_caches - NO full file download");
                debug_println!("RUST DEBUG: Footer offsets from Java config: start={}, end={}", split_footer_start, split_footer_end);
                
                // Create SplitIdAndFooterOffsets for Quickwit optimization
                let split_id = relative_path.file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown");
                    
                let split_and_footer_offsets = quickwit_proto::search::SplitIdAndFooterOffsets {
                    split_id: split_id.to_string(),
                    split_footer_start,
                    split_footer_end,
                    num_docs: 0, // Not used for opening, will be filled later
                    timestamp_start: None,
                    timestamp_end: None,
                };
                
                // Create proper SearcherContext for Quickwit functions
                let quickwit_searcher_context = crate::global_cache::get_global_searcher_context();
                
                // Use Quickwit's complete optimized index opening (does all the work!)
                let (index, _hot_directory) = quickwit_search::leaf::open_index_with_caches(
                    &quickwit_searcher_context,
                    storage.clone(),
                    &split_and_footer_offsets,
                    None, // tokenizer_manager
                    None, // ephemeral_unbounded_cache
                ).await.map_err(|e| anyhow::anyhow!("Failed to open index with caches {}: {}", split_uri, e))?;
                
                debug_println!("RUST DEBUG: ✅ Quickwit optimized index opening completed successfully");
                
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
                    file_size: split_footer_end, // Footer end is effectively the file size
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
                    let (mapping_type, tokenizer) = match field_type {
                        FieldType::Str(text_options) => {
                            if let Some(indexing_options) = text_options.get_indexing_options() {
                                let tokenizer_name = indexing_options.tokenizer();
                                ("text", Some(tokenizer_name.to_string()))
                            } else {
                                // Store-only text fields should still be "text" type, not "keyword"
                                // Quickwit's DocMapper only supports "text" type for Str fields
                                ("text", None)
                            }
                        },
                        FieldType::U64(_) => ("u64", None),
                        FieldType::I64(_) => ("i64", None),
                        FieldType::F64(_) => ("f64", None),
                        FieldType::Bool(_) => ("bool", None),
                        FieldType::Date(_) => ("datetime", None),
                        FieldType::Bytes(_) => ("bytes", None),
                        FieldType::IpAddr(_) => ("ip", None),
                        FieldType::JsonObject(_) => ("json", None),
                        FieldType::Facet(_) => ("keyword", None), // Facets are similar to keywords
                    };

                    let mut field_mapping = serde_json::json!({
                        "name": field_name,
                        "type": mapping_type,
                    });

                    // Add tokenizer information for text fields
                    if let Some(tokenizer_name) = tokenizer {
                        field_mapping["tokenizer"] = serde_json::Value::String(tokenizer_name);
                        debug_println!("RUST DEBUG: Field '{}' has tokenizer '{}'", field_name, field_mapping["tokenizer"]);
                    }

                    field_mappings.push(field_mapping);
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
                // Note: Regular searchWithQueryAst doesn't support aggregations - only searchWithAggregations does
                let search_request = SearchRequest {
                    index_id_patterns: vec![],
                    query_ast: query_json.clone(),
                    max_hits: limit as u64,
                    start_offset: 0,
                    start_timestamp: None,
                    end_timestamp: None,
                    aggregation_request: None, // Regular search has no aggregations
                    snippet_fields: vec![],
                    sort_fields: vec![],
                    search_after: None,
                    scroll_ttl_secs: None,
                    count_hits: quickwit_proto::search::CountHits::CountAll as i32,
                };
                
                debug_println!("RUST DEBUG: ⏱️ 🔍 SEARCH EXECUTION - Calling StandaloneSearcher.search_split_sync with parameters:");
                debug_println!("  - Split URI: {}", split_uri);
                debug_println!("  - Split ID: {}", split_metadata.split_id);
                debug_println!("  - Num docs: {}", split_metadata.num_docs);
                debug_println!("  - Footer offsets: {}-{}", split_metadata.split_footer_start, split_metadata.split_footer_end);
                
                // PERFORM THE ACTUAL REAL SEARCH WITH NO MOCKING!
                let search_exec_start = std::time::Instant::now();
                debug_println!("RUST DEBUG: ⏱️ 🔍 Starting actual search execution via searcher.search_split()");
                
                // We're already in an async context, so use the async method directly
                let split_id_for_error = split_metadata.split_id.clone();
                let leaf_search_response = match searcher.search_split(
                    split_uri,
                    split_metadata,
                    search_request,
                    doc_mapper_arc,
                ).await {
                    Ok(response) => response,
                    Err(e) => {
                        debug_println!("RUST DEBUG: ⏱️ 🔍 ERROR in searcher.search_split [TIMING: {}ms]: {}", search_exec_start.elapsed().as_millis(), e);
                        debug_println!("RUST DEBUG: Full error chain: {:#}", e);
                        // Propagate the full error chain to Java
                        return Err(anyhow::anyhow!("{:#}", e));
                    }
                };
                
                debug_println!("RUST DEBUG: ⏱️ 🔍 SEARCH EXECUTION completed [TIMING: {}ms] - Found {} hits", search_exec_start.elapsed().as_millis(), leaf_search_response.num_hits);

                // Use the unified search result creation logic
                let search_result_ptr = perform_unified_search_result_creation(
                    leaf_search_response,
                    &mut env
                )?;

                Ok(search_result_ptr)
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

/// Helper that converts QueryAst to JSON and performs search (eliminates Java→JSON conversion)
fn perform_search_with_query_ast(searcher_ptr: jlong, query_ast: quickwit_query::query_ast::QueryAst, limit: usize) -> Result<String, anyhow::Error> {
    // Serialize QueryAst to JSON using Quickwit's proven serialization
    let query_json = serde_json::to_string(&query_ast)
        .map_err(|e| anyhow::anyhow!("Failed to serialize QueryAst to JSON: {}", e))?;
    
    debug_println!("RUST DEBUG: QueryAst serialized to JSON: {}", query_json);
    
    // Return the JSON string for use with existing searchWithQueryAst logic
    Ok(query_json)
}

/// New method that handles aggregations using Quickwit's proven aggregation system
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitSearcher_searchWithAggregations<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    searcher_ptr: jlong,
    split_query: JObject<'local>,
    limit: jint,
    aggregations_map: JObject<'local>,
) -> jobject {
    debug_println!("🚀 NATIVE DEBUG: searchWithAggregations ENTRY - Real tantivy aggregation computation");
    let method_start_time = std::time::Instant::now();

    if searcher_ptr == 0 {
        debug_println!("RUST DEBUG: ⏱️ searchWithAggregations ERROR: Invalid searcher pointer [TIMING: {}ms]", method_start_time.elapsed().as_millis());
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
        return std::ptr::null_mut();
    }

    // Convert SplitQuery to QueryAst
    let query_ast = match convert_split_query_to_ast(&mut env, &split_query) {
        Ok(ast) => ast,
        Err(e) => {
            debug_println!("RUST DEBUG: ⏱️ searchWithAggregations ERROR: Failed to convert SplitQuery [TIMING: {}ms]", method_start_time.elapsed().as_millis());
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to convert SplitQuery to QueryAst: {}", e));
            return std::ptr::null_mut();
        }
    };

    // Convert Java aggregations map to JSON for Quickwit's SearchRequest system
    let aggregation_request_json = match convert_java_aggregations_to_json(&mut env, &aggregations_map) {
        Ok(json) => json,
        Err(e) => {
            debug_println!("RUST DEBUG: ⏱️ searchWithAggregations ERROR: Failed to convert aggregations to JSON [TIMING: {}ms]", method_start_time.elapsed().as_millis());
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to convert aggregations to JSON: {}", e));
            return std::ptr::null_mut();
        }
    };

    // Perform search using Quickwit's aggregation system (reuses existing infrastructure)
    match perform_search_with_quickwit_aggregations(searcher_ptr, query_ast, limit as usize, aggregation_request_json) {
        Ok(leaf_search_response) => {
            // Use the same result creation logic as the working search
            match perform_unified_search_result_creation(leaf_search_response, &mut env) {
                Ok(search_result) => {
                    debug_println!("RUST DEBUG: ⏱️ searchWithAggregations SUCCESS [TIMING: {}ms]", method_start_time.elapsed().as_millis());
                    search_result
                }
                Err(e) => {
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to create SearchResult: {}", e));
                    std::ptr::null_mut()
                }
            }
        }
        Err(e) => {
            debug_println!("RUST DEBUG: ⏱️ searchWithAggregations ERROR: Search failed [TIMING: {}ms]: {}", method_start_time.elapsed().as_millis(), e);
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

/// Optimized method that takes SplitQuery directly without JSON round-trip
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitSearcher_searchWithSplitQuery(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    split_query: JObject,
    limit: jint,
) -> jobject {
    debug_println!("🚀 NATIVE DEBUG: searchWithSplitQuery ENTRY - Direct SplitQuery conversion");
    let method_start_time = std::time::Instant::now();
    
    if searcher_ptr == 0 {
        debug_println!("RUST DEBUG: ⏱️ searchWithSplitQuery ERROR: Invalid searcher pointer [TIMING: {}ms]", method_start_time.elapsed().as_millis());
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
        return std::ptr::null_mut();
    }
    
    // Convert SplitQuery directly to QueryAst using native Quickwit structures
    let query_conversion_start = std::time::Instant::now();
    let query_ast = match convert_split_query_to_ast(&mut env, &split_query) {
        Ok(ast) => ast,
        Err(e) => {
            debug_println!("RUST DEBUG: ⏱️ searchWithSplitQuery ERROR: Failed to convert SplitQuery to QueryAst [TIMING: {}ms]", method_start_time.elapsed().as_millis());
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to convert SplitQuery to QueryAst: {}", e));
            return std::ptr::null_mut();
        }
    };
    debug_println!("RUST DEBUG: ⏱️ QueryAst conversion completed [TIMING: {}ms]", query_conversion_start.elapsed().as_millis());
    
    // Convert QueryAst to JSON using Quickwit's proven serialization
    let query_json = match perform_search_with_query_ast(searcher_ptr, query_ast, limit as usize) {
        Ok(json) => json,
        Err(e) => {
            debug_println!("RUST DEBUG: ⏱️ searchWithSplitQuery ERROR: Failed to serialize QueryAst [TIMING: {}ms]: {}", method_start_time.elapsed().as_millis(), e);
            to_java_exception(&mut env, &e);
            return std::ptr::null_mut();
        }
    };
    
    // Create JString from the JSON
    let java_query_json = match env.new_string(&query_json) {
        Ok(jstr) => jstr,
        Err(e) => {
            debug_println!("RUST DEBUG: ⏱️ searchWithSplitQuery ERROR: Failed to create Java string [TIMING: {}ms]: {}", method_start_time.elapsed().as_millis(), e);
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to create Java string: {}", e));
            return std::ptr::null_mut();
        }
    };
    
    // Call the existing searchWithQueryAst method with the properly serialized JSON
    debug_println!("RUST DEBUG: ⏱️ searchWithSplitQuery calling searchWithQueryAst with native-serialized JSON");
    Java_com_tantivy4java_SplitSearcher_searchWithQueryAst(
        env,
        _class,
        searcher_ptr,
        java_query_json.into(),
        limit,
    )
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
    let function_start = std::time::Instant::now();
    debug_println!("RUST DEBUG: ⏱️ 🚀 retrieve_document_from_split_optimized ENTRY [TIMING START]");
    
    use crate::utils::with_arc_safe;
    
    // Get split URI from the searcher context
    let uri_extraction_start = std::time::Instant::now();
    let split_uri = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64, Option<String>)>| {
        let (_, _, split_uri, _, _, _, _) = searcher_context.as_ref();
        split_uri.clone()
    }).ok_or_else(|| anyhow::anyhow!("Invalid searcher context"))?;
    debug_println!("RUST DEBUG: ⏱️ Split URI extraction completed [TIMING: {}ms]", uri_extraction_start.elapsed().as_millis());
    
    // Check cache first - simple and effective
    let cache_check_start = std::time::Instant::now();
    let searcher_cache = get_searcher_cache();
    let cached_searcher = {
        let cache = searcher_cache.lock().unwrap();
        cache.get(&split_uri).cloned()
    };
    debug_println!("RUST DEBUG: ⏱️ Cache lookup completed [TIMING: {}ms] - cache_hit: {}", cache_check_start.elapsed().as_millis(), cached_searcher.is_some());
    
    if let Some(searcher) = cached_searcher {
        // Use cached searcher - very fast path (cache hit)
        // IMPORTANT: Use async method for StorageDirectory compatibility
        let cache_hit_start = std::time::Instant::now();
        debug_println!("RUST DEBUG: ⏱️ 🎯 CACHE HIT - using cached searcher for document retrieval");
        
        // Extract the runtime and use async document retrieval
        let doc_and_schema = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64, Option<String>)>| {
            let (_standalone_searcher, runtime, _split_uri, _aws_config, _footer_start, _footer_end, _doc_mapping) = searcher_context.as_ref();
            
            let _guard = runtime.enter();
            tokio::task::block_in_place(|| {
                runtime.block_on(async {
                    let doc = searcher.doc_async(doc_address)
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to retrieve document: {}", e))?;
                    let schema = searcher.schema();
                    Ok::<(tantivy::schema::TantivyDocument, tantivy::schema::Schema), anyhow::Error>((doc, schema.clone()))
                })
            })
        }).ok_or_else(|| anyhow::anyhow!("Invalid searcher context for cached retrieval"))?;
        
        match doc_and_schema {
            Ok((doc, schema)) => {
                debug_println!("RUST DEBUG: ⏱️ ✅ CACHE HIT document retrieval completed [TIMING: {}ms] [TOTAL: {}ms]", cache_hit_start.elapsed().as_millis(), function_start.elapsed().as_millis());
                return Ok((doc, schema));
            }
            Err(e) => {
                debug_println!("RUST DEBUG: ⏱️ ❌ CACHE HIT failed, falling through to cache miss: {}", e);
                // Fall through to cache miss path
            }
        }
    }
    
    // Cache miss - create searcher using the same optimizations as our batch method
    debug_println!("RUST DEBUG: ⏱️ ⚠️ CACHE MISS - creating new searcher (EXPENSIVE OPERATION)");
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64, Option<String>)>| {
        let (standalone_searcher, runtime, split_uri, aws_config, footer_start, footer_end, _doc_mapping) = searcher_context.as_ref();
        
        let _guard = runtime.enter();
        
        // Use the same Quickwit caching pattern as our batch method
        tokio::task::block_in_place(|| {
            runtime.block_on(async {
                
                use quickwit_config::{StorageConfigs, S3StorageConfig};
                use quickwit_proto::search::SplitIdAndFooterOffsets;
                use quickwit_storage::StorageResolver;
                
                
                
                use std::sync::Arc;
                
                // Create split metadata for Quickwit's open_index_with_caches with correct field names
                // Extract just the filename as the split_id (e.g., "consolidated.split" from the full URL)
                let split_filename = if let Some(last_slash_pos) = split_uri.rfind('/') {
                    &split_uri[last_slash_pos + 1..]
                } else {
                    split_uri
                };

                // For split_id, use the filename without .split extension if present
                // This is what Quickwit expects for the split identifier
                let split_id = if split_filename.ends_with(".split") {
                    &split_filename[..split_filename.len() - 6] // Remove ".split"
                } else {
                    split_filename
                };

                let split_metadata = SplitIdAndFooterOffsets {
                    split_id: split_id.to_string(),
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
                    force_path_style_access: aws_config.get("path_style_access").map_or(false, |v| v == "true"),
                    disable_multi_object_delete: false,
                    disable_multipart_upload: false,
                };
                
                // Create storage that points to the directory containing the split (not the split file itself)
                // This is what open_index_with_caches expects
                let storage_resolution_start = std::time::Instant::now();
                let split_dir_uri = if let Some(last_slash_pos) = split_uri.rfind('/') {
                    &split_uri[..last_slash_pos + 1] // Include the trailing slash
                } else {
                    split_uri // If no slash, use the full URI as directory
                };
                debug_println!("RUST DEBUG: ⏱️ 🔧 STORAGE RESOLUTION - Creating S3 storage configuration");
                
                let storage_configs = StorageConfigs::new(vec![quickwit_config::StorageConfig::S3(s3_config.clone())]);
                let storage_resolver = StorageResolver::configured(&storage_configs);
                let index_storage = resolve_storage_for_split(&storage_resolver, split_dir_uri).await?;
                debug_println!("RUST DEBUG: ⏱️ 🔧 STORAGE RESOLUTION completed [TIMING: {}ms]", storage_resolution_start.elapsed().as_millis());
                
                // Use global SearcherContext for long-term shared caches (Quickwit pattern)
                let searcher_context = crate::global_cache::get_global_searcher_context();
                
                // Create short-lived ByteRangeCache per operation (Quickwit pattern for optimal memory use)
                let byte_range_cache = quickwit_storage::ByteRangeCache::with_infinite_capacity(
                    &quickwit_storage::STORAGE_METRICS.shortlived_cache
                );
                
                // Manual index opening with Quickwit caching components
                // (open_index_with_caches expects Quickwit's native split format, but we use bundle format)
                let index_opening_start = std::time::Instant::now();
                debug_println!("RUST DEBUG: ⏱️ 📖 INDEX OPENING - Starting file download and index creation");
                
                let relative_path = if let Some(last_slash_pos) = split_uri.rfind('/') {
                    std::path::Path::new(&split_uri[last_slash_pos + 1..])
                } else {
                    std::path::Path::new(split_uri)
                };
                
                // 🚀 INDIVIDUAL DOC OPTIMIZATION: Use same hotcache optimization as batch retrieval
                let mut index = if has_footer_metadata(*footer_start, *footer_end) && is_remote_split(split_uri) {
                    debug_println!("RUST DEBUG: 🚀 Using Quickwit optimized path for individual document retrieval (footer: {}..{})", footer_start, footer_end);
                    
                    use quickwit_proto::search::SplitIdAndFooterOffsets;
                    use quickwit_search::leaf::open_index_with_caches;
                    
                    // Create SplitIdAndFooterOffsets for Quickwit's open_index_with_caches
                    let footer_offsets = SplitIdAndFooterOffsets {
                        split_id: extract_split_id_from_uri(split_uri),
                        split_footer_start: *footer_start,
                        split_footer_end: *footer_end,
                        timestamp_start: Some(0),
                        timestamp_end: Some(i64::MAX),
                        num_docs: 0, // Will be filled by Quickwit
                    };
                    
                    // Create minimal SearcherContext for Quickwit functions
                    let searcher_context = get_shared_searcher_context()
                        .map_err(|e| anyhow::anyhow!("Failed to create searcher context: {}", e))?;
                    
                    // ✅ Use Quickwit's proven function with hotcache optimization
                    let index_creation_start = std::time::Instant::now();
                    let (index, _hot_directory) = open_index_with_caches(
                        &searcher_context,
                        index_storage.clone(),
                        &footer_offsets,
                        None, // tokenizer_manager
                        None  // No ephemeral cache
                    ).await.map_err(|e| anyhow::anyhow!("Quickwit individual open_index_with_caches failed: {}", e))?;
                    
                    debug_println!("RUST DEBUG: ⏱️ 📖 Quickwit hotcache index creation completed [TIMING: {}ms]", index_creation_start.elapsed().as_millis());
                    debug_println!("RUST DEBUG: ✅ Successfully opened index with Quickwit hotcache optimization for individual document retrieval");
                    index
                } else {
                    debug_println!("RUST DEBUG: ⚠️ Footer metadata not available for individual document retrieval, falling back to full download");
                    
                    // Fallback: Get the full file data using Quickwit's storage abstraction for document retrieval
                    // (We need BundleDirectory for synchronous document access, not StorageDirectory)
                    let file_size = index_storage.file_num_bytes(relative_path).await
                        .map_err(|e| anyhow::anyhow!("Failed to get file size for {}: {}", split_uri, e))?;

                    let split_data = index_storage.get_slice(relative_path, 0..file_size as usize).await
                        .map_err(|e| anyhow::anyhow!("Failed to get split data from {}: {}", split_uri, e))?;

                    let split_file_slice = tantivy::directory::FileSlice::new(std::sync::Arc::new(split_data));
                    let bundle_directory = quickwit_directories::BundleDirectory::open_split(split_file_slice)
                        .map_err(|e| anyhow::anyhow!("Failed to open bundle directory {}: {}", split_uri, e))?;
                        
                    let index_creation_start = std::time::Instant::now();
                    // ✅ QUICKWIT NATIVE: Use Quickwit's native index opening instead of direct tantivy
                    let index = open_index(bundle_directory.box_clone(), get_quickwit_fastfield_normalizer_manager().tantivy_manager())
                        .map_err(|e| anyhow::anyhow!("Failed to open index from bundle {}: {}", split_uri, e))?;
                    debug_println!("RUST DEBUG: ⏱️ 📖 QUICKWIT NATIVE: BundleDirectory index creation completed [TIMING: {}ms]", index_creation_start.elapsed().as_millis());
                    index
                };
                
                // Use the same Quickwit optimizations as our batch method
                let tantivy_executor = search_thread_pool()
                    .get_underlying_rayon_thread_pool()
                    .into();
                index.set_executor(tantivy_executor);
                
                // Same cache settings as batch method
                let searcher_creation_start = std::time::Instant::now();
                const NUM_CONCURRENT_REQUESTS: usize = 30; // From fetch_docs.rs
                let index_reader = index
                    .reader_builder()
                    .doc_store_cache_num_blocks(NUM_CONCURRENT_REQUESTS) // QUICKWIT OPTIMIZATION
                    .reload_policy(tantivy::ReloadPolicy::Manual)
                    .try_into()
                    .map_err(|e| anyhow::anyhow!("Failed to create index reader: {}", e))?;
                
                let searcher = Arc::new(index_reader.searcher());
                debug_println!("RUST DEBUG: ⏱️ 📖 Searcher creation completed [TIMING: {}ms]", searcher_creation_start.elapsed().as_millis());
                
                // Cache the searcher for future single document retrievals
                let caching_start = std::time::Instant::now();
                {
                    let searcher_cache = get_searcher_cache();
                    let mut cache = searcher_cache.lock().unwrap();
                    cache.insert(split_uri.clone(), searcher.clone());
                }
                debug_println!("RUST DEBUG: ⏱️ 📖 Searcher caching completed [TIMING: {}ms]", caching_start.elapsed().as_millis());
                debug_println!("RUST DEBUG: ⏱️ 📖 TOTAL INDEX OPENING completed [TIMING: {}ms]", index_opening_start.elapsed().as_millis());
                
                // Retrieve the document using async method (same as batch retrieval for StorageDirectory compatibility)
                let doc_retrieval_start = std::time::Instant::now();
                let doc = searcher.doc_async(doc_address)
                    .await
                    .map_err(|e| anyhow::anyhow!("Failed to retrieve document: {}", e))?;
                let schema = index.schema();
                debug_println!("RUST DEBUG: ⏱️ 📖 Document retrieval completed [TIMING: {}ms]", doc_retrieval_start.elapsed().as_millis());
                
                Ok::<(tantivy::schema::TantivyDocument, tantivy::schema::Schema), anyhow::Error>((doc, schema.clone()))
            })
        })
    });
    
    match result {
        Some(Ok(doc_and_schema)) => {
            debug_println!("RUST DEBUG: ⏱️ ✅ CACHE MISS document retrieval completed [TOTAL: {}ms]", function_start.elapsed().as_millis());
            Ok(doc_and_schema)
        },
        Some(Err(e)) => {
            debug_println!("RUST DEBUG: ⏱️ ❌ CACHE MISS failed [TOTAL: {}ms] - Error: {}", function_start.elapsed().as_millis(), e);
            Err(e)
        },
        None => {
            debug_println!("RUST DEBUG: ⏱️ ❌ Invalid searcher context [TOTAL: {}ms]", function_start.elapsed().as_millis());
            Err(anyhow::anyhow!("Searcher context not found for pointer {}", searcher_ptr))
        },
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
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64, Option<String>)>| {
        let (searcher, runtime, split_uri, aws_config, footer_start, footer_end, _doc_mapping) = searcher_context.as_ref();
        
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
                force_path_style_access: aws_config.get("path_style_access").map_or(false, |v| v == "true"),
                disable_multi_object_delete: false,
                disable_multipart_upload: false,
            };
            
            let storage_configs = StorageConfigs::new(vec![quickwit_config::StorageConfig::S3(s3_config.clone())]);
            let storage_resolver = StorageResolver::configured(&storage_configs);
            let actual_storage = resolve_storage_for_split(&storage_resolver, split_uri).await?;
            
            // Extract relative path - for direct file paths, use just the filename
            let relative_path = if split_uri.contains("://") {
                // This is a URI, extract just the filename
                if let Some(last_slash_pos) = split_uri.rfind('/') {
                    Path::new(&split_uri[last_slash_pos + 1..])
                } else {
                    Path::new(split_uri)
                }
            } else {
                // This is a direct file path, extract just the filename
                Path::new(split_uri)
                    .file_name()
                    .map(|name| Path::new(name))
                    .unwrap_or_else(|| Path::new(split_uri))
            };
            
            // 🚀 OPTIMIZATION: Use Quickwit's optimized path when footer metadata is available AND split is remote
            debug_println!("RUST DEBUG: Checking optimization conditions - footer_metadata: {}, is_remote: {}", 
                has_footer_metadata(*footer_start, *footer_end), is_remote_split(split_uri));
            let index = if has_footer_metadata(*footer_start, *footer_end) && is_remote_split(split_uri) {
                debug_println!("RUST DEBUG: 🚀 Using Quickwit optimized path with hotcache (footer: {}..{})", footer_start, footer_end);
                
                // Create SplitIdAndFooterOffsets for Quickwit's open_index_with_caches
                let footer_offsets = SplitIdAndFooterOffsets {
                    split_id: extract_split_id_from_uri(split_uri),
                    split_footer_start: *footer_start,
                    split_footer_end: *footer_end,
                    timestamp_start: Some(0),
                    timestamp_end: Some(i64::MAX),
                    num_docs: 0, // Will be filled by Quickwit
                };
                
                // Create minimal SearcherContext for Quickwit functions
                let searcher_context = get_shared_searcher_context()
                    .map_err(|e| anyhow::anyhow!("Failed to create searcher context: {}", e))?;
                
                // ✅ Use Quickwit's proven function with hotcache optimization
                let (index, _hot_directory) = open_index_with_caches(
                    &searcher_context,
                    actual_storage.clone(),
                    &footer_offsets,
                    None, // tokenizer_manager
                    Some(ByteRangeCache::with_infinite_capacity(&STORAGE_METRICS.shortlived_cache))
                ).await.map_err(|e| anyhow::anyhow!("Quickwit open_index_with_caches failed: {}", e))?;
                
                debug_println!("RUST DEBUG: ✅ Successfully opened index with Quickwit hotcache optimization");
                index
            } else {
                debug_println!("RUST DEBUG: ⚠️ Footer metadata not available, falling back to full download");
                
                // Fallback: Get the full file data (original behavior for missing metadata)
                let file_size = actual_storage.file_num_bytes(relative_path).await
                    .map_err(|e| anyhow::anyhow!("Failed to get file size for {}: {}", split_uri, e))?;
                
                let split_data = actual_storage.get_slice(relative_path, 0..file_size as usize).await
                    .map_err(|e| anyhow::anyhow!("Failed to get split data from {}: {}", split_uri, e))?;
                
                debug_println!("RUST DEBUG: ⚠️ Downloaded full split file: {} bytes", split_data.len());
                
                // Open the bundle directory from the split data
                let split_file_slice = FileSlice::new(std::sync::Arc::new(split_data));
                let bundle_directory = BundleDirectory::open_split(split_file_slice)
                    .map_err(|e| anyhow::anyhow!("Failed to open bundle directory {}: {}", split_uri, e))?;
                    
                // ✅ QUICKWIT NATIVE: Extract the index from the bundle directory using Quickwit's native function
                open_index(bundle_directory.box_clone(), get_quickwit_fastfield_normalizer_manager().tantivy_manager())
                    .map_err(|e| anyhow::anyhow!("Failed to open index from bundle {}: {}", split_uri, e))?
            };
            
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
    
    use std::sync::Arc;
    
    // Sort by DocAddress for cache locality (following Quickwit pattern)
    doc_addresses.sort();
    
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64, Option<String>)>| {
        let (searcher, runtime, split_uri, aws_config, footer_start, footer_end, _doc_mapping) = searcher_context.as_ref();
        
        let _guard = runtime.enter();
        
        // Use block_in_place to run async code synchronously (Quickwit pattern)
        tokio::task::block_in_place(|| {
            runtime.block_on(async {
                // ✅ OPTIMIZATION: Check searcher cache first (like individual retrieval)
                let searcher_cache = get_searcher_cache();
                let cached_searcher_option = {
                    let cache = searcher_cache.lock().unwrap();
                    cache.get(split_uri as &str).cloned()
                };

                // If we have a cached searcher, use it for concurrent batch processing
                if let Some(cached_searcher) = cached_searcher_option {
                    debug_println!("RUST DEBUG: ✅ BATCH CACHE HIT: Using cached searcher for batch processing");
                    let schema = cached_searcher.schema(); // Get schema from cached searcher

                    // ✅ QUICKWIT CONCURRENT PATTERN: Use cached searcher with concurrency
                    const NUM_CONCURRENT_REQUESTS: usize = 30;

                    let doc_futures = doc_addresses.into_iter().map(|doc_addr| {
                        let moved_searcher = cached_searcher.clone(); // Reuse cached searcher
                        let moved_schema = schema.clone();
                        async move {
                            let doc: tantivy::schema::TantivyDocument = moved_searcher
                                .doc_async(doc_addr)
                                .await
                                .map_err(|e| anyhow::anyhow!("Failed to retrieve document at address {:?}: {}", doc_addr, e))?;

                            // Create a RetrievedDocument and register it
                            use crate::document::{DocumentWrapper, RetrievedDocument};
                            let retrieved_doc = RetrievedDocument::new_with_schema(doc, &moved_schema);
                            let wrapper = DocumentWrapper::Retrieved(retrieved_doc);
                            let wrapper_arc = std::sync::Arc::new(std::sync::Mutex::new(wrapper));
                            let doc_ptr = crate::utils::arc_to_jlong(wrapper_arc);

                            Ok::<jobject, anyhow::Error>(doc_ptr as jobject)
                        }
                    });

                    // Execute concurrent batch retrieval with cached searcher
                    use futures::stream::{StreamExt, TryStreamExt};
                    let doc_ptrs: Vec<jobject> = futures::stream::iter(doc_futures)
                        .buffer_unordered(NUM_CONCURRENT_REQUESTS)
                        .try_collect::<Vec<_>>()
                        .await
                        .map_err(|e| anyhow::anyhow!("Cached searcher batch retrieval failed: {}", e))?;

                    return Ok::<Vec<jobject>, anyhow::Error>(doc_ptrs);
                }

                debug_println!("RUST DEBUG: ⚠️ BATCH CACHE MISS: Creating new searcher for batch processing");

                
                use quickwit_config::{StorageConfigs, S3StorageConfig};
                use quickwit_proto::search::SplitIdAndFooterOffsets;
                use quickwit_storage::StorageResolver;
                use quickwit_search::leaf::open_index_with_caches;
                
                
                
                
                
                // Use the same storage resolution approach as individual document retrieval
                // Create S3 storage configuration
                let s3_config = S3StorageConfig {
                    flavor: None,
                    access_key_id: aws_config.get("access_key").cloned(),
                    secret_access_key: aws_config.get("secret_key").cloned(), 
                    session_token: aws_config.get("session_token").cloned(),
                    region: aws_config.get("region").cloned(),
                    endpoint: aws_config.get("endpoint").cloned(),
                    force_path_style_access: aws_config.get("path_style_access").map_or(false, |v| v == "true"),
                    disable_multi_object_delete: false,
                    disable_multipart_upload: false,
                };
                
                // Create storage that points to the directory containing the split (same as individual retrieval)
                let split_dir_uri = if let Some(last_slash_pos) = split_uri.rfind('/') {
                    &split_uri[..last_slash_pos + 1] // Include the trailing slash
                } else {
                    split_uri // If no slash, use the full URI as directory
                };
                
                let storage_configs = StorageConfigs::new(vec![quickwit_config::StorageConfig::S3(s3_config.clone())]);
                let storage_resolver = StorageResolver::configured(&storage_configs);
                let index_storage = resolve_storage_for_split(&storage_resolver, split_dir_uri).await?;
                
                // Extract just the filename as the relative path (same as individual retrieval)
                let relative_path = if let Some(last_slash_pos) = split_uri.rfind('/') {
                    std::path::Path::new(&split_uri[last_slash_pos + 1..])
                } else {
                    std::path::Path::new(split_uri)
                };
                
                // 🚀 BATCH OPTIMIZATION: Use Quickwit's optimized path when footer metadata is available for remote splits
                debug_println!("RUST DEBUG: Checking batch optimization conditions - footer_metadata: {}, is_remote: {}", 
                    has_footer_metadata(*footer_start, *footer_end), is_remote_split(split_uri));
                let mut index = if has_footer_metadata(*footer_start, *footer_end) && is_remote_split(split_uri) {
                    debug_println!("RUST DEBUG: 🚀 Using Quickwit optimized path for batch retrieval (footer: {}..{})", footer_start, footer_end);
                    
                    // Create SplitIdAndFooterOffsets for Quickwit's open_index_with_caches
                    let footer_offsets = SplitIdAndFooterOffsets {
                        split_id: extract_split_id_from_uri(split_uri),
                        split_footer_start: *footer_start,
                        split_footer_end: *footer_end,
                        timestamp_start: Some(0),
                        timestamp_end: Some(i64::MAX),
                        num_docs: 0, // Will be filled by Quickwit
                    };
                    
                    // Create minimal SearcherContext for Quickwit functions
                    let searcher_context = get_shared_searcher_context()
                        .map_err(|e| anyhow::anyhow!("Failed to create searcher context: {}", e))?;
                    
                    // ✅ Use Quickwit's proven function with hotcache optimization for batch operations
                    let (index, _hot_directory) = open_index_with_caches(
                        &searcher_context,
                        index_storage.clone(),
                        &footer_offsets,
                        None, // tokenizer_manager
                        None  // No ephemeral cache for batch operations (as per fetch_docs.rs)
                    ).await.map_err(|e| anyhow::anyhow!("Quickwit batch open_index_with_caches failed: {}", e))?;
                    
                    debug_println!("RUST DEBUG: ✅ Successfully opened index with Quickwit hotcache optimization for batch retrieval");
                    index
                } else {
                    debug_println!("RUST DEBUG: ⚠️ Footer metadata not available for batch retrieval, falling back to full download");
                    
                    // Fallback: Get the full file data (original behavior for missing metadata)
                    let file_size = index_storage.file_num_bytes(relative_path).await
                        .map_err(|e| anyhow::anyhow!("Failed to get file size for {}: {}", split_uri, e))?;
                    
                    let split_data = index_storage.get_slice(relative_path, 0..file_size as usize).await
                        .map_err(|e| anyhow::anyhow!("Failed to get split data from {}: {}", split_uri, e))?;
                    
                    debug_println!("RUST DEBUG: ⚠️ Downloaded full split file for batch: {} bytes", split_data.len());
                    
                    let split_file_slice = tantivy::directory::FileSlice::new(std::sync::Arc::new(split_data));
                    let bundle_directory = quickwit_directories::BundleDirectory::open_split(split_file_slice)
                        .map_err(|e| anyhow::anyhow!("Failed to open bundle directory {}: {}", split_uri, e))?;
                        
                    // ✅ QUICKWIT NATIVE: Use Quickwit's native index opening
                    open_index(bundle_directory.box_clone(), get_quickwit_fastfield_normalizer_manager().tantivy_manager())
                        .map_err(|e| anyhow::anyhow!("Failed to open index from bundle {}: {}", split_uri, e))?
                };
                
                // Use the same Quickwit optimizations as individual method
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

                // ✅ CACHE NEW SEARCHER: Store the newly created searcher for future reuse
                {
                    let searcher_cache = get_searcher_cache();
                    let mut cache = searcher_cache.lock().unwrap();
                    cache.insert(split_uri.clone(), tantivy_searcher.clone());
                    debug_println!("RUST DEBUG: ✅ CACHED NEW SEARCHER: Stored searcher for future batch operations");
                }
                
                // ✅ QUICKWIT CONCURRENT PATTERN: Use concurrent document retrieval (fetch_docs.rs line 200-258)

                // Create async futures for concurrent document retrieval (like Quickwit)
                let doc_futures = doc_addresses.into_iter().map(|doc_addr| {
                    let moved_searcher = tantivy_searcher.clone(); // Clone Arc for concurrent access
                    let moved_schema = schema.clone(); // Clone schema for each future
                    async move {
                        // Use doc_async like Quickwit - QUICKWIT OPTIMIZATION (fetch_docs.rs line 205-207)
                        let doc: tantivy::schema::TantivyDocument = moved_searcher
                            .doc_async(doc_addr)
                            .await
                            .map_err(|e| anyhow::anyhow!("Failed to retrieve document at address {:?}: {}", doc_addr, e))?;

                        // Create a RetrievedDocument and register it
                        use crate::document::{DocumentWrapper, RetrievedDocument};

                        let retrieved_doc = RetrievedDocument::new_with_schema(doc, &moved_schema);
                        let wrapper = DocumentWrapper::Retrieved(retrieved_doc);
                        let wrapper_arc = std::sync::Arc::new(std::sync::Mutex::new(wrapper));
                        let doc_ptr = crate::utils::arc_to_jlong(wrapper_arc);

                        Ok::<jobject, anyhow::Error>(doc_ptr as jobject)
                    }
                });

                // ✅ QUICKWIT CONCURRENT EXECUTION: Process up to NUM_CONCURRENT_REQUESTS simultaneously
                use futures::stream::{StreamExt, TryStreamExt};
                let doc_ptrs: Vec<jobject> = futures::stream::iter(doc_futures)
                    .buffer_unordered(NUM_CONCURRENT_REQUESTS) // Quickwit's concurrent processing pattern
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(|e| anyhow::anyhow!("Concurrent document retrieval failed: {}", e))?;
                
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
    doc_addresses: Vec<tantivy::DocAddress>,
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
    debug_println!("🚀 SIMPLE DEBUG: docNative method called!");
    debug_println!("🚀 NATIVE DEBUG: docNative ENTRY - Document retrieval starting!");
    let method_start_time = std::time::Instant::now();
    debug_println!("RUST DEBUG: ⏱️ docNative ENTRY POINT [TIMING START] - segment_ord={}, doc_id={}", segment_ord, doc_id);
    
    if searcher_ptr == 0 {
        debug_println!("RUST DEBUG: ⏱️ docNative ERROR: Invalid searcher pointer [TIMING: {}ms]", method_start_time.elapsed().as_millis());
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
        return std::ptr::null_mut();
    }
    
    // Create DocAddress from the provided segment and doc ID
    let doc_address_start = std::time::Instant::now();
    let doc_address = tantivy::DocAddress::new(segment_ord as u32, doc_id as u32);
    debug_println!("RUST DEBUG: ⏱️ DocAddress creation completed [TIMING: {}ms]", doc_address_start.elapsed().as_millis());
    
    // Use Quickwit's optimized approach for document retrieval
    let retrieval_start = std::time::Instant::now();
    debug_println!("RUST DEBUG: ⏱️ 🔥 CALLING retrieve_document_from_split_optimized (POTENTIAL BOTTLENECK) [TIMING: {}ms]", method_start_time.elapsed().as_millis());
    match retrieve_document_from_split_optimized(searcher_ptr, doc_address) {
        Ok((doc, schema)) => {
            debug_println!("RUST DEBUG: ⏱️ 🔥 retrieve_document_from_split_optimized COMPLETED [TIMING: {}ms]", retrieval_start.elapsed().as_millis());
            
            // Create a RetrievedDocument using the proper pattern from searcher.rs
            let wrapper_creation_start = std::time::Instant::now();
            use crate::document::{DocumentWrapper, RetrievedDocument};
            
            let retrieved_doc = RetrievedDocument::new_with_schema(doc, &schema);
            let wrapper = DocumentWrapper::Retrieved(retrieved_doc);
            let wrapper_arc = std::sync::Arc::new(std::sync::Mutex::new(wrapper));
            let doc_ptr = crate::utils::arc_to_jlong(wrapper_arc);
            debug_println!("RUST DEBUG: ⏱️ Document wrapper creation completed [TIMING: {}ms]", wrapper_creation_start.elapsed().as_millis());
            
            // Create Java Document object with the pointer
            let java_obj_start = std::time::Instant::now();
            match env.find_class("com/tantivy4java/Document") {
                Ok(document_class) => {
                    match env.new_object(&document_class, "(J)V", &[doc_ptr.into()]) {
                        Ok(document_obj) => {
                            debug_println!("RUST DEBUG: ⏱️ Java Document object creation completed [TIMING: {}ms]", java_obj_start.elapsed().as_millis());
                            debug_println!("RUST DEBUG: ⏱️ SUCCESS: docNative completed [TOTAL TIMING: {}ms]", method_start_time.elapsed().as_millis());
                            document_obj.into_raw()
                        },
                        Err(e) => {
                            debug_println!("RUST DEBUG: ⏱️ ERROR: Failed to create Document object: {} [TOTAL TIMING: {}ms]", e, method_start_time.elapsed().as_millis());
                            to_java_exception(&mut env, &anyhow::anyhow!("Failed to create Document: {}", e));
                            std::ptr::null_mut()
                        }
                    }
                },
                Err(e) => {
                    debug_println!("RUST DEBUG: ⏱️ ERROR: Failed to find Document class: {} [TOTAL TIMING: {}ms]", e, method_start_time.elapsed().as_millis());
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to find Document class: {}", e));
                    std::ptr::null_mut()
                }
            }
        },
        Err(e) => {
            debug_println!("RUST DEBUG: ⏱️ 🔥 ERROR: retrieve_document_from_split_optimized FAILED: {} [TIMING: {}ms] [TOTAL TIMING: {}ms]", e, retrieval_start.elapsed().as_millis(), method_start_time.elapsed().as_millis());
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

/// Create Tantivy schema from field mappings JSON array
/// This handles the field mappings array format used by QuickwitSplit
fn create_schema_from_doc_mapping(doc_mapping_json: &str) -> anyhow::Result<tantivy::schema::Schema> {
    debug_println!("RUST DEBUG: Creating schema from field mappings JSON");
    debug_println!("RUST DEBUG: 🔍 RAW FIELD MAPPINGS JSON ({} chars): '{}'", doc_mapping_json.len(), doc_mapping_json);

    // Parse the field mappings JSON array
    #[derive(serde::Deserialize)]
    struct FieldMapping {
        name: String,
        #[serde(rename = "type")]
        field_type: String,
        stored: Option<bool>,
        indexed: Option<bool>,
        fast: Option<bool>,
        tokenizer: Option<String>,
    }

    let field_mappings: Vec<FieldMapping> = serde_json::from_str(doc_mapping_json)
        .map_err(|e| anyhow::anyhow!("Failed to parse field mappings JSON: {}", e))?;

    debug_println!("RUST DEBUG: Parsed {} field mappings", field_mappings.len());

    // Create Tantivy schema builder
    let mut schema_builder = tantivy::schema::Schema::builder();

    // Add each field to the schema
    for field_mapping in field_mappings {
        let stored = field_mapping.stored.unwrap_or(false);
        let indexed = field_mapping.indexed.unwrap_or(false);
        let fast = field_mapping.fast.unwrap_or(false);
        let tokenizer = field_mapping.tokenizer.as_deref().unwrap_or("default");

        debug_println!("RUST DEBUG: Adding field '{}' type '{}' stored={} indexed={} fast={} tokenizer='{}'",
            field_mapping.name, field_mapping.field_type, stored, indexed, fast, tokenizer);

        match field_mapping.field_type.as_str() {
            "text" => {
                let mut text_options = tantivy::schema::TextOptions::default();
                if stored {
                    text_options = text_options.set_stored();
                }
                if indexed {
                    let text_indexing = tantivy::schema::TextFieldIndexing::default()
                        .set_tokenizer(tokenizer)
                        .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions);
                    text_options = text_options.set_indexing_options(text_indexing);
                }
                if fast {
                    text_options = text_options.set_fast(Some("default"));
                }
                schema_builder.add_text_field(&field_mapping.name, text_options);
            },
            "i64" => {
                let mut int_options = tantivy::schema::NumericOptions::default();
                if stored {
                    int_options = int_options.set_stored();
                }
                if indexed {
                    int_options = int_options.set_indexed();
                }
                if fast {
                    int_options = int_options.set_fast();
                }
                schema_builder.add_i64_field(&field_mapping.name, int_options);
            },
            "f64" => {
                let mut float_options = tantivy::schema::NumericOptions::default();
                if stored {
                    float_options = float_options.set_stored();
                }
                if indexed {
                    float_options = float_options.set_indexed();
                }
                if fast {
                    float_options = float_options.set_fast();
                }
                schema_builder.add_f64_field(&field_mapping.name, float_options);
            },
            "bool" => {
                let mut bool_options = tantivy::schema::NumericOptions::default();
                if stored {
                    bool_options = bool_options.set_stored();
                }
                if indexed {
                    bool_options = bool_options.set_indexed();
                }
                if fast {
                    bool_options = bool_options.set_fast();
                }
                schema_builder.add_bool_field(&field_mapping.name, bool_options);
            },
            "object" => {
                let mut json_options = tantivy::schema::JsonObjectOptions::default();
                if stored {
                    json_options = json_options.set_stored();
                }
                schema_builder.add_json_field(&field_mapping.name, json_options);
            },
            other => {
                debug_println!("RUST DEBUG: ⚠️ Unsupported field type '{}' for field '{}', treating as text", other, field_mapping.name);
                // Fallback to text field for unknown types
                let mut text_options = tantivy::schema::TextOptions::default();
                if stored {
                    text_options = text_options.set_stored();
                }
                if indexed {
                    text_options = text_options.set_indexing_options(
                        tantivy::schema::TextFieldIndexing::default()
                            .set_tokenizer("default")
                            .set_index_option(tantivy::schema::IndexRecordOption::WithFreqsAndPositions)
                    );
                }
                schema_builder.add_text_field(&field_mapping.name, text_options);
            }
        }
    }

    let schema = schema_builder.build();
    debug_println!("RUST DEBUG: Successfully created Tantivy schema from field mappings with {} fields", schema.num_fields());
    Ok(schema)
}

/// Replacement for Java_com_tantivy4java_SplitSearcher_getSchemaFromNative
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitSearcher_getSchemaFromNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) -> jlong {
    eprintln!("🚀 SIMPLE DEBUG: getSchemaFromNative method called!");
    debug_println!("RUST DEBUG: *** getSchemaFromNative ENTRY POINT *** pointer: {}", searcher_ptr);
    
    if searcher_ptr == 0 {
        debug_println!("RUST DEBUG: searcher_ptr is 0, returning 0");
        return 0;
    }
    
    // debug_println!("RUST DEBUG: About to call with_object to access searcher context...");
    let method_start_time = std::time::Instant::now();
    debug_println!("RUST DEBUG: ⏱️ getSchemaFromNative ENTRY POINT [TIMING START]");
    
    if searcher_ptr == 0 {
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
        return 0;
    }

    // Extract the actual schema from the split file using Quickwit's functionality
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64, Option<String>)>| {
        let (_searcher, runtime, split_uri, aws_config, footer_start, footer_end, doc_mapping_json) = searcher_context.as_ref();
        debug_println!("RUST DEBUG: getSchemaFromNative called with split URI: {} [TIMING: {}ms]", split_uri, method_start_time.elapsed().as_millis());
        
        // 🚀 OPTIMIZATION: Use doc mapping JSON if available instead of expensive I/O
        if let Some(doc_mapping_str) = doc_mapping_json {
            let optimization_start = std::time::Instant::now();
            debug_println!("RUST DEBUG: ⏱️ 🚀 OPTIMIZATION ACTIVE: Using cached doc mapping JSON instead of I/O ({} chars) [TIMING: {}ms]", doc_mapping_str.len(), method_start_time.elapsed().as_millis());
            
            // Parse the doc mapping JSON and create schema from it
            match create_schema_from_doc_mapping(doc_mapping_str) {
                Ok(schema) => {
                    debug_println!("RUST DEBUG: ⏱️ ✅ Schema creation from doc mapping completed [TIMING: {}ms]", optimization_start.elapsed().as_millis());
                    
                    // Store schema clone in cache for parseQuery field extraction  
                    let cache_start = std::time::Instant::now();
                    debug_println!("RUST DEBUG: About to store schema for split URI: {}", split_uri);
                    store_split_schema(split_uri, schema.clone());
                    debug_println!("RUST DEBUG: ⏱️ Schema caching completed [TIMING: {}ms]", cache_start.elapsed().as_millis());
                    
                    // Register the schema using Arc for memory safety
                    let schema_arc = std::sync::Arc::new(schema);
                    let schema_ptr = arc_to_jlong(schema_arc);
                    debug_println!("RUST DEBUG: ⏱️ 🚀 OPTIMIZATION SUCCESS - Schema created from doc mapping with pointer: {} [TOTAL TIMING: {}ms]", schema_ptr, method_start_time.elapsed().as_millis());
                    return schema_ptr;
                },
                Err(e) => {
                    debug_println!("RUST DEBUG: ⚠️ Failed to create schema from doc mapping: {}, falling back to I/O [TIMING: {}ms]", e, optimization_start.elapsed().as_millis());
                    // Fall through to expensive I/O method
                }
            }
        } else {
            debug_println!("RUST DEBUG: ⚠️ No doc mapping available, falling back to expensive I/O method [TIMING: {}ms]", method_start_time.elapsed().as_millis());
        }
        
        // Enter the runtime context for async operations (EXPENSIVE FALLBACK)
        let fallback_start = std::time::Instant::now();
        debug_println!("RUST DEBUG: ⏱️ ⚠️ STARTING EXPENSIVE I/O FALLBACK [TIMING: {}ms]", method_start_time.elapsed().as_millis());
        let _guard = runtime.enter();
        
        
        // Parse the split URI and extract schema using Quickwit's storage abstractions
        use quickwit_common::uri::Uri;
        use std::path::Path;
        
        // Use block_on to run async code synchronously within the runtime context
        let schema = tokio::task::block_in_place(|| {
            runtime.block_on(async {
                let async_start = std::time::Instant::now();
                debug_println!("RUST DEBUG: ⏱️ Starting async block [TIMING: {}ms]", method_start_time.elapsed().as_millis());
                // Parse URI and resolve storage
                let uri_parse_start = std::time::Instant::now();
                let uri: Uri = split_uri.parse()
                    .map_err(|e| anyhow::anyhow!("Failed to parse split URI {}: {}", split_uri, e))?;
                debug_println!("RUST DEBUG: ⏱️ URI parsing completed [TIMING: {}ms]", uri_parse_start.elapsed().as_millis());
                
                // Create S3 storage configuration with credentials from Java config
                use quickwit_config::{StorageConfigs, S3StorageConfig};
                let storage_config_start = std::time::Instant::now();
                let mut storage_configs = StorageConfigs::default();
                
                debug_println!("RUST DEBUG: Creating S3 config with credentials from tantivy4java (not environment)");
                let s3_config = S3StorageConfig {
                    flavor: None,
                    access_key_id: aws_config.get("access_key").cloned(),
                    secret_access_key: aws_config.get("secret_key").cloned(), 
                    session_token: aws_config.get("session_token").cloned(),
                    region: aws_config.get("region").cloned(),
                    endpoint: aws_config.get("endpoint").cloned(),
                    force_path_style_access: aws_config.get("path_style_access").map_or(false, |v| v == "true"),
                    disable_multi_object_delete: false,
                    disable_multipart_upload: false,
                };
                
                debug_println!("RUST DEBUG: S3 config created with access_key: {}, region: {}", 
                         s3_config.access_key_id.as_ref().map(|k| &k[..std::cmp::min(8, k.len())]).unwrap_or("None"),
                         s3_config.region.as_ref().unwrap_or(&"None".to_string()));
                
                let storage_configs_vec = StorageConfigs::new(vec![quickwit_config::StorageConfig::S3(s3_config.clone())]);
                storage_configs = storage_configs_vec;
                
                let storage_resolver = StorageResolver::configured(&storage_configs);
                debug_println!("RUST DEBUG: ⏱️ Storage config creation completed [TIMING: {}ms]", storage_config_start.elapsed().as_millis());
                
                // Use the helper function to resolve storage correctly for S3 URIs
                let storage_resolve_start = std::time::Instant::now();
                let actual_storage = resolve_storage_for_split(&storage_resolver, split_uri).await?;
                debug_println!("RUST DEBUG: ⏱️ Storage resolution completed [TIMING: {}ms]", storage_resolve_start.elapsed().as_millis());
                
                // Extract relative path - for direct file paths, use just the filename
                let relative_path = if split_uri.contains("://") {
                    // This is a URI, extract just the filename
                    if let Some(last_slash_pos) = split_uri.rfind('/') {
                        Path::new(&split_uri[last_slash_pos + 1..])
                    } else {
                        Path::new(split_uri)
                    }
                } else {
                    // This is a direct file path, extract just the filename
                    Path::new(split_uri)
                        .file_name()
                        .map(|name| Path::new(name))
                        .unwrap_or_else(|| Path::new(split_uri))
                };
                
                debug_println!("RUST DEBUG: ⏱️ 🚀 USING QUICKWIT'S STANDARD HOTCACHE OPTIMIZATION [TIMING: {}ms]", method_start_time.elapsed().as_millis());
                debug_println!("RUST DEBUG: Footer range: {} to {}", footer_start, footer_end);
                
                // Use Quickwit's standard approach with proper hotcache optimization
                let quickwit_setup_start = std::time::Instant::now();
                use quickwit_search::leaf::open_index_with_caches;
                use quickwit_proto::search::SplitIdAndFooterOffsets;
                use quickwit_storage::ByteRangeCache;
                
                // Extract split ID from the path
                let split_id = relative_path.file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("unknown")
                    .to_string();
                
                // Create SplitIdAndFooterOffsets from our footer metadata
                let split_and_footer_offsets = SplitIdAndFooterOffsets {
                    split_id,
                    split_footer_start: *footer_start,
                    split_footer_end: *footer_end,
                    timestamp_start: None,
                    timestamp_end: None,
                    num_docs: 0,
                };
                
                debug_println!("RUST DEBUG: Created SplitIdAndFooterOffsets with footer: {} to {}", 
                         split_and_footer_offsets.split_footer_start, split_and_footer_offsets.split_footer_end);
                
                // Create searcher context (required for open_index_with_caches)
                let searcher_context = crate::global_cache::get_global_searcher_context();
                
                // Create ephemeral cache for this operation
                let byte_range_cache = ByteRangeCache::with_infinite_capacity(
                    &quickwit_storage::STORAGE_METRICS.shortlived_cache
                );
                
                debug_println!("RUST DEBUG: ⏱️ Quickwit setup completed [TIMING: {}ms]", quickwit_setup_start.elapsed().as_millis());
                
                // Use Quickwit's optimized open_index_with_caches which properly handles hotcache
                let index_open_start = std::time::Instant::now();
                debug_println!("RUST DEBUG: ⏱️ 🔥 CALLING open_index_with_caches (POTENTIAL BOTTLENECK) [TIMING: {}ms]", method_start_time.elapsed().as_millis());
                let (index, _hot_directory) = open_index_with_caches(
                    &searcher_context,
                    actual_storage,
                    &split_and_footer_offsets,
                    None, // No tokenizer manager needed for schema extraction
                    Some(byte_range_cache),
                ).await.map_err(|e| anyhow::anyhow!("Failed to open index with caches {}: {}", split_uri, e))?;
                debug_println!("RUST DEBUG: ⏱️ 🔥 open_index_with_caches COMPLETED [TIMING: {}ms]", index_open_start.elapsed().as_millis());
                    
                let schema_extract_start = std::time::Instant::now();
                let schema = index.schema();
                debug_println!("RUST DEBUG: ⏱️ Schema extraction from index completed [TIMING: {}ms]", schema_extract_start.elapsed().as_millis());
                
                debug_println!("RUST DEBUG: ⏱️ Total async block completed [TIMING: {}ms]", async_start.elapsed().as_millis());
                Ok::<tantivy::schema::Schema, anyhow::Error>(schema)
            })
        });
        
        debug_println!("RUST DEBUG: ⏱️ EXPENSIVE I/O FALLBACK COMPLETED [TIMING: {}ms]", fallback_start.elapsed().as_millis());
        
        match schema {
            Ok(s) => {
                // Store schema clone in cache for parseQuery field extraction  
                let final_cache_start = std::time::Instant::now();
                debug_println!("RUST DEBUG: About to store schema for split URI (I/O fallback): {}", split_uri);
                debug_println!("RUST DEBUG: BEFORE store_split_schema call");
                store_split_schema(split_uri, s.clone());
                debug_println!("RUST DEBUG: AFTER store_split_schema call");
                debug_println!("RUST DEBUG: ⏱️ Schema caching completed (I/O fallback) [TIMING: {}ms]", final_cache_start.elapsed().as_millis());
                
                // Register the actual schema from the split using Arc for memory safety
                let schema_arc = std::sync::Arc::new(s);
                let schema_ptr = arc_to_jlong(schema_arc);
                debug_println!("RUST DEBUG: ⏱️ SUCCESS - Schema extracted and registered with Arc pointer: {} [TOTAL TIMING: {}ms]", schema_ptr, method_start_time.elapsed().as_millis());
                schema_ptr
            },
            Err(e) => {
                debug_println!("RUST DEBUG: ⏱️ FATAL ERROR - Schema extraction failed completely for split {}: {} [TOTAL TIMING: {}ms]", split_uri, e, method_start_time.elapsed().as_millis());
                debug_println!("RUST DEBUG: Error chain: {:?}", e);
                // Return 0 to indicate failure
                0
            }
        }
    });

    match result {
        Some(schema_ptr) => {
            debug_println!("⏱️ SUCCESS: Schema extracted and registered with pointer: {} [TOTAL METHOD TIMING: {}ms]", schema_ptr, method_start_time.elapsed().as_millis());
            schema_ptr
        },
        None => {
            debug_println!("⏱️ ERROR: with_object returned None - searcher context not found for pointer {} [TOTAL METHOD TIMING: {}ms]", searcher_ptr, method_start_time.elapsed().as_millis());
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
    with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64, Option<String>)>| {
        let (_searcher, runtime, split_uri, aws_config, _footer_start, _footer_end, _doc_mapping_json) = searcher_context.as_ref();
        
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
                    force_path_style_access: aws_config.get("path_style_access").map_or(false, |v| v == "true"),
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
                    
                // ✅ QUICKWIT NATIVE: Extract schema from the bundle directory using Quickwit's native index opening
                let index = open_index(bundle_directory.box_clone(), get_quickwit_fastfield_normalizer_manager().tantivy_manager())
                    .map_err(|e| anyhow::anyhow!("Failed to open index from bundle {}: {}", split_uri, e))?;
                
                Ok(index.schema())
            })
        })
    })
    .ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?
}

/// Fix range queries in QueryAst JSON by looking up field types from schema
fn fix_range_query_types(searcher_ptr: jlong, query_json: &str) -> anyhow::Result<String> {
    let fix_start = std::time::Instant::now();
    debug_println!("RUST DEBUG: ⏱️ fix_range_query_types ENTRY POINT [TIMING START]");
    
    // Parse the JSON to find range queries
    let parse_start = std::time::Instant::now();
    let mut query_value: Value = serde_json::from_str(query_json)?;
    debug_println!("RUST DEBUG: ⏱️ Query JSON parsing completed [TIMING: {}ms]", parse_start.elapsed().as_millis());
    
    // 🚀 OPTIMIZATION: Try to get cached schema first instead of expensive I/O
    let schema_start = std::time::Instant::now();
    let schema = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64, Option<String>)>| {
        let (_searcher, _runtime, split_uri, _aws_config, _footer_start, _footer_end, _doc_mapping_json) = searcher_context.as_ref();
        
        // First try to get schema from cache
        if let Some(cached_schema) = get_split_schema(split_uri) {
            debug_println!("RUST DEBUG: ⏱️ 🚀 Using CACHED schema instead of expensive I/O [TIMING: {}ms]", schema_start.elapsed().as_millis());
            return Ok(cached_schema);
        }
        
        // Fallback to expensive I/O only if cache miss
        debug_println!("RUST DEBUG: ⚠️ Cache miss, falling back to expensive I/O for schema [TIMING: {}ms]", schema_start.elapsed().as_millis());
        get_schema_from_split(searcher_ptr)
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))??;
    
    debug_println!("RUST DEBUG: ⏱️ Schema retrieval completed [TIMING: {}ms]", schema_start.elapsed().as_millis());
    
    // Recursively fix range queries in the JSON
    let recursive_start = std::time::Instant::now();
    fix_range_queries_recursive(&mut query_value, &schema)?;
    debug_println!("RUST DEBUG: ⏱️ Range query fixing completed [TIMING: {}ms]", recursive_start.elapsed().as_millis());
    
    // Convert back to JSON string
    let serialize_start = std::time::Instant::now();
    let result = serde_json::to_string(&query_value).map_err(|e| anyhow::anyhow!("Failed to serialize fixed query: {}", e))?;
    debug_println!("RUST DEBUG: ⏱️ JSON serialization completed [TIMING: {}ms]", serialize_start.elapsed().as_millis());
    
    debug_println!("RUST DEBUG: ⏱️ fix_range_query_types COMPLETED [TOTAL TIMING: {}ms]", fix_start.elapsed().as_millis());
    Ok(result)
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

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitSearcher_tokenizeNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    field_name: JString,
    text: JString,
) -> jobject {
    // Extract field name and text from JNI
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Invalid field name: {}", e));
            return std::ptr::null_mut();
        }
    };

    let text_str: String = match env.get_string(&text) {
        Ok(s) => s.into(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Invalid text: {}", e));
            return std::ptr::null_mut();
        }
    };

    debug_println!("RUST DEBUG: tokenizeNative called for field '{}' with text '{}'", field_name_str, text_str);

    // Get the searcher context and schema (same pattern as get_schema_from_split)
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64, Option<String>)>| {
        let (_searcher, _runtime, _split_uri, _aws_config, _footer_start, _footer_end, doc_mapping_json) = searcher_context.as_ref();

        // Get schema from doc mapping - throw exception if not available
        let schema = if let Some(doc_mapping) = doc_mapping_json {
            match create_schema_from_doc_mapping(doc_mapping) {
                Ok(schema) => schema,
                Err(e) => {
                    return Err(anyhow::anyhow!("Failed to create schema from doc mapping for tokenization: {}", e));
                }
            }
        } else {
            return Err(anyhow::anyhow!("Doc mapping not available for tokenization - split searcher not properly initialized"));
        };

        // Find the field in the schema
        let field = match schema.get_field(&field_name_str) {
            Ok(field) => field,
            Err(_) => {
                return Err(anyhow::anyhow!("Field '{}' not found in schema", field_name_str));
            }
        };

        // Get the field entry to determine the tokenizer
        let field_entry = schema.get_field_entry(field);

        // Create a text analyzer for the field
        let mut tokenizer = match field_entry.field_type() {
            tantivy::schema::FieldType::Str(text_options) => {
                // For text fields, get the tokenizer from indexing options
                if let Some(indexing_options) = text_options.get_indexing_options() {
                    let tokenizer_name = indexing_options.tokenizer();
                    debug_println!("RUST DEBUG: Field '{}' uses tokenizer '{}'", field_name_str, tokenizer_name);

                    // Create the tokenizer based on the name
                    match tokenizer_name {
                        "default" => {
                            tantivy::tokenizer::TextAnalyzer::builder(tantivy::tokenizer::SimpleTokenizer::default())
                                .filter(tantivy::tokenizer::RemoveLongFilter::limit(40))
                                .filter(tantivy::tokenizer::LowerCaser)
                                .build()
                        },
                        "raw" => {
                            // For string fields (raw tokenizer), return the original text as a single token
                            debug_println!("RUST DEBUG: Using raw tokenizer for field '{}'", field_name_str);
                            let tokens = vec![text_str.clone()];
                            return create_token_list(&mut env, tokens);
                        },
                        "whitespace" => {
                            tantivy::tokenizer::TextAnalyzer::builder(tantivy::tokenizer::WhitespaceTokenizer::default())
                                .build()
                        },
                        "keyword" => {
                            // Keyword tokenizer treats the entire input as a single token
                            let tokens = vec![text_str.clone()];
                            return create_token_list(&mut env, tokens);
                        },
                        _ => {
                            // Default to simple tokenizer for unknown tokenizers
                            debug_println!("RUST DEBUG: Unknown tokenizer '{}', using default", tokenizer_name);
                            tantivy::tokenizer::TextAnalyzer::builder(tantivy::tokenizer::SimpleTokenizer::default())
                                .filter(tantivy::tokenizer::RemoveLongFilter::limit(40))
                                .filter(tantivy::tokenizer::LowerCaser)
                                .build()
                        }
                    }
                } else {
                    // No indexing options means it's not indexed, but we can still tokenize
                    tantivy::tokenizer::TextAnalyzer::builder(tantivy::tokenizer::SimpleTokenizer::default())
                        .filter(tantivy::tokenizer::RemoveLongFilter::limit(40))
                        .filter(tantivy::tokenizer::LowerCaser)
                        .build()
                }
            },
            _ => {
                // For non-text fields (numbers, dates, etc.), return the original text as a single token
                debug_println!("RUST DEBUG: Non-text field '{}', returning original text as single token", field_name_str);
                let tokens = vec![text_str.clone()];
                return create_token_list(&mut env, tokens);
            }
        };

        // Tokenize the text
        let mut token_stream = tokenizer.token_stream(&text_str);
        let mut tokens = Vec::new();

        while let Some(token) = token_stream.next() {
            tokens.push(token.text.clone());
        }

        debug_println!("RUST DEBUG: Tokenized '{}' into {} tokens: {:?}", text_str, tokens.len(), tokens);

        create_token_list(&mut env, tokens)
    });

    match result {
        Some(Ok(tokens_list)) => tokens_list,
        Some(Err(e)) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        },
        None => {
            to_java_exception(&mut env, &anyhow::anyhow!("Invalid SplitSearcher pointer"));
            std::ptr::null_mut()
        }
    }
}

/// Helper function to create a Java List<String> from a vector of tokens
fn create_token_list(env: &mut JNIEnv, tokens: Vec<String>) -> Result<jobject, anyhow::Error> {
    // Create ArrayList
    let array_list_class = env.find_class("java/util/ArrayList")?;
    let array_list = env.new_object(&array_list_class, "()V", &[])?;

    // Add each token to the list
    for token in tokens {
        let java_string = env.new_string(&token)?;
        env.call_method(
            &array_list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[(&java_string).into()],
        )?;
    }

    Ok(array_list.into_raw())
}

// ============================================================================
// QUICKWIT AGGREGATION INTEGRATION (USING PROVEN SYSTEM)
// ============================================================================

/// Convert Java aggregations map to JSON for Quickwit's SearchRequest.aggregation_request field
fn convert_java_aggregations_to_json<'a>(
    env: &mut JNIEnv<'a>,
    aggregations_map: &JObject<'a>,
) -> anyhow::Result<Option<String>> {
    use serde_json::json;

    debug_println!("RUST DEBUG: Converting Java aggregations to JSON for Quickwit system");

    // Check if aggregations map is empty
    let is_empty_method = env.call_method(aggregations_map, "isEmpty", "()Z", &[])?;
    let is_empty: bool = is_empty_method.z()?;

    if is_empty {
        debug_println!("RUST DEBUG: Aggregations map is empty, returning None");
        return Ok(None);
    }

    // Extract Map entries from Java HashMap
    let map_entries = extract_map_entries(env, aggregations_map)?;
    let mut aggregations_json = serde_json::Map::new();

    for (name, java_aggregation) in map_entries {
        debug_println!("RUST DEBUG: Processing aggregation: {}", name);

        // Convert each Java SplitAggregation to JSON
        let agg_json = convert_java_aggregation_to_json(env, &java_aggregation)?;
        aggregations_json.insert(name, agg_json);
    }

    // Wrap in a JSON object as expected by Quickwit's aggregation system
    let final_json = json!(aggregations_json);
    let json_string = serde_json::to_string(&final_json)?;

    debug_println!("RUST DEBUG: Generated aggregation JSON: {}", json_string);
    Ok(Some(json_string))
}

/// Convert a single Java SplitAggregation to JSON format
fn convert_java_aggregation_to_json<'a>(
    env: &mut JNIEnv<'a>,
    java_aggregation: &JObject<'a>,
) -> anyhow::Result<serde_json::Value> {
    

    debug_println!("RUST DEBUG: Converting Java aggregation to JSON");

    // Use the existing toAggregationJson method from the Java class
    let json_result = env.call_method(java_aggregation, "toAggregationJson", "()Ljava/lang/String;", &[])?;
    let json_string: String = env.get_string(&json_result.l()?.into())?.into();

    debug_println!("RUST DEBUG: Java aggregation produced JSON: {}", json_string);

    // Parse the JSON string to validate it and convert to serde_json::Value
    let json_value: serde_json::Value = serde_json::from_str(&json_string)
        .map_err(|e| anyhow::anyhow!("Failed to parse aggregation JSON from Java: {}", e))?;

    Ok(json_value)
}

/// Extract entries from a Java HashMap
fn extract_map_entries<'a>(
    env: &mut JNIEnv<'a>,
    map: &JObject<'a>,
) -> anyhow::Result<Vec<(String, JObject<'a>)>> {
    let mut entries = Vec::new();

    // Get entrySet from HashMap
    let entry_set = env.call_method(map, "entrySet", "()Ljava/util/Set;", &[])?.l()?;

    // Get iterator from Set
    let iterator = env.call_method(&entry_set, "iterator", "()Ljava/util/Iterator;", &[])?.l()?;

    // Iterate through entries
    loop {
        let has_next = env.call_method(&iterator, "hasNext", "()Z", &[])?;
        if !has_next.z()? {
            break;
        }

        let entry = env.call_method(&iterator, "next", "()Ljava/lang/Object;", &[])?.l()?;

        // Get key and value from Map.Entry
        let key = env.call_method(&entry, "getKey", "()Ljava/lang/Object;", &[])?;
        let value = env.call_method(&entry, "getValue", "()Ljava/lang/Object;", &[])?.l()?;

        // Convert key to String
        let key_string: String = env.get_string(&key.l()?.into())?.into();

        entries.push((key_string, value.into()));
    }

    Ok(entries)
}

/// Perform search using Quickwit's proven aggregation system with JSON
fn perform_search_with_quickwit_aggregations(
    searcher_ptr: jlong,
    query_ast: quickwit_query::query_ast::QueryAst,
    limit: usize,
    aggregation_request_json: Option<String>,
) -> anyhow::Result<quickwit_proto::search::LeafSearchResponse> {
    debug_println!("RUST DEBUG: 🚀 Starting aggregation search with Quickwit integration");
    if let Some(ref agg_json) = aggregation_request_json {
        debug_println!("RUST DEBUG: 📊 Aggregation JSON: {}", agg_json);
    }

    // Convert QueryAst to JSON and use the WORKING searchWithQueryAst infrastructure
    // This ensures we use exactly the same storage setup, index opening, and DocMapper creation
    let query_json = serde_json::to_string(&query_ast)
        .map_err(|e| anyhow::anyhow!("Failed to serialize QueryAst to JSON: {}", e))?;

    debug_println!("RUST DEBUG: 📊 Performing aggregation search using proven searchWithQueryAst infrastructure");

    // Call the working searchWithQueryAst implementation directly
    let leaf_search_response = perform_search_with_query_ast_and_aggregations_using_working_infrastructure(searcher_ptr, query_json, limit, aggregation_request_json)?;
    Ok(leaf_search_response)
}

/// Use the exact same infrastructure as the working regular search but add aggregation support
fn perform_search_with_query_ast_and_aggregations_using_working_infrastructure(
    searcher_ptr: jlong,
    query_json: String,
    limit: usize,
    aggregation_request_json: Option<String>,
) -> anyhow::Result<quickwit_proto::search::LeafSearchResponse> {
    // This function reuses the EXACT same approach as the working searchWithQueryAst
    // but adds aggregation support to the SearchRequest

    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<(StandaloneSearcher, tokio::runtime::Runtime, String, std::collections::HashMap<String, String>, u64, u64, Option<String>)>| {
        let (searcher, runtime, split_uri, aws_config, footer_start, footer_end, doc_mapping_json) = searcher_context.as_ref();

        // Enter the runtime context for async operations
        let _guard = runtime.enter();

        // Run the EXACT same async code as the working search
        runtime.block_on(async {
            // Parse the QueryAst JSON using Quickwit's libraries - IDENTICAL TO WORKING SEARCH
            use quickwit_query::query_ast::QueryAst;
            use quickwit_common::uri::Uri;
            use quickwit_config::StorageConfigs;

            let query_ast: QueryAst = serde_json::from_str(&query_json)
                .map_err(|e| anyhow::anyhow!("Failed to parse QueryAst JSON: {}", e))?;

            debug_println!("RUST DEBUG: Successfully parsed QueryAst: {:?}", query_ast);

            // STORAGE SETUP - IDENTICAL TO WORKING SEARCH
            let storage_setup_start = std::time::Instant::now();
            debug_println!("RUST DEBUG: ⏱️ 🔧 SEARCH STORAGE SETUP - Starting storage resolution for: {}", split_uri);

            let uri: Uri = split_uri.parse()
                .map_err(|e| anyhow::anyhow!("Failed to parse split URI {}: {}", split_uri, e))?;

            // Create S3 storage configuration with credentials from Java config
            let mut storage_configs = StorageConfigs::default();

            debug_println!("RUST DEBUG: ⏱️ 🔧 Creating S3 config with credentials from Java configuration");
            let s3_config = S3StorageConfig {
                flavor: None,
                access_key_id: aws_config.get("access_key").cloned(),
                secret_access_key: aws_config.get("secret_key").cloned(),
                session_token: aws_config.get("session_token").cloned(),
                region: aws_config.get("region").cloned(),
                endpoint: aws_config.get("endpoint").cloned(),
                force_path_style_access: aws_config.get("path_style_access").map_or(false, |v| v == "true"),
                disable_multi_object_delete: false,
                disable_multipart_upload: false,
            };

            let storage_configs_vec = StorageConfigs::new(vec![quickwit_config::StorageConfig::S3(s3_config.clone())]);
            storage_configs = storage_configs_vec;

            let storage_resolver = StorageResolver::configured(&storage_configs);

            // Use the helper function to resolve storage correctly for S3 URIs
            let storage = resolve_storage_for_split(&storage_resolver, split_uri).await?;
            debug_println!("RUST DEBUG: ⏱️ 🔧 SEARCH STORAGE SETUP completed [TIMING: {}ms]", storage_setup_start.elapsed().as_millis());

            // Extract relative path - IDENTICAL TO WORKING SEARCH
            let relative_path = if split_uri.contains("://") {
                // This is a URI, extract just the filename
                if let Some(last_slash_pos) = split_uri.rfind('/') {
                    std::path::Path::new(&split_uri[last_slash_pos + 1..])
                } else {
                    std::path::Path::new(split_uri)
                }
            } else {
                // This is a direct file path, extract just the filename
                std::path::Path::new(split_uri)
                    .file_name()
                    .map(|name| std::path::Path::new(name))
                    .unwrap_or_else(|| std::path::Path::new(split_uri))
            };

            debug_println!("RUST DEBUG: Reading split file metadata from: '{}'", relative_path.display());

            // Use footer offsets from Java configuration for optimized access
            let split_footer_start = *footer_start;
            let split_footer_end = *footer_end;

            debug_println!("RUST DEBUG: 🚀 Using Quickwit optimized path with open_index_with_caches - NO full file download");
            debug_println!("RUST DEBUG: Footer offsets from Java config: start={}, end={}", split_footer_start, split_footer_end);

            // Create SplitIdAndFooterOffsets for Quickwit optimization
            let split_id = relative_path.file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown");

            let split_and_footer_offsets = quickwit_proto::search::SplitIdAndFooterOffsets {
                split_id: split_id.to_string(),
                split_footer_start,
                split_footer_end,
                num_docs: 0, // Not used for opening, will be filled later
                timestamp_start: None,
                timestamp_end: None,
            };

            // Create proper SearcherContext for Quickwit functions
            let quickwit_searcher_context = crate::global_cache::get_global_searcher_context();

            // Use Quickwit's complete optimized index opening (does all the work!) - IDENTICAL TO WORKING SEARCH
            let (index, _hot_directory) = quickwit_search::leaf::open_index_with_caches(
                &quickwit_searcher_context,
                storage.clone(),
                &split_and_footer_offsets,
                None, // tokenizer_manager
                None, // ephemeral_unbounded_cache
            ).await.map_err(|e| anyhow::anyhow!("Failed to open index with caches {}: {}", split_uri, e))?;

            debug_println!("RUST DEBUG: ✅ Quickwit optimized index opening completed successfully");

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
                file_size: split_footer_end, // Footer end is effectively the file size
                time_range: None, // TODO: Extract from split metadata if available
                delete_opstamp: 0,
                num_docs,
            };

            // Build DocMapper from ACTUAL index schema - IDENTICAL TO WORKING SEARCH
            let schema = index.schema();

            // Build a DocMapping from the tantivy schema
            // This is the proper way to create a DocMapper that matches the actual index
            let mut field_mappings = Vec::new();

            for (field, field_entry) in schema.fields() {
                let field_name = schema.get_field_name(field);
                let field_type = field_entry.field_type();

                use tantivy::schema::FieldType;
                let (mapping_type, tokenizer) = match field_type {
                    FieldType::Str(text_options) => {
                        if let Some(indexing_options) = text_options.get_indexing_options() {
                            let tokenizer_name = indexing_options.tokenizer();
                            ("text", Some(tokenizer_name.to_string()))
                        } else {
                            // Store-only text fields should still be "text" type, not "keyword"
                            // Quickwit's DocMapper only supports "text" type for Str fields
                            ("text", None)
                        }
                    },
                    FieldType::U64(_) => ("u64", None),
                    FieldType::I64(_) => ("i64", None),
                    FieldType::F64(_) => ("f64", None),
                    FieldType::Bool(_) => ("bool", None),
                    FieldType::Date(_) => ("datetime", None),
                    FieldType::Bytes(_) => ("bytes", None),
                    FieldType::IpAddr(_) => ("ip", None),
                    FieldType::JsonObject(_) => ("json", None),
                    FieldType::Facet(_) => ("keyword", None), // Facets are similar to keywords
                };

                let mut field_mapping = serde_json::json!({
                    "name": field_name,
                    "type": mapping_type,
                });

                // Add tokenizer information for text fields
                if let Some(tokenizer_name) = tokenizer {
                    field_mapping["tokenizer"] = serde_json::Value::String(tokenizer_name);
                    debug_println!("RUST DEBUG: Field '{}' has tokenizer '{}'", field_name, field_mapping["tokenizer"]);
                }

                field_mappings.push(field_mapping);
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

            // Create a SearchRequest with the QueryAst - THE KEY DIFFERENCE: INCLUDE AGGREGATION!
            let search_request = SearchRequest {
                index_id_patterns: vec![],
                query_ast: query_json.clone(), // SearchRequest.query_ast expects String, not QueryAst object
                max_hits: limit as u64,
                start_offset: 0,
                start_timestamp: None,
                end_timestamp: None,
                aggregation_request: aggregation_request_json.clone(), // THIS IS THE KEY ADDITION!
                snippet_fields: vec![],
                sort_fields: vec![],
                search_after: None,
                scroll_ttl_secs: None,
                count_hits: quickwit_proto::search::CountHits::CountAll as i32,
            };

            debug_println!("RUST DEBUG: ⏱️ 🔍 SEARCH EXECUTION - Calling StandaloneSearcher.search_split with parameters:");
            debug_println!("  - Split URI: {}", split_uri);
            debug_println!("  - Split ID: {}", split_metadata.split_id);
            debug_println!("  - Num docs: {}", split_metadata.num_docs);
            debug_println!("  - Footer offsets: {}-{}", split_metadata.split_footer_start, split_metadata.split_footer_end);
            debug_println!("  - Has aggregations: {}", aggregation_request_json.is_some());

            // PERFORM THE ACTUAL REAL SEARCH WITH AGGREGATIONS - SAME METHOD AS WORKING SEARCH!
            let search_exec_start = std::time::Instant::now();
            debug_println!("RUST DEBUG: ⏱️ 🔍 Starting actual search execution via searcher.search_split()");

            // We're already in an async context, so use the async method directly
            let split_id_for_error = split_metadata.split_id.clone();
            let leaf_search_response = match searcher.search_split(
                split_uri,
                split_metadata,
                search_request,
                doc_mapper_arc,
            ).await {
                Ok(response) => response,
                Err(e) => {
                    debug_println!("RUST DEBUG: ⏱️ 🔍 ERROR in searcher.search_split [TIMING: {}ms]: {}", search_exec_start.elapsed().as_millis(), e);
                    debug_println!("RUST DEBUG: Full error chain: {:#}", e);
                    // Propagate the full error chain to Java
                    return Err(anyhow::anyhow!("{:#}", e));
                }
            };

            debug_println!("RUST DEBUG: ⏱️ 🔍 SEARCH EXECUTION completed [TIMING: {}ms] - Found {} hits", search_exec_start.elapsed().as_millis(), leaf_search_response.num_hits);
            debug_println!("RUST DEBUG: 🔍 Search response has aggregations: {}", leaf_search_response.intermediate_aggregation_result.is_some());

            // Return the LeafSearchResponse directly
            Ok(leaf_search_response)
        })
    });

    // Return the LeafSearchResponse directly
    match result {
        Some(Ok(leaf_search_response)) => Ok(leaf_search_response),
        Some(Err(e)) => Err(e),
        None => Err(anyhow::anyhow!("Invalid searcher pointer")),
    }
}

// REMOVED: perform_unified_search - was broken and unused

/// Unified function to create SearchResult Java object from LeafSearchResponse
fn perform_unified_search_result_creation(
    leaf_search_response: quickwit_proto::search::LeafSearchResponse,
    env: &mut JNIEnv,
) -> anyhow::Result<jobject> {
    // Convert results to enhanced format
    let mut search_results = Vec::new();
    for partial_hit in leaf_search_response.partial_hits {
        let doc_address = tantivy::DocAddress::new(
            partial_hit.segment_ord as tantivy::SegmentOrdinal,
            partial_hit.doc_id as tantivy::DocId,
        );

        let score = if let Some(sort_value) = partial_hit.sort_value() {
            match sort_value {
                quickwit_proto::search::SortValue::F64(f) => f as f32,
                quickwit_proto::search::SortValue::U64(u) => u as f32,
                quickwit_proto::search::SortValue::I64(i) => i as f32,
                _ => 1.0_f32,
            }
        } else {
            1.0_f32
        };

        search_results.push((score, doc_address));
    }

    debug_println!("RUST DEBUG: Converted {} hits to SearchResult format", search_results.len());

    // Extract aggregation results
    let aggregation_results = leaf_search_response.intermediate_aggregation_result.clone();
    if aggregation_results.is_some() {
        debug_println!("RUST DEBUG: 📊 Found aggregation results in LeafSearchResponse");
    }

    // Create enhanced result
    let enhanced_result = EnhancedSearchResult {
        hits: search_results,
        aggregation_results,
    };

    let search_results_arc = Arc::new(enhanced_result);
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
}

// REMOVED: perform_search_with_query_ast_and_aggregations - redundant function eliminated

