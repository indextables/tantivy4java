// jni_lifecycle.rs - JNI lifecycle management for SplitSearcher
// Extracted from mod.rs during refactoring
// Contains: createNativeWithSharedCache, closeNative, validateSplitNative, getCacheStatsNative

use std::sync::Arc;
use std::str::FromStr;
use jni::objects::{JClass, JString, JObject};
use jni::sys::{jlong, jobject, jboolean};
use jni::JNIEnv;

use crate::debug_println;
use crate::common::to_java_exception;
use crate::utils::{arc_to_jlong, with_arc_safe, release_arc};
use crate::standalone_searcher::{StandaloneSearcher, StandaloneSearchConfig};
use quickwit_config::S3StorageConfig;
use quickwit_storage::Storage;
use super::types::CachedSearcherContext;
use super::searcher_cache::parse_bundle_file_offsets;
use super::cache_config::{get_batch_doc_cache_blocks_with_metadata, extract_split_metadata_for_allocation};

/// Replacement for Java_io_indextables_tantivy4java_split_SplitSearcher_createNativeWithSharedCache
/// Now properly integrates StandaloneSearcher with runtime management and stores split URI
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_createNativeWithSharedCache(
    mut env: JNIEnv,
    _class: JClass,
    split_uri_jstr: JString,
    cache_manager_ptr: jlong,
    split_config_map: jobject,
) -> jlong {
    let thread_id = std::thread::current().id();
    let start_time = std::time::Instant::now();

    debug_println!("🚀 SIMPLE DEBUG: createNativeWithSharedCache method called!");
    debug_println!("🧵 SPLIT_SEARCHER: Thread {:?} ENTRY into createNativeWithSharedCache [{}ms]",
                  thread_id, start_time.elapsed().as_millis());
    debug_println!("🔗 SPLIT_SEARCHER: Thread {:?} cache_manager_ptr: 0x{:x} [{}ms]",
                  thread_id, cache_manager_ptr, start_time.elapsed().as_millis());

    // Register searcher with runtime manager for lifecycle tracking
    let runtime = crate::runtime_manager::QuickwitRuntimeManager::global();
    runtime.register_searcher();
    debug_println!("✅ RUNTIME_MANAGER: Searcher registered with runtime manager");
    // Validate JString parameter first to prevent SIGSEGV
    if split_uri_jstr.is_null() {
        runtime.unregister_searcher(); // Cleanup registration on error
        to_java_exception(&mut env, &anyhow::anyhow!("Split URI parameter is null"));
        return 0;
    }
    
    // Extract the split URI string with proper error handling
    let split_uri: String = match env.get_string(&split_uri_jstr) {
        Ok(java_str) => java_str.into(),
        Err(e) => {
            runtime.unregister_searcher(); // Cleanup registration on error
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to extract split URI: {}", e));
            return 0;
        }
    };
    
    // Validate that the extracted string is not empty
    if split_uri.is_empty() {
        runtime.unregister_searcher(); // Cleanup registration on error
        to_java_exception(&mut env, &anyhow::anyhow!("Split URI cannot be empty"));
        return 0;
    }
    
    // Validate cache manager pointer (though we're not using it in this implementation)
    if cache_manager_ptr == 0 {
        runtime.unregister_searcher(); // Cleanup registration on error
        to_java_exception(&mut env, &anyhow::anyhow!("Cache manager pointer is null"));
        return 0;
    }
    
    // Extract AWS/Azure configuration and split metadata from the split config map
    let mut aws_config: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut azure_config: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut split_footer_start: u64 = 0;
    let mut split_footer_end: u64 = 0;
    let mut doc_mapping_json: Option<String> = None;
    let mut parquet_table_root: Option<String> = None;
    let mut parquet_aws_config: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    let mut parquet_azure_config: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    
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

        // ✅ NEW: Extract Azure config
        if let Ok(azure_config_obj) = env.call_method(&split_config_jobject, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("azure_config").unwrap()).into()]) {
            let azure_config_jobject = azure_config_obj.l().unwrap();
            if !azure_config_jobject.is_null() {
                let azure_config_map = &azure_config_jobject;

                // Extract account_name
                if let Ok(account_name_obj) = env.call_method(azure_config_map, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("account_name").unwrap()).into()]) {
                    let account_name_jobject = account_name_obj.l().unwrap();
                    if !account_name_jobject.is_null() {
                        if let Ok(account_name_str) = env.get_string((&account_name_jobject).into()) {
                            azure_config.insert("account_name".to_string(), account_name_str.into());
                            debug_println!("RUST DEBUG: Extracted Azure account_name from Java config");
                        }
                    }
                }

                // Extract access_key (account key)
                if let Ok(access_key_obj) = env.call_method(azure_config_map, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("access_key").unwrap()).into()]) {
                    let access_key_jobject = access_key_obj.l().unwrap();
                    if !access_key_jobject.is_null() {
                        if let Ok(access_key_str) = env.get_string((&access_key_jobject).into()) {
                            azure_config.insert("access_key".to_string(), access_key_str.into());
                            debug_println!("RUST DEBUG: Extracted Azure access_key from Java config");
                        }
                    }
                }

                // Extract bearer_token (for OAuth authentication)
                if let Ok(bearer_token_obj) = env.call_method(azure_config_map, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("bearer_token").unwrap()).into()]) {
                    let bearer_token_jobject = bearer_token_obj.l().unwrap();
                    if !bearer_token_jobject.is_null() {
                        if let Ok(bearer_token_str) = env.get_string((&bearer_token_jobject).into()) {
                            let token_len = bearer_token_str.to_str().unwrap().len();
                            azure_config.insert("bearer_token".to_string(), bearer_token_str.into());
                            debug_println!("RUST DEBUG: Extracted Azure bearer_token from Java config (length: {} chars)", token_len);
                        }
                    }
                }
            }
        }

        // Extract parquet_table_root (String) for parquet companion mode
        if let Ok(ptr_obj) = env.call_method(&split_config_jobject, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("parquet_table_root").unwrap()).into()]) {
            let ptr_jobject = ptr_obj.l().unwrap();
            if !ptr_jobject.is_null() {
                if let Ok(ptr_str) = env.get_string((&ptr_jobject).into()) {
                    let s: String = ptr_str.into();
                    if !s.is_empty() {
                        debug_println!("RUST DEBUG: Extracted parquet_table_root from Java config: {}", s);
                        parquet_table_root = Some(s);
                    }
                }
            }
        }

        // Extract parquet_aws_config (Map) for parquet file access credentials
        if let Ok(pac_obj) = env.call_method(&split_config_jobject, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("parquet_aws_config").unwrap()).into()]) {
            let pac_jobject = pac_obj.l().unwrap();
            if !pac_jobject.is_null() {
                let pac_map = &pac_jobject;
                for key in &["access_key", "secret_key", "session_token", "region", "endpoint"] {
                    if let Ok(val_obj) = env.call_method(pac_map, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string(*key).unwrap()).into()]) {
                        let val_jobject = val_obj.l().unwrap();
                        if !val_jobject.is_null() {
                            if let Ok(val_str) = env.get_string((&val_jobject).into()) {
                                parquet_aws_config.insert(key.to_string(), val_str.into());
                            }
                        }
                    }
                }
                if !parquet_aws_config.is_empty() {
                    debug_println!("RUST DEBUG: Extracted parquet_aws_config with {} keys", parquet_aws_config.len());
                }
            }
        }

        // Extract parquet_azure_config (Map) for parquet file access credentials
        if let Ok(paz_obj) = env.call_method(&split_config_jobject, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string("parquet_azure_config").unwrap()).into()]) {
            let paz_jobject = paz_obj.l().unwrap();
            if !paz_jobject.is_null() {
                let paz_map = &paz_jobject;
                for key in &["account_name", "access_key", "bearer_token"] {
                    if let Ok(val_obj) = env.call_method(paz_map, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&env.new_string(*key).unwrap()).into()]) {
                        let val_jobject = val_obj.l().unwrap();
                        if !val_jobject.is_null() {
                            if let Ok(val_str) = env.get_string((&val_jobject).into()) {
                                parquet_azure_config.insert(key.to_string(), val_str.into());
                            }
                        }
                    }
                }
                if !parquet_azure_config.is_empty() {
                    debug_println!("RUST DEBUG: Extracted parquet_azure_config with {} keys", parquet_azure_config.len());
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
                    let doc_mapping_string: String = doc_mapping_str.into();
                    debug_println!("🔥 NATIVE DEBUG: RAW doc_mapping from Java ({} chars): '{}'", doc_mapping_string.len(), doc_mapping_string);
                    doc_mapping_json = Some(doc_mapping_string);
                    debug_println!("RUST DEBUG: ✅ SUCCESS - Extracted doc mapping JSON from Java config ({} chars)", doc_mapping_json.as_ref().unwrap().len());
                } else {
                    debug_println!("🔥 NATIVE DEBUG: ⚠️ Failed to convert doc_mapping_jobject to string");
                    debug_println!("RUST DEBUG: ⚠️ Failed to convert doc_mapping_jobject to string");
                }
            } else {
                debug_println!("🔥 NATIVE DEBUG: ⚠️ doc_mapping_jobject is null - no doc mapping provided by Java");
                debug_println!("RUST DEBUG: ⚠️ doc_mapping_jobject is null");
            }
        } else {
            debug_println!("🔥 NATIVE DEBUG: ⚠️ Failed to call get method on HashMap for 'doc_mapping' key");
            debug_println!("RUST DEBUG: ⚠️ Failed to call get method on HashMap for 'doc_mapping' key");
        }
    }
    
    debug_println!("RUST DEBUG: Config extracted - AWS keys: {}, footer offsets: {}-{}, doc_mapping: {}",
                aws_config.len(), split_footer_start, split_footer_end,
                doc_mapping_json.as_ref().map(|s| format!("{}chars", s.len())).unwrap_or_else(|| "None".to_string()));

    // ✅ CRITICAL FIX: Use shared global runtime instead of creating individual runtime
    // This eliminates multiple Tokio runtime conflicts that cause production hangs

    // ✅ CRITICAL FIX: Execute StandaloneSearcher creation in shared runtime context
    // Quickwit's SplitCache::with_root_path spawns tasks, so we need runtime context
    let _enter = runtime.handle().enter();
    let result = tokio::task::block_in_place(|| {
        // Create StandaloneSearcher using global caches
        // If AWS credentials are provided, use with_s3_config, otherwise use default
        if aws_config.contains_key("access_key") && aws_config.contains_key("secret_key") {
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
        StandaloneSearcher::with_s3_config(StandaloneSearchConfig::default(), s3_config.clone())
        } else {
            debug_println!("RUST DEBUG: Creating StandaloneSearcher with default config and global caches");
            // Use default() which now uses global caches
            StandaloneSearcher::default()
        }
    });

    // StandaloneSearcher creation succeeded, use result directly

    // Pre-create StorageResolver synchronously to avoid async issues during search
    let storage_resolver = if aws_config.contains_key("access_key") && aws_config.contains_key("secret_key") {
        debug_println!("RUST DEBUG: Pre-creating StorageResolver with S3 config to prevent deadlocks");
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

        crate::global_cache::get_configured_storage_resolver(Some(s3_config), None)
    } else if azure_config.contains_key("account_name") && (azure_config.contains_key("access_key") || azure_config.contains_key("bearer_token")) {
        debug_println!("RUST DEBUG: Pre-creating StorageResolver with Azure config to prevent deadlocks");
        debug_println!("RUST DEBUG: Azure account_name: {}", azure_config.get("account_name").unwrap());
        debug_println!("RUST DEBUG: Azure auth method: {}", if azure_config.contains_key("bearer_token") { "OAuth bearer token" } else { "account key" });
        use quickwit_config::AzureStorageConfig;

        let azure_storage_config = AzureStorageConfig {
            account_name: Some(azure_config.get("account_name").unwrap().clone()),
            access_key: azure_config.get("access_key").cloned(),
            bearer_token: azure_config.get("bearer_token").cloned(),
        };

        crate::global_cache::get_configured_storage_resolver(None, Some(azure_storage_config))
    } else {
        debug_println!("RUST DEBUG: Pre-creating default StorageResolver to prevent deadlocks");
        crate::global_cache::get_configured_storage_resolver(None, None)
    };

    match result {
        Ok(searcher) => {
            // Follow Quickwit pattern: resolve storage once and cache it for reuse
            let storage = runtime.handle().block_on(async {
                use crate::standalone_searcher::resolve_storage_for_split;
                let result = resolve_storage_for_split(&storage_resolver, &split_uri).await;
                result
            });

            match storage {
                Ok(raw_storage) => {
                    debug_println!("🔥 STORAGE RESOLVED: Raw storage resolved, instance: {:p}", Arc::as_ptr(&raw_storage));

                    // Wrap with L2 disk cache only (no L1 - Quickwit's caches handle memory caching)
                    use crate::persistent_cache_storage::StorageWithPersistentCache;
                    use crate::global_cache::get_global_disk_cache;

                    let split_filename = if let Some(last_slash_pos) = split_uri.rfind('/') {
                        &split_uri[last_slash_pos + 1..]
                    } else {
                        &split_uri
                    };
                    let split_id_for_cache = if split_filename.ends_with(".split") {
                        split_filename[..split_filename.len() - 6].to_string()
                    } else {
                        split_filename.to_string()
                    };

                    debug_println!("RUST TRACE: Checking disk cache for tiered storage wrapper (split_uri={})", split_uri);
                    let resolved_storage: Arc<dyn Storage> = match get_global_disk_cache() {
                        Some(disk_cache) => {
                            debug_println!("RUST TRACE: Creating StorageWithPersistentCache with L2 only for {}", split_uri);
                            debug_println!("🔄 TIERED_CACHE: Wrapping storage with L2 disk cache (no redundant L1)");
                            Arc::new(StorageWithPersistentCache::with_disk_cache_only(
                                raw_storage,
                                disk_cache,
                                split_uri.clone(),
                                split_id_for_cache,
                            ))
                        }
                        None => {
                            // No disk cache configured - use raw storage directly
                            debug_println!("RUST DEBUG: No disk cache configured - using raw storage");
                            raw_storage
                        }
                    };
                    debug_println!("🔥 STORAGE WRAPPED: Storage wrapped with tiered cache, instance: {:p}", Arc::as_ptr(&resolved_storage));

                    // Follow Quickwit pattern: open index once and cache it
                    // 🚀 BATCH OPTIMIZATION FIX: Create ByteRangeCache and parse bundle metadata
                    // Generate credential key for credential-specific SearcherContext
                    // This prevents cached data from one credential set being served to another
                    let credential_key = if aws_config.contains_key("access_key") {
                        let mut s3_config_for_key = quickwit_config::S3StorageConfig::default();
                        s3_config_for_key.access_key_id = aws_config.get("access_key").cloned();
                        s3_config_for_key.secret_access_key = aws_config.get("secret_key").cloned();
                        s3_config_for_key.session_token = aws_config.get("session_token").cloned();
                        s3_config_for_key.region = aws_config.get("region").cloned();
                        crate::global_cache::generate_storage_cache_key(Some(&s3_config_for_key), None)
                    } else if azure_config.contains_key("account_name") {
                        let azure_config_for_key = quickwit_config::AzureStorageConfig {
                            account_name: azure_config.get("account_name").cloned(),
                            access_key: azure_config.get("access_key").cloned(),
                            bearer_token: azure_config.get("bearer_token").cloned(),
                        };
                        crate::global_cache::generate_storage_cache_key(None, Some(&azure_config_for_key))
                    } else {
                        "global".to_string()
                    };

                    let (opened_index, byte_range_cache, bundle_file_offsets) = runtime.handle().block_on(async {
                        use quickwit_proto::search::SplitIdAndFooterOffsets;
                        use crate::global_cache::get_credential_searcher_context;

                        let split_filename = if let Some(last_slash_pos) = split_uri.rfind('/') {
                            &split_uri[last_slash_pos + 1..]
                        } else {
                            &split_uri
                        };
                        let split_id = if split_filename.ends_with(".split") {
                            &split_filename[..split_filename.len() - 6]
                        } else {
                            split_filename
                        };

                        let split_metadata = SplitIdAndFooterOffsets {
                            split_id: split_id.to_string(),
                            split_footer_start: split_footer_start,
                            split_footer_end: split_footer_end,
                            timestamp_start: None,
                            timestamp_end: None,
                            num_docs: 0,
                        };

                        // 🚀 BATCH OPTIMIZATION FIX: Use SHARED global ByteRangeCache for all splits
                        // This provides memory-efficient caching - one bounded 256MB cache shared by all splits
                        // instead of per-split caches that could exhaust memory.
                        //
                        // DEBUGGING: L1 cache can be disabled via TieredCacheConfig.withDisableL1Cache(true)
                        // to help debug cache key mismatches between prewarm and query operations.
                        let byte_range_cache = crate::global_cache::get_or_create_global_l1_cache();
                        let byte_range_cache_for_open = byte_range_cache.clone();
                        if byte_range_cache.is_some() {
                            debug_println!("🚀 L1_CACHE_ENABLED: Using shared global ByteRangeCache");
                        } else {
                            debug_println!("⚠️ L1_CACHE_DISABLED: ByteRangeCache set to None - all reads go to L2 disk cache / L3 storage");
                        }

                        // 🚀 OPTIMIZATION: Call open_index_with_caches FIRST to populate split_footer_cache
                        // This eliminates redundant footer fetches when we parse bundle offsets below
                        // 🔐 SECURITY FIX: Use credential-specific SearcherContext to prevent cached data
                        // from one credential set being served to another credential set
                        let searcher_context_global = get_credential_searcher_context(&credential_key);

                        let result = quickwit_search::open_index_with_caches(
                            &searcher_context_global,
                            resolved_storage.clone(),
                            &split_metadata,
                            None, // tokenizer_manager
                            byte_range_cache_for_open,  // 🚀 Pass ByteRangeCache for prefetch optimization (None if disabled)
                        ).await;

                        // 🚀 BATCH OPTIMIZATION FIX: Parse bundle file offsets from footer (now cached in split_footer_cache!)
                        // This allows prefetch to translate inner file byte ranges to split file byte ranges
                        let bundle_file_offsets = if split_footer_start > 0 && split_footer_end > split_footer_start {
                            // Use searcher_context_global.split_footer_cache - same cache open_index_with_caches uses
                            match parse_bundle_file_offsets(&resolved_storage, &split_uri, split_footer_start, split_footer_end, &searcher_context_global).await {
                                Ok(offsets) => {
                                    debug_println!("🚀 BATCH_OPT: Parsed {} bundle file offsets for prefetch optimization", offsets.len());
                                    offsets
                                }
                                Err(e) => {
                                    debug_println!("⚠️ BATCH_OPT: Failed to parse bundle file offsets (prefetch optimization disabled): {}", e);
                                    std::collections::HashMap::new()
                                }
                            }
                        } else {
                            debug_println!("⚠️ BATCH_OPT: No footer metadata available (prefetch optimization disabled)");
                            std::collections::HashMap::new()
                        };

                        (result, byte_range_cache, bundle_file_offsets)
                    });

                    match opened_index {
                        Ok((initial_index, hot_directory)) => {
                            debug_println!("🔥 INDEX CACHED: Index opened once for reuse, cached for all operations");

                            // Check for parquet companion manifest in the split bundle FIRST
                            // (needed to decide whether to wrap directory with ParquetAugmentedDirectory)
                            let parquet_manifest = {
                                use crate::parquet_companion::manifest_io::MANIFEST_FILENAME;
                                let manifest_path = std::path::PathBuf::from(MANIFEST_FILENAME);
                                if bundle_file_offsets.contains_key(&manifest_path) {
                                    debug_println!("📦 PARQUET_COMPANION: Found {} in split bundle, reading manifest...", MANIFEST_FILENAME);
                                    // Read manifest bytes from the split file using the byte range
                                    // from bundle_file_offsets (the HotDirectory only caches tantivy index files).
                                    let manifest_result = runtime.handle().block_on(async {
                                        let range = bundle_file_offsets.get(&manifest_path).unwrap().clone();
                                        let split_path = std::path::Path::new(&split_uri).file_name()
                                            .map(|f| std::path::PathBuf::from(f))
                                            .unwrap_or_else(|| std::path::PathBuf::from(&split_uri));
                                        let manifest_bytes = resolved_storage
                                            .get_slice(&split_path, range.start as usize..range.end as usize).await
                                            .map_err(|e| anyhow::anyhow!("Failed to read parquet manifest from split: {}", e))?;
                                        crate::parquet_companion::manifest_io::deserialize_manifest(&manifest_bytes)
                                    });
                                    match manifest_result {
                                        Ok(manifest) => {
                                            debug_println!("📦 PARQUET_COMPANION: Successfully loaded manifest (version={}, {} files, fast_field_mode={:?})",
                                                manifest.version, manifest.parquet_files.len(), manifest.fast_field_mode);
                                            Some(std::sync::Arc::new(manifest))
                                        }
                                        Err(e) => {
                                            debug_println!("⚠️ PARQUET_COMPANION: Failed to deserialize manifest: {}", e);
                                            None
                                        }
                                    }
                                } else {
                                    debug_println!("📦 PARQUET_COMPANION: No {} found in split bundle (standard mode)", MANIFEST_FILENAME);
                                    None
                                }
                            };

                            // Phase 2: If parquet fast field mode is active, create the
                            // ParquetAugmentedDirectory for later use during prewarm/aggregation.
                            // We do NOT re-open the index — the initial_index already has the
                            // correct inverted index for search. The augmented directory is only
                            // needed to intercept .fast file reads during aggregation/prewarm.
                            use crate::parquet_companion::manifest::FastFieldMode;

                            // Phase 2a: Create parquet_storage BEFORE the augmented directory,
                            // so the augmented directory uses it for reading parquet files from
                            // the correct table_root (which may be on S3/Azure).
                            let effective_parquet_table_root: Option<String> = if parquet_manifest.is_some() {
                                match &parquet_table_root {
                                    Some(root) => Some(root.clone()),
                                    None => {
                                        debug_println!("⚠️ PARQUET_COMPANION: No parquet_table_root provided in config");
                                        None
                                    }
                                }
                            } else {
                                None
                            };

                            let parquet_storage: Option<Arc<dyn Storage>> = if let Some(ref table_root) = effective_parquet_table_root {
                                let pq_storage_config = {
                                    use crate::parquet_companion::parquet_storage::ParquetStorageConfig;
                                    let effective_aws = if !parquet_aws_config.is_empty() { &parquet_aws_config } else { &aws_config };
                                    let effective_azure = if !parquet_azure_config.is_empty() { &parquet_azure_config } else { &azure_config };
                                    ParquetStorageConfig {
                                        aws_access_key: effective_aws.get("access_key").cloned(),
                                        aws_secret_key: effective_aws.get("secret_key").cloned(),
                                        aws_session_token: effective_aws.get("session_token").cloned(),
                                        aws_region: effective_aws.get("region").cloned(),
                                        aws_endpoint: effective_aws.get("endpoint").cloned(),
                                        aws_force_path_style: effective_aws.get("path_style_access").map(|v| v == "true").unwrap_or(false),
                                        azure_account_name: effective_azure.get("account_name").cloned(),
                                        azure_access_key: effective_azure.get("access_key").cloned(),
                                        azure_bearer_token: effective_azure.get("bearer_token").cloned(),
                                    }
                                };

                                let table_uri = if table_root.contains("://") {
                                    table_root.clone()
                                } else {
                                    format!("file://{}/", table_root.trim_end_matches('/'))
                                };
                                debug_println!("📦 PARQUET_COMPANION: Creating parquet storage for table_root='{}' (uri='{}')", table_root, table_uri);

                                match crate::parquet_companion::parquet_storage::create_parquet_storage(&pq_storage_config, &table_uri) {
                                    Ok(storage) => {
                                        debug_println!("📦 PARQUET_COMPANION: Parquet storage created for '{}'", table_uri);
                                        Some(storage)
                                    }
                                    Err(e) => {
                                        eprintln!("WARNING: PARQUET_COMPANION: Failed to create parquet storage for '{}': {}. \
                                                   Doc retrieval/prewarm will fail for this split.", table_uri, e);
                                        debug_println!("⚠️ PARQUET_COMPANION: Failed to create parquet storage for '{}': {}", table_uri, e);
                                        None
                                    }
                                }
                            } else {
                                None
                            };

                            let (cached_index, augmented_directory, split_overrides, parquet_meta_json, segment_fast_paths) = {
                                let needs_augmented = parquet_manifest.as_ref()
                                    .map(|m| m.fast_field_mode != FastFieldMode::Disabled)
                                    .unwrap_or(false);

                                if needs_augmented {
                                    let manifest = parquet_manifest.as_ref().unwrap();
                                    debug_println!("📦 PARQUET_COMPANION: Fast field mode {:?} active, re-opening index with all-fast schema",
                                        manifest.fast_field_mode);

                                    // Create the augmented directory (wraps hot_directory).
                                    // Use parquet_storage for reading parquet files (rooted at table_root),
                                    // and resolved_storage for reading native .fast bytes from the split bundle.
                                    let split_file_path = std::path::Path::new(&split_uri).file_name()
                                        .map(|f| std::path::PathBuf::from(f))
                                        .unwrap_or_else(|| std::path::PathBuf::from(&split_uri));
                                    let pq_storage_for_augmented = parquet_storage.clone()
                                        .unwrap_or_else(|| resolved_storage.clone());
                                    let augmented = crate::parquet_companion::augmented_directory::ParquetAugmentedDirectory::new(
                                        std::sync::Arc::new(hot_directory.clone()),
                                        manifest.fast_field_mode,
                                        manifest.clone(),
                                        pq_storage_for_augmented,
                                        resolved_storage.clone(),
                                        bundle_file_offsets.clone(),
                                        split_file_path,
                                        split_uri.clone(),
                                    );
                                    let augmented_arc = std::sync::Arc::new(augmented);
                                    debug_println!("📦 PARQUET_COMPANION: ParquetAugmentedDirectory created");

                                    // Re-open the Index with all fields marked as fast in the schema.
                                    // Uses Quickwit's UnionDirectory pattern: shadow meta.json with a
                                    // RamDirectory containing a modified version where all fast flags are true.
                                    let (final_index, overrides, parquet_meta_json_bytes, seg_fast_paths) = {
                                        use tantivy::directory::{Directory, RamDirectory};
                                        use quickwit_directories::UnionDirectory;

                                        // Read original meta.json from the hot_directory
                                        let original_meta = hot_directory.atomic_read(std::path::Path::new("meta.json"))
                                            .map_err(|e| anyhow::anyhow!("Failed to read meta.json from split: {:?}", e));

                                        match original_meta.and_then(|bytes| {
                                            crate::parquet_companion::schema_derivation::promote_meta_json_all_fast(&bytes)
                                                .map_err(|e| anyhow::anyhow!("Failed to promote meta.json all-fast: {}", e))
                                        }) {
                                            Ok(modified_meta) => {
                                                // Drop the initial index to release ManagedDirectory lock
                                                drop(initial_index);

                                                // Shadow meta.json with the all-fast version via UnionDirectory
                                                let shadow_dir = RamDirectory::default();
                                                shadow_dir.atomic_write(
                                                    std::path::Path::new("meta.json"),
                                                    &modified_meta,
                                                ).expect("RamDirectory atomic_write should not fail");

                                                let union_dir = UnionDirectory::union_of(vec![
                                                    Box::new(shadow_dir),
                                                    Box::new(hot_directory.clone()),
                                                ]);

                                                match tantivy::Index::open(union_dir) {
                                                    Ok(mut idx) => {
                                                        idx.set_tokenizers(
                                                            quickwit_query::get_quickwit_fastfield_normalizer_manager()
                                                                .tantivy_manager()
                                                                .clone(),
                                                        );
                                                        idx.set_fast_field_tokenizers(
                                                            quickwit_query::get_quickwit_fastfield_normalizer_manager()
                                                                .tantivy_manager()
                                                                .clone(),
                                                        );
                                                        debug_println!("📦 PARQUET_COMPANION: Re-opened index with all-fast schema via UnionDirectory");

                                                        // Lazy transcoding: do NOT pre-transcode all columns.
                                                        // Instead, discover segment .fast paths and store them
                                                        // for on-demand transcoding when queries need fast fields.
                                                        let segment_metas = idx.searchable_segment_metas()
                                                            .unwrap_or_default();
                                                        let seg_fast_paths: Vec<std::path::PathBuf> = segment_metas.iter()
                                                            .map(|m| m.relative_path(tantivy::index::SegmentComponent::FastFields))
                                                            .collect();
                                                        debug_println!(
                                                            "📦 PARQUET_COMPANION: Discovered {} segment .fast paths for lazy transcoding",
                                                            seg_fast_paths.len()
                                                        );

                                                        // Create SplitOverrides with meta_json only, empty fast_field_data.
                                                        // Fast field data will be populated lazily per-query.
                                                        let overrides = quickwit_search::SplitOverrides {
                                                            meta_json: modified_meta.clone(),
                                                            fast_field_data: std::collections::HashMap::new(),
                                                        };
                                                        (idx, Some(std::sync::Arc::new(overrides)), Some(modified_meta), seg_fast_paths)
                                                    }
                                                    Err(e) => {
                                                        debug_println!("⚠️ PARQUET_COMPANION: Failed to re-open with all-fast schema: {}. This should not happen.", e);
                                                        panic!("Failed to re-open split index with all-fast schema: {}", e);
                                                    }
                                                }
                                            }
                                            Err(e) => {
                                                debug_println!("⚠️ PARQUET_COMPANION: Failed to promote meta.json: {}. Using original index.", e);
                                                (initial_index, None, None, vec![])
                                            }
                                        }
                                    };

                                    debug_println!("📦 PARQUET_COMPANION: SplitOverrides ready (has_overrides={}), lazy transcoding enabled", overrides.is_some());
                                    (final_index, Some(augmented_arc), overrides, parquet_meta_json_bytes, seg_fast_paths)
                                } else {
                                    (initial_index, None, None, None, vec![])
                                }
                            };

                            // Follow Quickwit's exact pattern: create index reader and cached searcher
                            // Extract metadata for enhanced memory allocation
                            let split_metadata = extract_split_metadata_for_allocation(&split_uri);
                            let batch_cache_blocks = get_batch_doc_cache_blocks_with_metadata(split_metadata);
                            debug_println!("⚡ CACHE_OPTIMIZATION: Applying advanced adaptive doc store cache optimization - blocks: {} (batch operations with metadata)", batch_cache_blocks);
                            let index_reader = match cached_index
                                .reader_builder()
                                .doc_store_cache_num_blocks(batch_cache_blocks) // Advanced adaptive cache sizing
                                .reload_policy(tantivy::ReloadPolicy::Manual)
                                .try_into() {
                                Ok(reader) => reader,
                                Err(e) => {
                                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to create index reader: {}", e));
                                    return 0;
                                }
                            };

                            let cached_searcher = std::sync::Arc::new(index_reader.searcher());
                            debug_println!("🔥 SEARCHER CACHED: Created cached searcher following Quickwit's exact pattern for optimal cache reuse");

                            // ✅ FIX: Always use index.schema() for now - doc_mapping reconstruction has issues with dynamic JSON fields
                            debug_println!("RUST DEBUG: 🔧 Using index.schema() directly (doc_mapping has compatibility issues with dynamic JSON fields)");
                            let schema = cached_index.schema();
                            let schema_ptr = crate::utils::arc_to_jlong(std::sync::Arc::new(schema));
                            debug_println!("RUST DEBUG: 🔧 Created schema_ptr={} from reconstructed schema", schema_ptr);

                            // Create clean struct-based context instead of complex tuple
                            let cached_context = CachedSearcherContext {
                                standalone_searcher: std::sync::Arc::new(searcher),
                                split_uri: split_uri.clone(),
                                aws_config,
                                footer_start: split_footer_start,
                                footer_end: split_footer_end,
                                doc_mapping_json,
                                cached_storage: resolved_storage,
                                cached_index: std::sync::Arc::new(cached_index),
                                cached_searcher,
                                byte_range_cache,
                                bundle_file_offsets,
                                parquet_manifest,
                                parquet_table_root: effective_parquet_table_root,
                                parquet_storage,
                                augmented_directory,
                                split_overrides,
                                parquet_meta_json,
                                transcoded_fast_columns: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
                                segment_fast_paths,
                                parquet_metadata_cache: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
                                parquet_byte_range_cache: crate::parquet_companion::cached_reader::new_byte_range_cache(),
                            };

                            let searcher_context = std::sync::Arc::new(cached_context);
                            let pointer = arc_to_jlong(searcher_context);
                            debug_println!("RUST DEBUG: SUCCESS: Stored searcher context with cached index for split '{}' with Arc pointer: {}, footer: {}-{}",
                                     split_uri, pointer, split_footer_start, split_footer_end);

                            // ✅ DEBUG: Immediately verify the Arc can be retrieved
                            if let Some(_test_context) = crate::utils::jlong_to_arc::<CachedSearcherContext>(pointer) {
                                debug_println!("✅ VERIFICATION: Arc {} successfully stored and retrieved from registry", pointer);
                            } else {
                                debug_println!("❌ CRITICAL BUG: Arc {} was stored but CANNOT be retrieved immediately!", pointer);
                            }

                            // ✅ FIX: Store direct mapping from searcher pointer to schema pointer for fallback
                            debug_println!("RUST DEBUG: 🔧 Storing schema mapping: searcher_ptr={} -> schema_ptr={}", pointer, schema_ptr);
                            crate::split_query::store_searcher_schema(pointer, schema_ptr);
                            debug_println!("RUST DEBUG: 🔧 Schema mapping stored successfully");
                            debug_println!("✅ SEARCHER_SCHEMA_MAPPING: Stored mapping {} -> {} for reliable schema access", pointer, schema_ptr);

                            debug_println!("🏁 SPLIT_SEARCHER: Thread {:?} COMPLETED successfully in {}ms - pointer: 0x{:x}",
                                          thread_id, start_time.elapsed().as_millis(), pointer);
                            pointer
                        },
                        Err(e) => {
                            runtime.unregister_searcher(); // Cleanup registration on error
                            to_java_exception(&mut env, &anyhow::anyhow!("Failed to open index for split '{}': {}", split_uri, e));
                            0
                        }
                    }
                },
                Err(e) => {
                    runtime.unregister_searcher(); // Cleanup registration on error
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to resolve storage for split '{}': {}", split_uri, e));
                    0
                }
            }
        },
        Err(error) => {
            debug_println!("❌ SPLIT_SEARCHER: Thread {:?} FAILED after {}ms - error: {}",
                          thread_id, start_time.elapsed().as_millis(), error);
            runtime.unregister_searcher(); // Cleanup registration on error
            to_java_exception(&mut env, &error);
            0
        }
    }
}

/// Replacement for Java_io_indextables_tantivy4java_split_SplitSearcher_closeNative
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_closeNative(
    _env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) {
    let thread_id = std::thread::current().id();
    debug_println!("🧵 SPLIT_SEARCHER_CLOSE: Thread {:?} ENTRY into closeNative - pointer: 0x{:x}",
                  thread_id, searcher_ptr);

    if searcher_ptr == 0 {
        debug_println!("⚠️  SPLIT_SEARCHER_CLOSE: Thread {:?} - null pointer, nothing to close", thread_id);
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

    // ✅ FIX: Clean up direct schema mapping when searcher is closed
    crate::split_query::remove_searcher_schema(searcher_ptr);
    debug_println!("✅ CLEANUP: Removed direct schema mapping for searcher {}", searcher_ptr);

    // Unregister searcher from runtime manager
    let runtime = crate::runtime_manager::QuickwitRuntimeManager::global();
    runtime.unregister_searcher();
    debug_println!("✅ RUNTIME_MANAGER: Unregistered searcher from runtime manager");

    // SAFE: Release Arc from registry to prevent memory leaks
    release_arc(searcher_ptr);
    debug_println!("RUST DEBUG: Closed searcher and released Arc with ID: {}", searcher_ptr);
    debug_println!("🏁 SPLIT_SEARCHER_CLOSE: Thread {:?} COMPLETED successfully - pointer: 0x{:x}",
                  thread_id, searcher_ptr);
}

/// Replacement for Java_io_indextables_tantivy4java_split_SplitSearcher_validateSplitNative  
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_validateSplitNative(
    _env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) -> jboolean {
    // Simple validation - check if the searcher pointer is valid
    if searcher_ptr == 0 {
        return 0; // false
    }
    
    let is_valid = with_arc_safe(searcher_ptr, |_searcher_context: &Arc<CachedSearcherContext>| {
        // Searcher exists and is valid
        true
    }).unwrap_or(false);
    
    if is_valid { 1 } else { 0 }
}

/// Replacement for Java_io_indextables_tantivy4java_split_SplitSearcher_getCacheStatsNative
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_getCacheStatsNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) -> jobject {
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
        let context = searcher_context.as_ref();
        let stats = context.standalone_searcher.cache_stats();
        
        // Create a CacheStats Java object
        match env.find_class("io/indextables/tantivy4java/split/SplitSearcher$CacheStats") {
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
