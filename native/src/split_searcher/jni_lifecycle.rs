// jni_lifecycle.rs - JNI lifecycle management for SplitSearcher
// Extracted from mod.rs during refactoring
// Contains: createNativeWithSharedCache, closeNative, validateSplitNative, getCacheStatsNative

use std::sync::Arc;
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
                        Ok((cached_index, _hot_directory)) => {
                            debug_println!("🔥 INDEX CACHED: Index opened once for reuse, cached for all operations");

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
                            // Quickwit's DocMapper requires at least one field mapping for JSON/object fields, but Tantivy's dynamic JSON fields don't have predefined mappings
                            debug_println!("RUST DEBUG: 🔧 Using index.schema() directly (doc_mapping has compatibility issues with dynamic JSON fields)");
                            let schema = cached_index.schema();
                            let schema_ptr = crate::utils::arc_to_jlong(std::sync::Arc::new(schema));
                            debug_println!("RUST DEBUG: 🔧 Created schema_ptr={} from reconstructed schema", schema_ptr);

                            // Create clean struct-based context instead of complex tuple
                            let cached_context = CachedSearcherContext {
                                standalone_searcher: std::sync::Arc::new(searcher),
                                // ✅ CRITICAL FIX: No longer storing runtime - using shared global runtime
                                split_uri: split_uri.clone(),
                                aws_config,
                                footer_start: split_footer_start,
                                footer_end: split_footer_end,
                                doc_mapping_json,
                                cached_storage: resolved_storage,
                                cached_index: std::sync::Arc::new(cached_index),
                                cached_searcher,
                                // 🚀 BATCH OPTIMIZATION FIX: Store ByteRangeCache and bundle file offsets
                                // Note: byte_range_cache is None if L1 cache is disabled for debugging
                                byte_range_cache,
                                bundle_file_offsets,
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
