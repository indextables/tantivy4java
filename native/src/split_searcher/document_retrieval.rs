// document_retrieval.rs - Document retrieval functions for SplitSearcher
// Extracted from mod.rs during refactoring

use std::sync::Arc;
use std::sync::atomic::Ordering;
use jni::objects::{JClass, JObject};
use jni::sys::{jlong, jobject, jint};
use jni::JNIEnv;

use crate::common::to_java_exception;
use crate::debug_println;
use crate::runtime_manager::block_on_operation;
use crate::global_cache::get_configured_storage_resolver;
use crate::standalone_searcher::resolve_storage_for_split;

use crate::split_searcher::types::CachedSearcherContext;
use crate::split_searcher::cache_config::{SINGLE_DOC_CACHE_BLOCKS, BASE_CONCURRENT_REQUESTS, get_batch_doc_cache_blocks};
use crate::split_searcher::searcher_cache::{
    get_searcher_cache, has_footer_metadata, is_remote_split, search_thread_pool,
    SEARCHER_CACHE_HITS, SEARCHER_CACHE_MISSES, SEARCHER_CACHE_EVICTIONS,
};

// Import async functions from parent module
use super::perform_doc_retrieval_async_impl_thread_safe;

use quickwit_indexing::open_index;
use quickwit_query::get_quickwit_fastfield_normalizer_manager;
use tantivy::directory::DirectoryClone;

/// Batch document retrieval for SplitSearcher using Quickwit's optimized approach
/// This implementation follows Quickwit's patterns from fetch_docs.rs:
/// 1. Sort addresses by segment for cache locality
/// 2. Open index with proper cache settings
/// 3. Use doc_async for optimal performance
/// 4. Reuse index, reader, and searcher across all documents
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_docBatchNative(
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
            let document_class = match env.find_class("io/indextables/tantivy4java/core/Document") {
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
    let split_uri = with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
        let context = searcher_context.as_ref();
        context.split_uri.clone()
    }).ok_or_else(|| anyhow::anyhow!("Invalid searcher context"))?;
    debug_println!("RUST DEBUG: ⏱️ Split URI extraction completed [TIMING: {}ms]", uri_extraction_start.elapsed().as_millis());
    
    // Check cache first - simple and effective
    let cache_check_start = std::time::Instant::now();
    let searcher_cache = get_searcher_cache();
    let cached_searcher = {
        let mut cache = searcher_cache.lock().unwrap();
        cache.get(&split_uri).cloned()
    };

    // Track cache statistics
    if cached_searcher.is_some() {
        SEARCHER_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
    } else {
        SEARCHER_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
    }

    debug_println!("RUST DEBUG: ⏱️ Cache lookup completed [TIMING: {}ms] - cache_hit: {}", cache_check_start.elapsed().as_millis(), cached_searcher.is_some());

    if let Some(searcher) = cached_searcher {
        // Use cached searcher - very fast path (cache hit)
        // IMPORTANT: Use async method for StorageDirectory compatibility
        let cache_hit_start = std::time::Instant::now();
        debug_println!("RUST DEBUG: ⏱️ 🎯 CACHE HIT - using cached searcher for document retrieval");
        
        // Extract the runtime and use async document retrieval
        let doc_and_schema = with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
            let _context = searcher_context.as_ref();

            // ✅ CRITICAL FIX: Use shared global runtime instead of context.runtime
            tokio::task::block_in_place(|| {
                crate::runtime_manager::QuickwitRuntimeManager::global().handle().block_on(async {
                    let doc = tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        searcher.doc_async(doc_address)
                    )
                    .await
                    .map_err(|_| anyhow::anyhow!("Document retrieval timed out for {:?}", doc_address))?
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
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
        let context = searcher_context.as_ref();

        // ✅ CRITICAL FIX: Use shared global runtime instead of context.runtime

        // Extract variables from context for compatibility with existing code
        let split_uri = &context.split_uri;
        let footer_start = context.footer_start;
        let footer_end = context.footer_end;
        let storage_resolver = &context.cached_storage;
        let cached_index = &context.cached_index;

        // Use the same Quickwit caching pattern as our batch method
        tokio::task::block_in_place(|| {
            crate::runtime_manager::QuickwitRuntimeManager::global().handle().block_on(async {

                use std::sync::Arc;

                // Use pre-created storage resolver from searcher context
                debug_println!("✅ QUICKWIT_LIFECYCLE: Using cached storage from searcher context (Quickwit pattern)");
                debug_println!("✅ CACHED_STORAGE_USED: Storage at address {:p} (Quickwit lifecycle)", Arc::as_ptr(storage_resolver));
                let index_storage = storage_resolver.clone();
                
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
                let mut index = if has_footer_metadata(footer_start, footer_end) && is_remote_split(split_uri) {
                    debug_println!("RUST DEBUG: 🚀 Using Quickwit optimized path for individual document retrieval (footer: {}..{})", footer_start, footer_end);

                    // Use cached index to eliminate repeated open_index_with_caches calls
                    let index_creation_start = std::time::Instant::now();
                    let index = cached_index.as_ref().clone();
                    debug_println!("🔥 INDEX CACHED: Reusing cached index instead of expensive open_index_with_caches call");
                    
                    debug_println!("RUST DEBUG: ⏱️ 📖 Quickwit hotcache index creation completed [TIMING: {}ms]", index_creation_start.elapsed().as_millis());
                    debug_println!("RUST DEBUG: ✅ Successfully opened index with Quickwit hotcache optimization for individual document retrieval");
                    index
                } else {
                    debug_println!("RUST DEBUG: ⚠️ Footer metadata not available for individual document retrieval, falling back to full download");
                    
                    // Fallback: Get the full file data using Quickwit's storage abstraction for document retrieval
                    // (We need BundleDirectory for synchronous document access, not StorageDirectory)
                    let file_size = tokio::time::timeout(
                        std::time::Duration::from_secs(3),
                        index_storage.file_num_bytes(relative_path)
                    )
                    .await
                    .map_err(|_| anyhow::anyhow!("Timeout getting file size for {}", split_uri))?
                    .map_err(|e| anyhow::anyhow!("Failed to get file size for {}: {}", split_uri, e))?;

                    let split_data = tokio::time::timeout(
                        std::time::Duration::from_secs(10),
                        index_storage.get_slice(relative_path, 0..file_size as usize)
                    )
                    .await
                    .map_err(|_| anyhow::anyhow!("Timeout getting split data from {}", split_uri))?
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
                // Using adaptive cache configuration
                let batch_cache_blocks = get_batch_doc_cache_blocks();
                debug_println!("⚡ CACHE_OPTIMIZATION: Fallback path - applying adaptive doc store cache optimization - blocks: {} (batch operations)", batch_cache_blocks);
                let index_reader = index
                    .reader_builder()
                    .doc_store_cache_num_blocks(batch_cache_blocks) // ADAPTIVE CACHE OPTIMIZATION
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
                    // LRU push returns Some(evicted_value) if an entry was evicted
                    if cache.push(split_uri.clone(), searcher.clone()).is_some() {
                        SEARCHER_CACHE_EVICTIONS.fetch_add(1, Ordering::Relaxed);
                        debug_println!("RUST DEBUG: 🗑️ LRU EVICTION - cache full, evicted oldest searcher");
                    }
                }
                debug_println!("RUST DEBUG: ⏱️ 📖 Searcher caching completed [TIMING: {}ms]", caching_start.elapsed().as_millis());
                debug_println!("RUST DEBUG: ⏱️ 📖 TOTAL INDEX OPENING completed [TIMING: {}ms]", index_opening_start.elapsed().as_millis());
                
                // Retrieve the document using async method with timeout (same as batch retrieval for StorageDirectory compatibility)
                let doc_retrieval_start = std::time::Instant::now();
                let doc = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    searcher.doc_async(doc_address)
                )
                .await
                .map_err(|_| anyhow::anyhow!("Document retrieval timed out for {:?}", doc_address))?
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
    
    use std::sync::Arc;
    
    // Use the searcher context to retrieve the document from the split
    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
        let context = searcher_context.as_ref();
        // ✅ CRITICAL FIX: Use shared global runtime handle instead of context.runtime
        let runtime = crate::runtime_manager::QuickwitRuntimeManager::global().handle();
        let split_uri = &context.split_uri;
        let aws_config = &context.aws_config;
        let footer_start = context.footer_start;
        let footer_end = context.footer_end;
        let cached_index = &context.cached_index;
        
        // Enter the runtime context for async operations
        let _guard = runtime.enter();
        
        // Run async document retrieval with Quickwit optimizations
        runtime.block_on(async {
            // Parse URI and resolve storage (same as before)
            use quickwit_common::uri::Uri;
            use quickwit_config::S3StorageConfig;
            use quickwit_directories::BundleDirectory;
            use tantivy::directory::FileSlice;
            use tantivy::ReloadPolicy;
            use std::path::Path;
            
            let _uri: Uri = split_uri.parse()
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

            // ✅ BYPASS FIX #3: Use centralized storage resolver function
            debug_println!("✅ BYPASS_FIXED: Using get_configured_storage_resolver() for cache sharing [FIX #3]");
            debug_println!("   📍 Location: split_searcher_replacement.rs:1365 (actual storage path)");
            let storage_resolver = get_configured_storage_resolver(Some(s3_config.clone()), None);
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
                has_footer_metadata(footer_start, footer_end), is_remote_split(split_uri));
            let index = if has_footer_metadata(footer_start, footer_end) && is_remote_split(split_uri) {
                debug_println!("RUST DEBUG: 🚀 Using Quickwit optimized path with hotcache (footer: {}..{})", footer_start, footer_end);

                // Use cached index to eliminate repeated open_index_with_caches calls
                let index = cached_index.as_ref().clone();
                debug_println!("RUST DEBUG: ✅ Successfully reused cached index");
                index
            } else {
                debug_println!("RUST DEBUG: ⚠️ Footer metadata not available, falling back to full download");
                
                // Fallback: Get the full file data (original behavior for missing metadata)
                let file_size = tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    actual_storage.file_num_bytes(relative_path)
                )
                .await
                .map_err(|_| anyhow::anyhow!("Timeout getting file size for {}", split_uri))?
                .map_err(|e| anyhow::anyhow!("Failed to get file size for {}: {}", split_uri, e))?;

                let split_data = tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    actual_storage.get_slice(relative_path, 0..file_size as usize)
                )
                .await
                .map_err(|_| anyhow::anyhow!("Timeout getting split data from {}", split_uri))?
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
            // Using global cache configuration constant for individual document retrieval
            debug_println!("⚡ CACHE_OPTIMIZATION: Individual retrieval - applying doc store cache optimization - blocks: {} (single document)", SINGLE_DOC_CACHE_BLOCKS);
            let index_reader = index
                .reader_builder()
                .doc_store_cache_num_blocks(SINGLE_DOC_CACHE_BLOCKS) // QUICKWIT OPTIMIZATION
                .reload_policy(ReloadPolicy::Manual)
                .try_into()
                .map_err(|e| anyhow::anyhow!("Failed to create index reader: {}", e))?;
            
            let tantivy_searcher = index_reader.searcher();
            
            // Use doc_async like Quickwit does (fetch_docs.rs line 205-207) - QUICKWIT OPTIMIZATION
            let doc: tantivy::schema::TantivyDocument = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                tantivy_searcher.doc_async(doc_address)
            )
            .await
            .map_err(|_| anyhow::anyhow!("Document retrieval timed out for {:?}", doc_address))?
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
    use crate::simple_batch_optimization::{SimpleBatchOptimizer, SimpleBatchConfig};

    use std::sync::Arc;

    // TRACE: Entry point
    debug_println!("🔍 TRACE: retrieve_documents_batch_from_split_optimized called with {} docs", doc_addresses.len());

    // Sort by DocAddress for cache locality (following Quickwit pattern)
    doc_addresses.sort();

    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
        let context = searcher_context.as_ref();
        // ✅ CRITICAL FIX: Use shared global runtime handle instead of context.runtime
        let runtime = crate::runtime_manager::QuickwitRuntimeManager::global().handle();
        let split_uri = &context.split_uri;
        let footer_start = context.footer_start;
        let footer_end = context.footer_end;
        let storage_resolver = &context.cached_storage;
        let cached_index = &context.cached_index;
        // 🚀 BATCH OPTIMIZATION FIX: Access cache and file offsets
        let byte_range_cache = &context.byte_range_cache;
        let bundle_file_offsets = &context.bundle_file_offsets;

        let _guard = runtime.enter();

        // Use block_in_place to run async code synchronously (Quickwit pattern) with timeout
        tokio::task::block_in_place(|| {
            // Add timeout to prevent hanging during runtime shutdown
            let timeout_duration = std::time::Duration::from_secs(5);
            runtime.block_on(tokio::time::timeout(timeout_duration, async {
                // 🚀 BATCH OPTIMIZATION FIX: Use cached_searcher from context directly
                // instead of looking up from the LRU cache (which may not be populated)
                let cached_searcher = &context.cached_searcher;
                debug_println!("🔍 TRACE: Using cached_searcher from CachedSearcherContext");

                // The context always has a cached searcher, so use it directly
                {
                    debug_println!("RUST DEBUG: ✅ BATCH CACHE HIT: Using cached searcher for batch processing");
                    debug_println!("🔍 TRACE: Entered batch processing path with {} docs", doc_addresses.len());
                    debug_println!("🔍 TRACE: byte_range_cache is_some = {}", byte_range_cache.is_some());
                    debug_println!("🔍 TRACE: bundle_file_offsets len = {}", bundle_file_offsets.len());
                    let schema = cached_searcher.schema(); // Get schema from cached searcher

                    // 🚀 BATCH OPTIMIZATION: Prefetch consolidated byte ranges for better S3 performance
                    let optimizer = SimpleBatchOptimizer::new(SimpleBatchConfig::default());

                    if optimizer.should_optimize(doc_addresses.len()) {
                        debug_println!("🚀 BATCH_OPT: Starting range consolidation for {} documents", doc_addresses.len());
                        debug_println!("🔍 TRACE: Should optimize = true");

                        match optimizer.consolidate_ranges(&doc_addresses, &cached_searcher) {
                            Ok(ranges) => {
                                let num_segments = ranges.iter().map(|r| r.file_path.clone()).collect::<std::collections::HashSet<_>>().len();
                                debug_println!("🚀 BATCH_OPT: Consolidated {} docs → {} ranges across {} segments",
                                    doc_addresses.len(), ranges.len(), num_segments);
                                debug_println!("🔍 TRACE: Consolidated to {} ranges", ranges.len());
                                if *crate::debug::DEBUG_ENABLED {
                                    for (i, range) in ranges.iter().enumerate() {
                                        debug_println!("🔍 TRACE:   Range {}: {:?}, {}..{}", i, range.file_path, range.start, range.end);
                                    }
                                }

                                // 🚀 BATCH OPTIMIZATION FIX: Use prefetch_ranges_with_cache to populate the correct cache
                                let prefetch_result = if let Some(cache) = byte_range_cache {
                                    debug_println!("🚀 BATCH_OPT: Using prefetch_ranges_with_cache (OPTIMIZED)");
                                    debug_println!("🔍 TRACE: Calling prefetch_ranges_with_cache with {} bundle offsets", bundle_file_offsets.len());
                                    crate::simple_batch_optimization::prefetch_ranges_with_cache(
                                        ranges.clone(),
                                        storage_resolver.clone(),
                                        split_uri,
                                        cache,
                                        bundle_file_offsets,
                                    ).await
                                } else {
                                    debug_println!("⚠️ BATCH_OPT: ByteRangeCache not available, using fallback prefetch");
                                    debug_println!("🔍 TRACE: FALLBACK - ByteRangeCache is None!");
                                    optimizer.prefetch_ranges(ranges.clone(), storage_resolver.clone(), split_uri).await
                                };

                                match prefetch_result {
                                    Ok(stats) => {
                                        debug_println!("🚀 BATCH_OPT SUCCESS: Prefetched {} ranges, {} bytes in {}ms",
                                            stats.ranges_fetched, stats.bytes_fetched, stats.duration_ms);
                                        debug_println!("   📊 Consolidation ratio: {:.1}x (docs/ranges)",
                                            stats.consolidation_ratio(doc_addresses.len()));

                                        // Record metrics (estimate bytes wasted from gaps)
                                        let bytes_wasted = 0; // TODO: Calculate actual gap bytes
                                        crate::split_cache_manager::record_batch_metrics(
                                            None, // cache_name not available in this context
                                            doc_addresses.len(),
                                            &stats,
                                            num_segments,
                                            bytes_wasted,
                                        );
                                    }
                                    Err(e) => {
                                        debug_println!("⚠️ BATCH_OPT: Prefetch failed (continuing with normal retrieval): {}", e);
                                        // Non-fatal: continue with normal doc_async which will fetch on-demand
                                    }
                                }
                            }
                            Err(e) => {
                                debug_println!("⚠️ BATCH_OPT: Range consolidation failed (continuing with normal retrieval): {}", e);
                                // Non-fatal: continue with normal doc_async
                            }
                        }
                    } else {
                        debug_println!("⏭️ BATCH_OPT: Skipping optimization (only {} docs, threshold: {})",
                            doc_addresses.len(), optimizer.config.min_docs_for_optimization);
                    }

                    // ✅ QUICKWIT CONCURRENT PATTERN: Use cached searcher with concurrency
                    // Using global cache configuration for concurrent batch processing
                    // Note: doc_async calls will now benefit from prefetched ByteRangeCache

                    let doc_futures = doc_addresses.into_iter().map(|doc_addr| {
                        let moved_searcher = cached_searcher.clone(); // Reuse cached searcher
                        let moved_schema = schema.clone();
                        async move {
                            // Add timeout to individual doc_async calls to prevent hanging
                            let doc: tantivy::schema::TantivyDocument = tokio::time::timeout(
                                std::time::Duration::from_secs(5),
                                moved_searcher.doc_async(doc_addr)
                            )
                            .await
                            .map_err(|_| anyhow::anyhow!("Document retrieval timed out for {:?}", doc_addr))?
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
                        .buffer_unordered(BASE_CONCURRENT_REQUESTS) // Keep base concurrency for stream processing
                        .try_collect::<Vec<_>>()
                        .await
                        .map_err(|e| anyhow::anyhow!("Cached searcher batch retrieval failed: {}", e))?;

                    // 🚀 BATCH OPTIMIZATION FIX: Always return here - using context.cached_searcher
                    return Ok::<Vec<jobject>, anyhow::Error>(doc_ptrs);
                }
                // 🚀 BATCH OPTIMIZATION FIX: Fallback code below is now unreachable
                // The code is kept for compatibility but should be removed in cleanup
                #[allow(unreachable_code)]
                {
                // Use pre-created storage resolver from searcher context
                debug_println!("✅ QUICKWIT_LIFECYCLE: Using cached storage from searcher context (Quickwit pattern)");
                debug_println!("✅ CACHED_STORAGE_USED: Storage at address {:p} (Quickwit lifecycle)", Arc::as_ptr(storage_resolver));
                let index_storage = storage_resolver.clone();
                
                // Extract just the filename as the relative path (same as individual retrieval)
                let relative_path = if let Some(last_slash_pos) = split_uri.rfind('/') {
                    std::path::Path::new(&split_uri[last_slash_pos + 1..])
                } else {
                    std::path::Path::new(split_uri)
                };
                
                // 🚀 BATCH OPTIMIZATION: Use Quickwit's optimized path when footer metadata is available for remote splits
                debug_println!("RUST DEBUG: Checking batch optimization conditions - footer_metadata: {}, is_remote: {}", 
                    has_footer_metadata(footer_start, footer_end), is_remote_split(split_uri));
                let mut index = if has_footer_metadata(footer_start, footer_end) && is_remote_split(split_uri) {
                    debug_println!("RUST DEBUG: 🚀 Using Quickwit optimized path for batch retrieval (footer: {}..{})", footer_start, footer_end);

                    // Use cached index to eliminate repeated open_index_with_caches calls
                    let index = cached_index.as_ref().clone();
                    debug_println!("🔥 INDEX CACHED: Reusing cached index for batch operations instead of expensive open_index_with_caches call");

                    debug_println!("RUST DEBUG: ✅ Successfully reused cached index for batch retrieval");
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
                // Using adaptive cache configuration for batch operations
                let batch_cache_blocks = get_batch_doc_cache_blocks();
                debug_println!("⚡ CACHE_OPTIMIZATION: Batch retrieval fallback - applying adaptive doc store cache optimization - blocks: {} (batch operations)", batch_cache_blocks);
                let index_reader = index
                    .reader_builder()
                    .doc_store_cache_num_blocks(batch_cache_blocks) // ADAPTIVE CACHE OPTIMIZATION
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
                    // LRU push returns Some(evicted_value) if an entry was evicted
                    if cache.push(split_uri.clone(), tantivy_searcher.clone()).is_some() {
                        SEARCHER_CACHE_EVICTIONS.fetch_add(1, Ordering::Relaxed);
                        debug_println!("RUST DEBUG: 🗑️ LRU EVICTION - cache full, evicted oldest searcher");
                    }
                    debug_println!("RUST DEBUG: ✅ CACHED NEW SEARCHER: Stored searcher for future batch operations");
                }

                // 🚀 BATCH OPTIMIZATION: Prefetch consolidated byte ranges for better S3 performance
                let optimizer = SimpleBatchOptimizer::new(SimpleBatchConfig::default());

                if optimizer.should_optimize(doc_addresses.len()) {
                    debug_println!("🚀 BATCH_OPT: Starting range consolidation for {} documents", doc_addresses.len());

                    match optimizer.consolidate_ranges(&doc_addresses, &tantivy_searcher) {
                        Ok(ranges) => {
                            debug_println!("🚀 BATCH_OPT: Consolidated {} docs → {} ranges", doc_addresses.len(), ranges.len());

                            // Prefetch the consolidated ranges to populate ByteRangeCache
                            match optimizer.prefetch_ranges(ranges.clone(), index_storage.clone(), split_uri).await {
                                Ok(stats) => {
                                    debug_println!("🚀 BATCH_OPT SUCCESS: Prefetched {} ranges, {} bytes in {}ms",
                                        stats.ranges_fetched, stats.bytes_fetched, stats.duration_ms);
                                    debug_println!("   📊 Consolidation ratio: {:.1}x (docs/ranges)",
                                        stats.consolidation_ratio(doc_addresses.len()));

                                    // Record metrics (estimate bytes wasted from gaps)
                                    let num_segments = ranges.iter().map(|r| r.file_path.clone()).collect::<std::collections::HashSet<_>>().len();
                                    let bytes_wasted = 0; // TODO: Calculate actual gap bytes
                                    crate::split_cache_manager::record_batch_metrics(
                                        None, // cache_name not available in this context
                                        doc_addresses.len(),
                                        &stats,
                                        num_segments,
                                        bytes_wasted,
                                    );
                                }
                                Err(e) => {
                                    debug_println!("⚠️ BATCH_OPT: Prefetch failed (continuing with normal retrieval): {}", e);
                                    // Non-fatal: continue with normal doc_async which will fetch on-demand
                                }
                            }
                        }
                        Err(e) => {
                            debug_println!("⚠️ BATCH_OPT: Range consolidation failed (continuing with normal retrieval): {}", e);
                            // Non-fatal: continue with normal doc_async
                        }
                    }
                } else {
                    debug_println!("⏭️ BATCH_OPT: Skipping optimization (only {} docs, threshold: {})",
                        doc_addresses.len(), optimizer.config.min_docs_for_optimization);
                }

                // ✅ QUICKWIT CONCURRENT PATTERN: Use concurrent document retrieval (fetch_docs.rs line 200-258)
                // Note: doc_async calls will now benefit from prefetched ByteRangeCache

                // Create async futures for concurrent document retrieval (like Quickwit)
                let doc_futures = doc_addresses.into_iter().map(|doc_addr| {
                    let moved_searcher = tantivy_searcher.clone(); // Clone Arc for concurrent access
                    let moved_schema = schema.clone(); // Clone schema for each future
                    async move {
                        // Use doc_async like Quickwit with timeout - QUICKWIT OPTIMIZATION (fetch_docs.rs line 205-207)
                        let doc: tantivy::schema::TantivyDocument = tokio::time::timeout(
                            std::time::Duration::from_secs(5),
                            moved_searcher.doc_async(doc_addr)
                        )
                        .await
                        .map_err(|_| anyhow::anyhow!("Document retrieval timed out for {:?}", doc_addr))?
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

                // ✅ QUICKWIT CONCURRENT EXECUTION: Process up to BASE_CONCURRENT_REQUESTS simultaneously
                use futures::stream::{StreamExt, TryStreamExt};
                let doc_ptrs: Vec<jobject> = futures::stream::iter(doc_futures)
                    .buffer_unordered(BASE_CONCURRENT_REQUESTS) // Quickwit's concurrent processing pattern
                    .try_collect::<Vec<_>>()
                    .await
                    .map_err(|e| anyhow::anyhow!("Concurrent document retrieval failed: {}", e))?;

                Ok::<Vec<jobject>, anyhow::Error>(doc_ptrs)
                } // Close the #[allow(unreachable_code)] block
            }))
            .map_err(|timeout_err| {
                debug_println!("🕐 TIMEOUT: Document retrieval timed out after 10 seconds: {}", timeout_err);
                anyhow::anyhow!("Document retrieval timed out after 10 seconds - likely due to runtime shutdown")
            })?
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

/// Async-first replacement for Java_io_indextables_tantivy4java_split_SplitSearcher_docNative
/// Implements document retrieval using Quickwit's async approach without deadlocks
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_docNative(
    env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    segment_ord: jint,
    doc_id: jint,
) -> jobject {
    debug_println!("🔥🔥🔥 JNI DEBUG: docNative called - ptr:{}, seg:{}, doc:{}", searcher_ptr, segment_ord, doc_id);
    debug_println!("🚀 ASYNC_JNI: docNative called with async-first architecture");

    // Add this line to verify the method is actually being called
    // Use simplified async pattern that returns thread-safe types
    // Note: env cannot be moved into async block due to thread safety
    match block_on_operation(async move {
        perform_doc_retrieval_async_impl_thread_safe(searcher_ptr, segment_ord as u32, doc_id as u32).await
    }) {
        Ok(document_ptr) => {
            debug_println!("🔥 JNI DEBUG: Document retrieval successful, creating Java Document object from pointer: {}", document_ptr);

            // Check if the pointer is valid (non-zero)
            if document_ptr == 0 {
                debug_println!("🔥 JNI DEBUG: ERROR - Document pointer is null/zero!");
                std::ptr::null_mut()
            } else {
                debug_println!("🔥 JNI DEBUG: Document pointer is valid ({}), proceeding with Java object creation", document_ptr);

                // Create Java Document object properly using JNI
                let mut env_mut = env;
                debug_println!("🔥 JNI DEBUG: About to call create_java_document_object...");
                match create_java_document_object(&mut env_mut, document_ptr) {
                    Ok(java_doc_obj) => {
                        debug_println!("🔥 JNI DEBUG: Successfully created Java Document object, returning: {:?}", java_doc_obj);
                        java_doc_obj
                    },
                    Err(e) => {
                        debug_println!("🔥 JNI DEBUG: Failed to create Java Document object: {}", e);
                        crate::common::to_java_exception(&mut env_mut, &e);
                        std::ptr::null_mut()
                    }
                }
            }
        },
        Err(e) => {
            debug_println!("🔥 JNI DEBUG: Document retrieval failed: {}", e);
            debug_println!("❌ ASYNC_JNI: Document retrieval operation failed: {}", e);
            std::ptr::null_mut()
        }
    }
}

/// Create a Java Document object from a native document pointer
/// This properly converts the Rust DocumentWrapper pointer to a Java Document object
fn create_java_document_object(env: &mut JNIEnv, document_ptr: jlong) -> anyhow::Result<jobject> {
    debug_println!("🔧 JNI_CONVERT: Creating Java Document object from pointer: {}", document_ptr);

    // Find the Document class
    let document_class = env.find_class("io/indextables/tantivy4java/core/Document")
        .map_err(|e| anyhow::anyhow!("Failed to find Document class: {}", e))?;

    // Create a new Document object with the pointer constructor: Document(long nativePtr)
    let document_obj = env.new_object(
        &document_class,
        "(J)V", // Constructor signature: takes a long (J) and returns void (V)
        &[jni::objects::JValue::Long(document_ptr)]
    ).map_err(|e| anyhow::anyhow!("Failed to create Document object: {}", e))?;

    debug_println!("🔧 JNI_CONVERT: Successfully created Java Document object");
    Ok(document_obj.into_raw())
}
