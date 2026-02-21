// single_doc_retrieval.rs - Single document retrieval functions
// Extracted from document_retrieval.rs during refactoring

use std::sync::atomic::Ordering;
use std::sync::Arc;

use jni::sys::jlong;
use quickwit_directories::BundleDirectory;
use quickwit_indexing::open_index;
use quickwit_query::get_quickwit_fastfield_normalizer_manager;
use tantivy::directory::{DirectoryClone, FileSlice};
use tantivy::ReloadPolicy;

use crate::debug_println;
use crate::global_cache::get_configured_storage_resolver;
use crate::split_searcher::cache_config::{get_batch_doc_cache_blocks, SINGLE_DOC_CACHE_BLOCKS};
use crate::split_searcher::searcher_cache::{
    get_searcher_cache, has_footer_metadata, is_remote_split, search_thread_pool,
    SEARCHER_CACHE_EVICTIONS, SEARCHER_CACHE_HITS, SEARCHER_CACHE_MISSES,
};
use crate::split_searcher::types::CachedSearcherContext;
use crate::standalone_searcher::resolve_storage_for_split;
use crate::utils::with_arc_safe;

/// Simple but effective searcher caching for single document retrieval
/// Uses the same optimizations as our batch method but caches searchers for reuse
pub fn retrieve_document_from_split_optimized(
    searcher_ptr: jlong,
    doc_address: tantivy::DocAddress,
) -> Result<(tantivy::schema::TantivyDocument, tantivy::schema::Schema), anyhow::Error> {
    let function_start = std::time::Instant::now();
    debug_println!("RUST DEBUG: ⏱️ 🚀 retrieve_document_from_split_optimized ENTRY [TIMING START]");

    // Get split URI from the searcher context
    let uri_extraction_start = std::time::Instant::now();
    let split_uri = with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
        let context = searcher_context.as_ref();
        context.split_uri.clone()
    })
    .ok_or_else(|| anyhow::anyhow!("Invalid searcher context"))?;
    debug_println!(
        "RUST DEBUG: ⏱️ Split URI extraction completed [TIMING: {}ms]",
        uri_extraction_start.elapsed().as_millis()
    );

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

    debug_println!(
        "RUST DEBUG: ⏱️ Cache lookup completed [TIMING: {}ms] - cache_hit: {}",
        cache_check_start.elapsed().as_millis(),
        cached_searcher.is_some()
    );

    if let Some(searcher) = cached_searcher {
        // Use cached searcher - very fast path (cache hit)
        // IMPORTANT: Use async method for StorageDirectory compatibility
        let cache_hit_start = std::time::Instant::now();
        debug_println!(
            "RUST DEBUG: ⏱️ 🎯 CACHE HIT - using cached searcher for document retrieval"
        );

        // Extract the runtime and use async document retrieval
        let doc_and_schema =
            with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
                let _context = searcher_context.as_ref();

                // ✅ CRITICAL FIX: Use shared global runtime instead of context.runtime
                tokio::task::block_in_place(|| {
                    crate::runtime_manager::QuickwitRuntimeManager::global()
                        .handle()
                        .block_on(async {
                            // Validate segment ordinal to prevent index-out-of-bounds panic
                            let num_segments = searcher.segment_readers().len();
                            if doc_address.segment_ord as usize >= num_segments {
                                return Err(anyhow::anyhow!(
                                    "Invalid segment ordinal {}: index has {} segment(s)",
                                    doc_address.segment_ord, num_segments
                                ));
                            }
                            let doc = tokio::time::timeout(
                                std::time::Duration::from_secs(5),
                                searcher.doc_async(doc_address),
                            )
                            .await
                            .map_err(|_| {
                                anyhow::anyhow!(
                                    "Document retrieval timed out for {:?}",
                                    doc_address
                                )
                            })?
                            .map_err(|e| anyhow::anyhow!("Failed to retrieve document: {}", e))?;
                            let schema = searcher.schema();
                            Ok::<
                                (tantivy::schema::TantivyDocument, tantivy::schema::Schema),
                                anyhow::Error,
                            >((doc, schema.clone()))
                        })
                })
            })
            .ok_or_else(|| anyhow::anyhow!("Invalid searcher context for cached retrieval"))?;

        match doc_and_schema {
            Ok((doc, schema)) => {
                debug_println!(
                    "RUST DEBUG: ⏱️ ✅ CACHE HIT document retrieval completed [TIMING: {}ms] [TOTAL: {}ms]",
                    cache_hit_start.elapsed().as_millis(),
                    function_start.elapsed().as_millis()
                );
                return Ok((doc, schema));
            }
            Err(e) => {
                debug_println!(
                    "RUST DEBUG: ⏱️ ❌ CACHE HIT failed, falling through to cache miss: {}",
                    e
                );
                // Fall through to cache miss path
            }
        }
    }

    // Cache miss - create searcher using the same optimizations as our batch method
    debug_println!(
        "RUST DEBUG: ⏱️ ⚠️ CACHE MISS - creating new searcher (EXPENSIVE OPERATION)"
    );
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
            crate::runtime_manager::QuickwitRuntimeManager::global()
                .handle()
                .block_on(async {
                    // Use pre-created storage resolver from searcher context
                    debug_println!(
                        "✅ QUICKWIT_LIFECYCLE: Using cached storage from searcher context (Quickwit pattern)"
                    );
                    debug_println!(
                        "✅ CACHED_STORAGE_USED: Storage at address {:p} (Quickwit lifecycle)",
                        Arc::as_ptr(storage_resolver)
                    );
                    let index_storage = storage_resolver.clone();

                    // Manual index opening with Quickwit caching components
                    // (open_index_with_caches expects Quickwit's native split format, but we use bundle format)
                    let index_opening_start = std::time::Instant::now();
                    debug_println!(
                        "RUST DEBUG: ⏱️ 📖 INDEX OPENING - Starting file download and index creation"
                    );

                    let relative_path = if let Some(last_slash_pos) = split_uri.rfind('/') {
                        std::path::Path::new(&split_uri[last_slash_pos + 1..])
                    } else {
                        std::path::Path::new(split_uri)
                    };

                    // 🚀 INDIVIDUAL DOC OPTIMIZATION: Use same hotcache optimization as batch retrieval
                    let mut index =
                        if has_footer_metadata(footer_start, footer_end) && is_remote_split(split_uri) {
                            debug_println!(
                            "RUST DEBUG: 🚀 Using Quickwit optimized path for individual document retrieval (footer: {}..{})",
                            footer_start, footer_end
                        );

                            // Use cached index to eliminate repeated open_index_with_caches calls
                            let index_creation_start = std::time::Instant::now();
                            let index = cached_index.as_ref().clone();
                            debug_println!(
                            "🔥 INDEX CACHED: Reusing cached index instead of expensive open_index_with_caches call"
                        );

                            debug_println!(
                            "RUST DEBUG: ⏱️ 📖 Quickwit hotcache index creation completed [TIMING: {}ms]",
                            index_creation_start.elapsed().as_millis()
                        );
                            debug_println!(
                            "RUST DEBUG: ✅ Successfully opened index with Quickwit hotcache optimization for individual document retrieval"
                        );
                            index
                        } else {
                            debug_println!(
                            "RUST DEBUG: ⚠️ Footer metadata not available for individual document retrieval, falling back to full download"
                        );

                            // Fallback: Get the full file data using Quickwit's storage abstraction for document retrieval
                            // (We need BundleDirectory for synchronous document access, not StorageDirectory)
                            let file_size = tokio::time::timeout(
                                std::time::Duration::from_secs(3),
                                index_storage.file_num_bytes(relative_path),
                            )
                            .await
                            .map_err(|_| {
                                anyhow::anyhow!("Timeout getting file size for {}", split_uri)
                            })?
                            .map_err(|e| {
                                anyhow::anyhow!("Failed to get file size for {}: {}", split_uri, e)
                            })?;

                            let split_data = tokio::time::timeout(
                                std::time::Duration::from_secs(10),
                                index_storage.get_slice(relative_path, 0..file_size as usize),
                            )
                            .await
                            .map_err(|_| {
                                anyhow::anyhow!("Timeout getting split data from {}", split_uri)
                            })?
                            .map_err(|e| {
                                anyhow::anyhow!("Failed to get split data from {}: {}", split_uri, e)
                            })?;

                            let split_file_slice =
                                FileSlice::new(std::sync::Arc::new(split_data));
                            let bundle_directory =
                                BundleDirectory::open_split(split_file_slice).map_err(|e| {
                                    anyhow::anyhow!(
                                        "Failed to open bundle directory {}: {}",
                                        split_uri,
                                        e
                                    )
                                })?;

                            let index_creation_start = std::time::Instant::now();
                            // ✅ QUICKWIT NATIVE: Use Quickwit's native index opening instead of direct tantivy
                            let index = open_index(
                                bundle_directory.box_clone(),
                                get_quickwit_fastfield_normalizer_manager().tantivy_manager(),
                            )
                            .map_err(|e| {
                                anyhow::anyhow!(
                                    "Failed to open index from bundle {}: {}",
                                    split_uri,
                                    e
                                )
                            })?;
                            debug_println!(
                            "RUST DEBUG: ⏱️ 📖 QUICKWIT NATIVE: BundleDirectory index creation completed [TIMING: {}ms]",
                            index_creation_start.elapsed().as_millis()
                        );
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
                    debug_println!(
                    "⚡ CACHE_OPTIMIZATION: Fallback path - applying adaptive doc store cache optimization - blocks: {} (batch operations)",
                    batch_cache_blocks
                );
                    let index_reader = index
                        .reader_builder()
                        .doc_store_cache_num_blocks(batch_cache_blocks) // ADAPTIVE CACHE OPTIMIZATION
                        .reload_policy(ReloadPolicy::Manual)
                        .try_into()
                        .map_err(|e| anyhow::anyhow!("Failed to create index reader: {}", e))?;

                    let searcher = Arc::new(index_reader.searcher());
                    debug_println!(
                        "RUST DEBUG: ⏱️ 📖 Searcher creation completed [TIMING: {}ms]",
                        searcher_creation_start.elapsed().as_millis()
                    );

                    // Cache the searcher for future single document retrievals
                    let caching_start = std::time::Instant::now();
                    {
                        let searcher_cache = get_searcher_cache();
                        let mut cache = searcher_cache.lock().unwrap();
                        // LRU push returns Some(evicted_value) if an entry was evicted
                        if cache.push(split_uri.clone(), searcher.clone()).is_some() {
                            SEARCHER_CACHE_EVICTIONS.fetch_add(1, Ordering::Relaxed);
                            debug_println!(
                                "RUST DEBUG: 🗑️ LRU EVICTION - cache full, evicted oldest searcher"
                            );
                        }
                    }
                    debug_println!(
                        "RUST DEBUG: ⏱️ 📖 Searcher caching completed [TIMING: {}ms]",
                        caching_start.elapsed().as_millis()
                    );
                    debug_println!(
                        "RUST DEBUG: ⏱️ 📖 TOTAL INDEX OPENING completed [TIMING: {}ms]",
                        index_opening_start.elapsed().as_millis()
                    );

                    // Validate segment ordinal to prevent index-out-of-bounds panic
                    let num_segments = searcher.segment_readers().len();
                    if doc_address.segment_ord as usize >= num_segments {
                        return Err(anyhow::anyhow!(
                            "Invalid segment ordinal {}: index has {} segment(s)",
                            doc_address.segment_ord, num_segments
                        ));
                    }

                    // Retrieve the document using async method with timeout (same as batch retrieval for StorageDirectory compatibility)
                    let doc_retrieval_start = std::time::Instant::now();
                    let doc = tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        searcher.doc_async(doc_address),
                    )
                    .await
                    .map_err(|_| {
                        anyhow::anyhow!("Document retrieval timed out for {:?}", doc_address)
                    })?
                    .map_err(|e| anyhow::anyhow!("Failed to retrieve document: {}", e))?;
                    let schema = index.schema();
                    debug_println!(
                        "RUST DEBUG: ⏱️ 📖 Document retrieval completed [TIMING: {}ms]",
                        doc_retrieval_start.elapsed().as_millis()
                    );

                    Ok::<
                        (tantivy::schema::TantivyDocument, tantivy::schema::Schema),
                        anyhow::Error,
                    >((doc, schema.clone()))
                })
        })
    });

    match result {
        Some(Ok(doc_and_schema)) => {
            debug_println!(
                "RUST DEBUG: ⏱️ ✅ CACHE MISS document retrieval completed [TOTAL: {}ms]",
                function_start.elapsed().as_millis()
            );
            Ok(doc_and_schema)
        }
        Some(Err(e)) => {
            debug_println!(
                "RUST DEBUG: ⏱️ ❌ CACHE MISS failed [TOTAL: {}ms] - Error: {}",
                function_start.elapsed().as_millis(),
                e
            );
            Err(e)
        }
        None => {
            debug_println!(
                "RUST DEBUG: ⏱️ ❌ Invalid searcher context [TOTAL: {}ms]",
                function_start.elapsed().as_millis()
            );
            Err(anyhow::anyhow!(
                "Searcher context not found for pointer {}",
                searcher_ptr
            ))
        }
    }
}

/// Helper function to retrieve a single document from a split
/// Legacy implementation - improved with Quickwit optimizations: doc_async and doc_store_cache_num_blocks
pub fn retrieve_document_from_split(
    searcher_ptr: jlong,
    doc_address: tantivy::DocAddress,
) -> Result<(tantivy::schema::TantivyDocument, tantivy::schema::Schema), anyhow::Error> {
    use quickwit_config::S3StorageConfig;
    use std::path::Path;

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

            let _uri: Uri = split_uri
                .parse()
                .map_err(|e| anyhow::anyhow!("Failed to parse split URI {}: {}", split_uri, e))?;

            // Create S3 storage configuration with credentials from Java config
            let s3_config = S3StorageConfig {
                flavor: None,
                access_key_id: aws_config.get("access_key").cloned(),
                secret_access_key: aws_config.get("secret_key").cloned(),
                session_token: aws_config.get("session_token").cloned(),
                region: aws_config.get("region").cloned(),
                endpoint: aws_config.get("endpoint").cloned(),
                force_path_style_access: aws_config
                    .get("path_style_access")
                    .map_or(false, |v| v == "true"),
                disable_multi_object_delete: false,
                disable_multipart_upload: false,
            };

            // ✅ BYPASS FIX #3: Use centralized storage resolver function
            debug_println!(
                "✅ BYPASS_FIXED: Using get_configured_storage_resolver() for cache sharing [FIX #3]"
            );
            debug_println!(
                "   📍 Location: split_searcher_replacement.rs:1365 (actual storage path)"
            );
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
            debug_println!(
                "RUST DEBUG: Checking optimization conditions - footer_metadata: {}, is_remote: {}",
                has_footer_metadata(footer_start, footer_end),
                is_remote_split(split_uri)
            );
            let index =
                if has_footer_metadata(footer_start, footer_end) && is_remote_split(split_uri) {
                    debug_println!(
                        "RUST DEBUG: 🚀 Using Quickwit optimized path with hotcache (footer: {}..{})",
                        footer_start,
                        footer_end
                    );

                    // Use cached index to eliminate repeated open_index_with_caches calls
                    let index = cached_index.as_ref().clone();
                    debug_println!("RUST DEBUG: ✅ Successfully reused cached index");
                    index
                } else {
                    debug_println!(
                        "RUST DEBUG: ⚠️ Footer metadata not available, falling back to full download"
                    );

                    // Fallback: Get the full file data (original behavior for missing metadata)
                    let file_size = tokio::time::timeout(
                        std::time::Duration::from_secs(3),
                        actual_storage.file_num_bytes(relative_path),
                    )
                    .await
                    .map_err(|_| anyhow::anyhow!("Timeout getting file size for {}", split_uri))?
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to get file size for {}: {}", split_uri, e)
                    })?;

                    let split_data = tokio::time::timeout(
                        std::time::Duration::from_secs(10),
                        actual_storage.get_slice(relative_path, 0..file_size as usize),
                    )
                    .await
                    .map_err(|_| anyhow::anyhow!("Timeout getting split data from {}", split_uri))?
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to get split data from {}: {}", split_uri, e)
                    })?;

                    debug_println!(
                        "RUST DEBUG: ⚠️ Downloaded full split file: {} bytes",
                        split_data.len()
                    );

                    // Open the bundle directory from the split data
                    let split_file_slice = FileSlice::new(std::sync::Arc::new(split_data));
                    let bundle_directory =
                        BundleDirectory::open_split(split_file_slice).map_err(|e| {
                            anyhow::anyhow!("Failed to open bundle directory {}: {}", split_uri, e)
                        })?;

                    // ✅ QUICKWIT NATIVE: Extract the index from the bundle directory using Quickwit's native function
                    open_index(
                        bundle_directory.box_clone(),
                        get_quickwit_fastfield_normalizer_manager().tantivy_manager(),
                    )
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to open index from bundle {}: {}", split_uri, e)
                    })?
                };

            // Create index reader using Quickwit's optimizations (from fetch_docs.rs line 187-192)
            // Using global cache configuration constant for individual document retrieval
            debug_println!(
                "⚡ CACHE_OPTIMIZATION: Individual retrieval - applying doc store cache optimization - blocks: {} (single document)",
                SINGLE_DOC_CACHE_BLOCKS
            );
            let index_reader = index
                .reader_builder()
                .doc_store_cache_num_blocks(SINGLE_DOC_CACHE_BLOCKS) // QUICKWIT OPTIMIZATION
                .reload_policy(ReloadPolicy::Manual)
                .try_into()
                .map_err(|e| anyhow::anyhow!("Failed to create index reader: {}", e))?;

            let tantivy_searcher = index_reader.searcher();

            // Validate segment ordinal to prevent index-out-of-bounds panic
            let num_segments = tantivy_searcher.segment_readers().len();
            if doc_address.segment_ord as usize >= num_segments {
                return Err(anyhow::anyhow!(
                    "Invalid segment ordinal {}: index has {} segment(s)",
                    doc_address.segment_ord, num_segments
                ));
            }

            // Use doc_async like Quickwit does (fetch_docs.rs line 205-207) - QUICKWIT OPTIMIZATION
            let doc: tantivy::schema::TantivyDocument = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                tantivy_searcher.doc_async(doc_address),
            )
            .await
            .map_err(|_| anyhow::anyhow!("Document retrieval timed out for {:?}", doc_address))?
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to retrieve document at address {:?}: {}",
                    doc_address,
                    e
                )
            })?;

            // Return the document and schema for processing
            Ok::<(tantivy::schema::TantivyDocument, tantivy::schema::Schema), anyhow::Error>((
                doc,
                index.schema(),
            ))
        })
    });

    match result {
        Some(Ok(result)) => Ok(result),
        Some(Err(e)) => Err(e),
        None => Err(anyhow::anyhow!(
            "Searcher context not found for pointer {}",
            searcher_ptr
        )),
    }
}
