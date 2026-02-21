// batch_doc_retrieval.rs - Batch document retrieval functions
// Extracted from document_retrieval.rs during refactoring

use std::sync::atomic::Ordering;
use std::sync::Arc;

use jni::sys::{jlong, jobject};
use quickwit_directories::BundleDirectory;
use quickwit_indexing::open_index;
use quickwit_query::get_quickwit_fastfield_normalizer_manager;
use tantivy::directory::DirectoryClone;
use tantivy::ReloadPolicy;

use crate::batch_retrieval::simple::{SimpleBatchConfig, SimpleBatchOptimizer};
use crate::debug_println;
use crate::document::{DocumentWrapper, RetrievedDocument};
use crate::split_searcher::cache_config::{get_batch_doc_cache_blocks, BASE_CONCURRENT_REQUESTS};
use crate::split_searcher::searcher_cache::{
    get_searcher_cache, has_footer_metadata, is_remote_split, search_thread_pool,
    SEARCHER_CACHE_EVICTIONS,
};
use crate::split_searcher::types::CachedSearcherContext;
use crate::utils::{arc_to_jlong, with_arc_safe};

use super::single_doc_retrieval::retrieve_document_from_split;

/// Optimized bulk document retrieval using Quickwit's proven patterns from fetch_docs.rs
/// Key optimizations:
/// 1. Reuse index, reader and searcher across all documents
/// 2. Sort by DocAddress for better cache locality
/// 3. Use doc_async for optimal I/O performance
/// 4. Use proper cache sizing (NUM_CONCURRENT_REQUESTS)
/// 5. Return raw pointers for JNI integration
pub fn retrieve_documents_batch_from_split_optimized(
    searcher_ptr: jlong,
    mut doc_addresses: Vec<tantivy::DocAddress>,
) -> Result<Vec<jobject>, anyhow::Error> {
    // TRACE: Entry point
    debug_println!(
        "🔍 TRACE: retrieve_documents_batch_from_split_optimized called with {} docs",
        doc_addresses.len()
    );

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
                    debug_println!(
                        "RUST DEBUG: ✅ BATCH CACHE HIT: Using cached searcher for batch processing"
                    );
                    debug_println!(
                        "🔍 TRACE: Entered batch processing path with {} docs",
                        doc_addresses.len()
                    );
                    debug_println!(
                        "🔍 TRACE: byte_range_cache is_some = {}",
                        byte_range_cache.is_some()
                    );
                    debug_println!(
                        "🔍 TRACE: bundle_file_offsets len = {}",
                        bundle_file_offsets.len()
                    );
                    let schema = cached_searcher.schema(); // Get schema from cached searcher

                    // 🚀 BATCH OPTIMIZATION: Prefetch consolidated byte ranges for better S3 performance
                    let optimizer = SimpleBatchOptimizer::new(SimpleBatchConfig::default());

                    if optimizer.should_optimize(doc_addresses.len()) {
                        debug_println!(
                            "🚀 BATCH_OPT: Starting range consolidation for {} documents",
                            doc_addresses.len()
                        );
                        debug_println!("🔍 TRACE: Should optimize = true");

                        match optimizer.consolidate_ranges(&doc_addresses, &cached_searcher) {
                            Ok(ranges) => {
                                let num_segments = ranges
                                    .iter()
                                    .map(|r| r.file_path.clone())
                                    .collect::<std::collections::HashSet<_>>()
                                    .len();
                                debug_println!(
                                    "🚀 BATCH_OPT: Consolidated {} docs → {} ranges across {} segments",
                                    doc_addresses.len(),
                                    ranges.len(),
                                    num_segments
                                );
                                debug_println!("🔍 TRACE: Consolidated to {} ranges", ranges.len());
                                if *crate::debug::DEBUG_ENABLED {
                                    for (i, range) in ranges.iter().enumerate() {
                                        debug_println!(
                                            "🔍 TRACE:   Range {}: {:?}, {}..{}",
                                            i,
                                            range.file_path,
                                            range.start,
                                            range.end
                                        );
                                    }
                                }

                                // 🚀 BATCH OPTIMIZATION FIX: Use prefetch_ranges_with_cache to populate the correct cache
                                let prefetch_result = if let Some(cache) = byte_range_cache {
                                    debug_println!(
                                        "🚀 BATCH_OPT: Using prefetch_ranges_with_cache (OPTIMIZED)"
                                    );
                                    debug_println!(
                                        "🔍 TRACE: Calling prefetch_ranges_with_cache with {} bundle offsets",
                                        bundle_file_offsets.len()
                                    );
                                    crate::batch_retrieval::simple::prefetch_ranges_with_cache(
                                        ranges.clone(),
                                        storage_resolver.clone(),
                                        split_uri,
                                        cache,
                                        bundle_file_offsets,
                                    )
                                    .await
                                } else {
                                    debug_println!(
                                        "⚠️ BATCH_OPT: ByteRangeCache not available, using fallback prefetch"
                                    );
                                    debug_println!("🔍 TRACE: FALLBACK - ByteRangeCache is None!");
                                    optimizer
                                        .prefetch_ranges(
                                            ranges.clone(),
                                            storage_resolver.clone(),
                                            split_uri,
                                        )
                                        .await
                                };

                                match prefetch_result {
                                    Ok(stats) => {
                                        debug_println!(
                                            "🚀 BATCH_OPT SUCCESS: Prefetched {} ranges, {} bytes in {}ms",
                                            stats.ranges_fetched,
                                            stats.bytes_fetched,
                                            stats.duration_ms
                                        );
                                        debug_println!(
                                            "   📊 Consolidation ratio: {:.1}x (docs/ranges)",
                                            stats.consolidation_ratio(doc_addresses.len())
                                        );

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
                                        debug_println!(
                                            "⚠️ BATCH_OPT: Prefetch failed (continuing with normal retrieval): {}",
                                            e
                                        );
                                        // Non-fatal: continue with normal doc_async which will fetch on-demand
                                    }
                                }
                            }
                            Err(e) => {
                                debug_println!(
                                    "⚠️ BATCH_OPT: Range consolidation failed (continuing with normal retrieval): {}",
                                    e
                                );
                                // Non-fatal: continue with normal doc_async
                            }
                        }
                    } else {
                        debug_println!(
                            "⏭️ BATCH_OPT: Skipping optimization (only {} docs, threshold: {})",
                            doc_addresses.len(),
                            optimizer.config.min_docs_for_optimization
                        );
                    }

                    // ✅ QUICKWIT CONCURRENT PATTERN: Use cached searcher with concurrency
                    // Using global cache configuration for concurrent batch processing
                    // Note: doc_async calls will now benefit from prefetched ByteRangeCache

                    // Validate segment ordinals before retrieval to prevent index-out-of-bounds panics
                    let num_segments_cached = cached_searcher.segment_readers().len();

                    let doc_futures = doc_addresses.into_iter().map(|doc_addr| {
                        let moved_searcher = cached_searcher.clone(); // Reuse cached searcher
                        let moved_schema = schema.clone();
                        async move {
                            if doc_addr.segment_ord as usize >= num_segments_cached {
                                return Err(anyhow::anyhow!(
                                    "Invalid segment ordinal {}: index has {} segment(s)",
                                    doc_addr.segment_ord, num_segments_cached
                                ));
                            }
                            // Add timeout to individual doc_async calls to prevent hanging
                            let doc: tantivy::schema::TantivyDocument = tokio::time::timeout(
                                std::time::Duration::from_secs(5),
                                moved_searcher.doc_async(doc_addr),
                            )
                            .await
                            .map_err(|_| {
                                anyhow::anyhow!(
                                    "Document retrieval timed out for {:?}",
                                    doc_addr
                                )
                            })?
                            .map_err(|e| {
                                anyhow::anyhow!(
                                    "Failed to retrieve document at address {:?}: {}",
                                    doc_addr,
                                    e
                                )
                            })?;

                            // Create a RetrievedDocument and register it
                            let retrieved_doc = RetrievedDocument::new_with_schema(doc, &moved_schema);
                            let wrapper = DocumentWrapper::Retrieved(retrieved_doc);
                            let wrapper_arc =
                                std::sync::Arc::new(std::sync::Mutex::new(wrapper));
                            let doc_ptr = arc_to_jlong(wrapper_arc);

                            Ok::<jobject, anyhow::Error>(doc_ptr as jobject)
                        }
                    });

                    // Execute concurrent batch retrieval with cached searcher
                    // IMPORTANT: Use buffered() NOT buffer_unordered() to maintain document order.
                    // buffer_unordered() returns results in completion order, which corrupts the
                    // document-to-address mapping and causes data corruption (wrong field values).
                    use futures::stream::{StreamExt, TryStreamExt};
                    let doc_ptrs: Vec<jobject> = futures::stream::iter(doc_futures)
                        .buffered(BASE_CONCURRENT_REQUESTS)
                        .try_collect::<Vec<_>>()
                        .await
                        .map_err(|e| {
                            anyhow::anyhow!("Cached searcher batch retrieval failed: {}", e)
                        })?;

                    // 🚀 BATCH OPTIMIZATION FIX: Always return here - using context.cached_searcher
                    return Ok::<Vec<jobject>, anyhow::Error>(doc_ptrs);
                }
                // 🚀 BATCH OPTIMIZATION FIX: Fallback code below is now unreachable
                // The code is kept for compatibility but should be removed in cleanup
                #[allow(unreachable_code)]
                {
                    // Use pre-created storage resolver from searcher context
                    debug_println!(
                        "✅ QUICKWIT_LIFECYCLE: Using cached storage from searcher context (Quickwit pattern)"
                    );
                    debug_println!(
                        "✅ CACHED_STORAGE_USED: Storage at address {:p} (Quickwit lifecycle)",
                        Arc::as_ptr(storage_resolver)
                    );
                    let index_storage = storage_resolver.clone();

                    // Extract just the filename as the relative path (same as individual retrieval)
                    let relative_path = if let Some(last_slash_pos) = split_uri.rfind('/') {
                        std::path::Path::new(&split_uri[last_slash_pos + 1..])
                    } else {
                        std::path::Path::new(split_uri)
                    };

                    // 🚀 BATCH OPTIMIZATION: Use Quickwit's optimized path when footer metadata is available for remote splits
                    debug_println!(
                        "RUST DEBUG: Checking batch optimization conditions - footer_metadata: {}, is_remote: {}",
                        has_footer_metadata(footer_start, footer_end),
                        is_remote_split(split_uri)
                    );
                    let mut index = if has_footer_metadata(footer_start, footer_end)
                        && is_remote_split(split_uri)
                    {
                        debug_println!(
                            "RUST DEBUG: 🚀 Using Quickwit optimized path for batch retrieval (footer: {}..{})",
                            footer_start,
                            footer_end
                        );

                        // Use cached index to eliminate repeated open_index_with_caches calls
                        let index = cached_index.as_ref().clone();
                        debug_println!(
                            "🔥 INDEX CACHED: Reusing cached index for batch operations instead of expensive open_index_with_caches call"
                        );

                        debug_println!(
                            "RUST DEBUG: ✅ Successfully reused cached index for batch retrieval"
                        );
                        index
                    } else {
                        debug_println!(
                            "RUST DEBUG: ⚠️ Footer metadata not available for batch retrieval, falling back to full download"
                        );

                        // Fallback: Get the full file data (original behavior for missing metadata)
                        let file_size = index_storage
                            .file_num_bytes(relative_path)
                            .await
                            .map_err(|e| {
                                anyhow::anyhow!(
                                    "Failed to get file size for {}: {}",
                                    split_uri,
                                    e
                                )
                            })?;

                        let split_data = index_storage
                            .get_slice(relative_path, 0..file_size as usize)
                            .await
                            .map_err(|e| {
                                anyhow::anyhow!(
                                    "Failed to get split data from {}: {}",
                                    split_uri,
                                    e
                                )
                            })?;

                        debug_println!(
                            "RUST DEBUG: ⚠️ Downloaded full split file for batch: {} bytes",
                            split_data.len()
                        );

                        let split_file_slice =
                            tantivy::directory::FileSlice::new(std::sync::Arc::new(split_data));
                        let bundle_directory =
                            BundleDirectory::open_split(split_file_slice).map_err(|e| {
                                anyhow::anyhow!(
                                    "Failed to open bundle directory {}: {}",
                                    split_uri,
                                    e
                                )
                            })?;

                        // ✅ QUICKWIT NATIVE: Use Quickwit's native index opening
                        open_index(
                            bundle_directory.box_clone(),
                            get_quickwit_fastfield_normalizer_manager().tantivy_manager(),
                        )
                        .map_err(|e| {
                            anyhow::anyhow!("Failed to open index from bundle {}: {}", split_uri, e)
                        })?
                    };

                    // Use the same Quickwit optimizations as individual method
                    let tantivy_executor = search_thread_pool()
                        .get_underlying_rayon_thread_pool()
                        .into();
                    index.set_executor(tantivy_executor);

                    // Create index reader with Quickwit optimizations (fetch_docs.rs line 187-192)
                    // Using adaptive cache configuration for batch operations
                    let batch_cache_blocks = get_batch_doc_cache_blocks();
                    debug_println!(
                        "⚡ CACHE_OPTIMIZATION: Batch retrieval fallback - applying adaptive doc store cache optimization - blocks: {} (batch operations)",
                        batch_cache_blocks
                    );
                    let index_reader = index
                        .reader_builder()
                        .doc_store_cache_num_blocks(batch_cache_blocks) // ADAPTIVE CACHE OPTIMIZATION
                        .reload_policy(ReloadPolicy::Manual)
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
                            debug_println!(
                                "RUST DEBUG: 🗑️ LRU EVICTION - cache full, evicted oldest searcher"
                            );
                        }
                        debug_println!(
                            "RUST DEBUG: ✅ CACHED NEW SEARCHER: Stored searcher for future batch operations"
                        );
                    }

                    // 🚀 BATCH OPTIMIZATION: Prefetch consolidated byte ranges for better S3 performance
                    let optimizer = SimpleBatchOptimizer::new(SimpleBatchConfig::default());

                    if optimizer.should_optimize(doc_addresses.len()) {
                        debug_println!(
                            "🚀 BATCH_OPT: Starting range consolidation for {} documents",
                            doc_addresses.len()
                        );

                        match optimizer.consolidate_ranges(&doc_addresses, &tantivy_searcher) {
                            Ok(ranges) => {
                                debug_println!(
                                    "🚀 BATCH_OPT: Consolidated {} docs → {} ranges",
                                    doc_addresses.len(),
                                    ranges.len()
                                );

                                // Prefetch the consolidated ranges to populate ByteRangeCache
                                match optimizer
                                    .prefetch_ranges(ranges.clone(), index_storage.clone(), split_uri)
                                    .await
                                {
                                    Ok(stats) => {
                                        debug_println!(
                                            "🚀 BATCH_OPT SUCCESS: Prefetched {} ranges, {} bytes in {}ms",
                                            stats.ranges_fetched,
                                            stats.bytes_fetched,
                                            stats.duration_ms
                                        );
                                        debug_println!(
                                            "   📊 Consolidation ratio: {:.1}x (docs/ranges)",
                                            stats.consolidation_ratio(doc_addresses.len())
                                        );

                                        // Record metrics (estimate bytes wasted from gaps)
                                        let num_segments = ranges
                                            .iter()
                                            .map(|r| r.file_path.clone())
                                            .collect::<std::collections::HashSet<_>>()
                                            .len();
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
                                        debug_println!(
                                            "⚠️ BATCH_OPT: Prefetch failed (continuing with normal retrieval): {}",
                                            e
                                        );
                                        // Non-fatal: continue with normal doc_async which will fetch on-demand
                                    }
                                }
                            }
                            Err(e) => {
                                debug_println!(
                                    "⚠️ BATCH_OPT: Range consolidation failed (continuing with normal retrieval): {}",
                                    e
                                );
                                // Non-fatal: continue with normal doc_async
                            }
                        }
                    } else {
                        debug_println!(
                            "⏭️ BATCH_OPT: Skipping optimization (only {} docs, threshold: {})",
                            doc_addresses.len(),
                            optimizer.config.min_docs_for_optimization
                        );
                    }

                    // ✅ QUICKWIT CONCURRENT PATTERN: Use concurrent document retrieval (fetch_docs.rs line 200-258)
                    // Note: doc_async calls will now benefit from prefetched ByteRangeCache

                    // Validate segment ordinals before retrieval to prevent index-out-of-bounds panics
                    let num_segments_tantivy = tantivy_searcher.segment_readers().len();

                    // Create async futures for concurrent document retrieval (like Quickwit)
                    let doc_futures = doc_addresses.into_iter().map(|doc_addr| {
                        let moved_searcher = tantivy_searcher.clone(); // Clone Arc for concurrent access
                        let moved_schema = schema.clone(); // Clone schema for each future
                        async move {
                            if doc_addr.segment_ord as usize >= num_segments_tantivy {
                                return Err(anyhow::anyhow!(
                                    "Invalid segment ordinal {}: index has {} segment(s)",
                                    doc_addr.segment_ord, num_segments_tantivy
                                ));
                            }
                            // Use doc_async like Quickwit with timeout - QUICKWIT OPTIMIZATION (fetch_docs.rs line 205-207)
                            let doc: tantivy::schema::TantivyDocument = tokio::time::timeout(
                                std::time::Duration::from_secs(5),
                                moved_searcher.doc_async(doc_addr),
                            )
                            .await
                            .map_err(|_| {
                                anyhow::anyhow!(
                                    "Document retrieval timed out for {:?}",
                                    doc_addr
                                )
                            })?
                            .map_err(|e| {
                                anyhow::anyhow!(
                                    "Failed to retrieve document at address {:?}: {}",
                                    doc_addr,
                                    e
                                )
                            })?;

                            // Create a RetrievedDocument and register it
                            let retrieved_doc = RetrievedDocument::new_with_schema(doc, &moved_schema);
                            let wrapper = DocumentWrapper::Retrieved(retrieved_doc);
                            let wrapper_arc =
                                std::sync::Arc::new(std::sync::Mutex::new(wrapper));
                            let doc_ptr = arc_to_jlong(wrapper_arc);

                            Ok::<jobject, anyhow::Error>(doc_ptr as jobject)
                        }
                    });

                    // ✅ QUICKWIT CONCURRENT EXECUTION: Process up to BASE_CONCURRENT_REQUESTS simultaneously
                    // IMPORTANT: Use buffered() NOT buffer_unordered() to maintain document order.
                    // buffer_unordered() returns results in completion order, which corrupts the
                    // document-to-address mapping and causes data corruption (wrong field values).
                    use futures::stream::{StreamExt, TryStreamExt};
                    let doc_ptrs: Vec<jobject> = futures::stream::iter(doc_futures)
                        .buffered(BASE_CONCURRENT_REQUESTS)
                        .try_collect::<Vec<_>>()
                        .await
                        .map_err(|e| {
                            anyhow::anyhow!("Concurrent document retrieval failed: {}", e)
                        })?;

                    Ok::<Vec<jobject>, anyhow::Error>(doc_ptrs)
                } // Close the #[allow(unreachable_code)] block
            }))
            .map_err(|timeout_err| {
                debug_println!(
                    "🕐 TIMEOUT: Document retrieval timed out after 10 seconds: {}",
                    timeout_err
                );
                anyhow::anyhow!(
                    "Document retrieval timed out after 10 seconds - likely due to runtime shutdown"
                )
            })?
        })
    });

    match result {
        Some(Ok(doc_ptrs)) => Ok(doc_ptrs),
        Some(Err(e)) => Err(e),
        None => Err(anyhow::anyhow!(
            "Searcher context not found for pointer {}",
            searcher_ptr
        )),
    }
}

/// Legacy method - kept for compatibility but not optimized
pub fn retrieve_documents_batch_from_split(
    searcher_ptr: jlong,
    doc_addresses: Vec<tantivy::DocAddress>,
) -> Result<Vec<(tantivy::DocAddress, tantivy::schema::TantivyDocument, tantivy::schema::Schema)>, anyhow::Error>
{
    // Fallback to single document retrieval for now
    let mut results = Vec::new();
    for addr in doc_addresses {
        match retrieve_document_from_split(searcher_ptr, addr) {
            Ok((doc, schema)) => {
                results.push((addr, doc, schema));
            }
            Err(e) => return Err(e),
        }
    }
    Ok(results)
}
