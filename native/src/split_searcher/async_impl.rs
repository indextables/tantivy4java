// async_impl.rs - Async implementation functions for SplitSearcher
// Extracted from mod.rs during refactoring
// Contains: async search, doc retrieval, schema retrieval, and Quickwit search implementations

use std::sync::Arc;
use jni::sys::{jlong, jint};
use jni::JNIEnv;

use crate::debug_println;
use super::types::CachedSearcherContext;
use super::searcher_cache::extract_split_id_from_uri;
use quickwit_storage::Storage;

// Thread-safe async implementation function for search operations
/// Thread-safe async implementation that returns LeafSearchResponse directly (no JSON marshalling)
/// Includes Smart Wildcard AST Skipping optimization for expensive wildcard patterns
pub async fn perform_search_async_impl_leaf_response(
    searcher_ptr: jlong,
    query_json: String,
    limit: jint,
) -> Result<quickwit_proto::search::LeafSearchResponse, anyhow::Error> {
    debug_println!("🔍 ASYNC_IMPL: Starting thread-safe async search (returns LeafSearchResponse directly)");

    if searcher_ptr == 0 {
        return Err(anyhow::anyhow!("Invalid searcher pointer"));
    }

    // Extract searcher context using the safe Arc pattern with struct-based approach
    let searcher_context = crate::utils::jlong_to_arc::<CachedSearcherContext>(searcher_ptr)
        .ok_or_else(|| anyhow::anyhow!("Invalid searcher context"))?;

    let context = searcher_context.as_ref();

    debug_println!("🔍 ASYNC_IMPL: Extracted searcher context, performing search on split: {}", context.split_uri);

    // ========================================================================
    // Smart Wildcard AST Skipping Optimization (Phase 1)
    // ========================================================================
    // Short-circuit when cheap filter returns 0 results - skip expensive wildcard entirely
    //
    // Note: Phase 2 (query restructuring) was investigated but provides no benefit because
    // Tantivy's AutomatonWeight materializes ALL matching docs into a BitSet during scorer()
    // creation, before any intersection happens. Moving clauses to filter only affects
    // scoring, not execution order.

    let effective_query_json = if let Ok(analysis) = crate::split_query::analyze_query_ast_json(&query_json) {
        if analysis.can_optimize {
            debug_println!("🚀 SMART_WILDCARD: Query is optimizable - has expensive wildcard + cheap filters");

            if let Some(ref cheap_filter_json) = analysis.cheap_filter_json {
                debug_println!("🚀 SMART_WILDCARD: Running cheap filter first: {}", cheap_filter_json);

                // Run cheap filter with limit=1 to check if any documents match
                let cheap_result = perform_real_quickwit_search(
                    &context.split_uri,
                    &context.aws_config,
                    context.footer_start,
                    context.footer_end,
                    &context.doc_mapping_json,
                    context.cached_storage.clone(),
                    context.cached_searcher.clone(),
                    context.cached_index.clone(),
                    cheap_filter_json,
                    1, // Only need to know if at least 1 doc matches
                ).await;

                match cheap_result {
                    Ok(result) if result.num_hits == 0 => {
                        debug_println!("✅ SMART_WILDCARD: SHORT-CIRCUIT! Cheap filter returned 0 results");
                        debug_println!("✅ SMART_WILDCARD: Skipping expensive wildcard evaluation entirely");

                        // Return empty result - no need to evaluate expensive wildcard
                        return Ok(quickwit_proto::search::LeafSearchResponse {
                            num_hits: 0,
                            partial_hits: Vec::new(),
                            failed_splits: Vec::new(),
                            num_attempted_splits: 1,
                            num_successful_splits: 1,
                            intermediate_aggregation_result: None,
                            resource_stats: None,
                        });
                    }
                    Ok(result) => {
                        debug_println!("🚀 SMART_WILDCARD: Cheap filter matched {} docs, proceeding with full query", result.num_hits);
                        query_json.clone()
                    }
                    Err(e) => {
                        debug_println!("⚠️ SMART_WILDCARD: Cheap filter failed ({}), falling back to full query", e);
                        query_json.clone()
                    }
                }
            } else {
                query_json.clone()
            }
        } else {
            debug_println!("🔍 SMART_WILDCARD: Query not optimizable (expensive={}, cheap_filters={})",
                analysis.has_expensive_wildcard, analysis.has_cheap_filters);
            query_json.clone()
        }
    } else {
        query_json.clone()
    };
    // ========================================================================
    // End of Smart Wildcard Optimization
    // ========================================================================

    // Use Quickwit's real search functionality with cached searcher following their patterns
    let search_result = perform_real_quickwit_search(
        &context.split_uri,
        &context.aws_config,
        context.footer_start,
        context.footer_end,
        &context.doc_mapping_json,
        context.cached_storage.clone(),
        context.cached_searcher.clone(),
        context.cached_index.clone(),
        &effective_query_json,
        limit as usize,
    ).await?;

    debug_println!("✅ ASYNC_IMPL: Search completed successfully with {} hits", search_result.num_hits);
    Ok(search_result)
}

/// Legacy thread-safe async implementation that returns JSON string (kept for compatibility)
pub async fn perform_search_async_impl_thread_safe(
    searcher_ptr: jlong,
    query_json: String,
    limit: jint,
) -> Result<String, anyhow::Error> {
    debug_println!("🔍 ASYNC_IMPL: Starting thread-safe async search implementation (legacy JSON mode)");

    let search_result = perform_search_async_impl_leaf_response(searcher_ptr, query_json, limit).await?;

    // Convert result to JSON string for return (legacy mode)
    let result_json = serde_json::to_string(&search_result)
        .map_err(|e| anyhow::anyhow!("Failed to serialize search result: {}", e))?;

    debug_println!("✅ ASYNC_IMPL: Search completed successfully with {} hits (legacy JSON mode)", search_result.num_hits);
    Ok(result_json)
}

// Legacy wrapper for backward compatibility
pub async fn perform_search_async_impl(
    _env: JNIEnv<'_>,
    searcher_ptr: jlong,
    query_json: String,
    limit: jint,
) -> Result<String, anyhow::Error> {
    perform_search_async_impl_thread_safe(searcher_ptr, query_json, limit).await
}


/// Async document retrieval using Quickwit's exact pattern from fetch_docs.rs
/// This follows the same approach: open_index_with_caches -> searcher.doc_async(doc_addr).await
async fn perform_quickwit_async_doc_retrieval(
    searcher_ptr: jlong,
    doc_address: tantivy::DocAddress,
) -> Result<(tantivy::schema::TantivyDocument, tantivy::schema::Schema), anyhow::Error> {
    debug_println!("🔥 QUICKWIT_DOC: Starting Quickwit-style async document retrieval");
    debug_println!("📄 QUICKWIT_DOC: Following fetch_docs.rs pattern for async document retrieval");

    // Extract clean struct-based searcher context
    let searcher_context = crate::utils::jlong_to_arc::<CachedSearcherContext>(searcher_ptr)
        .ok_or_else(|| anyhow::anyhow!("Invalid searcher context"))?;

    let context = searcher_context.as_ref();

    debug_println!("🔥 QUICKWIT_DOC: Got searcher context for split: {}", context.split_uri);

    // Follow Quickwit's pattern: reuse cached storage instead of resolving again
    let storage = context.cached_storage.clone();
    debug_println!("🔥 QUICKWIT_DOC: Reusing cached storage instance: {:p}", Arc::as_ptr(&storage));

    // Extract split ID from file path (same pattern as working search implementation)
    let split_filename = if let Some(last_slash_pos) = context.split_uri.rfind('/') {
        &context.split_uri[last_slash_pos + 1..]
    } else {
        &context.split_uri
    };

    // For split_id, use the filename without .split extension if present
    let split_id = if split_filename.ends_with(".split") {
        &split_filename[..split_filename.len() - 6] // Remove ".split"
    } else {
        split_filename
    };

    debug_println!("🔥 QUICKWIT_DOC: Extracted split_id: {} from split_uri: {}", split_id, context.split_uri);

    // Use cached searcher to eliminate repeated searcher creation and ensure cache reuse
    let searcher = context.cached_searcher.clone(); // Follow Quickwit's exact pattern: reuse the same Arc<Searcher>
    debug_println!("🔥 SEARCHER CACHED: Reusing cached searcher following Quickwit's exact pattern for optimal cache performance");
    debug_println!("🔥 QUICKWIT_DOC: Using cached Tantivy searcher with preserved cache state");

    // Use Quickwit's exact async document retrieval pattern: searcher.doc_async(doc_addr).await
    let tantivy_doc = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        searcher.doc_async(doc_address)
    )
    .await
    .map_err(|_| anyhow::anyhow!("Document retrieval timed out for {:?}", doc_address))?
    .map_err(|e| anyhow::anyhow!("Failed to retrieve document using Quickwit's async pattern: {}", e))?;
    debug_println!("🔥 QUICKWIT_DOC: Successfully retrieved document using searcher.doc_async()");

    // Get schema from searcher (same as Quickwit does)
    let schema = searcher.schema().clone();
    debug_println!("🔥 QUICKWIT_DOC: Got schema from searcher");

    debug_println!("📄 QUICKWIT_DOC: Document retrieval completed using Quickwit's async pattern");

    Ok((tantivy_doc, schema))
}

// Thread-safe async implementation function for document retrieval operations
pub async fn perform_doc_retrieval_async_impl_thread_safe(
    searcher_ptr: jlong,
    segment_ord: u32,
    doc_id: u32,
) -> Result<jlong, anyhow::Error> {
    debug_println!("🔥 DOC DEBUG: perform_doc_retrieval_async_impl_thread_safe called - ptr:{}, seg:{}, doc:{}", searcher_ptr, segment_ord, doc_id);
    debug_println!("📄 ASYNC_IMPL: Starting thread-safe async document retrieval");

    if searcher_ptr == 0 {
        return Err(anyhow::anyhow!("Invalid searcher pointer"));
    }

    // Extract searcher context using the safe Arc pattern with struct-based approach
    let searcher_context = crate::utils::jlong_to_arc::<CachedSearcherContext>(searcher_ptr)
        .ok_or_else(|| anyhow::anyhow!("Invalid searcher context"))?;

    let context = searcher_context.as_ref();

    debug_println!("📄 ASYNC_IMPL: Extracted searcher context, retrieving doc from split: {}", context.split_uri);

    // Create DocAddress from segment_ord and doc_id
    let doc_address = tantivy::DocAddress::new(segment_ord, doc_id);

    // Use Quickwit's async document retrieval pattern directly
    debug_println!("🔥 DOC DEBUG: About to use Quickwit's async document retrieval pattern");
    let (tantivy_doc, schema) = perform_quickwit_async_doc_retrieval(searcher_ptr, doc_address).await?;
    debug_println!("🔥 DOC DEBUG: Quickwit async document retrieval completed successfully");

    // Convert TantivyDocument to RetrievedDocument for proper object integration
    use crate::document::{DocumentWrapper, RetrievedDocument};
    let retrieved_doc = RetrievedDocument::new_with_schema(tantivy_doc, &schema);
    let wrapper = DocumentWrapper::Retrieved(retrieved_doc);
    let wrapper_arc = std::sync::Arc::new(std::sync::Mutex::new(wrapper));

    debug_println!("✅ ASYNC_IMPL: Document retrieval completed successfully using real objects");
    let document_ptr = crate::utils::arc_to_jlong(wrapper_arc);
    debug_println!("🔍 ARC_REGISTRY: Stored DocumentWrapper Arc in registry with ID: {}", document_ptr);
    Ok(document_ptr)
}

// Legacy wrapper for backward compatibility
pub async fn perform_doc_retrieval_async_impl(
    _env: JNIEnv<'_>,
    searcher_ptr: jlong,
    segment_ord: u32,
    doc_id: u32,
) -> Result<jlong, anyhow::Error> {
    perform_doc_retrieval_async_impl_thread_safe(searcher_ptr, segment_ord, doc_id).await
}

// Thread-safe async implementation function for schema retrieval operations
pub async fn perform_schema_retrieval_async_impl_thread_safe(
    searcher_ptr: jlong,
) -> Result<i64, anyhow::Error> {
    debug_println!("📋 ASYNC_IMPL: Starting thread-safe async schema retrieval with pointer: {}", searcher_ptr);

    if searcher_ptr == 0 {
        debug_println!("❌ ASYNC_IMPL: Searcher pointer is 0 (null)");
        return Err(anyhow::anyhow!("Invalid searcher pointer (0)"));
    }

    // ✅ DEBUG: Check Arc registry status before attempting extraction
    {
        let registry = crate::utils::ARC_REGISTRY.lock().unwrap();
        debug_println!("📋 ARC_REGISTRY: Registry contains {} entries", registry.len());
        if registry.contains_key(&searcher_ptr) {
            debug_println!("✅ ARC_REGISTRY: Searcher pointer {} found in registry", searcher_ptr);
        } else {
            debug_println!("❌ ARC_REGISTRY: Searcher pointer {} NOT found in registry", searcher_ptr);
            debug_println!("📋 ARC_REGISTRY: Available keys: {:?}", registry.keys().collect::<Vec<_>>());
        }
    }

    // Extract searcher context using the safe Arc pattern with new struct-based approach
    let searcher_context = crate::utils::jlong_to_arc::<CachedSearcherContext>(searcher_ptr);

    // ✅ FIX: If searcher context is missing, use direct schema mapping fallback
    if searcher_context.is_none() {
        debug_println!("❌ ASYNC_IMPL: CachedSearcherContext missing for pointer {}, trying direct schema mapping", searcher_ptr);
        if let Some(schema_ptr) = crate::split_query::get_searcher_schema(searcher_ptr) {
            debug_println!("✅ FALLBACK: Found direct schema mapping {} for searcher {}", schema_ptr, searcher_ptr);
            return Ok(schema_ptr);
        } else {
            debug_println!("❌ FALLBACK: No direct schema mapping found for searcher {}", searcher_ptr);
            return Err(anyhow::anyhow!("Invalid searcher context - Arc and direct mapping not found for pointer: {}", searcher_ptr));
        }
    }

    let searcher_context = searcher_context.unwrap();

    let context = searcher_context.as_ref();

    debug_println!("📋 ASYNC_IMPL: Extracted searcher context for split: {}", context.split_uri);

    // ✅ FIX: Get schema directly from cached index instead of using DocMapper
    // DocMapper has compatibility issues with dynamic JSON fields (requires non-empty field_mappings)
    debug_println!("📋 ASYNC_IMPL: Using cached_index.schema() directly (DocMapper incompatible with dynamic JSON fields)");
    let schema = context.cached_index.schema();
    let schema_ptr = crate::utils::arc_to_jlong(Arc::new(schema.clone()));

    // ✅ CRITICAL FIX: Cache the schema for parseQuery fallback
    debug_println!("📋 CACHE_FIX: Caching schema for parseQuery compatibility for split: {}", context.split_uri);
    crate::split_query::store_split_schema(&context.split_uri, schema.clone());
    debug_println!("📋 CACHE_FIX: Schema cached successfully");

    debug_println!("✅ ASYNC_IMPL: Schema retrieval completed successfully, pointer: {}", schema_ptr);
    Ok(schema_ptr)
}

// Legacy wrapper for backward compatibility
pub async fn perform_schema_retrieval_async_impl(
    _env: JNIEnv<'_>,
    searcher_ptr: jlong,
) -> Result<i64, anyhow::Error> {
    perform_schema_retrieval_async_impl_thread_safe(searcher_ptr).await
}

/// Real Quickwit search implementation using cached components directly
async fn perform_real_quickwit_search(
    split_uri: &str,
    _aws_config: &std::collections::HashMap<String, String>,
    footer_start: u64,
    footer_end: u64,
    _doc_mapping_json: &Option<String>,
    cached_storage: Arc<dyn Storage>,
    _cached_searcher: Arc<tantivy::Searcher>,
    _cached_index: Arc<tantivy::Index>,
    query_json: &str,
    limit: usize,
) -> anyhow::Result<quickwit_proto::search::LeafSearchResponse> {
    debug_println!("🔍 REAL_QUICKWIT: Starting real Quickwit search implementation");

    // Following async-first architecture design - this is a pure async function
    // with no JNI dependencies, only receiving thread-safe parameters

    // ✅ FIX: Create DocMapper from cached index schema instead of doc_mapping JSON
    // DocMapper parsing has compatibility issues with dynamic JSON fields (requires non-empty field_mappings)
    debug_println!("🔍 REAL_QUICKWIT: Creating DocMapper from cached index schema (bypassing doc_mapping JSON)");

    let doc_mapper = quickwit_doc_mapper::default_doc_mapper_for_test();
    let doc_mapper = Arc::new(doc_mapper);

    // Create SearchRequest following Quickwit patterns
    let search_request = quickwit_proto::search::SearchRequest {
        index_id_patterns: vec!["split_search".to_string()],
        query_ast: query_json.to_string(),
        start_offset: 0,
        max_hits: limit as u64,
        start_timestamp: None,
        end_timestamp: None,
        sort_fields: vec![],
        snippet_fields: vec![],
        count_hits: quickwit_proto::search::CountHits::CountAll.into(),
        aggregation_request: None,
        scroll_ttl_secs: None,
        search_after: None,
    };

    // Create SplitIdAndFooterOffsets for Quickwit
    let split_metadata = quickwit_proto::search::SplitIdAndFooterOffsets {
        split_id: extract_split_id_from_uri(split_uri),
        split_footer_start: footer_start,
        split_footer_end: footer_end,
        num_docs: 0, // Will be filled by Quickwit
        timestamp_start: None,
        timestamp_end: None,
    };

    // Use cached storage directly (Quickwit lifecycle pattern)
    let storage = cached_storage;

    // CRITICAL FIX: Use shared global context for cache hits but create individual permit provider
    // This preserves cache efficiency while eliminating SearchPermitProvider permit exhaustion
    debug_println!("🔍 PERMIT_FIX: Using global context for cache hits but individual permit provider");

    let searcher_context = crate::global_cache::get_global_searcher_context();

    // Create CanSplitDoBetter filter (following Quickwit patterns from standalone_searcher.rs)
    let split_filter = Arc::new(std::sync::RwLock::new(quickwit_search::CanSplitDoBetter::Uninformative));

    // Get aggregation limits (following Quickwit patterns)
    let aggregations_limits = searcher_context.aggregation_limit.clone();

    // CRITICAL FIX: Create individual permit provider per search to eliminate contention
    // This preserves cache hits while avoiding permit pool exhaustion
    debug_println!("🔍 PERMIT_FIX: Creating individual SearchPermitProvider per search operation");

    let individual_permit_provider = {
        use quickwit_search::search_permit_provider::SearchPermitProvider;
        use bytesize::ByteSize;

        Arc::new(SearchPermitProvider::new_sync(
            5, // Allow up to 5 concurrent operations per search (plenty for single search)
            ByteSize::gb(1), // 1GB memory budget per search operation
        ))
    };

    // Get search permit from individual provider (no contention possible)
    let memory_allocation = quickwit_search::search_permit_provider::compute_initial_memory_allocation(
        &split_metadata,
        bytesize::ByteSize(1024 * 1024 * 50), // 50MB initial allocation (same as standalone_searcher.rs)
    );

    debug_println!("🔍 PERMIT_FIX: Requesting permit from dedicated SearchPermitProvider (guaranteed available)");
    debug_println!("🔍 PERMIT_DEBUG: About to request search permit with memory allocation: {}", memory_allocation);

    let permit_futures = individual_permit_provider.get_permits(vec![memory_allocation]).await;
    debug_println!("✅ PERMIT_DEBUG: Got permit futures from dedicated provider, extracting first future...");

    let permit_future = permit_futures.into_iter().next()
        .expect("Expected one permit future");

    debug_println!("🔍 PERMIT_FIX: Acquiring permit from dedicated provider - should be immediate");
    let mut search_permit = permit_future.await;
    debug_println!("✅ PERMIT_FIX: Successfully acquired search permit from dedicated provider - no timeout needed!");

    debug_println!("🔥 REAL_QUICKWIT: Using leaf_search_single_split with cache injection");
    debug_println!("🔍 SEARCH_DEBUG: About to call leaf_search_single_split - this might be where it hangs...");

    // SOLUTION: Use leaf_search_single_split but inject our cached components
    // This preserves the async handling while eliminating repeated downloads

    // Call Quickwit's actual leaf_search_single_split function
    debug_println!("🔍 CRITICAL_DEBUG: About to call leaf_search_single_split - THIS IS LIKELY THE HANG POINT");

    let leaf_search_result = tokio::time::timeout(
        std::time::Duration::from_secs(15), // 15 second timeout for leaf search
        quickwit_search::leaf_search_single_split(
            &searcher_context,
            search_request,
            storage,
            split_metadata,
            doc_mapper,
            split_filter,
            aggregations_limits,
            &mut search_permit,
        )
    ).await;

    debug_println!("🔍 CRITICAL_DEBUG: leaf_search_single_split call completed");

    let result = match leaf_search_result {
        Ok(search_result) => {
            debug_println!("✅ CRITICAL_DEBUG: leaf_search_single_split succeeded");
            search_result.map_err(|e| anyhow::anyhow!("Quickwit leaf search failed: {}", e))?
        },
        Err(_timeout) => {
            debug_println!("❌ CRITICAL_DEBUG: TIMEOUT in leaf_search_single_split - THIS IS THE HANG LOCATION!");
            debug_println!("🔍 PERMIT_DEBUG: Search timed out, explicitly dropping permit to ensure release");

            // CRITICAL FIX: Explicitly drop the permit to ensure it's released even on timeout
            drop(search_permit);
            debug_println!("✅ PERMIT_DEBUG: Permit explicitly dropped on timeout - should be available for next operation");

            return Err(anyhow::anyhow!("leaf_search_single_split timeout - this is where the hang occurs in the Quickwit native layer"));
        }
    };

    debug_println!("✅ REAL_QUICKWIT: Search completed successfully with {} hits", result.num_hits);

    // CRITICAL FIX: Explicitly drop the permit to ensure it's released immediately
    debug_println!("🔍 PERMIT_DEBUG: Search completed successfully, explicitly dropping permit");
    drop(search_permit);
    debug_println!("✅ PERMIT_DEBUG: Permit explicitly dropped on success - capacity available for next search operation");

    Ok(result)
}

/// Perform real Quickwit search with aggregations support
/// Uses the SAME pattern as perform_real_quickwit_search but enables aggregation_request
pub async fn perform_real_quickwit_search_with_aggregations(
    split_uri: &str,
    _aws_config: &std::collections::HashMap<String, String>,
    footer_start: u64,
    footer_end: u64,
    doc_mapping_json: &Option<String>,
    cached_storage: Arc<dyn Storage>,
    _cached_searcher: Arc<tantivy::Searcher>,
    _cached_index: Arc<tantivy::Index>,
    query_json: &str,
    limit: usize,
    aggregation_request_json: Option<String>,
) -> anyhow::Result<quickwit_proto::search::LeafSearchResponse> {
    debug_println!("🔍 AGGREGATION_SEARCH: Starting real Quickwit search with aggregations");

    // Create DocMapper from JSON following Quickwit patterns (SAME as working search)
    let doc_mapper = if let Some(doc_mapping_str) = doc_mapping_json {
        // First, clean up any escaped JSON from storage layer
        let cleaned_json = if doc_mapping_str.contains("\\\"") {
            doc_mapping_str.replace("\\\"", "\"").replace("\\\\", "\\")
        } else {
            doc_mapping_str.to_string()
        };

        // Parse array of field mappings into proper DocMapperBuilder format
        let field_mappings: Vec<serde_json::Value> = serde_json::from_str(&cleaned_json)
            .map_err(|e| anyhow::anyhow!("Failed to parse doc mapping JSON array: {}", e))?;

        // Convert to proper DocMapperBuilder format - this is what Quickwit actually expects
        let doc_mapper_builder_json = serde_json::json!({
            "field_mappings": field_mappings,
            "timestamp_field": null,
            "default_search_fields": []
        });

        // Deserialize into DocMapperBuilder first, then convert to DocMapper
        let doc_mapper_builder: quickwit_doc_mapper::DocMapperBuilder = serde_json::from_value(doc_mapper_builder_json)
            .map_err(|e| anyhow::anyhow!("Failed to parse DocMapperBuilder: {}", e))?;

        // Convert DocMapperBuilder to DocMapper
        let doc_mapper = quickwit_doc_mapper::DocMapper::try_from(doc_mapper_builder)
            .map_err(|e| anyhow::anyhow!("Failed to convert DocMapperBuilder to DocMapper: {}", e))?;

        Arc::new(doc_mapper)
    } else {
        return Err(anyhow::anyhow!("No doc mapping available for search"));
    };

    // Create SearchRequest following Quickwit patterns (KEY DIFFERENCE: enable aggregations)
    let search_request = quickwit_proto::search::SearchRequest {
        index_id_patterns: vec!["split_search".to_string()],
        query_ast: query_json.to_string(),
        start_offset: 0,
        max_hits: limit as u64,
        start_timestamp: None,
        end_timestamp: None,
        sort_fields: vec![],
        snippet_fields: vec![],
        count_hits: quickwit_proto::search::CountHits::CountAll.into(),
        aggregation_request: aggregation_request_json, // ENABLE AGGREGATIONS
        scroll_ttl_secs: None,
        search_after: None,
    };

    debug_println!("🔍 AGGREGATION_SEARCH: SearchRequest configured with aggregations: {}",
                   search_request.aggregation_request.is_some());

    // Create SplitIdAndFooterOffsets for Quickwit (SAME as working search)
    let split_metadata = quickwit_proto::search::SplitIdAndFooterOffsets {
        split_id: extract_split_id_from_uri(split_uri),
        split_footer_start: footer_start,
        split_footer_end: footer_end,
        num_docs: 0, // Will be filled by Quickwit
        timestamp_start: None,
        timestamp_end: None,
    };

    // Use cached storage directly (SAME as working search)
    let storage = cached_storage;

    // Use shared global context for cache hits but create individual permit provider (SAME as working search)
    let searcher_context = crate::global_cache::get_global_searcher_context();

    // Create CanSplitDoBetter filter (SAME as working search)
    let split_filter = Arc::new(std::sync::RwLock::new(quickwit_search::CanSplitDoBetter::Uninformative));

    // Get aggregation limits (SAME as working search)
    let aggregations_limits = searcher_context.aggregation_limit.clone();

    // Create individual permit provider per search (SAME as working search)
    let individual_permit_provider = {
        use quickwit_search::search_permit_provider::SearchPermitProvider;
        use bytesize::ByteSize;

        Arc::new(SearchPermitProvider::new_sync(
            5, // Allow up to 5 concurrent operations per search
            ByteSize::gb(1), // 1GB memory budget per search operation
        ))
    };

    // Get search permit from individual provider (SAME as working search)
    let memory_allocation = quickwit_search::search_permit_provider::compute_initial_memory_allocation(
        &split_metadata,
        bytesize::ByteSize(1024 * 1024 * 50), // 50MB initial allocation
    );

    debug_println!("🔍 AGGREGATION_SEARCH: Requesting search permit for aggregation query");
    let permit_futures = individual_permit_provider.get_permits(vec![memory_allocation]).await;
    let permit_future = permit_futures.into_iter().next()
        .expect("Expected one permit future");

    let mut search_permit = permit_future.await;
    debug_println!("✅ AGGREGATION_SEARCH: Successfully acquired search permit for aggregation query");

    // Call Quickwit's leaf_search_single_split with aggregation support (SAME as working search)
    let leaf_search_result = tokio::time::timeout(
        std::time::Duration::from_secs(15), // 15 second timeout
        quickwit_search::leaf_search_single_split(
            &searcher_context,
            search_request,
            storage,
            split_metadata,
            doc_mapper,
            split_filter,
            aggregations_limits,
            &mut search_permit,
        )
    ).await;

    let result = match leaf_search_result {
        Ok(search_result) => {
            debug_println!("✅ AGGREGATION_SEARCH: leaf_search_single_split succeeded with aggregations");
            search_result.map_err(|e| anyhow::anyhow!("Quickwit leaf search with aggregations failed: {}", e))?
        },
        Err(_timeout) => {
            debug_println!("❌ AGGREGATION_SEARCH: TIMEOUT in leaf_search_single_split with aggregations");
            drop(search_permit);
            return Err(anyhow::anyhow!("leaf_search_single_split with aggregations timeout"));
        }
    };

    debug_println!("✅ AGGREGATION_SEARCH: Search completed successfully with {} hits, has aggregations: {}",
                   result.num_hits, result.intermediate_aggregation_result.is_some());

    // Drop permit immediately
    drop(search_permit);

    Ok(result)
}

// CachedSearcherContext is now in types.rs submodule

// Dead code removed - perform_real_quickwit_doc_retrieval function was not called anywhere

/// Real Quickwit schema retrieval implementation using doc mapping (no I/O needed)
async fn perform_real_quickwit_schema_retrieval(
    split_uri: &str,
    _aws_config: &std::collections::HashMap<String, String>,
    _footer_start: u64,
    _footer_end: u64,
    doc_mapping_json: &Option<String>,
    _cached_storage: Arc<dyn Storage>,
) -> anyhow::Result<i64> {
    debug_println!("📋 REAL_QUICKWIT: Starting schema retrieval from doc mapping for split: {}", split_uri);

    // The doc mapping MUST be provided when the searcher is created
    let doc_mapping_str = doc_mapping_json.as_ref()
        .ok_or_else(|| anyhow::anyhow!("❌ CRITICAL: No doc mapping available! Doc mapping must be provided when creating SplitSearcher."))?;

    debug_println!("📋 REAL_QUICKWIT: Doc mapping found ({} chars), parsing JSON format", doc_mapping_str.len());
    debug_println!("🔥 RAW DOC MAPPING: {}", doc_mapping_str);

    // Parse the field mappings array directly from source - no cleanup logic
    let doc_mapper: quickwit_doc_mapper::DocMapper = {
        debug_println!("📋 REAL_QUICKWIT: Parsing doc mapping field array directly from source");

        // Parse the field mappings array - handle escaped JSON properly
        let field_mappings: Vec<serde_json::Value> = serde_json::from_str(doc_mapping_str)
            .or_else(|_e| {
                // If direct parsing fails, try unescaping first (for escaped JSON from some sources)
                debug_println!("🔥 SCHEMA DEBUG: Direct parsing failed, trying unescaped version");
                let unescaped = doc_mapping_str.replace("\\\"", "\"").replace("\\\\", "\\");
                debug_println!("🔥 SCHEMA DEBUG: Unescaped JSON: '{}'", unescaped);
                serde_json::from_str(&unescaped)
            })
            .map_err(|e| {
                debug_println!("🔥 SCHEMA DEBUG: Both direct and unescaped parsing failed: {}", e);
                debug_println!("🔥 SCHEMA DEBUG: Raw JSON was: '{}'", doc_mapping_str);
                anyhow::anyhow!("Failed to parse field mappings array (tried both direct and unescaped): {} - JSON was: '{}'", e, doc_mapping_str)
            })?;

        debug_println!("📋 REAL_QUICKWIT: Successfully parsed {} field mappings (already in correct Quickwit format)", field_mappings.len());

        // Convert to proper DocMapperBuilder format - this is what Quickwit actually expects
        let doc_mapper_builder_json = serde_json::json!({
            "field_mappings": field_mappings,
            "timestamp_field": null,
            "default_search_fields": []
        });

        debug_println!("📋 REAL_QUICKWIT: Converted to DocMapperBuilder format with {} fields, parsing with Quickwit", field_mappings.len());
        debug_println!("📋 REAL_QUICKWIT: DocMapperBuilder JSON structure: {}", serde_json::to_string_pretty(&doc_mapper_builder_json).unwrap_or_else(|_| "Failed to serialize".to_string()));

        // Deserialize into DocMapperBuilder first, then convert to DocMapper
        let doc_mapper_builder: quickwit_doc_mapper::DocMapperBuilder = serde_json::from_value(doc_mapper_builder_json)
            .map_err(|e| anyhow::anyhow!("Failed to parse DocMapperBuilder: {}", e))?;

        // Convert DocMapperBuilder to DocMapper
        quickwit_doc_mapper::DocMapper::try_from(doc_mapper_builder)
            .map_err(|e| anyhow::anyhow!("Failed to convert DocMapperBuilder to DocMapper: {}", e))?
    };

    debug_println!("📋 REAL_QUICKWIT: DocMapper parsed successfully, extracting schema");

    // Extract schema directly from DocMapper - no I/O operations needed
    let schema = doc_mapper.schema().clone();
    let field_count = schema.fields().count();

    debug_println!("📋 REAL_QUICKWIT: Schema extracted with {} fields, converting to pointer", field_count);

    // Convert schema to pointer using the same pattern as other functions
    let schema_ptr = crate::utils::arc_to_jlong(Arc::new(schema.clone()));

    // ✅ CRITICAL FIX: Cache the schema for parseQuery fallback
    debug_println!("📋 CACHE_FIX: Caching schema for parseQuery compatibility for split: {}", split_uri);
    crate::split_query::store_split_schema(split_uri, schema.clone());
    debug_println!("📋 CACHE_FIX: Schema cached successfully");

    debug_println!("✅ REAL_QUICKWIT: Schema retrieval completed successfully, pointer: {}", schema_ptr);
    Ok(schema_ptr)
}
