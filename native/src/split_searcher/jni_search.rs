// jni_search.rs - JNI search functions for SplitSearcher
// Extracted from mod.rs during refactoring
// Contains: searchWithQueryAst, searchWithSplitQuery, searchWithAggregations, getSchemaFromNative

use jni::objects::{JClass, JString, JObject};
use jni::sys::{jlong, jobject, jint};
use jni::JNIEnv;

use crate::debug_println;
use crate::common::to_java_exception;
use crate::runtime_manager::block_on_operation;
use crate::split_query::convert_split_query_to_json;
use super::types::CachedSearcherContext;
use super::aggregation::{
    convert_java_aggregations_to_json, perform_unified_search_result_creation,
};
use super::{perform_search_async_impl_leaf_response, perform_schema_retrieval_async_impl_thread_safe, perform_real_quickwit_search_with_aggregations};

/// Async-first method for Java_io_indextables_tantivy4java_split_SplitSearcher_searchWithQueryAst
/// This method uses the new async-first architecture to eliminate deadlocks
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_searchWithQueryAst(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    query_ast_json: JString,
    limit: jint,
) -> jobject {
    debug_println!("🚀 ASYNC_JNI: searchWithQueryAst called with async-first architecture");

    // Extract query JSON first (JNI types can't be sent across threads)
    let query_json: String = match env.get_string(&query_ast_json) {
        Ok(java_str) => java_str.into(),
        Err(e) => {
            debug_println!("❌ ASYNC_JNI: Failed to extract query JSON: {}", e);
            return std::ptr::null_mut();
        }
    };

    // Use async pattern that returns LeafSearchResponse directly (avoid unnecessary JSON marshalling)
    debug_println!("🔍 ASYNC_JNI: About to call perform_search_async_impl_leaf_response");
    match block_on_operation(async move {
        perform_search_async_impl_leaf_response(searcher_ptr, query_json, limit).await
    }) {
        Ok(leaf_search_response) => {
            debug_println!("✅ ASYNC_JNI: Got LeafSearchResponse, creating SearchResult object");
            // Create proper SearchResult object directly from LeafSearchResponse (no JSON marshalling)
            match perform_unified_search_result_creation(leaf_search_response, &mut env, None) {
                Ok(search_result_obj) => {
                    debug_println!("✅ ASYNC_JNI: Successfully created SearchResult object");
                    search_result_obj
                },
                Err(e) => {
                    debug_println!("❌ ASYNC_JNI: Failed to create SearchResult object: {}", e);
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to create SearchResult: {}", e));
                    std::ptr::null_mut()
                }
            }
        },
        Err(e) => {
            debug_println!("❌ ASYNC_JNI: Search operation failed: {}", e);

            // CRITICAL FIX: Throw proper exception instead of returning null
            // This ensures Java code gets a meaningful error message instead of NullPointerException
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

/// Method to search with SplitQuery objects using async-first pattern
/// This method follows Quickwit's cache management lifecycle
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_searchWithSplitQuery(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    split_query_obj: JObject,
    limit: jint,
) -> jobject {
    debug_println!("🚨 ENTRY_POINT: Java_io_indextables_tantivy4java_split_SplitSearcher_searchWithSplitQuery ENTRY");
    debug_println!("🚨 ENTRY_POINT: Function parameters - searcher_ptr: {}, limit: {}", searcher_ptr, limit);
    debug_println!("🚨 ENTRY_POINT: About to proceed with function body");
    debug_println!("🔥 NATIVE DEBUG: searchWithSplitQuery called with pointer {} and limit {}", searcher_ptr, limit);
    debug_println!("🚀 ASYNC_JNI: searchWithSplitQuery called with async-first architecture");

    // Extract all JNI data at entry point - no JNI types should go into core functions
    debug_println!("🔥 NATIVE DEBUG: Converting SplitQuery to JSON");
    let query_json_str = match convert_split_query_to_json(&mut env, &split_query_obj) {
        Ok(json_str) => {
            debug_println!("🔥 NATIVE DEBUG: Successfully converted SplitQuery to JSON: {}", json_str);
            json_str
        },
        Err(e) => {
            debug_println!("🔥 NATIVE DEBUG: Failed to convert SplitQuery to JSON: {}", e);
            debug_println!("❌ ASYNC_JNI: Failed to convert SplitQuery to JSON: {}", e);
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to convert SplitQuery to JSON: {}", e));
            return std::ptr::null_mut();
        }
    };

    // Use async pattern that returns LeafSearchResponse directly (avoid unnecessary JSON marshalling)
    // No JNI types passed to core functions - all data extracted at entry point
    debug_println!("🔥 NATIVE DEBUG: About to call block_on_operation with JSON: {}", query_json_str);
    debug_println!("🔍 ASYNC_JNI: About to call perform_search_async_impl_leaf_response (SplitQuery version)");
    debug_println!("🚨 CRITICAL: About to call block_on_operation - checking runtime context");

    // Check if we're in a Tokio runtime context before calling block_on
    if let Ok(_handle) = tokio::runtime::Handle::try_current() {
        debug_println!("❌ CRITICAL: WE ARE IN TOKIO RUNTIME CONTEXT - this will cause deadlock!");
        return std::ptr::null_mut();
    } else {
        debug_println!("✅ CRITICAL: Not in Tokio runtime context - safe to call block_on");
    }

    debug_println!("🚨 CRITICAL: Calling block_on_operation with async search operation");
    match block_on_operation(async move {
        debug_println!("🔍 ASYNC_START: Inside async block - about to call perform_search_async_impl_leaf_response");
        let result = perform_search_async_impl_leaf_response(searcher_ptr, query_json_str, limit).await;
        debug_println!("🔍 ASYNC_END: perform_search_async_impl_leaf_response completed");
        result
    }) {
        Ok(leaf_search_response) => {
            debug_println!("🔥 NATIVE DEBUG: block_on_operation SUCCESS - Got LeafSearchResponse from SplitQuery");
            debug_println!("✅ ASYNC_JNI: Got LeafSearchResponse from SplitQuery, creating SearchResult object");
            // Create proper SearchResult object directly from LeafSearchResponse (no JSON marshalling)
            match perform_unified_search_result_creation(leaf_search_response, &mut env, None) {
                Ok(search_result_obj) => {
                    debug_println!("🔥 NATIVE DEBUG: Successfully created SearchResult object from SplitQuery");
                    debug_println!("✅ ASYNC_JNI: Successfully created SearchResult object from SplitQuery");
                    search_result_obj
                },
                Err(e) => {
                    debug_println!("🔥 NATIVE DEBUG: Failed to create SearchResult object from SplitQuery: {}", e);
                    debug_println!("❌ ASYNC_JNI: Failed to create SearchResult object from SplitQuery: {}", e);
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to create SearchResult: {}", e));
                    std::ptr::null_mut()
                }
            }
        },
        Err(e) => {
            debug_println!("🔥 NATIVE DEBUG: block_on_operation FAILED: {}", e);
            debug_println!("❌ ASYNC_JNI: SplitQuery search operation failed: {}", e);

            // CRITICAL FIX: Throw proper exception instead of returning null
            // This ensures Java code gets a meaningful error message instead of NullPointerException
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

/// Search with aggregations support for SplitSearcher
/// This method combines regular search with statistical aggregations
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_searchWithAggregations<'local>(
    mut env: JNIEnv<'local>,
    _obj: JObject<'local>,
    searcher_ptr: jlong,
    split_query: JObject<'local>,
    limit: jint,
    aggregations_map: JObject<'local>,
) -> jobject {
    let method_start_time = std::time::Instant::now();
    debug_println!("🚀 RUST NATIVE: searchWithAggregations ENTRY - Real aggregation processing starting");

    if searcher_ptr == 0 {
        debug_println!("RUST DEBUG: ⏱️ searchWithAggregations ERROR: Invalid searcher pointer [TIMING: {}ms]", method_start_time.elapsed().as_millis());
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
        return std::ptr::null_mut();
    }
    debug_println!("RUST DEBUG: searcher_ptr validation passed: {}", searcher_ptr);

    // Convert SplitQuery to QueryAst JSON (using existing infrastructure)
    let query_json_result = convert_split_query_to_json(&mut env, &split_query);
    let query_json = match query_json_result {
        Ok(json) => {
            debug_println!("RUST DEBUG: Query conversion successful");
            json
        },
        Err(e) => {
            debug_println!("RUST DEBUG: ⏱️ searchWithAggregations ERROR: Failed to convert SplitQuery [TIMING: {}ms]", method_start_time.elapsed().as_millis());
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to convert SplitQuery to JSON: {}", e));
            return std::ptr::null_mut();
        }
    };

    // Convert Java aggregations map to JSON
    debug_println!("RUST DEBUG: Starting aggregation conversion");
    let aggregation_request_json = match convert_java_aggregations_to_json(&mut env, &aggregations_map) {
        Ok(agg_json) => {
            debug_println!("RUST DEBUG: Aggregation conversion successful");
            agg_json
        },
        Err(e) => {
            debug_println!("RUST DEBUG: ⏱️ searchWithAggregations ERROR: Failed to convert aggregations to JSON [TIMING: {}ms]", method_start_time.elapsed().as_millis());
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to convert aggregations to JSON: {}", e));
            return std::ptr::null_mut();
        }
    };

    debug_println!("RUST DEBUG: Query JSON: {}", query_json);
    if let Some(ref agg_json) = aggregation_request_json {
        debug_println!("RUST DEBUG: Aggregation JSON: {}", agg_json);
    } else {
        debug_println!("RUST DEBUG: No aggregation JSON - aggregation_request_json is None");
    }

    // Use block_on_operation to perform the async search with aggregations
    let searcher_ptr_copy = searcher_ptr;
    let limit_copy = limit as usize;
    let aggregation_request_json_copy = aggregation_request_json.clone();
    match block_on_operation(async move {
        perform_search_async_impl_leaf_response_with_aggregations(searcher_ptr_copy, query_json, limit_copy, aggregation_request_json_copy).await
    }) {
        Ok(leaf_search_response) => {
            debug_println!("RUST DEBUG: ⏱️ searchWithAggregations SUCCESS [TIMING: {}ms]", method_start_time.elapsed().as_millis());
            debug_println!("RUST DEBUG: Found {} hits, has aggregations: {}",
                         leaf_search_response.num_hits,
                         leaf_search_response.intermediate_aggregation_result.is_some());

            match perform_unified_search_result_creation(leaf_search_response, &mut env, aggregation_request_json.clone()) {
                Ok(search_result_obj) => search_result_obj,
                Err(e) => {
                    debug_println!("RUST DEBUG: ⏱️ searchWithAggregations ERROR: Failed to create SearchResult [TIMING: {}ms]: {}", method_start_time.elapsed().as_millis(), e);
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

/// Async implementation for search with aggregations
pub async fn perform_search_async_impl_leaf_response_with_aggregations(
    searcher_ptr: jlong,
    query_json: String,
    limit: usize,
    aggregation_request_json: Option<String>,
) -> anyhow::Result<quickwit_proto::search::LeafSearchResponse> {
    debug_println!("🔍 ASYNC_JNI: Starting search with aggregations using working pattern");

    if searcher_ptr == 0 {
        return Err(anyhow::anyhow!("Invalid searcher pointer"));
    }

    // Extract searcher context using the SAME pattern as working search
    let searcher_context = crate::utils::jlong_to_arc::<CachedSearcherContext>(searcher_ptr)
        .ok_or_else(|| anyhow::anyhow!("Invalid searcher context"))?;

    let context = searcher_context.as_ref();

    debug_println!("🔍 ASYNC_JNI: Extracted searcher context, performing search with aggregations on split: {}", context.split_uri);

    // Lazy transcoding: ensure fast fields needed by aggregation + range queries are transcoded
    let overrides = if context.augmented_directory.is_some() {
        super::async_impl::ensure_fast_fields_for_query(
            context,
            &query_json,
            aggregation_request_json.as_deref(),
        ).await?
    } else {
        context.split_overrides.as_ref().map(|o| quickwit_search::SplitOverrides {
            meta_json: o.meta_json.clone(),
            fast_field_data: o.fast_field_data.clone(),
        })
    };

    // Use the SAME working search functionality but with aggregations
    let search_result = perform_real_quickwit_search_with_aggregations(
        &context.split_uri,
        &context.aws_config,
        context.footer_start,
        context.footer_end,
        &context.doc_mapping_json,
        context.cached_storage.clone(),
        context.cached_searcher.clone(),
        context.cached_index.clone(),
        &query_json,
        limit,
        aggregation_request_json,
        overrides,
    ).await?;

    debug_println!("✅ ASYNC_JNI: Search with aggregations completed successfully with {} hits", search_result.num_hits);
    Ok(search_result)
}

/// Async-first replacement for Java_io_indextables_tantivy4java_split_SplitSearcher_getSchemaFromNative
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_getSchemaFromNative(
    _env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) -> jlong {
    debug_println!("🔥 NATIVE: getSchemaFromNative called with pointer: {}", searcher_ptr);

    if searcher_ptr == 0 {
        debug_println!("❌ NATIVE: Invalid searcher pointer (0)");
        return 0;
    }

    debug_println!("🔥 NATIVE: About to call block_on_operation");

    // Use simplified async pattern that returns thread-safe types
    // Note: env cannot be moved into async block due to thread safety
    match block_on_operation(async move {
        debug_println!("🔥 NATIVE: Inside async block, calling perform_schema_retrieval_async_impl_thread_safe");
        perform_schema_retrieval_async_impl_thread_safe(searcher_ptr).await
    }) {
        Ok(result) => {
            debug_println!("✅ NATIVE: block_on_operation succeeded, result: {}", result);
            result
        },
        Err(e) => {
            debug_println!("❌ NATIVE: block_on_operation FAILED: {}", e);
            0
        }
    }
}
