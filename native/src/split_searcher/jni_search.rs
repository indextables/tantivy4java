// jni_search.rs - JNI search functions for SplitSearcher
// Extracted from mod.rs during refactoring
// Contains: searchWithQueryAst, searchWithSplitQuery, searchWithAggregations, getSchemaFromNative

use std::collections::{HashMap, HashSet};

use jni::objects::{JClass, JString, JObject};
use jni::sys::{jlong, jobject, jint};
use jni::JNIEnv;

use crate::debug_println;
use crate::perf_println;
use crate::common::to_java_exception;
use crate::runtime_manager::block_on_operation;
use crate::split_query::convert_split_query_to_json;
use super::types::CachedSearcherContext;
use super::aggregation::{
    convert_java_aggregations_to_json, perform_unified_search_result_creation,
};
use super::{perform_search_async_impl_leaf_response, perform_schema_retrieval_async_impl_thread_safe, perform_real_quickwit_search_with_aggregations};

/// Result of `perform_search_async_impl_leaf_response_with_aggregations`, bundling
/// the Quickwit leaf response with optional hash-field touchup context (Phase 2/3).
pub struct SearchWithHashContext {
    pub leaf_response: quickwit_proto::search::LeafSearchResponse,
    /// Aggregation JSON actually used for the search (may be rewritten from original).
    pub effective_agg_json: Option<String>,
    /// Aggregation names whose field was redirected to a `_phash_*` hash field.
    pub redirected_hash_agg_names: Option<HashSet<String>>,
    /// Hash value → original string resolution map (populated after Phase 3 touchup).
    pub hash_resolution_map: Option<HashMap<u64, String>>,
    /// Touchup infos from Phase 2 (include/exclude string filters for post-processing).
    pub touchup_infos: Option<Vec<crate::parquet_companion::hash_field_rewriter::HashFieldTouchupInfo>>,
}

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
    let t_total = std::time::Instant::now();
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
    perf_println!("⏱️ PROJ_DIAG: === searchWithQueryAst START === limit={}", limit);
    match block_on_operation(async move {
        perform_search_async_impl_leaf_response(searcher_ptr, query_json, limit).await
    }) {
        Ok(leaf_search_response) => {
            let t_search = t_total.elapsed();
            perf_println!("⏱️ PROJ_DIAG: searchWithQueryAst async search took {}ms, {} hits",
                t_search.as_millis(), leaf_search_response.num_hits);
            debug_println!("✅ ASYNC_JNI: Got LeafSearchResponse, creating SearchResult object");
            let t_result = std::time::Instant::now();
            // Create proper SearchResult object directly from LeafSearchResponse (no JSON marshalling)
            match perform_unified_search_result_creation(leaf_search_response, &mut env, None, None, None, None) {
                Ok(search_result_obj) => {
                    perf_println!("⏱️ PROJ_DIAG: === searchWithQueryAst TOTAL {}ms (search={}ms, result_creation={}ms) ===",
                        t_total.elapsed().as_millis(), t_search.as_millis(), t_result.elapsed().as_millis());
                    debug_println!("✅ ASYNC_JNI: Successfully created SearchResult object");
                    search_result_obj
                },
                Err(e) => {
                    perf_println!("⏱️ PROJ_DIAG: === searchWithQueryAst FAILED after {}ms: {} ===",
                        t_total.elapsed().as_millis(), e);
                    debug_println!("❌ ASYNC_JNI: Failed to create SearchResult object: {}", e);
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to create SearchResult: {}", e));
                    std::ptr::null_mut()
                }
            }
        },
        Err(e) => {
            perf_println!("⏱️ PROJ_DIAG: === searchWithQueryAst FAILED after {}ms: {} ===",
                t_total.elapsed().as_millis(), e);
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
    let t_total = std::time::Instant::now();
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
    perf_println!("⏱️ PROJ_DIAG: === searchWithSplitQuery START === limit={}", limit);
    let t_query_convert = t_total.elapsed();
    perf_println!("⏱️ PROJ_DIAG: searchWithSplitQuery query JSON conversion took {}ms", t_query_convert.as_millis());
    match block_on_operation(async move {
        debug_println!("🔍 ASYNC_START: Inside async block - about to call perform_search_async_impl_leaf_response");
        let result = perform_search_async_impl_leaf_response(searcher_ptr, query_json_str, limit).await;
        debug_println!("🔍 ASYNC_END: perform_search_async_impl_leaf_response completed");
        result
    }) {
        Ok(leaf_search_response) => {
            let t_search = t_total.elapsed();
            perf_println!("⏱️ PROJ_DIAG: searchWithSplitQuery async search took {}ms, {} hits",
                t_search.as_millis(), leaf_search_response.num_hits);
            debug_println!("🔥 NATIVE DEBUG: block_on_operation SUCCESS - Got LeafSearchResponse from SplitQuery");
            debug_println!("✅ ASYNC_JNI: Got LeafSearchResponse from SplitQuery, creating SearchResult object");
            let t_result = std::time::Instant::now();
            // Create proper SearchResult object directly from LeafSearchResponse (no JSON marshalling)
            match perform_unified_search_result_creation(leaf_search_response, &mut env, None, None, None, None) {
                Ok(search_result_obj) => {
                    perf_println!("⏱️ PROJ_DIAG: === searchWithSplitQuery TOTAL {}ms (query_convert={}ms, search={}ms, result_creation={}ms) ===",
                        t_total.elapsed().as_millis(), t_query_convert.as_millis(), t_search.as_millis() - t_query_convert.as_millis(), t_result.elapsed().as_millis());
                    debug_println!("🔥 NATIVE DEBUG: Successfully created SearchResult object from SplitQuery");
                    debug_println!("✅ ASYNC_JNI: Successfully created SearchResult object from SplitQuery");
                    search_result_obj
                },
                Err(e) => {
                    perf_println!("⏱️ PROJ_DIAG: === searchWithSplitQuery FAILED after {}ms: {} ===",
                        t_total.elapsed().as_millis(), e);
                    debug_println!("🔥 NATIVE DEBUG: Failed to create SearchResult object from SplitQuery: {}", e);
                    debug_println!("❌ ASYNC_JNI: Failed to create SearchResult object from SplitQuery: {}", e);
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to create SearchResult: {}", e));
                    std::ptr::null_mut()
                }
            }
        },
        Err(e) => {
            perf_println!("⏱️ PROJ_DIAG: === searchWithSplitQuery FAILED after {}ms: {} ===",
                t_total.elapsed().as_millis(), e);
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
    perf_println!("⏱️ PROJ_DIAG: === searchWithAggregations START ===");
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
    let t_jni_prep = method_start_time.elapsed();
    perf_println!("⏱️ PROJ_DIAG: searchWithAggregations JNI prep (query+agg conversion) took {}ms", t_jni_prep.as_millis());
    match block_on_operation(async move {
        perform_search_async_impl_leaf_response_with_aggregations(searcher_ptr_copy, query_json, limit_copy, aggregation_request_json_copy).await
    }) {
        Ok(ctx) => {
            let t_search = method_start_time.elapsed();
            perf_println!("⏱️ PROJ_DIAG: searchWithAggregations async search took {}ms, {} hits, has_aggs={}",
                t_search.as_millis(), ctx.leaf_response.num_hits,
                ctx.leaf_response.intermediate_aggregation_result.is_some());
            debug_println!("RUST DEBUG: ⏱️ searchWithAggregations SUCCESS [TIMING: {}ms]", method_start_time.elapsed().as_millis());
            debug_println!("RUST DEBUG: Found {} hits, has aggregations: {}",
                         ctx.leaf_response.num_hits,
                         ctx.leaf_response.intermediate_aggregation_result.is_some());

            let t_result = std::time::Instant::now();
            match perform_unified_search_result_creation(
                ctx.leaf_response,
                &mut env,
                ctx.effective_agg_json,
                ctx.redirected_hash_agg_names,
                ctx.hash_resolution_map,
                ctx.touchup_infos,
            ) {
                Ok(search_result_obj) => {
                    perf_println!("⏱️ PROJ_DIAG: === searchWithAggregations TOTAL {}ms (jni_prep={}ms, search={}ms, result_creation={}ms) ===",
                        method_start_time.elapsed().as_millis(), t_jni_prep.as_millis(),
                        t_search.as_millis() - t_jni_prep.as_millis(), t_result.elapsed().as_millis());
                    search_result_obj
                }
                Err(e) => {
                    perf_println!("⏱️ PROJ_DIAG: === searchWithAggregations FAILED after {}ms: {} ===",
                        method_start_time.elapsed().as_millis(), e);
                    debug_println!("RUST DEBUG: ⏱️ searchWithAggregations ERROR: Failed to create SearchResult [TIMING: {}ms]: {}", method_start_time.elapsed().as_millis(), e);
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to create SearchResult: {}", e));
                    std::ptr::null_mut()
                }
            }
        }
        Err(e) => {
            perf_println!("⏱️ PROJ_DIAG: === searchWithAggregations FAILED after {}ms: {} ===",
                method_start_time.elapsed().as_millis(), e);
            debug_println!("RUST DEBUG: ⏱️ searchWithAggregations ERROR: Search failed [TIMING: {}ms]: {}", method_start_time.elapsed().as_millis(), e);
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

/// Async implementation for search with aggregations.
/// Returns a `SearchWithHashContext` which bundles the Quickwit leaf response with
/// optional Phase 2/3 hash-field touchup data (see `hash_field_rewriter` and `hash_touchup`).
pub async fn perform_search_async_impl_leaf_response_with_aggregations(
    searcher_ptr: jlong,
    query_json: String,
    limit: usize,
    aggregation_request_json: Option<String>,
) -> anyhow::Result<SearchWithHashContext> {
    let t_total = std::time::Instant::now();
    perf_println!("⏱️ PROJ_DIAG: perform_search_with_aggregations START — limit={}, has_agg={}",
        limit, aggregation_request_json.is_some());
    debug_println!("🔍 ASYNC_JNI: Starting search with aggregations using working pattern");

    if searcher_ptr == 0 {
        return Err(anyhow::anyhow!("Invalid searcher pointer"));
    }

    // Extract searcher context using the SAME pattern as working search
    let searcher_context = crate::utils::jlong_to_arc::<CachedSearcherContext>(searcher_ptr)
        .ok_or_else(|| anyhow::anyhow!("Invalid searcher context"))?;

    let context = searcher_context.as_ref();

    debug_println!("🔍 ASYNC_JNI: Extracted searcher context, performing search with aggregations on split: {}", context.split_uri);

    // Phase 2a: Rewrite query JSON to redirect FieldPresence (exists) queries on string
    // hash fields to the native _phash_* U64 field, avoiding string column transcoding.
    let effective_query_json = if let Some(ref manifest) = context.parquet_manifest {
        if !manifest.string_hash_fields.is_empty() {
            if let Some(rewritten) = crate::parquet_companion::hash_field_rewriter::rewrite_query_for_hash_fields(
                &query_json,
                &manifest.string_hash_fields,
            ) {
                debug_println!(
                    "📊 HASH_OPT: Rewrote query FieldPresence to hash field(s)"
                );
                rewritten
            } else {
                query_json.clone()
            }
        } else {
            query_json.clone()
        }
    } else {
        query_json.clone()
    };

    // Phase 2a2: Rewrite term queries for compact string indexing modes.
    // Also blocks unsupported query types (wildcard, phrase, regex) on exact_only fields.
    let effective_query_json = if let Some(ref manifest) = context.parquet_manifest {
        if !manifest.string_indexing_modes.is_empty() {
            match crate::parquet_companion::hash_field_rewriter::rewrite_query_for_string_indexing(
                &effective_query_json,
                &manifest.string_indexing_modes,
            )? {
                Some(rewritten) => {
                    debug_println!("📊 STRING_IDX: Rewrote query for compact string indexing mode(s)");
                    rewritten
                }
                None => effective_query_json,
            }
        } else {
            effective_query_json
        }
    } else {
        effective_query_json
    };

    // Phase 2b: Rewrite aggregation JSON to replace string field references with _phash_* fields.
    // This avoids expensive parquet transcoding for terms/value_count/cardinality aggregations.
    let (effective_agg_json, rewrite_output) =
        if let (Some(ref agg_json), Some(ref manifest)) =
            (&aggregation_request_json, &context.parquet_manifest)
        {
            if !manifest.string_hash_fields.is_empty() {
                match crate::parquet_companion::hash_field_rewriter::rewrite_aggs_for_hash_fields(
                    agg_json,
                    &manifest.string_hash_fields,
                ) {
                    Ok(output) => {
                        if !output.touchup_infos.is_empty() {
                            debug_println!(
                                "📊 HASH_OPT: Rewrote {} agg(s) to hash fields",
                                output.touchup_infos.len()
                            );
                        }
                        let rewritten = output.rewritten_json.clone();
                        (Some(rewritten), Some(output))
                    }
                    Err(e) => {
                        debug_println!(
                            "⚠️ HASH_OPT: Failed to rewrite agg JSON: {} — using original",
                            e
                        );
                        (aggregation_request_json.clone(), None)
                    }
                }
            } else {
                (aggregation_request_json.clone(), None)
            }
        } else {
            (aggregation_request_json.clone(), None)
        };

    // Lazy transcoding: ensure fast fields needed by aggregation + range queries are transcoded.
    // Use the rewritten query/agg JSON so that hash fields (already native) are not transcoded from parquet.
    let t_rewrite = t_total.elapsed();
    perf_println!("⏱️ PROJ_DIAG: perform_search_with_aggregations query/agg rewriting took {}ms", t_rewrite.as_millis());
    let t_transcode = std::time::Instant::now();
    let overrides = if context.augmented_directory.is_some() {
        super::async_impl::ensure_fast_fields_for_query(
            context,
            &effective_query_json,
            effective_agg_json.as_deref(),
        ).await?
    } else {
        context.split_overrides.as_ref().map(|o| quickwit_search::SplitOverrides {
            meta_json: o.meta_json.clone(),
            fast_field_data: o.fast_field_data.clone(),
        })
    };
    perf_println!("⏱️ PROJ_DIAG: perform_search_with_aggregations ensure_fast_fields took {}ms",
        t_transcode.elapsed().as_millis());

    // Perform the search using the (possibly rewritten) query and aggregation JSON
    let t_leaf = std::time::Instant::now();
    let leaf_response = perform_real_quickwit_search_with_aggregations(
        &context.split_uri,
        &context.aws_config,
        context.footer_start,
        context.footer_end,
        &context.doc_mapping_json,
        context.cached_storage.clone(),
        context.cached_searcher.clone(),
        context.cached_index.clone(),
        &effective_query_json,
        limit,
        effective_agg_json.clone(),
        overrides,
    ).await?;
    perf_println!("⏱️ PROJ_DIAG: perform_search_with_aggregations leaf_search took {}ms, {} hits",
        t_leaf.elapsed().as_millis(), leaf_response.num_hits);

    debug_println!(
        "✅ ASYNC_JNI: Search with aggregations completed successfully with {} hits",
        leaf_response.num_hits
    );

    // Phase 3: Build hash → string resolution map for any redirected terms aggregations.
    let t_phase3 = std::time::Instant::now();
    let (redirected_hash_agg_names, hash_resolution_map) =
        if let Some(ref output) = rewrite_output {
            if !output.touchup_infos.is_empty()
                && leaf_response.intermediate_aggregation_result.is_some()
            {
                match crate::parquet_companion::hash_touchup::build_hash_resolution_map(
                    &output.touchup_infos,
                    &leaf_response,
                    effective_agg_json.as_deref().unwrap_or(""),
                    &context.cached_index,
                    &context.cached_searcher,
                    &context.split_uri,
                    &context.cached_storage,
                    context.parquet_manifest.as_ref(),
                    context.parquet_storage.as_ref(),
                    &context.parquet_metadata_cache,
                    &context.parquet_byte_range_cache,
                    &context.parquet_file_hash_index,
                    &context.pq_columns,
                )
                .await
                {
                    Ok(resolution_map) => {
                        let redirected: HashSet<String> = output
                            .touchup_infos
                            .iter()
                            .map(|t| t.agg_name.clone())
                            .collect();
                        debug_println!(
                            "✅ HASH_OPT: Phase 3 resolved {} hash values",
                            resolution_map.len()
                        );
                        (Some(redirected), Some(resolution_map))
                    }
                    Err(e) => {
                        debug_println!(
                            "⚠️ HASH_OPT: Phase 3 failed to build resolution map: {}",
                            e
                        );
                        (None, None)
                    }
                }
            } else {
                (None, None)
            }
        } else {
            (None, None)
        };

    perf_println!("⏱️ PROJ_DIAG: perform_search_with_aggregations phase3 hash_resolution took {}ms",
        t_phase3.elapsed().as_millis());

    // Extract touchup_infos from rewrite_output (if any) for include/exclude post-filtering
    let touchup_infos = rewrite_output.map(|o| o.touchup_infos);

    perf_println!("⏱️ PROJ_DIAG: perform_search_with_aggregations TOTAL {}ms (rewrite={}ms, transcode={}ms, leaf={}ms, phase3={}ms)",
        t_total.elapsed().as_millis(), t_rewrite.as_millis(), t_transcode.elapsed().as_millis(),
        t_leaf.elapsed().as_millis(), t_phase3.elapsed().as_millis());

    Ok(SearchWithHashContext {
        leaf_response,
        effective_agg_json,
        redirected_hash_agg_names,
        hash_resolution_map,
        touchup_infos,
    })
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
