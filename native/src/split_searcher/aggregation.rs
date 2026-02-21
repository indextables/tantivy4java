// aggregation.rs - Quickwit aggregation integration for split search
// Extracted from mod.rs during refactoring

use std::sync::Arc;
use jni::objects::JObject;
use jni::sys::jlong;
use jni::JNIEnv;

use crate::debug_println;
use crate::utils::{arc_to_jlong, with_arc_safe};
use crate::standalone_searcher::SplitSearchMetadata;
use super::types::{CachedSearcherContext, EnhancedSearchResult};
use quickwit_proto::search::SearchRequest;

// ============================================================================
// QUICKWIT AGGREGATION INTEGRATION (USING PROVEN SYSTEM)
// ============================================================================

/// Convert Java aggregations map to JSON for Quickwit's SearchRequest.aggregation_request field
pub(crate) fn convert_java_aggregations_to_json<'a>(
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
pub(crate) fn perform_search_with_quickwit_aggregations(
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

    let result = with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
        let context = searcher_context.as_ref();
        let searcher = &context.standalone_searcher;
        // CRITICAL FIX: Use shared global runtime handle instead of context.runtime
        let runtime = crate::runtime_manager::QuickwitRuntimeManager::global().handle();
        let split_uri = &context.split_uri;
        let footer_start = context.footer_start;
        let footer_end = context.footer_end;
        let cached_index = &context.cached_index;

        // Enter the runtime context for async operations
        let _guard = runtime.enter();

        // Run the EXACT same async code as the working search
        runtime.block_on(async {
            // Parse the QueryAst JSON using Quickwit's libraries - IDENTICAL TO WORKING SEARCH
            use quickwit_query::query_ast::QueryAst;

            let query_ast: QueryAst = serde_json::from_str(&query_json)
                .map_err(|e| anyhow::anyhow!("Failed to parse QueryAst JSON: {}", e))?;

            debug_println!("RUST DEBUG: Successfully parsed QueryAst: {:?}", query_ast);

            // STORAGE SETUP - IDENTICAL TO WORKING SEARCH
            let storage_setup_start = std::time::Instant::now();
            debug_println!("RUST DEBUG: ⏱️ 🔧 SEARCH STORAGE SETUP - Starting storage resolution for: {}", split_uri);

            debug_println!("RUST DEBUG: ⏱️ 🔧 SEARCH STORAGE SETUP completed [TIMING: {}ms]", storage_setup_start.elapsed().as_millis());
            debug_println!("RUST DEBUG: 🚀 Using Quickwit optimized path - NO full file download");
            debug_println!("RUST DEBUG: Footer offsets from Java config: start={}, end={}", footer_start, footer_end);

            // Extract the relative path from the split URI
            let relative_path = if let Some(last_slash_pos) = split_uri.rfind('/') {
                std::path::Path::new(&split_uri[last_slash_pos + 1..])
            } else {
                std::path::Path::new(split_uri)
            };

            // Use context footer offsets for split metadata
            let split_footer_start = footer_start;
            let split_footer_end = footer_end;

            // Use cached index to eliminate repeated open_index_with_caches calls - OPTIMAL PERFORMANCE
            let index = cached_index.as_ref().clone();
            debug_println!("🔥 INDEX CACHED: Reusing cached index for aggregation search instead of expensive open_index_with_caches call");

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

/// Unified function to create SearchResult Java object from LeafSearchResponse
pub(crate) fn perform_unified_search_result_creation(
    leaf_search_response: quickwit_proto::search::LeafSearchResponse,
    env: &mut JNIEnv,
    aggregation_request_json: Option<String>,
    redirected_hash_agg_names: Option<std::collections::HashSet<String>>,
    hash_resolution_map: Option<std::collections::HashMap<u64, String>>,
    hash_agg_touchup_infos: Option<Vec<crate::parquet_companion::hash_field_rewriter::HashFieldTouchupInfo>>,
) -> anyhow::Result<jni::sys::jobject> {
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
        aggregation_json: aggregation_request_json.clone(),
        redirected_hash_agg_names,
        hash_resolution_map,
        hash_agg_touchup_infos,
    };

    let search_results_arc = Arc::new(enhanced_result);
    let search_result_ptr = arc_to_jlong(search_results_arc);

    // Create SearchResult Java object
    let search_result_class = env.find_class("io/indextables/tantivy4java/result/SearchResult")
        .map_err(|e| anyhow::anyhow!("Failed to find SearchResult class: {}", e))?;

    let search_result = env.new_object(
        &search_result_class,
        "(J)V",
        &[(search_result_ptr).into()]
    ).map_err(|e| anyhow::anyhow!("Failed to create SearchResult: {}", e))?;

    debug_println!("RUST DEBUG: Successfully created SearchResult with {} hits", leaf_search_response.num_hits);

    Ok(search_result.into_raw())
}
