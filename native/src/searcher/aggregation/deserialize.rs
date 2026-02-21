// deserialize.rs - Main aggregation deserialization functions
// Extracted from aggregation.rs during refactoring

use jni::objects::JObject;
use jni::sys::{jlong, jobject};
use jni::JNIEnv;
use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::agg_result::AggregationResults;
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::aggregation::AggregationLimitsGuard;

use crate::debug_println;
use crate::utils::with_arc_safe;

use super::json_helpers::is_date_histogram_aggregation;
use super::sub_aggregations::create_java_aggregation_from_final_result;

/// Deserialize aggregation results to Java HashMap using proper Quickwit deserialization
/// This handles real Quickwit aggregation results from LeafSearchResponse
pub(crate) fn deserialize_aggregation_results(
    env: &mut JNIEnv,
    intermediate_agg_bytes: &[u8],
    ptr: jlong,
) -> anyhow::Result<jobject> {
    debug_println!(
        "RUST DEBUG: deserialize_aggregation_results called with {} bytes",
        intermediate_agg_bytes.len()
    );

    // Get the aggregation request JSON and optional hash-touchup context from the enhanced result
    let (aggregation_json, hash_resolution_map, redirected_hash_agg_names) =
        with_arc_safe::<crate::split_searcher::EnhancedSearchResult, _>(ptr, |r| {
            (
                r.aggregation_json.clone().unwrap_or_default(),
                r.hash_resolution_map.clone(),
                r.redirected_hash_agg_names.clone(),
            )
        })
        .unwrap_or_else(|| (String::new(), None, None));
    let aggregation_json = aggregation_json;

    // If we don't have the aggregation JSON, we can't properly deserialize
    if aggregation_json.is_empty() {
        debug_println!("RUST DEBUG: No aggregation JSON available, using fallback approach");
        return create_fallback_aggregation_results(env, intermediate_agg_bytes);
    }

    debug_println!(
        "RUST DEBUG: Using aggregation JSON: {}",
        aggregation_json
    );

    // Use the proper Quickwit deserialization approach (same as find_specific_aggregation_result)

    // Step 1: Parse aggregation request from JSON
    // The aggregation_json contains wrapped format like {"agg_0":{"terms":{"field":"status","size":1000}}}
    // But Tantivy expects unwrapped format like {"terms":{"field":"status","size":1000}}
    debug_println!(
        "RUST DEBUG: Parsing aggregation JSON: {}",
        aggregation_json
    );

    let aggregations: Aggregations = if aggregation_json.starts_with('{') {
        // Parse as a JSON object to extract the inner aggregation structures
        let json_value: serde_json::Value = serde_json::from_str(&aggregation_json)?;
        if let Some(obj) = json_value.as_object() {
            // Convert the wrapped format to unwrapped format that Tantivy expects
            let mut unwrapped = serde_json::Map::new();
            for (key, value) in obj {
                // Each value should be the actual aggregation definition
                unwrapped.insert(key.clone(), value.clone());
            }
            let unwrapped_json = serde_json::to_string(&unwrapped)?;
            debug_println!(
                "RUST DEBUG: Unwrapped aggregation JSON: {}",
                unwrapped_json
            );
            serde_json::from_str(&unwrapped_json)?
        } else {
            serde_json::from_str(&aggregation_json)?
        }
    } else {
        serde_json::from_str(&aggregation_json)?
    };

    // Step 2: Deserialize binary data to intermediate results using postcard
    let intermediate_results: IntermediateAggregationResults =
        postcard::from_bytes(intermediate_agg_bytes)?;

    // Step 3: Create aggregation limits (using reasonable defaults like Quickwit)
    let aggregation_limits = AggregationLimitsGuard::new(Some(50_000_000), Some(65_000));

    // Step 4: Convert to final results using Quickwit's proven method
    let final_results: AggregationResults =
        intermediate_results.into_final_result(aggregations, aggregation_limits)?;

    // Step 5: Create HashMap and convert all aggregations to Java objects
    let hashmap = env
        .new_object("java/util/HashMap", "()V", &[])
        .map_err(|e| anyhow::anyhow!("Failed to create HashMap: {}", e))?;

    debug_println!(
        "RUST DEBUG: Processing {} aggregation results",
        final_results.0.len()
    );

    for (agg_name, agg_result) in final_results.0 {
        debug_println!("RUST DEBUG: Processing aggregation '{}'", agg_name);

        // Check if this aggregation is a date_histogram based on the original request
        let is_date_histogram_hint = is_date_histogram_aggregation(&aggregation_json, &agg_name);

        match create_java_aggregation_from_final_result(
            env,
            &agg_name,
            &agg_result,
            is_date_histogram_hint,
            hash_resolution_map.as_ref(),
            redirected_hash_agg_names.as_ref(),
            None, // include_filter: not available in this deserialization path
            None, // exclude_filter: not available in this deserialization path
        ) {
            Ok(java_obj) => {
                if !java_obj.is_null() {
                    let name_string = env.new_string(&agg_name)?;
                    env.call_method(
                        &hashmap,
                        "put",
                        "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
                        &[
                            (&name_string).into(),
                            (&unsafe { JObject::from_raw(java_obj) }).into(),
                        ],
                    )?;
                    debug_println!("RUST DEBUG: Added aggregation '{}' to HashMap", agg_name);
                } else {
                    debug_println!(
                        "RUST DEBUG: Skipping null result for aggregation '{}'",
                        agg_name
                    );
                }
            }
            Err(e) => {
                debug_println!(
                    "RUST DEBUG: Failed to create Java object for aggregation '{}': {}",
                    agg_name,
                    e
                );
            }
        }
    }

    Ok(hashmap.into_raw())
}

/// Fallback aggregation results creation when proper deserialization isn't possible
fn create_fallback_aggregation_results(
    env: &mut JNIEnv,
    _intermediate_agg_bytes: &[u8],
) -> anyhow::Result<jobject> {
    debug_println!("RUST DEBUG: Creating fallback aggregation results");

    // Create HashMap for results
    let hashmap = env
        .new_object("java/util/HashMap", "()V", &[])
        .map_err(|e| anyhow::anyhow!("Failed to create HashMap: {}", e))?;

    // For now, create a simple mock result to test the Java integration
    let name_string = env
        .new_string("test_count")
        .map_err(|e| anyhow::anyhow!("Failed to create test name string: {}", e))?;

    let count_class = env
        .find_class("io/indextables/tantivy4java/aggregation/CountResult")
        .map_err(|e| anyhow::anyhow!("Failed to find CountResult class: {}", e))?;

    let mock_count = env
        .new_object(
            &count_class,
            "(Ljava/lang/String;J)V",
            &[(&name_string).into(), (5 as jlong).into()],
        )
        .map_err(|e| anyhow::anyhow!("Failed to create mock CountResult: {}", e))?;

    env.call_method(
        &hashmap,
        "put",
        "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
        &[(&name_string).into(), (&mock_count).into()],
    )
    .map_err(|e| anyhow::anyhow!("Failed to add mock result to HashMap: {}", e))?;

    debug_println!("RUST DEBUG: Added mock CountResult to HashMap");

    Ok(hashmap.into_raw())
}

/// Find and convert a specific aggregation result by name
pub(crate) fn find_specific_aggregation_result(
    env: &mut JNIEnv,
    intermediate_agg_bytes: &[u8],
    aggregation_name: &str,
    aggregation_json: &str,
    hash_resolution_map: Option<&std::collections::HashMap<u64, String>>,
    redirected_hash_agg_names: Option<&std::collections::HashSet<String>>,
    touchup_infos: Option<&[crate::parquet_companion::hash_field_rewriter::HashFieldTouchupInfo]>,
) -> anyhow::Result<jobject> {
    debug_println!(
        "RUST DEBUG: find_specific_aggregation_result called for '{}' with {} bytes",
        aggregation_name,
        intermediate_agg_bytes.len()
    );
    debug_println!("RUST DEBUG: aggregation_json: {}", aggregation_json);

    if intermediate_agg_bytes.is_empty() {
        debug_println!("RUST DEBUG: No aggregation bytes available");
        return Ok(std::ptr::null_mut());
    }

    // Step 1: Parse aggregation request from JSON
    // Handle wrapped JSON format like {"agg_0":{"terms":{"field":"status","size":1000}}}
    debug_println!(
        "RUST DEBUG: Parsing specific aggregation JSON: {}",
        aggregation_json
    );

    let aggregations: Aggregations = if aggregation_json.starts_with('{') {
        // Parse as a JSON object to extract the inner aggregation structures
        match serde_json::from_str::<serde_json::Value>(aggregation_json) {
            Ok(json_value) => {
                if let Some(obj) = json_value.as_object() {
                    // Convert the wrapped format to unwrapped format that Tantivy expects
                    let mut unwrapped = serde_json::Map::new();
                    for (key, value) in obj {
                        unwrapped.insert(key.clone(), value.clone());
                    }
                    match serde_json::to_string(&unwrapped) {
                        Ok(unwrapped_json) => {
                            debug_println!(
                                "RUST DEBUG: Unwrapped specific aggregation JSON: {}",
                                unwrapped_json
                            );
                            match serde_json::from_str(&unwrapped_json) {
                                Ok(aggs) => {
                                    debug_println!(
                                        "RUST DEBUG: Successfully parsed specific aggregation request JSON"
                                    );
                                    aggs
                                }
                                Err(e) => {
                                    debug_println!(
                                        "RUST DEBUG: Failed to parse unwrapped aggregation JSON: {}",
                                        e
                                    );
                                    return Ok(std::ptr::null_mut());
                                }
                            }
                        }
                        Err(e) => {
                            debug_println!(
                                "RUST DEBUG: Failed to serialize unwrapped JSON: {}",
                                e
                            );
                            return Ok(std::ptr::null_mut());
                        }
                    }
                } else {
                    match serde_json::from_str(aggregation_json) {
                        Ok(aggs) => {
                            debug_println!(
                                "RUST DEBUG: Successfully parsed direct aggregation request JSON"
                            );
                            aggs
                        }
                        Err(e) => {
                            debug_println!(
                                "RUST DEBUG: Failed to parse direct aggregation JSON: {}",
                                e
                            );
                            return Ok(std::ptr::null_mut());
                        }
                    }
                }
            }
            Err(e) => {
                debug_println!(
                    "RUST DEBUG: Failed to parse aggregation JSON as Value: {}",
                    e
                );
                return Ok(std::ptr::null_mut());
            }
        }
    } else {
        match serde_json::from_str(aggregation_json) {
            Ok(aggs) => {
                debug_println!(
                    "RUST DEBUG: Successfully parsed aggregation request JSON (fallback)"
                );
                aggs
            }
            Err(e) => {
                debug_println!(
                    "RUST DEBUG: Failed to parse aggregation JSON (fallback): {}",
                    e
                );
                return Ok(std::ptr::null_mut());
            }
        }
    };

    // Step 2: Deserialize binary data to intermediate results using postcard
    let intermediate_results: IntermediateAggregationResults =
        match postcard::from_bytes(intermediate_agg_bytes) {
            Ok(results) => {
                debug_println!(
                    "RUST DEBUG: Successfully deserialized IntermediateAggregationResults from postcard bytes"
                );
                results
            }
            Err(e) => {
                debug_println!(
                    "RUST DEBUG: Failed to deserialize IntermediateAggregationResults: {}",
                    e
                );
                return Ok(std::ptr::null_mut());
            }
        };

    // Step 3: Create aggregation limits (using reasonable defaults like Quickwit)
    let aggregation_limits = AggregationLimitsGuard::new(
        Some(50_000_000), // 50MB memory limit
        Some(65_000),     // 65k bucket limit
    );

    // Step 4: Convert to final results using Quickwit's proven method
    let final_results: AggregationResults =
        match intermediate_results.into_final_result(aggregations, aggregation_limits) {
            Ok(results) => {
                debug_println!("RUST DEBUG: Successfully converted to final AggregationResults");
                results
            }
            Err(e) => {
                debug_println!("RUST DEBUG: Failed to convert to final results: {}", e);
                return Ok(std::ptr::null_mut());
            }
        };

    // Step 5: Extract specific aggregation values
    debug_println!(
        "RUST DEBUG: Extracting values for aggregation '{}'",
        aggregation_name
    );

    // Determine if this is a date_histogram request from the JSON
    let is_date_histogram_request = is_date_histogram_aggregation(aggregation_json, aggregation_name);
    debug_println!(
        "RUST DEBUG: is_date_histogram_request for '{}': {}",
        aggregation_name,
        is_date_histogram_request
    );

    if let Some(agg_result) = final_results.0.get(aggregation_name) {
        debug_println!("RUST DEBUG: Found aggregation result: {:?}", agg_result);

        // Look up per-aggregation include/exclude string filters from the touchup infos.
        // These were captured during Phase 2 rewriting and removed from the JSON sent to Tantivy
        // because Tantivy ignores numeric include arrays for U64 fast fields.
        let (include_filter, exclude_filter) = touchup_infos
            .and_then(|infos| infos.iter().find(|i| i.agg_name == aggregation_name))
            .map(|info| (info.include_strings.as_ref(), info.exclude_strings.as_ref()))
            .unwrap_or((None, None));

        return create_java_aggregation_from_final_result(
            env,
            aggregation_name,
            agg_result,
            is_date_histogram_request,
            hash_resolution_map,
            redirected_hash_agg_names,
            include_filter,
            exclude_filter,
        );
    } else {
        debug_println!(
            "RUST DEBUG: Aggregation '{}' not found in final results",
            aggregation_name
        );
        let available_keys: Vec<String> = final_results.0.keys().cloned().collect();
        debug_println!("RUST DEBUG: Available aggregation keys: {:?}", available_keys);
    }

    debug_println!(
        "RUST DEBUG: Failed to find aggregation '{}'",
        aggregation_name
    );
    Ok(std::ptr::null_mut())
}
