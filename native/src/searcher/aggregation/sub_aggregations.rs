// sub_aggregations.rs - Sub-aggregation map creation and result dispatcher
// Extracted from aggregation.rs during refactoring

use jni::objects::{JObject, JValue};
use jni::sys::jobject;
use jni::JNIEnv;
use tantivy::aggregation::agg_result::{AggregationResult, AggregationResults, MetricResult};

use crate::debug_println;

use super::bucket_results::{
    create_date_histogram_result_object, create_histogram_result_object,
    create_range_result_object, create_terms_result_object,
};
use super::metric_results::{
    create_average_result_object, create_count_result_object, create_max_result_object,
    create_min_result_object, create_stats_result_object, create_sum_result_object,
};

/// Helper function to create a Java HashMap of sub-aggregations from AggregationResults.
///
/// `resolution_map` and `redirected_names` are threaded through to support hash-field
/// touchup in nested terms aggregations (Phase 3).
pub(crate) fn create_sub_aggregations_map(
    env: &mut JNIEnv,
    sub_aggregations: &AggregationResults,
    resolution_map: Option<&std::collections::HashMap<u64, String>>,
    redirected_names: Option<&std::collections::HashSet<String>>,
) -> anyhow::Result<jobject> {
    debug_println!(
        "RUST DEBUG: Creating sub-aggregations map with {} entries",
        sub_aggregations.0.len()
    );

    // Create HashMap to store sub-aggregations
    let hashmap_class = env.find_class("java/util/HashMap")?;
    let sub_agg_map = env.new_object(&hashmap_class, "()V", &[])?;

    // Process each sub-aggregation
    for (agg_name, agg_result) in sub_aggregations.0.iter() {
        debug_println!(
            "RUST DEBUG: Processing sub-aggregation: {} -> {:?}",
            agg_name,
            agg_result
        );

        // Convert aggregation result to Java object, passing through hash resolution context.
        // Sub-aggregations don't carry per-name include/exclude filters.
        let java_agg_result = create_java_aggregation_from_final_result(
            env, agg_name, agg_result, false, resolution_map, redirected_names, None, None,
        )?;

        if !java_agg_result.is_null() {
            // Add to HashMap - Convert jobject to JObject for JValue::Object
            let name_string = env.new_string(agg_name)?;
            let java_agg_obj = unsafe { JObject::from_raw(java_agg_result) };
            env.call_method(
                &sub_agg_map,
                "put",
                "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
                &[JValue::Object(&name_string), JValue::Object(&java_agg_obj)],
            )?;
            debug_println!("RUST DEBUG: Added sub-aggregation '{}' to map", agg_name);
        } else {
            debug_println!("RUST DEBUG: Skipping null sub-aggregation '{}'", agg_name);
        }
    }

    debug_println!("RUST DEBUG: Successfully created sub-aggregations map");
    Ok(sub_agg_map.into_raw())
}

/// Create Java aggregation result from final Tantivy aggregation result.
///
/// `is_date_histogram_hint`: true if the original request was a date_histogram aggregation.
/// `resolution_map`: optional hash → string map for Phase 3 touchup of terms buckets.
/// `redirected_names`: set of aggregation names redirected to `_phash_*` hash fields.
/// `include_filter`: if `Some`, only terms buckets with keys in this set are returned.
/// `exclude_filter`: if `Some`, terms buckets with keys in this set are excluded.
pub(crate) fn create_java_aggregation_from_final_result(
    env: &mut JNIEnv,
    aggregation_name: &str,
    agg_result: &AggregationResult,
    is_date_histogram_hint: bool,
    resolution_map: Option<&std::collections::HashMap<u64, String>>,
    redirected_names: Option<&std::collections::HashSet<String>>,
    include_filter: Option<&std::collections::HashSet<String>>,
    exclude_filter: Option<&std::collections::HashSet<String>>,
) -> anyhow::Result<jobject> {
    debug_println!(
        "RUST DEBUG: Creating Java aggregation for '{}', type: {:?}, date_histogram_hint: {}",
        aggregation_name,
        agg_result,
        is_date_histogram_hint
    );

    match agg_result {
        AggregationResult::MetricResult(metric_result) => {
            match metric_result {
                MetricResult::Stats(stats) => {
                    debug_println!(
                        "RUST DEBUG: Creating StatsResult - count: {}, sum: {}, min: {:?}, max: {:?}",
                        stats.count,
                        stats.sum,
                        stats.min,
                        stats.max
                    );

                    create_stats_result_object(
                        env,
                        aggregation_name,
                        stats.count,
                        stats.sum,
                        stats.min.unwrap_or(0.0),
                        stats.max.unwrap_or(0.0),
                    )
                }
                MetricResult::Average(avg_result) => {
                    debug_println!(
                        "RUST DEBUG: Creating AverageResult - average: {:?}",
                        avg_result.value
                    );
                    create_average_result_object(
                        env,
                        aggregation_name,
                        avg_result.value.unwrap_or(0.0),
                    )
                }
                MetricResult::Count(count_result) => {
                    debug_println!(
                        "RUST DEBUG: Creating CountResult - count: {:?}",
                        count_result.value
                    );
                    create_count_result_object(
                        env,
                        aggregation_name,
                        count_result.value.unwrap_or(0.0) as u64,
                    )
                }
                MetricResult::Max(max_result) => {
                    debug_println!(
                        "RUST DEBUG: Creating MaxResult - max: {:?}",
                        max_result.value
                    );
                    create_max_result_object(env, aggregation_name, max_result.value.unwrap_or(0.0))
                }
                MetricResult::Min(min_result) => {
                    debug_println!(
                        "RUST DEBUG: Creating MinResult - min: {:?}",
                        min_result.value
                    );
                    create_min_result_object(env, aggregation_name, min_result.value.unwrap_or(0.0))
                }
                MetricResult::Sum(sum_result) => {
                    debug_println!(
                        "RUST DEBUG: Creating SumResult - sum: {:?}",
                        sum_result.value
                    );
                    create_sum_result_object(env, aggregation_name, sum_result.value.unwrap_or(0.0))
                }
                MetricResult::ExtendedStats(_) => {
                    debug_println!("RUST DEBUG: ExtendedStats not yet implemented");
                    Ok(std::ptr::null_mut())
                }
                MetricResult::Percentiles(_) => {
                    debug_println!("RUST DEBUG: Percentiles not yet implemented");
                    Ok(std::ptr::null_mut())
                }
                MetricResult::TopHits(_) => {
                    debug_println!("RUST DEBUG: TopHits not yet implemented");
                    Ok(std::ptr::null_mut())
                }
                MetricResult::Cardinality(_) => {
                    debug_println!("RUST DEBUG: Cardinality not yet implemented");
                    Ok(std::ptr::null_mut())
                }
            }
        }
        AggregationResult::BucketResult(bucket_result) => {
            use tantivy::aggregation::agg_result::BucketResult;
            debug_println!("RUST DEBUG: Processing BucketResult: {:?}", bucket_result);

            match bucket_result {
                BucketResult::Terms { buckets, .. } => {
                    debug_println!(
                        "RUST DEBUG: Creating TermsResult with {} buckets",
                        buckets.len()
                    );
                    // Pass the resolution map only if this aggregation was redirected to a hash field
                    let effective_map = if redirected_names
                        .map(|s| s.contains(aggregation_name))
                        .unwrap_or(false)
                    {
                        resolution_map
                    } else {
                        None
                    };
                    create_terms_result_object(env, aggregation_name, buckets, effective_map, redirected_names, include_filter, exclude_filter)
                }
                BucketResult::Range { buckets } => {
                    debug_println!("RUST DEBUG: Creating RangeResult");
                    create_range_result_object(env, aggregation_name, buckets)
                }
                BucketResult::Histogram { buckets } => {
                    // Use the hint from the aggregation request JSON if available
                    // Fall back to detecting by checking if any bucket has key_as_string set
                    use tantivy::aggregation::agg_result::BucketEntries;
                    let is_date_histogram = if is_date_histogram_hint {
                        // Trust the hint from the request JSON
                        true
                    } else {
                        // Fall back to bucket-based detection
                        match buckets {
                            BucketEntries::Vec(vec) => {
                                vec.iter().any(|b| b.key_as_string.is_some())
                            }
                            BucketEntries::HashMap(map) => {
                                map.values().any(|b| b.key_as_string.is_some())
                            }
                        }
                    };

                    if is_date_histogram {
                        debug_println!(
                            "RUST DEBUG: Creating DateHistogramResult (hint={}, detected from buckets={})",
                            is_date_histogram_hint,
                            !is_date_histogram_hint
                        );
                        create_date_histogram_result_object(env, aggregation_name, buckets)
                    } else {
                        debug_println!("RUST DEBUG: Creating HistogramResult (numeric field)");
                        create_histogram_result_object(env, aggregation_name, buckets)
                    }
                }
            }
        }
    }
}
