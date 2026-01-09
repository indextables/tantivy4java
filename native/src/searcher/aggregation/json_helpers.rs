// json_helpers.rs - JSON parsing utilities for aggregation results
// Extracted from aggregation.rs during refactoring

use jni::objects::JObject;
use jni::sys::{jlong, jobject};
use jni::JNIEnv;

use crate::debug_println;

/// Check if the aggregation JSON indicates a date_histogram aggregation for the given name
pub(crate) fn is_date_histogram_aggregation(aggregation_json: &str, aggregation_name: &str) -> bool {
    if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(aggregation_json) {
        if let Some(obj) = json_value.as_object() {
            if let Some(agg_def) = obj.get(aggregation_name) {
                // Check if this aggregation definition contains "date_histogram"
                if agg_def.get("date_histogram").is_some() {
                    return true;
                }
            }
        }
    }
    false
}

/// Parse JSON-based aggregation results
pub(crate) fn parse_json_aggregation_results(
    env: &mut JNIEnv,
    hashmap: &JObject,
    json_value: &serde_json::Value,
) -> anyhow::Result<()> {
    debug_println!(
        "RUST DEBUG: parse_json_aggregation_results: {}",
        json_value
    );

    if let Some(agg_map) = json_value.as_object() {
        for (agg_name, agg_value) in agg_map {
            debug_println!(
                "RUST DEBUG: Processing JSON aggregation '{}': {}",
                agg_name,
                agg_value
            );

            // Try to create Java result object based on the JSON structure
            if let Some(java_result) = create_java_result_from_json(env, agg_name, agg_value)? {
                let name_string = env
                    .new_string(agg_name)
                    .map_err(|e| anyhow::anyhow!("Failed to create aggregation name string: {}", e))?;

                env.call_method(
                    hashmap,
                    "put",
                    "(Ljava/lang/Object;Ljava/lang/Object;)Ljava/lang/Object;",
                    &[
                        (&name_string).into(),
                        (&unsafe { JObject::from_raw(java_result) }).into(),
                    ],
                )
                .map_err(|e| anyhow::anyhow!("Failed to add aggregation to HashMap: {}", e))?;

                debug_println!(
                    "RUST DEBUG: Added JSON aggregation '{}' to HashMap",
                    agg_name
                );
            }
        }
    }

    Ok(())
}

/// Create Java aggregation result object from JSON value
fn create_java_result_from_json(
    env: &mut JNIEnv,
    agg_name: &str,
    agg_value: &serde_json::Value,
) -> anyhow::Result<Option<jobject>> {
    debug_println!(
        "RUST DEBUG: create_java_result_from_json for '{}': {}",
        agg_name,
        agg_value
    );

    // For now, create a simple CountResult as fallback
    let count_class = env
        .find_class("io/indextables/tantivy4java/aggregation/CountResult")
        .map_err(|e| anyhow::anyhow!("Failed to find CountResult class: {}", e))?;

    let name_string = env
        .new_string(agg_name)
        .map_err(|e| anyhow::anyhow!("Failed to create name string: {}", e))?;

    let java_count = env
        .new_object(
            &count_class,
            "(Ljava/lang/String;J)V",
            &[(&name_string).into(), (1 as jlong).into()],
        )
        .map_err(|e| anyhow::anyhow!("Failed to create CountResult: {}", e))?;

    Ok(Some(java_count.into_raw()))
}

/// Create Java aggregation result from serde_json Value
pub(crate) fn create_java_aggregation_result(
    env: &mut JNIEnv,
    aggregation_name: &str,
    agg_result: &serde_json::Value,
) -> anyhow::Result<jobject> {
    debug_println!(
        "RUST DEBUG: Creating Java aggregation result for '{}': {}",
        aggregation_name,
        agg_result
    );

    // Check if this is a stats aggregation based on the structure
    if let Some(stats_obj) = agg_result.get("stats") {
        debug_println!("RUST DEBUG: Creating StatsResult from: {}", stats_obj);
        return create_stats_result(env, stats_obj);
    }

    // Check for other aggregation types
    if let Some(count_val) = agg_result.get("count") {
        debug_println!("RUST DEBUG: Creating CountResult from: {}", count_val);
        return create_count_result(env, count_val);
    }

    debug_println!("RUST DEBUG: Unknown aggregation structure, returning null");
    Ok(std::ptr::null_mut())
}

fn create_stats_result(env: &mut JNIEnv, stats_json: &serde_json::Value) -> anyhow::Result<jobject> {
    debug_println!(
        "RUST DEBUG: Creating StatsResult from JSON: {}",
        stats_json
    );

    // Extract stats values from JSON
    let count = stats_json
        .get("count")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as jlong;
    let sum = stats_json
        .get("sum")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let _avg = stats_json
        .get("avg")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let min = stats_json
        .get("min")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);
    let max = stats_json
        .get("max")
        .and_then(|v| v.as_f64())
        .unwrap_or(0.0);

    debug_println!(
        "RUST DEBUG: Stats values - count: {}, sum: {}, min: {}, max: {}",
        count,
        sum,
        min,
        max
    );

    // Create StatsResult Java object with name parameter
    let stats_result_class =
        env.find_class("io/indextables/tantivy4java/aggregation/StatsResult")?;

    // Create name string (we don't have the original aggregation name here, so use "stats")
    let name_string = env.new_string("stats")?;

    let stats_result = env.new_object(
        &stats_result_class,
        "(Ljava/lang/String;JDDDD)V",
        &[
            (&name_string).into(),
            count.into(),
            sum.into(),
            min.into(),
            max.into(),
        ],
    )?;

    debug_println!("RUST DEBUG: Successfully created StatsResult object");
    Ok(stats_result.into_raw())
}

fn create_count_result(
    env: &mut JNIEnv,
    count_json: &serde_json::Value,
) -> anyhow::Result<jobject> {
    let count = count_json.as_u64().unwrap_or(0) as jlong;

    let count_result_class =
        env.find_class("io/indextables/tantivy4java/aggregation/CountResult")?;
    let count_result = env.new_object(&count_result_class, "(J)V", &[count.into()])?;

    Ok(count_result.into_raw())
}

/// Create Java aggregation from Quickwit result
pub(crate) fn create_java_aggregation_from_quickwit(
    env: &mut JNIEnv,
    aggregation_name: &str,
    agg_result: &serde_json::Value,
) -> anyhow::Result<jobject> {
    debug_println!(
        "RUST DEBUG: Creating Java aggregation from Quickwit result for '{}': {}",
        aggregation_name,
        agg_result
    );

    // Use the same logic as create_java_aggregation_result but with better error handling
    create_java_aggregation_result(env, aggregation_name, agg_result)
}
