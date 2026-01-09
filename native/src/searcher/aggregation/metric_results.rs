// metric_results.rs - Metric aggregation result Java object creation
// Extracted from aggregation.rs during refactoring

use jni::objects::JValue;
use jni::sys::{jlong, jobject};
use jni::JNIEnv;

use crate::debug_println;

/// Helper function to create a StatsResult Java object
pub(crate) fn create_stats_result_object(
    env: &mut JNIEnv,
    aggregation_name: &str,
    count: u64,
    sum: f64,
    min: f64,
    max: f64,
) -> anyhow::Result<jobject> {
    let stats_result_class =
        env.find_class("io/indextables/tantivy4java/aggregation/StatsResult")?;
    let name_string = env.new_string(aggregation_name)?;

    debug_println!(
        "RUST DEBUG: Creating StatsResult with count={}, sum={}, min={}, max={}",
        count,
        sum,
        min,
        max
    );

    let stats_result = env.new_object(
        &stats_result_class,
        "(Ljava/lang/String;JDDD)V",
        &[
            JValue::Object(&name_string),
            JValue::Long(count as jlong),
            JValue::Double(sum),
            JValue::Double(min),
            JValue::Double(max),
        ],
    )?;

    debug_println!("RUST DEBUG: Successfully created StatsResult object");
    Ok(stats_result.into_raw())
}

/// Helper function to create an AverageResult Java object
pub(crate) fn create_average_result_object(
    env: &mut JNIEnv,
    aggregation_name: &str,
    average: f64,
) -> anyhow::Result<jobject> {
    let class = env.find_class("io/indextables/tantivy4java/aggregation/AverageResult")?;
    let name_string = env.new_string(aggregation_name)?;

    let result = env.new_object(
        &class,
        "(Ljava/lang/String;D)V",
        &[JValue::Object(&name_string), JValue::Double(average)],
    )?;

    Ok(result.into_raw())
}

/// Helper function to create a CountResult Java object
pub(crate) fn create_count_result_object(
    env: &mut JNIEnv,
    aggregation_name: &str,
    count: u64,
) -> anyhow::Result<jobject> {
    let class = env.find_class("io/indextables/tantivy4java/aggregation/CountResult")?;
    let name_string = env.new_string(aggregation_name)?;

    let result = env.new_object(
        &class,
        "(Ljava/lang/String;J)V",
        &[
            JValue::Object(&name_string),
            JValue::Long(count as jlong),
        ],
    )?;

    Ok(result.into_raw())
}

/// Helper function to create a MinResult Java object
pub(crate) fn create_min_result_object(
    env: &mut JNIEnv,
    aggregation_name: &str,
    min: f64,
) -> anyhow::Result<jobject> {
    let class = env.find_class("io/indextables/tantivy4java/aggregation/MinResult")?;
    let name_string = env.new_string(aggregation_name)?;

    let result = env.new_object(
        &class,
        "(Ljava/lang/String;D)V",
        &[JValue::Object(&name_string), JValue::Double(min)],
    )?;

    Ok(result.into_raw())
}

/// Helper function to create a MaxResult Java object
pub(crate) fn create_max_result_object(
    env: &mut JNIEnv,
    aggregation_name: &str,
    max: f64,
) -> anyhow::Result<jobject> {
    let class = env.find_class("io/indextables/tantivy4java/aggregation/MaxResult")?;
    let name_string = env.new_string(aggregation_name)?;

    let result = env.new_object(
        &class,
        "(Ljava/lang/String;D)V",
        &[JValue::Object(&name_string), JValue::Double(max)],
    )?;

    Ok(result.into_raw())
}

/// Helper function to create a SumResult Java object
pub(crate) fn create_sum_result_object(
    env: &mut JNIEnv,
    aggregation_name: &str,
    sum: f64,
) -> anyhow::Result<jobject> {
    let class = env.find_class("io/indextables/tantivy4java/aggregation/SumResult")?;
    let name_string = env.new_string(aggregation_name)?;

    let result = env.new_object(
        &class,
        "(Ljava/lang/String;D)V",
        &[JValue::Object(&name_string), JValue::Double(sum)],
    )?;

    Ok(result.into_raw())
}
