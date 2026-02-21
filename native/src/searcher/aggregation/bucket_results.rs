// bucket_results.rs - Bucket aggregation result Java object creation
// Extracted from aggregation.rs during refactoring

use jni::objects::{JObject, JValue};
use jni::sys::{jlong, jobject};
use jni::JNIEnv;
use tantivy::aggregation::agg_result::{BucketEntries, BucketEntry, RangeBucketEntry};
use tantivy::aggregation::Key;

use crate::debug_println;

use super::sub_aggregations::create_sub_aggregations_map;

/// Helper function to create a TermsResult Java object.
///
/// `resolution_map`: if `Some`, U64 bucket keys are looked up in this map and replaced
/// with their original string values (used when the field was redirected to a `_phash_*`
/// hash fast field). Keys not found in the map fall back to numeric string formatting.
/// `include_filter`: if `Some`, only buckets whose resolved key is in this set are included.
/// `exclude_filter`: if `Some`, buckets whose resolved key is in this set are excluded.
/// `missing_value`: if `Some`, the U64 zero-sentinel bucket (null docs) is relabeled with
/// the original `missing` string value from the aggregation request instead of "0".
/// `needs_resort`: if `true`, the bucket list is re-sorted alphabetically by key after hash
/// resolution (needed when `order: {"_key": ...}` was specified, since Tantivy sorted by
/// U64 hash value which doesn't match string ordering).
pub(crate) fn create_terms_result_object(
    env: &mut JNIEnv,
    aggregation_name: &str,
    buckets: &Vec<BucketEntry>,
    resolution_map: Option<&std::collections::HashMap<u64, String>>,
    redirected_names: Option<&std::collections::HashSet<String>>,
    include_filter: Option<&std::collections::HashSet<String>>,
    exclude_filter: Option<&std::collections::HashSet<String>>,
    missing_value: Option<&str>,
    needs_resort: bool,
) -> anyhow::Result<jobject> {
    debug_println!(
        "RUST DEBUG: Creating TermsResult for '{}' with {} buckets",
        aggregation_name,
        buckets.len()
    );

    // Phase 1: Resolve keys and filter. Collect (resolved_key, bucket_ref) pairs.
    let mut resolved_buckets: Vec<(String, &BucketEntry)> = Vec::with_capacity(buckets.len());
    for bucket in buckets {
        debug_println!(
            "RUST DEBUG: Processing bucket - key: {:?}, doc_count: {}, has_sub_aggs: {}",
            bucket.key,
            bucket.doc_count,
            !bucket.sub_aggregation.0.is_empty()
        );

        // Resolve the bucket key to a Rust String first so we can apply include/exclude filters.
        // For U64 keys, attempt to resolve via the hash resolution map first
        // (populated when the field was a _phash_* redirect). Falls back to n.to_string().
        // Special case: U64 key 0 is the null sentinel. If `missing_value` is set, relabel it
        // with the original `missing` string from the aggregation request.
        let key_resolved: String = match &bucket.key {
            Key::Str(s) => s.clone(),
            Key::U64(n) => {
                if *n == 0 {
                    if let Some(mv) = missing_value {
                        mv.to_string()
                    } else {
                        resolution_map.and_then(|m| m.get(n)).cloned().unwrap_or_else(|| n.to_string())
                    }
                } else {
                    resolution_map.and_then(|m| m.get(n)).cloned().unwrap_or_else(|| n.to_string())
                }
            },
            Key::I64(n) => n.to_string(),
            Key::F64(n) => n.to_string(),
        };

        // Apply include/exclude filters (post-filter for hash-field aggs where Tantivy
        // ignores numeric include/exclude arrays on U64 fast fields).
        if let Some(include) = include_filter {
            if !include.contains(&key_resolved) {
                continue;
            }
        }
        if let Some(exclude) = exclude_filter {
            if exclude.contains(&key_resolved) {
                continue;
            }
        }

        resolved_buckets.push((key_resolved, bucket));
    }

    // Phase 2: Sort by resolved string key if order:_key was requested.
    // Tantivy sorted by U64 hash value which doesn't match string ordering.
    if needs_resort {
        resolved_buckets.sort_by(|(a, _), (b, _)| a.cmp(b));
    }

    // Phase 3: Create Java objects from the resolved, filtered, sorted list.
    let terms_result_class =
        env.find_class("io/indextables/tantivy4java/aggregation/TermsResult")?;
    let name_string = env.new_string(aggregation_name)?;

    let arraylist_class = env.find_class("java/util/ArrayList")?;
    let bucket_list = env.new_object(&arraylist_class, "()V", &[])?;

    let bucket_class =
        env.find_class("io/indextables/tantivy4java/aggregation/TermsResult$TermsBucket")?;

    for (key_resolved, bucket) in &resolved_buckets {
        let key_string = env.new_string(key_resolved)?;

        // Process sub-aggregations if any
        let sub_agg_map = if !bucket.sub_aggregation.0.is_empty() {
            debug_println!(
                "RUST DEBUG: Processing {} sub-aggregations in bucket",
                bucket.sub_aggregation.0.len()
            );
            create_sub_aggregations_map(env, &bucket.sub_aggregation, resolution_map, redirected_names)?
        } else {
            debug_println!("RUST DEBUG: No sub-aggregations in bucket");
            // Create empty HashMap
            let hashmap_class = env.find_class("java/util/HashMap")?;
            env.new_object(&hashmap_class, "()V", &[])?.into_raw()
        };

        // Create TermsBucket object with sub-aggregations
        let sub_agg_map_obj = unsafe { JObject::from_raw(sub_agg_map) };
        let bucket_obj = env.new_object(
            &bucket_class,
            "(Ljava/lang/Object;JLjava/util/Map;)V",
            &[
                JValue::Object(&key_string),
                JValue::Long(bucket.doc_count as jlong),
                JValue::Object(&sub_agg_map_obj),
            ],
        )?;

        // Add bucket to list
        env.call_method(
            &bucket_list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[JValue::Object(&bucket_obj)],
        )?;
    }

    // Create TermsResult with name, buckets, docCountErrorUpperBound, sumOtherDocCount
    let terms_result_obj = env.new_object(
        &terms_result_class,
        "(Ljava/lang/String;Ljava/util/List;JJ)V",
        &[
            JValue::Object(&name_string),
            JValue::Object(&bucket_list),
            JValue::Long(0), // docCountErrorUpperBound - using 0 for now
            JValue::Long(0), // sumOtherDocCount - using 0 for now
        ],
    )?;

    debug_println!("RUST DEBUG: Successfully created TermsResult object");
    Ok(terms_result_obj.into_raw())
}

/// Helper function to create a HistogramResult Java object from Tantivy bucket results
pub(crate) fn create_histogram_result_object(
    env: &mut JNIEnv,
    aggregation_name: &str,
    buckets: &BucketEntries<BucketEntry>,
) -> anyhow::Result<jobject> {
    debug_println!(
        "RUST DEBUG: Creating HistogramResult for '{}'",
        aggregation_name
    );

    // Find Java classes
    let histogram_result_class =
        env.find_class("io/indextables/tantivy4java/aggregation/HistogramResult")?;
    let bucket_class =
        env.find_class("io/indextables/tantivy4java/aggregation/HistogramResult$HistogramBucket")?;
    let arraylist_class = env.find_class("java/util/ArrayList")?;

    // Create ArrayList for buckets
    let bucket_list = env.new_object(&arraylist_class, "()V", &[])?;

    // Get bucket iterator based on BucketEntries type
    let bucket_vec: Vec<&BucketEntry> = match buckets {
        BucketEntries::Vec(vec) => vec.iter().collect(),
        BucketEntries::HashMap(map) => map.values().collect(),
    };

    // Iterate over buckets
    let mut bucket_count = 0;
    for bucket in bucket_vec {
        bucket_count += 1;

        // Extract the numeric key (no dereference needed - these are Copy types)
        let key = match &bucket.key {
            Key::F64(f) => *f,
            Key::I64(i) => *i as f64,
            Key::U64(u) => *u as f64,
            Key::Str(s) => s.parse::<f64>().unwrap_or(0.0),
        };

        debug_println!(
            "RUST DEBUG: Processing histogram bucket - key: {}, doc_count: {}, has_sub_aggs: {}",
            key,
            bucket.doc_count,
            !bucket.sub_aggregation.0.is_empty()
        );

        // Process sub-aggregations if any
        let sub_agg_map = if !bucket.sub_aggregation.0.is_empty() {
            debug_println!(
                "RUST DEBUG: Processing {} sub-aggregations in histogram bucket",
                bucket.sub_aggregation.0.len()
            );
            create_sub_aggregations_map(env, &bucket.sub_aggregation, None, None)?
        } else {
            // Create empty HashMap
            let hashmap_class = env.find_class("java/util/HashMap")?;
            env.new_object(&hashmap_class, "()V", &[])?.into_raw()
        };

        // Create HistogramBucket object with sub-aggregations
        let sub_agg_map_obj = unsafe { JObject::from_raw(sub_agg_map) };
        let bucket_obj = env.new_object(
            &bucket_class,
            "(DJLjava/util/Map;)V",
            &[
                JValue::Double(key),
                JValue::Long(bucket.doc_count as jlong),
                JValue::Object(&sub_agg_map_obj),
            ],
        )?;

        // Add bucket to list
        env.call_method(
            &bucket_list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[JValue::Object(&bucket_obj)],
        )?;
    }

    debug_println!("RUST DEBUG: Created {} histogram buckets", bucket_count);

    // Create HistogramResult
    let name_string = env.new_string(aggregation_name)?;
    let result = env.new_object(
        &histogram_result_class,
        "(Ljava/lang/String;Ljava/util/List;)V",
        &[JValue::Object(&name_string), JValue::Object(&bucket_list)],
    )?;

    debug_println!("RUST DEBUG: Successfully created HistogramResult object");
    Ok(result.into_raw())
}

/// Helper function to create a DateHistogramResult Java object from Tantivy bucket results
pub(crate) fn create_date_histogram_result_object(
    env: &mut JNIEnv,
    aggregation_name: &str,
    buckets: &BucketEntries<BucketEntry>,
) -> anyhow::Result<jobject> {
    debug_println!(
        "RUST DEBUG: Creating DateHistogramResult for '{}'",
        aggregation_name
    );

    // Find Java classes
    let date_histogram_result_class =
        env.find_class("io/indextables/tantivy4java/aggregation/DateHistogramResult")?;
    let bucket_class = env.find_class(
        "io/indextables/tantivy4java/aggregation/DateHistogramResult$DateHistogramBucket",
    )?;
    let arraylist_class = env.find_class("java/util/ArrayList")?;

    // Create ArrayList for buckets
    let bucket_list = env.new_object(&arraylist_class, "()V", &[])?;

    // Get bucket iterator based on BucketEntries type
    let bucket_vec: Vec<&BucketEntry> = match buckets {
        BucketEntries::Vec(vec) => vec.iter().collect(),
        BucketEntries::HashMap(map) => map.values().collect(),
    };

    // Iterate over buckets
    let mut bucket_count = 0;
    for bucket in bucket_vec {
        bucket_count += 1;

        // Extract the numeric key (milliseconds since epoch for date histograms)
        let key = match &bucket.key {
            Key::F64(f) => *f,
            Key::I64(i) => *i as f64,
            Key::U64(u) => *u as f64,
            Key::Str(s) => s.parse::<f64>().unwrap_or(0.0),
        };

        // Use key_as_string if available (RFC3339 format from Tantivy), otherwise generate it
        let key_as_string = if let Some(ref s) = bucket.key_as_string {
            env.new_string(s)?
        } else {
            // Fallback: format as ISO timestamp from milliseconds
            let millis = key as i64;
            let secs = millis / 1000;
            let nanos = ((millis % 1000) * 1_000_000) as u32;
            let formatted = if secs >= 0 {
                chrono::DateTime::from_timestamp(secs, nanos)
                    .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                    .unwrap_or_else(|| millis.to_string())
            } else {
                millis.to_string()
            };
            env.new_string(&formatted)?
        };

        debug_println!(
            "RUST DEBUG: Processing date histogram bucket - key: {}, key_as_string: {:?}, doc_count: {}, has_sub_aggs: {}",
            key, bucket.key_as_string, bucket.doc_count, !bucket.sub_aggregation.0.is_empty()
        );

        // Process sub-aggregations if any
        let sub_agg_map = if !bucket.sub_aggregation.0.is_empty() {
            debug_println!(
                "RUST DEBUG: Processing {} sub-aggregations in date histogram bucket",
                bucket.sub_aggregation.0.len()
            );
            create_sub_aggregations_map(env, &bucket.sub_aggregation, None, None)?
        } else {
            // Create empty HashMap
            let hashmap_class = env.find_class("java/util/HashMap")?;
            env.new_object(&hashmap_class, "()V", &[])?.into_raw()
        };

        // Create DateHistogramBucket object with sub-aggregations
        let sub_agg_map_obj = unsafe { JObject::from_raw(sub_agg_map) };
        let bucket_obj = env.new_object(
            &bucket_class,
            "(DLjava/lang/String;JLjava/util/Map;)V",
            &[
                JValue::Double(key),
                JValue::Object(&key_as_string),
                JValue::Long(bucket.doc_count as jlong),
                JValue::Object(&sub_agg_map_obj),
            ],
        )?;

        // Add bucket to list
        env.call_method(
            &bucket_list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[JValue::Object(&bucket_obj)],
        )?;
    }

    debug_println!(
        "RUST DEBUG: Created {} date histogram buckets",
        bucket_count
    );

    // Create DateHistogramResult
    let name_string = env.new_string(aggregation_name)?;
    let result = env.new_object(
        &date_histogram_result_class,
        "(Ljava/lang/String;Ljava/util/List;)V",
        &[JValue::Object(&name_string), JValue::Object(&bucket_list)],
    )?;

    debug_println!("RUST DEBUG: Successfully created DateHistogramResult object");
    Ok(result.into_raw())
}

/// Helper function to create a RangeResult Java object from Tantivy range bucket results
pub(crate) fn create_range_result_object(
    env: &mut JNIEnv,
    aggregation_name: &str,
    buckets: &BucketEntries<RangeBucketEntry>,
) -> anyhow::Result<jobject> {
    debug_println!(
        "RUST DEBUG: Creating RangeResult for '{}'",
        aggregation_name
    );

    // Find Java classes
    let range_result_class =
        env.find_class("io/indextables/tantivy4java/aggregation/RangeResult")?;
    let bucket_class =
        env.find_class("io/indextables/tantivy4java/aggregation/RangeResult$RangeBucket")?;
    let arraylist_class = env.find_class("java/util/ArrayList")?;
    let double_class = env.find_class("java/lang/Double")?;

    // Create ArrayList for buckets
    let bucket_list = env.new_object(&arraylist_class, "()V", &[])?;

    // Get bucket iterator based on BucketEntries type
    let bucket_vec: Vec<&RangeBucketEntry> = match buckets {
        BucketEntries::Vec(vec) => vec.iter().collect(),
        BucketEntries::HashMap(map) => map.values().collect(),
    };

    // Iterate over buckets
    let mut bucket_count = 0;
    for bucket in bucket_vec {
        bucket_count += 1;

        // Extract the key as string
        let key_str = match &bucket.key {
            Key::Str(s) => s.clone(),
            Key::F64(f) => f.to_string(),
            Key::I64(i) => i.to_string(),
            Key::U64(u) => u.to_string(),
        };
        let key_jstring = env.new_string(&key_str)?;

        debug_println!(
            "RUST DEBUG: Processing range bucket - key: {}, from: {:?}, to: {:?}, doc_count: {}, has_sub_aggs: {}",
            key_str, bucket.from, bucket.to, bucket.doc_count, !bucket.sub_aggregation.0.is_empty()
        );

        // Create from value (Double or null)
        let from_obj = if let Some(from) = bucket.from {
            let obj = env.new_object(&double_class, "(D)V", &[JValue::Double(from)])?;
            JObject::from(obj)
        } else {
            JObject::null()
        };

        // Create to value (Double or null)
        let to_obj = if let Some(to) = bucket.to {
            let obj = env.new_object(&double_class, "(D)V", &[JValue::Double(to)])?;
            JObject::from(obj)
        } else {
            JObject::null()
        };

        // Process sub-aggregations if any
        let sub_agg_map = if !bucket.sub_aggregation.0.is_empty() {
            debug_println!(
                "RUST DEBUG: Processing {} sub-aggregations in range bucket",
                bucket.sub_aggregation.0.len()
            );
            create_sub_aggregations_map(env, &bucket.sub_aggregation, None, None)?
        } else {
            // Create empty HashMap
            let hashmap_class = env.find_class("java/util/HashMap")?;
            env.new_object(&hashmap_class, "()V", &[])?.into_raw()
        };

        // Create RangeBucket object with sub-aggregations
        let sub_agg_map_obj = unsafe { JObject::from_raw(sub_agg_map) };
        let bucket_obj = env.new_object(
            &bucket_class,
            "(Ljava/lang/String;Ljava/lang/Double;Ljava/lang/Double;JLjava/util/Map;)V",
            &[
                JValue::Object(&key_jstring),
                JValue::Object(&from_obj),
                JValue::Object(&to_obj),
                JValue::Long(bucket.doc_count as jlong),
                JValue::Object(&sub_agg_map_obj),
            ],
        )?;

        // Add bucket to list
        env.call_method(
            &bucket_list,
            "add",
            "(Ljava/lang/Object;)Z",
            &[JValue::Object(&bucket_obj)],
        )?;
    }

    debug_println!("RUST DEBUG: Created {} range buckets", bucket_count);

    // Create RangeResult
    let name_string = env.new_string(aggregation_name)?;
    let result = env.new_object(
        &range_result_class,
        "(Ljava/lang/String;Ljava/util/List;)V",
        &[JValue::Object(&name_string), JValue::Object(&bucket_list)],
    )?;

    debug_println!("RUST DEBUG: Successfully created RangeResult object");
    Ok(result.into_raw())
}
