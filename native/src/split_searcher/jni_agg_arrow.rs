// jni_agg_arrow.rs - JNI entry points for aggregation Arrow FFI export
//
// Provides JNI methods for:
// 1. Single-split aggregation → Arrow FFI
// 2. Multi-split aggregation merge → Arrow FFI
// 3. Schema query (column names/types/count)

use jni::objects::{JClass, JLongArray, JString};
use jni::sys::{jint, jlong, jstring};
use jni::JNIEnv;
use arrow::ffi::{FFI_ArrowArray, FFI_ArrowSchema};

use tantivy::aggregation::agg_req::Aggregations;
use tantivy::aggregation::agg_result::AggregationResults;
use tantivy::aggregation::intermediate_agg_result::IntermediateAggregationResults;
use tantivy::aggregation::AggregationLimitsGuard;

use crate::common::to_java_exception;
use crate::debug_println;
use crate::perf_println;
use crate::runtime_manager::block_on_operation;

use crate::searcher::aggregation::json_helpers::is_date_histogram_aggregation;
use super::aggregation_arrow_ffi::{
    aggregation_result_arrow_schema_json, aggregation_result_to_record_batch,
    export_record_batch_ffi,
};
use super::jni_search::perform_search_async_impl_leaf_response_with_aggregations;

/// Single-split aggregation exported as Arrow RecordBatch via FFI.
///
/// Java signature: nativeAggregateArrowFfi(long searcherPtr, String queryAstJson,
///     String aggName, String aggJson, long[] arrayAddrs, long[] schemaAddrs) → int
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_nativeAggregateArrowFfi(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    query_ast_json: JString,
    agg_name: JString,
    agg_json: JString,
    array_addrs: JLongArray,
    schema_addrs: JLongArray,
) -> jint {
    let t0 = std::time::Instant::now();
    perf_println!("⏱️ AGG_FFI: nativeAggregateArrowFfi START");

    let result: Result<jint, anyhow::Error> = (|| {
        let query_json: String = env
            .get_string(&query_ast_json)
            .map_err(|e| anyhow::anyhow!("Failed to get query JSON: {}", e))?
            .into();
        let agg_name_str: String = env
            .get_string(&agg_name)
            .map_err(|e| anyhow::anyhow!("Failed to get agg name: {}", e))?
            .into();
        let agg_json_str: String = env
            .get_string(&agg_json)
            .map_err(|e| anyhow::anyhow!("Failed to get agg JSON: {}", e))?
            .into();

        let arr_addrs = unsafe {
            env.get_array_elements(&array_addrs, jni::objects::ReleaseMode::NoCopyBack)
                .map_err(|e| anyhow::anyhow!("Failed to get array addrs: {}", e))?
        };
        let sch_addrs = unsafe {
            env.get_array_elements(&schema_addrs, jni::objects::ReleaseMode::NoCopyBack)
                .map_err(|e| anyhow::anyhow!("Failed to get schema addrs: {}", e))?
        };

        let arr_slice: &[i64] =
            unsafe { std::slice::from_raw_parts(arr_addrs.as_ptr(), arr_addrs.len()) };
        let sch_slice: &[i64] =
            unsafe { std::slice::from_raw_parts(sch_addrs.as_ptr(), sch_addrs.len()) };

        // Run search with aggregations (limit=0, we only want agg results)
        let agg_json_for_search = agg_json_str.clone();
        let ctx = block_on_operation(async move {
            perform_search_async_impl_leaf_response_with_aggregations(
                searcher_ptr,
                query_json,
                0, // limit=0: only aggregations, no hits
                Some(agg_json_for_search),
            )
            .await
        })?;

        let leaf = ctx.leaf_response;
        let intermediate_bytes = leaf
            .intermediate_aggregation_result
            .ok_or_else(|| anyhow::anyhow!("No aggregation results in response"))?;

        // Determine the effective aggregation JSON (may have been rewritten for hash fields)
        let effective_agg_json = ctx.effective_agg_json.unwrap_or(agg_json_str);

        // Deserialize and finalize
        let intermediate: IntermediateAggregationResults =
            postcard::from_bytes(&intermediate_bytes)
                .map_err(|e| anyhow::anyhow!("Failed to deserialize intermediate aggs: {}", e))?;

        let aggregations: Aggregations = serde_json::from_str(&effective_agg_json)
            .map_err(|e| anyhow::anyhow!("Failed to parse aggregation JSON: {}", e))?;

        let limits = AggregationLimitsGuard::new(Some(50_000_000), Some(65_000));
        let final_results: AggregationResults =
            intermediate.into_final_result(aggregations, limits)?;

        // Find the requested aggregation
        let agg_result = final_results
            .0
            .get(&agg_name_str)
            .ok_or_else(|| {
                let available: Vec<&String> = final_results.0.keys().collect();
                anyhow::anyhow!(
                    "Aggregation '{}' not found. Available: {:?}",
                    agg_name_str,
                    available
                )
            })?;

        let is_date_hist = is_date_histogram_aggregation(&effective_agg_json, &agg_name_str);
        let batch =
            aggregation_result_to_record_batch(&agg_name_str, agg_result, is_date_hist)?;

        let row_count = export_record_batch_ffi(&batch, arr_slice, sch_slice)?;

        perf_println!(
            "⏱️ AGG_FFI: nativeAggregateArrowFfi DONE — {} rows, {} cols, {}ms",
            row_count,
            batch.num_columns(),
            t0.elapsed().as_millis()
        );

        Ok(row_count as jint)
    })();

    match result {
        Ok(count) => count,
        Err(e) => {
            perf_println!(
                "⏱️ AGG_FFI: nativeAggregateArrowFfi FAILED after {}ms: {}",
                t0.elapsed().as_millis(),
                e
            );
            to_java_exception(&mut env, &e);
            -1
        }
    }
}

/// Multi-split aggregation: search each split, merge intermediate results in Rust,
/// finalize, and export one aggregation as Arrow RecordBatch via FFI.
///
/// Java signature: nativeMultiSplitAggregateArrowFfi(long cacheManagerPtr,
///     long[] searcherPtrs, String queryAstJson, String aggName, String aggJson,
///     long[] arrayAddrs, long[] schemaAddrs) → int
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeMultiSplitAggregateArrowFfi(
    mut env: JNIEnv,
    _class: JClass,
    _cache_mgr_ptr: jlong,
    searcher_ptrs: JLongArray,
    query_ast_json: JString,
    agg_name: JString,
    agg_json: JString,
    array_addrs: JLongArray,
    schema_addrs: JLongArray,
) -> jint {
    let t0 = std::time::Instant::now();
    perf_println!("⏱️ AGG_FFI: nativeMultiSplitAggregateArrowFfi START");

    let result: Result<jint, anyhow::Error> = (|| {
        let query_json: String = env
            .get_string(&query_ast_json)
            .map_err(|e| anyhow::anyhow!("Failed to get query JSON: {}", e))?
            .into();
        let agg_name_str: String = env
            .get_string(&agg_name)
            .map_err(|e| anyhow::anyhow!("Failed to get agg name: {}", e))?
            .into();
        let agg_json_str: String = env
            .get_string(&agg_json)
            .map_err(|e| anyhow::anyhow!("Failed to get agg JSON: {}", e))?
            .into();

        // Extract searcher pointers
        let ptrs = unsafe {
            env.get_array_elements(&searcher_ptrs, jni::objects::ReleaseMode::NoCopyBack)
                .map_err(|e| anyhow::anyhow!("Failed to get searcher ptrs: {}", e))?
        };
        let ptr_slice: &[i64] =
            unsafe { std::slice::from_raw_parts(ptrs.as_ptr(), ptrs.len()) };
        let num_splits = ptr_slice.len();

        let arr_addrs_elems = unsafe {
            env.get_array_elements(&array_addrs, jni::objects::ReleaseMode::NoCopyBack)
                .map_err(|e| anyhow::anyhow!("Failed to get array addrs: {}", e))?
        };
        let sch_addrs_elems = unsafe {
            env.get_array_elements(&schema_addrs, jni::objects::ReleaseMode::NoCopyBack)
                .map_err(|e| anyhow::anyhow!("Failed to get schema addrs: {}", e))?
        };

        let arr_slice: &[i64] = unsafe {
            std::slice::from_raw_parts(arr_addrs_elems.as_ptr(), arr_addrs_elems.len())
        };
        let sch_slice: &[i64] = unsafe {
            std::slice::from_raw_parts(sch_addrs_elems.as_ptr(), sch_addrs_elems.len())
        };

        if num_splits == 0 {
            anyhow::bail!("No searcher pointers provided");
        }

        debug_println!(
            "AGG_FFI: Multi-split aggregation across {} splits",
            num_splits
        );

        // Collect owned data before entering async block
        let ptrs_owned: Vec<jlong> = ptr_slice.to_vec();
        let query_json_owned = query_json.clone();
        let agg_json_owned = agg_json_str.clone();

        // Search each split and collect intermediate aggregation bytes
        let intermediate_bytes_vec: Vec<Vec<u8>> = block_on_operation(async move {
            let mut results = Vec::with_capacity(ptrs_owned.len());
            for &sptr in &ptrs_owned {
                let ctx = perform_search_async_impl_leaf_response_with_aggregations(
                    sptr,
                    query_json_owned.clone(),
                    0,
                    Some(agg_json_owned.clone()),
                )
                .await?;

                if let Some(bytes) = ctx.leaf_response.intermediate_aggregation_result {
                    results.push(bytes);
                }
            }
            Ok::<Vec<Vec<u8>>, anyhow::Error>(results)
        })?;

        if intermediate_bytes_vec.is_empty() {
            anyhow::bail!("No splits returned aggregation results");
        }

        perf_println!(
            "⏱️ AGG_FFI: Collected {} intermediate results in {}ms",
            intermediate_bytes_vec.len(),
            t0.elapsed().as_millis()
        );

        // Deserialize and merge all intermediate results
        let t_merge = std::time::Instant::now();
        let mut merged: IntermediateAggregationResults =
            postcard::from_bytes(&intermediate_bytes_vec[0])
                .map_err(|e| anyhow::anyhow!("Failed to deserialize intermediate[0]: {}", e))?;

        for (i, bytes) in intermediate_bytes_vec.iter().enumerate().skip(1) {
            let next: IntermediateAggregationResults =
                postcard::from_bytes(bytes).map_err(|e| {
                    anyhow::anyhow!("Failed to deserialize intermediate[{}]: {}", i, e)
                })?;
            merged
                .merge_fruits(next)
                .map_err(|e| anyhow::anyhow!("Failed to merge intermediate[{}]: {}", i, e))?;
        }

        perf_println!(
            "⏱️ AGG_FFI: Merged {} intermediates in {}ms",
            intermediate_bytes_vec.len(),
            t_merge.elapsed().as_millis()
        );

        // Finalize
        let aggregations: Aggregations = serde_json::from_str(&agg_json_str)
            .map_err(|e| anyhow::anyhow!("Failed to parse aggregation JSON: {}", e))?;

        let limits = AggregationLimitsGuard::new(Some(50_000_000), Some(65_000));
        let final_results: AggregationResults =
            merged.into_final_result(aggregations, limits)?;

        let agg_result = final_results
            .0
            .get(&agg_name_str)
            .ok_or_else(|| {
                let available: Vec<&String> = final_results.0.keys().collect();
                anyhow::anyhow!(
                    "Aggregation '{}' not found. Available: {:?}",
                    agg_name_str,
                    available
                )
            })?;

        let is_date_hist = is_date_histogram_aggregation(&agg_json_str, &agg_name_str);
        let batch =
            aggregation_result_to_record_batch(&agg_name_str, agg_result, is_date_hist)?;

        let row_count = export_record_batch_ffi(&batch, arr_slice, sch_slice)?;

        perf_println!(
            "⏱️ AGG_FFI: nativeMultiSplitAggregateArrowFfi DONE — {} splits, {} rows, {}ms",
            num_splits,
            row_count,
            t0.elapsed().as_millis()
        );

        Ok(row_count as jint)
    })();

    match result {
        Ok(count) => count,
        Err(e) => {
            perf_println!(
                "⏱️ AGG_FFI: nativeMultiSplitAggregateArrowFfi FAILED after {}ms: {}",
                t0.elapsed().as_millis(),
                e
            );
            to_java_exception(&mut env, &e);
            -1
        }
    }
}

/// Schema query: returns JSON describing the Arrow schema for the given aggregation.
/// Format: {"columns": [{"name": "key", "type": "Utf8"}, ...], "row_count": N}
///
/// Java signature: nativeAggregationArrowSchema(long searcherPtr, String queryAstJson,
///     String aggName, String aggJson) → String
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitSearcher_nativeAggregationArrowSchema(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    query_ast_json: JString,
    agg_name: JString,
    agg_json: JString,
) -> jstring {
    let t0 = std::time::Instant::now();

    let result: Result<String, anyhow::Error> = (|| {
        let query_json: String = env
            .get_string(&query_ast_json)
            .map_err(|e| anyhow::anyhow!("Failed to get query JSON: {}", e))?
            .into();
        let agg_name_str: String = env
            .get_string(&agg_name)
            .map_err(|e| anyhow::anyhow!("Failed to get agg name: {}", e))?
            .into();
        let agg_json_str: String = env
            .get_string(&agg_json)
            .map_err(|e| anyhow::anyhow!("Failed to get agg JSON: {}", e))?
            .into();

        // Run search with aggregations
        let agg_json_for_search = agg_json_str.clone();
        let ctx = block_on_operation(async move {
            perform_search_async_impl_leaf_response_with_aggregations(
                searcher_ptr,
                query_json,
                0,
                Some(agg_json_for_search),
            )
            .await
        })?;

        let leaf = ctx.leaf_response;
        let intermediate_bytes = leaf
            .intermediate_aggregation_result
            .ok_or_else(|| anyhow::anyhow!("No aggregation results"))?;

        let effective_agg_json = ctx.effective_agg_json.unwrap_or(agg_json_str);

        let intermediate: IntermediateAggregationResults =
            postcard::from_bytes(&intermediate_bytes)
                .map_err(|e| anyhow::anyhow!("Failed to deserialize: {}", e))?;

        let aggregations: Aggregations = serde_json::from_str(&effective_agg_json)?;
        let limits = AggregationLimitsGuard::new(Some(50_000_000), Some(65_000));
        let final_results: AggregationResults =
            intermediate.into_final_result(aggregations, limits)?;

        let agg_result = final_results
            .0
            .get(&agg_name_str)
            .ok_or_else(|| anyhow::anyhow!("Aggregation '{}' not found", agg_name_str))?;

        let is_date_hist = is_date_histogram_aggregation(&effective_agg_json, &agg_name_str);
        aggregation_result_arrow_schema_json(&agg_name_str, agg_result, is_date_hist)
    })();

    match result {
        Ok(json) => {
            perf_println!(
                "⏱️ AGG_FFI: nativeAggregationArrowSchema done in {}ms",
                t0.elapsed().as_millis()
            );
            match env.new_string(&json) {
                Ok(s) => s.into_raw(),
                Err(e) => {
                    to_java_exception(
                        &mut env,
                        &anyhow::anyhow!("Failed to create Java string: {}", e),
                    );
                    std::ptr::null_mut()
                }
            }
        }
        Err(e) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

/// Test helper: read back Arrow FFI column data as JSON for validation.
///
/// Takes the arrayAddrs and schemaAddrs that were previously written by aggregateArrowFfi
/// and reconstructs the data, returning a JSON string like:
/// {"columns":[{"name":"key","type":"Utf8","values":["ok","error"]},
///             {"name":"doc_count","type":"Int64","values":[3,2]}]}
///
/// Java signature: nativeReadAggArrowColumnsAsJson(long[] arrayAddrs, long[] schemaAddrs, int numCols, int numRows) -> String
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_AggregationArrowFfiTest_nativeReadAggArrowColumnsAsJson(
    mut env: JNIEnv,
    _class: JClass,
    array_addrs: JLongArray,
    schema_addrs: JLongArray,
    num_cols: jint,
    num_rows: jint,
) -> jstring {
    let result: Result<String, anyhow::Error> = (|| {
        let arr_addrs = unsafe {
            env.get_array_elements(&array_addrs, jni::objects::ReleaseMode::NoCopyBack)
                .map_err(|e| anyhow::anyhow!("Failed to get array addrs: {}", e))?
        };
        let sch_addrs = unsafe {
            env.get_array_elements(&schema_addrs, jni::objects::ReleaseMode::NoCopyBack)
                .map_err(|e| anyhow::anyhow!("Failed to get schema addrs: {}", e))?
        };

        let arr_slice: &[i64] =
            unsafe { std::slice::from_raw_parts(arr_addrs.as_ptr(), arr_addrs.len()) };
        let sch_slice: &[i64] =
            unsafe { std::slice::from_raw_parts(sch_addrs.as_ptr(), sch_addrs.len()) };

        let n_cols = num_cols as usize;
        let n_rows = num_rows as usize;

        let mut columns = Vec::with_capacity(n_cols);

        for i in 0..n_cols {
            let array_ptr = arr_slice[i] as *mut FFI_ArrowArray;
            let schema_ptr = sch_slice[i] as *mut FFI_ArrowSchema;

            // Read FFI structs (take ownership)
            let ffi_array = unsafe { std::ptr::read_unaligned(array_ptr) };
            let ffi_schema = unsafe { std::ptr::read_unaligned(schema_ptr) };

            // Get field name before from_ffi consumes the array
            let field_name = ffi_schema.name().unwrap_or("unknown").to_string();

            // Import back to Arrow
            let data = unsafe {
                arrow::ffi::from_ffi(ffi_array, &ffi_schema)
                    .map_err(|e| anyhow::anyhow!("Failed to import FFI column {}: {}", i, e))?
            };
            let array = arrow_array::make_array(data);
            let type_name = format!("{:?}", array.data_type());

            // Extract values as JSON
            let mut values = Vec::with_capacity(n_rows);
            for row in 0..n_rows {
                use arrow_array::Array;
                if array.is_null(row) {
                    values.push(serde_json::Value::Null);
                } else if let Some(arr) = array.as_any().downcast_ref::<arrow_array::StringArray>() {
                    values.push(serde_json::Value::String(arr.value(row).to_string()));
                } else if let Some(arr) = array.as_any().downcast_ref::<arrow_array::Int64Array>() {
                    values.push(serde_json::json!(arr.value(row)));
                } else if let Some(arr) = array.as_any().downcast_ref::<arrow_array::Float64Array>() {
                    values.push(serde_json::json!(arr.value(row)));
                } else if let Some(arr) = array.as_any().downcast_ref::<arrow_array::TimestampMicrosecondArray>() {
                    values.push(serde_json::json!(arr.value(row)));
                } else {
                    values.push(serde_json::json!(format!("<unsupported type: {}>", type_name)));
                }
            }

            columns.push(serde_json::json!({
                "name": field_name,
                "type": type_name,
                "values": values,
            }));
        }

        Ok(serde_json::json!({"columns": columns}).to_string())
    })();

    match result {
        Ok(json) => match env.new_string(&json) {
            Ok(s) => s.into_raw(),
            Err(e) => {
                to_java_exception(
                    &mut env,
                    &anyhow::anyhow!("Failed to create Java string: {}", e),
                );
                std::ptr::null_mut()
            }
        },
        Err(e) => {
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}
