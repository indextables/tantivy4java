/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use jni::objects::{JClass, JString, JObject};
use jni::sys::{jlong, jboolean, jobject};
use jni::JNIEnv;
use tantivy::Term;
use tantivy::schema::Schema;
use std::net::IpAddr;
use std::sync::Arc;
use crate::utils::{handle_error, with_arc_safe, arc_to_jlong, release_arc};
use crate::debug_println;

// Submodules
pub mod batch_parsing;
pub mod jni_searcher;
pub mod jni_index_writer;
pub mod aggregation;

// Re-exports
use aggregation::{deserialize_aggregation_results, find_specific_aggregation_result};

// Supporting classes native methods - moved to separate modules

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_result_SearchResult_nativeGetHits(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jobject {
    // Clone the search results to avoid holding locks during object creation
    // First try regular search results (Vec<(f32, DocAddress)>)
    let search_results_clone = if let Some(results) = with_arc_safe::<Vec<(f32, tantivy::DocAddress)>, Vec<(f32, tantivy::DocAddress)>>(
        _ptr,
        |search_results_arc| search_results_arc.as_ref().clone()
    ) {
        results
    } else if let Some(results) = with_arc_safe::<crate::split_searcher::EnhancedSearchResult, Vec<(f32, tantivy::DocAddress)>>(
        _ptr,
        |enhanced_result_arc| enhanced_result_arc.hits.clone()
    ) {
        results
    } else {
        handle_error(&mut env, "Invalid SearchResult pointer");
        return std::ptr::null_mut();
    };
    
    // Create the ArrayList with proper Hit objects containing scores and DocAddress
    match (|| -> Result<jobject, String> {
        let array_list_class = env.find_class("java/util/ArrayList").map_err(|e| e.to_string())?;
        let array_list = env.new_object(&array_list_class, "()V", &[]).map_err(|e| e.to_string())?;
        
        for (score, doc_address) in search_results_clone.iter() {
            // Create DocAddress object first
            let doc_address_arc = Arc::new(*doc_address);
            let doc_address_ptr = arc_to_jlong(doc_address_arc);
            let doc_address_class = env.find_class("io/indextables/tantivy4java/core/DocAddress").map_err(|e| e.to_string())?;
            let doc_address_obj = env.new_object(&doc_address_class, "(J)V", &[doc_address_ptr.into()]).map_err(|e| e.to_string())?;
            
            // Create Hit object (SearchResult$Hit is the inner class)
            let hit_class = env.find_class("io/indextables/tantivy4java/result/SearchResult$Hit").map_err(|e| e.to_string())?;
            let hit_obj = env.new_object(
                &hit_class, 
                "(DLio/indextables/tantivy4java/core/DocAddress;)V", 
                &[(*score as f64).into(), (&doc_address_obj).into()]
            ).map_err(|e| e.to_string())?;
            
            // Add Hit to the ArrayList
            env.call_method(
                &array_list,
                "add",
                "(Ljava/lang/Object;)Z",
                &[(&hit_obj).into()]
            ).map_err(|e| e.to_string())?;
        }
        
        Ok(array_list.into_raw())
    })() {
        Ok(obj) => obj,
        Err(err) => {
            handle_error(&mut env, &err);
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_result_SearchResult_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    release_arc(ptr);
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_result_SearchResult_nativeHasAggregations(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jboolean {
    // Check if the SearchResult contains aggregation results
    debug_println!("RUST DEBUG: ========== nativeHasAggregations CALLED with ptr={} ==========", ptr);
    // First try regular search results, then try enhanced search results
    debug_println!("RUST DEBUG: nativeHasAggregations checking ptr type...");
    let has_aggregations = if let Some(has_aggs) = with_arc_safe::<Vec<(f32, tantivy::DocAddress)>, bool>(
        ptr,
        |_search_results_arc| {
            debug_println!("RUST DEBUG: SearchResult is regular Vec type (no aggregations)");
            false  // Regular search results don't have aggregations
        }
    ) {
        has_aggs
    } else if let Some(has_aggs) = with_arc_safe::<crate::split_searcher::EnhancedSearchResult, bool>(
        ptr,
        |enhanced_result_arc| {
            let has_aggs = enhanced_result_arc.aggregation_results.is_some();
            debug_println!("RUST DEBUG: SearchResult is EnhancedSearchResult type, has_aggregations: {}", has_aggs);
            if has_aggs {
                if let Some(ref agg_bytes) = enhanced_result_arc.aggregation_results {
                    debug_println!("RUST DEBUG: Aggregation bytes length: {}", agg_bytes.len());
                }
                if let Some(ref agg_json) = enhanced_result_arc.aggregation_json {
                    debug_println!("RUST DEBUG: Aggregation JSON: {}", agg_json);
                } else {
                    debug_println!("RUST DEBUG: No aggregation JSON stored");
                }
            }
            has_aggs
        }
    ) {
        has_aggs
    } else {
        debug_println!("RUST DEBUG: Invalid SearchResult pointer - unknown type");
        handle_error(&mut env, "Invalid SearchResult pointer");
        return 0; // false
    };

    debug_println!("RUST DEBUG: hasAggregations returning: {}", has_aggregations);
    if has_aggregations { 1 } else { 0 } // Convert bool to jboolean
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_result_SearchResult_nativeGetAggregations(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jobject {
    // For now, return a simple implementation that shows the method is working
    // TODO: Implement full aggregation deserialization and Java object creation

    debug_println!("RUST DEBUG: ========== nativeGetAggregations CALLED with ptr={} ==========", ptr);
    debug_println!("RUST DEBUG: nativeGetAggregations checking ptr type...");
    let has_aggregations = if let Some(has_aggs) = with_arc_safe::<Vec<(f32, tantivy::DocAddress)>, bool>(
        ptr,
        |_search_results_arc| {
            debug_println!("RUST DEBUG: nativeGetAggregations - SearchResult is regular Vec type (no aggregations)");
            false  // Regular search results don't have aggregations
        }
    ) {
        has_aggs
    } else if let Some(has_aggs) = with_arc_safe::<crate::split_searcher::EnhancedSearchResult, bool>(
        ptr,
        |enhanced_result_arc| {
            let has_aggs = enhanced_result_arc.aggregation_results.is_some();
            debug_println!("RUST DEBUG: nativeGetAggregations - SearchResult is EnhancedSearchResult type, has_aggregations: {}", has_aggs);
            has_aggs
        }
    ) {
        has_aggs
    } else {
        debug_println!("RUST DEBUG: nativeGetAggregations - Invalid SearchResult pointer - unknown type");
        handle_error(&mut env, "Invalid SearchResult pointer");
        return std::ptr::null_mut();
    };

    debug_println!("RUST DEBUG: nativeGetAggregations determined has_aggregations={}", has_aggregations);

    if has_aggregations {
        debug_println!("RUST DEBUG: has_aggregations=true, extracting aggregation results");
        // Extract and deserialize aggregation results
        let aggregation_map_result = with_arc_safe::<crate::split_searcher::EnhancedSearchResult, jobject>(
            ptr,
            |enhanced_result_arc| {
                debug_println!("RUST DEBUG: Inside enhanced_result_arc callback");
                if let Some(ref intermediate_agg_bytes) = enhanced_result_arc.aggregation_results {
                    debug_println!("RUST DEBUG: Found aggregation results with {} bytes", intermediate_agg_bytes.len());
                    // Deserialize postcard aggregation results
                    match deserialize_aggregation_results(&mut env, intermediate_agg_bytes, ptr) {
                        Ok(agg_map) => agg_map,
                        Err(e) => {
                            handle_error(&mut env, &format!("Failed to deserialize aggregations: {}", e));
                            std::ptr::null_mut()
                        }
                    }
                } else {
                    debug_println!("RUST DEBUG: enhanced_result_arc.aggregation_results is None");
                    // Return empty HashMap if no aggregation bytes
                    match env.new_object("java/util/HashMap", "()V", &[]) {
                        Ok(hashmap) => hashmap.into_raw(),
                        Err(e) => {
                            handle_error(&mut env, &format!("Failed to create HashMap: {}", e));
                            std::ptr::null_mut()
                        }
                    }
                }
            }
        );

        aggregation_map_result.unwrap_or_else(|| {
            handle_error(&mut env, "Failed to extract aggregation results");
            std::ptr::null_mut()
        })
    } else {
        debug_println!("RUST DEBUG: has_aggregations=false, returning empty HashMap");
        // Return empty HashMap if no aggregations
        match env.new_object("java/util/HashMap", "()V", &[]) {
            Ok(hashmap) => hashmap.into_raw(),
            Err(e) => {
                handle_error(&mut env, &format!("Failed to create HashMap: {}", e));
                std::ptr::null_mut()
            }
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_result_SearchResult_nativeGetAggregation(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    name: JString,
) -> jobject {
    let aggregation_name: String = match env.get_string(&name) {
        Ok(java_str) => java_str.into(),
        Err(e) => {
            handle_error(&mut env, &format!("Failed to extract aggregation name: {}", e));
            return std::ptr::null_mut();
        }
    };

    // Get the aggregation results from the SearchResult
    // First try regular search results, then try enhanced search results
    let aggregation_result = if let Some(result) = with_arc_safe::<Vec<(f32, tantivy::DocAddress)>, jobject>(
        ptr,
        |_search_results_arc| {
            // Regular search results don't have aggregations
            std::ptr::null_mut()
        }
    ) {
        Some(result)
    } else {
        with_arc_safe::<crate::split_searcher::EnhancedSearchResult, jobject>(
            ptr,
            |enhanced_result_arc| {
                if let Some(ref intermediate_agg_bytes) = enhanced_result_arc.aggregation_results {
                    // Get the aggregation JSON for proper deserialization
                    let aggregation_json = enhanced_result_arc.aggregation_json.as_deref().unwrap_or("{}");
                    // Deserialize and find the specific aggregation by name
                    match find_specific_aggregation_result(&mut env, intermediate_agg_bytes, &aggregation_name, aggregation_json) {
                        Ok(agg_result) => agg_result,
                        Err(e) => {
                            handle_error(&mut env, &format!("Failed to find aggregation '{}': {}", aggregation_name, e));
                            std::ptr::null_mut()
                        }
                    }
                } else {
                    std::ptr::null_mut()
                }
            }
        )
    };

    aggregation_result.unwrap_or(std::ptr::null_mut())
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_query_Explanation_nativeToJson(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jobject {
    handle_error(&mut env, "Explanation native methods not fully implemented yet");
    std::ptr::null_mut()
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_query_Explanation_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    release_arc(ptr);
}

// TextAnalyzer methods are now implemented in text_analyzer.rs

// Facet native methods
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_util_Facet_nativeFromEncoded(
    mut env: JNIEnv,
    _class: JClass,
    _encoded_bytes: jni::objects::JByteArray,
) -> jlong {
    handle_error(&mut env, "Facet native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_util_Facet_nativeRoot(
    mut env: JNIEnv,
    _class: JClass,
) -> jlong {
    handle_error(&mut env, "Facet native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_util_Facet_nativeFromString(
    mut env: JNIEnv,
    _class: JClass,
    _facet_string: JString,
) -> jlong {
    handle_error(&mut env, "Facet native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_util_Facet_nativeIsRoot(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jboolean {
    handle_error(&mut env, "Facet native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_util_Facet_nativeIsPrefixOf(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
    _other_ptr: jlong,
) -> jboolean {
    handle_error(&mut env, "Facet native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_util_Facet_nativeToPath(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jobject {
    handle_error(&mut env, "Facet native methods not fully implemented yet");
    std::ptr::null_mut()
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_util_Facet_nativeToPathStr(
    mut env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) -> jobject {
    handle_error(&mut env, "Facet native methods not fully implemented yet");
    std::ptr::null_mut()
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_util_Facet_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    release_arc(ptr);
}

// Helper function to convert Java Object to Tantivy Term
fn convert_jobject_to_term(
    env: &mut JNIEnv,
    field_value: &JObject,
    field: tantivy::schema::Field,
    schema: &Schema,
) -> Result<Term, String> {
    if field_value.is_null() {
        return Err("Field value cannot be null for delete operation".to_string());
    }
    
    // Get field entry to determine the field type
    let _field_entry = schema.get_field_entry(field);
    
    // Check types in specific order to avoid method call errors
    
    // Check if it's a Boolean first (to avoid calling wrong methods)
    if let Ok(true) = env.is_instance_of(field_value, "java/lang/Boolean") {
        match env.call_method(field_value, "booleanValue", "()Z", &[]) {
            Ok(bool_value) => {
                if let Ok(value) = bool_value.z() {
                    return Ok(Term::from_field_bool(field, value));
                }
            },
            Err(e) => return Err(format!("Failed to get boolean value: {}", e)),
        }
    }
    
    // Check if it's a Long (i64)
    if let Ok(true) = env.is_instance_of(field_value, "java/lang/Long") {
        match env.call_method(field_value, "longValue", "()J", &[]) {
            Ok(long_value) => {
                if let Ok(value) = long_value.j() {
                    return Ok(Term::from_field_i64(field, value));
                }
            },
            Err(e) => return Err(format!("Failed to get long value: {}", e)),
        }
    }
    
    // Check if it's a Double (f64)
    if let Ok(true) = env.is_instance_of(field_value, "java/lang/Double") {
        match env.call_method(field_value, "doubleValue", "()D", &[]) {
            Ok(double_value) => {
                if let Ok(value) = double_value.d() {
                    return Ok(Term::from_field_f64(field, value));
                }
            },
            Err(e) => return Err(format!("Failed to get double value: {}", e)),
        }
    }
    
    // Check if it's a LocalDateTime (for date fields)
    if let Ok(_) = env.is_instance_of(field_value, "java/time/LocalDateTime") {
        // Convert LocalDateTime to DateTime and create term
        match crate::document::convert_java_localdatetime_to_tantivy(env, field_value) {
            Ok(datetime) => return Ok(Term::from_field_date(field, datetime)),
            Err(e) => return Err(format!("Failed to convert LocalDateTime: {}", e)),
        }
    }
    
    // Try to convert to string as a fallback
    let string_obj = env.call_method(field_value, "toString", "()Ljava/lang/String;", &[])
        .map_err(|_| "Failed to call toString on field value")?;
    let java_string = string_obj.l()
        .map_err(|_| "Failed to get string object")?;
    let java_string_obj = JString::from(java_string);
    let rust_string = env.get_string(&java_string_obj)
        .map_err(|_| "Failed to convert Java string to Rust string")?;
    let string_value: String = rust_string.into();
    
    // Try to parse as IP address first
    if let Ok(ip_addr) = string_value.parse::<IpAddr>() {
        let ipv6_addr = match ip_addr {
            IpAddr::V4(ipv4) => ipv4.to_ipv6_mapped(),
            IpAddr::V6(ipv6) => ipv6,
        };
        return Ok(Term::from_field_ip_addr(field, ipv6_addr));
    }
    
    // Default to text term
    Ok(Term::from_field_text(field, &string_value))
}

// SegmentMeta native methods
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_SegmentMeta_nativeGetSegmentId(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jobject {
    let result = with_arc_safe::<tantivy::SegmentMeta, Option<String>>(ptr, |segment_meta_arc| {
        let segment_meta = segment_meta_arc.as_ref();
        Some(segment_meta.id().uuid_string())
    });
    
    match result {
        Some(Some(segment_id)) => {
            match env.new_string(&segment_id) {
                Ok(string) => string.into_raw(),
                Err(_) => {
                    handle_error(&mut env, "Failed to create Java string");
                    std::ptr::null_mut()
                }
            }
        },
        _ => {
            handle_error(&mut env, "Invalid SegmentMeta pointer");
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_SegmentMeta_nativeGetMaxDoc(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jlong {
    let result = with_arc_safe::<tantivy::SegmentMeta, Option<u32>>(ptr, |segment_meta_arc| {
        let segment_meta = segment_meta_arc.as_ref();
        Some(segment_meta.max_doc())
    });
    
    match result {
        Some(Some(max_doc)) => max_doc as jlong,
        _ => {
            handle_error(&mut env, "Invalid SegmentMeta pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_SegmentMeta_nativeGetNumDeletedDocs(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jlong {
    let result = with_arc_safe::<tantivy::SegmentMeta, Option<u32>>(ptr, |segment_meta_arc| {
        let segment_meta = segment_meta_arc.as_ref();
        Some(segment_meta.num_deleted_docs())
    });
    
    match result {
        Some(Some(num_deleted)) => num_deleted as jlong,
        _ => {
            handle_error(&mut env, "Invalid SegmentMeta pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_SegmentMeta_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    release_arc(ptr);
}

