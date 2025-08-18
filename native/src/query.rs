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
use jni::sys::{jlong, jboolean, jint, jdouble, jlongArray};
use jni::JNIEnv;
use tantivy::query::{Query as TantivyQuery, TermQuery, AllQuery, BooleanQuery, Occur};
use tantivy::schema::{Schema, Term, IndexRecordOption};
use crate::utils::{register_object, remove_object, with_object, handle_error};

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeTermQuery(
    mut env: JNIEnv,
    _class: JClass,
    schema_ptr: jlong,
    field_name: JString,
    field_value: JString,
    index_option: JString,
) -> jlong {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return 0;
        }
    };
    
    let field_value_str: String = match env.get_string(&field_value) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field value");
            return 0;
        }
    };
    
    let index_option_str: String = match env.get_string(&index_option) {
        Ok(s) => s.into(),
        Err(_) => "position".to_string(),
    };
    
    let result = with_object::<Schema, Result<Box<dyn TantivyQuery>, String>>(schema_ptr as u64, |schema| {
        // Get field by name
        let field = match schema.get_field(&field_name_str) {
            Ok(f) => f,
            Err(_) => return Err(format!("Field '{}' not found in schema", field_name_str)),
        };
        
        // Create term
        let term = Term::from_field_text(field, &field_value_str);
        
        // Parse index option
        let idx_option = match index_option_str.as_str() {
            "position" => IndexRecordOption::WithFreqsAndPositions,
            "freq" => IndexRecordOption::WithFreqs,
            "basic" => IndexRecordOption::Basic,
            _ => IndexRecordOption::WithFreqsAndPositions,
        };
        
        // Create term query
        let query = TermQuery::new(term, idx_option);
        Ok(Box::new(query) as Box<dyn TantivyQuery>)
    });
    
    match result {
        Some(Ok(query)) => {
            register_object(query) as jlong
        },
        Some(Err(err)) => {
            handle_error(&mut env, &err);
            0
        },
        None => {
            handle_error(&mut env, "Invalid Schema pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeTermSetQuery(
    mut env: JNIEnv,
    _class: JClass,
    _schema_ptr: jlong,
    _field_name: JString,
    _field_values: JObject,
) -> jlong {
    handle_error(&mut env, "Query native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeAllQuery(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let query = AllQuery;
    register_object(Box::new(query) as Box<dyn TantivyQuery>) as jlong
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeFuzzyTermQuery(
    mut env: JNIEnv,
    _class: JClass,
    _schema_ptr: jlong,
    _field_name: JString,
    _text: JString,
    _distance: jint,
    _transposition_cost_one: jboolean,
    _prefix: jboolean,
) -> jlong {
    handle_error(&mut env, "Query native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativePhraseQuery(
    mut env: JNIEnv,
    _class: JClass,
    _schema_ptr: jlong,
    _field_name: JString,
    _words: JObject,
    _slop: jint,
) -> jlong {
    handle_error(&mut env, "Query native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeBooleanQuery(
    mut env: JNIEnv,
    _class: JClass,
    subqueries: JObject,
) -> jlong {
    if subqueries.is_null() {
        handle_error(&mut env, "Subqueries list cannot be null");
        return 0;
    }
    
    // Extract the list of OccurQuery objects
    let occur_queries = match extract_occur_queries(&mut env, &subqueries) {
        Ok(queries) => queries,
        Err(e) => {
            handle_error(&mut env, &e);
            return 0;
        }
    };
    
    if occur_queries.is_empty() {
        handle_error(&mut env, "BooleanQuery requires at least one subquery");
        return 0;
    }
    
    // Build the subqueries vector for BooleanQuery
    let mut tantivy_subqueries = Vec::new();
    
    for (occur, query_ptr) in occur_queries {
        // Get the query from the object store
        let query_box = match with_object::<Box<dyn TantivyQuery>, Option<Box<dyn TantivyQuery>>>(query_ptr as u64, |query| {
            // We need to clone the query to avoid ownership issues
            Some(query.box_clone())
        }) {
            Some(Some(query)) => query,
            _ => {
                handle_error(&mut env, "Invalid query pointer in boolean query");
                return 0;
            }
        };
        
        tantivy_subqueries.push((occur, query_box));
    }
    
    // Create the BooleanQuery
    let boolean_query = BooleanQuery::from(tantivy_subqueries);
    let boxed_query: Box<dyn TantivyQuery> = Box::new(boolean_query);
    
    register_object(boxed_query) as jlong
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeDisjunctionMaxQuery(
    mut env: JNIEnv,
    _class: JClass,
    _query_ptrs: jlongArray,
    _tie_breaker: JObject,
) -> jlong {
    handle_error(&mut env, "Query native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeBoostQuery(
    mut env: JNIEnv,
    _class: JClass,
    _query_ptr: jlong,
    _boost: jdouble,
) -> jlong {
    handle_error(&mut env, "Query native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeRegexQuery(
    mut env: JNIEnv,
    _class: JClass,
    _schema_ptr: jlong,
    _field_name: JString,
    _regex_pattern: JString,
) -> jlong {
    handle_error(&mut env, "Query native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeMoreLikeThisQuery(
    mut env: JNIEnv,
    _class: JClass,
    _doc_address_ptr: jlong,
    _min_doc_frequency: JObject,
    _max_doc_frequency: JObject,
    _min_term_frequency: JObject,
    _max_query_terms: JObject,
    _min_word_length: JObject,
    _max_word_length: JObject,
    _boost_factor: JObject,
    _stop_words: JObject,
) -> jlong {
    handle_error(&mut env, "Query native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeConstScoreQuery(
    mut env: JNIEnv,
    _class: JClass,
    _query_ptr: jlong,
    _score: jdouble,
) -> jlong {
    handle_error(&mut env, "Query native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeRangeQuery(
    mut env: JNIEnv,
    _class: JClass,
    _schema_ptr: jlong,
    _field_name: JString,
    _field_type: jint,
    _lower_bound: JObject,
    _upper_bound: JObject,
    _include_lower: jboolean,
    _include_upper: jboolean,
) -> jlong {
    // RangeQuery implementation is complex due to type conversion requirements
    // For now, return a placeholder error until we implement proper type handling
    handle_error(&mut env, "RangeQuery implementation requires proper type conversion - not fully implemented");
    0
}


#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeExplain(
    mut env: JNIEnv,
    _class: JClass,
    _query_ptr: jlong,
    _searcher_ptr: jlong,
    _doc_address_ptr: jlong,
) -> jlong {
    handle_error(&mut env, "Query native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    remove_object(ptr as u64);
}

// Helper function to extract OccurQuery objects from Java List
fn extract_occur_queries(env: &mut JNIEnv, list_obj: &JObject) -> Result<Vec<(Occur, i64)>, String> {
    // Get the List size
    let size = match env.call_method(list_obj, "size", "()I", &[]) {
        Ok(result) => match result.i() {
            Ok(s) => s,
            Err(_) => return Err("Failed to get list size".to_string()),
        },
        Err(_) => return Err("Failed to call size() on list".to_string()),
    };
    
    let mut occur_queries = Vec::with_capacity(size as usize);
    
    // Extract each OccurQuery from the list
    for i in 0..size {
        let element = match env.call_method(list_obj, "get", "(I)Ljava/lang/Object;", &[i.into()]) {
            Ok(result) => result.l().map_err(|_| "Failed to get object from list")?,
            Err(_) => return Err("Failed to call get() on list".to_string()),
        };
        
        // Get the Occur enum value
        let occur_obj = match env.call_method(&element, "getOccur", "()Lcom/tantivy4java/Occur;", &[]) {
            Ok(result) => result.l().map_err(|_| "Failed to get Occur object")?,
            Err(_) => return Err("Failed to call getOccur() on OccurQuery".to_string()),
        };
        
        let occur_value = match env.call_method(&occur_obj, "getValue", "()I", &[]) {
            Ok(result) => match result.i() {
                Ok(v) => v,
                Err(_) => return Err("Failed to get occur value".to_string()),
            },
            Err(_) => return Err("Failed to call getValue() on Occur".to_string()),
        };
        
        // Convert Java Occur to Tantivy Occur
        let occur = match occur_value {
            1 => Occur::Must,
            2 => Occur::Should,
            3 => Occur::MustNot,
            _ => return Err(format!("Unknown Occur value: {}", occur_value)),
        };
        
        // Get the Query object
        let query_obj = match env.call_method(&element, "getQuery", "()Lcom/tantivy4java/Query;", &[]) {
            Ok(result) => result.l().map_err(|_| "Failed to get Query object")?,
            Err(_) => return Err("Failed to call getQuery() on OccurQuery".to_string()),
        };
        
        // Get the native pointer from the Query
        let query_ptr = match env.call_method(&query_obj, "getNativePtr", "()J", &[]) {
            Ok(result) => match result.j() {
                Ok(ptr) => ptr,
                Err(_) => return Err("Failed to get query native pointer".to_string()),
            },
            Err(_) => return Err("Failed to call getNativePtr() on Query".to_string()),
        };
        
        occur_queries.push((occur, query_ptr));
    }
    
    Ok(occur_queries)
}