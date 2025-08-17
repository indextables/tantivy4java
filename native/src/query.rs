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
use tantivy::query::{Query as TantivyQuery, TermQuery};
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
    mut env: JNIEnv,
    _class: JClass,
) -> jlong {
    handle_error(&mut env, "Query native methods not fully implemented yet");
    0
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
    _subqueries: JObject,
) -> jlong {
    handle_error(&mut env, "Query native methods not fully implemented yet");
    0
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
    handle_error(&mut env, "Query native methods not fully implemented yet");
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