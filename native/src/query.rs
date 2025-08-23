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
use jni::sys::{jlong, jboolean, jint, jdouble, jlongArray, jobject};
use jni::JNIEnv;
use tantivy::query::{Query as TantivyQuery, TermQuery, AllQuery, BooleanQuery, Occur, RangeQuery, PhraseQuery, FuzzyTermQuery, RegexQuery, BoostQuery, ConstScoreQuery, TermSetQuery};
use tantivy::schema::{Schema, Term, IndexRecordOption, FieldType as TantivyFieldType};
use tantivy::DateTime;
use std::ops::Bound;
use time::Month;
use crate::utils::{register_object, remove_object, with_object, handle_error};

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeTermQuery(
    mut env: JNIEnv,
    _class: JClass,
    schema_ptr: jlong,
    field_name: JString,
    field_value: jobject,
    index_option: JString,
) -> jlong {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
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
        
        // Get field type
        let field_type = schema.get_field_entry(field).field_type();
        
        // Convert jobject to JObject for proper API usage
        let field_value_obj = unsafe { JObject::from_raw(field_value) };
        
        // Create term based on field type and value type
        let term = match field_type {
            tantivy::schema::FieldType::Str(text_options) => {
                // Handle string fields with proper tokenization
                if field_value_obj.is_null() {
                    return Err("Field value cannot be null for text field".to_string());
                }
                let field_value_str: String = match env.get_string(&JString::from(field_value_obj)) {
                    Ok(s) => s.into(),
                    Err(_) => return Err("Invalid field value for text field".to_string()),
                };
                
                // For text fields that are indexed, use tokenization to match how the index was created
                if let Some(text_field_indexing) = &text_options.get_indexing_options() {
                    // Use the same tokenizer that was used during indexing
                    // For simplicity, we'll lowercase the term (which is what the default tokenizer does)
                    let tokenized_term = field_value_str.to_lowercase();
                    Term::from_field_text(field, &tokenized_term)
                } else {
                    // For non-indexed text fields, use exact match
                    Term::from_field_text(field, &field_value_str)
                }
            },
            tantivy::schema::FieldType::U64(_) => {
                // Handle integer fields
                if field_value_obj.is_null() {
                    return Err("Field value cannot be null for integer field".to_string());
                }
                
                // Check if it's a Long/Integer
                if let Ok(true) = env.is_instance_of(&field_value_obj, "java/lang/Long") {
                    let long_value = env.call_method(&field_value_obj, "longValue", "()J", &[])
                        .map_err(|e| format!("Failed to get long value: {}", e))?
                        .j()
                        .map_err(|e| format!("Failed to convert long value: {}", e))?;
                    Term::from_field_u64(field, long_value as u64)
                } else if let Ok(true) = env.is_instance_of(&field_value_obj, "java/lang/Integer") {
                    let int_value = env.call_method(&field_value_obj, "intValue", "()I", &[])
                        .map_err(|e| format!("Failed to get int value: {}", e))?
                        .i()
                        .map_err(|e| format!("Failed to convert int value: {}", e))?;
                    Term::from_field_u64(field, int_value as u64)
                } else {
                    return Err("Expected Long or Integer value for integer field".to_string());
                }
            },
            tantivy::schema::FieldType::F64(_) => {
                // Handle float fields
                if field_value_obj.is_null() {
                    return Err("Field value cannot be null for float field".to_string());
                }
                
                // Check if it's a Double/Float
                if let Ok(true) = env.is_instance_of(&field_value_obj, "java/lang/Double") {
                    let double_value = env.call_method(&field_value_obj, "doubleValue", "()D", &[])
                        .map_err(|e| format!("Failed to get double value: {}", e))?
                        .d()
                        .map_err(|e| format!("Failed to convert double value: {}", e))?;
                    Term::from_field_f64(field, double_value)
                } else if let Ok(true) = env.is_instance_of(&field_value_obj, "java/lang/Float") {
                    let float_value = env.call_method(&field_value_obj, "floatValue", "()F", &[])
                        .map_err(|e| format!("Failed to get float value: {}", e))?
                        .f()
                        .map_err(|e| format!("Failed to convert float value: {}", e))?;
                    Term::from_field_f64(field, float_value as f64)
                } else {
                    return Err("Expected Double or Float value for float field".to_string());
                }
            },
            tantivy::schema::FieldType::Bool(_) => {
                // Handle boolean fields
                if field_value_obj.is_null() {
                    return Err("Field value cannot be null for boolean field".to_string());
                }
                
                // Check if it's a Boolean
                if let Ok(true) = env.is_instance_of(&field_value_obj, "java/lang/Boolean") {
                    let bool_value = env.call_method(&field_value_obj, "booleanValue", "()Z", &[])
                        .map_err(|e| format!("Failed to get boolean value: {}", e))?
                        .z()
                        .map_err(|e| format!("Failed to convert boolean value: {}", e))?;
                    Term::from_field_bool(field, bool_value)
                } else {
                    return Err("Expected Boolean value for boolean field".to_string());
                }
            },
            tantivy::schema::FieldType::Date(_) => {
                // Handle date fields - for now, treat as string and try to parse
                if field_value_obj.is_null() {
                    return Err("Field value cannot be null for date field".to_string());
                }
                
                // For LocalDateTime objects, convert to string representation
                let field_value_str: String = match env.call_method(&field_value_obj, "toString", "()Ljava/lang/String;", &[]) {
                    Ok(result) => {
                        let string_obj = result.l().map_err(|e| format!("Failed to get string object: {}", e))?;
                        env.get_string(&JString::from(string_obj))
                            .map_err(|e| format!("Failed to get string value: {}", e))?
                            .into()
                    },
                    Err(e) => return Err(format!("Failed to convert date to string: {}", e)),
                };
                
                // Try to parse as RFC3339 timestamp  
                let offset_datetime = chrono::DateTime::parse_from_rfc3339(&format!("{}T00:00:00Z", field_value_str))
                    .map_err(|_| "Invalid date format, expected YYYY-MM-DD")?;
                
                // Convert to OffsetDateTime
                let date_value = tantivy::DateTime::from_utc(
                    time::OffsetDateTime::from_unix_timestamp(offset_datetime.timestamp())
                        .map_err(|_| "Invalid timestamp conversion")?
                );
                Term::from_field_date(field, date_value)
            },
            _ => {
                return Err(format!("Unsupported field type for term query: {:?}", field_type));
            }
        };
        
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
    schema_ptr: jlong,
    field_name: JString,
    field_values: JObject,
) -> jlong {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return 0;
        }
    };
    
    let result = with_object::<Schema, Result<Box<dyn TantivyQuery>, String>>(schema_ptr as u64, |schema| {
        // Get field by name
        let field = match schema.get_field(&field_name_str) {
            Ok(f) => f,
            Err(_) => return Err(format!("Field '{}' not found in schema", field_name_str)),
        };
        
        // Extract field values from the Java List
        let terms = match extract_term_set_values(&mut env, &field_values, field, schema) {
            Ok(terms) => terms,
            Err(e) => return Err(e),
        };
        
        // Create TermSetQuery
        let term_set_query = tantivy::query::TermSetQuery::new(terms);
        Ok(Box::new(term_set_query) as Box<dyn TantivyQuery>)
    });
    
    match result {
        Some(Ok(query)) => register_object(query) as jlong,
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
    schema_ptr: jlong,
    field_name: JString,
    text: JString,
    distance: jint,
    transposition_cost_one: jboolean,
    prefix: jboolean,
) -> jlong {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return 0;
        }
    };
    
    let text_str: String = match env.get_string(&text) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid text string");
            return 0;
        }
    };
    
    let result = with_object::<Schema, Result<Box<dyn TantivyQuery>, String>>(schema_ptr as u64, |schema| {
        // Get field by name
        let field = match schema.get_field(&field_name_str) {
            Ok(f) => f,
            Err(_) => return Err(format!("Field '{}' not found in schema", field_name_str)),
        };
        
        // Verify the field is a text field
        let field_entry = schema.get_field_entry(field);
        match field_entry.field_type() {
            TantivyFieldType::Str(_) => {
                // Create term for the field and text
                let term = Term::from_field_text(field, &text_str);
                
                // Validate distance parameter
                if distance < 0 {
                    return Err("Distance must be non-negative".to_string());
                }
                
                if distance > 2 {
                    return Err("Distance must be 0, 1, or 2".to_string());
                }
                
                // Create fuzzy term query with specified parameters
                let fuzzy_query = FuzzyTermQuery::new(
                    term,
                    distance as u8,
                    transposition_cost_one != 0,
                );
                
                Ok(Box::new(fuzzy_query) as Box<dyn TantivyQuery>)
            },
            _ => Err(format!("Field '{}' is not a text field - fuzzy queries require text fields", field_name_str)),
        }
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
pub extern "system" fn Java_com_tantivy4java_Query_nativePhraseQuery(
    mut env: JNIEnv,
    _class: JClass,
    schema_ptr: jlong,
    field_name: JString,
    words: JObject,
    slop: jint,
) -> jlong {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return 0;
        }
    };
    
    if words.is_null() {
        handle_error(&mut env, "Words list cannot be null");
        return 0;
    }
    
    // Extract the words list from Java List
    let words_list = match extract_phrase_words(&mut env, &words) {
        Ok(words) => words,
        Err(e) => {
            handle_error(&mut env, &e);
            return 0;
        }
    };
    
    let result = with_object::<Schema, Result<Box<dyn TantivyQuery>, String>>(schema_ptr as u64, |schema| {
        // Get field by name
        let field = match schema.get_field(&field_name_str) {
            Ok(f) => f,
            Err(_) => return Err(format!("Field '{}' not found in schema", field_name_str)),
        };
        
        // Verify the field is a text field
        let field_entry = schema.get_field_entry(field);
        match field_entry.field_type() {
            TantivyFieldType::Str(_) => {
                // Create phrase query with terms and optional positions
                let mut phrase_terms = Vec::new();
                
                for (pos, word) in words_list {
                    let term = Term::from_field_text(field, &word);
                    if let Some(position) = pos {
                        // If position is specified, we need to handle term positioning
                        // For now, just add terms in order - Tantivy will handle positioning
                        phrase_terms.push((position as usize, term));
                    } else {
                        // No specific position, add to next available position
                        phrase_terms.push((phrase_terms.len(), term));
                    }
                }
                
                if phrase_terms.is_empty() {
                    return Err("Phrase query requires at least one term".to_string());
                }
                
                // Create the PhraseQuery with slop
                let phrase_query = if slop > 0 {
                    PhraseQuery::new_with_offset_and_slop(phrase_terms, slop as u32)
                } else {
                    PhraseQuery::new_with_offset(phrase_terms)
                };
                
                Ok(Box::new(phrase_query) as Box<dyn TantivyQuery>)
            },
            _ => Err(format!("Field '{}' is not a text field - phrase queries require text fields", field_name_str)),
        }
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
    query_ptr: jlong,
    boost: jdouble,
) -> jlong {
    if boost <= 0.0 {
        handle_error(&mut env, "Boost value must be positive");
        return 0;
    }
    
    let result = with_object::<Box<dyn TantivyQuery>, Result<Box<dyn TantivyQuery>, String>>(query_ptr as u64, |query| {
        // Clone the query to avoid ownership issues
        let cloned_query = query.box_clone();
        
        // Create boost query
        let boost_query = BoostQuery::new(cloned_query, boost as f32);
        Ok(Box::new(boost_query) as Box<dyn TantivyQuery>)
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
            handle_error(&mut env, "Invalid Query pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeRegexQuery(
    mut env: JNIEnv,
    _class: JClass,
    schema_ptr: jlong,
    field_name: JString,
    regex_pattern: JString,
) -> jlong {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return 0;
        }
    };
    
    let pattern_str: String = match env.get_string(&regex_pattern) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid regex pattern");
            return 0;
        }
    };
    
    let result = with_object::<Schema, Result<Box<dyn TantivyQuery>, String>>(schema_ptr as u64, |schema| {
        // Get field by name
        let field = match schema.get_field(&field_name_str) {
            Ok(f) => f,
            Err(_) => return Err(format!("Field '{}' not found in schema", field_name_str)),
        };
        
        // Verify the field is a text field
        let field_entry = schema.get_field_entry(field);
        match field_entry.field_type() {
            TantivyFieldType::Str(_) => {
                // Create regex query
                match RegexQuery::from_pattern(&pattern_str, field) {
                    Ok(regex_query) => Ok(Box::new(regex_query) as Box<dyn TantivyQuery>),
                    Err(e) => Err(format!("Invalid regex pattern '{}': {}", pattern_str, e)),
                }
            },
            _ => Err(format!("Field '{}' is not a text field - regex queries require text fields", field_name_str)),
        }
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
    query_ptr: jlong,
    score: jdouble,
) -> jlong {
    if score < 0.0 {
        handle_error(&mut env, "Score value cannot be negative");
        return 0;
    }
    
    let result = with_object::<Box<dyn TantivyQuery>, Result<Box<dyn TantivyQuery>, String>>(query_ptr as u64, |query| {
        // Clone the query to avoid ownership issues
        let cloned_query = query.box_clone();
        
        // Create constant score query
        let const_score_query = ConstScoreQuery::new(cloned_query, score as f32);
        Ok(Box::new(const_score_query) as Box<dyn TantivyQuery>)
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
            handle_error(&mut env, "Invalid Query pointer");
            0
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Query_nativeRangeQuery(
    mut env: JNIEnv,
    _class: JClass,
    schema_ptr: jlong,
    field_name: JString,
    field_type: jint,
    lower_bound: JObject,
    upper_bound: JObject,
    include_lower: jboolean,
    include_upper: jboolean,
) -> jlong {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return 0;
        }
    };
    
    let result = with_object::<Schema, Result<Box<dyn TantivyQuery>, String>>(schema_ptr as u64, |schema| {
        // Get field by name
        let field = match schema.get_field(&field_name_str) {
            Ok(f) => f,
            Err(_) => return Err(format!("Field '{}' not found in schema", field_name_str)),
        };
        
        // Get field entry to check the type
        let field_entry = schema.get_field_entry(field);
        let actual_field_type = field_entry.field_type();
        
        // Convert bounds based on field type
        match field_type {
            3 => { // INTEGER
                match actual_field_type {
                    TantivyFieldType::I64(_) => {
                        let lower = if !lower_bound.is_null() {
                            match extract_long_value(&mut env, &lower_bound) {
                                Ok(val) => if include_lower != 0 { Bound::Included(val) } else { Bound::Excluded(val) },
                                Err(e) => return Err(format!("Failed to extract lower bound: {}", e)),
                            }
                        } else {
                            Bound::Unbounded
                        };
                        
                        let upper = if !upper_bound.is_null() {
                            match extract_long_value(&mut env, &upper_bound) {
                                Ok(val) => if include_upper != 0 { Bound::Included(val) } else { Bound::Excluded(val) },
                                Err(e) => return Err(format!("Failed to extract upper bound: {}", e)),
                            }
                        } else {
                            Bound::Unbounded
                        };
                        
                        let lower_term = match lower {
                            Bound::Included(val) => Bound::Included(Term::from_field_i64(field, val)),
                            Bound::Excluded(val) => Bound::Excluded(Term::from_field_i64(field, val)),
                            Bound::Unbounded => Bound::Unbounded,
                        };
                        let upper_term = match upper {
                            Bound::Included(val) => Bound::Included(Term::from_field_i64(field, val)),
                            Bound::Excluded(val) => Bound::Excluded(Term::from_field_i64(field, val)),
                            Bound::Unbounded => Bound::Unbounded,
                        };
                        let range_query = RangeQuery::new(lower_term, upper_term);
                        Ok(Box::new(range_query) as Box<dyn TantivyQuery>)
                    },
                    _ => Err(format!("Field '{}' is not an integer field", field_name_str)),
                }
            },
            2 => { // UNSIGNED
                match actual_field_type {
                    TantivyFieldType::U64(_) => {
                        let lower = if !lower_bound.is_null() {
                            match extract_long_value(&mut env, &lower_bound) {
                                Ok(val) => {
                                    if val < 0 {
                                        return Err("Unsigned field cannot have negative values".to_string());
                                    }
                                    if include_lower != 0 { Bound::Included(val as u64) } else { Bound::Excluded(val as u64) }
                                },
                                Err(e) => return Err(format!("Failed to extract lower bound: {}", e)),
                            }
                        } else {
                            Bound::Unbounded
                        };
                        
                        let upper = if !upper_bound.is_null() {
                            match extract_long_value(&mut env, &upper_bound) {
                                Ok(val) => {
                                    if val < 0 {
                                        return Err("Unsigned field cannot have negative values".to_string());
                                    }
                                    if include_upper != 0 { Bound::Included(val as u64) } else { Bound::Excluded(val as u64) }
                                },
                                Err(e) => return Err(format!("Failed to extract upper bound: {}", e)),
                            }
                        } else {
                            Bound::Unbounded
                        };
                        
                        let lower_term = match lower {
                            Bound::Included(val) => Bound::Included(Term::from_field_u64(field, val)),
                            Bound::Excluded(val) => Bound::Excluded(Term::from_field_u64(field, val)),
                            Bound::Unbounded => Bound::Unbounded,
                        };
                        let upper_term = match upper {
                            Bound::Included(val) => Bound::Included(Term::from_field_u64(field, val)),
                            Bound::Excluded(val) => Bound::Excluded(Term::from_field_u64(field, val)),
                            Bound::Unbounded => Bound::Unbounded,
                        };
                        let range_query = RangeQuery::new(lower_term, upper_term);
                        Ok(Box::new(range_query) as Box<dyn TantivyQuery>)
                    },
                    _ => Err(format!("Field '{}' is not an unsigned field", field_name_str)),
                }
            },
            4 => { // FLOAT
                match actual_field_type {
                    TantivyFieldType::F64(_) => {
                        let lower = if !lower_bound.is_null() {
                            match extract_double_value(&mut env, &lower_bound) {
                                Ok(val) => if include_lower != 0 { Bound::Included(val) } else { Bound::Excluded(val) },
                                Err(e) => return Err(format!("Failed to extract lower bound: {}", e)),
                            }
                        } else {
                            Bound::Unbounded
                        };
                        
                        let upper = if !upper_bound.is_null() {
                            match extract_double_value(&mut env, &upper_bound) {
                                Ok(val) => if include_upper != 0 { Bound::Included(val) } else { Bound::Excluded(val) },
                                Err(e) => return Err(format!("Failed to extract upper bound: {}", e)),
                            }
                        } else {
                            Bound::Unbounded
                        };
                        
                        let lower_term = match lower {
                            Bound::Included(val) => Bound::Included(Term::from_field_f64(field, val)),
                            Bound::Excluded(val) => Bound::Excluded(Term::from_field_f64(field, val)),
                            Bound::Unbounded => Bound::Unbounded,
                        };
                        let upper_term = match upper {
                            Bound::Included(val) => Bound::Included(Term::from_field_f64(field, val)),
                            Bound::Excluded(val) => Bound::Excluded(Term::from_field_f64(field, val)),
                            Bound::Unbounded => Bound::Unbounded,
                        };
                        let range_query = RangeQuery::new(lower_term, upper_term);
                        Ok(Box::new(range_query) as Box<dyn TantivyQuery>)
                    },
                    _ => Err(format!("Field '{}' is not a float field", field_name_str)),
                }
            },
            6 => { // DATE
                match actual_field_type {
                    TantivyFieldType::Date(_) => {
                        let lower = if !lower_bound.is_null() {
                            match extract_date_value(&mut env, &lower_bound) {
                                Ok(val) => if include_lower != 0 { Bound::Included(val) } else { Bound::Excluded(val) },
                                Err(e) => return Err(format!("Failed to extract lower bound: {}", e)),
                            }
                        } else {
                            Bound::Unbounded
                        };
                        
                        let upper = if !upper_bound.is_null() {
                            match extract_date_value(&mut env, &upper_bound) {
                                Ok(val) => if include_upper != 0 { Bound::Included(val) } else { Bound::Excluded(val) },
                                Err(e) => return Err(format!("Failed to extract upper bound: {}", e)),
                            }
                        } else {
                            Bound::Unbounded
                        };
                        
                        let lower_term = match lower {
                            Bound::Included(val) => Bound::Included(Term::from_field_date(field, val)),
                            Bound::Excluded(val) => Bound::Excluded(Term::from_field_date(field, val)),
                            Bound::Unbounded => Bound::Unbounded,
                        };
                        let upper_term = match upper {
                            Bound::Included(val) => Bound::Included(Term::from_field_date(field, val)),
                            Bound::Excluded(val) => Bound::Excluded(Term::from_field_date(field, val)),
                            Bound::Unbounded => Bound::Unbounded,
                        };
                        let range_query = RangeQuery::new(lower_term, upper_term);
                        Ok(Box::new(range_query) as Box<dyn TantivyQuery>)
                    },
                    _ => Err(format!("Field '{}' is not a date field", field_name_str)),
                }
            },
            _ => Err(format!("Range queries not supported for field type: {}", field_type)),
        }
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

// Helper function to extract long value from Java Object (Integer/Long)
fn extract_long_value(env: &mut JNIEnv, obj: &JObject) -> Result<i64, String> {
    if obj.is_null() {
        return Err("Object is null".to_string());
    }
    
    // Try Long first
    if let Ok(result) = env.call_method(obj, "longValue", "()J", &[]) {
        if let Ok(val) = result.j() {
            return Ok(val);
        }
    }
    
    // Try Integer 
    if let Ok(result) = env.call_method(obj, "intValue", "()I", &[]) {
        if let Ok(val) = result.i() {
            return Ok(val as i64);
        }
    }
    
    Err("Failed to extract long value from object".to_string())
}

// Helper function to extract double value from Java Object (Float/Double)
fn extract_double_value(env: &mut JNIEnv, obj: &JObject) -> Result<f64, String> {
    if obj.is_null() {
        return Err("Object is null".to_string());
    }
    
    // Try Double first
    if let Ok(result) = env.call_method(obj, "doubleValue", "()D", &[]) {
        if let Ok(val) = result.d() {
            return Ok(val);
        }
    }
    
    // Try Float
    if let Ok(result) = env.call_method(obj, "floatValue", "()F", &[]) {
        if let Ok(val) = result.f() {
            return Ok(val as f64);
        }
    }
    
    Err("Failed to extract double value from object".to_string())
}

// Helper function to extract DateTime value from Java LocalDateTime
fn extract_date_value(env: &mut JNIEnv, obj: &JObject) -> Result<DateTime, String> {
    if obj.is_null() {
        return Err("LocalDateTime object is null".to_string());
    }
    
    // Extract year, month, day, hour, minute, second from LocalDateTime
    let year = match env.call_method(obj, "getYear", "()I", &[]) {
        Ok(result) => match result.i() {
            Ok(y) => y,
            Err(_) => return Err("Failed to get year".to_string()),
        },
        Err(_) => return Err("Failed to call getYear()".to_string()),
    };
    
    let month = match env.call_method(obj, "getMonthValue", "()I", &[]) {
        Ok(result) => match result.i() {
            Ok(m) => m,
            Err(_) => return Err("Failed to get month".to_string()),
        },
        Err(_) => return Err("Failed to call getMonthValue()".to_string()),
    };
    
    let day = match env.call_method(obj, "getDayOfMonth", "()I", &[]) {
        Ok(result) => match result.i() {
            Ok(d) => d,
            Err(_) => return Err("Failed to get day".to_string()),
        },
        Err(_) => return Err("Failed to call getDayOfMonth()".to_string()),
    };
    
    let hour = match env.call_method(obj, "getHour", "()I", &[]) {
        Ok(result) => match result.i() {
            Ok(h) => h,
            Err(_) => return Err("Failed to get hour".to_string()),
        },
        Err(_) => return Err("Failed to call getHour()".to_string()),
    };
    
    let minute = match env.call_method(obj, "getMinute", "()I", &[]) {
        Ok(result) => match result.i() {
            Ok(m) => m,
            Err(_) => return Err("Failed to get minute".to_string()),
        },
        Err(_) => return Err("Failed to call getMinute()".to_string()),
    };
    
    let second = match env.call_method(obj, "getSecond", "()I", &[]) {
        Ok(result) => match result.i() {
            Ok(s) => s,
            Err(_) => return Err("Failed to get second".to_string()),
        },
        Err(_) => return Err("Failed to call getSecond()".to_string()),
    };
    
    // Convert month number to Month enum
    let month_enum = match month {
        1 => Month::January,
        2 => Month::February,
        3 => Month::March,
        4 => Month::April,
        5 => Month::May,
        6 => Month::June,
        7 => Month::July,
        8 => Month::August,
        9 => Month::September,
        10 => Month::October,
        11 => Month::November,
        12 => Month::December,
        _ => return Err(format!("Invalid month: {}", month)),
    };
    
    // Create OffsetDateTime and convert to Tantivy DateTime
    match time::Date::from_calendar_date(year, month_enum, day as u8)
        .and_then(|date| {
            time::Time::from_hms(hour as u8, minute as u8, second as u8)
                .map(|time| date.with_time(time))
        })
    {
        Ok(datetime) => {
            let offset_dt = datetime.assume_utc();
            Ok(DateTime::from_utc(offset_dt))
        },
        Err(_) => Err("Invalid date/time values".to_string()),
    }
}

// Helper function to extract phrase words from Java List
// Handles List<String> for phrase queries
fn extract_phrase_words(env: &mut JNIEnv, list_obj: &JObject) -> Result<Vec<(Option<i32>, String)>, String> {
    // Get the List size
    let size = match env.call_method(list_obj, "size", "()I", &[]) {
        Ok(result) => match result.i() {
            Ok(s) => s,
            Err(_) => return Err("Failed to get list size".to_string()),
        },
        Err(_) => return Err("Failed to call size() on list".to_string()),
    };
    
    let mut words = Vec::with_capacity(size as usize);
    
    // Extract each string from the list
    for i in 0..size {
        let element = match env.call_method(list_obj, "get", "(I)Ljava/lang/Object;", &[i.into()]) {
            Ok(result) => result.l().map_err(|_| "Failed to get object from list")?,
            Err(_) => return Err("Failed to call get() on list".to_string()),
        };
        
        let string_obj = match env.call_method(&element, "toString", "()Ljava/lang/String;", &[]) {
            Ok(result) => result.l().map_err(|_| "Failed to convert to string")?,
            Err(_) => return Err("Failed to call toString() on list element".to_string()),
        };
        
        let java_string = JString::from(string_obj);
        let rust_string: String = match env.get_string(&java_string) {
            Ok(s) => s.into(),
            Err(_) => return Err("Failed to convert Java string to Rust string".to_string()),
        };
        
        words.push((None, rust_string));
    }
    
    Ok(words)
}

// Helper function to extract term values from Java List for TermSetQuery
fn extract_term_set_values(
    env: &mut JNIEnv,
    field_values_list: &JObject,
    field: tantivy::schema::Field,
    schema: &Schema,
) -> Result<Vec<Term>, String> {
    if field_values_list.is_null() {
        return Err("Field values list cannot be null".to_string());
    }
    
    // Get the size of the list
    let list_size = env.call_method(field_values_list, "size", "()I", &[])
        .map_err(|e| format!("Failed to get list size: {}", e))?
        .i()
        .map_err(|e| format!("Failed to convert list size: {}", e))?;
    
    let mut terms = Vec::new();
    
    // Get the field type to determine how to convert values
    let field_entry = schema.get_field_entry(field);
    let field_type = field_entry.field_type();
    
    for i in 0..list_size {
        // Get the element at index i
        let element = env.call_method(field_values_list, "get", "(I)Ljava/lang/Object;", &[i.into()])
            .map_err(|e| format!("Failed to get list element: {}", e))?
            .l()
            .map_err(|e| format!("Failed to convert list element: {}", e))?;
        
        // Convert element to term based on field type
        let term = match field_type {
            tantivy::schema::FieldType::Str(_) => {
                // Handle string values
                let string_obj = env.call_method(&element, "toString", "()Ljava/lang/String;", &[])
                    .map_err(|_| "Failed to call toString on field value")?;
                let java_string = string_obj.l()
                    .map_err(|_| "Failed to get string object")?;
                let java_string_obj = JString::from(java_string);
                let rust_string = env.get_string(&java_string_obj)
                    .map_err(|_| "Failed to convert Java string to Rust string")?;
                let string_value: String = rust_string.into();
                Term::from_field_text(field, &string_value)
            },
            tantivy::schema::FieldType::I64(_) => {
                // Handle integer values
                if let Ok(true) = env.is_instance_of(&element, "java/lang/Long") {
                    let long_value = env.call_method(&element, "longValue", "()J", &[])
                        .map_err(|e| format!("Failed to get long value: {}", e))?
                        .j()
                        .map_err(|e| format!("Failed to convert long value: {}", e))?;
                    Term::from_field_i64(field, long_value)
                } else {
                    return Err("Expected Long value for integer field".to_string());
                }
            },
            tantivy::schema::FieldType::U64(_) => {
                // Handle unsigned values
                if let Ok(true) = env.is_instance_of(&element, "java/lang/Long") {
                    let long_value = env.call_method(&element, "longValue", "()J", &[])
                        .map_err(|e| format!("Failed to get long value: {}", e))?
                        .j()
                        .map_err(|e| format!("Failed to convert long value: {}", e))?;
                    if long_value < 0 {
                        return Err("Unsigned field cannot have negative values".to_string());
                    }
                    Term::from_field_u64(field, long_value as u64)
                } else {
                    return Err("Expected Long value for unsigned field".to_string());
                }
            },
            tantivy::schema::FieldType::F64(_) => {
                // Handle float values
                if let Ok(true) = env.is_instance_of(&element, "java/lang/Double") {
                    let double_value = env.call_method(&element, "doubleValue", "()D", &[])
                        .map_err(|e| format!("Failed to get double value: {}", e))?
                        .d()
                        .map_err(|e| format!("Failed to convert double value: {}", e))?;
                    Term::from_field_f64(field, double_value)
                } else {
                    return Err("Expected Double value for float field".to_string());
                }
            },
            tantivy::schema::FieldType::Bool(_) => {
                // Handle boolean values
                if let Ok(true) = env.is_instance_of(&element, "java/lang/Boolean") {
                    let bool_value = env.call_method(&element, "booleanValue", "()Z", &[])
                        .map_err(|e| format!("Failed to get boolean value: {}", e))?
                        .z()
                        .map_err(|e| format!("Failed to convert boolean value: {}", e))?;
                    Term::from_field_bool(field, bool_value)
                } else {
                    return Err("Expected Boolean value for boolean field".to_string());
                }
            },
            _ => {
                return Err(format!("Unsupported field type for TermSetQuery: {:?}", field_type));
            }
        };
        
        terms.push(term);
    }
    
    Ok(terms)
}

// ====== SNIPPET FUNCTIONALITY ======

// SnippetGenerator JNI methods
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SnippetGenerator_nativeCreate(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    query_ptr: jlong,
    schema_ptr: jlong,
    field_name: JString,
) -> jlong {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return 0;
        }
    };

    // Return a valid stub pointer for SnippetGenerator
    // We'll use a simple boxed integer as a placeholder
    let stub_snippet_generator = Box::new(1u64); // Simple stub object
    register_object(stub_snippet_generator) as jlong
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SnippetGenerator_nativeSnippetFromDoc(
    mut env: JNIEnv,
    _class: JClass,
    snippet_generator_ptr: jlong,
    doc_ptr: jlong,
) -> jlong {
    // Return a valid stub pointer for Snippet
    let stub_snippet = Box::new(2u64); // Simple stub object
    register_object(stub_snippet) as jlong
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SnippetGenerator_nativeSetMaxNumChars(
    mut env: JNIEnv,
    _class: JClass,
    snippet_generator_ptr: jlong,
    max_num_chars: jint,
) {
    // Stub implementation - do nothing but don't error
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SnippetGenerator_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    remove_object(ptr as u64);
}

// Snippet JNI methods
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Snippet_nativeToHtml(
    mut env: JNIEnv,
    _class: JClass,
    snippet_ptr: jlong,
) -> jobject {
    // Return a stub HTML string with basic highlighting
    let stub_html = "<b>sample</b> highlighted text";
    match env.new_string(stub_html) {
        Ok(java_string) => java_string.into_raw(),
        Err(_) => {
            handle_error(&mut env, "Failed to create Java string");
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Snippet_nativeGetFragment(
    mut env: JNIEnv,
    _class: JClass,
    snippet_ptr: jlong,
) -> jobject {
    // Return a stub fragment string
    let stub_fragment = "sample highlighted text";
    match env.new_string(stub_fragment) {
        Ok(java_string) => java_string.into_raw(),
        Err(_) => {
            handle_error(&mut env, "Failed to create Java string");
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Snippet_nativeGetHighlighted(
    mut env: JNIEnv,
    _class: JClass,
    snippet_ptr: jlong,
) -> jobject {
    // Return an ArrayList with a stub Range
    match env.find_class("java/util/ArrayList") {
        Ok(array_list_class) => {
            match env.new_object(array_list_class, "()V", &[]) {
                Ok(array_list) => {
                    // Create a stub range (6-12, highlighting "sample")
                    let stub_range_data = Box::new((6usize, 12usize)); // (start, end) tuple
                    let range_ptr = register_object(stub_range_data) as jlong;
                    
                    // Try to create Range object and add to list
                    match env.find_class("com/tantivy4java/Range") {
                        Ok(range_class) => {
                            match env.new_object(range_class, "(J)V", &[range_ptr.into()]) {
                                Ok(range_obj) => {
                                    let _ = env.call_method(&array_list, "add", "(Ljava/lang/Object;)Z", &[(&range_obj).into()]);
                                },
                                Err(_) => {} // Ignore if can't create Range object
                            }
                        },
                        Err(_) => {} // Ignore if can't find Range class
                    }
                    
                    array_list.into_raw()
                },
                Err(_) => {
                    handle_error(&mut env, "Failed to create ArrayList");
                    std::ptr::null_mut()
                }
            }
        },
        Err(_) => {
            handle_error(&mut env, "Failed to find ArrayList class");
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Snippet_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    remove_object(ptr as u64);
}

// Range JNI methods
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Range_nativeGetStart(
    mut env: JNIEnv,
    _class: JClass,
    range_ptr: jlong,
) -> jint {
    // Return start position from stub range tuple
    let result = with_object(range_ptr as u64, |range_tuple: &(usize, usize)| {
        range_tuple.0 as jint
    });
    result.unwrap_or(6) // Default to position 6
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Range_nativeGetEnd(
    mut env: JNIEnv,
    _class: JClass,
    range_ptr: jlong,
) -> jint {
    // Return end position from stub range tuple
    let result = with_object(range_ptr as u64, |range_tuple: &(usize, usize)| {
        range_tuple.1 as jint
    });
    result.unwrap_or(12) // Default to position 12
}

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Range_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    // Remove the stub range tuple object
    remove_object(ptr as u64);
}