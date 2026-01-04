// jni_core.rs - Core Query JNI methods
// Extracted from mod.rs during refactoring
// Contains: nativeTermQuery, nativeTermSetQuery, nativeAllQuery, nativeFuzzyTermQuery,
//           nativePhraseQuery, nativeBooleanQuery, nativeDisjunctionMaxQuery, nativeBoostQuery

use jni::objects::{JClass, JString, JObject};
use jni::sys::{jlong, jboolean, jint, jdouble, jlongArray, jobject};
use jni::JNIEnv;
use tantivy::query::{Query as TantivyQuery, TermQuery, AllQuery, BooleanQuery, PhraseQuery, FuzzyTermQuery, BoostQuery};
use tantivy::schema::{Schema, Term, IndexRecordOption, FieldType as TantivyFieldType};
use std::sync::Arc;
use crate::utils::{handle_error, with_arc_safe, arc_to_jlong};
use super::extraction::{extract_occur_queries, extract_phrase_words, extract_term_set_values};

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_query_Query_nativeTermQuery(
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
    
    let result = with_arc_safe::<Schema, Result<Box<dyn TantivyQuery>, String>>(schema_ptr, |schema_arc| {
        let schema = schema_arc.as_ref();
        // Get field by name
        let field = match schema.get_field(&field_name_str) {
            Ok(f) => f,
            Err(_) => return Err(format!("Field '{}' not found in schema", field_name_str)),
        };
        
        // Get field type
        let field_type = schema.get_field_entry(field).field_type();
        
        // Safely validate field_value before use
        if field_value.is_null() {
            return Err("Field value cannot be null".to_string());
        }
        
        // Use safe JObject construction from validated jobject
        let field_value_obj = unsafe {
            // SAFETY: We've validated field_value is not null above
            JObject::from_raw(field_value)
        };
        
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
                
                // Text fields are always indexed in tantivy (matching tantivy-py behavior)
                // Check which tokenizer is being used
                let uses_default_tokenizer = text_options.get_indexing_options()
                    .map(|opts| opts.tokenizer().to_string() == "default")
                    .unwrap_or(true); // Default to true if no explicit indexing options
                
                if uses_default_tokenizer {
                    // The default tokenizer lowercases text, so we need to lowercase the search term
                    let tokenized_term = field_value_str.to_lowercase();
                    Term::from_field_text(field, &tokenized_term)
                } else {
                    // For other tokenizers, use the term as-is
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
            tantivy::schema::FieldType::I64(_) => {
                // Handle signed integer fields (I64)
                if field_value_obj.is_null() {
                    return Err("Field value cannot be null for integer field".to_string());
                }

                // Check if it's a Long/Integer
                if let Ok(true) = env.is_instance_of(&field_value_obj, "java/lang/Long") {
                    let long_value = env.call_method(&field_value_obj, "longValue", "()J", &[])
                        .map_err(|e| format!("Failed to get long value: {}", e))?
                        .j()
                        .map_err(|e| format!("Failed to convert long value: {}", e))?;
                    Term::from_field_i64(field, long_value)
                } else if let Ok(true) = env.is_instance_of(&field_value_obj, "java/lang/Integer") {
                    let int_value = env.call_method(&field_value_obj, "intValue", "()I", &[])
                        .map_err(|e| format!("Failed to get int value: {}", e))?
                        .i()
                        .map_err(|e| format!("Failed to convert int value: {}", e))?;
                    Term::from_field_i64(field, int_value as i64)
                } else {
                    return Err("Expected Long or Integer value for signed integer field".to_string());
                }
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
            let query_arc = Arc::new(query);
            arc_to_jlong(query_arc)
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
pub extern "system" fn Java_io_indextables_tantivy4java_query_Query_nativeTermSetQuery(
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
    
    let result = with_arc_safe::<Schema, Result<Box<dyn TantivyQuery>, String>>(schema_ptr, |schema_arc| {
        let schema = schema_arc.as_ref();
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
        Some(Ok(query)) => {
            let query_arc = Arc::new(query);
            arc_to_jlong(query_arc)
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
pub extern "system" fn Java_io_indextables_tantivy4java_query_Query_nativeAllQuery(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    let query = AllQuery;
    let query_arc = Arc::new(Box::new(query) as Box<dyn TantivyQuery>);
    arc_to_jlong(query_arc)
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_query_Query_nativeFuzzyTermQuery(
    mut env: JNIEnv,
    _class: JClass,
    schema_ptr: jlong,
    field_name: JString,
    text: JString,
    distance: jint,
    transposition_cost_one: jboolean,
    _prefix: jboolean,
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
    
    let result = with_arc_safe::<Schema, Result<Box<dyn TantivyQuery>, String>>(schema_ptr, |schema_arc| {
        let schema = schema_arc.as_ref();
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
            let query_arc = Arc::new(query);
            arc_to_jlong(query_arc)
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
pub extern "system" fn Java_io_indextables_tantivy4java_query_Query_nativePhraseQuery(
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
    
    let result = with_arc_safe::<Schema, Result<Box<dyn TantivyQuery>, String>>(schema_ptr, |schema_arc| {
        let schema = schema_arc.as_ref();
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
                
                // Fix: Tantivy requires phrase queries to have more than one term
                // If there's only one term, create a TermQuery instead
                if phrase_terms.len() == 1 {
                    let (_, term) = phrase_terms.into_iter().next().unwrap();
                    let term_query = TermQuery::new(term, tantivy::schema::IndexRecordOption::Basic);
                    return Ok(Box::new(term_query) as Box<dyn TantivyQuery>);
                }
                
                // Create the PhraseQuery with slop (only for multi-term phrases)
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
            let query_arc = Arc::new(query);
            arc_to_jlong(query_arc)
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
pub extern "system" fn Java_io_indextables_tantivy4java_query_Query_nativeBooleanQuery(
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
        let query_box = match with_arc_safe::<Box<dyn TantivyQuery>, Option<Box<dyn TantivyQuery>>>(query_ptr, |query_arc| {
            let query = query_arc.as_ref();
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
    
    let query_arc = Arc::new(boxed_query);
    arc_to_jlong(query_arc)
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_query_Query_nativeDisjunctionMaxQuery(
    mut env: JNIEnv,
    _class: JClass,
    _query_ptrs: jlongArray,
    _tie_breaker: JObject,
) -> jlong {
    handle_error(&mut env, "Query native methods not fully implemented yet");
    0
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_query_Query_nativeBoostQuery(
    mut env: JNIEnv,
    _class: JClass,
    query_ptr: jlong,
    boost: jdouble,
) -> jlong {
    if boost <= 0.0 {
        handle_error(&mut env, "Boost value must be positive");
        return 0;
    }
    
    let result = with_arc_safe::<Box<dyn TantivyQuery>, Result<Box<dyn TantivyQuery>, String>>(query_ptr, |query_arc| {
        let query = query_arc.as_ref();
        // Clone the query to avoid ownership issues
        let cloned_query = query.box_clone();
        
        // Create boost query
        let boost_query = BoostQuery::new(cloned_query, boost as f32);
        Ok(Box::new(boost_query) as Box<dyn TantivyQuery>)
    });
    
    match result {
        Some(Ok(query)) => {
            let query_arc = Arc::new(query);
            arc_to_jlong(query_arc)
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

