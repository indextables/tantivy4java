// jni_advanced.rs - Advanced Query JNI methods
// Extracted from mod.rs during refactoring
// Contains: nativeRegexQuery, nativeWildcardQuery, nativeRangeQuery, nativeExplain,
//           nativeToString, nativeClose, Range methods, nativeParseQuerySimple

use jni::objects::{JClass, JString, JObject};
use jni::sys::{jlong, jboolean, jint, jdouble, jobject};
use jni::JNIEnv;
use tantivy::query::{Query as TantivyQuery, BooleanQuery, RangeQuery, RegexQuery, ConstScoreQuery, QueryParser};
use tantivy::schema::{Schema, Term, FieldType as TantivyFieldType};
use std::ops::Bound;
use std::sync::Arc;
use crate::utils::{handle_error, with_arc_safe, arc_to_jlong, release_arc};
use crate::extract_helpers::{extract_long_value, extract_double_value};
use super::extraction::extract_date_value;
use super::wildcard::{
    create_single_wildcard_query_with_tokenizer, is_multi_token_pattern,
    contains_multi_wildcards, create_multi_wildcard_regex_query, create_fixed_tokenized_wildcard_query
};

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_query_Query_nativeRegexQuery(
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
pub extern "system" fn Java_io_indextables_tantivy4java_query_Query_nativeWildcardQuery(
    mut env: JNIEnv,
    _class: JClass,
    schema_ptr: jlong,
    field_name: JString,
    pattern: JString,
) -> jlong {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return 0;
        }
    };
    
    let pattern_str: String = match env.get_string(&pattern) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid wildcard pattern");
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
        
        // Verify the field is a text field and check tokenizer
        let field_entry = schema.get_field_entry(field);
        match field_entry.field_type() {
            TantivyFieldType::Str(text_options) => {
                // Check if field uses "default" tokenizer (which lowercases)
                let uses_default_tokenizer = text_options.get_indexing_options()
                    .map(|opts| opts.tokenizer().to_string() == "default")
                    .unwrap_or(true); // Default to true if no indexing options
                
                // Lowercase pattern if using default tokenizer
                let processed_pattern = if uses_default_tokenizer {
                    pattern_str.to_lowercase()
                } else {
                    pattern_str.clone()
                };
                
                // Check if pattern results in multiple tokens when tokenized
                if is_multi_token_pattern(schema, field, &processed_pattern) {
                    // Multi-token pattern - create boolean query with individual wildcard terms
                    // This creates a query like: (*ell* AND *orld*) which searches for documents 
                    // that contain terms matching *ell* AND terms matching *orld*
                    create_fixed_tokenized_wildcard_query(schema, field, &processed_pattern, uses_default_tokenizer)
                } else if contains_multi_wildcards(&processed_pattern) {
                    // Single token but complex multi-wildcard pattern - use regex-based approach
                    create_multi_wildcard_regex_query(field, &processed_pattern)
                } else {
                    // Single token, simple pattern - use simple query
                    create_single_wildcard_query_with_tokenizer(field, &processed_pattern, uses_default_tokenizer)
                }
            },
            _ => Err(format!("Field '{}' is not a text field - wildcard queries require text fields", field_name_str)),
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
pub extern "system" fn Java_io_indextables_tantivy4java_query_Query_nativeWildcardQueryLenient(
    mut env: JNIEnv,
    _class: JClass,
    schema_ptr: jlong,
    field_name: JString,
    pattern: JString,
    lenient: jboolean,
) -> jlong {
    let field_name_str: String = match env.get_string(&field_name) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid field name");
            return 0;
        }
    };
    
    let pattern_str: String = match env.get_string(&pattern) {
        Ok(s) => s.into(),
        Err(_) => {
            handle_error(&mut env, "Invalid wildcard pattern");
            return 0;
        }
    };
    
    
    let is_lenient = lenient != 0;
    
    let result = with_arc_safe::<Schema, Result<Box<dyn TantivyQuery>, String>>(schema_ptr, |schema_arc| {
        let schema = schema_arc.as_ref();
        // Get field by name
        let field = match schema.get_field(&field_name_str) {
            Ok(f) => f,
            Err(_) => {
                if is_lenient {
                    // Return a query that matches no documents
                    let bool_query = BooleanQuery::new(vec![]);
                    return Ok(Box::new(bool_query) as Box<dyn TantivyQuery>);
                } else {
                    return Err(format!("Field '{}' not found in schema", field_name_str));
                }
            }
        };
        
        // Verify the field is a text field and check tokenizer
        let field_entry = schema.get_field_entry(field);
        match field_entry.field_type() {
            TantivyFieldType::Str(text_options) => {
                // Check if field uses "default" tokenizer (which lowercases)
                let uses_default_tokenizer = text_options.get_indexing_options()
                    .map(|opts| opts.tokenizer().to_string() == "default")
                    .unwrap_or(true); // Default to true if no indexing options
                
                // Lowercase pattern if using default tokenizer
                let processed_pattern = if uses_default_tokenizer {
                    pattern_str.to_lowercase()
                } else {
                    pattern_str.clone()
                };
                
                // Check if pattern results in multiple tokens when tokenized
                if is_multi_token_pattern(schema, field, &processed_pattern) {
                    // Multi-token pattern - create boolean query with individual wildcard terms
                    // This creates a query like: (*ell* AND *orld*) which searches for documents 
                    // that contain terms matching *ell* AND terms matching *orld*
                    create_fixed_tokenized_wildcard_query(schema, field, &processed_pattern, uses_default_tokenizer)
                } else if contains_multi_wildcards(&processed_pattern) {
                    // Single token but complex multi-wildcard pattern - use regex-based approach
                    create_multi_wildcard_regex_query(field, &processed_pattern)
                } else {
                    // Single token, simple pattern - use simple query
                    create_single_wildcard_query_with_tokenizer(field, &processed_pattern, uses_default_tokenizer)
                }
            },
            _ => {
                if is_lenient {
                    // Return a query that matches no documents
                    let bool_query = BooleanQuery::new(vec![]);
                    Ok(Box::new(bool_query) as Box<dyn TantivyQuery>)
                } else {
                    Err(format!("Field '{}' is not a text field - wildcard queries require text fields", field_name_str))
                }
            }
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
pub extern "system" fn Java_io_indextables_tantivy4java_query_Query_nativeMoreLikeThisQuery(
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
pub extern "system" fn Java_io_indextables_tantivy4java_query_Query_nativeConstScoreQuery(
    mut env: JNIEnv,
    _class: JClass,
    query_ptr: jlong,
    score: jdouble,
) -> jlong {
    if score < 0.0 {
        handle_error(&mut env, "Score value cannot be negative");
        return 0;
    }
    
    let result = with_arc_safe::<Box<dyn TantivyQuery>, Result<Box<dyn TantivyQuery>, String>>(query_ptr, |query_arc| {
        let query = query_arc.as_ref();
        // Clone the query to avoid ownership issues
        let cloned_query = query.box_clone();
        
        // Create constant score query
        let const_score_query = ConstScoreQuery::new(cloned_query, score as f32);
        Ok(Box::new(const_score_query) as Box<dyn TantivyQuery>)
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

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_query_Query_nativeRangeQuery(
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
    
    let result = with_arc_safe::<Schema, Result<Box<dyn TantivyQuery>, String>>(schema_ptr, |schema_arc| {
        let schema = schema_arc.as_ref();
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
pub extern "system" fn Java_io_indextables_tantivy4java_query_Query_nativeExplain(
    mut env: JNIEnv,
    _class: JClass,
    _query_ptr: jlong,
    _searcher_ptr: jlong,
    _doc_address_ptr: jlong,
) -> jlong {
    handle_error(&mut env, "Query native methods not fully implemented yet");
    0
}

/// Format a query with concise, readable output avoiding verbose automaton dumps
fn format_query_concise(query: &Box<dyn TantivyQuery>) -> String {
    let debug_string = format!("{:?}", query);
    
    // If the output contains newlines (like regex automaton dumps), just take the first line
    if debug_string.contains('\n') {
        if let Some(first_line) = debug_string.lines().next() {
            return first_line.to_string();
        }
    }
    
    // For normal, single-line queries, return the debug output as-is
    debug_string
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_query_Query_nativeToString(
    mut env: JNIEnv,
    _class: JClass,
    query_ptr: jlong,
) -> jobject {
    let result = with_arc_safe::<Box<dyn TantivyQuery>, Option<String>>(query_ptr, |query_arc| {
        let query = query_arc.as_ref();
        Some(format_query_concise(query))
    });
    
    match result {
        Some(Some(debug_string)) => {
            match env.new_string(&debug_string) {
                Ok(java_string) => java_string.into_raw(),
                Err(_) => {
                    handle_error(&mut env, "Failed to create Java string for query debug");
                    std::ptr::null_mut()
                }
            }
        },
        _ => {
            handle_error(&mut env, "Invalid Query pointer for toString");
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_query_Query_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    release_arc(ptr);
}

// Helper function to extract OccurQuery objects from Java List
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_query_Range_nativeGetStart(
    _env: JNIEnv,
    _class: JClass,
    _range_ptr: jlong,
) -> jint {
    // Stub implementation - ranges not yet implemented
    // Would use with_arc_safe for real implementation
    6 // Default to position 6
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_query_Range_nativeGetEnd(
    _env: JNIEnv,
    _class: JClass,
    _range_ptr: jlong,
) -> jint {
    // Stub implementation - ranges not yet implemented
    // Would use with_arc_safe for real implementation
    12 // Default to position 12
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_query_Range_nativeClose(
    _env: JNIEnv,
    _class: JClass,
    _ptr: jlong,
) {
    // Stub implementation - ranges not yet implemented
    // In future, would use: release_arc(ptr);
}

// QueryParser JNI methods
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_core_Index_nativeParseQuerySimple(
    mut env: JNIEnv,
    _class: JClass,
    index_ptr: jlong,
    query_string: JString,
) -> jlong {
    let query_str: String = match env.get_string(&query_string) {
        Ok(s) => s.into(),
        Err(e) => {
            handle_error(&mut env, &format!("Invalid query string: {}", e));
            return 0;
        }
    };
    
    // Validate query string is not empty
    if query_str.trim().is_empty() {
        handle_error(&mut env, "Query string cannot be empty");
        return 0;
    }
    
    // Parse query using the Arc-based index
    let parsed_query = with_arc_safe::<std::sync::Mutex<tantivy::Index>, Option<Box<dyn TantivyQuery>>>(
        index_ptr,
        |index_mutex| {
            let index = index_mutex.lock().unwrap();
            
            // Get schema and find text fields
            let schema = index.schema();
            let mut default_fields = Vec::new();
            
            for (field, field_entry) in schema.fields() {
                if let tantivy::schema::FieldType::Str(_) = field_entry.field_type() {
                    default_fields.push(field);
                }
            }
            
            if default_fields.is_empty() {
                return None;
            }
            
            // Create query parser and parse query
            let query_parser = QueryParser::for_index(&*index, default_fields);
            match query_parser.parse_query(&query_str) {
                Ok(query) => Some(query),
                Err(_) => None
            }
        }
    );
    
    match parsed_query {
        Some(Some(query)) => {
            // Register the query using Arc registry
            let query_arc = Arc::new(query);
            arc_to_jlong(query_arc)
        },
        Some(None) => {
            handle_error(&mut env, &format!("Failed to parse query '{}'", query_str));
            0
        },
        None => {
            handle_error(&mut env, "Invalid index pointer or no text fields found");
            0
        }
    }
}

