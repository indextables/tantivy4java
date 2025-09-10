// split_query.rs - Native implementation for SplitQuery classes using Quickwit libraries

use jni::objects::{JClass, JObject, JString, JValue, JObjectArray};
use jni::sys::{jlong, jobject, jstring};
use crate::debug_println;
use jni::JNIEnv;
use anyhow::{Result, anyhow};

use std::ops::Bound;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use once_cell::sync::Lazy;
use quickwit_query::query_ast::{QueryAst, query_ast_from_user_text};
use quickwit_query::JsonLiteral;

// Global cache mapping split URI to schema for parseQuery field extraction
static SPLIT_SCHEMA_CACHE: Lazy<Arc<Mutex<HashMap<String, tantivy::schema::Schema>>>> = 
    Lazy::new(|| Arc::new(Mutex::new(HashMap::new())));

/// Store schema clone for a split URI
pub fn store_split_schema(split_uri: &str, schema: tantivy::schema::Schema) {
    debug_println!("RUST DEBUG: *** STORE_SPLIT_SCHEMA CALLED WITH URI: {}", split_uri);
    debug_println!("RUST DEBUG: Storing schema clone in cache for split: {}", split_uri);
    let mut cache = SPLIT_SCHEMA_CACHE.lock().unwrap();
    cache.insert(split_uri.to_string(), schema);
    debug_println!("RUST DEBUG: Schema cache now contains {} entries", cache.len());
    debug_println!("RUST DEBUG: *** STORE_SPLIT_SCHEMA COMPLETED");
}

/// Retrieve schema clone for a split URI
pub fn get_split_schema(split_uri: &str) -> Option<tantivy::schema::Schema> {
    let cache = SPLIT_SCHEMA_CACHE.lock().unwrap();
    if let Some(schema) = cache.get(split_uri) {
        debug_println!("RUST DEBUG: ✅ Retrieved schema from cache for split: {}", split_uri);
        Some(schema.clone())
    } else {
        debug_println!("RUST DEBUG: ❌ Schema not found in cache for split: {}", split_uri);
        debug_println!("RUST DEBUG: Available cache entries: {:?}", cache.keys().collect::<Vec<_>>());
        None
    }
}

/// Convert a SplitTermQuery to QueryAst JSON
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitTermQuery_toQueryAstJson(
    mut env: JNIEnv,
    obj: JObject,
) -> jstring {
    let result = convert_term_query_to_ast(&mut env, &obj);
    match result {
        Ok(json) => {
            let jstring = env.new_string(json).unwrap_or_else(|_| env.new_string("{}").unwrap());
            jstring.into_raw()
        }
        Err(e) => {
            debug_println!("RUST DEBUG: Error converting SplitTermQuery to QueryAst: {}", e);
            let error_json = format!(r#"{{"error": "{}"}}"#, e);
            let jstring = env.new_string(error_json).unwrap_or_else(|_| env.new_string("{}").unwrap());
            jstring.into_raw()
        }
    }
}

/// Convert a SplitBooleanQuery to QueryAst JSON
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitBooleanQuery_toQueryAstJson(
    mut env: JNIEnv,
    obj: JObject,
) -> jstring {
    let result = convert_boolean_query_to_ast(&mut env, &obj);
    match result {
        Ok(json) => {
            let jstring = env.new_string(json).unwrap_or_else(|_| env.new_string("{}").unwrap());
            jstring.into_raw()
        }
        Err(e) => {
            debug_println!("RUST DEBUG: Error converting SplitBooleanQuery to QueryAst: {}", e);
            let error_json = format!(r#"{{"error": "{}"}}"#, e);
            let jstring = env.new_string(error_json).unwrap_or_else(|_| env.new_string("{}").unwrap());
            jstring.into_raw()
        }
    }
}

/// Convert a SplitRangeQuery to QueryAst JSON
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitRangeQuery_toQueryAstJson(
    mut env: JNIEnv,
    obj: JObject,
) -> jstring {
    let result = convert_range_query_to_ast(&mut env, &obj);
    match result {
        Ok(json) => {
            let jstring = env.new_string(json).unwrap_or_else(|_| env.new_string("{}").unwrap());
            jstring.into_raw()
        }
        Err(e) => {
            debug_println!("RUST DEBUG: Error converting SplitRangeQuery to QueryAst: {}", e);
            let error_json = format!(r#"{{"error": "{}"}}"#, e);
            let jstring = env.new_string(error_json).unwrap_or_else(|_| env.new_string("{}").unwrap());
            jstring.into_raw()
        }
    }
}

/// Convert a SplitMatchAllQuery to QueryAst JSON
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitMatchAllQuery_toQueryAstJson(
    env: JNIEnv,
    _obj: JObject,
) -> jstring {
    // MatchAll is simple - just return the MatchAll QueryAst
    let query_ast = QueryAst::MatchAll;
    match serde_json::to_string(&query_ast) {
        Ok(json) => {
            let jstring = env.new_string(json).unwrap_or_else(|_| env.new_string("{}").unwrap());
            jstring.into_raw()
        }
        Err(e) => {
            debug_println!("RUST DEBUG: Error serializing MatchAll QueryAst: {}", e);
            let jstring = env.new_string(r#"{"type": "match_all"}"#).unwrap();
            jstring.into_raw()
        }
    }
}

/// Parse a query string into a SplitQuery using Quickwit's query parser
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_SplitQuery_parseQuery(
    mut env: JNIEnv,
    _class: JClass,
    query_string: JString,
    schema_ptr: jlong,
    default_search_fields: jobject,
) -> jobject {
    let result = parse_query_string(&mut env, query_string, schema_ptr, default_search_fields);
    match result {
        Ok(query_obj) => query_obj,
        Err(e) => {
            debug_println!("RUST DEBUG: Error parsing query string: {}", e);
            // Return null on error
            std::ptr::null_mut()
        }
    }
}

fn convert_term_query_to_ast(env: &mut JNIEnv, obj: &JObject) -> Result<String> {
    // Extract field and value from Java SplitTermQuery object
    let field_obj = env.get_field(obj, "field", "Ljava/lang/String;")
        .map_err(|e| anyhow!("Failed to get field: {}", e))?;
    let value_obj = env.get_field(obj, "value", "Ljava/lang/String;")
        .map_err(|e| anyhow!("Failed to get value: {}", e))?;
    
    let field_jstring: JString = field_obj.l()?.into();
    let value_jstring: JString = value_obj.l()?.into();
    
    let field: String = env.get_string(&field_jstring)?.into();
    let mut value: String = env.get_string(&value_jstring)?.into();
    
    // The default tokenizer in Quickwit/Tantivy lowercases all text during indexing.
    // For text fields, we need to lowercase the search term to match.
    // TODO: This should ideally check the field's tokenizer configuration,
    // but for now we'll lowercase all term queries since most text fields use the default tokenizer.
    value = value.to_lowercase();
    
    debug_println!("RUST DEBUG: SplitTermQuery - field: '{}', original value: '{}', lowercased: '{}'", 
                   field, env.get_string(&value_jstring)?.to_str()?, value);
    
    // Create QueryAst using Quickwit's term query structure
    let term_query = quickwit_query::query_ast::TermQuery {
        field,
        value,
    };
    let query_ast = QueryAst::Term(term_query);
    
    // Serialize to JSON
    serde_json::to_string(&query_ast).map_err(|e| anyhow!("Serialization error: {}", e))
}

fn convert_boolean_query_to_ast(env: &mut JNIEnv, obj: &JObject) -> Result<String> {
    // Extract clauses from Java SplitBooleanQuery object
    let must_list_obj = env.get_field(obj, "mustQueries", "Ljava/util/List;")
        .map_err(|e| anyhow!("Failed to get mustQueries: {}", e))?;
    let should_list_obj = env.get_field(obj, "shouldQueries", "Ljava/util/List;")
        .map_err(|e| anyhow!("Failed to get shouldQueries: {}", e))?;
    let must_not_list_obj = env.get_field(obj, "mustNotQueries", "Ljava/util/List;")
        .map_err(|e| anyhow!("Failed to get mustNotQueries: {}", e))?;
    let min_should_match_obj = env.get_field(obj, "minimumShouldMatch", "Ljava/lang/Integer;")
        .map_err(|e| anyhow!("Failed to get minimumShouldMatch: {}", e))?;
    
    // Convert Java lists to Rust QueryAst clauses
    let must_clauses = convert_query_list(env, must_list_obj.l()?)?;
    let should_clauses = convert_query_list(env, should_list_obj.l()?)?;
    let must_not_clauses = convert_query_list(env, must_not_list_obj.l()?)?;
    
    // Handle minimum should match
    let min_should_match_jobj = min_should_match_obj.l()?;
    let minimum_should_match = if min_should_match_jobj.is_null() {
        None
    } else {
        let int_val = env.call_method(min_should_match_jobj, "intValue", "()I", &[])?;
        Some(int_val.i()? as usize)
    };
    
    // Create QueryAst using Quickwit's bool query structure
    let bool_query = quickwit_query::query_ast::BoolQuery {
        must: must_clauses,
        should: should_clauses,
        must_not: must_not_clauses,
        filter: Vec::new(), // SplitBooleanQuery doesn't have filter clauses yet
        minimum_should_match,
    };
    let query_ast = QueryAst::Bool(bool_query);
    
    // Serialize to JSON
    serde_json::to_string(&query_ast).map_err(|e| anyhow!("Serialization error: {}", e))
}

fn convert_range_query_to_ast(env: &mut JNIEnv, obj: &JObject) -> Result<String> {
    // Extract field, bounds, and field type from Java SplitRangeQuery object
    let field_obj = env.get_field(obj, "field", "Ljava/lang/String;")
        .map_err(|e| anyhow!("Failed to get field: {}", e))?;
    let lower_bound_obj = env.get_field(obj, "lowerBound", "Lcom/tantivy4java/SplitRangeQuery$RangeBound;")
        .map_err(|e| anyhow!("Failed to get lowerBound: {}", e))?;
    let upper_bound_obj = env.get_field(obj, "upperBound", "Lcom/tantivy4java/SplitRangeQuery$RangeBound;")
        .map_err(|e| anyhow!("Failed to get upperBound: {}", e))?;
    let field_type_obj = env.get_field(obj, "fieldType", "Ljava/lang/String;")
        .map_err(|e| anyhow!("Failed to get fieldType: {}", e))?;
    
    let field_jstring: JString = field_obj.l()?.into();
    let field: String = env.get_string(&field_jstring)?.into();
    
    let field_type_jstring: JString = field_type_obj.l()?.into();
    let field_type: String = env.get_string(&field_type_jstring)?.into();
    
    debug_println!("RUST DEBUG: Converting range query for field '{}' with type '{}'", field, field_type);
    
    // Convert bounds with field type information
    let lower_bound_jobject = lower_bound_obj.l()?;
    let upper_bound_jobject = upper_bound_obj.l()?;
    let lower_bound = convert_range_bound(env, &lower_bound_jobject, &field_type)?;
    let upper_bound = convert_range_bound(env, &upper_bound_jobject, &field_type)?;
    
    // Create QueryAst using Quickwit's range query structure
    let range_query = quickwit_query::query_ast::RangeQuery {
        field,
        lower_bound,
        upper_bound,
    };
    let query_ast = QueryAst::Range(range_query);
    
    // Serialize to JSON
    serde_json::to_string(&query_ast).map_err(|e| anyhow!("Serialization error: {}", e))
}

fn convert_range_bound(env: &mut JNIEnv, bound_obj: &JObject, field_type: &str) -> Result<Bound<JsonLiteral>> {
    // Get the bound type
    let type_obj = env.get_field(bound_obj, "type", "Lcom/tantivy4java/SplitRangeQuery$RangeBound$BoundType;")?;
    let type_enum = type_obj.l()?;
    
    // Get enum name
    let name_obj = env.call_method(type_enum, "name", "()Ljava/lang/String;", &[])?;
    let name_jstring: JString = name_obj.l()?.into();
    let bound_type: String = env.get_string(&name_jstring)?.into();
    
    match bound_type.as_str() {
        "UNBOUNDED" => Ok(Bound::Unbounded),
        "INCLUSIVE" | "EXCLUSIVE" => {
            // Get the value
            let value_obj = env.get_field(bound_obj, "value", "Ljava/lang/String;")?;
            let value_jstring: JString = value_obj.l()?.into();
            let value: String = env.get_string(&value_jstring)?.into();
            let value_for_debug = value.clone(); // Clone for debug printing
            
            // Debug: Log field type and value before conversion
            debug_println!("RUST DEBUG: Converting range bound - field_type: '{}', value: '{}'", field_type, value_for_debug);
            
            // Convert string value to JsonLiteral based on field type
            let json_literal = match field_type {
                "i64" | "int" | "integer" => {
                    let parsed: i64 = value.parse()
                        .map_err(|e| anyhow!("Failed to parse '{}' as i64: {}", value, e))?;
                    JsonLiteral::Number(serde_json::Number::from(parsed))
                }
                "u64" | "uint" => {
                    let parsed: u64 = value.parse()
                        .map_err(|e| anyhow!("Failed to parse '{}' as u64: {}", value, e))?;
                    JsonLiteral::Number(serde_json::Number::from(parsed))
                }
                "f64" | "float" | "double" => {
                    let parsed: f64 = value.parse()
                        .map_err(|e| anyhow!("Failed to parse '{}' as f64: {}", value, e))?;
                    let number = serde_json::Number::from_f64(parsed)
                        .ok_or_else(|| anyhow!("Invalid f64 value: {}", parsed))?;
                    JsonLiteral::Number(number)
                }
                "str" | "string" | "text" => {
                    // Special case: if the field type is string but the value looks numeric, try to parse it
                    // This handles cases where numeric fields like "price" are incorrectly typed as "str"
                    if value.parse::<i64>().is_ok() {
                        let parsed: i64 = value.parse().unwrap();
                        debug_println!("RUST DEBUG: Field type '{}' but value '{}' looks like i64, converting to Number", field_type, value);
                        JsonLiteral::Number(serde_json::Number::from(parsed))
                    } else if value.parse::<f64>().is_ok() {
                        let parsed: f64 = value.parse().unwrap();
                        debug_println!("RUST DEBUG: Field type '{}' but value '{}' looks like f64, converting to Number", field_type, value);
                        let number = serde_json::Number::from_f64(parsed).unwrap();
                        JsonLiteral::Number(number)
                    } else {
                        JsonLiteral::String(value)
                    }
                }
                _ => {
                    debug_println!("RUST DEBUG: Unknown field type '{}', defaulting to string", field_type);
                    JsonLiteral::String(value)
                }
            };
            
            // Debug: Log the final JsonLiteral that was created
            debug_println!("RUST DEBUG: Created JsonLiteral: {:?}", json_literal);
            
            debug_println!("RUST DEBUG: Converted bound value '{}' to {:?} for field type '{}'", value_for_debug, json_literal, field_type);
            
            match bound_type.as_str() {
                "INCLUSIVE" => Ok(Bound::Included(json_literal)),
                "EXCLUSIVE" => Ok(Bound::Excluded(json_literal)),
                _ => unreachable!()
            }
        }
        _ => Err(anyhow!("Unknown bound type: {}", bound_type))
    }
}

fn convert_query_list(env: &mut JNIEnv, list_obj: JObject) -> Result<Vec<QueryAst>> {
    if list_obj.is_null() {
        return Ok(Vec::new());
    }
    
    // Get list size
    let size_result = env.call_method(&list_obj, "size", "()I", &[])?;
    let size = size_result.i()? as usize;
    
    let mut queries = Vec::new();
    
    for i in 0..size {
        // Get element at index i
        let get_result = env.call_method(&list_obj, "get", "(I)Ljava/lang/Object;", &[JValue::Int(i as i32)])?;
        let query_obj = get_result.l()?;
        
        // Convert based on the actual type
        let query_ast = convert_split_query_to_ast(env, &query_obj)?;
        queries.push(query_ast);
    }
    
    Ok(queries)
}

fn convert_split_query_to_ast(env: &mut JNIEnv, query_obj: &JObject) -> Result<QueryAst> {
    // Determine the actual type of the SplitQuery object
    let class = env.get_object_class(query_obj)?;
    let class_name_obj = env.call_method(class, "getName", "()Ljava/lang/String;", &[])?;
    let class_name_jstring: JString = class_name_obj.l()?.into();
    let class_name: String = env.get_string(&class_name_jstring)?.into();
    
    match class_name.as_str() {
        "com.tantivy4java.SplitTermQuery" => {
            // Parse as term query
            let json = convert_term_query_to_ast(env, query_obj)?;
            serde_json::from_str(&json).map_err(|e| anyhow!("Failed to parse term query JSON: {}", e))
        }
        "com.tantivy4java.SplitBooleanQuery" => {
            // Parse as boolean query  
            let json = convert_boolean_query_to_ast(env, query_obj)?;
            serde_json::from_str(&json).map_err(|e| anyhow!("Failed to parse boolean query JSON: {}", e))
        }
        "com.tantivy4java.SplitRangeQuery" => {
            // Parse as range query
            let json = convert_range_query_to_ast(env, query_obj)?;
            serde_json::from_str(&json).map_err(|e| anyhow!("Failed to parse range query JSON: {}", e))
        }
        "com.tantivy4java.SplitMatchAllQuery" => {
            Ok(QueryAst::MatchAll)
        }
        _ => {
            Err(anyhow!("Unsupported SplitQuery type: {}", class_name))
        }
    }
}

fn parse_query_string(env: &mut JNIEnv, query_string: JString, schema_ptr: jlong, default_search_fields: jobject) -> Result<jobject> {
    debug_println!("RUST DEBUG: *** parse_query_string CALLED ***");
    
    // Get the query string
    let query_str: String = env.get_string(&query_string)?.into();
    
    debug_println!("RUST DEBUG: 🚀 Parsing query string: '{}' with schema_ptr: {}", query_str, schema_ptr);
    
    // ✅ PROPER DEFAULT FIELD EXTRACTION: Extract default search fields from JNI
    let mut default_fields_vec = extract_default_search_fields(env, default_search_fields)?;
    
    // 🎯 KEY LOGIC: If default fields is empty, extract ALL indexed text fields from schema
    if default_fields_vec.is_empty() {
        debug_println!("RUST DEBUG: Default fields empty, extracting all indexed text fields from schema");
        
        // Extract all indexed text field names from the schema
        default_fields_vec = extract_text_fields_from_schema(env, schema_ptr)?;
        
        debug_println!("RUST DEBUG: Auto-detected {} text fields: {:?}", default_fields_vec.len(), default_fields_vec);
        
        if default_fields_vec.is_empty() {
            debug_println!("RUST DEBUG: Warning: No indexed text fields found in schema");
        }
    }
    
    debug_println!("RUST DEBUG: Final default search fields: {:?}", default_fields_vec);
    
    // 🚀 PROPER QUICKWIT PARSING: Use Quickwit's proven two-step process
    // Step 1: Create UserInputQuery AST with proper default fields
    // Use None if default fields is empty to let Quickwit handle field-less queries properly
    let default_fields_option = if default_fields_vec.is_empty() {
        debug_println!("RUST DEBUG: Using None for default fields (empty vector)");
        None
    } else {
        debug_println!("RUST DEBUG: Using Some({:?}) for default fields", default_fields_vec);
        Some(default_fields_vec.clone())
    };
    
    let query_ast = query_ast_from_user_text(&query_str, default_fields_option.clone());
    debug_println!("RUST DEBUG: Created UserInputQuery AST: {:?}", query_ast);
    
    // Step 2: Parse the user query using Quickwit's parser with default search fields
    // Pass the right default fields based on what we have
    let parse_fields = match &default_fields_option {
        Some(fields) => fields,
        None => &Vec::new() // Use empty vector when None
    };
    
    let parsed_ast = match query_ast.parse_user_query(parse_fields) {
        Ok(ast) => {
            debug_println!("RUST DEBUG: ✅ Successfully parsed with Quickwit: {:?}", ast);
            ast
        },
        Err(e) => {
            debug_println!("RUST DEBUG: ❌ Parsing failed: {}", e);
            
            // 🚨 THROW ERROR instead of falling back to match_all
            return Err(anyhow!("Failed to parse query '{}': {}. This query requires explicit field names (e.g., 'field:term') or valid default search fields in the schema.", query_str, e));
        }
    };
    
    debug_println!("RUST DEBUG: 🎯 Final parsed QueryAst: {:?}", parsed_ast);
    
    // ✅ QUICKWIT BEST PRACTICE: Serialize QueryAst directly to JSON
    // instead of converting to individual SplitQuery objects
    let query_ast_json = serde_json::to_string(&parsed_ast)
        .map_err(|e| anyhow!("Failed to serialize QueryAst to JSON: {}", e))?;
    
    debug_println!("RUST DEBUG: 📄 QueryAst JSON: {}", query_ast_json);
    
    // Create a SplitParsedQuery that holds the QueryAst JSON directly
    let split_query_obj = create_split_parsed_query(env, &query_ast_json)?;
    
    Ok(split_query_obj)
}

fn create_split_query_from_ast(env: &mut JNIEnv, query_ast: &QueryAst) -> Result<jobject> {
    match query_ast {
        QueryAst::Term(term_query) => {
            // Create SplitTermQuery Java object
            let class = env.find_class("com/tantivy4java/SplitTermQuery")?;
            let field_jstring = env.new_string(&term_query.field)?;
            let value_jstring = env.new_string(&term_query.value)?;
            
            let obj = env.new_object(class, "(Ljava/lang/String;Ljava/lang/String;)V", 
                &[JValue::Object(&field_jstring.into()), JValue::Object(&value_jstring.into())])?;
            
            debug_println!("RUST DEBUG: ✅ Created SplitTermQuery for Term: field='{}', value='{}'", term_query.field, term_query.value);
            Ok(obj.into_raw())
        }
        QueryAst::FullText(fulltext_query) => {
            // Convert FullTextQuery to SplitTermQuery 
            // FullTextQuery and TermQuery are conceptually the same for our purposes
            let class = env.find_class("com/tantivy4java/SplitTermQuery")?;
            let field_jstring = env.new_string(&fulltext_query.field)?;
            let value_jstring = env.new_string(&fulltext_query.text)?;
            
            let obj = env.new_object(class, "(Ljava/lang/String;Ljava/lang/String;)V", 
                &[JValue::Object(&field_jstring.into()), JValue::Object(&value_jstring.into())])?;
            
            debug_println!("RUST DEBUG: ✅ Created SplitTermQuery for FullText: field='{}', text='{}'", fulltext_query.field, fulltext_query.text);
            Ok(obj.into_raw())
        }
        QueryAst::MatchAll => {
            // Create SplitMatchAllQuery Java object
            let class = env.find_class("com/tantivy4java/SplitMatchAllQuery")?;
            let obj = env.new_object(class, "()V", &[])?;
            debug_println!("RUST DEBUG: ✅ Created SplitMatchAllQuery for MatchAll");
            Ok(obj.into_raw())
        }
        QueryAst::Bool(_bool_query) => {
            // TODO: Implement SplitBooleanQuery creation from QueryAst
            // This is more complex as we need to recursively convert subqueries
            debug_println!("RUST DEBUG: Boolean query creation from QueryAst not yet implemented");
            
            // Fallback to MatchAll for now
            let class = env.find_class("com/tantivy4java/SplitMatchAllQuery")?;
            let obj = env.new_object(class, "()V", &[])?;
            Ok(obj.into_raw())
        }
        _ => {
            debug_println!("RUST DEBUG: Unsupported QueryAst type for SplitQuery conversion: {:?}", query_ast);
            
            // Fallback to MatchAll
            let class = env.find_class("com/tantivy4java/SplitMatchAllQuery")?;
            let obj = env.new_object(class, "()V", &[])?;
            Ok(obj.into_raw())
        }
    }
}

fn create_split_parsed_query(env: &mut JNIEnv, query_ast_json: &str) -> Result<jobject> {
    // Create SplitParsedQuery Java object with the QueryAst JSON
    let class = env.find_class("com/tantivy4java/SplitParsedQuery")?;
    let json_jstring = env.new_string(query_ast_json)?;
    
    let obj = env.new_object(class, "(Ljava/lang/String;)V", 
        &[JValue::Object(&json_jstring.into())])?;
    
    debug_println!("RUST DEBUG: ✅ Created SplitParsedQuery with JSON: {}", query_ast_json);
    Ok(obj.into_raw())
}

fn extract_default_search_fields(env: &mut JNIEnv, default_search_fields: jobject) -> Result<Vec<String>> {
    // Handle null case - return empty vec
    if default_search_fields.is_null() {
        debug_println!("RUST DEBUG: Default search fields is null, using empty list");
        return Ok(Vec::new());
    }
    
    // Convert to JObjectArray (requires unsafe block)
    let fields_array = JObjectArray::from(unsafe { JObject::from_raw(default_search_fields) });
    
    // Get array length
    let array_len = env.get_array_length(&fields_array)?;
    debug_println!("RUST DEBUG: Default search fields array length: {}", array_len);
    
    let mut fields = Vec::new();
    
    // Extract each string from the array
    for i in 0..array_len {
        let element = env.get_object_array_element(&fields_array, i)?;
        if !element.is_null() {
            let field_str: String = env.get_string(&JString::from(element))?.into();
            debug_println!("RUST DEBUG: Extracted default search field[{}]: '{}'", i, field_str);
            fields.push(field_str);
        }
    }
    
    debug_println!("RUST DEBUG: Final default search fields: {:?}", fields);
    Ok(fields)
}

fn extract_text_fields_from_schema(_env: &mut JNIEnv, schema_ptr: jlong) -> Result<Vec<String>> {
    debug_println!("RUST DEBUG: extract_text_fields_from_schema called with schema pointer: {}", schema_ptr);
    
    // First try to get schema from registry (original approach)
    if let Some(schema) = crate::utils::jlong_to_arc::<tantivy::schema::Schema>(schema_ptr) {
        debug_println!("RUST DEBUG: ✅ Successfully retrieved schema from registry with pointer: {}", schema_ptr);
        return extract_fields_from_schema(&schema);
    }
    
    debug_println!("RUST DEBUG: ❌ Failed to retrieve schema from registry with pointer: {}", schema_ptr);
    debug_println!("RUST DEBUG: This suggests either the pointer is invalid or the schema is not in the registry");
    
    // NEW APPROACH: Try to get any cached schema from the split schema cache
    // Since we don't have the split URI in this context, iterate through all cached schemas
    debug_println!("RUST DEBUG: Attempting to retrieve schema from split schema cache...");
    let cache = SPLIT_SCHEMA_CACHE.lock().unwrap();
    for (split_uri, cached_schema) in cache.iter() {
        debug_println!("RUST DEBUG: Found cached schema for split URI: {}", split_uri);
        let text_fields = extract_fields_from_schema(cached_schema)?;
        debug_println!("RUST DEBUG: ✅ Using cached schema from split: {} with text fields: {:?}", split_uri, text_fields);
        return Ok(text_fields);
    }
    drop(cache);
    
    debug_println!("RUST DEBUG: ❌ No cached schemas found in split schema cache");
    
    Err(anyhow!("Schema registry lookup failed for pointer: {} and no cached schemas found - this indicates a schema pointer lifecycle issue", schema_ptr))
}

/// Helper function to extract text fields from a schema
fn extract_fields_from_schema(schema: &tantivy::schema::Schema) -> Result<Vec<String>> {
    let mut text_fields = Vec::new();
    
    // Iterate through all fields in the schema
    for (_field, field_entry) in schema.fields() {
        let field_name = field_entry.name();
        
        // Check if this field is a text field that's indexed
        if let tantivy::schema::FieldType::Str(text_options) = field_entry.field_type() {
            if text_options.get_indexing_options().is_some() {
                debug_println!("RUST DEBUG: Found indexed text field: '{}'", field_name);
                text_fields.push(field_name.to_string());
            } else {
                debug_println!("RUST DEBUG: Skipping non-indexed text field: '{}'", field_name);
            }
        }
    }
    
    debug_println!("RUST DEBUG: Extracted {} text fields from schema: {:?}", text_fields.len(), text_fields);
    Ok(text_fields)
}