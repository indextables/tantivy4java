// parse_query.rs - Parse query string implementation
// Extracted from split_query.rs during refactoring

use anyhow::{anyhow, Result};
use jni::objects::{JObject, JObjectArray, JString, JValue};
use jni::sys::jobject;
use jni::JNIEnv;
use quickwit_query::query_ast::{query_ast_from_user_text, QueryAst};

use crate::debug_println;

use super::schema_cache::SPLIT_SCHEMA_CACHE;

/// Parse a query string into a SplitQuery
pub fn parse_query_string(
    env: &mut JNIEnv,
    query_string: JString,
    schema_ptr: jni::sys::jlong,
    default_search_fields: jobject,
) -> Result<jobject> {
    debug_println!("RUST DEBUG: *** parse_query_string CALLED ***");

    // Get the query string
    let query_str: String = env.get_string(&query_string)?.into();

    debug_println!(
        "RUST DEBUG: 🚀 Parsing query string: '{}' with schema_ptr: {}",
        query_str,
        schema_ptr
    );

    // ✅ PROPER DEFAULT FIELD EXTRACTION: Extract default search fields from JNI
    let mut default_fields_vec = extract_default_search_fields(env, default_search_fields)?;

    // 🎯 KEY LOGIC: If default fields is empty, extract ALL indexed text fields from schema
    if default_fields_vec.is_empty() {
        debug_println!(
            "RUST DEBUG: Default fields empty, extracting all indexed text fields from schema"
        );

        // Extract all indexed text field names from the schema
        default_fields_vec = extract_text_fields_from_schema(env, schema_ptr)?;

        debug_println!(
            "RUST DEBUG: Auto-detected {} text fields: {:?}",
            default_fields_vec.len(),
            default_fields_vec
        );

        if default_fields_vec.is_empty() {
            debug_println!("RUST DEBUG: Warning: No indexed text fields found in schema");
        }
    }

    debug_println!(
        "RUST DEBUG: Final default search fields: {:?}",
        default_fields_vec
    );

    // 🚀 PROPER QUICKWIT PARSING: Use Quickwit's proven two-step process
    // Step 1: Create UserInputQuery AST with proper default fields
    // Use None if default fields is empty to let Quickwit handle field-less queries properly
    let default_fields_option = if default_fields_vec.is_empty() {
        debug_println!("RUST DEBUG: Using None for default fields (empty vector)");
        None
    } else {
        debug_println!(
            "RUST DEBUG: Using Some({:?}) for default fields",
            default_fields_vec
        );
        Some(default_fields_vec.clone())
    };

    let query_ast = query_ast_from_user_text(&query_str, default_fields_option.clone());
    debug_println!("RUST DEBUG: Created UserInputQuery AST: {:?}", query_ast);

    // Step 2: Parse the user query using Quickwit's parser with default search fields
    // Pass the right default fields based on what we have
    let parse_fields = match &default_fields_option {
        Some(fields) => fields,
        None => &Vec::new(), // Use empty vector when None
    };

    let parsed_ast = match query_ast.parse_user_query(parse_fields) {
        Ok(ast) => {
            debug_println!("RUST DEBUG: ✅ Successfully parsed with Quickwit: {:?}", ast);
            ast
        }
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

#[allow(dead_code)]
pub fn create_split_query_from_ast(env: &mut JNIEnv, query_ast: &QueryAst) -> Result<jobject> {
    match query_ast {
        QueryAst::Term(term_query) => {
            // Create SplitTermQuery Java object
            let class = env.find_class("io/indextables/tantivy4java/split/SplitTermQuery")?;
            let field_jstring = env.new_string(&term_query.field)?;
            let value_jstring = env.new_string(&term_query.value)?;

            let obj = env.new_object(
                class,
                "(Ljava/lang/String;Ljava/lang/String;)V",
                &[
                    JValue::Object(&field_jstring.into()),
                    JValue::Object(&value_jstring.into()),
                ],
            )?;

            debug_println!(
                "RUST DEBUG: ✅ Created SplitTermQuery for Term: field='{}', value='{}'",
                term_query.field,
                term_query.value
            );
            Ok(obj.into_raw())
        }
        QueryAst::FullText(fulltext_query) => {
            // Convert FullTextQuery to SplitTermQuery
            // FullTextQuery and TermQuery are conceptually the same for our purposes
            let class = env.find_class("io/indextables/tantivy4java/split/SplitTermQuery")?;
            let field_jstring = env.new_string(&fulltext_query.field)?;
            let value_jstring = env.new_string(&fulltext_query.text)?;

            let obj = env.new_object(
                class,
                "(Ljava/lang/String;Ljava/lang/String;)V",
                &[
                    JValue::Object(&field_jstring.into()),
                    JValue::Object(&value_jstring.into()),
                ],
            )?;

            debug_println!(
                "RUST DEBUG: ✅ Created SplitTermQuery for FullText: field='{}', text='{}'",
                fulltext_query.field,
                fulltext_query.text
            );
            Ok(obj.into_raw())
        }
        QueryAst::MatchAll => {
            // Create SplitMatchAllQuery Java object
            let class = env.find_class("io/indextables/tantivy4java/split/SplitMatchAllQuery")?;
            let obj = env.new_object(class, "()V", &[])?;
            debug_println!("RUST DEBUG: ✅ Created SplitMatchAllQuery for MatchAll");
            Ok(obj.into_raw())
        }
        QueryAst::Bool(_bool_query) => {
            // TODO: Implement SplitBooleanQuery creation from QueryAst
            // This is more complex as we need to recursively convert subqueries
            debug_println!(
                "RUST DEBUG: Boolean query creation from QueryAst not yet implemented"
            );

            // Fallback to MatchAll for now
            let class = env.find_class("io/indextables/tantivy4java/split/SplitMatchAllQuery")?;
            let obj = env.new_object(class, "()V", &[])?;
            Ok(obj.into_raw())
        }
        _ => {
            debug_println!(
                "RUST DEBUG: Unsupported QueryAst type for SplitQuery conversion: {:?}",
                query_ast
            );

            // Fallback to MatchAll
            let class = env.find_class("io/indextables/tantivy4java/split/SplitMatchAllQuery")?;
            let obj = env.new_object(class, "()V", &[])?;
            Ok(obj.into_raw())
        }
    }
}

pub fn create_split_parsed_query(env: &mut JNIEnv, query_ast_json: &str) -> Result<jobject> {
    // Create SplitParsedQuery Java object with the QueryAst JSON
    let class = env.find_class("io/indextables/tantivy4java/split/SplitParsedQuery")?;
    let json_jstring = env.new_string(query_ast_json)?;

    let obj = env.new_object(
        class,
        "(Ljava/lang/String;)V",
        &[JValue::Object(&json_jstring.into())],
    )?;

    debug_println!(
        "RUST DEBUG: ✅ Created SplitParsedQuery with JSON: {}",
        query_ast_json
    );
    Ok(obj.into_raw())
}

pub fn extract_default_search_fields(
    env: &mut JNIEnv,
    default_search_fields: jobject,
) -> Result<Vec<String>> {
    // Handle null case - return empty vec
    if default_search_fields.is_null() {
        debug_println!("RUST DEBUG: Default search fields is null, using empty list");
        return Ok(Vec::new());
    }

    // Convert to JObjectArray (requires unsafe block)
    let fields_array = JObjectArray::from(unsafe { JObject::from_raw(default_search_fields) });

    // Get array length
    let array_len = env.get_array_length(&fields_array)?;
    debug_println!(
        "RUST DEBUG: Default search fields array length: {}",
        array_len
    );

    let mut fields = Vec::new();

    // Extract each string from the array
    for i in 0..array_len {
        let element = env.get_object_array_element(&fields_array, i)?;
        if !element.is_null() {
            let field_str: String = env.get_string(&JString::from(element))?.into();
            debug_println!(
                "RUST DEBUG: Extracted default search field[{}]: '{}'",
                i,
                field_str
            );
            fields.push(field_str);
        }
    }

    debug_println!("RUST DEBUG: Final default search fields: {:?}", fields);
    Ok(fields)
}

pub fn extract_text_fields_from_schema(
    _env: &mut JNIEnv,
    schema_ptr: jni::sys::jlong,
) -> Result<Vec<String>> {
    debug_println!(
        "RUST DEBUG: extract_text_fields_from_schema called with schema pointer: {}",
        schema_ptr
    );

    // First try to get schema from registry (original approach)
    if let Some(schema) = crate::utils::jlong_to_arc::<tantivy::schema::Schema>(schema_ptr) {
        debug_println!(
            "RUST DEBUG: ✅ Successfully retrieved schema from registry with pointer: {}",
            schema_ptr
        );
        return extract_fields_from_schema(&schema);
    }

    debug_println!(
        "RUST DEBUG: ❌ Failed to retrieve schema from registry with pointer: {}",
        schema_ptr
    );
    debug_println!(
        "RUST DEBUG: This suggests either the pointer is invalid or the schema is not in the registry"
    );

    // NEW APPROACH: Try to get any cached schema from the split schema cache
    // Since we don't have the split URI in this context, iterate through all cached schemas
    debug_println!("RUST DEBUG: Attempting to retrieve schema from split schema cache...");
    let cache = SPLIT_SCHEMA_CACHE.lock().unwrap();
    for (split_uri, cached_schema) in cache.iter() {
        debug_println!("RUST DEBUG: Found cached schema for split URI: {}", split_uri);
        let text_fields = extract_fields_from_schema(cached_schema)?;
        debug_println!(
            "RUST DEBUG: ✅ Using cached schema from split: {} with text fields: {:?}",
            split_uri,
            text_fields
        );
        return Ok(text_fields);
    }
    drop(cache);

    debug_println!("RUST DEBUG: ❌ No cached schemas found in split schema cache");

    Err(anyhow!(
        "Schema registry lookup failed for pointer: {} and no cached schemas found - this indicates a schema pointer lifecycle issue",
        schema_ptr
    ))
}

/// Helper function to extract text fields from a schema
pub fn extract_fields_from_schema(schema: &tantivy::schema::Schema) -> Result<Vec<String>> {
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
                debug_println!(
                    "RUST DEBUG: Skipping non-indexed text field: '{}'",
                    field_name
                );
            }
        }
    }

    debug_println!(
        "RUST DEBUG: Extracted {} text fields from schema: {:?}",
        text_fields.len(),
        text_fields
    );
    Ok(text_fields)
}
