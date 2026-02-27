// range_query_fix.rs - Fix range queries in QueryAst JSON by looking up field types from schema
// Extracted from mod.rs during refactoring

use std::sync::Arc;
use anyhow::Result;
use jni::sys::jlong;
use serde_json::{Map, Value};

use crate::debug_println;
use crate::utils::with_arc_safe;
use crate::split_searcher::types::CachedSearcherContext;
use crate::split_query::get_split_schema;
use quickwit_indexing::open_index;
use quickwit_query::get_quickwit_fastfield_normalizer_manager;
use tantivy::directory::DirectoryClone;

/// Helper function to extract schema from split file - extracted from getSchemaFromNative
pub fn get_schema_from_split(searcher_ptr: jlong) -> Result<tantivy::schema::Schema> {
    with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
        let context = searcher_context.as_ref();

        // ✅ CRITICAL FIX: Use shared global runtime instead of context.runtime

        // Parse the split URI and extract schema using Quickwit's storage abstractions
        use std::path::Path;

        // Use block_on to run async code synchronously within the runtime context
        tokio::task::block_in_place(|| {
            crate::runtime_manager::QuickwitRuntimeManager::global().handle().block_on(async {
                // Use pre-created storage resolver from searcher context
                debug_println!("✅ CACHED_STORAGE_USED: Storage at address {:p} (Quickwit lifecycle)", Arc::as_ptr(&context.cached_storage));

                // Use cached storage directly (Quickwit pattern)
                let actual_storage = context.cached_storage.clone();

                // Extract just the filename for the relative path
                let relative_path = if let Some(last_slash_pos) = context.split_uri.rfind('/') {
                    Path::new(&context.split_uri[last_slash_pos + 1..])
                } else {
                    Path::new(&context.split_uri)
                };

                // Get the full file data using Quickwit's storage abstraction
                let file_size = actual_storage.file_num_bytes(relative_path).await
                    .map_err(|e| anyhow::anyhow!("Failed to get file size for {}: {}", context.split_uri, e))?;

                let split_data = actual_storage.get_slice(relative_path, 0..file_size as usize).await
                    .map_err(|e| anyhow::anyhow!("Failed to get split data from {}: {}", context.split_uri, e))?;

                // Open the bundle directory from the split data
                use quickwit_directories::BundleDirectory;
                use tantivy::directory::FileSlice;

                let split_file_slice = FileSlice::new(std::sync::Arc::new(split_data));

                // Use BundleDirectory::open_split which takes just the FileSlice and handles everything internally
                let bundle_directory = BundleDirectory::open_split(split_file_slice)
                    .map_err(|e| anyhow::anyhow!("Failed to open bundle directory {}: {}", context.split_uri, e))?;

                // ✅ QUICKWIT NATIVE: Extract schema from the bundle directory using Quickwit's native index opening
                let index = open_index(bundle_directory.box_clone(), get_quickwit_fastfield_normalizer_manager().tantivy_manager())
                    .map_err(|e| anyhow::anyhow!("Failed to open index from bundle {}: {}", context.split_uri, e))?;

                // ✅ FIX: Always use index.schema() - doc_mapping reconstruction incompatible with dynamic JSON fields
                debug_println!("RUST DEBUG: 🔧 get_schema_from_split: Using index.schema() directly (doc_mapping has compatibility issues with dynamic JSON fields)");
                let schema = index.schema();

                Ok(schema)
            })
        })
    })
    .ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))?
}

/// Fix range queries in QueryAst JSON by looking up field types from schema
pub fn fix_range_query_types(searcher_ptr: jlong, query_json: &str) -> Result<String> {
    let fix_start = std::time::Instant::now();
    debug_println!("RUST DEBUG: ⏱️ fix_range_query_types ENTRY POINT [TIMING START]");

    // Parse the JSON to find range queries
    let parse_start = std::time::Instant::now();
    let mut query_value: Value = serde_json::from_str(query_json)?;
    debug_println!("RUST DEBUG: ⏱️ Query JSON parsing completed [TIMING: {}ms]", parse_start.elapsed().as_millis());

    // 🚀 OPTIMIZATION: Try to get cached schema first instead of expensive I/O
    let schema_start = std::time::Instant::now();
    let schema = with_arc_safe(searcher_ptr, |searcher_context: &Arc<CachedSearcherContext>| {
        let context = searcher_context.as_ref();
        let split_uri = &context.split_uri;

        // First try to get schema from cache
        if let Some(cached_schema) = get_split_schema(split_uri) {
            debug_println!("RUST DEBUG: ⏱️ 🚀 Using CACHED schema instead of expensive I/O [TIMING: {}ms]", schema_start.elapsed().as_millis());
            return Ok(cached_schema);
        }

        // Fallback to expensive I/O only if cache miss
        debug_println!("RUST DEBUG: ⚠️ Cache miss, falling back to expensive I/O for schema [TIMING: {}ms]", schema_start.elapsed().as_millis());
        get_schema_from_split(searcher_ptr)
    }).ok_or_else(|| anyhow::anyhow!("Failed to access searcher context"))??;

    debug_println!("RUST DEBUG: ⏱️ Schema retrieval completed [TIMING: {}ms]", schema_start.elapsed().as_millis());

    // Recursively fix range queries in the JSON
    let recursive_start = std::time::Instant::now();
    fix_range_queries_recursive(&mut query_value, &schema)?;
    debug_println!("RUST DEBUG: ⏱️ Range query fixing completed [TIMING: {}ms]", recursive_start.elapsed().as_millis());

    // Convert back to JSON string
    let serialize_start = std::time::Instant::now();
    let result = serde_json::to_string(&query_value).map_err(|e| anyhow::anyhow!("Failed to serialize fixed query: {}", e))?;
    debug_println!("RUST DEBUG: ⏱️ JSON serialization completed [TIMING: {}ms]", serialize_start.elapsed().as_millis());

    debug_println!("RUST DEBUG: ⏱️ fix_range_query_types COMPLETED [TOTAL TIMING: {}ms]", fix_start.elapsed().as_millis());
    Ok(result)
}

/// Recursively fix range queries in a JSON value
fn fix_range_queries_recursive(value: &mut Value, schema: &tantivy::schema::Schema) -> Result<()> {
    match value {
        Value::Object(map) => {
            // Check if this is a range query
            if let Some(range_obj) = map.get_mut("range") {
                if let Some(range_map) = range_obj.as_object_mut() {
                    fix_range_query_object(range_map, schema)?;
                }
            }

            // Recursively process nested objects
            for (_, v) in map.iter_mut() {
                fix_range_queries_recursive(v, schema)?;
            }
        }
        Value::Array(arr) => {
            // Recursively process array elements
            for item in arr.iter_mut() {
                fix_range_queries_recursive(item, schema)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Fix a specific range query object by converting string values to proper types
fn fix_range_query_object(range_map: &mut Map<String, Value>, schema: &tantivy::schema::Schema) -> Result<()> {
    // Extract field name
    let field_name = range_map.get("field")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Range query missing field name"))?;

    // Get field from schema
    let field = schema.get_field(field_name)
        .map_err(|_| anyhow::anyhow!("Field '{}' not found in schema", field_name))?;

    let field_entry = schema.get_field_entry(field);
    let field_type = field_entry.field_type();

    // Determine the target JSON literal type based on Tantivy field type
    let target_type = match field_type {
        tantivy::schema::FieldType::I64(_) => "i64",
        tantivy::schema::FieldType::U64(_) => "u64",
        tantivy::schema::FieldType::F64(_) => "f64",
        tantivy::schema::FieldType::Bool(_) => "bool",
        tantivy::schema::FieldType::Date(_) => "date",
        tantivy::schema::FieldType::Str(_) => "str",
        tantivy::schema::FieldType::Facet(_) => "str",
        tantivy::schema::FieldType::Bytes(_) => "str",
        tantivy::schema::FieldType::JsonObject(_) => "str",
        tantivy::schema::FieldType::IpAddr(_) => "str",
    };

    debug_println!("RUST DEBUG: Field '{}' has type '{}', converting range bounds", field_name, target_type);

    // Fix lower_bound and upper_bound
    if let Some(lower_bound) = range_map.get_mut("lower_bound") {
        fix_bound_value(lower_bound, target_type, "lower_bound")?;
    }

    if let Some(upper_bound) = range_map.get_mut("upper_bound") {
        fix_bound_value(upper_bound, target_type, "upper_bound")?;
    }

    Ok(())
}

/// Fix a bound value (Included/Excluded with JsonLiteral)
fn fix_bound_value(bound: &mut Value, target_type: &str, bound_name: &str) -> Result<()> {
    if let Some(bound_obj) = bound.as_object_mut() {
        // Handle Included/Excluded bounds
        for (bound_type, json_literal) in bound_obj.iter_mut() {
            if bound_type == "Included" || bound_type == "Excluded" {
                if let Some(literal_obj) = json_literal.as_object_mut() {
                    // Check if this is a String literal that needs conversion
                    if let Some(string_value) = literal_obj.get("String") {
                        if let Some(string_str) = string_value.as_str() {
                            // Convert string to appropriate type
                            let new_literal = match target_type {
                                "i64" => {
                                    let parsed: i64 = string_str.parse()
                                        .map_err(|_| anyhow::anyhow!("Cannot parse '{}' as i64 in {}", string_str, bound_name))?;
                                    serde_json::json!({"Number": parsed})
                                }
                                "u64" => {
                                    let parsed: u64 = string_str.parse()
                                        .map_err(|_| anyhow::anyhow!("Cannot parse '{}' as u64 in {}", string_str, bound_name))?;
                                    serde_json::json!({"Number": parsed})
                                }
                                "f64" => {
                                    let parsed: f64 = string_str.parse()
                                        .map_err(|_| anyhow::anyhow!("Cannot parse '{}' as f64 in {}", string_str, bound_name))?;
                                    serde_json::json!({"Number": parsed})
                                }
                                "bool" => {
                                    let parsed: bool = string_str.parse()
                                        .map_err(|_| anyhow::anyhow!("Cannot parse '{}' as bool in {}", string_str, bound_name))?;
                                    serde_json::json!({"Bool": parsed})
                                }
                                "date" => {
                                    // For date fields: if the value is numeric (raw timestamp micros),
                                    // convert to Number. Otherwise keep as String for ISO8601 parsing.
                                    if let Ok(parsed) = string_str.parse::<i64>() {
                                        serde_json::json!({"Number": parsed})
                                    } else {
                                        // Keep as ISO8601 string — Quickwit handles date parsing
                                        continue;
                                    }
                                }
                                _ => {
                                    // Keep as string for other types
                                    continue;
                                }
                            };

                            debug_println!("RUST DEBUG: Converted {} '{}' from String to {} for type {}", bound_name, string_str, new_literal, target_type);

                            *json_literal = new_literal;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
