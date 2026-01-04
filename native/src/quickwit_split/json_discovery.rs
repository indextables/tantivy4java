// json_discovery.rs - JSON field discovery and doc mapping extraction
// Discovers sub-fields in JSON fields by sampling documents

use std::collections::HashMap;
use anyhow::{anyhow, Result};
use crate::debug_println;

// Debug logging macro
macro_rules! debug_log {
    ($($arg:tt)*) => {
        debug_println!("DEBUG: {}", format!($($arg)*))
    };
}

/// Discover sub-fields in a JSON field by sampling documents from the index
/// Returns a vector of discovered field mappings for nested fields
pub fn discover_json_subfields(
    tantivy_index: &tantivy::Index,
    _json_field: tantivy::schema::Field,
    field_name: &str,
    sample_size: usize
) -> Result<Vec<serde_json::Value>> {
    use tantivy::collector::TopDocs;
    use tantivy::query::AllQuery;
    use tantivy::schema::Document;

    debug_log!("🔍 Discovering JSON sub-fields for field '{}'", field_name);

    let reader = tantivy_index.reader()?;
    let searcher = reader.searcher();

    // Collect sample documents (up to sample_size)
    let top_docs = searcher.search(&AllQuery, &TopDocs::with_limit(sample_size))?;

    debug_log!("  Sampling {} documents for field discovery", top_docs.len());

    // Track discovered sub-fields and their types
    let mut discovered_fields: HashMap<String, String> = HashMap::new();

    for (_score, doc_address) in top_docs {
        let tantivy_doc: tantivy::schema::TantivyDocument = searcher.doc(doc_address)?;

        // Convert to named document to get OwnedValue types
        let named_doc = tantivy_doc.to_named_doc(&tantivy_index.schema());

        // Get JSON values from the document
        if let Some(values) = named_doc.0.get(field_name) {
            for value in values {
                if let tantivy::schema::OwnedValue::Object(obj) = value {
                    // Recursively discover fields in this object
                    discover_fields_from_object(&obj, "", &mut discovered_fields);
                }
            }
        }
    }

    debug_log!("  Discovered {} sub-fields", discovered_fields.len());

    // Convert discovered fields to Quickwit field mappings format
    let mut field_mappings = Vec::new();
    for (subfield_name, subfield_type) in discovered_fields {
        let mapping = match subfield_type.as_str() {
            "i64" => serde_json::json!({
                "name": subfield_name,
                "type": "i64",
                "stored": false,
                "indexed": true,
                "fast": true  // Numeric fields typically need fast fields
            }),
            "u64" => serde_json::json!({
                "name": subfield_name,
                "type": "u64",
                "stored": false,
                "indexed": true,
                "fast": true
            }),
            "f64" => serde_json::json!({
                "name": subfield_name,
                "type": "f64",
                "stored": false,
                "indexed": true,
                "fast": true
            }),
            "bool" => serde_json::json!({
                "name": subfield_name,
                "type": "bool",
                "stored": false,
                "indexed": true,
                "fast": true
            }),
            "text" => serde_json::json!({
                "name": subfield_name,
                "type": "text",
                "stored": false,
                "indexed": true,
                "fast": false  // Text fields don't need fast by default
            }),
            "date" => serde_json::json!({
                "name": subfield_name,
                "type": "datetime",
                "stored": false,
                "indexed": true,
                "fast": true
            }),
            _ => {
                // Default to text for unknown types
                debug_log!("  Unknown type '{}' for field '{}', defaulting to text", subfield_type, subfield_name);
                serde_json::json!({
                    "name": subfield_name,
                    "type": "text",
                    "stored": false,
                    "indexed": true,
                    "fast": false
                })
            }
        };

        debug_log!("  Mapped sub-field: {} -> {}", subfield_name, subfield_type);
        field_mappings.push(mapping);
    }

    Ok(field_mappings)
}

/// Recursively discover fields from a JSON object
pub fn discover_fields_from_object(
    obj: &[(String, tantivy::schema::OwnedValue)],
    prefix: &str,
    discovered: &mut HashMap<String, String>
) {
    for (key, value) in obj {
        let full_key = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{}.{}", prefix, key)
        };

        // Determine the type of this field
        let field_type = match value {
            tantivy::schema::OwnedValue::Str(_) => "text",
            tantivy::schema::OwnedValue::U64(_) => "u64",
            tantivy::schema::OwnedValue::I64(_) => "i64",
            tantivy::schema::OwnedValue::F64(_) => "f64",
            tantivy::schema::OwnedValue::Bool(_) => "bool",
            tantivy::schema::OwnedValue::Date(_) => "date",
            tantivy::schema::OwnedValue::Object(nested_obj) => {
                // Recursively process nested objects
                discover_fields_from_object(nested_obj, &full_key, discovered);
                continue;  // Don't add the object itself as a field
            }
            tantivy::schema::OwnedValue::Array(arr) => {
                // For arrays, discover type from first element
                if let Some(first_elem) = arr.first() {
                    match first_elem {
                        tantivy::schema::OwnedValue::Str(_) => "text",
                        tantivy::schema::OwnedValue::U64(_) => "u64",
                        tantivy::schema::OwnedValue::I64(_) => "i64",
                        tantivy::schema::OwnedValue::F64(_) => "f64",
                        tantivy::schema::OwnedValue::Bool(_) => "bool",
                        tantivy::schema::OwnedValue::Date(_) => "date",
                        _ => "text"  // Default for complex types
                    }
                } else {
                    "text"  // Empty array, default to text
                }
            }
            _ => "text"  // Default for unknown types
        };

        // Store or update the discovered field type
        // If we've seen this field before with a different type, prefer numeric types
        if let Some(existing_type) = discovered.get(&full_key) {
            // Type priority: f64 > i64 > u64 > bool > date > text
            let should_update = match (existing_type.as_str(), field_type) {
                ("text", _) => true,  // Always override text
                ("date", "f64") | ("date", "i64") | ("date", "u64") => true,
                ("bool", "f64") | ("bool", "i64") | ("bool", "u64") | ("bool", "date") => true,
                ("u64", "f64") | ("u64", "i64") => true,
                ("i64", "f64") => true,
                _ => false
            };

            if should_update {
                discovered.insert(full_key, field_type.to_string());
            }
        } else {
            discovered.insert(full_key, field_type.to_string());
        }
    }
}

/// Extract or create a DocMapper from a Tantivy index schema
pub fn extract_doc_mapping_from_index(tantivy_index: &tantivy::Index) -> Result<String> {
    // Get the schema from the Tantivy index
    let schema = tantivy_index.schema();

    debug_log!("Extracting doc mapping from Tantivy schema with {} fields", schema.fields().count());

    // Create field mappings from the actual Tantivy schema as an array (not HashMap)
    let mut field_mappings = Vec::new();

    for (field, field_entry) in schema.fields() {
        let field_name = field_entry.name();
        debug_log!("  Processing field: {} with type: {:?}, is_fast: {}, is_indexed: {}, is_stored: {}",
                   field_name, field_entry.field_type(), field_entry.is_fast(), field_entry.is_indexed(), field_entry.is_stored());

        // Map Tantivy field types to Quickwit field mapping types
        let field_mapping = match field_entry.field_type() {
            tantivy::schema::FieldType::Str(_) => {
                // Text field
                serde_json::json!({
                    "name": field_name,
                    "type": "text",
                    "stored": field_entry.is_stored(),
                    "indexed": field_entry.is_indexed(),
                    "fast": field_entry.is_fast()
                })
            },
            tantivy::schema::FieldType::U64(_) => {
                // Unsigned integer field
                serde_json::json!({
                    "name": field_name,
                    "type": "u64",
                    "stored": field_entry.is_stored(),
                    "indexed": field_entry.is_indexed(),
                    "fast": field_entry.is_fast()
                })
            },
            tantivy::schema::FieldType::I64(_) => {
                // Signed integer field
                serde_json::json!({
                    "name": field_name,
                    "type": "i64",
                    "stored": field_entry.is_stored(),
                    "indexed": field_entry.is_indexed(),
                    "fast": field_entry.is_fast()
                })
            },
            tantivy::schema::FieldType::F64(_) => {
                // Float field
                serde_json::json!({
                    "name": field_name,
                    "type": "f64",
                    "stored": field_entry.is_stored(),
                    "indexed": field_entry.is_indexed(),
                    "fast": field_entry.is_fast()
                })
            },
            tantivy::schema::FieldType::Bool(_) => {
                // Boolean field
                serde_json::json!({
                    "name": field_name,
                    "type": "bool",
                    "stored": field_entry.is_stored(),
                    "indexed": field_entry.is_indexed(),
                    "fast": field_entry.is_fast()
                })
            },
            tantivy::schema::FieldType::Date(_) => {
                // Date field
                serde_json::json!({
                    "name": field_name,
                    "type": "datetime",
                    "stored": field_entry.is_stored(),
                    "indexed": field_entry.is_indexed(),
                    "fast": field_entry.is_fast()
                })
            },
            tantivy::schema::FieldType::Facet(_) => {
                // Facet field
                serde_json::json!({
                    "name": field_name,
                    "type": "text",
                    "tokenizer": "raw",
                    "stored": field_entry.is_stored(),
                    "indexed": field_entry.is_indexed(),
                    "fast": field_entry.is_fast()
                })
            },
            tantivy::schema::FieldType::Bytes(_) => {
                // Bytes field
                serde_json::json!({
                    "name": field_name,
                    "type": "bytes",
                    "stored": field_entry.is_stored(),
                    "indexed": field_entry.is_indexed(),
                    "fast": field_entry.is_fast()
                })
            },
            tantivy::schema::FieldType::JsonObject(json_options) => {
                // JSON object field - discover sub-fields from indexed documents
                // This enables Quickwit DocMapper compatibility by providing explicit field mappings

                debug_log!("🔍 Processing JSON field '{}' - discovering sub-fields from index", field_name);

                // Discover sub-fields by sampling documents (sample size: 1000)
                let discovered_subfields = discover_json_subfields(tantivy_index, field, field_name, 1000)
                    .unwrap_or_else(|e| {
                        debug_log!("⚠️  Failed to discover sub-fields for '{}': {}. Using empty array.", field_name, e);
                        Vec::new()
                    });

                debug_log!("✅ Discovered {} sub-fields for JSON field '{}'", discovered_subfields.len(), field_name);

                let mut json_obj = serde_json::json!({
                    "name": field_name,
                    "type": "object",
                    "field_mappings": discovered_subfields,  // Populated from actual data!
                    "stored": field_entry.is_stored(),
                    "indexed": json_options.is_indexed(),
                    "fast": json_options.is_fast(),
                    "expand_dots": json_options.is_expand_dots_enabled()
                });

                // Add tokenizer if indexing is enabled
                if let Some(text_indexing) = json_options.get_text_indexing_options() {
                    json_obj["tokenizer"] = serde_json::json!(text_indexing.tokenizer());
                }

                // Add fast field tokenizer if available
                if let Some(fast_tokenizer) = json_options.get_fast_field_tokenizer_name() {
                    json_obj["fast_tokenizer"] = serde_json::json!(fast_tokenizer);
                }

                debug_log!("🔧 JSON FIELD EXPORT: field '{}' => {}", field_name, serde_json::to_string_pretty(&json_obj).unwrap_or_else(|_| "error".to_string()));
                json_obj
            },
            tantivy::schema::FieldType::IpAddr(_) => {
                // IP address field
                serde_json::json!({
                    "name": field_name,
                    "type": "ip",
                    "stored": field_entry.is_stored(),
                    "indexed": field_entry.is_indexed(),
                    "fast": field_entry.is_fast()
                })
            }
        };

        field_mappings.push(field_mapping);
    }

    // Create the doc mapping JSON structure as just the field_mappings array
    // DocMapperBuilder expects the root to be a sequence of field mappings
    let doc_mapping_json = serde_json::json!(field_mappings);

    let doc_mapping_str = serde_json::to_string(&doc_mapping_json)
        .map_err(|e| anyhow!("Failed to serialize DocMapping to JSON: {}", e))?;

    debug_log!("Successfully created doc mapping JSON from actual Tantivy schema ({} bytes)", doc_mapping_str.len());

    Ok(doc_mapping_str)
}
