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

/// Type alias for the nested HashMap used to track JSON sub-field names and types.
/// Outer key: JSON field name, Inner key: sub-field path, Inner value: type string.
pub type JsonSubfieldMap = HashMap<String, HashMap<String, String>>;

/// Convert a sub-field type string to a Quickwit field mapping JSON value.
fn subfield_type_to_mapping(name: &str, type_str: &str) -> serde_json::Value {
    match type_str {
        "i64" => serde_json::json!({
            "name": name, "type": "i64",
            "stored": false, "indexed": true, "fast": true
        }),
        "u64" => serde_json::json!({
            "name": name, "type": "u64",
            "stored": false, "indexed": true, "fast": true
        }),
        "f64" => serde_json::json!({
            "name": name, "type": "f64",
            "stored": false, "indexed": true, "fast": true
        }),
        "bool" => serde_json::json!({
            "name": name, "type": "bool",
            "stored": false, "indexed": true, "fast": true
        }),
        "text" => serde_json::json!({
            "name": name, "type": "text",
            "stored": false, "indexed": true, "fast": false
        }),
        "date" => serde_json::json!({
            "name": name, "type": "datetime",
            "stored": false, "indexed": true, "fast": true
        }),
        _ => {
            debug_log!("  Unknown type '{}' for field '{}', defaulting to text", type_str, name);
            serde_json::json!({
                "name": name, "type": "text",
                "stored": false, "indexed": true, "fast": false
            })
        }
    }
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
    let top_docs = searcher.search(&AllQuery, &TopDocs::with_limit(sample_size).order_by_score())?;

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
    for (subfield_name, subfield_type) in &discovered_fields {
        let mapping = subfield_type_to_mapping(subfield_name, subfield_type);
        debug_log!("  Mapped sub-field: {} -> {}", subfield_name, subfield_type);
        field_mappings.push(mapping);
    }

    Ok(field_mappings)
}

/// Returns true if `new_type` should replace `existing_type` based on type priority.
/// Priority: f64 > i64 > u64 > bool > date > text
fn should_widen_type(existing_type: &str, new_type: &str) -> bool {
    match (existing_type, new_type) {
        ("text", _) => true,
        ("date", "f64") | ("date", "i64") | ("date", "u64") => true,
        ("bool", "f64") | ("bool", "i64") | ("bool", "u64") | ("bool", "date") => true,
        ("u64", "f64") | ("u64", "i64") => true,
        ("i64", "f64") => true,
        _ => false
    }
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
        // If we've seen this field before with a different type, widen using priority logic
        if let Some(existing_type) = discovered.get(&full_key) {
            if should_widen_type(existing_type, field_type) {
                discovered.insert(full_key, field_type.to_string());
            }
        } else {
            discovered.insert(full_key, field_type.to_string());
        }
    }
}

/// Extract JSON sub-fields from a doc_mapping JSON string.
/// Parses `type: "object"` fields and extracts their `field_mappings` into
/// the `JsonSubfieldMap` format, enabling sub-field recovery from serialized doc_mappings.
pub fn extract_json_subfields_from_doc_mapping(doc_mapping_json: &str) -> JsonSubfieldMap {
    let mut result: JsonSubfieldMap = HashMap::new();

    let parsed: serde_json::Value = match serde_json::from_str(doc_mapping_json) {
        Ok(v) => v,
        Err(_) => return result,
    };

    let field_mappings = match &parsed {
        serde_json::Value::Array(arr) => arr,
        _ => return result,
    };

    for mapping in field_mappings {
        let field_type = mapping.get("type").and_then(|t| t.as_str()).unwrap_or("");
        if field_type != "object" {
            continue;
        }

        let field_name = match mapping.get("name").and_then(|n| n.as_str()) {
            Some(n) => n.to_string(),
            None => continue,
        };

        let sub_mappings = match mapping.get("field_mappings").and_then(|fm| fm.as_array()) {
            Some(fm) => fm,
            None => continue,
        };

        let mut subfield_map: HashMap<String, String> = HashMap::new();
        for sub in sub_mappings {
            let sub_name = sub.get("name").and_then(|n| n.as_str()).unwrap_or("");
            let sub_type = sub.get("type").and_then(|t| t.as_str()).unwrap_or("text");
            if !sub_name.is_empty() {
                // Map "datetime" back to "date" for internal representation
                let internal_type = if sub_type == "datetime" { "date" } else { sub_type };
                subfield_map.insert(sub_name.to_string(), internal_type.to_string());
            }
        }

        if !subfield_map.is_empty() {
            result.insert(field_name, subfield_map);
        }
    }

    result
}

/// Common implementation for extracting doc mapping from a Tantivy index schema.
///
/// - `json_subfields: None` → sample from .store (standard splits with stored data)
/// - `json_subfields: Some(map)` → use pre-collected map (companion mode, or merge recovery)
fn extract_doc_mapping_impl(
    tantivy_index: &tantivy::Index,
    json_subfields: Option<&JsonSubfieldMap>,
) -> Result<String> {
    let schema = tantivy_index.schema();

    debug_log!("Extracting doc mapping from Tantivy schema with {} fields (json_subfields: {})",
               schema.fields().count(),
               if json_subfields.is_some() { "pre-collected" } else { "sample from store" });

    let mut field_mappings = Vec::new();

    for (field, field_entry) in schema.fields() {
        let field_name = field_entry.name();

        let field_mapping = match field_entry.field_type() {
            tantivy::schema::FieldType::Str(text_options) => {
                let tokenizer = text_options.get_indexing_options()
                    .map(|opts| opts.tokenizer().to_string())
                    .unwrap_or_else(|| "default".to_string());
                serde_json::json!({
                    "name": field_name,
                    "type": "text",
                    "tokenizer": tokenizer,
                    "stored": field_entry.is_stored(),
                    "indexed": field_entry.is_indexed(),
                    "fast": field_entry.is_fast()
                })
            },
            tantivy::schema::FieldType::U64(_) => {
                serde_json::json!({
                    "name": field_name,
                    "type": "u64",
                    "stored": field_entry.is_stored(),
                    "indexed": field_entry.is_indexed(),
                    "fast": field_entry.is_fast()
                })
            },
            tantivy::schema::FieldType::I64(_) => {
                serde_json::json!({
                    "name": field_name,
                    "type": "i64",
                    "stored": field_entry.is_stored(),
                    "indexed": field_entry.is_indexed(),
                    "fast": field_entry.is_fast()
                })
            },
            tantivy::schema::FieldType::F64(_) => {
                serde_json::json!({
                    "name": field_name,
                    "type": "f64",
                    "stored": field_entry.is_stored(),
                    "indexed": field_entry.is_indexed(),
                    "fast": field_entry.is_fast()
                })
            },
            tantivy::schema::FieldType::Bool(_) => {
                serde_json::json!({
                    "name": field_name,
                    "type": "bool",
                    "stored": field_entry.is_stored(),
                    "indexed": field_entry.is_indexed(),
                    "fast": field_entry.is_fast()
                })
            },
            tantivy::schema::FieldType::Date(_) => {
                serde_json::json!({
                    "name": field_name,
                    "type": "datetime",
                    "stored": field_entry.is_stored(),
                    "indexed": field_entry.is_indexed(),
                    "fast": field_entry.is_fast()
                })
            },
            tantivy::schema::FieldType::Facet(_) => {
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
                serde_json::json!({
                    "name": field_name,
                    "type": "bytes",
                    "stored": field_entry.is_stored(),
                    "indexed": field_entry.is_indexed(),
                    "fast": field_entry.is_fast()
                })
            },
            tantivy::schema::FieldType::JsonObject(json_options) => {
                // Resolve JSON sub-fields: either from pre-collected map or by sampling .store
                let discovered_subfields = if let Some(subfield_map) = json_subfields {
                    // Companion / merge mode: use pre-collected sub-fields
                    if let Some(field_subfields) = subfield_map.get(field_name) {
                        field_subfields.iter()
                            .map(|(name, type_str)| subfield_type_to_mapping(name, type_str))
                            .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    }
                } else {
                    // Standard mode: discover by sampling stored documents
                    debug_log!("🔍 Processing JSON field '{}' - discovering sub-fields from index", field_name);
                    discover_json_subfields(tantivy_index, field, field_name, 1000)
                        .unwrap_or_else(|e| {
                            debug_log!("⚠️  Failed to discover sub-fields for '{}': {}. Using empty array.", field_name, e);
                            Vec::new()
                        })
                };

                if discovered_subfields.is_empty() {
                    debug_log!("⚠️  Skipping JSON field '{}' from doc_mapping — no sub-fields discovered", field_name);
                    continue;
                }

                let mut json_obj = serde_json::json!({
                    "name": field_name,
                    "type": "object",
                    "field_mappings": discovered_subfields,
                    "stored": field_entry.is_stored(),
                    "indexed": json_options.is_indexed(),
                    "fast": json_options.is_fast(),
                    "expand_dots": json_options.is_expand_dots_enabled()
                });

                if let Some(text_indexing) = json_options.get_text_indexing_options() {
                    json_obj["tokenizer"] = serde_json::json!(text_indexing.tokenizer());
                }
                if let Some(fast_tokenizer) = json_options.get_fast_field_tokenizer_name() {
                    json_obj["fast_tokenizer"] = serde_json::json!(fast_tokenizer);
                }

                json_obj
            },
            tantivy::schema::FieldType::IpAddr(_) => {
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

    let doc_mapping_json = serde_json::json!(field_mappings);
    let doc_mapping_str = serde_json::to_string(&doc_mapping_json)
        .map_err(|e| anyhow!("Failed to serialize DocMapping to JSON: {}", e))?;

    debug_log!("Successfully created doc mapping JSON ({} bytes)", doc_mapping_str.len());

    Ok(doc_mapping_str)
}

/// Discover JSON sub-fields by enumerating fast field columns in the index.
/// This works for companion splits that have no .store files — the JSON data
/// is still present in the columnar fast field format.
///
/// Column names for JSON sub-fields use `\x01` as the path separator.
/// For a field "data" with sub-field "user.name", the column is "data\x01user\x01name".
pub fn discover_json_subfields_from_fast_fields(
    tantivy_index: &tantivy::Index,
) -> Result<JsonSubfieldMap> {
    use tantivy::index::SegmentComponent;
    use tantivy::columnar::{ColumnarReader, ColumnType};
    use tantivy::directory::Directory;

    let mut result: JsonSubfieldMap = HashMap::new();

    // Identify which fields are JSON type in the schema
    let schema = tantivy_index.schema();
    let json_field_names: Vec<String> = schema.fields()
        .filter_map(|(_, entry)| {
            if matches!(entry.field_type(), tantivy::schema::FieldType::JsonObject(_)) {
                Some(entry.name().to_string())
            } else {
                None
            }
        })
        .collect();

    if json_field_names.is_empty() {
        return Ok(result);
    }

    let sep = 1u8 as char; // JSON_PATH_SEGMENT_SEP

    let segment_metas = tantivy_index.searchable_segment_metas()?;

    for segment_meta in &segment_metas {
        let fast_field_path = segment_meta.relative_path(SegmentComponent::FastFields);
        let file_slice = match tantivy_index.directory().open_read(&fast_field_path) {
            Ok(f) => f,
            Err(_) => continue,
        };

        let columnar_reader: ColumnarReader = match ColumnarReader::open(file_slice) {
            Ok(r) => r,
            Err(_) => continue,
        };

        let columns: Vec<(String, tantivy::columnar::DynamicColumnHandle)> = match columnar_reader.list_columns() {
            Ok(c) => c,
            Err(_) => continue,
        };

        for (col_name, col_handle) in &columns {
            // Check if this column belongs to one of our JSON fields
            for json_field in &json_field_names {
                let prefix = format!("{}{}", json_field, sep);
                if col_name.starts_with(&prefix) {
                    // Extract the sub-field path after the field name + separator
                    let subpath = &col_name[prefix.len()..];
                    // Convert \x01 separators back to dots for display
                    let dotted_path = subpath.replace(sep, ".");

                    let type_str = match col_handle.column_type() {
                        ColumnType::I64 => "i64",
                        ColumnType::U64 => "u64",
                        ColumnType::F64 => "f64",
                        ColumnType::Bool => "bool",
                        ColumnType::DateTime => "date",
                        ColumnType::Str => "text",
                        ColumnType::Bytes => "text",
                        ColumnType::IpAddr => "text",
                    };

                    let subfields = result.entry(json_field.clone()).or_default();
                    // Apply type widening: if the same sub-field appears with different
                    // types across segments, keep the widest type (e.g., f64 > i64 > u64).
                    match subfields.entry(dotted_path) {
                        std::collections::hash_map::Entry::Occupied(mut e) => {
                            if should_widen_type(e.get(), type_str) {
                                e.insert(type_str.to_string());
                            }
                        }
                        std::collections::hash_map::Entry::Vacant(e) => {
                            e.insert(type_str.to_string());
                        }
                    }
                }
            }
        }
    }

    debug_log!("Discovered {} JSON fields with sub-fields from fast field columns",
               result.len());
    for (field, subs) in &result {
        debug_log!("  JSON field '{}': {} sub-fields", field, subs.len());
    }

    Ok(result)
}

/// Extract or create a DocMapper from a Tantivy index schema.
/// Discovers JSON sub-fields by sampling documents from the .store.
/// Use this for standard splits that have stored fields.
pub fn extract_doc_mapping_from_index(tantivy_index: &tantivy::Index) -> Result<String> {
    extract_doc_mapping_impl(tantivy_index, None)
}

/// Extract doc mapping from a Tantivy index schema, using pre-collected JSON sub-field info
/// instead of sampling from .store files. Used in companion mode where .store is skipped.
pub fn extract_doc_mapping_with_json_subfields(
    tantivy_index: &tantivy::Index,
    json_subfields: &JsonSubfieldMap,
) -> Result<String> {
    extract_doc_mapping_impl(tantivy_index, Some(json_subfields))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subfield_type_to_mapping_all_types() {
        let cases = vec![
            ("count", "i64", "i64", true),
            ("total", "u64", "u64", true),
            ("price", "f64", "f64", true),
            ("active", "bool", "bool", true),
            ("name", "text", "text", false),
            ("created", "date", "datetime", true),
            ("weird", "unknown_type", "text", false),
        ];

        for (name, input_type, expected_type, expected_fast) in cases {
            let mapping = subfield_type_to_mapping(name, input_type);
            assert_eq!(mapping["name"], name, "name mismatch for input '{}'", input_type);
            assert_eq!(mapping["type"], expected_type, "type mismatch for input '{}'", input_type);
            assert_eq!(mapping["fast"], expected_fast, "fast mismatch for input '{}'", input_type);
            assert_eq!(mapping["stored"], false, "stored should be false for '{}'", input_type);
            assert_eq!(mapping["indexed"], true, "indexed should be true for '{}'", input_type);
        }
    }

    #[test]
    fn test_discover_fields_from_object_flat() {
        use tantivy::schema::OwnedValue;

        let obj = vec![
            ("user".to_string(), OwnedValue::Str("alice".to_string())),
            ("score".to_string(), OwnedValue::I64(42)),
        ];

        let mut discovered = HashMap::new();
        discover_fields_from_object(&obj, "", &mut discovered);

        assert_eq!(discovered.len(), 2);
        assert_eq!(discovered.get("user").unwrap(), "text");
        assert_eq!(discovered.get("score").unwrap(), "i64");
    }

    #[test]
    fn test_discover_fields_from_object_nested() {
        use tantivy::schema::OwnedValue;

        let obj = vec![
            ("addr".to_string(), OwnedValue::Object(vec![
                ("city".to_string(), OwnedValue::Str("NYC".to_string())),
            ])),
        ];

        let mut discovered = HashMap::new();
        discover_fields_from_object(&obj, "", &mut discovered);

        assert_eq!(discovered.len(), 1);
        assert_eq!(discovered.get("addr.city").unwrap(), "text");
    }

    #[test]
    fn test_discover_fields_from_object_type_widening() {
        use tantivy::schema::OwnedValue;

        // First doc: score is i64
        let obj1 = vec![
            ("score".to_string(), OwnedValue::I64(42)),
        ];
        let mut discovered = HashMap::new();
        discover_fields_from_object(&obj1, "", &mut discovered);
        assert_eq!(discovered.get("score").unwrap(), "i64");

        // Second doc: score is f64 → should widen to f64
        let obj2 = vec![
            ("score".to_string(), OwnedValue::F64(3.14)),
        ];
        discover_fields_from_object(&obj2, "", &mut discovered);
        assert_eq!(discovered.get("score").unwrap(), "f64",
            "i64 should widen to f64 when f64 value is seen");

        // Third doc: score is i64 again → must stay f64 (no narrowing)
        let obj3 = vec![
            ("score".to_string(), OwnedValue::I64(99)),
        ];
        discover_fields_from_object(&obj3, "", &mut discovered);
        assert_eq!(discovered.get("score").unwrap(), "f64",
            "f64 must not narrow back to i64");
    }

    #[test]
    fn test_extract_json_subfields_from_doc_mapping_roundtrip() {
        let doc_mapping = serde_json::json!([
            {
                "name": "data",
                "type": "object",
                "field_mappings": [
                    {"name": "user", "type": "text", "stored": false, "indexed": true, "fast": false},
                    {"name": "score", "type": "i64", "stored": false, "indexed": true, "fast": true},
                    {"name": "created", "type": "datetime", "stored": false, "indexed": true, "fast": true}
                ]
            }
        ]);

        let json_str = serde_json::to_string(&doc_mapping).unwrap();
        let result = extract_json_subfields_from_doc_mapping(&json_str);

        assert_eq!(result.len(), 1);
        let data_subs = result.get("data").expect("Should have 'data' entry");
        assert_eq!(data_subs.len(), 3);
        assert_eq!(data_subs.get("user").unwrap(), "text");
        assert_eq!(data_subs.get("score").unwrap(), "i64");
        // datetime maps back to "date" internally
        assert_eq!(data_subs.get("created").unwrap(), "date");
    }

    #[test]
    fn test_extract_json_subfields_from_doc_mapping_empty() {
        // Invalid JSON → empty map
        let result = extract_json_subfields_from_doc_mapping("not json");
        assert!(result.is_empty());

        // Valid JSON but not an array → empty map
        let result = extract_json_subfields_from_doc_mapping(r#"{"foo": "bar"}"#);
        assert!(result.is_empty());

        // Empty array → empty map
        let result = extract_json_subfields_from_doc_mapping("[]");
        assert!(result.is_empty());
    }

    /// Verify that the non-companion .store sampling path
    /// (`extract_doc_mapping_from_index`) correctly discovers JSON sub-fields
    /// by reading stored documents from the index.
    #[test]
    fn test_extract_doc_mapping_from_index_discovers_json_subfields_via_store() {
        use tantivy::schema::*;
        use std::collections::BTreeMap;

        // Build schema: a stored JSON field + a regular text field
        let mut builder = SchemaBuilder::new();
        let title_field = builder.add_text_field("title", TEXT | STORED);
        // Stored-only (no indexing) is sufficient to test the .store sampling path;
        // production schemas also set indexing+fast, but that doesn't affect store round-trip.
        let json_field = builder.add_json_field(
            "data",
            JsonObjectOptions::default().set_stored(),
        );
        let schema = builder.build();

        // Create in-memory index and add documents with varied sub-field types
        let index = tantivy::Index::create_in_ram(schema);
        let mut writer = index.writer_with_num_threads(1, 15_000_000).unwrap();

        // Doc 1: flat fields + nested object + array
        let mut doc1 = TantivyDocument::new();
        doc1.add_text(title_field, "hello");
        let mut map1 = BTreeMap::new();
        map1.insert("user".to_string(), OwnedValue::Str("alice".to_string()));
        map1.insert("score".to_string(), OwnedValue::U64(42));
        map1.insert("active".to_string(), OwnedValue::Bool(true));
        // Nested object — exercises recursive dotted-path discovery
        let addr1 = vec![
            ("city".to_string(), OwnedValue::Str("NYC".to_string())),
            ("zip".to_string(), OwnedValue::U64(10001)),
        ];
        map1.insert("addr".to_string(), OwnedValue::Object(addr1));
        // Array field — exercises first-element type inference
        map1.insert("tags".to_string(), OwnedValue::Array(vec![
            OwnedValue::Str("admin".to_string()),
            OwnedValue::Str("dev".to_string()),
        ]));
        doc1.add_object(json_field, map1);
        writer.add_document(doc1).unwrap();

        // Doc 2: same structure, different values
        let mut doc2 = TantivyDocument::new();
        doc2.add_text(title_field, "world");
        let mut map2 = BTreeMap::new();
        map2.insert("user".to_string(), OwnedValue::Str("bob".to_string()));
        map2.insert("score".to_string(), OwnedValue::U64(99));
        map2.insert("active".to_string(), OwnedValue::Bool(false));
        let addr2 = vec![
            ("city".to_string(), OwnedValue::Str("SF".to_string())),
            ("zip".to_string(), OwnedValue::U64(94102)),
        ];
        map2.insert("addr".to_string(), OwnedValue::Object(addr2));
        map2.insert("tags".to_string(), OwnedValue::Array(vec![
            OwnedValue::Str("user".to_string()),
        ]));
        doc2.add_object(json_field, map2);
        writer.add_document(doc2).unwrap();

        writer.commit().unwrap();

        // Call the non-companion path: samples from .store to discover sub-fields
        let doc_mapping_str = extract_doc_mapping_from_index(&index)
            .expect("extract_doc_mapping_from_index should succeed on stored JSON fields");

        let doc_mapping: serde_json::Value = serde_json::from_str(&doc_mapping_str)
            .expect("doc mapping should be valid JSON");

        let field_mappings = doc_mapping.as_array()
            .expect("doc mapping should be an array");

        // Find the "data" field — should be type "object" with discovered sub-fields
        let data_mapping = field_mappings.iter()
            .find(|m| m["name"] == "data")
            .expect("Should find 'data' field in doc mapping");

        assert_eq!(data_mapping["type"], "object",
            "JSON field should be mapped as object type");

        let sub_fields = data_mapping["field_mappings"].as_array()
            .expect("data object should have field_mappings from .store sampling");

        assert_eq!(sub_fields.len(), 6,
            "Should discover 6 sub-fields (user, score, active, addr.city, addr.zip, tags), got: {:?}",
            sub_fields.iter().map(|f| f["name"].as_str().unwrap_or("?")).collect::<Vec<_>>());

        let sub_by_name: HashMap<&str, &serde_json::Value> = sub_fields.iter()
            .map(|f| (f["name"].as_str().unwrap(), f))
            .collect();

        // Flat fields
        assert_eq!(sub_by_name["user"]["type"], "text");
        assert_eq!(sub_by_name["score"]["type"], "u64");
        assert_eq!(sub_by_name["active"]["type"], "bool");
        // Nested object — discovered via dotted path recursion
        assert_eq!(sub_by_name["addr.city"]["type"], "text");
        assert_eq!(sub_by_name["addr.zip"]["type"], "u64");
        // Array field — type inferred from first element
        assert_eq!(sub_by_name["tags"]["type"], "text");

        // Verify "title" is present as a regular text field (non-JSON fields unaffected)
        let title_mapping = field_mappings.iter()
            .find(|m| m["name"] == "title")
            .expect("Should find 'title' field in doc mapping");
        assert_eq!(title_mapping["type"], "text");
    }
}
