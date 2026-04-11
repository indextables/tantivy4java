// json_discovery.rs - JSON field discovery, doc mapping extraction, and _doc_mapping.json persistence
// Three discovery strategies: .store sampling, fast-field column enumeration, persisted doc_mapping files

use std::collections::HashMap;
use std::path::Path;
use anyhow::{anyhow, Context, Result};
use crate::debug_println;

/// Filename for the persisted doc_mapping JSON, embedded in split bundles.
/// Follows the `_parquet_manifest.json` convention (underscore prefix avoids
/// collision with tantivy's own files).
pub const DOC_MAPPING_FILENAME: &str = "_doc_mapping.json";

/// Tantivy's internal separator between JSON path segments in columnar column names.
const JSON_PATH_SEP: char = '\x01';

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
/// `stored` is parameterized so callers control it per mode (companion: false, standard: true).
fn subfield_type_to_mapping(name: &str, type_str: &str, stored: bool) -> serde_json::Value {
    match type_str {
        "i64" => serde_json::json!({
            "name": name, "type": "i64",
            "stored": stored, "indexed": true, "fast": true
        }),
        "u64" => serde_json::json!({
            "name": name, "type": "u64",
            "stored": stored, "indexed": true, "fast": true
        }),
        "f64" => serde_json::json!({
            "name": name, "type": "f64",
            "stored": stored, "indexed": true, "fast": true
        }),
        "bool" => serde_json::json!({
            "name": name, "type": "bool",
            "stored": stored, "indexed": true, "fast": true
        }),
        "text" => serde_json::json!({
            "name": name, "type": "text",
            "stored": stored, "indexed": true, "fast": false
        }),
        "date" => serde_json::json!({
            "name": name, "type": "datetime",
            "stored": stored, "indexed": true, "fast": true
        }),
        "ip" => serde_json::json!({
            "name": name, "type": "ip",
            "stored": stored, "indexed": true, "fast": true
        }),
        _ => {
            debug_log!("  Unknown type '{}' for field '{}', defaulting to text", type_str, name);
            serde_json::json!({
                "name": name, "type": "text",
                "stored": stored, "indexed": true, "fast": false
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
    sample_size: usize,
    stored: bool,
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
        let mapping = subfield_type_to_mapping(subfield_name, subfield_type, stored);
        debug_log!("  Mapped sub-field: {} -> {}", subfield_name, subfield_type);
        field_mappings.push(mapping);
    }

    Ok(field_mappings)
}

/// Returns true if `new_type` should replace `existing_type` based on type widening rules.
/// Numeric hierarchy: f64 > i64 > u64. Bool widens to numerics and date.
/// Date widens to numerics only. Text (lowest priority) widens to anything.
/// IP is independent (no cross-type widening).
fn should_widen_type(existing_type: &str, new_type: &str) -> bool {
    if existing_type == new_type {
        return false;
    }
    match (existing_type, new_type) {
        ("text", _) => true,
        ("date", "f64") | ("date", "i64") | ("date", "u64") => true,
        ("bool", "f64") | ("bool", "i64") | ("bool", "u64") | ("bool", "date") => true,
        ("u64", "f64") | ("u64", "i64") => true,
        ("i64", "f64") => true,
        _ => false
    }
}

/// Insert a type into a sub-field map, widening if the key already exists.
fn insert_or_widen(map: &mut HashMap<String, String>, key: String, type_str: &str) {
    if let Some(existing) = map.get(&key) {
        if should_widen_type(existing, type_str) {
            map.insert(key, type_str.to_string());
        }
    } else {
        map.insert(key, type_str.to_string());
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

        insert_or_widen(discovered, full_key, field_type);
    }
}

/// Extract JSON sub-fields from a doc_mapping JSON string.
/// Parses `type: "object"` fields and extracts their `field_mappings` into
/// the `JsonSubfieldMap` format, enabling sub-field recovery from serialized doc_mappings.
pub fn extract_json_subfields_from_doc_mapping(doc_mapping_json: &str) -> JsonSubfieldMap {
    let mut result: JsonSubfieldMap = HashMap::new();

    let parsed: serde_json::Value = match serde_json::from_str(doc_mapping_json) {
        Ok(v) => v,
        Err(e) => {
            debug_log!("⚠️ Failed to parse doc_mapping JSON ({}): {} bytes", e, doc_mapping_json.len());
            return result;
        }
    };

    let field_mappings = match &parsed {
        serde_json::Value::Array(arr) => arr,
        _ => {
            debug_log!("⚠️ doc_mapping JSON is not an array, got: {}", parsed.to_string().chars().take(100).collect::<String>());
            return result;
        }
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
                let is_stored = field_entry.is_stored();
                let discovered_subfields = if let Some(subfield_map) = json_subfields {
                    // Companion / merge mode: use pre-collected sub-fields
                    if let Some(field_subfields) = subfield_map.get(field_name) {
                        field_subfields.iter()
                            .map(|(name, type_str)| subfield_type_to_mapping(name, type_str, is_stored))
                            .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    }
                } else {
                    // Standard mode: discover by sampling stored documents
                    debug_log!("🔍 Processing JSON field '{}' - discovering sub-fields from index", field_name);
                    discover_json_subfields(tantivy_index, field, field_name, 1000, is_stored)
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

    let json_prefixes: Vec<(String, String)> = json_field_names.iter()
        .map(|name| (name.clone(), format!("{}{}", name, JSON_PATH_SEP)))
        .collect();

    let segment_metas = tantivy_index.searchable_segment_metas()?;

    for segment_meta in &segment_metas {
        let fast_field_path = segment_meta.relative_path(SegmentComponent::FastFields);
        let file_slice = match tantivy_index.directory().open_read(&fast_field_path) {
            Ok(f) => f,
            Err(e) => {
                debug_log!("⚠️ Could not open fast field file {:?}: {}. Sub-fields from this segment will be missing.", fast_field_path, e);
                continue;
            }
        };

        let columnar_reader: ColumnarReader = match ColumnarReader::open(file_slice) {
            Ok(r) => r,
            Err(e) => {
                debug_log!("⚠️ Could not open columnar reader for {:?}: {}. Sub-fields from this segment will be missing.", fast_field_path, e);
                continue;
            }
        };

        let columns: Vec<(String, tantivy::columnar::DynamicColumnHandle)> = match columnar_reader.list_columns() {
            Ok(c) => c,
            Err(e) => {
                debug_log!("⚠️ Could not list columns for {:?}: {}. Sub-fields from this segment will be missing.", fast_field_path, e);
                continue;
            }
        };

        for (col_name, col_handle) in &columns {
            for (json_field, prefix) in &json_prefixes {
                if col_name.starts_with(prefix) {
                    let subpath = &col_name[prefix.len()..];
                    let dotted_path = subpath.replace(JSON_PATH_SEP, ".");

                    let type_str = match col_handle.column_type() {
                        ColumnType::I64 => "i64",
                        ColumnType::U64 => "u64",
                        ColumnType::F64 => "f64",
                        ColumnType::Bool => "bool",
                        ColumnType::DateTime => "date",
                        ColumnType::Str => "text",
                        ColumnType::Bytes => "text",
                        ColumnType::IpAddr => "ip",
                    };

                    let subfields = result.entry(json_field.clone()).or_default();
                    insert_or_widen(subfields, dotted_path, type_str);
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

/// Write a doc_mapping JSON string to a directory as `_doc_mapping.json`.
/// The file will be picked up by the filesystem scan in `create_quickwit_split`
/// and bundled into the split, following the same pattern as `_parquet_manifest.json`.
pub fn write_doc_mapping_to_dir(dir: &Path, doc_mapping_json: &str) -> Result<()> {
    let path = dir.join(DOC_MAPPING_FILENAME);
    std::fs::write(&path, doc_mapping_json.as_bytes())
        .with_context(|| format!("Failed to write {} to {:?}", DOC_MAPPING_FILENAME, path))?;
    debug_log!("📦 DOC_MAPPING: Wrote {} bytes to {:?}", doc_mapping_json.len(), path);
    Ok(())
}

/// Read `_doc_mapping.json` from an extracted split directory.
/// Returns `Ok(None)` if the file does not exist (e.g. legacy splits).
pub fn read_doc_mapping_from_dir(dir: &Path) -> Result<Option<String>> {
    let path = dir.join(DOC_MAPPING_FILENAME);
    match std::fs::read_to_string(&path) {
        Ok(content) => Ok(Some(content)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(anyhow!(e).context(format!("Failed to read {} from {:?}", DOC_MAPPING_FILENAME, path))),
    }
}

/// Union multiple `JsonSubfieldMap`s with type widening.
/// Used at merge time to combine sub-field metadata from input splits.
pub fn combine_json_subfield_maps(maps: &[JsonSubfieldMap]) -> JsonSubfieldMap {
    let mut combined: JsonSubfieldMap = HashMap::new();
    for map in maps {
        for (field_name, subfields) in map {
            let combined_subs = combined.entry(field_name.clone()).or_default();
            for (sub_name, sub_type) in subfields {
                insert_or_widen(combined_subs, sub_name.clone(), sub_type);
            }
        }
    }
    combined
}

/// Extract doc mapping from a Tantivy index schema.
/// Discovers JSON sub-fields by reading stored documents from the index.
/// Requires that the index was built with stored fields enabled (standard/FFI mode).
pub fn extract_doc_mapping_from_index(tantivy_index: &tantivy::Index) -> Result<String> {
    extract_doc_mapping_impl(tantivy_index, None)
}

/// Extract doc mapping from a Tantivy index schema using pre-collected JSON sub-field info.
/// Used when sub-fields are known in advance (companion mode with no stored fields,
/// or merge-time recovery from `_doc_mapping.json`).
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
            ("addr", "ip", "ip", true),
            ("weird", "unknown_type", "text", false),
        ];

        for (name, input_type, expected_type, expected_fast) in cases {
            // Test with stored=false (companion mode)
            let mapping = subfield_type_to_mapping(name, input_type, false);
            assert_eq!(mapping["name"], name, "name mismatch for input '{}'", input_type);
            assert_eq!(mapping["type"], expected_type, "type mismatch for input '{}'", input_type);
            assert_eq!(mapping["fast"], expected_fast, "fast mismatch for input '{}'", input_type);
            assert_eq!(mapping["stored"], false, "stored should be false for '{}'", input_type);
            assert_eq!(mapping["indexed"], true, "indexed should be true for '{}'", input_type);

            // Test with stored=true (standard mode)
            let mapping_stored = subfield_type_to_mapping(name, input_type, true);
            assert_eq!(mapping_stored["stored"], true, "stored should be true when requested for '{}'", input_type);
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

    #[test]
    fn test_should_widen_type_exhaustive() {
        // Widening cases (should return true)
        assert!(should_widen_type("text", "i64"), "text → i64");
        assert!(should_widen_type("text", "u64"), "text → u64");
        assert!(should_widen_type("text", "f64"), "text → f64");
        assert!(should_widen_type("text", "bool"), "text → bool");
        assert!(should_widen_type("text", "date"), "text → date");
        assert!(should_widen_type("u64", "i64"), "u64 → i64");
        assert!(should_widen_type("u64", "f64"), "u64 → f64");
        assert!(should_widen_type("i64", "f64"), "i64 → f64");
        assert!(should_widen_type("bool", "i64"), "bool → i64");
        assert!(should_widen_type("bool", "f64"), "bool → f64");
        assert!(should_widen_type("bool", "date"), "bool → date");
        assert!(should_widen_type("date", "f64"), "date → f64");

        // Non-widening cases (should return false)
        assert!(!should_widen_type("f64", "i64"), "f64 → i64 should not widen");
        assert!(!should_widen_type("f64", "u64"), "f64 → u64 should not widen");
        assert!(!should_widen_type("i64", "u64"), "i64 → u64 should not narrow");
        assert!(!should_widen_type("i64", "bool"), "i64 → bool should not narrow");
        assert!(!should_widen_type("date", "bool"), "date → bool unrelated");
        assert!(!should_widen_type("u64", "date"), "u64 → date unrelated");

        // Same-type pairs (should never widen)
        for t in &["text", "i64", "u64", "f64", "bool", "date", "ip"] {
            assert!(!should_widen_type(t, t), "{} → {} same type should not widen", t, t);
        }

        // IP type widening
        assert!(should_widen_type("text", "ip"), "text → ip should widen");
        assert!(!should_widen_type("ip", "text"), "ip → text should not narrow");
    }

    #[test]
    fn test_extract_json_subfields_from_doc_mapping_dotted_paths() {
        let doc_mapping = serde_json::json!([
            {
                "name": "data",
                "type": "object",
                "field_mappings": [
                    {"name": "addr.city", "type": "text"},
                    {"name": "addr.zip", "type": "u64"},
                    {"name": "user", "type": "text"}
                ]
            }
        ]);

        let json_str = serde_json::to_string(&doc_mapping).unwrap();
        let result = extract_json_subfields_from_doc_mapping(&json_str);

        let data_subs = result.get("data").expect("Should have 'data' entry");
        assert_eq!(data_subs.len(), 3);
        assert_eq!(data_subs.get("addr.city").unwrap(), "text");
        assert_eq!(data_subs.get("addr.zip").unwrap(), "u64");
        assert_eq!(data_subs.get("user").unwrap(), "text");
    }

    #[test]
    fn test_combine_json_subfield_maps() {
        // Split A: data has {user: text, score: i64}
        let mut map_a: JsonSubfieldMap = HashMap::new();
        let mut subs_a = HashMap::new();
        subs_a.insert("user".to_string(), "text".to_string());
        subs_a.insert("score".to_string(), "i64".to_string());
        map_a.insert("data".to_string(), subs_a);

        // Split B: data has {score: f64, active: bool}, meta has {version: u64}
        let mut map_b: JsonSubfieldMap = HashMap::new();
        let mut subs_b = HashMap::new();
        subs_b.insert("score".to_string(), "f64".to_string());
        subs_b.insert("active".to_string(), "bool".to_string());
        map_b.insert("data".to_string(), subs_b);
        let mut meta_b = HashMap::new();
        meta_b.insert("version".to_string(), "u64".to_string());
        map_b.insert("meta".to_string(), meta_b);

        let combined = combine_json_subfield_maps(&[map_a, map_b]);

        // data should have union of all sub-fields
        let data_subs = combined.get("data").expect("Should have 'data'");
        assert_eq!(data_subs.get("user").unwrap(), "text", "user preserved from A");
        assert_eq!(data_subs.get("score").unwrap(), "f64", "score widened from i64 to f64");
        assert_eq!(data_subs.get("active").unwrap(), "bool", "active from B");

        // meta should be preserved from B
        let meta_subs = combined.get("meta").expect("Should have 'meta'");
        assert_eq!(meta_subs.get("version").unwrap(), "u64");
    }

    #[test]
    fn test_write_and_read_doc_mapping_roundtrip() {
        let temp_dir = tempfile::tempdir().unwrap();
        let doc_mapping = r#"[{"name":"title","type":"text"}]"#;

        write_doc_mapping_to_dir(temp_dir.path(), doc_mapping)
            .expect("write should succeed");

        let read_back = read_doc_mapping_from_dir(temp_dir.path())
            .expect("read should succeed")
            .expect("file should exist");

        assert_eq!(read_back, doc_mapping);
    }

    #[test]
    fn test_read_doc_mapping_missing_returns_none() {
        let temp_dir = tempfile::tempdir().unwrap();
        let result = read_doc_mapping_from_dir(temp_dir.path())
            .expect("read should succeed even if file missing");
        assert!(result.is_none(), "Should return None for missing file");
    }

    #[test]
    fn test_discover_json_subfields_from_fast_fields_unit() {
        use tantivy::schema::*;
        use std::collections::BTreeMap;

        // Build a schema with a fast JSON field (not stored)
        let mut builder = SchemaBuilder::new();
        builder.add_json_field(
            "data",
            JsonObjectOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("raw")
                        .set_index_option(IndexRecordOption::Basic)
                )
                .set_fast(None),
        );
        let schema = builder.build();

        let index = tantivy::Index::create_in_ram(schema);
        let mut writer = index.writer_with_num_threads(1, 15_000_000).unwrap();

        let json_field = index.schema().get_field("data").unwrap();
        let mut map = BTreeMap::new();
        map.insert("user".to_string(), OwnedValue::Str("alice".to_string()));
        map.insert("score".to_string(), OwnedValue::U64(42));
        map.insert("active".to_string(), OwnedValue::Bool(true));
        let mut doc = TantivyDocument::new();
        doc.add_object(json_field, map);
        writer.add_document(doc).unwrap();
        writer.commit().unwrap();

        let result = discover_json_subfields_from_fast_fields(&index)
            .expect("fast field discovery should succeed");

        let data_subs = result.get("data").expect("Should discover 'data' field");
        assert_eq!(data_subs.get("user").unwrap(), "text");
        // tantivy stores JSON integers as i64 in columnar fast fields regardless of the
        // OwnedValue variant (U64 vs I64) used during indexing.
        assert_eq!(data_subs.get("score").unwrap(), "i64");
        assert_eq!(data_subs.get("active").unwrap(), "bool");
    }

    #[test]
    fn test_discover_json_subfields_cross_segment_widening() {
        use tantivy::schema::*;
        use std::collections::BTreeMap;

        let mut builder = SchemaBuilder::new();
        builder.add_json_field(
            "data",
            JsonObjectOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("raw")
                        .set_index_option(IndexRecordOption::Basic)
                )
                .set_fast(None),
        );
        let schema = builder.build();

        let index = tantivy::Index::create_in_ram(schema);
        let json_field = index.schema().get_field("data").unwrap();

        // Segment 1: score as u64
        {
            let mut writer = index.writer_with_num_threads(1, 15_000_000).unwrap();
            let mut map = BTreeMap::new();
            map.insert("score".to_string(), OwnedValue::U64(42));
            let mut doc = TantivyDocument::new();
            doc.add_object(json_field, map);
            writer.add_document(doc).unwrap();
            writer.commit().unwrap();
        }

        // Segment 2: score as f64
        {
            let mut writer = index.writer_with_num_threads(1, 15_000_000).unwrap();
            let mut map = BTreeMap::new();
            map.insert("score".to_string(), OwnedValue::F64(3.14));
            let mut doc = TantivyDocument::new();
            doc.add_object(json_field, map);
            writer.add_document(doc).unwrap();
            writer.commit().unwrap();
        }

        let result = discover_json_subfields_from_fast_fields(&index)
            .expect("fast field discovery should succeed");

        let data_subs = result.get("data").expect("Should discover 'data' field");
        assert_eq!(data_subs.get("score").unwrap(), "f64",
            "u64 in segment 1 + f64 in segment 2 should widen to f64");
    }

    #[test]
    fn test_insert_or_widen_direct() {
        let mut map = HashMap::new();

        // Insert into empty map
        insert_or_widen(&mut map, "score".to_string(), "i64");
        assert_eq!(map.get("score").unwrap(), "i64");

        // Widen i64 → f64
        insert_or_widen(&mut map, "score".to_string(), "f64");
        assert_eq!(map.get("score").unwrap(), "f64");

        // No narrowing f64 → i64
        insert_or_widen(&mut map, "score".to_string(), "i64");
        assert_eq!(map.get("score").unwrap(), "f64");

        // Same type is a no-op
        insert_or_widen(&mut map, "score".to_string(), "f64");
        assert_eq!(map.get("score").unwrap(), "f64");

        // Independent key
        insert_or_widen(&mut map, "name".to_string(), "text");
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("name").unwrap(), "text");
    }

    #[test]
    fn test_combine_json_subfield_maps_empty_and_single() {
        // Empty input
        let combined = combine_json_subfield_maps(&[]);
        assert!(combined.is_empty());

        // Single map (identity)
        let mut map = JsonSubfieldMap::new();
        let mut subs = HashMap::new();
        subs.insert("user".to_string(), "text".to_string());
        map.insert("data".to_string(), subs);

        let combined = combine_json_subfield_maps(&[map.clone()]);
        assert_eq!(combined.get("data").unwrap().get("user").unwrap(), "text");
        assert_eq!(combined.len(), 1);
    }

    #[test]
    fn test_discover_json_subfields_from_fast_fields_nested_dotted_paths() {
        use tantivy::schema::*;
        use std::collections::BTreeMap;

        let mut builder = SchemaBuilder::new();
        builder.add_json_field(
            "data",
            JsonObjectOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("raw")
                        .set_index_option(IndexRecordOption::Basic)
                )
                .set_fast(None),
        );
        let schema = builder.build();

        let index = tantivy::Index::create_in_ram(schema);
        let json_field = index.schema().get_field("data").unwrap();
        let mut writer = index.writer_with_num_threads(1, 15_000_000).unwrap();

        // Nested object: data.addr.city and data.addr.zip
        let mut map = BTreeMap::new();
        let mut addr = BTreeMap::new();
        addr.insert("city".to_string(), OwnedValue::Str("NYC".to_string()));
        addr.insert("zip".to_string(), OwnedValue::I64(10001));
        map.insert("addr".to_string(), OwnedValue::Object(addr.into_iter().collect()));
        map.insert("name".to_string(), OwnedValue::Str("alice".to_string()));
        let mut doc = TantivyDocument::new();
        doc.add_object(json_field, map);
        writer.add_document(doc).unwrap();
        writer.commit().unwrap();

        let result = discover_json_subfields_from_fast_fields(&index)
            .expect("fast field discovery should succeed");

        let data_subs = result.get("data").expect("Should discover 'data' field");
        assert_eq!(data_subs.get("name").unwrap(), "text");
        assert_eq!(data_subs.get("addr.city").unwrap(), "text",
            "Nested path should use dot notation");
        assert_eq!(data_subs.get("addr.zip").unwrap(), "i64",
            "Nested numeric path should be discovered with correct type");
    }
}
