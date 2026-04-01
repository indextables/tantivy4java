// txlog/schema_dedup.rs - Schema deduplication via SHA-256 hashing
//
// Replicates Scala's SchemaDeduplication: replaces docMappingJson with a
// 16-char Base64 reference, storing the full schema in MetadataAction.configuration.

use std::collections::HashMap;
use sha2::{Sha256, Digest};
use base64::Engine;

use super::actions::{Action, AddAction, FileEntry, MetadataAction};

const DOC_MAPPING_SCHEMA_PREFIX: &str = "docMappingSchema.";

/// Compute a 16-char Base64 hash of a canonical JSON schema string.
fn compute_schema_hash(json: &str) -> String {
    let canonical = canonical_json(json);
    let hash = Sha256::digest(canonical.as_bytes());
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&hash[..12])
}

/// Normalize JSON to canonical form: sorted keys, no extraneous whitespace.
/// Explicitly sorts keys via BTreeMap to guarantee deterministic output
/// regardless of whether serde_json's `preserve_order` feature is enabled.
fn canonical_json(json: &str) -> String {
    match serde_json::from_str::<serde_json::Value>(json) {
        Ok(val) => {
            let sorted = sort_json_value(&val);
            serde_json::to_string(&sorted).unwrap_or_else(|_| json.to_string())
        }
        Err(_) => json.to_string(),
    }
}

/// Recursively sort all object keys in a JSON value.
///
/// Matches Scala's `sortJsonNode()`: sorts object keys alphabetically AND
/// sorts arrays of named objects by the "name" field. This handles the case
/// where `field_mappings` arrays have fields in different orders but are
/// semantically identical.
fn sort_json_value(val: &serde_json::Value) -> serde_json::Value {
    match val {
        serde_json::Value::Object(map) => {
            let mut sorted: std::collections::BTreeMap<String, serde_json::Value> = std::collections::BTreeMap::new();
            for (k, v) in map {
                sorted.insert(k.clone(), sort_json_value(v));
            }
            serde_json::Value::Object(sorted.into_iter().collect())
        }
        serde_json::Value::Array(arr) => {
            let mut processed: Vec<serde_json::Value> = arr.iter().map(sort_json_value).collect();

            // Sort arrays of named objects by "name" field (matches Scala's isNamedObjectArray + sortBy)
            if is_named_object_array(&processed) {
                processed.sort_by(|a, b| {
                    let a_name = a.get("name").and_then(|v| v.as_str()).unwrap_or("");
                    let b_name = b.get("name").and_then(|v| v.as_str()).unwrap_or("");
                    a_name.cmp(b_name)
                });
            }

            serde_json::Value::Array(processed)
        }
        other => other.clone(),
    }
}

/// Check if all array elements are objects with a "name" field.
/// Matches Scala's `isNamedObjectArray`.
fn is_named_object_array(arr: &[serde_json::Value]) -> bool {
    !arr.is_empty() && arr.iter().all(|v| v.is_object() && v.get("name").is_some())
}

/// On write: replace docMappingJson with docMappingRef in AddActions.
///
/// Returns new schema map entries to merge into MetadataAction.configuration.
/// The caller is responsible for writing these into the metadata.
pub fn deduplicate_schemas(
    actions: &mut Vec<Action>,
    existing_schemas: &HashMap<String, String>,
) -> HashMap<String, String> {
    let mut new_schemas: HashMap<String, String> = HashMap::new();

    for action in actions.iter_mut() {
        if let Action::Add(add) = action {
            if let Some(ref json) = add.doc_mapping_json {
                let hash = compute_schema_hash(json);

                // Check if schema already exists
                let key = format!("{}{}", DOC_MAPPING_SCHEMA_PREFIX, hash);
                if !existing_schemas.contains_key(&key) && !new_schemas.contains_key(&key) {
                    new_schemas.insert(key, json.clone());
                }

                // Replace json with ref
                add.doc_mapping_ref = Some(hash);
                add.doc_mapping_json = None;
            }
        }
    }

    new_schemas
}

/// On read: restore docMappingJson from docMappingRef using metadata config.
///
/// The schema registry may store keys either with prefix (`docMappingSchema.HASH`)
/// or without prefix (`HASH`) depending on whether it came from MetadataAction.configuration
/// (prefixed) or StateManifest.schemaRegistry (may be unprefixed in Scala-written manifests).
pub fn restore_schemas(
    entries: &mut [FileEntry],
    metadata_config: &HashMap<String, String>,
) {
    // Build lookup map: hash → schema JSON
    // Support both prefixed and unprefixed keys
    let mut schema_map: HashMap<&str, &str> = HashMap::new();
    for (k, v) in metadata_config {
        if let Some(hash) = k.strip_prefix(DOC_MAPPING_SCHEMA_PREFIX) {
            schema_map.insert(hash, v.as_str());
        } else {
            // Unprefixed key — treat as raw hash
            schema_map.insert(k.as_str(), v.as_str());
        }
    }

    for entry in entries.iter_mut() {
        if let Some(ref doc_ref) = entry.add.doc_mapping_ref {
            if entry.add.doc_mapping_json.is_none() {
                if let Some(json) = schema_map.get(doc_ref.as_str()) {
                    entry.add.doc_mapping_json = Some(json.to_string());
                }
            }
        }
    }
}

/// Restore schemas on AddActions directly (for post-checkpoint version files).
pub fn restore_schemas_on_adds(
    adds: &mut [AddAction],
    metadata_config: &HashMap<String, String>,
) {
    let mut schema_map: HashMap<&str, &str> = HashMap::new();
    for (k, v) in metadata_config {
        if let Some(hash) = k.strip_prefix(DOC_MAPPING_SCHEMA_PREFIX) {
            schema_map.insert(hash, v.as_str());
        } else {
            schema_map.insert(k.as_str(), v.as_str());
        }
    }

    for add in adds.iter_mut() {
        if let Some(ref doc_ref) = add.doc_mapping_ref {
            if add.doc_mapping_json.is_none() {
                if let Some(json) = schema_map.get(doc_ref.as_str()) {
                    add.doc_mapping_json = Some(json.to_string());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_add(path: &str, doc_mapping: Option<&str>) -> Action {
        Action::Add(AddAction {
            path: path.to_string(),
            partition_values: HashMap::new(),
            size: 100,
            modification_time: 1700000000000,
            data_change: true,
            stats: None,
            min_values: None,
            max_values: None,
            num_records: None,
            footer_start_offset: None,
            footer_end_offset: None,
            has_footer_offsets: None, delete_opstamp: None,
            split_tags: None,
            num_merge_ops: None,
            doc_mapping_json: doc_mapping.map(|s| s.to_string()),
            doc_mapping_ref: None,
            uncompressed_size_bytes: None,
            time_range_start: None,
            time_range_end: None,
            companion_source_files: None,
            companion_delta_version: None,
            companion_fast_field_mode: None,
        })
    }

    #[test]
    fn test_dedup_replaces_json_with_ref() {
        let schema = r#"{"fields":[{"name":"title","type":"text"}]}"#;
        let mut actions = vec![make_add("split-1.split", Some(schema))];
        let new = deduplicate_schemas(&mut actions, &HashMap::new());

        // Should have one new schema entry
        assert_eq!(new.len(), 1);

        // AddAction should have ref, not json
        match &actions[0] {
            Action::Add(a) => {
                assert!(a.doc_mapping_json.is_none());
                assert!(a.doc_mapping_ref.is_some());
            }
            other => panic!("expected Add, got {:?}", other),
        }
    }

    #[test]
    fn test_dedup_same_schema_deduplicates() {
        let schema = r#"{"fields":[{"name":"title","type":"text"}]}"#;
        let mut actions = vec![
            make_add("split-1.split", Some(schema)),
            make_add("split-2.split", Some(schema)),
        ];
        let new = deduplicate_schemas(&mut actions, &HashMap::new());

        // Only one schema entry even though two adds
        assert_eq!(new.len(), 1);

        // Both should have same ref
        let ref1 = match &actions[0] {
            Action::Add(a) => a.doc_mapping_ref.clone().unwrap(),
            other => panic!("unexpected action: {:?}", other),
        };
        let ref2 = match &actions[1] {
            Action::Add(a) => a.doc_mapping_ref.clone().unwrap(),
            other => panic!("unexpected action: {:?}", other),
        };
        assert_eq!(ref1, ref2);
    }

    #[test]
    fn test_restore_schemas() {
        let schema = r#"{"fields":[{"name":"title","type":"text"}]}"#;
        let hash = compute_schema_hash(schema);
        let key = format!("{}{}", DOC_MAPPING_SCHEMA_PREFIX, hash);

        let mut config = HashMap::new();
        config.insert(key, schema.to_string());

        let mut entries = vec![FileEntry {
            add: AddAction {
                path: "split-1.split".to_string(),
                partition_values: HashMap::new(),
                size: 100,
                modification_time: 0,
                data_change: true,
                stats: None,
                min_values: None,
                max_values: None,
                num_records: None,
                footer_start_offset: None,
                footer_end_offset: None,
                has_footer_offsets: None, delete_opstamp: None,
                split_tags: None,
                num_merge_ops: None,
                doc_mapping_json: None,
                doc_mapping_ref: Some(hash),
                uncompressed_size_bytes: None,
                time_range_start: None,
                time_range_end: None,
                companion_source_files: None,
                companion_delta_version: None,
                companion_fast_field_mode: None,
            },
            added_at_version: 1,
            added_at_timestamp: 0,
        }];

        restore_schemas(&mut entries, &config);
        assert!(entries[0].add.doc_mapping_json.is_some());
    }

    #[test]
    fn test_hash_deterministic() {
        let s1 = r#"{"a":1,"b":2}"#;
        let s2 = r#"{"b":2,"a":1}"#;
        // serde_json::Value sorts keys, so these should produce the same hash
        assert_eq!(compute_schema_hash(s1), compute_schema_hash(s2));
    }

    #[test]
    fn test_hash_length() {
        let hash = compute_schema_hash(r#"{"test":"value"}"#);
        assert_eq!(hash.len(), 16);
    }

    // ========================================================================
    // Named object array sorting tests (matches Scala's sortJsonNode behavior)
    // ========================================================================

    #[test]
    fn test_named_array_sorting_field_mappings() {
        // The most common case: field_mappings array with fields in different orders
        let s1 = r#"{"field_mappings":[{"name":"id","type":"i64"},{"name":"content","type":"text"}]}"#;
        let s2 = r#"{"field_mappings":[{"name":"content","type":"text"},{"name":"id","type":"i64"}]}"#;
        assert_eq!(compute_schema_hash(s1), compute_schema_hash(s2),
            "field_mappings with different element order should produce same hash");
    }

    #[test]
    fn test_named_array_sorting_three_fields() {
        let s1 = r#"{"fields":[{"name":"a"},{"name":"b"},{"name":"c"}]}"#;
        let s2 = r#"{"fields":[{"name":"c"},{"name":"b"},{"name":"a"}]}"#;
        let s3 = r#"{"fields":[{"name":"b"},{"name":"a"},{"name":"c"}]}"#;
        let h1 = compute_schema_hash(s1);
        let h2 = compute_schema_hash(s2);
        let h3 = compute_schema_hash(s3);
        assert_eq!(h1, h2, "any permutation of named array should hash the same");
        assert_eq!(h2, h3, "any permutation of named array should hash the same");
    }

    #[test]
    fn test_named_array_sorting_complex_fields() {
        // Real-world schema with multiple properties per field
        let s1 = r#"{"field_mappings":[{"name":"id","type":"i64","fast":true},{"name":"content","type":"text","tokenizer":"default"},{"name":"score","type":"f64","fast":true}]}"#;
        let s2 = r#"{"field_mappings":[{"name":"score","type":"f64","fast":true},{"name":"id","type":"i64","fast":true},{"name":"content","type":"text","tokenizer":"default"}]}"#;
        assert_eq!(compute_schema_hash(s1), compute_schema_hash(s2),
            "complex field_mappings with different order should produce same hash");
    }

    #[test]
    fn test_named_array_sorting_nested_structs() {
        // Nested structures: field_mappings containing sub-fields
        let s1 = r#"{"field_mappings":[{"name":"address","type":"object","field_mappings":[{"name":"city","type":"text"},{"name":"zip","type":"text"}]},{"name":"id","type":"i64"}]}"#;
        let s2 = r#"{"field_mappings":[{"name":"id","type":"i64"},{"name":"address","type":"object","field_mappings":[{"name":"zip","type":"text"},{"name":"city","type":"text"}]}]}"#;
        assert_eq!(compute_schema_hash(s1), compute_schema_hash(s2),
            "nested field_mappings with different order at both levels should produce same hash");
    }

    #[test]
    fn test_named_array_sorting_with_key_reorder() {
        // Both array element order AND key order differ
        let s1 = r#"{"field_mappings":[{"type":"i64","name":"id","fast":true},{"type":"text","name":"content"}]}"#;
        let s2 = r#"{"field_mappings":[{"name":"content","type":"text"},{"fast":true,"name":"id","type":"i64"}]}"#;
        assert_eq!(compute_schema_hash(s1), compute_schema_hash(s2),
            "different key order + different array order should still match");
    }

    #[test]
    fn test_non_named_array_preserves_order() {
        // Arrays without "name" fields should NOT be reordered
        let s1 = r#"{"values":[1,2,3]}"#;
        let s2 = r#"{"values":[3,2,1]}"#;
        assert_ne!(compute_schema_hash(s1), compute_schema_hash(s2),
            "non-named arrays should preserve order (different hashes)");
    }

    #[test]
    fn test_mixed_array_preserves_order() {
        // Array where not all elements have "name" — should NOT sort
        let s1 = r#"[{"name":"a"},{"value":1}]"#;
        let s2 = r#"[{"value":1},{"name":"a"}]"#;
        assert_ne!(compute_schema_hash(s1), compute_schema_hash(s2),
            "mixed arrays (not all have name) should preserve order");
    }

    #[test]
    fn test_canonical_json_sorts_named_arrays() {
        let json = r#"{"fields":[{"name":"z"},{"name":"a"},{"name":"m"}]}"#;
        let canonical = canonical_json(json);
        let parsed: serde_json::Value = serde_json::from_str(&canonical).unwrap();
        let fields = parsed["fields"].as_array().unwrap();
        assert_eq!(fields[0]["name"], "a");
        assert_eq!(fields[1]["name"], "m");
        assert_eq!(fields[2]["name"], "z");
    }

    #[test]
    fn test_dedup_different_field_order_same_hash() {
        // Two splits with same schema but different field_mappings order
        let schema1 = r#"{"field_mappings":[{"name":"title","type":"text"},{"name":"id","type":"i64"}]}"#;
        let schema2 = r#"{"field_mappings":[{"name":"id","type":"i64"},{"name":"title","type":"text"}]}"#;

        let mut actions = vec![
            make_add("split-1.split", Some(schema1)),
            make_add("split-2.split", Some(schema2)),
        ];
        let new = deduplicate_schemas(&mut actions, &HashMap::new());

        // Should produce only ONE unique schema (same hash)
        assert_eq!(new.len(), 1, "different field order should deduplicate to one schema");

        let ref1 = match &actions[0] {
            Action::Add(a) => a.doc_mapping_ref.clone().unwrap(),
            _ => panic!(),
        };
        let ref2 = match &actions[1] {
            Action::Add(a) => a.doc_mapping_ref.clone().unwrap(),
            _ => panic!(),
        };
        assert_eq!(ref1, ref2, "both splits should reference the same schema hash");
    }
}
