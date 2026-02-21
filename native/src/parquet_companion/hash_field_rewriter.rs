// hash_field_rewriter.rs - Phase 2: Rewrite aggregation JSON and query AST to use hash fields
//
// For HYBRID mode parquet companion splits, string fast fields are expensive to transcode
// (requires reading all parquet rows). This rewriter substitutes `_phash_<field>` U64 fields
// for string fields in aggregation operations that don't need the actual string values:
//
//   value_count  → redirect field: counts non-null values, hash absent = null
//   cardinality  → redirect field: distinct hash count ≈ distinct string count
//   terms        → redirect field; bucket keys resolved in Phase 3 touchup
//
// Query AST rewriting:
//   FieldPresence (exists query) → redirect to `_phash_<field>`: non-null strings have a
//   hash value stored; null strings have no hash value. FieldPresence on the hash field
//   is equivalent to FieldPresence on the original string field.
//
// Operations that cannot be redirected:
//   terms with min_doc_count: 0  → needs full dictionary (not possible with hashes)
//   range aggregation            → hash ordering ≠ string ordering
//   stats/extended_stats         → meaningless min/max/sum for hash values

use std::collections::{HashMap, HashSet};
use serde_json::{json, Value};

use crate::parquet_companion::indexing::hash_string_value;

/// Info needed to resolve hash bucket keys back to strings after aggregation (Phase 3).
#[derive(Debug, Clone)]
pub struct HashFieldTouchupInfo {
    /// Top-level aggregation name (key in the aggregation JSON object).
    pub agg_name: String,
    /// The hash field name used in the rewritten query (e.g. "_phash_status").
    pub hash_field_name: String,
    /// The original tantivy field name (e.g. "status").
    pub original_field_name: String,
    /// Whether buckets need re-sorting by key (order: {"_key": ...} was specified).
    pub needs_resort: bool,
    /// The original `missing` string value, if `missing` was present.
    pub missing_value: Option<String>,
    /// Original include filter string values (before hashing).
    /// Tantivy does not filter U64 fields by numeric include arrays, so we post-filter here.
    pub include_strings: Option<std::collections::HashSet<String>>,
    /// Original exclude filter string values (before hashing).
    pub exclude_strings: Option<std::collections::HashSet<String>>,
}

/// Output of the aggregation rewriter.
pub struct HashRewriteOutput {
    /// The rewritten aggregation JSON (with `_phash_*` field substitutions).
    pub rewritten_json: String,
    /// String fields that were NOT fully redirected to hash fields and still need
    /// parquet transcoding. Used to prune `columns_to_transcode`.
    pub fields_still_needing_transcoding: HashSet<String>,
    /// Touchup infos for terms aggs whose field was redirected to a hash field.
    /// Used in Phase 3 to resolve bucket hash keys back to original strings.
    pub touchup_infos: Vec<HashFieldTouchupInfo>,
}

/// Rewrite aggregation JSON to use hash fields for hash-compatible operations.
///
/// Returns [`HashRewriteOutput`] with the rewritten JSON, the set of fields that
/// still require transcoding, and the touchup infos for Phase 3.
///
/// If `string_hash_fields` is empty or the JSON has no redirectable aggregations,
/// the JSON is returned unmodified.
pub fn rewrite_aggs_for_hash_fields(
    agg_json: &str,
    string_hash_fields: &HashMap<String, String>,
) -> anyhow::Result<HashRewriteOutput> {
    if string_hash_fields.is_empty() {
        return Ok(HashRewriteOutput {
            rewritten_json: agg_json.to_string(),
            fields_still_needing_transcoding: HashSet::new(),
            touchup_infos: Vec::new(),
        });
    }

    let mut agg_value: Value = serde_json::from_str(agg_json)
        .map_err(|e| anyhow::anyhow!("Failed to parse aggregation JSON: {}", e))?;

    let mut touchup_infos = Vec::new();
    let mut fields_still_needed = HashSet::new();

    if let Some(obj) = agg_value.as_object_mut() {
        // The top-level object is { agg_name: agg_def, ... }
        let keys: Vec<String> = obj.keys().cloned().collect();
        for agg_name in keys {
            if let Some(agg_def) = obj.get_mut(&agg_name) {
                rewrite_agg_def(
                    &agg_name,
                    agg_def,
                    string_hash_fields,
                    &mut touchup_infos,
                    &mut fields_still_needed,
                );
            }
        }
    }

    Ok(HashRewriteOutput {
        rewritten_json: serde_json::to_string(&agg_value)?,
        fields_still_needing_transcoding: fields_still_needed,
        touchup_infos,
    })
}

/// Rewrite a single aggregation definition in-place.
///
/// `agg_def` is the value associated with an aggregation name in the JSON object.
/// It has the form: `{ "terms": {...}, "aggs": { ... } }` or similar.
fn rewrite_agg_def(
    agg_name: &str,
    agg_def: &mut Value,
    string_hash_fields: &HashMap<String, String>,
    touchup_infos: &mut Vec<HashFieldTouchupInfo>,
    fields_still_needed: &mut HashSet<String>,
) {
    let obj = match agg_def.as_object_mut() {
        Some(o) => o,
        None => return,
    };

    // --- terms ---
    if let Some(terms_val) = obj.get_mut("terms") {
        if let Some(terms_map) = terms_val.as_object_mut() {
            let field_name = terms_map
                .get("field")
                .and_then(|f| f.as_str())
                .map(|s| s.to_string());

            if let Some(field_name) = field_name {
                if let Some(hash_field_name) = string_hash_fields.get(&field_name) {
                    // min_doc_count: 0 requires full dictionary — cannot redirect
                    let min_doc_count = terms_map
                        .get("min_doc_count")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(1);

                    if min_doc_count == 0 {
                        // Fall back to transcoding for this field
                        fields_still_needed.insert(field_name);
                    } else {
                        // Check for order: _key (needs re-sort after touchup)
                        let needs_resort = terms_map
                            .get("order")
                            .and_then(|o| o.as_object())
                            .map(|om| om.contains_key("_key"))
                            .unwrap_or(false);

                        // Get the original `missing` value before we overwrite it
                        let missing_value = terms_map
                            .get("missing")
                            .and_then(|m| m.as_str())
                            .map(|s| s.to_string());

                        // Capture include filter strings for post-processing.
                        // Tantivy ignores numeric include arrays for U64 fields, so we
                        // record the original strings and apply the filter ourselves in
                        // Phase 3 / result creation. Remove the include from the rewritten
                        // JSON so Tantivy returns all buckets for the hash field.
                        let include_strings: Option<std::collections::HashSet<String>> =
                            if let Some(include_val) = terms_map.get("include") {
                                if let Some(arr) = include_val.as_array() {
                                    let strings: std::collections::HashSet<String> = arr
                                        .iter()
                                        .filter_map(|v| v.as_str())
                                        .map(|s| s.to_string())
                                        .collect();
                                    if strings.is_empty() { None } else { Some(strings) }
                                } else {
                                    None
                                }
                            } else {
                                None
                            };
                        if include_strings.is_some() {
                            // Remove the include from the rewritten JSON; we post-filter
                            terms_map.remove("include");
                        }

                        // Capture exclude filter strings for post-processing.
                        let exclude_strings: Option<std::collections::HashSet<String>> =
                            if let Some(exclude_val) = terms_map.get("exclude") {
                                if let Some(arr) = exclude_val.as_array() {
                                    let strings: std::collections::HashSet<String> = arr
                                        .iter()
                                        .filter_map(|v| v.as_str())
                                        .map(|s| s.to_string())
                                        .collect();
                                    if strings.is_empty() { None } else { Some(strings) }
                                } else {
                                    None
                                }
                            } else {
                                None
                            };
                        if exclude_strings.is_some() {
                            terms_map.remove("exclude");
                        }

                        // Map missing string → 0 (null sentinel for hash field)
                        if missing_value.is_some() {
                            terms_map.insert("missing".to_string(), json!(0u64));
                        }

                        // Replace the field name with the hash field name
                        terms_map.insert("field".to_string(), json!(hash_field_name));

                        // Record for Phase 3 touchup
                        touchup_infos.push(HashFieldTouchupInfo {
                            agg_name: agg_name.to_string(),
                            hash_field_name: hash_field_name.clone(),
                            original_field_name: field_name,
                            needs_resort,
                            missing_value,
                            include_strings,
                            exclude_strings,
                        });
                    }
                } else {
                    // Not a string field → leave as-is (needs transcoding if it was a string)
                    fields_still_needed.insert(field_name);
                }
            }
        }
    }

    // --- value_count ---
    if let Some(vc_val) = obj.get_mut("value_count") {
        if let Some(vc_map) = vc_val.as_object_mut() {
            if let Some(field_name) = vc_map
                .get("field")
                .and_then(|f| f.as_str())
                .map(|s| s.to_string())
            {
                if let Some(hash_field_name) = string_hash_fields.get(&field_name) {
                    vc_map.insert("field".to_string(), json!(hash_field_name));
                    // value_count returns a number — no touchup needed
                } else {
                    fields_still_needed.insert(field_name);
                }
            }
        }
    }

    // --- cardinality ---
    if let Some(card_val) = obj.get_mut("cardinality") {
        if let Some(card_map) = card_val.as_object_mut() {
            if let Some(field_name) = card_map
                .get("field")
                .and_then(|f| f.as_str())
                .map(|s| s.to_string())
            {
                if let Some(hash_field_name) = string_hash_fields.get(&field_name) {
                    card_map.insert("field".to_string(), json!(hash_field_name));
                    // cardinality returns a number — no touchup needed
                } else {
                    fields_still_needed.insert(field_name);
                }
            }
        }
    }

    // --- Recurse into sub-aggregations ("aggs" key) ---
    // We collect keys first to avoid borrow conflicts with the mutable obj reference.
    if obj.contains_key("aggs") {
        // Extract sub-agg names first to avoid double-borrow
        let sub_names: Vec<String> = obj
            .get("aggs")
            .and_then(|a| a.as_object())
            .map(|o| o.keys().cloned().collect())
            .unwrap_or_default();

        for sub_name in sub_names {
            // We need to get a mutable ref to the sub-agg def. Since obj is already mutably
            // borrowed, we access it directly via chained gets.
            if let Some(sub_aggs_obj) = obj.get_mut("aggs").and_then(|a| a.as_object_mut()) {
                if let Some(sub_def) = sub_aggs_obj.get_mut(&sub_name) {
                    rewrite_agg_def(
                        &sub_name,
                        sub_def,
                        string_hash_fields,
                        touchup_infos,
                        fields_still_needed,
                    );
                }
            }
        }
    }
}

// ─── Query AST rewriting ─────────────────────────────────────────────────────
//
// Rewrites `FieldPresence` nodes in the Quickwit QueryAst JSON to use the
// `_phash_*` hash field instead of the original string field. This avoids
// transcoding the full parquet column just to check for non-null values.
//
// The replacement is semantically correct because during indexing:
// - Non-null string → `doc.add_u64(hash_field, hash)` → field is present
// - Null string → no `add_u64` call → field is absent
//
// So `FieldPresence(_phash_X)` ≡ `FieldPresence(X)` for string fields.

/// Rewrite a Quickwit QueryAst JSON string, replacing `FieldPresence` queries
/// on string hash fields with `FieldPresence` queries on the corresponding
/// `_phash_*` field.
///
/// Returns `None` if no changes were made (the JSON is already optimal).
/// Returns `Some(rewritten_json)` if at least one field was redirected.
pub fn rewrite_query_for_hash_fields(
    query_json: &str,
    string_hash_fields: &HashMap<String, String>,
) -> Option<String> {
    if string_hash_fields.is_empty() {
        return None;
    }

    let mut value: Value = match serde_json::from_str(query_json) {
        Ok(v) => v,
        Err(_) => return None,
    };

    if rewrite_query_node(&mut value, string_hash_fields) {
        serde_json::to_string(&value).ok()
    } else {
        None
    }
}

/// Recursively walk a QueryAst JSON node, replacing `FieldPresence` fields.
/// Returns `true` if any modification was made.
fn rewrite_query_node(
    node: &mut Value,
    string_hash_fields: &HashMap<String, String>,
) -> bool {
    let obj = match node.as_object_mut() {
        Some(o) => o,
        None => return false,
    };

    // Check if this node is a FieldPresence query
    let is_field_presence = obj
        .get("type")
        .and_then(|t| t.as_str())
        .map(|t| t == "field_presence")
        .unwrap_or(false);

    if is_field_presence {
        if let Some(field) = obj.get("field").and_then(|f| f.as_str()).map(|s| s.to_string()) {
            if let Some(hash_field) = string_hash_fields.get(&field) {
                obj.insert("field".to_string(), Value::String(hash_field.clone()));
                return true;
            }
        }
        return false;
    }

    // Recurse into Bool query clauses
    let is_bool = obj
        .get("type")
        .and_then(|t| t.as_str())
        .map(|t| t == "bool")
        .unwrap_or(false);

    if is_bool {
        let mut changed = false;
        for clause_key in &["must", "should", "must_not", "filter"] {
            if let Some(arr) = obj.get_mut(*clause_key).and_then(|v| v.as_array_mut()) {
                for item in arr.iter_mut() {
                    changed |= rewrite_query_node(item, string_hash_fields);
                }
            }
        }
        return changed;
    }

    // Recurse into Boost query
    let is_boost = obj
        .get("type")
        .and_then(|t| t.as_str())
        .map(|t| t == "boost")
        .unwrap_or(false);

    if is_boost {
        if let Some(underlying) = obj.get_mut("underlying") {
            return rewrite_query_node(underlying, string_hash_fields);
        }
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_hash_fields() -> HashMap<String, String> {
        let mut m = HashMap::new();
        m.insert("status".to_string(), "_phash_status".to_string());
        m.insert("category".to_string(), "_phash_category".to_string());
        m
    }

    #[test]
    fn test_rewrite_value_count() {
        let agg_json = r#"{"cnt": {"value_count": {"field": "status"}}}"#;
        let out = rewrite_aggs_for_hash_fields(agg_json, &make_hash_fields()).unwrap();
        let v: Value = serde_json::from_str(&out.rewritten_json).unwrap();
        assert_eq!(
            v["cnt"]["value_count"]["field"].as_str().unwrap(),
            "_phash_status"
        );
        assert!(out.touchup_infos.is_empty(), "value_count needs no touchup");
        assert!(!out.fields_still_needing_transcoding.contains("status"));
    }

    #[test]
    fn test_rewrite_cardinality() {
        let agg_json = r#"{"uniq": {"cardinality": {"field": "category"}}}"#;
        let out = rewrite_aggs_for_hash_fields(agg_json, &make_hash_fields()).unwrap();
        let v: Value = serde_json::from_str(&out.rewritten_json).unwrap();
        assert_eq!(
            v["uniq"]["cardinality"]["field"].as_str().unwrap(),
            "_phash_category"
        );
        assert!(out.touchup_infos.is_empty());
    }

    #[test]
    fn test_rewrite_terms_basic() {
        let agg_json = r#"{"by_status": {"terms": {"field": "status", "size": 10}}}"#;
        let out = rewrite_aggs_for_hash_fields(agg_json, &make_hash_fields()).unwrap();
        let v: Value = serde_json::from_str(&out.rewritten_json).unwrap();
        assert_eq!(
            v["by_status"]["terms"]["field"].as_str().unwrap(),
            "_phash_status"
        );
        assert_eq!(out.touchup_infos.len(), 1);
        assert_eq!(out.touchup_infos[0].agg_name, "by_status");
        assert_eq!(out.touchup_infos[0].hash_field_name, "_phash_status");
        assert_eq!(out.touchup_infos[0].original_field_name, "status");
        assert!(!out.touchup_infos[0].needs_resort);
    }

    #[test]
    fn test_rewrite_terms_min_doc_count_zero_no_redirect() {
        let agg_json = r#"{"by_status": {"terms": {"field": "status", "min_doc_count": 0}}}"#;
        let out = rewrite_aggs_for_hash_fields(agg_json, &make_hash_fields()).unwrap();
        let v: Value = serde_json::from_str(&out.rewritten_json).unwrap();
        // Should NOT be rewritten — field stays as "status"
        assert_eq!(v["by_status"]["terms"]["field"].as_str().unwrap(), "status");
        assert!(out.touchup_infos.is_empty());
        assert!(out.fields_still_needing_transcoding.contains("status"));
    }

    #[test]
    fn test_rewrite_terms_order_key() {
        let agg_json = r#"{"by_status": {"terms": {"field": "status", "order": {"_key": "asc"}}}}"#;
        let out = rewrite_aggs_for_hash_fields(agg_json, &make_hash_fields()).unwrap();
        assert_eq!(out.touchup_infos.len(), 1);
        assert!(out.touchup_infos[0].needs_resort);
    }

    #[test]
    fn test_rewrite_terms_include_filter() {
        // include strings are removed from the rewritten JSON and stored in touchup_infos
        // for post-filtering after hash resolution (Tantivy ignores numeric include arrays
        // on U64 fast fields, so we must apply the filter ourselves in Phase 3).
        let agg_json = r#"{"by_status": {"terms": {"field": "status", "include": ["active", "pending"]}}}"#;
        let out = rewrite_aggs_for_hash_fields(agg_json, &make_hash_fields()).unwrap();
        let v: Value = serde_json::from_str(&out.rewritten_json).unwrap();
        // include must be absent from the rewritten JSON
        assert!(
            v["by_status"]["terms"]["include"].is_null(),
            "include should be removed from rewritten JSON"
        );
        // The original strings must be stored in touchup_infos for post-filtering
        assert_eq!(out.touchup_infos.len(), 1);
        let include_strings = out.touchup_infos[0].include_strings.as_ref().unwrap();
        assert_eq!(include_strings.len(), 2);
        assert!(include_strings.contains("active"));
        assert!(include_strings.contains("pending"));
    }

    #[test]
    fn test_rewrite_terms_exclude_filter() {
        // exclude strings are removed from the rewritten JSON and stored in touchup_infos
        // for post-filtering after hash resolution.
        let agg_json = r#"{"by_status": {"terms": {"field": "status", "exclude": ["inactive", "deleted"]}}}"#;
        let out = rewrite_aggs_for_hash_fields(agg_json, &make_hash_fields()).unwrap();
        let v: Value = serde_json::from_str(&out.rewritten_json).unwrap();
        // exclude must be absent from the rewritten JSON
        assert!(
            v["by_status"]["terms"]["exclude"].is_null(),
            "exclude should be removed from rewritten JSON"
        );
        // The original strings must be stored in touchup_infos for post-filtering
        assert_eq!(out.touchup_infos.len(), 1);
        let exclude_strings = out.touchup_infos[0].exclude_strings.as_ref().unwrap();
        assert_eq!(exclude_strings.len(), 2);
        assert!(exclude_strings.contains("inactive"));
        assert!(exclude_strings.contains("deleted"));
        // include should be None
        assert!(out.touchup_infos[0].include_strings.is_none());
    }

    #[test]
    fn test_rewrite_non_string_field_not_redirected() {
        let agg_json = r#"{"by_score": {"terms": {"field": "score"}}}"#;
        let out = rewrite_aggs_for_hash_fields(agg_json, &make_hash_fields()).unwrap();
        let v: Value = serde_json::from_str(&out.rewritten_json).unwrap();
        // "score" has no hash field — should remain unchanged
        assert_eq!(v["by_score"]["terms"]["field"].as_str().unwrap(), "score");
        assert!(out.touchup_infos.is_empty());
    }

    #[test]
    fn test_rewrite_empty_hash_fields_noop() {
        let agg_json = r#"{"cnt": {"value_count": {"field": "status"}}}"#;
        let out = rewrite_aggs_for_hash_fields(agg_json, &HashMap::new()).unwrap();
        assert_eq!(out.rewritten_json, agg_json);
        assert!(out.touchup_infos.is_empty());
    }

    #[test]
    fn test_rewrite_nested_terms() {
        let agg_json = r#"{
            "by_status": {
                "terms": {"field": "status"},
                "aggs": {
                    "by_category": {"terms": {"field": "category"}}
                }
            }
        }"#;
        let out = rewrite_aggs_for_hash_fields(agg_json, &make_hash_fields()).unwrap();
        let v: Value = serde_json::from_str(&out.rewritten_json).unwrap();
        assert_eq!(
            v["by_status"]["terms"]["field"].as_str().unwrap(),
            "_phash_status"
        );
        assert_eq!(
            v["by_status"]["aggs"]["by_category"]["terms"]["field"].as_str().unwrap(),
            "_phash_category"
        );
        // Both levels should have touchup infos
        assert_eq!(out.touchup_infos.len(), 2);
    }

    #[test]
    fn test_hash_value_stability() {
        let h1 = hash_string_value("active");
        let h2 = hash_string_value("active");
        assert_eq!(h1, h2, "hash must be deterministic");
        assert_ne!(h1, 0, "hash must never be zero");
        assert_ne!(hash_string_value("active"), hash_string_value("pending"));
    }

    // ─── Query rewriting tests ──────────────────────────────────────────────

    #[test]
    fn test_rewrite_query_field_presence_redirected() {
        let query = r#"{"type":"field_presence","field":"status"}"#;
        let result = rewrite_query_for_hash_fields(query, &make_hash_fields());
        assert!(result.is_some(), "field_presence on hash-mapped field should be rewritten");
        let v: Value = serde_json::from_str(&result.unwrap()).unwrap();
        assert_eq!(v["field"].as_str().unwrap(), "_phash_status");
        assert_eq!(v["type"].as_str().unwrap(), "field_presence");
    }

    #[test]
    fn test_rewrite_query_field_presence_non_hash_field_unchanged() {
        let query = r#"{"type":"field_presence","field":"score"}"#;
        let result = rewrite_query_for_hash_fields(query, &make_hash_fields());
        assert!(result.is_none(), "field_presence on non-hash field should not be rewritten");
    }

    #[test]
    fn test_rewrite_query_non_field_presence_unchanged() {
        let query = r#"{"type":"term","field":"status","value":"active"}"#;
        let result = rewrite_query_for_hash_fields(query, &make_hash_fields());
        assert!(result.is_none(), "Term query should not be rewritten");
    }

    #[test]
    fn test_rewrite_query_bool_with_nested_field_presence() {
        let query = r#"{"type":"bool","must":[{"type":"field_presence","field":"status"},{"type":"term","field":"name","value":"alice"}],"should":[],"must_not":[],"filter":[]}"#;
        let result = rewrite_query_for_hash_fields(query, &make_hash_fields());
        assert!(result.is_some());
        let v: Value = serde_json::from_str(&result.unwrap()).unwrap();
        assert_eq!(v["must"][0]["field"].as_str().unwrap(), "_phash_status");
        assert_eq!(v["must"][0]["type"].as_str().unwrap(), "field_presence");
        // Term query should be unchanged
        assert_eq!(v["must"][1]["field"].as_str().unwrap(), "name");
    }

    #[test]
    fn test_rewrite_query_empty_hash_fields_noop() {
        let query = r#"{"type":"field_presence","field":"status"}"#;
        let result = rewrite_query_for_hash_fields(query, &HashMap::new());
        assert!(result.is_none());
    }

    #[test]
    fn test_rewrite_query_boost_wrapping_field_presence() {
        let query = r#"{"type":"boost","underlying":{"type":"field_presence","field":"category"},"boost":2.0}"#;
        let result = rewrite_query_for_hash_fields(query, &make_hash_fields());
        assert!(result.is_some());
        let v: Value = serde_json::from_str(&result.unwrap()).unwrap();
        assert_eq!(v["underlying"]["field"].as_str().unwrap(), "_phash_category");
        assert_eq!(v["underlying"]["type"].as_str().unwrap(), "field_presence");
    }

    #[test]
    fn test_rewrite_query_match_all_unchanged() {
        let query = r#"{"type":"match_all"}"#;
        let result = rewrite_query_for_hash_fields(query, &make_hash_fields());
        assert!(result.is_none());
    }
}
