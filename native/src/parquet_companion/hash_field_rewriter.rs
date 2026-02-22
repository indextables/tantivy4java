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
use crate::parquet_companion::string_indexing::{
    StringIndexingMode, companion_field_name, compile_regexes,
};

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

// ─── Query rewriting for compact string indexing modes ──────────────────────
//
// Rewrites term queries on fields with compact string indexing modes:
// - exact_only: Hash the search term and query the U64 field directly
// - text_*_exactonly: If the search term matches the field's regex pattern,
//   redirect to the companion __uuids U64 hash field
// - text_*_strip: No rewriting needed (queries hit the text field directly)

/// Rewrite a Quickwit QueryAst JSON string for compact string indexing modes.
///
/// Handles `term` query nodes:
/// - `exact_only` fields: hash the value, keep same field name (field is U64)
/// - `text_*_exactonly` fields: if value matches regex, redirect to companion hash field
/// - `text_*_strip` fields: no change
///
/// Converts `full_text` and `phrase` queries to `term` queries for compact modes:
/// - `full_text` on `exact_only`: extracts `text`, converts to hashed term query
/// - `full_text` on `text_*_exactonly`: if text matches regex, converts to companion term
/// - `phrase` on `exact_only`: joins phrases, converts to hashed term query
/// - `phrase` on `text_*_exactonly`: if joined text matches regex, converts to companion term
///
/// This allows `parseQuery("field:value")` to work transparently on compact fields.
///
/// **Blocks** wildcard, regex, and phrase_prefix queries on `exact_only` fields
/// (these cannot be meaningfully converted to term queries on a U64 hash field).
///
/// Returns `Ok(None)` if no changes were made.
/// Returns `Ok(Some(rewritten))` if at least one node was rewritten.
/// Returns `Err(...)` if an unsupported query type targets an `exact_only` field.
pub fn rewrite_query_for_string_indexing(
    query_json: &str,
    string_indexing_modes: &HashMap<String, StringIndexingMode>,
) -> anyhow::Result<Option<String>> {
    if string_indexing_modes.is_empty() {
        return Ok(None);
    }

    let mut value: Value = match serde_json::from_str(query_json) {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };

    // Compile unanchored regexes for pattern extraction. For text_*_exactonly modes,
    // we extract the matching portion from the search value and hash that (not the
    // full value). This allows values like "{UUID}" or "prefix-UUID" to still find
    // the document via the companion hash field.
    // Compilation errors are propagated instead of silently swallowed.
    let compiled_regexes = compile_regexes(string_indexing_modes)?;

    if rewrite_string_indexing_node(&mut value, string_indexing_modes, &compiled_regexes)? {
        Ok(serde_json::to_string(&value).ok())
    } else {
        Ok(None)
    }
}

/// Recursively walk a QueryAst JSON node, rewriting term queries for string indexing modes.
/// Returns `Err` if an unsupported query type targets an `exact_only` field.
fn rewrite_string_indexing_node(
    node: &mut Value,
    string_indexing_modes: &HashMap<String, StringIndexingMode>,
    compiled_regexes: &HashMap<String, regex::Regex>,
) -> anyhow::Result<bool> {
    let obj = match node.as_object_mut() {
        Some(o) => o,
        None => return Ok(false),
    };

    let node_type = obj.get("type").and_then(|t| t.as_str()).unwrap_or("").to_string();

    match node_type.as_str() {
        "term" => {
            let field = match obj.get("field").and_then(|f| f.as_str()).map(|s| s.to_string()) {
                Some(f) => f,
                None => return Ok(false),
            };

            if let Some(mode) = string_indexing_modes.get(&field) {
                match mode {
                    StringIndexingMode::ExactOnly => {
                        // Hash the search value and replace it
                        if let Some(val) = obj.get("value").and_then(|v| v.as_str()).map(|s| s.to_string()) {
                            let hash = hash_string_value(&val);
                            obj.insert("value".to_string(), Value::String(hash.to_string()));
                            return Ok(true);
                        }
                    }
                    StringIndexingMode::TextUuidExactonly | StringIndexingMode::TextCustomExactonly { .. } => {
                        // If the value contains a regex match, extract the matched portion,
                        // hash it, and redirect to the companion field. This handles both
                        // pure values ("UUID") and wrapped values ("{UUID}").
                        if let Some(val) = obj.get("value").and_then(|v| v.as_str()).map(|s| s.to_string()) {
                            if let Some(regex) = compiled_regexes.get(&field) {
                                if let Some(m) = regex.find(&val) {
                                    let companion = companion_field_name(&field);
                                    let hash = hash_string_value(m.as_str());
                                    obj.insert("field".to_string(), Value::String(companion));
                                    obj.insert("value".to_string(), Value::String(hash.to_string()));
                                    return Ok(true);
                                }
                            }
                        }
                        // Otherwise leave as-is (text query on main field)
                    }
                    StringIndexingMode::TextUuidStrip | StringIndexingMode::TextCustomStrip { .. } => {
                        // No rewriting needed
                    }
                }
            }
            Ok(false)
        }

        // Convert full_text queries to term queries for compact string indexing modes.
        // parseQuery("field:value") generates full_text nodes with the original text
        // preserved in the "text" field. We convert to a term query so the existing
        // term handler can hash the value (exact_only) or redirect to companion
        // (text_*_exactonly when regex matches).
        "full_text" => {
            let field = match obj.get("field").and_then(|f| f.as_str()).map(|s| s.to_string()) {
                Some(f) => f,
                None => return Ok(false),
            };

            if let Some(mode) = string_indexing_modes.get(&field) {
                match mode {
                    StringIndexingMode::ExactOnly => {
                        // Convert full_text → term, then hash the value
                        if let Some(text) = obj.get("text").and_then(|t| t.as_str()).map(|s| s.to_string()) {
                            let hash = hash_string_value(&text);
                            obj.insert("type".to_string(), Value::String("term".to_string()));
                            obj.insert("value".to_string(), Value::String(hash.to_string()));
                            obj.remove("text");
                            obj.remove("params");
                            return Ok(true);
                        }
                    }
                    StringIndexingMode::TextUuidExactonly | StringIndexingMode::TextCustomExactonly { .. } => {
                        // If text contains a regex match, extract and hash the matched portion,
                        // then convert to a term query on the companion hash field.
                        if let Some(text) = obj.get("text").and_then(|t| t.as_str()).map(|s| s.to_string()) {
                            if let Some(regex) = compiled_regexes.get(&field) {
                                if let Some(m) = regex.find(&text) {
                                    let companion = companion_field_name(&field);
                                    let hash = hash_string_value(m.as_str());
                                    obj.insert("type".to_string(), Value::String("term".to_string()));
                                    obj.insert("field".to_string(), Value::String(companion));
                                    obj.insert("value".to_string(), Value::String(hash.to_string()));
                                    obj.remove("text");
                                    obj.remove("params");
                                    return Ok(true);
                                }
                            }
                        }
                        // Non-matching text: leave as full_text on stripped text field
                    }
                    _ => {} // strip modes: leave as-is
                }
            }
            Ok(false)
        }

        // Convert phrase queries to term queries for compact string indexing modes.
        // Phrase queries have tokenized text in "phrases" array. We reconstruct the
        // original text (best effort by joining with spaces) and convert to a term query.
        "phrase" => {
            let field = match obj.get("field").and_then(|f| f.as_str()).map(|s| s.to_string()) {
                Some(f) => f,
                None => return Ok(false),
            };

            if let Some(mode) = string_indexing_modes.get(&field) {
                match mode {
                    StringIndexingMode::ExactOnly => {
                        // Reconstruct text from phrases, convert to term, hash
                        if let Some(phrases) = obj.get("phrases").and_then(|p| p.as_array()) {
                            let text: String = phrases.iter()
                                .filter_map(|v| v.as_str())
                                .collect::<Vec<_>>()
                                .join(" ");
                            if !text.is_empty() {
                                let hash = hash_string_value(&text);
                                obj.insert("type".to_string(), Value::String("term".to_string()));
                                obj.insert("value".to_string(), Value::String(hash.to_string()));
                                obj.remove("phrases");
                                obj.remove("slop");
                                return Ok(true);
                            }
                        }
                    }
                    StringIndexingMode::TextUuidExactonly | StringIndexingMode::TextCustomExactonly { .. } => {
                        // Reconstruct text; if it contains a regex match, extract the matched
                        // portion and convert to a hashed term query on the companion field.
                        if let Some(phrases) = obj.get("phrases").and_then(|p| p.as_array()) {
                            let text: String = phrases.iter()
                                .filter_map(|v| v.as_str())
                                .collect::<Vec<_>>()
                                .join(" ");
                            if !text.is_empty() {
                                if let Some(regex) = compiled_regexes.get(&field) {
                                    if let Some(m) = regex.find(&text) {
                                        let companion = companion_field_name(&field);
                                        let hash = hash_string_value(m.as_str());
                                        obj.insert("type".to_string(), Value::String("term".to_string()));
                                        obj.insert("field".to_string(), Value::String(companion));
                                        obj.insert("value".to_string(), Value::String(hash.to_string()));
                                        obj.remove("phrases");
                                        obj.remove("slop");
                                        return Ok(true);
                                    }
                                }
                            }
                        }
                        // Non-matching: leave as phrase query on stripped text field
                    }
                    _ => {} // strip modes: leave as-is
                }
            }
            Ok(false)
        }

        // Block unsupported query types on exact_only fields.
        // Wildcard, regex, and phrase_prefix queries cannot be meaningfully converted
        // to term queries on a U64 hash field.
        "wildcard" | "regex" | "phrase_prefix" => {
            let field = match obj.get("field").and_then(|f| f.as_str()) {
                Some(f) => f,
                None => return Ok(false),
            };

            if let Some(StringIndexingMode::ExactOnly) = string_indexing_modes.get(field) {
                anyhow::bail!(
                    "Cannot use {} query on exact_only field '{}'. \
                     exact_only fields only support exact term queries via parseQuery() or SplitTermQuery.",
                    node_type, field
                );
            }
            Ok(false)
        }

        "bool" => {
            let mut changed = false;
            for clause_key in &["must", "should", "must_not", "filter"] {
                if let Some(arr) = obj.get_mut(*clause_key).and_then(|v| v.as_array_mut()) {
                    for item in arr.iter_mut() {
                        changed |= rewrite_string_indexing_node(item, string_indexing_modes, compiled_regexes)?;
                    }
                }
            }
            Ok(changed)
        }

        "boost" => {
            if let Some(underlying) = obj.get_mut("underlying") {
                rewrite_string_indexing_node(underlying, string_indexing_modes, compiled_regexes)
            } else {
                Ok(false)
            }
        }

        _ => Ok(false),
    }
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

    // ─── String indexing mode query rewriting tests ─────────────────────

    fn make_string_indexing_modes() -> HashMap<String, StringIndexingMode> {
        let mut m = HashMap::new();
        m.insert("trace_id".to_string(), StringIndexingMode::ExactOnly);
        m.insert("message".to_string(), StringIndexingMode::TextUuidExactonly);
        m.insert("log".to_string(), StringIndexingMode::TextUuidStrip);
        m.insert("data".to_string(), StringIndexingMode::TextCustomExactonly {
            regex: r"\d{3}-\d{2}-\d{4}".to_string()
        });
        m
    }

    #[test]
    fn test_string_indexing_exact_only_term_hashed() {
        let query = r#"{"type":"term","field":"trace_id","value":"abc123"}"#;
        let modes = make_string_indexing_modes();

        let result = rewrite_query_for_string_indexing(query, &modes).unwrap();
        assert!(result.is_some(), "exact_only term query should be rewritten");
        let v: Value = serde_json::from_str(&result.unwrap()).unwrap();
        assert_eq!(v["field"].as_str().unwrap(), "trace_id");
        // Value should be the hash of "abc123"
        let expected_hash = hash_string_value("abc123");
        assert_eq!(v["value"].as_str().unwrap(), expected_hash.to_string());
    }

    #[test]
    fn test_string_indexing_uuid_exactonly_uuid_value_redirected() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let query = format!(r#"{{"type":"term","field":"message","value":"{}"}}"#, uuid);
        let modes = make_string_indexing_modes();

        let result = rewrite_query_for_string_indexing(&query, &modes).unwrap();
        assert!(result.is_some(), "UUID value on uuid_exactonly field should be redirected");
        let v: Value = serde_json::from_str(&result.unwrap()).unwrap();
        assert_eq!(v["field"].as_str().unwrap(), "message__uuids");
        let expected_hash = hash_string_value(uuid);
        assert_eq!(v["value"].as_str().unwrap(), expected_hash.to_string());
    }

    #[test]
    fn test_string_indexing_uuid_exactonly_non_uuid_unchanged() {
        let query = r#"{"type":"term","field":"message","value":"error occurred"}"#;
        let modes = make_string_indexing_modes();

        let result = rewrite_query_for_string_indexing(query, &modes).unwrap();
        assert!(result.is_none(), "non-UUID value on uuid_exactonly should not be rewritten");
    }

    #[test]
    fn test_string_indexing_strip_no_rewrite() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let query = format!(r#"{{"type":"term","field":"log","value":"{}"}}"#, uuid);
        let modes = make_string_indexing_modes();

        let result = rewrite_query_for_string_indexing(&query, &modes).unwrap();
        assert!(result.is_none(), "text_uuid_strip should not rewrite term queries");
    }

    #[test]
    fn test_string_indexing_custom_exactonly_matching_value() {
        let query = r#"{"type":"term","field":"data","value":"123-45-6789"}"#;
        let modes = make_string_indexing_modes();

        let result = rewrite_query_for_string_indexing(query, &modes).unwrap();
        assert!(result.is_some(), "matching custom pattern should be redirected");
        let v: Value = serde_json::from_str(&result.unwrap()).unwrap();
        assert_eq!(v["field"].as_str().unwrap(), "data__uuids");
    }

    #[test]
    fn test_string_indexing_custom_exactonly_non_matching_value() {
        let query = r#"{"type":"term","field":"data","value":"hello world"}"#;
        let modes = make_string_indexing_modes();

        let result = rewrite_query_for_string_indexing(query, &modes).unwrap();
        assert!(result.is_none(), "non-matching value should not be redirected");
    }

    #[test]
    fn test_string_indexing_empty_modes_noop() {
        let query = r#"{"type":"term","field":"trace_id","value":"abc"}"#;
        let result = rewrite_query_for_string_indexing(
            query, &HashMap::new()
        ).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_string_indexing_bool_with_nested_term() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let query = format!(
            r#"{{"type":"bool","must":[{{"type":"term","field":"trace_id","value":"abc"}},{{"type":"term","field":"message","value":"{}"}}],"should":[],"must_not":[],"filter":[]}}"#,
            uuid
        );
        let modes = make_string_indexing_modes();

        let result = rewrite_query_for_string_indexing(&query, &modes).unwrap();
        assert!(result.is_some());
        let v: Value = serde_json::from_str(&result.unwrap()).unwrap();
        // trace_id term should be hashed
        assert_eq!(v["must"][0]["field"].as_str().unwrap(), "trace_id");
        let expected_hash = hash_string_value("abc");
        assert_eq!(v["must"][0]["value"].as_str().unwrap(), expected_hash.to_string());
        // message UUID should be redirected to companion
        assert_eq!(v["must"][1]["field"].as_str().unwrap(), "message__uuids");
    }

    // ─── Unsupported query type blocking tests ─────────────────────

    #[test]
    fn test_wildcard_on_exact_only_returns_error() {
        let query = r#"{"type":"wildcard","field":"trace_id","value":"abc*"}"#;
        let modes = make_string_indexing_modes();

        let result = rewrite_query_for_string_indexing(query, &modes);
        assert!(result.is_err(), "wildcard on exact_only should return Err");
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("wildcard"), "error should mention 'wildcard': {}", err_msg);
        assert!(err_msg.contains("trace_id"), "error should mention field name: {}", err_msg);
        assert!(err_msg.contains("exact_only"), "error should mention 'exact_only': {}", err_msg);
    }

    #[test]
    fn test_phrase_on_exact_only_converted_to_term() {
        // Phrase queries on exact_only are converted to term queries (joined with spaces)
        let query = r#"{"type":"phrase","field":"trace_id","phrases":["abc","def"],"slop":0}"#;
        let modes = make_string_indexing_modes();

        let result = rewrite_query_for_string_indexing(query, &modes).unwrap();
        assert!(result.is_some(), "phrase on exact_only should be converted");
        let v: Value = serde_json::from_str(&result.unwrap()).unwrap();
        assert_eq!(v["type"].as_str().unwrap(), "term");
        assert_eq!(v["field"].as_str().unwrap(), "trace_id");
        // Value should be hash of "abc def" (phrases joined with space)
        let expected_hash = hash_string_value("abc def");
        assert_eq!(v["value"].as_str().unwrap(), expected_hash.to_string());
        // phrases and slop should be removed
        assert!(v.get("phrases").is_none());
        assert!(v.get("slop").is_none());
    }

    #[test]
    fn test_regex_on_exact_only_returns_error() {
        let query = r#"{"type":"regex","field":"trace_id","pattern":"abc.*"}"#;
        let modes = make_string_indexing_modes();

        let result = rewrite_query_for_string_indexing(query, &modes);
        assert!(result.is_err(), "regex on exact_only should return Err");
    }

    #[test]
    fn test_full_text_on_exact_only_converted_to_term() {
        // full_text queries (from parseQuery) on exact_only are converted to term queries
        let query = r#"{"type":"full_text","field":"trace_id","text":"hello-world","params":{"tokenizer":"default"}}"#;
        let modes = make_string_indexing_modes();

        let result = rewrite_query_for_string_indexing(query, &modes).unwrap();
        assert!(result.is_some(), "full_text on exact_only should be converted to term");
        let v: Value = serde_json::from_str(&result.unwrap()).unwrap();
        assert_eq!(v["type"].as_str().unwrap(), "term");
        assert_eq!(v["field"].as_str().unwrap(), "trace_id");
        let expected_hash = hash_string_value("hello-world");
        assert_eq!(v["value"].as_str().unwrap(), expected_hash.to_string());
        // text and params should be removed
        assert!(v.get("text").is_none());
        assert!(v.get("params").is_none());
    }

    #[test]
    fn test_full_text_on_uuid_exactonly_uuid_converted() {
        // full_text on text_uuid_exactonly with UUID text → convert to companion term
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let query = format!(
            r#"{{"type":"full_text","field":"message","text":"{}","params":{{"tokenizer":"default"}}}}"#,
            uuid
        );
        let modes = make_string_indexing_modes();

        let result = rewrite_query_for_string_indexing(&query, &modes).unwrap();
        assert!(result.is_some(), "full_text UUID on uuid_exactonly should be converted");
        let v: Value = serde_json::from_str(&result.unwrap()).unwrap();
        assert_eq!(v["type"].as_str().unwrap(), "term");
        assert_eq!(v["field"].as_str().unwrap(), "message__uuids");
        let expected_hash = hash_string_value(uuid);
        assert_eq!(v["value"].as_str().unwrap(), expected_hash.to_string());
    }

    #[test]
    fn test_full_text_on_uuid_exactonly_non_uuid_unchanged() {
        // full_text on text_uuid_exactonly with non-UUID text → leave as full_text
        let query = r#"{"type":"full_text","field":"message","text":"error occurred","params":{"tokenizer":"default"}}"#;
        let modes = make_string_indexing_modes();

        let result = rewrite_query_for_string_indexing(query, &modes).unwrap();
        assert!(result.is_none(), "non-UUID full_text on uuid_exactonly should not be rewritten");
    }

    #[test]
    fn test_phrase_prefix_on_exact_only_returns_error() {
        let query = r#"{"type":"phrase_prefix","field":"trace_id","phrases":["abc"],"max_expansions":50}"#;
        let modes = make_string_indexing_modes();

        let result = rewrite_query_for_string_indexing(query, &modes);
        assert!(result.is_err(), "phrase_prefix on exact_only should return Err");
    }

    #[test]
    fn test_wildcard_on_text_uuid_exactonly_passes() {
        // text_uuid_exactonly keeps a real text field, so wildcard is valid
        let query = r#"{"type":"wildcard","field":"message","value":"error*"}"#;
        let modes = make_string_indexing_modes();

        let result = rewrite_query_for_string_indexing(query, &modes);
        assert!(result.is_ok(), "wildcard on text_uuid_exactonly should not error");
        assert!(result.unwrap().is_none(), "wildcard on text_uuid_exactonly should not be rewritten");
    }

    #[test]
    fn test_wildcard_on_non_overridden_field_passes() {
        // "unknown_field" has no string indexing mode — should pass through
        let query = r#"{"type":"wildcard","field":"unknown_field","value":"test*"}"#;
        let modes = make_string_indexing_modes();

        let result = rewrite_query_for_string_indexing(query, &modes);
        assert!(result.is_ok(), "wildcard on non-overridden field should not error");
        assert!(result.unwrap().is_none(), "wildcard on non-overridden field should not be rewritten");
    }

    #[test]
    fn test_wildcard_on_text_uuid_strip_passes() {
        // text_uuid_strip keeps a text field, so wildcard is valid
        let query = r#"{"type":"wildcard","field":"log","value":"error*"}"#;
        let modes = make_string_indexing_modes();

        let result = rewrite_query_for_string_indexing(query, &modes);
        assert!(result.is_ok(), "wildcard on text_uuid_strip should not error");
    }

    #[test]
    fn test_wildcard_on_exact_only_nested_in_bool_returns_error() {
        // A wildcard on exact_only nested inside a bool query should still error
        let query = r#"{"type":"bool","must":[{"type":"wildcard","field":"trace_id","value":"abc*"}],"should":[],"must_not":[],"filter":[]}"#;
        let modes = make_string_indexing_modes();

        let result = rewrite_query_for_string_indexing(query, &modes);
        assert!(result.is_err(), "wildcard on exact_only nested in bool should return Err");
    }

    #[test]
    fn test_uuid_substring_extracted_and_redirected() {
        // A value that CONTAINS a UUID should have the UUID portion extracted,
        // hashed, and redirected to the companion field. This handles values
        // like "{UUID}" or "prefix-UUID-suffix".
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let uuid_embedded = format!("prefix-{}-suffix", uuid);
        let query = format!(r#"{{"type":"term","field":"message","value":"{}"}}"#, uuid_embedded);
        let modes = make_string_indexing_modes();

        let result = rewrite_query_for_string_indexing(&query, &modes).unwrap();
        assert!(result.is_some(),
            "value containing a UUID should be redirected to companion field");
        let v: Value = serde_json::from_str(&result.unwrap()).unwrap();
        assert_eq!(v["field"].as_str().unwrap(), "message__uuids");
        // Should hash just the extracted UUID, not the full value with prefix/suffix
        let expected_hash = hash_string_value(uuid);
        assert_eq!(v["value"].as_str().unwrap(), expected_hash.to_string());
    }

    #[test]
    fn test_invalid_regex_pattern_returns_error() {
        // A field with an invalid regex should return an error, not silently ignore
        let mut modes = HashMap::new();
        modes.insert("bad_field".to_string(), StringIndexingMode::TextCustomExactonly {
            regex: "[invalid".to_string(),
        });

        let query = r#"{"type":"term","field":"bad_field","value":"test"}"#;
        let result = rewrite_query_for_string_indexing(query, &modes);
        assert!(result.is_err(), "invalid regex should return Err");
        assert!(result.unwrap_err().to_string().contains("Invalid regex"),
            "error should mention invalid regex");
    }
}
