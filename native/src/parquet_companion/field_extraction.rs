// field_extraction.rs - Extract fast field names from query and aggregation JSON
//
// Used for lazy transcoding: before running a search or aggregation query,
// we parse the JSON to discover which fast field columns are needed,
// then transcode only those columns from parquet.

use std::collections::HashSet;

/// Extract all fast field names from an aggregation request JSON.
///
/// Aggregation format (Elasticsearch-compatible):
/// ```json
/// {
///   "agg_name": {
///     "terms": { "field": "category" },
///     "aggs": { "sub_agg": { "stats": { "field": "price" } } }
///   }
/// }
/// ```
///
/// Recursively walks all aggregations and sub-aggregations to find "field" values.
pub fn extract_aggregation_fields(agg_json: &str) -> HashSet<String> {
    let mut fields = HashSet::new();
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(agg_json) {
        extract_fields_from_agg_value(&value, &mut fields);
    }
    fields
}

/// Extract field names from range queries in a query AST JSON.
///
/// Quickwit query AST format:
/// ```json
/// { "type": "range", "field": "id", "lower_bound": ..., "upper_bound": ... }
/// ```
/// or nested in bool queries:
/// ```json
/// { "type": "bool", "must": [{ "type": "range", "field": "id", ... }] }
/// ```
pub fn extract_range_query_fields(query_json: &str) -> HashSet<String> {
    let mut fields = HashSet::new();
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(query_json) {
        extract_fields_from_query_value(&value, &mut fields);
    }
    fields
}

/// Extract all fast field names needed for a search operation.
///
/// Combines fields from both the query (range queries) and aggregation request.
pub fn extract_all_fast_field_names(
    query_json: &str,
    aggregation_json: Option<&str>,
) -> HashSet<String> {
    let mut fields = extract_range_query_fields(query_json);
    if let Some(agg_json) = aggregation_json {
        fields.extend(extract_aggregation_fields(agg_json));
    }
    fields
}

// --- Internal helpers ---

/// Known aggregation type keys that contain a "field" property.
const AGG_TYPE_KEYS: &[&str] = &[
    "terms", "histogram", "date_histogram", "stats",
    "min", "max", "avg", "sum", "value_count",
    "percentiles", "cardinality",
];

fn extract_fields_from_agg_value(value: &serde_json::Value, fields: &mut HashSet<String>) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, val) in map {
                // Check if this key is an aggregation type with a "field" property
                if AGG_TYPE_KEYS.contains(&key.as_str()) {
                    if let Some(field_name) = val.get("field").and_then(|f| f.as_str()) {
                        fields.insert(field_name.to_string());
                    }
                }
                // Recurse into sub-aggregations ("aggs" key) and all nested objects
                extract_fields_from_agg_value(val, fields);
            }
        }
        serde_json::Value::Array(arr) => {
            for item in arr {
                extract_fields_from_agg_value(item, fields);
            }
        }
        _ => {}
    }
}

fn extract_fields_from_query_value(value: &serde_json::Value, fields: &mut HashSet<String>) {
    match value {
        serde_json::Value::Object(map) => {
            // Check for range or field_presence query pattern:
            //   { "type": "range", "field": "..." }
            //   { "type": "field_presence", "field": "..." }
            if let Some(type_val) = map.get("type").and_then(|t| t.as_str()) {
                if type_val == "range" || type_val == "field_presence" {
                    if let Some(field_name) = map.get("field").and_then(|f| f.as_str()) {
                        fields.insert(field_name.to_string());
                    }
                }
            }
            // Also check Elasticsearch-style: { "range": { "field": "..." } }
            if let Some(range_obj) = map.get("range") {
                if let Some(field_name) = range_obj.get("field").and_then(|f| f.as_str()) {
                    fields.insert(field_name.to_string());
                }
            }
            // Recurse into all nested objects (bool must/should/must_not, etc.)
            for val in map.values() {
                extract_fields_from_query_value(val, fields);
            }
        }
        serde_json::Value::Array(arr) => {
            for item in arr {
                extract_fields_from_query_value(item, fields);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_terms_agg_field() {
        let json = r#"{"my_agg": {"terms": {"field": "category", "size": 10}}}"#;
        let fields = extract_aggregation_fields(json);
        assert_eq!(fields, HashSet::from(["category".to_string()]));
    }

    #[test]
    fn test_extract_stats_agg_field() {
        let json = r#"{"price_stats": {"stats": {"field": "price"}}}"#;
        let fields = extract_aggregation_fields(json);
        assert_eq!(fields, HashSet::from(["price".to_string()]));
    }

    #[test]
    fn test_extract_histogram_agg_field() {
        let json = r#"{"h": {"histogram": {"field": "score", "interval": 10.0}}}"#;
        let fields = extract_aggregation_fields(json);
        assert_eq!(fields, HashSet::from(["score".to_string()]));
    }

    #[test]
    fn test_extract_date_histogram_agg_field() {
        let json = r#"{"dh": {"date_histogram": {"field": "created_at", "fixed_interval": "1d"}}}"#;
        let fields = extract_aggregation_fields(json);
        assert_eq!(fields, HashSet::from(["created_at".to_string()]));
    }

    #[test]
    fn test_extract_nested_sub_aggregations() {
        let json = r#"{
            "by_category": {
                "terms": {"field": "category", "size": 10},
                "aggs": {
                    "avg_price": {"avg": {"field": "price"}},
                    "max_score": {"max": {"field": "score"}}
                }
            }
        }"#;
        let fields = extract_aggregation_fields(json);
        assert_eq!(fields, HashSet::from([
            "category".to_string(),
            "price".to_string(),
            "score".to_string(),
        ]));
    }

    #[test]
    fn test_extract_multiple_top_level_aggs() {
        let json = r#"{
            "agg1": {"terms": {"field": "status"}},
            "agg2": {"stats": {"field": "latency"}}
        }"#;
        let fields = extract_aggregation_fields(json);
        assert_eq!(fields, HashSet::from([
            "status".to_string(),
            "latency".to_string(),
        ]));
    }

    #[test]
    fn test_extract_range_query_quickwit_ast() {
        let json = r#"{"type": "range", "field": "id", "lower_bound": {"included": "5"}, "upper_bound": {"included": "10"}}"#;
        let fields = extract_range_query_fields(json);
        assert_eq!(fields, HashSet::from(["id".to_string()]));
    }

    #[test]
    fn test_extract_range_query_es_style() {
        let json = r#"{"range": {"field": "price", "gte": "10", "lte": "100"}}"#;
        let fields = extract_range_query_fields(json);
        assert_eq!(fields, HashSet::from(["price".to_string()]));
    }

    #[test]
    fn test_extract_range_in_bool_query() {
        let json = r#"{
            "type": "bool",
            "must": [
                {"type": "range", "field": "age", "lower_bound": {"included": "18"}},
                {"type": "term", "field": "status", "value": "active"}
            ]
        }"#;
        let fields = extract_range_query_fields(json);
        assert_eq!(fields, HashSet::from(["age".to_string()]));
    }

    #[test]
    fn test_extract_all_fields_combined() {
        let query = r#"{"type": "range", "field": "score", "lower_bound": {"included": "50"}}"#;
        let agg = r#"{"my_agg": {"terms": {"field": "category"}}}"#;
        let fields = extract_all_fast_field_names(query, Some(agg));
        assert_eq!(fields, HashSet::from([
            "score".to_string(),
            "category".to_string(),
        ]));
    }

    #[test]
    fn test_extract_no_fast_fields() {
        let query = r#"{"type": "term", "field": "title", "value": "hello"}"#;
        let fields = extract_all_fast_field_names(query, None);
        assert!(fields.is_empty());
    }

    #[test]
    fn test_extract_invalid_json() {
        let fields = extract_aggregation_fields("not json");
        assert!(fields.is_empty());
        let fields = extract_range_query_fields("also not json");
        assert!(fields.is_empty());
    }
}
