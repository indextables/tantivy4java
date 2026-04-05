// ip_rewriter.rs - Execution-time IP CIDR/wildcard term query rewriting
//
// Rewrites {"type":"term"} nodes for IP address fields that carry CIDR or
// wildcard values into the corresponding {"type":"range"} or {"type":"match_all"}
// nodes.  Called inside the query-rewrite pipeline (async_impl.rs and
// jni_search.rs) where CachedSearcherContext always provides a valid schema,
// so all execution paths (regular search, simple aggregate, groupBy,
// histogram) receive correct expanded queries.
//
// Design:
//   - Fast-path guard (no '/' or '*'): returns Ok(None) with zero allocation
//   - Deserialize query JSON to QueryAst; walk the typed tree recursively
//   - Delegate CIDR/wildcard math to ip_expansion.rs (unchanged)
//   - Re-serialize only when at least one node was rewritten
//   - Non-contiguous wildcards (e.g. "10.*.1.*") return Err so callers can
//     propagate an explicit exception to Java callers.

use std::ops::Bound;

use anyhow::{anyhow, Result};
use quickwit_query::query_ast::{BoolQuery, QueryAst, RangeQuery};
use quickwit_query::JsonLiteral;

use crate::debug_println;

/// Rewrite IP CIDR/wildcard term queries to range/match_all queries.
///
/// Returns `Ok(Some(rewritten_json))` if at least one term node was rewritten,
/// `Ok(None)` if the query needed no changes (including fast-path),
/// `Err(msg)` if a wildcard on an IP field cannot be expanded to a contiguous
/// range (e.g. "10.*.1.*") — callers should surface this as a user-visible error.
///
/// Called after the hash-field and string-indexing rewrites in both the
/// regular-search and aggregation-search execution paths.
#[inline]
pub fn rewrite_ip_term_queries(
    query_json: &str,
    schema: &tantivy::schema::Schema,
) -> Result<Option<String>> {
    // Fast-path: skip JSON parsing entirely for queries with no IP patterns.
    // Regular IP addresses never contain '/' or '*'.
    if !query_json.contains('/') && !query_json.contains('*') {
        return Ok(None);
    }

    let ast: QueryAst = match serde_json::from_str(query_json) {
        Ok(a) => a,
        Err(e) => {
            debug_println!("IP_REWRITE: Failed to parse query JSON: {}", e);
            return Ok(None);
        }
    };

    let (rewritten, changed) = rewrite_ast(ast, schema)?;
    if !changed {
        return Ok(None);
    }

    let json = serde_json::to_string(&rewritten).map_err(|e| {
        debug_println!("IP_REWRITE: Failed to serialize rewritten query: {}", e);
        anyhow!("IP_REWRITE: Failed to serialize rewritten query: {}", e)
    })?;
    Ok(Some(json))
}

/// Recursively rewrite a QueryAst node.  Returns `(node, changed)` or
/// `Err` if a non-contiguous wildcard was found on an IP field.
fn rewrite_ast(
    ast: QueryAst,
    schema: &tantivy::schema::Schema,
) -> Result<(QueryAst, bool)> {
    match ast {
        QueryAst::Term(ref term_query) => {
            let value = &term_query.value;
            if (value.contains('/') || value.contains('*'))
                && is_ip_field(&term_query.field, schema)
            {
                match crate::ip_expansion::try_expand_ip_range(value) {
                    Some((lower, upper)) => {
                        debug_println!(
                            "IP_REWRITE: Expanding '{}' on field '{}' to range [{} TO {}]",
                            value, term_query.field, lower, upper
                        );
                        if crate::ip_expansion::is_match_all_range(&lower, &upper) {
                            debug_println!(
                                "IP_REWRITE: Pattern '{}' covers all IPs — emitting match_all",
                                value
                            );
                            return Ok((QueryAst::MatchAll, true));
                        }
                        let range_query = RangeQuery {
                            field: term_query.field.clone(),
                            lower_bound: Bound::Included(JsonLiteral::String(lower)),
                            upper_bound: Bound::Included(JsonLiteral::String(upper)),
                        };
                        return Ok((QueryAst::Range(range_query), true));
                    }
                    None => {
                        // Non-contiguous wildcard (e.g. "10.*.1.*") — cannot be collapsed
                        // to a single contiguous range. Return error so callers get an
                        // exception instead of silently returning 0 results.
                        return Err(anyhow::anyhow!(
                            "Non-contiguous IP wildcard pattern '{}' on field '{}' is not supported. \
                             Only trailing wildcards (e.g., '10.0.*') are allowed.",
                            value, term_query.field
                        ));
                    }
                }
            }
            Ok((ast, false))
        }

        QueryAst::Bool(bool_query) => {
            let (must, c1) = rewrite_vec(bool_query.must, schema)?;
            let (must_not, c2) = rewrite_vec(bool_query.must_not, schema)?;
            let (should, c3) = rewrite_vec(bool_query.should, schema)?;
            let (filter, c4) = rewrite_vec(bool_query.filter, schema)?;
            let changed = c1 || c2 || c3 || c4;
            let new_bool = BoolQuery {
                must,
                must_not,
                should,
                filter,
                minimum_should_match: bool_query.minimum_should_match,
            };
            Ok((QueryAst::Bool(new_bool), changed))
        }

        // All other variants (Range, MatchAll, FullText, FieldPresence, etc.)
        // cannot contain IP term nodes and pass through unchanged.
        other => Ok((other, false)),
    }
}

fn rewrite_vec(
    queries: Vec<QueryAst>,
    schema: &tantivy::schema::Schema,
) -> Result<(Vec<QueryAst>, bool)> {
    let mut any_changed = false;
    let mut result = Vec::with_capacity(queries.len());
    for q in queries {
        let (rewritten, changed) = rewrite_ast(q, schema)?;
        if changed {
            any_changed = true;
        }
        result.push(rewritten);
    }
    Ok((result, any_changed))
}

/// Returns true if `field` is mapped to an IP address type in the schema.
#[inline]
fn is_ip_field(field: &str, schema: &tantivy::schema::Schema) -> bool {
    schema.fields().any(|(_, entry)| {
        entry.name() == field
            && matches!(
                entry.field_type(),
                tantivy::schema::FieldType::IpAddr(_)
            )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tantivy::schema::{IpAddrOptions, SchemaBuilder, TextOptions};

    fn build_test_schema() -> tantivy::schema::Schema {
        let mut builder = SchemaBuilder::new();
        builder.add_ip_addr_field("ip", IpAddrOptions::default());
        builder.add_text_field("name", TextOptions::default());
        builder.build()
    }

    #[test]
    fn test_fast_path_no_slash_or_star() {
        let schema = build_test_schema();
        let json = r#"{"type":"term","field":"ip","value":"192.168.1.1"}"#;
        assert_eq!(rewrite_ip_term_queries(json, &schema).unwrap(), None);
    }

    #[test]
    fn test_cidr_term_rewrites_to_range() {
        let schema = build_test_schema();
        let json = r#"{"type":"term","field":"ip","value":"192.168.1.0/24"}"#;
        let result = rewrite_ip_term_queries(json, &schema).unwrap();
        assert!(result.is_some());
        let rewritten = result.unwrap();
        assert!(rewritten.contains("range"));
        assert!(rewritten.contains("192.168.1.0"));
        assert!(rewritten.contains("192.168.1.255"));
    }

    #[test]
    fn test_wildcard_term_rewrites_to_range() {
        let schema = build_test_schema();
        let json = r#"{"type":"term","field":"ip","value":"10.0.*.*"}"#;
        let result = rewrite_ip_term_queries(json, &schema).unwrap();
        assert!(result.is_some());
        let rewritten = result.unwrap();
        assert!(rewritten.contains("range"));
        assert!(rewritten.contains("10.0.0.0"));
        assert!(rewritten.contains("10.0.255.255"));
    }

    #[test]
    fn test_match_all_cidr() {
        let schema = build_test_schema();
        let json = r#"{"type":"term","field":"ip","value":"0.0.0.0/0"}"#;
        let result = rewrite_ip_term_queries(json, &schema).unwrap();
        assert!(result.is_some());
        assert!(result.unwrap().contains("match_all"));
    }

    #[test]
    fn test_non_ip_field_not_rewritten() {
        let schema = build_test_schema();
        let json = r#"{"type":"term","field":"name","value":"192.168.1.0/24"}"#;
        assert_eq!(rewrite_ip_term_queries(json, &schema).unwrap(), None);
    }

    #[test]
    fn test_bool_query_rewrites_nested_term() {
        let schema = build_test_schema();
        let json = r#"{"type":"bool","must":[{"type":"term","field":"ip","value":"10.0.0.0/8"}],"should":[],"must_not":[]}"#;
        let result = rewrite_ip_term_queries(json, &schema).unwrap();
        assert!(result.is_some());
        let rewritten = result.unwrap();
        assert!(rewritten.contains("range"));
        assert!(rewritten.contains("10.0.0.0"));
        assert!(rewritten.contains("10.255.255.255"));
    }

    #[test]
    fn test_non_contiguous_wildcard_returns_error() {
        // Non-contiguous wildcards return an error so callers get an exception
        let schema = build_test_schema();
        let json = r#"{"type":"term","field":"ip","value":"10.*.1.*"}"#;
        let result = rewrite_ip_term_queries(json, &schema);
        assert!(result.is_err(), "Non-contiguous wildcard should return error");
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Non-contiguous"), "Error should mention non-contiguous: {}", err_msg);
    }
}
