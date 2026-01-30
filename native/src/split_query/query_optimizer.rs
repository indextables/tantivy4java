// query_optimizer.rs - Query optimization for expensive wildcard patterns
// Part of the Smart Wildcard AST Skipping optimization feature
//
// This module analyzes QueryAst to detect expensive wildcard patterns and
// extracts cheap filters that can be evaluated first for short-circuit optimization.

use anyhow::Result;
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::debug_println;

// =====================================================================
// Statistics for Smart Wildcard Optimization (for testing/monitoring)
// =====================================================================

/// Counter for queries analyzed for optimization
static QUERIES_ANALYZED: AtomicU64 = AtomicU64::new(0);
/// Counter for queries that were optimizable (had expensive wildcard + cheap filter)
static QUERIES_OPTIMIZABLE: AtomicU64 = AtomicU64::new(0);
/// Counter for queries where cheap filter returned 0 results (short-circuit triggered)
static SHORT_CIRCUITS_TRIGGERED: AtomicU64 = AtomicU64::new(0);

/// Statistics for the smart wildcard optimization
#[derive(Debug, Clone)]
pub struct SmartWildcardStats {
    pub queries_analyzed: u64,
    pub queries_optimizable: u64,
    pub short_circuits_triggered: u64,
}

/// Get current statistics
pub fn get_smart_wildcard_stats() -> SmartWildcardStats {
    SmartWildcardStats {
        queries_analyzed: QUERIES_ANALYZED.load(Ordering::Relaxed),
        queries_optimizable: QUERIES_OPTIMIZABLE.load(Ordering::Relaxed),
        short_circuits_triggered: SHORT_CIRCUITS_TRIGGERED.load(Ordering::Relaxed),
    }
}

/// Reset all statistics counters (for testing)
pub fn reset_smart_wildcard_stats() {
    QUERIES_ANALYZED.store(0, Ordering::Relaxed);
    QUERIES_OPTIMIZABLE.store(0, Ordering::Relaxed);
    SHORT_CIRCUITS_TRIGGERED.store(0, Ordering::Relaxed);
}

/// Increment queries_analyzed counter
pub fn increment_queries_analyzed() {
    QUERIES_ANALYZED.fetch_add(1, Ordering::Relaxed);
}

/// Increment queries_optimizable counter
pub fn increment_queries_optimizable() {
    QUERIES_OPTIMIZABLE.fetch_add(1, Ordering::Relaxed);
}

/// Increment short_circuits_triggered counter
pub fn increment_short_circuits_triggered() {
    SHORT_CIRCUITS_TRIGGERED.fetch_add(1, Ordering::Relaxed);
}

/// Query cost classification for optimization decisions
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum QueryCost {
    /// Low cost - single FST lookup O(1), e.g., term query
    Low,
    /// Medium cost - prefix FST traversal or sorted scan
    Medium,
    /// High cost - full FST scan or multiple regex expansions
    High,
}

/// Result of analyzing a query for optimization opportunities
#[derive(Debug)]
pub struct QueryAnalysis {
    /// Whether the query contains expensive wildcard patterns
    pub has_expensive_wildcard: bool,
    /// Whether cheap filters exist that can be evaluated first
    pub has_cheap_filters: bool,
    /// The cheap filter portion as QueryAst JSON (if extractable)
    pub cheap_filter_json: Option<String>,
    /// Overall cost classification
    pub overall_cost: QueryCost,
    /// Whether optimization can be applied
    pub can_optimize: bool,
}

impl QueryAnalysis {
    fn new_non_optimizable(cost: QueryCost) -> Self {
        Self {
            has_expensive_wildcard: false,
            has_cheap_filters: false,
            cheap_filter_json: None,
            overall_cost: cost,
            can_optimize: false,
        }
    }
}

/// Check if a wildcard pattern is expensive to evaluate
fn is_expensive_pattern(pattern: &str) -> bool {
    if pattern.is_empty() {
        return false;
    }

    // Leading wildcard (starts with * or ?)
    let first_char = pattern.chars().next().unwrap();
    if first_char == '*' || first_char == '?' {
        return true;
    }

    // Multi-wildcard pattern (more than one *)
    pattern.chars().filter(|&c| c == '*').count() > 1
}

/// Analyze a QueryAst JSON string for optimization opportunities
pub fn analyze_query_ast_json(query_json: &str) -> Result<QueryAnalysis> {
    debug_println!("🔍 QUERY_OPTIMIZER: Analyzing query for optimization opportunities");

    let query_value: Value = serde_json::from_str(query_json)
        .map_err(|e| anyhow::anyhow!("Failed to parse query JSON: {}", e))?;

    analyze_query_value(&query_value)
}

/// Analyze a serde_json Value representing a QueryAst
fn analyze_query_value(value: &Value) -> Result<QueryAnalysis> {
    // QueryAst can be serialized in two formats:
    // 1. Quickwit format: { "TypeName": { ...fields... } } - e.g., {"Bool": {"must": [...]}}
    // 2. Type-field format: { "type": "typename", ...fields... } - e.g., {"type": "bool", "must": [...]}

    if let Value::Object(map) = value {
        // Check for type-field format (e.g., {"type": "bool", "must": [...]})
        if let Some(Value::String(type_str)) = map.get("type") {
            return analyze_query_by_type(type_str, value);
        }

        // Check for Quickwit format (e.g., {"Bool": {"must": [...]}})
        if let Some((query_type, query_data)) = map.iter().next() {
            if query_type != "type" {
                return analyze_query_by_type(query_type, query_data);
            }
        }
    }

    // Handle simple string values (like "MatchAll")
    if let Value::String(s) = value {
        if s == "MatchAll" {
            return Ok(QueryAnalysis::new_non_optimizable(QueryCost::Low));
        }
    }

    debug_println!("🔍 QUERY_OPTIMIZER: Unknown query structure, skipping optimization");
    Ok(QueryAnalysis::new_non_optimizable(QueryCost::Medium))
}

/// Analyze query based on its type
fn analyze_query_by_type(query_type: &str, query_data: &Value) -> Result<QueryAnalysis> {
    // Normalize type to lowercase for comparison
    let query_type_lower = query_type.to_lowercase();
    match query_type_lower.as_str() {
        "wildcard" => analyze_wildcard_query(query_data),
        "bool" => analyze_bool_query(query_data),
        "term" | "termset" => Ok(QueryAnalysis::new_non_optimizable(QueryCost::Low)),
        "range" => Ok(QueryAnalysis::new_non_optimizable(QueryCost::Low)),
        "exists" => Ok(QueryAnalysis::new_non_optimizable(QueryCost::Low)),
        "matchall" | "match_all" => Ok(QueryAnalysis::new_non_optimizable(QueryCost::Low)),
        "phrase" | "phraseprefix" | "phrase_prefix" => Ok(QueryAnalysis::new_non_optimizable(QueryCost::Medium)),
        "regex" => Ok(QueryAnalysis::new_non_optimizable(QueryCost::High)),
        "full_text" => Ok(QueryAnalysis::new_non_optimizable(QueryCost::Medium)), // full_text queries are medium cost
        _ => {
            debug_println!("🔍 QUERY_OPTIMIZER: Unknown query type '{}', assuming medium cost", query_type);
            Ok(QueryAnalysis::new_non_optimizable(QueryCost::Medium))
        }
    }
}

/// Analyze a Wildcard query
fn analyze_wildcard_query(query_data: &Value) -> Result<QueryAnalysis> {
    let pattern = query_data
        .get("value")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let is_expensive = is_expensive_pattern(pattern);

    debug_println!(
        "🔍 QUERY_OPTIMIZER: Wildcard pattern '{}' is {}",
        pattern,
        if is_expensive { "EXPENSIVE" } else { "cheap" }
    );

    Ok(QueryAnalysis {
        has_expensive_wildcard: is_expensive,
        has_cheap_filters: false,
        cheap_filter_json: None,
        overall_cost: if is_expensive { QueryCost::High } else { QueryCost::Medium },
        can_optimize: false, // Wildcard alone cannot be optimized
    })
}

/// Analyze a Bool query for optimization opportunities
/// Recursively extracts cheap filters from nested MUST clauses
fn analyze_bool_query(query_data: &Value) -> Result<QueryAnalysis> {
    let must = query_data.get("must").and_then(|v| v.as_array());
    let should = query_data.get("should").and_then(|v| v.as_array());
    let filter = query_data.get("filter").and_then(|v| v.as_array());
    let must_not = query_data.get("must_not").and_then(|v| v.as_array());

    let mut has_expensive_wildcard = false;
    let mut cheap_must_clauses: Vec<Value> = Vec::new();
    let mut overall_cost = QueryCost::Low;

    // Analyze MUST clauses - recursively extract cheap filters from nested ANDs
    if let Some(must_clauses) = must {
        for clause in must_clauses {
            let analysis = analyze_query_value(clause)?;
            overall_cost = std::cmp::max(overall_cost, analysis.overall_cost);

            if analysis.has_expensive_wildcard || analysis.overall_cost == QueryCost::High {
                has_expensive_wildcard = true;

                // RECURSIVE EXTRACTION: If this is an expensive nested Bool with cheap filters,
                // extract and flatten those cheap filters to our level
                if analysis.has_cheap_filters {
                    if let Some(nested_cheap) = extract_cheap_filters_from_nested_must(clause) {
                        cheap_must_clauses.extend(nested_cheap);
                    }
                }
            } else {
                // This clause is entirely cheap - add it directly
                cheap_must_clauses.push(clause.clone());
            }
        }
    }

    // Analyze SHOULD clauses (expensive SHOULD doesn't allow optimization)
    // NOTE: We don't extract from SHOULD because OR semantics mean the clause might not apply
    if let Some(should_clauses) = should {
        for clause in should_clauses {
            let analysis = analyze_query_value(clause)?;
            overall_cost = std::cmp::max(overall_cost, analysis.overall_cost);
            if analysis.has_expensive_wildcard || analysis.overall_cost == QueryCost::High {
                has_expensive_wildcard = true;
            }
        }
    }

    // Analyze FILTER clauses (these are always cheap by nature)
    if let Some(filter_clauses) = filter {
        for clause in filter_clauses {
            let analysis = analyze_query_value(clause)?;
            if analysis.overall_cost == QueryCost::Low {
                cheap_must_clauses.push(clause.clone());
            }
        }
    }

    // Analyze MUST_NOT clauses
    if let Some(must_not_clauses) = must_not {
        for clause in must_not_clauses {
            let analysis = analyze_query_value(clause)?;
            overall_cost = std::cmp::max(overall_cost, analysis.overall_cost);
        }
    }

    let has_cheap_filters = !cheap_must_clauses.is_empty();
    let can_optimize = has_expensive_wildcard && has_cheap_filters;

    // Build cheap filter JSON if optimization is possible
    let cheap_filter_json = if can_optimize {
        build_cheap_filter_json(&cheap_must_clauses, must_not)?
    } else {
        None
    };

    debug_println!(
        "🔍 QUERY_OPTIMIZER: Bool query analysis - expensive={}, cheap_filters={}, can_optimize={}",
        has_expensive_wildcard, has_cheap_filters, can_optimize
    );

    Ok(QueryAnalysis {
        has_expensive_wildcard,
        has_cheap_filters,
        cheap_filter_json,
        overall_cost,
        can_optimize,
    })
}

/// Recursively extract cheap filters from a nested MUST clause
/// Only extracts from Bool queries that have MUST clauses (AND semantics)
fn extract_cheap_filters_from_nested_must(clause: &Value) -> Option<Vec<Value>> {
    // Check if this is a Bool query
    let is_bool = if let Some(obj) = clause.as_object() {
        obj.get("type").and_then(|v| v.as_str()) == Some("bool") ||
        obj.contains_key("Bool")
    } else {
        false
    };

    if !is_bool {
        return None;
    }

    // Get the query data (handle both formats)
    let query_data = if let Some(obj) = clause.as_object() {
        if obj.contains_key("Bool") {
            obj.get("Bool")
        } else {
            Some(clause)
        }
    } else {
        None
    }?;

    let must = query_data.get("must").and_then(|v| v.as_array())?;

    let mut cheap_filters = Vec::new();

    for inner_clause in must {
        let analysis = analyze_query_value(inner_clause).ok()?;

        if analysis.has_expensive_wildcard || analysis.overall_cost == QueryCost::High {
            // This inner clause is expensive - recursively check if it has extractable cheap filters
            if analysis.has_cheap_filters {
                if let Some(nested) = extract_cheap_filters_from_nested_must(inner_clause) {
                    cheap_filters.extend(nested);
                }
            }
        } else {
            // This inner clause is cheap - extract it
            cheap_filters.push(inner_clause.clone());
        }
    }

    if cheap_filters.is_empty() {
        None
    } else {
        Some(cheap_filters)
    }
}

/// Build the cheap filter JSON from extracted clauses
fn build_cheap_filter_json(cheap_clauses: &[Value], must_not: Option<&Vec<Value>>) -> Result<Option<String>> {
    if cheap_clauses.is_empty() {
        return Ok(None);
    }

    // If only one cheap clause and no must_not, return it directly
    if cheap_clauses.len() == 1 && must_not.map_or(true, |v| v.is_empty()) {
        let json = serde_json::to_string(&cheap_clauses[0])?;
        return Ok(Some(json));
    }

    // Build a Bool query with the cheap clauses using type-field format
    // to match the format of the input clauses
    let bool_query = serde_json::json!({
        "type": "bool",
        "must": cheap_clauses,
        "should": [],
        "must_not": must_not.cloned().unwrap_or_default()
    });

    let json = serde_json::to_string(&bool_query)?;
    Ok(Some(json))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_expensive_pattern() {
        // Leading wildcards are expensive
        assert!(is_expensive_pattern("*foo"));
        assert!(is_expensive_pattern("*foo*"));
        assert!(is_expensive_pattern("?bar"));

        // Multi-wildcards are expensive
        assert!(is_expensive_pattern("foo*bar*"));
        assert!(is_expensive_pattern("*foo*bar*"));

        // Suffix wildcards are NOT expensive
        assert!(!is_expensive_pattern("foo*"));
        assert!(!is_expensive_pattern("hello*"));

        // Simple patterns are NOT expensive
        assert!(!is_expensive_pattern("foo"));
        assert!(!is_expensive_pattern("hello"));

        // Empty pattern is NOT expensive
        assert!(!is_expensive_pattern(""));
    }

    #[test]
    fn test_analyze_term_query() {
        let json = r#"{"Term":{"field":"title","value":"hello"}}"#;
        let analysis = analyze_query_ast_json(json).unwrap();
        assert!(!analysis.has_expensive_wildcard);
        assert!(!analysis.can_optimize);
        assert_eq!(analysis.overall_cost, QueryCost::Low);
    }

    #[test]
    fn test_analyze_expensive_wildcard() {
        let json = r#"{"Wildcard":{"field":"title","value":"*phone*","lenient":true}}"#;
        let analysis = analyze_query_ast_json(json).unwrap();
        assert!(analysis.has_expensive_wildcard);
        assert!(!analysis.can_optimize); // Wildcard alone can't be optimized
        assert_eq!(analysis.overall_cost, QueryCost::High);
    }

    #[test]
    fn test_analyze_cheap_wildcard() {
        let json = r#"{"Wildcard":{"field":"title","value":"phone*","lenient":true}}"#;
        let analysis = analyze_query_ast_json(json).unwrap();
        assert!(!analysis.has_expensive_wildcard);
        assert_eq!(analysis.overall_cost, QueryCost::Medium);
    }

    #[test]
    fn test_analyze_optimizable_bool_query() {
        // Bool query with cheap term filter + expensive wildcard
        let json = r#"{
            "Bool": {
                "must": [
                    {"Term":{"field":"category","value":"Electronics"}},
                    {"Wildcard":{"field":"title","value":"*phone*","lenient":true}}
                ],
                "should": [],
                "must_not": [],
                "filter": [],
                "minimum_should_match": null
            }
        }"#;
        let analysis = analyze_query_ast_json(json).unwrap();
        assert!(analysis.has_expensive_wildcard);
        assert!(analysis.has_cheap_filters);
        assert!(analysis.can_optimize);
        assert!(analysis.cheap_filter_json.is_some());

        // Verify cheap filter contains only the term query
        let cheap_json = analysis.cheap_filter_json.unwrap();
        assert!(cheap_json.contains("Term"));
        assert!(cheap_json.contains("category"));
        assert!(!cheap_json.contains("Wildcard"));
    }

    #[test]
    fn test_short_circuit_returns_empty_on_zero_results() {
        // This test validates Phase 1 behavior: when cheap filter matches 0 docs,
        // the expensive wildcard should be skipped entirely.
        // The actual short-circuit happens in async_impl.rs, but this test verifies
        // the query analysis correctly identifies optimizable queries.
        let json = r#"{
            "Bool": {
                "must": [
                    {"Term":{"field":"category","value":"NonExistentCategory"}},
                    {"Wildcard":{"field":"title","value":"*phone*","lenient":true}}
                ],
                "should": [],
                "must_not": [],
                "filter": [],
                "minimum_should_match": null
            }
        }"#;
        let analysis = analyze_query_ast_json(json).unwrap();

        // Query should be identified as optimizable
        assert!(analysis.can_optimize, "Query with term + wildcard should be optimizable");
        assert!(analysis.has_expensive_wildcard, "Should detect expensive wildcard");
        assert!(analysis.has_cheap_filters, "Should detect cheap term filter");
        assert!(analysis.cheap_filter_json.is_some(), "Should extract cheap filter");

        // The cheap filter should only contain the term query
        let cheap_json = analysis.cheap_filter_json.unwrap();
        assert!(cheap_json.contains("NonExistentCategory"), "Cheap filter should contain term value");
        assert!(!cheap_json.contains("Wildcard"), "Cheap filter should NOT contain wildcard");
    }

    #[test]
    fn test_analyze_non_optimizable_all_expensive() {
        // All clauses are expensive
        let json = r#"{
            "Bool": {
                "must": [
                    {"Wildcard":{"field":"title","value":"*foo*","lenient":true}},
                    {"Wildcard":{"field":"content","value":"*bar*","lenient":true}}
                ],
                "should": [],
                "must_not": [],
                "filter": [],
                "minimum_should_match": null
            }
        }"#;
        let analysis = analyze_query_ast_json(json).unwrap();
        assert!(analysis.has_expensive_wildcard);
        assert!(!analysis.has_cheap_filters);
        assert!(!analysis.can_optimize);
    }
}
