/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Query evaluation for Binary Fuse Filter XRef
//!
//! Evaluates Quickwit QueryAst against Binary Fuse8 filters to determine
//! which splits possibly contain matching documents.
//!
//! # Query Support
//!
//! - **Term queries**: Check if field:value exists in filter (with type normalization)
//! - **Phrase queries**: Check if all terms exist (cannot verify positions)
//! - **Boolean queries**: AND = all must pass, OR = any must pass
//! - **Range/Wildcard**: Return `Exists` (cannot evaluate, must search split)
//!
//! # Type Normalization
//!
//! Query values are normalized to match the format used during indexing:
//! - **Text fields**: Tokenized using the field's tokenizer
//! - **Date fields**: Parsed and converted to nanoseconds since epoch
//! - **Numeric fields**: Converted to string representation
//! - **Boolean fields**: Converted to "true" or "false"
//!
//! # False Positive Rate
//!
//! Binary Fuse8 filters have ~0.4% FPR, meaning ~4 in 1000 splits might
//! be returned as "PossiblyPresent" when they don't actually contain the term.

use std::time::Instant;
use quickwit_query::query_ast::QueryAst;
use tantivy::tokenizer::{TokenizerManager, TextAnalyzer};

use super::types::{FuseXRef, FuseXRefSearchResult, MatchingSplit, FieldTypeInfo};
use crate::debug_println;

/// Result of evaluating a query against a single filter
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterResult {
    /// Term definitely not present in split (filter returned false)
    NotPresent,
    /// Term possibly present in split (filter returned true, could be false positive)
    PossiblyPresent,
    /// Query contains clauses that cannot be evaluated (range, wildcard, etc.)
    /// Split must be searched to determine if it matches
    Exists,
}

impl FilterResult {
    /// Combine two results with AND logic
    pub fn and(self, other: FilterResult) -> FilterResult {
        match (self, other) {
            // If either is NotPresent, result is NotPresent
            (FilterResult::NotPresent, _) | (_, FilterResult::NotPresent) => FilterResult::NotPresent,
            // If either is Exists, result is Exists (cannot eliminate)
            (FilterResult::Exists, _) | (_, FilterResult::Exists) => FilterResult::Exists,
            // Both PossiblyPresent = PossiblyPresent
            (FilterResult::PossiblyPresent, FilterResult::PossiblyPresent) => FilterResult::PossiblyPresent,
        }
    }

    /// Combine two results with OR logic
    pub fn or(self, other: FilterResult) -> FilterResult {
        match (self, other) {
            // If either is PossiblyPresent, result is PossiblyPresent
            (FilterResult::PossiblyPresent, _) | (_, FilterResult::PossiblyPresent) => FilterResult::PossiblyPresent,
            // If either is Exists, result is Exists
            (FilterResult::Exists, _) | (_, FilterResult::Exists) => FilterResult::Exists,
            // Both NotPresent = NotPresent
            (FilterResult::NotPresent, FilterResult::NotPresent) => FilterResult::NotPresent,
        }
    }
}

/// Normalize a query value based on field type
///
/// For text fields, this tokenizes the value and returns all tokens.
/// For other types, returns the value in the format used during indexing.
fn normalize_value(field_info: Option<&FieldTypeInfo>, value: &str) -> Vec<String> {
    let Some(info) = field_info else {
        // No field info - assume text field with default tokenizer
        return tokenize_with_default(value);
    };

    match info.field_type.as_str() {
        "text" => {
            // Tokenize text values
            let tokenizer_name = info.tokenizer.as_deref().unwrap_or("default");
            tokenize_value(value, tokenizer_name)
        }
        "datetime" => {
            // Try to parse date and convert to nanoseconds
            parse_datetime_to_nanos(value)
                .map(|nanos| vec![nanos.to_string()])
                .unwrap_or_else(|| vec![value.to_string()])
        }
        "u64" | "i64" | "f64" => {
            // Numeric values - keep as-is (already string)
            vec![value.to_string()]
        }
        "bool" => {
            // Normalize boolean to "true" or "false"
            let normalized = match value.to_lowercase().as_str() {
                "true" | "1" | "yes" => "true",
                "false" | "0" | "no" => "false",
                _ => value,
            };
            vec![normalized.to_string()]
        }
        "json" => {
            // JSON fields - tokenize the value part
            tokenize_with_default(value)
        }
        _ => {
            // Unknown field type - return as-is
            vec![value.to_string()]
        }
    }
}

/// Tokenize a value using the specified tokenizer
fn tokenize_value(value: &str, tokenizer_name: &str) -> Vec<String> {
    let tokenizer_manager = TokenizerManager::default();

    // Get tokenizer, falling back to default if not found
    let mut tokenizer = tokenizer_manager
        .get(tokenizer_name)
        .unwrap_or_else(|| tokenizer_manager.get("default").unwrap());

    let mut tokens = Vec::new();
    let mut token_stream = tokenizer.token_stream(value);

    while token_stream.advance() {
        tokens.push(token_stream.token().text.clone());
    }

    if tokens.is_empty() {
        // If tokenization produced no tokens, return original value lowercased
        // (default tokenizer behavior for very short terms)
        tokens.push(value.to_lowercase());
    }

    tokens
}

/// Tokenize with default tokenizer (lowercases and splits on whitespace/punctuation)
fn tokenize_with_default(value: &str) -> Vec<String> {
    tokenize_value(value, "default")
}

/// Parse a datetime string and convert to nanoseconds since epoch
fn parse_datetime_to_nanos(value: &str) -> Option<i64> {
    use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};

    // Try parsing as RFC3339/ISO8601
    if let Ok(dt) = DateTime::parse_from_rfc3339(value) {
        return Some(dt.timestamp_nanos_opt()?);
    }

    // Try parsing as simple date (YYYY-MM-DD)
    if let Ok(date) = NaiveDate::parse_from_str(value, "%Y-%m-%d") {
        let datetime = date.and_hms_opt(0, 0, 0)?;
        let utc_dt: DateTime<Utc> = DateTime::from_naive_utc_and_offset(datetime, Utc);
        return Some(utc_dt.timestamp_nanos_opt()?);
    }

    // Try parsing as datetime without timezone
    if let Ok(dt) = NaiveDateTime::parse_from_str(value, "%Y-%m-%d %H:%M:%S") {
        let utc_dt: DateTime<Utc> = DateTime::from_naive_utc_and_offset(dt, Utc);
        return Some(utc_dt.timestamp_nanos_opt()?);
    }

    // Try parsing as Unix timestamp (seconds)
    if let Ok(ts) = value.parse::<i64>() {
        // If value is already in nanoseconds range (> year 2100 in seconds), use as-is
        if ts > 4_000_000_000 {
            return Some(ts);
        }
        // Convert seconds to nanoseconds
        return Some(ts * 1_000_000_000);
    }

    None
}

/// Check if any of the normalized values exist in the filter
fn check_normalized_values(xref: &FuseXRef, split_idx: usize, field: &str, values: &[String]) -> bool {
    for value in values {
        if xref.check(split_idx, field, value) {
            return true;
        }
    }
    false
}

/// Evaluate a QueryAst against a split's filter
///
/// # Arguments
///
/// * `xref` - The FuseXRef containing filters
/// * `split_idx` - Index of the split to check
/// * `query` - QueryAst to evaluate
///
/// # Returns
///
/// * `FilterResult::NotPresent` - Split definitely doesn't contain matching docs
/// * `FilterResult::PossiblyPresent` - Split might contain matching docs
/// * `FilterResult::Exists` - Query has unevaluatable clauses, must search split
pub fn evaluate_query(xref: &FuseXRef, split_idx: usize, query: &QueryAst) -> FilterResult {
    match query {
        QueryAst::Term(term_query) => {
            // Get field type info for normalization
            let field_info = xref.header.get_field_info(&term_query.field);

            // Normalize the query value (tokenize for text fields, convert dates, etc.)
            let normalized_values = normalize_value(field_info, &term_query.value);

            // Check if any normalized value exists in the filter
            if check_normalized_values(xref, split_idx, &term_query.field, &normalized_values) {
                FilterResult::PossiblyPresent
            } else {
                FilterResult::NotPresent
            }
        }

        QueryAst::FullText(fulltext_query) => {
            // Get field type info for normalization
            let field_info = xref.header.get_field_info(&fulltext_query.field);

            // Tokenize the fulltext query
            let normalized_values = normalize_value(field_info, &fulltext_query.text);

            // For fulltext, ALL tokens should exist (for AND semantics)
            // If any token is missing, we can eliminate this split
            let all_present = normalized_values.iter().all(|value| {
                xref.check(split_idx, &fulltext_query.field, value)
            });

            if all_present {
                FilterResult::PossiblyPresent
            } else {
                // Some tokens are definitely missing
                FilterResult::NotPresent
            }
        }

        QueryAst::Bool(bool_query) => {
            // Evaluate MUST clauses - all must pass
            for clause in &bool_query.must {
                let result = evaluate_query(xref, split_idx, clause);
                if result == FilterResult::NotPresent {
                    return FilterResult::NotPresent;
                }
            }

            // Evaluate SHOULD clauses - at least one must pass (if any exist)
            if !bool_query.should.is_empty() {
                let mut any_present = false;
                let mut has_exists = false;

                for clause in &bool_query.should {
                    match evaluate_query(xref, split_idx, clause) {
                        FilterResult::PossiblyPresent => {
                            any_present = true;
                            break;
                        }
                        FilterResult::Exists => {
                            has_exists = true;
                        }
                        FilterResult::NotPresent => {}
                    }
                }

                if !any_present && !has_exists {
                    return FilterResult::NotPresent;
                }

                if has_exists && !any_present {
                    return FilterResult::Exists;
                }
            }

            // MUST_NOT clauses: We cannot definitively exclude with filters
            // because the filter only tells us if a term MIGHT be present
            // If the term is present, the doc could still match other criteria
            // So we cannot eliminate the split based on must_not clauses

            // FILTER clauses (same as MUST but without scoring)
            for clause in &bool_query.filter {
                let result = evaluate_query(xref, split_idx, clause);
                if result == FilterResult::NotPresent {
                    return FilterResult::NotPresent;
                }
            }

            FilterResult::PossiblyPresent
        }

        QueryAst::Range(_) => {
            // Range queries cannot be evaluated with filter
            debug_println!("[FUSE_XREF] Range query found - returning Exists");
            FilterResult::Exists
        }

        QueryAst::Wildcard(_) => {
            // Wildcard queries cannot be evaluated with filter
            debug_println!("[FUSE_XREF] Wildcard query found - returning Exists");
            FilterResult::Exists
        }

        QueryAst::Regex(_) => {
            // Regex queries cannot be evaluated with filter
            debug_println!("[FUSE_XREF] Regex query found - returning Exists");
            FilterResult::Exists
        }

        QueryAst::FieldPresence(_) => {
            // FieldPresence queries check if field has any value - cannot evaluate
            FilterResult::Exists
        }

        QueryAst::PhrasePrefix(_) => {
            // Phrase prefix cannot be evaluated with filter
            FilterResult::Exists
        }

        QueryAst::TermSet(term_set_query) => {
            // For term set queries, check if any of the terms exist in any field
            // Apply normalization for each field's terms
            for (field, terms) in &term_set_query.terms_per_field {
                let field_info = xref.header.get_field_info(field);
                for value in terms {
                    // Normalize the term value based on field type
                    let normalized_values = normalize_value(field_info, value);
                    if check_normalized_values(xref, split_idx, field, &normalized_values) {
                        return FilterResult::PossiblyPresent;
                    }
                }
            }
            FilterResult::NotPresent
        }

        QueryAst::UserInput(_) => {
            // UserInput queries need parsing - cannot evaluate directly
            debug_println!("[FUSE_XREF] UserInput query found - returning Exists");
            FilterResult::Exists
        }

        QueryAst::MatchAll => {
            // MatchAll always matches
            FilterResult::PossiblyPresent
        }

        QueryAst::MatchNone => {
            // MatchNone never matches
            FilterResult::NotPresent
        }

        QueryAst::Boost { underlying, .. } => {
            // Boost doesn't affect filter evaluation
            evaluate_query(xref, split_idx, underlying)
        }
    }
}

/// Search all splits in XRef and return matching ones
///
/// # Arguments
///
/// * `xref` - The FuseXRef to search
/// * `query` - QueryAst to evaluate
/// * `limit` - Maximum number of splits to return (0 = unlimited)
///
/// # Returns
///
/// Search result with matching splits and metadata
pub fn search(xref: &FuseXRef, query: &QueryAst, limit: usize) -> FuseXRefSearchResult {
    let start = Instant::now();
    let mut result = FuseXRefSearchResult::new();
    let mut has_unevaluated = false;

    for (idx, meta) in xref.metadata.iter().enumerate() {
        let filter_result = evaluate_query(xref, idx, query);

        match filter_result {
            FilterResult::NotPresent => {
                // Skip this split - definitely doesn't match
                continue;
            }
            FilterResult::Exists => {
                // Has unevaluatable clauses - must include split
                has_unevaluated = true;
                result.matching_splits.push(MatchingSplit::from_metadata(meta));
            }
            FilterResult::PossiblyPresent => {
                // Might match - include split
                result.matching_splits.push(MatchingSplit::from_metadata(meta));
            }
        }

        // Check limit
        if limit > 0 && result.matching_splits.len() >= limit {
            break;
        }
    }

    result.num_matching_splits = result.matching_splits.len();
    result.has_unevaluated_clauses = has_unevaluated;
    result.search_time_ms = start.elapsed().as_millis() as u64;

    debug_println!(
        "[FUSE_XREF] Search complete: {} matching splits, has_unevaluated={}, time={}ms",
        result.num_matching_splits,
        result.has_unevaluated_clauses,
        result.search_time_ms
    );

    result
}

/// Search with a JSON-serialized QueryAst
///
/// Convenience function for JNI layer
pub fn search_with_json(xref: &FuseXRef, query_json: &str, limit: usize) -> Result<FuseXRefSearchResult, String> {
    let query: QueryAst = serde_json::from_str(query_json)
        .map_err(|e| format!("Failed to parse query JSON: {}", e))?;

    Ok(search(xref, &query, limit))
}

/// Parse a query string using Quickwit's query parser and the schema stored in FuseXRef
///
/// This uses the SAME parsing logic as SplitSearcher.parseQuery() to ensure
/// full query compatibility between XRefSearcher and SplitSearcher.
///
/// # Arguments
///
/// * `xref` - The FuseXRef containing field type information
/// * `query_str` - Query string in Quickwit syntax (e.g., "title:hello AND body:world")
///
/// # Returns
///
/// Parsed QueryAst that can be used with search()
pub fn parse_query_string(xref: &FuseXRef, query_str: &str) -> Result<QueryAst, String> {
    use quickwit_query::query_ast::query_ast_from_user_text;

    // Special case: match all query
    if query_str == "*" || query_str.is_empty() {
        return Ok(QueryAst::MatchAll);
    }

    // Extract default fields (indexed text fields) from stored schema
    // This matches the behavior in split_query.rs extract_text_fields_from_schema
    let default_fields: Vec<String> = xref.header.fields
        .iter()
        .filter(|f| f.indexed && f.field_type == "text")
        .map(|f| f.name.clone())
        .collect();

    debug_println!(
        "[FUSE_XREF] Parsing query '{}' with default fields: {:?}",
        query_str,
        default_fields
    );

    // Use Quickwit's proven two-step process (SAME as SplitSearcher.parseQuery)
    // Step 1: Create UserInputQuery AST with proper default fields
    let default_fields_option = if default_fields.is_empty() {
        None
    } else {
        Some(default_fields.clone())
    };

    let query_ast = query_ast_from_user_text(query_str, default_fields_option.clone());
    debug_println!("[FUSE_XREF] Created UserInputQuery AST: {:?}", query_ast);

    // Step 2: Parse the user query using Quickwit's parser
    let parse_fields = match &default_fields_option {
        Some(fields) => fields,
        None => &Vec::new(),
    };

    match query_ast.parse_user_query(parse_fields) {
        Ok(parsed_ast) => {
            debug_println!("[FUSE_XREF] ✅ Successfully parsed: {:?}", parsed_ast);
            Ok(parsed_ast)
        }
        Err(e) => {
            debug_println!("[FUSE_XREF] ❌ Parsing failed: {}", e);
            Err(format!(
                "Failed to parse query '{}': {}. Use explicit field names (e.g., 'field:term') or ensure schema has indexed text fields.",
                query_str, e
            ))
        }
    }
}

/// Try to parse simple query patterns without full Quickwit parser
///
/// Supports:
/// - `field:value` - simple term query
/// - `field:value AND field:value` - boolean AND
/// - `field:value OR field:value` - boolean OR
/// - `NOT field:value` - boolean NOT (must_not)
/// - `*` - match all
fn try_parse_simple_query(query_str: &str, default_fields: &[String]) -> Option<QueryAst> {
    let query_str = query_str.trim();

    // Handle match all
    if query_str == "*" {
        return Some(QueryAst::MatchAll);
    }

    // Check for boolean operators (case-insensitive)
    let upper = query_str.to_uppercase();

    // Handle AND queries
    if upper.contains(" AND ") {
        return parse_and_query(query_str, default_fields);
    }

    // Handle OR queries
    if upper.contains(" OR ") {
        return parse_or_query(query_str, default_fields);
    }

    // Handle NOT at start
    if upper.starts_with("NOT ") {
        return parse_not_query(query_str, default_fields);
    }

    // Handle simple field:value
    if let Some(parsed) = parse_term_query(query_str, default_fields) {
        return Some(parsed);
    }

    None
}

/// Parse a term query: "field:value" or just "value" (searches default fields)
fn parse_term_query(query_str: &str, default_fields: &[String]) -> Option<QueryAst> {
    let query_str = query_str.trim();

    // Handle quoted values
    let (field, value) = if query_str.contains(':') {
        let parts: Vec<&str> = query_str.splitn(2, ':').collect();
        if parts.len() == 2 {
            let field = parts[0].trim();
            let value = parts[1].trim().trim_matches('"').trim_matches('\'');
            (Some(field.to_string()), value.to_string())
        } else {
            (None, query_str.to_string())
        }
    } else {
        // Unqualified term - search in default fields
        (None, query_str.trim_matches('"').trim_matches('\'').to_string())
    };

    if let Some(field) = field {
        // Check for range syntax
        if value.starts_with('[') || value.starts_with('{') {
            return None; // Let full parser handle ranges
        }

        // Check for wildcard
        if value.contains('*') || value.contains('?') {
            return None; // Let full parser handle wildcards
        }

        Some(QueryAst::Term(quickwit_query::query_ast::TermQuery {
            field,
            value,
        }))
    } else if !default_fields.is_empty() {
        // Search in all default fields with OR
        let should: Vec<QueryAst> = default_fields.iter()
            .map(|f| QueryAst::Term(quickwit_query::query_ast::TermQuery {
                field: f.clone(),
                value: value.clone(),
            }))
            .collect();

        if should.len() == 1 {
            Some(should.into_iter().next().unwrap())
        } else {
            Some(QueryAst::Bool(quickwit_query::query_ast::BoolQuery {
                must: vec![],
                should,
                must_not: vec![],
                filter: vec![],
                minimum_should_match: Some(1),
            }))
        }
    } else {
        None
    }
}

/// Parse AND query: "field:value AND field:value"
fn parse_and_query(query_str: &str, default_fields: &[String]) -> Option<QueryAst> {
    // Split by AND (case-insensitive)
    let parts: Vec<&str> = query_str
        .split(|c: char| c.is_whitespace())
        .collect();

    let mut clauses = Vec::new();
    let mut current_clause = String::new();

    for part in parts {
        if part.eq_ignore_ascii_case("AND") {
            if !current_clause.is_empty() {
                clauses.push(current_clause.trim().to_string());
                current_clause = String::new();
            }
        } else {
            if !current_clause.is_empty() {
                current_clause.push(' ');
            }
            current_clause.push_str(part);
        }
    }
    if !current_clause.is_empty() {
        clauses.push(current_clause.trim().to_string());
    }

    let must: Vec<QueryAst> = clauses.iter()
        .filter_map(|c| try_parse_simple_query(c, default_fields))
        .collect();

    if must.is_empty() {
        return None;
    }

    Some(QueryAst::Bool(quickwit_query::query_ast::BoolQuery {
        must,
        should: vec![],
        must_not: vec![],
        filter: vec![],
        minimum_should_match: None,
    }))
}

/// Parse OR query: "field:value OR field:value"
fn parse_or_query(query_str: &str, default_fields: &[String]) -> Option<QueryAst> {
    let parts: Vec<&str> = query_str
        .split(|c: char| c.is_whitespace())
        .collect();

    let mut clauses = Vec::new();
    let mut current_clause = String::new();

    for part in parts {
        if part.eq_ignore_ascii_case("OR") {
            if !current_clause.is_empty() {
                clauses.push(current_clause.trim().to_string());
                current_clause = String::new();
            }
        } else {
            if !current_clause.is_empty() {
                current_clause.push(' ');
            }
            current_clause.push_str(part);
        }
    }
    if !current_clause.is_empty() {
        clauses.push(current_clause.trim().to_string());
    }

    let should: Vec<QueryAst> = clauses.iter()
        .filter_map(|c| try_parse_simple_query(c, default_fields))
        .collect();

    if should.is_empty() {
        return None;
    }

    Some(QueryAst::Bool(quickwit_query::query_ast::BoolQuery {
        must: vec![],
        should,
        must_not: vec![],
        filter: vec![],
        minimum_should_match: Some(1),
    }))
}

/// Parse NOT query: "NOT field:value"
fn parse_not_query(query_str: &str, default_fields: &[String]) -> Option<QueryAst> {
    // Remove "NOT " prefix (case-insensitive)
    let remaining = if query_str.to_uppercase().starts_with("NOT ") {
        &query_str[4..]
    } else {
        return None;
    };

    let inner = try_parse_simple_query(remaining.trim(), default_fields)?;

    Some(QueryAst::Bool(quickwit_query::query_ast::BoolQuery {
        must: vec![QueryAst::MatchAll], // Must have a positive clause with must_not
        should: vec![],
        must_not: vec![inner],
        filter: vec![],
        minimum_should_match: None,
    }))
}

/// Search with a query string using Quickwit syntax
///
/// Parses the query string and evaluates against filters
pub fn search_with_query_string(xref: &FuseXRef, query_str: &str, limit: usize) -> Result<FuseXRefSearchResult, String> {
    let query = parse_query_string(xref, query_str)?;
    Ok(search(xref, &query, limit))
}

/// Convert a QueryAst to JSON
pub fn query_to_json(query: &QueryAst) -> Result<String, String> {
    serde_json::to_string(query)
        .map_err(|e| format!("Failed to serialize query: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use xorf::BinaryFuse8;

    fn create_test_xref() -> FuseXRef {
        // Create filters with known keys
        let keys1: Vec<u64> = vec![
            fxhash::hash64("title:hello"),
            fxhash::hash64("title:world"),
            fxhash::hash64("body:test"),
        ];
        let keys2: Vec<u64> = vec![
            fxhash::hash64("title:goodbye"),
            fxhash::hash64("body:example"),
        ];

        let filter1 = BinaryFuse8::try_from(&keys1[..]).unwrap();
        let filter2 = BinaryFuse8::try_from(&keys2[..]).unwrap();

        let mut xref = FuseXRef::new("test".to_string(), "test-index".to_string());
        xref.filters.push(FuseFilter::Fuse8(filter1));
        xref.filters.push(FuseFilter::Fuse8(filter2));

        xref.metadata.push(super::super::types::SplitFilterMetadata::new(
            0, "file:///split1.split".to_string(), "split1".to_string(), 0, 1000,
        ));
        xref.metadata.push(super::super::types::SplitFilterMetadata::new(
            1, "file:///split2.split".to_string(), "split2".to_string(), 0, 2000,
        ));

        xref
    }

    #[test]
    fn test_term_query_evaluation() {
        let xref = create_test_xref();

        // Split 0 has "title:hello"
        let query = QueryAst::Term(quickwit_query::query_ast::TermQuery {
            field: "title".to_string(),
            value: "hello".to_string(),
        });

        assert_eq!(evaluate_query(&xref, 0, &query), FilterResult::PossiblyPresent);
        assert_eq!(evaluate_query(&xref, 1, &query), FilterResult::NotPresent);
    }

    #[test]
    fn test_bool_query_must() {
        let xref = create_test_xref();

        // Split 0 has both "title:hello" and "body:test"
        let query = QueryAst::Bool(quickwit_query::query_ast::BoolQuery {
            must: vec![
                QueryAst::Term(quickwit_query::query_ast::TermQuery {
                    field: "title".to_string(),
                    value: "hello".to_string(),
                }),
                QueryAst::Term(quickwit_query::query_ast::TermQuery {
                    field: "body".to_string(),
                    value: "test".to_string(),
                }),
            ],
            should: vec![],
            must_not: vec![],
            filter: vec![],
            minimum_should_match: None,
        });

        assert_eq!(evaluate_query(&xref, 0, &query), FilterResult::PossiblyPresent);

        // Split 0 doesn't have "title:goodbye"
        let query_missing = QueryAst::Bool(quickwit_query::query_ast::BoolQuery {
            must: vec![
                QueryAst::Term(quickwit_query::query_ast::TermQuery {
                    field: "title".to_string(),
                    value: "hello".to_string(),
                }),
                QueryAst::Term(quickwit_query::query_ast::TermQuery {
                    field: "title".to_string(),
                    value: "goodbye".to_string(),
                }),
            ],
            should: vec![],
            must_not: vec![],
            filter: vec![],
            minimum_should_match: None,
        });

        assert_eq!(evaluate_query(&xref, 0, &query_missing), FilterResult::NotPresent);
    }

    #[test]
    fn test_range_query_returns_exists() {
        let xref = create_test_xref();

        let query = QueryAst::Range(quickwit_query::query_ast::RangeQuery {
            field: "price".to_string(),
            lower_bound: std::ops::Bound::Included(quickwit_query::JsonLiteral::Number(10.into())),
            upper_bound: std::ops::Bound::Excluded(quickwit_query::JsonLiteral::Number(100.into())),
        });

        assert_eq!(evaluate_query(&xref, 0, &query), FilterResult::Exists);
    }

    #[test]
    fn test_search_function() {
        let xref = create_test_xref();

        let query = QueryAst::Term(quickwit_query::query_ast::TermQuery {
            field: "title".to_string(),
            value: "hello".to_string(),
        });

        let result = search(&xref, &query, 0);

        assert_eq!(result.num_matching_splits, 1);
        assert!(!result.has_unevaluated_clauses);
        assert_eq!(result.matching_splits[0].split_id, "split1");
    }

    #[test]
    fn test_search_with_limit() {
        let xref = create_test_xref();

        // MatchAll should match all splits
        let query = QueryAst::MatchAll;

        let result = search(&xref, &query, 1);
        assert_eq!(result.num_matching_splits, 1); // Limited to 1
    }
}
