// prescan.rs - Term existence checking in FST for split filtering
//
// This module provides prescan functionality to check if query terms exist
// in a split's term dictionary (FST) WITHOUT accessing posting lists.
// This enables fast split filtering before expensive full searches.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use anyhow::{Result, anyhow, Context};
use futures::future::join_all;
use serde::{Deserialize, Serialize};

use quickwit_storage::{Storage, StorageResolver, ByteRangeCache, STORAGE_METRICS};
use quickwit_query::query_ast::QueryAst;
use quickwit_search::leaf::open_index_with_caches;
use quickwit_search::warmup;
use quickwit_doc_mapper::WarmupInfo;
use quickwit_proto::search::SplitIdAndFooterOffsets;
use tantivy::schema::Field;
use tantivy::Term;
use tantivy::DateTime as TantivyDateTime;
use tantivy::json_utils::convert_to_fast_value_and_append_to_json_term;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use crate::debug_println;
use crate::runtime_manager::QuickwitRuntimeManager;
use crate::global_cache::{get_configured_storage_resolver_async, get_global_searcher_context};
use crate::split_searcher_replacement::{
    get_cached_searcher, cache_searcher,
    SEARCHER_CACHE_HITS, SEARCHER_CACHE_MISSES
};
use std::sync::atomic::{Ordering, AtomicUsize};

// Configured maximum prescan parallelism (0 = use dynamic calculation)
static CONFIGURED_MAX_PRESCAN_PARALLELISM: AtomicUsize = AtomicUsize::new(0);

/// Set the maximum prescan parallelism (upper bound for concurrent prescan operations).
/// If not called or set to 0, parallelism is dynamically calculated as:
///   max(num_splits, num_cpu_cores)
/// This provides optimal parallelism automatically while allowing an upper bound for resource control.
pub fn set_prescan_parallelism(max_parallelism: usize) {
    CONFIGURED_MAX_PRESCAN_PARALLELISM.store(max_parallelism, Ordering::SeqCst);
    if max_parallelism > 0 {
        debug_println!("RUST DEBUG: Prescan max parallelism configured to {} concurrent operations", max_parallelism);
    } else {
        debug_println!("RUST DEBUG: Prescan parallelism set to dynamic mode (based on splits and CPU cores)");
    }
}

/// Get the number of CPU cores available
fn get_num_cpus() -> usize {
    // Use available_parallelism which respects cgroups/container limits
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4) // Fallback to 4 if detection fails
}

/// Split info with required footer offset and file size for efficient prescan.
/// Both values are required to avoid S3/Azure HEAD requests.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SplitInfo {
    pub split_url: String,
    pub footer_offset: u64,  // = split_footer_start
    pub file_size: u64,      // = split_footer_end (total file size)
}

/// Field mapping from doc mapping JSON
#[derive(Clone, Debug, Deserialize)]
pub struct FieldMapping {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: String,
    #[serde(default)]
    pub tokenizer: Option<String>,
    #[allow(dead_code)] // Required for JSON deserialization but not used in prescan logic
    #[serde(default = "default_true")]
    pub indexed: bool,
}

fn default_true() -> bool {
    true
}

/// Document mapping containing field information
#[derive(Clone, Debug, Deserialize)]
pub struct DocMapping {
    pub field_mappings: Vec<FieldMapping>,
}

impl DocMapping {
    pub fn from_json(json: &str) -> Result<Self> {
        // Try parsing as {"field_mappings": [...]} first
        if let Ok(mapping) = serde_json::from_str::<DocMapping>(json) {
            return Ok(mapping);
        }

        // Try parsing as direct array [...]
        if let Ok(field_mappings) = serde_json::from_str::<Vec<FieldMapping>>(json) {
            return Ok(DocMapping { field_mappings });
        }

        Err(anyhow!("Failed to parse doc mapping JSON - expected either {{\"field_mappings\": [...]}} or [...] format"))
    }

    pub fn get_field(&self, name: &str) -> Option<&FieldMapping> {
        self.field_mappings.iter().find(|f| f.name == name)
    }

    /// Check if a field uses a tokenizer that lowercases terms
    pub fn uses_lowercase_tokenizer(&self, field_name: &str) -> bool {
        self.get_field(field_name)
            .and_then(|f| f.tokenizer.as_ref())
            .map(|t| t == "default" || t == "en_stem" || t == "whitespace_lowercase")
            .unwrap_or(true) // Default tokenizer lowercases
    }

    /// Check if a field is a JSON/object type
    /// JSON fields have complex term encoding that includes paths, so we handle them conservatively
    pub fn is_json_field(&self, field_name: &str) -> bool {
        self.get_field(field_name)
            .map(|f| f.field_type == "object" || f.field_type == "json")
            .unwrap_or(false)
    }

    /// Parse a field name that may include a JSON path (e.g., "metadata.color")
    /// Returns (base_field_name, Option<json_path>)
    ///
    /// If the full field name exists in the schema, returns (full_name, None)
    /// If a prefix is a JSON field, returns (prefix, Some(remainder))
    pub fn parse_json_path(&self, field_name: &str) -> (String, Option<String>) {
        // First check if the full field name exists as-is
        if self.get_field(field_name).is_some() {
            return (field_name.to_string(), None);
        }

        // Try to find a JSON field that is a prefix of the field name
        // e.g., "metadata.color.shade" -> check "metadata.color", then "metadata"
        let parts: Vec<&str> = field_name.splitn(2, '.').collect();
        if parts.len() == 2 {
            let base_field = parts[0];
            let json_path = parts[1];

            if self.is_json_field(base_field) {
                debug_println!("RUST DEBUG: Parsed JSON path: base='{}', path='{}'", base_field, json_path);
                return (base_field.to_string(), Some(json_path.to_string()));
            }
        }

        // No JSON path found, return as-is
        (field_name.to_string(), None)
    }

    /// Get the field type string (e.g., "text", "i64", "u64", "f64", "bool", "datetime", "object")
    pub fn get_field_type(&self, field_name: &str) -> Option<&str> {
        self.get_field(field_name).map(|f| f.field_type.as_str())
    }
}

/// Enum representing the parsed field type for term construction
#[derive(Clone, Debug, PartialEq)]
pub enum PrescanFieldType {
    Text,
    I64,
    U64,
    F64,
    Bool,
    DateTime,
    IpAddr,
    Json,
    Bytes,
}

impl PrescanFieldType {
    /// Parse a field type string into PrescanFieldType
    pub fn from_str(type_str: &str) -> Self {
        match type_str {
            "i64" => PrescanFieldType::I64,
            "u64" => PrescanFieldType::U64,
            "f64" => PrescanFieldType::F64,
            "bool" => PrescanFieldType::Bool,
            "datetime" => PrescanFieldType::DateTime,
            "ip" | "ip_addr" | "ipaddr" => PrescanFieldType::IpAddr,
            "object" | "json" => PrescanFieldType::Json,
            "bytes" => PrescanFieldType::Bytes,
            _ => PrescanFieldType::Text, // Default to text for "text" and unknown types
        }
    }
}

/// A term to check in the prescan
#[derive(Clone, Debug)]
pub struct PrescanTerm {
    /// The base field name (for schema lookup, e.g., "metadata" for JSON fields)
    pub field: String,
    /// The original field name from the query (for key matching, e.g., "metadata.color")
    pub original_field: String,
    pub term: String,
    pub occurrence: TermOccurrence,
    pub match_type: TermMatchType,
    /// For JSON fields: the path within the JSON object (e.g., "color" for "metadata.color")
    pub json_path: Option<String>,
}

/// How to match a term in the FST
#[derive(Clone, Debug, PartialEq)]
pub enum TermMatchType {
    /// Exact term match - term must exist exactly in FST
    Exact,
    /// Wildcard pattern match - convert pattern to FST automaton
    Wildcard(WildcardPattern),
    /// Regex pattern match - use FST automaton directly
    Regex(String),
}

/// Parsed wildcard pattern for efficient FST matching
#[derive(Clone, Debug, PartialEq)]
pub enum WildcardPattern {
    /// Simple prefix like "hel*" - very efficient FST prefix scan
    Prefix(String),
    /// Simple suffix like "*world" - requires FST scan with automaton
    Suffix(String),
    /// Contains pattern like "*hello*" - requires FST automaton scan
    Contains(String),
    /// Complex pattern with multiple wildcards like "h*llo" or "*w*rld*"
    Complex(String), // Original pattern for regex conversion
}

impl WildcardPattern {
    /// Parse a wildcard pattern into an optimized form
    pub fn parse(pattern: &str) -> Self {
        let has_leading_star = pattern.starts_with('*');
        let has_trailing_star = pattern.ends_with('*');
        let inner_stars = pattern.trim_matches('*').contains('*');

        if inner_stars {
            // Complex pattern like "h*llo" or "*w*rld*"
            WildcardPattern::Complex(pattern.to_string())
        } else if has_leading_star && has_trailing_star && pattern.len() > 2 {
            // Contains pattern like "*hello*"
            WildcardPattern::Contains(pattern.trim_matches('*').to_string())
        } else if has_leading_star && !has_trailing_star {
            // Suffix pattern like "*world"
            WildcardPattern::Suffix(pattern.trim_start_matches('*').to_string())
        } else if has_trailing_star && !has_leading_star {
            // Prefix pattern like "hel*"
            WildcardPattern::Prefix(pattern.trim_end_matches('*').to_string())
        } else {
            // No wildcards or just single * - treat as complex
            WildcardPattern::Complex(pattern.to_string())
        }
    }

}

/// Escape special regex characters in a string
/// This avoids needing the `regex` crate as a dependency
fn escape_regex(s: &str) -> String {
    let mut result = String::with_capacity(s.len() * 2);
    for c in s.chars() {
        match c {
            '\\' | '.' | '+' | '*' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '|' => {
                result.push('\\');
                result.push(c);
            }
            _ => result.push(c),
        }
    }
    result
}

/// How a term occurs in the query (for evaluation logic)
#[derive(Clone, Debug, PartialEq)]
pub enum TermOccurrence {
    Must,     // Term MUST exist (AND logic)
    Should,   // Term SHOULD exist (OR logic)
}

/// Result of prescanning a single split
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PrescanResult {
    pub split_url: String,
    pub could_have_results: bool,
    pub status: String,  // "SUCCESS", "TIMEOUT", "ERROR"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub term_existence: Option<HashMap<String, bool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

impl PrescanResult {
    pub fn success(split_url: String, could_have_results: bool, term_existence: HashMap<String, bool>) -> Self {
        Self {
            split_url,
            could_have_results,
            status: "SUCCESS".to_string(),
            term_existence: Some(term_existence),
            error_message: None,
        }
    }

    pub fn error(split_url: String, error: &str) -> Self {
        Self {
            split_url,
            could_have_results: true, // Conservative: include on error
            status: "ERROR".to_string(),
            term_existence: None,
            error_message: Some(error.to_string()),
        }
    }
}

/// Extract terms from a QueryAst for prescan checking
/// Uses doc_mapping to normalize terms based on tokenizer
pub fn extract_prescan_terms(query_ast: &QueryAst, doc_mapping: &DocMapping) -> Vec<PrescanTerm> {
    let mut terms = Vec::new();
    extract_terms_recursive(query_ast, doc_mapping, TermOccurrence::Must, &mut terms);
    terms
}

fn extract_terms_recursive(
    query_ast: &QueryAst,
    doc_mapping: &DocMapping,
    occurrence: TermOccurrence,
    terms: &mut Vec<PrescanTerm>,
) {
    debug_println!("RUST DEBUG: extract_terms_recursive called with query_ast variant");
    match query_ast {
        QueryAst::MatchAll | QueryAst::MatchNone => {
            debug_println!("RUST DEBUG:   -> MatchAll/MatchNone - no terms to extract");
            // No terms to extract
        }

        QueryAst::Term(term_query) => {
            debug_println!("RUST DEBUG:   -> Term(field='{}', value='{}')", term_query.field, term_query.value);

            // Parse field name to extract JSON path if present (e.g., "metadata.color" -> "metadata", "color")
            let original_field = term_query.field.clone();
            let (base_field, json_path) = doc_mapping.parse_json_path(&term_query.field);
            let mut term_value = term_query.value.clone();

            // Normalize term based on tokenizer
            if doc_mapping.uses_lowercase_tokenizer(&base_field) {
                term_value = term_value.to_lowercase();
            }

            terms.push(PrescanTerm {
                field: base_field,
                original_field,
                term: term_value,
                occurrence: occurrence.clone(),
                match_type: TermMatchType::Exact,
                json_path,
            });
        }

        QueryAst::TermSet(term_set_query) => {
            debug_println!("RUST DEBUG:   -> TermSet with {} fields", term_set_query.terms_per_field.len());
            // TermSet has terms_per_field: HashMap<String, BTreeSet<String>>
            for (field, field_terms) in &term_set_query.terms_per_field {
                let original_field = field.clone();
                let (base_field, json_path) = doc_mapping.parse_json_path(field);
                let uses_lowercase = doc_mapping.uses_lowercase_tokenizer(&base_field);

                for term_value in field_terms {
                    let mut value = term_value.clone();
                    if uses_lowercase {
                        value = value.to_lowercase();
                    }
                    debug_println!("RUST DEBUG:   -> TermSet term: field='{}', value='{}'", base_field, value);
                    terms.push(PrescanTerm {
                        field: base_field.clone(),
                        original_field: original_field.clone(),
                        term: value,
                        occurrence: TermOccurrence::Should, // Any term in set can match
                        match_type: TermMatchType::Exact,
                        json_path: json_path.clone(),
                    });
                }
            }
        }

        QueryAst::FullText(full_text) => {
            debug_println!("RUST DEBUG:   -> FullText(field='{}', text='{}', mode={:?})",
                full_text.field, full_text.text, full_text.params.mode);
            let original_field = full_text.field.clone();
            let (base_field, json_path) = doc_mapping.parse_json_path(&full_text.field);
            let uses_lowercase = doc_mapping.uses_lowercase_tokenizer(&base_field);

            // Check if this is a phrase query - if so, we need to check ALL terms exist
            match &full_text.params.mode {
                quickwit_query::query_ast::FullTextMode::Phrase { .. } |
                quickwit_query::query_ast::FullTextMode::PhraseFallbackToIntersection => {
                    // For phrase queries, split the text into terms and check each one
                    // If any term doesn't exist, the phrase definitely can't match
                    debug_println!("RUST DEBUG:   -> FullText is Phrase mode - extracting individual terms");
                    for word in full_text.text.split_whitespace() {
                        let mut term_value = word.to_string();
                        if uses_lowercase {
                            term_value = term_value.to_lowercase();
                        }
                        debug_println!("RUST DEBUG:   -> Phrase term: '{}'", term_value);
                        terms.push(PrescanTerm {
                            field: base_field.clone(),
                            original_field: original_field.clone(),
                            term: term_value,
                            occurrence: TermOccurrence::Must, // All phrase terms must exist
                            match_type: TermMatchType::Exact,
                            json_path: json_path.clone(),
                        });
                    }
                }
                _ => {
                    // For other full text modes, check the whole text
                    let mut term_value = full_text.text.clone();
                    if uses_lowercase {
                        term_value = term_value.to_lowercase();
                    }
                    terms.push(PrescanTerm {
                        field: base_field,
                        original_field,
                        term: term_value,
                        occurrence: occurrence.clone(),
                        match_type: TermMatchType::Exact,
                        json_path,
                    });
                }
            }
        }

        QueryAst::PhrasePrefix(phrase_prefix) => {
            debug_println!("RUST DEBUG:   -> PhrasePrefix(field='{}', phrase='{}')", phrase_prefix.field, phrase_prefix.phrase);
            // PhrasePrefixQuery.phrase is a String, not Vec<String>
            // We treat the whole phrase as a single term for existence checking
            let original_field = phrase_prefix.field.clone();
            let (base_field, json_path) = doc_mapping.parse_json_path(&phrase_prefix.field);
            let mut term_value = phrase_prefix.phrase.clone();

            if doc_mapping.uses_lowercase_tokenizer(&base_field) {
                term_value = term_value.to_lowercase();
            }

            // For phrase prefix, just check if the phrase text exists
            // This is conservative - actual query would tokenize the phrase
            terms.push(PrescanTerm {
                field: base_field,
                original_field,
                term: term_value,
                occurrence: occurrence.clone(),
                match_type: TermMatchType::Exact,
                json_path,
            });
        }

        QueryAst::Bool(bool_query) => {
            debug_println!("RUST DEBUG:   -> Bool(must={}, should={}, filter={}, must_not={})",
                bool_query.must.len(), bool_query.should.len(),
                bool_query.filter.len(), bool_query.must_not.len());
            // Process MUST clauses
            for must_clause in &bool_query.must {
                extract_terms_recursive(must_clause, doc_mapping, TermOccurrence::Must, terms);
            }

            // Process SHOULD clauses
            for should_clause in &bool_query.should {
                extract_terms_recursive(should_clause, doc_mapping, TermOccurrence::Should, terms);
            }

            // Process FILTER clauses (same as MUST for prescan purposes)
            for filter_clause in &bool_query.filter {
                extract_terms_recursive(filter_clause, doc_mapping, TermOccurrence::Must, terms);
            }

            // MUST_NOT clauses are ignored for prescan - term absence doesn't guarantee results
        }

        QueryAst::Boost { underlying, .. } => {
            debug_println!("RUST DEBUG:   -> Boost - recursing into underlying query");
            extract_terms_recursive(underlying.as_ref(), doc_mapping, occurrence, terms);
        }

        QueryAst::Wildcard(wildcard_query) => {
            debug_println!("RUST DEBUG:   -> Wildcard(field='{}', value='{}')", wildcard_query.field, wildcard_query.value);
            let original_field = wildcard_query.field.clone();
            let (base_field, json_path) = doc_mapping.parse_json_path(&wildcard_query.field);
            let mut pattern = wildcard_query.value.clone();

            // Normalize pattern based on tokenizer (lowercase if applicable)
            if doc_mapping.uses_lowercase_tokenizer(&base_field) {
                pattern = pattern.to_lowercase();
            }

            // Parse the wildcard pattern
            let wildcard_pattern = WildcardPattern::parse(&pattern);

            debug_println!("RUST DEBUG:   -> Extracted wildcard term: field={}, pattern={:?}", base_field, wildcard_pattern);

            terms.push(PrescanTerm {
                field: base_field,
                original_field,
                term: pattern,
                occurrence: occurrence.clone(),
                match_type: TermMatchType::Wildcard(wildcard_pattern),
                json_path,
            });
        }

        // Range queries, regex, etc. - skip for now (conservative: assume could match)
        QueryAst::Range(_) => {
            debug_println!("RUST DEBUG:   -> Range query - handled conservatively (no terms extracted)");
            // Don't extract terms - these are handled conservatively
        }
        QueryAst::Regex(regex_query) => {
            debug_println!("RUST DEBUG:   -> Regex(field='{}', regex='{}')", regex_query.field, regex_query.regex);
            let original_field = regex_query.field.clone();
            let (base_field, json_path) = doc_mapping.parse_json_path(&regex_query.field);
            let regex_pattern = regex_query.regex.clone();

            debug_println!("RUST DEBUG:   -> Extracted regex term: field={}, regex={}", base_field, regex_pattern);

            terms.push(PrescanTerm {
                field: base_field,
                original_field,
                term: regex_pattern.clone(),
                occurrence: occurrence.clone(),
                match_type: TermMatchType::Regex(regex_pattern),
                json_path,
            });
        }
        QueryAst::UserInput(_) => {
            debug_println!("RUST DEBUG:   -> UserInput query - handled conservatively (no terms extracted)");
            // Don't extract terms - these are handled conservatively
        }
        QueryAst::FieldPresence(_) => {
            debug_println!("RUST DEBUG:   -> FieldPresence query - handled conservatively (no terms extracted)");
            // Don't extract terms - these are handled conservatively
        }
    }
}

/// Evaluate prescan results by recursively evaluating the query AST structure
/// This properly handles nested boolean queries unlike a flattened approach
pub fn evaluate_prescan_recursive(
    query_ast: &QueryAst,
    term_existence: &HashMap<String, bool>,
    doc_mapping: &DocMapping,
) -> bool {
    match query_ast {
        QueryAst::MatchAll => {
            debug_println!("RUST DEBUG: evaluate_prescan: MatchAll -> true");
            true
        }
        QueryAst::MatchNone => {
            debug_println!("RUST DEBUG: evaluate_prescan: MatchNone -> false");
            false
        }

        QueryAst::Term(term_query) => {
            let field = &term_query.field;
            let mut term_value = term_query.value.clone();

            // Normalize term based on tokenizer
            if doc_mapping.uses_lowercase_tokenizer(field) {
                term_value = term_value.to_lowercase();
            }

            let key = format!("{}:{}", field, term_value);
            let exists = term_existence.get(&key).copied().unwrap_or(false);
            debug_println!("RUST DEBUG: evaluate_prescan: Term({}) -> {}", key, exists);
            exists
        }

        QueryAst::TermSet(term_set_query) => {
            // At least one term in the set must exist
            for (field, field_terms) in &term_set_query.terms_per_field {
                let uses_lowercase = doc_mapping.uses_lowercase_tokenizer(field);
                for term_value in field_terms {
                    let mut value = term_value.clone();
                    if uses_lowercase {
                        value = value.to_lowercase();
                    }
                    let key = format!("{}:{}", field, value);
                    if term_existence.get(&key).copied().unwrap_or(false) {
                        debug_println!("RUST DEBUG: evaluate_prescan: TermSet has matching term {} -> true", key);
                        return true;
                    }
                }
            }
            debug_println!("RUST DEBUG: evaluate_prescan: TermSet no matches -> false");
            false
        }

        QueryAst::PhrasePrefix(phrase_query) => {
            // Phrase queries contain a string that gets tokenized
            // For prescan, check if each word in the phrase exists
            // Conservative: treat as AND of all words in the phrase
            let field = &phrase_query.field;
            let uses_lowercase = doc_mapping.uses_lowercase_tokenizer(field);

            // Split phrase into words (simple tokenization)
            for term_value in phrase_query.phrase.split_whitespace() {
                let mut value = term_value.to_string();
                if uses_lowercase {
                    value = value.to_lowercase();
                }
                let key = format!("{}:{}", field, value);
                if !term_existence.get(&key).copied().unwrap_or(false) {
                    debug_println!("RUST DEBUG: evaluate_prescan: PhrasePrefix missing {} -> false", key);
                    return false;
                }
            }
            debug_println!("RUST DEBUG: evaluate_prescan: PhrasePrefix all terms exist -> true");
            true
        }

        QueryAst::Bool(bool_query) => {
            // Evaluate ALL must clauses - all must be true
            for must_clause in &bool_query.must {
                if !evaluate_prescan_recursive(must_clause, term_existence, doc_mapping) {
                    debug_println!("RUST DEBUG: evaluate_prescan: Bool MUST clause failed -> false");
                    return false;
                }
            }

            // Evaluate FILTER clauses (same as MUST)
            for filter_clause in &bool_query.filter {
                if !evaluate_prescan_recursive(filter_clause, term_existence, doc_mapping) {
                    debug_println!("RUST DEBUG: evaluate_prescan: Bool FILTER clause failed -> false");
                    return false;
                }
            }

            // Handle SHOULD clauses with minimum_should_match support
            if !bool_query.should.is_empty() {
                // Count how many SHOULD clauses are satisfied
                let matching_should_count = bool_query.should.iter()
                    .filter(|should_clause| {
                        evaluate_prescan_recursive(should_clause, term_existence, doc_mapping)
                    })
                    .count();

                // Determine required minimum - defaults to 1 if not specified
                let minimum_required = bool_query.minimum_should_match.unwrap_or(1);

                debug_println!("RUST DEBUG: evaluate_prescan: Bool SHOULD clauses: {} matching out of {}, minimum_should_match={}",
                    matching_should_count, bool_query.should.len(), minimum_required);

                if matching_should_count < minimum_required {
                    debug_println!("RUST DEBUG: evaluate_prescan: Bool SHOULD count {} < minimum {} -> false",
                        matching_should_count, minimum_required);
                    return false;
                }
            }

            // MUST_NOT is ignored for prescan (conservative)
            debug_println!("RUST DEBUG: evaluate_prescan: Bool all conditions passed -> true");
            true
        }

        QueryAst::Boost { underlying, .. } => {
            // Boost just wraps another query
            evaluate_prescan_recursive(underlying.as_ref(), term_existence, doc_mapping)
        }

        QueryAst::Wildcard(wildcard_query) => {
            let field = &wildcard_query.field;
            let mut pattern = wildcard_query.value.clone();

            if doc_mapping.uses_lowercase_tokenizer(field) {
                pattern = pattern.to_lowercase();
            }

            let key = format!("{}:{}", field, pattern);
            let exists = term_existence.get(&key).copied().unwrap_or(false);
            debug_println!("RUST DEBUG: evaluate_prescan: Wildcard({}) -> {}", key, exists);
            exists
        }

        // Conservative: Range, UserInput, FieldPresence queries return true
        QueryAst::Range(_) => {
            debug_println!("RUST DEBUG: evaluate_prescan: Range -> true (conservative)");
            true
        }
        QueryAst::Regex(regex_query) => {
            // Regex queries are now checked via FST automaton
            let (field, _) = doc_mapping.parse_json_path(&regex_query.field);
            let key = format!("{}:{}", field, regex_query.regex);
            let exists = term_existence.get(&key).copied().unwrap_or(true); // Conservative default
            debug_println!("RUST DEBUG: evaluate_prescan: Regex({}) -> {}", key, exists);
            exists
        }
        QueryAst::UserInput(_) => {
            debug_println!("RUST DEBUG: evaluate_prescan: UserInput -> true (conservative)");
            true
        }
        QueryAst::FieldPresence(_) => {
            debug_println!("RUST DEBUG: evaluate_prescan: FieldPresence -> true (conservative)");
            true
        }
        QueryAst::FullText(full_text) => {
            // Check if this is a phrase query - if so, check all terms exist
            match &full_text.params.mode {
                quickwit_query::query_ast::FullTextMode::Phrase { .. } |
                quickwit_query::query_ast::FullTextMode::PhraseFallbackToIntersection => {
                    // For phrase queries, ALL terms must exist
                    let uses_lowercase = doc_mapping.uses_lowercase_tokenizer(&full_text.field);
                    for word in full_text.text.split_whitespace() {
                        let mut term_value = word.to_string();
                        if uses_lowercase {
                            term_value = term_value.to_lowercase();
                        }
                        let key = format!("{}:{}", full_text.field, term_value);
                        if !term_existence.get(&key).copied().unwrap_or(false) {
                            debug_println!("RUST DEBUG: evaluate_prescan: FullText phrase term '{}' not found -> false", key);
                            return false;
                        }
                    }
                    debug_println!("RUST DEBUG: evaluate_prescan: FullText phrase all terms exist -> true");
                    true
                }
                _ => {
                    debug_println!("RUST DEBUG: evaluate_prescan: FullText (non-phrase) -> true (conservative)");
                    true
                }
            }
        }
    }
}

/// Extract split_id from URL (filename without .split extension)
fn extract_split_id(url: &str) -> Option<String> {
    let filename = extract_filename(url)?;
    if filename.ends_with(".split") {
        Some(filename[..filename.len() - 6].to_string())
    } else {
        Some(filename)
    }
}

// NOTE: Old prescan_single_split function using FstCache has been removed.
// We now use prescan_single_split_v2 which uses Quickwit's open_index_with_caches
// for correct term dictionary format handling (SSTable vs FST).

/// Prescan a single split using Quickwit's open_index_with_caches
///
/// This approach uses Quickwit's battle-tested code path for opening splits
/// and ensures correct term dictionary format handling (SSTable vs FST).
///
/// Benefits over FstCache approach:
/// 1. Correct term dictionary format handling (uses Quickwit's SSTable support)
/// 2. Populated caches for subsequent searches (shared SearcherContext)
/// 3. Optional fast field warmup for better subsequent search performance
async fn prescan_single_split_v2(
    split_info: &SplitInfo,
    prescan_terms: &[PrescanTerm],
    query_ast: &QueryAst,
    doc_mapping: &DocMapping,
    storage: Arc<dyn Storage>,
    searcher_context: Arc<quickwit_search::SearcherContext>,
    warmup_fast_fields: bool,
    pre_cached_searcher: Option<Arc<tantivy::Searcher>>,  // Pre-looked-up searcher to avoid lock contention
) -> PrescanResult {
    debug_println!("RUST DEBUG: Prescan V2: Processing split {} (using open_index_with_caches)", split_info.split_url);
    debug_println!("RUST DEBUG: Prescan V2: warmup_fast_fields={}", warmup_fast_fields);

    // If no terms to check, conservatively include the split
    if prescan_terms.is_empty() {
        return PrescanResult::success(
            split_info.split_url.clone(),
            true,
            HashMap::new(),
        );
    }

    // Extract split_id from URL
    let split_id = match extract_split_id(&split_info.split_url) {
        Some(id) => id,
        None => {
            return PrescanResult::error(
                split_info.split_url.clone(),
                "Could not extract split_id from URL",
            );
        }
    };

    debug_println!("RUST DEBUG: Prescan V2: split_id={}", split_id);

    // 🚀 OPTIMIZATION: Use pre-cached searcher if available (avoids lock contention)
    // The caller pre-looks-up searchers ONCE per unique URL before the parallel loop,
    // so 5000 identical splits don't contend on the cache lock.
    let searcher: Arc<tantivy::Searcher> = if let Some(cached) = pre_cached_searcher {
        SEARCHER_CACHE_HITS.fetch_add(1, Ordering::Relaxed);
        debug_println!("RUST DEBUG: Prescan V2: USING PRE-CACHED SEARCHER");
        cached
    } else {
        SEARCHER_CACHE_MISSES.fetch_add(1, Ordering::Relaxed);
        debug_println!("RUST DEBUG: Prescan V2: SEARCHER CACHE MISS - need to open index");

        // Use the file_size provided in SplitInfo (required field, no HEAD request needed)
        let file_size = split_info.file_size;
        debug_println!("RUST DEBUG: Prescan V2: file_size={}, split_footer_start={}", file_size, split_info.footer_offset);

        // Validate footer offset is within file bounds
        if split_info.footer_offset >= file_size {
            return PrescanResult::error(
                split_info.split_url.clone(),
                &format!("Invalid footer offset: {} >= file_size {}", split_info.footer_offset, file_size),
            );
        }

        // Create SplitIdAndFooterOffsets for open_index_with_caches
        let split_and_footer = SplitIdAndFooterOffsets {
            split_id: split_id.clone(),
            split_footer_start: split_info.footer_offset,
            split_footer_end: file_size, // Use file size as footer end
            timestamp_start: None,
            timestamp_end: None,
            num_docs: 0, // Unknown at this point
        };

        // 🚀 CACHE FIX: Create ByteRangeCache for prescan (same pattern as search path)
        // This cache stores byte ranges fetched from S3, preventing repeated fetches
        let byte_range_cache = ByteRangeCache::with_infinite_capacity(
            &STORAGE_METRICS.shortlived_cache,
        );
        debug_println!("RUST DEBUG: Prescan V2: Created ByteRangeCache for caching");

        // Open the index using Quickwit's open_index_with_caches
        // This only downloads the footer and hotcache metadata (NOT fast fields)
        let (index, _hot_directory) = match open_index_with_caches(
            &searcher_context,
            storage.clone(),
            &split_and_footer,
            None, // No custom tokenizer manager
            Some(byte_range_cache), // 🚀 Pass ByteRangeCache to cache S3 fetches
        ).await {
            Ok(result) => result,
            Err(e) => {
                return PrescanResult::error(
                    split_info.split_url.clone(),
                    &format!("Failed to open index: {}", e),
                );
            }
        };

        // Get a Tantivy Searcher
        let reader = match index.reader() {
            Ok(r) => r,
            Err(e) => {
                return PrescanResult::error(
                    split_info.split_url.clone(),
                    &format!("Failed to create reader: {}", e),
                );
            }
        };
        let new_searcher = Arc::new(reader.searcher());

        // Collect unique fields we need to check for warmup
        let fields_to_check: HashSet<String> = prescan_terms.iter()
            .map(|t| t.field.clone())
            .collect();

        debug_println!("RUST DEBUG: Prescan V2: Need to warmup {} unique fields: {:?}",
                       fields_to_check.len(), fields_to_check);

        // Build WarmupInfo with only term_dict_fields (and optionally fast_fields)
        let schema = new_searcher.schema();
        let mut term_dict_fields: HashSet<Field> = HashSet::new();

        for field_name in &fields_to_check {
            if let Ok(field) = schema.get_field(field_name) {
                term_dict_fields.insert(field);
            } else {
                debug_println!("RUST DEBUG: Prescan V2: Field '{}' not found in schema", field_name);
            }
        }

        // Build WarmupInfo
        let mut warmup_info = WarmupInfo {
            term_dict_fields,
            ..WarmupInfo::default()
        };

        // Optionally include fast fields if requested
        if warmup_fast_fields {
            // Add all fields from doc_mapping as fast fields
            for field_mapping in &doc_mapping.field_mappings {
                if let Ok(_field) = schema.get_field(&field_mapping.name) {
                    warmup_info.fast_fields.insert(quickwit_doc_mapper::FastFieldWarmupInfo {
                        name: field_mapping.name.clone(),
                        with_subfields: false,
                    });
                    debug_println!("RUST DEBUG: Prescan V2: Adding fast field warmup for '{}'", field_mapping.name);
                }
            }
        }

        // Perform warmup - this downloads only what's specified in warmup_info
        debug_println!("RUST DEBUG: Prescan V2: Performing warmup with {} term_dict_fields, {} fast_fields",
                       warmup_info.term_dict_fields.len(), warmup_info.fast_fields.len());

        if let Err(e) = warmup(&*new_searcher, &warmup_info).await {
            return PrescanResult::error(
                split_info.split_url.clone(),
                &format!("Warmup failed: {}", e),
            );
        }

        debug_println!("RUST DEBUG: Prescan V2: Warmup complete, caching searcher");

        // Cache the searcher for future prescan and regular search operations
        cache_searcher(split_info.split_url.clone(), new_searcher.clone());
        debug_println!("RUST DEBUG: Prescan V2: Searcher cached for {}", split_info.split_url);

        new_searcher
    };

    // Now we have a searcher (either from cache or newly created)
    let schema = searcher.schema();

    // Collect unique fields we need to check
    let fields_to_check: HashSet<String> = prescan_terms.iter()
        .map(|t| t.field.clone())
        .collect();

    debug_println!("RUST DEBUG: Prescan V2: Checking term existence in {} fields", fields_to_check.len());

    // Debug: Print number of segment readers
    let num_segments = searcher.segment_readers().len();
    debug_println!("RUST DEBUG: Prescan V2: Index has {} segment readers", num_segments);

    // Debug: Print schema fields
    let field_names: Vec<String> = schema.fields().map(|(f, e)| format!("{}:{}", e.name(), f.field_id())).collect();
    debug_println!("RUST DEBUG: Prescan V2: Schema fields: {:?}", field_names);

    // Check term existence for each term
    let mut term_existence = HashMap::new();

    for prescan_term in prescan_terms {
        // Use original_field for the key (e.g., "metadata.color" not "metadata")
        // This ensures evaluate_prescan_recursive can find the term by its query field name
        let key = format!("{}:{}", prescan_term.original_field, prescan_term.term);
        debug_println!("RUST DEBUG: Prescan V2: Checking term '{}' in field '{}' (original: {}), json_path={:?}",
            prescan_term.term, prescan_term.field, prescan_term.original_field, prescan_term.json_path);

        // Get field from schema
        let field = match schema.get_field(&prescan_term.field) {
            Ok(f) => f,
            Err(e) => {
                debug_println!("RUST DEBUG: Prescan V2: Field '{}' not in schema, term doesn't exist: {}", prescan_term.field, e);
                term_existence.insert(key, false);
                continue;
            }
        };

        // Determine the field type from doc_mapping
        let field_type = doc_mapping.get_field_type(&prescan_term.field)
            .map(PrescanFieldType::from_str)
            .unwrap_or(PrescanFieldType::Text);

        debug_println!("RUST DEBUG: Prescan V2: Field '{}' has type {:?}", prescan_term.field, field_type);

        // For JSON fields WITHOUT a path, we can't construct the proper term - return conservative true
        if field_type == PrescanFieldType::Json && prescan_term.json_path.is_none() {
            debug_println!("RUST DEBUG: Prescan V2: Field '{}' is JSON without path - returning conservative true", prescan_term.field);
            term_existence.insert(key, true);
            continue;
        }

        // Get expand_dots setting for JSON fields
        let expand_dots = if field_type == PrescanFieldType::Json {
            match schema.get_field_entry(field).field_type() {
                tantivy::schema::FieldType::JsonObject(json_opts) => json_opts.is_expand_dots_enabled(),
                _ => false,
            }
        } else {
            false
        };

        // Check term existence in each segment
        let mut exists = false;
        for (seg_idx, segment_reader) in searcher.segment_readers().iter().enumerate() {
            let inverted_index = match segment_reader.inverted_index(field) {
                Ok(ii) => ii,
                Err(e) => {
                    debug_println!("RUST DEBUG: Prescan V2: Failed to get inverted index for segment {}: {}", seg_idx, e);
                    continue;
                },
            };

            let term_dict = inverted_index.terms();

            // Check based on match type (use async for StorageDirectory compatibility)
            let segment_has_term = match &prescan_term.match_type {
                TermMatchType::Exact => {
                    check_exact_term_exists_v2(
                        term_dict, field, &prescan_term.term,
                        &field_type, prescan_term.json_path.as_deref(), expand_dots
                    ).await
                }
                TermMatchType::Wildcard(pattern) => {
                    // For wildcards on non-text fields, return conservative true (complex to implement)
                    if field_type != PrescanFieldType::Text {
                        debug_println!("RUST DEBUG: Prescan V2: Wildcard on non-text field - returning conservative true");
                        true
                    } else {
                        check_wildcard_exists_v2(term_dict, field, pattern, &prescan_term.term).await
                    }
                }
                TermMatchType::Regex(pattern) => {
                    // For regex on non-text fields, return conservative true
                    if field_type != PrescanFieldType::Text {
                        debug_println!("RUST DEBUG: Prescan V2: Regex on non-text field - returning conservative true");
                        true
                    } else {
                        check_regex_exists_v2(term_dict, pattern).await
                    }
                }
            };

            if segment_has_term {
                exists = true;
                break;
            }
        }

        debug_println!("RUST DEBUG: Prescan V2: Term {} exists={}", key, exists);
        term_existence.insert(key, exists);
    }

    // Evaluate if split could have results based on term existence
    debug_println!("RUST DEBUG: Prescan V2: term_existence map: {:?}", term_existence);
    let could_have_results = evaluate_prescan_recursive(query_ast, &term_existence, doc_mapping);
    debug_println!("RUST DEBUG: Prescan V2: Split {} could_have_results={}", split_info.split_url, could_have_results);

    PrescanResult::success(
        split_info.split_url.clone(),
        could_have_results,
        term_existence,
    )
}

/// Helper function to parse an IPv4 address to IPv6 (mapped)
fn parse_ip_to_v6(text: &str) -> Option<Ipv6Addr> {
    // Try parsing as IPv6 first
    if let Ok(ipv6) = text.parse::<Ipv6Addr>() {
        return Some(ipv6);
    }
    // Try parsing as IPv4 and convert to IPv4-mapped IPv6
    if let Ok(ipv4) = text.parse::<Ipv4Addr>() {
        return Some(ipv4.to_ipv6_mapped());
    }
    // Try parsing as IpAddr enum
    if let Ok(ip) = text.parse::<IpAddr>() {
        return Some(match ip {
            IpAddr::V4(v4) => v4.to_ipv6_mapped(),
            IpAddr::V6(v6) => v6,
        });
    }
    None
}

/// Construct the proper Term based on field type (following Quickwit's pattern from utils.rs)
fn construct_term_for_field_type(
    field: Field,
    term_text: &str,
    field_type: &PrescanFieldType,
    json_path: Option<&str>,
    expand_dots: bool,
) -> Option<Term> {
    match field_type {
        PrescanFieldType::Text => {
            // Text fields use from_field_text
            Some(Term::from_field_text(field, term_text))
        }
        PrescanFieldType::I64 => {
            // Parse as i64 and use from_field_i64
            term_text.parse::<i64>().ok().map(|val| Term::from_field_i64(field, val))
        }
        PrescanFieldType::U64 => {
            // Parse as u64 and use from_field_u64
            term_text.parse::<u64>().ok().map(|val| Term::from_field_u64(field, val))
        }
        PrescanFieldType::F64 => {
            // Parse as f64 and use from_field_f64
            term_text.parse::<f64>().ok().map(|val| Term::from_field_f64(field, val))
        }
        PrescanFieldType::Bool => {
            // Parse as bool and use from_field_bool
            term_text.parse::<bool>().ok().map(|val| Term::from_field_bool(field, val))
        }
        PrescanFieldType::DateTime => {
            // Parse as RFC3339 datetime and use from_field_date_for_search
            // Following Quickwit's pattern from utils.rs line 144-148
            if let Ok(dt) = OffsetDateTime::parse(term_text, &Rfc3339) {
                let tantivy_dt = TantivyDateTime::from_utc(dt.to_offset(time::UtcOffset::UTC));
                // Use from_field_date_for_search for search compatibility (truncates to indexed precision)
                Some(Term::from_field_date_for_search(field, tantivy_dt))
            } else {
                // Also try parsing millisecond timestamps
                if let Ok(millis) = term_text.parse::<i64>() {
                    let tantivy_dt = TantivyDateTime::from_timestamp_millis(millis);
                    Some(Term::from_field_date_for_search(field, tantivy_dt))
                } else {
                    None
                }
            }
        }
        PrescanFieldType::IpAddr => {
            // Parse as IP address (supports both IPv4 and IPv6)
            // Tantivy stores IPs as IPv6 internally (IPv4 is mapped)
            parse_ip_to_v6(term_text).map(|ipv6| Term::from_field_ip_addr(field, ipv6))
        }
        PrescanFieldType::Json => {
            // For JSON fields, we need a path to construct the term
            if let Some(path) = json_path {
                // Create JSON term with path using Quickwit's pattern
                let json_term = Term::from_field_json_path(field, path, expand_dots);
                // Try to convert value to fast value (numeric, bool, datetime) first
                if let Some(fast_term) = convert_to_fast_value_and_append_to_json_term(json_term.clone(), term_text, true) {
                    debug_println!("RUST DEBUG: JSON term with fast value for path='{}', value='{}'", path, term_text);
                    Some(fast_term)
                } else {
                    // Fall back to text term for JSON path
                    let mut text_term = json_term;
                    text_term.append_type_and_str(term_text);
                    debug_println!("RUST DEBUG: JSON term with text value for path='{}', value='{}'", path, term_text);
                    Some(text_term)
                }
            } else {
                // JSON without path - can't construct proper term
                None
            }
        }
        PrescanFieldType::Bytes => {
            // Bytes fields are rarely used and require hex decoding
            // Return None to trigger conservative true behavior
            debug_println!("RUST DEBUG: Bytes field type - returning conservative true");
            None
        }
    }
}

/// Check if exact term exists using Tantivy's native Term API (async version)
///
/// Constructs proper terms based on field type following Quickwit's pattern.
/// - Text fields: Term::from_field_text
/// - Numeric fields (i64, u64, f64): Term::from_field_i64/u64/f64
/// - Boolean fields: Term::from_field_bool
/// - DateTime fields: Term::from_field_date_for_search
/// - IP fields: Term::from_field_ip_addr
/// - JSON fields with path: Term::from_field_json_path + convert_to_fast_value_and_append_to_json_term
async fn check_exact_term_exists_v2(
    term_dict: &tantivy::termdict::TermDictionary,
    field: Field,
    term_text: &str,
    field_type: &PrescanFieldType,
    json_path: Option<&str>,
    expand_dots: bool,
) -> bool {
    // Construct the proper term based on field type
    let term = match construct_term_for_field_type(field, term_text, field_type, json_path, expand_dots) {
        Some(t) => t,
        None => {
            debug_println!(
                "RUST DEBUG: Prescan V2: Failed to construct term for type {:?}, value='{}' - returning conservative true",
                field_type, term_text
            );
            // Can't construct proper term - return conservative true
            return true;
        }
    };

    // Get the term bytes (just the value, no field prefix - per-field term dict)
    let term_bytes = term.serialized_value_bytes();

    debug_println!(
        "RUST DEBUG: Prescan V2: check_exact_term_exists: term='{}', type={:?}, bytes_len={}, json_path={:?}",
        term_text, field_type, term_bytes.len(), json_path
    );

    // Use get_async for StorageDirectory compatibility
    match term_dict.get_async(term_bytes).await {
        Ok(Some(_)) => {
            debug_println!("RUST DEBUG: Prescan V2: Term '{}' FOUND", term_text);
            true
        }
        Ok(None) => {
            debug_println!("RUST DEBUG: Prescan V2: Term '{}' NOT FOUND", term_text);
            false
        }
        Err(e) => {
            debug_println!(
                "RUST DEBUG: Prescan V2: Error checking term '{}': {} - returning true (conservative)",
                term_text, e
            );
            // Conservative: return true (could have results) on error
            // This ensures we don't incorrectly skip splits due to cache issues
            true
        }
    }
}

/// Check if any terms matching wildcard pattern exist using Tantivy's term dictionary
///
/// After warmup, the term dictionary data is cached in memory, so sync iteration works.
/// - Prefix patterns (`hel*`): Efficient range scan starting at prefix
/// - Suffix patterns (`*world`): Scan terms checking suffix (limited)
/// - Contains patterns (`*hello*`): Scan terms checking contains (limited)
/// - Complex patterns: Conservative true (would need full regex)
async fn check_wildcard_exists_v2(
    term_dict: &tantivy::termdict::TermDictionary,
    _field: Field,  // Currently unused but kept for API consistency
    pattern: &WildcardPattern,
    original_pattern: &str,
) -> bool {
    debug_println!("RUST DEBUG: Prescan V2: check_wildcard_exists: pattern='{}'", original_pattern);

    match pattern {
        WildcardPattern::Prefix(prefix) => {
            check_prefix_pattern(term_dict, prefix).await
        }
        WildcardPattern::Suffix(suffix) => {
            check_suffix_pattern(term_dict, suffix).await
        }
        WildcardPattern::Contains(substring) => {
            check_contains_pattern(term_dict, substring).await
        }
        WildcardPattern::Complex(_) => {
            // Complex patterns like "h*llo" would need regex matching
            // Return conservative true
            debug_println!("RUST DEBUG: Prescan V2: Complex wildcard '{}' - returning conservative true", original_pattern);
            true
        }
    }
}

/// Check if any term starts with the given prefix using FST automaton (async)
async fn check_prefix_pattern(
    term_dict: &tantivy::termdict::TermDictionary,
    prefix: &str,
) -> bool {
    debug_println!("RUST DEBUG: Prescan V2: Checking prefix pattern '{}*'", prefix);

    // Convert prefix to regex pattern: "prefix.*" (escaped)
    let escaped_prefix = escape_regex(prefix);
    let regex_pattern = format!("{}.*", escaped_prefix);

    debug_println!("RUST DEBUG: Prescan V2: Using regex pattern '{}'", regex_pattern);

    // Create FST automaton from regex
    let automaton = match tantivy_fst::Regex::new(&regex_pattern) {
        Ok(a) => a,
        Err(e) => {
            debug_println!("RUST DEBUG: Prescan V2: Error creating regex automaton: {} - returning conservative true", e);
            return true; // Conservative on error
        }
    };

    // Use async automaton search (required for StorageDirectory)
    let mut stream = match term_dict.search(automaton).into_stream_async().await {
        Ok(s) => s,
        Err(e) => {
            debug_println!("RUST DEBUG: Prescan V2: Error creating automaton stream: {} - returning conservative true", e);
            return true; // Conservative on error
        }
    };

    // Check if any term matches
    if stream.advance() {
        debug_println!("RUST DEBUG: Prescan V2: Prefix '{}*' FOUND - matching term exists", prefix);
        return true;
    }

    debug_println!("RUST DEBUG: Prescan V2: Prefix '{}*' NOT FOUND - no matching terms", prefix);
    false
}

/// Check if any term ends with the given suffix using FST automaton (async)
async fn check_suffix_pattern(
    term_dict: &tantivy::termdict::TermDictionary,
    suffix: &str,
) -> bool {
    debug_println!("RUST DEBUG: Prescan V2: Checking suffix pattern '*{}'", suffix);

    // Convert suffix to regex pattern: ".*suffix" (escaped)
    let escaped_suffix = escape_regex(suffix);
    let regex_pattern = format!(".*{}", escaped_suffix);

    debug_println!("RUST DEBUG: Prescan V2: Using regex pattern '{}'", regex_pattern);

    // Create FST automaton from regex
    let automaton = match tantivy_fst::Regex::new(&regex_pattern) {
        Ok(a) => a,
        Err(e) => {
            debug_println!("RUST DEBUG: Prescan V2: Error creating regex automaton for suffix: {} - returning conservative true", e);
            return true; // Conservative on error
        }
    };

    // Use async automaton search (required for StorageDirectory)
    let mut stream = match term_dict.search(automaton).into_stream_async().await {
        Ok(s) => s,
        Err(e) => {
            debug_println!("RUST DEBUG: Prescan V2: Error creating automaton stream for suffix: {} - returning conservative true", e);
            return true; // Conservative on error
        }
    };

    // Check if any term matches
    if stream.advance() {
        debug_println!("RUST DEBUG: Prescan V2: Suffix '*{}' FOUND - matching term exists", suffix);
        return true;
    }

    debug_println!("RUST DEBUG: Prescan V2: Suffix '*{}' NOT FOUND - no matching terms", suffix);
    false
}

/// Check if any term contains the given substring using FST automaton (async)
async fn check_contains_pattern(
    term_dict: &tantivy::termdict::TermDictionary,
    substring: &str,
) -> bool {
    debug_println!("RUST DEBUG: Prescan V2: Checking contains pattern '*{}*'", substring);

    // Convert substring to regex pattern: ".*substring.*" (escaped)
    let escaped_substring = escape_regex(substring);
    let regex_pattern = format!(".*{}.*", escaped_substring);

    debug_println!("RUST DEBUG: Prescan V2: Using regex pattern '{}'", regex_pattern);

    // Create FST automaton from regex
    let automaton = match tantivy_fst::Regex::new(&regex_pattern) {
        Ok(a) => a,
        Err(e) => {
            debug_println!("RUST DEBUG: Prescan V2: Error creating regex automaton for contains: {} - returning conservative true", e);
            return true; // Conservative on error
        }
    };

    // Use async automaton search (required for StorageDirectory)
    let mut stream = match term_dict.search(automaton).into_stream_async().await {
        Ok(s) => s,
        Err(e) => {
            debug_println!("RUST DEBUG: Prescan V2: Error creating automaton stream for contains: {} - returning conservative true", e);
            return true; // Conservative on error
        }
    };

    // Check if any term matches
    if stream.advance() {
        debug_println!("RUST DEBUG: Prescan V2: Contains '*{}*' FOUND - matching term exists", substring);
        return true;
    }

    debug_println!("RUST DEBUG: Prescan V2: Contains '*{}*' NOT FOUND - no matching terms", substring);
    false
}

/// Check if any term matches the given regex pattern using FST automaton (async)
async fn check_regex_exists_v2(
    term_dict: &tantivy::termdict::TermDictionary,
    pattern: &str,
) -> bool {
    debug_println!("RUST DEBUG: Prescan V2: Checking regex pattern '{}'", pattern);

    // Create FST automaton from the regex pattern directly
    // The pattern is expected to be a valid regex (from Quickwit's RegexQuery)
    let automaton = match tantivy_fst::Regex::new(pattern) {
        Ok(a) => a,
        Err(e) => {
            debug_println!("RUST DEBUG: Prescan V2: Error creating regex automaton '{}': {} - returning conservative true", pattern, e);
            return true; // Conservative on error
        }
    };

    // Use async automaton search (required for StorageDirectory)
    let mut stream = match term_dict.search(automaton).into_stream_async().await {
        Ok(s) => s,
        Err(e) => {
            debug_println!("RUST DEBUG: Prescan V2: Error creating automaton stream for regex '{}': {} - returning conservative true", pattern, e);
            return true; // Conservative on error
        }
    };

    // Check if any term matches
    if stream.advance() {
        debug_println!("RUST DEBUG: Prescan V2: Regex '{}' FOUND - matching term exists", pattern);
        return true;
    }

    debug_println!("RUST DEBUG: Prescan V2: Regex '{}' NOT FOUND - no matching terms", pattern);
    false
}

/// Construct a Tantivy term key with field_id prefix and type code
///
/// Tantivy's term dictionary stores terms as:
/// [field_id: 4 bytes BIG ENDIAN][type_code: 1 byte][term_value_bytes]
///
/// Type codes (from tantivy::schema::Type):
/// - Str = 115 ('s')
/// - U64 = 117 ('u')
/// - I64 = 105 ('i')
/// - F64 = 102 ('f')
/// - Bool = 98 ('b')
/// - Date = 100 ('d')
// NOTE: Old check_prefix_exists_fst has been removed.
// Prefix checking is now done in check_wildcard_exists_v2.

/// Extract filename from a URL
fn extract_filename(url: &str) -> Option<String> {
    // Handle s3://, azure://, file:// URLs
    if let Some(idx) = url.rfind('/') {
        Some(url[idx + 1..].to_string())
    } else {
        Some(url.to_string())
    }
}

/// Extract parent directory from a split URL
/// e.g., "s3://bucket/path/split.split" -> "s3://bucket/path/"
fn get_parent_directory(split_url: &str) -> String {
    if let Some(last_slash) = split_url.rfind('/') {
        split_url[..last_slash + 1].to_string()
    } else {
        split_url.to_string()
    }
}

/// Resolve storage for a parent directory URL
/// This is used to pre-resolve storage instances once per unique directory
async fn resolve_storage_for_parent(
    storage_resolver: &StorageResolver,
    parent_dir: &str,
) -> Result<Arc<dyn Storage>> {
    use quickwit_common::uri::Uri;

    let uri: Uri = parent_dir.parse()
        .with_context(|| format!("Failed to parse parent URI: {}", parent_dir))?;

    storage_resolver.resolve(&uri).await
        .with_context(|| format!("Failed to resolve storage for parent: {}", parent_dir))
}

/// Resolve storage for a split URL
async fn resolve_storage_for_prescan(
    storage_resolver: &StorageResolver,
    split_url: &str,
) -> Result<Arc<dyn Storage>> {
    use quickwit_common::uri::Uri;

    // Get parent directory URI
    let parent_uri = if let Some(last_slash) = split_url.rfind('/') {
        let parent = &split_url[..last_slash + 1];
        parent.to_string()
    } else {
        return Err(anyhow!("Invalid split URL: {}", split_url));
    };

    let uri: Uri = parent_uri.parse()
        .with_context(|| format!("Failed to parse URI: {}", parent_uri))?;

    storage_resolver.resolve(&uri).await
        .with_context(|| format!("Failed to resolve storage for: {}", parent_uri))
}

/// Main prescan function that processes multiple splits in parallel
///
/// Uses Quickwit's `open_index_with_caches` and `warmup` to:
/// 1. Open each split (downloads only footer/hotcache metadata)
/// 2. Warmup ONLY term dictionaries (not fast fields by default)
/// 3. Check term existence using Tantivy's TermDictionary API
///
/// This ensures correct term dictionary format handling (SSTable vs FST)
/// and populates Quickwit's caches for subsequent searches.
///
/// Set `warmup_fast_fields=true` to also cache fast fields during prescan.
pub async fn prescan_splits_async(
    splits: Vec<SplitInfo>,
    doc_mapping_json: &str,
    query_ast_json: &str,
    warmup_fast_fields: bool,
    s3_config: Option<quickwit_config::S3StorageConfig>,
    azure_config: Option<quickwit_config::AzureStorageConfig>,
) -> Result<Vec<PrescanResult>> {
    debug_println!("RUST DEBUG: prescan_splits_async called with {} splits (using open_index_with_caches)", splits.len());
    debug_println!("RUST DEBUG: warmup_fast_fields = {}", warmup_fast_fields);
    debug_println!("RUST DEBUG: query_ast_json = {}", query_ast_json);

    // Parse doc mapping ONCE before parallel processing, wrap in Arc for sharing
    let doc_mapping = Arc::new(
        DocMapping::from_json(doc_mapping_json)
            .context("Failed to parse doc mapping")?
    );
    debug_println!("RUST DEBUG: Parsed doc mapping with {} fields (shared via Arc)", doc_mapping.field_mappings.len());

    // Parse query AST ONCE before parallel processing, wrap in Arc for sharing
    let query_ast: Arc<QueryAst> = Arc::new(
        serde_json::from_str(query_ast_json)
            .context("Failed to parse query AST")?
    );
    debug_println!("RUST DEBUG: Parsed query_ast (shared via Arc)");

    // Extract terms for prescan ONCE, wrap in Arc for sharing
    let prescan_terms = Arc::new(extract_prescan_terms(&query_ast, &doc_mapping));
    debug_println!("RUST DEBUG: Extracted {} prescan terms (shared via Arc)", prescan_terms.len());
    for term in prescan_terms.iter() {
        debug_println!("RUST DEBUG:   Term: field='{}', term='{}', occurrence={:?}, match_type={:?}",
            term.field, term.term, term.occurrence, term.match_type);
    }

    // Get storage resolver
    let storage_resolver = get_configured_storage_resolver_async(s3_config, azure_config).await;

    // Get the global SearcherContext (shared caches for all splits)
    let searcher_context = get_global_searcher_context();
    debug_println!("RUST DEBUG: Using global SearcherContext with shared caches");

    // PRE-RESOLVE: Collect unique parent directories and resolve storage ONCE per directory
    // This prevents creating N storage instances for N splits in the same directory
    let mut parent_to_storage: std::collections::HashMap<String, Arc<dyn quickwit_storage::Storage>> =
        std::collections::HashMap::new();

    for split_info in &splits {
        let parent_dir = get_parent_directory(&split_info.split_url);
        if !parent_to_storage.contains_key(&parent_dir) {
            debug_println!("RUST DEBUG: Pre-resolving storage for parent directory: {}", parent_dir);
            match resolve_storage_for_parent(&storage_resolver, &parent_dir).await {
                Ok(storage) => {
                    parent_to_storage.insert(parent_dir, storage);
                }
                Err(e) => {
                    debug_println!("RUST DEBUG: Failed to pre-resolve storage for {}: {}", parent_dir, e);
                    // Will handle individual failures in the parallel loop
                }
            }
        }
    }
    debug_println!("RUST DEBUG: Pre-resolved {} unique storage instances for {} splits",
        parent_to_storage.len(), splits.len());

    // Wrap in Arc for sharing across parallel tasks
    let parent_to_storage = Arc::new(parent_to_storage);

    // PRE-CACHE: Look up cached searchers ONCE per unique URL (avoids lock contention)
    // With 5000 identical splits, this does ONE cache lookup instead of 5000 contending locks
    let mut url_to_searcher: std::collections::HashMap<String, Option<Arc<tantivy::Searcher>>> =
        std::collections::HashMap::new();

    for split_info in &splits {
        if !url_to_searcher.contains_key(&split_info.split_url) {
            let cached = get_cached_searcher(&split_info.split_url);
            url_to_searcher.insert(split_info.split_url.clone(), cached);
        }
    }
    debug_println!("RUST DEBUG: Pre-cached {} unique searcher lookups for {} splits",
        url_to_searcher.len(), splits.len());

    // Wrap in Arc for sharing across parallel tasks
    let url_to_searcher = Arc::new(url_to_searcher);

    // Calculate number of worker threads (one per CPU core)
    let num_workers = get_num_cpus();
    let num_splits = splits.len();

    debug_println!("RUST DEBUG: Splitting {} splits across {} worker threads", num_splits, num_workers);

    // Split the list N ways (one chunk per worker)
    let chunk_size = (num_splits + num_workers - 1) / num_workers;
    let chunks: Vec<Vec<SplitInfo>> = splits
        .chunks(chunk_size)
        .map(|c| c.to_vec())
        .collect();

    debug_println!("RUST DEBUG: Created {} chunks of ~{} splits each", chunks.len(), chunk_size);

    // Create a worker task for each chunk - each worker processes its splits sequentially
    // All Arc clones are cheap (just reference count increment)
    let worker_tasks: Vec<_> = chunks
        .into_iter()
        .enumerate()
        .map(|(worker_id, chunk)| {
            let terms = Arc::clone(&prescan_terms);
            let query_ast_clone = Arc::clone(&query_ast);
            let doc_mapping_clone = Arc::clone(&doc_mapping);
            let resolver = storage_resolver.clone();
            let ctx = Arc::clone(&searcher_context);
            let do_warmup_fast_fields = warmup_fast_fields;
            let storage_map = Arc::clone(&parent_to_storage);
            let searcher_map = Arc::clone(&url_to_searcher);

            async move {
                let chunk_len = chunk.len();
                debug_println!("RUST DEBUG: Worker {} starting with {} splits", worker_id, chunk_len);

                let mut results = Vec::with_capacity(chunk_len);

                // Process each split in this chunk sequentially
                for split_info in chunk {
                    // Get pre-resolved storage for this split's parent directory
                    let parent_dir = get_parent_directory(&split_info.split_url);
                    let storage = match storage_map.get(&parent_dir) {
                        Some(s) => s.clone(),
                        None => {
                            // Fallback: resolve storage if not pre-resolved
                            debug_println!("RUST DEBUG: Worker {}: Storage not pre-resolved for {}", worker_id, parent_dir);
                            match resolve_storage_for_prescan(&resolver, &split_info.split_url).await {
                                Ok(s) => s,
                                Err(e) => {
                                    results.push(PrescanResult::error(
                                        split_info.split_url.clone(),
                                        &format!("Failed to resolve storage: {}", e),
                                    ));
                                    continue;
                                }
                            }
                        }
                    };

                    // Get pre-cached searcher
                    let pre_cached = searcher_map.get(&split_info.split_url).cloned().flatten();

                    // Prescan the split
                    let result = prescan_single_split_v2(
                        &split_info,
                        &terms,
                        &query_ast_clone,
                        &doc_mapping_clone,
                        storage,
                        ctx.clone(),
                        do_warmup_fast_fields,
                        pre_cached,
                    ).await;

                    results.push(result);
                }

                debug_println!("RUST DEBUG: Worker {} completed {} splits", worker_id, results.len());
                results
            }
        })
        .collect();

    // Execute all worker tasks in parallel, then merge results
    let chunk_results: Vec<Vec<PrescanResult>> = join_all(worker_tasks).await;

    // Flatten all chunk results into a single vector (maintains order within chunks)
    let results: Vec<PrescanResult> = chunk_results.into_iter().flatten().collect();

    debug_println!("RUST DEBUG: prescan_splits_async completed with {} results", results.len());
    Ok(results)
}

/// Synchronous wrapper for JNI
pub fn prescan_splits_sync(
    splits: Vec<SplitInfo>,
    doc_mapping_json: String,
    query_ast_json: String,
    warmup_fast_fields: bool,
    s3_config: Option<quickwit_config::S3StorageConfig>,
    azure_config: Option<quickwit_config::AzureStorageConfig>,
) -> Result<Vec<PrescanResult>> {
    let runtime_manager = QuickwitRuntimeManager::global();

    runtime_manager.block_on(async move {
        prescan_splits_async(
            splits,
            &doc_mapping_json,
            &query_ast_json,
            warmup_fast_fields,
            s3_config,
            azure_config,
        ).await
    })
}

// ================================
// JNI Bridge Function
// ================================

use jni::JNIEnv;
use jni::objects::{JClass, JString};
use jni::sys::jstring;

/// JNI entry point for prescan operation
///
/// @param warmup_fast_fields - If true, also cache fast fields during prescan for subsequent searches
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativePrescanSplits(
    mut env: JNIEnv,
    _class: JClass,
    cache_manager_ptr: jni::sys::jlong,
    split_infos_json: JString,
    doc_mapping_json: JString,
    query_ast_json: JString,
    warmup_fast_fields: jni::sys::jboolean,
) -> jstring {
    // Force output to trace execution
    debug_println!("PRESCAN TRACE: nativePrescanSplits JNI entry point called");
    debug_println!("RUST DEBUG: nativePrescanSplits called, warmup_fast_fields={}", warmup_fast_fields != 0);

    let do_warmup_fast_fields = warmup_fast_fields != 0;
    let result = prescan_splits_jni(&mut env, cache_manager_ptr, split_infos_json, doc_mapping_json, query_ast_json, do_warmup_fast_fields);

    match result {
        Ok(json_result) => {
            match env.new_string(&json_result) {
                Ok(jstring) => jstring.into_raw(),
                Err(e) => {
                    debug_println!("RUST DEBUG: Failed to create result string: {}", e);
                    std::ptr::null_mut()
                }
            }
        }
        Err(e) => {
            debug_println!("RUST DEBUG: Prescan error: {}", e);
            // Return error as JSON
            let error_json = format!(r#"[{{"splitUrl":"error","couldHaveResults":true,"status":"ERROR","errorMessage":"{}"}}]"#,
                                     e.to_string().replace('"', "\\\""));
            match env.new_string(&error_json) {
                Ok(jstring) => jstring.into_raw(),
                Err(_) => std::ptr::null_mut()
            }
        }
    }
}

fn prescan_splits_jni(
    env: &mut JNIEnv,
    cache_manager_ptr: jni::sys::jlong,
    split_infos_json: JString,
    doc_mapping_json: JString,
    query_ast_json: JString,
    warmup_fast_fields: bool,
) -> Result<String> {
    // Extract Java strings
    let split_infos_str: String = env.get_string(&split_infos_json)
        .map_err(|e| anyhow!("Failed to get split_infos_json: {}", e))?
        .into();

    let doc_mapping_str: String = env.get_string(&doc_mapping_json)
        .map_err(|e| anyhow!("Failed to get doc_mapping_json: {}", e))?
        .into();

    let query_ast_str: String = env.get_string(&query_ast_json)
        .map_err(|e| anyhow!("Failed to get query_ast_json: {}", e))?
        .into();

    debug_println!("RUST DEBUG: Parsing {} bytes of split infos, warmup_fast_fields={}", split_infos_str.len(), warmup_fast_fields);

    // Parse split infos from JSON
    let splits: Vec<SplitInfo> = serde_json::from_str(&split_infos_str)
        .map_err(|e| anyhow!("Failed to parse split infos JSON: {}", e))?;

    debug_println!("RUST DEBUG: Parsed {} splits", splits.len());

    // Get S3/Azure config from cache manager if available
    let (s3_config, azure_config) = get_config_from_cache_manager(cache_manager_ptr);

    // Execute prescan
    let results = prescan_splits_sync(
        splits,
        doc_mapping_str,
        query_ast_str,
        warmup_fast_fields,
        s3_config,
        azure_config,
    )?;

    // Serialize results to JSON
    let json_result = serde_json::to_string(&results)
        .map_err(|e| anyhow!("Failed to serialize prescan results: {}", e))?;

    debug_println!("RUST DEBUG: Prescan complete, returning {} bytes of JSON", json_result.len());

    Ok(json_result)
}

/// Get S3/Azure config from cache manager pointer
///
/// The pointer is actually a pointer to the cache name String (stored via Arc),
/// so we need to:
/// 1. Get the cache name from the pointer using jlong_to_arc
/// 2. Use the cache name to look up the manager in CACHE_MANAGERS
fn get_config_from_cache_manager(ptr: jni::sys::jlong) -> (Option<quickwit_config::S3StorageConfig>, Option<quickwit_config::AzureStorageConfig>) {
    if ptr == 0 {
        debug_println!("RUST DEBUG: get_config_from_cache_manager: ptr is 0, returning None");
        return (None, None);
    }

    // The ptr is a pointer to a cache name String Arc, not the manager itself
    // Use jlong_to_arc to get the cache name
    let cache_name = match crate::utils::jlong_to_arc::<String>(ptr) {
        Some(name_arc) => (*name_arc).clone(),
        None => {
            debug_println!("RUST DEBUG: get_config_from_cache_manager: failed to get cache name from ptr");
            return (None, None);
        }
    };

    debug_println!("RUST DEBUG: get_config_from_cache_manager: looking up manager for cache '{}'", cache_name);

    // Access the cache manager from the global registry
    use crate::split_cache_manager::CACHE_MANAGERS;

    let managers = match CACHE_MANAGERS.lock() {
        Ok(m) => m,
        Err(e) => {
            debug_println!("RUST DEBUG: get_config_from_cache_manager: failed to lock CACHE_MANAGERS: {}", e);
            return (None, None);
        }
    };

    // Find the manager by cache name
    if let Some(manager) = managers.get(&cache_name) {
        let s3_config = manager.get_s3_config();
        let azure_config = manager.get_azure_config();
        debug_println!("RUST DEBUG: get_config_from_cache_manager: found manager '{}', s3_config={}, azure_config={}",
            cache_name, s3_config.is_some(), azure_config.is_some());
        return (s3_config, azure_config);
    }

    debug_println!("RUST DEBUG: get_config_from_cache_manager: manager '{}' not found", cache_name);
    (None, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_doc_mapping_parse() {
        let json = r#"{
            "field_mappings": [
                {"name": "title", "type": "text", "tokenizer": "default", "indexed": true},
                {"name": "body", "type": "text", "indexed": true},
                {"name": "tags", "type": "text", "tokenizer": "raw", "indexed": true}
            ]
        }"#;

        let mapping = DocMapping::from_json(json).unwrap();
        assert_eq!(mapping.field_mappings.len(), 3);
        assert!(mapping.uses_lowercase_tokenizer("title"));
        assert!(mapping.uses_lowercase_tokenizer("body")); // Default tokenizer
        assert!(!mapping.uses_lowercase_tokenizer("tags")); // Raw tokenizer
    }

    #[test]
    fn test_prescan_term_extraction() {
        let doc_mapping = DocMapping {
            field_mappings: vec![
                FieldMapping {
                    name: "title".to_string(),
                    field_type: "text".to_string(),
                    tokenizer: Some("default".to_string()),
                    indexed: true,
                },
            ],
        };

        // Create a simple term query AST
        let query_ast = QueryAst::Term(quickwit_query::query_ast::TermQuery {
            field: "title".to_string(),
            value: "Hello".to_string(),
        });

        let terms = extract_prescan_terms(&query_ast, &doc_mapping);
        assert_eq!(terms.len(), 1);
        assert_eq!(terms[0].field, "title");
        assert_eq!(terms[0].term, "hello"); // Lowercased
    }

    #[test]
    fn test_evaluate_prescan_must() {
        let terms = vec![
            PrescanTerm {
                field: "title".to_string(),
                original_field: "title".to_string(),
                term: "hello".to_string(),
                occurrence: TermOccurrence::Must,
                match_type: TermMatchType::Exact,
                json_path: None,
            },
        ];

        let mut existence = HashMap::new();
        existence.insert("title:hello".to_string(), true);
        assert!(evaluate_prescan(&existence, &terms));

        existence.insert("title:hello".to_string(), false);
        assert!(!evaluate_prescan(&existence, &terms));
    }

    #[test]
    fn test_evaluate_prescan_should() {
        let terms = vec![
            PrescanTerm {
                field: "title".to_string(),
                original_field: "title".to_string(),
                term: "hello".to_string(),
                occurrence: TermOccurrence::Should,
                match_type: TermMatchType::Exact,
                json_path: None,
            },
            PrescanTerm {
                field: "title".to_string(),
                original_field: "title".to_string(),
                term: "world".to_string(),
                occurrence: TermOccurrence::Should,
                match_type: TermMatchType::Exact,
                json_path: None,
            },
        ];

        let mut existence = HashMap::new();
        existence.insert("title:hello".to_string(), false);
        existence.insert("title:world".to_string(), true);
        assert!(evaluate_prescan(&existence, &terms)); // At least one exists

        existence.insert("title:world".to_string(), false);
        assert!(!evaluate_prescan(&existence, &terms)); // None exist
    }

    #[test]
    fn test_wildcard_pattern_parsing() {
        // Prefix pattern like "hel*"
        let prefix = WildcardPattern::parse("hel*");
        assert_eq!(prefix, WildcardPattern::Prefix("hel".to_string()));
        assert_eq!(prefix.to_regex(), "^hel.*$");
        assert_eq!(prefix.get_prefix(), Some("hel"));

        // Suffix pattern like "*world"
        let suffix = WildcardPattern::parse("*world");
        assert_eq!(suffix, WildcardPattern::Suffix("world".to_string()));
        assert_eq!(suffix.to_regex(), "^.*world$");
        assert_eq!(suffix.get_prefix(), None);

        // Contains pattern like "*hello*"
        let contains = WildcardPattern::parse("*hello*");
        assert_eq!(contains, WildcardPattern::Contains("hello".to_string()));
        assert_eq!(contains.to_regex(), "^.*hello.*$");

        // Complex pattern like "h*llo"
        let complex = WildcardPattern::parse("h*llo");
        assert_eq!(complex, WildcardPattern::Complex("h*llo".to_string()));
        assert_eq!(complex.to_regex(), "^h.*llo$");

        // Complex pattern with multiple wildcards
        let multi = WildcardPattern::parse("*w*rld*");
        assert_eq!(multi, WildcardPattern::Complex("*w*rld*".to_string()));
        assert_eq!(multi.to_regex(), "^.*w.*rld.*$");
    }

    #[test]
    fn test_wildcard_term_extraction() {
        use quickwit_query::query_ast::WildcardQuery;

        let doc_mapping = DocMapping {
            field_mappings: vec![
                FieldMapping {
                    name: "title".to_string(),
                    field_type: "text".to_string(),
                    tokenizer: Some("default".to_string()),
                    indexed: true,
                },
            ],
        };

        // Create a wildcard query AST
        let query_ast = QueryAst::Wildcard(WildcardQuery {
            field: "title".to_string(),
            value: "Hel*".to_string(),
            lenient: false,
        });

        let terms = extract_prescan_terms(&query_ast, &doc_mapping);
        assert_eq!(terms.len(), 1);
        assert_eq!(terms[0].field, "title");
        assert_eq!(terms[0].term, "hel*"); // Lowercased pattern
        assert_eq!(
            terms[0].match_type,
            TermMatchType::Wildcard(WildcardPattern::Prefix("hel".to_string()))
        );
    }
}
