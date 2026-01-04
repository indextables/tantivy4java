// wildcard.rs - Wildcard query helper functions
// Extracted from mod.rs during refactoring
// Contains: Wildcard pattern parsing, regex conversion, multi-wildcard queries

use tantivy::query::{Query as TantivyQuery, TermQuery, BooleanQuery, Occur, RegexQuery};
use tantivy::schema::{Schema, Term, IndexRecordOption, FieldType as TantivyFieldType, Field};

/// Create a single wildcard query with tokenizer-aware processing
pub(crate) fn create_single_wildcard_query_with_tokenizer(field: Field, pattern: &str, uses_default_tokenizer: bool) -> Result<Box<dyn TantivyQuery>, String> {
    // If pattern contains no wildcards, use exact term query for better performance and accuracy
    if !contains_wildcards(pattern) {
        let term = if uses_default_tokenizer {
            // Default tokenizer lowercases during indexing, so pattern is already lowercased
            Term::from_field_text(field, pattern)
        } else {
            // Other tokenizers preserve case
            Term::from_field_text(field, pattern)
        };
        let term_query = TermQuery::new(term, IndexRecordOption::Basic);
        return Ok(Box::new(term_query) as Box<dyn TantivyQuery>);
    }
    
    // For patterns with wildcards, use regex query
    // Pattern is already processed (lowercased if needed) by caller
    let regex_pattern = wildcard_to_regex_preserve_case(pattern);
    match RegexQuery::from_pattern(&regex_pattern, field) {
        Ok(regex_query) => Ok(Box::new(regex_query) as Box<dyn TantivyQuery>),
        Err(e) => Err(format!("Failed to create wildcard query for pattern '{}': {}", pattern, e)),
    }
}


/// Create regex pattern preserving original case
pub(crate) fn wildcard_to_regex_preserve_case(pattern: &str) -> String {
    let mut regex = String::new();
    let mut chars = pattern.chars().peekable();
    
    while let Some(ch) = chars.next() {
        match ch {
            '*' => regex.push_str(".*"),
            '?' => regex.push('.'),
            '\\' => {
                if let Some(&next_ch) = chars.peek() {
                    chars.next();
                    match next_ch {
                        '*' | '?' | '\\' => regex.push(next_ch),
                        _ => {
                            regex.push('\\');
                            regex.push(next_ch);
                        }
                    }
                } else {
                    regex.push('\\');
                }
            }
            '.' | '^' | '$' | '+' | '|' | '(' | ')' | '[' | ']' | '{' | '}' => {
                regex.push('\\');
                regex.push(ch);
            }
            _ => regex.push(ch),
        }
    }
    regex
}


/// Tokenize a pattern using the field's actual tokenizer to determine token boundaries
pub(crate) fn tokenize_pattern_for_field(schema: &Schema, field: Field, pattern: &str) -> Vec<String> {
    let field_entry = schema.get_field_entry(field);
    
    if let TantivyFieldType::Str(text_options) = field_entry.field_type() {
        if let Some(indexing_options) = text_options.get_indexing_options() {
            // Try to get the tokenizer - for now we'll simulate since direct access is complex
            let tokenizer_name = indexing_options.tokenizer();
            
            // For most tokenizers, we can simulate tokenization by splitting on common boundaries
            match tokenizer_name {
                "default" | "simple" | "en_stem" => {
                    // For wildcard patterns, split only on whitespace to preserve wildcards
                    // Don't split on punctuation because * and ? are wildcard characters
                    pattern.split_whitespace()
                        .filter(|token| !token.is_empty())
                        .map(|s| s.to_string())
                        .collect()
                },
                "keyword" => {
                    // Keyword tokenizer treats entire input as single token
                    vec![pattern.to_string()]
                },
                "whitespace" => {
                    // Whitespace tokenizer only splits on whitespace (preserves case)
                    pattern.split_whitespace().map(|s| s.to_string()).collect()
                },
                _ => {
                    // Unknown tokenizer - fall back to whitespace splitting
                    pattern.split_whitespace().map(|s| s.to_string()).collect()
                }
            }
        } else {
            // No indexing options - treat as single token
            vec![pattern.to_string()]
        }
    } else {
        // Not a text field - treat as single token
        vec![pattern.to_string()]
    }
}

/// Check if a pattern would result in multiple tokens when processed by the field's tokenizer
pub(crate) fn is_multi_token_pattern(schema: &Schema, field: Field, pattern: &str) -> bool {
    let tokens = tokenize_pattern_for_field(schema, field, pattern);
    tokens.len() > 1
}


/// Check if a token contains wildcard characters
/// Process escape sequences in a wildcard pattern and return the literal text
fn process_escapes(pattern: &str) -> String {
    let mut result = String::new();
    let mut chars = pattern.chars().peekable();
    
    while let Some(ch) = chars.next() {
        match ch {
            '\\' => {
                if let Some(&next_ch) = chars.peek() {
                    chars.next();
                    // All escaped characters become literals
                    result.push(next_ch);
                } else {
                    result.push('\\');
                }
            }
            _ => result.push(ch),
        }
    }
    result
}

pub(crate) fn contains_wildcards(pattern: &str) -> bool {
    let processed = process_escapes(pattern);
    processed.contains('*') || processed.contains('?')
}

/// Check if pattern contains multiple wildcards that should be split into regex segments
pub(crate) fn contains_multi_wildcards(pattern: &str) -> bool {
    // Count the number of asterisks - multiple wildcards indicate complex patterns
    // Examples:
    // - "*hello*world*" -> 3 asterisks (multi-wildcard)
    // - "*hello*" -> 2 asterisks (multi-wildcard)  
    // - "hello*world" -> 1 asterisk (simple wildcard)
    // - "*" -> 1 asterisk (simple wildcard)
    
    pattern.chars().filter(|&c| c == '*').count() > 1
}

/// Parse complex multi-wildcard pattern into raw segments (no regex escaping)
/// Example: "*y*me*key*y" -> ["y", "me", "key", "y"]
fn parse_multi_wildcard_pattern(pattern: &str) -> Vec<String> {
    let mut segments = Vec::new();
    let mut current_segment = String::new();
    let mut chars = pattern.chars().peekable();
    
    while let Some(ch) = chars.next() {
        match ch {
            '\\' => {
                // Handle escaped characters
                if let Some(next_ch) = chars.next() {
                    current_segment.push(next_ch);
                }
            }
            '*' => {
                // End current segment if it has content
                if !current_segment.is_empty() {
                    segments.push(current_segment.trim().to_string());
                    current_segment.clear();
                }
            }
            '?' => {
                // For single-char wildcard, we'll treat it as a literal character in regex context
                current_segment.push('.');
            }
            _ => {
                current_segment.push(ch);
            }
        }
    }
    
    // Add final segment if it exists
    if !current_segment.is_empty() {
        segments.push(current_segment.trim().to_string());
    }
    
    // Return raw segments (no regex escaping here)
    segments.into_iter()
        .filter(|s| !s.is_empty())
        .collect()
}

/// Escape regex special characters for safe use in patterns
fn escape_regex_chars(input: &str) -> String {
    let mut result = String::new();
    for ch in input.chars() {
        match ch {
            '\\' | '^' | '$' | '.' | '|' | '?' | '*' | '+' | '(' | ')' | '[' | ']' | '{' | '}' => {
                result.push('\\');
                result.push(ch);
            }
            _ => result.push(ch),
        }
    }
    result
}

/// Create enhanced multi-wildcard query with comprehensive matching strategies
/// Example: "*Wild*Joe*" becomes:
/// ((regex(".*Wild.*") OR regex(".*Wild") OR regex("Wild.*") OR term("Wild")) 
///  AND 
///  (regex(".*Joe.*") OR regex(".*Joe") OR regex("Joe.*") OR term("Joe")))
pub(crate) fn create_multi_wildcard_regex_query(field: Field, pattern: &str) -> Result<Box<dyn TantivyQuery>, String> {
    let segments = parse_multi_wildcard_pattern(pattern);
    
    if segments.is_empty() {
        // Pattern was all wildcards - match everything
        let regex_pattern = ".*";
        match RegexQuery::from_pattern(regex_pattern, field) {
            Ok(regex_query) => Ok(Box::new(regex_query) as Box<dyn TantivyQuery>),
            Err(e) => Err(format!("Failed to create regex query: {}", e)),
        }
    } else {
        // Create boolean AND query where each segment has multiple matching strategies
        let mut and_subqueries = Vec::new();
        
        for segment in segments {
            // Create OR query for this segment with multiple matching strategies
            let mut or_strategies = Vec::new();
            
            // For case-sensitive multi-wildcard, lowercase the segment since default tokenizer lowercases
            let segment_lower = segment.to_lowercase();
            let escaped_segment = escape_regex_chars(&segment_lower);
            
            // Strategy 1: Contains pattern (.*segment.*)
            let contains_pattern = format!(".*{}.*", escaped_segment);
            if let Ok(regex_query) = RegexQuery::from_pattern(&contains_pattern, field) {
                or_strategies.push((Occur::Should, Box::new(regex_query) as Box<dyn TantivyQuery>));
            }
            
            // Strategy 2: Prefix pattern (segment.*)
            let prefix_pattern = format!("{}.*", escaped_segment);
            if let Ok(regex_query) = RegexQuery::from_pattern(&prefix_pattern, field) {
                or_strategies.push((Occur::Should, Box::new(regex_query) as Box<dyn TantivyQuery>));
            }
            
            // Strategy 3: Suffix pattern (.*segment)
            let suffix_pattern = format!(".*{}", escaped_segment);
            if let Ok(regex_query) = RegexQuery::from_pattern(&suffix_pattern, field) {
                or_strategies.push((Occur::Should, Box::new(regex_query) as Box<dyn TantivyQuery>));
            }
            
            // Strategy 4: Exact term match with lowercase
            let term_orig = Term::from_field_text(field, &segment_lower);
            let term_query_orig = TermQuery::new(term_orig, IndexRecordOption::WithFreqs);
            or_strategies.push((Occur::Should, Box::new(term_query_orig) as Box<dyn TantivyQuery>));
            
            // Combine all strategies for this segment with OR
            if or_strategies.len() > 1 {
                let segment_or_query = BooleanQuery::new(or_strategies);
                and_subqueries.push((Occur::Must, Box::new(segment_or_query) as Box<dyn TantivyQuery>));
            } else if or_strategies.len() == 1 {
                and_subqueries.push((Occur::Must, or_strategies.into_iter().next().unwrap().1));
            }
        }
        
        // Combine all segments with AND
        if and_subqueries.len() > 1 {
            let boolean_query = BooleanQuery::new(and_subqueries);
            Ok(Box::new(boolean_query) as Box<dyn TantivyQuery>)
        } else if and_subqueries.len() == 1 {
            Ok(and_subqueries.into_iter().next().unwrap().1)
        } else {
            Err("No valid query strategies could be created".to_string())
        }
    }
}


/// Convert wildcard pattern to regex pattern
/// * -> .*
/// ? -> .
/// Escape other regex special characters
/// Note: Don't use anchors as Tantivy's RegexQuery doesn't seem to like them

/// Create a tokenized wildcard query that preserves wildcards correctly
/// This fixes the issue where wildcards were being stripped in the original implementation
pub(crate) fn create_fixed_tokenized_wildcard_query(_schema: &Schema, field: Field, pattern: &str, uses_default_tokenizer: bool) -> Result<Box<dyn TantivyQuery>, String> {
    // Split pattern on whitespace to get individual wildcard tokens
    let tokens: Vec<&str> = pattern.split_whitespace()
        .filter(|token| !token.is_empty())
        .collect();
    
    if tokens.is_empty() {
        return Err("Empty wildcard pattern after tokenization".to_string());
    }
    
    if tokens.len() == 1 {
        // Single token after splitting - use single wildcard query
        return create_single_wildcard_query_with_tokenizer(field, tokens[0], uses_default_tokenizer);
    }
    
    // Create individual wildcard queries for each token
    let mut subqueries = Vec::new();
    
    for token in tokens {
        // Each token should be treated as a wildcard pattern
        // Don't check if it contains wildcards - preserve the original token as-is
        match create_single_wildcard_query_with_tokenizer(field, token, uses_default_tokenizer) {
            Ok(query) => {
                subqueries.push((Occur::Must, query));
            },
            Err(e) => return Err(format!("Failed to create wildcard query for token '{}': {}", token, e)),
        }
    }
    
    if subqueries.is_empty() {
        return Err("No valid wildcard tokens in pattern".to_string());
    }
    
    if subqueries.len() == 1 {
        // Only one subquery - return it directly
        Ok(subqueries.into_iter().next().unwrap().1)
    } else {
        // Multiple subqueries - create boolean AND query
        let boolean_query = BooleanQuery::new(subqueries);
        Ok(Box::new(boolean_query) as Box<dyn TantivyQuery>)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wildcard_to_regex_preserve_case() {
        // Basic patterns
        assert_eq!(wildcard_to_regex_preserve_case("Hello*"), "Hello.*");
        assert_eq!(wildcard_to_regex_preserve_case("*World"), ".*World");
        assert_eq!(wildcard_to_regex_preserve_case("He?lo"), "He.lo");

        // Complex patterns
        assert_eq!(wildcard_to_regex_preserve_case("Hello*World"), "Hello.*World");
        assert_eq!(wildcard_to_regex_preserve_case("*test*"), ".*test.*");

        // Escape sequences - \* becomes literal *, \? becomes literal ?
        assert_eq!(wildcard_to_regex_preserve_case("test\\*"), "test*");
        assert_eq!(wildcard_to_regex_preserve_case("test\\?"), "test?");

        // Special regex characters
        assert_eq!(wildcard_to_regex_preserve_case("test.txt"), "test\\.txt");
        assert_eq!(wildcard_to_regex_preserve_case("test(1)"), "test\\(1\\)");

        // Exact match (no wildcards)
        assert_eq!(wildcard_to_regex_preserve_case("Hello"), "Hello");
    }
}

