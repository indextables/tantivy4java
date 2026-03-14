// parse_query.rs - Parse query string implementation
// Extracted from split_query.rs during refactoring

use anyhow::{anyhow, Result};
use jni::objects::{JObject, JObjectArray, JString, JValue};
use jni::sys::jobject;
use jni::JNIEnv;
use quickwit_query::query_ast::{query_ast_from_user_text, QueryAst};

use crate::debug_println;

use super::schema_cache::SPLIT_SCHEMA_CACHE;

/// Parse a query string into a SplitQuery
pub fn parse_query_string(
    env: &mut JNIEnv,
    query_string: JString,
    schema_ptr: jni::sys::jlong,
    default_search_fields: jobject,
) -> Result<jobject> {
    debug_println!("RUST DEBUG: *** parse_query_string CALLED ***");

    // Get the query string
    let query_str: String = env.get_string(&query_string)?.into();

    debug_println!(
        "RUST DEBUG: 🚀 Parsing query string: '{}' with schema_ptr: {}",
        query_str,
        schema_ptr
    );

    // ✅ PROPER DEFAULT FIELD EXTRACTION: Extract default search fields from JNI
    let mut default_fields_vec = extract_default_search_fields(env, default_search_fields)?;

    // 🎯 KEY LOGIC: If default fields is empty, extract ALL indexed text fields from schema
    if default_fields_vec.is_empty() {
        debug_println!(
            "RUST DEBUG: Default fields empty, extracting all indexed text fields from schema"
        );

        // Extract all indexed text field names from the schema
        default_fields_vec = extract_text_fields_from_schema(env, schema_ptr)?;

        debug_println!(
            "RUST DEBUG: Auto-detected {} text fields: {:?}",
            default_fields_vec.len(),
            default_fields_vec
        );

        if default_fields_vec.is_empty() {
            debug_println!("RUST DEBUG: Warning: No indexed text fields found in schema");
        }
    }

    debug_println!(
        "RUST DEBUG: Final default search fields: {:?}",
        default_fields_vec
    );

    // Pre-process the query string: expand IP CIDR/wildcard patterns into explicit range syntax.
    //
    // Step A — when a single default field is set, prefix any bare CIDR/wildcard/quoted-IPv6
    //   tokens with the field name so that Step B can expand them uniformly.
    //   This handles the Spark `indexquery` operator which passes field name separately:
    //     parseQuery("192.168.1.0/24", "ip")   → prefix → "ip:192.168.1.0/24"
    //     parseQuery("192.168.1.0/24 OR 10.0.0.0/8", "ip")
    //                                          → prefix → "ip:192.168.1.0/24 OR ip:10.0.0.0/8"
    //     parseQuery("\"2001:db8::/32\"", "ip") → prefix → "ip:\"2001:db8::/32\""
    //
    // Step B — rewrite all field:CIDR and field:"quoted-CIDR" tokens to range syntax:
    //     "ip:192.168.1.0/24"       → "ip:[192.168.1.0 TO 192.168.1.255]"
    //     "ip:\"2001:db8::/32\""    → "ip:[2001:db8:: TO 2001:db8:ffff:...]"
    //
    // Both preprocessors now receive the set of IP-typed field names so that expansion
    // is only attempted for fields whose schema type is actually IpAddr. This prevents
    // accidental expansion for fields like `url` whose values may contain '/' or '*'.
    //
    // Outer fast-path: skip both preprocessing steps when no IP patterns are possible.
    // This avoids both string clones and schema inspection in the common case (pure text queries).
    let original_query_str = query_str.clone();
    let query_str = if query_str.contains('/') || query_str.contains('*') {
        let ip_fields = extract_ip_fields_from_schema_ptr(schema_ptr);
        if !ip_fields.is_empty() {
            let after_prefix = if default_fields_vec.len() == 1 {
                prefix_bare_ip_tokens(&query_str, &default_fields_vec[0], &ip_fields)
            } else {
                query_str
            };
            preprocess_ip_query_string(&after_prefix, &ip_fields)
        } else {
            query_str
        }
    } else {
        query_str
    };
    debug_println!(
        "RUST DEBUG: After IP preprocessing, query_str: '{}'",
        query_str
    );

    // 🚀 PROPER QUICKWIT PARSING: Use Quickwit's proven two-step process
    // Step 1: Create UserInputQuery AST with proper default fields
    // Use None if default fields is empty to let Quickwit handle field-less queries properly
    let default_fields_option = if default_fields_vec.is_empty() {
        debug_println!("RUST DEBUG: Using None for default fields (empty vector)");
        None
    } else {
        debug_println!(
            "RUST DEBUG: Using Some({:?}) for default fields",
            default_fields_vec
        );
        Some(default_fields_vec.clone())
    };

    let query_ast = query_ast_from_user_text(&query_str, default_fields_option.clone());
    debug_println!("RUST DEBUG: Created UserInputQuery AST: {:?}", query_ast);

    // Step 2: Parse the user query using Quickwit's parser with default search fields
    // Pass the right default fields based on what we have
    let parse_fields = match &default_fields_option {
        Some(fields) => fields,
        None => &Vec::new(), // Use empty vector when None
    };

    let parsed_ast = match query_ast.parse_user_query(parse_fields) {
        Ok(ast) => {
            debug_println!("RUST DEBUG: ✅ Successfully parsed with Quickwit: {:?}", ast);
            ast
        }
        Err(e) => {
            debug_println!("RUST DEBUG: ❌ Parsing failed: {}", e);

            // 🚨 THROW ERROR instead of falling back to match_all
            return Err(anyhow!("Failed to parse query '{}': {}. This query requires explicit field names (e.g., 'field:term') or valid default search fields in the schema.", query_str, e));
        }
    };

    // Post-process the parsed AST to transparently expand IP CIDR/wildcard patterns.
    // This handles cases like "ip:192.168.1.0/24" where the parser may emit a
    // TermQuery with a literal slash that needs to become a RangeQuery.
    // Guard: skip the tree walk entirely when the original query string contained no
    // IP-like patterns — zero cost for the 99% of queries that have no '/' or '*'.
    let parsed_ast = if original_query_str.contains('/') || original_query_str.contains('*') {
        rewrite_ip_terms(parsed_ast)
    } else {
        parsed_ast
    };

    debug_println!("RUST DEBUG: 🎯 Final parsed QueryAst: {:?}", parsed_ast);

    // ✅ QUICKWIT BEST PRACTICE: Serialize QueryAst directly to JSON
    // instead of converting to individual SplitQuery objects
    let query_ast_json = serde_json::to_string(&parsed_ast)
        .map_err(|e| anyhow!("Failed to serialize QueryAst to JSON: {}", e))?;

    debug_println!("RUST DEBUG: 📄 QueryAst JSON: {}", query_ast_json);

    // Create a SplitParsedQuery that holds the QueryAst JSON directly
    let split_query_obj = create_split_parsed_query(env, &query_ast_json)?;

    Ok(split_query_obj)
}

#[allow(dead_code)]
pub fn create_split_query_from_ast(env: &mut JNIEnv, query_ast: &QueryAst) -> Result<jobject> {
    match query_ast {
        QueryAst::Term(term_query) => {
            // Create SplitTermQuery Java object
            let class = env.find_class("io/indextables/tantivy4java/split/SplitTermQuery")?;
            let field_jstring = env.new_string(&term_query.field)?;
            let value_jstring = env.new_string(&term_query.value)?;

            let obj = env.new_object(
                class,
                "(Ljava/lang/String;Ljava/lang/String;)V",
                &[
                    JValue::Object(&field_jstring.into()),
                    JValue::Object(&value_jstring.into()),
                ],
            )?;

            debug_println!(
                "RUST DEBUG: ✅ Created SplitTermQuery for Term: field='{}', value='{}'",
                term_query.field,
                term_query.value
            );
            Ok(obj.into_raw())
        }
        QueryAst::FullText(fulltext_query) => {
            // Convert FullTextQuery to SplitTermQuery
            // FullTextQuery and TermQuery are conceptually the same for our purposes
            let class = env.find_class("io/indextables/tantivy4java/split/SplitTermQuery")?;
            let field_jstring = env.new_string(&fulltext_query.field)?;
            let value_jstring = env.new_string(&fulltext_query.text)?;

            let obj = env.new_object(
                class,
                "(Ljava/lang/String;Ljava/lang/String;)V",
                &[
                    JValue::Object(&field_jstring.into()),
                    JValue::Object(&value_jstring.into()),
                ],
            )?;

            debug_println!(
                "RUST DEBUG: ✅ Created SplitTermQuery for FullText: field='{}', text='{}'",
                fulltext_query.field,
                fulltext_query.text
            );
            Ok(obj.into_raw())
        }
        QueryAst::MatchAll => {
            // Create SplitMatchAllQuery Java object
            let class = env.find_class("io/indextables/tantivy4java/split/SplitMatchAllQuery")?;
            let obj = env.new_object(class, "()V", &[])?;
            debug_println!("RUST DEBUG: ✅ Created SplitMatchAllQuery for MatchAll");
            Ok(obj.into_raw())
        }
        QueryAst::Bool(_bool_query) => {
            // TODO: Implement SplitBooleanQuery creation from QueryAst
            // This is more complex as we need to recursively convert subqueries
            debug_println!(
                "RUST DEBUG: Boolean query creation from QueryAst not yet implemented"
            );

            // Fallback to MatchAll for now
            let class = env.find_class("io/indextables/tantivy4java/split/SplitMatchAllQuery")?;
            let obj = env.new_object(class, "()V", &[])?;
            Ok(obj.into_raw())
        }
        _ => {
            debug_println!(
                "RUST DEBUG: Unsupported QueryAst type for SplitQuery conversion: {:?}",
                query_ast
            );

            // Fallback to MatchAll
            let class = env.find_class("io/indextables/tantivy4java/split/SplitMatchAllQuery")?;
            let obj = env.new_object(class, "()V", &[])?;
            Ok(obj.into_raw())
        }
    }
}

pub fn create_split_parsed_query(env: &mut JNIEnv, query_ast_json: &str) -> Result<jobject> {
    // Create SplitParsedQuery Java object with the QueryAst JSON
    let class = env.find_class("io/indextables/tantivy4java/split/SplitParsedQuery")?;
    let json_jstring = env.new_string(query_ast_json)?;

    let obj = env.new_object(
        class,
        "(Ljava/lang/String;)V",
        &[JValue::Object(&json_jstring.into())],
    )?;

    debug_println!(
        "RUST DEBUG: ✅ Created SplitParsedQuery with JSON: {}",
        query_ast_json
    );
    Ok(obj.into_raw())
}

pub fn extract_default_search_fields(
    env: &mut JNIEnv,
    default_search_fields: jobject,
) -> Result<Vec<String>> {
    // Handle null case - return empty vec
    if default_search_fields.is_null() {
        debug_println!("RUST DEBUG: Default search fields is null, using empty list");
        return Ok(Vec::new());
    }

    // Convert to JObjectArray (requires unsafe block)
    let fields_array = JObjectArray::from(unsafe { JObject::from_raw(default_search_fields) });

    // Get array length
    let array_len = env.get_array_length(&fields_array)?;
    debug_println!(
        "RUST DEBUG: Default search fields array length: {}",
        array_len
    );

    let mut fields = Vec::new();

    // Extract each string from the array
    for i in 0..array_len {
        let element = env.get_object_array_element(&fields_array, i)?;
        if !element.is_null() {
            let field_str: String = env.get_string(&JString::from(element))?.into();
            debug_println!(
                "RUST DEBUG: Extracted default search field[{}]: '{}'",
                i,
                field_str
            );
            fields.push(field_str);
        }
    }

    debug_println!("RUST DEBUG: Final default search fields: {:?}", fields);
    Ok(fields)
}

pub fn extract_text_fields_from_schema(
    _env: &mut JNIEnv,
    schema_ptr: jni::sys::jlong,
) -> Result<Vec<String>> {
    debug_println!(
        "RUST DEBUG: extract_text_fields_from_schema called with schema pointer: {}",
        schema_ptr
    );

    // First try to get schema from registry (original approach)
    if let Some(schema) = crate::utils::jlong_to_arc::<tantivy::schema::Schema>(schema_ptr) {
        debug_println!(
            "RUST DEBUG: ✅ Successfully retrieved schema from registry with pointer: {}",
            schema_ptr
        );
        return extract_fields_from_schema(&schema);
    }

    debug_println!(
        "RUST DEBUG: ❌ Failed to retrieve schema from registry with pointer: {}",
        schema_ptr
    );
    debug_println!(
        "RUST DEBUG: This suggests either the pointer is invalid or the schema is not in the registry"
    );

    // NEW APPROACH: Try to get any cached schema from the split schema cache
    // Since we don't have the split URI in this context, iterate through all cached schemas
    debug_println!("RUST DEBUG: Attempting to retrieve schema from split schema cache...");
    let cache = SPLIT_SCHEMA_CACHE.lock().unwrap();
    for (split_uri, cached_schema) in cache.iter() {
        debug_println!("RUST DEBUG: Found cached schema for split URI: {}", split_uri);
        let text_fields = extract_fields_from_schema(cached_schema)?;
        debug_println!(
            "RUST DEBUG: ✅ Using cached schema from split: {} with text fields: {:?}",
            split_uri,
            text_fields
        );
        return Ok(text_fields);
    }
    drop(cache);

    debug_println!("RUST DEBUG: ❌ No cached schemas found in split schema cache");

    Err(anyhow!(
        "Schema registry lookup failed for pointer: {} and no cached schemas found - this indicates a schema pointer lifecycle issue",
        schema_ptr
    ))
}

/// Helper function to extract text fields from a schema
pub fn extract_fields_from_schema(schema: &tantivy::schema::Schema) -> Result<Vec<String>> {
    let mut text_fields = Vec::new();

    // Iterate through all fields in the schema
    for (_field, field_entry) in schema.fields() {
        let field_name = field_entry.name();

        // Check if this field is a text field that's indexed
        if let tantivy::schema::FieldType::Str(text_options) = field_entry.field_type() {
            if text_options.get_indexing_options().is_some() {
                debug_println!("RUST DEBUG: Found indexed text field: '{}'", field_name);
                text_fields.push(field_name.to_string());
            } else {
                debug_println!(
                    "RUST DEBUG: Skipping non-indexed text field: '{}'",
                    field_name
                );
            }
        }
    }

    debug_println!(
        "RUST DEBUG: Extracted {} text fields from schema: {:?}",
        text_fields.len(),
        text_fields
    );
    Ok(text_fields)
}

/// Extract all unique field names from a QueryAst
pub fn extract_fields_from_query_ast(query_ast: &QueryAst) -> std::collections::HashSet<String> {
    let mut fields = std::collections::HashSet::new();
    collect_fields_recursive(query_ast, &mut fields);
    fields
}

/// Recursively collect field names from QueryAst
fn collect_fields_recursive(query_ast: &QueryAst, fields: &mut std::collections::HashSet<String>) {
    match query_ast {
        QueryAst::Term(term_query) => {
            fields.insert(term_query.field.clone());
        }
        QueryAst::TermSet(term_set_query) => {
            // TermSetQuery has terms_per_field: HashMap<String, BTreeSet<String>>
            for field_name in term_set_query.terms_per_field.keys() {
                fields.insert(field_name.clone());
            }
        }
        QueryAst::FullText(full_text_query) => {
            fields.insert(full_text_query.field.clone());
        }
        QueryAst::PhrasePrefix(phrase_prefix_query) => {
            fields.insert(phrase_prefix_query.field.clone());
        }
        QueryAst::Range(range_query) => {
            fields.insert(range_query.field.clone());
        }
        QueryAst::FieldPresence(field_presence_query) => {
            fields.insert(field_presence_query.field.clone());
        }
        QueryAst::Wildcard(wildcard_query) => {
            fields.insert(wildcard_query.field.clone());
        }
        QueryAst::Regex(regex_query) => {
            fields.insert(regex_query.field.clone());
        }
        QueryAst::Bool(bool_query) => {
            for sub_query in bool_query.must.iter()
                .chain(bool_query.should.iter())
                .chain(bool_query.must_not.iter())
                .chain(bool_query.filter.iter())
            {
                collect_fields_recursive(sub_query, fields);
            }
        }
        QueryAst::Boost { underlying, .. } => {
            collect_fields_recursive(underlying, fields);
        }
        QueryAst::UserInput(_) => {
            // UserInput queries contain unparsed text, fields are not known until parsed
        }
        QueryAst::MatchAll | QueryAst::MatchNone => {
            // No fields for match_all/match_none
        }
    }
}

/// When a single default field is set, scan the query string for bare CIDR/wildcard tokens
/// (those without an explicit `field:` qualifier) and prefix them with the default field name,
/// but only when that default field is in `ip_fields`.
///
/// Also handles double-quoted IPv6 CIDR patterns: `"2001:db8::/32"` → `ip:"2001:db8::/32"`.
///
/// After this step, `preprocess_ip_query_string` can expand all `field:CIDR` patterns uniformly.
///
/// Performance design:
///   - Fast-path early return when query contains neither '/' nor '*' (zero allocation).
///   - Short-circuits immediately when `default_field` is not an IP field.
///   - Byte-level scanner: no Vec<char> allocation; query strings are always ASCII.
///   - `try_expand_ip_range` guards every prefix decision with zero allocation for plain IPs.
///
/// NOTE: Unquoted IPv6 CIDRs (e.g., fe80::1/10) are NOT supported here.
/// The has_field_prefix heuristic detects letters before ':' to distinguish field names
/// from pure-hex IPv6 groups (2001:db8), but link-local prefixes like fe80 contain
/// letters and would be misidentified as field names. Users must quote IPv6 CIDRs.
fn prefix_bare_ip_tokens(
    query: &str,
    default_field: &str,
    ip_fields: &std::collections::HashSet<String>,
) -> String {
    // Fast path: CIDR requires '/' and wildcards require '*'.
    // If neither is present there are no expandable patterns.
    if !query.contains('/') && !query.contains('*') {
        return query.to_string();
    }

    // Fast path: if the default field is not an IP field, there is nothing to prefix.
    if !ip_fields.contains(default_field) {
        return query.to_string();
    }

    // Byte-level scanner — query strings are ASCII, so byte indices == char indices.
    let bytes = query.as_bytes();
    let n = bytes.len();
    let mut result = String::with_capacity(query.len() + 32);
    let mut i = 0;

    while i < n {
        let b = bytes[i];

        // Whitespace — pass through
        if b.is_ascii_whitespace() {
            result.push(b as char);
            i += 1;
            continue;
        }

        // Structural characters — pass through individually
        if matches!(b, b'(' | b')' | b'[' | b']' | b'{' | b'}') {
            result.push(b as char);
            i += 1;
            continue;
        }

        // Double-quoted token: `"2001:db8::/32"` — quoted because IPv6 colons
        if b == b'"' {
            let quote_start = i;
            i += 1;
            while i < n && bytes[i] != b'"' {
                i += 1;
            }
            if i < n {
                i += 1; // consume closing quote
            }
            // Byte slice is safe: all chars in range are ASCII
            let quoted_token = &query[quote_start..i];
            let inner = if quoted_token.len() >= 2 {
                &quoted_token[1..quoted_token.len() - 1]
            } else {
                quoted_token
            };
            if crate::ip_expansion::try_expand_ip_range(inner).is_some() {
                debug_println!(
                    "RUST DEBUG: prefix_bare_ip_tokens: quoted '{}' → '{}:{}'",
                    quoted_token, default_field, quoted_token
                );
                result.push_str(default_field);
                result.push(':');
            }
            result.push_str(quoted_token);
            continue;
        }

        // Collect a word token (stop at whitespace, parens, quotes)
        let token_start = i;
        while i < n
            && !bytes[i].is_ascii_whitespace()
            && !matches!(bytes[i], b'(' | b')' | b'[' | b']' | b'{' | b'}' | b'"')
        {
            i += 1;
        }
        let token = &query[token_start..i];

        if token.is_empty() {
            continue;
        }

        // Check if already has an explicit `field:` prefix.
        // Rule: the substring before the first ':' consists entirely of word chars AND
        // contains at least one letter — this distinguishes field names from IPv6 hex groups.
        let has_field_prefix = token.find(':').map_or(false, |colon_pos| {
            let before = &token[..colon_pos];
            !before.is_empty()
                && before.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
                && before.bytes().any(|c| c.is_ascii_alphabetic())
        });

        if has_field_prefix {
            result.push_str(token);
            continue;
        }

        // Prefix with the default field only for expandable IP patterns (CIDR / wildcard).
        // Boolean keywords (AND, OR, NOT) and plain IPs are left as-is for Quickwit.
        if crate::ip_expansion::try_expand_ip_range(token).is_some() {
            debug_println!(
                "RUST DEBUG: prefix_bare_ip_tokens: '{}' → '{}:{}'",
                token, default_field, token
            );
            result.push_str(default_field);
            result.push(':');
        }
        result.push_str(token);
    }

    result
}

/// Pre-process a Quickwit user-text query string, rewriting any IP CIDR or wildcard patterns
/// into explicit Quickwit range syntax so that the Quickwit parser never sees the '/' character
/// in IP field values.
///
/// Only tokens whose field name is in `ip_fields` are candidates for expansion; all other
/// `field:value` tokens (e.g. `url:http://example.com/path`) are passed through unchanged.
///
/// Handles both unquoted (`ip:192.168.1.0/24`) and quoted (`ip:"2001:db8::/32"`) forms.
///
/// Examples:
///   `ip_addr:192.168.1.0/24`       →  `ip_addr:[192.168.1.0 TO 192.168.1.255]`
///   `ip_addr:192.168.1.*`          →  `ip_addr:[192.168.1.0 TO 192.168.1.255]`
///   `ip_addr:"2001:db8::/32"`      →  `ip_addr:[2001:db8:: TO 2001:db8:ffff:...]`
///
/// Performance design:
///   - Fast-path early return when query contains neither '/' nor '*' (zero allocation).
///   - Single regex pass handles both quoted and unquoted forms via alternation.
///   - Regex compiled once via `once_cell::sync::Lazy`.
///   - `ip_fields.contains(field)` short-circuits before `try_expand_ip_range` for non-IP tokens.
///
/// Must be called BEFORE `query_ast_from_user_text`.
fn preprocess_ip_query_string(
    query: &str,
    ip_fields: &std::collections::HashSet<String>,
) -> String {
    use once_cell::sync::Lazy;
    use regex::Regex;

    // Fast path 1: no CIDR or wildcard patterns possible without '/' or '*'.
    if !query.contains('/') && !query.contains('*') {
        return query.to_string();
    }

    // Fast path 2: a field:CIDR or field:wildcard token always has a digit or opening
    // quote immediately after the ':' (e.g. `ip:192...` or `ip:"2001...`).
    // Queries like `url:http://example.com/path` have '/' present but are followed by
    // '/' not a digit, so we can skip the regex entirely and avoid the Cow::Borrowed
    // → into_owned() clone that would otherwise occur with no matches.
    let has_ip_like_colon = query
        .as_bytes()
        .windows(2)
        .any(|w| w[0] == b':' && (w[1].is_ascii_digit() || w[1] == b'"'));
    if !has_ip_like_colon {
        return query.to_string();
    }

    // Single pass: alternation matches both quoted and unquoted field:value forms.
    // Group layout: 1 = field name (both arms), 2 = quoted value, 3 = unquoted value.
    // The quoted arm is listed first so it takes priority when both could match.
    static IP_TOKEN: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r#"(\b\w+):(?:"([\d.:/a-fA-F*]+)"|([\d.:/a-fA-F*]+))"#).unwrap()
    });

    IP_TOKEN
        .replace_all(query, |caps: &regex::Captures| {
            let field = &caps[1];
            // Only expand tokens for fields that are actually IP-typed in the schema.
            if !ip_fields.contains(field) {
                // Reconstruct original token unchanged (avoids allocation for the common case).
                return caps[0].to_string();
            }
            // Exactly one of caps[2] (quoted) or caps[3] (unquoted) is non-empty per match.
            let (value, was_quoted) = match (caps.get(2), caps.get(3)) {
                (Some(m), _) => (m.as_str(), true),
                (_, Some(m)) => (m.as_str(), false),
                _ => return String::new(),
            };
            if let Some((lower, upper)) = crate::ip_expansion::try_expand_ip_range(value) {
                debug_println!(
                    "RUST DEBUG: preprocess_ip_query_string: '{}:{}' → '{}:[{} TO {}]'",
                    field, value, field, lower, upper
                );
                format!("{}:[{} TO {}]", field, lower, upper)
            } else if was_quoted {
                format!("{}:\"{}\"", field, value)
            } else {
                format!("{}:{}", field, value)
            }
        })
        .into_owned()
}

/// Extract the names of all IpAddr-typed fields from the schema referenced by `schema_ptr`.
/// Returns an empty set when the pointer is invalid or refers to no IP fields, so callers
/// can safely skip IP preprocessing without schema context.
fn extract_ip_fields_from_schema_ptr(schema_ptr: jni::sys::jlong) -> std::collections::HashSet<String> {
    if let Some(schema) = crate::utils::jlong_to_arc::<tantivy::schema::Schema>(schema_ptr) {
        schema
            .fields()
            .filter_map(|(_, entry)| {
                if matches!(entry.field_type(), tantivy::schema::FieldType::IpAddr(_)) {
                    Some(entry.name().to_string())
                } else {
                    None
                }
            })
            .collect()
    } else {
        // Also try the split schema cache as a fallback (same pattern as extract_text_fields_from_schema)
        let cache = SPLIT_SCHEMA_CACHE.lock().unwrap();
        for (_, cached_schema) in cache.iter() {
            return cached_schema
                .fields()
                .filter_map(|(_, entry)| {
                    if matches!(entry.field_type(), tantivy::schema::FieldType::IpAddr(_)) {
                        Some(entry.name().to_string())
                    } else {
                        None
                    }
                })
                .collect();
        }
        std::collections::HashSet::new()
    }
}

/// Recursively rewrite a QueryAst, expanding IP CIDR and wildcard patterns in TermQuery and
/// WildcardQuery nodes.
///
/// The Quickwit parser may emit either a `QueryAst::Term` (for CIDR patterns containing '/') or a
/// `QueryAst::Wildcard` (for patterns containing '*' like `ip:192.168.1.*`) when the query string
/// contains IP range shorthands. This function walks the AST and replaces such nodes with the
/// correct `QueryAst::Range` or `QueryAst::MatchAll`.
fn rewrite_ip_terms(ast: QueryAst) -> QueryAst {
    // Helper: attempt expansion given a field name and value string, returning a rewritten node
    // if the value is an IP CIDR or wildcard pattern.
    let try_rewrite = |field: &str, value: &str| -> Option<QueryAst> {
        let (lower, upper) = crate::ip_expansion::try_expand_ip_range(value)?;
        if crate::ip_expansion::is_match_all_range(&lower, &upper) {
            debug_println!(
                "RUST DEBUG: rewrite_ip_terms: '{}:{}' → MatchAll",
                field, value
            );
            return Some(QueryAst::MatchAll);
        }
        debug_println!(
            "RUST DEBUG: rewrite_ip_terms: '{}:{}' → Range [{} TO {}]",
            field, value, lower, upper
        );
        use quickwit_query::query_ast::RangeQuery;
        use quickwit_query::JsonLiteral;
        use std::ops::Bound;
        Some(QueryAst::Range(RangeQuery {
            field: field.to_string(),
            lower_bound: Bound::Included(JsonLiteral::String(lower)),
            upper_bound: Bound::Included(JsonLiteral::String(upper)),
        }))
    };

    // QueryAst::Term — CIDR patterns (e.g. ip:192.168.1.0/24) parse here
    if let QueryAst::Term(ref t) = ast {
        if let Some(rewritten) = try_rewrite(&t.field, &t.value) {
            return rewritten;
        }
    }

    // QueryAst::Wildcard — wildcard patterns (e.g. ip:192.168.1.*) parse here
    if let QueryAst::Wildcard(ref w) = ast {
        if let Some(rewritten) = try_rewrite(&w.field, &w.value) {
            return rewritten;
        }
    }

    // Recursively rewrite compound subqueries; pass all other variants through unchanged
    match ast {
        QueryAst::Bool(b) => {
            use quickwit_query::query_ast::BoolQuery;
            let rewritten = BoolQuery {
                must: b.must.into_iter().map(rewrite_ip_terms).collect(),
                should: b.should.into_iter().map(rewrite_ip_terms).collect(),
                must_not: b.must_not.into_iter().map(rewrite_ip_terms).collect(),
                filter: b.filter.into_iter().map(rewrite_ip_terms).collect(),
                minimum_should_match: b.minimum_should_match,
            };
            QueryAst::Bool(rewritten)
        }
        QueryAst::Boost { underlying, boost } => QueryAst::Boost {
            underlying: Box::new(rewrite_ip_terms(*underlying)),
            boost,
        },
        other => other,
    }
}

/// Count the number of unique fields in a parsed query
pub fn count_query_fields(
    env: &mut JNIEnv,
    query_string: JString,
    schema_ptr: jni::sys::jlong,
    default_search_fields: jobject,
) -> Result<usize> {
    debug_println!("RUST DEBUG: count_query_fields called");

    // Get the query string
    let query_str: String = env.get_string(&query_string)?.into();

    debug_println!(
        "RUST DEBUG: Counting fields for query: '{}' with schema_ptr: {}",
        query_str,
        schema_ptr
    );

    // Extract default search fields from JNI
    let mut default_fields_vec = extract_default_search_fields(env, default_search_fields)?;

    // If default fields is empty, extract ALL indexed text fields from schema
    if default_fields_vec.is_empty() {
        default_fields_vec = extract_text_fields_from_schema(env, schema_ptr)?;
    }

    // Parse the query using Quickwit's parser
    let default_fields_option = if default_fields_vec.is_empty() {
        None
    } else {
        Some(default_fields_vec.clone())
    };

    let query_ast = query_ast_from_user_text(&query_str, default_fields_option.clone());

    let parse_fields = match &default_fields_option {
        Some(fields) => fields,
        None => &Vec::new(),
    };

    let parsed_ast = query_ast.parse_user_query(parse_fields)
        .map_err(|e| anyhow!("Failed to parse query '{}': {}", query_str, e))?;

    // Extract unique fields from the parsed query
    let fields = extract_fields_from_query_ast(&parsed_ast);

    debug_println!(
        "RUST DEBUG: Query '{}' impacts {} fields: {:?}",
        query_str,
        fields.len(),
        fields
    );

    Ok(fields.len())
}
