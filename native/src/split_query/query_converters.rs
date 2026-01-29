// query_converters.rs - Query conversion functions
// Extracted from split_query.rs during refactoring

use std::ops::Bound;

use anyhow::{anyhow, Result};
use jni::objects::{JObject, JString, JValue};
use jni::JNIEnv;
use quickwit_query::query_ast::{QueryAst, FieldPresenceQuery};
use quickwit_query::JsonLiteral;

use crate::debug_println;

/// Convert a QueryAst to JSON string using Quickwit's proven serialization
pub fn convert_query_ast_to_json_string(query: QueryAst) -> Result<String> {
    // Use Quickwit's proven serde serialization - this automatically handles
    // the "type" field and proper format that Quickwit expects
    serde_json::to_string(&query)
        .map_err(|e| anyhow!("Failed to serialize QueryAst to JSON: {}", e))
}

// Wrapper function for JNI layer - converts to JSON string
pub fn convert_term_query_to_ast(env: &mut JNIEnv, obj: &JObject) -> Result<String> {
    let query_ast = convert_term_query_to_query_ast(env, obj)?;
    convert_query_ast_to_json_string(query_ast)
}

pub fn convert_term_query_to_query_ast(env: &mut JNIEnv, obj: &JObject) -> Result<QueryAst> {
    // Extract field and value from Java SplitTermQuery object
    let field_obj = env
        .get_field(obj, "field", "Ljava/lang/String;")
        .map_err(|e| anyhow!("Failed to get field: {}", e))?;
    let value_obj = env
        .get_field(obj, "value", "Ljava/lang/String;")
        .map_err(|e| anyhow!("Failed to get value: {}", e))?;

    let field_jstring: JString = field_obj.l()?.into();
    let value_jstring: JString = value_obj.l()?.into();

    let field: String = env.get_string(&field_jstring)?.into();
    let value: String = env.get_string(&value_jstring)?.into();

    // CRITICAL FIX: Do NOT automatically lowercase term values!
    // Different tokenizers handle case differently:
    // - "default" tokenizer: lowercases during indexing, so queries should be lowercased
    // - "raw" tokenizer: preserves case exactly, so queries should preserve case
    // - "keyword" tokenizer: preserves case exactly, so queries should preserve case
    //
    // The correct approach is to let Quickwit/Tantivy handle tokenization according to
    // the field's configured tokenizer. Automatic lowercasing breaks fields that use
    // case-preserving tokenizers like "raw" and "keyword".
    //
    // This was the root cause of the boolean query bug where:
    // - Data indexed as "Sales" with raw tokenizer
    // - Queries lowercased to "sales" by this code
    // - No matches found because "sales" != "Sales"

    debug_println!(
        "RUST DEBUG: SplitTermQuery - field: '{}', value: '{}' (preserving original case)",
        field,
        value
    );

    // Create proper Quickwit QueryAst using official structures
    use quickwit_query::query_ast::TermQuery;

    let term_query = TermQuery { field, value };

    let query_ast = QueryAst::Term(term_query);

    debug_println!("RUST DEBUG: Created TermQuery QueryAst using native Quickwit structures");
    Ok(query_ast)
}

// Wrapper function for JNI layer - converts to JSON string
pub fn convert_boolean_query_to_ast(env: &mut JNIEnv, obj: &JObject) -> Result<String> {
    let query_ast = convert_boolean_query_to_query_ast(env, obj)?;
    convert_query_ast_to_json_string(query_ast)
}

pub fn convert_boolean_query_to_query_ast(env: &mut JNIEnv, obj: &JObject) -> Result<QueryAst> {
    // Extract clauses from Java SplitBooleanQuery object
    let must_list_obj = env
        .get_field(obj, "mustQueries", "Ljava/util/List;")
        .map_err(|e| anyhow!("Failed to get mustQueries: {}", e))?;
    let should_list_obj = env
        .get_field(obj, "shouldQueries", "Ljava/util/List;")
        .map_err(|e| anyhow!("Failed to get shouldQueries: {}", e))?;
    let must_not_list_obj = env
        .get_field(obj, "mustNotQueries", "Ljava/util/List;")
        .map_err(|e| anyhow!("Failed to get mustNotQueries: {}", e))?;
    let min_should_match_obj = env
        .get_field(obj, "minimumShouldMatch", "Ljava/lang/Integer;")
        .map_err(|e| anyhow!("Failed to get minimumShouldMatch: {}", e))?;

    // Convert Java lists to Rust QueryAst clauses
    let must_clauses = convert_query_list(env, must_list_obj.l()?)?;
    let should_clauses = convert_query_list(env, should_list_obj.l()?)?;
    let must_not_clauses = convert_query_list(env, must_not_list_obj.l()?)?;

    // Handle minimum should match
    let min_should_match_jobj = min_should_match_obj.l()?;
    let minimum_should_match = if min_should_match_jobj.is_null() {
        None
    } else {
        let int_val = env.call_method(min_should_match_jobj, "intValue", "()I", &[])?;
        Some(int_val.i()? as usize)
    };

    // Create proper Quickwit BoolQuery using official structures
    use quickwit_query::query_ast::BoolQuery;

    // CRITICAL FIX: Handle pure should clauses according to Lucene/tantivy semantics
    // In Lucene/tantivy, a boolean query with only should clauses should match
    // documents that satisfy at least one should clause. However, Quickwit
    // requires explicit minimum_should_match for this behavior.
    let effective_minimum_should_match =
        if must_clauses.is_empty() && !should_clauses.is_empty() && minimum_should_match.is_none() {
            // Pure should clauses: automatically set minimum_should_match to 1
            debug_println!(
            "RUST DEBUG: 🔧 BOOLEAN QUERY FIX: Pure should clauses detected, setting minimum_should_match=1"
        );
            Some(1)
        } else {
            minimum_should_match
        };

    let bool_query = BoolQuery {
        must: must_clauses,
        must_not: must_not_clauses,
        should: should_clauses,
        filter: Vec::new(), // Java SplitBooleanQuery doesn't have filter, but Quickwit BoolQuery does
        minimum_should_match: effective_minimum_should_match,
    };

    let query_ast = QueryAst::Bool(bool_query);

    debug_println!("RUST DEBUG: Created BooleanQuery QueryAst using native Quickwit structures");
    Ok(query_ast)
}

// Wrapper function for JNI layer - converts to JSON string
pub fn convert_range_query_to_ast(env: &mut JNIEnv, obj: &JObject) -> Result<String> {
    let query_ast = convert_range_query_to_query_ast(env, obj)?;
    convert_query_ast_to_json_string(query_ast)
}

pub fn convert_range_query_to_query_ast(env: &mut JNIEnv, obj: &JObject) -> Result<QueryAst> {
    // Extract field, bounds, and field type from Java SplitRangeQuery object
    let field_obj = env
        .get_field(obj, "field", "Ljava/lang/String;")
        .map_err(|e| anyhow!("Failed to get field: {}", e))?;
    let lower_bound_obj = env
        .get_field(
            obj,
            "lowerBound",
            "Lio/indextables/tantivy4java/split/SplitRangeQuery$RangeBound;",
        )
        .map_err(|e| anyhow!("Failed to get lowerBound: {}", e))?;
    let upper_bound_obj = env
        .get_field(
            obj,
            "upperBound",
            "Lio/indextables/tantivy4java/split/SplitRangeQuery$RangeBound;",
        )
        .map_err(|e| anyhow!("Failed to get upperBound: {}", e))?;
    let field_type_obj = env
        .get_field(obj, "fieldType", "Ljava/lang/String;")
        .map_err(|e| anyhow!("Failed to get fieldType: {}", e))?;

    let field_jstring: JString = field_obj.l()?.into();
    let field: String = env.get_string(&field_jstring)?.into();

    let field_type_jstring: JString = field_type_obj.l()?.into();
    let field_type: String = env.get_string(&field_type_jstring)?.into();

    debug_println!(
        "RUST DEBUG: Converting range query for field '{}' with type '{}'",
        field,
        field_type
    );

    // Convert bounds with field type information
    let lower_bound_jobject = lower_bound_obj.l()?;
    let upper_bound_jobject = upper_bound_obj.l()?;
    let lower_bound = convert_range_bound(env, &lower_bound_jobject, &field_type)?;
    let upper_bound = convert_range_bound(env, &upper_bound_jobject, &field_type)?;

    // Create proper Quickwit RangeQuery using official structures
    use quickwit_query::query_ast::RangeQuery;

    let range_query = RangeQuery {
        field,
        lower_bound,
        upper_bound,
    };

    let query_ast = QueryAst::Range(range_query);

    debug_println!("RUST DEBUG: Created RangeQuery QueryAst using native Quickwit structures");
    Ok(query_ast)
}

fn convert_range_bound(
    env: &mut JNIEnv,
    bound_obj: &JObject,
    field_type: &str,
) -> Result<Bound<JsonLiteral>> {
    // Get the bound type
    let type_obj = env.get_field(
        bound_obj,
        "type",
        "Lio/indextables/tantivy4java/split/SplitRangeQuery$RangeBound$BoundType;",
    )?;
    let type_enum = type_obj.l()?;

    // Get enum name
    let name_obj = env.call_method(type_enum, "name", "()Ljava/lang/String;", &[])?;
    let name_jstring: JString = name_obj.l()?.into();
    let bound_type: String = env.get_string(&name_jstring)?.into();

    match bound_type.as_str() {
        "UNBOUNDED" => Ok(Bound::Unbounded),
        "INCLUSIVE" | "EXCLUSIVE" => {
            // Get the value
            let value_obj = env.get_field(bound_obj, "value", "Ljava/lang/String;")?;
            let value_jstring: JString = value_obj.l()?.into();
            let value: String = env.get_string(&value_jstring)?.into();
            let value_for_debug = value.clone(); // Clone for debug printing

            // Debug: Log field type and value before conversion
            debug_println!(
                "RUST DEBUG: Converting range bound - field_type: '{}', value: '{}'",
                field_type,
                value_for_debug
            );

            // Convert string value to JsonLiteral based on field type
            let json_literal = match field_type {
                "i64" | "int" | "integer" => {
                    let parsed: i64 = value
                        .parse()
                        .map_err(|e| anyhow!("Failed to parse '{}' as i64: {}", value, e))?;
                    JsonLiteral::Number(serde_json::Number::from(parsed))
                }
                "u64" | "uint" => {
                    let parsed: u64 = value
                        .parse()
                        .map_err(|e| anyhow!("Failed to parse '{}' as u64: {}", value, e))?;
                    JsonLiteral::Number(serde_json::Number::from(parsed))
                }
                "f64" | "float" | "double" => {
                    let parsed: f64 = value
                        .parse()
                        .map_err(|e| anyhow!("Failed to parse '{}' as f64: {}", value, e))?;
                    let number = serde_json::Number::from_f64(parsed)
                        .ok_or_else(|| anyhow!("Invalid f64 value: {}", parsed))?;
                    JsonLiteral::Number(number)
                }
                "str" | "string" | "text" => {
                    // Special case: if the field type is string but the value looks numeric, try to parse it
                    // This handles cases where numeric fields like "price" are incorrectly typed as "str"
                    if value.parse::<i64>().is_ok() {
                        let parsed: i64 = value.parse().unwrap();
                        debug_println!(
                            "RUST DEBUG: Field type '{}' but value '{}' looks like i64, converting to Number",
                            field_type,
                            value
                        );
                        JsonLiteral::Number(serde_json::Number::from(parsed))
                    } else if value.parse::<f64>().is_ok() {
                        let parsed: f64 = value.parse().unwrap();
                        debug_println!(
                            "RUST DEBUG: Field type '{}' but value '{}' looks like f64, converting to Number",
                            field_type,
                            value
                        );
                        let number = serde_json::Number::from_f64(parsed).unwrap();
                        JsonLiteral::Number(number)
                    } else {
                        JsonLiteral::String(value)
                    }
                }
                _ => {
                    debug_println!(
                        "RUST DEBUG: Unknown field type '{}', defaulting to string",
                        field_type
                    );
                    JsonLiteral::String(value)
                }
            };

            // Debug: Log the final JsonLiteral that was created
            debug_println!("RUST DEBUG: Created JsonLiteral: {:?}", json_literal);

            debug_println!(
                "RUST DEBUG: Converted bound value '{}' to {:?} for field type '{}'",
                value_for_debug,
                json_literal,
                field_type
            );

            match bound_type.as_str() {
                "INCLUSIVE" => Ok(Bound::Included(json_literal)),
                "EXCLUSIVE" => Ok(Bound::Excluded(json_literal)),
                _ => unreachable!(),
            }
        }
        _ => Err(anyhow!("Unknown bound type: {}", bound_type)),
    }
}

pub fn convert_query_list(env: &mut JNIEnv, list_obj: JObject) -> Result<Vec<QueryAst>> {
    if list_obj.is_null() {
        return Ok(Vec::new());
    }

    // Get list size
    let size_result = env.call_method(&list_obj, "size", "()I", &[])?;
    let size = size_result.i()? as usize;

    let mut queries = Vec::new();

    for i in 0..size {
        // Get element at index i
        let get_result = env.call_method(
            &list_obj,
            "get",
            "(I)Ljava/lang/Object;",
            &[JValue::Int(i as i32)],
        )?;
        let query_obj = get_result.l()?;

        // Convert based on the actual type
        let query_ast = convert_split_query_to_ast(env, &query_obj)?;
        queries.push(query_ast);
    }

    Ok(queries)
}

pub fn convert_split_query_to_ast(env: &mut JNIEnv, query_obj: &JObject) -> Result<QueryAst> {
    // Determine the actual type of the SplitQuery object
    let class = env.get_object_class(query_obj)?;
    let class_name_obj = env.call_method(class, "getName", "()Ljava/lang/String;", &[])?;
    let class_name_jstring: JString = class_name_obj.l()?.into();
    let class_name: String = env.get_string(&class_name_jstring)?.into();

    match class_name.as_str() {
        "io.indextables.tantivy4java.split.SplitTermQuery" => {
            // Create QueryAst directly using native Quickwit structures
            convert_term_query_to_query_ast(env, query_obj)
        }
        "io.indextables.tantivy4java.split.SplitBooleanQuery" => {
            // Create QueryAst directly using native Quickwit structures
            convert_boolean_query_to_query_ast(env, query_obj)
        }
        "io.indextables.tantivy4java.split.SplitRangeQuery" => {
            // Create QueryAst directly using native Quickwit structures
            convert_range_query_to_query_ast(env, query_obj)
        }
        "io.indextables.tantivy4java.split.SplitMatchAllQuery" => Ok(QueryAst::MatchAll),
        "io.indextables.tantivy4java.split.SplitParsedQuery" => {
            // SplitParsedQuery already contains the QueryAst as JSON - parse it back
            convert_parsed_query_to_query_ast(env, query_obj)
        }
        "io.indextables.tantivy4java.split.SplitPhraseQuery" => {
            // Create QueryAst directly using native Quickwit structures
            convert_phrase_query_to_query_ast(env, query_obj)
        }
        "io.indextables.tantivy4java.split.SplitExistsQuery" => {
            // Create QueryAst using FieldPresenceQuery for exists checks
            convert_exists_query_to_query_ast(env, query_obj)
        }
        _ => Err(anyhow!("Unsupported SplitQuery type: {}", class_name)),
    }
}

pub fn convert_parsed_query_to_query_ast(env: &mut JNIEnv, obj: &JObject) -> Result<QueryAst> {
    // Extract the queryAstJson field from SplitParsedQuery
    let json_obj = env
        .get_field(obj, "queryAstJson", "Ljava/lang/String;")
        .map_err(|e| anyhow!("Failed to get queryAstJson field: {}", e))?;
    let json_jstring: JString = json_obj.l()?.into();
    let json_str: String = env.get_string(&json_jstring)?.into();

    debug_println!(
        "RUST DEBUG: SplitParsedQuery - deserializing JSON: {}",
        json_str
    );

    // Parse the JSON back into QueryAst using Quickwit's proven deserialization
    let query_ast: QueryAst = serde_json::from_str(&json_str)
        .map_err(|e| anyhow!("Failed to deserialize QueryAst JSON '{}': {}", json_str, e))?;

    debug_println!(
        "RUST DEBUG: Successfully converted SplitParsedQuery to QueryAst: {:?}",
        query_ast
    );
    Ok(query_ast)
}

// Convert SplitQuery to JSON string for async operations
pub fn convert_split_query_to_json(env: &mut JNIEnv, query_obj: &JObject) -> Result<String, String> {
    debug_println!("🔄 CONVERT: Converting SplitQuery to JSON");

    // First convert to QueryAst
    let query_ast = convert_split_query_to_ast(env, query_obj)
        .map_err(|e| format!("Failed to convert SplitQuery to QueryAst: {}", e))?;

    // Then serialize QueryAst to JSON
    let json_str = serde_json::to_string(&query_ast)
        .map_err(|e| format!("Failed to serialize QueryAst to JSON: {}", e))?;

    debug_println!(
        "✅ CONVERT: Successfully converted SplitQuery to JSON: {}",
        json_str
    );
    Ok(json_str)
}

// Wrapper function for JNI layer - converts to JSON string
pub fn convert_phrase_query_to_ast(env: &mut JNIEnv, obj: &JObject) -> Result<String> {
    let query_ast = convert_phrase_query_to_query_ast(env, obj)?;
    convert_query_ast_to_json_string(query_ast)
}

pub fn convert_phrase_query_to_query_ast(env: &mut JNIEnv, obj: &JObject) -> Result<QueryAst> {
    // Extract field, terms, and slop from Java SplitPhraseQuery object
    let field_obj = env
        .get_field(obj, "field", "Ljava/lang/String;")
        .map_err(|e| anyhow!("Failed to get field: {}", e))?;
    let terms_obj = env
        .get_field(obj, "terms", "Ljava/util/List;")
        .map_err(|e| anyhow!("Failed to get terms: {}", e))?;
    let slop_obj = env
        .get_field(obj, "slop", "I")
        .map_err(|e| anyhow!("Failed to get slop: {}", e))?;

    let field_jstring: JString = field_obj.l()?.into();
    let field: String = env.get_string(&field_jstring)?.into();

    let slop = slop_obj.i()? as u32;

    // Convert Java List<String> to Rust Vec<String>
    let terms_list = terms_obj.l()?;
    let mut terms = Vec::new();

    // Get list size
    let list_size = env.call_method(&terms_list, "size", "()I", &[])?;
    let size = list_size.i()?;

    // Extract each term from the list
    for i in 0..size {
        let index_value = JValue::Int(i);
        let term_obj = env.call_method(&terms_list, "get", "(I)Ljava/lang/Object;", &[index_value])?;
        let term_jstring: JString = term_obj.l()?.into();
        let term: String = env.get_string(&term_jstring)?.into();

        // Lowercase term for text field tokenization compatibility
        terms.push(term.to_lowercase());
    }

    // Create phrase string by joining terms with spaces
    let phrase = terms.join(" ");

    debug_println!(
        "RUST DEBUG: SplitPhraseQuery - field: '{}', terms: {:?}, phrase: '{}', slop: {}",
        field,
        terms,
        phrase,
        slop
    );

    // Create proper Quickwit QueryAst using FullText query with Phrase mode
    use quickwit_query::query_ast::{FullTextMode, FullTextParams, FullTextQuery};
    use quickwit_query::MatchAllOrNone;

    let full_text_query = FullTextQuery {
        field,
        text: phrase,
        params: FullTextParams {
            tokenizer: None,
            mode: FullTextMode::Phrase { slop },
            zero_terms_query: MatchAllOrNone::MatchNone,
        },
        lenient: false,
    };

    let query_ast = QueryAst::FullText(full_text_query);

    debug_println!("RUST DEBUG: Created PhraseQuery QueryAst using FullText with Phrase mode");
    Ok(query_ast)
}

/// Helper function to convert JsonLiteral to serde_json::Value
#[allow(dead_code)]
pub fn convert_json_literal_to_value(literal: JsonLiteral) -> serde_json::Value {
    match literal {
        JsonLiteral::String(s) => serde_json::Value::String(s),
        JsonLiteral::Number(n) => serde_json::Value::Number(n),
        JsonLiteral::Bool(b) => serde_json::Value::Bool(b),
    }
}

// Wrapper function for JNI layer - converts to JSON string
pub fn convert_exists_query_to_ast(env: &mut JNIEnv, obj: &JObject) -> Result<String> {
    let query_ast = convert_exists_query_to_query_ast(env, obj)?;
    convert_query_ast_to_json_string(query_ast)
}

pub fn convert_exists_query_to_query_ast(env: &mut JNIEnv, obj: &JObject) -> Result<QueryAst> {
    // Extract field from Java SplitExistsQuery object
    let field_obj = env
        .get_field(obj, "field", "Ljava/lang/String;")
        .map_err(|e| anyhow!("Failed to get field: {}", e))?;

    let field_jstring: JString = field_obj.l()?.into();
    let field: String = env.get_string(&field_jstring)?.into();

    debug_println!(
        "RUST DEBUG: SplitExistsQuery - field: '{}'",
        field
    );

    // Create proper Quickwit QueryAst using FieldPresenceQuery
    let field_presence_query = FieldPresenceQuery { field };
    let query_ast = QueryAst::FieldPresence(field_presence_query);

    debug_println!("RUST DEBUG: Created FieldPresenceQuery QueryAst using native Quickwit structures");
    Ok(query_ast)
}
