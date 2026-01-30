// wildcard_analysis.rs - Wildcard pattern analysis and SplitWildcardQuery JNI implementation
// Part of the Smart Wildcard AST Skipping optimization feature

use anyhow::{anyhow, Result};
use jni::objects::{JClass, JObject, JString};
use jni::sys::{jboolean, jstring, JNI_FALSE, JNI_TRUE};
use jni::JNIEnv;
use quickwit_query::query_ast::{QueryAst, WildcardQuery};

use crate::debug_println;
use crate::query::wildcard::contains_multi_wildcards;
use super::query_converters::convert_query_ast_to_json_string;

/// Check if a wildcard pattern is expensive to evaluate.
///
/// A pattern is expensive if it:
/// - Starts with a wildcard character (leading wildcard): requires full FST scan
/// - Contains multiple wildcards (multi-wildcard): requires multiple regex expansions
///
/// Efficient patterns (not expensive):
/// - Suffix wildcards like "foo*": use FST prefix traversal
/// - Single character wildcards like "fo?bar": limited regex matching
pub fn is_expensive_wildcard_pattern(pattern: &str) -> bool {
    if pattern.is_empty() {
        return false;
    }

    // Check for leading wildcard (starts with * or ?)
    let first_char = pattern.chars().next().unwrap();
    if first_char == '*' || first_char == '?' {
        debug_println!(
            "RUST DEBUG: Pattern '{}' is expensive (leading wildcard)",
            pattern
        );
        return true;
    }

    // Check for multi-wildcard pattern
    if contains_multi_wildcards(pattern) {
        debug_println!(
            "RUST DEBUG: Pattern '{}' is expensive (multi-wildcard)",
            pattern
        );
        return true;
    }

    debug_println!(
        "RUST DEBUG: Pattern '{}' is NOT expensive (prefix or simple wildcard)",
        pattern
    );
    false
}

/// Convert a SplitWildcardQuery to QueryAst.
///
/// This converts the Java wildcard query to a Quickwit WildcardQuery,
/// which properly handles wildcard patterns with * and ? characters.
pub fn convert_wildcard_query_to_query_ast(env: &mut JNIEnv, obj: &JObject) -> Result<QueryAst> {
    // Extract field and pattern from Java SplitWildcardQuery object
    let field_obj = env
        .get_field(obj, "field", "Ljava/lang/String;")
        .map_err(|e| anyhow!("Failed to get field: {}", e))?;
    let pattern_obj = env
        .get_field(obj, "pattern", "Ljava/lang/String;")
        .map_err(|e| anyhow!("Failed to get pattern: {}", e))?;

    let field_jstring: JString = field_obj.l()?.into();
    let pattern_jstring: JString = pattern_obj.l()?.into();

    let field: String = env.get_string(&field_jstring)?.into();
    let pattern: String = env.get_string(&pattern_jstring)?.into();

    debug_println!(
        "RUST DEBUG: SplitWildcardQuery - field: '{}', pattern: '{}'",
        field,
        pattern
    );

    // Create Quickwit WildcardQuery directly - it handles * and ? wildcards natively
    let wildcard_query = WildcardQuery {
        field,
        value: pattern,
        lenient: true, // Be lenient with missing fields
    };

    let query_ast = QueryAst::Wildcard(wildcard_query);

    debug_println!(
        "RUST DEBUG: Created WildcardQuery QueryAst for wildcard pattern"
    );
    Ok(query_ast)
}

/// Wrapper function for JNI layer - converts to JSON string
pub fn convert_wildcard_query_to_ast(env: &mut JNIEnv, obj: &JObject) -> Result<String> {
    let query_ast = convert_wildcard_query_to_query_ast(env, obj)?;
    convert_query_ast_to_json_string(query_ast)
}

// =====================================================================
// JNI Entry Points
// =====================================================================

/// Check if a wildcard pattern is expensive to evaluate.
///
/// JNI signature: static native boolean nativeIsExpensivePattern(String pattern)
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitWildcardQuery_nativeIsExpensivePattern(
    mut env: JNIEnv,
    _class: JClass,
    pattern: JString,
) -> jboolean {
    let pattern_result = env.get_string(&pattern);

    match pattern_result {
        Ok(pattern_str) => {
            let pattern_rust: String = pattern_str.into();
            let is_expensive = is_expensive_wildcard_pattern(&pattern_rust);
            if is_expensive {
                JNI_TRUE
            } else {
                JNI_FALSE
            }
        }
        Err(e) => {
            debug_println!(
                "RUST DEBUG: Failed to get pattern string: {}",
                e
            );
            JNI_FALSE
        }
    }
}

/// Convert a SplitWildcardQuery to QueryAst JSON.
///
/// JNI signature: native String toQueryAstJson()
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitWildcardQuery_toQueryAstJson(
    mut env: JNIEnv,
    obj: JObject,
) -> jstring {
    let result = convert_wildcard_query_to_ast(&mut env, &obj);

    match result {
        Ok(json) => match env.new_string(json) {
            Ok(jstring) => jstring.into_raw(),
            Err(e) => {
                debug_println!(
                    "RUST DEBUG: Failed to create JString from QueryAst JSON: {}",
                    e
                );
                crate::common::to_java_exception(
                    &mut env,
                    &anyhow!("Failed to create JString from QueryAst JSON: {}", e),
                );
                std::ptr::null_mut()
            }
        },
        Err(e) => {
            debug_println!(
                "RUST DEBUG: Error converting SplitWildcardQuery to QueryAst: {}",
                e
            );
            crate::common::to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_expensive_wildcard_pattern() {
        // Leading wildcards are expensive
        assert!(is_expensive_wildcard_pattern("*foo"));
        assert!(is_expensive_wildcard_pattern("*foo*"));
        assert!(is_expensive_wildcard_pattern("?bar"));

        // Multi-wildcards are expensive
        assert!(is_expensive_wildcard_pattern("foo*bar*"));
        assert!(is_expensive_wildcard_pattern("*foo*bar*"));

        // Suffix wildcards are NOT expensive
        assert!(!is_expensive_wildcard_pattern("foo*"));
        assert!(!is_expensive_wildcard_pattern("hello*"));

        // Simple patterns are NOT expensive
        assert!(!is_expensive_wildcard_pattern("foo"));
        assert!(!is_expensive_wildcard_pattern("hello"));

        // Single character wildcard at end is NOT expensive
        assert!(!is_expensive_wildcard_pattern("foo?"));

        // Empty pattern is NOT expensive
        assert!(!is_expensive_wildcard_pattern(""));
    }

}
