// split_query/mod.rs - Native implementation for SplitQuery classes using Quickwit libraries
// Refactored from split_query.rs into submodules for better maintainability

pub mod parse_query;
pub mod query_converters;
pub mod schema_cache;

// Re-exports from submodules
pub use parse_query::{
    count_query_fields, create_split_parsed_query, create_split_query_from_ast,
    extract_default_search_fields, extract_fields_from_query_ast, extract_fields_from_schema,
    extract_text_fields_from_schema, parse_query_string,
};
pub use query_converters::{
    convert_boolean_query_to_ast, convert_boolean_query_to_query_ast,
    convert_json_literal_to_value, convert_phrase_query_to_ast, convert_phrase_query_to_query_ast,
    convert_query_ast_to_json_string, convert_query_list, convert_range_query_to_ast,
    convert_range_query_to_query_ast, convert_split_query_to_ast, convert_split_query_to_json,
    convert_term_query_to_ast, convert_term_query_to_query_ast,
};
pub use schema_cache::{
    get_searcher_schema, get_split_schema, remove_searcher_schema, store_searcher_schema,
    store_split_schema, SEARCHER_SCHEMA_MAPPING, SPLIT_SCHEMA_CACHE,
};

use jni::objects::{JClass, JObject, JString};
use jni::sys::{jlong, jobject, jstring};
use jni::JNIEnv;
use quickwit_query::query_ast::QueryAst;

use crate::debug_println;

// =====================================================================
// JNI Entry Points
// =====================================================================

/// Convert a SplitTermQuery to QueryAst JSON (FOR TESTING ONLY - Production should use SplitSearcher.search() directly)
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitTermQuery_toQueryAstJson(
    mut env: JNIEnv,
    obj: JObject,
) -> jstring {
    let result = convert_term_query_to_ast(&mut env, &obj);
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
                    &anyhow::anyhow!("Failed to create JString from QueryAst JSON: {}", e),
                );
                std::ptr::null_mut()
            }
        },
        Err(e) => {
            debug_println!(
                "RUST DEBUG: Error converting SplitTermQuery to QueryAst: {}",
                e
            );
            crate::common::to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

/// Convert a SplitBooleanQuery to QueryAst JSON (FOR TESTING ONLY - Production should use SplitSearcher.search() directly)
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitBooleanQuery_toQueryAstJson(
    mut env: JNIEnv,
    obj: JObject,
) -> jstring {
    let result = convert_boolean_query_to_ast(&mut env, &obj);
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
                    &anyhow::anyhow!("Failed to create JString from QueryAst JSON: {}", e),
                );
                std::ptr::null_mut()
            }
        },
        Err(e) => {
            debug_println!(
                "RUST DEBUG: Error converting SplitBooleanQuery to QueryAst: {}",
                e
            );
            crate::common::to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

/// Convert a SplitMatchAllQuery to QueryAst JSON (FOR TESTING ONLY - Production should use SplitSearcher.search() directly)
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitMatchAllQuery_toQueryAstJson(
    mut env: JNIEnv,
    _obj: JObject,
) -> jstring {
    // MatchAll is simple - just convert to JSON directly
    let query_ast = QueryAst::MatchAll;
    let result = convert_query_ast_to_json_string(query_ast);
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
                    &anyhow::anyhow!("Failed to create JString from QueryAst JSON: {}", e),
                );
                std::ptr::null_mut()
            }
        },
        Err(e) => {
            debug_println!(
                "RUST DEBUG: Error converting SplitMatchAllQuery to QueryAst: {}",
                e
            );
            crate::common::to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

/// Convert a SplitRangeQuery to QueryAst JSON (FOR TESTING ONLY - Production should use SplitSearcher.search() directly)
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitRangeQuery_toQueryAstJson(
    mut env: JNIEnv,
    obj: JObject,
) -> jstring {
    let result = convert_range_query_to_ast(&mut env, &obj);
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
                    &anyhow::anyhow!("Failed to create JString from QueryAst JSON: {}", e),
                );
                std::ptr::null_mut()
            }
        },
        Err(e) => {
            debug_println!(
                "RUST DEBUG: Error converting SplitRangeQuery to QueryAst: {}",
                e
            );
            crate::common::to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

/// Count the number of unique fields impacted by a query
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitQuery_nativeCountQueryFields(
    mut env: JNIEnv,
    _class: JClass,
    query_string: JString,
    schema_ptr: jlong,
    default_search_fields: jobject,
) -> jni::sys::jint {
    debug_println!("🚀 ENTRY: nativeCountQueryFields called");

    let result = count_query_fields(&mut env, query_string, schema_ptr, default_search_fields);

    match result {
        Ok(count) => {
            debug_println!("🚀 SUCCESS: Query impacts {} fields", count);
            count as jni::sys::jint
        }
        Err(e) => {
            debug_println!("🚀 ERROR: Error counting query fields: {}", e);
            crate::common::to_java_exception(&mut env, &e);
            -1
        }
    }
}

/// Parse a query string into a SplitQuery using Quickwit's query parser
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitQuery_nativeParseQuery(
    mut env: JNIEnv,
    _class: JClass,
    query_string: JString,
    schema_ptr: jlong,
    default_search_fields: jobject,
) -> jobject {
    debug_println!(
        "🚀 ENTRY: Java_io_indextables_tantivy4java_split_SplitQuery_parseQuery called with schema_ptr={}",
        schema_ptr
    );
    let result = parse_query_string(&mut env, query_string, schema_ptr, default_search_fields);
    debug_println!(
        "🚀 RESULT: parse_query_string returned result type: {}",
        if result.is_ok() { "Ok" } else { "Err" }
    );
    match result {
        Ok(query_obj) => {
            debug_println!("🚀 SUCCESS: Returning valid query object");
            query_obj
        }
        Err(e) => {
            debug_println!("🚀 ERROR: Error parsing query string: {}", e);
            // Throw exception with the error message so user knows what went wrong
            crate::common::to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

/// Convert a SplitPhraseQuery to QueryAst JSON (FOR TESTING ONLY - Production should use SplitSearcher.search() directly)
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitPhraseQuery_toQueryAstJson(
    mut env: JNIEnv,
    obj: JObject,
) -> jstring {
    let result = convert_phrase_query_to_ast(&mut env, &obj);
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
                    &anyhow::anyhow!("Failed to create JString from QueryAst JSON: {}", e),
                );
                std::ptr::null_mut()
            }
        },
        Err(e) => {
            debug_println!(
                "RUST DEBUG: Error converting SplitPhraseQuery to QueryAst: {}",
                e
            );
            crate::common::to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}
