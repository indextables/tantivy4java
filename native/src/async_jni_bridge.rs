// async_jni_bridge.rs - Async-first JNI bridge utilities
//
// This module provides utilities to convert sync JNI functions to async-first patterns
// that work seamlessly with Quickwit's async architecture.

use jni::objects::{JClass, JString, JObject};
use jni::sys::{jlong, jobject, jstring, jint, jboolean};
use jni::JNIEnv;
use std::future::Future;
use crate::runtime_manager::{QuickwitRuntimeManager, block_on_operation};
use crate::common::to_java_exception;
use crate::debug_println;

/// Async-first JNI wrapper that handles all the boilerplate for converting
/// sync JNI functions to use async Quickwit operations without deadlocks.
///
/// This version avoids thread-safety issues by working with String results
/// instead of jobject directly.
pub fn execute_async_jni_string_operation<F>(
    mut env: JNIEnv,
    operation_name: &str,
    future_fn: F,
) -> jobject
where
    F: FnOnce() -> std::pin::Pin<Box<dyn Future<Output = Result<String, anyhow::Error>> + Send + 'static>>,
{
    debug_println!("🔄 ASYNC_JNI: Starting {} operation", operation_name);

    // Create the future using the provided function
    let future = future_fn();

    // Execute using our async-first runtime manager
    match block_on_operation(future) {
        Ok(result_string) => {
            debug_println!("✅ ASYNC_JNI: {} operation completed successfully", operation_name);
            // Convert string result to JNI object on main thread
            match env.new_string(&result_string) {
                Ok(jstring) => jstring.into_raw(),
                Err(e) => {
                    debug_println!("❌ ASYNC_JNI: Failed to create result string: {}", e);
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to create result string: {}", e));
                    std::ptr::null_mut()
                }
            }
        }
        Err(e) => {
            debug_println!("❌ ASYNC_JNI: {} operation failed: {}", operation_name, e);
            to_java_exception(&mut env, &e);
            std::ptr::null_mut()
        }
    }
}

// DISABLED: Legacy async JNI wrapper - replaced with thread-safe entry point pattern
// where all JNI data is extracted at entry points and no JNI types are passed to core functions

/// Create an async implementation function for search operations
///
/// This is used to convert the sync JNI function body to pure async.
/// The resulting function can be passed to execute_async_jni_operation.
pub async fn perform_async_search(
    mut env: JNIEnv<'_>,
    searcher_ptr: jlong,
    query_ast_json: JString<'_>,
    limit: jint,
) -> Result<String, anyhow::Error> {
    debug_println!("🔍 ASYNC_JNI: Starting async search operation");

    // Extract the query JSON string first (since JNI types can't be sent across threads)
    let query_json: String = env.get_string(&query_ast_json)?.into();

    // Import the actual async search implementation
    use crate::split_searcher_replacement::perform_search_async_impl;

    // Delegate to the async implementation with extracted data
    perform_search_async_impl(env, searcher_ptr, query_json, limit).await
}

/// Create an async implementation function for document retrieval operations
pub async fn perform_async_doc_retrieval(
    _env: JNIEnv<'_>,
    searcher_ptr: jlong,
    segment_ord: u32,
    doc_id: u32,
) -> Result<jlong, anyhow::Error> {
    debug_println!("📄 ASYNC_JNI: Starting async document retrieval operation");

    // Import the actual async document retrieval implementation
    use crate::split_searcher_replacement::perform_doc_retrieval_async_impl;

    // Delegate to the async implementation
    perform_doc_retrieval_async_impl(_env, searcher_ptr, segment_ord, doc_id).await
}

/// Create an async implementation function for schema retrieval operations
pub async fn perform_async_schema_retrieval(
    _env: JNIEnv<'_>,
    searcher_ptr: jlong,
) -> Result<i64, anyhow::Error> {
    debug_println!("📋 ASYNC_JNI: Starting async schema retrieval operation");

    // Import the actual async schema retrieval implementation
    use crate::split_searcher_replacement::perform_schema_retrieval_async_impl;

    // Delegate to the async implementation
    perform_schema_retrieval_async_impl(_env, searcher_ptr).await
}

/// Macro to simplify creating async-first JNI functions
///
/// Usage:
/// ```rust
/// async_jni_function! {
///     fn Java_com_example_Class_method(
///         env: JNIEnv,
///         _class: JClass,
///         param1: jlong,
///         param2: JString,
///     ) -> jobject {
///         perform_async_operation(env, param1, param2).await
///     }
/// }
/// ```
#[macro_export]
macro_rules! async_jni_function {
    (
        fn $fn_name:ident(
            $env:ident: JNIEnv,
            $class:ident: JClass,
            $($param_name:ident: $param_type:ty),*
        ) -> $return_type:ty {
            $async_body:expr
        }
    ) => {
        #[no_mangle]
        pub extern "system" fn $fn_name(
            $env: JNIEnv,
            $class: JClass,
            $($param_name: $param_type),*
        ) -> $return_type {
            use crate::async_jni_bridge::execute_async_jni_operation;

            execute_async_jni_operation($env, stringify!($fn_name), |env| {
                Box::new(async move {
                    $async_body
                })
            })
        }
    };
}

/// Macro for async JNI functions that return primitives
#[macro_export]
macro_rules! async_jni_primitive_function {
    (
        fn $fn_name:ident(
            $env:ident: JNIEnv,
            $class:ident: JClass,
            $($param_name:ident: $param_type:ty),*
        ) -> $return_type:ty, default = $default_value:expr, body = $async_body:expr
    ) => {
        #[no_mangle]
        pub extern "system" fn $fn_name(
            $env: JNIEnv,
            $class: JClass,
            $($param_name: $param_type),*
        ) -> $return_type {
            use crate::async_jni_bridge::execute_async_jni_primitive;

            execute_async_jni_primitive($env, stringify!($fn_name), |env| {
                Box::new(async move {
                    $async_body
                })
            }, $default_value)
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests are basic structural tests.
    // Full integration tests require JNI environment setup.

    #[test]
    fn test_runtime_manager_integration() {
        let manager = QuickwitRuntimeManager::global();
        assert!(!manager.is_in_runtime_context());
    }
}