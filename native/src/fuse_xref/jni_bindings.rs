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

//! JNI bindings for Binary Fuse Filter XRef
//!
//! Provides native methods for:
//! - Building FuseXRef from source splits
//! - Searching FuseXRef with queries
//! - Loading and saving FuseXRef bundles

use std::path::Path;
use std::sync::Arc;
use std::collections::HashMap;
use std::sync::Mutex;

use jni::objects::{JClass, JObject, JObjectArray, JString};
use jni::sys::{jlong, jstring, jint};
use jni::JNIEnv;

use once_cell::sync::Lazy;

use super::builder::FuseXRefBuilder;
use super::query::search_with_json;
use super::storage::{load_fuse_xref, load_fuse_xref_from_uri, save_fuse_xref};
use super::types::*;
use crate::debug_println;
use crate::merge_types::{MergeAwsConfig, MergeAzureConfig};
use crate::utils::jstring_to_string;

// Global registry for loaded FuseXRef instances
static FUSE_XREF_REGISTRY: Lazy<Mutex<HashMap<jlong, Arc<FuseXRef>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

static NEXT_HANDLE: Lazy<Mutex<jlong>> = Lazy::new(|| Mutex::new(1));

/// Get the next handle ID
fn next_handle() -> jlong {
    let mut handle = NEXT_HANDLE.lock().unwrap();
    let id = *handle;
    *handle += 1;
    id
}

/// Store a FuseXRef and return its handle
fn store_fuse_xref(xref: FuseXRef) -> jlong {
    let handle = next_handle();
    let mut registry = FUSE_XREF_REGISTRY.lock().unwrap();
    registry.insert(handle, Arc::new(xref));
    handle
}

/// Get a FuseXRef by handle
fn get_fuse_xref(handle: jlong) -> Option<Arc<FuseXRef>> {
    let registry = FUSE_XREF_REGISTRY.lock().unwrap();
    registry.get(&handle).cloned()
}

/// Remove a FuseXRef by handle
fn remove_fuse_xref(handle: jlong) -> Option<Arc<FuseXRef>> {
    let mut registry = FUSE_XREF_REGISTRY.lock().unwrap();
    registry.remove(&handle)
}

/// Helper to create a Java string
fn make_jstring(env: &mut JNIEnv, s: &str) -> jstring {
    match env.new_string(s) {
        Ok(js) => js.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// JNI: Build a FuseXRef from source splits
///
/// Returns JSON metadata on success, or "ERROR: message" on failure
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_xref_XRefSplit_nativeBuildFuseXRef(
    mut env: JNIEnv,
    _class: JClass,
    xref_id: JString,
    index_uid: JString,
    source_splits: JObject, // XRefSourceSplit[]
    output_path: JString,
    included_fields: JObject, // String[] or null
    temp_directory_path: JString,
    filter_type: JString, // "fuse8" or "fuse16"
    compression: JString, // "none", "zstd", "zstd1", "zstd3", "zstd6", "zstd9"
    // AWS config (all nullable)
    aws_access_key: JString,
    aws_secret_key: JString,
    aws_session_token: JString,
    aws_region: JString,
    aws_endpoint_url: JString,
    aws_force_path_style: jni::sys::jboolean,
    // Azure config (all nullable)
    azure_account_name: JString,
    azure_account_key: JString,
    azure_bearer_token: JString,
    _azure_endpoint_url: JString,
) -> jstring {
    debug_println!("[FUSE_XREF_JNI] nativeBuildFuseXRef called");

    // Extract XRef ID
    let xref_id_str = match jstring_to_string(&mut env, &xref_id) {
        Ok(s) => s,
        Err(e) => return make_jstring(&mut env, &format!("ERROR: Invalid xref_id: {}", e)),
    };

    // Extract index UID
    let index_uid_str = match jstring_to_string(&mut env, &index_uid) {
        Ok(s) => s,
        Err(e) => return make_jstring(&mut env, &format!("ERROR: Invalid index_uid: {}", e)),
    };

    // Extract output path
    let output_path_str = match jstring_to_string(&mut env, &output_path) {
        Ok(s) => s,
        Err(e) => return make_jstring(&mut env, &format!("ERROR: Invalid output_path: {}", e)),
    };

    // Extract source splits
    let source_splits_vec = match extract_fuse_source_split_array(&mut env, &source_splits) {
        Ok(v) => v,
        Err(e) => return make_jstring(&mut env, &format!("ERROR: Invalid source_splits: {}", e)),
    };

    // Extract included fields (optional)
    let included_fields_vec = if !included_fields.is_null() {
        match extract_string_array(&mut env, &included_fields) {
            Ok(v) => v,
            Err(e) => return make_jstring(&mut env, &format!("ERROR: Invalid included_fields: {}", e)),
        }
    } else {
        Vec::new()
    };

    // Extract temp directory (optional)
    let temp_dir_path = if !temp_directory_path.is_null() {
        jstring_to_string(&mut env, &temp_directory_path).ok()
    } else {
        None
    };

    // Extract filter type (default to fuse8)
    let filter_type_enum = if !filter_type.is_null() {
        match jstring_to_string(&mut env, &filter_type) {
            Ok(s) if s == "fuse16" => FuseFilterType::Fuse16,
            _ => FuseFilterType::Fuse8,
        }
    } else {
        FuseFilterType::Fuse8
    };

    // Extract compression type (default to zstd3)
    let compression_enum = if !compression.is_null() {
        match jstring_to_string(&mut env, &compression) {
            Ok(s) => CompressionType::from_str(&s),
            _ => CompressionType::default(),
        }
    } else {
        CompressionType::default()
    };

    debug_println!(
        "[FUSE_XREF_JNI] Filter type: {:?}, Compression: {:?}",
        filter_type_enum,
        compression_enum
    );

    // Build AWS config if provided
    let aws_config = if !aws_access_key.is_null() {
        let access_key = jstring_to_string(&mut env, &aws_access_key).unwrap_or_default();
        let secret_key = jstring_to_string(&mut env, &aws_secret_key).unwrap_or_default();

        if !access_key.is_empty() && !secret_key.is_empty() {
            Some(MergeAwsConfig {
                access_key,
                secret_key,
                session_token: jstring_to_string(&mut env, &aws_session_token).ok(),
                region: jstring_to_string(&mut env, &aws_region)
                    .unwrap_or_else(|_| "us-east-1".to_string()),
                endpoint_url: jstring_to_string(&mut env, &aws_endpoint_url).ok(),
                force_path_style: aws_force_path_style != 0,
            })
        } else {
            None
        }
    } else {
        None
    };

    // Build Azure config if provided
    let azure_config = if !azure_account_name.is_null() {
        let account_name = jstring_to_string(&mut env, &azure_account_name).unwrap_or_default();

        if !account_name.is_empty() {
            Some(MergeAzureConfig {
                account_name,
                account_key: jstring_to_string(&mut env, &azure_account_key).ok(),
                bearer_token: jstring_to_string(&mut env, &azure_bearer_token).ok(),
                endpoint_url: None, // Not used for FuseXRef
            })
        } else {
            None
        }
    } else {
        None
    };

    // Build config
    let config = FuseXRefBuildConfig {
        xref_id: xref_id_str.clone(),
        index_uid: index_uid_str.clone(),
        source_splits: source_splits_vec,
        aws_config,
        azure_config,
        included_fields: included_fields_vec,
        temp_directory_path: temp_dir_path,
        filter_type: filter_type_enum,
        compression: compression_enum,
    };

    debug_println!(
        "[FUSE_XREF_JNI] Building FuseXRef: id={}, index={}, {} splits",
        xref_id_str,
        index_uid_str,
        config.source_splits.len()
    );

    // Build the FuseXRef
    let builder = FuseXRefBuilder::new(config);
    let output_path = Path::new(&output_path_str);

    match builder.build(output_path) {
        Ok(metadata) => {
            debug_println!(
                "[FUSE_XREF_JNI] ✅ Build succeeded: {} splits, {} terms",
                metadata.split_registry.splits.len(),
                metadata.total_terms
            );
            match serde_json::to_string(&metadata) {
                Ok(json) => make_jstring(&mut env, &json),
                Err(e) => make_jstring(&mut env, &format!("ERROR: Failed to serialize result: {}", e)),
            }
        }
        Err(e) => {
            debug_println!("[FUSE_XREF_JNI] ❌ Build failed: {}", e);
            make_jstring(&mut env, &format!("ERROR: {}", e))
        }
    }
}

/// JNI: Load a FuseXRef from a local file and return a handle
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_xref_XRefSearcher_nativeLoadFuseXRef(
    mut env: JNIEnv,
    _class: JClass,
    path: JString,
) -> jlong {
    let path_str = match jstring_to_string(&mut env, &path) {
        Ok(s) => s,
        Err(e) => {
            debug_println!("[FUSE_XREF_JNI] Invalid path: {}", e);
            return -1;
        }
    };

    debug_println!("[FUSE_XREF_JNI] Loading FuseXRef from local file: {}", path_str);

    match load_fuse_xref(Path::new(&path_str)) {
        Ok(xref) => {
            let handle = store_fuse_xref(xref);
            debug_println!("[FUSE_XREF_JNI] ✅ Loaded FuseXRef, handle={}", handle);
            handle
        }
        Err(e) => {
            debug_println!("[FUSE_XREF_JNI] ❌ Failed to load: {}", e);
            -1
        }
    }
}

/// JNI: Load a FuseXRef from a URI (supports s3://, azure://, file://) and return a handle
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_xref_XRefSearcher_nativeLoadFuseXRefFromUri(
    mut env: JNIEnv,
    _class: JClass,
    uri: JString,
    // AWS credentials
    aws_access_key: JString,
    aws_secret_key: JString,
    aws_session_token: JString,
    aws_region: JString,
    aws_endpoint_url: JString,
    aws_force_path_style: jni::sys::jboolean,
    // Azure credentials
    azure_account_name: JString,
    azure_account_key: JString,
    azure_bearer_token: JString,
) -> jlong {
    let uri_str = match jstring_to_string(&mut env, &uri) {
        Ok(s) => s,
        Err(e) => {
            debug_println!("[FUSE_XREF_JNI] Invalid URI: {}", e);
            return -1;
        }
    };

    debug_println!("[FUSE_XREF_JNI] Loading FuseXRef from URI: {}", uri_str);

    // Build AWS config if provided
    let aws_config = if !aws_access_key.is_null() {
        let access_key = jstring_to_string(&mut env, &aws_access_key).unwrap_or_default();
        let secret_key = jstring_to_string(&mut env, &aws_secret_key).unwrap_or_default();

        if !access_key.is_empty() && !secret_key.is_empty() {
            Some(MergeAwsConfig {
                access_key,
                secret_key,
                session_token: jstring_to_string(&mut env, &aws_session_token).ok(),
                region: jstring_to_string(&mut env, &aws_region)
                    .unwrap_or_else(|_| "us-east-1".to_string()),
                endpoint_url: jstring_to_string(&mut env, &aws_endpoint_url).ok(),
                force_path_style: aws_force_path_style != 0,
            })
        } else {
            None
        }
    } else {
        None
    };

    // Build Azure config if provided
    let azure_config = if !azure_account_name.is_null() {
        let account_name = jstring_to_string(&mut env, &azure_account_name).unwrap_or_default();

        if !account_name.is_empty() {
            Some(MergeAzureConfig {
                account_name,
                account_key: jstring_to_string(&mut env, &azure_account_key).ok(),
                bearer_token: jstring_to_string(&mut env, &azure_bearer_token).ok(),
                endpoint_url: None,
            })
        } else {
            None
        }
    } else {
        None
    };

    // Load FuseXRef from URI
    match load_fuse_xref_from_uri(&uri_str, aws_config, azure_config) {
        Ok(xref) => {
            let handle = store_fuse_xref(xref);
            debug_println!("[FUSE_XREF_JNI] ✅ Loaded FuseXRef from URI, handle={}", handle);
            handle
        }
        Err(e) => {
            debug_println!("[FUSE_XREF_JNI] ❌ Failed to load from URI: {}", e);
            -1
        }
    }
}

/// JNI: Close a FuseXRef handle
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_xref_XRefSearcher_nativeCloseFuseXRef(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    if let Some(_) = remove_fuse_xref(handle) {
        debug_println!("[FUSE_XREF_JNI] Closed FuseXRef handle={}", handle);
    }
}

/// JNI: Search a FuseXRef with a query
///
/// Returns JSON search result or "ERROR: message"
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_xref_XRefSearcher_nativeSearchFuseXRef(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query_json: JString,
    limit: jint,
) -> jstring {
    let xref = match get_fuse_xref(handle) {
        Some(x) => x,
        None => return make_jstring(&mut env, "ERROR: Invalid FuseXRef handle"),
    };

    let query_str = match jstring_to_string(&mut env, &query_json) {
        Ok(s) => s,
        Err(e) => return make_jstring(&mut env, &format!("ERROR: Invalid query JSON: {}", e)),
    };

    debug_println!(
        "[FUSE_XREF_JNI] Searching handle={} with limit={}",
        handle,
        limit
    );

    match search_with_json(&xref, &query_str, limit as usize) {
        Ok(result) => {
            debug_println!(
                "[FUSE_XREF_JNI] ✅ Search found {} matching splits",
                result.num_matching_splits
            );
            match serde_json::to_string(&result) {
                Ok(json) => make_jstring(&mut env, &json),
                Err(e) => make_jstring(&mut env, &format!("ERROR: Failed to serialize result: {}", e)),
            }
        }
        Err(e) => {
            debug_println!("[FUSE_XREF_JNI] ❌ Search failed: {}", e);
            make_jstring(&mut env, &format!("ERROR: {}", e))
        }
    }
}

/// JNI: Get metadata for a loaded FuseXRef
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_xref_XRefSearcher_nativeGetFuseXRefMetadata(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jstring {
    let xref = match get_fuse_xref(handle) {
        Some(x) => x,
        None => return make_jstring(&mut env, "ERROR: Invalid FuseXRef handle"),
    };

    match serde_json::to_string(&xref.header) {
        Ok(json) => make_jstring(&mut env, &json),
        Err(e) => make_jstring(&mut env, &format!("ERROR: Failed to serialize header: {}", e)),
    }
}

/// JNI: Get the number of splits in a FuseXRef
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_xref_XRefSearcher_nativeGetFuseXRefSplitCount(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jint {
    match get_fuse_xref(handle) {
        Some(xref) => xref.num_splits() as jint,
        None => -1,
    }
}

/// JNI: Search a FuseXRef with a query string (Quickwit syntax)
///
/// Parses the query string using the schema stored in the FuseXRef and evaluates it.
/// Returns JSON search result or "ERROR: message"
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_xref_XRefSearcher_nativeSearchWithQueryString(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query_string: JString,
    limit: jint,
) -> jstring {
    use super::query::search_with_query_string;

    let xref = match get_fuse_xref(handle) {
        Some(x) => x,
        None => return make_jstring(&mut env, "ERROR: Invalid FuseXRef handle"),
    };

    let query_str = match jstring_to_string(&mut env, &query_string) {
        Ok(s) => s,
        Err(e) => return make_jstring(&mut env, &format!("ERROR: Invalid query string: {}", e)),
    };

    debug_println!(
        "[FUSE_XREF_JNI] Searching with query string: '{}', limit={}",
        query_str,
        limit
    );

    match search_with_query_string(&xref, &query_str, limit as usize) {
        Ok(result) => {
            debug_println!(
                "[FUSE_XREF_JNI] ✅ Query string search found {} matching splits",
                result.num_matching_splits
            );
            match serde_json::to_string(&result) {
                Ok(json) => make_jstring(&mut env, &json),
                Err(e) => make_jstring(&mut env, &format!("ERROR: Failed to serialize result: {}", e)),
            }
        }
        Err(e) => {
            debug_println!("[FUSE_XREF_JNI] ❌ Query string search failed: {}", e);
            make_jstring(&mut env, &format!("ERROR: {}", e))
        }
    }
}

/// JNI: Parse a query string into QueryAst JSON
///
/// This allows clients to parse a query string using the schema stored in the FuseXRef
/// without needing a SplitSearcher. Returns the QueryAst as JSON or "ERROR: message".
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_xref_XRefSearcher_nativeParseQuery(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    query_string: JString,
) -> jstring {
    use super::query::{parse_query_string, query_to_json};

    let xref = match get_fuse_xref(handle) {
        Some(x) => x,
        None => return make_jstring(&mut env, "ERROR: Invalid FuseXRef handle"),
    };

    let query_str = match jstring_to_string(&mut env, &query_string) {
        Ok(s) => s,
        Err(e) => return make_jstring(&mut env, &format!("ERROR: Invalid query string: {}", e)),
    };

    debug_println!("[FUSE_XREF_JNI] Parsing query string: '{}'", query_str);

    match parse_query_string(&xref, &query_str) {
        Ok(query) => {
            match query_to_json(&query) {
                Ok(json) => {
                    debug_println!("[FUSE_XREF_JNI] ✅ Parsed query: {}", json);
                    make_jstring(&mut env, &json)
                }
                Err(e) => make_jstring(&mut env, &format!("ERROR: Failed to serialize query: {}", e)),
            }
        }
        Err(e) => {
            debug_println!("[FUSE_XREF_JNI] ❌ Query parsing failed: {}", e);
            make_jstring(&mut env, &format!("ERROR: {}", e))
        }
    }
}

/// JNI: Transform range queries to match-all (static utility)
///
/// This is useful for pre-processing queries before XRef evaluation.
/// Range queries cannot be evaluated by filters, so they're transformed to MatchAll.
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_xref_XRefSearcher_nativeTransformRangeToMatchAll(
    mut env: JNIEnv,
    _class: JClass,
    query_json: JString,
) -> jstring {
    use quickwit_query::query_ast::QueryAst;

    let json_str = match jstring_to_string(&mut env, &query_json) {
        Ok(s) => s,
        Err(e) => return make_jstring(&mut env, &format!("ERROR: Invalid query JSON: {}", e)),
    };

    // Parse the query
    let query: QueryAst = match serde_json::from_str(&json_str) {
        Ok(q) => q,
        Err(e) => return make_jstring(&mut env, &format!("ERROR: Failed to parse query: {}", e)),
    };

    // Transform range queries to match-all
    let transformed = transform_range_to_match_all(&query);

    // Serialize back to JSON
    match serde_json::to_string(&transformed) {
        Ok(json) => make_jstring(&mut env, &json),
        Err(e) => make_jstring(&mut env, &format!("ERROR: Failed to serialize transformed query: {}", e)),
    }
}

/// JNI: Get the schema from a FuseXRef
///
/// Returns the Tantivy schema pointer, or 0 on failure
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_xref_XRefSearcher_nativeGetSchema(
    _env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jlong {
    let xref = match get_fuse_xref(handle) {
        Some(x) => x,
        None => {
            debug_println!("[FUSE_XREF_JNI] nativeGetSchema: Invalid handle {}", handle);
            return 0;
        }
    };

    // Get schema JSON from header
    let schema_json = match &xref.header.schema_json {
        Some(json) => json,
        None => {
            debug_println!("[FUSE_XREF_JNI] nativeGetSchema: No schema_json in FuseXRef header");
            return 0;
        }
    };

    // Parse the schema JSON using serde
    match serde_json::from_str::<tantivy::schema::Schema>(schema_json) {
        Ok(schema) => {
            // Register schema in Arc registry (required for Schema JNI methods to work)
            let schema_arc = std::sync::Arc::new(schema);
            let ptr = crate::utils::arc_to_jlong(schema_arc);
            debug_println!("[FUSE_XREF_JNI] ✅ nativeGetSchema: Registered schema in Arc registry, ptr={}", ptr);
            ptr
        }
        Err(e) => {
            debug_println!("[FUSE_XREF_JNI] ❌ nativeGetSchema: Failed to parse schema: {}", e);
            0
        }
    }
}

/// Transform range/wildcard queries to MatchAll recursively
fn transform_range_to_match_all(query: &quickwit_query::query_ast::QueryAst) -> quickwit_query::query_ast::QueryAst {
    use quickwit_query::query_ast::QueryAst;

    match query {
        QueryAst::Range(_) | QueryAst::Wildcard(_) | QueryAst::Regex(_) => {
            QueryAst::MatchAll
        }
        QueryAst::Bool(bool_query) => {
            QueryAst::Bool(quickwit_query::query_ast::BoolQuery {
                must: bool_query.must.iter().map(transform_range_to_match_all).collect(),
                should: bool_query.should.iter().map(transform_range_to_match_all).collect(),
                must_not: bool_query.must_not.iter().map(transform_range_to_match_all).collect(),
                filter: bool_query.filter.iter().map(transform_range_to_match_all).collect(),
                minimum_should_match: bool_query.minimum_should_match,
            })
        }
        QueryAst::Boost { underlying, boost } => {
            QueryAst::Boost {
                underlying: Box::new(transform_range_to_match_all(underlying)),
                boost: *boost,
            }
        }
        // Return other query types unchanged
        other => other.clone(),
    }
}

// ============================================================================
// Helper functions
// ============================================================================

/// Extract a String array from JNI
fn extract_string_array(env: &mut JNIEnv, array: &JObject) -> anyhow::Result<Vec<String>> {
    if array.is_null() {
        return Ok(Vec::new());
    }

    let array_ref: &JObjectArray = unsafe { &*(array as *const JObject as *const JObjectArray) };
    let len = env.get_array_length(array_ref)? as usize;
    let mut result = Vec::with_capacity(len);

    for i in 0..len {
        let elem = env.get_object_array_element(array_ref, i as i32)?;
        let jstr: JString = elem.into();
        let s = jstring_to_string(env, &jstr)?;
        result.push(s);
    }

    Ok(result)
}

/// Extract FuseXRefSourceSplit from a Java XRefSourceSplit object
fn extract_fuse_source_split(env: &mut JNIEnv, obj: &JObject) -> anyhow::Result<FuseXRefSourceSplit> {
    use anyhow::{anyhow, Context};

    if obj.is_null() {
        return Err(anyhow!("XRefSourceSplit object is null"));
    }

    // Extract uri
    let uri_obj = env
        .call_method(obj, "getUri", "()Ljava/lang/String;", &[])?
        .l()
        .context("Failed to get uri")?;
    let uri: JString = uri_obj.into();
    let uri_str = jstring_to_string(env, &uri)?;

    // Extract splitId
    let split_id_obj = env
        .call_method(obj, "getSplitId", "()Ljava/lang/String;", &[])?
        .l()
        .context("Failed to get splitId")?;
    let split_id: JString = split_id_obj.into();
    let split_id_str = jstring_to_string(env, &split_id)?;

    // Extract footerStart
    let footer_start = env
        .call_method(obj, "getFooterStart", "()J", &[])?
        .j()
        .context("Failed to get footerStart")? as u64;

    // Extract footerEnd
    let footer_end = env
        .call_method(obj, "getFooterEnd", "()J", &[])?
        .j()
        .context("Failed to get footerEnd")? as u64;

    // Extract numDocs (optional)
    let num_docs_obj = env
        .call_method(obj, "getNumDocs", "()Ljava/lang/Long;", &[])?
        .l()
        .context("Failed to get numDocs")?;
    let num_docs = if !num_docs_obj.is_null() {
        let value = env
            .call_method(&num_docs_obj, "longValue", "()J", &[])?
            .j()
            .context("Failed to unbox numDocs")?;
        Some(value as u64)
    } else {
        None
    };

    Ok(FuseXRefSourceSplit {
        uri: uri_str,
        split_id: split_id_str,
        footer_start,
        footer_end,
        num_docs,
    })
}

/// Extract an array of FuseXRefSourceSplit from JNI
fn extract_fuse_source_split_array(
    env: &mut JNIEnv,
    array: &JObject,
) -> anyhow::Result<Vec<FuseXRefSourceSplit>> {
    if array.is_null() {
        return Ok(Vec::new());
    }

    let array_ref: &JObjectArray = unsafe { &*(array as *const JObject as *const JObjectArray) };
    let len = env.get_array_length(array_ref)? as usize;
    let mut result = Vec::with_capacity(len);

    for i in 0..len {
        let elem = env.get_object_array_element(array_ref, i as i32)?;
        let source_split = extract_fuse_source_split(env, &elem)?;
        result.push(source_split);
    }

    Ok(result)
}
