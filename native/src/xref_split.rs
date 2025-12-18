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

//! Cross-Reference Split Builder
//!
//! This module provides the XRef split builder which creates a lightweight index
//! that consolidates term dictionaries from multiple source splits. Each document
//! in the XRef split represents one source split, enabling fast query routing.
//!
//! KEY DESIGN: Creates N documents where N = number of source splits.
//! Each document represents one source split, and posting lists point
//! to these split-level documents (not original documents).
//!
//! MEMORY EFFICIENCY: Uses streaming merge with memory-mapped I/O to minimize
//! memory footprint. See xref_streaming module for implementation details.

use std::path::Path;

use anyhow::Result;

use crate::debug_println;
use crate::xref_streaming::{StreamingXRefBuilder, StreamingXRefConfig};
use crate::xref_types::*;

/// XRef split file extension
pub const XREF_EXTENSION: &str = ".xref.split";

/// Builder for creating XRef splits
///
/// KEY DESIGN: Creates N documents where N = number of source splits.
/// Each document represents one source split, and posting lists point
/// to these split-level documents (not original documents).
///
/// This implementation uses streaming merge with memory-mapped I/O for
/// minimal memory footprint, even with thousands of source splits.
pub struct XRefSplitBuilder {
    config: XRefBuildConfig,
}

impl XRefSplitBuilder {
    /// Create a new XRef split builder
    pub fn new(config: XRefBuildConfig) -> Self {
        Self { config }
    }

    /// Build the XRef split using streaming merge
    ///
    /// Memory profile:
    /// - Per-split overhead: ~1KB (iterator state)
    /// - Min-heap: O(N) where N = number of source splits
    /// - Total for 10,000 splits: ~15MB (vs previous approach: potentially GB+)
    pub fn build(&self, output_path: &Path) -> Result<XRefMetadata> {
        debug_println!(
            "🔧 XREF BUILD: Building XRef split with {} source splits (streaming mode)",
            self.config.source_splits.len()
        );

        // Create streaming config from base config
        let streaming_config = StreamingXRefConfig {
            base_config: self.config.clone(),
            max_concurrent_splits: 500,   // Process up to 500 splits at once
            term_batch_size: 100_000,      // Commit every 100K terms
        };

        // Use streaming builder
        let streaming_builder = StreamingXRefBuilder::new(streaming_config);
        streaming_builder.build(output_path)
    }
}

// ============================================================================
// JNI bindings
// ============================================================================

use jni::objects::{JClass, JObject, JString};
use jni::sys::{jobject, jstring};
use jni::JNIEnv;

use crate::utils::jstring_to_string;
use crate::merge_types::{MergeAwsConfig, MergeAzureConfig};

/// Helper to convert a string to jstring, returning null on error
fn make_jstring(env: &mut JNIEnv, s: &str) -> jstring {
    match env.new_string(s) {
        Ok(js) => js.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// JNI: Build an XRef split from multiple source splits
///
/// The source_splits parameter is an array of XRefSourceSplit objects, each containing:
/// - uri (String): Split URI
/// - splitId (String): Split identifier
/// - footerStart (long): Footer start offset for efficient split opening
/// - footerEnd (long): Footer end offset for efficient split opening
/// - numDocs (Long): Optional document count
/// - sizeBytes (Long): Optional size in bytes
/// - docMappingJson (String): Optional doc mapping JSON for schema derivation
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_xref_XRefSplit_nativeBuildXRefSplit(
    mut env: JNIEnv,
    _class: JClass,
    xref_id: JString,
    index_uid: JString,
    source_splits: JObject, // XRefSourceSplit[]
    output_path: JString,
    include_positions: jni::sys::jboolean,
    included_fields: JObject, // String[] or null
    temp_directory_path: JString, // Temp directory for intermediate files (nullable)
    heap_size: jni::sys::jlong, // Heap size for index writer in bytes
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
    azure_endpoint_url: JString,
) -> jstring {
    // Extract XRef ID
    let xref_id_str = match jstring_to_string(&mut env, &xref_id) {
        Ok(s) => s,
        Err(e) => {
            return make_jstring(&mut env, &format!("ERROR: Invalid xref_id: {}", e));
        }
    };

    // Extract index UID
    let index_uid_str = match jstring_to_string(&mut env, &index_uid) {
        Ok(s) => s,
        Err(e) => {
            return make_jstring(&mut env, &format!("ERROR: Invalid index_uid: {}", e));
        }
    };

    // Extract output path
    let output_path_str = match jstring_to_string(&mut env, &output_path) {
        Ok(s) => s,
        Err(e) => {
            return make_jstring(&mut env, &format!("ERROR: Invalid output_path: {}", e));
        }
    };

    // Extract source splits array
    let source_splits_vec = match extract_xref_source_split_array(&mut env, &source_splits) {
        Ok(v) => v,
        Err(e) => {
            return make_jstring(&mut env, &format!("ERROR: Invalid source_splits: {}", e));
        }
    };

    // Extract included fields (optional)
    let included_fields_vec = if !included_fields.is_null() {
        match extract_string_array(&mut env, &included_fields) {
            Ok(v) => v,
            Err(e) => {
                return make_jstring(&mut env, &format!("ERROR: Invalid included_fields: {}", e));
            }
        }
    } else {
        Vec::new()
    };

    // Extract temp directory path (optional)
    let temp_dir_path = if !temp_directory_path.is_null() {
        jstring_to_string(&mut env, &temp_directory_path).ok()
    } else {
        None
    };

    // Build AWS config if provided
    let aws_config = if !aws_access_key.is_null() {
        let access_key = jstring_to_string(&mut env, &aws_access_key).unwrap_or_default();
        let secret_key = jstring_to_string(&mut env, &aws_secret_key).unwrap_or_default();

        if !access_key.is_empty() && !secret_key.is_empty() {
            Some(MergeAwsConfig {
                access_key,
                secret_key,
                session_token: jstring_to_string(&mut env, &aws_session_token).ok(),
                region: jstring_to_string(&mut env, &aws_region).unwrap_or_else(|_| "us-east-1".to_string()),
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
            let account_key = jstring_to_string(&mut env, &azure_account_key).ok();
            let bearer_token = jstring_to_string(&mut env, &azure_bearer_token).ok();
            let endpoint_url = jstring_to_string(&mut env, &azure_endpoint_url).ok();

            Some(MergeAzureConfig {
                account_name,
                account_key,
                bearer_token,
                endpoint_url,
            })
        } else {
            None
        }
    } else {
        None
    };

    // Build XRef config
    let config = XRefBuildConfig {
        xref_id: xref_id_str,
        index_uid: index_uid_str,
        source_splits: source_splits_vec,
        aws_config,
        azure_config,
        included_fields: included_fields_vec,
        include_positions: include_positions != 0,
        temp_directory_path: temp_dir_path,
        heap_size: heap_size as usize,
    };

    // Debug logging
    debug_println!("🔧 XREF JNI: Configuration received:");
    debug_println!("  xref_id: {}", config.xref_id);
    debug_println!("  index_uid: {}", config.index_uid);
    debug_println!("  source_splits: {} splits", config.source_splits.len());
    debug_println!("  temp_directory_path: {:?}", config.temp_directory_path);
    debug_println!("  heap_size: {} bytes", config.heap_size);
    debug_println!("  aws_config present: {}", config.aws_config.is_some());
    debug_println!("  azure_config present: {}", config.azure_config.is_some());

    // Build the XRef split using streaming implementation
    let builder = XRefSplitBuilder::new(config);
    let output_path = Path::new(&output_path_str);

    match builder.build(output_path) {
        Ok(metadata) => {
            debug_println!("🔧 XREF JNI: Build succeeded! {} splits in registry", metadata.num_splits());
            match serde_json::to_string(&metadata) {
                Ok(json) => make_jstring(&mut env, &json),
                Err(e) => make_jstring(&mut env, &format!("ERROR: Failed to serialize metadata: {}", e)),
            }
        }
        Err(e) => {
            debug_println!("🔧 XREF JNI: Build FAILED: {}", e);
            make_jstring(&mut env, &format!("ERROR: Failed to build XRef split: {}", e))
        }
    }
}

/// Helper to extract a String array from JNI
fn extract_string_array(env: &mut JNIEnv, array: &JObject) -> Result<Vec<String>> {
    use jni::objects::JObjectArray;
    use anyhow::Context;

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

/// Helper to extract an XRefSourceSplit from a Java XRefSourceSplit object
fn extract_xref_source_split(env: &mut JNIEnv, obj: &JObject) -> Result<XRefSourceSplit> {
    use anyhow::{anyhow, Context};

    if obj.is_null() {
        return Err(anyhow!("XRefSourceSplit object is null"));
    }

    // Extract uri (String)
    let uri_obj = env.call_method(obj, "getUri", "()Ljava/lang/String;", &[])?
        .l()
        .context("Failed to get uri")?;
    let uri: JString = uri_obj.into();
    let uri_str = jstring_to_string(env, &uri)?;

    // Extract splitId (String)
    let split_id_obj = env.call_method(obj, "getSplitId", "()Ljava/lang/String;", &[])?
        .l()
        .context("Failed to get splitId")?;
    let split_id: JString = split_id_obj.into();
    let split_id_str = jstring_to_string(env, &split_id)?;

    // Extract footerStart (long)
    let footer_start = env.call_method(obj, "getFooterStart", "()J", &[])?
        .j()
        .context("Failed to get footerStart")? as u64;

    // Extract footerEnd (long)
    let footer_end = env.call_method(obj, "getFooterEnd", "()J", &[])?
        .j()
        .context("Failed to get footerEnd")? as u64;

    // Extract numDocs (Long - nullable)
    let num_docs_obj = env.call_method(obj, "getNumDocs", "()Ljava/lang/Long;", &[])?
        .l()
        .context("Failed to get numDocs")?;
    let num_docs = if !num_docs_obj.is_null() {
        let value = env.call_method(&num_docs_obj, "longValue", "()J", &[])?
            .j()
            .context("Failed to unbox numDocs")?;
        Some(value as u64)
    } else {
        None
    };

    // Extract sizeBytes (Long - nullable)
    let size_bytes_obj = env.call_method(obj, "getSizeBytes", "()Ljava/lang/Long;", &[])?
        .l()
        .context("Failed to get sizeBytes")?;
    let size_bytes = if !size_bytes_obj.is_null() {
        let value = env.call_method(&size_bytes_obj, "longValue", "()J", &[])?
            .j()
            .context("Failed to unbox sizeBytes")?;
        Some(value as u64)
    } else {
        None
    };

    // Extract docMappingJson (String - nullable)
    let doc_mapping_obj = env.call_method(obj, "getDocMappingJson", "()Ljava/lang/String;", &[])?
        .l()
        .context("Failed to get docMappingJson")?;
    let doc_mapping_json = if !doc_mapping_obj.is_null() {
        let doc_mapping: JString = doc_mapping_obj.into();
        Some(jstring_to_string(env, &doc_mapping)?)
    } else {
        None
    };

    let mut source_split = XRefSourceSplit::new(uri_str, split_id_str, footer_start, footer_end);
    if let Some(n) = num_docs {
        source_split = source_split.with_num_docs(n);
    }
    if let Some(s) = size_bytes {
        source_split = source_split.with_size_bytes(s);
    }
    if let Some(json) = doc_mapping_json {
        source_split = source_split.with_doc_mapping_json(json);
    }

    Ok(source_split)
}

/// Helper to extract an array of XRefSourceSplit objects from JNI
fn extract_xref_source_split_array(env: &mut JNIEnv, array: &JObject) -> Result<Vec<XRefSourceSplit>> {
    use jni::objects::JObjectArray;

    if array.is_null() {
        return Ok(Vec::new());
    }

    let array_ref: &JObjectArray = unsafe { &*(array as *const JObject as *const JObjectArray) };
    let len = env.get_array_length(array_ref)? as usize;
    let mut result = Vec::with_capacity(len);

    for i in 0..len {
        let elem = env.get_object_array_element(array_ref, i as i32)?;
        let source_split = extract_xref_source_split(env, &elem)?;
        result.push(source_split);
    }

    Ok(result)
}

// ============================================================================
// XRef Query Transformation
// ============================================================================

use serde_json::Value;

/// Transform a QueryAst JSON by replacing all range queries with match_all queries.
/// This is needed for XRef splits because they don't have fast fields.
pub fn transform_range_to_match_all(query_json: &str) -> Result<String> {
    let mut query_value: Value = serde_json::from_str(query_json)
        .map_err(|e| anyhow::anyhow!("Failed to parse query JSON: {}", e))?;

    let transformed = transform_range_recursive(&mut query_value);

    if transformed {
        debug_println!("🔧 XREF: Transformed range queries to match_all for XRef compatibility");
    }

    serde_json::to_string(&query_value)
        .map_err(|e| anyhow::anyhow!("Failed to serialize transformed query: {}", e))
}

/// Recursively transform range queries to match_all in a JSON value.
fn transform_range_recursive(value: &mut Value) -> bool {
    let mut transformed = false;

    match value {
        Value::Object(map) => {
            if let Some(type_val) = map.get("type") {
                if type_val.as_str() == Some("range") {
                    map.clear();
                    map.insert("type".to_string(), Value::String("match_all".to_string()));
                    return true;
                }
            }

            for (_, v) in map.iter_mut() {
                if transform_range_recursive(v) {
                    transformed = true;
                }
            }
        }
        Value::Array(arr) => {
            for item in arr.iter_mut() {
                if transform_range_recursive(item) {
                    transformed = true;
                }
            }
        }
        _ => {}
    }

    transformed
}

// NOTE: nativeTransformRangeToMatchAll JNI binding moved to fuse_xref/jni_bindings.rs

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transform_range_to_match_all_simple() {
        let input = r#"{"type":"range","field":"age","lower_bound":{"Included":25},"upper_bound":{"Included":35}}"#;
        let result = transform_range_to_match_all(input).unwrap();
        assert_eq!(result, r#"{"type":"match_all"}"#);
    }

    #[test]
    fn test_transform_range_to_match_all_nested_boolean() {
        let input = r#"{"type":"bool","must":[{"type":"term","field":"status","value":"active"},{"type":"range","field":"age","lower_bound":{"Included":25},"upper_bound":{"Included":35}}]}"#;
        let result = transform_range_to_match_all(input).unwrap();
        assert!(result.contains(r#""type":"match_all""#));
        assert!(!result.contains(r#""type":"range""#));
        assert!(result.contains(r#""type":"term""#));
    }

    #[test]
    fn test_transform_no_range_query() {
        let input = r#"{"type":"term","field":"name","value":"test"}"#;
        let result = transform_range_to_match_all(input).unwrap();
        assert!(result.contains(r#""type":"term""#));
    }

    #[test]
    fn test_transform_match_all_unchanged() {
        let input = r#"{"type":"match_all"}"#;
        let result = transform_range_to_match_all(input).unwrap();
        assert!(result.contains(r#""type":"match_all""#));
    }
}
