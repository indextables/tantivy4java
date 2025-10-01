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

use jni::objects::JClass;
use jni::sys::jstring;
use jni::JNIEnv;

mod debug;  // Debug utilities and conditional logging
mod cache_debug;  // Enhanced cache debugging and monitoring utilities
mod runtime_manager;  // Global Quickwit runtime manager for async-first architecture
mod async_jni_bridge;  // Async-first JNI bridge utilities
mod async_split_searcher;  // Minimal async-first wrappers using Quickwit's existing components
mod schema;
mod document;
mod query;
mod index;
mod searcher;
mod doc_address;
mod utils;
// mod query_conversion;  // Disabled in favor of split_query approach
mod text_analyzer;
pub mod quickwit_split;
pub mod merge_types;  // Types for standalone merge binary
mod standalone_searcher;  // Clean standalone searcher implementation
mod standalone_searcher_jni;  // JNI bindings for standalone searcher
mod split_searcher_replacement;  // Replacement SplitSearcher JNI methods using StandaloneSearcher
mod split_query;  // SplitQuery Java objects and native conversion using Quickwit libraries
// mod split_searcher;  // Legacy implementation (now disabled)
mod split_cache_manager;  // Global cache manager following Quickwit patterns
// mod split_searcher_simple;  // Disabled to avoid conflicts
mod test_query_parser;  // Test module for query parser debugging
mod common;
mod extract_helpers;
mod global_cache;  // Global cache infrastructure following Quickwit's pattern

pub use schema::*;
pub use document::*;
pub use query::*;
pub use index::*;
pub use searcher::*;
pub use doc_address::*;
pub use utils::*;
pub use text_analyzer::*;
pub use quickwit_split::{merge_splits_impl, InternalMergeConfig, InternalAwsConfig};
pub use merge_types::*;
// pub use split_searcher::*;  // Disabled - now using replacement JNI methods
// pub use split_searcher_simple::*;  // Disabled to avoid conflicts

#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_Tantivy_getVersion(
    env: JNIEnv,
    _class: JClass,
) -> jstring {
    let version = env.new_string("0.24.0").unwrap();
    version.into_raw()
}

// Global cache configuration JNI functions
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_GlobalCacheConfig_initializeGlobalCache(
    mut env: JNIEnv,
    _class: JClass,
    fast_field_cache_mb: jni::sys::jlong,
    split_footer_cache_mb: jni::sys::jlong,
    partial_request_cache_mb: jni::sys::jlong,
    max_concurrent_splits: jni::sys::jint,
    aggregation_memory_mb: jni::sys::jlong,
    aggregation_bucket_limit: jni::sys::jint,
    warmup_memory_gb: jni::sys::jlong,
    split_cache_gb: jni::sys::jlong,
    split_cache_max_splits: jni::sys::jint,
    split_cache_path: jni::objects::JString,
) -> jni::sys::jboolean {
    use bytesize::ByteSize;
    use std::num::NonZeroU32;
    use std::path::PathBuf;
    use crate::global_cache::{GlobalCacheConfig, initialize_global_cache};
    use quickwit_config::SplitCacheLimits;
    
    let split_cache_path_opt = if !split_cache_path.is_null() {
        match env.get_string(&split_cache_path) {
            Ok(path_str) => {
                let path = path_str.to_string_lossy().to_string();
                if !path.is_empty() {
                    Some(PathBuf::from(path))
                } else {
                    None
                }
            }
            Err(_) => None
        }
    } else {
        None
    };
    
    let split_cache_limits = if split_cache_gb > 0 && split_cache_max_splits > 0 {
        Some(SplitCacheLimits {
            max_num_bytes: ByteSize::gb(split_cache_gb as u64),
            max_num_splits: NonZeroU32::new(split_cache_max_splits as u32).unwrap_or(NonZeroU32::new(10_000).unwrap()),
            num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
            max_file_descriptors: NonZeroU32::new(100).unwrap(),
        })
    } else {
        None
    };
    
    let config = GlobalCacheConfig {
        fast_field_cache_capacity: ByteSize::mb(fast_field_cache_mb as u64),
        split_footer_cache_capacity: ByteSize::mb(split_footer_cache_mb as u64),
        partial_request_cache_capacity: ByteSize::mb(partial_request_cache_mb as u64),
        max_concurrent_splits: max_concurrent_splits as usize,
        aggregation_memory_limit: ByteSize::mb(aggregation_memory_mb as u64),
        aggregation_bucket_limit: aggregation_bucket_limit as u32,
        warmup_memory_budget: ByteSize::gb(warmup_memory_gb as u64),
        split_cache_limits,
        split_cache_root_path: split_cache_path_opt,
    };
    
    if initialize_global_cache(config) {
        jni::sys::JNI_TRUE
    } else {
        jni::sys::JNI_FALSE
    }
}