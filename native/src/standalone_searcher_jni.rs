// standalone_searcher_jni.rs - JNI bindings for the clean StandaloneSearcher implementation

use std::sync::Arc;
use jni::objects::{JClass, JString};
use jni::sys::{jlong, jstring};
use jni::JNIEnv;

use crate::standalone_searcher::{StandaloneSearcher, StandaloneSearchConfig, SplitSearchMetadata};
use crate::utils::{register_object, remove_object, with_object};
use crate::common::to_java_exception;

use quickwit_proto::search::{SearchRequest, LeafSearchResponse};
use quickwit_doc_mapper::DocMapper;

/// Create a new StandaloneSearcher with default configuration
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_StandaloneSearcher_createNative(
    mut env: JNIEnv,
    _class: JClass,
) -> jlong {
    let result = StandaloneSearcher::default();
    match result {
        Ok(searcher) => register_object(searcher) as jlong,
        Err(error) => {
            to_java_exception(&mut env, &error);
            0
        }
    }
}

/// Create StandaloneSearcher with custom configuration
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_StandaloneSearcher_createWithConfigNative(
    mut env: JNIEnv,
    _class: JClass,
    fast_field_cache_mb: jlong,
    split_footer_cache_mb: jlong,
    max_concurrent_splits: jlong,
) -> jlong {
    let config = StandaloneSearchConfig {
        cache: crate::standalone_searcher::CacheConfig {
            fast_field_cache_capacity: bytesize::ByteSize::mb(fast_field_cache_mb as u64),
            split_footer_cache_capacity: bytesize::ByteSize::mb(split_footer_cache_mb as u64),
            partial_request_cache_capacity: bytesize::ByteSize::mb(64), // default
        },
        resources: crate::standalone_searcher::ResourceConfig {
            max_concurrent_splits: max_concurrent_splits as usize,
            aggregation_memory_limit: bytesize::ByteSize::mb(500),
            aggregation_bucket_limit: 65000,
        },
        timeouts: crate::standalone_searcher::TimeoutConfig {
            request_timeout: std::time::Duration::from_secs(30),
        },
        warmup: crate::standalone_searcher::WarmupConfig {
            memory_budget: bytesize::ByteSize::gb(100),
            split_initial_allocation: bytesize::ByteSize::gb(1),
        },
    };

    let result = StandaloneSearcher::new(config);
    match result {
        Ok(searcher) => register_object(searcher) as jlong,
        Err(error) => {
            to_java_exception(&mut env, &error);
            0
        }
    }
}

/// Search a single split
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_StandaloneSearcher_searchSplitNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
    split_uri_jstr: JString,
    split_id_jstr: JString,
    footer_start: jlong,
    footer_end: jlong,
    num_docs: jlong,
    delete_opstamp: jlong,
    search_request_ptr: jlong,
    doc_mapper_ptr: jlong,
) -> jlong {
    // Extract string values first (outside of with_object due to borrow checker)
    let split_uri: String = match env.get_string(&split_uri_jstr) {
        Ok(s) => s.into(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to get split URI: {}", e));
            return 0;
        }
    };
    let split_id: String = match env.get_string(&split_id_jstr) {
        Ok(s) => s.into(),
        Err(e) => {
            to_java_exception(&mut env, &anyhow::anyhow!("Failed to get split ID: {}", e));
            return 0;
        }
    };

    let result = with_object(searcher_ptr as u64, |searcher: &StandaloneSearcher| {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to create async runtime: {}", e))?;

        runtime.block_on(async {
            // Create split metadata
            let metadata = SplitSearchMetadata {
                split_id,
                split_footer_start: footer_start as u64,
                split_footer_end: footer_end as u64,
                file_size: 0, // TODO: Need to get actual file size from storage.file_num_bytes()
                num_docs: num_docs as u64,
                time_range: None, // Not provided in this basic interface
                delete_opstamp: delete_opstamp as u64,
            };

            // Get search request and doc mapper from pointers
            let search_request = with_object(search_request_ptr as u64, |req: &SearchRequest| {
                req.clone()
            }).ok_or_else(|| anyhow::anyhow!("Invalid search request pointer"))?;

            let doc_mapper = with_object(doc_mapper_ptr as u64, |mapper: &Arc<DocMapper>| {
                mapper.clone()
            }).ok_or_else(|| anyhow::anyhow!("Invalid doc mapper pointer"))?;

            // Perform the search
            let response = searcher.search_split(&split_uri, metadata, search_request, doc_mapper).await?;
            
            Ok::<LeafSearchResponse, anyhow::Error>(response)
        })
    });

    match result {
        Some(Ok(response)) => register_object(response) as jlong,
        Some(Err(error)) => {
            to_java_exception(&mut env, &error);
            0
        },
        None => {
            to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
            0
        }
    }
}

/// Get cache statistics
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_StandaloneSearcher_getCacheStatsNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) -> jstring {
    let result = with_object(searcher_ptr as u64, |searcher: &StandaloneSearcher| {
        let stats = searcher.cache_stats();
        let stats_json = serde_json::json!({
            "fast_field_bytes": stats.fast_field_bytes,
            "split_footer_bytes": stats.split_footer_bytes,
            "partial_request_count": stats.partial_request_count
        });
        stats_json.to_string()
    });

    match result {
        Some(json_str) => {
            match env.new_string(json_str) {
                Ok(jstr) => jstr.into_raw(),
                Err(error) => {
                    to_java_exception(&mut env, &anyhow::anyhow!("Failed to create Java string: {}", error));
                    std::ptr::null_mut()
                }
            }
        },
        None => {
            to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
            std::ptr::null_mut()
        }
    }
}

/// Clear all caches
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_StandaloneSearcher_clearCachesNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) {
    let result = with_object(searcher_ptr as u64, |searcher: &StandaloneSearcher| {
        searcher.clear_caches();
    });

    if result.is_none() {
        to_java_exception(&mut env, &anyhow::anyhow!("Invalid searcher pointer"));
    }
}

/// Close the searcher and free resources
#[no_mangle]
pub extern "system" fn Java_com_tantivy4java_StandaloneSearcher_closeNative(
    mut env: JNIEnv,
    _class: JClass,
    searcher_ptr: jlong,
) {
    if searcher_ptr == 0 {
        return;
    }

    if !remove_object(searcher_ptr as u64) {
        to_java_exception(&mut env, &anyhow::anyhow!("Failed to remove searcher object or invalid pointer"));
    }
}