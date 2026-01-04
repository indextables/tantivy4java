// jni_cache_ops.rs - JNI functions for cache operations (stats, eviction, preload, search)
// Extracted from split_cache_manager.rs during refactoring

use std::sync::atomic::Ordering;
use std::sync::Arc;

use jni::objects::{JClass, JObject, JString};
use jni::sys::{jint, jlong, jobject};
use jni::JNIEnv;

use crate::debug_println;

use super::manager::CACHE_MANAGERS;

// Debug logging macro - controlled by TANTIVY4JAVA_DEBUG environment variable
macro_rules! debug_log {
    ($($arg:tt)*) => {
        debug_println!("DEBUG: {}", format!($($arg)*));
    };
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_getGlobalCacheStatsNative(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jobject {
    if ptr == 0 {
        return std::ptr::null_mut();
    }

    // Get cache name FIRST to avoid double-locking
    let cache_name = match crate::utils::jlong_to_arc::<String>(ptr) {
        Some(name_arc) => (*name_arc).clone(),
        None => return std::ptr::null_mut(),
    };

    // THEN access the cache managers registry
    let managers = CACHE_MANAGERS.lock().unwrap();
    let manager = match managers.get(&cache_name) {
        Some(manager_arc) => manager_arc,
        None => return std::ptr::null_mut(),
    };
    let stats = manager.get_cache_stats();

    // Create GlobalCacheStats Java object
    match env.find_class("io/indextables/tantivy4java/split/SplitCacheManager$GlobalCacheStats") {
        Ok(stats_class) => {
            match env.new_object(
                stats_class,
                "(JJJJJI)V",
                &[
                    (stats.total_hits as jlong).into(),
                    (stats.total_misses as jlong).into(),
                    (stats.total_evictions as jlong).into(),
                    (stats.current_size as jlong).into(),
                    (stats.max_size as jlong).into(),
                    (stats.active_splits as jint).into(),
                ],
            ) {
                Ok(obj) => obj.as_raw(),
                Err(_) => std::ptr::null_mut(),
            }
        }
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_forceEvictionNative(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    target_size_bytes: jlong,
) {
    if ptr == 0 {
        return;
    }

    // Get cache name FIRST to avoid double-locking
    let cache_name = match crate::utils::jlong_to_arc::<String>(ptr) {
        Some(name_arc) => (*name_arc).clone(),
        None => return,
    };

    // THEN access the cache managers registry
    let managers = CACHE_MANAGERS.lock().unwrap();
    let manager = match managers.get(&cache_name) {
        Some(manager_arc) => manager_arc,
        None => return,
    };
    manager.force_eviction(target_size_bytes as u64);
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_getComprehensiveCacheStatsNative(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jobject {
    if ptr == 0 {
        return std::ptr::null_mut();
    }

    // Safely access through global registry instead of unsafe pointer cast
    let managers = CACHE_MANAGERS.lock().unwrap();
    let _manager = match managers.values().find(|m| Arc::as_ptr(m) as jlong == ptr) {
        Some(manager_arc) => manager_arc,
        None => return std::ptr::null_mut(),
    };

    // 🚀 COMPREHENSIVE CACHE METRICS: Access real Quickwit storage metrics
    let storage_metrics = &quickwit_storage::STORAGE_METRICS;

    // Debug: Print the actual values being returned
    let byte_range_size = storage_metrics.shortlived_cache.in_cache_num_bytes.get();
    println!(
        "📊 NATIVE getComprehensiveCacheStatsNative: ByteRangeCache.in_cache_num_bytes = {}",
        byte_range_size
    );

    // Create 2D array: [cache_type][metrics] where metrics = [hits, misses, evictions, sizeBytes]
    let per_cache_metrics = vec![
        // ByteRangeCache metrics (shortlived_cache)
        vec![
            storage_metrics.shortlived_cache.hits_num_items.get() as jlong,
            storage_metrics.shortlived_cache.misses_num_items.get() as jlong,
            storage_metrics.shortlived_cache.evict_num_items.get() as jlong,
            byte_range_size as jlong,
        ],
        // FooterCache metrics (split_footer_cache)
        vec![
            storage_metrics.split_footer_cache.hits_num_items.get() as jlong,
            storage_metrics.split_footer_cache.misses_num_items.get() as jlong,
            storage_metrics.split_footer_cache.evict_num_items.get() as jlong,
            storage_metrics.split_footer_cache.in_cache_num_bytes.get() as jlong,
        ],
        // FastFieldCache metrics (fast_field_cache)
        vec![
            storage_metrics.fast_field_cache.hits_num_items.get() as jlong,
            storage_metrics.fast_field_cache.misses_num_items.get() as jlong,
            storage_metrics.fast_field_cache.evict_num_items.get() as jlong,
            storage_metrics.fast_field_cache.in_cache_num_bytes.get() as jlong,
        ],
        // SplitCache metrics (searcher_split_cache)
        vec![
            storage_metrics.searcher_split_cache.hits_num_items.get() as jlong,
            storage_metrics.searcher_split_cache.misses_num_items.get() as jlong,
            storage_metrics.searcher_split_cache.evict_num_items.get() as jlong,
            storage_metrics.searcher_split_cache.in_cache_num_bytes.get() as jlong,
        ],
    ];

    // Create Java 2D long array
    // First create the long array class to use as the component type
    let long_array_class = match env.find_class("[J") {
        Ok(class) => class,
        Err(_) => return std::ptr::null_mut(),
    };

    match env.new_object_array(4, &long_array_class, jni::objects::JObject::null()) {
        Ok(outer_array) => {
            for (i, cache_metrics) in per_cache_metrics.iter().enumerate() {
                match env.new_long_array(4) {
                    Ok(inner_array) => {
                        let _ = env.set_long_array_region(&inner_array, 0, cache_metrics);
                        let _ = env.set_object_array_element(&outer_array, i as i32, &inner_array);
                    }
                    Err(_) => return std::ptr::null_mut(),
                }
            }

            debug_log!(
                "📊 Comprehensive cache metrics returned: {} cache types with detailed stats",
                per_cache_metrics.len()
            );

            outer_array.as_raw()
        }
        Err(_) => std::ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_preloadComponentsNative(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    _split_path: JString,
    _components: JObject,
) {
    if ptr == 0 {
        return;
    }

    // Get cache name FIRST to avoid double-locking
    let cache_name = match crate::utils::jlong_to_arc::<String>(ptr) {
        Some(name_arc) => (*name_arc).clone(),
        None => return,
    };

    // THEN access the cache managers registry
    let managers = CACHE_MANAGERS.lock().unwrap();
    let manager = match managers.get(&cache_name) {
        Some(manager_arc) => manager_arc,
        None => return,
    };
    // Simulate preloading by updating cache stats
    manager.current_size.fetch_add(1024, Ordering::Relaxed);
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_evictComponentsNative(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    _split_path: JString,
    _components: JObject,
) {
    if ptr == 0 {
        return;
    }

    // Get cache name FIRST to avoid double-locking
    let cache_name = match crate::utils::jlong_to_arc::<String>(ptr) {
        Some(name_arc) => (*name_arc).clone(),
        None => return,
    };

    // THEN access the cache managers registry
    let managers = CACHE_MANAGERS.lock().unwrap();
    let manager = match managers.get(&cache_name) {
        Some(manager_arc) => manager_arc,
        None => return,
    };
    // Simulate eviction by incrementing counter
    manager.total_evictions.fetch_add(1, Ordering::Relaxed);
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_searchAcrossAllSplitsNative(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    _query_ptr: jlong,
    _total_limit: jint,
) -> jobject {
    if ptr == 0 {
        let _ = env.throw_new(
            "java/lang/RuntimeException",
            "Invalid SplitCacheManager pointer",
        );
        return std::ptr::null_mut();
    }

    // Get cache name FIRST to avoid double-locking
    let cache_name = match crate::utils::jlong_to_arc::<String>(ptr) {
        Some(name_arc) => (*name_arc).clone(),
        None => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                "SplitCacheManager not found in registry",
            );
            return std::ptr::null_mut();
        }
    };

    // THEN access the cache managers registry
    let managers = CACHE_MANAGERS.lock().unwrap();
    let _manager = match managers.get(&cache_name) {
        Some(manager_arc) => manager_arc,
        None => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                "SplitCacheManager not found in registry",
            );
            return std::ptr::null_mut();
        }
    };

    // Multi-split search is not yet implemented
    let error_msg = "Multi-split search across all splits is not yet implemented. Use individual SplitSearcher instances for searching specific splits.";
    let _ = env.throw_new("java/lang/UnsupportedOperationException", error_msg);
    std::ptr::null_mut()
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_searchAcrossSplitsNative(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    _split_paths: JObject,
    _query_ptr: jlong,
    _total_limit: jint,
) -> jobject {
    if ptr == 0 {
        let _ = env.throw_new(
            "java/lang/RuntimeException",
            "Invalid SplitCacheManager pointer",
        );
        return std::ptr::null_mut();
    }

    // Get cache name FIRST to avoid double-locking
    let cache_name = match crate::utils::jlong_to_arc::<String>(ptr) {
        Some(name_arc) => (*name_arc).clone(),
        None => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                "SplitCacheManager not found in registry",
            );
            return std::ptr::null_mut();
        }
    };

    // THEN access the cache managers registry
    let managers = CACHE_MANAGERS.lock().unwrap();
    let _manager = match managers.get(&cache_name) {
        Some(manager_arc) => manager_arc,
        None => {
            let _ = env.throw_new(
                "java/lang/RuntimeException",
                "SplitCacheManager not found in registry",
            );
            return std::ptr::null_mut();
        }
    };

    // Multi-split search is not yet implemented
    let error_msg = "Multi-split search across specified splits is not yet implemented. Use individual SplitSearcher instances for searching specific splits.";
    let _ = env.throw_new("java/lang/UnsupportedOperationException", error_msg);
    std::ptr::null_mut()
}
