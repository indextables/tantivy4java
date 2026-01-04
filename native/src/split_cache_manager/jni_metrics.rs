// jni_metrics.rs - JNI functions for all metrics (batch, searcher, object storage, L1, L2, download)
// Extracted from split_cache_manager.rs during refactoring

use std::sync::atomic::Ordering;

use jni::objects::{JClass, JString};
use jni::sys::{jboolean, jdouble, jint, jlong};
use jni::JNIEnv;

use crate::global_cache::{get_storage_download_metrics, reset_storage_download_metrics};
use crate::split_searcher::{SEARCHER_CACHE_EVICTIONS, SEARCHER_CACHE_HITS, SEARCHER_CACHE_MISSES};

use super::manager::{CACHE_MANAGERS, GLOBAL_BATCH_METRICS};

// ============================================================================
// Batch Optimization Metrics JNI Methods
// ============================================================================

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetBatchMetricsTotalOperations(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    GLOBAL_BATCH_METRICS.get_total_batch_operations() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetBatchMetricsTotalDocuments(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    GLOBAL_BATCH_METRICS.get_total_documents_requested() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetBatchMetricsTotalRequests(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    GLOBAL_BATCH_METRICS.get_total_requests() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetBatchMetricsConsolidatedRequests(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    GLOBAL_BATCH_METRICS.get_consolidated_requests() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetBatchMetricsBytesTransferred(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    GLOBAL_BATCH_METRICS.get_bytes_transferred() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetBatchMetricsBytesWasted(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    GLOBAL_BATCH_METRICS.get_bytes_wasted() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetBatchMetricsTotalPrefetchDuration(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    GLOBAL_BATCH_METRICS.get_total_prefetch_duration_ms() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetBatchMetricsSegmentsProcessed(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    GLOBAL_BATCH_METRICS.get_segments_processed() as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetBatchMetricsConsolidationRatio(
    _env: JNIEnv,
    _class: JClass,
) -> jdouble {
    GLOBAL_BATCH_METRICS.get_consolidation_ratio()
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetBatchMetricsCostSavingsPercent(
    _env: JNIEnv,
    _class: JClass,
) -> jdouble {
    GLOBAL_BATCH_METRICS.get_cost_savings_percent()
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetBatchMetricsEfficiencyPercent(
    _env: JNIEnv,
    _class: JClass,
) -> jdouble {
    GLOBAL_BATCH_METRICS.get_efficiency_percent()
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeResetBatchMetrics(
    _env: JNIEnv,
    _class: JClass,
) {
    GLOBAL_BATCH_METRICS.reset();
}

// ===================================
// Searcher Cache Statistics JNI Methods
// ===================================

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetSearcherCacheHits(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    SEARCHER_CACHE_HITS.load(Ordering::Relaxed) as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetSearcherCacheMisses(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    SEARCHER_CACHE_MISSES.load(Ordering::Relaxed) as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetSearcherCacheEvictions(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    SEARCHER_CACHE_EVICTIONS.load(Ordering::Relaxed) as jlong
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeResetSearcherCacheStats(
    _env: JNIEnv,
    _class: JClass,
) {
    SEARCHER_CACHE_HITS.store(0, Ordering::Relaxed);
    SEARCHER_CACHE_MISSES.store(0, Ordering::Relaxed);
    SEARCHER_CACHE_EVICTIONS.store(0, Ordering::Relaxed);
}

// ===================================
// Object Storage Request Statistics JNI Methods
// ===================================

/// Get the total number of object storage get_slice requests made (accurate count from storage layer)
/// This includes both S3 and Azure requests
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetObjectStorageRequestCount(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    quickwit_storage::get_object_storage_request_count() as jlong
}

/// Get the total bytes fetched via object storage get_slice requests
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetObjectStorageBytesFetched(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    quickwit_storage::get_object_storage_bytes_fetched() as jlong
}

/// Reset object storage request statistics (useful for per-operation tracking)
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeResetObjectStorageRequestStats(
    _env: JNIEnv,
    _class: JClass,
) {
    quickwit_storage::reset_object_storage_request_stats();
}

// ===================================
// L2 Disk Cache Statistics JNI Methods
// ===================================

/// Get disk cache total bytes used
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetDiskCacheTotalBytes(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jlong {
    if ptr == 0 {
        return 0;
    }

    let cache_name = match crate::utils::jlong_to_arc::<String>(ptr) {
        Some(name_arc) => (*name_arc).clone(),
        None => return 0,
    };

    let managers = CACHE_MANAGERS.lock().unwrap();
    match managers.get(&cache_name) {
        Some(manager) => match manager.get_disk_cache() {
            Some(disk_cache) => disk_cache.stats().total_bytes as jlong,
            None => 0,
        },
        None => 0,
    }
}

/// Get disk cache max bytes limit
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetDiskCacheMaxBytes(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jlong {
    if ptr == 0 {
        return 0;
    }

    let cache_name = match crate::utils::jlong_to_arc::<String>(ptr) {
        Some(name_arc) => (*name_arc).clone(),
        None => return 0,
    };

    let managers = CACHE_MANAGERS.lock().unwrap();
    match managers.get(&cache_name) {
        Some(manager) => match manager.get_disk_cache() {
            Some(disk_cache) => disk_cache.stats().max_bytes as jlong,
            None => 0,
        },
        None => 0,
    }
}

/// Get number of splits cached on disk
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetDiskCacheSplitCount(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jint {
    if ptr == 0 {
        return 0;
    }

    let cache_name = match crate::utils::jlong_to_arc::<String>(ptr) {
        Some(name_arc) => (*name_arc).clone(),
        None => return 0,
    };

    let managers = CACHE_MANAGERS.lock().unwrap();
    match managers.get(&cache_name) {
        Some(manager) => match manager.get_disk_cache() {
            Some(disk_cache) => disk_cache.stats().split_count as jint,
            None => 0,
        },
        None => 0,
    }
}

/// Get number of components cached on disk
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetDiskCacheComponentCount(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jint {
    if ptr == 0 {
        return 0;
    }

    let cache_name = match crate::utils::jlong_to_arc::<String>(ptr) {
        Some(name_arc) => (*name_arc).clone(),
        None => return 0,
    };

    let managers = CACHE_MANAGERS.lock().unwrap();
    match managers.get(&cache_name) {
        Some(manager) => match manager.get_disk_cache() {
            Some(disk_cache) => disk_cache.stats().component_count as jint,
            None => 0,
        },
        None => 0,
    }
}

/// Check if disk cache is enabled
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeIsDiskCacheEnabled(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) -> jboolean {
    if ptr == 0 {
        return 0;
    }

    let cache_name = match crate::utils::jlong_to_arc::<String>(ptr) {
        Some(name_arc) => (*name_arc).clone(),
        None => return 0,
    };

    let managers = CACHE_MANAGERS.lock().unwrap();
    match managers.get(&cache_name) {
        Some(manager) => {
            if manager.get_disk_cache().is_some() {
                1
            } else {
                0
            }
        }
        None => 0,
    }
}

/// Evict a specific split from the disk cache
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeEvictSplitFromDiskCache(
    mut env: JNIEnv,
    _class: JClass,
    ptr: jlong,
    split_uri: JString,
) -> jboolean {
    if ptr == 0 {
        return 0;
    }

    let split_uri_str: String = match env.get_string(&split_uri) {
        Ok(s) => s.into(),
        Err(_) => return 0,
    };

    let cache_name = match crate::utils::jlong_to_arc::<String>(ptr) {
        Some(name_arc) => (*name_arc).clone(),
        None => return 0,
    };

    let managers = CACHE_MANAGERS.lock().unwrap();
    match managers.get(&cache_name) {
        Some(manager) => match manager.get_disk_cache() {
            Some(disk_cache) => {
                // Extract split_id from URI (e.g., "s3://bucket/path/split-id.split" -> "split-id")
                let split_id = split_uri_str
                    .rsplit('/')
                    .next()
                    .unwrap_or(&split_uri_str)
                    .trim_end_matches(".split");

                // The storage_loc is the URI without the split filename
                let storage_loc = split_uri_str
                    .rsplit_once('/')
                    .map(|(base, _)| base)
                    .unwrap_or(&split_uri_str);

                disk_cache.evict_split(storage_loc, split_id);
                1
            }
            None => 0,
        },
        None => 0,
    }
}

// =====================================================================
// L1 ByteRangeCache Statistics JNI Methods
// =====================================================================
// These functions expose the global shared L1 cache statistics to Java
// for monitoring and testing cache eviction behavior.
// =====================================================================

/// Get the current size of the global L1 cache in bytes
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetL1CacheSize(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    crate::global_cache::get_global_l1_cache_size() as jlong
}

/// Get the configured capacity of the global L1 cache in bytes
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetL1CacheCapacity(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    crate::global_cache::get_global_l1_cache_capacity() as jlong
}

/// Get the number of items evicted from the L1 cache (from Quickwit's shortlived_cache metrics)
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetL1CacheEvictions(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    quickwit_storage::STORAGE_METRICS
        .shortlived_cache
        .evict_num_items
        .get() as jlong
}

/// Get the total bytes evicted from the L1 cache
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetL1CacheEvictedBytes(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    quickwit_storage::STORAGE_METRICS
        .shortlived_cache
        .evict_num_bytes
        .get() as jlong
}

/// Get the number of cache hits in the L1 cache
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetL1CacheHits(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    quickwit_storage::STORAGE_METRICS
        .shortlived_cache
        .hits_num_items
        .get() as jlong
}

/// Get the number of cache misses in the L1 cache
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetL1CacheMisses(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    quickwit_storage::STORAGE_METRICS
        .shortlived_cache
        .misses_num_items
        .get() as jlong
}

/// Clear the global L1 cache
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeClearL1Cache(
    _env: JNIEnv,
    _class: JClass,
) {
    crate::global_cache::clear_global_l1_cache();
}

/// Reset the global L1 cache (for testing - allows reinitialization with new capacity)
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeResetL1Cache(
    _env: JNIEnv,
    _class: JClass,
) {
    crate::global_cache::reset_global_l1_cache();
}

// =====================================================================
// Global Storage Download Metrics
// =====================================================================
// These functions expose global storage download counters to Java for
// programmatic verification of caching behavior in tests.
// =====================================================================

/// Get total storage downloads (all sources)
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetStorageDownloadCount(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    get_storage_download_metrics().total_downloads as jlong
}

/// Get total bytes downloaded (all sources)
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetStorageDownloadBytes(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    get_storage_download_metrics().total_bytes as jlong
}

/// Get storage downloads during prewarm operations
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetStoragePrewarmDownloadCount(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    get_storage_download_metrics().prewarm_downloads as jlong
}

/// Get bytes downloaded during prewarm operations
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetStoragePrewarmDownloadBytes(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    get_storage_download_metrics().prewarm_bytes as jlong
}

/// Get storage downloads during query operations (L3 cache misses)
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetStorageQueryDownloadCount(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    get_storage_download_metrics().query_downloads as jlong
}

/// Get bytes downloaded during query operations
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeGetStorageQueryDownloadBytes(
    _env: JNIEnv,
    _class: JClass,
) -> jlong {
    get_storage_download_metrics().query_bytes as jlong
}

/// Reset all storage download metrics (useful between tests)
#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_nativeResetStorageDownloadMetrics(
    _env: JNIEnv,
    _class: JClass,
) {
    reset_storage_download_metrics();
}
