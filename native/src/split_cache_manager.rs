use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use crate::debug_println;
use crate::simple_batch_optimization::{PrefetchStats, BatchOptimizationMetrics};

// Debug logging macro - controlled by TANTIVY4JAVA_DEBUG environment variable
macro_rules! debug_log {
    ($($arg:tt)*) => {
        debug_println!("DEBUG: {}", format!($($arg)*));
    };
}

use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString};
use jni::sys::{jlong, jint, jobject, jdouble};

use tokio::runtime::Runtime;
use crate::global_cache::{get_configured_storage_resolver, get_global_searcher_context};
use quickwit_config::{S3StorageConfig, AzureStorageConfig};
// Note: Using global caches following Quickwit's pattern

/// Global cache manager that follows Quickwit's multi-level caching architecture
/// This now uses the global caches from GLOBAL_SEARCHER_COMPONENTS
pub struct GlobalSplitCacheManager {
    cache_name: String,
    max_cache_size: u64,
    
    // REMOVED: runtime field - using shared global runtime to prevent deadlocks
    
    // Storage configurations for resolver
    s3_config: Option<S3StorageConfig>,
    azure_config: Option<AzureStorageConfig>,
    
    // Statistics
    total_hits: AtomicU64,
    total_misses: AtomicU64,
    total_evictions: AtomicU64,
    current_size: AtomicU64,
    
    // Managed splits
    managed_splits: Mutex<HashMap<String, u64>>, // split_path -> last_access_time
}

impl GlobalSplitCacheManager {
    pub fn new(cache_name: String, max_cache_size: u64) -> Self {
        debug_println!("RUST DEBUG: Creating GlobalSplitCacheManager '{}' using global caches", cache_name);
        
        // CRITICAL FIX: DO NOT create separate Tokio runtime - causes multiple runtime deadlocks
        // All async operations should use the shared global runtime via QuickwitRuntimeManager
        debug_println!("🔧 RUNTIME_FIX: Eliminating separate Tokio runtime to prevent deadlocks");
        
        // Note: We're using the global caches from GLOBAL_SEARCHER_COMPONENTS
        // This ensures all split cache managers share the same underlying caches
        // following Quickwit's architecture pattern
        
        Self {
            cache_name,
            max_cache_size,
            // REMOVED: runtime field to prevent multiple runtime conflicts
            s3_config: None,
            azure_config: None,
            total_hits: AtomicU64::new(0),
            total_misses: AtomicU64::new(0),
            total_evictions: AtomicU64::new(0),
            current_size: AtomicU64::new(0),
            managed_splits: Mutex::new(HashMap::new()),
        }
    }
    
    pub fn add_split(&self, split_path: String) {
        let mut splits = self.managed_splits.lock().unwrap();
        splits.insert(split_path, std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs());
    }
    
    pub fn remove_split(&self, split_path: &str) {
        let mut splits = self.managed_splits.lock().unwrap();
        splits.remove(split_path);
    }
    
    pub fn get_managed_split_count(&self) -> usize {
        self.managed_splits.lock().unwrap().len()
    }
    
    pub fn get_cache_stats(&self) -> GlobalCacheStats {
        // 🚀 OPTIMIZATION: Access real Quickwit cache metrics instead of basic counters
        // This provides comprehensive per-cache-type metrics with ByteRangeCache-specific tracking
        
        // Access Quickwit's comprehensive storage metrics
        let storage_metrics = &quickwit_storage::STORAGE_METRICS;
        
        // 🎯 ByteRangeCache-specific metrics (shortlived_cache is used by ByteRangeCache)
        let byte_range_hits = storage_metrics.shortlived_cache.hits_num_items.get();
        let byte_range_misses = storage_metrics.shortlived_cache.misses_num_items.get(); 
        let byte_range_evictions = storage_metrics.shortlived_cache.evict_num_items.get();
        let byte_range_bytes = storage_metrics.shortlived_cache.in_cache_num_bytes.get() as u64;
        
        // 🎯 Split Footer Cache metrics (MemorySizedCache)
        let footer_hits = storage_metrics.split_footer_cache.hits_num_items.get();
        let footer_misses = storage_metrics.split_footer_cache.misses_num_items.get();
        let footer_evictions = storage_metrics.split_footer_cache.evict_num_items.get();
        let footer_bytes = storage_metrics.split_footer_cache.in_cache_num_bytes.get() as u64;
        
        // 🎯 Fast Field Cache metrics (component-level caching)
        let fastfield_hits = storage_metrics.fast_field_cache.hits_num_items.get();
        let fastfield_misses = storage_metrics.fast_field_cache.misses_num_items.get();
        let fastfield_evictions = storage_metrics.fast_field_cache.evict_num_items.get();
        let fastfield_bytes = storage_metrics.fast_field_cache.in_cache_num_bytes.get() as u64;
        
        // 🎯 Searcher Split Cache metrics (SplitCache tracking)
        let split_hits = storage_metrics.searcher_split_cache.hits_num_items.get();
        let split_misses = storage_metrics.searcher_split_cache.misses_num_items.get();
        let split_evictions = storage_metrics.searcher_split_cache.evict_num_items.get();
        let split_bytes = storage_metrics.searcher_split_cache.in_cache_num_bytes.get() as u64;
        
        // Aggregate comprehensive metrics across all cache types
        let total_hits = byte_range_hits + footer_hits + fastfield_hits + split_hits;
        let total_misses = byte_range_misses + footer_misses + fastfield_misses + split_misses;
        let total_evictions = byte_range_evictions + footer_evictions + fastfield_evictions + split_evictions;
        let current_size = byte_range_bytes + footer_bytes + fastfield_bytes + split_bytes;
        
        debug_log!("📊 Comprehensive Cache Metrics:");
        debug_log!("  📦 ByteRangeCache: {} hits, {} misses, {} evictions, {} bytes",
                  byte_range_hits, byte_range_misses, byte_range_evictions, byte_range_bytes);
        debug_log!("  📄 FooterCache: {} hits, {} misses, {} evictions, {} bytes",
                  footer_hits, footer_misses, footer_evictions, footer_bytes);
        debug_log!("  ⚡ FastFieldCache: {} hits, {} misses, {} evictions, {} bytes",
                  fastfield_hits, fastfield_misses, fastfield_evictions, fastfield_bytes);
        debug_log!("  🔍 SplitCache: {} hits, {} misses, {} evictions, {} bytes",
                  split_hits, split_misses, split_evictions, split_bytes);
        debug_log!("  🏆 Total: {} hits, {} misses, {} evictions, {} bytes ({}% hit rate)",
                  total_hits, total_misses, total_evictions, current_size,
                  if total_hits + total_misses > 0 { (total_hits * 100) / (total_hits + total_misses) } else { 0 });
        
        GlobalCacheStats {
            total_hits: total_hits as u64,
            total_misses: total_misses as u64,
            total_evictions: total_evictions as u64,
            current_size,
            max_size: self.max_cache_size,
            active_splits: self.get_managed_split_count() as u64,
        }
    }
    
    // Set AWS configuration for storage resolver
    pub fn set_aws_config(&mut self, s3_config: S3StorageConfig) {
        debug_println!("RUST DEBUG: Setting AWS config for cache '{}'", self.cache_name);
        self.s3_config = Some(s3_config);
    }

    // Set Azure configuration for storage resolver
    pub fn set_azure_config(&mut self, azure_config: AzureStorageConfig) {
        debug_println!("RUST DEBUG: Setting Azure config for cache '{}'", self.cache_name);
        debug_println!("   📋 Account: {:?}", azure_config.account_name);
        self.azure_config = Some(azure_config);
    }

    // Get configured storage resolver using the AWS or Azure config if set
    pub fn get_storage_resolver(&self) -> quickwit_storage::StorageResolver {
        get_configured_storage_resolver(self.s3_config.clone(), self.azure_config.clone())
    }
    
    // Get the global searcher context with all shared caches
    pub fn get_searcher_context(&self) -> Arc<quickwit_search::SearcherContext> {
        get_global_searcher_context()
    }
    
    pub fn force_eviction(&self, _target_size_bytes: u64) {
        // Simulate eviction by incrementing counter
        self.total_evictions.fetch_add(1, Ordering::Relaxed);
        // In a real implementation, this would evict cache entries
    }
}

#[derive(Debug)]
pub struct GlobalCacheStats {
    pub total_hits: u64,
    pub total_misses: u64,
    pub total_evictions: u64,
    pub current_size: u64,
    pub max_size: u64,
    pub active_splits: u64,
}

// Global registry for cache managers
lazy_static::lazy_static! {
    pub static ref CACHE_MANAGERS: Mutex<HashMap<String, Arc<GlobalSplitCacheManager>>> =
        Mutex::new(HashMap::new());

    // Global registry for batch optimization metrics (one metrics instance per cache manager)
    pub static ref BATCH_METRICS: Mutex<HashMap<String, Arc<BatchOptimizationMetrics>>> =
        Mutex::new(HashMap::new());

    // Global batch optimization metrics (aggregated across all cache managers)
    pub static ref GLOBAL_BATCH_METRICS: Arc<BatchOptimizationMetrics> =
        Arc::new(BatchOptimizationMetrics::new());
}

/// Helper function to record batch optimization metrics
pub fn record_batch_metrics(
    cache_name: Option<&str>,
    doc_count: usize,
    stats: &PrefetchStats,
    segments: usize,
    bytes_wasted: u64,
) {
    // Always record to global metrics
    GLOBAL_BATCH_METRICS.record_batch_operation(doc_count, stats, segments, bytes_wasted);

    // Also record to cache-specific metrics if cache_name is provided
    if let Some(name) = cache_name {
        if let Ok(batch_metrics) = BATCH_METRICS.lock() {
            if let Some(metrics) = batch_metrics.get(name) {
                metrics.record_batch_operation(doc_count, stats, segments, bytes_wasted);
            }
        }
    }
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_createNativeCacheManager(
    mut env: JNIEnv,
    _class: JClass,
    config: JObject,
) -> jlong {
    // Extract cache name from config
    let cache_name = match env.call_method(&config, "getCacheName", "()Ljava/lang/String;", &[]) {
        Ok(result) => {
            let name_obj = result.l().unwrap();
            match env.get_string(&JString::from(name_obj)) {
                Ok(name) => name.to_string_lossy().to_string(),
                Err(_) => "default".to_string(),
            }
        }
        _ => "default".to_string(),
    };
    
    // Extract max cache size
    let max_cache_size = match env.call_method(&config, "getMaxCacheSize", "()J", &[]) {
        Ok(result) => result.j().unwrap() as u64,
        _ => 200_000_000, // 200MB default
    };
    
    let mut manager = GlobalSplitCacheManager::new(cache_name.clone(), max_cache_size);
    
    // Extract AWS configuration from the Java CacheConfig
    if let Ok(aws_config_map) = env.call_method(&config, "getAwsConfig", "()Ljava/util/Map;", &[]) {
        let aws_map_obj = aws_config_map.l().unwrap();
        
        // Extract AWS configuration values
        let mut access_key = None;
        let mut secret_key = None;
        let mut session_token = None;
        let mut region = None;
        let mut endpoint = None;
        let mut path_style_access = false;
        
        // Helper function to extract string value from HashMap
        let extract_string_value = |env: &mut JNIEnv, map_obj: &JObject, key: &str| -> Option<String> {
            let key_jstring = env.new_string(key).ok()?;
            let value = env.call_method(map_obj, "get", "(Ljava/lang/Object;)Ljava/lang/Object;", &[(&key_jstring).into()]).ok()?.l().ok()?;
            if value.is_null() {
                return None;
            }
            let value_jstring = JString::from(value);
            let value_string = env.get_string(&value_jstring).ok()?;
            Some(value_string.to_string_lossy().to_string())
        };
        
        access_key = extract_string_value(&mut env, &aws_map_obj, "access_key");
        secret_key = extract_string_value(&mut env, &aws_map_obj, "secret_key");
        session_token = extract_string_value(&mut env, &aws_map_obj, "session_token");
        region = extract_string_value(&mut env, &aws_map_obj, "region");
        endpoint = extract_string_value(&mut env, &aws_map_obj, "endpoint");
        
        // Extract path_style_access boolean value
        if let Some(path_style_str) = extract_string_value(&mut env, &aws_map_obj, "path_style_access") {
            path_style_access = path_style_str == "true";
        }
        
        // Create S3StorageConfig if we have access key and secret key
        if let (Some(access_key), Some(secret_key)) = (access_key, secret_key) {
            debug_println!("RUST DEBUG: Configuring S3 storage with access_key, region: {:?}, endpoint: {:?}, path_style_access: {}", 
                         region, endpoint, path_style_access);
            
            let s3_config = S3StorageConfig {
                access_key_id: Some(access_key),
                secret_access_key: Some(secret_key),
                session_token,
                region: Some(region.unwrap_or_else(|| "us-east-1".to_string())),
                endpoint,
                force_path_style_access: path_style_access,
                ..Default::default()
            };
            
            // Set the S3 config on the manager
            manager.set_aws_config(s3_config);
        }
    }
    
    let manager_arc = Arc::new(manager);
    
    // Store in global registry
    {
        let mut managers = CACHE_MANAGERS.lock().unwrap();
        managers.insert(cache_name.clone(), manager_arc.clone());
    }

    // Create and store batch optimization metrics for this cache manager
    {
        let metrics = Arc::new(crate::simple_batch_optimization::BatchOptimizationMetrics::new());
        let mut batch_metrics = BATCH_METRICS.lock().unwrap();
        batch_metrics.insert(cache_name.clone(), metrics);
        debug_println!("RUST DEBUG: Created batch optimization metrics for cache '{}'", cache_name);
    }

    // Use Arc registry for safe memory management
    let cache_name_arc = Arc::new(cache_name);
    crate::utils::arc_to_jlong(cache_name_arc)
}

#[no_mangle]
pub extern "system" fn Java_io_indextables_tantivy4java_split_SplitCacheManager_closeNativeCacheManager(
    _env: JNIEnv,
    _class: JClass,
    ptr: jlong,
) {
    if ptr != 0 {
        // Safely find and remove from global registry instead of dangerous Arc::from_raw()
        let mut managers = CACHE_MANAGERS.lock().unwrap();
        
        // Use Arc registry to find cache name, then remove from managers
        if let Some(cache_name_arc) = crate::utils::jlong_to_arc::<String>(ptr) {
            managers.remove(&*cache_name_arc);
            // Release the Arc from registry
            crate::utils::release_arc(ptr);
        }
    }
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
    
    // Create 2D array: [cache_type][metrics] where metrics = [hits, misses, evictions, sizeBytes]
    let per_cache_metrics = vec![
        // ByteRangeCache metrics (shortlived_cache)
        vec![
            storage_metrics.shortlived_cache.hits_num_items.get() as jlong,
            storage_metrics.shortlived_cache.misses_num_items.get() as jlong,
            storage_metrics.shortlived_cache.evict_num_items.get() as jlong,
            storage_metrics.shortlived_cache.in_cache_num_bytes.get() as jlong,
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
            
            debug_log!("📊 Comprehensive cache metrics returned: {} cache types with detailed stats",
                      per_cache_metrics.len());
            
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
        let _ = env.throw_new("java/lang/RuntimeException", "Invalid SplitCacheManager pointer");
        return std::ptr::null_mut();
    }
    
    // Get cache name FIRST to avoid double-locking
    let cache_name = match crate::utils::jlong_to_arc::<String>(ptr) {
        Some(name_arc) => (*name_arc).clone(),
        None => {
            let _ = env.throw_new("java/lang/RuntimeException", "SplitCacheManager not found in registry");
            return std::ptr::null_mut();
        }
    };
    
    // THEN access the cache managers registry
    let managers = CACHE_MANAGERS.lock().unwrap();
    let manager = match managers.get(&cache_name) {
        Some(manager_arc) => manager_arc,
        None => {
            let _ = env.throw_new("java/lang/RuntimeException", "SplitCacheManager not found in registry");
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
        let _ = env.throw_new("java/lang/RuntimeException", "Invalid SplitCacheManager pointer");
        return std::ptr::null_mut();
    }
    
    // Get cache name FIRST to avoid double-locking
    let cache_name = match crate::utils::jlong_to_arc::<String>(ptr) {
        Some(name_arc) => (*name_arc).clone(),
        None => {
            let _ = env.throw_new("java/lang/RuntimeException", "SplitCacheManager not found in registry");
            return std::ptr::null_mut();
        }
    };
    
    // THEN access the cache managers registry
    let managers = CACHE_MANAGERS.lock().unwrap();
    let manager = match managers.get(&cache_name) {
        Some(manager_arc) => manager_arc,
        None => {
            let _ = env.throw_new("java/lang/RuntimeException", "SplitCacheManager not found in registry");
            return std::ptr::null_mut();
        }
    };
    
    // Multi-split search is not yet implemented
    let error_msg = "Multi-split search across specified splits is not yet implemented. Use individual SplitSearcher instances for searching specific splits.";
    let _ = env.throw_new("java/lang/UnsupportedOperationException", error_msg);
    std::ptr::null_mut()
}

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

use crate::split_searcher_replacement::{SEARCHER_CACHE_HITS, SEARCHER_CACHE_MISSES, SEARCHER_CACHE_EVICTIONS};

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