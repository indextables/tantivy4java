use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::path::PathBuf;
use crate::debug_println;
use crate::simple_batch_optimization::{PrefetchStats, BatchOptimizationMetrics};
use crate::disk_cache::{L2DiskCache, DiskCacheConfig, CompressionAlgorithm};

// Debug logging macro - controlled by TANTIVY4JAVA_DEBUG environment variable
macro_rules! debug_log {
    ($($arg:tt)*) => {
        debug_println!("DEBUG: {}", format!($($arg)*));
    };
}

use jni::JNIEnv;
use jni::objects::{JClass, JObject, JString};
use jni::sys::{jlong, jint, jobject, jdouble, jboolean};

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

    // L2 Disk cache for persistent caching
    disk_cache: Option<Arc<L2DiskCache>>,

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
            disk_cache: None,
            total_hits: AtomicU64::new(0),
            total_misses: AtomicU64::new(0),
            total_evictions: AtomicU64::new(0),
            current_size: AtomicU64::new(0),
            managed_splits: Mutex::new(HashMap::new()),
        }
    }

    /// Initialize L2 disk cache with the given configuration
    pub fn set_disk_cache(&mut self, config: DiskCacheConfig) {
        debug_println!("RUST DEBUG: Initializing L2DiskCache for cache '{}' at {:?}",
                      self.cache_name, config.root_path);

        match L2DiskCache::new(config) {
            Ok(cache) => {
                let stats = cache.stats();
                debug_println!("RUST DEBUG: L2DiskCache created successfully. Max size: {} bytes, {} splits cached",
                             stats.max_bytes, stats.split_count);
                // Also set the global disk cache so StandaloneSearcher can access it
                crate::global_cache::set_global_disk_cache(cache.clone());
                self.disk_cache = Some(cache);
            }
            Err(e) => {
                debug_println!("RUST WARNING: Failed to create L2DiskCache: {}. Continuing without disk cache.", e);
            }
        }
    }

    /// Get reference to L2 disk cache if configured
    pub fn get_disk_cache(&self) -> Option<&Arc<L2DiskCache>> {
        self.disk_cache.as_ref()
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
    debug_println!("RUST DEBUG: createNativeCacheManager called");
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

        // Extract AWS configuration values
        let access_key = extract_string_value(&mut env, &aws_map_obj, "access_key");
        let secret_key = extract_string_value(&mut env, &aws_map_obj, "secret_key");
        let session_token = extract_string_value(&mut env, &aws_map_obj, "session_token");
        let region = extract_string_value(&mut env, &aws_map_obj, "region");
        let endpoint = extract_string_value(&mut env, &aws_map_obj, "endpoint");

        // Extract path_style_access boolean value
        let path_style_access = extract_string_value(&mut env, &aws_map_obj, "path_style_access")
            .map(|s| s == "true")
            .unwrap_or(false);
        
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

    // Extract TieredCacheConfig from the Java CacheConfig
    debug_println!("RUST DEBUG: Attempting to extract TieredCacheConfig");
    if let Ok(tiered_config_result) = env.call_method(&config, "getTieredCacheConfig",
        "()Lio/indextables/tantivy4java/split/SplitCacheManager$TieredCacheConfig;", &[]) {
        let tiered_config_obj = tiered_config_result.l().unwrap();
        debug_println!("RUST DEBUG: TieredCacheConfig result obtained, is_null={}", tiered_config_obj.is_null());

        if !tiered_config_obj.is_null() {
            debug_println!("RUST DEBUG: Found non-null TieredCacheConfig");
            debug_println!("RUST DEBUG: Found TieredCacheConfig in Java CacheConfig");

            // Extract disk cache path
            debug_println!("RUST DEBUG: Extracting disk cache path");
            let disk_path = match env.call_method(&tiered_config_obj, "getDiskCachePath", "()Ljava/lang/String;", &[]) {
                Ok(result) => {
                    let path_obj = result.l().unwrap();
                    debug_println!("RUST DEBUG: getDiskCachePath result is_null={}", path_obj.is_null());
                    if !path_obj.is_null() {
                        match env.get_string(&JString::from(path_obj)) {
                            Ok(path) => {
                                let path_str = path.to_string_lossy().to_string();
                                debug_println!("RUST DEBUG: Got disk cache path: {}", path_str);
                                Some(path_str)
                            },
                            Err(e) => {
                                debug_println!("RUST DEBUG: Failed to get path string: {:?}", e);
                                None
                            },
                        }
                    } else {
                        debug_println!("RUST DEBUG: path_obj is null");
                        None
                    }
                }
                Err(e) => {
                    debug_println!("RUST DEBUG: getDiskCachePath call failed: {:?}", e);
                    None
                }
            };

            // Extract max disk size
            let max_disk_size = match env.call_method(&tiered_config_obj, "getMaxDiskSizeBytes", "()J", &[]) {
                Ok(result) => result.j().unwrap() as u64,
                _ => 0, // 0 = auto-detect
            };

            // Extract compression ordinal (0=None, 1=LZ4, 2=ZSTD)
            let compression_ordinal = match env.call_method(&tiered_config_obj, "getCompressionOrdinal", "()I", &[]) {
                Ok(result) => result.i().unwrap(),
                _ => 1, // Default to LZ4
            };

            // Extract min compress size
            let min_compress_size = match env.call_method(&tiered_config_obj, "getMinCompressSizeBytes", "()I", &[]) {
                Ok(result) => result.i().unwrap() as usize,
                _ => 4096, // 4KB default
            };

            // Extract manifest sync interval
            let manifest_sync_interval = match env.call_method(&tiered_config_obj, "getManifestSyncIntervalSecs", "()I", &[]) {
                Ok(result) => result.i().unwrap() as u64,
                _ => 30, // 30 seconds default
            };

            // Extract disableL1Cache flag for debugging
            let disable_l1_cache = match env.call_method(&tiered_config_obj, "isDisableL1Cache", "()Z", &[]) {
                Ok(result) => result.z().unwrap_or(false),
                _ => false,
            };

            // Set the global flag for L1 cache disabling
            crate::global_cache::set_disable_l1_cache(disable_l1_cache);

            // Create disk cache config if we have a path
            debug_println!("RUST DEBUG: disk_path is_some={}", disk_path.is_some());
            if let Some(path) = disk_path {
                debug_println!("RUST DEBUG: Creating L2DiskCache config at '{}'", path);
                debug_println!("RUST DEBUG: Configuring L2DiskCache at '{}', max_size={}, compression={}, min_compress={}",
                             path, max_disk_size, compression_ordinal, min_compress_size);

                let disk_config = DiskCacheConfig {
                    root_path: PathBuf::from(path.clone()),
                    max_size_bytes: max_disk_size,
                    compression: CompressionAlgorithm::from_ordinal(compression_ordinal),
                    min_compress_size,
                    manifest_sync_interval_secs: manifest_sync_interval,
                    mmap_cache_size: 0, // Use default (1024)
                };

                debug_println!("RUST DEBUG: Calling set_disk_cache with path: {}", path);
                manager.set_disk_cache(disk_config);
                debug_println!("RUST DEBUG: set_disk_cache complete");
            } else {
                debug_println!("RUST DEBUG: disk_path is None, skipping disk cache setup");
            }
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
            // Check if this manager has a disk cache and clear caches
            if let Some(manager) = managers.get(&*cache_name_arc) {
                if let Some(disk_cache) = &manager.disk_cache {
                    // 🔧 CRITICAL FIX: Flush all pending writes BEFORE clearing references
                    // This ensures the background writer can still access the cache to write data
                    debug_println!("🔵 FLUSH: Flushing disk cache for '{}' before cleanup", *cache_name_arc);
                    disk_cache.flush_blocking();
                    debug_println!("🔵 FLUSH: Disk cache flush complete for '{}'", *cache_name_arc);

                    debug_println!("RUST DEBUG: Clearing caches for cache '{}' with disk cache", *cache_name_arc);
                    // Clear searcher cache - this releases Arc<Searcher> which holds
                    // references to StorageWithPersistentCache -> L2DiskCache
                    crate::split_searcher_replacement::clear_searcher_cache();
                    // Then clear the global disk cache reference
                    crate::global_cache::clear_global_disk_cache();
                }
            }
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
    let _manager = match managers.get(&cache_name) {
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
    let _manager = match managers.get(&cache_name) {
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
        Some(manager) => if manager.get_disk_cache().is_some() { 1 } else { 0 },
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
            },
            None => 0,
        },
        None => 0,
    }
}

// =====================================================================
// Global Storage Download Metrics
// =====================================================================
// These functions expose global storage download counters to Java for
// programmatic verification of caching behavior in tests.
// =====================================================================

use crate::global_cache::{get_storage_download_metrics, reset_storage_download_metrics};

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