// jni_lifecycle.rs - JNI functions for cache manager lifecycle (create/close)
// Extracted from split_cache_manager.rs during refactoring

use std::path::PathBuf;
use std::sync::Arc;

use jni::objects::{JClass, JObject, JString};
use jni::sys::jlong;
use jni::JNIEnv;
use quickwit_config::S3StorageConfig;

use crate::debug_println;
use crate::disk_cache::{CompressionAlgorithm, DiskCacheConfig};

use super::manager::{GlobalSplitCacheManager, BATCH_METRICS, CACHE_MANAGERS};

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
        let extract_string_value =
            |env: &mut JNIEnv, map_obj: &JObject, key: &str| -> Option<String> {
                let key_jstring = env.new_string(key).ok()?;
                let value = env
                    .call_method(
                        map_obj,
                        "get",
                        "(Ljava/lang/Object;)Ljava/lang/Object;",
                        &[(&key_jstring).into()],
                    )
                    .ok()?
                    .l()
                    .ok()?;
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
            debug_println!(
                "RUST DEBUG: Configuring S3 storage with access_key, region: {:?}, endpoint: {:?}, path_style_access: {}",
                region,
                endpoint,
                path_style_access
            );

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
    if let Ok(tiered_config_result) = env.call_method(
        &config,
        "getTieredCacheConfig",
        "()Lio/indextables/tantivy4java/split/SplitCacheManager$TieredCacheConfig;",
        &[],
    ) {
        let tiered_config_obj = tiered_config_result.l().unwrap();
        debug_println!(
            "RUST DEBUG: TieredCacheConfig result obtained, is_null={}",
            tiered_config_obj.is_null()
        );

        if !tiered_config_obj.is_null() {
            debug_println!("RUST DEBUG: Found non-null TieredCacheConfig");
            debug_println!("RUST DEBUG: Found TieredCacheConfig in Java CacheConfig");

            // Extract disk cache path
            debug_println!("RUST DEBUG: Extracting disk cache path");
            let disk_path = match env.call_method(
                &tiered_config_obj,
                "getDiskCachePath",
                "()Ljava/lang/String;",
                &[],
            ) {
                Ok(result) => {
                    let path_obj = result.l().unwrap();
                    debug_println!(
                        "RUST DEBUG: getDiskCachePath result is_null={}",
                        path_obj.is_null()
                    );
                    if !path_obj.is_null() {
                        match env.get_string(&JString::from(path_obj)) {
                            Ok(path) => {
                                let path_str = path.to_string_lossy().to_string();
                                debug_println!("RUST DEBUG: Got disk cache path: {}", path_str);
                                Some(path_str)
                            }
                            Err(e) => {
                                debug_println!("RUST DEBUG: Failed to get path string: {:?}", e);
                                None
                            }
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
            let max_disk_size =
                match env.call_method(&tiered_config_obj, "getMaxDiskSizeBytes", "()J", &[]) {
                    Ok(result) => result.j().unwrap() as u64,
                    _ => 0, // 0 = auto-detect
                };

            // Extract compression ordinal (0=None, 1=LZ4, 2=ZSTD)
            let compression_ordinal =
                match env.call_method(&tiered_config_obj, "getCompressionOrdinal", "()I", &[]) {
                    Ok(result) => result.i().unwrap(),
                    _ => 1, // Default to LZ4
                };

            // Extract min compress size
            let min_compress_size =
                match env.call_method(&tiered_config_obj, "getMinCompressSizeBytes", "()I", &[]) {
                    Ok(result) => result.i().unwrap() as usize,
                    _ => 4096, // 4KB default
                };

            // Extract manifest sync interval
            let manifest_sync_interval = match env.call_method(
                &tiered_config_obj,
                "getManifestSyncIntervalSecs",
                "()I",
                &[],
            ) {
                Ok(result) => result.i().unwrap() as u64,
                _ => 30, // 30 seconds default
            };

            // Extract disableL1Cache flag for debugging
            let disable_l1_cache =
                match env.call_method(&tiered_config_obj, "isDisableL1Cache", "()Z", &[]) {
                    Ok(result) => result.z().unwrap_or(false),
                    _ => false,
                };

            // Set the global flag for L1 cache disabling
            crate::global_cache::set_disable_l1_cache(disable_l1_cache);

            // Create disk cache config if we have a path
            debug_println!("RUST DEBUG: disk_path is_some={}", disk_path.is_some());
            if let Some(path) = disk_path {
                debug_println!("RUST DEBUG: Creating L2DiskCache config at '{}'", path);
                debug_println!(
                    "RUST DEBUG: Configuring L2DiskCache at '{}', max_size={}, compression={}, min_compress={}",
                    path,
                    max_disk_size,
                    compression_ordinal,
                    min_compress_size
                );

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
        let metrics = Arc::new(crate::batch_retrieval::simple::BatchOptimizationMetrics::new());
        let mut batch_metrics = BATCH_METRICS.lock().unwrap();
        batch_metrics.insert(cache_name.clone(), metrics);
        debug_println!(
            "RUST DEBUG: Created batch optimization metrics for cache '{}'",
            cache_name
        );
    }

    // Increment active manager count for test isolation tracking
    super::manager::increment_manager_count();

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
            // Check if this manager has a disk cache and flush before cleanup
            if let Some(manager) = managers.get(&*cache_name_arc) {
                if let Some(disk_cache) = &manager.disk_cache {
                    // 🔧 CRITICAL FIX: Flush all pending writes BEFORE clearing references
                    // This ensures the background writer can still access the cache to write data
                    debug_println!(
                        "🔵 FLUSH: Flushing disk cache for '{}' before cleanup",
                        *cache_name_arc
                    );
                    disk_cache.flush_blocking();
                    debug_println!(
                        "🔵 FLUSH: Disk cache flush complete for '{}'",
                        *cache_name_arc
                    );
                }
            }

            // Remove batch metrics for this cache
            {
                let mut batch_metrics = BATCH_METRICS.lock().unwrap();
                batch_metrics.remove(&*cache_name_arc);
                debug_println!(
                    "RUST DEBUG: Removed batch optimization metrics for cache '{}'",
                    *cache_name_arc
                );
            }

            managers.remove(&*cache_name_arc);
            // Release the Arc from registry
            crate::utils::release_arc(ptr);

            // Decrement manager count and clear all global caches if this was the last manager
            // This is critical for test isolation - prevents data leaking between tests
            let was_last = super::manager::decrement_manager_count_and_maybe_cleanup();
            if was_last {
                debug_println!(
                    "🧹 CLOSE: Last cache manager '{}' closed - all global caches cleared",
                    *cache_name_arc
                );
            }
        }
    }
}
