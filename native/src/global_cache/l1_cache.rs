// l1_cache.rs - Global L1 ByteRangeCache management
// Extracted from global_cache.rs during refactoring

use std::sync::OnceLock;

use quickwit_storage::ByteRangeCache;

use crate::debug_println;

/// Global L1 ByteRangeCache shared across all SplitSearcher instances
/// This provides memory-efficient caching - one bounded cache instead of per-split caches
static GLOBAL_L1_CACHE: OnceLock<std::sync::RwLock<Option<ByteRangeCache>>> = OnceLock::new();

/// Configured L1 cache capacity from Java CacheConfig.withMaxCacheSize()
/// This is set when SplitCacheManager is created and used instead of default/env values
static CONFIGURED_L1_CACHE_CAPACITY: OnceLock<std::sync::RwLock<Option<u64>>> = OnceLock::new();

/// Global flag to disable L1 ByteRangeCache for debugging
/// When true, all storage requests bypass L1 memory cache and go to L2 disk cache / L3 storage
static DISABLE_L1_CACHE: OnceLock<std::sync::RwLock<bool>> = OnceLock::new();

fn get_l1_cache_holder() -> &'static std::sync::RwLock<Option<ByteRangeCache>> {
    GLOBAL_L1_CACHE.get_or_init(|| std::sync::RwLock::new(None))
}

fn get_configured_capacity_holder() -> &'static std::sync::RwLock<Option<u64>> {
    CONFIGURED_L1_CACHE_CAPACITY.get_or_init(|| std::sync::RwLock::new(None))
}

fn get_disable_l1_cache_holder() -> &'static std::sync::RwLock<bool> {
    DISABLE_L1_CACHE.get_or_init(|| std::sync::RwLock::new(false))
}

/// Set the L1 cache capacity from Java CacheConfig.withMaxCacheSize()
/// This should be called when SplitCacheManager is created
pub fn set_l1_cache_capacity(capacity_bytes: u64) {
    let holder = get_configured_capacity_holder();
    let mut guard = holder.write().unwrap();
    debug_println!(
        "⚙️ L1_CACHE_CONFIG: Setting L1 cache capacity from Java: {} MB",
        capacity_bytes / 1024 / 1024
    );
    *guard = Some(capacity_bytes);
}

/// Reset the L1 cache to allow reinitialization with new capacity
/// This is useful for testing scenarios where you need to change the L1 capacity
pub fn reset_global_l1_cache() {
    // First clear the configured capacity
    {
        let holder = get_configured_capacity_holder();
        let mut guard = holder.write().unwrap();
        *guard = None;
    }

    // Then clear the cache itself (will be recreated on next access)
    {
        let holder = get_l1_cache_holder();
        let mut guard = holder.write().unwrap();
        if guard.is_some() {
            debug_println!(
                "🔄 L1_CACHE_RESET: Resetting global L1 cache (will be recreated on next access)"
            );
        }
        *guard = None;
    }
}

/// Get or create the global shared L1 ByteRangeCache
/// Creates the cache lazily on first access with configurable capacity
pub fn get_or_create_global_l1_cache() -> Option<ByteRangeCache> {
    // Return None if L1 is disabled
    if is_l1_cache_disabled() {
        return None;
    }

    let holder = get_l1_cache_holder();

    // Fast path: check if already initialized
    {
        let guard = holder.read().unwrap();
        if let Some(cache) = guard.as_ref() {
            return Some(cache.clone());
        }
    }

    // Slow path: initialize the cache
    let mut guard = holder.write().unwrap();
    // Double-check after acquiring write lock
    if let Some(cache) = guard.as_ref() {
        return Some(cache.clone());
    }

    // Create bounded L1 cache with configurable capacity
    let capacity = get_l1_cache_capacity_bytes();
    let cache = ByteRangeCache::with_capacity(
        capacity,
        &quickwit_storage::STORAGE_METRICS.shortlived_cache,
    );
    debug_println!(
        "🚀 GLOBAL_L1_CACHE: Created shared ByteRangeCache with {} MB capacity",
        capacity / 1024 / 1024
    );
    *guard = Some(cache.clone());
    Some(cache)
}

/// Get the configured L1 cache capacity in bytes
/// Priority: 1) Java CacheConfig.withMaxCacheSize(), 2) TANTIVY4JAVA_L1_CACHE_MB env var, 3) 256MB default
fn get_l1_cache_capacity_bytes() -> u64 {
    const DEFAULT_L1_CACHE_CAPACITY_MB: u64 = 256;

    // Priority 1: Check if Java configured a capacity via CacheConfig.withMaxCacheSize()
    let holder = get_configured_capacity_holder();
    if let Ok(guard) = holder.read() {
        if let Some(capacity) = *guard {
            debug_println!(
                "⚙️ L1_CACHE_CONFIG: Using Java-configured capacity: {} MB",
                capacity / 1024 / 1024
            );
            return capacity;
        }
    }

    // Priority 2: Check environment variable
    if let Ok(env_mb) = std::env::var("TANTIVY4JAVA_L1_CACHE_MB") {
        if let Ok(mb) = env_mb.parse::<u64>() {
            if mb > 0 && mb <= 8192 {
                // Reasonable bounds: 1MB to 8GB
                debug_println!("⚙️ L1_CACHE_CONFIG: Using env var capacity: {} MB", mb);
                return mb * 1024 * 1024;
            }
            debug_println!(
                "⚠️ L1_CACHE_CONFIG: Invalid TANTIVY4JAVA_L1_CACHE_MB value: {}, using default",
                env_mb
            );
        }
    }

    // Priority 3: Default
    debug_println!(
        "⚙️ L1_CACHE_CONFIG: Using default capacity: {} MB",
        DEFAULT_L1_CACHE_CAPACITY_MB
    );
    DEFAULT_L1_CACHE_CAPACITY_MB * 1024 * 1024
}

/// Clear the global L1 cache (called after prewarm to free memory)
pub fn clear_global_l1_cache() {
    let holder = get_l1_cache_holder();
    let guard = holder.read().unwrap();
    if let Some(cache) = guard.as_ref() {
        cache.clear();
        debug_println!("🧹 GLOBAL_L1_CACHE: Cleared L1 ByteRangeCache");
    }
}

/// Get the current size of the global L1 cache in bytes
pub fn get_global_l1_cache_size() -> u64 {
    let holder = get_l1_cache_holder();
    if let Ok(guard) = holder.read() {
        if let Some(cache) = guard.as_ref() {
            return cache.get_num_bytes();
        }
    }
    0
}

/// Get the configured capacity of the global L1 cache in bytes
pub fn get_global_l1_cache_capacity() -> u64 {
    let holder = get_l1_cache_holder();
    if let Ok(guard) = holder.read() {
        if let Some(cache) = guard.as_ref() {
            return cache.get_capacity().unwrap_or(0);
        }
    }
    0
}

/// Set whether to disable L1 ByteRangeCache (called by SplitCacheManager from TieredCacheConfig)
pub fn set_disable_l1_cache(disable: bool) {
    let holder = get_disable_l1_cache_holder();
    let mut guard = holder.write().unwrap();
    if disable {
        debug_println!(
            "⚠️ L1_CACHE_DISABLED: ByteRangeCache disabled for debugging via TieredCacheConfig"
        );
        debug_println!(
            "   All storage requests will bypass L1 memory cache and go to L2 disk cache / L3 storage"
        );
    } else {
        debug_println!("🟢 L1_CACHE_ENABLED: ByteRangeCache enabled (normal operation)");
    }
    *guard = disable;
}

/// Check if L1 ByteRangeCache is disabled
pub fn is_l1_cache_disabled() -> bool {
    let holder = get_disable_l1_cache_holder();
    let guard = holder.read().unwrap();
    *guard
}
