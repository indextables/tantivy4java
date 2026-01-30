// global_cache/mod.rs - Global cache infrastructure following Quickwit's pattern
//
// This module provides global, shared caches and storage resolvers that are
// reused across all split searcher instances, following the same architecture
// as Quickwit to ensure efficient resource utilization.
//
// Extracted from global_cache.rs during refactoring for better maintainability.

mod cache_debug;  // Enhanced cache debugging and monitoring utilities
mod components;
mod config;
mod l1_cache;
mod metrics;
mod storage_resolver;

// Re-export from components
pub use components::GlobalSearcherComponents;

// Re-export from config
pub use config::GlobalCacheConfig;

// Re-export from l1_cache
pub use l1_cache::{
    clear_global_l1_cache, get_global_l1_cache_capacity, get_global_l1_cache_size,
    get_or_create_global_l1_cache, is_l1_cache_disabled, reset_global_l1_cache,
    set_disable_l1_cache, set_l1_cache_capacity,
};

// Re-export from metrics
pub use metrics::{
    get_storage_download_metrics, record_prewarm_download, record_query_download,
    reset_storage_download_metrics, StorageDownloadMetrics,
};

// Re-export from storage_resolver
pub use storage_resolver::{
    generate_storage_cache_key, get_configured_storage_resolver, get_configured_storage_resolver_async,
    tracked_storage_resolve, GLOBAL_STORAGE_RESOLVER,
};

// =====================================================================
// Global Instances - Searcher Components and Context
// =====================================================================

use std::sync::{Arc, OnceLock};

use quickwit_config::SearcherConfig;
use quickwit_search::SearcherContext;

use crate::debug_println;
use crate::disk_cache::L2DiskCache;

/// Global instance of searcher components - using OnceLock for better thread safety
/// This ensures only one instance is ever created across all Spark tasks/executors
static GLOBAL_SEARCHER_COMPONENTS: OnceLock<Arc<GlobalSearcherComponents>> = OnceLock::new();

/// Global cached SearcherContext - using RwLock<Option<...>> pattern to allow reset for test isolation
/// The OnceLock holds the RwLock, which holds an Option that can be cleared
static GLOBAL_SEARCHER_CONTEXT: OnceLock<std::sync::RwLock<Option<Arc<SearcherContext>>>> = OnceLock::new();

fn get_searcher_context_holder() -> &'static std::sync::RwLock<Option<Arc<SearcherContext>>> {
    GLOBAL_SEARCHER_CONTEXT.get_or_init(|| std::sync::RwLock::new(None))
}

/// Credential-specific SearcherContext cache
/// This ensures that different credentials get separate caches, preventing the security issue
/// where data loaded with valid credentials could be served to requests with invalid credentials.
/// Key: credential cache key (from generate_storage_cache_key), Value: SearcherContext
static CREDENTIAL_SEARCHER_CONTEXTS: OnceLock<std::sync::Mutex<std::collections::HashMap<String, Arc<SearcherContext>>>> = OnceLock::new();

fn get_credential_contexts_holder() -> &'static std::sync::Mutex<std::collections::HashMap<String, Arc<SearcherContext>>> {
    CREDENTIAL_SEARCHER_CONTEXTS.get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
}

/// Global L2 disk cache that can be set by SplitCacheManager
/// This is separate from GlobalSearcherComponents to allow dynamic configuration
static GLOBAL_DISK_CACHE: OnceLock<std::sync::RwLock<Option<Arc<L2DiskCache>>>> = OnceLock::new();

fn get_disk_cache_holder() -> &'static std::sync::RwLock<Option<Arc<L2DiskCache>>> {
    GLOBAL_DISK_CACHE.get_or_init(|| std::sync::RwLock::new(None))
}

/// Set the global L2 disk cache (called by SplitCacheManager when TieredCacheConfig is provided)
pub fn set_global_disk_cache(cache: Arc<L2DiskCache>) {
    let holder = get_disk_cache_holder();
    let mut guard = holder.write().unwrap();
    debug_println!("🟢 SET_GLOBAL_DISK_CACHE: Setting global L2 disk cache");
    *guard = Some(cache);
}

/// Clear the global L2 disk cache (called when SplitCacheManager is closed)
/// This removes the global Arc reference, allowing the disk cache to be dropped
pub fn clear_global_disk_cache() {
    let holder = get_disk_cache_holder();
    let mut guard = holder.write().unwrap();
    debug_println!("🔴 CLEAR_GLOBAL_DISK_CACHE: Clearing global L2 disk cache");
    *guard = None;
}

/// Initialize the global cache with custom configuration
/// This should be called once at startup from Java if custom configuration is needed
/// Returns true if initialization succeeded, false if already initialized
pub fn initialize_global_cache(config: GlobalCacheConfig) -> bool {
    debug_println!("RUST DEBUG: Attempting to initialize global cache with custom config");
    let components = Arc::new(GlobalSearcherComponents::new(config));
    let cache_ptr = Arc::as_ptr(&components) as usize;
    debug_println!(
        "RUST DEBUG: Created GlobalSearcherComponents at address: 0x{:x}",
        cache_ptr
    );
    GLOBAL_SEARCHER_COMPONENTS.set(components).is_ok()
}

/// Get the global searcher components, initializing with defaults if needed
/// CRITICAL: Returns Arc to ensure shared ownership across all access points
pub fn get_global_components() -> &'static Arc<GlobalSearcherComponents> {
    GLOBAL_SEARCHER_COMPONENTS.get_or_init(|| {
        debug_println!("RUST DEBUG: Initializing global searcher components with SHARED defaults");
        let components = Arc::new(GlobalSearcherComponents::new(GlobalCacheConfig::default()));
        let cache_ptr = Arc::as_ptr(&components) as usize;
        debug_println!(
            "RUST DEBUG: Created default GlobalSearcherComponents at address: 0x{:x}",
            cache_ptr
        );
        components
    })
}

/// Get the global SearcherContext
/// CRITICAL FIX: Return the SAME SearcherContext instance every time for true cache sharing
/// Uses RwLock<Option<...>> pattern to allow reset for test isolation
pub fn get_global_searcher_context() -> Arc<SearcherContext> {
    debug_println!(
        "🔍 CACHE ENTRY: get_global_searcher_context() called - using SHARED global caches"
    );

    let holder = get_searcher_context_holder();

    // First, try to get existing context with read lock (fast path)
    {
        let guard = holder.read().unwrap();
        if let Some(ref context) = *guard {
            let ref_count = Arc::strong_count(context);
            debug_println!(
                "🔍 CACHE REUSE: Returning SHARED SearcherContext at {:p}, ref_count: {}",
                Arc::as_ptr(context),
                ref_count
            );
            return context.clone();
        }
    }

    // Need to create - acquire write lock
    let mut guard = holder.write().unwrap();

    // Double-check after acquiring write lock
    if let Some(ref context) = *guard {
        debug_println!(
            "🔍 CACHE REUSE: Another thread created context, returning {:p}",
            Arc::as_ptr(context)
        );
        return context.clone();
    }

    // Create new context
    debug_println!("🔍 CACHE INIT: Creating THE SINGLE SHARED SearcherContext instance");
    let components = get_global_components();
    let context = components.create_searcher_context(SearcherConfig::default());
    debug_println!(
        "🔍 CACHE CREATED: THE SHARED SearcherContext created at {:p}",
        Arc::as_ptr(&context)
    );
    debug_println!(
        "🔍 CACHE IDENTITY: Split footer cache instance at: {:p}",
        &context.split_footer_cache as *const _
    );

    *guard = Some(context.clone());
    context
}

/// Clear the global SearcherContext - called when last cache manager is closed
/// This is critical for test isolation to prevent cached data from leaking between tests
pub fn clear_global_searcher_context() {
    debug_println!("🧹 CLEAR_GLOBAL_SEARCHER_CONTEXT: Clearing global searcher context");

    let holder = get_searcher_context_holder();
    let mut guard = holder.write().unwrap();
    if guard.is_some() {
        *guard = None;
        debug_println!("🧹 CLEAR_GLOBAL_SEARCHER_CONTEXT: Context cleared");
    } else {
        debug_println!("🧹 CLEAR_GLOBAL_SEARCHER_CONTEXT: Context was already None");
    }
}

/// Clear all credential-specific SearcherContexts - called when last cache manager is closed
/// This is critical for test isolation to prevent cached data from leaking between tests
pub fn clear_credential_contexts() {
    debug_println!("🧹 CLEAR_CREDENTIAL_CONTEXTS: Clearing all credential-specific contexts");

    let holder = get_credential_contexts_holder();
    let mut cache = holder.lock().unwrap();
    let count = cache.len();
    cache.clear();

    debug_println!("🧹 CLEAR_CREDENTIAL_CONTEXTS: Cleared {} credential contexts", count);
}

/// Get a credential-specific SearcherContext
/// This creates separate SearcherContext instances per credential set, ensuring that
/// cached data from one credential set is not served to another credential set.
/// This is critical for multi-tenant security.
pub fn get_credential_searcher_context(credential_key: &str) -> Arc<SearcherContext> {
    debug_println!(
        "🔐 CREDENTIAL_CONTEXT: Getting SearcherContext for credential key: {}",
        credential_key
    );

    // Check if we already have a context for this credential key
    {
        let cache = get_credential_contexts_holder().lock().unwrap();
        if let Some(context) = cache.get(credential_key) {
            debug_println!(
                "🔐 CREDENTIAL_CONTEXT_HIT: Reusing SearcherContext for key: {} at {:p}",
                credential_key,
                Arc::as_ptr(context)
            );
            return context.clone();
        }
    }

    // Create a new SearcherContext for this credential key
    debug_println!(
        "🔐 CREDENTIAL_CONTEXT_MISS: Creating new SearcherContext for key: {}",
        credential_key
    );
    let components = get_global_components();
    let context = components.create_searcher_context(SearcherConfig::default());
    debug_println!(
        "🔐 CREDENTIAL_CONTEXT_CREATED: New SearcherContext at {:p} for key: {}",
        Arc::as_ptr(&context),
        credential_key
    );

    // Cache it
    {
        let mut cache = get_credential_contexts_holder().lock().unwrap();
        // Double-check in case another thread created it
        if let Some(existing) = cache.get(credential_key) {
            debug_println!(
                "🔐 CREDENTIAL_CONTEXT_RACE: Another thread created context, using existing at {:p}",
                Arc::as_ptr(existing)
            );
            return existing.clone();
        }
        cache.insert(credential_key.to_string(), context.clone());
    }

    context
}

/// Get a SearcherContext with custom configuration but using global caches
pub fn get_searcher_context_with_config(searcher_config: SearcherConfig) -> Arc<SearcherContext> {
    debug_println!("RUST DEBUG: Getting SearcherContext with custom config");
    get_global_components().create_searcher_context(searcher_config)
}

/// Get the global L2 disk cache if configured
/// First checks the dynamically set cache (from SplitCacheManager),
/// then falls back to the components' disk_cache if set at initialization
pub fn get_global_disk_cache() -> Option<Arc<L2DiskCache>> {
    // First check the dynamically set disk cache
    let holder = get_disk_cache_holder();
    let guard = holder.read().unwrap();
    if let Some(ref cache) = *guard {
        debug_println!("🟢 GET_GLOBAL_DISK_CACHE: Found dynamic disk cache");
        return Some(cache.clone());
    }
    drop(guard);

    // Fall back to the components' disk_cache
    let result = get_global_components().disk_cache.clone();
    if result.is_some() {
        debug_println!("🟢 GET_GLOBAL_DISK_CACHE: Found components disk cache");
    } else {
        debug_println!("🔴 GET_GLOBAL_DISK_CACHE: No disk cache configured - WILL HIT S3!");
    }
    result
}
