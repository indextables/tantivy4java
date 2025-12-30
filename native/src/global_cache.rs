// global_cache.rs - Global cache infrastructure following Quickwit's pattern
//
// This module provides global, shared caches and storage resolvers that are
// reused across all split searcher instances, following the same architecture
// as Quickwit to ensure efficient resource utilization.

use std::sync::{Arc, OnceLock};
use tokio::sync::RwLock as TokioRwLock;
use std::path::PathBuf;
use std::num::NonZeroU32;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use bytesize::ByteSize;

use quickwit_storage::{
    StorageResolver, MemorySizedCache, QuickwitCache, StorageCache,
    STORAGE_METRICS,
    SplitCache
};
use quickwit_config::{StorageConfigs, S3StorageConfig, AzureStorageConfig, SearcherConfig, SplitCacheLimits};
use quickwit_search::{SearcherContext, search_permit_provider::SearchPermitProvider};
use quickwit_search::leaf_cache::LeafSearchCache;
use quickwit_search::list_fields_cache::ListFieldsCache;
use tantivy::aggregation::AggregationLimitsGuard;
use tokio::sync::Semaphore;
use tempfile::TempDir;

use crate::debug_println;
use crate::cache_debug::{debug_arc_string_cache_identity, debug_cache_summary};
use crate::disk_cache::{L2DiskCache, DiskCacheConfig};

/// Helper function to track storage instance creation for debugging
/// This helps us understand when and where multiple storage instances are created
pub async fn tracked_storage_resolve(
    resolver: &StorageResolver,
    uri: &quickwit_common::uri::Uri,
    context: &str
) -> Result<Arc<dyn quickwit_storage::Storage>, quickwit_storage::StorageResolverError> {
    static STORAGE_COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);
    let storage_id = STORAGE_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    debug_println!("🏗️  STORAGE_RESOLVE: Starting storage resolve #{} [{}]", storage_id, context);
    debug_println!("   📍 Resolver address: {:p}", resolver);
    debug_println!("   🌐 URI: {}", uri);

    let resolve_start = std::time::Instant::now();
    let result = resolver.resolve(uri).await;

    match &result {
        Ok(storage) => {
            debug_println!("✅ STORAGE_RESOLVED: Storage instance #{} created in {}ms [{}]",
                     storage_id, resolve_start.elapsed().as_millis(), context);
            debug_println!("   🏭 Storage address: {:p}", &**storage);
            debug_println!("   📊 Storage type: {}", std::any::type_name::<dyn quickwit_storage::Storage>());
        }
        Err(e) => {
            debug_println!("❌ STORAGE_RESOLVE_FAILED: Storage resolve #{} failed in {}ms [{}]: {}",
                     storage_id, resolve_start.elapsed().as_millis(), context, e);
        }
    }

    result
}

/// Global configuration for caches and storage
pub struct GlobalCacheConfig {
    /// Fast field cache capacity (default: 1GB)
    pub fast_field_cache_capacity: ByteSize,
    /// Split footer cache capacity (default: 500MB)
    pub split_footer_cache_capacity: ByteSize,
    /// Partial request cache capacity (default: 64MB)
    pub partial_request_cache_capacity: ByteSize,
    /// Maximum concurrent split searches (default: 100)
    pub max_concurrent_splits: usize,
    /// Aggregation memory limit (default: 500MB)
    pub aggregation_memory_limit: ByteSize,
    /// Maximum aggregation buckets (default: 65000)
    pub aggregation_bucket_limit: u32,
    /// Warmup memory budget (default: 100GB)
    pub warmup_memory_budget: ByteSize,
    /// Split cache configuration (optional)
    pub split_cache_limits: Option<SplitCacheLimits>,
    /// Split cache root directory (if not specified, uses temp directory)
    pub split_cache_root_path: Option<PathBuf>,
    /// L2 disk cache configuration (optional)
    pub disk_cache_config: Option<DiskCacheConfig>,
}

impl Default for GlobalCacheConfig {
    fn default() -> Self {
        // Default split cache configuration following Quickwit's defaults
        let default_split_cache_limits = Some(SplitCacheLimits {
            max_num_bytes: ByteSize::gb(10), // 10GB default for split cache
            max_num_splits: NonZeroU32::new(10_000).unwrap(),
            num_concurrent_downloads: NonZeroU32::new(1).unwrap(),
            max_file_descriptors: NonZeroU32::new(100).unwrap(),
        });
        
        Self {
            fast_field_cache_capacity: ByteSize::gb(1),
            split_footer_cache_capacity: ByteSize::mb(500),
            partial_request_cache_capacity: ByteSize::mb(64),
            max_concurrent_splits: 100,
            aggregation_memory_limit: ByteSize::mb(500),
            aggregation_bucket_limit: 65000,
            warmup_memory_budget: ByteSize::gb(100),
            split_cache_limits: default_split_cache_limits,
            split_cache_root_path: None, // Will use temp directory if not specified
            disk_cache_config: None, // L2 disk cache disabled by default
        }
    }
}

/// Global StorageResolver instance following Quickwit's pattern
/// This is a singleton that is shared across all searcher instances
pub static GLOBAL_STORAGE_RESOLVER: Lazy<StorageResolver> = Lazy::new(|| {
    debug_println!("RUST DEBUG: Initializing global StorageResolver singleton");
    let storage_configs = StorageConfigs::default();
    StorageResolver::configured(&storage_configs)
});

/// Global storage resolver cache for configured S3 instances (async-compatible)
/// Uses tokio::sync::RwLock to prevent deadlocks in async context
static CONFIGURED_STORAGE_RESOLVERS: std::sync::OnceLock<TokioRwLock<std::collections::HashMap<String, StorageResolver>>> = std::sync::OnceLock::new();

/// Get or create a cached StorageResolver with specific S3/Azure credentials (async version)
/// This follows Quickwit's pattern but enables caching for optimal storage instance reuse
///
/// 🚨 CRITICAL: This function should be used for ALL storage resolver creation in ASYNC contexts
/// to ensure consistent cache sharing. Direct calls to StorageResolver::configured()
/// bypass caching and cause multiple storage instances.
///
/// ✅ FIXED: Async-compatible using tokio::sync::RwLock to prevent deadlocks
pub async fn get_configured_storage_resolver_async(
    s3_config_opt: Option<S3StorageConfig>,
    azure_config_opt: Option<AzureStorageConfig>
) -> StorageResolver {
    static RESOLVER_COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);

    if let Some(s3_config) = s3_config_opt {
        // Create a cache key from S3 configuration
        // NOTE: We may also need to add "user id" later for multi-tenant scenarios
        let cache_key = format!("{}:{}:{}:{}",
            s3_config.region.as_ref().map(|r| r.as_str()).unwrap_or("default"),
            s3_config.endpoint.as_ref().map(|e| e.as_str()).unwrap_or("default"),
            s3_config.access_key_id.as_ref().map(|k| &k[..8]).unwrap_or("none"), // First 8 chars for security
            s3_config.force_path_style_access
        );

        // ✅ FIXED: Use async tokio RwLock to prevent deadlocks in async context
        // Initialize cache if needed
        let cache = CONFIGURED_STORAGE_RESOLVERS.get_or_init(|| {
            TokioRwLock::new(std::collections::HashMap::new())
        });

        // Try to get from cache first (async read lock) - DEADLOCK FIXED
        {
            let read_cache = cache.read().await;
            if let Some(cached_resolver) = read_cache.get(&cache_key) {
                debug_println!("🎯 STORAGE_RESOLVER_CACHE_HIT: Reusing cached resolver for key: {} at address {:p}",
                         cache_key, cached_resolver);
                return cached_resolver.clone();
            }
        } // <- async read lock released here

        // Cache miss - create new resolver (outside of any locks to prevent deadlock)
        let resolver_id = RESOLVER_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        debug_println!("❌ STORAGE_RESOLVER_CACHE_MISS: Creating new resolver #{} for key: {}", resolver_id, cache_key);
        debug_println!("   📋 S3 Config: region={:?}, endpoint={:?}, path_style={}",
                  s3_config.region, s3_config.endpoint, s3_config.force_path_style_access);

        let storage_configs = StorageConfigs::new(vec![
            quickwit_config::StorageConfig::S3(s3_config)
        ]);
        let resolver = StorageResolver::configured(&storage_configs); // <- DEADLOCK SUSPECT: Quickwit resolver creation
        debug_println!("✅ STORAGE_RESOLVER_CREATED: Resolver #{} created at address {:p}", resolver_id, &resolver);

        // Cache the new resolver (async write lock) - DEADLOCK FIXED
        {
            let mut write_cache = cache.write().await;
            // Double-check in case another thread created it while we were creating ours
            if let Some(existing_resolver) = write_cache.get(&cache_key) {
                debug_println!("🏃 STORAGE_RESOLVER_RACE: Another thread created resolver, using existing at {:p}", existing_resolver);
                return existing_resolver.clone();
            }
            write_cache.insert(cache_key.clone(), resolver.clone());
            debug_println!("💾 STORAGE_RESOLVER_CACHED: Resolver #{} cached for key: {}", resolver_id, cache_key);
        } // <- async write lock released here

        resolver
    }
    else if let Some(azure_config) = azure_config_opt {
        // ✅ NEW AZURE LOGIC - EXACT S3 PATTERN
        let cache_key = format!("azure:{}:{}",
            azure_config.account_name
                .as_ref()
                .map(|n| n.as_str())
                .unwrap_or("default"),
            azure_config.access_key
                .as_ref()
                .map(|k| &k[..8.min(k.len())])  // First 8 chars for security
                .unwrap_or("none")
        );

        let cache = CONFIGURED_STORAGE_RESOLVERS.get_or_init(|| {
            TokioRwLock::new(HashMap::new())
        });

        // Try read lock first (async)
        {
            let read_cache = cache.read().await;
            if let Some(cached_resolver) = read_cache.get(&cache_key) {
                debug_println!("🎯 AZURE_RESOLVER_CACHE_HIT: Reusing resolver for key: {}", cache_key);
                return cached_resolver.clone();
            }
        } // <- Lock released immediately

        // Create resolver OUTSIDE lock (deadlock prevention)
        let resolver_id = RESOLVER_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        debug_println!("❌ AZURE_RESOLVER_CACHE_MISS: Creating resolver #{} for key: {}",
                      resolver_id, cache_key);
        debug_println!("   📋 Azure Config: account={:?}", azure_config.account_name);

        let storage_configs = StorageConfigs::new(vec![
            quickwit_config::StorageConfig::Azure(azure_config)
        ]);
        let resolver = StorageResolver::configured(&storage_configs);
        debug_println!("✅ AZURE_RESOLVER_CREATED: Resolver #{} at {:p}", resolver_id, &resolver);

        // Cache with write lock (async)
        {
            let mut write_cache = cache.write().await;
            // Double-check for race condition
            if let Some(existing_resolver) = write_cache.get(&cache_key) {
                debug_println!("🏃 AZURE_RESOLVER_RACE: Using existing at {:p}", existing_resolver);
                return existing_resolver.clone();
            }
            write_cache.insert(cache_key.clone(), resolver.clone());
            debug_println!("💾 AZURE_RESOLVER_CACHED: Resolver #{} cached", resolver_id);
        } // <- Lock released immediately

        resolver
    }
    else {
        let resolver_id = RESOLVER_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        debug_println!("🌐 STORAGE_RESOLVER_GLOBAL: Resolver #{} - Using global unconfigured StorageResolver", resolver_id);
        let resolver = GLOBAL_STORAGE_RESOLVER.clone();
        debug_println!("♻️  STORAGE_RESOLVER_REUSED: Resolver #{} reused global at address {:p}", resolver_id, &resolver);
        resolver
    }
}

/// Get or create a cached StorageResolver with specific S3/Azure credentials (sync version)
/// This version uses a simple sync-safe caching approach for sync contexts
///
/// ✅ FIXED: Now uses deadlock-safe sync caching approach
pub fn get_configured_storage_resolver(
    s3_config_opt: Option<S3StorageConfig>,
    azure_config_opt: Option<AzureStorageConfig>
) -> StorageResolver {
    use std::sync::{Mutex, Arc};
    use once_cell::sync::Lazy;

    static SYNC_STORAGE_RESOLVERS: Lazy<Arc<Mutex<std::collections::HashMap<String, StorageResolver>>>> =
        Lazy::new(|| Arc::new(Mutex::new(std::collections::HashMap::new())));
    static RESOLVER_COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(1);

    if let Some(s3_config) = s3_config_opt {
        // Create a cache key from S3 configuration
        // NOTE: We may also need to add "user id" later for multi-tenant scenarios
        let cache_key = format!("{}:{}:{}:{}",
            s3_config.region.as_ref().map(|r| r.as_str()).unwrap_or("default"),
            s3_config.endpoint.as_ref().map(|e| e.as_str()).unwrap_or("default"),
            s3_config.access_key_id.as_ref().map(|k| &k[..8]).unwrap_or("none"), // First 8 chars for security
            s3_config.force_path_style_access
        );

        // Try to get from sync cache (simple mutex approach)
        {
            let cache = SYNC_STORAGE_RESOLVERS.lock().unwrap();
            if let Some(cached_resolver) = cache.get(&cache_key) {
                debug_println!("🎯 STORAGE_RESOLVER_SYNC_CACHE_HIT: Reusing cached sync resolver for key: {} at address {:p}",
                         cache_key, cached_resolver);
                return cached_resolver.clone();
            }
        }

        // Cache miss - create new resolver
        let resolver_id = RESOLVER_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        debug_println!("❌ STORAGE_RESOLVER_SYNC_CACHE_MISS: Creating new sync resolver #{} for key: {}", resolver_id, cache_key);
        debug_println!("   📋 S3 Config: region={:?}, endpoint={:?}, path_style={}",
                  s3_config.region, s3_config.endpoint, s3_config.force_path_style_access);

        let storage_configs = StorageConfigs::new(vec![
            quickwit_config::StorageConfig::S3(s3_config)
        ]);
        let resolver = StorageResolver::configured(&storage_configs);
        debug_println!("✅ STORAGE_RESOLVER_CREATED: Sync resolver #{} created at address {:p}", resolver_id, &resolver);

        // Cache the new resolver
        {
            let mut cache = SYNC_STORAGE_RESOLVERS.lock().unwrap();
            // Double-check in case another thread created it while we were creating ours
            if let Some(existing_resolver) = cache.get(&cache_key) {
                debug_println!("🏃 STORAGE_RESOLVER_SYNC_RACE: Another thread created sync resolver, using existing at {:p}", existing_resolver);
                return existing_resolver.clone();
            }
            cache.insert(cache_key.clone(), resolver.clone());
            debug_println!("💾 STORAGE_RESOLVER_SYNC_CACHED: Resolver #{} cached for key: {}", resolver_id, cache_key);
        }

        resolver
    }
    else if let Some(azure_config) = azure_config_opt {
        // ✅ SAME PATTERN as async but with std::sync::Mutex
        let cache_key = format!("azure:{}:{}",
            azure_config.account_name
                .as_ref()
                .map(|n| n.as_str())
                .unwrap_or("default"),
            azure_config.access_key
                .as_ref()
                .map(|k| &k[..8.min(k.len())])  // First 8 chars for security
                .unwrap_or("none")
        );

        // Try to get from sync cache
        {
            let cache = SYNC_STORAGE_RESOLVERS.lock().unwrap();
            if let Some(cached_resolver) = cache.get(&cache_key) {
                debug_println!("🎯 AZURE_RESOLVER_SYNC_CACHE_HIT: Reusing cached sync resolver for key: {} at address {:p}",
                         cache_key, cached_resolver);
                return cached_resolver.clone();
            }
        }

        // Cache miss - create new resolver
        let resolver_id = RESOLVER_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        debug_println!("❌ AZURE_RESOLVER_SYNC_CACHE_MISS: Creating new sync resolver #{} for key: {}", resolver_id, cache_key);
        debug_println!("   📋 Azure Config: account={:?}", azure_config.account_name);

        let storage_configs = StorageConfigs::new(vec![
            quickwit_config::StorageConfig::Azure(azure_config)
        ]);
        let resolver = StorageResolver::configured(&storage_configs);
        debug_println!("✅ AZURE_RESOLVER_CREATED: Sync resolver #{} created at address {:p}", resolver_id, &resolver);

        // Cache the new resolver
        {
            let mut cache = SYNC_STORAGE_RESOLVERS.lock().unwrap();
            // Double-check in case another thread created it while we were creating ours
            if let Some(existing_resolver) = cache.get(&cache_key) {
                debug_println!("🏃 AZURE_RESOLVER_SYNC_RACE: Another thread created sync resolver, using existing at {:p}", existing_resolver);
                return existing_resolver.clone();
            }
            cache.insert(cache_key.clone(), resolver.clone());
            debug_println!("💾 AZURE_RESOLVER_SYNC_CACHED: Resolver #{} cached for key: {}", resolver_id, cache_key);
        }

        resolver
    }
    else {
        let resolver_id = RESOLVER_COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        debug_println!("🌐 STORAGE_RESOLVER_GLOBAL_SYNC: Resolver #{} - Using global unconfigured StorageResolver", resolver_id);
        let resolver = GLOBAL_STORAGE_RESOLVER.clone();
        debug_println!("♻️  STORAGE_RESOLVER_REUSED_SYNC: Resolver #{} reused global at address {:p}", resolver_id, &resolver);
        resolver
    }
}

/// Global SearcherContext components
/// These are the shared caches that should be reused across all searcher instances
/// We use Arc to share these non-clonable types across multiple SearcherContext instances
pub struct GlobalSearcherComponents {
    /// Fast fields cache - shared across all searchers
    pub fast_fields_cache: Arc<dyn StorageCache>,
    /// Split footer cache - shared across all searchers (wrapped in Arc for sharing)
    pub split_footer_cache: Arc<MemorySizedCache<String>>,
    /// Leaf search cache - shared across all searchers (wrapped in Arc for sharing)
    pub leaf_search_cache: Arc<LeafSearchCache>,
    /// List fields cache - shared across all searchers (wrapped in Arc for sharing)
    pub list_fields_cache: Arc<ListFieldsCache>,
    /// Search permit provider - manages concurrent searches (wrapped in Arc for sharing)
    pub search_permit_provider: Arc<SearchPermitProvider>,
    /// Split stream semaphore - limits concurrent streams
    pub split_stream_semaphore: Arc<Semaphore>,
    /// Aggregation limits guard - shared memory tracking
    pub aggregation_limit: AggregationLimitsGuard,
    /// Split cache - caches entire split files on disk (optional)
    pub split_cache_opt: Option<Arc<SplitCache>>,
    /// L2 disk cache - tiered persistent disk cache with compression (optional)
    pub disk_cache: Option<Arc<L2DiskCache>>,
    /// Temp directory for split cache (kept alive to prevent cleanup)
    _temp_dir: Option<TempDir>,
}

impl GlobalSearcherComponents {
    /// Create new global searcher components with the given configuration
    pub fn new(config: GlobalCacheConfig) -> Self {
        debug_println!("RUST DEBUG: Creating new GlobalSearcherComponents");
        
        // Create fast field cache
        let fast_field_cache_capacity = config.fast_field_cache_capacity.as_u64() as usize;
        let fast_fields_cache = Arc::new(QuickwitCache::new(fast_field_cache_capacity));
        
        // Create split footer cache (wrapped in Arc for sharing)
        let split_footer_cache_capacity = config.split_footer_cache_capacity.as_u64() as usize;
        let split_footer_cache = Arc::new(MemorySizedCache::with_capacity_in_bytes(
            split_footer_cache_capacity,
            &STORAGE_METRICS.split_footer_cache,
        ));
        debug_arc_string_cache_identity(&split_footer_cache, "split_footer_cache");
        
        // Create leaf search cache (wrapped in Arc for sharing)
        let partial_cache_capacity = config.partial_request_cache_capacity.as_u64() as usize;
        let leaf_search_cache = Arc::new(LeafSearchCache::new(partial_cache_capacity));
        
        // Create list fields cache (wrapped in Arc for sharing)
        let list_fields_cache = Arc::new(ListFieldsCache::new(partial_cache_capacity));
        
        // Create sync search permit provider to avoid async channel conflicts
        // Using new_sync() method that doesn't use async channels
        let search_permit_provider = Arc::new(SearchPermitProvider::new_sync(
            config.max_concurrent_splits,
            config.warmup_memory_budget,
        ));
        
        // Create split stream semaphore (using 10 as default like Quickwit)
        let split_stream_semaphore = Arc::new(Semaphore::new(10));
        
        // Create aggregation limits guard
        let aggregation_limit = AggregationLimitsGuard::new(
            Some(config.aggregation_memory_limit.as_u64()),
            Some(config.aggregation_bucket_limit),
        );
        
        // Create SplitCache if configured
        let (split_cache_opt, temp_dir) = if let Some(limits) = config.split_cache_limits {
            debug_println!("RUST DEBUG: Creating SplitCache with limits: max_bytes={}, max_splits={}", 
                         limits.max_num_bytes, limits.max_num_splits);
            
            // Determine the root path for the split cache
            let (root_path, temp_dir) = if let Some(path) = config.split_cache_root_path {
                (path, None)
            } else {
                // Create a persistent temp directory for the split cache
                let temp_dir = TempDir::new()
                    .expect("Failed to create temp directory for split cache");
                let path = temp_dir.path().to_path_buf();
                debug_println!("RUST DEBUG: Using temp directory for SplitCache: {}", path.display());
                (path, Some(temp_dir))
            };
            
            // Create the SplitCache following Quickwit's pattern
            match SplitCache::with_root_path(
                root_path.clone(),
                GLOBAL_STORAGE_RESOLVER.clone(),
                limits,
            ) {
                Ok(split_cache) => {
                    debug_println!("RUST DEBUG: Successfully created SplitCache at {}", root_path.display());
                    (Some(split_cache), temp_dir)
                },
                Err(e) => {
                    debug_println!("RUST WARNING: Failed to create SplitCache: {}. Continuing without split cache.", e);
                    (None, None)
                }
            }
        } else {
            debug_println!("RUST DEBUG: SplitCache not configured, skipping creation");
            (None, None)
        };

        // Create L2 disk cache if configured
        let disk_cache = if let Some(disk_config) = config.disk_cache_config {
            debug_println!("RUST DEBUG: Creating L2DiskCache at {}", disk_config.root_path.display());
            match L2DiskCache::new(disk_config) {
                Ok(cache) => {
                    let stats = cache.stats();
                    debug_println!("RUST DEBUG: L2DiskCache created successfully. Max size: {} bytes, {} splits cached",
                                 stats.max_bytes, stats.split_count);
                    Some(cache)
                }
                Err(e) => {
                    debug_println!("RUST WARNING: Failed to create L2DiskCache: {}. Continuing without disk cache.", e);
                    None
                }
            }
        } else {
            debug_println!("RUST DEBUG: L2DiskCache not configured, skipping creation");
            None
        };

        Self {
            fast_fields_cache,
            split_footer_cache,
            leaf_search_cache,
            list_fields_cache,
            search_permit_provider,
            split_stream_semaphore,
            aggregation_limit,
            split_cache_opt,
            disk_cache,
            _temp_dir: temp_dir,
        }
    }
    
    /// Create a SearcherContext from these global components
    /// This ensures all SearcherContext instances share the same cache instances
    /// FIXED: Now properly shares ALL cache instances including split_footer_cache
    pub fn create_searcher_context(&self, searcher_config: SearcherConfig) -> Arc<SearcherContext> {
        debug_println!("RUST DEBUG: Creating SearcherContext from SHARED global components");
        debug_arc_string_cache_identity(&self.split_footer_cache, "split_footer_cache");
        debug_cache_summary();

        // CRITICAL: Create a custom SearcherContext that shares ALL cache instances
        // This requires careful construction to ensure cache sharing
        Arc::new(SearcherContext {
            searcher_config: searcher_config.clone(),
            // ✅ SHARED: Fast fields cache (already Arc<dyn StorageCache>)
            fast_fields_cache: self.fast_fields_cache.clone(),
            // ✅ SHARED: Create sync search permit provider for this context
            // Using sync mode to avoid async channel conflicts when sharing contexts
            search_permit_provider: SearchPermitProvider::new_sync(
                searcher_config.max_num_concurrent_split_searches,
                searcher_config.warmup_memory_budget,
            ),
            // ✅ INDIVIDUAL: Split footer cache (follows Quickwit pattern - individual instances, shared metrics)
            // This follows Quickwit's design where each SearcherContext gets its own cache instance
            // but all instances share the same STORAGE_METRICS for global coordination
            split_footer_cache: MemorySizedCache::with_capacity_in_bytes(
                searcher_config.split_footer_cache_capacity.as_u64() as usize,
                &STORAGE_METRICS.split_footer_cache, // SHARED metrics for global coordination
            ),
            // ✅ INDIVIDUAL: Split stream semaphore (per-context limiting is correct)
            split_stream_semaphore: Semaphore::new(searcher_config.max_num_concurrent_split_streams),
            // ✅ INDIVIDUAL: Leaf search cache (follows Quickwit pattern - individual instances)
            leaf_search_cache: LeafSearchCache::new(
                searcher_config.partial_request_cache_capacity.as_u64() as usize
            ),
            // ✅ INDIVIDUAL: List fields cache (follows Quickwit pattern - individual instances)
            list_fields_cache: ListFieldsCache::new(
                searcher_config.partial_request_cache_capacity.as_u64() as usize
            ),
            // ✅ SHARED: Split cache (already Option<Arc<SplitCache>>)
            split_cache_opt: self.split_cache_opt.clone(),
            // ✅ SHARED: Aggregation limits (clone preserves shared memory tracking)
            aggregation_limit: self.aggregation_limit.clone(),
        })
    }
}

/// Global instance of searcher components - using OnceLock for better thread safety
/// This ensures only one instance is ever created across all Spark tasks/executors
static GLOBAL_SEARCHER_COMPONENTS: OnceLock<Arc<GlobalSearcherComponents>> = OnceLock::new();

/// Global cached SearcherContext - the ACTUAL shared instance that should be reused
static GLOBAL_SEARCHER_CONTEXT: OnceLock<Arc<SearcherContext>> = OnceLock::new();

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
    debug_println!("RUST DEBUG: Created GlobalSearcherComponents at address: 0x{:x}", cache_ptr);
    GLOBAL_SEARCHER_COMPONENTS.set(components).is_ok()
}

/// Get the global searcher components, initializing with defaults if needed
/// CRITICAL: Returns Arc to ensure shared ownership across all access points
pub fn get_global_components() -> &'static Arc<GlobalSearcherComponents> {
    GLOBAL_SEARCHER_COMPONENTS.get_or_init(|| {
        debug_println!("RUST DEBUG: Initializing global searcher components with SHARED defaults");
        let components = Arc::new(GlobalSearcherComponents::new(GlobalCacheConfig::default()));
        let cache_ptr = Arc::as_ptr(&components) as usize;
        debug_println!("RUST DEBUG: Created default GlobalSearcherComponents at address: 0x{:x}", cache_ptr);
        components
    })
}

/// Get the global SearcherContext
/// CRITICAL FIX: Return the SAME SearcherContext instance every time for true cache sharing
pub fn get_global_searcher_context() -> Arc<SearcherContext> {
    debug_println!("🔍 CACHE ENTRY: get_global_searcher_context() called - using SHARED global caches");
    debug_println!("🔍 CACHE ENTRY: get_global_searcher_context() called - using SHARED global caches");

    let context = GLOBAL_SEARCHER_CONTEXT.get_or_init(|| {
        debug_println!("🔍 CACHE INIT: Creating THE SINGLE SHARED SearcherContext instance");
        let components = get_global_components();
        let context = components.create_searcher_context(SearcherConfig::default());
        debug_println!("🔍 CACHE CREATED: THE SHARED SearcherContext created at {:p}", Arc::as_ptr(&context));
        debug_println!("🔍 CACHE IDENTITY: Split footer cache instance at: {:p}", &context.split_footer_cache as *const _);
        context
    });

    let ref_count = Arc::strong_count(context);
    debug_println!("🔍 CACHE REUSE: Returning SHARED SearcherContext at {:p}, ref_count: {}", Arc::as_ptr(context), ref_count);
    debug_println!("🔍 CACHE IDENTITY: Reusing split footer cache instance at: {:p}", &context.split_footer_cache as *const _);
    context.clone()
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

// =====================================================================
// Global S3/Storage Download Metrics
// =====================================================================
// These metrics track all remote storage downloads for debugging and
// verification of caching behavior. They are incremented from:
// - persistent_cache_storage.rs (StorageWithPersistentCache) on L3 fetch
// - prewarm.rs on data download during prewarm operations
//
// Use get_storage_download_metrics() to retrieve current counts and
// reset_storage_download_metrics() to reset counters between tests.
// =====================================================================

use std::sync::atomic::{AtomicU64, Ordering};

/// Global counters for storage download metrics
static STORAGE_DOWNLOAD_COUNT: AtomicU64 = AtomicU64::new(0);
static STORAGE_DOWNLOAD_BYTES: AtomicU64 = AtomicU64::new(0);
static STORAGE_DOWNLOAD_PREWARM_COUNT: AtomicU64 = AtomicU64::new(0);
static STORAGE_DOWNLOAD_PREWARM_BYTES: AtomicU64 = AtomicU64::new(0);
static STORAGE_DOWNLOAD_QUERY_COUNT: AtomicU64 = AtomicU64::new(0);
static STORAGE_DOWNLOAD_QUERY_BYTES: AtomicU64 = AtomicU64::new(0);

/// Storage download metrics snapshot
#[derive(Debug, Clone, Default)]
pub struct StorageDownloadMetrics {
    /// Total number of storage downloads (all sources)
    pub total_downloads: u64,
    /// Total bytes downloaded (all sources)
    pub total_bytes: u64,
    /// Downloads during prewarm operations
    pub prewarm_downloads: u64,
    /// Bytes downloaded during prewarm operations
    pub prewarm_bytes: u64,
    /// Downloads during query operations (L3 cache misses)
    pub query_downloads: u64,
    /// Bytes downloaded during query operations
    pub query_bytes: u64,
}

impl StorageDownloadMetrics {
    /// Returns true if there were any downloads
    pub fn has_downloads(&self) -> bool {
        self.total_downloads > 0
    }

    /// Returns formatted summary string
    pub fn summary(&self) -> String {
        format!(
            "Downloads: {} total ({} bytes), {} prewarm ({} bytes), {} query ({} bytes)",
            self.total_downloads, self.total_bytes,
            self.prewarm_downloads, self.prewarm_bytes,
            self.query_downloads, self.query_bytes
        )
    }
}

/// Record a storage download from prewarm operations
pub fn record_prewarm_download(bytes: u64) {
    STORAGE_DOWNLOAD_COUNT.fetch_add(1, Ordering::Relaxed);
    STORAGE_DOWNLOAD_BYTES.fetch_add(bytes, Ordering::Relaxed);
    STORAGE_DOWNLOAD_PREWARM_COUNT.fetch_add(1, Ordering::Relaxed);
    STORAGE_DOWNLOAD_PREWARM_BYTES.fetch_add(bytes, Ordering::Relaxed);
    debug_println!("📊 METRIC: Prewarm download recorded: {} bytes (total prewarm: {} downloads, {} bytes)",
        bytes,
        STORAGE_DOWNLOAD_PREWARM_COUNT.load(Ordering::Relaxed),
        STORAGE_DOWNLOAD_PREWARM_BYTES.load(Ordering::Relaxed));
}

/// Record a storage download from query operations (L3 cache miss)
pub fn record_query_download(bytes: u64) {
    STORAGE_DOWNLOAD_COUNT.fetch_add(1, Ordering::Relaxed);
    STORAGE_DOWNLOAD_BYTES.fetch_add(bytes, Ordering::Relaxed);
    STORAGE_DOWNLOAD_QUERY_COUNT.fetch_add(1, Ordering::Relaxed);
    STORAGE_DOWNLOAD_QUERY_BYTES.fetch_add(bytes, Ordering::Relaxed);
    debug_println!("📊 METRIC: Query download recorded: {} bytes (total query: {} downloads, {} bytes)",
        bytes,
        STORAGE_DOWNLOAD_QUERY_COUNT.load(Ordering::Relaxed),
        STORAGE_DOWNLOAD_QUERY_BYTES.load(Ordering::Relaxed));
}

/// Get current storage download metrics
pub fn get_storage_download_metrics() -> StorageDownloadMetrics {
    StorageDownloadMetrics {
        total_downloads: STORAGE_DOWNLOAD_COUNT.load(Ordering::Relaxed),
        total_bytes: STORAGE_DOWNLOAD_BYTES.load(Ordering::Relaxed),
        prewarm_downloads: STORAGE_DOWNLOAD_PREWARM_COUNT.load(Ordering::Relaxed),
        prewarm_bytes: STORAGE_DOWNLOAD_PREWARM_BYTES.load(Ordering::Relaxed),
        query_downloads: STORAGE_DOWNLOAD_QUERY_COUNT.load(Ordering::Relaxed),
        query_bytes: STORAGE_DOWNLOAD_QUERY_BYTES.load(Ordering::Relaxed),
    }
}

/// Reset all storage download metrics (useful between tests)
pub fn reset_storage_download_metrics() {
    STORAGE_DOWNLOAD_COUNT.store(0, Ordering::Relaxed);
    STORAGE_DOWNLOAD_BYTES.store(0, Ordering::Relaxed);
    STORAGE_DOWNLOAD_PREWARM_COUNT.store(0, Ordering::Relaxed);
    STORAGE_DOWNLOAD_PREWARM_BYTES.store(0, Ordering::Relaxed);
    STORAGE_DOWNLOAD_QUERY_COUNT.store(0, Ordering::Relaxed);
    STORAGE_DOWNLOAD_QUERY_BYTES.store(0, Ordering::Relaxed);
    debug_println!("📊 METRIC: Storage download metrics reset");
}