// global_cache.rs - Global cache infrastructure following Quickwit's pattern
//
// This module provides global, shared caches and storage resolvers that are
// reused across all split searcher instances, following the same architecture
// as Quickwit to ensure efficient resource utilization.

use std::sync::Arc;
use std::path::PathBuf;
use std::num::NonZeroU32;
use once_cell::sync::{Lazy, OnceCell};
use bytesize::ByteSize;

use quickwit_storage::{
    StorageResolver, MemorySizedCache, QuickwitCache, StorageCache,
    STORAGE_METRICS,
    SplitCache
};
use quickwit_config::{StorageConfigs, S3StorageConfig, SearcherConfig, SplitCacheLimits};
use quickwit_search::{SearcherContext, search_permit_provider::SearchPermitProvider};
use quickwit_search::leaf_cache::LeafSearchCache;
use quickwit_search::list_fields_cache::ListFieldsCache;
use tantivy::aggregation::AggregationLimitsGuard;
use tokio::sync::Semaphore;
use tempfile::TempDir;

use crate::debug_println;

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

/// Get or create a configured StorageResolver with specific S3 credentials
/// This follows Quickwit's pattern but allows for dynamic S3 configuration
pub fn get_configured_storage_resolver(s3_config_opt: Option<S3StorageConfig>) -> StorageResolver {
    if let Some(s3_config) = s3_config_opt {
        debug_println!("RUST DEBUG: Creating StorageResolver with custom S3 config");
        let storage_configs = StorageConfigs::new(vec![
            quickwit_config::StorageConfig::S3(s3_config)
        ]);
        StorageResolver::configured(&storage_configs)
    } else {
        debug_println!("RUST DEBUG: Using global unconfigured StorageResolver");
        GLOBAL_STORAGE_RESOLVER.clone()
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
        
        // Create leaf search cache (wrapped in Arc for sharing)
        let partial_cache_capacity = config.partial_request_cache_capacity.as_u64() as usize;
        let leaf_search_cache = Arc::new(LeafSearchCache::new(partial_cache_capacity));
        
        // Create list fields cache (wrapped in Arc for sharing)
        let list_fields_cache = Arc::new(ListFieldsCache::new(partial_cache_capacity));
        
        // Create search permit provider (wrapped in Arc for sharing)
        let search_permit_provider = Arc::new(SearchPermitProvider::new(
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
        
        Self {
            fast_fields_cache,
            split_footer_cache,
            leaf_search_cache,
            list_fields_cache,
            search_permit_provider,
            split_stream_semaphore,
            aggregation_limit,
            split_cache_opt,
            _temp_dir: temp_dir,
        }
    }
    
    /// Create a SearcherContext from these global components
    /// This is similar to how Quickwit creates its SearcherContext
    /// Note: Unfortunately, SearcherContext in Quickwit expects owned types, not Arc references
    /// So for now we have to create new instances, but they should still share the underlying
    /// storage caches through Arc
    pub fn create_searcher_context(&self, searcher_config: SearcherConfig) -> Arc<SearcherContext> {
        debug_println!("RUST DEBUG: Creating SearcherContext from global components");
        
        // Create new instances for types that don't support Arc sharing
        // But reuse the Arc-wrapped caches where possible
        Arc::new(SearcherContext {
            searcher_config: searcher_config.clone(),
            fast_fields_cache: self.fast_fields_cache.clone(),
            search_permit_provider: SearchPermitProvider::new(
                searcher_config.max_num_concurrent_split_searches,
                searcher_config.warmup_memory_budget,
            ),
            split_footer_cache: MemorySizedCache::with_capacity_in_bytes(
                searcher_config.split_footer_cache_capacity.as_u64() as usize,
                &STORAGE_METRICS.split_footer_cache,
            ),
            split_stream_semaphore: Semaphore::new(searcher_config.max_num_concurrent_split_streams),
            leaf_search_cache: LeafSearchCache::new(
                searcher_config.partial_request_cache_capacity.as_u64() as usize
            ),
            list_fields_cache: ListFieldsCache::new(
                searcher_config.partial_request_cache_capacity.as_u64() as usize
            ),
            split_cache_opt: self.split_cache_opt.clone(),
            aggregation_limit: self.aggregation_limit.clone(),
        })
    }
}

/// Global instance of searcher components
/// This can be initialized with custom configuration from Java
static GLOBAL_SEARCHER_COMPONENTS: OnceCell<GlobalSearcherComponents> = OnceCell::new();

/// Initialize the global cache with custom configuration
/// This should be called once at startup from Java if custom configuration is needed
/// Returns true if initialization succeeded, false if already initialized
pub fn initialize_global_cache(config: GlobalCacheConfig) -> bool {
    debug_println!("RUST DEBUG: Attempting to initialize global cache with custom config");
    GLOBAL_SEARCHER_COMPONENTS.set(GlobalSearcherComponents::new(config)).is_ok()
}

/// Get the global searcher components, initializing with defaults if needed
pub fn get_global_components() -> &'static GlobalSearcherComponents {
    GLOBAL_SEARCHER_COMPONENTS.get_or_init(|| {
        debug_println!("RUST DEBUG: Initializing global searcher components with defaults");
        GlobalSearcherComponents::new(GlobalCacheConfig::default())
    })
}

/// Get the global SearcherContext
/// This provides a convenient way to get a SearcherContext with all global caches
pub fn get_global_searcher_context() -> Arc<SearcherContext> {
    debug_println!("RUST DEBUG: Getting global SearcherContext");
    get_global_components().create_searcher_context(SearcherConfig::default())
}

/// Get a SearcherContext with custom configuration but using global caches
pub fn get_searcher_context_with_config(searcher_config: SearcherConfig) -> Arc<SearcherContext> {
    debug_println!("RUST DEBUG: Getting SearcherContext with custom config");
    get_global_components().create_searcher_context(searcher_config)
}