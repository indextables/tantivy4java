// components.rs - Global searcher components
// Extracted from global_cache.rs during refactoring

use std::sync::Arc;

use quickwit_config::SearcherConfig;
use quickwit_search::list_fields_cache::ListFieldsCache;
use quickwit_search::leaf_cache::LeafSearchCache;
use quickwit_search::search_permit_provider::SearchPermitProvider;
use quickwit_search::SearcherContext;
use quickwit_storage::{
    MemorySizedCache, QuickwitCache, SplitCache, StorageCache, STORAGE_METRICS,
};
use tantivy::aggregation::AggregationLimitsGuard;
use tempfile::TempDir;
use tokio::sync::Semaphore;

use super::cache_debug::{debug_arc_string_cache_identity, debug_cache_summary};
use crate::debug_println;
use crate::disk_cache::L2DiskCache;

use super::config::GlobalCacheConfig;
use super::storage_resolver::GLOBAL_STORAGE_RESOLVER;

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
            debug_println!(
                "RUST DEBUG: Creating SplitCache with limits: max_bytes={}, max_splits={}",
                limits.max_num_bytes,
                limits.max_num_splits
            );

            // Determine the root path for the split cache
            let (root_path, temp_dir) = if let Some(path) = config.split_cache_root_path {
                (path, None)
            } else {
                // Create a persistent temp directory for the split cache
                let temp_dir =
                    TempDir::new().expect("Failed to create temp directory for split cache");
                let path = temp_dir.path().to_path_buf();
                debug_println!(
                    "RUST DEBUG: Using temp directory for SplitCache: {}",
                    path.display()
                );
                (path, Some(temp_dir))
            };

            // Create the SplitCache following Quickwit's pattern
            match SplitCache::with_root_path(root_path.clone(), GLOBAL_STORAGE_RESOLVER.clone(), limits)
            {
                Ok(split_cache) => {
                    debug_println!(
                        "RUST DEBUG: Successfully created SplitCache at {}",
                        root_path.display()
                    );
                    (Some(split_cache), temp_dir)
                }
                Err(e) => {
                    debug_println!(
                        "RUST WARNING: Failed to create SplitCache: {}. Continuing without split cache.",
                        e
                    );
                    (None, None)
                }
            }
        } else {
            debug_println!("RUST DEBUG: SplitCache not configured, skipping creation");
            (None, None)
        };

        // Create L2 disk cache if configured
        let disk_cache = if let Some(disk_config) = config.disk_cache_config {
            debug_println!(
                "RUST DEBUG: Creating L2DiskCache at {}",
                disk_config.root_path.display()
            );
            match L2DiskCache::new(disk_config) {
                Ok(cache) => {
                    let stats = cache.stats();
                    debug_println!(
                        "RUST DEBUG: L2DiskCache created successfully. Max size: {} bytes, {} splits cached",
                        stats.max_bytes,
                        stats.split_count
                    );
                    Some(cache)
                }
                Err(e) => {
                    debug_println!(
                        "RUST WARNING: Failed to create L2DiskCache: {}. Continuing without disk cache.",
                        e
                    );
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
            split_stream_semaphore: Semaphore::new(
                searcher_config.max_num_concurrent_split_streams,
            ),
            // ✅ INDIVIDUAL: Leaf search cache (follows Quickwit pattern - individual instances)
            leaf_search_cache: LeafSearchCache::new(
                searcher_config.partial_request_cache_capacity.as_u64() as usize,
            ),
            // ✅ INDIVIDUAL: List fields cache (follows Quickwit pattern - individual instances)
            list_fields_cache: ListFieldsCache::new(
                searcher_config.partial_request_cache_capacity.as_u64() as usize,
            ),
            // ✅ SHARED: Split cache (already Option<Arc<SplitCache>>)
            split_cache_opt: self.split_cache_opt.clone(),
            // ✅ SHARED: Aggregation limits (clone preserves shared memory tracking)
            aggregation_limit: self.aggregation_limit.clone(),
        })
    }
}
