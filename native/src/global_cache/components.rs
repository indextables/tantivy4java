// components.rs - Global searcher components
// Extracted from global_cache.rs during refactoring

use std::sync::Arc;

use bytesize::ByteSize;
use quickwit_config::{CacheConfig, SearcherConfig};
use quickwit_search::SearcherContext;
use quickwit_storage::SplitCache;
use tempfile::TempDir;

use crate::memory_pool::{global_pool, MemoryReservation};

use crate::debug_println;
use crate::disk_cache::L2DiskCache;

use super::config::GlobalCacheConfig;
use super::storage_resolver::GLOBAL_STORAGE_RESOLVER;

/// Global SearcherContext components.
///
/// Holds the process-wide split cache, L2 disk cache, and the cache/limit sizing
/// derived from `GlobalCacheConfig`. Every `SearcherContext` is built by
/// `SearcherContext::new_without_invoker`, which constructs its *own* fast-field,
/// footer, leaf-search, list-fields, permit-provider and aggregation-limit
/// instances from the `SearcherConfig` we hand it — so the way `GlobalCacheConfig`
/// knobs take effect is by being written into that `SearcherConfig` (see
/// `build_searcher_config`), NOT by holding separate cache instances here. Sharing
/// across searchers is achieved by there being a single cached `SearcherContext`
/// (see `get_global_searcher_context`), plus one per credential set.
pub struct GlobalSearcherComponents {
    /// Split cache - caches entire split files on disk (optional)
    pub split_cache_opt: Option<Arc<SplitCache>>,
    /// L2 disk cache - tiered persistent disk cache (optional)
    pub disk_cache: Option<Arc<L2DiskCache>>,
    /// Temp directory for split cache (kept alive to prevent cleanup)
    _temp_dir: Option<TempDir>,
    /// Cache/limit sizing written into every `SearcherConfig` we build, so the
    /// Java-configured `GlobalCacheConfig` knobs actually size the live caches.
    fast_field_cache_capacity: ByteSize,
    split_footer_cache_capacity: ByteSize,
    partial_request_cache_capacity: ByteSize,
    predicate_cache_capacity: ByteSize,
    max_concurrent_splits: usize,
    aggregation_memory_limit: ByteSize,
    aggregation_bucket_limit: u32,
    warmup_memory_budget: ByteSize,
    /// Memory reservation for the predicate cache, held for the lifetime of the components
    _predicate_cache_reservation: Option<MemoryReservation>,
}

impl GlobalSearcherComponents {
    /// Create new global searcher components with the given configuration
    pub fn new(config: GlobalCacheConfig) -> Self {
        debug_println!("RUST DEBUG: Creating new GlobalSearcherComponents");

        // Capture the cache/limit sizing so build_searcher_config can apply it. The
        // actual cache instances are created inside each SearcherContext from the
        // SearcherConfig these values produce.
        let fast_field_cache_capacity = config.fast_field_cache_capacity;
        let split_footer_cache_capacity = config.split_footer_cache_capacity;
        let partial_request_cache_capacity = config.partial_request_cache_capacity;
        let max_concurrent_splits = config.max_concurrent_splits;
        let aggregation_memory_limit = config.aggregation_memory_limit;
        let aggregation_bucket_limit = config.aggregation_bucket_limit;
        let warmup_memory_budget = config.warmup_memory_budget;

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

        // Reserve memory for the predicate cache in the unified memory pool
        let predicate_cache_capacity = config.predicate_cache_capacity;
        let predicate_cache_reservation = match MemoryReservation::try_new(
            &global_pool(),
            predicate_cache_capacity.as_u64() as usize,
            "predicate_cache",
        ) {
            Ok(r) => {
                debug_println!(
                    "RUST DEBUG: Reserved {} MB for predicate_cache in memory pool",
                    predicate_cache_capacity.as_u64() / 1024 / 1024
                );
                Some(r)
            }
            Err(e) => {
                debug_println!(
                    "RUST WARNING: Memory pool denied predicate_cache reservation of {} MB: {}. Proceeding anyway.",
                    predicate_cache_capacity.as_u64() / 1024 / 1024, e
                );
                None
            }
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
            split_cache_opt,
            disk_cache,
            _temp_dir: temp_dir,
            fast_field_cache_capacity,
            split_footer_cache_capacity,
            partial_request_cache_capacity,
            predicate_cache_capacity,
            max_concurrent_splits,
            aggregation_memory_limit,
            aggregation_bucket_limit,
            warmup_memory_budget,
            _predicate_cache_reservation: predicate_cache_reservation,
        }
    }

    /// Configured aggregation limits `(memory_bytes, bucket_limit)` from the
    /// Java-supplied `GlobalCacheConfig`, for code paths that build their own
    /// `AggregationLimitsGuard` (which are not fed by the `SearcherContext`).
    pub fn aggregation_limits(&self) -> (u64, u32) {
        (self.aggregation_memory_limit.as_u64(), self.aggregation_bucket_limit)
    }

    /// Build a `SearcherConfig` carrying every cache/limit size from the
    /// Java-supplied `GlobalCacheConfig`. `SearcherContext::new_without_invoker`
    /// constructs its fast-field / footer / leaf-search / list-fields caches, its
    /// concurrency permit provider, and its aggregation-memory guard from these
    /// values, so this is what makes those knobs actually take effect.
    pub fn build_searcher_config(&self) -> SearcherConfig {
        let mut config = SearcherConfig::default();
        config.fast_field_cache = CacheConfig::default_with_capacity(self.fast_field_cache_capacity);
        config.split_footer_cache =
            CacheConfig::default_with_capacity(self.split_footer_cache_capacity);
        config.partial_request_cache =
            CacheConfig::default_with_capacity(self.partial_request_cache_capacity);
        config.predicate_cache = CacheConfig::default_with_capacity(self.predicate_cache_capacity);
        config.max_num_concurrent_split_searches = self.max_concurrent_splits;
        config.aggregation_memory_limit = self.aggregation_memory_limit;
        config.aggregation_bucket_limit = self.aggregation_bucket_limit;
        config.warmup_memory_budget = self.warmup_memory_budget;
        config
    }

    /// Create a SearcherContext sized from `build_searcher_config`. Cache *sharing*
    /// across searchers comes from there being a single cached context (see
    /// `get_global_searcher_context`) — each context still owns its cache instances.
    pub fn create_searcher_context(&self, searcher_config: SearcherConfig) -> Arc<SearcherContext> {
        debug_println!("RUST DEBUG: Creating SearcherContext (sized from GlobalCacheConfig)");

        // Use SearcherContext::new_without_invoker which handles all required fields correctly
        Arc::new(SearcherContext::new_without_invoker(
            searcher_config,
            self.split_cache_opt.clone(),
        ))
    }
}
