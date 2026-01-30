// manager.rs - Core GlobalSplitCacheManager struct and implementation
// Extracted from split_cache_manager.rs during refactoring

#![allow(dead_code)]

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use quickwit_config::{AzureStorageConfig, S3StorageConfig};

use crate::debug_println;
use crate::disk_cache::{CompressionAlgorithm, DiskCacheConfig, L2DiskCache};
use crate::global_cache::{get_configured_storage_resolver, get_global_searcher_context};
use crate::batch_retrieval::simple::{BatchOptimizationMetrics, PrefetchStats};

// Debug logging macro - controlled by TANTIVY4JAVA_DEBUG environment variable
macro_rules! debug_log {
    ($($arg:tt)*) => {
        debug_println!("DEBUG: {}", format!($($arg)*));
    };
}

/// Global cache manager that follows Quickwit's multi-level caching architecture
/// This now uses the global caches from GLOBAL_SEARCHER_COMPONENTS
pub struct GlobalSplitCacheManager {
    pub(crate) cache_name: String,
    pub(crate) max_cache_size: u64,

    // Storage configurations for resolver
    pub(crate) s3_config: Option<S3StorageConfig>,
    pub(crate) azure_config: Option<AzureStorageConfig>,

    // L2 Disk cache for persistent caching
    pub(crate) disk_cache: Option<Arc<L2DiskCache>>,

    // Statistics
    pub(crate) total_hits: AtomicU64,
    pub(crate) total_misses: AtomicU64,
    pub(crate) total_evictions: AtomicU64,
    pub(crate) current_size: AtomicU64,

    // Managed splits
    pub(crate) managed_splits: Mutex<HashMap<String, u64>>, // split_path -> last_access_time
}

impl GlobalSplitCacheManager {
    pub fn new(cache_name: String, max_cache_size: u64) -> Self {
        debug_println!(
            "RUST DEBUG: Creating GlobalSplitCacheManager '{}' using global caches",
            cache_name
        );

        // CRITICAL FIX: DO NOT create separate Tokio runtime - causes multiple runtime deadlocks
        // All async operations should use the shared global runtime via QuickwitRuntimeManager
        debug_println!("🔧 RUNTIME_FIX: Eliminating separate Tokio runtime to prevent deadlocks");

        // Set the L1 cache capacity from Java's CacheConfig.withMaxCacheSize()
        // This ensures the global shared L1 cache uses the Java-configured size
        crate::global_cache::set_l1_cache_capacity(max_cache_size);

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
        debug_println!(
            "RUST DEBUG: Initializing L2DiskCache for cache '{}' at {:?}",
            self.cache_name,
            config.root_path
        );

        match L2DiskCache::new(config) {
            Ok(cache) => {
                let stats = cache.stats();
                debug_println!(
                    "RUST DEBUG: L2DiskCache created successfully. Max size: {} bytes, {} splits cached",
                    stats.max_bytes,
                    stats.split_count
                );
                // Also set the global disk cache so StandaloneSearcher can access it
                crate::global_cache::set_global_disk_cache(cache.clone());
                self.disk_cache = Some(cache);
            }
            Err(e) => {
                debug_println!(
                    "RUST WARNING: Failed to create L2DiskCache: {}. Continuing without disk cache.",
                    e
                );
            }
        }
    }

    /// Get reference to L2 disk cache if configured
    pub fn get_disk_cache(&self) -> Option<&Arc<L2DiskCache>> {
        self.disk_cache.as_ref()
    }

    pub fn add_split(&self, split_path: String) {
        let mut splits = self.managed_splits.lock().unwrap();
        splits.insert(
            split_path,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );
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
        let total_evictions =
            byte_range_evictions + footer_evictions + fastfield_evictions + split_evictions;
        let current_size = byte_range_bytes + footer_bytes + fastfield_bytes + split_bytes;

        debug_log!("📊 Comprehensive Cache Metrics:");
        debug_log!(
            "  📦 ByteRangeCache: {} hits, {} misses, {} evictions, {} bytes",
            byte_range_hits,
            byte_range_misses,
            byte_range_evictions,
            byte_range_bytes
        );
        debug_log!(
            "  📄 FooterCache: {} hits, {} misses, {} evictions, {} bytes",
            footer_hits,
            footer_misses,
            footer_evictions,
            footer_bytes
        );
        debug_log!(
            "  ⚡ FastFieldCache: {} hits, {} misses, {} evictions, {} bytes",
            fastfield_hits,
            fastfield_misses,
            fastfield_evictions,
            fastfield_bytes
        );
        debug_log!(
            "  🔍 SplitCache: {} hits, {} misses, {} evictions, {} bytes",
            split_hits,
            split_misses,
            split_evictions,
            split_bytes
        );
        debug_log!(
            "  🏆 Total: {} hits, {} misses, {} evictions, {} bytes ({}% hit rate)",
            total_hits,
            total_misses,
            total_evictions,
            current_size,
            if total_hits + total_misses > 0 {
                (total_hits * 100) / (total_hits + total_misses)
            } else {
                0
            }
        );

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
        debug_println!(
            "RUST DEBUG: Setting AWS config for cache '{}'",
            self.cache_name
        );
        self.s3_config = Some(s3_config);
    }

    // Set Azure configuration for storage resolver
    pub fn set_azure_config(&mut self, azure_config: AzureStorageConfig) {
        debug_println!(
            "RUST DEBUG: Setting Azure config for cache '{}'",
            self.cache_name
        );
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

/// Counter for active cache managers - used to determine when to clear global caches
/// When counter goes from 1 to 0 (last manager closed), all global caches are cleared
/// This is critical for test isolation
pub static ACTIVE_MANAGER_COUNT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Increment active manager count - called when a new cache manager is created
pub fn increment_manager_count() {
    let old_count = ACTIVE_MANAGER_COUNT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    debug_println!("🔢 MANAGER_COUNT: Incremented from {} to {}", old_count, old_count + 1);
}

/// Decrement active manager count and clear global caches if this was the last manager
/// Returns true if global caches were cleared (this was the last manager)
pub fn decrement_manager_count_and_maybe_cleanup() -> bool {
    let old_count = ACTIVE_MANAGER_COUNT.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    debug_println!("🔢 MANAGER_COUNT: Decremented from {} to {}", old_count, old_count - 1);

    if old_count == 1 {
        // This was the last manager - clear all global caches for test isolation
        debug_println!("🧹 MANAGER_COUNT: Last manager closed, clearing all global caches");
        clear_all_global_caches();
        true
    } else {
        false
    }
}

/// Clear all global caches - called when last cache manager is closed
/// This is critical for test isolation to prevent data leaking between tests
fn clear_all_global_caches() {
    debug_println!("🧹 CLEAR_ALL_GLOBAL_CACHES: Starting global cache cleanup");

    // 1. Clear split schema cache
    crate::split_query::clear_split_schema_cache();

    // 2. Clear global searcher context (holds leaf_search_cache, split_footer_cache, etc.)
    crate::global_cache::clear_global_searcher_context();

    // 3. Clear credential-specific contexts
    crate::global_cache::clear_credential_contexts();

    // 4. Clear searcher cache (LRU cache of Arc<Searcher>)
    crate::split_searcher::clear_searcher_cache();

    // 5. Clear global disk cache reference
    crate::global_cache::clear_global_disk_cache();

    // 6. Reset L1 cache (clear all entries)
    crate::global_cache::reset_global_l1_cache();

    // 7. Reset storage download metrics
    crate::global_cache::reset_storage_download_metrics();

    debug_println!("🧹 CLEAR_ALL_GLOBAL_CACHES: Complete");
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
