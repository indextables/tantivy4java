// config.rs - Global cache configuration types
// Extracted from global_cache.rs during refactoring

use std::num::NonZeroU32;
use std::path::PathBuf;

use bytesize::ByteSize;
use quickwit_config::SplitCacheLimits;

use crate::disk_cache::DiskCacheConfig;

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
            disk_cache_config: None,     // L2 disk cache disabled by default
        }
    }
}
