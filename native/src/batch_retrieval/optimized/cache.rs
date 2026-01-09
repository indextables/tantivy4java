// cache.rs - Quickwit storage-based persistent cache management

use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use quickwit_storage::{ByteRangeCache, OwnedBytes, Storage, StorageResolver, STORAGE_METRICS};

use crate::debug_println;

/// Configuration for Quickwit-integrated persistent cache
#[derive(Debug, Clone)]
pub struct QuickwitCacheConfig {
    /// Cache storage URI (file:// or s3:// for persistent cache)
    pub cache_storage_uri: String,

    /// Maximum memory cache size (default: 256MB)
    pub memory_cache_size: usize,

    /// Cache key prefix to avoid conflicts
    pub cache_key_prefix: String,

    /// Enable storage-level compression (handled by Quickwit storage)
    pub enable_storage_compression: bool,
}

impl Default for QuickwitCacheConfig {
    fn default() -> Self {
        Self {
            cache_storage_uri: "file://./cache".to_string(),
            memory_cache_size: 256 * 1024 * 1024, // 256MB
            cache_key_prefix: "tantivy4java".to_string(),
            enable_storage_compression: true,
        }
    }
}

/// Multi-tier caching system leveraging Quickwit's async storage
pub struct QuickwitPersistentCacheManager {
    pub(crate) memory_cache: ByteRangeCache,
    disk_storage: Arc<dyn Storage>,
    config: QuickwitCacheConfig,
}

impl QuickwitPersistentCacheManager {
    /// Create cache manager using Quickwit's async storage system
    pub async fn new(
        config: QuickwitCacheConfig,
        storage_resolver: Arc<StorageResolver>,
    ) -> Result<Self> {
        // Use Quickwit's storage resolver to create persistent cache storage
        use quickwit_common::uri::Uri;
        let cache_uri: Uri = config
            .cache_storage_uri
            .parse()
            .map_err(|e| anyhow::anyhow!("Failed to parse cache storage URI: {}", e))?;
        let disk_storage = storage_resolver
            .resolve(&cache_uri)
            .await
            .context("Failed to resolve cache storage URI")?;

        // 🚀 MEMORY SAFETY: Use bounded L1 cache with configured memory size
        // Cache clears itself when capacity is exceeded - data is safe in L2 disk cache
        let memory_cache = ByteRangeCache::with_capacity(
            config.memory_cache_size as u64,
            &STORAGE_METRICS.shortlived_cache,
        );

        debug_println!(
            "💾 QUICKWIT_CACHE_INIT: Initialized persistent cache with {}MB bounded memory, storage: {}",
            config.memory_cache_size / (1024 * 1024),
            config.cache_storage_uri
        );

        Ok(Self {
            memory_cache,
            disk_storage,
            config,
        })
    }

    /// Multi-tier cache lookup with automatic promotion using Quickwit storage
    pub async fn get_range(
        &self,
        split_path: &Path,
        byte_range: Range<usize>,
    ) -> Option<OwnedBytes> {
        // L1: Memory cache lookup
        if let Some(data) = self.memory_cache.get_slice(split_path, byte_range.clone()) {
            debug_println!(
                "✅ L1_CACHE_HIT: Memory cache hit for {}:{:?}",
                split_path.display(),
                byte_range
            );
            return Some(data);
        }

        // L2: Disk cache lookup using Quickwit storage
        let cache_key = self.generate_cache_key(split_path, &byte_range);
        match self.disk_storage.get_slice(&cache_key, 0..usize::MAX).await {
            Ok(data) => {
                debug_println!(
                    "✅ L2_CACHE_HIT: Quickwit storage cache hit for {}:{:?}",
                    split_path.display(),
                    byte_range
                );

                // Promote to memory cache
                self.memory_cache
                    .put_slice(split_path.to_owned(), byte_range, data.clone());
                Some(data)
            }
            Err(_) => {
                debug_println!(
                    "❌ CACHE_MISS: No cache hit for {}:{:?}",
                    split_path.display(),
                    byte_range
                );
                None
            }
        }
    }

    /// Store range in both memory and Quickwit storage cache
    pub async fn put_range(
        &self,
        split_path: PathBuf,
        byte_range: Range<usize>,
        data: OwnedBytes,
    ) -> Result<()> {
        // Store in memory cache
        self.memory_cache
            .put_slice(split_path.clone(), byte_range.clone(), data.clone());

        // Store in Quickwit storage asynchronously
        let cache_key = self.generate_cache_key(&split_path, &byte_range);
        let payload = Box::new(data.as_slice().to_vec());
        self.disk_storage
            .put(&cache_key, payload)
            .await
            .context("Failed to store data in Quickwit storage cache")?;

        debug_println!(
            "💾 QUICKWIT_CACHED: Stored {} bytes to Quickwit storage with key: {}",
            data.len(),
            cache_key.display()
        );

        Ok(())
    }

    /// Generate cache key using Quickwit storage path conventions
    fn generate_cache_key(&self, split_path: &Path, byte_range: &Range<usize>) -> PathBuf {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        split_path.hash(&mut hasher);
        byte_range.start.hash(&mut hasher);
        byte_range.end.hash(&mut hasher);

        let hash = format!("{:016x}", hasher.finish());

        // Use hierarchical path structure for better storage performance
        PathBuf::from(format!(
            "{}/ranges/{}/{}/{}.cache",
            self.config.cache_key_prefix,
            &hash[0..2],  // First level
            &hash[2..4],  // Second level
            &hash[4..]
        )) // Filename
    }
}
