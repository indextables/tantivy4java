// metrics.rs - Global S3/Storage Download Metrics
// Extracted from global_cache.rs during refactoring
//
// These metrics track all remote storage downloads for debugging and
// verification of caching behavior. They are incremented from:
// - persistent_cache_storage.rs (StorageWithPersistentCache) on L3 fetch
// - prewarm.rs on data download during prewarm operations
//
// Use get_storage_download_metrics() to retrieve current counts and
// reset_storage_download_metrics() to reset counters between tests.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::debug_println;

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
            self.total_downloads,
            self.total_bytes,
            self.prewarm_downloads,
            self.prewarm_bytes,
            self.query_downloads,
            self.query_bytes
        )
    }
}

/// Record a storage download from prewarm operations
pub fn record_prewarm_download(bytes: u64) {
    STORAGE_DOWNLOAD_COUNT.fetch_add(1, Ordering::Relaxed);
    STORAGE_DOWNLOAD_BYTES.fetch_add(bytes, Ordering::Relaxed);
    STORAGE_DOWNLOAD_PREWARM_COUNT.fetch_add(1, Ordering::Relaxed);
    STORAGE_DOWNLOAD_PREWARM_BYTES.fetch_add(bytes, Ordering::Relaxed);
    debug_println!(
        "📊 METRIC: Prewarm download recorded: {} bytes (total prewarm: {} downloads, {} bytes)",
        bytes,
        STORAGE_DOWNLOAD_PREWARM_COUNT.load(Ordering::Relaxed),
        STORAGE_DOWNLOAD_PREWARM_BYTES.load(Ordering::Relaxed)
    );
}

/// Record a storage download from query operations (L3 cache miss)
pub fn record_query_download(bytes: u64) {
    STORAGE_DOWNLOAD_COUNT.fetch_add(1, Ordering::Relaxed);
    STORAGE_DOWNLOAD_BYTES.fetch_add(bytes, Ordering::Relaxed);
    STORAGE_DOWNLOAD_QUERY_COUNT.fetch_add(1, Ordering::Relaxed);
    STORAGE_DOWNLOAD_QUERY_BYTES.fetch_add(bytes, Ordering::Relaxed);
    debug_println!(
        "📊 METRIC: Query download recorded: {} bytes (total query: {} downloads, {} bytes)",
        bytes,
        STORAGE_DOWNLOAD_QUERY_COUNT.load(Ordering::Relaxed),
        STORAGE_DOWNLOAD_QUERY_BYTES.load(Ordering::Relaxed)
    );
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
