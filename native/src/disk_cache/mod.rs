//! Tiered Disk Cache (L2) for tantivy4java
//!
//! Provides persistent disk-based caching with intelligent compression.
//! Read path: L1 (memory) -> L2 (disk) -> L3 (remote storage)
//!
//! Features:
//! - LZ4 compression by default (fast, good ratio for index data)
//! - Component-aware compression (skips small/hot data)
//! - Split-level LRU eviction
//! - Background async writes
//! - Crash-safe manifest persistence
//! - **Range coalescing** - serve partial cache hits, only fetch gaps from S3
//!
//! This module is organized into:
//! - `types` - Configuration and entry types
//! - `range_index` - Range coalescing and overlap queries
//! - `manifest` - Split and cache manifest persistence
//! - `lru` - LRU eviction tracking
//! - `mmap_cache` - Memory-mapped file cache
//! - `background` - Background writer thread
//! - `compression` - Compression logic
//! - `path_helpers` - Path generation utilities
//! - `get_ops` - Get operations (get, get_subrange, get_coalesced)
//! - `write_ops` - Write operations (do_put, do_evict, do_sync_manifest)

// Submodules
pub mod types;
pub mod range_index;
pub mod manifest;
pub mod lru;
pub mod mmap_cache;
pub mod background;
pub mod compression;
pub mod path_helpers;
pub mod get_ops;
pub mod write_ops;

#[cfg(test)]
mod tests;

// Re-exports
pub use types::{CompressionAlgorithm, DiskCacheConfig, ComponentEntry, DEFAULT_MMAP_CACHE_SIZE, WriteQueueMode};
pub use range_index::{CachedRange, CachedSegment, CoalesceResult};
pub use manifest::{SplitEntry, SplitState, CacheManifest};

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Read};
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::Duration;

use crate::debug_println;
use tantivy::directory::OwnedBytes;

use lru::SplitLruTable;
use mmap_cache::MmapCache;
use background::WriteRequest;

/// Abstraction over the write channel sender supporting both fragment-based
/// (bounded sync_channel) and size-based (unbounded channel + byte counter) modes.
pub(crate) enum WriteSender {
    /// Fragment-based: bounded sync_channel with N slots.
    Fragment(std::sync::mpsc::SyncSender<WriteRequest>),
    /// Size-based: unbounded channel + byte counter for backpressure.
    SizeBased {
        tx: std::sync::mpsc::Sender<WriteRequest>,
        queued_bytes: Arc<AtomicU64>,
        max_bytes: u64,
        backpressure: Arc<(Mutex<()>, Condvar)>,
    },
}

impl WriteSender {
    /// Send a write request, blocking if backpressure is needed.
    /// For `Put` requests in size-based mode, tracks data size for backpressure.
    fn send(&self, req: WriteRequest) -> Result<(), ()> {
        match self {
            WriteSender::Fragment(tx) => tx.send(req).map_err(|_| ()),
            WriteSender::SizeBased {
                tx,
                queued_bytes,
                max_bytes,
                backpressure,
            } => {
                // Track bytes for Put requests
                let data_len = match &req {
                    WriteRequest::Put { data, .. } => data.len() as u64,
                    _ => 0,
                };

                if data_len > 0 {
                    // Wait until queued bytes are below the limit
                    let (lock, cvar) = &**backpressure;
                    let mut guard = lock.lock().unwrap();
                    while queued_bytes.load(Ordering::Acquire) + data_len > *max_bytes {
                        guard = cvar.wait(guard).unwrap();
                    }
                    queued_bytes.fetch_add(data_len, Ordering::Release);
                }

                tx.send(req).map_err(|_| ())
            }
        }
    }

    /// Try to send without blocking (used in Drop).
    fn try_send(&self, req: WriteRequest) -> Result<(), ()> {
        match self {
            WriteSender::Fragment(tx) => tx.try_send(req).map_err(|_| ()),
            WriteSender::SizeBased { tx, queued_bytes, .. } => {
                let data_len = match &req {
                    WriteRequest::Put { data, .. } => data.len() as u64,
                    _ => 0,
                };
                if data_len > 0 {
                    queued_bytes.fetch_add(data_len, Ordering::Release);
                }
                tx.send(req).map_err(|_| ())
            }
        }
    }

    /// Send a Put request if the queue has capacity, otherwise drop it silently.
    /// Returns `true` if enqueued, `false` if dropped.
    /// Non-Put requests are always sent (they're small control messages).
    fn send_or_drop(&self, req: WriteRequest) -> bool {
        match self {
            WriteSender::Fragment(tx) => tx.try_send(req).is_ok(),
            WriteSender::SizeBased {
                tx,
                queued_bytes,
                max_bytes,
                ..
            } => {
                let data_len = match &req {
                    WriteRequest::Put { data, .. } => data.len() as u64,
                    _ => 0,
                };

                if data_len > 0 && queued_bytes.load(Ordering::Acquire) + data_len > *max_bytes {
                    // Queue is over capacity — drop the write
                    return false;
                }

                if data_len > 0 {
                    queued_bytes.fetch_add(data_len, Ordering::Release);
                }
                tx.send(req).is_ok()
            }
        }
    }
}

/// L2 Disk Cache implementation
pub struct L2DiskCache {
    config: DiskCacheConfig,
    manifest: RwLock<CacheManifest>,
    /// In-memory state for fast range coalescing (split_key -> SplitState)
    split_states: RwLock<HashMap<String, SplitState>>,
    lru_table: Mutex<SplitLruTable>,
    /// Memory-mapped file cache for fast random access
    mmap_cache: Mutex<MmapCache>,
    /// Write channel sender with configurable backpressure mode.
    write_tx: WriteSender,
    total_bytes: AtomicU64,
    max_bytes: u64,
    /// Shutdown flag for background threads
    shutdown_flag: Arc<std::sync::atomic::AtomicBool>,
    /// Thread handles for cleanup
    thread_handles: Mutex<Vec<std::thread::JoinHandle<()>>>,
    /// Dirty flag - set when manifest has uncommitted changes
    manifest_dirty: Arc<std::sync::atomic::AtomicBool>,
}

#[allow(dead_code)]
impl L2DiskCache {
    /// Create a new L2 disk cache
    pub fn new(config: DiskCacheConfig) -> io::Result<Arc<Self>> {
        // Ensure cache directory exists (root_path/tantivy4java_slicecache)
        let cache_dir = path_helpers::cache_dir(&config.root_path);
        fs::create_dir_all(&cache_dir)?;

        debug_println!("🔵 L2DiskCache::new() - cache_dir={:?}", cache_dir);

        // Calculate max size
        let max_bytes = config.effective_max_size()?;

        // Load or create manifest
        let manifest = Self::load_manifest(&cache_dir)?;
        let total_bytes = manifest.total_bytes;
        debug_println!("🔵 L2DiskCache::new() - manifest loaded: {} splits, {} total_bytes",
                 manifest.splits.len(), total_bytes);

        // Build split states from manifest for fast coalescing
        let mut split_states = HashMap::new();
        for (split_key, split_entry) in &manifest.splits {
            split_states.insert(split_key.clone(), SplitState::from_entry(split_entry));
        }

        // Create background writer channel based on configured mode.
        let (write_tx, write_rx, size_based_state) = match &config.write_queue_mode {
            WriteQueueMode::Fragment { capacity } => {
                let (tx, rx) = std::sync::mpsc::sync_channel(*capacity);
                (WriteSender::Fragment(tx), rx, None)
            }
            WriteQueueMode::SizeBased { max_bytes } => {
                let (tx, rx) = std::sync::mpsc::channel();
                let queued_bytes = Arc::new(AtomicU64::new(0));
                let backpressure = Arc::new((Mutex::new(()), Condvar::new()));
                let sender = WriteSender::SizeBased {
                    tx,
                    queued_bytes: Arc::clone(&queued_bytes),
                    max_bytes: *max_bytes,
                    backpressure: Arc::clone(&backpressure),
                };
                (sender, rx, Some((queued_bytes, backpressure)))
            }
        };
        let shutdown_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Determine mmap cache size (use default if 0)
        let mmap_size = if config.mmap_cache_size > 0 {
            config.mmap_cache_size
        } else {
            DEFAULT_MMAP_CACHE_SIZE
        };

        let manifest_dirty = Arc::new(std::sync::atomic::AtomicBool::new(false));

        let cache = Arc::new(Self {
            config: config.clone(),
            manifest: RwLock::new(manifest),
            split_states: RwLock::new(split_states),
            lru_table: Mutex::new(SplitLruTable::new()),
            mmap_cache: Mutex::new(MmapCache::new(mmap_size)),
            write_tx,
            total_bytes: AtomicU64::new(total_bytes),
            max_bytes,
            shutdown_flag: Arc::clone(&shutdown_flag),
            thread_handles: Mutex::new(Vec::new()),
            manifest_dirty: Arc::clone(&manifest_dirty),
        });

        // Start background writer (uses Weak reference - doesn't prevent Drop)
        let cache_weak = Arc::downgrade(&cache);
        let writer_handle = std::thread::spawn(move || {
            Self::background_writer_static(write_rx, cache_weak, size_based_state);
        });

        if let Ok(mut handles) = cache.thread_handles.lock() {
            handles.push(writer_handle);
        }

        // Start manifest sync timer - checks every second, syncs if dirty
        {
            let shutdown_flag_clone = Arc::clone(&shutdown_flag);
            let cache_weak = Arc::downgrade(&cache);
            let dirty_flag = Arc::clone(&manifest_dirty);

            let timer_handle = std::thread::spawn(move || {
                Self::manifest_sync_timer_static(cache_weak, shutdown_flag_clone, dirty_flag);
            });

            if let Ok(mut handles) = cache.thread_handles.lock() {
                handles.push(timer_handle);
            }
        }

        Ok(cache)
    }

    /// Load manifest from disk or create new one
    fn load_manifest(root_path: &std::path::Path) -> io::Result<CacheManifest> {
        let manifest_path = root_path.join("manifest.json");
        let backup_path = root_path.join("manifest.json.bak");

        // Try primary manifest
        if manifest_path.exists() {
            match Self::read_manifest_file(&manifest_path) {
                Ok(manifest) => return Ok(manifest),
                Err(e) => {
                    debug_println!("Failed to read manifest, trying backup: {}", e);
                }
            }
        }

        // Try backup
        if backup_path.exists() {
            match Self::read_manifest_file(&backup_path) {
                Ok(manifest) => {
                    // Restore backup
                    if let Err(e) = fs::copy(&backup_path, &manifest_path) {
                        debug_println!("Failed to restore backup manifest: {}", e);
                    }
                    return Ok(manifest);
                }
                Err(e) => {
                    debug_println!("Failed to read backup manifest: {}", e);
                }
            }
        }

        // Create new manifest
        Ok(CacheManifest::new())
    }

    fn read_manifest_file(path: &std::path::Path) -> io::Result<CacheManifest> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        serde_json::from_str(&contents)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Get cached data (synchronous read from disk)
    pub fn get(
        &self,
        storage_loc: &str,
        split_id: &str,
        component: &str,
        byte_range: Option<Range<u64>>,
    ) -> Option<OwnedBytes> {
        get_ops::get(
            &self.config,
            &self.manifest,
            &self.mmap_cache,
            &self.lru_table,
            storage_loc,
            split_id,
            component,
            byte_range,
        )
    }

    /// Check if data exists in the cache WITHOUT reading or copying it.
    ///
    /// This is a lightweight O(1) manifest lookup. Use this for existence checks
    /// instead of `get().is_some()` which performs expensive file I/O.
    ///
    /// # Performance
    /// - `exists()`: ~1μs (manifest lookup only)
    /// - `get().is_some()`: ~50-100ms for large files (opens file, mmaps, copies entire contents)
    pub fn exists(
        &self,
        storage_loc: &str,
        split_id: &str,
        component: &str,
        byte_range: Option<Range<u64>>,
    ) -> bool {
        get_ops::exists(
            &self.manifest,
            storage_loc,
            split_id,
            component,
            byte_range,
        )
    }

    /// Get a sub-range from a cached file using mmap (fast path for random access).
    pub fn get_subrange(
        &self,
        storage_loc: &str,
        split_id: &str,
        component: &str,
        cached_range: Range<u64>,
        requested_range: Range<u64>,
    ) -> Option<OwnedBytes> {
        get_ops::get_subrange(
            &self.config,
            &self.manifest,
            &self.mmap_cache,
            storage_loc,
            split_id,
            component,
            cached_range,
            requested_range,
        )
    }

    /// Get cached data with range coalescing
    pub fn get_coalesced(
        &self,
        storage_loc: &str,
        split_id: &str,
        component: &str,
        requested_range: Range<u64>,
    ) -> CoalesceResult {
        get_ops::get_coalesced(
            &self.config,
            &self.manifest,
            &self.split_states,
            &self.mmap_cache,
            &self.lru_table,
            storage_loc,
            split_id,
            component,
            requested_range,
        )
    }

    /// Cache data (async write via background thread).
    /// Blocks if the write queue is full (backpressure).
    /// Use this for prewarm operations where data must be written.
    pub fn put(
        &self,
        storage_loc: &str,
        split_id: &str,
        component: &str,
        byte_range: Option<Range<u64>>,
        data: &[u8],
    ) {
        // Check if we need to evict before adding
        let current = self.total_bytes.load(Ordering::Relaxed);
        let new_size = data.len() as u64;

        // Trigger eviction if we'd exceed 95% capacity
        if current + new_size > (self.max_bytes * 95) / 100 {
            self.trigger_eviction((self.max_bytes * 90) / 100);
        }

        // Send to background writer with backpressure.
        let _ = self.write_tx.send(WriteRequest::Put {
            storage_loc: storage_loc.to_string(),
            split_id: split_id.to_string(),
            component: component.to_string(),
            byte_range: byte_range.map(|r| r.start..r.end),
            data: data.to_vec(),
        });
    }

    /// Cache data if the write queue has capacity, otherwise drop silently.
    /// Returns `true` if the write was enqueued, `false` if dropped.
    /// Use this for query-path opportunistic caching where dropping is acceptable.
    pub fn put_if_ready(
        &self,
        storage_loc: &str,
        split_id: &str,
        component: &str,
        byte_range: Option<Range<u64>>,
        data: &[u8],
    ) -> bool {
        let current = self.total_bytes.load(Ordering::Relaxed);
        let new_size = data.len() as u64;

        if current + new_size > (self.max_bytes * 95) / 100 {
            self.trigger_eviction((self.max_bytes * 90) / 100);
        }

        self.write_tx.send_or_drop(WriteRequest::Put {
            storage_loc: storage_loc.to_string(),
            split_id: split_id.to_string(),
            component: component.to_string(),
            byte_range: byte_range.map(|r| r.start..r.end),
            data: data.to_vec(),
        })
    }

    /// Whether non-prewarm writes should be dropped when the queue is full.
    pub fn drop_writes_when_full(&self) -> bool {
        self.config.drop_writes_when_full
    }

    /// Cache data for the query path — blocks or drops depending on config.
    /// When `drop_writes_when_full` is enabled, silently drops writes if the queue is full.
    /// When disabled, behaves identically to `put()` (blocks until enqueued).
    pub fn put_query_path(
        &self,
        storage_loc: &str,
        split_id: &str,
        component: &str,
        byte_range: Option<Range<u64>>,
        data: &[u8],
    ) {
        if self.config.drop_writes_when_full {
            self.put_if_ready(storage_loc, split_id, component, byte_range, data);
        } else {
            self.put(storage_loc, split_id, component, byte_range, data);
        }
    }

    /// Evict a split from cache
    pub fn evict_split(&self, storage_loc: &str, split_id: &str) {
        let _ = self.write_tx.send(WriteRequest::Evict {
            storage_loc: storage_loc.to_string(),
            split_id: split_id.to_string(),
        });
    }

    /// Trigger eviction to reach target bytes
    fn trigger_eviction(&self, target_bytes: u64) {
        let current = self.total_bytes.load(Ordering::Relaxed);
        if current <= target_bytes {
            return;
        }

        let candidates = {
            let lru = self.lru_table.lock().unwrap();
            lru.get_eviction_candidates(current, target_bytes)
        };

        for key in candidates {
            // Parse storage_loc/split_id from key
            if let Some((storage_loc, split_id)) = key.split_once('/') {
                self.evict_split(storage_loc, split_id);
            }
        }
    }

    /// Sync manifest to disk
    pub fn sync_manifest(&self) -> io::Result<()> {
        let _ = self.write_tx.send(WriteRequest::SyncManifest);
        Ok(())
    }

    /// Mark manifest as dirty (has uncommitted changes)
    pub fn mark_dirty(&self) {
        self.manifest_dirty.store(true, Ordering::Release);
    }

    /// Check if manifest is dirty and clear the flag atomically
    /// Returns true if it was dirty
    pub fn check_and_clear_dirty(&self) -> bool {
        self.manifest_dirty.swap(false, Ordering::AcqRel)
    }

    /// Flush all pending writes to disk and wait for completion (async version).
    pub async fn flush_async(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.write_tx.send(WriteRequest::Flush(tx)).is_ok() {
            let _ = rx.await;
        }
    }

    /// Flush all pending writes to disk and wait for completion (blocking version).
    pub fn flush_blocking(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.write_tx.send(WriteRequest::Flush(tx)).is_ok() {
            let _ = rx.blocking_recv();
        }
    }

    /// Actually write data to disk (called by background writer)
    fn do_put(
        &self,
        storage_loc: &str,
        split_id: &str,
        component: &str,
        byte_range: Option<Range<u64>>,
        data: &[u8],
    ) -> io::Result<()> {
        write_ops::do_put(
            &self.config,
            &self.manifest,
            &self.split_states,
            &self.lru_table,
            &self.total_bytes,
            storage_loc,
            split_id,
            component,
            byte_range,
            data,
        )
    }

    /// Actually evict a split from disk (called by background writer)
    fn do_evict(&self, storage_loc: &str, split_id: &str) -> io::Result<()> {
        write_ops::do_evict(
            &self.config,
            &self.manifest,
            &self.split_states,
            &self.lru_table,
            &self.total_bytes,
            storage_loc,
            split_id,
        )
    }

    /// Async version of do_put using tokio::fs for parallel I/O (called by background writer)
    pub(crate) async fn do_put_async(
        &self,
        storage_loc: &str,
        split_id: &str,
        component: &str,
        byte_range: Option<Range<u64>>,
        data: &[u8],
    ) -> io::Result<()> {
        write_ops::do_put_async(
            &self.config,
            &self.manifest,
            &self.split_states,
            &self.lru_table,
            &self.total_bytes,
            storage_loc,
            split_id,
            component,
            byte_range,
            data,
        )
        .await
    }

    /// Async version of do_evict using tokio::fs (called by background writer)
    pub(crate) async fn do_evict_async(&self, storage_loc: &str, split_id: &str) -> io::Result<()> {
        write_ops::do_evict_async(
            &self.config,
            &self.manifest,
            &self.split_states,
            &self.lru_table,
            &self.total_bytes,
            storage_loc,
            split_id,
        )
        .await
    }

    /// Actually sync manifest to disk (called by background writer)
    fn do_sync_manifest(&self) -> io::Result<()> {
        write_ops::do_sync_manifest(&self.config, &self.manifest)
    }

    /// Get cache statistics
    pub fn stats(&self) -> DiskCacheStats {
        let manifest = self.manifest.read().unwrap();
        DiskCacheStats {
            total_bytes: self.total_bytes.load(Ordering::Relaxed),
            max_bytes: self.max_bytes,
            split_count: manifest.splits.len(),
            component_count: manifest.splits.values().map(|s| s.components.len()).sum(),
        }
    }

    /// Get total bytes currently cached on disk
    pub fn get_total_bytes(&self) -> u64 {
        self.total_bytes.load(Ordering::Relaxed)
    }

    /// Get number of splits in cache
    pub fn get_split_count(&self) -> usize {
        let manifest = self.manifest.read().unwrap();
        manifest.splits.len()
    }

    /// Get total number of cached components across all splits
    pub fn get_component_count(&self) -> usize {
        let manifest = self.manifest.read().unwrap();
        manifest.splits.values().map(|s| s.components.len()).sum()
    }
}

impl Drop for L2DiskCache {
    fn drop(&mut self) {
        debug_println!("🔄 L2DiskCache::drop() - Starting cleanup");

        // Signal shutdown to timer thread
        self.shutdown_flag.store(true, Ordering::SeqCst);

        // Signal shutdown to writer thread and sync manifest first.
        let _ = self.write_tx.try_send(WriteRequest::SyncManifest);
        let _ = self.write_tx.try_send(WriteRequest::Shutdown);

        // Wait for background threads to process shutdown
        std::thread::sleep(Duration::from_millis(250));

        debug_println!("🔄 L2DiskCache::drop() - Cleanup complete");
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct DiskCacheStats {
    pub total_bytes: u64,
    pub max_bytes: u64,
    pub split_count: usize,
    pub component_count: usize,
}

#[allow(dead_code)]
impl DiskCacheStats {
    pub fn usage_percent(&self) -> f64 {
        if self.max_bytes == 0 {
            0.0
        } else {
            (self.total_bytes as f64 / self.max_bytes as f64) * 100.0
        }
    }
}
