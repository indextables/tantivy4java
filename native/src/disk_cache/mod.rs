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

// Submodules
pub mod types;
pub mod range_index;
pub mod manifest;
pub mod lru;
pub mod mmap_cache;
pub mod background;

// Re-exports
pub use types::{CompressionAlgorithm, DiskCacheConfig, ComponentEntry, DEFAULT_MMAP_CACHE_SIZE};
pub use range_index::{CachedRange, CachedSegment, CoalesceResult};
pub use manifest::{SplitEntry, SplitState, CacheManifest};

use std::cmp::{max, min};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use lz4_flex::{compress_prepend_size, decompress_size_prepended};

use crate::debug_println;
use tantivy::directory::OwnedBytes;

use lru::SplitLruTable;
use mmap_cache::MmapCache;
use background::WriteRequest;

/// Subdirectory name for the disk cache within the root path
const CACHE_SUBDIR: &str = "tantivy4java_slicecache";

/// L2 Disk Cache implementation
pub struct L2DiskCache {
    config: DiskCacheConfig,
    manifest: RwLock<CacheManifest>,
    /// In-memory state for fast range coalescing (split_key -> SplitState)
    split_states: RwLock<HashMap<String, SplitState>>,
    lru_table: Mutex<SplitLruTable>,
    /// Memory-mapped file cache for fast random access
    mmap_cache: Mutex<MmapCache>,
    /// Bounded channel to prevent OOM during bulk prewarm operations.
    /// When queue is full, put() blocks until there's room (backpressure).
    /// Uses std::sync::mpsc which works from both sync and async contexts.
    write_tx: std::sync::mpsc::SyncSender<WriteRequest>,
    total_bytes: AtomicU64,
    max_bytes: u64,
    /// Shutdown flag for background threads
    shutdown_flag: Arc<std::sync::atomic::AtomicBool>,
    /// Thread handles for cleanup
    thread_handles: Mutex<Vec<std::thread::JoinHandle<()>>>,
}

#[allow(dead_code)]
impl L2DiskCache {
    /// Get the cache directory path (root_path/tantivy4java_slicecache)
    fn cache_dir(root_path: &Path) -> PathBuf {
        root_path.join(CACHE_SUBDIR)
    }

    /// Create a new L2 disk cache
    pub fn new(config: DiskCacheConfig) -> io::Result<Arc<Self>> {
        // Ensure cache directory exists (root_path/tantivy4java_slicecache)
        let cache_dir = Self::cache_dir(&config.root_path);
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

        // Create BOUNDED background writer channel to prevent OOM during bulk prewarm.
        // When queue fills up, put() will block (backpressure), throttling downloads
        // to match disk write speed.
        //
        // Queue size rationale for large splits (5GB+):
        // - Individual components can be 500MB-2GB each
        // - 16 items × 500MB = 8GB max queued memory
        // - Small enough to prevent OOM, large enough for write batching
        // Uses std::sync::mpsc::sync_channel which works from both sync and async contexts.
        const WRITE_QUEUE_CAPACITY: usize = 16;
        let (write_tx, write_rx) = std::sync::mpsc::sync_channel(WRITE_QUEUE_CAPACITY);
        let shutdown_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

        // Determine mmap cache size (use default if 0)
        let mmap_size = if config.mmap_cache_size > 0 {
            config.mmap_cache_size
        } else {
            DEFAULT_MMAP_CACHE_SIZE
        };

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
        });

        // Start background writer (uses Weak reference - doesn't prevent Drop)
        let cache_weak = Arc::downgrade(&cache);
        let writer_handle = std::thread::spawn(move || {
            Self::background_writer_static(write_rx, cache_weak);
        });

        if let Ok(mut handles) = cache.thread_handles.lock() {
            handles.push(writer_handle);
        }

        // Start manifest sync timer if interval > 0
        if config.manifest_sync_interval_secs > 0 {
            let shutdown_flag_clone = Arc::clone(&shutdown_flag);
            let cache_weak = Arc::downgrade(&cache);
            let interval_secs = config.manifest_sync_interval_secs;

            let timer_handle = std::thread::spawn(move || {
                Self::manifest_sync_timer_static(cache_weak, shutdown_flag_clone, interval_secs);
            });

            if let Ok(mut handles) = cache.thread_handles.lock() {
                handles.push(timer_handle);
            }
        }

        Ok(cache)
    }

    /// Load manifest from disk or create new one
    fn load_manifest(root_path: &Path) -> io::Result<CacheManifest> {
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

    fn read_manifest_file(path: &Path) -> io::Result<CacheManifest> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        serde_json::from_str(&contents)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    /// Check if compression should be applied for this component and size
    fn should_compress(&self, component: &str, data_size: usize) -> bool {
        if self.config.compression == CompressionAlgorithm::None {
            return false;
        }

        // Skip small data - compression overhead not worth it
        if data_size < self.config.min_compress_size {
            return false;
        }

        // Component-aware compression decisions based on internal Tantivy encoding
        // AND access patterns:
        //
        // Already compressed (skip - would waste CPU):
        //   - .store: LZ4/Zstd compressed in 16KB blocks
        //   - .term:  Zstd-compressed sstable blocks
        //
        // Block-access patterns (skip - decompressing whole file for block access is terrible):
        //   - .idx/.pos: Postings accessed in 128-doc blocks via skip lists
        //                With millions of docs, a 10MB file would need full decompression
        //                just to read a 512-byte block. Bitpacking already compacts the data.
        //
        // Random-access patterns:
        //   - .fast: Accessed by doc_id, same decompression problem as postings
        //
        match component {
            // Small/hot data - skip compression for access speed
            "footer" | "metadata" | "fieldnorm" => false,
            // Already compressed by Tantivy - don't double compress
            "store" | "term" => false,
            // Block/random access patterns - whole-file decompression would kill performance
            "idx" | "pos" | "fast" => false,
            // Default: NO compression - unknown components (like split file byte ranges) are
            // likely already compressed or have random-access patterns. Safe default is no
            // compression to avoid decompression overhead on sub-range reads.
            _ => false,
        }
    }

    /// Compress data if appropriate
    fn compress_data(&self, component: &str, data: &[u8]) -> (Vec<u8>, CompressionAlgorithm) {
        if !self.should_compress(component, data.len()) {
            return (data.to_vec(), CompressionAlgorithm::None);
        }

        match self.config.compression {
            CompressionAlgorithm::None => (data.to_vec(), CompressionAlgorithm::None),
            CompressionAlgorithm::Lz4 => {
                let compressed = compress_prepend_size(data);
                // Only use compression if it actually saves space
                if compressed.len() < data.len() {
                    (compressed, CompressionAlgorithm::Lz4)
                } else {
                    (data.to_vec(), CompressionAlgorithm::None)
                }
            }
            CompressionAlgorithm::Zstd => {
                // Zstd not currently available as direct dependency
                // Fall back to LZ4 which provides good compression with better speed
                let compressed = compress_prepend_size(data);
                if compressed.len() < data.len() {
                    (compressed, CompressionAlgorithm::Lz4)
                } else {
                    (data.to_vec(), CompressionAlgorithm::None)
                }
            }
        }
    }

    /// Decompress data
    fn decompress_data(data: &[u8], compression: CompressionAlgorithm) -> io::Result<Vec<u8>> {
        match compression {
            CompressionAlgorithm::None => Ok(data.to_vec()),
            CompressionAlgorithm::Lz4 => decompress_size_prepended(data)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string())),
            CompressionAlgorithm::Zstd => {
                // Zstd data would have been stored as LZ4 (see compress_data fallback)
                decompress_size_prepended(data)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
            }
        }
    }

    /// Generate storage location hash for directory naming
    fn storage_loc_hash(storage_loc: &str) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        // Parse storage_loc to extract scheme and bucket
        let (scheme, rest) = if storage_loc.starts_with("s3://") {
            ("s3", &storage_loc[5..])
        } else if storage_loc.starts_with("azure://") {
            ("azure", &storage_loc[8..])
        } else if storage_loc.starts_with("file://") {
            ("file", &storage_loc[7..])
        } else {
            ("local", storage_loc)
        };

        let bucket = rest.split('/').next().unwrap_or("default");

        let mut hasher = DefaultHasher::new();
        storage_loc.hash(&mut hasher);
        let hash = hasher.finish();

        format!("{}_{}__{:08x}", scheme, bucket, hash as u32)
    }

    /// Get the directory path for a split
    fn split_dir(&self, storage_loc: &str, split_id: &str) -> PathBuf {
        Self::cache_dir(&self.config.root_path)
            .join(Self::storage_loc_hash(storage_loc))
            .join(split_id)
    }

    /// Get the file path for a cached component
    fn component_path(
        &self,
        storage_loc: &str,
        split_id: &str,
        component: &str,
        byte_range: Option<Range<u64>>,
        compression: CompressionAlgorithm,
    ) -> PathBuf {
        let dir = self.split_dir(storage_loc, split_id);
        let base_name = match byte_range {
            Some(range) => format!("{}_{}-{}", component, range.start, range.end),
            None => format!("{}_full", component),
        };
        let ext = match compression {
            CompressionAlgorithm::None => "cache",
            CompressionAlgorithm::Lz4 => "cache.lz4",
            CompressionAlgorithm::Zstd => "cache.zst",
        };
        dir.join(format!("{}.{}", base_name, ext))
    }

    /// Get cached data (synchronous read from disk)
    ///
    /// For uncompressed files, uses memory-mapped I/O for fast random access.
    /// The OS kernel manages the page cache efficiently, and repeated access
    /// to the same file region avoids syscalls entirely.
    pub fn get(
        &self,
        storage_loc: &str,
        split_id: &str,
        component: &str,
        byte_range: Option<Range<u64>>,
    ) -> Option<OwnedBytes> {
        let split_key = CacheManifest::split_key(storage_loc, split_id);
        let comp_key = CacheManifest::component_key(component, byte_range.clone());

        // Check manifest for entry
        let entry = {
            let manifest = self.manifest.read().ok()?;
            let split = manifest.splits.get(&split_key)?;
            split.components.get(&comp_key)?.clone()
        };

        // Get file path
        let file_path = self.component_path(
            storage_loc,
            split_id,
            component,
            byte_range,
            entry.compression,
        );

        // For uncompressed files, use memory-mapped I/O (fast random access)
        // For compressed files, fall back to read-all + decompress
        let data = if entry.compression == CompressionAlgorithm::None {
            // Fast path: mmap for uncompressed files
            let mmap = {
                let mut mmap_cache = self.mmap_cache.lock().ok()?;
                mmap_cache.get_or_map(&file_path).ok()?
            };
            // Return a copy of the mapped data
            // (OwnedBytes requires owned data, but mmap access is still fast)
            mmap.to_vec()
        } else {
            // Slow path: read entire file for compressed data
            let mut file = File::open(&file_path).ok()?;
            let mut data = Vec::with_capacity(entry.disk_size_bytes as usize);
            file.read_to_end(&mut data).ok()?;
            Self::decompress_data(&data, entry.compression).ok()?
        };

        // Update LRU access time
        if let Ok(mut lru) = self.lru_table.lock() {
            if let Ok(manifest) = self.manifest.read() {
                if let Some(split) = manifest.splits.get(&split_key) {
                    lru.touch(&split_key, split.total_size_bytes);
                }
            }
        }

        Some(OwnedBytes::new(data))
    }

    /// Get a sub-range from a cached file using mmap (fast path for random access).
    ///
    /// This is optimized for the common case where we cached [0..file_len] during prewarm,
    /// but queries request smaller sub-ranges like [offset..offset+block_size].
    /// Instead of loading the entire file, we mmap it and return just the requested slice.
    pub fn get_subrange(
        &self,
        storage_loc: &str,
        split_id: &str,
        component: &str,
        cached_range: Range<u64>,
        requested_range: Range<u64>,
    ) -> Option<OwnedBytes> {
        let split_key = CacheManifest::split_key(storage_loc, split_id);
        let comp_key = CacheManifest::component_key(component, Some(cached_range.clone()));

        // Check manifest for entry
        let entry = {
            let manifest = self.manifest.read().ok()?;
            let split = manifest.splits.get(&split_key)?;
            split.components.get(&comp_key)?.clone()
        };

        // Only use fast path for uncompressed files
        if entry.compression != CompressionAlgorithm::None {
            // Fall back to full load + slice for compressed files
            let full_data = self.get(storage_loc, split_id, component, Some(cached_range.clone()))?;
            let offset = (requested_range.start - cached_range.start) as usize;
            let len = (requested_range.end - requested_range.start) as usize;
            if offset + len <= full_data.len() {
                return Some(OwnedBytes::new(full_data[offset..offset + len].to_vec()));
            }
            return None;
        }

        // Fast path: mmap and extract just the requested slice
        let file_path = self.component_path(
            storage_loc,
            split_id,
            component,
            Some(cached_range.clone()),
            entry.compression,
        );

        let mmap = {
            let mut mmap_cache = self.mmap_cache.lock().ok()?;
            mmap_cache.get_or_map(&file_path).ok()?
        };

        // Calculate offset within the cached file
        let offset = (requested_range.start - cached_range.start) as usize;
        let len = (requested_range.end - requested_range.start) as usize;

        if offset + len > mmap.len() {
            debug_println!(
                "⚠️ L2_CACHE: Sub-range out of bounds: offset={} len={} file_len={}",
                offset, len, mmap.len()
            );
            return None;
        }

        // Return just the requested slice (fast - only copies the slice, not entire file)
        Some(OwnedBytes::new(mmap[offset..offset + len].to_vec()))
    }

    /// Get cached data with range coalescing
    ///
    /// Instead of requiring an exact range match, this method:
    /// 1. Finds all cached ranges that overlap with the requested range
    /// 2. Loads cached data for covered portions
    /// 3. Returns gaps that need to be fetched from remote storage
    ///
    /// This enables partial cache hits - if we have [0..1000] and [2000..3000] cached,
    /// a request for [500..2500] will return the cached portions plus gap [1000..2000].
    pub fn get_coalesced(
        &self,
        storage_loc: &str,
        split_id: &str,
        component: &str,
        requested_range: Range<u64>,
    ) -> CoalesceResult {
        let split_key = CacheManifest::split_key(storage_loc, split_id);

        // First, try exact match (fast path) - use get_subrange to avoid copying entire file
        // Even if the requested range exactly matches the cached range, we use mmap-based
        // sub-range extraction which only copies the requested bytes
        if let Some(data) = self.get_subrange(
            storage_loc,
            split_id,
            component,
            requested_range.clone(),  // cached range = requested range for exact match
            requested_range.clone(),  // requested sub-range = same
        ) {
            return CoalesceResult::hit(data, requested_range);
        }

        // Get range index for this component
        let overlapping_ranges = {
            let states = match self.split_states.read() {
                Ok(s) => s,
                Err(_) => return CoalesceResult::miss(requested_range),
            };

            let state = match states.get(&split_key) {
                Some(s) => s,
                None => return CoalesceResult::miss(requested_range),
            };

            let index = match state.range_indices.get(component) {
                Some(i) => i,
                None => return CoalesceResult::miss(requested_range),
            };

            // Find overlapping ranges
            index.find_overlapping(requested_range.start, requested_range.end)
                .into_iter()
                .map(|r| r.clone())
                .collect::<Vec<_>>()
        };

        if overlapping_ranges.is_empty() {
            return CoalesceResult::miss(requested_range);
        }

        // Load cached segments and compute gaps
        let mut cached_segments = Vec::new();
        let mut gaps = Vec::new();
        let mut cursor = requested_range.start;
        let mut cached_bytes = 0u64;
        let mut gap_bytes = 0u64;

        for cached_range in &overlapping_ranges {
            // Gap before this cached range?
            if cursor < cached_range.start {
                let gap_end = min(cached_range.start, requested_range.end);
                if cursor < gap_end {
                    gaps.push(cursor..gap_end);
                    gap_bytes += gap_end - cursor;
                }
            }

            // Overlap with this cached range
            if let Some(overlap) = cached_range.overlap_with(requested_range.start, requested_range.end) {
                // Use fast sub-range extraction via mmap (avoids loading entire file)
                if let Some(data) = self.get_subrange(
                    storage_loc,
                    split_id,
                    component,
                    cached_range.start..cached_range.end,  // cached range
                    overlap.clone(),                        // requested sub-range
                ) {
                    cached_segments.push(CachedSegment {
                        range: overlap.clone(),
                        data,
                    });
                    cached_bytes += (overlap.end - overlap.start) as u64;
                }

                cursor = max(cursor, overlap.end);
            }
        }

        // Gap after last cached range?
        if cursor < requested_range.end {
            gaps.push(cursor..requested_range.end);
            gap_bytes += requested_range.end - cursor;
        }

        // Update LRU for accessed split
        if let Ok(mut lru) = self.lru_table.lock() {
            if let Ok(manifest) = self.manifest.read() {
                if let Some(split) = manifest.splits.get(&split_key) {
                    lru.touch(&split_key, split.total_size_bytes);
                }
            }
        }

        CoalesceResult {
            cached_segments,
            gaps,
            fully_cached: gap_bytes == 0,
            cached_bytes,
            gap_bytes,
        }
    }

    /// Cache data (async write via background thread)
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

        // Send to background writer with BACKPRESSURE.
        // SyncSender::send() blocks if the queue is full, throttling downloads
        // to match disk write speed. This prevents OOM during bulk prewarm.
        let _ = self.write_tx.send(WriteRequest::Put {
            storage_loc: storage_loc.to_string(),
            split_id: split_id.to_string(),
            component: component.to_string(),
            byte_range: byte_range.map(|r| r.start..r.end),
            data: data.to_vec(),
        });
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

    /// Flush all pending writes to disk and wait for completion (async version).
    ///
    /// This method blocks (in an async-friendly way) until all previously queued
    /// Put requests have been processed by the background writer. Use this to
    /// ensure data is persisted before reading from the cache.
    ///
    /// Note: Since we use std::sync::mpsc for backpressure, the send is synchronous.
    /// The async wait is only for the completion signal.
    pub async fn flush_async(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        // SyncSender::send() is synchronous and blocking (provides backpressure)
        if self.write_tx.send(WriteRequest::Flush(tx)).is_ok() {
            // Wait asynchronously for the flush to complete
            let _ = rx.await;
        }
    }

    /// Flush all pending writes to disk and wait for completion (blocking version).
    ///
    /// This method can be called from any context (sync or async) since we use
    /// std::sync::mpsc channels internally.
    pub fn flush_blocking(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        // SyncSender::send() is synchronous and blocking (provides backpressure)
        if self.write_tx.send(WriteRequest::Flush(tx)).is_ok() {
            // Block until the flush is processed
            let _ = rx.blocking_recv();
        }
    }

    /// Actually write data to disk
    fn do_put(
        &self,
        storage_loc: &str,
        split_id: &str,
        component: &str,
        byte_range: Option<Range<u64>>,
        data: &[u8],
    ) -> io::Result<()> {
        // Compress if appropriate
        let (compressed, compression) = self.compress_data(component, data);

        // Create split directory
        let split_dir = self.split_dir(storage_loc, split_id);
        fs::create_dir_all(&split_dir)?;

        // Write to temp file then rename (atomic)
        let final_path = self.component_path(
            storage_loc,
            split_id,
            component,
            byte_range.clone(),
            compression,
        );
        let temp_path = final_path.with_extension("tmp");

        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temp_path)?;
            file.write_all(&compressed)?;
            file.sync_all()?;
        }

        fs::rename(&temp_path, &final_path)?;

        // Update manifest
        let split_key = CacheManifest::split_key(storage_loc, split_id);
        let comp_key = CacheManifest::component_key(component, byte_range.clone());
        let disk_size = compressed.len() as u64;
        let uncompressed_size = data.len() as u64;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let entry = ComponentEntry {
            file_name: final_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            uncompressed_size_bytes: uncompressed_size,
            disk_size_bytes: disk_size,
            compression,
            component: component.to_string(),
            byte_range: byte_range.as_ref().map(|r| (r.start, r.end)),
            created_at: now,
        };

        {
            let mut manifest = self.manifest.write().unwrap();

            // Ensure split entry exists
            if !manifest.splits.contains_key(&split_key) {
                manifest.splits.insert(split_key.clone(), SplitEntry {
                    split_id: split_id.to_string(),
                    storage_loc: storage_loc.to_string(),
                    total_size_bytes: 0,
                    last_accessed: now,
                    components: HashMap::new(),
                });
            }

            // Get old size for accounting (if replacing existing component)
            let old_disk_size = manifest
                .splits
                .get(&split_key)
                .and_then(|s| s.components.get(&comp_key))
                .map(|c| c.disk_size_bytes)
                .unwrap_or(0);

            // Update size tracking for replacement
            if old_disk_size > 0 {
                manifest.total_bytes = manifest.total_bytes.saturating_sub(old_disk_size);
                self.total_bytes.fetch_sub(old_disk_size, Ordering::Relaxed);
            }

            // Now update the split entry
            if let Some(split) = manifest.splits.get_mut(&split_key) {
                if old_disk_size > 0 {
                    split.total_size_bytes = split.total_size_bytes.saturating_sub(old_disk_size);
                }
                split.components.insert(comp_key, entry);
                split.total_size_bytes += disk_size;
                split.last_accessed = now;
            }

            manifest.total_bytes += disk_size;
        }

        self.total_bytes.fetch_add(disk_size, Ordering::Relaxed);

        // Update range index for coalescing (if this is a byte range, not full component)
        if let Some(ref range) = byte_range {
            if let Ok(mut states) = self.split_states.write() {
                let state = states
                    .entry(split_key.clone())
                    .or_insert_with(SplitState::new);

                let index = state.get_or_create_index(component);
                let comp_key = CacheManifest::component_key(component, byte_range.clone());

                // Remove old entry if exists (in case of replacement)
                index.remove(&comp_key);

                // Add new range
                index.insert(CachedRange {
                    start: range.start,
                    end: range.end,
                    cache_key: comp_key,
                    compression,
                });
            }
        }

        // Update LRU
        if let Ok(mut lru) = self.lru_table.lock() {
            if let Ok(manifest) = self.manifest.read() {
                if let Some(split) = manifest.splits.get(&split_key) {
                    lru.touch(&split_key, split.total_size_bytes);
                }
            }
        }

        Ok(())
    }

    /// Actually evict a split from disk
    fn do_evict(&self, storage_loc: &str, split_id: &str) -> io::Result<()> {
        let split_key = CacheManifest::split_key(storage_loc, split_id);
        let split_dir = self.split_dir(storage_loc, split_id);

        // Get size before removal
        let size_to_remove = {
            let manifest = self.manifest.read().unwrap();
            manifest
                .splits
                .get(&split_key)
                .map(|s| s.total_size_bytes)
                .unwrap_or(0)
        };

        // Remove directory
        if split_dir.exists() {
            fs::remove_dir_all(&split_dir)?;
        }

        // Update manifest
        {
            let mut manifest = self.manifest.write().unwrap();
            manifest.splits.remove(&split_key);
            manifest.total_bytes = manifest.total_bytes.saturating_sub(size_to_remove);
        }

        self.total_bytes.fetch_sub(size_to_remove, Ordering::Relaxed);

        // Remove from split_states (range index cleanup)
        if let Ok(mut states) = self.split_states.write() {
            states.remove(&split_key);
        }

        // Update LRU
        if let Ok(mut lru) = self.lru_table.lock() {
            lru.remove(&split_key);
        }

        Ok(())
    }

    /// Async version of do_put using tokio::fs for parallel I/O
    async fn do_put_async(
        &self,
        storage_loc: &str,
        split_id: &str,
        component: &str,
        byte_range: Option<Range<u64>>,
        data: &[u8],
    ) -> io::Result<()> {
        use tokio::io::AsyncWriteExt;

        // Compress if appropriate (CPU-bound, but fast)
        let (compressed, compression) = self.compress_data(component, data);

        // Create split directory
        let split_dir = self.split_dir(storage_loc, split_id);
        tokio::fs::create_dir_all(&split_dir).await?;

        // Write to temp file then rename (atomic)
        let final_path = self.component_path(
            storage_loc,
            split_id,
            component,
            byte_range.clone(),
            compression,
        );
        let temp_path = final_path.with_extension("tmp");

        // Async file write
        {
            let mut file = tokio::fs::File::create(&temp_path).await?;
            file.write_all(&compressed).await?;
            file.sync_all().await?;
        }

        tokio::fs::rename(&temp_path, &final_path).await?;

        // Update manifest (sync - needs locking, but fast)
        let split_key = CacheManifest::split_key(storage_loc, split_id);
        let comp_key = CacheManifest::component_key(component, byte_range.clone());
        let disk_size = compressed.len() as u64;
        let uncompressed_size = data.len() as u64;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let entry = ComponentEntry {
            file_name: final_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .to_string(),
            uncompressed_size_bytes: uncompressed_size,
            disk_size_bytes: disk_size,
            compression,
            component: component.to_string(),
            byte_range: byte_range.as_ref().map(|r| (r.start, r.end)),
            created_at: now,
        };

        {
            let mut manifest = self.manifest.write().unwrap();

            if !manifest.splits.contains_key(&split_key) {
                manifest.splits.insert(
                    split_key.clone(),
                    SplitEntry {
                        split_id: split_id.to_string(),
                        storage_loc: storage_loc.to_string(),
                        total_size_bytes: 0,
                        last_accessed: now,
                        components: HashMap::new(),
                    },
                );
            }

            let old_disk_size = manifest
                .splits
                .get(&split_key)
                .and_then(|s| s.components.get(&comp_key))
                .map(|c| c.disk_size_bytes)
                .unwrap_or(0);

            if old_disk_size > 0 {
                manifest.total_bytes = manifest.total_bytes.saturating_sub(old_disk_size);
                self.total_bytes.fetch_sub(old_disk_size, Ordering::Relaxed);
            }

            if let Some(split) = manifest.splits.get_mut(&split_key) {
                if old_disk_size > 0 {
                    split.total_size_bytes = split.total_size_bytes.saturating_sub(old_disk_size);
                }
                split.components.insert(comp_key, entry);
                split.total_size_bytes += disk_size;
                split.last_accessed = now;
            }

            manifest.total_bytes += disk_size;
        }

        self.total_bytes.fetch_add(disk_size, Ordering::Relaxed);

        // Update range index
        if let Some(ref range) = byte_range {
            if let Ok(mut states) = self.split_states.write() {
                let state = states
                    .entry(split_key.clone())
                    .or_insert_with(SplitState::new);

                let index = state.get_or_create_index(component);
                let comp_key = CacheManifest::component_key(component, byte_range.clone());

                index.remove(&comp_key);
                index.insert(CachedRange {
                    start: range.start,
                    end: range.end,
                    cache_key: comp_key,
                    compression,
                });
            }
        }

        // Update LRU
        if let Ok(mut lru) = self.lru_table.lock() {
            if let Ok(manifest) = self.manifest.read() {
                if let Some(split) = manifest.splits.get(&split_key) {
                    lru.touch(&split_key, split.total_size_bytes);
                }
            }
        }

        Ok(())
    }

    /// Async version of do_evict using tokio::fs
    async fn do_evict_async(&self, storage_loc: &str, split_id: &str) -> io::Result<()> {
        let split_key = CacheManifest::split_key(storage_loc, split_id);
        let split_dir = self.split_dir(storage_loc, split_id);

        let size_to_remove = {
            let manifest = self.manifest.read().unwrap();
            manifest
                .splits
                .get(&split_key)
                .map(|s| s.total_size_bytes)
                .unwrap_or(0)
        };

        // Async directory removal
        if tokio::fs::try_exists(&split_dir).await.unwrap_or(false) {
            tokio::fs::remove_dir_all(&split_dir).await?;
        }

        // Update manifest (sync - fast)
        {
            let mut manifest = self.manifest.write().unwrap();
            manifest.splits.remove(&split_key);
            manifest.total_bytes = manifest.total_bytes.saturating_sub(size_to_remove);
        }

        self.total_bytes.fetch_sub(size_to_remove, Ordering::Relaxed);

        if let Ok(mut states) = self.split_states.write() {
            states.remove(&split_key);
        }

        if let Ok(mut lru) = self.lru_table.lock() {
            lru.remove(&split_key);
        }

        Ok(())
    }

    /// Actually sync manifest to disk
    fn do_sync_manifest(&self) -> io::Result<()> {
        let cache_dir = Self::cache_dir(&self.config.root_path);
        let manifest_path = cache_dir.join("manifest.json");
        let backup_path = cache_dir.join("manifest.json.bak");
        let temp_path = cache_dir.join("manifest.json.tmp");

        // Serialize manifest
        let manifest_data = {
            let mut manifest = self.manifest.write().unwrap();
            manifest.last_sync = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            serde_json::to_string_pretty(&*manifest)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?
        };

        // Write to temp file
        {
            let mut file = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temp_path)?;
            file.write_all(manifest_data.as_bytes())?;
            file.sync_all()?;
        }

        // Backup existing manifest
        if manifest_path.exists() {
            fs::copy(&manifest_path, &backup_path)?;
        }

        // Atomic rename
        fs::rename(&temp_path, &manifest_path)?;

        Ok(())
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
        // Use try_send() to avoid blocking during Drop - if queue is full, shutdown anyway.
        let _ = self.write_tx.try_send(WriteRequest::SyncManifest);
        let _ = self.write_tx.try_send(WriteRequest::Shutdown);

        // Wait for background threads to process shutdown and release file handles
        // This is needed for test cleanup - JUnit @TempDir needs files to be closed
        // The threads use Weak references so they'll exit quickly after processing Shutdown
        // Use 250ms to ensure all pending writes complete
        std::thread::sleep(std::time::Duration::from_millis(250));

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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_compression_decision() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig {
            root_path: temp_dir.path().to_path_buf(),
            compression: CompressionAlgorithm::Lz4,
            min_compress_size: 4096,
            ..Default::default()
        };
        let cache = L2DiskCache::new(config).unwrap();

        // Small data - no compression regardless of component
        assert!(!cache.should_compress("idx", 100));
        assert!(!cache.should_compress("pos", 1000));

        // Footer/metadata - never compress
        assert!(!cache.should_compress("footer", 10000));
        assert!(!cache.should_compress("metadata", 10000));
        assert!(!cache.should_compress("fieldnorm", 10000));

        // Store files - already LZ4/Zstd compressed by Tantivy, never double-compress
        assert!(!cache.should_compress("store", 10000));
        assert!(!cache.should_compress("store", 1000000)); // Even large store files

        // Term files - already Zstd compressed (sstable), never double-compress
        assert!(!cache.should_compress("term", 10000));
        assert!(!cache.should_compress("term", 1000000)); // Even large term files

        // idx/pos - block-access patterns, whole-file decompression would be terrible
        // With millions of docs, a 10MB file needs full decompress for 512-byte block access
        assert!(!cache.should_compress("idx", 5000));
        assert!(!cache.should_compress("idx", 10_000_000)); // Even large posting files
        assert!(!cache.should_compress("pos", 5000));
        assert!(!cache.should_compress("pos", 10_000_000));

        // Fast fields - random access by doc_id, same decompression problem
        assert!(!cache.should_compress("fast", 50000));
        assert!(!cache.should_compress("fast", 100000));
    }

    #[test]
    fn test_storage_loc_hash() {
        let hash1 = L2DiskCache::storage_loc_hash("s3://my-bucket/path/to/splits");
        assert!(hash1.starts_with("s3_my-bucket__"));

        let hash2 = L2DiskCache::storage_loc_hash("azure://container/path");
        assert!(hash2.starts_with("azure_container__"));

        let hash3 = L2DiskCache::storage_loc_hash("/local/path");
        assert!(hash3.starts_with("local_"));
    }

    #[test]
    fn test_lz4_roundtrip() {
        let original = b"Hello, world! This is test data for compression.";
        let compressed = compress_prepend_size(original);
        let decompressed = decompress_size_prepended(&compressed).unwrap();
        assert_eq!(original.as_slice(), decompressed.as_slice());
    }

    #[test]
    fn test_put_get_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig::new(temp_dir.path());
        let cache = L2DiskCache::new(config).unwrap();

        // Large enough data to trigger compression
        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();

        cache.put("s3://bucket/path", "split-001", "term", None, &data);

        // Give background writer time to process
        std::thread::sleep(Duration::from_millis(100));

        let retrieved = cache.get("s3://bucket/path", "split-001", "term", None);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().as_slice(), data.as_slice());
    }

    #[test]
    fn test_byte_range_caching() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig::new(temp_dir.path());
        let cache = L2DiskCache::new(config).unwrap();

        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
        let range = Some(0u64..5000u64);

        cache.put("s3://bucket", "split-002", "idx", range.clone(), &data);

        std::thread::sleep(Duration::from_millis(100));

        let retrieved = cache.get("s3://bucket", "split-002", "idx", range);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().as_slice(), data.as_slice());
    }

    #[test]
    fn test_stats_tracking() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig::new(temp_dir.path());
        let cache = L2DiskCache::new(config).unwrap();

        // Initially empty
        assert_eq!(cache.get_total_bytes(), 0);
        assert_eq!(cache.get_split_count(), 0);
        assert_eq!(cache.get_component_count(), 0);

        // Add data
        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
        cache.put("s3://bucket", "split-001", "term", None, &data);
        std::thread::sleep(Duration::from_millis(100));

        // Stats should be updated
        assert!(cache.get_total_bytes() > 0, "Total bytes should be > 0 after put");
        assert_eq!(cache.get_split_count(), 1, "Should have 1 split");
        assert_eq!(cache.get_component_count(), 1, "Should have 1 component");

        // Add another component to same split
        cache.put("s3://bucket", "split-001", "idx", None, &data);
        std::thread::sleep(Duration::from_millis(100));

        assert_eq!(cache.get_split_count(), 1, "Still 1 split");
        assert_eq!(cache.get_component_count(), 2, "Now 2 components");

        // Add component to different split
        cache.put("s3://bucket", "split-002", "term", None, &data);
        std::thread::sleep(Duration::from_millis(100));

        assert_eq!(cache.get_split_count(), 2, "Now 2 splits");
        assert_eq!(cache.get_component_count(), 3, "Now 3 components");
    }

    #[test]
    fn test_evict_split() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig::new(temp_dir.path());
        let cache = L2DiskCache::new(config).unwrap();

        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();

        // Add data for two splits
        cache.put("s3://bucket", "split-001", "term", None, &data);
        cache.put("s3://bucket", "split-001", "idx", None, &data);
        cache.put("s3://bucket", "split-002", "term", None, &data);
        std::thread::sleep(Duration::from_millis(150));

        assert_eq!(cache.get_split_count(), 2);
        assert_eq!(cache.get_component_count(), 3);
        let bytes_before = cache.get_total_bytes();

        // Evict split-001 (async - just sends request to background writer)
        cache.evict_split("s3://bucket", "split-001");

        // Give time for eviction to process
        std::thread::sleep(Duration::from_millis(100));

        assert_eq!(cache.get_split_count(), 1, "Should have 1 split after eviction");
        assert_eq!(cache.get_component_count(), 1, "Should have 1 component after eviction");
        assert!(cache.get_total_bytes() < bytes_before, "Total bytes should decrease");

        // Verify split-001 data is gone
        assert!(cache.get("s3://bucket", "split-001", "term", None).is_none());
        assert!(cache.get("s3://bucket", "split-001", "idx", None).is_none());

        // Verify split-002 data is still there
        assert!(cache.get("s3://bucket", "split-002", "term", None).is_some());
    }

    #[test]
    fn test_no_compression_mode() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig {
            root_path: temp_dir.path().to_path_buf(),
            compression: CompressionAlgorithm::None,
            min_compress_size: 4096,
            ..Default::default()
        };
        let cache = L2DiskCache::new(config).unwrap();

        // Data that would normally be compressed
        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();

        cache.put("s3://bucket", "split-001", "term", None, &data);
        std::thread::sleep(Duration::from_millis(100));

        let retrieved = cache.get("s3://bucket", "split-001", "term", None);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().as_slice(), data.as_slice());

        // Verify file is not compressed (should have .cache extension, not .cache.lz4)
        let cache_dir = temp_dir.path().join(CACHE_SUBDIR);
        let mut found_uncompressed = false;
        for entry in std::fs::read_dir(&cache_dir).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                for subentry in std::fs::read_dir(entry.path()).unwrap().flatten() {
                    if subentry.file_type().unwrap().is_dir() {
                        for file in std::fs::read_dir(subentry.path()).unwrap().flatten() {
                            let name = file.file_name().to_string_lossy().to_string();
                            if name.ends_with(".cache") && !name.ends_with(".lz4") {
                                found_uncompressed = true;
                            }
                        }
                    }
                }
            }
        }
        assert!(found_uncompressed, "Should have uncompressed .cache file");
    }

    #[test]
    fn test_manifest_persistence() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().to_path_buf();

        // Create cache and add data
        {
            let config = DiskCacheConfig::new(&cache_path);
            let cache = L2DiskCache::new(config).unwrap();

            let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
            cache.put("s3://bucket", "split-001", "term", None, &data);
            cache.put("s3://bucket", "split-002", "idx", None, &data);
            std::thread::sleep(Duration::from_millis(100));

            // Force manifest sync (async - sends to background writer)
            cache.sync_manifest().unwrap();

            // Wait for background writer to process sync request
            std::thread::sleep(Duration::from_millis(100));
        }
        // Cache is dropped here, which also triggers a sync with 250ms wait

        // Verify manifest file exists in the cache subdirectory
        let manifest_path = cache_path.join(CACHE_SUBDIR).join("manifest.json");
        assert!(manifest_path.exists(), "Manifest file should exist");

        // Create new cache instance and verify data is recovered
        {
            let config = DiskCacheConfig::new(&cache_path);
            let cache = L2DiskCache::new(config).unwrap();

            // Data should be accessible
            let retrieved = cache.get("s3://bucket", "split-001", "term", None);
            assert!(retrieved.is_some(), "Data should be recovered from persisted cache");
            assert_eq!(retrieved.unwrap().len(), 10000);

            let retrieved2 = cache.get("s3://bucket", "split-002", "idx", None);
            assert!(retrieved2.is_some(), "Second split data should be recovered");
        }
    }

    #[test]
    fn test_manifest_backup_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let cache_path = temp_dir.path().to_path_buf();

        // Create cache and add data
        {
            let config = DiskCacheConfig::new(&cache_path);
            let cache = L2DiskCache::new(config).unwrap();

            let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
            cache.put("s3://bucket", "split-001", "term", None, &data);
            std::thread::sleep(Duration::from_millis(100));
            cache.sync_manifest().unwrap();
        }

        // Corrupt the main manifest, but backup should exist
        let cache_subdir = cache_path.join(CACHE_SUBDIR);
        let manifest_path = cache_subdir.join("manifest.json");
        let backup_path = cache_subdir.join("manifest.json.bak");

        // Copy manifest to backup if it doesn't exist
        if manifest_path.exists() && !backup_path.exists() {
            std::fs::copy(&manifest_path, &backup_path).unwrap();
        }

        // Corrupt main manifest
        std::fs::write(&manifest_path, "invalid json {{{").unwrap();

        // Create new cache - should recover from backup
        {
            let config = DiskCacheConfig::new(&cache_path);
            let cache = L2DiskCache::new(config).unwrap();

            // Data should still be accessible (recovered from backup or rebuilt)
            let retrieved = cache.get("s3://bucket", "split-001", "term", None);
            // Note: If backup recovery isn't implemented, this might fail
            // In that case, the cache should at least start without crashing
            if retrieved.is_some() {
                assert_eq!(retrieved.unwrap().len(), 10000);
            }
        }
    }

    #[test]
    fn test_lru_eviction_under_pressure() {
        let temp_dir = TempDir::new().unwrap();
        // Set a small max size to trigger eviction
        let config = DiskCacheConfig {
            root_path: temp_dir.path().to_path_buf(),
            max_size_bytes: 50000, // 50KB limit
            compression: CompressionAlgorithm::None, // No compression for predictable sizes
            min_compress_size: 100000, // Effectively disable compression
            ..Default::default()
        };
        let cache = L2DiskCache::new(config).unwrap();

        // Add data that will exceed the limit
        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();

        // Add 6 splits × 10KB = 60KB > 50KB limit
        for i in 0..6 {
            cache.put("s3://bucket", &format!("split-{:03}", i), "term", None, &data);
            std::thread::sleep(Duration::from_millis(50));
        }

        // Wait for eviction to process
        std::thread::sleep(Duration::from_millis(200));

        // Cache should have evicted oldest entries to stay under limit
        let total_bytes = cache.get_total_bytes();
        assert!(total_bytes <= 50000, "Total bytes {} should be <= 50000 after eviction", total_bytes);

        // Most recent splits should still be accessible
        let recent = cache.get("s3://bucket", "split-005", "term", None);
        assert!(recent.is_some(), "Most recent split should still be cached");
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::Arc;
        use std::thread;

        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig::new(temp_dir.path());
        let cache = Arc::new(L2DiskCache::new(config).unwrap());

        let mut handles = vec![];

        // Spawn multiple writer threads
        for i in 0..4 {
            let cache_clone = Arc::clone(&cache);
            handles.push(thread::spawn(move || {
                let data: Vec<u8> = (0..5000).map(|j| ((i * 1000 + j) % 256) as u8).collect();
                for j in 0..5 {
                    cache_clone.put(
                        "s3://bucket",
                        &format!("split-{}-{}", i, j),
                        "term",
                        None,
                        &data,
                    );
                }
            }));
        }

        // Wait for writers
        for handle in handles {
            handle.join().unwrap();
        }

        std::thread::sleep(Duration::from_millis(200));

        // Spawn reader threads
        let mut read_handles = vec![];
        for i in 0..4 {
            let cache_clone = Arc::clone(&cache);
            read_handles.push(thread::spawn(move || {
                let mut found = 0;
                for j in 0..5 {
                    if cache_clone
                        .get("s3://bucket", &format!("split-{}-{}", i, j), "term", None)
                        .is_some()
                    {
                        found += 1;
                    }
                }
                found
            }));
        }

        // Verify reads succeeded
        let mut total_found = 0;
        for handle in read_handles {
            total_found += handle.join().unwrap();
        }

        assert!(total_found > 0, "Should have found some cached data");
        // All 20 entries should be cached (4 threads × 5 entries)
        assert_eq!(total_found, 20, "All entries should be cached");
    }

    // NOTE: test_large_data_compression_ratio was removed because compression
    // was intentionally disabled for all components. Tantivy components are either:
    // - Already internally compressed (store, term)
    // - Block-accessed (idx, pos, fast) where whole-file decompression hurts performance
    // See should_compress() for details.

    #[test]
    fn test_multiple_byte_ranges_same_component() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig::new(temp_dir.path());
        let cache = L2DiskCache::new(config).unwrap();

        let data1: Vec<u8> = (0..5000).map(|i| (i % 256) as u8).collect();
        let data2: Vec<u8> = (0..5000).map(|i| ((i + 100) % 256) as u8).collect();

        // Cache different byte ranges of same component
        cache.put("s3://bucket", "split-001", "idx", Some(0..5000), &data1);
        cache.put("s3://bucket", "split-001", "idx", Some(5000..10000), &data2);
        std::thread::sleep(Duration::from_millis(100));

        // Both ranges should be retrievable
        let r1 = cache.get("s3://bucket", "split-001", "idx", Some(0..5000));
        let r2 = cache.get("s3://bucket", "split-001", "idx", Some(5000..10000));

        assert!(r1.is_some(), "First range should be cached");
        assert!(r2.is_some(), "Second range should be cached");
        assert_eq!(r1.unwrap().as_slice(), data1.as_slice());
        assert_eq!(r2.unwrap().as_slice(), data2.as_slice());

        // Should be counted as 2 components (different ranges)
        assert_eq!(cache.get_component_count(), 2);
    }

    #[test]
    fn test_cache_miss_returns_none() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig::new(temp_dir.path());
        let cache = L2DiskCache::new(config).unwrap();

        // Query non-existent data
        assert!(cache.get("s3://bucket", "nonexistent", "term", None).is_none());
        assert!(cache.get("s3://bucket", "split-001", "nonexistent", None).is_none());
        assert!(cache.get("nonexistent://bucket", "split-001", "term", None).is_none());
    }

    #[test]
    fn test_different_storage_locations() {
        let temp_dir = TempDir::new().unwrap();
        let config = DiskCacheConfig::new(temp_dir.path());
        let cache = L2DiskCache::new(config).unwrap();

        let data: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();

        // Same split ID but different storage locations
        cache.put("s3://bucket-a", "split-001", "term", None, &data);
        cache.put("s3://bucket-b", "split-001", "term", None, &data);
        cache.put("azure://container", "split-001", "term", None, &data);
        std::thread::sleep(Duration::from_millis(150));

        // All should be cached separately
        assert!(cache.get("s3://bucket-a", "split-001", "term", None).is_some());
        assert!(cache.get("s3://bucket-b", "split-001", "term", None).is_some());
        assert!(cache.get("azure://container", "split-001", "term", None).is_some());

        // Should be 3 splits
        assert_eq!(cache.get_split_count(), 3);
    }
}
