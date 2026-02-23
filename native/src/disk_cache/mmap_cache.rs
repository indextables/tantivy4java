// mmap_cache.rs - Memory-mapped file cache for fast random access
// Extracted from mod.rs during refactoring

use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use memmap2::Mmap;
use tantivy::directory::OwnedBytes;

use crate::debug_println;

/// Wrapper around Arc<Mmap> that implements StableDeref, allowing zero-copy
/// conversion to OwnedBytes. Without this, every disk cache hit requires
/// copying the entire mmap'd region into a Vec<u8>.
#[derive(Clone)]
pub(crate) struct StableMmap(pub Arc<Mmap>);

impl std::ops::Deref for StableMmap {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self.0
    }
}

// SAFETY: Mmap is backed by kernel-managed virtual memory pages.
// The pointer returned by Deref is stable for the lifetime of the Mmap —
// the kernel guarantees the mapping address doesn't change after mmap().
unsafe impl stable_deref_trait::StableDeref for StableMmap {}

/// Wrapper around a sub-slice of an Arc<Mmap> that implements StableDeref.
/// Enables zero-copy OwnedBytes for sub-range reads from mmap'd files.
#[derive(Clone)]
pub(crate) struct StableMmapSlice {
    _mmap: Arc<Mmap>,
    offset: usize,
    len: usize,
}

impl StableMmapSlice {
    pub fn new(mmap: Arc<Mmap>, offset: usize, len: usize) -> Self {
        debug_assert!(offset + len <= mmap.len());
        Self { _mmap: mmap, offset, len }
    }
}

impl std::ops::Deref for StableMmapSlice {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        &self._mmap[self.offset..self.offset + self.len]
    }
}

// SAFETY: Same as StableMmap — the slice is a fixed sub-range of a stable mmap.
unsafe impl stable_deref_trait::StableDeref for StableMmapSlice {}

impl StableMmap {
    /// Convert to OwnedBytes with zero copy.
    pub fn into_owned_bytes(self) -> OwnedBytes {
        OwnedBytes::new(self)
    }
}

impl StableMmapSlice {
    /// Convert to OwnedBytes with zero copy.
    pub fn into_owned_bytes(self) -> OwnedBytes {
        OwnedBytes::new(self)
    }
}

/// LRU cache of memory-mapped files for fast random access reads.
///
/// Instead of open/read/close for every cache hit, we keep files memory-mapped.
/// Random access becomes just pointer indexing + potential page fault.
/// The OS kernel manages the page cache efficiently.
pub(crate) struct MmapCache {
    /// Map from file path to memory-mapped region
    maps: HashMap<PathBuf, Arc<Mmap>>,
    /// LRU order tracking (front = oldest, back = newest)
    lru_order: VecDeque<PathBuf>,
    /// Maximum number of mappings to keep
    max_size: usize,
}

impl MmapCache {
    pub fn new(max_size: usize) -> Self {
        Self {
            maps: HashMap::with_capacity(max_size),
            lru_order: VecDeque::with_capacity(max_size),
            max_size,
        }
    }

    /// Get or create a memory mapping for the given file path.
    /// Returns Arc<Mmap> so the mapping can be used while the cache is unlocked.
    pub fn get_or_map(&mut self, path: &Path) -> io::Result<Arc<Mmap>> {
        // Check if already mapped
        if self.maps.contains_key(path) {
            // Move to back of LRU (most recently used)
            self.touch_lru(path);
            return Ok(Arc::clone(self.maps.get(path).unwrap()));
        }

        // Evict oldest if at capacity
        while self.maps.len() >= self.max_size {
            if let Some(oldest_path) = self.lru_order.pop_front() {
                self.maps.remove(&oldest_path);
                debug_println!("📤 MMAP_CACHE: Evicted {:?}", oldest_path);
            }
        }

        // Create new mapping
        let file = File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let mmap = Arc::new(mmap);

        // Add to cache
        self.maps.insert(path.to_path_buf(), Arc::clone(&mmap));
        self.lru_order.push_back(path.to_path_buf());

        debug_println!("📥 MMAP_CACHE: Mapped {:?} ({} bytes)", path, mmap.len());
        Ok(mmap)
    }

    /// Move a path to the back of the LRU order (mark as recently used)
    fn touch_lru(&mut self, path: &Path) {
        // Remove from current position
        if let Some(pos) = self.lru_order.iter().position(|p| p == path) {
            self.lru_order.remove(pos);
        }
        // Add to back (most recently used)
        self.lru_order.push_back(path.to_path_buf());
    }

    /// Remove a specific path from the cache (e.g., when file is deleted)
    #[allow(dead_code)]
    pub fn remove(&mut self, path: &Path) {
        self.maps.remove(path);
        if let Some(pos) = self.lru_order.iter().position(|p| p == path) {
            self.lru_order.remove(pos);
        }
    }

    /// Clear all mappings
    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.maps.clear();
        self.lru_order.clear();
    }

    /// Get cache statistics
    #[allow(dead_code)]
    pub fn stats(&self) -> (usize, usize) {
        (self.maps.len(), self.max_size)
    }
}
