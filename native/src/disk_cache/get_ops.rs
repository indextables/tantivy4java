// get_ops.rs - Get operations for disk cache
// Extracted from mod.rs during refactoring

#![allow(dead_code)]

use std::cmp::{max, min};
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::ops::Range;
use std::sync::{Mutex, RwLock};

use tantivy::directory::OwnedBytes;

use crate::debug_println;
use super::compression::decompress_data;
use super::lru::SplitLruTable;
use super::manifest::{CacheManifest, SplitState};
use super::mmap_cache::MmapCache;
use super::path_helpers::component_path;
use super::range_index::{CachedRange, CachedSegment, CoalesceResult};
use super::types::{CompressionAlgorithm, DiskCacheConfig};

/// Get cached data (synchronous read from disk)
///
/// For uncompressed files, uses memory-mapped I/O for fast random access.
/// The OS kernel manages the page cache efficiently, and repeated access
/// to the same file region avoids syscalls entirely.
pub fn get(
    config: &DiskCacheConfig,
    manifest: &RwLock<CacheManifest>,
    mmap_cache: &Mutex<MmapCache>,
    lru_table: &Mutex<SplitLruTable>,
    storage_loc: &str,
    split_id: &str,
    component: &str,
    byte_range: Option<Range<u64>>,
) -> Option<OwnedBytes> {
    let split_key = CacheManifest::split_key(storage_loc, split_id);
    let comp_key = CacheManifest::component_key(component, byte_range.clone());

    // Check manifest for entry
    let entry = {
        let manifest = manifest.read().ok()?;
        let split = manifest.splits.get(&split_key)?;
        split.components.get(&comp_key)?.clone()
    };

    // Get file path
    let file_path = component_path(
        &config.root_path,
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
            let mut mmap_cache_guard = mmap_cache.lock().ok()?;
            mmap_cache_guard.get_or_map(&file_path).ok()?
        };
        // Return a copy of the mapped data
        // (OwnedBytes requires owned data, but mmap access is still fast)
        mmap.to_vec()
    } else {
        // Slow path: read entire file for compressed data
        let mut file = File::open(&file_path).ok()?;
        let mut data = Vec::with_capacity(entry.disk_size_bytes as usize);
        file.read_to_end(&mut data).ok()?;
        decompress_data(&data, entry.compression).ok()?
    };

    // Update LRU access time
    if let Ok(mut lru) = lru_table.lock() {
        if let Ok(manifest_guard) = manifest.read() {
            if let Some(split) = manifest_guard.splits.get(&split_key) {
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
    config: &DiskCacheConfig,
    manifest: &RwLock<CacheManifest>,
    mmap_cache: &Mutex<MmapCache>,
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
        let manifest_guard = manifest.read().ok()?;
        let split = manifest_guard.splits.get(&split_key)?;
        split.components.get(&comp_key)?.clone()
    };

    // Only use fast path for uncompressed files
    if entry.compression != CompressionAlgorithm::None {
        // Fall back to full load + slice for compressed files
        let full_data = get(
            config,
            manifest,
            mmap_cache,
            &Mutex::new(SplitLruTable::new()), // Dummy LRU for this call
            storage_loc,
            split_id,
            component,
            Some(cached_range.clone()),
        )?;
        let offset = (requested_range.start - cached_range.start) as usize;
        let len = (requested_range.end - requested_range.start) as usize;
        if offset + len <= full_data.len() {
            return Some(OwnedBytes::new(full_data[offset..offset + len].to_vec()));
        }
        return None;
    }

    // Fast path: mmap and extract just the requested slice
    let file_path = component_path(
        &config.root_path,
        storage_loc,
        split_id,
        component,
        Some(cached_range.clone()),
        entry.compression,
    );

    let mmap = {
        let mut mmap_cache_guard = mmap_cache.lock().ok()?;
        mmap_cache_guard.get_or_map(&file_path).ok()?
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
    config: &DiskCacheConfig,
    manifest: &RwLock<CacheManifest>,
    split_states: &RwLock<HashMap<String, SplitState>>,
    mmap_cache: &Mutex<MmapCache>,
    lru_table: &Mutex<SplitLruTable>,
    storage_loc: &str,
    split_id: &str,
    component: &str,
    requested_range: Range<u64>,
) -> CoalesceResult {
    let split_key = CacheManifest::split_key(storage_loc, split_id);

    // First, try exact match (fast path) - use get_subrange to avoid copying entire file
    // Even if the requested range exactly matches the cached range, we use mmap-based
    // sub-range extraction which only copies the requested bytes
    if let Some(data) = get_subrange(
        config,
        manifest,
        mmap_cache,
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
        let states = match split_states.read() {
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
            if let Some(data) = get_subrange(
                config,
                manifest,
                mmap_cache,
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
    if let Ok(mut lru) = lru_table.lock() {
        if let Ok(manifest_guard) = manifest.read() {
            if let Some(split) = manifest_guard.splits.get(&split_key) {
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
