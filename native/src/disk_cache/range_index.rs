// range_index.rs - Range coalescing and overlap queries
// Extracted from mod.rs during refactoring
// Provides efficient O(log n) overlap queries for partial cache hits

#![allow(dead_code)]

use std::cmp::{max, min};
use std::collections::HashMap;
use std::ops::Range;

use tantivy::directory::OwnedBytes;

use super::types::{CompressionAlgorithm, ComponentEntry};

/// A cached byte range with its location info
#[derive(Debug, Clone)]
pub struct CachedRange {
    /// Start of cached range (inclusive)
    pub start: u64,
    /// End of cached range (exclusive)
    pub end: u64,
    /// Cache key for lookup (component_start-end format)
    pub cache_key: String,
    /// Compression algorithm used
    pub compression: CompressionAlgorithm,
}

impl CachedRange {
    /// Check if this range overlaps with [start, end)
    #[inline]
    pub fn overlaps(&self, start: u64, end: u64) -> bool {
        self.start < end && start < self.end
    }

    /// Get the overlapping portion with [start, end)
    #[inline]
    pub fn overlap_with(&self, start: u64, end: u64) -> Option<Range<u64>> {
        if !self.overlaps(start, end) {
            return None;
        }
        Some(max(self.start, start)..min(self.end, end))
    }
}

/// Index for efficient range overlap queries
/// Maintains sorted ranges for O(log n) binary search
#[derive(Debug, Clone, Default)]
pub struct RangeIndex {
    /// Ranges sorted by start position
    ranges: Vec<CachedRange>,
}

impl RangeIndex {
    pub fn new() -> Self {
        Self { ranges: Vec::new() }
    }

    /// Add a new range to the index (maintains sorted order)
    pub fn insert(&mut self, range: CachedRange) {
        // Binary search for insertion point
        let pos = self.ranges
            .binary_search_by_key(&range.start, |r| r.start)
            .unwrap_or_else(|pos| pos);
        self.ranges.insert(pos, range);
    }

    /// Remove a range by its cache key
    pub fn remove(&mut self, cache_key: &str) {
        self.ranges.retain(|r| r.cache_key != cache_key);
    }

    /// Find all ranges that overlap with [start, end)
    /// Returns ranges in sorted order by start position
    /// O(log n + k) where k is the number of overlapping ranges
    pub fn find_overlapping(&self, start: u64, end: u64) -> Vec<&CachedRange> {
        if self.ranges.is_empty() {
            return Vec::new();
        }

        // Binary search to find first range that might overlap
        // A range overlaps if: range.start < end && start < range.end
        // First candidate: last range where range.start < end
        let first_idx = match self.ranges.binary_search_by(|r| {
            if r.start >= end {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            }
        }) {
            Ok(i) | Err(i) => i.saturating_sub(1),
        };

        // Scan backwards to find first actually overlapping range
        // (needed because binary search found last range with start < end)
        let mut scan_start = first_idx;
        while scan_start > 0 && self.ranges[scan_start - 1].end > start {
            scan_start -= 1;
        }

        // Collect all overlapping ranges
        let mut result = Vec::new();
        for range in &self.ranges[scan_start..] {
            if range.start >= end {
                break; // No more overlaps possible
            }
            if range.overlaps(start, end) {
                result.push(range);
            }
        }

        result
    }

    /// Rebuild index from manifest components
    pub fn from_components(components: &HashMap<String, ComponentEntry>) -> Self {
        let mut index = Self::new();
        for (key, entry) in components {
            if let Some((start, end)) = entry.byte_range {
                index.insert(CachedRange {
                    start,
                    end,
                    cache_key: key.clone(),
                    compression: entry.compression,
                });
            }
        }
        index
    }
}

/// A segment of data from the cache
#[derive(Debug)]
pub struct CachedSegment {
    /// The byte range this segment covers in the original file
    pub range: Range<u64>,
    /// The actual data bytes
    pub data: OwnedBytes,
}

/// Result of a coalescing query
#[derive(Debug)]
pub struct CoalesceResult {
    /// Segments we have cached (sorted by range.start)
    pub cached_segments: Vec<CachedSegment>,
    /// Gaps that need to be fetched from remote storage
    pub gaps: Vec<Range<u64>>,
    /// True if the entire requested range is cached (no gaps)
    pub fully_cached: bool,
    /// Total bytes served from cache
    pub cached_bytes: u64,
    /// Total bytes that need fetching
    pub gap_bytes: u64,
}

impl CoalesceResult {
    /// Create a result indicating complete cache miss
    pub fn miss(requested: Range<u64>) -> Self {
        let gap_bytes = requested.end - requested.start;
        Self {
            cached_segments: Vec::new(),
            gaps: vec![requested],
            fully_cached: false,
            cached_bytes: 0,
            gap_bytes,
        }
    }

    /// Create a result indicating complete cache hit
    pub fn hit(data: OwnedBytes, range: Range<u64>) -> Self {
        let cached_bytes = data.len() as u64;
        Self {
            cached_segments: vec![CachedSegment { range, data }],
            gaps: Vec::new(),
            fully_cached: true,
            cached_bytes,
            gap_bytes: 0,
        }
    }
}
