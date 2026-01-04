// byterange_cache.rs - ByteRange cache merging support
// Enables partial cache hits and range coalescing for efficient storage access

use std::sync::Arc;
use super::cache_config::MAX_ACCEPTABLE_GAPS;

/// Represents a cached byte range with metadata
#[derive(Debug, Clone)]
pub struct CachedRange {
    pub start: usize,
    pub end: usize,
    pub data: Arc<Vec<u8>>,
    pub last_accessed: std::time::SystemTime,
}

impl CachedRange {
    /// Check if this range overlaps with the requested range
    pub fn overlaps_with(&self, start: usize, end: usize) -> bool {
        !(self.end <= start || self.start >= end)
    }

    /// Get the intersection of this range with the requested range
    pub fn intersection(&self, start: usize, end: usize) -> Option<(usize, usize)> {
        if self.overlaps_with(start, end) {
            Some((self.start.max(start), self.end.min(end)))
        } else {
            None
        }
    }

    /// Get data slice for the specified range (relative to this cached range)
    pub fn get_slice(&self, start: usize, end: usize) -> Option<&[u8]> {
        if start >= self.start && end <= self.end {
            let relative_start = start - self.start;
            let relative_end = end - self.start;
            Some(&self.data[relative_start..relative_end])
        } else {
            None
        }
    }
}

/// Result of attempting to serve a request from cache with range merging
pub enum CacheResult {
    /// Complete cache hit - all data available from cache
    Hit(Vec<u8>),
    /// Partial cache hit - some data cached, some needs to be fetched
    PartialHit {
        cached_segments: Vec<CachedRange>,
        missing_gaps: Vec<(usize, usize)>,
    },
    /// Cache miss - no useful cached data
    Miss,
}

/// Calculate missing gaps between cached ranges for a requested range
pub fn calculate_missing_gaps(
    requested_start: usize,
    requested_end: usize,
    cached_ranges: &[CachedRange]
) -> Vec<(usize, usize)> {
    let mut gaps = Vec::new();
    let mut current_pos = requested_start;

    // Sort cached ranges by start position
    let mut sorted_ranges: Vec<_> = cached_ranges.iter()
        .filter(|r| r.overlaps_with(requested_start, requested_end))
        .collect();
    sorted_ranges.sort_by_key(|r| r.start);

    for range in sorted_ranges {
        let range_start = range.start.max(requested_start);
        let range_end = range.end.min(requested_end);

        // Add gap before this range if it exists
        if current_pos < range_start {
            gaps.push((current_pos, range_start));
        }

        // Move past this range
        current_pos = current_pos.max(range_end);
    }

    // Add final gap if needed
    if current_pos < requested_end {
        gaps.push((current_pos, requested_end));
    }

    gaps
}

/// Try to merge cached ranges to serve a complete request
pub fn try_merge_cached_ranges(
    requested_start: usize,
    requested_end: usize,
    cached_ranges: &[CachedRange]
) -> CacheResult {
    let gaps = calculate_missing_gaps(requested_start, requested_end, cached_ranges);

    if gaps.is_empty() {
        // Complete cache hit possible - merge the data
        let mut result_data = vec![0u8; requested_end - requested_start];
        let mut covered = false;

        for range in cached_ranges {
            if let Some((int_start, int_end)) = range.intersection(requested_start, requested_end) {
                if let Some(slice) = range.get_slice(int_start, int_end) {
                    let result_start = int_start - requested_start;
                    let result_end = result_start + slice.len();
                    result_data[result_start..result_end].copy_from_slice(slice);
                    covered = true;
                }
            }
        }

        if covered {
            CacheResult::Hit(result_data)
        } else {
            CacheResult::Miss
        }
    } else if gaps.len() <= MAX_ACCEPTABLE_GAPS {
        // Partial hit with acceptable number of gaps
        let relevant_ranges: Vec<_> = cached_ranges.iter()
            .filter(|r| r.overlaps_with(requested_start, requested_end))
            .cloned()
            .collect();

        CacheResult::PartialHit {
            cached_segments: relevant_ranges,
            missing_gaps: gaps,
        }
    } else {
        // Too many gaps - treat as miss
        CacheResult::Miss
    }
}
