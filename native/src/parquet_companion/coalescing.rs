// coalescing.rs - Cross-column range coalescing (Phase 4)
//
// Merges nearby byte ranges to reduce the number of storage requests.
// Especially beneficial for S3/Azure where each request has high latency.

use std::ops::Range;

/// Default maximum gap between ranges that will be coalesced (64 KB)
pub const DEFAULT_MAX_GAP: u64 = 64 * 1024;

/// A byte range request for a specific file
#[derive(Debug, Clone)]
pub struct FileByteRange {
    /// File identifier (index into manifest.parquet_files)
    pub file_idx: usize,
    /// Column name (for debugging)
    pub column_name: String,
    /// Byte range within the file
    pub range: Range<u64>,
}

/// A coalesced byte range (may span multiple original requests)
#[derive(Debug, Clone)]
pub struct CoalescedRange {
    pub file_idx: usize,
    pub range: Range<u64>,
    /// Original ranges that were merged into this coalesced range
    pub source_ranges: Vec<Range<u64>>,
}

/// Coalesce nearby byte ranges within the same file.
///
/// Ranges separated by at most `max_gap` bytes are merged into a single
/// request. This trades a small amount of extra bytes for fewer storage
/// round-trips.
pub fn coalesce_page_ranges(
    ranges: &mut Vec<FileByteRange>,
    max_gap: u64,
) -> Vec<CoalescedRange> {
    if ranges.is_empty() {
        return Vec::new();
    }

    // Sort by (file_idx, range start)
    ranges.sort_by(|a, b| {
        a.file_idx
            .cmp(&b.file_idx)
            .then(a.range.start.cmp(&b.range.start))
    });

    let mut coalesced = Vec::new();
    let mut current_file = ranges[0].file_idx;
    let mut current_start = ranges[0].range.start;
    let mut current_end = ranges[0].range.end;
    let mut current_sources = vec![ranges[0].range.clone()];

    for range in ranges.iter().skip(1) {
        if range.file_idx == current_file && range.range.start <= current_end + max_gap {
            // Merge with current range
            current_end = current_end.max(range.range.end);
            current_sources.push(range.range.clone());
        } else {
            // Emit current and start new
            coalesced.push(CoalescedRange {
                file_idx: current_file,
                range: current_start..current_end,
                source_ranges: std::mem::take(&mut current_sources),
            });
            current_file = range.file_idx;
            current_start = range.range.start;
            current_end = range.range.end;
            current_sources = vec![range.range.clone()];
        }
    }

    // Emit final range
    coalesced.push(CoalescedRange {
        file_idx: current_file,
        range: current_start..current_end,
        source_ranges: current_sources,
    });

    coalesced
}

/// Cross-column coalescing: flatten ranges from multiple columns for the same file,
/// then coalesce with gap filling.
pub fn safe_cross_column_coalesce(
    column_ranges: Vec<Vec<FileByteRange>>,
    max_gap: u64,
) -> Vec<CoalescedRange> {
    let mut all_ranges: Vec<FileByteRange> = column_ranges.into_iter().flatten().collect();
    coalesce_page_ranges(&mut all_ranges, max_gap)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coalesce_adjacent() {
        let mut ranges = vec![
            FileByteRange { file_idx: 0, column_name: "a".into(), range: 0..100 },
            FileByteRange { file_idx: 0, column_name: "b".into(), range: 100..200 },
        ];
        let coalesced = coalesce_page_ranges(&mut ranges, DEFAULT_MAX_GAP);
        assert_eq!(coalesced.len(), 1);
        assert_eq!(coalesced[0].range, 0..200);
    }

    #[test]
    fn test_coalesce_with_gap() {
        let mut ranges = vec![
            FileByteRange { file_idx: 0, column_name: "a".into(), range: 0..100 },
            FileByteRange { file_idx: 0, column_name: "b".into(), range: 200..300 },
        ];
        // Gap of 100 < DEFAULT_MAX_GAP (64KB), so should coalesce
        let coalesced = coalesce_page_ranges(&mut ranges, DEFAULT_MAX_GAP);
        assert_eq!(coalesced.len(), 1);
        assert_eq!(coalesced[0].range, 0..300);
    }

    #[test]
    fn test_no_coalesce_large_gap() {
        let gap = DEFAULT_MAX_GAP + 1;
        let mut ranges = vec![
            FileByteRange { file_idx: 0, column_name: "a".into(), range: 0..100 },
            FileByteRange { file_idx: 0, column_name: "b".into(), range: (100 + gap)..(200 + gap) },
        ];
        let coalesced = coalesce_page_ranges(&mut ranges, DEFAULT_MAX_GAP);
        assert_eq!(coalesced.len(), 2);
    }

    #[test]
    fn test_different_files_never_coalesce() {
        let mut ranges = vec![
            FileByteRange { file_idx: 0, column_name: "a".into(), range: 0..100 },
            FileByteRange { file_idx: 1, column_name: "a".into(), range: 0..100 },
        ];
        let coalesced = coalesce_page_ranges(&mut ranges, DEFAULT_MAX_GAP);
        assert_eq!(coalesced.len(), 2);
    }

    #[test]
    fn test_cross_column_coalesce() {
        let col_a = vec![
            FileByteRange { file_idx: 0, column_name: "a".into(), range: 0..100 },
        ];
        let col_b = vec![
            FileByteRange { file_idx: 0, column_name: "b".into(), range: 150..250 },
        ];
        let coalesced = safe_cross_column_coalesce(vec![col_a, col_b], DEFAULT_MAX_GAP);
        assert_eq!(coalesced.len(), 1);
        assert_eq!(coalesced[0].range, 0..250);
        assert_eq!(coalesced[0].source_ranges.len(), 2);
    }
}
