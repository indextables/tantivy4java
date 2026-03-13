// read_strategy.rs - Adaptive I/O strategy for parquet companion reads
//
// Selects the optimal read strategy per-file (or per-row-group) based on
// selectivity — the fraction of rows needed from the file. Low selectivity
// warrants surgical page-level reads; high selectivity is better served by
// reading full column chunks or entire row groups to minimize S3 request count.
//
// In-region S3 cost model: $0.0004/GET, $0.00/GB transfer.
// The only cost is request count, so gap-filling is almost always worthwhile.

use super::cached_reader::CoalesceConfig;

/// Per-file I/O strategy determined by selectivity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadStrategy {
    /// < 5% selectivity: surgical page-level reads with RowSelection.
    /// Uses page index offsets for byte-level precision.
    /// Coalesce: base config (default 512KB gap).
    PageLevel,

    /// 5-25% selectivity: still use RowSelection to skip unneeded rows,
    /// but with larger coalesce gap to merge nearby page reads.
    /// Coalesce: 1MB gap, 16MB max.
    CoalescedPageLevel,

    /// 25-50% selectivity: read full column chunks per selected row group.
    /// Skip page index — read the whole column in one request per RG.
    /// Still apply RowSelection for row-level filtering after decode.
    /// Coalesce: 4MB gap, 32MB max.
    FullColumnChunk,

    /// > 50% selectivity: read entire selected row groups without
    /// RowSelection. Let the downstream `take()` filter rows.
    /// Maximum I/O efficiency — fewest S3 requests possible.
    /// Coalesce: 4MB gap, 64MB max.
    FullRowGroup,
}

impl ReadStrategy {
    /// Select read strategy based on selectivity (fraction of rows needed).
    pub fn for_selectivity(selectivity: f64) -> Self {
        if selectivity > 0.50 {
            ReadStrategy::FullRowGroup
        } else if selectivity > 0.25 {
            ReadStrategy::FullColumnChunk
        } else if selectivity > 0.05 {
            ReadStrategy::CoalescedPageLevel
        } else {
            ReadStrategy::PageLevel
        }
    }

    /// Compute the effective CoalesceConfig for this strategy.
    /// Uses the base config for PageLevel, progressively larger gaps for others.
    pub fn coalesce_config(&self, base: CoalesceConfig) -> CoalesceConfig {
        match self {
            ReadStrategy::PageLevel => base,
            ReadStrategy::CoalescedPageLevel => CoalesceConfig {
                max_gap: 1024 * 1024,        // 1MB
                max_total: 16 * 1024 * 1024, // 16MB
            },
            ReadStrategy::FullColumnChunk => CoalesceConfig {
                max_gap: 4 * 1024 * 1024,    // 4MB
                max_total: 32 * 1024 * 1024, // 32MB
            },
            ReadStrategy::FullRowGroup => CoalesceConfig {
                max_gap: 4 * 1024 * 1024,    // 4MB
                max_total: 64 * 1024 * 1024, // 64MB
            },
        }
    }

    /// Whether to use RowSelection for row-level filtering.
    /// FullRowGroup reads everything and filters via take() later.
    pub fn use_row_selection(&self) -> bool {
        match self {
            ReadStrategy::PageLevel
            | ReadStrategy::CoalescedPageLevel
            | ReadStrategy::FullColumnChunk => true,
            ReadStrategy::FullRowGroup => false,
        }
    }

    /// Whether to use page index for byte-level read precision.
    /// Only needed for surgical page-level reads.
    pub fn use_page_index(&self) -> bool {
        match self {
            ReadStrategy::PageLevel | ReadStrategy::CoalescedPageLevel => true,
            ReadStrategy::FullColumnChunk | ReadStrategy::FullRowGroup => false,
        }
    }
}

/// Compute selectivity: fraction of file rows selected.
pub fn compute_selectivity(selected_rows: usize, total_rows: usize) -> f64 {
    if total_rows == 0 {
        return 1.0;
    }
    selected_rows as f64 / total_rows as f64
}

/// Per-row-group strategy selection for mixed-selectivity files.
///
/// A file with 10% overall selectivity might have 90% in one row group
/// and 0% in the rest. The row-group filter handles the 0% case (skips
/// entirely). For the 90% row group, we use FullRowGroup even though
/// the file average is only 10%.
pub fn strategy_for_row_group(rows_in_rg: usize, rg_total_rows: usize) -> ReadStrategy {
    let rg_selectivity = compute_selectivity(rows_in_rg, rg_total_rows);
    ReadStrategy::for_selectivity(rg_selectivity)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_selectivity_thresholds() {
        assert_eq!(ReadStrategy::for_selectivity(0.01), ReadStrategy::PageLevel);
        assert_eq!(ReadStrategy::for_selectivity(0.04), ReadStrategy::PageLevel);
        assert_eq!(
            ReadStrategy::for_selectivity(0.06),
            ReadStrategy::CoalescedPageLevel
        );
        assert_eq!(
            ReadStrategy::for_selectivity(0.24),
            ReadStrategy::CoalescedPageLevel
        );
        assert_eq!(
            ReadStrategy::for_selectivity(0.30),
            ReadStrategy::FullColumnChunk
        );
        assert_eq!(
            ReadStrategy::for_selectivity(0.49),
            ReadStrategy::FullColumnChunk
        );
        assert_eq!(
            ReadStrategy::for_selectivity(0.51),
            ReadStrategy::FullRowGroup
        );
        assert_eq!(
            ReadStrategy::for_selectivity(1.0),
            ReadStrategy::FullRowGroup
        );
    }

    #[test]
    fn test_boundary_values() {
        // Exact boundary: 0.05 is PageLevel (not >0.05)
        assert_eq!(ReadStrategy::for_selectivity(0.05), ReadStrategy::PageLevel);
        // Just above
        assert_eq!(
            ReadStrategy::for_selectivity(0.0501),
            ReadStrategy::CoalescedPageLevel
        );
        // 0.25 boundary
        assert_eq!(
            ReadStrategy::for_selectivity(0.25),
            ReadStrategy::CoalescedPageLevel
        );
        assert_eq!(
            ReadStrategy::for_selectivity(0.2501),
            ReadStrategy::FullColumnChunk
        );
        // 0.50 boundary
        assert_eq!(
            ReadStrategy::for_selectivity(0.50),
            ReadStrategy::FullColumnChunk
        );
        assert_eq!(
            ReadStrategy::for_selectivity(0.5001),
            ReadStrategy::FullRowGroup
        );
    }

    #[test]
    fn test_coalesce_config_scaling() {
        let base = CoalesceConfig {
            max_gap: 512 * 1024,
            max_total: 8 * 1024 * 1024,
        };

        let page = ReadStrategy::PageLevel.coalesce_config(base);
        assert_eq!(page.max_gap, 512 * 1024);

        let coalesced = ReadStrategy::CoalescedPageLevel.coalesce_config(base);
        assert_eq!(coalesced.max_gap, 1024 * 1024);

        let full_col = ReadStrategy::FullColumnChunk.coalesce_config(base);
        assert_eq!(full_col.max_gap, 4 * 1024 * 1024);

        let full_rg = ReadStrategy::FullRowGroup.coalesce_config(base);
        assert_eq!(full_rg.max_gap, 4 * 1024 * 1024);
        assert_eq!(full_rg.max_total, 64 * 1024 * 1024);
    }

    #[test]
    fn test_use_row_selection() {
        assert!(ReadStrategy::PageLevel.use_row_selection());
        assert!(ReadStrategy::CoalescedPageLevel.use_row_selection());
        assert!(ReadStrategy::FullColumnChunk.use_row_selection());
        assert!(!ReadStrategy::FullRowGroup.use_row_selection());
    }

    #[test]
    fn test_use_page_index() {
        assert!(ReadStrategy::PageLevel.use_page_index());
        assert!(ReadStrategy::CoalescedPageLevel.use_page_index());
        assert!(!ReadStrategy::FullColumnChunk.use_page_index());
        assert!(!ReadStrategy::FullRowGroup.use_page_index());
    }

    #[test]
    fn test_compute_selectivity() {
        assert_eq!(compute_selectivity(0, 1000), 0.0);
        assert_eq!(compute_selectivity(100, 1000), 0.1);
        assert_eq!(compute_selectivity(500, 1000), 0.5);
        assert_eq!(compute_selectivity(1000, 1000), 1.0);
        // Edge case: empty file
        assert_eq!(compute_selectivity(0, 0), 1.0);
    }

    #[test]
    fn test_strategy_for_row_group() {
        // A row group where we need 90% of rows
        assert_eq!(
            strategy_for_row_group(900, 1000),
            ReadStrategy::FullRowGroup
        );
        // A row group where we need 1% of rows
        assert_eq!(
            strategy_for_row_group(10, 1000),
            ReadStrategy::PageLevel
        );
    }
}
