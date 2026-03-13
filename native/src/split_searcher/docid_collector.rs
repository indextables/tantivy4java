// docid_collector.rs - No-score bulk document ID collector
//
// Custom tantivy Collector that collects (segment_ord, doc_id) pairs without
// computing BM25 scores. Uses tantivy's no-score batch path (collect_block
// with 64-doc chunks) for maximum throughput.
//
// Memory: 8 bytes per hit (2x u32). 2M hits = 16MB.
//
// This collector is the foundation for both:
// - Fused search+retrieve (Phase 0): small-to-medium companion queries
// - Streaming retrieval (Phase 3): large result set companion queries

use tantivy::collector::{Collector, SegmentCollector};
use tantivy::{DocId, Score, SegmentOrdinal, SegmentReader};

/// Collector that returns all matching doc addresses without scoring.
///
/// Returns `requires_scoring() = false`, which tells tantivy to:
/// 1. Skip BM25 term-frequency decompression (biggest CPU savings)
/// 2. Use `for_each_no_score()` → `collect_block()` with 64-doc batches
/// 3. Pass score=0.0 (ignored)
///
/// Use this instead of Quickwit's leaf_search path when you need all matching
/// doc IDs and don't care about relevance scores (companion mode row retrieval).
pub struct DocIdCollector;

impl Collector for DocIdCollector {
    type Fruit = Vec<(u32, u32)>; // (segment_ord, doc_id)
    type Child = DocIdSegmentCollector;

    fn for_segment(
        &self,
        segment_local_id: SegmentOrdinal,
        _reader: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        Ok(DocIdSegmentCollector {
            segment_ord: segment_local_id,
            doc_ids: Vec::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<Vec<(u32, u32)>>,
    ) -> tantivy::Result<Self::Fruit> {
        let total: usize = segment_fruits.iter().map(|v| v.len()).sum();
        let mut result = Vec::with_capacity(total);
        for fruit in segment_fruits {
            result.extend(fruit);
        }
        Ok(result)
    }
}

pub struct DocIdSegmentCollector {
    segment_ord: u32,
    doc_ids: Vec<(u32, u32)>,
}

impl SegmentCollector for DocIdSegmentCollector {
    type Fruit = Vec<(u32, u32)>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        self.doc_ids.push((self.segment_ord, doc));
    }

    /// Batch collection — called with up to 64 doc IDs at a time when
    /// requires_scoring() is false and no alive_bitset filtering is needed.
    fn collect_block(&mut self, docs: &[DocId]) {
        self.doc_ids.reserve(docs.len());
        for &doc in docs {
            self.doc_ids.push((self.segment_ord, doc));
        }
    }

    fn harvest(self) -> Vec<(u32, u32)> {
        self.doc_ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_docid_collector_requires_no_scoring() {
        let collector = DocIdCollector;
        assert!(!collector.requires_scoring());
    }

    #[test]
    fn test_segment_collector_collect() {
        let mut seg_collector = DocIdSegmentCollector {
            segment_ord: 2,
            doc_ids: Vec::new(),
        };
        seg_collector.collect(10, 0.0);
        seg_collector.collect(20, 1.5); // score ignored
        seg_collector.collect(30, 0.0);

        let result = seg_collector.harvest();
        assert_eq!(result, vec![(2, 10), (2, 20), (2, 30)]);
    }

    #[test]
    fn test_segment_collector_collect_block() {
        let mut seg_collector = DocIdSegmentCollector {
            segment_ord: 0,
            doc_ids: Vec::new(),
        };
        seg_collector.collect_block(&[100, 101, 102, 103]);
        seg_collector.collect_block(&[200, 201]);

        let result = seg_collector.harvest();
        assert_eq!(
            result,
            vec![(0, 100), (0, 101), (0, 102), (0, 103), (0, 200), (0, 201)]
        );
    }

    #[test]
    fn test_merge_fruits_combines_segments() {
        let collector = DocIdCollector;
        let fruits = vec![
            vec![(0, 10), (0, 20)],
            vec![(1, 5), (1, 15), (1, 25)],
            vec![(2, 100)],
        ];
        let merged = collector.merge_fruits(fruits).unwrap();
        assert_eq!(merged.len(), 6);
        assert_eq!(merged[0], (0, 10));
        assert_eq!(merged[2], (1, 5));
        assert_eq!(merged[5], (2, 100));
    }

    #[test]
    fn test_merge_fruits_empty() {
        let collector = DocIdCollector;
        let fruits: Vec<Vec<(u32, u32)>> = vec![vec![], vec![], vec![]];
        let merged = collector.merge_fruits(fruits).unwrap();
        assert!(merged.is_empty());
    }

}
