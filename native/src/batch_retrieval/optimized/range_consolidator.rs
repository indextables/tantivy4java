// range_consolidator.rs - Smart range consolidation for batch retrieval

use crate::debug_println;

use super::types::{DocAddress, DocumentRange, SmartRangeConfig};

/// Smart range consolidator for intelligent document grouping
pub struct SmartRangeConsolidator {
    config: SmartRangeConfig,
}

impl SmartRangeConsolidator {
    pub fn new(config: SmartRangeConfig) -> Self {
        Self { config }
    }

    /// Consolidate documents into optimal ranges for parallel fetching
    pub fn consolidate_into_ranges(
        &self,
        doc_addresses: Vec<DocAddress>,
        doc_position_fn: impl Fn(&DocAddress) -> usize,
        doc_size_fn: impl Fn(&DocAddress) -> usize,
    ) -> Vec<DocumentRange> {
        debug_println!(
            "📋 RANGE_CONSOLIDATION: Processing {} documents",
            doc_addresses.len()
        );

        if doc_addresses.is_empty() {
            return Vec::new();
        }

        // Sort by byte position for locality
        let mut sorted_docs = doc_addresses;
        sorted_docs.sort_by_key(|addr| doc_position_fn(addr));

        let mut ranges = Vec::new();
        let mut current_range_docs = Vec::new();
        let mut current_range_start = 0usize;
        let mut current_range_size = 0usize;

        let _doc_count = sorted_docs.len();
        for doc_addr in sorted_docs.iter().copied() {
            let doc_size = doc_size_fn(&doc_addr);
            let doc_position = doc_position_fn(&doc_addr);

            // Calculate gap from previous document
            let gap_size = if let Some(last_doc) = current_range_docs.last() {
                let last_end = doc_position_fn(last_doc) + doc_size_fn(last_doc);
                doc_position.saturating_sub(last_end)
            } else {
                0
            };

            let new_range_size = current_range_size + doc_size + gap_size;

            // Decide whether to add to current range or start new one
            let should_start_new_range = !current_range_docs.is_empty()
                && (new_range_size > self.config.max_range_size
                    || gap_size > self.config.gap_tolerance);

            if should_start_new_range {
                // Finalize current range if it has enough documents
                if current_range_docs.len() >= self.config.min_docs_per_range {
                    ranges.push(self.create_document_range(
                        current_range_docs.clone(),
                        current_range_start,
                        &doc_position_fn,
                        &doc_size_fn,
                    ));
                } else {
                    // Add individual documents as single-doc ranges
                    for single_doc in current_range_docs {
                        ranges.push(self.create_single_document_range(
                            single_doc,
                            &doc_position_fn,
                            &doc_size_fn,
                        ));
                    }
                }

                // Start new range
                current_range_docs = vec![doc_addr];
                current_range_start = doc_position;
                current_range_size = doc_size;
            } else {
                // Add to current range
                if current_range_docs.is_empty() {
                    current_range_start = doc_position;
                }
                current_range_docs.push(doc_addr);
                current_range_size = new_range_size;
            }
        }

        // Handle final range
        if !current_range_docs.is_empty() {
            if current_range_docs.len() >= self.config.min_docs_per_range {
                ranges.push(self.create_document_range(
                    current_range_docs,
                    current_range_start,
                    &doc_position_fn,
                    &doc_size_fn,
                ));
            } else {
                for single_doc in current_range_docs {
                    ranges.push(self.create_single_document_range(
                        single_doc,
                        &doc_position_fn,
                        &doc_size_fn,
                    ));
                }
            }
        }

        debug_println!(
            "📊 CONSOLIDATION_RESULT: {} documents -> {} ranges",
            sorted_docs.len(),
            ranges.len()
        );

        // Log range statistics
        for (i, range) in ranges.iter().enumerate() {
            debug_println!(
                "  📦 Range {}: {} docs, {} bytes ({}..{})",
                i + 1,
                range.document_count(),
                range.size_bytes(),
                range.byte_start,
                range.byte_end
            );
        }

        ranges
    }

    /// Create a document range from multiple documents
    fn create_document_range(
        &self,
        documents: Vec<DocAddress>,
        _range_start: usize,
        doc_position_fn: &impl Fn(&DocAddress) -> usize,
        doc_size_fn: &impl Fn(&DocAddress) -> usize,
    ) -> DocumentRange {
        let start_address = documents[0];
        let end_address = documents[documents.len() - 1];

        // Calculate the actual byte range spanning all documents
        let byte_start = doc_position_fn(&start_address);
        let last_doc_pos = doc_position_fn(&end_address);
        let last_doc_size = doc_size_fn(&end_address);
        let byte_end = last_doc_pos + last_doc_size;

        DocumentRange {
            start_address,
            end_address,
            byte_start,
            byte_end,
            documents,
        }
    }

    /// Create a single document range
    fn create_single_document_range(
        &self,
        doc_addr: DocAddress,
        doc_position_fn: &impl Fn(&DocAddress) -> usize,
        doc_size_fn: &impl Fn(&DocAddress) -> usize,
    ) -> DocumentRange {
        let byte_start = doc_position_fn(&doc_addr);
        let byte_end = byte_start + doc_size_fn(&doc_addr);

        DocumentRange {
            start_address: doc_addr,
            end_address: doc_addr,
            byte_start,
            byte_end,
            documents: vec![doc_addr],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_smart_range_consolidator_basic() {
        let consolidator = SmartRangeConsolidator::new(SmartRangeConfig::default());

        let docs = vec![
            DocAddress::new(0, 0),
            DocAddress::new(0, 1),
            DocAddress::new(0, 2),
        ];

        let position_fn = |addr: &DocAddress| (addr.doc_id * 1000) as usize;
        let size_fn = |_addr: &DocAddress| 500usize;

        let ranges = consolidator.consolidate_into_ranges(docs, position_fn, size_fn);

        // Should consolidate into a single range since documents are contiguous
        assert_eq!(ranges.len(), 1);
        assert_eq!(ranges[0].documents.len(), 3);
    }

    #[test]
    fn test_smart_range_consolidator_gap_tolerance() {
        let mut config = SmartRangeConfig::default();
        config.gap_tolerance = 100; // Very small gap tolerance

        let consolidator = SmartRangeConsolidator::new(config);

        let docs = vec![
            DocAddress::new(0, 0),
            DocAddress::new(0, 1),
            DocAddress::new(0, 10), // Large gap
        ];

        let position_fn = |addr: &DocAddress| (addr.doc_id * 1000) as usize;
        let size_fn = |_addr: &DocAddress| 100usize;

        let ranges = consolidator.consolidate_into_ranges(docs, position_fn, size_fn);

        // Should split into multiple ranges due to large gap
        assert!(ranges.len() > 1);
    }
}
