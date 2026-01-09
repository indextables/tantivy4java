// direct.rs - Direct range-based cache management using actual document list

use std::sync::Arc;

use crate::debug_println;

use super::cache::QuickwitPersistentCacheManager;
use super::errors::BatchRetrievalError;
use super::types::{DocAddress, DocumentRange};

/// Configuration for direct range-based cache management
#[derive(Debug, Clone)]
pub struct DirectCacheConfig {
    /// Enable extended range caching (default: true)
    pub enable_extended_ranges: bool,

    /// Extended range padding (default: 128KB each direction)
    pub extended_range_padding: usize,

    /// Maximum gap to bridge between documents (default: 64KB)
    pub max_gap_bridge: usize,
}

impl Default for DirectCacheConfig {
    fn default() -> Self {
        Self {
            enable_extended_ranges: true,
            extended_range_padding: 128 * 1024, // 128KB
            max_gap_bridge: 64 * 1024,          // 64KB
        }
    }
}

/// Direct range-based cache management using actual document list
pub struct DirectRangeCacheManager {
    #[allow(dead_code)]
    cache_manager: Arc<QuickwitPersistentCacheManager>,
    config: DirectCacheConfig,
}

impl DirectRangeCacheManager {
    pub fn new(
        cache_manager: Arc<QuickwitPersistentCacheManager>,
        config: DirectCacheConfig,
    ) -> Self {
        Self {
            cache_manager,
            config,
        }
    }

    /// Cache management using the actual document list from batch request
    pub async fn prepare_ranges_for_documents(
        &self,
        doc_addresses: &[DocAddress],
        doc_position_fn: impl Fn(&DocAddress) -> usize,
        doc_size_fn: impl Fn(&DocAddress) -> usize,
    ) -> Result<Vec<DocumentRange>, BatchRetrievalError> {
        debug_println!(
            "📋 DIRECT_RANGE_PREP: Processing {} documents from batch request",
            doc_addresses.len()
        );

        // Sort documents by their byte positions
        let mut sorted_docs: Vec<_> = doc_addresses.iter().copied().collect();
        sorted_docs.sort_by_key(|addr| doc_position_fn(addr));

        // Group into ranges - no prediction needed, we have the exact list!
        let ranges =
            self.create_optimal_ranges_from_document_list(&sorted_docs, &doc_position_fn, &doc_size_fn);

        debug_println!(
            "📊 RANGE_OPTIMIZATION: {} documents consolidated into {} ranges",
            doc_addresses.len(),
            ranges.len()
        );

        // Optionally extend ranges to include nearby documents for future requests
        let extended_ranges = if self.config.enable_extended_ranges {
            self.extend_ranges_for_locality(&ranges)
        } else {
            ranges
        };

        Ok(extended_ranges)
    }

    /// Create optimal ranges from the exact document list (no guessing!)
    fn create_optimal_ranges_from_document_list(
        &self,
        sorted_docs: &[DocAddress],
        doc_position_fn: &impl Fn(&DocAddress) -> usize,
        doc_size_fn: &impl Fn(&DocAddress) -> usize,
    ) -> Vec<DocumentRange> {
        let mut ranges = Vec::new();
        let mut current_range_docs = Vec::new();
        let mut current_range_start = 0usize;
        let mut current_range_size = 0usize;

        for &doc_addr in sorted_docs {
            let doc_size = doc_size_fn(&doc_addr);
            let doc_position = doc_position_fn(&doc_addr);

            // Calculate gap from previous document
            let gap_size = if let Some(&last_doc) = current_range_docs.last() {
                let last_end = doc_position_fn(&last_doc) + doc_size_fn(&last_doc);
                doc_position.saturating_sub(last_end)
            } else {
                0
            };

            let new_range_size = current_range_size + doc_size + gap_size;

            // Decision: start new range if size exceeded or gap too large
            let should_start_new_range = !current_range_docs.is_empty()
                && (new_range_size > 8 * 1024 * 1024 ||  // 8MB max
                    gap_size > self.config.max_gap_bridge);

            if should_start_new_range {
                // Finalize current range (we know these docs will be accessed)
                ranges.push(self.create_document_range_from_docs(
                    current_range_docs.clone(),
                    doc_position_fn,
                    doc_size_fn,
                ));

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
            ranges.push(self.create_document_range_from_docs(
                current_range_docs,
                doc_position_fn,
                doc_size_fn,
            ));
        }

        let _ = current_range_start; // Silence unused warning
        ranges
    }

    /// Create document range from a list of documents
    fn create_document_range_from_docs(
        &self,
        documents: Vec<DocAddress>,
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

    /// Optionally extend ranges to include nearby documents for cache locality
    fn extend_ranges_for_locality(&self, ranges: &[DocumentRange]) -> Vec<DocumentRange> {
        if !self.config.enable_extended_ranges {
            return ranges.to_vec();
        }

        ranges
            .iter()
            .map(|range| {
                let extended_start = range
                    .byte_start
                    .saturating_sub(self.config.extended_range_padding);
                let extended_end = range.byte_end + self.config.extended_range_padding;

                debug_println!(
                    "🔍 RANGE_EXTENSION: Extended {}..{} to {}..{} (+{} KB padding)",
                    range.byte_start,
                    range.byte_end,
                    extended_start,
                    extended_end,
                    self.config.extended_range_padding / 1024
                );

                DocumentRange {
                    start_address: range.start_address,
                    end_address: range.end_address,
                    byte_start: extended_start,
                    byte_end: extended_end,
                    documents: range.documents.clone(),
                }
            })
            .collect()
    }
}
