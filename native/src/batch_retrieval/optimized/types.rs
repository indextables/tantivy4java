// types.rs - Core types for optimized batch retrieval

/// Document address for identifying specific documents within a split
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DocAddress {
    pub segment_ord: u32,
    pub doc_id: u32,
}

impl DocAddress {
    pub fn new(segment_ord: u32, doc_id: u32) -> Self {
        Self { segment_ord, doc_id }
    }
}

/// Represents a contiguous range of documents for batch fetching
#[derive(Debug, Clone)]
pub struct DocumentRange {
    pub start_address: DocAddress,
    pub end_address: DocAddress,
    pub byte_start: usize,
    pub byte_end: usize,
    pub documents: Vec<DocAddress>,
}

impl DocumentRange {
    pub fn size_bytes(&self) -> usize {
        self.byte_end.saturating_sub(self.byte_start)
    }

    pub fn document_count(&self) -> usize {
        self.documents.len()
    }

    pub fn is_single_document(&self) -> bool {
        self.documents.len() == 1
    }
}

/// Configuration for smart range consolidation
#[derive(Debug, Clone)]
pub struct SmartRangeConfig {
    /// Maximum size for a single range (default: 8MB)
    pub max_range_size: usize,

    /// Maximum gap to bridge between documents (default: 64KB)
    pub gap_tolerance: usize,

    /// Minimum documents per range to be worth consolidating (default: 2)
    pub min_docs_per_range: usize,
}

impl Default for SmartRangeConfig {
    fn default() -> Self {
        Self {
            max_range_size: 8 * 1024 * 1024, // 8MB
            gap_tolerance: 64 * 1024,        // 64KB
            min_docs_per_range: 2,
        }
    }
}
