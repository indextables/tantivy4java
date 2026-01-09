// errors.rs - Error types and result structs for batch retrieval

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Error types for batch retrieval operations
#[derive(Debug)]
pub enum BatchRetrievalError {
    Storage(quickwit_storage::StorageError),
    Cache(String),
    DocumentAccess(String),
    Configuration(String),
    Persistence(String),
}

impl std::fmt::Display for BatchRetrievalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BatchRetrievalError::Storage(e) => write!(f, "Storage error: {}", e),
            BatchRetrievalError::Cache(e) => write!(f, "Cache error: {}", e),
            BatchRetrievalError::DocumentAccess(e) => write!(f, "Document retrieval error: {}", e),
            BatchRetrievalError::Configuration(e) => write!(f, "Configuration error: {}", e),
            BatchRetrievalError::Persistence(e) => write!(f, "Persistence error: {}", e),
        }
    }
}

impl std::error::Error for BatchRetrievalError {}

impl From<quickwit_storage::StorageError> for BatchRetrievalError {
    fn from(error: quickwit_storage::StorageError) -> Self {
        BatchRetrievalError::Storage(error)
    }
}

/// Performance metrics for batch retrieval operations
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct BatchRetrievalMetrics {
    pub total_batches: u64,
    pub optimized_batches: u64,
    pub total_documents: u64,
    pub total_ranges: u64,

    // Cache performance
    pub l1_cache_hits: u64, // Memory cache
    pub l2_cache_hits: u64, // Disk cache
    pub cache_misses: u64,  // Network fetches

    // Timing breakdown
    pub total_time_ms: u64,
    pub consolidation_time_ms: u64,
    pub network_time_ms: u64,
    pub cache_time_ms: u64,
    pub persistence_time_ms: u64,

    // Compression metrics
    pub compression_ratio: f64,
    pub compression_time_ms: u64,
    pub decompression_time_ms: u64,
}

impl BatchRetrievalMetrics {
    pub fn cache_hit_ratio(&self) -> f64 {
        let total_requests = self.l1_cache_hits + self.l2_cache_hits + self.cache_misses;
        if total_requests == 0 {
            return 0.0;
        }
        (self.l1_cache_hits + self.l2_cache_hits) as f64 / total_requests as f64
    }

    pub fn l1_hit_ratio(&self) -> f64 {
        let total_requests = self.l1_cache_hits + self.l2_cache_hits + self.cache_misses;
        if total_requests == 0 {
            return 0.0;
        }
        self.l1_cache_hits as f64 / total_requests as f64
    }

    pub fn average_batch_size(&self) -> f64 {
        if self.total_batches == 0 {
            return 0.0;
        }
        self.total_documents as f64 / self.total_batches as f64
    }
}

/// Result of a batch document retrieval operation
#[derive(Debug)]
pub struct BatchRetrievalResult {
    pub documents: Vec<tantivy::schema::TantivyDocument>,
    pub optimization_used: bool,
    pub total_time: Duration,
    pub phases: Vec<(&'static str, Duration)>,
    pub cache_stats: Option<CacheOperationResult>,
    pub range_count: usize,
}

/// Result of cache operations during batch retrieval
#[derive(Debug)]
pub struct CacheOperationResult {
    pub cache_hits: usize,
    pub cache_misses: usize,
    pub ranges_fetched: usize,
    pub bytes_cached: usize,
}
